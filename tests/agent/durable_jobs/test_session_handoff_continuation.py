from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import sqlite3

import pytest

from agent.durable_jobs.clock import FrozenClock
from agent.durable_jobs.session_handoff_continuation import (
    ContinuationLeaseLost,
    ContinuationStore,
    DeliveryVerificationFailed,
)


def _digest(receipt: bytes) -> str:
    return hashlib.sha256(receipt).hexdigest()


def _store(tmp_path, clock: FrozenClock) -> ContinuationStore:
    return ContinuationStore(tmp_path / "continuations.sqlite3", now_fn=clock)


def _due(store: ContinuationStore):
    return store.enqueue(
        job_id="job-1",
        handoff_id="handoff-1",
        checkpoint_stage="DELIVER",
        next_action="deliver_handoff",
    )


def _clock_requiring_immediate_transaction(path, instant: str):
    def now() -> str:
        contender = sqlite3.connect(path, timeout=0)
        try:
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                contender.execute("BEGIN IMMEDIATE")
        finally:
            contender.close()
        return instant

    return now


class _AfterTransactionConnection(sqlite3.Connection):
    after_transaction = None

    def __exit__(self, exc_type, exc_value, traceback):
        result = super().__exit__(exc_type, exc_value, traceback)
        callback, self.after_transaction = self.after_transaction, None
        if callback is not None:
            callback()
        return result


def _force_after_transaction(store: ContinuationStore, callback) -> None:
    def connect():
        connection = sqlite3.connect(
            store.path, timeout=30, factory=_AfterTransactionConnection
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.after_transaction = callback
        return connection

    store._connect = connect


def test_existing_sqlite_store_migrates_scope_columns_without_inference(tmp_path):
    path = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE session_handoff_continuations ("
            "job_id TEXT NOT NULL, handoff_id TEXT NOT NULL, "
            "checkpoint_stage TEXT NOT NULL, next_action TEXT NOT NULL, "
            "due_at TEXT NOT NULL, owner_token TEXT, "
            "owner_generation INTEGER NOT NULL DEFAULT 0, lease_expires_at TEXT, "
            "heartbeat_at TEXT, verification_state TEXT NOT NULL DEFAULT 'PENDING', "
            "manual_resume_reason TEXT, manual_resume_operator_reason TEXT, "
            "wake_state TEXT NOT NULL DEFAULT 'DUE', PRIMARY KEY (job_id, handoff_id))"
        )
        connection.execute(
            "INSERT INTO session_handoff_continuations "
            "(job_id,handoff_id,checkpoint_stage,next_action,due_at) "
            "VALUES ('request-looking-job','handoff','DELIVER','deliver_handoff',"
            "'2026-01-01T00:00:00+00:00')"
        )

    store = ContinuationStore(path)

    migrated = store.get("request-looking-job", "handoff")
    assert migrated.request_id is None and migrated.session_id is None
    assert store.claim_due_scoped(
        request_id="request-looking-job",
        session_id="session",
        owner_token="owner",
        lease_seconds=10,
    ) is None


def test_scoped_claim_atomically_selects_only_exact_request_and_session(tmp_path):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    store = _store(tmp_path, clock)
    for job_id, request_id, session_id in (
        ("foreign-request", "request-2", "session-1"),
        ("foreign-session", "request-1", "session-2"),
        ("authorized", "request-1", "session-1"),
    ):
        store.enqueue(
            job_id=job_id,
            handoff_id="handoff",
            checkpoint_stage="DELIVER",
            next_action="deliver_handoff",
            request_id=request_id,
            session_id=session_id,
        )

    claim = store.claim_due_scoped(
        request_id="request-1",
        session_id="session-1",
        owner_token="owner",
        lease_seconds=10,
    )

    assert claim is not None and claim.job_id == "authorized"
    assert store.get("foreign-request", "handoff").owner_token is None
    assert store.get("foreign-session", "handoff").owner_token is None
    assert store.claim_due(owner_token="legacy-owner", lease_seconds=10) is None


def test_claim_samples_due_and_expiry_time_after_begin_immediate(tmp_path):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    store = _store(tmp_path, clock)
    _due(store)
    store._now_fn = _clock_requiring_immediate_transaction(store.path, clock())

    claim = store.claim_due(owner_token="process-a", lease_seconds=10)

    assert claim is not None


def test_renew_samples_expiry_time_once_after_begin_immediate(tmp_path):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    store = _store(tmp_path, clock)
    _due(store)
    claim = store.claim_due(owner_token="process-a", lease_seconds=10)
    assert claim is not None
    calls = 0
    transaction_clock = _clock_requiring_immediate_transaction(store.path, clock())

    def counted_now() -> str:
        nonlocal calls
        calls += 1
        return transaction_clock()

    store._now_fn = counted_now
    renewed = store.renew(claim, lease_seconds=10)

    assert renewed.owner_generation == claim.owner_generation
    assert calls == 1


def test_due_checkpoint_is_reclaimed_after_lease_expiry_and_resumes_persisted_next_action(
    tmp_path,
):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    store = _store(tmp_path, clock)
    _due(store)
    first = store.claim_due(owner_token="process-a", lease_seconds=10)
    assert first is not None and first.next_action == "deliver_handoff"
    clock.advance(11)
    second = _store(tmp_path, clock).claim_due(
        owner_token="process-b", lease_seconds=10
    )
    assert second is not None and second.next_action == "deliver_handoff"
    assert second.owner_generation == first.owner_generation + 1


def test_live_heartbeat_fences_second_process_and_old_generation(tmp_path):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    store = _store(tmp_path, clock)
    _due(store)
    first = store.claim_due(owner_token="process-a", lease_seconds=10)
    assert first is not None
    clock.advance(5)
    renewed = store.renew(first, lease_seconds=10)
    assert renewed.heartbeat_at == clock()
    assert (
        _store(tmp_path, clock).claim_due(owner_token="process-b", lease_seconds=10)
        is None
    )
    clock.advance(11)
    successor = _store(tmp_path, clock).claim_due(
        owner_token="process-b", lease_seconds=10
    )
    assert successor is not None
    with pytest.raises(ContinuationLeaseLost):
        store.renew(first, lease_seconds=10)


def test_stale_owner_takeover_preserves_verified_effect_and_fences_old_owner(tmp_path):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    store = _store(tmp_path, clock)
    _due(store)
    first = store.claim_due(owner_token="process-a", lease_seconds=10)
    assert first is not None
    store.record_verified_effect(
        first, effect_name="handoff_delivery", receipt_sha256=_digest(b"receipt-1")
    )
    persisted = _store(tmp_path, clock).get("job-1", "handoff-1")
    assert persisted.checkpoint_stage == "VERIFY_HANDOFF_DELIVERY"
    assert persisted.next_action == "verify_handoff_delivery"
    assert persisted.verification_state == "RECEIPT_DIGEST_PERSISTED"
    clock.advance(11)
    second = _store(tmp_path, clock).claim_due(
        owner_token="process-b", lease_seconds=10
    )
    assert second is not None and second.next_action == "verify_handoff_delivery"
    assert store.effect_is_verified(second, effect_name="handoff_delivery") is True
    with pytest.raises(ContinuationLeaseLost):
        store.record_verified_effect(
            first, effect_name="handoff_delivery", receipt_sha256=_digest(b"receipt-2")
        )


def test_delivery_cannot_complete_without_persisted_receipt_digest_verification(
    tmp_path,
):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    store = _store(tmp_path, clock)
    _due(store)
    claim = store.claim_due(owner_token="process-a", lease_seconds=10)
    assert claim is not None
    with pytest.raises(DeliveryVerificationFailed):
        store.complete_delivery(claim, observed_receipt_sha256=_digest(b"receipt-1"))
    verified = store.record_verified_effect(
        claim, effect_name="handoff_delivery", receipt_sha256=_digest(b"receipt-1")
    )
    completed = store.complete_delivery(
        verified, observed_receipt_sha256=_digest(b"receipt-1")
    )
    assert (
        completed.verification_state == "VERIFIED"
        and completed.wake_state == "COMPLETE"
    )
    reopened = _store(tmp_path, clock).get("job-1", "handoff-1")
    assert reopened.checkpoint_stage == "COMPLETE"
    assert reopened.wake_state == "COMPLETE"
    assert reopened.next_action == "complete"


def test_failed_delivery_verification_persists_manual_resume_and_is_not_rewoken(
    tmp_path,
):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    store = _store(tmp_path, clock)
    _due(store)
    claim = store.claim_due(owner_token="process-a", lease_seconds=10)
    assert claim is not None
    claim = store.record_verified_effect(
        claim, effect_name="handoff_delivery", receipt_sha256=_digest(b"receipt-1")
    )
    with pytest.raises(DeliveryVerificationFailed):
        store.complete_delivery(claim, observed_receipt_sha256=_digest(b"different"))
    persisted = store.get("job-1", "handoff-1")
    assert persisted.verification_state == "MANUAL_RESUME"
    assert persisted.manual_resume_reason == "delivery_receipt_digest_mismatch"
    clock.advance(100)
    assert (
        _store(tmp_path, clock).claim_due(owner_token="process-b", lease_seconds=10)
        is None
    )


def test_unrelated_effect_is_rejected_without_advancing_delivery_state(tmp_path):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    store = _store(tmp_path, clock)
    _due(store)
    claim = store.claim_due(owner_token="process-a", lease_seconds=10)
    assert claim is not None

    with pytest.raises(ValueError, match="only the handoff_delivery"):
        store.record_verified_effect(
            claim,
            effect_name="unrelated_effect",
            receipt_sha256=_digest(b"safe offline receipt"),
        )

    persisted = store.get("job-1", "handoff-1")
    assert persisted.next_action == "deliver_handoff"
    assert persisted.verification_state == "PENDING"
    assert store.effect_is_verified(claim, effect_name="unrelated_effect") is False


def test_receipt_values_must_be_lowercase_sha256_digests(tmp_path):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    store = _store(tmp_path, clock)
    _due(store)
    claim = store.claim_due(owner_token="process-a", lease_seconds=10)
    assert claim is not None

    for forbidden_value in ("provider response", "g" * 64, "A" * 64):
        with pytest.raises(ValueError, match="lowercase 64-hex"):
            store.record_verified_effect(
                claim,
                effect_name="handoff_delivery",
                receipt_sha256=forbidden_value,
            )


def test_manual_resume_is_fail_closed_then_valid_resume_can_be_claimed_and_completed(
    tmp_path,
):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    store = _store(tmp_path, clock)
    _due(store)
    claim = store.claim_due(owner_token="process-a", lease_seconds=10)
    assert claim is not None
    receipt_digest = _digest(b"safe offline receipt")
    claim = store.record_verified_effect(
        claim,
        effect_name="handoff_delivery",
        receipt_sha256=receipt_digest,
    )
    with pytest.raises(DeliveryVerificationFailed):
        store.complete_delivery(
            claim, observed_receipt_sha256=_digest(b"wrong receipt")
        )

    clock.advance(100)
    assert store.claim_due(owner_token="automatic", lease_seconds=10) is None
    with pytest.raises(ValueError, match="nonempty operator reason"):
        store.resume_after_manual_verification(
            job_id="job-1",
            handoff_id="handoff-1",
            operator_reason="   ",
            confirmed_receipt_sha256=receipt_digest,
        )
    with pytest.raises(DeliveryVerificationFailed, match="does not match"):
        store.resume_after_manual_verification(
            job_id="job-1",
            handoff_id="handoff-1",
            operator_reason="receipt inspected offline",
            confirmed_receipt_sha256=_digest(b"wrong receipt"),
        )
    still_blocked = store.get("job-1", "handoff-1")
    assert still_blocked.wake_state == "MANUAL_RESUME"
    assert still_blocked.manual_resume_operator_reason is None
    assert still_blocked.manual_resume_reason == "delivery_receipt_digest_mismatch"

    resumed = store.resume_after_manual_verification(
        job_id="job-1",
        handoff_id="handoff-1",
        operator_reason=" receipt inspected offline ",
        confirmed_receipt_sha256=receipt_digest,
    )
    assert resumed.wake_state == "DUE"
    assert resumed.checkpoint_stage == "VERIFY_HANDOFF_DELIVERY"
    assert resumed.next_action == "verify_handoff_delivery"
    assert resumed.manual_resume_reason == "delivery_receipt_digest_mismatch"
    assert resumed.manual_resume_operator_reason == "receipt inspected offline"
    reacquired = store.claim_due(owner_token="process-b", lease_seconds=10)
    assert reacquired is not None
    assert reacquired.owner_generation == claim.owner_generation + 1
    with pytest.raises(DeliveryVerificationFailed, match="immutable"):
        store.record_verified_effect(
            reacquired,
            effect_name="handoff_delivery",
            receipt_sha256=_digest(b"replacement evidence"),
        )
    completed = store.complete_delivery(
        reacquired, observed_receipt_sha256=receipt_digest
    )
    assert completed.wake_state == "COMPLETE"


def test_claim_returns_transaction_snapshot_not_successor_capability(tmp_path):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    store = _store(tmp_path, clock)
    _due(store)
    successor_store = _store(tmp_path, clock)

    def reclaim():
        clock.advance(11)
        assert successor_store.claim_due(owner_token="process-b", lease_seconds=10)

    _force_after_transaction(store, reclaim)
    claim = store.claim_due(owner_token="process-a", lease_seconds=10)

    assert claim is not None
    assert (claim.owner_token, claim.owner_generation) == ("process-a", 1)
    assert successor_store.get("job-1", "handoff-1").owner_token == "process-b"


def test_renew_returns_transaction_snapshot_not_successor_capability(tmp_path):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    store = _store(tmp_path, clock)
    _due(store)
    claim = store.claim_due(owner_token="process-a", lease_seconds=10)
    assert claim is not None
    successor_store = _store(tmp_path, clock)

    def reclaim():
        clock.advance(11)
        assert successor_store.claim_due(owner_token="process-b", lease_seconds=10)

    _force_after_transaction(store, reclaim)
    renewed = store.renew(claim, lease_seconds=10)

    assert (renewed.owner_token, renewed.owner_generation) == ("process-a", 1)
    assert successor_store.get("job-1", "handoff-1").owner_token == "process-b"


def test_record_returns_transaction_snapshot_not_successor_capability(tmp_path):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    store = _store(tmp_path, clock)
    _due(store)
    claim = store.claim_due(owner_token="process-a", lease_seconds=10)
    assert claim is not None
    successor_store = _store(tmp_path, clock)

    def reclaim():
        clock.advance(11)
        assert successor_store.claim_due(owner_token="process-b", lease_seconds=10)

    _force_after_transaction(store, reclaim)
    recorded = store.record_verified_effect(
        claim, effect_name="handoff_delivery", receipt_sha256=_digest(b"receipt")
    )

    assert (recorded.owner_token, recorded.owner_generation) == ("process-a", 1)
    assert recorded.checkpoint_stage == "VERIFY_HANDOFF_DELIVERY"
    assert recorded.next_action == "verify_handoff_delivery"
    assert recorded.verification_state == "RECEIPT_DIGEST_PERSISTED"
    assert successor_store.get("job-1", "handoff-1").owner_token == "process-b"


def test_enqueue_returns_pre_claim_transaction_snapshot(tmp_path):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    store = _store(tmp_path, clock)
    successor_store = _store(tmp_path, clock)

    def claim_immediately():
        assert successor_store.claim_due(owner_token="process-b", lease_seconds=10)

    _force_after_transaction(store, claim_immediately)
    enqueued = _due(store)

    assert enqueued.owner_token is None
    assert enqueued.owner_generation == 0
    assert successor_store.get("job-1", "handoff-1").owner_token == "process-b"


def test_manual_resume_returns_pre_claim_transaction_snapshot(tmp_path):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    store = _store(tmp_path, clock)
    _due(store)
    claim = store.claim_due(owner_token="process-a", lease_seconds=10)
    assert claim is not None
    digest = _digest(b"receipt")
    recorded = store.record_verified_effect(
        claim, effect_name="handoff_delivery", receipt_sha256=digest
    )
    with pytest.raises(DeliveryVerificationFailed):
        store.complete_delivery(recorded, observed_receipt_sha256=_digest(b"wrong"))
    successor_store = _store(tmp_path, clock)

    def claim_immediately():
        assert successor_store.claim_due(owner_token="process-b", lease_seconds=10)

    _force_after_transaction(store, claim_immediately)
    resumed = store.resume_after_manual_verification(
        job_id="job-1",
        handoff_id="handoff-1",
        operator_reason="verified offline",
        confirmed_receipt_sha256=digest,
    )

    assert resumed.owner_token is None
    assert resumed.owner_generation == 1
    assert resumed.wake_state == "DUE"
    assert successor_store.get("job-1", "handoff-1").owner_token == "process-b"


def test_complete_returns_transaction_snapshot_without_post_commit_get(
    tmp_path, monkeypatch
):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    store = _store(tmp_path, clock)
    _due(store)
    claim = store.claim_due(owner_token="process-a", lease_seconds=10)
    assert claim is not None
    digest = _digest(b"receipt")
    recorded = store.record_verified_effect(
        claim, effect_name="handoff_delivery", receipt_sha256=digest
    )

    def forbidden_get(*args, **kwargs):
        raise AssertionError("mutation return path performed a post-commit get")

    monkeypatch.setattr(store, "get", forbidden_get)
    completed = store.complete_delivery(recorded, observed_receipt_sha256=digest)

    assert completed.wake_state == "COMPLETE"
    assert completed.checkpoint_stage == "COMPLETE"
    assert completed.next_action == "complete"
    assert completed.owner_token is None
    assert completed.owner_generation == recorded.owner_generation
