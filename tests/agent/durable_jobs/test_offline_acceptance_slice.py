from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from pathlib import Path
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Lock

import pytest

from agent.durable_jobs.clock import FrozenClock
from agent.durable_jobs.legacy_migration import (
    FrozenSQLiteSnapshot,
    apply_legacy_adoption,
    plan_legacy_adoption,
)
from agent.durable_jobs.offline_acceptance import (
    OfflineAcceptanceError,
    initialize_disposable_application,
    materialize_disposable_adoption,
    readback_disposable_adoption,
    rollback_disposable_adoption,
)
from agent.durable_jobs.offline_continuation_harness import (
    ENG110_CRITERIA_EVIDENCE,
    DisposableReceiptAdapter,
    ExternalPortCalled,
    FailIfCalledPorts,
    OfflineContinuationScheduler,
    OfflineHarnessDisabled,
    initialize_disposable_receipt_root,
)
from agent.durable_jobs.session_handoff_continuation import ContinuationStore


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _adoption(tmp_path):
    source = tmp_path / "source.sqlite3"
    connection = sqlite3.connect(source)
    connection.executescript(
        """
        CREATE TABLE sessions(id TEXT PRIMARY KEY, parent_session_id TEXT);
        CREATE TABLE messages(id INTEGER PRIMARY KEY, session_id TEXT, body TEXT);
        INSERT INTO sessions VALUES('s-1',NULL);
        INSERT INTO messages VALUES(1,'s-1','offline');
        """
    )
    connection.close()
    snapshot = FrozenSQLiteSnapshot(source, _sha(source))
    plan = plan_legacy_adoption(snapshot)
    ledger = tmp_path / "ledger.sqlite3"
    apply_legacy_adoption(
        plan, ledger, dispositions={}, expected_source_snapshot=snapshot
    )
    return snapshot, plan, ledger


def test_materialization_dedupes_reads_back_and_rolls_back(tmp_path):
    snapshot, plan, ledger = _adoption(tmp_path)
    target = tmp_path / "acceptance" / "application.sqlite3"
    initialize_disposable_application(target, disposable_root=tmp_path)

    first = materialize_disposable_adoption(
        plan,
        ledger,
        target,
        disposable_root=tmp_path,
        dispositions={},
        expected_source_snapshot=snapshot,
    )
    second = materialize_disposable_adoption(
        plan,
        ledger,
        target,
        disposable_root=tmp_path,
        dispositions={},
        expected_source_snapshot=snapshot,
    )
    readback = readback_disposable_adoption(plan, target, disposable_root=tmp_path)

    assert first.inserted_count == first.total_count == 2
    assert second.inserted_count == 0 and second.duplicate_count == 2
    assert readback.verified and readback.batch_sha256 == first.batch_sha256
    rolled_back = rollback_disposable_adoption(
        target, disposable_root=tmp_path, batch_sha256=first.batch_sha256
    )
    assert rolled_back.removed_count == 2
    assert not readback_disposable_adoption(
        plan, target, disposable_root=tmp_path
    ).verified


def test_materialization_refuses_unmarked_existing_or_outside_target(tmp_path):
    snapshot, plan, ledger = _adoption(tmp_path)
    existing = tmp_path / "existing.sqlite3"
    existing.touch()
    with pytest.raises(OfflineAcceptanceError, match="new file"):
        initialize_disposable_application(existing, disposable_root=tmp_path)
    outside = tmp_path.parent / "outside-eng118.sqlite3"
    with pytest.raises(OfflineAcceptanceError, match="beneath"):
        initialize_disposable_application(outside, disposable_root=tmp_path)
    with pytest.raises(OfflineAcceptanceError, match="not disposable"):
        materialize_disposable_adoption(
            plan,
            ledger,
            existing,
            disposable_root=tmp_path,
            dispositions={},
            expected_source_snapshot=snapshot,
        )


def test_default_off_scheduler_binds_adapter_receipt_and_survives_restart(tmp_path):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    path = tmp_path / "continuations.sqlite3"
    store = ContinuationStore(path, now_fn=clock)
    store.enqueue(
        job_id="job-1",
        handoff_id="handoff-1",
        checkpoint_stage="DELIVER",
        next_action="deliver_handoff",
    )
    receipt_root = tmp_path / "receipts"
    initialize_disposable_receipt_root(receipt_root)
    adapter = DisposableReceiptAdapter(
        receipt_root / "adapter.sqlite3",
        receipt_root,
        lambda record, key: f"offline:{key}".encode(),
    )
    disabled = OfflineContinuationScheduler(store, adapter)
    with pytest.raises(OfflineHarnessDisabled):
        disabled.run_once(owner_token="process-a", lease_seconds=10)
    assert adapter.delivery_attempts == 0

    first = OfflineContinuationScheduler(store, adapter, enabled=True).run_once(
        owner_token="process-a", lease_seconds=10
    )
    assert first is not None
    assert first.next_action == "verify_handoff_delivery"
    clock.advance(11)
    restarted_store = ContinuationStore(path, now_fn=clock)
    restarted_adapter = DisposableReceiptAdapter(
        receipt_root / "adapter.sqlite3",
        receipt_root,
        lambda record, key: (_ for _ in ()).throw(
            AssertionError("verification must read durable authoritative bytes")
        ),
    )
    completed = OfflineContinuationScheduler(
        restarted_store, restarted_adapter, enabled=True
    ).run_once(owner_token="process-b", lease_seconds=10)
    assert completed is not None and completed.wake_state == "COMPLETE"
    assert completed.owner_generation == first.owner_generation + 1
    assert adapter.delivery_attempts == 1 and restarted_adapter.delivery_attempts == 0


def test_crash_after_durable_adapter_receipt_does_not_reinvoke_factory(tmp_path):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    store_path = tmp_path / "continuations.sqlite3"
    store = ContinuationStore(store_path, now_fn=clock)
    store.enqueue(
        job_id="job",
        handoff_id="handoff",
        checkpoint_stage="DELIVER",
        next_action="deliver_handoff",
    )
    root = tmp_path / "receipts"
    initialize_disposable_receipt_root(root)
    calls = 0

    def factory(record, key):
        nonlocal calls
        calls += 1
        return f"winner:{key}".encode()

    adapter = DisposableReceiptAdapter(root / "adapter.sqlite3", root, factory)
    original = store.record_verified_effect
    store.record_verified_effect = lambda *args, **kwargs: (_ for _ in ()).throw(
        RuntimeError("crash window")
    )
    with pytest.raises(RuntimeError, match="crash window"):
        OfflineContinuationScheduler(store, adapter, enabled=True).run_once(
            owner_token="a", lease_seconds=10
        )
    store.record_verified_effect = original
    clock.advance(11)
    restarted = DisposableReceiptAdapter(
        root / "adapter.sqlite3",
        root,
        lambda record, key: (_ for _ in ()).throw(AssertionError("factory re-invoked")),
    )
    result = OfflineContinuationScheduler(
        ContinuationStore(store_path, now_fn=clock), restarted, enabled=True
    ).run_once(owner_token="b", lease_seconds=10)
    assert result is not None and result.next_action == "verify_handoff_delivery"
    assert calls == 1 and restarted.delivery_attempts == 0


def test_concurrent_adapter_callers_return_one_durable_winner(tmp_path):
    root = tmp_path / "receipts"
    initialize_disposable_receipt_root(root)
    store = ContinuationStore(tmp_path / "continuations.sqlite3")
    record = store.enqueue(
        job_id="job",
        handoff_id="handoff",
        checkpoint_stage="DELIVER",
        next_action="deliver_handoff",
    )
    barrier = Barrier(8)
    lock = Lock()
    effects = []

    def call(index):
        adapter = DisposableReceiptAdapter(
            root / "adapter.sqlite3",
            root,
            lambda _record, _key: _factory(index),
        )
        barrier.wait()
        return adapter.deliver(record)

    def _factory(index):
        with lock:
            effects.append(index)
        return f"receipt-{index}".encode()

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(call, range(8)))
    assert len(set(results)) == 1
    assert len(effects) == 1
    reader = DisposableReceiptAdapter(
        root / "adapter.sqlite3", root, lambda *_: b"forbidden"
    )
    assert results[0] == reader.authoritative_receipt_bytes(record)


def test_disabled_and_ambiguous_enablement_do_not_touch_adapter_path(tmp_path):
    path = tmp_path / "arbitrary-existing.sqlite3"
    path.write_bytes(b"not sqlite")
    adapter = DisposableReceiptAdapter(path, tmp_path / "unattested", lambda *_: b"x")
    store = ContinuationStore(tmp_path / "continuations.sqlite3")
    scheduler = OfflineContinuationScheduler(store, adapter, enabled=False)
    with pytest.raises(OfflineHarnessDisabled):
        scheduler.run_once(owner_token="a", lease_seconds=1)
    assert path.read_bytes() == b"not sqlite"
    for ambiguous in (1, "true", None, [], object()):
        with pytest.raises(OfflineHarnessDisabled, match="literal bool"):
            OfflineContinuationScheduler(store, adapter, enabled=ambiguous)
    assert path.read_bytes() == b"not sqlite"


def test_missing_materialization_target_fails_without_creating_artifact(tmp_path):
    snapshot, plan, ledger = _adoption(tmp_path)
    missing = tmp_path / "missing.sqlite3"
    with pytest.raises(OfflineAcceptanceError, match="not an initialized"):
        materialize_disposable_adoption(
            plan,
            ledger,
            missing,
            disposable_root=tmp_path,
            dispositions={},
            expected_source_snapshot=snapshot,
        )
    assert not missing.exists()


def test_external_ports_fail_if_called_and_eng110_mapping_is_explicit():
    ports = FailIfCalledPorts()
    with pytest.raises(ExternalPortCalled, match="slack_send"):
        ports.slack_send("forbidden")
    assert set(ENG110_CRITERIA_EVIDENCE) == {
        "durable_checkpoint",
        "restart_reclaim",
        "effect_dedupe",
        "authoritative_receipt_binding",
        "manual_resume",
        "external_effect_isolation",
        "default_off",
    }
