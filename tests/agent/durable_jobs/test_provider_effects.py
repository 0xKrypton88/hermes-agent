"""ENG-26 — Cursor provider reconciliation contract (isolated, default-off).

Deterministic fakes only. No live Cursor/provider dispatch, network, or
gateway wiring. SQLite paths are explicit disposable test files.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pytest


def _db(tmp_path: Path) -> Path:
    return tmp_path / "pilot_jobs.sqlite"


def _make_job(tmp_path: Path, *, idempotency_key: str = "idem-eng26"):
    from agent.durable_jobs.store import DurableJobStore

    store = DurableJobStore(sqlite_path=_db(tmp_path))
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="ENG-26 slice",
        repository_identity="github.com/example/repo",
        idempotency_key=idempotency_key,
    )
    from agent.durable_jobs.eng29 import install_default_adapter_authorization

    install_default_adapter_authorization(store.sqlite_path, job.job_id)
    return store, job


@dataclass
class FakeRun:
    run_id: str
    idempotency_key: str


@dataclass
class FakeCreateResult:
    kind: str  # accepted | lost_response | ambiguous_response
    run: Optional[FakeRun] = None
    candidates: tuple[FakeRun, ...] = ()


class FakeCursorProvider:
    def __init__(
        self,
        create_result: FakeCreateResult,
        lookups: Optional[List[FakeRun]] = None,
    ) -> None:
        self.create_result = create_result
        self.lookups = list(lookups or [])
        self.create_calls: list[dict] = []
        self.lookup_calls: list[str] = []

    def create_run(self, *, idempotency_key: str, job_id: str) -> FakeCreateResult:
        self.create_calls.append(
            {"idempotency_key": idempotency_key, "job_id": job_id}
        )
        return self.create_result

    def lookup_runs(self, *, idempotency_key: str) -> list[FakeRun]:
        self.lookup_calls.append(idempotency_key)
        return list(self.lookups)


def test_duplicate_effect_claims_adopt_original_without_second_row(tmp_path):
    from agent.durable_jobs.effects import EffectStatus, ProviderEffectLedger

    store, job = _make_job(tmp_path)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    kwargs = dict(
        job_id=job.job_id,
        action_id="create_run",
        origin_platform=job.origin_platform,
        origin_chat_id=job.origin_chat_id,
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )
    first = ledger.claim_effect(**kwargs)
    second = ledger.claim_effect(**kwargs)

    assert first.won is True
    assert second.won is False
    assert second.claim.job_id == first.claim.job_id
    assert second.claim.action_id == first.claim.action_id
    assert second.claim.provider_idempotency_key == first.claim.provider_idempotency_key
    assert first.claim.status is EffectStatus.CLAIMED
    assert ledger.count_claims() == 1


def test_concurrent_effect_claims_single_winner_same_idempotency_key(tmp_path):
    from agent.durable_jobs.effects import ProviderEffectLedger

    store, job = _make_job(tmp_path)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    barrier = threading.Barrier(2)
    results = []

    def worker() -> None:
        barrier.wait()
        results.append(
            ledger.claim_effect(
                job_id=job.job_id,
                action_id="create_run",
                origin_platform=job.origin_platform,
                origin_chat_id=job.origin_chat_id,
                origin_root_thread_id=job.origin_root_thread_id,
                candidate_id="cand-1",
                candidate_version="v1",
            )
        )

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == 2
    winners = [r for r in results if r.won]
    assert len(winners) == 1
    keys = {r.claim.provider_idempotency_key for r in results}
    assert len(keys) == 1
    assert ledger.count_claims() == 1


def test_provider_idempotency_key_is_stable_across_store_recreation(tmp_path):
    from agent.durable_jobs.effects import (
        ProviderEffectLedger,
        provider_idempotency_key,
    )

    store, job = _make_job(tmp_path)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    claimed = ledger.claim_effect(
        job_id=job.job_id,
        action_id="create_run",
        origin_platform=job.origin_platform,
        origin_chat_id=job.origin_chat_id,
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )
    expected = provider_idempotency_key(job.job_id, "create_run")
    assert claimed.claim.provider_idempotency_key == expected

    reopened = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    loaded = reopened.get_claim(job.job_id, "create_run")
    assert loaded is not None
    assert loaded.provider_idempotency_key == expected


def test_mapping_job_id_equals_langgraph_thread_id_and_freezes_origin_candidate_fields(
    tmp_path,
):
    from agent.durable_jobs.effects import ProviderEffectLedger

    store, job = _make_job(tmp_path)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    ledger.claim_effect(
        job_id=job.job_id,
        action_id="create_run",
        origin_platform=job.origin_platform,
        origin_chat_id=job.origin_chat_id,
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )
    mapping = ledger.get_mapping(job.job_id)
    assert mapping is not None
    assert mapping.job_id == job.job_id
    assert mapping.langgraph_thread_id == job.job_id
    assert mapping.provider_run_id is None
    assert mapping.origin_platform == "slack"
    assert mapping.origin_chat_id == "C123"
    assert mapping.origin_root_thread_id == "111.222"
    assert mapping.candidate_id == "cand-1"
    assert mapping.candidate_version == "v1"

    with pytest.raises(ValueError):
        ledger.claim_effect(
            job_id=job.job_id,
            action_id="other_action",
            origin_platform=job.origin_platform,
            origin_chat_id=job.origin_chat_id,
            origin_root_thread_id=job.origin_root_thread_id,
            candidate_id="cand-1",
            candidate_version="v2",
        )
    frozen = ledger.get_mapping(job.job_id)
    assert frozen is not None
    assert frozen.candidate_version == "v1"


def test_mapping_survives_process_and_store_recreation(tmp_path):
    from agent.durable_jobs.effects import EffectStatus, ProviderEffectLedger

    store, job = _make_job(tmp_path)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    claimed = ledger.claim_effect(
        job_id=job.job_id,
        action_id="create_run",
        origin_platform=job.origin_platform,
        origin_chat_id=job.origin_chat_id,
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )
    ledger.mark_accepted(
        job.job_id,
        "create_run",
        provider_run_id="run-abc",
        owner_token=claimed.owner_token,
    )

    reopened = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    mapping = reopened.get_mapping(job.job_id)
    claim = reopened.get_claim(job.job_id, "create_run")
    assert mapping is not None
    assert mapping.langgraph_thread_id == job.job_id
    assert mapping.provider_run_id == "run-abc"
    assert mapping.candidate_id == "cand-1"
    assert claim is not None
    assert claim.status is EffectStatus.ACCEPTED
    assert claim.provider_run_id == "run-abc"


def test_claim_happens_before_provider_create_call(tmp_path):
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    seen = {"claimed": False}

    class OrderingProvider(FakeCursorProvider):
        def create_run(self, *, idempotency_key: str, job_id: str) -> FakeCreateResult:
            claim = ledger.get_claim(job_id, "create_run")
            seen["claimed"] = claim is not None and claim.status is EffectStatus.CLAIMED
            return super().create_run(idempotency_key=idempotency_key, job_id=job_id)

    provider = OrderingProvider(
        FakeCreateResult(kind="accepted", run=FakeRun("run-1", "pending"))
    )
    result = reconcile_cursor_create(
        ledger,
        provider,
        job_id=job.job_id,
        action_id="create_run",
        origin_platform=job.origin_platform,
        origin_chat_id=job.origin_chat_id,
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )
    assert seen["claimed"] is True
    assert result.status is EffectStatus.ACCEPTED
    assert result.provider_run_id == "run-1"
    assert len(provider.create_calls) == 1


def test_lost_create_uniquely_reconcilable_is_adopted_without_redispatch(tmp_path):
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        provider_idempotency_key,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    key = provider_idempotency_key(job.job_id, "create_run")
    provider = FakeCursorProvider(
        FakeCreateResult(kind="lost_response"),
        lookups=[FakeRun("run-unique", key)],
    )
    kwargs = dict(
        job_id=job.job_id,
        action_id="create_run",
        origin_platform=job.origin_platform,
        origin_chat_id=job.origin_chat_id,
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )
    first = reconcile_cursor_create(ledger, provider, **kwargs)
    assert first.status is EffectStatus.ADOPTED
    assert first.provider_run_id == "run-unique"
    assert ledger.get_mapping(job.job_id).provider_run_id == "run-unique"

    second = reconcile_cursor_create(ledger, provider, **kwargs)
    assert second.status is EffectStatus.ADOPTED
    assert len(provider.create_calls) == 1


def test_ambiguous_lookup_persists_typed_unknown_and_never_redispatches(tmp_path):
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        UnknownReason,
        provider_idempotency_key,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    key = provider_idempotency_key(job.job_id, "create_run")
    provider = FakeCursorProvider(
        FakeCreateResult(kind="lost_response"),
        lookups=[FakeRun("run-a", key), FakeRun("run-b", key)],
    )
    kwargs = dict(
        job_id=job.job_id,
        action_id="create_run",
        origin_platform=job.origin_platform,
        origin_chat_id=job.origin_chat_id,
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )
    first = reconcile_cursor_create(ledger, provider, **kwargs)
    assert first.status is EffectStatus.UNKNOWN
    assert first.unknown_reason == UnknownReason.AMBIGUOUS_LOOKUP.value
    assert first.provider_run_id is None

    second = reconcile_cursor_create(ledger, provider, **kwargs)
    assert second.status is EffectStatus.UNKNOWN
    assert len(provider.create_calls) == 1


def test_empty_lookup_after_lost_create_recovers_then_unknown_without_redispatch(tmp_path):
    from agent.durable_jobs.clock import DEFAULT_RECOVERY_WINDOW_SECONDS, FrozenClock
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        UnknownReason,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path)
    clock = FrozenClock()
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path, now_fn=clock)
    provider = FakeCursorProvider(FakeCreateResult(kind="lost_response"), lookups=[])
    kwargs = dict(
        job_id=job.job_id,
        action_id="create_run",
        origin_platform=job.origin_platform,
        origin_chat_id=job.origin_chat_id,
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )
    first = reconcile_cursor_create(ledger, provider, **kwargs)
    assert first.status is EffectStatus.RECOVERING
    assert first.unknown_reason is None
    clock.advance(DEFAULT_RECOVERY_WINDOW_SECONDS + 1)
    terminal = reconcile_cursor_create(ledger, provider, **kwargs)
    assert terminal.status is EffectStatus.UNKNOWN
    assert terminal.unknown_reason == UnknownReason.EMPTY_LOOKUP.value
    retry = reconcile_cursor_create(ledger, provider, **kwargs)
    assert retry.status is EffectStatus.UNKNOWN
    assert len(provider.create_calls) == 1


def test_ambiguous_create_response_is_typed_unknown(tmp_path):
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        UnknownReason,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    provider = FakeCursorProvider(
        FakeCreateResult(
            kind="ambiguous_response",
            candidates=(FakeRun("r1", "k"), FakeRun("r2", "k")),
        )
    )
    result = reconcile_cursor_create(
        ledger,
        provider,
        job_id=job.job_id,
        action_id="create_run",
        origin_platform=job.origin_platform,
        origin_chat_id=job.origin_chat_id,
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )
    assert result.status is EffectStatus.UNKNOWN
    assert result.unknown_reason == UnknownReason.AMBIGUOUS_RESPONSE.value
    retry = reconcile_cursor_create(
        ledger,
        provider,
        job_id=job.job_id,
        action_id="create_run",
        origin_platform=job.origin_platform,
        origin_chat_id=job.origin_chat_id,
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )
    assert retry.status is EffectStatus.UNKNOWN
    assert len(provider.create_calls) == 1


def test_claimed_after_restart_unique_lookup_adopts_without_create_run(tmp_path):
    """Same-process unit evidence: stale lease takeover after reopen.

    Not crash/subprocess death evidence. A fresh unexpired CLAIMED must not
    be looked up; only an expired lease may be taken over.
    """
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS, FrozenClock
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        provider_idempotency_key,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path)
    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    kwargs = dict(
        job_id=job.job_id,
        action_id="create_run",
        origin_platform=job.origin_platform,
        origin_chat_id=job.origin_chat_id,
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )
    claimed = ledger.claim_effect(**kwargs)
    assert claimed.won is True
    assert claimed.claim.status is EffectStatus.CLAIMED
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)

    key = provider_idempotency_key(job.job_id, "create_run")
    provider = FakeCursorProvider(
        FakeCreateResult(kind="lost_response"),
        lookups=[FakeRun("run-unique", key)],
    )
    reopened = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    adopted = reconcile_cursor_create(reopened, provider, **kwargs)
    assert adopted.status is EffectStatus.ADOPTED
    assert adopted.provider_run_id == "run-unique"
    assert provider.create_calls == []
    assert provider.lookup_calls == [key]


def test_claimed_after_restart_empty_lookup_recovers_without_create_run(
    tmp_path,
):
    """Same-process unit evidence: stale lease + empty lookup → RECOVERING.

    Not crash/subprocess death evidence. UNKNOWN requires the recovery bound.
    """
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS, FrozenClock
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        provider_idempotency_key,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path)
    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    kwargs = dict(
        job_id=job.job_id,
        action_id="create_run",
        origin_platform=job.origin_platform,
        origin_chat_id=job.origin_chat_id,
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )
    ledger.claim_effect(**kwargs)
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    key = provider_idempotency_key(job.job_id, "create_run")
    provider = FakeCursorProvider(FakeCreateResult(kind="lost_response"), lookups=[])
    reopened = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    recovering = reconcile_cursor_create(reopened, provider, **kwargs)
    assert recovering.status is EffectStatus.RECOVERING
    assert recovering.unknown_reason is None
    assert provider.create_calls == []
    assert provider.lookup_calls == [key]


def test_claimed_after_process_recreation_ambiguous_lookup_is_typed_unknown_without_create(
    tmp_path,
):
    """Same-process unit evidence: stale lease + ambiguous lookup → UNKNOWN.

    Not crash/subprocess death evidence.
    """
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS, FrozenClock
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        UnknownReason,
        provider_idempotency_key,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path)
    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    kwargs = dict(
        job_id=job.job_id,
        action_id="create_run",
        origin_platform=job.origin_platform,
        origin_chat_id=job.origin_chat_id,
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )
    ledger.claim_effect(**kwargs)
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    key = provider_idempotency_key(job.job_id, "create_run")
    provider = FakeCursorProvider(
        FakeCreateResult(kind="lost_response"),
        lookups=[FakeRun("run-a", key), FakeRun("run-b", key)],
    )
    reopened = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    unknown = reconcile_cursor_create(reopened, provider, **kwargs)
    assert unknown.status is EffectStatus.UNKNOWN
    assert unknown.unknown_reason == UnknownReason.AMBIGUOUS_LOOKUP.value
    assert unknown.provider_run_id is None
    assert provider.create_calls == []
    assert provider.lookup_calls == [key]


def _enabled_lane(tmp_path, store):
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.lane import DurableLaneService

    cfg = load_durable_jobs_config(
        {
            "durable_jobs": {
                "enabled": True,
                "dispatch_enabled": False,
                "sqlite_path": str(store.sqlite_path),
                "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
            }
        }
    )
    return DurableLaneService(config=cfg, store=store)


def test_provider_origin_mismatch_rejected_before_effect_claim(tmp_path):
    from agent.durable_jobs.effects import ProviderEffectLedger
    from agent.durable_jobs.slack_contract import OriginMismatchError, SlackBindingLedger

    store, job = _make_job(tmp_path)
    slack = SlackBindingLedger(sqlite_path=store.sqlite_path)
    slack.bind(
        job_id=job.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    provider = FakeCursorProvider(
        FakeCreateResult(kind="accepted", run=FakeRun("run-x", "k"))
    )
    lane = _enabled_lane(tmp_path, store)
    with pytest.raises(OriginMismatchError):
        lane.reconcile_cursor_create(
            job_id=job.job_id,
            action_id="create_run",
            origin_platform="slack",
            origin_chat_id="C-OTHER",
            origin_root_thread_id="111.222",
            candidate_id="cand-1",
            candidate_version="v1",
            provider=provider,
        )
    with pytest.raises(OriginMismatchError):
        lane.reconcile_cursor_create(
            job_id=job.job_id,
            action_id="create_run",
            origin_platform="cli",
            origin_chat_id="C123",
            origin_root_thread_id="111.222",
            candidate_id="cand-1",
            candidate_version="v1",
            provider=provider,
        )
    with pytest.raises(OriginMismatchError):
        lane.reconcile_cursor_create(
            job_id=job.job_id,
            action_id="create_run",
            origin_platform="slack",
            origin_chat_id="C123",
            origin_root_thread_id="999.000",
            candidate_id="cand-1",
            candidate_version="v1",
            provider=provider,
        )
    assert provider.create_calls == []
    assert ProviderEffectLedger(sqlite_path=store.sqlite_path).get_claim(
        job.job_id, "create_run"
    ) is None


def test_provider_origin_is_derived_from_slack_binding(tmp_path):
    from agent.durable_jobs.effects import EffectStatus, ProviderEffectLedger
    from agent.durable_jobs.slack_contract import SlackBindingLedger

    store, job = _make_job(tmp_path)
    slack = SlackBindingLedger(sqlite_path=store.sqlite_path)
    binding = slack.bind(
        job_id=job.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    key = f"cursor:{job.job_id}:create_run"
    provider = FakeCursorProvider(
        FakeCreateResult(kind="accepted", run=FakeRun("run-bound", key))
    )
    lane = _enabled_lane(tmp_path, store)
    result = lane.reconcile_cursor_create(
        job_id=job.job_id,
        action_id="create_run",
        origin_platform="slack",
        origin_chat_id=binding.channel_id,
        origin_root_thread_id=binding.root_thread_ts,
        candidate_id=binding.candidate_id,
        candidate_version=binding.candidate_version,
        provider=provider,
    )
    assert result.status is EffectStatus.ACCEPTED
    claim = ProviderEffectLedger(sqlite_path=store.sqlite_path).get_claim(
        job.job_id, "create_run"
    )
    assert claim is not None
    assert claim.origin_platform == "slack"
    assert claim.origin_chat_id == binding.channel_id
    assert claim.origin_root_thread_id == binding.root_thread_ts
    mapping = ProviderEffectLedger(sqlite_path=store.sqlite_path).get_mapping(job.job_id)
    assert mapping is not None
    assert mapping.origin_chat_id == binding.channel_id
    assert mapping.origin_root_thread_id == binding.root_thread_ts


def test_reconcile_is_noop_when_pilot_disabled(tmp_path):
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.lane import DurableLaneService
    from agent.durable_jobs.service import PilotDisabledError

    cfg = load_durable_jobs_config(
        {
            "durable_jobs": {
                "enabled": False,
                "dispatch_enabled": False,
                "sqlite_path": str(_db(tmp_path)),
                "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
            }
        }
    )
    provider = FakeCursorProvider(FakeCreateResult(kind="accepted", run=FakeRun("x", "k")))
    lane = DurableLaneService(config=cfg)
    with pytest.raises(PilotDisabledError):
        lane.reconcile_cursor_create(
            job_id="dj_nope",
            action_id="create_run",
            origin_platform="slack",
            origin_chat_id="C1",
            origin_root_thread_id="1.1",
            candidate_id="cand-1",
            candidate_version="v1",
            provider=provider,
        )
    assert provider.create_calls == []
