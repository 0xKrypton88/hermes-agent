"""ENG-30 — inactive/fail-closed Cursor Cloud adapter behind CursorProviderPort.

Deterministic injected transport only. No live Cursor requests, network,
credentials, or dispatch enablement. Ledger identity stays in the existing
provider effect ledger.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List

import pytest


def _db(tmp_path: Path) -> Path:
    return tmp_path / "pilot_jobs.sqlite"


def _make_job(tmp_path: Path, *, idempotency_key: str = "idem-eng30"):
    from agent.durable_jobs.store import DurableJobStore

    store = DurableJobStore(sqlite_path=_db(tmp_path))
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="ENG-30 adapter",
        repository_identity="github.com/example/repo",
        idempotency_key=idempotency_key,
    )
    from tests.agent.durable_jobs.authz_fixtures import (
        install_default_adapter_authorization,
    )

    install_default_adapter_authorization(store.sqlite_path, job.job_id)
    return store, job


def _origin_kwargs(job):
    return dict(
        origin_platform=job.origin_platform,
        origin_chat_id=job.origin_chat_id,
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )


@dataclass
class MemoryCursorTransport:
    """In-memory Cursor Cloud stand-in. No sockets."""

    create_payload: Any = None
    lookups: List[Any] = field(default_factory=list)
    status_payload: Any = None
    create_calls: list = field(default_factory=list)
    lookup_calls: list = field(default_factory=list)
    status_calls: list = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def create(self, *, idempotency_key: str, job_id: str) -> Any:
        with self._lock:
            self.create_calls.append(
                {"idempotency_key": idempotency_key, "job_id": job_id}
            )
            if callable(self.create_payload) and not isinstance(
                self.create_payload, type
            ):
                return self.create_payload(idempotency_key=idempotency_key, job_id=job_id)
            if isinstance(self.create_payload, BaseException):
                raise self.create_payload
            return self.create_payload

    def lookup(self, *, idempotency_key: str) -> Any:
        with self._lock:
            self.lookup_calls.append(idempotency_key)
            if isinstance(self.lookups, BaseException):
                raise self.lookups
            # Official Cursor list envelopes are dicts ({"agents"|"items": [...]}).
            if isinstance(self.lookups, dict):
                return self.lookups
            return list(self.lookups)

    def status(self, *, run_id: str) -> Any:
        with self._lock:
            self.status_calls.append(run_id)
            if isinstance(self.status_payload, BaseException):
                raise self.status_payload
            return self.status_payload


def test_adapter_seam_requires_injected_transport_and_preserves_null_isolation():
    from agent.durable_jobs.adapters import CursorProviderPort, NullCursorProvider
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.cursor_cloud import CursorCloudAdapter
    import agent.durable_jobs.adapters as adapters
    import agent.durable_jobs.cursor_cloud as cursor_cloud

    assert hasattr(adapters, "NullCursorProvider")
    assert not hasattr(adapters, "CursorCloudAdapter")
    assert not hasattr(adapters, "SlackDispatchAdapter")
    assert not hasattr(cursor_cloud, "CursorCloudHttpClient")
    assert not hasattr(cursor_cloud, "LiveCursorCloudTransport")

    null = NullCursorProvider()
    with pytest.raises(RuntimeError, match="refuses create_run"):
        null.create_run(idempotency_key="cursor:job:create_run", job_id="job")
    with pytest.raises(RuntimeError, match="refuses lookup_runs"):
        null.lookup_runs(idempotency_key="cursor:job:create_run")
    with pytest.raises(RuntimeError, match="refuses status_run"):
        null.status_run(run_id="run")

    with pytest.raises((TypeError, RuntimeError)):
        CursorCloudAdapter()

    cfg = load_durable_jobs_config(
        {
            "durable_jobs": {
                "enabled": True,
                "dispatch_enabled": True,
            }
        }
    )
    assert cfg.dispatch_allowed is False
    from agent.durable_jobs.cursor_cloud import adapter_from_config

    default = adapter_from_config(cfg)
    assert isinstance(default, NullCursorProvider)

    adapter = CursorCloudAdapter(transport=_FakeTransport())
    for method in ("create_run", "lookup_runs", "status_run"):
        assert callable(getattr(adapter, method, None)), method
    # Structural CursorProviderPort: create + lookup, no live client.
    assert callable(getattr(CursorProviderPort, "create_run", None))
    assert callable(getattr(CursorProviderPort, "lookup_runs", None))


class _FakeTransport:
    def create(self, *, idempotency_key: str, job_id: str):
        raise AssertionError("transport must not be called in isolation test")

    def lookup(self, *, idempotency_key: str):
        raise AssertionError("transport must not be called in isolation test")

    def status(self, *, run_id: str):
        raise AssertionError("transport must not be called in isolation test")


def test_duplicate_claims_share_ledger_idempotency_without_second_create(tmp_path):
    from agent.durable_jobs.cursor_cloud import CursorCloudAdapter
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        provider_idempotency_key,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    key = provider_idempotency_key(job.job_id, "create_run")
    transport = MemoryCursorTransport(
        create_payload={
            "kind": "accepted",
            "run": {"run_id": "run-dup", "idempotency_key": key},
        }
    )
    adapter = CursorCloudAdapter(transport=transport)
    kwargs = dict(job_id=job.job_id, action_id="create_run", **_origin_kwargs(job))
    first = reconcile_cursor_create(ledger, adapter, **kwargs)
    second = reconcile_cursor_create(ledger, adapter, **kwargs)

    assert first.status is EffectStatus.ACCEPTED
    assert first.provider_run_id == "run-dup"
    assert first.provider_idempotency_key == key
    assert second.status is EffectStatus.ACCEPTED
    assert second.provider_run_id == "run-dup"
    assert len(transport.create_calls) == 1
    assert transport.create_calls[0]["idempotency_key"] == key
    assert ledger.count_claims() == 1
    mapping = ledger.get_mapping(job.job_id)
    assert mapping is not None
    assert mapping.provider_run_id == "run-dup"
    assert mapping.langgraph_thread_id == job.job_id


def test_concurrent_claims_single_winner_same_ledger_key(tmp_path):
    from agent.durable_jobs.cursor_cloud import CursorCloudAdapter
    from agent.durable_jobs.effects import (
        ProviderEffectLedger,
        provider_idempotency_key,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    key = provider_idempotency_key(job.job_id, "create_run")
    transport = MemoryCursorTransport(
        create_payload={
            "kind": "accepted",
            "run": {"run_id": "run-conc", "idempotency_key": key},
        }
    )
    adapter = CursorCloudAdapter(transport=transport)
    barrier = threading.Barrier(2)
    results = []

    def worker() -> None:
        barrier.wait()
        results.append(
            reconcile_cursor_create(
                ledger,
                adapter,
                job_id=job.job_id,
                action_id="create_run",
                **_origin_kwargs(job),
            )
        )

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == 2
    keys = {r.provider_idempotency_key for r in results}
    assert keys == {key}
    assert ledger.count_claims() == 1
    assert len(transport.create_calls) == 1
    assert all(c["idempotency_key"] == key for c in transport.create_calls)


def test_accepted_but_lost_create_is_recovered_by_lookup_adoption(tmp_path):
    from agent.durable_jobs.cursor_cloud import CursorCloudAdapter
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        provider_idempotency_key,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    key = provider_idempotency_key(job.job_id, "create_run")
    transport = MemoryCursorTransport(
        create_payload={"kind": "lost_response"},
        lookups=[{"run_id": "run-adopt", "idempotency_key": key}],
    )
    adapter = CursorCloudAdapter(transport=transport)
    kwargs = dict(job_id=job.job_id, action_id="create_run", **_origin_kwargs(job))
    first = reconcile_cursor_create(ledger, adapter, **kwargs)
    assert first.status is EffectStatus.ADOPTED
    assert first.provider_run_id == "run-adopt"
    assert first.provider_idempotency_key == key
    assert ledger.get_mapping(job.job_id).provider_run_id == "run-adopt"

    second = reconcile_cursor_create(ledger, adapter, **kwargs)
    assert second.status is EffectStatus.ADOPTED
    assert len(transport.create_calls) == 1
    assert transport.lookup_calls == [key]


def test_restart_adoption_looks_up_without_create(tmp_path):
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS, FrozenClock
    from agent.durable_jobs.cursor_cloud import CursorCloudAdapter
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
    kwargs = dict(job_id=job.job_id, action_id="create_run", **_origin_kwargs(job))
    claimed = ledger.claim_effect(**kwargs)
    assert claimed.won is True
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)

    key = provider_idempotency_key(job.job_id, "create_run")
    transport = MemoryCursorTransport(
        create_payload={"kind": "lost_response"},
        lookups=[{"run_id": "run-restart", "idempotency_key": key}],
    )
    adapter = CursorCloudAdapter(transport=transport)
    reopened = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    adopted = reconcile_cursor_create(reopened, adapter, **kwargs)
    assert adopted.status is EffectStatus.ADOPTED
    assert adopted.provider_run_id == "run-restart"
    assert transport.create_calls == []
    assert transport.lookup_calls == [key]
    assert reopened.get_mapping(job.job_id).provider_run_id == "run-restart"


def test_ambiguous_multiple_lookup_matches_fail_closed_without_redispatch(tmp_path):
    from agent.durable_jobs.cursor_cloud import CursorCloudAdapter
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
    transport = MemoryCursorTransport(
        create_payload={"kind": "lost_response"},
        lookups=[
            {"run_id": "run-a", "idempotency_key": key},
            {"run_id": "run-b", "idempotency_key": key},
        ],
    )
    adapter = CursorCloudAdapter(transport=transport)
    kwargs = dict(job_id=job.job_id, action_id="create_run", **_origin_kwargs(job))
    first = reconcile_cursor_create(ledger, adapter, **kwargs)
    assert first.status is EffectStatus.UNKNOWN
    assert first.unknown_reason == UnknownReason.AMBIGUOUS_LOOKUP.value
    assert first.provider_run_id is None
    second = reconcile_cursor_create(ledger, adapter, **kwargs)
    assert second.status is EffectStatus.UNKNOWN
    assert len(transport.create_calls) == 1


def test_unknown_provider_state_fails_closed_without_redispatch(tmp_path):
    from agent.durable_jobs.cursor_cloud import (
        CursorCloudAdapter,
        CursorCreateKind,
        CursorStatusKind,
    )
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
    transport = MemoryCursorTransport(
        create_payload={"kind": "garbled-provider-state", "status": "???"},
        lookups=[],
        status_payload={"run_id": "run-x", "state": "not-a-real-state"},
    )
    adapter = CursorCloudAdapter(transport=transport)
    created = adapter.create_run(idempotency_key=key, job_id=job.job_id)
    assert created.kind is CursorCreateKind.UNKNOWN
    assert created.run is None

    kwargs = dict(job_id=job.job_id, action_id="create_run", **_origin_kwargs(job))
    first = reconcile_cursor_create(ledger, adapter, **kwargs)
    assert first.status is EffectStatus.UNKNOWN
    assert first.unknown_reason == UnknownReason.UNKNOWN_PROVIDER_STATE.value
    assert first.provider_run_id is None
    second = reconcile_cursor_create(ledger, adapter, **kwargs)
    assert second.status is EffectStatus.UNKNOWN
    assert len(transport.create_calls) == 2  # one direct + one reconcile
    # Reconcile must not keep creating after typed unknown.
    third = reconcile_cursor_create(ledger, adapter, **kwargs)
    assert third.status is EffectStatus.UNKNOWN
    assert len(transport.create_calls) == 2

    status = adapter.status_run(run_id="run-x")
    assert status.kind is CursorStatusKind.UNKNOWN
    assert status.run is None or status.run.state.value == "unknown"


def test_provider_errors_are_secret_safe_and_redacted():
    from agent.durable_jobs.cursor_cloud import (
        CursorCloudAdapter,
        CursorCreateKind,
        CursorStatusKind,
        redact_provider_error,
    )

    secret = "postgresql://hermes:p@ssword@127.0.0.1:5432/durable_jobs"
    token = "sk-live-secret-token-value"
    transport = MemoryCursorTransport(
        create_payload=RuntimeError(f"auth failed api_key={token} dsn={secret}"),
        lookups=RuntimeError(f"lookup token={token}"),
        status_payload=RuntimeError(f"status password=p@ssword bearer={token}"),
    )
    adapter = CursorCloudAdapter(transport=transport)
    created = adapter.create_run(idempotency_key="cursor:job:create_run", job_id="job")
    assert created.kind is CursorCreateKind.UNKNOWN
    assert created.error is not None
    assert token not in created.error
    assert "p@ssword" not in created.error
    assert "sk-live-secret-token-value" not in created.error
    dumped = created.error
    assert "p@ssword" not in dumped
    assert token not in dumped
    assert "sk-live" not in dumped

    looked = adapter.lookup_runs(idempotency_key="cursor:job:create_run")
    blob = str(looked)
    assert token not in blob
    assert "p@ssword" not in blob

    status = adapter.status_run(run_id="run-secret")
    assert status.kind in (CursorStatusKind.UNKNOWN, CursorStatusKind.AMBIGUOUS)
    assert status.error is not None
    assert token not in status.error
    assert "p@ssword" not in status.error
    assert token not in redact_provider_error(f"dsn={secret} token={token}")
    assert "p@ssword" not in redact_provider_error(secret)


def test_status_and_reconcile_use_ledger_not_a_second_store(tmp_path):
    from agent.durable_jobs.cursor_cloud import (
        CursorCloudAdapter,
        CursorStatusKind,
    )
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        provider_idempotency_key,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    key = provider_idempotency_key(job.job_id, "create_run")
    transport = MemoryCursorTransport(
        create_payload={
            "kind": "accepted",
            "run": {"run_id": "run-status", "idempotency_key": key, "state": "running"},
        },
        status_payload={
            "run_id": "run-status",
            "idempotency_key": key,
            "state": "running",
        },
    )
    adapter = CursorCloudAdapter(transport=transport)
    bound = reconcile_cursor_create(
        ledger,
        adapter,
        job_id=job.job_id,
        action_id="create_run",
        **_origin_kwargs(job),
    )
    assert bound.status is EffectStatus.ACCEPTED
    assert bound.provider_run_id == "run-status"

    unique = adapter.status_run(run_id="run-status")
    assert unique.kind is CursorStatusKind.UNIQUE
    assert unique.run is not None
    assert unique.run.run_id == "run-status"

    reconciled = adapter.reconcile_status(
        ledger, job_id=job.job_id, action_id="create_run"
    )
    assert reconciled.ok is True
    assert reconciled.claim.status is EffectStatus.ACCEPTED
    assert reconciled.claim.provider_run_id == "run-status"
    assert ledger.get_mapping(job.job_id).provider_run_id == "run-status"

    transport.status_payload = [
        {"run_id": "run-status", "idempotency_key": key},
        {"run_id": "run-other", "idempotency_key": key},
    ]
    ambiguous = adapter.status_run(run_id="run-status")
    assert ambiguous.kind is CursorStatusKind.AMBIGUOUS
    still = adapter.reconcile_status(
        ledger, job_id=job.job_id, action_id="create_run"
    )
    assert still.ok is False
    assert still.observation is not None
    assert still.observation.kind is CursorStatusKind.AMBIGUOUS
    assert still.claim.status is EffectStatus.ACCEPTED
    assert still.claim.provider_run_id == "run-status"


def test_adapter_methods_open_no_network_sockets(monkeypatch):
    import socket

    from agent.durable_jobs.cursor_cloud import CursorCloudAdapter

    def _deny(*_args, **_kwargs):
        raise AssertionError("network socket open attempted in ENG-30 adapter")

    monkeypatch.setattr(socket.socket, "connect", _deny)
    monkeypatch.setattr(socket.socket, "connect_ex", _deny)

    transport = MemoryCursorTransport(
        create_payload={
            "kind": "accepted",
            "run": {"run_id": "run-offline", "idempotency_key": "cursor:job:create_run"},
        },
        lookups=[{"run_id": "run-offline", "idempotency_key": "cursor:job:create_run"}],
        status_payload={
            "run_id": "run-offline",
            "idempotency_key": "cursor:job:create_run",
            "state": "running",
        },
    )
    adapter = CursorCloudAdapter(transport=transport)
    created = adapter.create_run(idempotency_key="cursor:job:create_run", job_id="job")
    assert created.run is not None
    assert adapter.lookup_runs(idempotency_key="cursor:job:create_run")
    assert adapter.status_run(run_id="run-offline").run is not None


def test_explicit_nonempty_idempotency_mismatch_fails_closed_without_adoption(tmp_path):
    """Any explicit non-empty key mismatch must fail closed, not only cursor: prefixes."""
    from agent.durable_jobs.cursor_cloud import (
        CursorCloudAdapter,
        CursorCreateKind,
        CursorCreateResult,
        CursorRun,
        normalize_create_result,
    )
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        provider_idempotency_key,
        reconcile_cursor_create,
    )

    expected = "cursor:job:create_run"
    mismatched = normalize_create_result(
        {
            "kind": "accepted",
            "run": {"run_id": "run-1", "idempotency_key": "other-system-key"},
        },
        expected_key=expected,
    )
    assert mismatched.kind is CursorCreateKind.UNKNOWN
    assert mismatched.kind is not CursorCreateKind.ACCEPTED

    typed = CursorCreateResult(
        kind=CursorCreateKind.ACCEPTED,
        run=CursorRun(run_id="run-1", idempotency_key="other-system-key"),
    )
    typed_out = normalize_create_result(typed, expected_key=expected)
    assert typed_out.kind is CursorCreateKind.UNKNOWN

    missing = normalize_create_result(
        {"kind": "accepted", "run": {"run_id": "run-fallback"}},
        expected_key=expected,
    )
    assert missing.kind is CursorCreateKind.ACCEPTED
    assert missing.run is not None
    assert missing.run.idempotency_key == expected

    store, job = _make_job(tmp_path)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    key = provider_idempotency_key(job.job_id, "create_run")
    transport = MemoryCursorTransport(
        create_payload={
            "kind": "accepted",
            "run": {"run_id": "run-foreign", "idempotency_key": "other-system-key"},
        }
    )
    adapter = CursorCloudAdapter(transport=transport)
    claim = reconcile_cursor_create(
        ledger,
        adapter,
        job_id=job.job_id,
        action_id="create_run",
        **_origin_kwargs(job),
    )
    assert key == f"cursor:{job.job_id}:create_run"
    assert claim.status is EffectStatus.UNKNOWN
    assert claim.provider_run_id is None
    assert ledger.get_mapping(job.job_id).provider_run_id is None


def test_typed_status_result_validates_expected_run_identity():
    """Pre-typed UNIQUE results must still fail closed on run_id mismatch."""
    from agent.durable_jobs.cursor_cloud import (
        CursorCloudAdapter,
        CursorRun,
        CursorRunState,
        CursorStatusKind,
        CursorStatusResult,
        normalize_status_result,
    )

    typed = CursorStatusResult(
        kind=CursorStatusKind.UNIQUE,
        run=CursorRun(
            run_id="wrong-run",
            idempotency_key="cursor:job:create_run",
            state=CursorRunState.RUNNING,
        ),
    )
    result = normalize_status_result(typed, expected_run_id="expected-run")
    assert result.kind is CursorStatusKind.UNKNOWN
    assert result.kind is not CursorStatusKind.UNIQUE

    transport = MemoryCursorTransport(status_payload=typed)
    adapter = CursorCloudAdapter(transport=transport)
    observed = adapter.status_run(run_id="expected-run")
    assert observed.kind is CursorStatusKind.UNKNOWN
    assert observed.kind is not CursorStatusKind.UNIQUE

    matched = normalize_status_result(
        CursorStatusResult(
            kind=CursorStatusKind.UNIQUE,
            run=CursorRun(
                run_id="expected-run",
                idempotency_key="cursor:job:create_run",
                state=CursorRunState.RUNNING,
            ),
        ),
        expected_run_id="expected-run",
    )
    assert matched.kind is CursorStatusKind.UNIQUE
    assert matched.run is not None
    assert matched.run.run_id == "expected-run"


def test_reconcile_status_does_not_report_stale_success_when_provider_is_fail_closed(
    tmp_path,
):
    """Status UNKNOWN/AMBIGUOUS must not be reported as accepted ledger success."""
    from agent.durable_jobs.cursor_cloud import CursorCloudAdapter, CursorStatusKind
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        provider_idempotency_key,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    key = provider_idempotency_key(job.job_id, "create_run")
    transport = MemoryCursorTransport(
        create_payload={
            "kind": "accepted",
            "run": {"run_id": "run-status", "idempotency_key": key, "state": "running"},
        },
        status_payload={"run_id": "run-status", "state": "not-a-real-state"},
    )
    adapter = CursorCloudAdapter(transport=transport)
    bound = reconcile_cursor_create(
        ledger,
        adapter,
        job_id=job.job_id,
        action_id="create_run",
        **_origin_kwargs(job),
    )
    assert bound.status is EffectStatus.ACCEPTED
    assert bound.provider_run_id == "run-status"

    unknown = adapter.reconcile_status(
        ledger, job_id=job.job_id, action_id="create_run"
    )
    # Stale ACCEPTED claim must not be treated as a successful status confirm.
    assert getattr(unknown, "ok", True) is False
    observation = getattr(unknown, "observation", None)
    assert getattr(observation, "kind", None) in (
        CursorStatusKind.UNKNOWN,
        CursorStatusKind.AMBIGUOUS,
    )
    persisted = ledger.get_claim(job.job_id, "create_run")
    assert persisted is not None
    assert persisted.status is EffectStatus.ACCEPTED
    assert persisted.provider_run_id == "run-status"

    transport.status_payload = [
        {"run_id": "run-status", "idempotency_key": key},
        {"run_id": "run-other", "idempotency_key": key},
    ]
    ambiguous = adapter.reconcile_status(
        ledger, job_id=job.job_id, action_id="create_run"
    )
    assert getattr(ambiguous, "ok", True) is False
    amb_obs = getattr(ambiguous, "observation", None)
    assert getattr(amb_obs, "kind", None) is CursorStatusKind.AMBIGUOUS
    still = ledger.get_claim(job.job_id, "create_run")
    assert still is not None
    assert still.status is EffectStatus.ACCEPTED
    assert still.provider_run_id == "run-status"
    assert ledger.get_mapping(job.job_id).provider_run_id == "run-status"


def _official_v0_agent(*, agent_id: str, name: str, status: str) -> dict:
    """Cursor Cloud Agents API v0 agent record (no top-level idempotency_key)."""
    return {
        "id": agent_id,
        "name": name,
        "status": status,
        "source": {
            "repository": "https://github.com/example/repo",
            "ref": "main",
        },
        "target": {
            "branchName": "cursor/eng26-repair",
            "url": f"https://cursor.com/agents?id={agent_id}",
            "autoCreatePr": False,
            "openAsCursorGithubApp": False,
            "skipReviewerRequest": False,
        },
        "createdAt": "2024-01-15T10:30:00Z",
    }


def test_official_cursor_payloads_lost_create_adopts_unique_name_marker(tmp_path):
    """Real v0 create/list shapes: correlation is the preserved ``name`` marker.

    The Cloud Agents API does not echo a custom idempotency field. After an
    accepted-but-lost create, unique list match on that marker must be adopted.
    """
    from agent.durable_jobs.cursor_cloud import (
        CursorCloudAdapter,
        CursorCreateKind,
        cursor_correlation_name,
        normalize_create_result,
    )
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        provider_idempotency_key,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    key = provider_idempotency_key(job.job_id, "create_run")
    marker = cursor_correlation_name(key)
    created_id = "bc_abc123"
    official_create = _official_v0_agent(
        agent_id=created_id, name=marker, status="CREATING"
    )
    official_list = {
        "agents": [
            _official_v0_agent(
                agent_id=created_id, name=marker, status="RUNNING"
            ),
            _official_v0_agent(
                agent_id="bc_unrelated",
                name="Add README Documentation",
                status="FINISHED",
            ),
        ],
        "nextCursor": "bc_ghi789",
    }

    transport = MemoryCursorTransport(
        create_payload={"kind": "lost_response"},
        lookups=official_list,
    )
    adapter = CursorCloudAdapter(transport=transport)
    claim = reconcile_cursor_create(
        ledger,
        adapter,
        job_id=job.job_id,
        action_id="create_run",
        **_origin_kwargs(job),
    )
    assert claim.status is EffectStatus.ADOPTED
    assert claim.provider_run_id == created_id
    assert claim.provider_idempotency_key == key
    assert ledger.get_mapping(job.job_id).provider_run_id == created_id
    assert len(transport.create_calls) == 1
    assert transport.create_calls[0]["idempotency_key"] == key
    assert transport.lookup_calls == [key]

    normalized = normalize_create_result(official_create, expected_key=key)
    assert normalized.kind is CursorCreateKind.ACCEPTED
    assert normalized.run is not None
    assert normalized.run.run_id == created_id
    assert normalized.run.idempotency_key == key
    looked = adapter.lookup_runs(idempotency_key=key)
    assert [run.run_id for run in looked] == [created_id]
    assert all(run.idempotency_key == key for run in looked)
