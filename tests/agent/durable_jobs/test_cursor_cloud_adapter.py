"""ENG-30 — inactive/fail-closed Cursor Cloud adapter behind CursorProviderPort.

Deterministic injected transport only. No live Cursor requests, network,
credentials, or dispatch enablement. Ledger identity stays in the existing
provider effect ledger.
"""

from __future__ import annotations

import re
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

    def create(
        self, *, idempotency_key: str, job_id: str, name: str = "", agent_id: str = ""
    ) -> Any:
        with self._lock:
            self.create_calls.append(
                {
                    "idempotency_key": idempotency_key,
                    "job_id": job_id,
                    "name": name,
                    "agent_id": agent_id,
                }
            )
            if callable(self.create_payload) and not isinstance(
                self.create_payload, type
            ):
                return self.create_payload(
                    idempotency_key=idempotency_key,
                    job_id=job_id,
                    name=name,
                    agent_id=agent_id,
                )
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

    def status(self, *, run_id: str, agent_id: str = "") -> Any:
        with self._lock:
            self.status_calls.append({"run_id": run_id, "agent_id": agent_id})
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
    def create(
        self, *, idempotency_key: str, job_id: str, name: str = "", agent_id: str = ""
    ):
        raise AssertionError("transport must not be called in isolation test")

    def lookup(self, *, idempotency_key: str):
        raise AssertionError("transport must not be called in isolation test")

    def status(self, *, run_id: str, agent_id: str = ""):
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


@dataclass
class OfficialNameMarkerTransport:
    """Create stores the supplied ``name``; lookup echoes official list/get.

    Lookup payloads never include ``idempotency_key``. Agents are listed only
    from the ``name`` argument actually passed to ``create``.
    """

    extra_agents: List[Any] = field(default_factory=list)
    create_calls: list = field(default_factory=list)
    lookup_calls: list = field(default_factory=list)
    status_calls: list = field(default_factory=list)
    _agents: list = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def create(
        self, *, idempotency_key: str, job_id: str, name: str, agent_id: str = ""
    ) -> Any:
        with self._lock:
            self.create_calls.append(
                {
                    "idempotency_key": idempotency_key,
                    "job_id": job_id,
                    "name": name,
                    "agent_id": agent_id,
                }
            )
            agent = _official_v0_agent(
                agent_id="bc_abc123", name=name, status="CREATING"
            )
            assert "idempotency_key" not in agent
            self._agents.append(agent)
            return {"kind": "lost_response"}

    def lookup(self, *, idempotency_key: str) -> Any:
        with self._lock:
            self.lookup_calls.append(idempotency_key)
            listed = [{**agent, "status": "RUNNING"} for agent in self._agents]
            listed.extend(self.extra_agents)
            envelope = {"agents": listed, "nextCursor": "bc_ghi789"}
            assert all("idempotency_key" not in item for item in envelope["agents"])
            return envelope

    def status(self, *, run_id: str, agent_id: str = "") -> Any:
        with self._lock:
            self.status_calls.append({"run_id": run_id, "agent_id": agent_id})
            for agent in self._agents:
                if agent.get("id") == run_id:
                    return dict(agent)
            return None


def test_official_cursor_payloads_lost_create_adopts_unique_name_marker(tmp_path):
    """Official v0 list/get: exact ledger key is create ``name``, then adopted.

    Overlay-safe: imports only symbols present on parent deab218. Does not
    import cursor_correlation_name or other post-parent production helpers.
    """
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
    transport = OfficialNameMarkerTransport(
        extra_agents=[
            _official_v0_agent(
                agent_id="bc_unrelated",
                name="Add README Documentation",
                status="FINISHED",
            ),
            _official_v0_agent(
                agent_id="bc_substring",
                name=f"ENG-26 disposable {key} leftover",
                status="RUNNING",
            ),
            _official_v0_agent(
                agent_id="bc_foreign",
                name="cursor:other-job:create_run",
                status="RUNNING",
            ),
        ]
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
    assert claim.provider_run_id == "bc_abc123"
    assert claim.provider_idempotency_key == key
    assert ledger.get_mapping(job.job_id).provider_run_id == "bc_abc123"
    assert len(transport.create_calls) == 1
    assert transport.create_calls[0]["idempotency_key"] == key
    assert transport.create_calls[0]["name"] == key
    assert transport.lookup_calls == [key]
    raw_list = transport.lookup(idempotency_key=key)
    assert isinstance(raw_list, dict)
    assert all("idempotency_key" not in item for item in raw_list["agents"])
    looked = adapter.lookup_runs(idempotency_key=key)
    assert [run.run_id for run in looked] == ["bc_abc123"]
    assert all(run.idempotency_key == key for run in looked)


def test_overlong_provider_key_is_rejected_without_truncated_dispatch():
    from agent.durable_jobs.cursor_cloud import (
        CursorCloudAdapter,
        CursorCreateKind,
    )

    long_key = "cursor:" + ("j" * 120) + ":create_run"
    assert len(long_key) > 100
    transport = MemoryCursorTransport(
        create_payload={
            "kind": "accepted",
            "run": {"run_id": "run-trunc", "idempotency_key": long_key},
        }
    )
    adapter = CursorCloudAdapter(transport=transport)
    created = adapter.create_run(idempotency_key=long_key, job_id="job")
    assert created.kind is CursorCreateKind.UNKNOWN
    assert created.kind is not CursorCreateKind.ACCEPTED
    assert transport.create_calls == []
    assert adapter.lookup_runs(idempotency_key=long_key) == []


def test_cursor_correlation_name_equals_untruncated_ledger_key():
    from agent.durable_jobs.cursor_cloud import cursor_correlation_name
    from agent.durable_jobs.effects import provider_idempotency_key

    key = provider_idempotency_key("dj_" + ("a" * 32), "create_run")
    assert len(key) <= 100
    assert cursor_correlation_name(key) == key
    with pytest.raises(ValueError, match="name limit"):
        cursor_correlation_name("cursor:" + ("j" * 120) + ":create_run")


def _official_v1_list_item(
    *,
    agent_id: str,
    name: str,
    status: str = "ACTIVE",
    latest_run_id: str = "run-00000000-0000-0000-0000-000000000099",
) -> dict:
    """GET /v1/agents list item: identity fields only. No prompt, no idempotency_key."""
    item = {
        "id": agent_id,
        "name": name,
        "status": status,
        "env": {"type": "cloud"},
        "url": f"https://cursor.com/agents/{agent_id}",
        "createdAt": "2026-08-13T20:00:00.000Z",
        "updatedAt": "2026-08-13T20:00:00.000Z",
        "latestRunId": latest_run_id,
    }
    assert "prompt" not in item
    assert "idempotency_key" not in item
    return item


@dataclass
class OfficialV1GeneratedNameTransport:
    """Live v1 contract: create ``name`` is overwritten; list echoes ``items[].id``.

    ``create`` accepts optional ``agent_id`` so this double can run against
    f33ed150 (which does not pass it). Lookup never injects ``idempotency_key``.
    """

    extra_items: List[Any] = field(default_factory=list)
    create_calls: list = field(default_factory=list)
    lookup_calls: list = field(default_factory=list)
    status_calls: list = field(default_factory=list)
    generated_name: str = "ENG-26 disposable sandbox"
    _items: list = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def create(
        self,
        *,
        idempotency_key: str,
        job_id: str,
        name: str = "",
        agent_id: str = "",
    ) -> Any:
        with self._lock:
            self.create_calls.append(
                {
                    "idempotency_key": idempotency_key,
                    "job_id": job_id,
                    "name": name,
                    "agent_id": agent_id,
                }
            )
            stored_id = (agent_id or "").strip() or (
                "bc-aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
            )
            item = _official_v1_list_item(
                agent_id=stored_id,
                name=self.generated_name,
                status="ACTIVE",
            )
            assert item["name"] != name
            self._items.append(item)
            return {"kind": "lost_response"}

    def lookup(self, *, idempotency_key: str) -> Any:
        with self._lock:
            self.lookup_calls.append(idempotency_key)
            items = [dict(item) for item in self._items]
            items.extend(dict(extra) for extra in self.extra_items)
            envelope = {
                "items": items,
                "nextCursor": "bc-ffffffff-ffff-4fff-8fff-ffffffffffff",
            }
            assert all("idempotency_key" not in item for item in envelope["items"])
            assert all("prompt" not in item for item in envelope["items"])
            return envelope

    def status(self, *, run_id: str, agent_id: str = "") -> Any:
        with self._lock:
            self.status_calls.append({"run_id": run_id, "agent_id": agent_id})
            for item in self._items:
                latest = item.get("latestRunId")
                if latest != run_id:
                    continue
                if agent_id and str(item.get("id") or "").lower() != agent_id.lower():
                    continue
                return {
                    "id": latest,
                    "agentId": item.get("id"),
                    "status": "RUNNING",
                    "createdAt": item.get("createdAt"),
                    "updatedAt": item.get("updatedAt"),
                }
            return None


def test_v1_items_generated_name_lost_create_adopts_client_agent_id(tmp_path):
    """Live v1 list envelope: generated names are not correlation; ``id`` is.

    Overlay-safe: imports only symbols present on f33ed150. Does not import
    cursor_correlation_agent_id or other post-parent production helpers.

    Intended RED on f33ed150: create does not pass agent_id, list names are
    human-readable (not the ledger key), so lookup stays empty and reconcile
    returns RECOVERING instead of ADOPTED.
    """
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
    generated = "ENG-26 disposable sandbox"
    assert 23 <= len(generated) <= 33
    assert generated != key
    transport = OfficialV1GeneratedNameTransport(
        generated_name=generated,
        extra_items=[
            _official_v1_list_item(
                agent_id="bc-99999999-9999-4999-8999-999999999999",
                name="Investigate flaky CI tests",
            ),
            _official_v1_list_item(
                agent_id="bc-88888888-8888-4888-8888-888888888888",
                name=f"leftover {key} sandbox",
            ),
            _official_v1_list_item(
                agent_id="bc-77777777-7777-4777-8777-777777777777",
                name="Fix cursor adapter contract",
            ),
        ],
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
    sent = transport.create_calls[0]["agent_id"]
    assert sent
    assert re.fullmatch(
        r"^bc-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
        sent,
    )
    latest_run = "run-00000000-0000-0000-0000-000000000099"
    assert claim.provider_run_id == latest_run
    assert claim.provider_run_id != sent
    assert claim.provider_idempotency_key == key
    assert ledger.get_mapping(job.job_id).provider_run_id == latest_run
    assert len(transport.create_calls) == 1
    assert transport.create_calls[0]["idempotency_key"] == key
    assert transport.lookup_calls == [key]

    raw_list = transport.lookup(idempotency_key=key)
    assert isinstance(raw_list, dict)
    assert "items" in raw_list
    assert "nextCursor" in raw_list
    assert "agents" not in raw_list
    assert all("idempotency_key" not in item for item in raw_list["items"])
    assert all("prompt" not in item for item in raw_list["items"])
    created_item = next(item for item in raw_list["items"] if item["id"] == sent)
    assert created_item["name"] == generated
    assert created_item["name"] != key
    assert 23 <= len(created_item["name"]) <= 33

    looked = adapter.lookup_runs(idempotency_key=key)
    assert [run.run_id for run in looked] == [latest_run]
    assert all(run.idempotency_key == key for run in looked)
    assert all(run.agent_id == sent for run in looked)

    second = reconcile_cursor_create(
        ledger,
        adapter,
        job_id=job.job_id,
        action_id="create_run",
        **_origin_kwargs(job),
    )
    assert second.status is EffectStatus.ADOPTED
    assert second.provider_run_id == latest_run
    assert len(transport.create_calls) == 1


def test_typed_sdk_foreign_record_is_not_adoptable(tmp_path):
    """Typed SDK-like foreign records must not inherit the caller ledger key.

    On 65c3fe4, ``_looks_like_official_cursor_record`` is dict-only, so a
    typed object without explicit correlation falls back to ``expected_key``
    and is incorrectly adoptable. Repair must normalize typed/raw the same
    way — not special-case SimpleNamespace.
    """
    from types import SimpleNamespace

    from agent.durable_jobs.cursor_cloud import (
        CursorCloudAdapter,
        parse_lookup_runs,
    )
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        provider_idempotency_key,
        reconcile_cursor_create,
    )

    foreign = SimpleNamespace(
        id="bc-99999999-9999-4999-8999-999999999999",
        name="unrelated",
        status="ACTIVE",
    )
    assert not hasattr(foreign, "idempotency_key")
    parsed = parse_lookup_runs(
        [foreign], expected_key="cursor:job123:create_run"
    )
    assert parsed == []

    store, job = _make_job(tmp_path)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    key = provider_idempotency_key(job.job_id, "create_run")
    transport = MemoryCursorTransport(
        create_payload={"kind": "lost_response"},
        lookups=[
            SimpleNamespace(
                id="bc-99999999-9999-4999-8999-999999999999",
                name="unrelated",
                status="ACTIVE",
            )
        ],
    )
    adapter = CursorCloudAdapter(transport=transport)
    claim = reconcile_cursor_create(
        ledger,
        adapter,
        job_id=job.job_id,
        action_id="create_run",
        **_origin_kwargs(job),
    )
    assert claim.status is not EffectStatus.ADOPTED
    assert claim.status is EffectStatus.RECOVERING
    assert claim.provider_run_id is None
    assert len(transport.create_calls) == 1
    looked = adapter.lookup_runs(idempotency_key=key)
    assert looked == []


def test_typed_sdk_derived_id_adopts_and_matches_raw_dict(tmp_path):
    """Typed and raw official records must agree; exact derived id adopts."""
    from types import SimpleNamespace

    from agent.durable_jobs.cursor_cloud import (
        CursorCloudAdapter,
        cursor_correlation_agent_id,
        parse_lookup_runs,
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
    derived = cursor_correlation_agent_id(key)
    generated = "ENG-26 disposable sandbox"
    latest_run = "run-00000000-0000-0000-0000-000000000099"
    raw_exact = {
        "id": derived,
        "name": generated,
        "status": "ACTIVE",
        "latestRunId": latest_run,
    }
    typed_exact = SimpleNamespace(
        id=derived,
        name=generated,
        status="ACTIVE",
        latestRunId=latest_run,
    )
    raw_foreign = {
        "id": "bc-99999999-9999-4999-8999-999999999999",
        "name": "unrelated",
        "status": "ACTIVE",
    }
    typed_foreign = SimpleNamespace(
        id="bc-99999999-9999-4999-8999-999999999999",
        name="unrelated",
        status="ACTIVE",
    )
    typed_latest_trap = SimpleNamespace(
        id="bc-88888888-8888-4888-8888-888888888888",
        name="unrelated",
        status="ACTIVE",
        latestRunId=derived,
    )
    for raw, typed in (
        (raw_exact, typed_exact),
        (raw_foreign, typed_foreign),
    ):
        raw_parsed = parse_lookup_runs([raw], expected_key=key)
        typed_parsed = parse_lookup_runs([typed], expected_key=key)
        assert [
            (r.run_id, r.idempotency_key, r.agent_id) for r in raw_parsed
        ] == [(r.run_id, r.idempotency_key, r.agent_id) for r in typed_parsed]
    assert [
        (r.run_id, r.idempotency_key, r.agent_id)
        for r in parse_lookup_runs([typed_exact], expected_key=key)
    ] == [(latest_run, key, derived)]
    assert parse_lookup_runs([typed_foreign], expected_key=key) == []
    assert parse_lookup_runs([typed_latest_trap], expected_key=key) == []
    mixed = parse_lookup_runs(
        [typed_foreign, typed_exact, typed_latest_trap], expected_key=key
    )
    assert [r.run_id for r in mixed] == [latest_run]
    assert all(r.idempotency_key == key for r in mixed)
    assert all(r.agent_id == derived for r in mixed)

    transport = MemoryCursorTransport(
        create_payload={"kind": "lost_response"},
        lookups=[typed_foreign, typed_exact, typed_latest_trap],
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
    assert claim.provider_run_id == latest_run
    assert claim.provider_run_id != derived
    assert claim.provider_idempotency_key == key
    assert len(transport.create_calls) == 1
    second = reconcile_cursor_create(
        ledger,
        adapter,
        job_id=job.job_id,
        action_id="create_run",
        **_origin_kwargs(job),
    )
    assert second.status is EffectStatus.ADOPTED
    assert second.provider_run_id == latest_run
    assert len(transport.create_calls) == 1


@dataclass
class V1DistinctAgentRunTransport(OfficialV1GeneratedNameTransport):
    """v1 create/list plus GET /agents/{agent_id}/runs/{run_id} recording."""

    def status(self, *, run_id: str, agent_id: str = "") -> Any:
        with self._lock:
            self.status_calls.append({"run_id": run_id, "agent_id": agent_id})
            for item in self._items:
                latest = item.get("latestRunId")
                if latest != run_id:
                    continue
                if agent_id and str(item.get("id") or "").lower() != agent_id.lower():
                    continue
                return {
                    "id": latest,
                    "agentId": item.get("id"),
                    "status": "RUNNING",
                    "createdAt": item.get("createdAt"),
                    "updatedAt": item.get("updatedAt"),
                }
            return None


def test_v1_lost_create_persists_run_id_not_agent_id_for_status(tmp_path):
    """v1 agent id correlates; persisted provider_run_id is the run id.

    Live GET /v1/agents/{agent_id}/runs/{run_id} needs both. On e46384c the
    adapter persists the deterministic agent id as provider_run_id and then
    calls status with that agent id as the run id.
    """
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
    latest_run = "run-00000000-0000-0000-0000-000000000099"
    transport = V1DistinctAgentRunTransport(
        generated_name="ENG-26 disposable sandbox",
        extra_items=[
            _official_v1_list_item(
                agent_id="bc-99999999-9999-4999-8999-999999999999",
                name="Investigate flaky CI tests",
            ),
            _official_v1_list_item(
                agent_id="bc-88888888-8888-4888-8888-888888888888",
                name=f"leftover {key} sandbox",
                latest_run_id="run-11111111-1111-4111-8111-111111111111",
            ),
        ],
    )
    adapter = CursorCloudAdapter(transport=transport)
    kwargs = dict(job_id=job.job_id, action_id="create_run", **_origin_kwargs(job))
    claim = reconcile_cursor_create(ledger, adapter, **kwargs)
    assert claim.status is EffectStatus.ADOPTED
    sent_agent = transport.create_calls[0]["agent_id"]
    assert sent_agent
    assert claim.provider_run_id == latest_run
    assert claim.provider_run_id != sent_agent
    assert ledger.get_mapping(job.job_id).provider_run_id == latest_run
    assert len(transport.create_calls) == 1

    looked = adapter.lookup_runs(idempotency_key=key)
    assert [run.run_id for run in looked] == [latest_run]
    assert all(getattr(run, "agent_id", sent_agent) == sent_agent for run in looked)

    second = reconcile_cursor_create(ledger, adapter, **kwargs)
    assert second.status is EffectStatus.ADOPTED
    assert second.provider_run_id == latest_run
    assert len(transport.create_calls) == 1

    reopened = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    persisted = reopened.get_claim(job.job_id, "create_run")
    assert persisted is not None
    assert persisted.status is EffectStatus.ADOPTED
    assert persisted.provider_run_id == latest_run
    assert reopened.get_mapping(job.job_id).provider_run_id == latest_run

    restarted = CursorCloudAdapter(transport=transport)
    observed = restarted.reconcile_status(
        reopened, job_id=job.job_id, action_id="create_run"
    )
    assert observed.ok is True
    assert observed.claim.provider_run_id == latest_run
    assert transport.status_calls[-1]["run_id"] == latest_run
    assert transport.status_calls[-1]["agent_id"] == sent_agent
    assert transport.status_calls[-1]["run_id"] != sent_agent


def test_v1_matching_agent_without_distinct_run_id_is_not_adoptable(tmp_path):
    """v1 client agent id without a distinct run id must not be stored as the run."""
    from agent.durable_jobs.cursor_cloud import (
        CursorCloudAdapter,
        cursor_correlation_agent_id,
        parse_lookup_runs,
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
    derived = cursor_correlation_agent_id(key)
    item = {
        "id": derived,
        "name": "ENG-26 disposable sandbox",
        "status": "ACTIVE",
        "env": {"type": "cloud"},
        "url": f"https://cursor.com/agents/{derived}",
        "createdAt": "2026-08-13T20:00:00.000Z",
        "updatedAt": "2026-08-13T20:00:00.000Z",
    }
    assert "latestRunId" not in item
    assert parse_lookup_runs({"items": [item]}, expected_key=key) == []

    transport = MemoryCursorTransport(
        create_payload={"kind": "lost_response"},
        lookups={"items": [item], "nextCursor": "bc-ffffffff-ffff-4fff-8fff-ffffffffffff"},
    )
    adapter = CursorCloudAdapter(transport=transport)
    claim = reconcile_cursor_create(
        ledger,
        adapter,
        job_id=job.job_id,
        action_id="create_run",
        **_origin_kwargs(job),
    )
    assert claim.status is not EffectStatus.ADOPTED
    assert claim.status is EffectStatus.RECOVERING
    assert claim.provider_run_id is None
    assert claim.provider_run_id != derived
    assert len(transport.create_calls) == 1
    second = reconcile_cursor_create(
        ledger,
        adapter,
        job_id=job.job_id,
        action_id="create_run",
        **_origin_kwargs(job),
    )
    assert second.status is EffectStatus.RECOVERING
    assert len(transport.create_calls) == 1


def test_v1_duplicate_exact_matches_fail_closed_without_redispatch(tmp_path):
    """Two exact v1 agent+run matches are ambiguous; create is not retried."""
    from agent.durable_jobs.cursor_cloud import (
        CursorCloudAdapter,
        cursor_correlation_agent_id,
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
    derived = cursor_correlation_agent_id(key)
    item = _official_v1_list_item(
        agent_id=derived,
        name="ENG-26 disposable sandbox",
        latest_run_id="run-00000000-0000-0000-0000-000000000099",
    )
    transport = MemoryCursorTransport(
        create_payload={"kind": "lost_response"},
        lookups={"items": [dict(item), dict(item)]},
    )
    adapter = CursorCloudAdapter(transport=transport)
    claim = reconcile_cursor_create(
        ledger,
        adapter,
        job_id=job.job_id,
        action_id="create_run",
        **_origin_kwargs(job),
    )
    assert claim.status is EffectStatus.UNKNOWN
    assert claim.unknown_reason == UnknownReason.PROVIDER_AMBIGUOUS.value
    assert claim.provider_run_id is None
    second = reconcile_cursor_create(
        ledger,
        adapter,
        job_id=job.job_id,
        action_id="create_run",
        **_origin_kwargs(job),
    )
    assert second.status is EffectStatus.UNKNOWN
    assert len(transport.create_calls) == 1
