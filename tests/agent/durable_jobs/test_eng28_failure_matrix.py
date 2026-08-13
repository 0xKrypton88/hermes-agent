"""ENG-28 deterministic failure-injection matrix.

Real temp SQLite, stateful fakes, fresh subprocesses, FrozenClock.
No network. ENG-29 Go gating is not weakened.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import textwrap
import threading
from pathlib import Path

import pytest

from tests.agent.durable_jobs.eng28_support import (
    FakeCreateResult,
    FakePostResult,
    FakePosted,
    FakeRun,
    RecordingAckPort,
    StatefulCursorProvider,
    StatefulSlackPort,
    child_env,
    db_path,
    deny_network,
    load_matrix,
    make_job,
)


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    deny_network(monkeypatch)


def test_eng28_matrix_artifact_covers_all_22_rows():
    matrix = load_matrix()
    rows = matrix["rows"]
    assert len(rows) == 22
    ids = [int(row["id"]) for row in rows]
    assert ids == list(range(1, 23))
    allowed = {
        "PROVEN_LOCAL",
        "PARTIAL",
        "BLOCKED_EXTERNAL",
        "BLOCKED_MISSING_SEAM",
    }
    counts = {key: 0 for key in allowed}
    for row in rows:
        assert row["selectors"], f"row {row['id']} missing selectors"
        assert row["proof_layer"] in allowed
        assert row["proof_layer"] != "PENDING"
        counts[row["proof_layer"]] += 1
    coverage = matrix["coverage"]
    for key in allowed:
        assert coverage[key] == counts[key]
    assert sum(coverage[key] for key in allowed) == 22
    assert "SQLite is not PostgreSQL" in matrix["disclaimer"]
    assert "not sandbox E2E" in matrix["disclaimer"]


# ---------------------------------------------------------------------------
# Row 1 — before/after immutable job/package commit
# ---------------------------------------------------------------------------


def test_row01_crash_before_job_commit_persists_nothing(tmp_path, monkeypatch):
    from agent.durable_jobs import store as store_mod
    from agent.durable_jobs.store import DurableJobStore

    def boom() -> None:
        raise RuntimeError("injected crash before job commit")

    monkeypatch.setattr(
        store_mod, "after_job_rows_before_commit", boom, raising=False
    )

    store = DurableJobStore(sqlite_path=db_path(tmp_path))
    with pytest.raises(RuntimeError, match="injected crash before job commit"):
        store.create_job(
            origin_platform="slack",
            origin_chat_id="C123",
            origin_root_thread_id="111.222",
            objective="crash before commit",
            repository_identity="github.com/example/repo",
            idempotency_key="idem-row01-before",
        )
    conn = sqlite3.connect(store.sqlite_path)
    try:
        (n,) = conn.execute("SELECT COUNT(*) FROM durable_jobs").fetchone()
        (e,) = conn.execute("SELECT COUNT(*) FROM durable_job_events").fetchone()
    finally:
        conn.close()
    assert n == 0
    assert e == 0


def test_row01_crash_after_job_commit_reopens_exactly_one(tmp_path):
    script = textwrap.dedent(
        """
        import sys
        from pathlib import Path
        from agent.durable_jobs.store import DurableJobStore
        store = DurableJobStore(sqlite_path=Path(sys.argv[1]))
        store.create_job(
            origin_platform="slack",
            origin_chat_id="C123",
            origin_root_thread_id="111.222",
            objective="crash after commit",
            repository_identity="github.com/example/repo",
            idempotency_key="idem-row01-after",
        )
        """
    )
    db = db_path(tmp_path)
    proc = subprocess.run(
        [sys.executable, "-c", script, str(db)],
        env=child_env(),
        check=True,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0
    from agent.durable_jobs.store import DurableJobStore

    store = DurableJobStore(sqlite_path=db)
    first = store.get_job_by_idempotency_key("idem-row01-after")
    assert first is not None
    second = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="crash after commit",
        repository_identity="github.com/example/repo",
        idempotency_key="idem-row01-after",
    )
    assert second.job_id == first.job_id
    assert store.count_jobs() == 1
    events = store.list_events(first.job_id)
    assert any(ev["event_type"] == "job_created" for ev in events)


def test_row01_tuple_rejected_before_job_exists(tmp_path):
    from agent.durable_jobs.eng29 import (
        MATRIX_VERSION,
        PROVIDER_CREATE_TARGET_ACTION,
        register_authorization_tuple,
    )
    from agent.durable_jobs.store import DurableJobStore

    DurableJobStore(sqlite_path=db_path(tmp_path))
    result = register_authorization_tuple(
        db_path(tmp_path),
        job_id="dj_missing",
        source_package_id="github.com/example/repo",
        source_package_version="v1",
        candidate_sha="sha-eng28",
        candidate_id="cand-1",
        candidate_version="v1",
        target_environment="slack",
        target_action=PROVIDER_CREATE_TARGET_ACTION,
        authorized_actor="U-alice",
        expires_at="2099-01-01T00:00:00+00:00",
        policy_version="pol-1",
        matrix_version=MATRIX_VERSION,
        authorization_idempotency_key="tuple:missing:create",
    )
    assert result.ok is False
    assert "unauthorized" in result.reason_codes


def _bind_policy(store, job) -> None:
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.slack_contract import SlackBindingLedger

    SlackBindingLedger(sqlite_path=store.sqlite_path).bind(
        job_id=job.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )
    DecisionLedger(sqlite_path=store.sqlite_path).set_policy(
        job_id=job.job_id,
        policy_version="pol-1",
        allowed_actors=("U-alice",),
        expires_at="2099-01-01T00:00:00+00:00",
    )


def _provider_kwargs(job) -> dict:
    return dict(
        job_id=job.job_id,
        action_id="create_run",
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )


def _go_kwargs(job, **overrides) -> dict:
    base = dict(
        job_id=job.job_id,
        decision_type="go",
        candidate_id="cand-1",
        candidate_version="v1",
        actor_id="U-alice",
        policy_version="pol-1",
        decision_idempotency_key=f"go:{job.job_id}:cursor.create_run",
        source_package_id=job.repository_identity,
        source_package_version="v1",
        candidate_sha=job.frozen_baseline_sha,
        target_environment="slack",
        target_action="cursor.create_run",
        matrix_version="eng29-matrix-v1",
    )
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Row 2 — before decision and Go persist/consume
# ---------------------------------------------------------------------------


def test_row02_crash_before_go_commit_blocks_effect(tmp_path, monkeypatch):
    from agent.durable_jobs import decisions as decisions_mod
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.effects import ProviderEffectLedger
    from agent.durable_jobs.eng29 import AuthorizationDenied

    store, job = make_job(tmp_path, authorize=False, idempotency_key="idem-row02-before")
    _bind_policy(store, job)

    def boom() -> None:
        raise RuntimeError("injected crash before go commit")

    monkeypatch.setattr(
        decisions_mod, "after_decision_rows_before_commit", boom, raising=False
    )
    ledger = DecisionLedger(sqlite_path=store.sqlite_path)
    with pytest.raises(RuntimeError, match="injected crash before go commit"):
        ledger.record_decision(**_go_kwargs(job))
    assert ledger.count_decisions(job.job_id) == 0

    effects = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    with pytest.raises((AuthorizationDenied, Exception)):
        effects.claim_effect(**_provider_kwargs(job))
    assert effects.get_claim(job.job_id, "create_run") is None


def test_row02_go_persist_then_consume_claim(tmp_path):
    from agent.durable_jobs.effects import EffectStatus, ProviderEffectLedger

    store, job = make_job(tmp_path, idempotency_key="idem-row02-consume")
    effects = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    claimed = effects.claim_effect(**_provider_kwargs(job))
    assert claimed.won is True
    assert claimed.claim.status is EffectStatus.CLAIMED


# ---------------------------------------------------------------------------
# Row 3 — concurrent Go/Hold/Cancel and crash during consume+claim
# ---------------------------------------------------------------------------


def test_row03_concurrent_go_hold_cancel_cancel_is_terminal(tmp_path):
    from agent.durable_jobs.decisions import DecisionLedger, JobCanceledError
    from agent.durable_jobs.effects import ProviderEffectLedger

    store, job = make_job(tmp_path, authorize=False, idempotency_key="idem-row03-conc")
    _bind_policy(store, job)
    ledger = DecisionLedger(sqlite_path=store.sqlite_path)
    barrier = threading.Barrier(3)
    results = []

    def worker(kind: str) -> None:
        barrier.wait()
        results.append(
            ledger.record_decision(
                **_go_kwargs(
                    job,
                    decision_type=kind,
                    decision_idempotency_key=f"{kind}:{job.job_id}:row03",
                    target_action="cursor.create_run",
                )
            )
        )

    threads = [
        threading.Thread(target=worker, args=(kind,))
        for kind in ("go", "hold", "cancel")
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
    assert ledger.is_canceled(job.job_id) is True
    effects = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    with pytest.raises(JobCanceledError):
        effects.claim_effect(**_provider_kwargs(job))
    assert effects.get_claim(job.job_id, "create_run") is None


def test_row03_crash_during_consume_claim_no_create_run(tmp_path):
    store, job = make_job(tmp_path, idempotency_key="idem-row03-crash")
    script = textwrap.dedent(
        """
        import os, sys
        from pathlib import Path
        from agent.durable_jobs import eng29 as eng29_mod
        from agent.durable_jobs.effects import ProviderEffectLedger

        def boom():
            os._exit(17)

        eng29_mod.after_in_transaction_adapter_go = boom
        ledger = ProviderEffectLedger(sqlite_path=Path(sys.argv[1]))
        ledger.claim_effect(
            job_id=sys.argv[2],
            action_id="create_run",
            origin_platform="slack",
            origin_chat_id="C123",
            origin_root_thread_id="111.222",
            candidate_id="cand-1",
            candidate_version="v1",
        )
        """
    )
    proc = subprocess.run(
        [sys.executable, "-c", script, str(store.sqlite_path), job.job_id],
        env=child_env(),
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 17
    from agent.durable_jobs.effects import ProviderEffectLedger

    effects = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    assert effects.get_claim(job.job_id, "create_run") is None
    assert effects.count_claims() == 0


# ---------------------------------------------------------------------------
# Row 4 — effect claim before provider create
# ---------------------------------------------------------------------------


def test_row04_claim_before_create_zero_effect_without_go(tmp_path):
    from agent.durable_jobs.effects import (
        ProviderEffectLedger,
        reconcile_cursor_create,
    )
    from agent.durable_jobs.eng29 import AuthorizationDenied

    store, job = make_job(tmp_path, authorize=False, idempotency_key="idem-row04")
    _bind_policy(store, job)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    provider = StatefulCursorProvider(
        FakeCreateResult(kind="accepted", run=FakeRun("run-1", "k"))
    )
    with pytest.raises(AuthorizationDenied):
        reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    assert provider.create_calls == []
    assert ledger.get_claim(job.job_id, "create_run") is None


# ---------------------------------------------------------------------------
# Row 5 — timeout/5xx/busy; adopt or PROVIDER_AMBIGUOUS; never blind retry
# ---------------------------------------------------------------------------


def test_row05_timeout_unique_lookup_adopts_without_retry(tmp_path):
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        provider_idempotency_key,
        reconcile_cursor_create,
    )

    store, job = make_job(tmp_path, idempotency_key="idem-row05-timeout")
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    key = provider_idempotency_key(job.job_id, "create_run")
    provider = StatefulCursorProvider(
        FakeCreateResult(kind="timeout"),
        lookups=[FakeRun("run-t", key)],
    )
    first = reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    assert first.status is EffectStatus.ADOPTED
    assert first.provider_run_id == "run-t"
    second = reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    assert second.status is EffectStatus.ADOPTED
    assert provider.lookup_calls == [key]
    assert len(provider.create_calls) == 1


@pytest.mark.parametrize("kind", ("http_5xx", "busy"))
def test_row05_http_5xx_busy_never_blind_retry(tmp_path, kind):
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        provider_idempotency_key,
        reconcile_cursor_create,
    )

    store, job = make_job(tmp_path, idempotency_key=f"idem-row05-{kind}")
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    kwargs = _provider_kwargs(job)
    kwargs["action_id"] = f"create_run_{kind}"
    key = provider_idempotency_key(job.job_id, kwargs["action_id"])
    provider = StatefulCursorProvider(
        FakeCreateResult(kind=kind),
        lookups=[FakeRun(f"run-{kind}", key)],
    )
    result = reconcile_cursor_create(ledger, provider, **kwargs)
    assert result.status is EffectStatus.ADOPTED
    assert result.provider_run_id == f"run-{kind}"
    retry = reconcile_cursor_create(ledger, provider, **kwargs)
    assert retry.status is EffectStatus.ADOPTED
    assert retry.provider_run_id == f"run-{kind}"
    assert provider.lookup_calls == [key]
    assert len(provider.create_calls) == 1


def test_row05_multiple_lookup_is_provider_ambiguous(tmp_path):
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        UnknownReason,
        provider_idempotency_key,
        reconcile_cursor_create,
    )

    store, job = make_job(tmp_path, idempotency_key="idem-row05-ambig")
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    key = provider_idempotency_key(job.job_id, "create_run")
    provider = StatefulCursorProvider(
        FakeCreateResult(kind="timeout"),
        lookups=[FakeRun("run-a", key), FakeRun("run-b", key)],
    )
    first = reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    assert first.status is EffectStatus.UNKNOWN
    assert first.unknown_reason == UnknownReason.PROVIDER_AMBIGUOUS.value
    retry = reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    assert retry.status is EffectStatus.UNKNOWN
    assert len(provider.create_calls) == 1


# ---------------------------------------------------------------------------
# Row 6 — mismatch/orphan fail closed
# ---------------------------------------------------------------------------


def test_row06_accepted_orphan_run_fail_closed(tmp_path):
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        UnknownReason,
        reconcile_cursor_create,
    )

    store, job = make_job(tmp_path, idempotency_key="idem-row06-orphan")
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    provider = StatefulCursorProvider(
        FakeCreateResult(kind="accepted", run=FakeRun(None, "cursor:x:y"))
    )
    result = reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    assert result.status is EffectStatus.UNKNOWN
    assert result.unknown_reason == UnknownReason.ORPHAN_RESPONSE.value
    assert result.provider_run_id is None
    assert ledger.get_mapping(job.job_id).provider_run_id is None
    assert len(provider.create_calls) == 1


def test_row06_idempotency_mismatch_fail_closed(tmp_path):
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        UnknownReason,
        reconcile_cursor_create,
    )

    store, job = make_job(tmp_path, idempotency_key="idem-row06-mismatch")
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    provider = StatefulCursorProvider(
        FakeCreateResult(
            kind="accepted", run=FakeRun("run-other", "cursor:other:action")
        )
    )
    result = reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    assert result.status is EffectStatus.UNKNOWN
    assert result.unknown_reason == UnknownReason.CORRELATION_MISMATCH.value
    assert result.provider_run_id is None
    assert len(provider.create_calls) == 1


# ---------------------------------------------------------------------------
# Row 7 — duplicate ingress / restart one effect
# ---------------------------------------------------------------------------


def test_row07_restart_does_not_create_second_effect(tmp_path):
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        reconcile_cursor_create,
    )

    store, job = make_job(tmp_path, idempotency_key="idem-row07")
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    provider = StatefulCursorProvider(
        FakeCreateResult(kind="accepted", run=FakeRun("run-1", "pending"))
    )
    first = reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    assert first.status is EffectStatus.ACCEPTED
    reopened = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    second = reconcile_cursor_create(reopened, provider, **_provider_kwargs(job))
    assert second.status is EffectStatus.ACCEPTED
    assert second.provider_run_id == "run-1"
    assert len(provider.create_calls) == 1
    assert reopened.count_claims() == 1


# ---------------------------------------------------------------------------
# Row 8 — bounded lookup then typed Hold
# ---------------------------------------------------------------------------


def test_row08_empty_lookup_bound_then_typed_hold(tmp_path):
    from agent.durable_jobs.clock import DEFAULT_RECOVERY_WINDOW_SECONDS, FrozenClock
    from agent.durable_jobs.decisions import DecisionLedger, DecisionType
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        reconcile_cursor_create,
    )

    store, job = make_job(tmp_path, idempotency_key="idem-row08-empty")
    clock = FrozenClock()
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path, now_fn=clock)
    provider = StatefulCursorProvider(FakeCreateResult(kind="timeout"), lookups=[])
    first = reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    assert first.status is EffectStatus.RECOVERING
    clock.advance(DEFAULT_RECOVERY_WINDOW_SECONDS + 1)
    terminal = reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    assert terminal.status is EffectStatus.UNKNOWN
    latest = DecisionLedger(sqlite_path=store.sqlite_path).latest_accepted(job.job_id)
    assert latest is not None
    assert latest.decision_type is DecisionType.HOLD
    retry = reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    assert retry.status is EffectStatus.UNKNOWN
    assert len(provider.create_calls) == 1


def test_row08_wrong_lookup_key_is_provider_ambiguous_hold(tmp_path):
    from agent.durable_jobs.decisions import DecisionLedger, DecisionType
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        UnknownReason,
        reconcile_cursor_create,
    )

    store, job = make_job(tmp_path, idempotency_key="idem-row08-wrong")
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    provider = StatefulCursorProvider(
        FakeCreateResult(kind="timeout"),
        lookups=[FakeRun("run-wrong", "cursor:other:action")],
    )
    result = reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    assert result.status is EffectStatus.UNKNOWN
    assert result.unknown_reason == UnknownReason.PROVIDER_AMBIGUOUS.value
    assert result.provider_run_id is None
    latest = DecisionLedger(sqlite_path=store.sqlite_path).latest_accepted(job.job_id)
    assert latest is not None
    assert latest.decision_type is DecisionType.HOLD
    assert len(provider.create_calls) == 1


# ---------------------------------------------------------------------------
# Row 9 — cancellation ambiguity
# ---------------------------------------------------------------------------


def test_row09_cancel_during_inflight_does_not_bind(tmp_path):
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        reconcile_cursor_create,
    )

    store, job = make_job(tmp_path, idempotency_key="idem-row09-inflight")
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    decisions = DecisionLedger(sqlite_path=store.sqlite_path)
    gate = threading.Event()
    started = threading.Event()

    class BlockingProvider(StatefulCursorProvider):
        def create_run(self, *, idempotency_key: str, job_id: str):
            started.set()
            assert gate.wait(timeout=5)
            return FakeCreateResult(
                kind="accepted", run=FakeRun("run-late", idempotency_key)
            )

    provider = BlockingProvider(FakeCreateResult(kind="accepted"))
    outcome = []

    def runner() -> None:
        outcome.append(
            reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
        )

    thread = threading.Thread(target=runner)
    thread.start()
    assert started.wait(timeout=5)
    cancel = decisions.record_decision(
        **_go_kwargs(
            job,
            decision_type="cancel",
            decision_idempotency_key="k-cancel-row09",
        )
    )
    assert cancel.ok is True
    gate.set()
    thread.join(timeout=5)
    assert outcome
    row = ledger.get_claim(job.job_id, "create_run")
    assert row is not None
    assert row.status not in (EffectStatus.ACCEPTED, EffectStatus.ADOPTED)
    assert decisions.is_canceled(job.job_id) is True


def test_row09_crash_during_cancel_is_atomic(tmp_path, monkeypatch):
    from agent.durable_jobs import decisions as decisions_mod
    from agent.durable_jobs.decisions import DecisionLedger

    store, job = make_job(tmp_path, idempotency_key="idem-row09-crash")
    ledger = DecisionLedger(sqlite_path=store.sqlite_path)

    def boom() -> None:
        raise RuntimeError("injected crash during cancel")

    monkeypatch.setattr(
        decisions_mod, "after_decision_rows_before_commit", boom, raising=False
    )
    with pytest.raises(RuntimeError, match="injected crash during cancel"):
        ledger.record_decision(
            **_go_kwargs(
                job,
                decision_type="cancel",
                decision_idempotency_key="k-cancel-crash",
            )
        )
    assert ledger.is_canceled(job.job_id) is False
    monkeypatch.setattr(
        decisions_mod,
        "after_decision_rows_before_commit",
        lambda: None,
        raising=False,
    )
    accepted = ledger.record_decision(
        **_go_kwargs(
            job,
            decision_type="cancel",
            decision_idempotency_key="k-cancel-crash",
        )
    )
    assert accepted.ok is True
    assert ledger.is_canceled(job.job_id) is True


# ---------------------------------------------------------------------------
# Row 10 — Slack root intent before send
# ---------------------------------------------------------------------------


def test_row10_claim_before_post_root(tmp_path):
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = make_job(tmp_path, idempotency_key="idem-row10")
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    seen = {"claimed": False}

    class OrderingSlack(StatefulSlackPort):
        def post_root(self, **kwargs):
            current = ledger.get_binding(kwargs["job_id"])
            seen["claimed"] = (
                current is not None and current.status is SlackRootStatus.CLAIMED
            )
            return super().post_root(**kwargs)

    port = OrderingSlack(FakePostResult(kind="accepted", message_ts="10.1"))
    result = deliver_slack_root(ledger, port, job_id=job.job_id)
    assert seen["claimed"] is True
    assert result.status is SlackRootStatus.DELIVERED
    assert len(port.posts) == 1


# ---------------------------------------------------------------------------
# Row 11 — lost root adopt or REMOTE_DELIVERY_AMBIGUOUS
# ---------------------------------------------------------------------------


def test_row11_lost_root_unique_adopt(tmp_path):
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = make_job(tmp_path, idempotency_key="idem-row11-adopt")
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    bound = ledger.get_binding(job.job_id)
    port = StatefulSlackPort(
        FakePostResult(kind="timeout"),
        lookups=[FakePosted("10.9", bound.outbound_client_msg_id)],
    )
    first = deliver_slack_root(ledger, port, job_id=job.job_id)
    assert first.status is SlackRootStatus.ADOPTED
    assert first.delivered_message_ts == "10.9"
    second = deliver_slack_root(ledger, port, job_id=job.job_id)
    assert second.status is SlackRootStatus.ADOPTED
    assert len(port.posts) == 1


def test_row11_ambiguous_root_is_remote_delivery_ambiguous(tmp_path):
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        SlackUnknownReason,
        deliver_slack_root,
    )

    store, job = make_job(tmp_path, idempotency_key="idem-row11-ambig")
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    bound = ledger.get_binding(job.job_id)
    cmid = bound.outbound_client_msg_id
    port = StatefulSlackPort(
        FakePostResult(kind="http_5xx"),
        lookups=[FakePosted("10.1", cmid), FakePosted("10.2", cmid)],
    )
    first = deliver_slack_root(ledger, port, job_id=job.job_id)
    assert first.status is SlackRootStatus.UNKNOWN
    assert first.unknown_reason == SlackUnknownReason.REMOTE_DELIVERY_AMBIGUOUS.value
    retry = deliver_slack_root(ledger, port, job_id=job.job_id)
    assert retry.status is SlackRootStatus.UNKNOWN
    assert len(port.posts) == 1


# ---------------------------------------------------------------------------
# Row 12 — no bind from accepted remote; no cross bind
# ---------------------------------------------------------------------------


def test_row12_post_without_bind_rejected(tmp_path):
    from agent.durable_jobs.slack_contract import (
        BindingRequiredError,
        SlackBindingLedger,
        deliver_slack_root,
    )

    store, job = make_job(
        tmp_path, authorize=False, idempotency_key="idem-row12-nobind"
    )
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    port = StatefulSlackPort(FakePostResult(kind="accepted", message_ts="10.1"))
    with pytest.raises(BindingRequiredError):
        deliver_slack_root(ledger, port, job_id=job.job_id)
    assert port.posts == []
    assert ledger.get_binding(job.job_id) is None


def test_row12_accepted_root_cannot_cross_bind(tmp_path):
    from agent.durable_jobs.slack_contract import BindingConflict, SlackBindingLedger

    store, job_a = make_job(tmp_path, idempotency_key="idem-row12-a")
    job_b = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="other",
        repository_identity="github.com/example/repo",
        idempotency_key="idem-row12-b",
    )
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    with pytest.raises(BindingConflict):
        ledger.bind(
            job_id=job_b.job_id,
            workspace_id="T1",
            channel_id="C123",
            root_thread_ts="111.222",
            candidate_id="cand-1",
            candidate_version="v1",
        )
    assert ledger.get_binding(job_b.job_id) is None
    assert ledger.get_by_root("T1", "C123", "111.222").job_id == job_a.job_id


# ---------------------------------------------------------------------------
# Row 13 — inbound actions: duplicate/stale/cross + ACK after decision
# ---------------------------------------------------------------------------


def test_row13_cross_job_action_rejected_without_ack(tmp_path):
    from agent.durable_jobs.coordinator import consume_inbound_action

    store, job_a = make_job(tmp_path, idempotency_key="idem-row13-a")
    job_b = store.create_job(
        origin_platform="slack",
        origin_chat_id="C999",
        origin_root_thread_id="999.000",
        objective="other",
        repository_identity="github.com/example/repo",
        idempotency_key="idem-row13-b",
    )
    ack = RecordingAckPort()
    result = consume_inbound_action(
        store.sqlite_path,
        ack,
        job_id=job_b.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        actor_id="U-alice",
        decision_type="go",
        decision_idempotency_key="dec-cross",
        policy_version="pol-1",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    assert result.ok is False
    assert result.ack_status == "rejected"
    assert ack.acks == []


def test_row13_decision_commits_before_ack(tmp_path):
    from agent.durable_jobs.coordinator import consume_inbound_action
    from agent.durable_jobs.decisions import DecisionLedger

    store, job = make_job(tmp_path, authorize=False, idempotency_key="idem-row13-ack")
    _bind_policy(store, job)
    ack = RecordingAckPort()
    order = []

    class OrderedAck(RecordingAckPort):
        def ack(self, *, inbound_id: str, job_id: str) -> str:
            decisions = DecisionLedger(sqlite_path=store.sqlite_path)
            order.append("ack")
            assert decisions.count_decisions(job_id) >= 1
            return super().ack(inbound_id=inbound_id, job_id=job_id)

    port = OrderedAck()
    result = consume_inbound_action(
        store.sqlite_path,
        port,
        job_id=job.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        actor_id="U-alice",
        decision_type="hold",
        decision_idempotency_key="dec-hold-row13",
        policy_version="pol-1",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    assert result.ok is True
    assert result.ack_status == "acked"
    assert order == ["ack"]
    assert len(port.acks) == 1
    assert DecisionLedger(sqlite_path=store.sqlite_path).count_decisions(job.job_id) == 1


def test_row13_crash_after_decision_before_ack_reacks(tmp_path):
    from agent.durable_jobs.coordinator import consume_inbound_action

    store, job = make_job(tmp_path, authorize=False, idempotency_key="idem-row13-reack")
    _bind_policy(store, job)
    lost = RecordingAckPort(fail_once=True)
    first = consume_inbound_action(
        store.sqlite_path,
        lost,
        job_id=job.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        actor_id="U-alice",
        decision_type="hold",
        decision_idempotency_key="dec-reack",
        policy_version="pol-1",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    assert first.ok is True
    assert first.ack_status == "pending"
    assert lost.acks == []
    retry_port = RecordingAckPort()
    second = consume_inbound_action(
        store.sqlite_path,
        retry_port,
        job_id=job.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        actor_id="U-alice",
        decision_type="hold",
        decision_idempotency_key="dec-reack",
        policy_version="pol-1",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    assert second.ok is True
    assert second.ack_status == "acked"
    assert len(retry_port.acks) == 1


def test_row13_idempotency_key_reuse_mismatch_rejects_without_ack_or_decision(
    tmp_path,
):
    from agent.durable_jobs.coordinator import consume_inbound_action
    from agent.durable_jobs.decisions import DecisionLedger

    store, job = make_job(tmp_path, authorize=False, idempotency_key="idem-row13-reuse")
    _bind_policy(store, job)
    ack = RecordingAckPort()
    first = consume_inbound_action(
        store.sqlite_path,
        ack,
        job_id=job.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        actor_id="U-alice",
        decision_type="hold",
        decision_idempotency_key="dec-bound-tuple",
        policy_version="pol-1",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    assert first.ok is True
    assert first.ack_status == "acked"
    decisions = DecisionLedger(sqlite_path=store.sqlite_path)
    before = decisions.count_decisions(job.job_id)
    inbound_before = _count_inbound(store.sqlite_path)
    mismatch_ack = RecordingAckPort()
    reuse = consume_inbound_action(
        store.sqlite_path,
        mismatch_ack,
        job_id=job.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        actor_id="U-mallory",
        decision_type="go",
        decision_idempotency_key="dec-bound-tuple",
        policy_version="pol-other",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    assert reuse.ok is False
    assert reuse.ack_status == "rejected"
    assert mismatch_ack.acks == []
    assert decisions.count_decisions(job.job_id) == before
    assert _count_inbound(store.sqlite_path) == inbound_before
    latest = decisions.latest_accepted(job.job_id)
    assert latest is not None
    assert latest.decision_type.value == "hold"
    assert latest.actor_id == "U-alice"


def test_row13_two_processes_single_inbound_winner(tmp_path):
    from agent.durable_jobs.decisions import DecisionLedger

    store, job = make_job(
        tmp_path, authorize=False, idempotency_key="idem-row13-proc"
    )
    _bind_policy(store, job)
    key = "dec-inbound-race"
    ack_log = tmp_path / "inbound-acks.log"
    ack_log.write_text("", encoding="utf-8")
    barrier = tmp_path / "inbound-barrier"
    barrier.write_text("", encoding="utf-8")
    integrity_log = tmp_path / "inbound-integrity.log"
    integrity_log.write_text("", encoding="utf-8")
    outs = _two_python(
        _INBOUND_RACE,
        store.sqlite_path,
        job.job_id,
        key,
        str(ack_log),
        str(barrier),
        str(integrity_log),
    )
    assert all(code == 0 for code, _o, _e in outs), outs
    payloads = [json.loads(out) for _code, out, _err in outs]
    inbound_ids = {row["inbound_id"] for row in payloads}
    decision_ids = {row["decision_id"] for row in payloads}
    assert len(inbound_ids) == 1
    assert len(decision_ids) == 1
    assert all(row["ok"] is True for row in payloads)
    assert all(row["ack_status"] in ("acked", "pending") for row in payloads)
    conn = sqlite3.connect(store.sqlite_path)
    try:
        (inbounds,) = conn.execute(
            "SELECT COUNT(*) FROM job_inbound_actions WHERE decision_idempotency_key=?",
            (key,),
        ).fetchone()
        (decisions,) = conn.execute(
            "SELECT COUNT(*) FROM job_decisions WHERE decision_idempotency_key=?",
            (key,),
        ).fetchone()
        ack_status = conn.execute(
            "SELECT ack_status FROM job_inbound_actions WHERE decision_idempotency_key=?",
            (key,),
        ).fetchone()[0]
    finally:
        conn.close()
    assert int(inbounds) == 1
    assert int(decisions) == 1
    assert ack_status == "acked"
    ack_lines = [line for line in ack_log.read_text(encoding="utf-8").splitlines() if line]
    assert 1 <= len(ack_lines) <= 2
    integrity_hits = [
        line
        for line in integrity_log.read_text(encoding="utf-8").splitlines()
        if line == "inbound_unique"
    ]
    assert len(integrity_hits) >= 1
    persisted_job = f"{next(iter(inbound_ids))}:{job.job_id}"
    assert all(line == persisted_job for line in ack_lines)
    latest = DecisionLedger(sqlite_path=store.sqlite_path).latest_accepted(job.job_id)
    assert latest is not None
    assert latest.decision_id == next(iter(decision_ids))


def _count_inbound(path) -> int:
    conn = sqlite3.connect(path)
    try:
        (n,) = conn.execute("SELECT COUNT(*) FROM job_inbound_actions").fetchone()
        return int(n)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Row 14 — terminal evidence
# ---------------------------------------------------------------------------


def test_row14_terminal_evidence_requires_matching_durable_status(tmp_path):
    from agent.durable_jobs.coordinator import (
        TerminalEvidenceRequired,
        commit_terminal_evidence,
    )
    from agent.durable_jobs.effects import (
        ProviderEffectLedger,
        reconcile_cursor_create,
    )

    store, job = make_job(tmp_path, idempotency_key="idem-row14-req")
    with pytest.raises(TerminalEvidenceRequired):
        commit_terminal_evidence(
            store.sqlite_path,
            job_id=job.job_id,
            kind="provider_run",
            correlation_id="run-missing",
        )
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    key = f"cursor:{job.job_id}:create_run"
    provider = StatefulCursorProvider(
        FakeCreateResult(kind="accepted", run=FakeRun("run-ok", key))
    )
    result = reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    evidence = commit_terminal_evidence(
        store.sqlite_path,
        job_id=job.job_id,
        kind="provider_run",
        correlation_id=result.provider_run_id,
    )
    assert evidence.correlation_id == "run-ok"
    assert evidence.job_id == job.job_id


def test_row14_crash_before_evidence_commit_persists_nothing(tmp_path, monkeypatch):
    from agent.durable_jobs import coordinator as coord_mod
    from agent.durable_jobs.coordinator import commit_terminal_evidence
    from agent.durable_jobs.effects import (
        ProviderEffectLedger,
        reconcile_cursor_create,
    )

    store, job = make_job(tmp_path, idempotency_key="idem-row14-crash")
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    key = f"cursor:{job.job_id}:create_run"
    provider = StatefulCursorProvider(
        FakeCreateResult(kind="accepted", run=FakeRun("run-ok", key))
    )
    result = reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))

    def boom() -> None:
        raise RuntimeError("injected crash before evidence commit")

    monkeypatch.setattr(
        coord_mod, "after_evidence_rows_before_commit", boom, raising=False
    )
    with pytest.raises(RuntimeError, match="injected crash before evidence commit"):
        commit_terminal_evidence(
            store.sqlite_path,
            job_id=job.job_id,
            kind="provider_run",
            correlation_id=result.provider_run_id,
        )
    conn = sqlite3.connect(store.sqlite_path)
    try:
        (n,) = conn.execute("SELECT COUNT(*) FROM job_terminal_evidence").fetchone()
    finally:
        conn.close()
    assert n == 0


# ---------------------------------------------------------------------------
# Row 15 — resume enqueue
# ---------------------------------------------------------------------------


def test_row15_enqueue_requires_terminal_evidence(tmp_path):
    from agent.durable_jobs.coordinator import (
        TerminalEvidenceRequired,
        enqueue_resume,
    )

    store, job = make_job(tmp_path, idempotency_key="idem-row15-req")
    with pytest.raises(TerminalEvidenceRequired):
        enqueue_resume(store.sqlite_path, job_id=job.job_id)


def test_row15_cannot_local_mark_before_accepted_enqueue(tmp_path):
    from agent.durable_jobs.coordinator import (
        ResumeEnqueueError,
        commit_terminal_evidence,
        enqueue_resume,
        mark_resume_local,
    )
    from agent.durable_jobs.effects import (
        ProviderEffectLedger,
        reconcile_cursor_create,
    )

    store, job = make_job(tmp_path, idempotency_key="idem-row15-mark")
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    key = f"cursor:{job.job_id}:create_run"
    provider = StatefulCursorProvider(
        FakeCreateResult(kind="accepted", run=FakeRun("run-ok", key))
    )
    result = reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    evidence = commit_terminal_evidence(
        store.sqlite_path,
        job_id=job.job_id,
        kind="provider_run",
        correlation_id=result.provider_run_id,
    )
    with pytest.raises(ResumeEnqueueError):
        mark_resume_local(store.sqlite_path, job_id=job.job_id)
    queued = enqueue_resume(store.sqlite_path, job_id=job.job_id)
    assert queued.status == "accepted"
    assert queued.local_marked is True
    assert queued.idempotency_key == f"resume:{job.job_id}:{evidence.evidence_id}"


def test_row15_stable_resume_idempotency_key(tmp_path):
    from agent.durable_jobs.coordinator import (
        commit_terminal_evidence,
        enqueue_resume,
        resume_idempotency_key,
    )
    from agent.durable_jobs.effects import (
        ProviderEffectLedger,
        reconcile_cursor_create,
    )

    store, job = make_job(tmp_path, idempotency_key="idem-row15-key")
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    key = f"cursor:{job.job_id}:create_run"
    provider = StatefulCursorProvider(
        FakeCreateResult(kind="accepted", run=FakeRun("run-ok", key))
    )
    result = reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    evidence = commit_terminal_evidence(
        store.sqlite_path,
        job_id=job.job_id,
        kind="provider_run",
        correlation_id=result.provider_run_id,
    )
    first = enqueue_resume(store.sqlite_path, job_id=job.job_id)
    second = enqueue_resume(store.sqlite_path, job_id=job.job_id)
    expected = resume_idempotency_key(job.job_id, evidence.evidence_id)
    assert first.idempotency_key == expected
    assert second.idempotency_key == expected
    assert first.enqueue_id == second.enqueue_id


# ---------------------------------------------------------------------------
# Row 16 — Slack delivery lease: claim before post, ACK after accept
# ---------------------------------------------------------------------------


def test_row16_slack_delivery_claim_before_post_and_ack_after_accept(tmp_path):
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = make_job(tmp_path, idempotency_key="idem-row16-order")
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    order = []

    class Probe(StatefulSlackPort):
        def post_root(self, **kwargs):
            order.append("post")
            current = ledger.get_binding(kwargs["job_id"])
            assert current.status is SlackRootStatus.CLAIMED
            assert current.delivered_message_ts is None
            return super().post_root(**kwargs)

    port = Probe(FakePostResult(kind="accepted", message_ts="16.1"))
    result = deliver_slack_root(ledger, port, job_id=job.job_id)
    assert order == ["post"]
    assert result.status is SlackRootStatus.DELIVERED
    assert result.delivered_message_ts == "16.1"


def test_row16_lost_post_adopt_at_least_once(tmp_path):
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = make_job(tmp_path, idempotency_key="idem-row16-lost")
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    bound = ledger.get_binding(job.job_id)
    port = StatefulSlackPort(
        FakePostResult(kind="lost_response"),
        lookups=[FakePosted("16.2", bound.outbound_client_msg_id)],
    )
    first = deliver_slack_root(ledger, port, job_id=job.job_id)
    assert first.status is SlackRootStatus.ADOPTED
    retry = deliver_slack_root(ledger, port, job_id=job.job_id)
    assert retry.status is SlackRootStatus.ADOPTED
    assert len(port.posts) == 1


# ---------------------------------------------------------------------------
# Row 17 — exact expiry and clock rewind
# ---------------------------------------------------------------------------


def test_row17_exact_expiry_equality_is_expired(tmp_path):
    from agent.durable_jobs.clock import FrozenClock, claim_is_expired
    from agent.durable_jobs.effects import ProviderEffectLedger

    store, job = make_job(tmp_path, idempotency_key="idem-row17-eq")
    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path, now_fn=clock, lease_seconds=30
    )
    claimed = ledger.claim_effect(**_provider_kwargs(job))
    assert claimed.won is True
    clock.advance(30)
    now = clock()
    assert claim_is_expired(claimed.claim.claim_expires_at, now) is True
    taken = ledger.takeover_stale_claim(job.job_id, "create_run")
    assert taken.won is True


def test_row17_clock_rewind_cannot_unexpire_lease(tmp_path):
    from agent.durable_jobs.clock import FrozenClock, claim_is_expired
    from agent.durable_jobs.effects import ProviderEffectLedger

    store, job = make_job(tmp_path, idempotency_key="idem-row17-rewind")
    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path, now_fn=clock, lease_seconds=30
    )
    claimed = ledger.claim_effect(**_provider_kwargs(job))
    assert claimed.won is True
    clock.advance(31)
    # Record the expired instant on the durable watermark without taking over.
    expired_now = ledger._now()
    assert claim_is_expired(claimed.claim.claim_expires_at, expired_now) is True
    clock.advance(-120)
    assert claim_is_expired(claimed.claim.claim_expires_at, clock()) is False
    replay = ledger.takeover_stale_claim(job.job_id, "create_run")
    assert replay.won is True


# ---------------------------------------------------------------------------
# Row 18 — restart only persisted next_action
# ---------------------------------------------------------------------------


def test_row18_restart_executes_only_persisted_next_action(tmp_path):
    from agent.durable_jobs.models import JobPhase
    from agent.durable_jobs.restart import execute_persisted_next_action
    from agent.durable_jobs.store import DurableJobStore

    store, job = make_job(tmp_path, authorize=False, idempotency_key="idem-row18")
    assert job.phase is JobPhase.INTAKE
    assert job.next_action == "freeze_baseline"
    mid = execute_persisted_next_action(
        store, job.job_id, frozen_baseline_sha="sha-restart"
    )
    assert mid.phase is JobPhase.FREEZE_BASELINE
    assert mid.next_action == "await_dispatch"
    end = execute_persisted_next_action(store, job.job_id)
    assert end.phase is JobPhase.AWAIT_DISPATCH
    assert end.next_action == "package1_hard_disabled_dispatch"
    from agent.durable_jobs.service import DispatchDisabledError

    with pytest.raises(DispatchDisabledError):
        execute_persisted_next_action(store, job.job_id)
    reopened = DurableJobStore(sqlite_path=store.sqlite_path).get_job(job.job_id)
    assert reopened.phase is JobPhase.AWAIT_DISPATCH


def test_row18_tampered_next_action_is_denied(tmp_path):
    from agent.durable_jobs.models import JobPhase
    from agent.durable_jobs.restart import RestartDenied, execute_persisted_next_action

    store, job = make_job(tmp_path, authorize=False, idempotency_key="idem-row18-bad")
    conn = sqlite3.connect(store.sqlite_path)
    try:
        conn.execute(
            "UPDATE durable_jobs SET next_action=? WHERE job_id=?",
            ("launch_missiles", job.job_id),
        )
        conn.commit()
    finally:
        conn.close()
    with pytest.raises(RestartDenied):
        execute_persisted_next_action(store, job.job_id)
    loaded = store.get_job(job.job_id)
    assert loaded.phase is JobPhase.INTAKE
    assert loaded.next_action == "launch_missiles"


# ---------------------------------------------------------------------------
# Row 19 — OS-process single winner
# ---------------------------------------------------------------------------


_CLAIM_RACE = textwrap.dedent(
    """
    import sys
    from pathlib import Path
    from agent.durable_jobs.effects import ProviderEffectLedger
    ledger = ProviderEffectLedger(sqlite_path=Path(sys.argv[1]))
    result = ledger.claim_effect(
        job_id=sys.argv[2],
        action_id="create_run",
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    sys.stdout.write("WIN" if result.won else "LOSE")
    """
)

_DELIVER_RACE = textwrap.dedent(
    """
    import sys
    from pathlib import Path
    from agent.durable_jobs.slack_contract import SlackBindingLedger
    ledger = SlackBindingLedger(sqlite_path=Path(sys.argv[1]))
    result = ledger.claim_delivery(sys.argv[2])
    sys.stdout.write("WIN" if result.won else "LOSE")
    """
)

_ENQUEUE_RACE = textwrap.dedent(
    """
    import sys
    from pathlib import Path
    from agent.durable_jobs.coordinator import enqueue_resume
    row = enqueue_resume(Path(sys.argv[1]), job_id=sys.argv[2])
    sys.stdout.write(row.enqueue_id)
    """
)

_INBOUND_RACE = textwrap.dedent(
    """
    import json
    import os
    import sqlite3
    import sys
    import time
    from pathlib import Path

    flag = Path(sys.argv[5])
    integrity_log = Path(sys.argv[6])
    _orig_connect = sqlite3.connect

    class TracingConnection(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            try:
                return super().execute(sql, parameters)
            except sqlite3.IntegrityError:
                if "job_inbound_actions" in str(sql):
                    with integrity_log.open("a", encoding="utf-8") as handle:
                        handle.write("inbound_unique\\n")
                        handle.flush()
                        os.fsync(handle.fileno())
                raise

    def _connect(*args, **kwargs):
        kwargs.setdefault("factory", TracingConnection)
        return _orig_connect(*args, **kwargs)

    sqlite3.connect = _connect

    import agent.durable_jobs.coordinator as coordinator
    from agent.durable_jobs.coordinator import consume_inbound_action

    def _barrier():
        with flag.open("a", encoding="utf-8") as handle:
            handle.write("1")
            handle.flush()
            os.fsync(handle.fileno())
        for _ in range(500):
            if flag.read_text(encoding="utf-8").count("1") >= 2:
                time.sleep(0.05)
                return
            time.sleep(0.01)
        raise TimeoutError("inbound insert barrier")

    coordinator.after_inbound_select_before_insert = _barrier

    class FileAck:
        def ack(self, *, inbound_id, job_id):
            with open(sys.argv[4], "a", encoding="utf-8") as handle:
                handle.write(f"{inbound_id}:{job_id}\\n")
            return f"ack:{inbound_id}"

    result = consume_inbound_action(
        Path(sys.argv[1]),
        FileAck(),
        job_id=sys.argv[2],
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        actor_id="U-alice",
        decision_type="hold",
        decision_idempotency_key=sys.argv[3],
        policy_version="pol-1",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    sys.stdout.write(
        json.dumps(
            {
                "ok": result.ok,
                "ack_status": result.ack_status,
                "inbound_id": result.inbound_id,
                "decision_id": result.decision_id,
            }
        )
    )
    """
)


def _two_python(script: str, db, job_id: str, *extra):
    env = child_env()
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", script, str(db), job_id, *extra],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    outs = []
    for proc in procs:
        out, err = proc.communicate(timeout=20)
        outs.append((proc.returncode, out, err))
    return outs


def test_row19_two_processes_single_provider_claim_winner(tmp_path):
    store, job = make_job(tmp_path, idempotency_key="idem-row19-claim")
    outs = _two_python(_CLAIM_RACE, store.sqlite_path, job.job_id)
    wins = [out for code, out, _err in outs if out == "WIN"]
    loses = [out for code, out, _err in outs if out == "LOSE"]
    assert all(code == 0 for code, _o, _e in outs), outs
    assert len(wins) == 1
    assert len(loses) == 1


def test_row19_two_processes_single_slack_delivery_winner(tmp_path):
    store, job = make_job(tmp_path, idempotency_key="idem-row19-slack")
    outs = _two_python(_DELIVER_RACE, store.sqlite_path, job.job_id)
    wins = [out for code, out, _err in outs if out == "WIN"]
    loses = [out for code, out, _err in outs if out == "LOSE"]
    assert all(code == 0 for code, _o, _e in outs), outs
    assert len(wins) == 1
    assert len(loses) == 1


def test_row19_two_processes_single_resume_enqueue_winner(tmp_path):
    from agent.durable_jobs.coordinator import commit_terminal_evidence
    from agent.durable_jobs.effects import (
        ProviderEffectLedger,
        reconcile_cursor_create,
    )

    store, job = make_job(tmp_path, idempotency_key="idem-row19-resume")
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    key = f"cursor:{job.job_id}:create_run"
    provider = StatefulCursorProvider(
        FakeCreateResult(kind="accepted", run=FakeRun("run-ok", key))
    )
    result = reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    commit_terminal_evidence(
        store.sqlite_path,
        job_id=job.job_id,
        kind="provider_run",
        correlation_id=result.provider_run_id,
    )
    outs = _two_python(_ENQUEUE_RACE, store.sqlite_path, job.job_id)
    assert all(code == 0 for code, _o, _e in outs), outs
    ids = {out for _code, out, _err in outs}
    assert len(ids) == 1


# ---------------------------------------------------------------------------
# Row 20 — datastore locked/full/txn failure
# ---------------------------------------------------------------------------


def test_row20_locked_writer_no_partial_job(tmp_path):
    from agent.durable_jobs.store import DurableJobStore

    store = DurableJobStore(sqlite_path=db_path(tmp_path))
    holder = sqlite3.connect(store.sqlite_path, timeout=1)
    holder.execute("BEGIN IMMEDIATE")
    try:
        busy = sqlite3.connect(store.sqlite_path, timeout=0.2)
        busy.execute("PRAGMA busy_timeout = 200")
        raised = None
        try:
            store.create_job(
                origin_platform="cli",
                origin_chat_id="local",
                origin_root_thread_id="r",
                objective="locked",
                repository_identity="repo",
                idempotency_key="idem-row20-lock",
            )
        except sqlite3.OperationalError as exc:
            raised = exc
        finally:
            busy.close()
        assert raised is not None
        assert "locked" in str(raised).lower() or "busy" in str(raised).lower()
    finally:
        holder.rollback()
        holder.close()
    assert store.get_job_by_idempotency_key("idem-row20-lock") is None


def test_row20_injected_full_no_partial_mutation(tmp_path, monkeypatch):
    from agent.durable_jobs import store as store_mod
    from agent.durable_jobs.store import DurableJobStore

    def boom() -> None:
        raise sqlite3.OperationalError("database or disk is full")

    monkeypatch.setattr(
        store_mod, "after_job_rows_before_commit", boom, raising=False
    )
    store = DurableJobStore(sqlite_path=db_path(tmp_path))
    with pytest.raises(sqlite3.OperationalError, match="disk is full"):
        store.create_job(
            origin_platform="cli",
            origin_chat_id="local",
            origin_root_thread_id="r",
            objective="full",
            repository_identity="repo",
            idempotency_key="idem-row20-full",
        )
    assert store.count_jobs() == 0


def test_row20_crash_mid_transaction_rolls_back(tmp_path, monkeypatch):
    from agent.durable_jobs import store as store_mod
    from agent.durable_jobs.store import DurableJobStore

    def boom() -> None:
        raise RuntimeError("txn crash")

    monkeypatch.setattr(
        store_mod, "after_job_rows_before_commit", boom, raising=False
    )
    store = DurableJobStore(sqlite_path=db_path(tmp_path))
    with pytest.raises(RuntimeError, match="txn crash"):
        store.create_job(
            origin_platform="cli",
            origin_chat_id="local",
            origin_root_thread_id="r",
            objective="crash",
            repository_identity="repo",
            idempotency_key="idem-row20-txn",
        )
    conn = sqlite3.connect(store.sqlite_path)
    try:
        (n,) = conn.execute("SELECT COUNT(*) FROM durable_jobs").fetchone()
        (e,) = conn.execute("SELECT COUNT(*) FROM durable_job_events").fetchone()
    finally:
        conn.close()
    assert n == 0
    assert e == 0


# ---------------------------------------------------------------------------
# Row 21 — unknown schema / pruned key nonreuse
# ---------------------------------------------------------------------------


def _sqlite_dump(path) -> str:
    conn = sqlite3.connect(path)
    try:
        return "\n".join(conn.iterdump())
    finally:
        conn.close()


def test_row21_unknown_schema_version_refuses_writes(tmp_path):
    from agent.durable_jobs.store import DurableJobStore, UnknownSchemaError

    store = DurableJobStore(sqlite_path=db_path(tmp_path))
    conn = sqlite3.connect(store.sqlite_path)
    try:
        conn.execute(
            "UPDATE durable_jobs_meta SET value=? WHERE key='schema_version'",
            ("not-a-schema",),
        )
        conn.commit()
    finally:
        conn.close()
    before = _sqlite_dump(store.sqlite_path)
    with pytest.raises(UnknownSchemaError):
        DurableJobStore(sqlite_path=store.sqlite_path)
    assert _sqlite_dump(store.sqlite_path) == before


def test_row21_future_schema_version_refuses_writes_without_mutation(tmp_path):
    from agent.durable_jobs.store import SCHEMA_VERSION, DurableJobStore, UnknownSchemaError

    store = DurableJobStore(sqlite_path=db_path(tmp_path))
    conn = sqlite3.connect(store.sqlite_path)
    try:
        conn.execute(
            "UPDATE durable_jobs_meta SET value=? WHERE key='schema_version'",
            (str(SCHEMA_VERSION + 100),),
        )
        conn.commit()
    finally:
        conn.close()
    before = _sqlite_dump(store.sqlite_path)
    with pytest.raises(UnknownSchemaError):
        DurableJobStore(sqlite_path=store.sqlite_path)
    assert _sqlite_dump(store.sqlite_path) == before


def test_row21_missing_schema_marker_on_existing_db_fails_closed(tmp_path):
    from agent.durable_jobs.store import DurableJobStore, UnknownSchemaError

    store, job = make_job(
        tmp_path, authorize=False, idempotency_key="idem-row21-marker"
    )
    job_id = job.job_id
    conn = sqlite3.connect(store.sqlite_path)
    try:
        conn.execute(
            "DELETE FROM durable_jobs_meta WHERE key='schema_version'"
        )
        conn.commit()
        remaining = conn.execute(
            "SELECT 1 FROM durable_jobs_meta WHERE key='schema_version'"
        ).fetchone()
    finally:
        conn.close()
    assert remaining is None
    before = _sqlite_dump(store.sqlite_path)
    with pytest.raises(UnknownSchemaError):
        DurableJobStore(sqlite_path=store.sqlite_path)
    after = _sqlite_dump(store.sqlite_path)
    assert after == before
    conn = sqlite3.connect(store.sqlite_path)
    try:
        marker = conn.execute(
            "SELECT value FROM durable_jobs_meta WHERE key='schema_version'"
        ).fetchone()
        (n,) = conn.execute(
            "SELECT COUNT(*) FROM durable_jobs WHERE job_id=?",
            (job_id,),
        ).fetchone()
    finally:
        conn.close()
    assert marker is None
    assert int(n) == 1


def test_row21_empty_db_may_initialize(tmp_path):
    from agent.durable_jobs.store import SCHEMA_VERSION, DurableJobStore

    path = db_path(tmp_path)
    assert not path.exists()
    store = DurableJobStore(sqlite_path=path)
    conn = sqlite3.connect(store.sqlite_path)
    try:
        row = conn.execute(
            "SELECT value FROM durable_jobs_meta WHERE key='schema_version'"
        ).fetchone()
        tables = {
            name
            for (name,) in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    finally:
        conn.close()
    assert row is not None
    assert int(row[0]) == SCHEMA_VERSION
    assert "durable_jobs" in tables


def test_row21_pruned_authorization_key_not_reused(tmp_path):
    from agent.durable_jobs.eng29 import (
        MATRIX_VERSION,
        PROVIDER_CREATE_TARGET_ACTION,
        prune_authorization_tuple,
        register_authorization_tuple,
    )

    store, job = make_job(tmp_path, authorize=False, idempotency_key="idem-row21")
    _bind_policy(store, job)
    key = f"tuple:{job.job_id}:cursor.create_run"
    first = register_authorization_tuple(
        store.sqlite_path,
        job_id=job.job_id,
        source_package_id=job.repository_identity,
        source_package_version="v1",
        candidate_sha=job.frozen_baseline_sha,
        candidate_id="cand-1",
        candidate_version="v1",
        target_environment="slack",
        target_action=PROVIDER_CREATE_TARGET_ACTION,
        authorized_actor="U-alice",
        expires_at="2099-01-01T00:00:00+00:00",
        policy_version="pol-1",
        matrix_version=MATRIX_VERSION,
        authorization_idempotency_key=key,
        prerequisites_satisfied=True,
        provider_ambiguity_resolved=True,
    )
    assert first.ok is True
    pruned = prune_authorization_tuple(
        store.sqlite_path, job.job_id, PROVIDER_CREATE_TARGET_ACTION
    )
    assert pruned is True
    reuse = register_authorization_tuple(
        store.sqlite_path,
        job_id=job.job_id,
        source_package_id=job.repository_identity,
        source_package_version="v1",
        candidate_sha=job.frozen_baseline_sha,
        candidate_id="cand-1",
        candidate_version="v1",
        target_environment="slack",
        target_action=PROVIDER_CREATE_TARGET_ACTION,
        authorized_actor="U-alice",
        expires_at="2099-01-01T00:00:00+00:00",
        policy_version="pol-1",
        matrix_version=MATRIX_VERSION,
        authorization_idempotency_key=key,
        prerequisites_satisfied=True,
        provider_ambiguity_resolved=True,
    )
    assert reuse.ok is False
    assert "replayed" in reuse.reason_codes or "unauthorized" in reuse.reason_codes


# ---------------------------------------------------------------------------
# Row 22 — redaction + correlation
# ---------------------------------------------------------------------------


def test_row22_events_redact_tokens_and_prompts(tmp_path):
    store, job = make_job(tmp_path, authorize=False, idempotency_key="idem-row22")
    store.append_intent(
        job.job_id,
        event_type="debug_dump",
        payload={
            "owner_token": "super-secret-token",
            "prompt": "user private prompt text",
            "authorization": "Bearer abc.def",
            "job_id": job.job_id,
        },
        idempotency_key="debug-dump-1",
    )
    events = store.list_events(job.job_id)
    dump = [ev for ev in events if ev["event_type"] == "debug_dump"]
    assert dump
    payload = json.loads(dump[0]["payload_json"])
    assert payload["owner_token"] == "[REDACTED]"
    assert payload["prompt"] == "[REDACTED]"
    assert payload["authorization"] == "[REDACTED]"
    assert payload["job_id"] == job.job_id


def test_row22_events_preserve_job_and_idempotency_correlation(tmp_path):
    store, job = make_job(tmp_path, authorize=False, idempotency_key="idem-row22b")
    events = store.list_events(job.job_id)
    created = [ev for ev in events if ev["event_type"] == "job_created"]
    assert created
    assert created[0]["job_id"] == job.job_id
    assert created[0]["idempotency_key"] == f"create:{job.idempotency_key}"


def test_fault_injection_seams_cannot_authorize_or_bypass(tmp_path):
    from agent.durable_jobs.coordinator import after_evidence_rows_before_commit
    from agent.durable_jobs.decisions import after_decision_rows_before_commit
    from agent.durable_jobs.effects import ProviderEffectLedger
    from agent.durable_jobs.eng29 import (
        AuthorizationDenied,
        after_in_transaction_adapter_go,
        before_begin_immediate,
    )
    from agent.durable_jobs.store import (
        DurableJobStore,
        after_job_rows_before_commit,
    )

    store = DurableJobStore(sqlite_path=db_path(tmp_path))
    assert after_job_rows_before_commit() is None
    assert after_decision_rows_before_commit() is None
    assert after_evidence_rows_before_commit() is None
    assert after_in_transaction_adapter_go() is None
    assert before_begin_immediate() is None
    from agent.durable_jobs.coordinator import after_inbound_select_before_insert

    assert after_inbound_select_before_insert() is None
    assert store.count_jobs() == 0
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="seam cannot grant",
        repository_identity="github.com/example/repo",
        idempotency_key="idem-seam",
    )
    _bind_policy(store, job)
    after_job_rows_before_commit()
    after_decision_rows_before_commit()
    after_in_transaction_adapter_go()
    before_begin_immediate()
    effects = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    with pytest.raises(AuthorizationDenied):
        effects.claim_effect(**_provider_kwargs(job))
    assert effects.get_claim(job.job_id, "create_run") is None
    assert effects.count_claims() == 0
