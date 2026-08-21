"""ENG-36: persisted Slack workspace binding is required except bind_slack bootstrap.

Non-bootstrap writers must see a readable slack_job_bindings.workspace_id equal
to config.identity_binding.workspace_id before lease, write, adapter, or ACK.
bind_slack may create the first row only when the caller workspace matches
configured authority. Binding read/schema failure is fail-closed.

No live Slack/Cursor/network. PostgreSQL is not imported.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from tests.agent.durable_jobs.eng28_support import count_table
from tests.agent.durable_jobs.test_lane_identity_authority import (
    CONFIG_REPO,
    CONFIG_WORKSPACE,
    FOREIGN_WORKSPACE,
    TRACKED_TABLES,
    _complete,
    _invoke,
    _snapshot,
)


NON_BOOTSTRAP_WRITERS = (
    "consume_inbound_action",
    "deliver_slack_root",
    "reconcile_cursor_create",
    "set_job_policy",
    "record_decision",
)

ALL_WRITERS = NON_BOOTSTRAP_WRITERS + ("bind_slack",)


def _table_exists(path: Path, name: str) -> bool:
    conn = sqlite3.connect(path)
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (name,),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def _seed_unbound(tmp_path: Path, *, idempotency_key: str):
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.lane import DurableLaneService
    from agent.durable_jobs.store import DurableJobStore

    cfg = load_durable_jobs_config(_complete(tmp_path))
    store = DurableJobStore(sqlite_path=cfg.sqlite_path)
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="persisted-workspace-authority",
        repository_identity=CONFIG_REPO,
        idempotency_key=idempotency_key,
    )
    DecisionLedger(sqlite_path=store.sqlite_path).set_policy(
        job_id=job.job_id,
        policy_version="pol-1",
        allowed_actors=("U-alice",),
        expires_at="2099-01-01T00:00:00+00:00",
    )
    assert count_table(store.sqlite_path, "slack_job_bindings") == 0
    lane = DurableLaneService(config=cfg, store=store)
    return lane, job, store


def _spy_lease(lane) -> list[str]:
    marks: list[str] = []
    original_after = lane._after_identity_validation
    original_before = lane._before_mutation_lease

    def _after() -> None:
        marks.append("after_identity")
        return original_after()

    def _before() -> None:
        marks.append("before_lease")
        return original_before()

    lane._after_identity_validation = _after
    lane._before_mutation_lease = _before
    return marks


def _assert_typed_reject(writer: str, value, caught) -> None:
    if writer == "consume_inbound_action":
        assert caught is None, caught
        assert value is not None
        assert value.ok is False
        assert value.ack_status == "rejected"
        assert getattr(value, "retryable", False) is False
        return
    assert value is None
    assert caught is not None
    assert type(caught).__name__ == "LaneIdentityRejected"


def _assert_zero_effect(store, before, ack, slack, provider, marks) -> None:
    assert _snapshot(store.sqlite_path) == before
    assert ack.acks == []
    assert slack.posts == []
    assert slack.lookups == []
    assert provider.create_calls == []
    assert provider.lookup_calls == []
    assert marks == []


@pytest.mark.parametrize("writer", NON_BOOTSTRAP_WRITERS)
def test_missing_persisted_workspace_rejects_non_bootstrap_writers(tmp_path, writer):
    lane, job, store = _seed_unbound(
        tmp_path, idempotency_key=f"idem-missing-ws-{writer}"
    )
    marks = _spy_lease(lane)
    before = _snapshot(store.sqlite_path)
    value, caught, ack, slack, provider = _invoke(
        lane, writer, job, CONFIG_WORKSPACE, positive=False
    )
    _assert_typed_reject(writer, value, caught)
    _assert_zero_effect(store, before, ack, slack, provider, marks)
    assert count_table(store.sqlite_path, "slack_job_bindings") == 0


def test_consume_missing_persisted_workspace_does_not_enter_coordinator(
    tmp_path, monkeypatch
):
    def _boom(*_a, **_k):
        raise AssertionError(
            "coordinator must not run without persisted workspace authority"
        )

    monkeypatch.setattr(
        "agent.durable_jobs.lane.consume_durable_inbound_action", _boom
    )
    lane, job, store = _seed_unbound(
        tmp_path, idempotency_key="idem-missing-ws-coordinator"
    )
    marks = _spy_lease(lane)
    before = _snapshot(store.sqlite_path)
    value, caught, ack, slack, provider = _invoke(
        lane, "consume_inbound_action", job, CONFIG_WORKSPACE, positive=False
    )
    _assert_typed_reject("consume_inbound_action", value, caught)
    _assert_zero_effect(store, before, ack, slack, provider, marks)


def test_bind_slack_matching_bootstrap_succeeds_once_then_idempotent(tmp_path):
    lane, job, store = _seed_unbound(
        tmp_path, idempotency_key="idem-bind-bootstrap"
    )
    before = _snapshot(store.sqlite_path)
    first, caught, ack, slack, provider = _invoke(
        lane, "bind_slack", job, CONFIG_WORKSPACE, positive=True
    )
    assert caught is None, caught
    assert first is not None
    assert first.workspace_id == CONFIG_WORKSPACE
    assert count_table(store.sqlite_path, "slack_job_bindings") == (
        before["slack_job_bindings"] + 1
    )
    assert ack.acks == []
    assert slack.posts == []
    assert provider.create_calls == []
    after_first = _snapshot(store.sqlite_path)

    second, caught2, ack2, slack2, provider2 = _invoke(
        lane, "bind_slack", job, CONFIG_WORKSPACE, positive=True
    )
    assert caught2 is None, caught2
    assert second is not None
    assert second.workspace_id == CONFIG_WORKSPACE
    assert _snapshot(store.sqlite_path) == after_first
    assert ack2.acks == []
    assert slack2.posts == []
    assert provider2.create_calls == []


def test_bind_slack_foreign_bootstrap_rejects_zero_write(tmp_path):
    lane, job, store = _seed_unbound(
        tmp_path, idempotency_key="idem-bind-foreign-bootstrap"
    )
    marks = _spy_lease(lane)
    before = _snapshot(store.sqlite_path)
    value, caught, ack, slack, provider = _invoke(
        lane, "bind_slack", job, FOREIGN_WORKSPACE, positive=False
    )
    _assert_typed_reject("bind_slack", value, caught)
    _assert_zero_effect(store, before, ack, slack, provider, marks)
    assert count_table(store.sqlite_path, "slack_job_bindings") == 0


def test_bind_slack_empty_caller_workspace_rejects_zero_write(tmp_path):
    lane, job, store = _seed_unbound(
        tmp_path, idempotency_key="idem-bind-empty-ws"
    )
    marks = _spy_lease(lane)
    before = _snapshot(store.sqlite_path)
    value, caught, ack, slack, provider = _invoke(
        lane, "bind_slack", job, "   ", positive=False
    )
    _assert_typed_reject("bind_slack", value, caught)
    _assert_zero_effect(store, before, ack, slack, provider, marks)


def _drop_binding_table(store) -> None:
    with store._connect() as conn:
        conn.execute("DROP TABLE slack_job_bindings")
    assert _table_exists(store.sqlite_path, "slack_job_bindings") is False


def _snapshot_without_bindings(path: Path) -> dict[str, int]:
    return {
        table: count_table(path, table)
        for table in TRACKED_TABLES
        if table != "slack_job_bindings"
    }


@pytest.mark.parametrize("writer", ALL_WRITERS)
def test_binding_table_read_failure_rejects_zero_effect(tmp_path, writer):
    lane, job, store = _seed_unbound(
        tmp_path, idempotency_key=f"idem-drop-bindings-{writer}"
    )
    _drop_binding_table(store)
    marks = _spy_lease(lane)
    before = _snapshot_without_bindings(store.sqlite_path)
    value, caught, ack, slack, provider = _invoke(
        lane, writer, job, CONFIG_WORKSPACE, positive=False
    )
    _assert_typed_reject(writer, value, caught)
    assert _snapshot_without_bindings(store.sqlite_path) == before
    assert _table_exists(store.sqlite_path, "slack_job_bindings") is False
    assert ack.acks == []
    assert slack.posts == []
    assert slack.lookups == []
    assert provider.create_calls == []
    assert provider.lookup_calls == []
    assert marks == []


def test_persisted_workspace_check_does_not_import_psycopg(tmp_path, monkeypatch):
    import sys
    import types

    fake = types.ModuleType("psycopg")

    def _boom(*_a, **_k):
        raise AssertionError("psycopg must stay opt-in")

    fake.connect = _boom
    monkeypatch.setitem(sys.modules, "psycopg", fake)
    lane, job, store = _seed_unbound(tmp_path, idempotency_key="idem-ws-no-psycopg")
    before = _snapshot(store.sqlite_path)
    value, caught, ack, slack, provider = _invoke(
        lane, "set_job_policy", job, CONFIG_WORKSPACE, positive=False
    )
    _assert_typed_reject("set_job_policy", value, caught)
    assert _snapshot(store.sqlite_path) == before
    assert ack.acks == []
    assert slack.posts == []
    assert provider.create_calls == []
    assert "psycopg" not in sys.modules or sys.modules["psycopg"] is fake
