"""ENG-36: DurableLaneService writers require repository/workspace authority.

Every public mutating entrypoint must verify persisted job identity against
``config.identity_binding`` before the first write, effect, or ACK.
Platform wrappers are defense-in-depth only.

No live Slack/Cursor/network. PostgreSQL is not imported.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from tests.agent.durable_jobs.eng28_support import RecordingAckPort, count_table


CONFIG_REPO = "github.com/example/repo"
CONFIG_WORKSPACE = "T1"
FOREIGN_REPO = "github.com/evil/other"
FOREIGN_WORKSPACE = "T-FOREIGN"
CURSOR_TOKEN = "cursor-secret-token-value"
SLACK_TOKEN = "xoxb-super-secret-token"

WRITERS = (
    "consume_inbound_action",
    "bind_slack",
    "deliver_slack_root",
    "reconcile_cursor_create",
    "set_job_policy",
    "record_decision",
)

CASES = (
    "same_workspace_foreign_repo",
    "foreign_workspace_same_repo",
    "both_foreign",
    "missing_identity",
    "matching",
)

TRACKED_TABLES = (
    "durable_jobs",
    "durable_job_events",
    "provider_effect_claims",
    "provider_job_mappings",
    "slack_job_bindings",
    "job_authz_policies",
    "job_decisions",
    "job_inbound_actions",
)

MISSING_WORKSPACE_GAPS = (
    "missing_row",
    "empty_workspace",
    "missing_table",
)


def _complete(tmp_path: Path, **overrides) -> dict:
    section = {
        "enabled": True,
        "dispatch_enabled": False,
        "backend": "sqlite",
        "sqlite_path": str(tmp_path / "jobs.sqlite"),
        "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
        "cursor_adapter_mode": "injected",
        "slack_adapter_mode": "injected",
        "cursor_secret_ref": "CURSOR_API_KEY",
        "slack_secret_ref": "SLACK_BOT_TOKEN",
        "policy_version": "pol-1",
        "identity_binding": {
            "workspace_id": CONFIG_WORKSPACE,
            "repository_identity": CONFIG_REPO,
        },
    }
    section.update(overrides)
    return {"durable_jobs": section}


def _snapshot(path: Path) -> dict[str, int]:
    return {table: count_table(path, table) for table in TRACKED_TABLES}


def _snapshot_allow_missing(path: Path) -> dict[str, int | None]:
    out: dict[str, int | None] = {}
    for table in TRACKED_TABLES:
        try:
            out[table] = count_table(path, table)
        except sqlite3.OperationalError:
            out[table] = None
    return out


def _bindings_table_exists(path: Path) -> bool:
    conn = sqlite3.connect(path)
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='slack_job_bindings'",
        ).fetchone()
        return row is not None
    finally:
        conn.close()


class _IdleSlack:
    def __init__(self) -> None:
        self.posts: list[dict] = []
        self.lookups: list[str] = []

    def post_root(self, **kwargs):
        self.posts.append(dict(kwargs))
        raise AssertionError("identity reject must not post Slack")

    def lookup_by_client_msg_id(self, client_msg_id: str):
        self.lookups.append(client_msg_id)
        raise AssertionError("identity reject must not lookup Slack")


class _AcceptSlack:
    def __init__(self) -> None:
        self.posts: list[dict] = []
        self.lookups: list[str] = []

    def post_root(self, **kwargs):
        self.posts.append(dict(kwargs))

        class _R:
            kind = "accepted"
            message_ts = "42.1"

        return _R()

    def lookup_by_client_msg_id(self, client_msg_id: str):
        self.lookups.append(client_msg_id)
        return []


class _IdleProvider:
    def __init__(self) -> None:
        self.create_calls: list[dict] = []
        self.lookup_calls: list[str] = []

    def create_run(self, **kwargs):
        self.create_calls.append(dict(kwargs))
        raise AssertionError("identity reject must not create_run")

    def lookup_runs(self, **kwargs):
        self.lookup_calls.append(str(kwargs))
        raise AssertionError("identity reject must not lookup_runs")


class _AcceptProvider:
    def __init__(self) -> None:
        self.create_calls: list[dict] = []
        self.lookup_calls: list[str] = []

    def create_run(self, *, idempotency_key: str, job_id: str):
        self.create_calls.append(
            {"idempotency_key": idempotency_key, "job_id": job_id}
        )

        class _Run:
            run_id = "run-ok"
            idempotency_key = ""

        class _R:
            kind = "accepted"
            run = _Run()
            candidates = ()

        _R.run.idempotency_key = idempotency_key
        return _R()

    def lookup_runs(self, *, idempotency_key: str):
        self.lookup_calls.append(idempotency_key)
        return []


def _seed(
    tmp_path: Path,
    *,
    writer: str,
    case: str,
    idempotency_key: str,
):
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.lane import DurableLaneService
    from agent.durable_jobs.slack_contract import SlackBindingLedger
    from agent.durable_jobs.store import DurableJobStore
    from tests.agent.durable_jobs.authz_fixtures import (
        install_default_adapter_authorization,
    )

    repo = CONFIG_REPO
    workspace = CONFIG_WORKSPACE
    if case in ("same_workspace_foreign_repo", "both_foreign"):
        repo = FOREIGN_REPO
    if case in ("foreign_workspace_same_repo", "both_foreign"):
        workspace = FOREIGN_WORKSPACE

    raw = _complete(tmp_path)
    if case == "missing_identity":
        raw["durable_jobs"].pop("identity_binding", None)
        repo = CONFIG_REPO
        workspace = CONFIG_WORKSPACE

    cfg = load_durable_jobs_config(raw)
    store = DurableJobStore(sqlite_path=cfg.sqlite_path)
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="identity-authority",
        repository_identity=repo,
        idempotency_key=idempotency_key,
    )
    prebind = writer != "bind_slack"
    if prebind:
        SlackBindingLedger(sqlite_path=store.sqlite_path).bind(
            job_id=job.job_id,
            workspace_id=workspace,
            channel_id="C123",
            root_thread_ts="111.222",
            candidate_id="cand-1",
            candidate_version="v1",
        )
    DecisionLedger(sqlite_path=store.sqlite_path).set_policy(
        job_id=job.job_id,
        policy_version="pol-1",
        allowed_actors=("U-alice",),
        expires_at="2099-01-01T00:00:00+00:00",
    )
    if prebind:
        install_default_adapter_authorization(store.sqlite_path, job.job_id)
    lane = DurableLaneService(config=cfg, store=store)
    return lane, job, store, workspace


def _seed_matching_job_without_workspace(
    tmp_path: Path,
    *,
    gap: str,
    idempotency_key: str,
):
    """Matching repository identity, no verifiable persisted workspace binding."""
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.lane import DurableLaneService
    from agent.durable_jobs.store import DurableJobStore

    cfg = load_durable_jobs_config(_complete(tmp_path))
    store = DurableJobStore(sqlite_path=cfg.sqlite_path)
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="missing-workspace",
        repository_identity=CONFIG_REPO,
        idempotency_key=idempotency_key,
    )
    if gap == "empty_workspace":
        conn = sqlite3.connect(store.sqlite_path)
        try:
            now = "2099-01-01T00:00:00+00:00"
            conn.execute(
                """
                INSERT INTO slack_job_bindings(
                    job_id, workspace_id, channel_id, root_thread_ts,
                    candidate_id, candidate_version, outbound_client_msg_id,
                    delivered_message_ts, status, unknown_reason,
                    claim_owner_token, claim_leased_at, claim_expires_at,
                    claim_generation, created_at, updated_at
                ) VALUES (?, '', 'C123', '111.222', 'cand-1', 'v1', ?,
                          NULL, 'bound', NULL, NULL, NULL, NULL, 0, ?, ?)
                """,
                (job.job_id, f"empty-ws-{job.job_id}", now, now),
            )
            conn.commit()
        finally:
            conn.close()
    elif gap == "missing_table":
        conn = sqlite3.connect(store.sqlite_path)
        try:
            conn.execute("DROP TABLE slack_job_bindings")
            conn.commit()
        finally:
            conn.close()
    elif gap != "missing_row":
        raise AssertionError(f"unknown workspace gap {gap}")
    lane = DurableLaneService(config=cfg, store=store)
    return lane, job, store


def _inbound(job, workspace_id: str, **overrides):
    payload = dict(
        job_id=job.job_id,
        workspace_id=workspace_id,
        channel_id="C123",
        root_thread_ts="111.222",
        actor_id="U-alice",
        decision_type="go",
        decision_idempotency_key="dec-identity",
        policy_version="pol-1",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    payload.update(overrides)
    return payload


def _assert_no_secrets(payload: object) -> None:
    dumped = str(payload)
    assert CURSOR_TOKEN not in dumped
    assert SLACK_TOKEN not in dumped
    assert "xoxb-" not in dumped
    assert "supersecret" not in dumped


def _invoke(lane, writer: str, job, workspace_id: str, *, positive: bool):
    slack = _AcceptSlack() if positive else _IdleSlack()
    provider = _AcceptProvider() if positive else _IdleProvider()
    ack = RecordingAckPort()
    caught: BaseException | None = None
    value = None
    try:
        if writer == "consume_inbound_action":
            value = lane.consume_inbound_action(
                ack, **_inbound(job, workspace_id)
            )
        elif writer == "bind_slack":
            value = lane.bind_slack(
                job_id=job.job_id,
                workspace_id=workspace_id,
                channel_id="C123",
                root_thread_ts="111.222",
                candidate_id="cand-1",
                candidate_version="v1",
            )
        elif writer == "deliver_slack_root":
            value = lane.deliver_slack_root(job_id=job.job_id, slack_port=slack)
        elif writer == "reconcile_cursor_create":
            value = lane.reconcile_cursor_create(
                job_id=job.job_id,
                action_id="create_run",
                origin_platform="slack",
                origin_chat_id="C123",
                origin_root_thread_id="111.222",
                candidate_id="cand-1",
                candidate_version="v1",
                provider=provider,
            )
        elif writer == "set_job_policy":
            value = lane.set_job_policy(
                job_id=job.job_id,
                policy_version="pol-2",
                allowed_actors=("U-alice",),
            )
        elif writer == "record_decision":
            value = lane.record_decision(
                job_id=job.job_id,
                decision_type="hold",
                candidate_id="cand-1",
                candidate_version="v1",
                actor_id="U-alice",
                policy_version="pol-1",
                decision_idempotency_key="dec-identity-direct",
            )
        else:
            raise AssertionError(f"unknown writer {writer}")
    except BaseException as exc:  # noqa: BLE001 — typed fail-closed vs leak
        caught = exc
    return value, caught, ack, slack, provider


@pytest.mark.parametrize("writer", WRITERS)
@pytest.mark.parametrize("case", CASES)
def test_lane_writer_identity_matrix(tmp_path, writer, case):
    lane, job, store, workspace = _seed(
        tmp_path,
        writer=writer,
        case=case,
        idempotency_key=f"idem-{writer}-{case}",
    )
    before = _snapshot(store.sqlite_path)
    positive = case == "matching"
    value, caught, ack, slack, provider = _invoke(
        lane, writer, job, workspace, positive=positive
    )
    _assert_no_secrets(value)
    _assert_no_secrets(caught)
    _assert_no_secrets(ack.acks)

    if positive:
        assert caught is None, caught
        if writer == "consume_inbound_action":
            assert value is not None
            assert value.ok is True
            assert ack.acks
        else:
            assert value is not None
        if writer == "bind_slack":
            assert count_table(store.sqlite_path, "slack_job_bindings") == (
                before["slack_job_bindings"] + 1
            )
        if writer == "deliver_slack_root":
            assert slack.posts
        if writer == "reconcile_cursor_create":
            assert provider.create_calls
        return

    after = _snapshot(store.sqlite_path)
    assert after == before
    assert ack.acks == []
    assert slack.posts == []
    assert slack.lookups == []
    assert provider.create_calls == []
    assert provider.lookup_calls == []
    if writer == "consume_inbound_action":
        assert caught is None
        assert value is not None
        assert value.ok is False
        assert value.ack_status == "rejected"
        assert getattr(value, "retryable", False) is False
        _assert_no_secrets(value)
        return
    assert value is None
    assert caught is not None
    assert type(caught).__name__ == "LaneIdentityRejected"
    _assert_no_secrets(caught)


def test_identity_check_does_not_import_psycopg(tmp_path, monkeypatch):
    import sys
    import types

    fake = types.ModuleType("psycopg")

    def _boom(*_a, **_k):
        raise AssertionError("psycopg must stay opt-in")

    fake.connect = _boom
    monkeypatch.setitem(sys.modules, "psycopg", fake)
    lane, job, store, workspace = _seed(
        tmp_path,
        writer="set_job_policy",
        case="foreign_workspace_same_repo",
        idempotency_key="idem-no-psycopg",
    )
    before = _snapshot(store.sqlite_path)
    value, caught, ack, slack, provider = _invoke(
        lane, "set_job_policy", job, workspace, positive=False
    )
    assert value is None
    assert type(caught).__name__ == "LaneIdentityRejected"
    assert _snapshot(store.sqlite_path) == before
    assert ack.acks == []
    assert slack.posts == []
    assert provider.create_calls == []
    assert "psycopg" not in sys.modules or sys.modules["psycopg"] is fake


@pytest.mark.parametrize("writer", WRITERS)
def test_persisted_foreign_workspace_rejects_matching_inbound(tmp_path, writer):
    """Persisted workspace vs config is authoritative even if the call matches."""
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.lane import DurableLaneService
    from agent.durable_jobs.slack_contract import SlackBindingLedger
    from agent.durable_jobs.store import DurableJobStore
    from tests.agent.durable_jobs.authz_fixtures import (
        install_default_adapter_authorization,
    )

    cfg = load_durable_jobs_config(_complete(tmp_path))
    store = DurableJobStore(sqlite_path=cfg.sqlite_path)
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="identity-authority",
        repository_identity=CONFIG_REPO,
        idempotency_key=f"idem-persisted-foreign-{writer}",
    )
    SlackBindingLedger(sqlite_path=store.sqlite_path).bind(
        job_id=job.job_id,
        workspace_id=FOREIGN_WORKSPACE,
        channel_id="C123",
        root_thread_ts="111.222",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    DecisionLedger(sqlite_path=store.sqlite_path).set_policy(
        job_id=job.job_id,
        policy_version="pol-1",
        allowed_actors=("U-alice",),
        expires_at="2099-01-01T00:00:00+00:00",
    )
    install_default_adapter_authorization(store.sqlite_path, job.job_id)
    lane = DurableLaneService(config=cfg, store=store)
    before = _snapshot(store.sqlite_path)
    value, caught, ack, slack, provider = _invoke(
        lane, writer, job, CONFIG_WORKSPACE, positive=False
    )
    assert _snapshot(store.sqlite_path) == before
    assert ack.acks == []
    assert slack.posts == []
    assert provider.create_calls == []
    if writer == "consume_inbound_action":
        assert caught is None
        assert value is not None
        assert value.ok is False
        assert value.ack_status == "rejected"
        return
    assert value is None
    assert type(caught).__name__ == "LaneIdentityRejected"


def test_inbound_foreign_workspace_rejects_matching_persisted_binding(tmp_path):
    lane, job, store, _workspace = _seed(
        tmp_path,
        writer="consume_inbound_action",
        case="matching",
        idempotency_key="idem-inbound-foreign",
    )
    before = _snapshot(store.sqlite_path)
    value, caught, ack, slack, provider = _invoke(
        lane, "consume_inbound_action", job, FOREIGN_WORKSPACE, positive=False
    )
    assert caught is None
    assert value is not None
    assert value.ok is False
    assert value.ack_status == "rejected"
    assert _snapshot(store.sqlite_path) == before
    assert ack.acks == []
    assert slack.posts == []
    assert provider.create_calls == []


def test_sqlite_row_count_helper_sees_seeded_identity_state(tmp_path):
    """Sanity: foreign workspace is persisted on the job binding before the call."""
    _lane, job, store, workspace = _seed(
        tmp_path,
        writer="deliver_slack_root",
        case="foreign_workspace_same_repo",
        idempotency_key="idem-seed-check",
    )
    assert workspace == FOREIGN_WORKSPACE
    conn = sqlite3.connect(store.sqlite_path)
    try:
        row = conn.execute(
            "SELECT workspace_id, repository_identity FROM slack_job_bindings "
            "JOIN durable_jobs USING (job_id) WHERE job_id = ?",
            (job.job_id,),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] == FOREIGN_WORKSPACE
    assert row[1] == CONFIG_REPO


def _assert_typed_identity_reject(writer, value, caught, ack, slack, provider):
    assert ack.acks == []
    assert slack.posts == []
    assert slack.lookups == []
    assert provider.create_calls == []
    assert provider.lookup_calls == []
    if writer == "consume_inbound_action":
        assert caught is None
        assert value is not None
        assert value.ok is False
        assert value.ack_status == "rejected"
        assert getattr(value, "retryable", False) is False
        _assert_no_secrets(value)
        return
    assert value is None
    assert caught is not None
    assert type(caught).__name__ == "LaneIdentityRejected"
    _assert_no_secrets(caught)


@pytest.mark.parametrize("writer", WRITERS)
@pytest.mark.parametrize("gap", MISSING_WORKSPACE_GAPS)
def test_missing_persisted_workspace_is_fail_closed(tmp_path, writer, gap):
    """No persisted workspace_id is not implicit approval except bind_slack bootstrap."""
    lane, job, store = _seed_matching_job_without_workspace(
        tmp_path,
        gap=gap,
        idempotency_key=f"idem-missing-ws-{writer}-{gap}",
    )
    before = _snapshot_allow_missing(store.sqlite_path)
    bootstrap = writer == "bind_slack" and gap == "missing_row"
    value, caught, ack, slack, provider = _invoke(
        lane, writer, job, CONFIG_WORKSPACE, positive=bootstrap
    )
    _assert_no_secrets(value)
    _assert_no_secrets(caught)
    _assert_no_secrets(ack.acks)

    if bootstrap:
        assert caught is None, caught
        assert value is not None
        assert _bindings_table_exists(store.sqlite_path)
        assert count_table(store.sqlite_path, "slack_job_bindings") == (
            (before["slack_job_bindings"] or 0) + 1
        )
        return

    after = _snapshot_allow_missing(store.sqlite_path)
    assert after == before
    if gap == "missing_table":
        assert _bindings_table_exists(store.sqlite_path) is False
    _assert_typed_identity_reject(writer, value, caught, ack, slack, provider)


@pytest.mark.parametrize("writer", WRITERS)
def test_bind_slack_bootstrap_then_matching_writers_succeed(tmp_path, writer):
    from tests.agent.durable_jobs.authz_fixtures import (
        install_default_adapter_authorization,
    )

    lane, job, store = _seed_matching_job_without_workspace(
        tmp_path,
        gap="missing_row",
        idempotency_key=f"idem-bootstrap-{writer}",
    )
    if writer != "bind_slack":
        bound = lane.bind_slack(
            job_id=job.job_id,
            workspace_id=CONFIG_WORKSPACE,
            channel_id="C123",
            root_thread_ts="111.222",
            candidate_id="cand-1",
            candidate_version="v1",
        )
        assert bound is not None
        install_default_adapter_authorization(store.sqlite_path, job.job_id)
        if writer in ("consume_inbound_action", "record_decision"):
            lane.set_job_policy(
                job_id=job.job_id,
                policy_version="pol-1",
                allowed_actors=("U-alice",),
                expires_at="2099-01-01T00:00:00+00:00",
            )
    value, caught, ack, slack, provider = _invoke(
        lane, writer, job, CONFIG_WORKSPACE, positive=True
    )
    _assert_no_secrets(value)
    _assert_no_secrets(caught)
    assert caught is None, caught
    assert value is not None
    if writer == "consume_inbound_action":
        assert value.ok is True
        assert ack.acks
    if writer == "deliver_slack_root":
        assert slack.posts
    if writer == "reconcile_cursor_create":
        assert provider.create_calls
    if writer == "bind_slack":
        assert count_table(store.sqlite_path, "slack_job_bindings") == 1
    if writer == "set_job_policy":
        assert count_table(store.sqlite_path, "job_authz_policies") == 1


def test_close_after_checkout_precedes_missing_workspace_reject(tmp_path):
    from agent.durable_jobs.lane import LaneClosedError

    lane, job, store = _seed_matching_job_without_workspace(
        tmp_path,
        gap="missing_row",
        idempotency_key="idem-close-precedes-missing-ws",
    )
    original = lane._require_sqlite_path

    def _checkout_then_close():
        checked = original()
        lane.close()
        return checked

    lane._require_sqlite_path = _checkout_then_close
    before = _snapshot(store.sqlite_path)
    with pytest.raises(LaneClosedError):
        lane.set_job_policy(
            job_id=job.job_id,
            policy_version="pol-2",
            allowed_actors=("U-alice",),
        )
    assert _snapshot(store.sqlite_path) == before
    assert lane._closed is True
    assert lane._store is None
