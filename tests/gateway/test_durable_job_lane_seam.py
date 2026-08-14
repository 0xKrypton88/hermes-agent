"""ENG-36 Package 2 — Gateway lifecycle seam without activation.

Default-off, fail-closed construction, shutdown/restart, Slack ingress reuse,
and secret redaction. No live Slack/Cursor/network.
"""

from __future__ import annotations

import asyncio
import json
import socket
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.agent.durable_jobs.package2_support import (
    attach_runtime_ready_lane,
    runtime_ready_transport_kwargs,
)

SECRET_DSN = "postgresql://hermes:supersecret@127.0.0.1:5432/durable_jobs"
SLACK_TOKEN = "xoxb-super-secret-token"


def _complete(tmp_path: Path, **overrides) -> dict:
    section = {
        "enabled": True,
        "dispatch_enabled": True,
        "backend": "sqlite",
        "sqlite_path": str(tmp_path / "jobs.sqlite"),
        "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
        "cursor_adapter_mode": "injected",
        "slack_adapter_mode": "injected",
        "cursor_secret_ref": "CURSOR_API_KEY",
        "slack_secret_ref": "SLACK_BOT_TOKEN",
        "policy_version": "eng29-matrix-v1",
        "identity_binding": {
            "workspace_id": "T1",
            "repository_identity": "github.com/example/repo",
        },
    }
    section.update(overrides)
    return {"durable_jobs": section}


@pytest.fixture(autouse=True)
def _reset_lane_seam():
    from gateway.durable_job_lane import detach_durable_job_lane

    detach_durable_job_lane()
    yield
    detach_durable_job_lane()


def _count_rows(sqlite_path: Path, table: str) -> int:
    conn = sqlite3.connect(sqlite_path)
    try:
        (n,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return int(n)
    finally:
        conn.close()


def _seed_bound_job(
    handle,
    *,
    idempotency_key: str = "idem-seed",
    repository_identity: str = "github.com/example/repo",
):
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.slack_contract import SlackBindingLedger

    store = handle.lane._require_sqlite_path()
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="ingress",
        repository_identity=repository_identity,
        idempotency_key=idempotency_key,
    )
    SlackBindingLedger(sqlite_path=store.sqlite_path).bind(
        job_id=job.job_id,
        workspace_id="T1",
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
    return job, store


def _verified_body(**overrides):
    body = {
        "team": {"id": "T1"},
        "user": {"id": "U-alice"},
        "channel": {"id": "C123"},
        "message": {"thread_ts": "111.222", "ts": "111.222"},
    }
    body.update(overrides)
    return body


def _action(action_id: str, payload: dict) -> dict:
    return {"action_id": action_id, "value": json.dumps(payload)}


def _write_active_config(tmp_path: Path, raw: dict) -> None:
    import yaml
    from hermes_cli import config as cfg

    (tmp_path / "config.yaml").write_text(
        yaml.safe_dump(raw, sort_keys=False),
        encoding="utf-8",
    )
    cfg._LOAD_CONFIG_CACHE.clear()
    cfg._RAW_CONFIG_CACHE.clear()


def test_default_config_does_not_construct_lane():
    from gateway.durable_job_lane import (
        attach_durable_job_lane,
        get_active_durable_job_lane,
    )

    handle = attach_durable_job_lane(raw_config={})
    assert handle is None
    assert get_active_durable_job_lane() is None


def test_enabled_alone_does_not_construct_lane(tmp_path):
    from gateway.durable_job_lane import attach_durable_job_lane

    handle = attach_durable_job_lane(
        raw_config={
            "durable_jobs": {
                "enabled": True,
                "dispatch_enabled": True,
                "sqlite_path": str(tmp_path / "jobs.sqlite"),
                "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
            }
        }
    )
    assert handle is None


def test_invalid_config_fail_closes_without_raising(tmp_path):
    from gateway.durable_job_lane import attach_durable_job_lane

    handle = attach_durable_job_lane(
        raw_config={"durable_jobs": "not-a-mapping"}
    )
    assert handle is None
    handle = attach_durable_job_lane(
        raw_config=_complete(tmp_path, cursor_adapter_mode="http-client")
    )
    assert handle is None


def test_complete_gates_without_runtime_capability_do_not_attach(tmp_path):
    from gateway.durable_job_lane import (
        attach_durable_job_lane,
        get_active_durable_job_lane,
    )

    handle = attach_durable_job_lane(raw_config=_complete(tmp_path))
    assert handle is None
    assert get_active_durable_job_lane() is None


def test_double_construction_is_rejected(tmp_path, monkeypatch):
    from gateway.durable_job_lane import (
        DurableJobLaneAlreadyAttached,
        attach_durable_job_lane,
    )

    first = attach_runtime_ready_lane(
        raw_config=_complete(tmp_path), monkeypatch=monkeypatch
    )
    assert first is not None
    with pytest.raises(DurableJobLaneAlreadyAttached):
        attach_durable_job_lane(
            raw_config=_complete(tmp_path),
            **runtime_ready_transport_kwargs(monkeypatch),
        )


def test_shutdown_clears_active_handle_and_allows_restart(tmp_path, monkeypatch):
    from agent.durable_jobs.store import DurableJobStore
    from gateway.durable_job_lane import (
        detach_durable_job_lane,
        get_active_durable_job_lane,
    )

    first = attach_runtime_ready_lane(
        raw_config=_complete(tmp_path), monkeypatch=monkeypatch
    )
    store = DurableJobStore(sqlite_path=tmp_path / "jobs.sqlite")
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="restart takeover",
        repository_identity="github.com/example/repo",
        idempotency_key="idem-restart",
    )
    detach_durable_job_lane()
    assert get_active_durable_job_lane() is None

    second = attach_runtime_ready_lane(
        raw_config=_complete(tmp_path), monkeypatch=monkeypatch
    )
    assert second is not None
    recovered = second.lane._require_sqlite_path().recover_job(job.job_id)
    assert recovered is not None
    assert recovered.job_id == job.job_id
    assert recovered.idempotency_key == "idem-restart"


def test_explicit_injected_transports_are_wired_behind_existing_ports(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.cursor_cloud import CursorCloudAdapter
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.slack_bridge import SlackClientBridge
    from gateway.durable_job_lane import attach_durable_job_lane
    from tests.agent.durable_jobs.package2_support import bind_runtime_secret_env

    bind_runtime_secret_env(monkeypatch)

    def request(**_k):
        raise AssertionError("transport must stay idle during attach")

    handle = attach_durable_job_lane(
        raw_config=_complete(tmp_path),
        cursor_transport=CursorCloudInjectedTransport(
            request=request, secret_ref="CURSOR_API_KEY"
        ),
        slack_transport=SlackInjectedTransport(
            request=request, secret_ref="SLACK_BOT_TOKEN"
        ),
    )
    assert handle is not None
    assert isinstance(handle.cursor_adapter, CursorCloudAdapter)
    assert isinstance(handle.slack_adapter, SlackClientBridge)


def test_attach_and_preflight_open_no_sockets(tmp_path, monkeypatch):
    from gateway.durable_job_lane import attach_durable_job_lane

    def _deny(*_a, **_k):
        raise AssertionError("durable job lane must not open sockets")

    monkeypatch.setattr(socket.socket, "connect", _deny)
    monkeypatch.setattr(socket.socket, "connect_ex", _deny)
    handle = attach_runtime_ready_lane(
        raw_config=_complete(tmp_path), monkeypatch=monkeypatch
    )
    assert handle is not None


def test_status_and_errors_redact_secrets(tmp_path, monkeypatch):
    from gateway.durable_job_lane import (
        attach_durable_job_lane,
        durable_job_lane_status,
    )

    closed = attach_durable_job_lane(
        raw_config={
            "durable_jobs": {
                "enabled": True,
                "backend": "postgresql",
                "postgres_dsn": SECRET_DSN,
                "postgres_schema": "public",
                "checkpoint_postgres_dsn": SECRET_DSN,
                "checkpoint_postgres_schema": "durable_jobs_ckpt",
                "postgres_storage_id": "durable_app",
                "checkpoint_postgres_storage_id": "durable_ckpt",
                "slack_secret_ref": SLACK_TOKEN,
            }
        }
    )
    assert closed is None
    handle = attach_runtime_ready_lane(
        raw_config=_complete(tmp_path), monkeypatch=monkeypatch
    )
    assert handle is not None
    status = durable_job_lane_status()
    dumped = f"{status!r} {handle!r} {handle.config!r}"
    assert SECRET_DSN not in dumped
    assert "supersecret" not in dumped
    assert SLACK_TOKEN not in dumped
    assert "xoxb-" not in dumped


def test_inactive_slack_ingress_is_noop():
    from gateway.durable_job_lane import consume_slack_action_if_active

    result = consume_slack_action_if_active(
        body={"team": {"id": "T1"}, "user": {"id": "U-alice"}, "channel": {"id": "C123"}},
        action={"action_id": "hermes_durable_go", "value": "{}"},
    )
    assert result is None


def test_active_slack_ingress_reuses_lane_inbound_not_a_parallel_router(
    tmp_path, monkeypatch
):
    from tests.agent.durable_jobs.eng28_support import RecordingAckPort
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.slack_contract import SlackBindingLedger
    from gateway.durable_job_lane import consume_slack_action_if_active

    handle = attach_runtime_ready_lane(
        raw_config=_complete(tmp_path, dispatch_enabled=False),
        monkeypatch=monkeypatch,
    )
    assert handle is not None
    store = handle.lane._require_sqlite_path()
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="ingress",
        repository_identity="github.com/example/repo",
        idempotency_key="idem-ingress",
    )
    SlackBindingLedger(sqlite_path=store.sqlite_path).bind(
        job_id=job.job_id,
        workspace_id="T1",
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

    import json

    action = {
        "action_id": "hermes_durable_go",
        "value": json.dumps(
            {
                "job_id": job.job_id,
                "workspace_id": "T1",
                "channel_id": "C123",
                "root_thread_ts": "111.222",
                "actor_id": "U-alice",
                "decision_type": "go",
                "decision_idempotency_key": "dec-ingress",
                "policy_version": "pol-1",
                "candidate_id": "cand-1",
                "candidate_version": "v1",
            }
        ),
    }
    body = {
        "team": {"id": "T1"},
        "user": {"id": "U-alice"},
        "channel": {"id": "C123"},
        "message": {"thread_ts": "111.222", "ts": "111.222"},
    }
    result = consume_slack_action_if_active(body, action, ack_port=RecordingAckPort())
    assert result is not None
    assert result.ok is True


def test_malformed_slack_action_fail_closes_without_store_side_effects(
    tmp_path, monkeypatch
):
    from gateway.durable_job_lane import consume_slack_action_if_active

    handle = attach_runtime_ready_lane(
        raw_config=_complete(tmp_path, dispatch_enabled=False),
        monkeypatch=monkeypatch,
    )
    assert handle is not None
    result = consume_slack_action_if_active(
        body={"team": {"id": "T1"}},
        action={"action_id": "hermes_durable_go", "value": "not-json"},
    )
    assert result is not None
    assert result.ok is False
    assert result.ack_status == "rejected"


def test_gateway_runner_start_attaches_default_off(monkeypatch, tmp_path):
    from gateway.config import GatewayConfig
    from gateway.run import GatewayRunner

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    runner = GatewayRunner(GatewayConfig(platforms={}, sessions_dir=tmp_path / "sessions"))
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None


def test_gateway_runner_stop_detaches_constructed_lane(tmp_path, monkeypatch):
    from gateway.durable_job_lane import (
        attach_to_gateway_runner,
        detach_from_gateway_runner,
        get_active_durable_job_lane,
    )

    runner = SimpleNamespace(_durable_job_lane=None)
    attach_to_gateway_runner(
        runner,
        raw_config=_complete(tmp_path),
        **runtime_ready_transport_kwargs(monkeypatch),
    )
    assert runner._durable_job_lane is not None
    assert get_active_durable_job_lane() is runner._durable_job_lane
    detach_from_gateway_runner(runner)
    assert runner._durable_job_lane is None
    assert get_active_durable_job_lane() is None


def test_claim_takeover_survives_reconstructed_lane(tmp_path, monkeypatch):
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS, FrozenClock
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        provider_idempotency_key,
        reconcile_cursor_create,
    )
    from tests.agent.durable_jobs.authz_fixtures import (
        install_default_adapter_authorization,
    )
    from tests.agent.durable_jobs.test_claim_leases import FakeCreateResult, FakeCursorProvider, FakeRun
    from gateway.durable_job_lane import detach_durable_job_lane

    handle = attach_runtime_ready_lane(
        raw_config=_complete(tmp_path, dispatch_enabled=False),
        monkeypatch=monkeypatch,
    )
    store = handle.lane._require_sqlite_path()
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="takeover",
        repository_identity="github.com/example/repo",
        idempotency_key="idem-takeover",
    )
    install_default_adapter_authorization(store.sqlite_path, job.job_id)
    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    kwargs = dict(
        job_id=job.job_id,
        action_id="create_run",
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    first = ledger.claim_effect(**kwargs)
    assert first.won is True
    detach_durable_job_lane()

    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    handle2 = attach_runtime_ready_lane(
        raw_config=_complete(tmp_path, dispatch_enabled=False),
        monkeypatch=monkeypatch,
    )
    assert handle2 is not None
    key = provider_idempotency_key(job.job_id, "create_run")
    provider = FakeCursorProvider(
        FakeCreateResult(kind="lost_response"),
        lookups=[FakeRun("run-unique", key)],
    )
    reopened = ProviderEffectLedger(
        sqlite_path=handle2.lane._require_sqlite_path().sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    adopted = reconcile_cursor_create(reopened, provider, **kwargs)
    assert adopted.status is EffectStatus.ADOPTED
    assert adopted.provider_run_id == "run-unique"


def test_spoofed_action_value_identity_is_rejected_with_zero_consumption(
    tmp_path, monkeypatch
):
    from gateway.durable_job_lane import (
        consume_slack_action_if_active,
        parse_slack_durable_action,
    )

    handle = attach_runtime_ready_lane(
        raw_config=_complete(tmp_path, dispatch_enabled=False),
        monkeypatch=monkeypatch,
    )
    assert handle is not None
    job, store = _seed_bound_job(handle, idempotency_key="idem-spoof")
    spoof_payload = {
        "job_id": job.job_id,
        "workspace_id": "T1",
        "channel_id": "C123",
        "root_thread_ts": "111.222",
        "actor_id": "U-alice",
        "decision_type": "go",
        "decision_idempotency_key": "dec-spoof",
        "policy_version": "pol-1",
        "candidate_id": "cand-1",
        "candidate_version": "v1",
    }
    body = _verified_body(
        team={"id": "T-EVIL"},
        user={"id": "U-eve"},
        channel={"id": "C-EVIL"},
        message={"thread_ts": "999.000", "ts": "999.000"},
    )
    action = _action("hermes_durable_go", spoof_payload)
    parsed = parse_slack_durable_action(body, action)
    assert parsed is None
    result = consume_slack_action_if_active(body, action)
    assert result is not None
    assert result.ok is False
    assert result.ack_status == "rejected"
    assert getattr(result, "retryable", False) is False
    assert _count_rows(store.sqlite_path, "job_inbound_actions") == 0
    assert _count_rows(store.sqlite_path, "job_decisions") == 0


def test_decision_type_and_identity_are_not_taken_from_action_value(tmp_path):
    from gateway.durable_job_lane import parse_slack_durable_action

    body = _verified_body()
    parsed = parse_slack_durable_action(
        body,
        _action(
            "hermes_durable_hold",
            {
                "job_id": "job-1",
                "workspace_id": "T-SPOOF",
                "channel_id": "C-SPOOF",
                "root_thread_ts": "0.0",
                "actor_id": "U-eve",
                "decision_type": "go",
                "decision_idempotency_key": "dec-1",
                "policy_version": "pol-1",
                "candidate_id": "cand-1",
                "candidate_version": "v1",
            },
        ),
    )
    assert parsed is None

    matching = parse_slack_durable_action(
        body,
        _action(
            "hermes_durable_go",
            {
                "job_id": "job-1",
                "workspace_id": "T1",
                "channel_id": "C123",
                "root_thread_ts": "111.222",
                "actor_id": "U-alice",
                "decision_type": "go",
                "decision_idempotency_key": "dec-1",
                "policy_version": "pol-1",
                "candidate_id": "cand-1",
                "candidate_version": "v1",
            },
        ),
    )
    assert matching is not None
    assert matching["workspace_id"] == "T1"
    assert matching["channel_id"] == "C123"
    assert matching["root_thread_ts"] == "111.222"
    assert matching["actor_id"] == "U-alice"
    assert matching["decision_type"] == "go"
    assert matching["job_id"] == "job-1"


def test_identity_binding_mismatch_is_rejected_with_zero_consumption(
    tmp_path, monkeypatch
):
    from gateway.durable_job_lane import consume_slack_action_if_active

    handle = attach_runtime_ready_lane(
        raw_config=_complete(tmp_path, dispatch_enabled=False),
        monkeypatch=monkeypatch,
    )
    assert handle is not None
    job, store = _seed_bound_job(handle, idempotency_key="idem-binding")
    body = _verified_body(team={"id": "T-OTHER"})
    action = _action(
        "hermes_durable_go",
        {
            "job_id": job.job_id,
            "decision_idempotency_key": "dec-binding",
            "policy_version": "pol-1",
            "candidate_id": "cand-1",
            "candidate_version": "v1",
        },
    )
    result = consume_slack_action_if_active(body, action)
    assert result is not None
    assert result.ok is False
    assert result.ack_status == "rejected"
    assert _count_rows(store.sqlite_path, "job_inbound_actions") == 0
    assert _count_rows(store.sqlite_path, "job_decisions") == 0


def test_old_runner_stop_does_not_shutdown_new_runner_lane(tmp_path, monkeypatch):
    from gateway.config import GatewayConfig
    from gateway.durable_job_lane import consume_slack_action_if_active
    from gateway.run import GatewayRunner

    old = GatewayRunner(
        GatewayConfig(
            platforms={},
            sessions_dir=tmp_path / "sessions-old",
            loop_watchdog=False,
        )
    )
    new = GatewayRunner(
        GatewayConfig(
            platforms={},
            sessions_dir=tmp_path / "sessions-new",
            loop_watchdog=False,
        )
    )
    from gateway.durable_job_lane import attach_to_gateway_runner

    attach_to_gateway_runner(
        old,
        raw_config=_complete(tmp_path / "old"),
        **runtime_ready_transport_kwargs(monkeypatch),
    )
    attach_to_gateway_runner(
        new,
        raw_config=_complete(tmp_path / "new"),
        **runtime_ready_transport_kwargs(monkeypatch),
    )
    assert old._durable_job_lane is not None
    assert new._durable_job_lane is not None
    assert old._durable_job_lane is not new._durable_job_lane

    job, store = _seed_bound_job(new._durable_job_lane, idempotency_key="idem-live")
    old._maybe_detach_durable_job_lane()
    assert getattr(old, "_durable_job_lane", None) is None
    assert new._durable_job_lane is not None

    result = consume_slack_action_if_active(
        _verified_body(),
        _action(
            "hermes_durable_go",
            {
                "job_id": job.job_id,
                "decision_idempotency_key": "dec-live",
                "policy_version": "pol-1",
                "candidate_id": "cand-1",
                "candidate_version": "v1",
            },
        ),
    )
    assert result is not None
    assert result.ok is True
    assert _count_rows(store.sqlite_path, "job_inbound_actions") == 1


def test_consume_after_shutdown_is_retryable_without_durable_write(
    tmp_path, monkeypatch
):
    from gateway.durable_job_lane import consume_slack_action_if_active

    handle = attach_runtime_ready_lane(
        raw_config=_complete(tmp_path, dispatch_enabled=False),
        monkeypatch=monkeypatch,
    )
    assert handle is not None
    job, store = _seed_bound_job(handle, idempotency_key="idem-closed")
    handle.shutdown()
    result = consume_slack_action_if_active(
        _verified_body(),
        _action(
            "hermes_durable_go",
            {
                "job_id": job.job_id,
                "decision_idempotency_key": "dec-closed",
                "policy_version": "pol-1",
                "candidate_id": "cand-1",
                "candidate_version": "v1",
            },
        ),
    )
    assert result is not None
    assert result.ok is False
    assert result.retryable is True
    assert _count_rows(store.sqlite_path, "job_inbound_actions") == 0
    assert _count_rows(store.sqlite_path, "job_decisions") == 0


def test_cross_repo_slack_decision_is_rejected_with_zero_durable_writes(
    tmp_path, monkeypatch
):
    from gateway.durable_job_lane import consume_slack_action_if_active

    handle = attach_runtime_ready_lane(
        raw_config=_complete(tmp_path, dispatch_enabled=False),
        monkeypatch=monkeypatch,
    )
    assert handle is not None
    assert handle.config.identity_binding is not None
    assert (
        handle.config.identity_binding.repository_identity
        == "github.com/example/repo"
    )
    job, store = _seed_bound_job(
        handle,
        idempotency_key="idem-cross-repo",
        repository_identity="github.com/evil/other",
    )
    assert job.repository_identity == "github.com/evil/other"
    result = consume_slack_action_if_active(
        _verified_body(),
        _action(
            "hermes_durable_go",
            {
                "job_id": job.job_id,
                "decision_idempotency_key": "dec-cross-repo",
                "policy_version": "pol-1",
                "candidate_id": "cand-1",
                "candidate_version": "v1",
            },
        ),
    )
    assert result is not None
    assert result.ok is False
    assert result.ack_status == "rejected"
    assert getattr(result, "retryable", False) is False
    assert _count_rows(store.sqlite_path, "job_inbound_actions") == 0
    assert _count_rows(store.sqlite_path, "job_decisions") == 0


@pytest.mark.asyncio
async def test_gateway_runner_start_stop_does_not_attach_without_runtime_capability(
    monkeypatch, tmp_path
):
    from gateway.config import GatewayConfig
    from gateway.durable_job_lane import get_active_durable_job_lane
    from gateway.run import GatewayRunner

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    raw = _complete(tmp_path)
    raw["gateway"] = {"loop_watchdog": False}
    _write_active_config(tmp_path, raw)
    runner = GatewayRunner(
        GatewayConfig(
            platforms={},
            sessions_dir=tmp_path / "sessions",
            loop_watchdog=False,
        )
    )
    ok = await asyncio.wait_for(runner.start(), timeout=60)
    assert ok is True
    # Gateway start never injects transports. Complete yaml without a
    # bound runtime capability must fail closed — no handle, no adapters.
    assert getattr(runner, "_durable_job_lane", None) is None
    assert get_active_durable_job_lane() is None
    await asyncio.wait_for(runner.stop(), timeout=60)
    assert getattr(runner, "_durable_job_lane", None) is None
    assert get_active_durable_job_lane() is None


def test_seam_import_does_not_load_psycopg_or_slack_sdk():
    for name in ("psycopg", "slack_sdk", "slack_bolt"):
        sys.modules.pop(name, None)
    import gateway.durable_job_lane  # noqa: F401

    assert "psycopg" not in sys.modules
    assert "slack_sdk" not in sys.modules
    assert "slack_bolt" not in sys.modules
