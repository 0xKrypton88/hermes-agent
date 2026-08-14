"""ENG-36 Package 2 — Gateway lifecycle seam without activation.

Default-off, fail-closed construction, shutdown/restart, Slack ingress reuse,
and secret redaction. No live Slack/Cursor/network.
"""

from __future__ import annotations

import socket
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

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


def test_complete_gates_construct_lane_with_null_adapters_when_no_transport(
    tmp_path,
):
    from agent.durable_jobs.adapters import NullCursorProvider, NullSlackPort
    from gateway.durable_job_lane import (
        attach_durable_job_lane,
        get_active_durable_job_lane,
    )

    handle = attach_durable_job_lane(raw_config=_complete(tmp_path))
    assert handle is not None
    assert handle.config.dispatch_allowed is True
    assert handle.config.enabled is True
    assert isinstance(handle.cursor_adapter, NullCursorProvider)
    assert isinstance(handle.slack_adapter, NullSlackPort)
    assert get_active_durable_job_lane() is handle
    assert handle.lane is not None


def test_double_construction_is_rejected(tmp_path):
    from gateway.durable_job_lane import (
        DurableJobLaneAlreadyAttached,
        attach_durable_job_lane,
    )

    first = attach_durable_job_lane(raw_config=_complete(tmp_path))
    assert first is not None
    with pytest.raises(DurableJobLaneAlreadyAttached):
        attach_durable_job_lane(raw_config=_complete(tmp_path))


def test_shutdown_clears_active_handle_and_allows_restart(tmp_path):
    from agent.durable_jobs.store import DurableJobStore
    from gateway.durable_job_lane import (
        attach_durable_job_lane,
        detach_durable_job_lane,
        get_active_durable_job_lane,
    )

    first = attach_durable_job_lane(raw_config=_complete(tmp_path))
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

    second = attach_durable_job_lane(raw_config=_complete(tmp_path))
    assert second is not None
    recovered = second.lane._require_sqlite_path().recover_job(job.job_id)
    assert recovered is not None
    assert recovered.job_id == job.job_id
    assert recovered.idempotency_key == "idem-restart"


def test_explicit_injected_transports_are_wired_behind_existing_ports(tmp_path):
    from agent.durable_jobs.cursor_cloud import CursorCloudAdapter
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.slack_bridge import SlackClientBridge
    from gateway.durable_job_lane import attach_durable_job_lane

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
    handle = attach_durable_job_lane(raw_config=_complete(tmp_path))
    assert handle is not None


def test_status_and_errors_redact_secrets(tmp_path):
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
    handle = attach_durable_job_lane(raw_config=_complete(tmp_path))
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


def test_active_slack_ingress_reuses_lane_inbound_not_a_parallel_router(tmp_path):
    from tests.agent.durable_jobs.eng28_support import RecordingAckPort
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.slack_contract import SlackBindingLedger
    from gateway.durable_job_lane import (
        attach_durable_job_lane,
        consume_slack_action_if_active,
    )

    handle = attach_durable_job_lane(raw_config=_complete(tmp_path, dispatch_enabled=False))
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


def test_malformed_slack_action_fail_closes_without_store_side_effects(tmp_path):
    from gateway.durable_job_lane import (
        attach_durable_job_lane,
        consume_slack_action_if_active,
    )

    handle = attach_durable_job_lane(raw_config=_complete(tmp_path, dispatch_enabled=False))
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


def test_gateway_runner_stop_detaches_constructed_lane(tmp_path):
    from gateway.durable_job_lane import (
        attach_to_gateway_runner,
        detach_from_gateway_runner,
        get_active_durable_job_lane,
    )

    runner = SimpleNamespace(_durable_job_lane=None)
    attach_to_gateway_runner(runner, raw_config=_complete(tmp_path))
    assert runner._durable_job_lane is not None
    assert get_active_durable_job_lane() is runner._durable_job_lane
    detach_from_gateway_runner(runner)
    assert runner._durable_job_lane is None
    assert get_active_durable_job_lane() is None


def test_claim_takeover_survives_reconstructed_lane(tmp_path):
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
    from gateway.durable_job_lane import (
        attach_durable_job_lane,
        detach_durable_job_lane,
    )

    handle = attach_durable_job_lane(raw_config=_complete(tmp_path, dispatch_enabled=False))
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
    handle2 = attach_durable_job_lane(raw_config=_complete(tmp_path, dispatch_enabled=False))
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


def test_seam_import_does_not_load_psycopg_or_slack_sdk():
    for name in ("psycopg", "slack_sdk", "slack_bolt"):
        sys.modules.pop(name, None)
    import gateway.durable_job_lane  # noqa: F401

    assert "psycopg" not in sys.modules
    assert "slack_sdk" not in sys.modules
