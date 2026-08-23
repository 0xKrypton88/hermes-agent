"""Isolated shadow/E2E harness for injected Cursor Cloud and Slack request ports.

Deterministic fakes only. No production channel, live provider traffic,
live job, or release decision. ``dispatch_allowed`` stays false and
``attempt_dispatch`` remains hard-disabled.

Reworked onto ENG-50: attach binds through an owner that already stores a
matching runtime identity in instance ``__dict__``. Missing identity must
still refuse dispatch rather than mint a client.
"""

from __future__ import annotations

import socket
from pathlib import Path

import pytest

from tests.agent.durable_jobs.package2_support import bind_runtime_secret_env
from tests.agent.durable_jobs.test_injected_request_ports import (
    CHANNEL,
    CURSOR_KEY,
    CURSOR_SECRET_VALUE,
    REPO,
    SLACK_KEY,
    SLACK_SECRET_VALUE,
    THREAD,
    WORKSPACE,
    FakeCursorCloudClient,
    FakeSlackClient,
    _assert_no_secrets,
    _assert_receipts_sanitized,
    _complete,
    _cursor_ids,
    _load_ports,
    _owner_with_matching_identity,
    _SECRET_CURSOR,
    _SECRET_SLACK,
)


@pytest.fixture(autouse=True)
def _reset_lane_seam():
    from gateway.durable_job_lane import detach_durable_job_lane

    detach_durable_job_lane()
    yield
    detach_durable_job_lane()


def _ports_and_clients(ports):
    cursor_client = FakeCursorCloudClient()
    slack_client = FakeSlackClient()
    cursor_port = ports.CursorCloudInjectedRequestPort(
        client=cursor_client,
        secret_ref=_SECRET_CURSOR,
        workspace_id=WORKSPACE,
        repository_identity=REPO,
        credential_resolver=lambda _secret_ref: CURSOR_SECRET_VALUE,
    )
    slack_port = ports.SlackInjectedRequestPort(
        client=slack_client,
        secret_ref=_SECRET_SLACK,
        workspace_id=WORKSPACE,
        channel_id=CHANNEL,
        repository_identity=REPO,
        root_thread_ts=THREAD,
        credential_resolver=lambda _secret_ref: SLACK_SECRET_VALUE,
    )
    return cursor_client, slack_client, cursor_port, slack_port


def _attach_shadow_lane(tmp_path: Path, monkeypatch, cursor_port, slack_port):
    from gateway.durable_job_lane import attach_durable_job_lane
    from agent.durable_jobs.production_binding import bind_production_transports

    bind_runtime_secret_env(monkeypatch)
    raw = _complete(tmp_path)
    owner = _owner_with_matching_identity(
        _durable_job_slack_channel_id=CHANNEL,
        _durable_job_slack_root_thread_ts=THREAD,
    )
    bound = bind_production_transports(
        raw,
        owner=owner,
        cursor_request=cursor_port,
        slack_request=slack_port,
    )
    from tests.agent.durable_jobs.package2_support import make_test_writer_authority
    handle = attach_durable_job_lane(
        raw_config=raw, writer_authority_check=make_test_writer_authority(), **bound
    )
    return raw, bound, handle


def test_shadow_e2e_success_path_uses_only_injected_fakes(tmp_path, monkeypatch):
    from agent.durable_jobs.cursor_cloud import CursorCloudAdapter, CursorCreateKind
    from agent.durable_jobs.service import DispatchDisabledError, DurableJobService
    from agent.durable_jobs.slack_bridge import SlackClientBridge, SlackPostKind
    from gateway.durable_job_lane import get_active_durable_job_lane

    ports = _load_ports()
    cursor_client, slack_client, cursor_port, slack_port = _ports_and_clients(ports)
    raw, _bound, handle = _attach_shadow_lane(
        tmp_path, monkeypatch, cursor_port, slack_port
    )
    assert handle is not None
    assert get_active_durable_job_lane() is handle
    assert handle.config.dispatch_allowed is False
    assert handle.preflight.dispatch_allowed is False
    assert isinstance(handle.cursor_adapter, CursorCloudAdapter)
    assert isinstance(handle.slack_adapter, SlackClientBridge)

    created = handle.cursor_adapter.create_run(
        idempotency_key=CURSOR_KEY, job_id="job-shadow-1"
    )
    assert created.kind is CursorCreateKind.ACCEPTED
    name, agent_id = _cursor_ids()
    assert created.run is not None
    assert created.run.agent_id == agent_id
    assert cursor_client.calls[0][0] == "create_agent"
    assert cursor_client.calls[0][1]["name"] == name
    assert cursor_client.calls[0][1]["agentId"] == agent_id

    looked = handle.cursor_adapter.lookup_runs(idempotency_key=CURSOR_KEY)
    assert any(run.agent_id == agent_id for run in looked)
    assert cursor_client.calls[1] == ("get_agent", agent_id)

    posted = handle.slack_adapter.post_root(
        client_msg_id=SLACK_KEY,
        workspace_id=WORKSPACE,
        channel_id=CHANNEL,
        root_thread_ts=THREAD,
        job_id="job-shadow-1",
    )
    assert posted.kind is SlackPostKind.ACCEPTED
    assert slack_client.calls[0][0] == "chat_postMessage"
    assert slack_client.calls[0][1]["client_msg_id"] == SLACK_KEY

    slack_hits = handle.slack_adapter.lookup_by_client_msg_id(SLACK_KEY)
    assert slack_hits and slack_hits[0].client_msg_id == SLACK_KEY
    assert slack_client.calls[1][0] == "conversations_replies"

    with pytest.raises(DispatchDisabledError):
        DurableJobService(config=handle.config).attempt_dispatch("job-shadow-1")
    dumped = f"{handle!r} {handle.preflight!r} {raw!r} {cursor_port!r} {slack_port!r}"
    _assert_no_secrets(dumped)
    _assert_receipts_sanitized(cursor_port)
    _assert_receipts_sanitized(slack_port)
    assert [item["operation"] for item in cursor_port.receipts] == ["create", "lookup"]
    assert [item["operation"] for item in slack_port.receipts] == ["post_root", "lookup"]
    assert cursor_port.receipts[0] == cursor_port.receipts[0]
    assert slack_port.receipts[0]["payload"]["client_msg_id"] == SLACK_KEY


def test_shadow_e2e_mismatch_and_foreign_key_have_zero_provider_effect(
    tmp_path, monkeypatch
):
    ports = _load_ports()
    cursor_client, slack_client, cursor_port, slack_port = _ports_and_clients(ports)
    _raw, _bound, handle = _attach_shadow_lane(
        tmp_path, monkeypatch, cursor_port, slack_port
    )
    assert handle is not None
    before_cursor = list(cursor_client.calls)
    before_slack = list(slack_client.calls)

    with pytest.raises(ports.RequestPortMismatch):
        cursor_port(
            operation="create",
            secret_ref=_SECRET_CURSOR,
            payload={
                "idempotency_key": CURSOR_KEY,
                "name": "foreign-cursor-key",
                "agentId": _cursor_ids("foreign-cursor-key")[1],
            },
        )
    with pytest.raises(ports.RequestPortMismatch):
        slack_port(
            operation="post_root",
            secret_ref=_SECRET_SLACK,
            payload={
                "workspace_id": "T-FOREIGN",
                "channel_id": CHANNEL,
                "root_thread_ts": THREAD,
                "client_msg_id": SLACK_KEY,
                "text": "nope",
            },
        )
    assert cursor_client.calls == before_cursor
    assert slack_client.calls == before_slack
    assert cursor_port.receipts[-1]["client_invoked"] is False
    assert slack_port.receipts[-1]["client_invoked"] is False


def test_shadow_e2e_timeout_provider_error_retry_and_bounded_shutdown(
    tmp_path, monkeypatch
):
    from gateway.durable_job_lane import detach_durable_job_lane, get_active_durable_job_lane

    ports = _load_ports()
    cursor_client, slack_client, cursor_port, slack_port = _ports_and_clients(ports)
    _raw, _bound, handle = _attach_shadow_lane(
        tmp_path, monkeypatch, cursor_port, slack_port
    )
    assert handle is not None

    with pytest.raises(ports.RequestPortTimeout):
        cursor_port(
            operation="create",
            secret_ref=_SECRET_CURSOR,
            payload={
                "idempotency_key": CURSOR_KEY,
                "name": _cursor_ids()[0],
                "agentId": _cursor_ids()[1],
                "workspace_id": WORKSPACE,
                "repository_identity": REPO,
            },
            timeout_seconds=0,
        )
    assert cursor_client.calls == []

    cursor_client.create_error = RuntimeError(
        f"provider exploded token={CURSOR_SECRET_VALUE}"
    )
    with pytest.raises(ports.RequestPortError) as caught:
        cursor_port(
            operation="create",
            secret_ref=_SECRET_CURSOR,
            payload={
                "idempotency_key": "cursor-error-key",
                "name": _cursor_ids("cursor-error-key")[0],
                "agentId": _cursor_ids("cursor-error-key")[1],
                "workspace_id": WORKSPACE,
                "repository_identity": REPO,
            },
        )
    assert CURSOR_SECRET_VALUE not in str(caught.value)
    cursor_client.create_error = None
    cursor_client.calls.clear()

    first = handle.cursor_adapter.create_run(
        idempotency_key=CURSOR_KEY, job_id="job-retry"
    )
    second = handle.cursor_adapter.lookup_runs(idempotency_key=CURSOR_KEY)
    assert first.run is not None
    assert any(run.agent_id == first.run.agent_id for run in second)
    assert [call[0] for call in cursor_client.calls] == ["create_agent", "get_agent"]

    handle.slack_adapter.post_root(
        client_msg_id=SLACK_KEY,
        workspace_id=WORKSPACE,
        channel_id=CHANNEL,
        root_thread_ts=THREAD,
        job_id="job-retry",
    )
    retry_lookup = handle.slack_adapter.lookup_by_client_msg_id(SLACK_KEY)
    assert retry_lookup[0].client_msg_id == SLACK_KEY
    assert [call[0] for call in slack_client.calls] == [
        "chat_postMessage",
        "conversations_replies",
    ]

    detach_durable_job_lane(handle)
    assert get_active_durable_job_lane() is None
    # Lane detach must not close injected ports; they remain caller-owned.
    cursor_client.calls.clear()
    reused = cursor_port(
        operation="lookup",
        secret_ref=_SECRET_CURSOR,
        payload={
            "idempotency_key": CURSOR_KEY,
            "workspace_id": WORKSPACE,
            "repository_identity": REPO,
        },
    )
    assert reused["agentId"] == _cursor_ids()[1]
    cursor_port.close()
    slack_port.close()
    with pytest.raises(ports.RequestPortClosed):
        cursor_port(
            operation="lookup",
            secret_ref=_SECRET_CURSOR,
            payload={
            "idempotency_key": CURSOR_KEY,
            "workspace_id": WORKSPACE,
            "repository_identity": REPO,
        },
        )


def test_shadow_e2e_opens_no_sockets_and_does_not_read_secret_values(
    tmp_path, monkeypatch
):
    def _deny(*_a, **_k):
        raise AssertionError("shadow e2e must not open sockets")

    monkeypatch.setattr(socket.socket, "connect", _deny)
    monkeypatch.setattr(socket.socket, "connect_ex", _deny)
    ports = _load_ports()
    resolver_calls: list[str] = []

    def resolver(name: str) -> str:
        resolver_calls.append(name)
        return CURSOR_SECRET_VALUE if name == _SECRET_CURSOR else SLACK_SECRET_VALUE

    cursor_client = FakeCursorCloudClient()
    slack_client = FakeSlackClient()
    cursor_port = ports.CursorCloudInjectedRequestPort(
        client=cursor_client,
        secret_ref=_SECRET_CURSOR,
        workspace_id=WORKSPACE,
        repository_identity=REPO,
        credential_resolver=resolver,
    )
    slack_port = ports.SlackInjectedRequestPort(
        client=slack_client,
        secret_ref=_SECRET_SLACK,
        workspace_id=WORKSPACE,
        channel_id=CHANNEL,
        repository_identity=REPO,
        root_thread_ts=THREAD,
        credential_resolver=resolver,
    )
    assert resolver_calls == []
    _raw, _bound, handle = _attach_shadow_lane(
        tmp_path, monkeypatch, cursor_port, slack_port
    )
    assert handle is not None
    assert resolver_calls == []
    assert cursor_client.calls == []
    assert slack_client.calls == []
    handle.cursor_adapter.create_run(idempotency_key=CURSOR_KEY, job_id="job-secret")
    assert resolver_calls == [_SECRET_CURSOR]
    _assert_no_secrets(repr(cursor_port))
    _assert_no_secrets(repr(slack_port))
    _assert_receipts_sanitized(cursor_port)
