"""ENG-58: Gateway seam reachability for injected request ports.

Complete candidate-bound config plus a live-looking Slack adapter in
``runner.adapters`` must not attach or mint a client. Explicitly injected
request ports may attach with ``dispatch_allowed=false``. Attach/preflight
open no sockets and never log credential values. No production activation.

Reworked onto ENG-50: missing/wrong ports are proven with a matching
runtime identity already stored on the runner instance dict, so attach
refusal is the port gate rather than a weakened identity check.
"""

from __future__ import annotations

import socket
from pathlib import Path
from types import SimpleNamespace

import pytest

from gateway.config import Platform
from tests.agent.durable_jobs.package2_support import bind_runtime_secret_env
from tests.agent.durable_jobs.test_injected_request_ports import (
    CHANNEL,
    CURSOR_SECRET_VALUE,
    REPO,
    SLACK_SECRET_VALUE,
    THREAD,
    WORKSPACE,
    FakeCursorCloudClient,
    FakeSlackClient,
    _assert_no_secrets,
    _complete,
    _load_ports,
    _matching_identity,
    _SECRET_CURSOR,
    _SECRET_SLACK,
)
from tests.gateway.test_durable_job_lane_production_binding import (
    _make_runner,
    _write_active_config,
)


@pytest.fixture(autouse=True)
def _reset_lane_seam():
    from gateway.durable_job_lane import detach_durable_job_lane

    detach_durable_job_lane()
    yield
    detach_durable_job_lane()


def _prepare(tmp_path: Path, monkeypatch, **overrides):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    bind_runtime_secret_env(monkeypatch)
    raw = _complete(tmp_path, **overrides)
    _write_active_config(tmp_path, raw)
    return raw, _make_runner(tmp_path)


def _live_looking_slack_adapter():
    class _LiveLookingSlack:
        platform = Platform.SLACK

        def _get_client(self, *_a, **_k):
            raise AssertionError("Gateway must not pull a Slack client during attach")

        async def send(self, *_a, **_k):
            raise AssertionError("Gateway must not send on Slack during attach")

        def chat_postMessage(self, **_k):
            raise AssertionError("Gateway must not post Slack during attach")

    return _LiveLookingSlack()


def _install_injected_ports(runner, ports, cursor_client, slack_client):
    runner._durable_job_cursor_request = ports.CursorCloudInjectedRequestPort(
        client=cursor_client,
        secret_ref=_SECRET_CURSOR,
        workspace_id=WORKSPACE,
        repository_identity=REPO,
    )
    runner._durable_job_slack_request = ports.SlackInjectedRequestPort(
        client=slack_client,
        secret_ref=_SECRET_SLACK,
        workspace_id=WORKSPACE,
        channel_id=CHANNEL,
        repository_identity=REPO,
        root_thread_ts=THREAD,
    )
    runner._durable_job_slack_channel_id = CHANNEL
    runner._durable_job_slack_root_thread_ts = THREAD
    runner._durable_job_runtime_identity = _matching_identity()


def test_gateway_seam_does_not_attach_from_live_looking_slack_adapter(
    tmp_path, monkeypatch
):
    from gateway.durable_job_lane import get_active_durable_job_lane

    _raw, runner = _prepare(tmp_path, monkeypatch)
    runner.adapters[Platform.SLACK] = _live_looking_slack_adapter()
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert get_active_durable_job_lane() is None


def test_gateway_seam_does_not_attach_from_injected_clients_on_the_adapter_map(
    tmp_path, monkeypatch
):
    from gateway.durable_job_lane import get_active_durable_job_lane

    _raw, runner = _prepare(tmp_path, monkeypatch, dispatch_enabled=True)
    adapter = _live_looking_slack_adapter()
    adapter.client = FakeSlackClient()
    runner.adapters[Platform.SLACK] = adapter
    runner._durable_job_cursor_client = FakeCursorCloudClient()
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert get_active_durable_job_lane() is None


def test_gateway_seam_attaches_explicit_injected_ports_without_activation(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.cursor_cloud import CursorCloudAdapter
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.service import DispatchDisabledError, DurableJobService
    from agent.durable_jobs.slack_bridge import SlackClientBridge
    from gateway.durable_job_lane import get_active_durable_job_lane

    raw, runner = _prepare(tmp_path, monkeypatch)
    ports = _load_ports()
    cursor_client = FakeCursorCloudClient()
    slack_client = FakeSlackClient()
    _install_injected_ports(runner, ports, cursor_client, slack_client)
    runner.adapters[Platform.SLACK] = _live_looking_slack_adapter()
    runner._maybe_attach_durable_job_lane()
    handle = getattr(runner, "_durable_job_lane", None)
    assert handle is not None
    assert get_active_durable_job_lane() is handle
    assert isinstance(handle.cursor_adapter, CursorCloudAdapter)
    assert isinstance(handle.slack_adapter, SlackClientBridge)
    assert type(handle.cursor_adapter._transport) is CursorCloudInjectedTransport
    assert type(handle.slack_adapter._transport) is SlackInjectedTransport
    assert handle.config.dispatch_allowed is False
    assert handle.preflight.dispatch_allowed is False
    assert handle.preflight.runtime_ready is True
    assert cursor_client.calls == []
    assert slack_client.calls == []
    assert runner._durable_job_cursor_request.receipts == ()
    assert runner._durable_job_slack_request.receipts == ()
    with pytest.raises(DispatchDisabledError):
        DurableJobService(config=handle.config).attempt_dispatch("job-gateway")
    dumped = f"{handle!r} {handle.preflight!r} {raw!r}"
    _assert_no_secrets(dumped)
    assert CURSOR_SECRET_VALUE not in dumped
    assert SLACK_SECRET_VALUE not in dumped


def test_gateway_attach_and_preflight_open_no_sockets_with_request_ports(
    tmp_path, monkeypatch
):
    def _deny(*_a, **_k):
        raise AssertionError("request-port attach must not open sockets")

    monkeypatch.setattr(socket.socket, "connect", _deny)
    monkeypatch.setattr(socket.socket, "connect_ex", _deny)
    _raw, runner = _prepare(tmp_path, monkeypatch)
    ports = _load_ports()
    cursor_client = FakeCursorCloudClient()
    slack_client = FakeSlackClient()
    _install_injected_ports(runner, ports, cursor_client, slack_client)
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is not None
    assert cursor_client.calls == []
    assert slack_client.calls == []


def test_gateway_missing_or_wrong_port_has_zero_effect(tmp_path, monkeypatch):
    ports = _load_ports()
    _raw, runner = _prepare(tmp_path, monkeypatch)
    runner._durable_job_runtime_identity = _matching_identity()
    slack_client = FakeSlackClient()
    runner._durable_job_slack_request = ports.SlackInjectedRequestPort(
        client=slack_client,
        secret_ref=_SECRET_SLACK,
        workspace_id=WORKSPACE,
        channel_id=CHANNEL,
        repository_identity=REPO,
        root_thread_ts=THREAD,
    )
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert slack_client.calls == []

    cursor_client = FakeCursorCloudClient()
    runner._durable_job_cursor_request = SimpleNamespace()
    runner._durable_job_cursor_client = cursor_client
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert cursor_client.calls == []
    assert slack_client.calls == []


def test_gateway_identity_mismatch_does_not_attach_injected_ports(
    tmp_path, monkeypatch
):
    _raw, runner = _prepare(tmp_path, monkeypatch)
    ports = _load_ports()
    cursor_client = FakeCursorCloudClient()
    slack_client = FakeSlackClient()
    _install_injected_ports(runner, ports, cursor_client, slack_client)
    runner._durable_job_runtime_identity = {
        "workspace_id": "T-FOREIGN",
        "repository_identity": REPO,
    }
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert cursor_client.calls == []
    assert slack_client.calls == []


def test_gateway_detach_does_not_close_injected_ports(tmp_path, monkeypatch):
    from gateway.durable_job_lane import get_active_durable_job_lane

    _raw, runner = _prepare(tmp_path, monkeypatch)
    ports = _load_ports()
    cursor_client = FakeCursorCloudClient()
    slack_client = FakeSlackClient()
    _install_injected_ports(runner, ports, cursor_client, slack_client)
    runner._maybe_attach_durable_job_lane()
    handle = runner._durable_job_lane
    assert handle is not None
    port = runner._durable_job_cursor_request
    runner._maybe_detach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert get_active_durable_job_lane() is None
    looked = port(
        operation="lookup",
        secret_ref=_SECRET_CURSOR,
        payload={
            "idempotency_key": "cursor-idem-1",
            "workspace_id": WORKSPACE,
            "repository_identity": REPO,
        },
    )
    assert looked.get("error", {}).get("code") == "not_found"
    port.close()
    with pytest.raises(ports.RequestPortClosed):
        port(
            operation="lookup",
            secret_ref=_SECRET_CURSOR,
            payload={
            "idempotency_key": "cursor-idem-1",
            "workspace_id": WORKSPACE,
            "repository_identity": REPO,
        },
        )
