"""ENG-36: attach requires truthfully bound runtime transport capability.

``attach_durable_job_lane`` must not return a handle or construct adapters
when ``runtime_ready`` is false. Missing/mismatched secret refs, missing
resolver/request, metadata-only ducks, and self-attested subclasses are
fail-closed. Matching approved concrete transports may attach. Default-off
remains. Preflight/attach must not invoke the transport or open sockets.

No live Slack/Cursor/network. PostgreSQL is not imported.
"""

from __future__ import annotations

import socket
import sys
from pathlib import Path

import pytest


CURSOR_TOKEN = "cursor-secret-token-value"
SLACK_TOKEN = "xoxb-super-secret-token"
SECRET_DSN = "postgresql://hermes:supersecret@127.0.0.1:5432/durable_jobs"


@pytest.fixture(autouse=True)
def _reset_lane_seam():
    from gateway.durable_job_lane import detach_durable_job_lane

    detach_durable_job_lane()
    yield
    detach_durable_job_lane()


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


def _idle_request(calls: list):
    def request(**kwargs):
        calls.append(dict(kwargs))
        raise AssertionError("preflight/attach must not call the transport")

    return request


def _spy_adapter_factories(monkeypatch, cursor_calls: list, slack_calls: list):
    import agent.durable_jobs.cursor_cloud as cursor_cloud
    import agent.durable_jobs.slack_bridge as slack_bridge

    real_cursor = cursor_cloud.adapter_from_config
    real_slack = slack_bridge.adapter_from_config

    def _cursor(*args, **kwargs):
        cursor_calls.append({"args": args, "kwargs": kwargs})
        return real_cursor(*args, **kwargs)

    def _slack(*args, **kwargs):
        slack_calls.append({"args": args, "kwargs": kwargs})
        return real_slack(*args, **kwargs)

    monkeypatch.setattr(cursor_cloud, "adapter_from_config", _cursor)
    monkeypatch.setattr(
        "gateway.durable_job_lane.cursor_adapter_from_config", _cursor
    )
    monkeypatch.setattr(slack_bridge, "adapter_from_config", _slack)
    monkeypatch.setattr(
        "gateway.durable_job_lane.slack_adapter_from_config", _slack
    )


def _assert_no_secrets(payload: object) -> None:
    dumped = str(payload)
    assert CURSOR_TOKEN not in dumped
    assert SLACK_TOKEN not in dumped
    assert "xoxb-" not in dumped
    assert "supersecret" not in dumped
    assert SECRET_DSN not in dumped


def _assert_unattached(handle, transport_calls, cursor_calls, slack_calls):
    from gateway.durable_job_lane import get_active_durable_job_lane

    assert handle is None
    assert get_active_durable_job_lane() is None
    assert transport_calls == []
    assert cursor_calls == []
    assert slack_calls == []


def test_attach_disabled_has_no_handle_or_adapter_calls(tmp_path, monkeypatch):
    from gateway.durable_job_lane import attach_durable_job_lane

    transport_calls: list = []
    cursor_calls: list = []
    slack_calls: list = []
    _spy_adapter_factories(monkeypatch, cursor_calls, slack_calls)
    handle = attach_durable_job_lane(raw_config=_complete(tmp_path, enabled=False))
    _assert_unattached(handle, transport_calls, cursor_calls, slack_calls)


@pytest.mark.parametrize(
    "drop_key",
    [
        "cursor_adapter_mode",
        "slack_adapter_mode",
        "cursor_secret_ref",
        "slack_secret_ref",
        "policy_version",
        "identity_binding",
        "sqlite_path",
        "checkpoint_sqlite_path",
    ],
)
def test_attach_partial_config_has_no_handle_or_adapter_calls(
    tmp_path, monkeypatch, drop_key
):
    from gateway.durable_job_lane import attach_durable_job_lane

    transport_calls: list = []
    cursor_calls: list = []
    slack_calls: list = []
    _spy_adapter_factories(monkeypatch, cursor_calls, slack_calls)
    raw = _complete(tmp_path)
    raw["durable_jobs"].pop(drop_key, None)
    handle = attach_durable_job_lane(raw_config=raw)
    _assert_unattached(handle, transport_calls, cursor_calls, slack_calls)


def test_attach_missing_secret_env_does_not_wire_matching_transports(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.preflight import preflight_durable_jobs
    from gateway.durable_job_lane import attach_durable_job_lane

    monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    transport_calls: list = []
    cursor_calls: list = []
    slack_calls: list = []
    _spy_adapter_factories(monkeypatch, cursor_calls, slack_calls)
    raw = _complete(tmp_path)
    cursor = CursorCloudInjectedTransport(
        request=_idle_request(transport_calls), secret_ref="CURSOR_API_KEY"
    )
    slack = SlackInjectedTransport(
        request=_idle_request(transport_calls), secret_ref="SLACK_BOT_TOKEN"
    )
    report = preflight_durable_jobs(
        raw, cursor_transport=cursor, slack_transport=slack
    )
    assert report.constructible is True
    assert report.runtime_ready is False
    handle = attach_durable_job_lane(
        raw_config=raw, cursor_transport=cursor, slack_transport=slack
    )
    _assert_unattached(handle, transport_calls, cursor_calls, slack_calls)
    _assert_no_secrets(report)


def test_attach_mismatched_transport_secret_refs_does_not_wire(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.preflight import preflight_durable_jobs
    from gateway.durable_job_lane import attach_durable_job_lane

    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    monkeypatch.setenv("ACTUAL_CURSOR_REF_MISSING", "cursor-unbound-dummy-value")
    monkeypatch.setenv("ACTUAL_SLACK_REF_MISSING", "xoxb-unbound-dummy-token")
    transport_calls: list = []
    cursor_calls: list = []
    slack_calls: list = []
    _spy_adapter_factories(monkeypatch, cursor_calls, slack_calls)
    raw = _complete(tmp_path)
    cursor = CursorCloudInjectedTransport(
        request=_idle_request(transport_calls),
        secret_ref="ACTUAL_CURSOR_REF_MISSING",
    )
    slack = SlackInjectedTransport(
        request=_idle_request(transport_calls),
        secret_ref="ACTUAL_SLACK_REF_MISSING",
    )
    report = preflight_durable_jobs(
        raw, cursor_transport=cursor, slack_transport=slack
    )
    assert report.constructible is True
    assert report.runtime_ready is False
    assert "transport_secret_ref_mismatch" in report.reasons
    handle = attach_durable_job_lane(
        raw_config=raw, cursor_transport=cursor, slack_transport=slack
    )
    _assert_unattached(handle, transport_calls, cursor_calls, slack_calls)
    _assert_no_secrets(report)
    _assert_no_secrets(handle)


def test_attach_missing_request_resolver_does_not_wire(tmp_path, monkeypatch):
    from agent.durable_jobs.preflight import preflight_durable_jobs
    from gateway.durable_job_lane import attach_durable_job_lane

    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    transport_calls: list = []
    cursor_calls: list = []
    slack_calls: list = []
    _spy_adapter_factories(monkeypatch, cursor_calls, slack_calls)

    class MissingResolverCursor:
        _secret_ref = "CURSOR_API_KEY"
        secret_ref = "CURSOR_API_KEY"
        _request = None

        def create(self, **_k):
            transport_calls.append("create")

        def lookup(self, **_k):
            transport_calls.append("lookup")

        def status(self, **_k):
            transport_calls.append("status")

    class MissingResolverSlack:
        _secret_ref = "SLACK_BOT_TOKEN"
        secret_ref = "SLACK_BOT_TOKEN"
        _request = "not-callable"

        def post_root(self, **_k):
            transport_calls.append("post_root")

        def lookup_by_client_msg_id(self, client_msg_id: str):
            transport_calls.append(client_msg_id)

    raw = _complete(tmp_path)
    cursor = MissingResolverCursor()
    slack = MissingResolverSlack()
    report = preflight_durable_jobs(
        raw, cursor_transport=cursor, slack_transport=slack
    )
    assert report.constructible is True
    assert report.runtime_ready is False
    handle = attach_durable_job_lane(
        raw_config=raw, cursor_transport=cursor, slack_transport=slack
    )
    _assert_unattached(handle, transport_calls, cursor_calls, slack_calls)


def test_attach_metadata_only_duck_transports_do_not_wire(tmp_path, monkeypatch):
    from agent.durable_jobs.preflight import preflight_durable_jobs
    from gateway.durable_job_lane import attach_durable_job_lane

    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    transport_calls: list = []
    cursor_calls: list = []
    slack_calls: list = []
    _spy_adapter_factories(monkeypatch, cursor_calls, slack_calls)

    class MetadataOnlyCursorTransport:
        secret_ref = "CURSOR_API_KEY"
        _secret_ref = "CURSOR_API_KEY"

        def create(self, **_k):
            transport_calls.append("create")

        def lookup(self, **_k):
            transport_calls.append("lookup")

        def status(self, **_k):
            transport_calls.append("status")

    class MetadataOnlySlackTransport:
        secret_ref = "SLACK_BOT_TOKEN"
        _secret_ref = "SLACK_BOT_TOKEN"

        def post_root(self, **_k):
            transport_calls.append("post_root")

        def lookup_by_client_msg_id(self, client_msg_id: str):
            transport_calls.append(client_msg_id)

    raw = _complete(tmp_path)
    report = preflight_durable_jobs(
        raw,
        cursor_transport=MetadataOnlyCursorTransport(),
        slack_transport=MetadataOnlySlackTransport(),
    )
    assert report.constructible is True
    assert report.runtime_ready is False
    handle = attach_durable_job_lane(
        raw_config=raw,
        cursor_transport=MetadataOnlyCursorTransport(),
        slack_transport=MetadataOnlySlackTransport(),
    )
    _assert_unattached(handle, transport_calls, cursor_calls, slack_calls)
    _assert_no_secrets(report)


def test_attach_self_attested_subclass_does_not_wire(tmp_path, monkeypatch):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.preflight import preflight_durable_jobs
    from gateway.durable_job_lane import attach_durable_job_lane

    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    transport_calls: list = []
    cursor_calls: list = []
    slack_calls: list = []
    _spy_adapter_factories(monkeypatch, cursor_calls, slack_calls)

    class UnboundCursorSubclass(CursorCloudInjectedTransport):
        def __init__(self, **_k):
            self._secret_ref = "CURSOR_API_KEY"

        @property
        def secret_ref(self) -> str:
            return "CURSOR_API_KEY"

        def can_resolve_secret_ref(self) -> bool:
            return True

        def create(self, **_k):
            transport_calls.append("create")

    class UnboundSlackSubclass(SlackInjectedTransport):
        def __init__(self, **_k):
            self._secret_ref = "SLACK_BOT_TOKEN"

        @property
        def secret_ref(self) -> str:
            return "SLACK_BOT_TOKEN"

        def can_resolve_secret_ref(self) -> bool:
            return True

        def post_root(self, **_k):
            transport_calls.append("post_root")

    raw = _complete(tmp_path)
    cursor = UnboundCursorSubclass()
    slack = UnboundSlackSubclass()
    assert cursor.can_resolve_secret_ref() is True
    assert slack.can_resolve_secret_ref() is True
    report = preflight_durable_jobs(
        raw, cursor_transport=cursor, slack_transport=slack
    )
    assert report.constructible is True
    assert report.runtime_ready is False
    handle = attach_durable_job_lane(
        raw_config=raw, cursor_transport=cursor, slack_transport=slack
    )
    _assert_unattached(handle, transport_calls, cursor_calls, slack_calls)


def test_attach_matching_approved_concrete_transports_wires_without_calling(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.cursor_cloud import CursorCloudAdapter
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.preflight import preflight_durable_jobs
    from agent.durable_jobs.slack_bridge import SlackClientBridge
    from gateway.durable_job_lane import (
        attach_durable_job_lane,
        get_active_durable_job_lane,
    )

    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    transport_calls: list = []
    cursor_calls: list = []
    slack_calls: list = []
    _spy_adapter_factories(monkeypatch, cursor_calls, slack_calls)
    raw = _complete(tmp_path)
    cursor = CursorCloudInjectedTransport(
        request=_idle_request(transport_calls), secret_ref="CURSOR_API_KEY"
    )
    slack = SlackInjectedTransport(
        request=_idle_request(transport_calls), secret_ref="SLACK_BOT_TOKEN"
    )
    report = preflight_durable_jobs(
        raw, cursor_transport=cursor, slack_transport=slack
    )
    assert report.runtime_ready is True
    handle = attach_durable_job_lane(
        raw_config=raw, cursor_transport=cursor, slack_transport=slack
    )
    assert handle is not None
    assert get_active_durable_job_lane() is handle
    assert isinstance(handle.cursor_adapter, CursorCloudAdapter)
    assert isinstance(handle.slack_adapter, SlackClientBridge)
    assert transport_calls == []
    assert cursor_calls
    assert slack_calls
    _assert_no_secrets(handle)
    _assert_no_secrets(report)


def test_attach_and_preflight_open_no_sockets_even_when_unready(
    tmp_path, monkeypatch
):
    from gateway.durable_job_lane import attach_durable_job_lane

    def _deny(*_a, **_k):
        raise AssertionError("durable job lane must not open sockets")

    monkeypatch.setattr(socket.socket, "connect", _deny)
    monkeypatch.setattr(socket.socket, "connect_ex", _deny)
    handle = attach_durable_job_lane(raw_config=_complete(tmp_path))
    assert handle is None


def test_failed_attach_does_not_occupy_owner_slot(tmp_path, monkeypatch):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from gateway.durable_job_lane import attach_durable_job_lane

    raw = _complete(tmp_path)
    first = attach_durable_job_lane(raw_config=raw)
    assert first is None
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    calls: list = []
    handle = attach_durable_job_lane(
        raw_config=raw,
        cursor_transport=CursorCloudInjectedTransport(
            request=_idle_request(calls), secret_ref="CURSOR_API_KEY"
        ),
        slack_transport=SlackInjectedTransport(
            request=_idle_request(calls), secret_ref="SLACK_BOT_TOKEN"
        ),
    )
    assert handle is not None
    assert calls == []


def test_attach_does_not_import_psycopg_on_sqlite_path(tmp_path, monkeypatch):
    import types

    from gateway.durable_job_lane import attach_durable_job_lane

    fake = types.ModuleType("psycopg")
    fake.connect = lambda *a, **k: (_ for _ in ()).throw(
        AssertionError("psycopg.connect")
    )
    monkeypatch.setitem(sys.modules, "psycopg", fake)
    handle = attach_durable_job_lane(raw_config=_complete(tmp_path))
    assert handle is None
    assert "psycopg" not in sys.modules or sys.modules["psycopg"] is fake
