"""ENG-50: production transport binding seam without activation.

The Gateway startup path must bind only approved concrete
``CursorCloudInjectedTransport`` / ``SlackInjectedTransport`` instances.
Request callables come from an injectable provider-client seam — never from
invented HTTP/SDK clients, duck types, or config flags. Secret material is
reference names only.

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
CONFIG_WORKSPACE = "T1"
CONFIG_REPO = "github.com/example/repo"


@pytest.fixture(autouse=True)
def _reset_lane_seam():
    from gateway.durable_job_lane import detach_durable_job_lane

    detach_durable_job_lane()
    yield
    detach_durable_job_lane()


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
        "policy_version": "eng29-matrix-v1",
        "identity_binding": {
            "workspace_id": CONFIG_WORKSPACE,
            "repository_identity": CONFIG_REPO,
        },
    }
    section.update(overrides)
    return {"durable_jobs": section}


def _idle_request(calls: list):
    def request(*, operation: str, secret_ref: str, payload: dict):
        calls.append(
            {"operation": operation, "secret_ref": secret_ref, "payload": dict(payload)}
        )
        raise AssertionError("attach/preflight must not call the provider")

    return request


def _require_binding():
    try:
        from agent.durable_jobs.production_binding import bind_production_transports
    except ImportError as exc:
        pytest.fail(
            "production binding seam is missing; Gateway startup cannot inject "
            f"approved transports ({exc})"
        )
    return bind_production_transports


def _assert_no_secrets(payload: object) -> None:
    dumped = str(payload)
    assert CURSOR_TOKEN not in dumped
    assert SLACK_TOKEN not in dumped
    assert "xoxb-" not in dumped
    assert "supersecret" not in dumped
    assert SECRET_DSN not in dumped


def test_bind_production_transports_module_exists():
    _require_binding()


def test_missing_request_ports_do_not_bind(tmp_path, monkeypatch):
    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    bound = bind_production_transports(_complete(tmp_path))
    assert bound == {}
    _assert_no_secrets(bound)


def test_single_request_port_does_not_bind(tmp_path, monkeypatch):
    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    calls: list = []
    cursor_only = bind_production_transports(
        _complete(tmp_path),
        cursor_request=_idle_request(calls),
    )
    slack_only = bind_production_transports(
        _complete(tmp_path),
        slack_request=_idle_request(calls),
    )
    assert cursor_only == {}
    assert slack_only == {}
    assert calls == []


def test_wrong_concrete_transport_type_does_not_bind(tmp_path, monkeypatch):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )

    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    calls: list = []

    class DuckCursor:
        _secret_ref = "CURSOR_API_KEY"
        _request = _idle_request(calls)

        def create(self, **_k):
            calls.append("create")

    class UnboundCursorSubclass(CursorCloudInjectedTransport):
        def __init__(self, **_k):
            self._secret_ref = "CURSOR_API_KEY"
            self._request = _idle_request(calls)

    bound_duck = bind_production_transports(
        _complete(tmp_path),
        cursor_transport=DuckCursor(),
        slack_transport=SlackInjectedTransport(
            request=_idle_request(calls), secret_ref="SLACK_BOT_TOKEN"
        ),
        cursor_request=_idle_request(calls),
        slack_request=_idle_request(calls),
    )
    bound_subclass = bind_production_transports(
        _complete(tmp_path),
        cursor_transport=UnboundCursorSubclass(),
        slack_transport=SlackInjectedTransport(
            request=_idle_request(calls), secret_ref="SLACK_BOT_TOKEN"
        ),
        cursor_request=_idle_request(calls),
        slack_request=_idle_request(calls),
    )
    assert bound_duck == {}
    assert bound_subclass == {}
    assert calls == []


def test_secret_ref_mismatch_does_not_bind(tmp_path, monkeypatch):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )

    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    monkeypatch.setenv("ACTUAL_CURSOR_REF_MISSING", "cursor-unbound-dummy-value")
    monkeypatch.setenv("ACTUAL_SLACK_REF_MISSING", "xoxb-unbound-dummy-token")
    calls: list = []
    bound = bind_production_transports(
        _complete(tmp_path),
        cursor_transport=CursorCloudInjectedTransport(
            request=_idle_request(calls), secret_ref="ACTUAL_CURSOR_REF_MISSING"
        ),
        slack_transport=SlackInjectedTransport(
            request=_idle_request(calls), secret_ref="ACTUAL_SLACK_REF_MISSING"
        ),
    )
    assert bound == {}
    assert calls == []
    _assert_no_secrets(bound)


def test_identity_mismatch_does_not_bind(tmp_path, monkeypatch):
    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    calls: list = []
    owner = type("Owner", (), {})()
    owner._durable_job_runtime_identity = {
        "workspace_id": "T-FOREIGN",
        "repository_identity": CONFIG_REPO,
    }
    bound = bind_production_transports(
        _complete(tmp_path),
        owner=owner,
        cursor_request=_idle_request(calls),
        slack_request=_idle_request(calls),
    )
    assert bound == {}
    assert calls == []


def test_default_off_does_not_bind_even_with_request_ports(tmp_path, monkeypatch):
    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    calls: list = []
    bound = bind_production_transports(
        _complete(tmp_path, enabled=False),
        cursor_request=_idle_request(calls),
        slack_request=_idle_request(calls),
    )
    assert bound == {}
    assert calls == []


def test_correct_bound_request_ports_return_approved_transports(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.preflight import preflight_durable_jobs

    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    calls: list = []
    raw = _complete(tmp_path)
    bound = bind_production_transports(
        raw,
        cursor_request=_idle_request(calls),
        slack_request=_idle_request(calls),
    )
    assert type(bound.get("cursor_transport")) is CursorCloudInjectedTransport
    assert type(bound.get("slack_transport")) is SlackInjectedTransport
    assert bound["cursor_transport"].secret_ref == "CURSOR_API_KEY"
    assert bound["slack_transport"].secret_ref == "SLACK_BOT_TOKEN"
    report = preflight_durable_jobs(raw, **bound)
    assert report.runtime_ready is True
    assert report.dispatch_allowed is False
    assert calls == []
    _assert_no_secrets(bound)
    _assert_no_secrets(report)


def test_bind_and_preflight_open_no_sockets_or_provider_calls(
    tmp_path, monkeypatch
):
    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)

    def _deny(*_a, **_k):
        raise AssertionError("production binding must not open sockets")

    monkeypatch.setattr(socket.socket, "connect", _deny)
    monkeypatch.setattr(socket.socket, "connect_ex", _deny)
    calls: list = []
    bound = bind_production_transports(
        _complete(tmp_path),
        cursor_request=_idle_request(calls),
        slack_request=_idle_request(calls),
    )
    assert bound
    assert calls == []


def test_binding_does_not_import_provider_sdks(tmp_path, monkeypatch):
    for name in ("psycopg", "slack_sdk", "slack_bolt", "httpx"):
        sys.modules.pop(name, None)
    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    bind_production_transports(
        _complete(tmp_path),
        cursor_request=_idle_request([]),
        slack_request=_idle_request([]),
    )
    assert "psycopg" not in sys.modules
    assert "slack_sdk" not in sys.modules
    assert "slack_bolt" not in sys.modules


def test_flags_do_not_invent_a_request_callable(tmp_path, monkeypatch):
    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    bound = bind_production_transports(
        _complete(tmp_path, dispatch_enabled=True)
    )
    assert bound == {}
    assert "cursor_transport" not in bound
    assert "slack_transport" not in bound
