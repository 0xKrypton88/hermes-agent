"""ENG-36 Package 2 — fail-closed gates, preflight, and injected transports.

No live Slack/Cursor/network. Secret values must never appear in status.
"""

from __future__ import annotations

import socket
import sys
from pathlib import Path

import pytest


SECRET_DSN = "postgresql://hermes:supersecret@127.0.0.1:5432/durable_jobs"
CURSOR_TOKEN = "cursor-secret-token-value"
SLACK_TOKEN = "xoxb-super-secret-token"


def _complete_sqlite(tmp_path: Path, **overrides) -> dict:
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


def test_default_config_root_keeps_durable_jobs_disabled():
    from hermes_cli.config_defaults import DEFAULT_CONFIG

    section = DEFAULT_CONFIG["durable_jobs"]
    assert section["enabled"] is False
    assert section["dispatch_enabled"] is False
    assert section.get("cursor_adapter_mode") in (None, "")
    assert section.get("slack_adapter_mode") in (None, "")


def test_defaults_keep_enabled_and_dispatch_off():
    from agent.durable_jobs.config import (
        DEFAULT_DURABLE_JOBS_CONFIG,
        load_durable_jobs_config,
    )

    assert DEFAULT_DURABLE_JOBS_CONFIG["enabled"] is False
    assert DEFAULT_DURABLE_JOBS_CONFIG["dispatch_enabled"] is False
    assert DEFAULT_DURABLE_JOBS_CONFIG.get("cursor_adapter_mode") in (None, "")
    assert DEFAULT_DURABLE_JOBS_CONFIG.get("slack_adapter_mode") in (None, "")

    cfg = load_durable_jobs_config({})
    assert cfg.enabled is False
    assert cfg.dispatch_enabled is False
    assert cfg.dispatch_allowed is False


def test_flags_alone_cannot_allow_dispatch(tmp_path):
    from agent.durable_jobs.config import load_durable_jobs_config

    cfg = load_durable_jobs_config(
        {
            "durable_jobs": {
                "enabled": True,
                "dispatch_enabled": True,
                "sqlite_path": str(tmp_path / "jobs.sqlite"),
                "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
            }
        }
    )
    assert cfg.enabled is True
    assert cfg.dispatch_enabled is True
    assert cfg.dispatch_allowed is False


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
        "dispatch_enabled",
        "enabled",
    ],
)
def test_partial_config_keeps_dispatch_closed(tmp_path, drop_key):
    from agent.durable_jobs.config import load_durable_jobs_config

    raw = _complete_sqlite(tmp_path)
    if drop_key in ("enabled", "dispatch_enabled"):
        raw["durable_jobs"][drop_key] = False
    else:
        raw["durable_jobs"].pop(drop_key, None)
    cfg = load_durable_jobs_config(raw)
    assert cfg.dispatch_allowed is False


def test_unknown_adapter_mode_is_rejected(tmp_path):
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config(
            _complete_sqlite(tmp_path, cursor_adapter_mode="live")
        )
    msg = str(exc.value).lower()
    assert "adapter" in msg
    assert "live" in msg or "unknown" in msg or "injected" in msg


def test_secret_like_ref_values_are_rejected_and_redacted(tmp_path):
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config(
            _complete_sqlite(tmp_path, slack_secret_ref=SLACK_TOKEN)
        )
    dumped = str(exc.value)
    assert SLACK_TOKEN not in dumped
    assert "xoxb-" not in dumped


def test_partial_identity_binding_is_rejected(tmp_path):
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError):
        load_durable_jobs_config(
            _complete_sqlite(
                tmp_path,
                identity_binding={"workspace_id": "T1"},
            )
        )


def test_complete_sqlite_gates_allow_dispatch_flag_only(tmp_path):
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.service import DispatchDisabledError, DurableJobService

    cfg = load_durable_jobs_config(_complete_sqlite(tmp_path))
    assert cfg.dispatch_allowed is True

    calls: list[str] = []

    class FakeDispatch:
        def dispatch(self, job_id: str) -> None:
            calls.append(job_id)

    service = DurableJobService(config=cfg, dispatch_adapter=FakeDispatch())
    with pytest.raises(DispatchDisabledError):
        service.attempt_dispatch("job-not-activated")
    assert calls == []


def test_postgresql_complete_config_does_not_allow_lane_dispatch(tmp_path):
    from agent.durable_jobs.config import load_durable_jobs_config

    cfg = load_durable_jobs_config(
        {
            "durable_jobs": {
                "enabled": True,
                "dispatch_enabled": True,
                "backend": "postgresql",
                "postgres_dsn": SECRET_DSN,
                "postgres_schema": "durable_jobs_app",
                "checkpoint_postgres_dsn": (
                    "postgresql://hermes:supersecret@127.0.0.1:5432/other_jobs"
                ),
                "checkpoint_postgres_schema": "durable_jobs_ckpt",
                "postgres_storage_id": "durable_app",
                "checkpoint_postgres_storage_id": "durable_ckpt",
                "postgres_environment_id": "test",
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
        }
    )
    assert cfg.dispatch_allowed is False
    assert "supersecret" not in repr(cfg)


def test_preflight_postgres_storage_only_needs_no_provider_bindings():
    from agent.durable_jobs.preflight import preflight_durable_jobs

    report = preflight_durable_jobs(
        {
            "durable_jobs": {
                "enabled": True,
                "dispatch_enabled": False,
                "backend": "postgresql",
                "postgres_dsn": SECRET_DSN,
                "postgres_schema": "durable_jobs_app",
                "checkpoint_postgres_dsn": SECRET_DSN,
                "checkpoint_postgres_schema": "durable_jobs_ckpt",
                "postgres_storage_id": "durable_app",
                "checkpoint_postgres_storage_id": "durable_ckpt",
                "postgres_environment_id": "test",
            }
        }
    )

    assert report.constructible is True
    assert report.dispatch_allowed is False
    assert report.runtime_ready is False
    assert report.reasons == ()
    assert report.cursor_adapter_mode is None
    assert report.slack_adapter_mode is None
    assert report.secret_refs_configured is False
    assert report.secret_refs_present is False
    assert report.transport_capability is False


def test_preflight_default_off_has_no_external_effects(monkeypatch):
    from agent.durable_jobs.preflight import preflight_durable_jobs

    def _deny(*_a, **_k):
        raise AssertionError("preflight must not open sockets")

    monkeypatch.setattr(socket.socket, "connect", _deny)
    monkeypatch.setattr(socket.socket, "connect_ex", _deny)

    report = preflight_durable_jobs({})
    assert report.constructible is False
    assert report.dispatch_allowed is False
    assert report.runtime_ready is False
    dumped = str(report)
    assert "supersecret" not in dumped
    assert CURSOR_TOKEN not in dumped
    assert SLACK_TOKEN not in dumped


def test_preflight_complete_sqlite_is_constructible_without_sockets(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.preflight import preflight_durable_jobs

    def _deny(*_a, **_k):
        raise AssertionError("preflight must not open sockets")

    monkeypatch.setattr(socket.socket, "connect", _deny)
    monkeypatch.setattr(socket.socket, "connect_ex", _deny)
    monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)

    report = preflight_durable_jobs(_complete_sqlite(tmp_path))
    assert report.constructible is True
    assert report.dispatch_allowed is False
    assert report.runtime_ready is False
    assert report.secret_refs_present is False
    assert "secret_refs_missing" in report.reasons
    assert report.cursor_adapter_mode == "injected"
    assert report.slack_adapter_mode == "injected"
    dumped = str(report)
    assert "supersecret" not in dumped
    assert CURSOR_TOKEN not in dumped
    assert SLACK_TOKEN not in dumped


def test_preflight_runtime_ready_requires_both_injected_secret_refs(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.preflight import preflight_durable_jobs

    raw = _complete_sqlite(tmp_path)
    monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)

    missing_both = preflight_durable_jobs(raw)
    assert missing_both.constructible is True
    assert missing_both.dispatch_allowed is False
    assert missing_both.runtime_ready is False
    assert missing_both.secret_refs_present is False
    assert "secret_refs_missing" in missing_both.reasons

    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    missing_slack = preflight_durable_jobs(raw)
    assert missing_slack.constructible is True
    assert missing_slack.dispatch_allowed is False
    assert missing_slack.runtime_ready is False
    assert missing_slack.secret_refs_present is False
    assert "secret_refs_missing" in missing_slack.reasons
    assert CURSOR_TOKEN not in str(missing_slack)
    assert SLACK_TOKEN not in str(missing_slack)

    monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    missing_cursor = preflight_durable_jobs(raw)
    assert missing_cursor.constructible is True
    assert missing_cursor.dispatch_allowed is False
    assert missing_cursor.runtime_ready is False
    assert missing_cursor.secret_refs_present is False
    assert "secret_refs_missing" in missing_cursor.reasons
    assert CURSOR_TOKEN not in str(missing_cursor)
    assert SLACK_TOKEN not in str(missing_cursor)
    assert "xoxb-" not in str(missing_cursor)

    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    env_only = preflight_durable_jobs(raw)
    assert env_only.constructible is True
    assert env_only.secret_refs_present is False
    assert env_only.runtime_ready is False
    assert env_only.dispatch_allowed is False
    assert "secret_refs_missing" in env_only.reasons
    assert "transport_capability_missing" in env_only.reasons
    dumped = str(env_only)
    assert CURSOR_TOKEN not in dumped
    assert SLACK_TOKEN not in dumped
    assert "xoxb-" not in dumped
    assert "supersecret" not in dumped


def _idle_injected_transports():
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.request_ports import (
        CursorCloudInjectedRequestPort,
        SlackInjectedRequestPort,
    )

    def _idle(*_a, **_k):
        raise AssertionError("preflight/transport must not call the network")

    class _CursorClient:
        create_agent = _idle
        get_agent = _idle
        get_run = _idle

    class _SlackClient:
        chat_postMessage = _idle
        conversations_replies = _idle

    resolve = lambda _ref: "explicit-test-credential"
    cursor_request = CursorCloudInjectedRequestPort(
        client=_CursorClient(),
        secret_ref="CURSOR_API_KEY",
        workspace_id="T1",
        repository_identity="github.com/example/repo",
        credential_resolver=resolve,
    )
    slack_request = SlackInjectedRequestPort(
        client=_SlackClient(),
        secret_ref="SLACK_BOT_TOKEN",
        workspace_id="T1",
        repository_identity="github.com/example/repo",
        channel_id="C1",
        root_thread_ts="1.000000",
        credential_resolver=resolve,
    )

    return (
        CursorCloudInjectedTransport(
            request=cursor_request, secret_ref="CURSOR_API_KEY"
        ),
        SlackInjectedTransport(request=slack_request, secret_ref="SLACK_BOT_TOKEN"),
    )


def test_preflight_runtime_ready_requires_injected_transport_capability(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.preflight import preflight_durable_jobs

    raw = _complete_sqlite(tmp_path)
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    cursor, slack = _idle_injected_transports()
    ready = preflight_durable_jobs(
        raw, cursor_transport=cursor, slack_transport=slack
    )
    assert ready.constructible is True
    assert ready.secret_refs_present is True
    assert ready.transport_capability is True
    assert ready.runtime_ready is True
    assert ready.dispatch_allowed is True
    assert "secret_refs_missing" not in ready.reasons
    assert "transport_capability_missing" not in ready.reasons
    dumped = str(ready)
    assert CURSOR_TOKEN not in dumped
    assert SLACK_TOKEN not in dumped
    assert "xoxb-" not in dumped

    cursor_only = preflight_durable_jobs(
        raw, cursor_transport=cursor, slack_transport=None
    )
    assert cursor_only.constructible is True
    assert cursor_only.runtime_ready is False
    assert cursor_only.dispatch_allowed is False
    assert "transport_capability_missing" in cursor_only.reasons


def test_preflight_runtime_ready_requires_transport_secret_ref_binding(
    tmp_path, monkeypatch
):
    """Config env values must not make runtime_ready if transport refs differ."""
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.preflight import preflight_durable_jobs

    raw = _complete_sqlite(tmp_path)
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    monkeypatch.delenv("ACTUAL_CURSOR_REF_MISSING", raising=False)
    monkeypatch.delenv("ACTUAL_SLACK_REF_MISSING", raising=False)

    def _idle(**_k):
        raise AssertionError("preflight/transport must not call the network")

    mismatched = preflight_durable_jobs(
        raw,
        cursor_transport=CursorCloudInjectedTransport(
            request=_idle, secret_ref="ACTUAL_CURSOR_REF_MISSING"
        ),
        slack_transport=SlackInjectedTransport(
            request=_idle, secret_ref="ACTUAL_SLACK_REF_MISSING"
        ),
    )
    assert mismatched.constructible is True
    assert mismatched.runtime_ready is False
    assert mismatched.dispatch_allowed is False
    assert "transport_secret_ref_mismatch" in mismatched.reasons
    dumped = str(mismatched)
    assert CURSOR_TOKEN not in dumped
    assert SLACK_TOKEN not in dumped
    assert "xoxb-" not in dumped
    assert "supersecret" not in dumped

    other_cursor = "cursor-unbound-dummy-value"
    other_slack = "xoxb-unbound-dummy-token"
    monkeypatch.setenv("ACTUAL_CURSOR_REF_MISSING", other_cursor)
    monkeypatch.setenv("ACTUAL_SLACK_REF_MISSING", other_slack)
    still_unbound = preflight_durable_jobs(
        raw,
        cursor_transport=CursorCloudInjectedTransport(
            request=_idle, secret_ref="ACTUAL_CURSOR_REF_MISSING"
        ),
        slack_transport=SlackInjectedTransport(
            request=_idle, secret_ref="ACTUAL_SLACK_REF_MISSING"
        ),
    )
    assert still_unbound.runtime_ready is False
    assert still_unbound.dispatch_allowed is False
    assert "transport_secret_ref_mismatch" in still_unbound.reasons
    dumped = str(still_unbound)
    assert CURSOR_TOKEN not in dumped
    assert SLACK_TOKEN not in dumped
    assert other_cursor not in dumped
    assert other_slack not in dumped
    assert "xoxb-" not in dumped


@pytest.mark.parametrize(
    "case",
    (
        "missing_secrets",
        "missing_transport",
        "binding_mismatch",
        "complete_runtime",
    ),
)
def test_preflight_dispatch_allowed_matrix_requires_verified_runtime(
    tmp_path, monkeypatch, case
):
    """dispatch_allowed is true only for a complete verified production runtime."""
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.preflight import preflight_durable_jobs

    raw = _complete_sqlite(tmp_path)

    def _idle(**_k):
        raise AssertionError("preflight/transport must not call the network")

    if case == "missing_secrets":
        monkeypatch.delenv("CURSOR_API_KEY", raising=False)
        monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
        report = preflight_durable_jobs(raw)
        assert report.constructible is True
        assert report.secret_refs_present is False
        assert report.runtime_ready is False
        assert report.dispatch_allowed is False
        assert "secret_refs_missing" in report.reasons
        return

    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)

    if case == "missing_transport":
        report = preflight_durable_jobs(raw)
        assert report.constructible is True
        assert report.secret_refs_present is False
        assert report.transport_capability is False
        assert report.runtime_ready is False
        assert report.dispatch_allowed is False
        assert "transport_capability_missing" in report.reasons
        return

    if case == "binding_mismatch":
        report = preflight_durable_jobs(
            raw,
            cursor_transport=CursorCloudInjectedTransport(
                request=_idle, secret_ref="ACTUAL_CURSOR_REF_MISSING"
            ),
            slack_transport=SlackInjectedTransport(
                request=_idle, secret_ref="ACTUAL_SLACK_REF_MISSING"
            ),
        )
        assert report.constructible is True
        assert report.transport_capability is True
        assert report.runtime_ready is False
        assert report.dispatch_allowed is False
        assert "transport_secret_ref_mismatch" in report.reasons
        return

    cursor, slack = _idle_injected_transports()
    report = preflight_durable_jobs(
        raw, cursor_transport=cursor, slack_transport=slack
    )
    assert report.constructible is True
    assert report.secret_refs_present is True
    assert report.transport_capability is True
    assert report.runtime_ready is True
    assert report.dispatch_allowed is True
    assert CURSOR_TOKEN not in str(report)
    assert SLACK_TOKEN not in str(report)


def test_preflight_runtime_ready_rejects_metadata_only_duck_transports(
    tmp_path, monkeypatch
):
    """Matching method names + public secret_ref strings are not capability."""
    from agent.durable_jobs.preflight import preflight_durable_jobs

    class MetadataOnlyCursorTransport:
        secret_ref = "CURSOR_API_KEY"
        _secret_ref = "CURSOR_API_KEY"

        def create(self, **_k):
            return None

        def lookup(self, **_k):
            return None

        def status(self, **_k):
            return None

    class MetadataOnlySlackTransport:
        secret_ref = "SLACK_BOT_TOKEN"
        _secret_ref = "SLACK_BOT_TOKEN"

        def post_root(self, **_k):
            return None

        def lookup_by_client_msg_id(self, client_msg_id: str):
            return None

    raw = _complete_sqlite(tmp_path)
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)

    report = preflight_durable_jobs(
        raw,
        cursor_transport=MetadataOnlyCursorTransport(),
        slack_transport=MetadataOnlySlackTransport(),
    )
    assert report.constructible is True
    assert report.secret_refs_present is False
    assert report.transport_capability is False
    assert report.runtime_ready is False
    assert report.dispatch_allowed is False
    assert "transport_capability_missing" in report.reasons
    assert "transport_secret_ref_mismatch" not in report.reasons
    dumped = str(report)
    assert CURSOR_TOKEN not in dumped
    assert SLACK_TOKEN not in dumped
    assert "xoxb-" not in dumped
    assert "supersecret" not in dumped
    for reason in report.reasons:
        assert "CURSOR_API_KEY" not in reason
        assert "SLACK_BOT_TOKEN" not in reason
        assert CURSOR_TOKEN not in reason
        assert SLACK_TOKEN not in reason


def test_preflight_runtime_ready_rejects_unbound_subclass_transports(
    tmp_path, monkeypatch
):
    """Subclasses must not spoof capability by overriding can_resolve_secret_ref."""
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.preflight import preflight_durable_jobs

    class UnboundCursorSubclass(CursorCloudInjectedTransport):
        def __init__(self, **_k):
            self._secret_ref = "CURSOR_API_KEY"

        @property
        def secret_ref(self) -> str:
            return "CURSOR_API_KEY"

        def can_resolve_secret_ref(self) -> bool:
            return True

    class UnboundSlackSubclass(SlackInjectedTransport):
        def __init__(self, **_k):
            self._secret_ref = "SLACK_BOT_TOKEN"

        @property
        def secret_ref(self) -> str:
            return "SLACK_BOT_TOKEN"

        def can_resolve_secret_ref(self) -> bool:
            return True

    raw = _complete_sqlite(tmp_path)
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)

    cursor = UnboundCursorSubclass()
    slack = UnboundSlackSubclass()
    assert isinstance(cursor, CursorCloudInjectedTransport)
    assert isinstance(slack, SlackInjectedTransport)
    assert cursor.can_resolve_secret_ref() is True
    assert slack.can_resolve_secret_ref() is True
    assert not hasattr(cursor, "_request")
    assert not hasattr(slack, "_request")

    report = preflight_durable_jobs(
        raw, cursor_transport=cursor, slack_transport=slack
    )
    assert report.constructible is True
    assert report.secret_refs_present is False
    assert report.transport_capability is False
    assert report.runtime_ready is False
    assert report.dispatch_allowed is False
    assert "transport_capability_missing" in report.reasons
    dumped = str(report)
    assert CURSOR_TOKEN not in dumped
    assert SLACK_TOKEN not in dumped
    assert "xoxb-" not in dumped
    assert "supersecret" not in dumped
    for reason in report.reasons:
        assert "CURSOR_API_KEY" not in reason
        assert "SLACK_BOT_TOKEN" not in reason
        assert CURSOR_TOKEN not in reason
        assert SLACK_TOKEN not in reason


def test_preflight_does_not_import_psycopg_on_sqlite_path(tmp_path, monkeypatch):
    import types

    from agent.durable_jobs.preflight import preflight_durable_jobs

    def _boom(_name):
        raise AssertionError("psycopg must not be imported on sqlite preflight")

    fake = types.ModuleType("psycopg")
    fake.connect = lambda *a, **k: (_ for _ in ()).throw(
        AssertionError("psycopg.connect")
    )
    monkeypatch.setitem(sys.modules, "psycopg", fake)
    monkeypatch.setattr(
        "importlib.import_module",
        lambda name, *a, **k: _boom(name) if name == "psycopg" else __import__(name),
    )

    report = preflight_durable_jobs(_complete_sqlite(tmp_path))
    assert report.constructible is True
    assert "psycopg" not in sys.modules or sys.modules["psycopg"] is fake


def test_adapter_from_config_never_mints_live_client_when_dispatch_allowed(
    tmp_path,
):
    from agent.durable_jobs.adapters import NullCursorProvider, NullSlackPort
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.cursor_cloud import (
        CursorCloudAdapter,
        adapter_from_config as cursor_from_config,
    )
    from agent.durable_jobs.slack_bridge import (
        SlackClientBridge,
        adapter_from_config as slack_from_config,
    )

    cfg = load_durable_jobs_config(_complete_sqlite(tmp_path))
    assert cfg.dispatch_allowed is True
    assert isinstance(cursor_from_config(cfg), NullCursorProvider)
    assert isinstance(slack_from_config(cfg), NullSlackPort)

    class _CursorTransport:
        def create(self, **_k):
            raise AssertionError("no network")

        def lookup(self, **_k):
            raise AssertionError("no network")

        def status(self, **_k):
            raise AssertionError("no network")

    class _SlackTransport:
        def post_root(self, **_k):
            raise AssertionError("no network")

        def lookup_by_client_msg_id(self, client_msg_id: str):
            raise AssertionError("no network")

    assert isinstance(
        cursor_from_config(cfg, transport=_CursorTransport()), CursorCloudAdapter
    )
    assert isinstance(
        slack_from_config(cfg, transport=_SlackTransport()), SlackClientBridge
    )


def test_injected_transports_are_production_shaped_and_secret_ref_only():
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )

    calls: list[dict] = []

    def request(*, operation: str, secret_ref: str, payload: dict):
        calls.append(
            {"operation": operation, "secret_ref": secret_ref, "payload": payload}
        )
        dumped = str(payload)
        assert CURSOR_TOKEN not in dumped
        assert SLACK_TOKEN not in dumped
        assert "Authorization" not in dumped
        assert "Bearer" not in dumped
        if operation == "create":
            return {
                "id": "bc-11111111-1111-1111-1111-111111111111",
                "latestRunId": "run-1",
                "name": payload["name"],
            }
        if operation == "post_root":
            return {"ok": True, "ts": "222.333", "channel": payload["channel_id"]}
        return []

    with pytest.raises((TypeError, RuntimeError)):
        CursorCloudInjectedTransport()
    with pytest.raises((TypeError, RuntimeError)):
        SlackInjectedTransport()

    cursor = CursorCloudInjectedTransport(
        request=request, secret_ref="CURSOR_API_KEY"
    )
    slack = SlackInjectedTransport(request=request, secret_ref="SLACK_BOT_TOKEN")
    assert cursor.can_resolve_secret_ref() is True
    assert slack.can_resolve_secret_ref() is True
    created = cursor.create(
        idempotency_key="cursor:job:create_run",
        job_id="job-1",
        name="cursor:job:create_run",
        agent_id="bc-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    )
    posted = slack.post_root(
        client_msg_id="cmid-1",
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        job_id="job-1",
    )
    assert created is not None
    assert posted is not None
    assert {c["secret_ref"] for c in calls} == {"CURSOR_API_KEY", "SLACK_BOT_TOKEN"}
    for call in calls:
        assert CURSOR_TOKEN not in str(call)
        assert SLACK_TOKEN not in str(call)


def test_injected_transport_errors_redact_secrets():
    from agent.durable_jobs.injected_transports import CursorCloudInjectedTransport
    from agent.durable_jobs.cursor_cloud import CursorCloudAdapter, CursorCreateKind

    def boom(**_k):
        raise RuntimeError(
            f"upstream 401 token={CURSOR_TOKEN} dsn={SECRET_DSN} slack={SLACK_TOKEN}"
        )

    adapter = CursorCloudAdapter(
        transport=CursorCloudInjectedTransport(
            request=boom, secret_ref="CURSOR_API_KEY"
        )
    )
    result = adapter.create_run(
        idempotency_key="cursor:job:create_run", job_id="job-1"
    )
    assert result.kind is CursorCreateKind.UNKNOWN
    dumped = str(result.error or "")
    assert CURSOR_TOKEN not in dumped
    assert SLACK_TOKEN not in dumped
    assert "supersecret" not in dumped


def test_injected_transport_rejects_raw_token_secret_ref():
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )

    def _idle(**_k):
        raise AssertionError("must not call network")

    with pytest.raises((TypeError, ValueError, RuntimeError)) as cursor_exc:
        CursorCloudInjectedTransport(request=_idle, secret_ref=CURSOR_TOKEN)
    with pytest.raises((TypeError, ValueError, RuntimeError)) as slack_exc:
        SlackInjectedTransport(request=_idle, secret_ref=SLACK_TOKEN)
    for dumped in (str(cursor_exc.value), str(slack_exc.value)):
        assert CURSOR_TOKEN not in dumped
        assert SLACK_TOKEN not in dumped
        assert "xoxb-" not in dumped
        assert "supersecret" not in dumped


def test_injected_transport_modules_do_not_export_live_clients():
    import agent.durable_jobs.injected_transports as transports

    for banned in (
        "CursorCloudHttpClient",
        "LiveCursorCloudTransport",
        "SlackSdkClient",
        "LiveSlackTransport",
        "SlackHttpClient",
    ):
        assert not hasattr(transports, banned)


def test_package2_modules_do_not_import_psycopg_or_live_sdks():
    for name in ("psycopg", "slack_sdk", "slack_bolt"):
        sys.modules.pop(name, None)

    import agent.durable_jobs.injected_transports  # noqa: F401
    import agent.durable_jobs.preflight  # noqa: F401
    import agent.durable_jobs.request_ports  # noqa: F401

    assert "psycopg" not in sys.modules
    assert "slack_sdk" not in sys.modules
    assert "slack_bolt" not in sys.modules


def _seed_lane_job(
    tmp_path: Path,
    *,
    repository_identity: str,
    include_identity_binding: bool = True,
    idempotency_key: str = "idem-lane",
):
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.lane import DurableLaneService
    from agent.durable_jobs.slack_contract import SlackBindingLedger
    from agent.durable_jobs.store import DurableJobStore

    raw = _complete_sqlite(tmp_path, dispatch_enabled=False)
    if not include_identity_binding:
        raw["durable_jobs"].pop("identity_binding", None)
    cfg = load_durable_jobs_config(raw)
    store = DurableJobStore(sqlite_path=cfg.sqlite_path)
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="lane-invariant",
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
    lane = DurableLaneService(config=cfg, store=store)
    return lane, job, store


def _inbound_kwargs(job, **overrides):
    payload = dict(
        job_id=job.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        actor_id="U-alice",
        decision_type="go",
        decision_idempotency_key="dec-lane",
        policy_version="pol-1",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    payload.update(overrides)
    return payload


def test_lane_consume_rejects_cross_repo_job_with_zero_writes_or_ack(tmp_path):
    from tests.agent.durable_jobs.eng28_support import RecordingAckPort, count_table

    lane, job, store = _seed_lane_job(
        tmp_path,
        repository_identity="github.com/evil/other",
        idempotency_key="idem-generic-cross-repo",
    )
    assert lane.config.identity_binding is not None
    assert (
        lane.config.identity_binding.repository_identity
        == "github.com/example/repo"
    )
    assert job.repository_identity == "github.com/evil/other"
    ack = RecordingAckPort()
    result = lane.consume_inbound_action(ack, **_inbound_kwargs(job))
    assert result.ok is False
    assert result.ack_status == "rejected"
    assert getattr(result, "retryable", False) is False
    assert ack.acks == []
    assert count_table(store.sqlite_path, "job_inbound_actions") == 0
    assert count_table(store.sqlite_path, "job_decisions") == 0


def test_lane_consume_rejects_missing_identity_binding_with_zero_writes(tmp_path):
    from tests.agent.durable_jobs.eng28_support import RecordingAckPort, count_table

    lane, job, store = _seed_lane_job(
        tmp_path,
        repository_identity="github.com/example/repo",
        include_identity_binding=False,
        idempotency_key="idem-missing-binding",
    )
    assert lane.config.identity_binding is None
    ack = RecordingAckPort()
    result = lane.consume_inbound_action(ack, **_inbound_kwargs(job))
    assert result.ok is False
    assert result.ack_status == "rejected"
    assert ack.acks == []
    assert count_table(store.sqlite_path, "job_inbound_actions") == 0
    assert count_table(store.sqlite_path, "job_decisions") == 0


def test_lane_consume_does_not_reopen_store_after_close_between_check_and_use(
    tmp_path,
):
    from agent.durable_jobs.lane import DurableLaneService
    from tests.agent.durable_jobs.eng28_support import RecordingAckPort, count_table

    class _CloseBeforeStoreUse(DurableLaneService):
        def _require_sqlite_path(self):
            DurableLaneService.close(self)
            return DurableLaneService._require_sqlite_path(self)

    base_lane, job, store = _seed_lane_job(
        tmp_path,
        repository_identity="github.com/example/repo",
        idempotency_key="idem-toctou-close",
    )
    racing = _CloseBeforeStoreUse(config=base_lane.config, store=store)
    ack = RecordingAckPort()
    result = racing.consume_inbound_action(
        ack, **_inbound_kwargs(job, decision_idempotency_key="dec-toctou")
    )
    assert result.ok is False
    assert result.retryable is True
    assert result.ack_status == "pending"
    assert ack.acks == []
    assert racing._closed is True
    assert racing._store is None
    assert count_table(store.sqlite_path, "job_inbound_actions") == 0
    assert count_table(store.sqlite_path, "job_decisions") == 0
