"""ENG-50: Gateway startup must bind production transports without activation.

``GatewayRunner._maybe_attach_durable_job_lane`` is the lifecycle-owned
startup path. Complete candidate-bound config + secret refs is not enough:
approved concrete transports must be injected from the production binding
seam. Missing/wrong transports, secret-ref mismatch, and identity mismatch
fail closed. Attach/preflight make no sockets or provider calls.

No live Slack/Cursor/network. No Gateway adapter connect.
"""

from __future__ import annotations

import os
import socket
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.agent.durable_jobs.package2_support import bind_runtime_secret_env


CURSOR_TOKEN = "cursor-secret-token-value"
SLACK_TOKEN = "xoxb-super-secret-token"
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


def _write_active_config(tmp_path: Path, raw: dict) -> None:
    import yaml
    from hermes_cli import config as cfg

    (tmp_path / "config.yaml").write_text(
        yaml.safe_dump(raw, sort_keys=False),
        encoding="utf-8",
    )
    cfg._LOAD_CONFIG_CACHE.clear()
    cfg._RAW_CONFIG_CACHE.clear()


def _idle_request(calls: list):
    def request(*, operation: str, secret_ref: str, payload: dict):
        calls.append(
            {"operation": operation, "secret_ref": secret_ref, "payload": dict(payload)}
        )
        raise AssertionError("startup attach/preflight must not call the provider")

    return request


def _install_request_ports(
    owner, cursor_request, slack_request, *, install_identity=True, **identity
):
    owner._durable_job_cursor_request = cursor_request
    owner._durable_job_slack_request = slack_request
    if install_identity:
        owner._durable_job_runtime_identity = _matching_identity(**identity)


def _matching_identity(**overrides) -> dict:
    identity = {
        "workspace_id": CONFIG_WORKSPACE,
        "repository_identity": CONFIG_REPO,
    }
    identity.update(overrides)
    return identity


class _SeamDescriptor:
    """Data descriptor that records any get/set of an owner seam name."""

    def __init__(self, probes: list, name: str, value):
        self.probes = probes
        self.name = name
        self.value = value

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        self.probes.append(self.name)
        return self.value

    def __set__(self, obj, value):
        self.probes.append(f"set:{self.name}")
        raise AssertionError(f"owner seam descriptor {self.name} must not run")


_SECRET_VALUE_NAMES = frozenset({"CURSOR_API_KEY", "SLACK_BOT_TOKEN"})


def _install_secret_value_traps(monkeypatch, *, extra_names=()):
    """Raise if startup attach/preflight retrieves a credential value."""
    names = _SECRET_VALUE_NAMES | frozenset(extra_names)
    original_get = os.environ.get
    original_getenv = os.getenv
    original_getitem = os._Environ.__getitem__

    def _deny_get(key, default=None):
        if key in names:
            raise AssertionError("startup attach/preflight must not retrieve secret values")
        return original_get(key, default)

    def _deny_getenv(key, default=None):
        if key in names:
            raise AssertionError("startup attach/preflight must not retrieve secret values")
        return original_getenv(key, default)

    def _deny_getitem(self, key):
        if key in names:
            raise AssertionError("startup attach/preflight must not retrieve secret values")
        return original_getitem(self, key)

    monkeypatch.setattr(os.environ, "get", _deny_get)
    monkeypatch.setattr(os, "getenv", _deny_getenv)
    monkeypatch.setattr(os._Environ, "__getitem__", _deny_getitem)


def _make_runner(tmp_path: Path, runner_cls=None):
    from gateway.config import GatewayConfig
    from gateway.run import GatewayRunner

    cls = runner_cls or GatewayRunner
    return cls(
        GatewayConfig(
            platforms={},
            sessions_dir=tmp_path / "sessions",
            loop_watchdog=False,
        )
    )


def _prepare_startup(tmp_path: Path, monkeypatch, **overrides):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    bind_runtime_secret_env(monkeypatch)
    raw = _complete(tmp_path, **overrides)
    _write_active_config(tmp_path, raw)
    return raw, _make_runner(tmp_path)


def test_startup_without_production_ports_does_not_attach_valid_candidate_config(
    tmp_path, monkeypatch
):
    """Config + secrets alone cannot mint runtime_ready — no transports."""
    from gateway.durable_job_lane import get_active_durable_job_lane

    _prepare_startup(tmp_path, monkeypatch)
    runner = _make_runner(tmp_path)
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert get_active_durable_job_lane() is None


def test_startup_binds_approved_transports_when_request_ports_are_installed(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.cursor_cloud import CursorCloudAdapter
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.slack_bridge import SlackClientBridge
    from gateway.durable_job_lane import get_active_durable_job_lane

    raw, runner = _prepare_startup(tmp_path, monkeypatch)
    calls: list = []
    _install_request_ports(
        runner, _idle_request(calls), _idle_request(calls)
    )
    runner._maybe_attach_durable_job_lane()
    handle = getattr(runner, "_durable_job_lane", None)
    assert handle is not None
    assert get_active_durable_job_lane() is handle
    assert isinstance(handle.cursor_adapter, CursorCloudAdapter)
    assert isinstance(handle.slack_adapter, SlackClientBridge)
    assert type(handle.cursor_adapter._transport) is CursorCloudInjectedTransport
    assert type(handle.slack_adapter._transport) is SlackInjectedTransport
    assert handle.preflight.runtime_ready is True
    assert handle.config.dispatch_allowed is False
    assert handle.preflight.dispatch_allowed is False
    assert calls == []
    dumped = f"{handle!r} {handle.preflight!r} {raw!r}"
    assert CURSOR_TOKEN not in dumped
    assert SLACK_TOKEN not in dumped
    assert "xoxb-" not in dumped


def test_startup_missing_one_request_port_does_not_attach(tmp_path, monkeypatch):
    _, runner = _prepare_startup(tmp_path, monkeypatch)
    calls: list = []
    runner._durable_job_runtime_identity = _matching_identity()
    runner._durable_job_cursor_request = _idle_request(calls)
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert calls == []


def test_startup_wrong_concrete_transport_does_not_attach(tmp_path, monkeypatch):
    from agent.durable_jobs.injected_transports import SlackInjectedTransport

    _, runner = _prepare_startup(tmp_path, monkeypatch)
    calls: list = []

    class DuckCursor:
        _secret_ref = "CURSOR_API_KEY"
        _request = _idle_request(calls)

    _install_request_ports(runner, _idle_request(calls), _idle_request(calls))
    runner._durable_job_cursor_transport = DuckCursor()
    runner._durable_job_slack_transport = SlackInjectedTransport(
        request=_idle_request(calls), secret_ref="SLACK_BOT_TOKEN"
    )
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert calls == []


def test_startup_secret_ref_mismatch_does_not_attach(tmp_path, monkeypatch):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )

    _, runner = _prepare_startup(tmp_path, monkeypatch)
    monkeypatch.setenv("ACTUAL_CURSOR_REF_MISSING", "cursor-unbound-dummy-value")
    monkeypatch.setenv("ACTUAL_SLACK_REF_MISSING", "xoxb-unbound-dummy-token")
    calls: list = []
    runner._durable_job_runtime_identity = _matching_identity()
    runner._durable_job_cursor_transport = CursorCloudInjectedTransport(
        request=_idle_request(calls), secret_ref="ACTUAL_CURSOR_REF_MISSING"
    )
    runner._durable_job_slack_transport = SlackInjectedTransport(
        request=_idle_request(calls), secret_ref="ACTUAL_SLACK_REF_MISSING"
    )
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert calls == []


def test_startup_identity_mismatch_does_not_attach(tmp_path, monkeypatch):
    _, runner = _prepare_startup(tmp_path, monkeypatch)
    calls: list = []
    _install_request_ports(
        runner,
        _idle_request(calls),
        _idle_request(calls),
        workspace_id="T-FOREIGN",
        repository_identity=CONFIG_REPO,
    )
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert calls == []


def test_startup_default_off_does_not_attach(tmp_path, monkeypatch):
    _, runner = _prepare_startup(tmp_path, monkeypatch, enabled=False)
    calls: list = []
    _install_request_ports(runner, _idle_request(calls), _idle_request(calls))
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert calls == []


def test_startup_attach_and_preflight_open_no_sockets(tmp_path, monkeypatch):
    def _deny(*_a, **_k):
        raise AssertionError("durable job lane startup must not open sockets")

    monkeypatch.setattr(socket.socket, "connect", _deny)
    monkeypatch.setattr(socket.socket, "connect_ex", _deny)
    _, runner = _prepare_startup(tmp_path, monkeypatch)
    calls: list = []
    _install_request_ports(runner, _idle_request(calls), _idle_request(calls))
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is not None
    assert calls == []


def test_startup_attach_detach_lifecycle(tmp_path, monkeypatch):
    from gateway.durable_job_lane import get_active_durable_job_lane

    _, runner = _prepare_startup(tmp_path, monkeypatch)
    calls: list = []
    _install_request_ports(runner, _idle_request(calls), _idle_request(calls))
    runner._maybe_attach_durable_job_lane()
    handle = runner._durable_job_lane
    assert handle is not None
    assert get_active_durable_job_lane() is handle
    runner._maybe_detach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert get_active_durable_job_lane() is None
    runner._maybe_attach_durable_job_lane()
    restarted = runner._durable_job_lane
    assert restarted is not None
    assert restarted is not handle
    assert get_active_durable_job_lane() is restarted
    assert calls == []


def test_startup_close_fences_holders_and_preserves_lane_closed(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.lane import LaneClosedError
    from tests.agent.durable_jobs.eng28_support import RecordingAckPort
    from tests.agent.durable_jobs.test_handle_shutdown_lease_holder import _inbound
    from tests.gateway.test_durable_job_lane_seam import _seed_bound_job

    _, runner = _prepare_startup(tmp_path, monkeypatch)
    calls: list = []
    _install_request_ports(runner, _idle_request(calls), _idle_request(calls))
    runner._maybe_attach_durable_job_lane()
    handle = runner._durable_job_lane
    assert handle is not None
    job, store = _seed_bound_job(handle, idempotency_key="idem-prod-close")

    with pytest.raises(LaneClosedError):
        with handle.lane._mutation_lease():
            handle.shutdown()

    result = handle.lane.consume_inbound_action(
        RecordingAckPort(),
        **_inbound(job, decision_idempotency_key="dec-prod-close"),
    )
    assert result.ok is False
    assert result.ack_status == "pending"
    assert result.retryable is True
    assert calls == []
    conn = __import__("sqlite3").connect(store.sqlite_path)
    try:
        inbound = conn.execute("SELECT COUNT(*) FROM job_inbound_actions").fetchone()[0]
        decisions = conn.execute("SELECT COUNT(*) FROM job_decisions").fetchone()[0]
    finally:
        conn.close()
    assert inbound == 0
    assert decisions == 0


def test_startup_old_runner_stop_does_not_retire_new_runner_lane(
    tmp_path, monkeypatch
):
    from gateway.durable_job_lane import consume_slack_action_if_active
    from tests.gateway.test_durable_job_lane_seam import (
        _action,
        _count_rows,
        _seed_bound_job,
        _verified_body,
    )

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    bind_runtime_secret_env(monkeypatch)
    old_raw = _complete(tmp_path / "old")
    new_raw = _complete(tmp_path / "new")
    _write_active_config(tmp_path, new_raw)
    old = _make_runner(tmp_path / "old")
    new = _make_runner(tmp_path / "new")
    calls: list = []
    _install_request_ports(old, _idle_request(calls), _idle_request(calls))
    _install_request_ports(new, _idle_request(calls), _idle_request(calls))
    from gateway.durable_job_lane import attach_to_gateway_runner
    from agent.durable_jobs.production_binding import bind_production_transports

    attach_to_gateway_runner(
        old,
        raw_config=old_raw,
        **bind_production_transports(
            old_raw,
            owner=old,
            cursor_request=old._durable_job_cursor_request,
            slack_request=old._durable_job_slack_request,
        ),
    )
    attach_to_gateway_runner(
        new,
        raw_config=new_raw,
        **bind_production_transports(
            new_raw,
            owner=new,
            cursor_request=new._durable_job_cursor_request,
            slack_request=new._durable_job_slack_request,
        ),
    )
    assert old._durable_job_lane is not None
    assert new._durable_job_lane is not None
    assert old._durable_job_lane is not new._durable_job_lane
    job, store = _seed_bound_job(new._durable_job_lane, idempotency_key="idem-prod-live")
    old._maybe_detach_durable_job_lane()
    assert getattr(old, "_durable_job_lane", None) is None
    assert new._durable_job_lane is not None
    result = consume_slack_action_if_active(
        _verified_body(),
        _action(
            "hermes_durable_go",
            {
                "job_id": job.job_id,
                "decision_idempotency_key": "dec-prod-live",
                "policy_version": "pol-1",
                "candidate_id": "cand-1",
                "candidate_version": "v1",
            },
        ),
    )
    assert result is not None
    assert result.ok is True
    assert _count_rows(store.sqlite_path, "job_inbound_actions") == 1
    assert calls == []


def test_startup_does_not_construct_network_clients_from_flags(
    tmp_path, monkeypatch
):
    constructed: list = []

    class _Boom:
        def __init__(self, *a, **k):
            constructed.append((a, k))
            raise AssertionError("flags must not construct a provider client")

    monkeypatch.setitem(__import__("sys").modules, "slack_sdk", SimpleNamespace(WebClient=_Boom))
    _prepare_startup(tmp_path, monkeypatch, dispatch_enabled=True)
    runner = _make_runner(tmp_path)
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert constructed == []


def test_startup_missing_runtime_identity_does_not_attach(tmp_path, monkeypatch):
    _, runner = _prepare_startup(tmp_path, monkeypatch)
    calls: list = []
    runner._durable_job_cursor_request = _idle_request(calls)
    runner._durable_job_slack_request = _idle_request(calls)
    assert "_durable_job_runtime_identity" not in vars(runner)
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert calls == []


def test_startup_owner_seam_properties_are_not_executed(tmp_path, monkeypatch):
    from gateway.run import GatewayRunner

    probes: list = []
    calls: list = []
    request = _idle_request(calls)
    identity = _matching_identity()

    class TrapRunner(GatewayRunner):
        @property
        def _durable_job_runtime_identity(self):
            probes.append("identity")
            return identity

        @property
        def _durable_job_cursor_request(self):
            probes.append("cursor")
            return request

        @property
        def _durable_job_slack_request(self):
            probes.append("slack")
            return request

        @property
        def _durable_job_cursor_transport(self):
            probes.append("cursor_transport")
            raise AssertionError("cursor transport property must not run")

        @property
        def _durable_job_slack_transport(self):
            probes.append("slack_transport")
            raise AssertionError("slack transport property must not run")

    _prepare_startup(tmp_path, monkeypatch)
    runner = _make_runner(tmp_path, runner_cls=TrapRunner)
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert probes == []
    assert calls == []


def test_startup_owner_seam_class_attributes_are_not_read(tmp_path, monkeypatch):
    from gateway.run import GatewayRunner

    calls: list = []
    request = _idle_request(calls)

    class AttrRunner(GatewayRunner):
        _durable_job_runtime_identity = _matching_identity()
        _durable_job_cursor_request = request
        _durable_job_slack_request = request

    _prepare_startup(tmp_path, monkeypatch)
    runner = _make_runner(tmp_path, runner_cls=AttrRunner)
    assert "_durable_job_runtime_identity" not in vars(runner)
    assert "_durable_job_cursor_request" not in vars(runner)
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert calls == []


def test_startup_owner_seam_data_descriptors_are_not_executed(tmp_path, monkeypatch):
    from gateway.run import GatewayRunner

    probes: list = []
    calls: list = []
    request = _idle_request(calls)

    class TrapRunner(GatewayRunner):
        _durable_job_runtime_identity = _SeamDescriptor(
            probes, "identity", _matching_identity()
        )
        _durable_job_cursor_request = _SeamDescriptor(probes, "cursor", request)
        _durable_job_slack_request = _SeamDescriptor(probes, "slack", request)
        _durable_job_cursor_transport = _SeamDescriptor(
            probes, "cursor_transport", None
        )
        _durable_job_slack_transport = _SeamDescriptor(
            probes, "slack_transport", None
        )

    _prepare_startup(tmp_path, monkeypatch)
    runner = _make_runner(tmp_path, runner_cls=TrapRunner)
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert probes == []
    assert calls == []


def test_startup_concrete_instance_storage_ignores_class_descriptors(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from gateway.durable_job_lane import get_active_durable_job_lane
    from gateway.run import GatewayRunner

    probes: list = []
    calls: list = []
    request = _idle_request(calls)

    class TrapRunner(GatewayRunner):
        _durable_job_runtime_identity = _SeamDescriptor(
            probes, "identity", _matching_identity(workspace_id="T-TRAP")
        )
        _durable_job_cursor_request = _SeamDescriptor(probes, "cursor", request)
        _durable_job_slack_request = _SeamDescriptor(probes, "slack", request)
        _durable_job_cursor_transport = _SeamDescriptor(
            probes, "cursor_transport", None
        )
        _durable_job_slack_transport = _SeamDescriptor(
            probes, "slack_transport", None
        )

    _prepare_startup(tmp_path, monkeypatch)
    runner = _make_runner(tmp_path, runner_cls=TrapRunner)
    storage = object.__getattribute__(runner, "__dict__")
    storage["_durable_job_runtime_identity"] = _matching_identity()
    storage["_durable_job_cursor_request"] = request
    storage["_durable_job_slack_request"] = request
    runner._maybe_attach_durable_job_lane()
    handle = getattr(runner, "_durable_job_lane", None)
    assert handle is not None
    assert get_active_durable_job_lane() is handle
    assert type(handle.cursor_adapter._transport) is CursorCloudInjectedTransport
    assert type(handle.slack_adapter._transport) is SlackInjectedTransport
    assert handle.preflight.runtime_ready is True
    assert handle.preflight.dispatch_allowed is False
    assert probes == []
    assert calls == []


def test_startup_preflight_does_not_read_secret_values(tmp_path, monkeypatch):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from gateway.durable_job_lane import get_active_durable_job_lane

    raw, runner = _prepare_startup(tmp_path, monkeypatch)
    calls: list = []
    _install_request_ports(runner, _idle_request(calls), _idle_request(calls))
    _install_secret_value_traps(monkeypatch)
    runner._maybe_attach_durable_job_lane()
    handle = getattr(runner, "_durable_job_lane", None)
    assert handle is not None
    assert get_active_durable_job_lane() is handle
    assert type(handle.cursor_adapter._transport) is CursorCloudInjectedTransport
    assert type(handle.slack_adapter._transport) is SlackInjectedTransport
    assert handle.preflight.secret_refs_present is True
    assert handle.preflight.runtime_ready is True
    assert handle.config.dispatch_allowed is False
    assert handle.preflight.dispatch_allowed is False
    assert calls == []
    dumped = f"{handle!r} {handle.preflight!r} {raw!r}"
    assert CURSOR_TOKEN not in dumped
    assert SLACK_TOKEN not in dumped
    assert "xoxb-" not in dumped
    assert "cursor-test-ref-value" not in dumped
    assert "slack-test-ref-value" not in dumped


def test_startup_owner_metaclass_hooks_are_not_executed(tmp_path, monkeypatch):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from gateway.durable_job_lane import get_active_durable_job_lane
    from gateway.run import GatewayRunner

    probes: list = []
    calls: list = []
    request = _idle_request(calls)
    armed = {"on": False}

    class RecordingMeta(type):
        def __getattribute__(cls, name):
            if armed["on"] and name in ("__mro__", "__dict__"):
                probes.append(name)
                raise AssertionError(f"owner metaclass must not supply {name}")
            return type.__getattribute__(cls, name)

    class TrapRunner(GatewayRunner, metaclass=RecordingMeta):
        pass

    _prepare_startup(tmp_path, monkeypatch)
    runner = _make_runner(tmp_path, runner_cls=TrapRunner)
    storage = object.__getattribute__(runner, "__dict__")
    storage["_durable_job_runtime_identity"] = _matching_identity()
    storage["_durable_job_cursor_request"] = request
    storage["_durable_job_slack_request"] = request
    armed["on"] = True
    runner._maybe_attach_durable_job_lane()
    handle = getattr(runner, "_durable_job_lane", None)
    assert handle is not None
    assert get_active_durable_job_lane() is handle
    assert type(handle.cursor_adapter._transport) is CursorCloudInjectedTransport
    assert type(handle.slack_adapter._transport) is SlackInjectedTransport
    assert handle.preflight.runtime_ready is True
    assert handle.preflight.dispatch_allowed is False
    assert probes == []
    assert calls == []


def test_startup_owner_metaclass_does_not_revive_class_attribute_seams(
    tmp_path, monkeypatch
):
    from gateway.run import GatewayRunner

    probes: list = []
    calls: list = []
    request = _idle_request(calls)
    armed = {"on": False}

    class RecordingMeta(type):
        def __getattribute__(cls, name):
            if armed["on"] and name in ("__mro__", "__dict__"):
                probes.append(name)
                raise AssertionError(f"owner metaclass must not supply {name}")
            return type.__getattribute__(cls, name)

    class TrapRunner(GatewayRunner, metaclass=RecordingMeta):
        _durable_job_runtime_identity = _matching_identity()
        _durable_job_cursor_request = request
        _durable_job_slack_request = request

    _prepare_startup(tmp_path, monkeypatch)
    runner = _make_runner(tmp_path, runner_cls=TrapRunner)
    assert "_durable_job_runtime_identity" not in vars(runner)
    armed["on"] = True
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert probes == []
    assert calls == []
