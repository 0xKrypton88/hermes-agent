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
import site
import socket
import subprocess
import sys
from contextlib import contextmanager
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
    from agent.durable_jobs.request_ports import (
        CursorCloudInjectedRequestPort,
        SlackInjectedRequestPort,
    )

    class _CursorClient:
        def create_agent(self, *_a, **_k):
            return cursor_request(
                operation="create", secret_ref="CURSOR_API_KEY", payload={}
            )

        def get_agent(self, *_a, **_k):
            return cursor_request(
                operation="lookup", secret_ref="CURSOR_API_KEY", payload={}
            )

        def get_run(self, *_a, **_k):
            return cursor_request(
                operation="status", secret_ref="CURSOR_API_KEY", payload={}
            )

    class _SlackClient:
        def chat_postMessage(self, *_a, **_k):
            return slack_request(
                operation="post_root", secret_ref="SLACK_BOT_TOKEN", payload={}
            )

        def conversations_replies(self, *_a, **_k):
            return slack_request(
                operation="lookup", secret_ref="SLACK_BOT_TOKEN", payload={}
            )

    def _credential_resolver(_secret_ref):
        raise AssertionError("startup attach/preflight must not resolve credentials")

    bound_identity = _matching_identity(**identity)
    owner._durable_job_cursor_request = CursorCloudInjectedRequestPort(
        client=_CursorClient(),
        secret_ref="CURSOR_API_KEY",
        workspace_id=bound_identity["workspace_id"],
        repository_identity=bound_identity["repository_identity"],
        credential_resolver=_credential_resolver,
    )
    owner._durable_job_slack_request = SlackInjectedRequestPort(
        client=_SlackClient(),
        secret_ref="SLACK_BOT_TOKEN",
        workspace_id=bound_identity["workspace_id"],
        channel_id="C-ENG58",
        repository_identity=bound_identity["repository_identity"],
        root_thread_ts="1700000000.000001",
        credential_resolver=_credential_resolver,
    )
    owner._durable_job_slack_channel_id = "C-ENG58"
    owner._durable_job_slack_root_thread_ts = "1700000000.000001"
    if install_identity:
        owner._durable_job_runtime_identity = bound_identity
    owner.durable_job_writer_authority_check = _TestWriterAuthority()


def _matching_identity(**overrides) -> dict:
    identity = {
        "workspace_id": CONFIG_WORKSPACE,
        "repository_identity": CONFIG_REPO,
    }
    identity.update(overrides)
    return identity


class _TestWriterAuthority:
    def __call__(self):
        return None

    @contextmanager
    def effect_lease(self, _effect_key):
        yield


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


def _deny_environ_mapping_api(*_a, **_k):
    raise AssertionError("must not use overridable environ mapping APIs")


class _AdversarialEnviron:
    """Proxy whose mapping APIs fail if preflight delegates to os.environ."""

    def __getattribute__(self, name):
        if name in {
            "get",
            "keys",
            "items",
            "values",
            "copy",
            "setdefault",
            "pop",
            "popitem",
            "update",
            "clear",
        }:
            raise AssertionError(f"environ.{name} must not run")
        return object.__getattribute__(self, name)

    def __contains__(self, key):
        raise AssertionError("environ.__contains__ must not run")

    def __iter__(self):
        raise AssertionError("environ.__iter__ must not run")

    def __getitem__(self, key):
        raise AssertionError("environ.__getitem__ must not run")

    def __len__(self):
        raise AssertionError("environ.__len__ must not run")


def _install_overridable_environ_traps(monkeypatch):
    """Replace only preflight's os.environ view; leave process os.environ intact."""
    import agent.durable_jobs.preflight as preflight

    real_os = preflight.os

    class _OsView:
        def __getattr__(self, name):
            if name == "environ":
                return _AdversarialEnviron()
            if name == "getenv":
                return _deny_environ_mapping_api
            return getattr(real_os, name)

    monkeypatch.setattr(preflight, "os", _OsView())
    _install_secret_value_traps(monkeypatch)


def _replace_os_module_environ(stale):
    """Point posix/nt.environ at a snapshot that is not os.environ._data."""
    replaced = []
    for modname in ("posix", "nt"):
        try:
            module = __import__(modname)
        except ImportError:
            continue
        try:
            original = object.__getattribute__(module, "environ")
        except AttributeError:
            continue
        object.__setattr__(module, "environ", stale)
        replaced.append((module, original))
    return replaced


def _restore_os_module_environ(replaced):
    for module, original in replaced:
        object.__setattr__(module, "environ", original)


class _ArmedCollidingKey:
    """Key whose hash collides with a target after setup, then traps hooks."""

    def __init__(self, target, probes, label):
        self._target = target
        self._probes = probes
        self._label = label
        self._armed = False

    def arm(self):
        self._armed = True
        return self

    def __hash__(self):
        if self._armed:
            self._probes.append(f"{self._label}.__hash__")
            raise AssertionError(f"{self._label}.__hash__ must not run")
        return hash(self._target)

    def __eq__(self, other):
        self._probes.append(f"{self._label}.__eq__")
        raise AssertionError(f"{self._label}.__eq__ must not run")

    def __bool__(self):
        self._probes.append(f"{self._label}.__bool__")
        raise AssertionError(f"{self._label}.__bool__ must not run")


class _EvilSecretRefStr(str):
    """str subclass that traps equality, hash, strip, and bool hooks."""

    def __init__(self, value, probes, label):
        self._probes = probes
        self._label = label

    def __new__(cls, value, probes, label):
        return str.__new__(cls, value)

    def __eq__(self, other):
        self._probes.append(f"{self._label}.__eq__")
        raise AssertionError(f"{self._label}.__eq__ must not run")

    def __hash__(self):
        self._probes.append(f"{self._label}.__hash__")
        raise AssertionError(f"{self._label}.__hash__ must not run")

    def __bool__(self):
        self._probes.append(f"{self._label}.__bool__")
        raise AssertionError(f"{self._label}.__bool__ must not run")

    def strip(self, *args, **kwargs):
        self._probes.append(f"{self._label}.strip")
        raise AssertionError(f"{self._label}.strip must not run")


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in (here, *here.parents):
        if (parent / "agent" / "durable_jobs" / "preflight.py").is_file():
            return parent
    raise AssertionError("repository root not found")


@contextmanager
def _hide_ambient_environ_startup_pths():
    """Hide site-packages ``hermes_environ_startup.pth`` for a child process."""
    hidden = []
    destinations = []
    try:
        destinations.extend(site.getsitepackages())
    except Exception:
        pass
    try:
        user = site.getusersitepackages()
        if user:
            destinations.append(user)
    except Exception:
        pass
    seen = set()
    try:
        for dest_dir in destinations:
            if dest_dir in seen:
                continue
            seen.add(dest_dir)
            path = Path(dest_dir) / "hermes_environ_startup.pth"
            if not path.is_file():
                continue
            parked = path.with_name(path.name + ".hermes-hidden")
            path.replace(parked)
            hidden.append((path, parked))
        yield
    finally:
        for path, parked in hidden:
            if parked.is_file() and not path.exists():
                parked.replace(path)


def _child_env_with_worktree_startup(repo: Path) -> dict[str, str]:
    """Make worktree ``sitecustomize`` importable during ``site.main()``."""
    env = os.environ.copy()
    repo_s = str(repo)
    prior = env.get("PYTHONPATH", "")
    parts = [part for part in prior.split(os.pathsep) if part and part != repo_s]
    env["PYTHONPATH"] = os.pathsep.join([repo_s, *parts]) if parts else repo_s
    env.pop("PYTHONSTARTUP", None)
    return env


def _child_env_without_startup_hooks(repo: Path) -> dict[str, str]:
    """Hide worktree sitecustomize and PYTHONSTARTUP from a child process."""
    env = os.environ.copy()
    repo_s = str(repo)
    prior = env.get("PYTHONPATH", "")
    parts = [part for part in prior.split(os.pathsep) if part and part != repo_s]
    if parts:
        env["PYTHONPATH"] = os.pathsep.join(parts)
    else:
        env.pop("PYTHONPATH", None)
    env.pop("PYTHONSTARTUP", None)
    return env


def _child_inherits_env_name(name: str) -> bool:
    if type(name) is not str:
        raise AssertionError("name must be an exact str")
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import os,sys;sys.stdout.write('1' if sys.argv[1] in os.environ else '0')",
            name,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise AssertionError("child environment-name probe failed")
    return result.stdout == "1"


def _environ_instance_storage():
    from agent.durable_jobs.preflight import (
        _CAPTURED_OS_ENVIRON,
        _CAPTURED_OS_ENVIRON_DICT,
        _CAPTURED_OS_ENVIRON_TYPE,
    )

    return _CAPTURED_OS_ENVIRON_DICT.__get__(
        _CAPTURED_OS_ENVIRON, _CAPTURED_OS_ENVIRON_TYPE
    )


def _replace_environ_data(replacement):
    storage = _environ_instance_storage()
    original = dict.__getitem__(storage, "_data")
    dict.__setitem__(storage, "_data", replacement)
    return storage, original


def _insert_backing_only_name(name: str):
    from agent.durable_jobs.preflight import _process_environ_dict

    data = _process_environ_dict()
    if type(data) is not dict:
        raise AssertionError("expected intact environ backing dict")
    encoded = str.encode(name, "ascii")
    if dict.__contains__(data, name) or dict.__contains__(data, encoded):
        raise AssertionError("backing-only name already present")
    try:
        dict.__setitem__(data, encoded, object())
        return data, encoded
    except Exception:
        dict.__setitem__(data, name, object())
        return data, name


def _scrub_untrusted_environ_keys():
    try:
        storage = _environ_instance_storage()
        data = dict.__getitem__(storage, "_data")
    except Exception:
        return
    if type(data) is not dict:
        return
    drop = []
    for pair in dict.items(data):
        if type(pair) is not tuple or tuple.__len__(pair) != 2:
            continue
        key = tuple.__getitem__(pair, 0)
        if type(key) is str or type(key) is bytes:
            continue
        try:
            object.__getattribute__(key, "_armed")
            key._armed = False
        except AttributeError:
            pass
        drop.append(key)
    for key in drop:
        try:
            dict.__delitem__(data, key)
        except Exception:
            pass


@pytest.fixture(autouse=True)
def _scrub_environ_data_keys():
    _scrub_untrusted_environ_keys()
    yield
    _scrub_untrusted_environ_keys()


def _getset_descriptor_type():
    return type(type.__dict__["__dict__"])


def _evil_dict_descriptor(probes):
    getset = _getset_descriptor_type()

    class EvilMeta(type):
        def __eq__(cls, other):
            probes.append(("eq", other is getset))
            raise AssertionError("metaclass __eq__ must not compare to getset_descriptor")

        def __hash__(cls):
            probes.append("hash")
            return hash(getset)

    class EvilDictDesc(metaclass=EvilMeta):
        def __get__(self, obj, owner=None):
            probes.append("desc_get")
            raise AssertionError("custom __dict__ descriptor must not run")

        def __set__(self, obj, value):
            probes.append("desc_set")
            raise AssertionError("custom __dict__ descriptor must not run")

    return EvilDictDesc()


class _RecordingDataDescriptor:
    """Data descriptor that records get/set/eq/hash and can fake a seam value."""

    def __init__(self, probes, label, value):
        self.probes = probes
        self.label = label
        self.value = value

    def __get__(self, obj, owner=None):
        if obj is None:
            return self
        self.probes.append(f"{self.label}.__get__")
        return self.value

    def __set__(self, obj, value):
        self.probes.append(f"{self.label}.__set__")
        raise AssertionError(f"{self.label}.__set__ must not run")

    def __eq__(self, other):
        self.probes.append(f"{self.label}.__eq__")
        raise AssertionError(f"{self.label}.__eq__ must not run")

    def __hash__(self):
        self.probes.append(f"{self.label}.__hash__")
        raise AssertionError(f"{self.label}.__hash__ must not run")


def _drop_instance_name(obj, name):
    storage = object.__getattribute__(obj, "__dict__")
    if type(storage) is dict and dict.__contains__(storage, name):
        dict.__delitem__(storage, name)
    return storage


def _genuine_os_environ_replacement(data=None):
    old = os.environ
    encodekey = object.__getattribute__(old, "encodekey")
    decodekey = object.__getattribute__(old, "decodekey")
    encodevalue = object.__getattribute__(old, "encodevalue")
    decodevalue = object.__getattribute__(old, "decodevalue")
    if data is None:
        data = {}
    return os._Environ(data, encodekey, decodekey, encodevalue, decodevalue), old


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
    runner = _make_runner(tmp_path)
    runner.durable_job_writer_authority_check = _TestWriterAuthority()
    return raw, runner


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


@pytest.mark.parametrize(
    "case",
    (
        "missing_secrets",
        "missing_transport",
        "binding_mismatch",
        "complete_runtime",
    ),
)
def test_startup_preflight_dispatch_allowed_matrix(tmp_path, monkeypatch, case):
    """Gateway preflight dispatch_allowed stays closed unless runtime is verified."""
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.preflight import preflight_durable_jobs
    from gateway.durable_job_lane import get_active_durable_job_lane

    if case == "missing_secrets":
        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        monkeypatch.delenv("CURSOR_API_KEY", raising=False)
        monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
        raw = _complete(tmp_path, dispatch_enabled=True)
        _write_active_config(tmp_path, raw)
        runner = _make_runner(tmp_path)
        runner._maybe_attach_durable_job_lane()
        assert getattr(runner, "_durable_job_lane", None) is None
        assert get_active_durable_job_lane() is None
        report = preflight_durable_jobs(raw)
        assert report.constructible is True
        assert report.runtime_ready is False
        assert report.dispatch_allowed is False
        assert "secret_refs_missing" in report.reasons
        return

    raw, runner = _prepare_startup(tmp_path, monkeypatch, dispatch_enabled=True)
    calls: list = []

    if case == "missing_transport":
        runner._maybe_attach_durable_job_lane()
        assert getattr(runner, "_durable_job_lane", None) is None
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
                request=_idle_request(calls), secret_ref="ACTUAL_CURSOR_REF_MISSING"
            ),
            slack_transport=SlackInjectedTransport(
                request=_idle_request(calls), secret_ref="ACTUAL_SLACK_REF_MISSING"
            ),
        )
        assert report.constructible is True
        assert report.runtime_ready is False
        assert report.dispatch_allowed is False
        assert "transport_secret_ref_mismatch" in report.reasons
        assert calls == []
        return

    _install_request_ports(runner, _idle_request(calls), _idle_request(calls))
    runner._maybe_attach_durable_job_lane()
    handle = getattr(runner, "_durable_job_lane", None)
    assert handle is not None
    assert handle.preflight.runtime_ready is True
    assert handle.preflight.dispatch_allowed is True
    assert handle.config.dispatch_allowed is True
    assert calls == []
    dumped = f"{handle!r} {handle.preflight!r}"
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
        writer_authority_check=old.durable_job_writer_authority_check,
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
        writer_authority_check=new.durable_job_writer_authority_check,
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
    approved = SimpleNamespace()
    _install_request_ports(approved, request, request)
    approved_storage = vars(approved)
    storage["_durable_job_runtime_identity"] = approved_storage[
        "_durable_job_runtime_identity"
    ]
    storage["_durable_job_cursor_request"] = approved_storage[
        "_durable_job_cursor_request"
    ]
    storage["_durable_job_slack_request"] = approved_storage[
        "_durable_job_slack_request"
    ]
    storage["_durable_job_slack_channel_id"] = approved_storage[
        "_durable_job_slack_channel_id"
    ]
    storage["_durable_job_slack_root_thread_ts"] = approved_storage[
        "_durable_job_slack_root_thread_ts"
    ]
    storage["durable_job_writer_authority_check"] = approved_storage[
        "durable_job_writer_authority_check"
    ]
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
    approved = SimpleNamespace()
    _install_request_ports(approved, request, request)
    approved_storage = vars(approved)
    storage["_durable_job_runtime_identity"] = approved_storage[
        "_durable_job_runtime_identity"
    ]
    storage["_durable_job_cursor_request"] = approved_storage[
        "_durable_job_cursor_request"
    ]
    storage["_durable_job_slack_request"] = approved_storage[
        "_durable_job_slack_request"
    ]
    storage["_durable_job_slack_channel_id"] = approved_storage[
        "_durable_job_slack_channel_id"
    ]
    storage["_durable_job_slack_root_thread_ts"] = approved_storage[
        "_durable_job_slack_root_thread_ts"
    ]
    storage["durable_job_writer_authority_check"] = approved_storage[
        "durable_job_writer_authority_check"
    ]
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


def test_startup_presence_does_not_use_overridable_environ_mapping_apis(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from gateway.durable_job_lane import get_active_durable_job_lane

    raw, runner = _prepare_startup(tmp_path, monkeypatch)
    calls: list = []
    _install_request_ports(runner, _idle_request(calls), _idle_request(calls))
    _install_overridable_environ_traps(monkeypatch)
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


def test_startup_attach_follows_live_os_environ_not_stale_os_module_dict(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from gateway.durable_job_lane import get_active_durable_job_lane

    raw, runner = _prepare_startup(tmp_path, monkeypatch)
    calls: list = []
    _install_request_ports(runner, _idle_request(calls), _idle_request(calls))
    stale = {}
    replaced = _replace_os_module_environ(stale)
    try:
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
    finally:
        _restore_os_module_environ(replaced)


def test_startup_attach_stays_none_when_refs_only_in_stale_os_module_environ(
    tmp_path, monkeypatch
):
    _, runner = _prepare_startup(tmp_path, monkeypatch)
    calls: list = []
    monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    stale = {"CURSOR_API_KEY": b"stale", "SLACK_BOT_TOKEN": b"stale"}
    replaced = _replace_os_module_environ(stale)
    try:
        runner._maybe_attach_durable_job_lane()
        assert getattr(runner, "_durable_job_lane", None) is None
        assert calls == []
    finally:
        _restore_os_module_environ(replaced)


def test_startup_owner_dict_descriptor_metaclass_eq_is_not_compared_to_getset(
    tmp_path, monkeypatch
):
    from gateway.run import GatewayRunner

    probes: list = []
    calls: list = []

    class TrapRunner(GatewayRunner):
        __dict__ = _evil_dict_descriptor(probes)

    _prepare_startup(tmp_path, monkeypatch)
    runner = _make_runner(tmp_path, runner_cls=TrapRunner)
    _install_request_ports(runner, _idle_request(calls), _idle_request(calls))
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert probes == []
    assert calls == []


def test_startup_owner_dict_colliding_key_hooks_are_not_executed(
    tmp_path, monkeypatch
):
    probes: list = []
    calls: list = []
    _, runner = _prepare_startup(tmp_path, monkeypatch)
    _install_request_ports(
        runner,
        _idle_request(calls),
        _idle_request(calls),
        install_identity=False,
    )
    storage = object.__getattribute__(runner, "__dict__")
    assert type(storage) is dict
    key = _ArmedCollidingKey("_durable_job_runtime_identity", probes, "owner_key")
    storage[key] = _matching_identity()
    key.arm()
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert probes == []
    assert calls == []


def test_startup_runtime_identity_dict_colliding_key_hooks_are_not_executed(
    tmp_path, monkeypatch
):
    probes: list = []
    calls: list = []
    _, runner = _prepare_startup(tmp_path, monkeypatch)
    _install_request_ports(
        runner,
        _idle_request(calls),
        _idle_request(calls),
        install_identity=False,
    )
    storage = object.__getattribute__(runner, "__dict__")
    identity = {}
    key = _ArmedCollidingKey("workspace_id", probes, "id_key")
    identity[key] = CONFIG_WORKSPACE
    identity["repository_identity"] = CONFIG_REPO
    storage["_durable_job_runtime_identity"] = identity
    key.arm()
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert probes == []
    assert calls == []


def test_startup_does_not_expose_environ_backing_as_authority(tmp_path, monkeypatch):
    from agent.durable_jobs.preflight import _secret_ref_present
    from agent.durable_jobs.production_binding import production_attach_kwargs

    calls: list = []
    _, runner = _prepare_startup(tmp_path, monkeypatch)
    monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    _install_request_ports(runner, _idle_request(calls), _idle_request(calls))
    assert _secret_ref_present("CURSOR_API_KEY") is False
    assert production_attach_kwargs(owner=runner)
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is not None
    assert calls == []


def test_startup_preimport_fake_os_environ_boundary_fails_closed():
    script = """
import os
import sys
sys.path.insert(0, sys.argv[1])

class _Environ:
    pass

fake = _Environ()
fake._data = {"HERMES_ENG50_V6_PREIMPORT": object()}
os._Environ = _Environ
os.environ = fake
from agent.durable_jobs.preflight import _process_environ_dict, _secret_ref_present
data = _process_environ_dict()
present = _secret_ref_present("HERMES_ENG50_V6_PREIMPORT")
sys.stdout.write("accepted=" + ("1" if data is fake._data else "0"))
sys.stdout.write(" present=" + ("1" if present else "0"))
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(_repo_root())],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert result.stdout == "accepted=0 present=0"


def test_startup_does_not_eq_hostile_environ_data_key(tmp_path, monkeypatch):
    from agent.durable_jobs.preflight import _secret_ref_present

    calls: list = []
    _, runner = _prepare_startup(tmp_path, monkeypatch)
    _install_request_ports(runner, _idle_request(calls), _idle_request(calls))
    probes: list = []
    hostile = _ArmedCollidingKey("HERMES_ENG50_V6_HOSTILE_REF", probes, "env_key")
    hostile.arm()
    assert _secret_ref_present(hostile) is False
    runner._maybe_attach_durable_job_lane()
    handle = getattr(runner, "_durable_job_lane", None)
    assert handle is not None
    assert handle.preflight.secret_refs_present is True
    assert handle.preflight.runtime_ready is True
    assert handle.preflight.dispatch_allowed is False
    assert probes == []
    assert calls == []


def test_startup_follows_child_inherited_env_not_backing_cache(tmp_path, monkeypatch):
    calls: list = []
    _, runner = _prepare_startup(tmp_path, monkeypatch)
    monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    os.putenv("CURSOR_API_KEY", "1")
    os.putenv("SLACK_BOT_TOKEN", "1")
    try:
        assert _child_inherits_env_name("CURSOR_API_KEY") is True
        assert _child_inherits_env_name("SLACK_BOT_TOKEN") is True
        _install_request_ports(runner, _idle_request(calls), _idle_request(calls))
        runner._maybe_attach_durable_job_lane()
        handle = getattr(runner, "_durable_job_lane", None)
        assert handle is not None
        assert handle.preflight.secret_refs_present is True
        assert handle.preflight.runtime_ready is True
        assert handle.preflight.dispatch_allowed is False
        assert calls == []
    finally:
        os.unsetenv("CURSOR_API_KEY")
        os.unsetenv("SLACK_BOT_TOKEN")

    calls2: list = []
    _, runner2 = _prepare_startup(tmp_path, monkeypatch)
    monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    assert _child_inherits_env_name("CURSOR_API_KEY") is False
    assert _child_inherits_env_name("SLACK_BOT_TOKEN") is False
    runner2._durable_job_cursor_request = _idle_request(calls2)
    runner2._durable_job_slack_request = _idle_request(calls2)
    runner2._durable_job_runtime_identity = _matching_identity()
    runner2._maybe_attach_durable_job_lane()
    assert getattr(runner2, "_durable_job_lane", None) is None
    assert calls2 == []


def test_startup_rejects_str_subclass_secret_ref_without_hooks(tmp_path, monkeypatch):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )

    probes: list = []
    calls: list = []
    _, runner = _prepare_startup(tmp_path, monkeypatch)
    cursor = CursorCloudInjectedTransport(
        request=_idle_request(calls), secret_ref="CURSOR_API_KEY"
    )
    slack = SlackInjectedTransport(
        request=_idle_request(calls), secret_ref="SLACK_BOT_TOKEN"
    )
    dict.__setitem__(
        object.__getattribute__(cursor, "__dict__"),
        "_secret_ref",
        _EvilSecretRefStr("CURSOR_API_KEY", probes, "cursor"),
    )
    storage = object.__getattribute__(runner, "__dict__")
    storage["_durable_job_runtime_identity"] = _matching_identity()
    storage["_durable_job_cursor_transport"] = cursor
    storage["_durable_job_slack_transport"] = slack
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert probes == []
    assert calls == []


def test_startup_rejects_transport_secret_ref_data_descriptor(tmp_path, monkeypatch):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )

    probes: list = []
    calls: list = []
    _, runner = _prepare_startup(tmp_path, monkeypatch)
    cursor = CursorCloudInjectedTransport(
        request=_idle_request(calls), secret_ref="CURSOR_API_KEY"
    )
    slack = SlackInjectedTransport(
        request=_idle_request(calls), secret_ref="SLACK_BOT_TOKEN"
    )
    _drop_instance_name(cursor, "_secret_ref")
    type.__setattr__(
        CursorCloudInjectedTransport,
        "_secret_ref",
        _RecordingDataDescriptor(probes, "secret_ref", "CURSOR_API_KEY"),
    )
    try:
        storage = object.__getattribute__(runner, "__dict__")
        storage["_durable_job_runtime_identity"] = _matching_identity()
        storage["_durable_job_cursor_transport"] = cursor
        storage["_durable_job_slack_transport"] = slack
        runner._maybe_attach_durable_job_lane()
        assert getattr(runner, "_durable_job_lane", None) is None
        assert probes == []
        assert calls == []
    finally:
        type.__delattr__(CursorCloudInjectedTransport, "_secret_ref")


def test_startup_transport_colliding_instance_dict_key_hooks_are_not_executed(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )

    probes: list = []
    calls: list = []
    _, runner = _prepare_startup(tmp_path, monkeypatch)
    cursor = CursorCloudInjectedTransport(
        request=_idle_request(calls), secret_ref="CURSOR_API_KEY"
    )
    slack = SlackInjectedTransport(
        request=_idle_request(calls), secret_ref="SLACK_BOT_TOKEN"
    )
    storage = _drop_instance_name(cursor, "_secret_ref")
    key = _ArmedCollidingKey("_secret_ref", probes, "transport_key")
    dict.__setitem__(storage, key, "CURSOR_API_KEY")
    key.arm()
    runner_storage = object.__getattribute__(runner, "__dict__")
    runner_storage["_durable_job_runtime_identity"] = _matching_identity()
    runner_storage["_durable_job_cursor_transport"] = cursor
    runner_storage["_durable_job_slack_transport"] = slack
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is None
    assert probes == []
    assert calls == []


def test_startup_win32_envvar_not_found_is_not_overridden_by_stale_wgetenv(
    tmp_path, monkeypatch
):
    import agent.durable_jobs.preflight as preflight

    calls: list = []

    def get_var(name, buf, size):
        calls.append(("get_var", name, buf, size))
        return 0

    def wgetenv(name):
        calls.append(("wgetenv", name))
        return 0x1234

    class _Ctypes:
        def get_last_error(self):
            calls.append("get_last_error")
            return 203

    monkeypatch.setattr(
        preflight,
        "_NATIVE_ENV_NAME_PROBE",
        ("win32", get_var, wgetenv, _Ctypes()),
    )
    _, runner = _prepare_startup(tmp_path, monkeypatch)
    provider_calls: list = []
    _install_request_ports(
        runner, _idle_request(provider_calls), _idle_request(provider_calls)
    )
    runner._maybe_attach_durable_job_lane()
    assert getattr(runner, "_durable_job_lane", None) is not None
    assert ("wgetenv", "CURSOR_API_KEY") not in calls
    assert ("wgetenv", "SLACK_BOT_TOKEN") not in calls
    assert provider_calls == []
    for item in calls:
        if type(item) is tuple and tuple.__len__(item) == 4:
            assert tuple.__getitem__(item, 2) is None
            assert tuple.__getitem__(item, 3) == 0


def test_startup_postimport_genuine_os_environ_replacement_fails_closed(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.production_binding import production_attach_kwargs

    calls: list = []
    _, runner = _prepare_startup(tmp_path, monkeypatch)
    runner._durable_job_cursor_request = _idle_request(calls)
    runner._durable_job_slack_request = _idle_request(calls)
    runner._durable_job_runtime_identity = _matching_identity()
    original = os.environ
    original_storage = object.__getattribute__(original, "__dict__")
    original_data = dict.__getitem__(original_storage, "_data")
    replacement, _ = _genuine_os_environ_replacement(original_data)
    assert replacement is not original
    os.environ = replacement
    try:
        assert production_attach_kwargs(owner=runner) == {}
        runner._maybe_attach_durable_job_lane()
        assert getattr(runner, "_durable_job_lane", None) is None
        assert calls == []
    finally:
        os.environ = original


def test_startup_preimport_genuine_os_environ_replacement_fails_closed():
    script = """
import os
import sys
sys.path.insert(0, sys.argv[1])
old = os.environ
encodekey = object.__getattribute__(old, "encodekey")
decodekey = object.__getattribute__(old, "decodekey")
encodevalue = object.__getattribute__(old, "encodevalue")
decodevalue = object.__getattribute__(old, "decodevalue")
new_data = {}
new = os._Environ(new_data, encodekey, decodekey, encodevalue, decodevalue)
os.environ = new
del old
os.putenv("CURSOR_API_KEY", "1")
os.putenv("SLACK_BOT_TOKEN", "1")
from agent.durable_jobs.preflight import _process_environ_dict, _secret_ref_present
data = _process_environ_dict()
present = _secret_ref_present("CURSOR_API_KEY")
sys.stdout.write("accepted=" + ("1" if data is new_data else "0"))
sys.stdout.write(" present=" + ("1" if present else "0"))
sys.stdout.write(" none=" + ("1" if data is None else "0"))
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(_repo_root())],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert result.stdout == "accepted=0 present=0 none=1"

    # Windows has no environb; nt.environ is not os.environ._data. Force that
    # capture branch so POSIX CI sees the same pre-import singleton hole.
    win_script = r"""
import os
import sys
sys.path.insert(0, sys.argv[1])

class _Hostile:
    def __init__(self, probes):
        self._probes = probes
        self._armed = False
    def arm(self):
        self._armed = True
    def __hash__(self):
        if self._armed:
            self._probes.append("hash")
            raise AssertionError("hostile backing __hash__ must not run")
        return 1
    def __eq__(self, other):
        self._probes.append("eq")
        raise AssertionError("hostile backing __eq__ must not run")

probes = []
old = os.environ
encodekey = object.__getattribute__(old, "encodekey")
decodekey = object.__getattribute__(old, "decodekey")
encodevalue = object.__getattribute__(old, "encodevalue")
decodevalue = object.__getattribute__(old, "decodevalue")
new_data = {}
hostile = _Hostile(probes)
dict.__setitem__(new_data, hostile, object())
new = os._Environ(new_data, encodekey, decodekey, encodevalue, decodevalue)
os.environ = new
del old
try:
    object.__delattr__(os, "environb")
except AttributeError:
    pass
object.__setattr__(sys, "platform", "win32")
os.putenv("CURSOR_API_KEY", "1")
os.putenv("SLACK_BOT_TOKEN", "1")
hostile.arm()
from agent.durable_jobs.preflight import _process_environ_dict, _secret_ref_present
data = _process_environ_dict()
present = _secret_ref_present("CURSOR_API_KEY")
sys.stdout.write("accepted=" + ("1" if data is new_data else "0"))
sys.stdout.write(" present=" + ("1" if present else "0"))
sys.stdout.write(" none=" + ("1" if data is None else "0"))
sys.stdout.write(" probes=" + ("1" if probes else "0"))
"""
    win_result = subprocess.run(
        [sys.executable, "-c", win_script, str(_repo_root())],
        check=False,
        capture_output=True,
        text=True,
    )
    assert win_result.returncode == 0, win_result.stderr
    assert win_result.stdout == "accepted=0 present=0 none=1 probes=0"


def test_startup_preimport_double_environ_and_environb_replacement_fails_closed():
    script = r"""
import os
import shutil
import sys
import tempfile
from pathlib import Path
sys.path.insert(0, sys.argv[1])

class _Hostile:
    def __init__(self, probes):
        self._probes = probes
        self._armed = False
    def arm(self):
        self._armed = True
    def __hash__(self):
        if self._armed:
            self._probes.append("hash")
            raise AssertionError("hostile backing __hash__ must not run")
        return 1
    def __eq__(self, other):
        self._probes.append("eq")
        raise AssertionError("hostile backing __eq__ must not run")

probes = []
old = os.environ
encodekey = object.__getattribute__(old, "encodekey")
decodekey = object.__getattribute__(old, "decodekey")
encodevalue = object.__getattribute__(old, "encodevalue")
decodevalue = object.__getattribute__(old, "decodevalue")
try:
    old_b = object.__getattribute__(os, "environb")
except AttributeError:
    old_b = None
if old_b is not None:
    b_encodekey = object.__getattribute__(old_b, "encodekey")
    b_decodekey = object.__getattribute__(old_b, "decodekey")
    b_encodevalue = object.__getattribute__(old_b, "encodevalue")
    b_decodevalue = object.__getattribute__(old_b, "decodevalue")
else:
    def b_encodekey(value):
        if type(value) is not bytes:
            raise TypeError("bytes expected")
        return value
    b_decodekey = bytes
    b_encodevalue = b_encodekey
    b_decodevalue = bytes
# Cache home/temp before the empty spoof so Windows/SYSTEM Path.home()
# and hermes_constants still resolve. Insert via captured codecs only.
bootstrap = tempfile.mkdtemp()
new_data = {}
for _home_name in (
    "HOME",
    "USERPROFILE",
    "LOCALAPPDATA",
    "TEMP",
    "TMP",
    "TMPDIR",
    "HERMES_HOME",
):
    dict.__setitem__(new_data, encodekey(_home_name), encodevalue(bootstrap))
hostile = _Hostile(probes)
dict.__setitem__(new_data, hostile, object())
new = os._Environ(new_data, encodekey, decodekey, encodevalue, decodevalue)
new_b = os._Environ(new_data, b_encodekey, b_decodekey, b_encodevalue, b_decodevalue)
os.environ = new
os.environb = new_b
del old
os.putenv("CURSOR_API_KEY", "1")
os.putenv("SLACK_BOT_TOKEN", "1")
hostile.arm()
from agent.durable_jobs.preflight import _process_environ_dict, _secret_ref_present
from agent.durable_jobs.production_binding import production_attach_kwargs
from gateway.config import GatewayConfig
from gateway.run import GatewayRunner
data = _process_environ_dict()
present = _secret_ref_present("CURSOR_API_KEY")
td = bootstrap
home = Path(td)
(home / "config.yaml").write_text(
    "durable_jobs:\n"
    "  enabled: true\n"
    "  dispatch_enabled: false\n"
    "  backend: sqlite\n"
    "  sqlite_path: " + td + "/jobs.sqlite\n"
    "  checkpoint_sqlite_path: " + td + "/checkpoints.sqlite\n"
    "  cursor_adapter_mode: injected\n"
    "  slack_adapter_mode: injected\n"
    "  cursor_secret_ref: CURSOR_API_KEY\n"
    "  slack_secret_ref: SLACK_BOT_TOKEN\n"
    "  policy_version: eng29-matrix-v1\n"
    "  identity_binding:\n"
    "    workspace_id: T1\n"
    "    repository_identity: github.com/example/repo\n",
    encoding="utf-8",
)
from hermes_cli import config as cfg
cfg._LOAD_CONFIG_CACHE.clear()
cfg._RAW_CONFIG_CACHE.clear()
runner = GatewayRunner(
    GatewayConfig(
        platforms={},
        sessions_dir=home / "sessions",
        loop_watchdog=False,
    )
)
def _idle(*, operation, secret_ref, payload):
    raise AssertionError("startup attach/preflight must not call the provider")
storage = object.__getattribute__(runner, "__dict__")
storage["_durable_job_runtime_identity"] = {
    "workspace_id": "T1",
    "repository_identity": "github.com/example/repo",
}
storage["_durable_job_cursor_request"] = _idle
storage["_durable_job_slack_request"] = _idle
bound = production_attach_kwargs(owner=runner)
runner._maybe_attach_durable_job_lane()
attached = getattr(runner, "_durable_job_lane", None) is not None
sys.stdout.write(
    "double_preimport_replacement_accepted="
    + ("1" if data is new_data else "0")
)
sys.stdout.write(" present=" + ("1" if present else "0"))
sys.stdout.write(" none=" + ("1" if data is None else "0"))
sys.stdout.write(" bound=" + ("1" if bound else "0"))
sys.stdout.write(" attached=" + ("1" if attached else "0"))
sys.stdout.write(" probes=" + ("1" if probes else "0"))
shutil.rmtree(td, ignore_errors=True)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(_repo_root())],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert (
        result.stdout
        == "double_preimport_replacement_accepted=0 present=0 none=1 bound=0 attached=0 probes=0"
    )


@pytest.mark.skipif(sys.platform == "win32", reason="posix.environ triple-rebind is POSIX-only")
def test_startup_preimport_posix_triple_environ_rebind_fails_closed():
    script = r"""
import os
import posix
import shutil
import sys
import tempfile
from pathlib import Path
sys.path.insert(0, sys.argv[1])

class _Hostile:
    def __init__(self, probes):
        self._probes = probes
        self._armed = False
    def arm(self):
        self._armed = True
    def __hash__(self):
        if self._armed:
            self._probes.append("hash")
            raise AssertionError("hostile backing __hash__ must not run")
        return 1
    def __eq__(self, other):
        self._probes.append("eq")
        raise AssertionError("hostile backing __eq__ must not run")

probes = []
old = os.environ
encodekey = object.__getattribute__(old, "encodekey")
decodekey = object.__getattribute__(old, "decodekey")
encodevalue = object.__getattribute__(old, "encodevalue")
decodevalue = object.__getattribute__(old, "decodevalue")
old_b = object.__getattribute__(os, "environb")
b_encodekey = object.__getattribute__(old_b, "encodekey")
b_decodekey = object.__getattribute__(old_b, "decodekey")
b_encodevalue = object.__getattribute__(old_b, "encodevalue")
b_decodevalue = object.__getattribute__(old_b, "decodevalue")
bootstrap = tempfile.mkdtemp()
new_data = {}
for _home_name in (
    "HOME",
    "USERPROFILE",
    "LOCALAPPDATA",
    "TEMP",
    "TMP",
    "TMPDIR",
    "HERMES_HOME",
):
    dict.__setitem__(new_data, encodekey(_home_name), encodevalue(bootstrap))
hostile = _Hostile(probes)
dict.__setitem__(new_data, hostile, object())
new = os._Environ(new_data, encodekey, decodekey, encodevalue, decodevalue)
new_b = os._Environ(new_data, b_encodekey, b_decodekey, b_encodevalue, b_decodevalue)
os.environ = new
os.environb = new_b
posix.environ = new_data
del old
os.putenv("CURSOR_API_KEY", "1")
os.putenv("SLACK_BOT_TOKEN", "1")
hostile.arm()
from agent.durable_jobs.preflight import _process_environ_dict, _secret_ref_present
from agent.durable_jobs.production_binding import production_attach_kwargs
from gateway.config import GatewayConfig
from gateway.run import GatewayRunner
data = _process_environ_dict()
present = _secret_ref_present("CURSOR_API_KEY")
td = bootstrap
try:
    home = Path(td)
    (home / "config.yaml").write_text(
        "durable_jobs:\n"
        "  enabled: true\n"
        "  dispatch_enabled: false\n"
        "  backend: sqlite\n"
        "  sqlite_path: " + td + "/jobs.sqlite\n"
        "  checkpoint_sqlite_path: " + td + "/checkpoints.sqlite\n"
        "  cursor_adapter_mode: injected\n"
        "  slack_adapter_mode: injected\n"
        "  cursor_secret_ref: CURSOR_API_KEY\n"
        "  slack_secret_ref: SLACK_BOT_TOKEN\n"
        "  policy_version: eng29-matrix-v1\n"
        "  identity_binding:\n"
        "    workspace_id: T1\n"
        "    repository_identity: github.com/example/repo\n",
        encoding="utf-8",
    )
    from hermes_cli import config as cfg
    cfg._LOAD_CONFIG_CACHE.clear()
    cfg._RAW_CONFIG_CACHE.clear()
    runner = GatewayRunner(
        GatewayConfig(
            platforms={},
            sessions_dir=home / "sessions",
            loop_watchdog=False,
        )
    )
    def _idle(*, operation, secret_ref, payload):
        raise AssertionError("startup attach/preflight must not call the provider")
    storage = object.__getattribute__(runner, "__dict__")
    storage["_durable_job_runtime_identity"] = {
        "workspace_id": "T1",
        "repository_identity": "github.com/example/repo",
    }
    storage["_durable_job_cursor_request"] = _idle
    storage["_durable_job_slack_request"] = _idle
    bound = production_attach_kwargs(owner=runner)
    runner._maybe_attach_durable_job_lane()
    attached = getattr(runner, "_durable_job_lane", None) is not None
    sys.stdout.write("accepted=" + ("1" if data is new_data else "0"))
    sys.stdout.write(" present=" + ("1" if present else "0"))
    sys.stdout.write(" none=" + ("1" if data is None else "0"))
    sys.stdout.write(" bound=" + ("1" if bound else "0"))
    sys.stdout.write(" attached=" + ("1" if attached else "0"))
    sys.stdout.write(" probes=" + ("1" if probes else "0"))
finally:
    shutil.rmtree(td, ignore_errors=True)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(_repo_root())],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "accepted=0 present=0 none=1 bound=0 attached=0 probes=0"


def test_startup_preimport_win32_late_constants_import_does_not_self_attest():
    script = r"""
import os
import shutil
import sys
import tempfile
from pathlib import Path
sys.path.insert(0, sys.argv[1])

class _Hostile:
    def __init__(self, probes):
        self._probes = probes
        self._armed = False
    def arm(self):
        self._armed = True
    def __hash__(self):
        if self._armed:
            self._probes.append("hash")
            raise AssertionError("hostile backing __hash__ must not run")
        return 1
    def __eq__(self, other):
        self._probes.append("eq")
        raise AssertionError("hostile backing __eq__ must not run")

probes = []
old = os.environ
encodekey = object.__getattribute__(old, "encodekey")
decodekey = object.__getattribute__(old, "decodekey")
encodevalue = object.__getattribute__(old, "encodevalue")
decodevalue = object.__getattribute__(old, "decodevalue")
bootstrap = tempfile.mkdtemp()
new_data = {}
for _home_name in (
    "HOME",
    "USERPROFILE",
    "LOCALAPPDATA",
    "TEMP",
    "TMP",
    "TMPDIR",
    "HERMES_HOME",
):
    dict.__setitem__(new_data, encodekey(_home_name), encodevalue(bootstrap))
hostile = _Hostile(probes)
dict.__setitem__(new_data, hostile, object())
new = os._Environ(new_data, encodekey, decodekey, encodevalue, decodevalue)
os.environ = new
del old
os.putenv("CURSOR_API_KEY", "1")
os.putenv("SLACK_BOT_TOKEN", "1")
import hermes_constants
from gateway.config import GatewayConfig
from gateway.run import GatewayRunner
td = bootstrap
home = Path(td)
(home / "config.yaml").write_text(
    "durable_jobs:\n"
    "  enabled: true\n"
    "  dispatch_enabled: false\n"
    "  backend: sqlite\n"
    "  sqlite_path: " + td + "/jobs.sqlite\n"
    "  checkpoint_sqlite_path: " + td + "/checkpoints.sqlite\n"
    "  cursor_adapter_mode: injected\n"
    "  slack_adapter_mode: injected\n"
    "  cursor_secret_ref: CURSOR_API_KEY\n"
    "  slack_secret_ref: SLACK_BOT_TOKEN\n"
    "  policy_version: eng29-matrix-v1\n"
    "  identity_binding:\n"
    "    workspace_id: T1\n"
    "    repository_identity: github.com/example/repo\n",
    encoding="utf-8",
)
from hermes_cli import config as cfg
cfg._LOAD_CONFIG_CACHE.clear()
cfg._RAW_CONFIG_CACHE.clear()
runner = GatewayRunner(
    GatewayConfig(
        platforms={},
        sessions_dir=home / "sessions",
        loop_watchdog=False,
    )
)
def _idle(*, operation, secret_ref, payload):
    raise AssertionError("startup attach/preflight must not call the provider")
storage = object.__getattribute__(runner, "__dict__")
storage["_durable_job_runtime_identity"] = {
    "workspace_id": "T1",
    "repository_identity": "github.com/example/repo",
}
storage["_durable_job_cursor_request"] = _idle
storage["_durable_job_slack_request"] = _idle
try:
    object.__delattr__(os, "environb")
except AttributeError:
    pass
object.__setattr__(sys, "platform", "win32")
hostile.arm()
from agent.durable_jobs.preflight import _process_environ_dict, _secret_ref_present
from agent.durable_jobs.production_binding import production_attach_kwargs
data = _process_environ_dict()
present = _secret_ref_present("CURSOR_API_KEY")
try:
    bound = production_attach_kwargs(owner=runner)
    runner._maybe_attach_durable_job_lane()
    attached = getattr(runner, "_durable_job_lane", None) is not None
    sys.stdout.write("accepted=" + ("1" if data is new_data else "0"))
    sys.stdout.write(" present=" + ("1" if present else "0"))
    sys.stdout.write(" none=" + ("1" if data is None else "0"))
    sys.stdout.write(" bound=" + ("1" if bound else "0"))
    sys.stdout.write(" attached=" + ("1" if attached else "0"))
    sys.stdout.write(" probes=" + ("1" if probes else "0"))
finally:
    shutil.rmtree(td, ignore_errors=True)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(_repo_root())],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "accepted=0 present=0 none=1 bound=0 attached=0 probes=0"


def test_gateway_run_import_does_not_self_attest_trusted_startup():
    script = r"""
import os
import shutil
import sys
import tempfile
sys.path.insert(0, sys.argv[1])
home = tempfile.mkdtemp()
os.environ["HERMES_HOME"] = home
try:
    import gateway.run
    import hermes_environ_startup
    ready = hermes_environ_startup.trusted_startup_ready()
    sys.stdout.write("trusted=" + ("1" if ready else "0"))
finally:
    shutil.rmtree(home, ignore_errors=True)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(_repo_root())],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "trusted=0"


def test_fake_moduletype_injection_cannot_mint_startup_witness():
    """Exact ``types.ModuleType`` injection must not mint a startup witness."""
    script = r"""
import os
import sys
import types
sys.path.insert(0, sys.argv[1])
from agent.durable_jobs.preflight import (
    _capture_os_environ_boundary,
    _trusted_startup_pins,
)
ready0, _, _ = _trusted_startup_pins()
cap0 = _capture_os_environ_boundary()
fake = types.ModuleType("hermes_environ_startup")
fake._TRUSTED_CAPTURE_READY = True
fake._PINNED_OS_ENVIRON = os.environ
fake._PINNED_POSIX_ENVIRON = None
sys.modules["hermes_environ_startup"] = fake
ready1, _, _ = _trusted_startup_pins()
cap1 = _capture_os_environ_boundary()
sys.stdout.write("before=" + ("1" if ready0 or cap0[0] is not None else "0"))
sys.stdout.write(" after=" + ("1" if ready1 or cap1[0] is not None else "0"))
"""
    repo = _repo_root()
    with _hide_ambient_environ_startup_pths():
        result = subprocess.run(
            [sys.executable, "-S", "-c", script, str(repo)],
            check=False,
            capture_output=True,
            text=True,
            env=_child_env_without_startup_hooks(repo),
        )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "before=0 after=0"


def test_plant_known_environ_seal_keys_before_import_cannot_mint_witness():
    """Planting known seal keys before import must not mint startup trust."""
    script = r"""
import os
import sys
sys.path.insert(0, sys.argv[1])
os.environ.__dict__["__hermes_trusted_environ_pin__"] = (
    os.environ,
    os.environ.__dict__["_data"],
)
import hermes_environ_startup as h
assert h.trusted_startup_ready() is False
import agent.durable_jobs.preflight as p
assert p._CAPTURED_OS_ENVIRON is None
assert p._trusted_startup_pins()[0] is False
sys.stdout.write("plant_before_import=0")
"""
    repo = _repo_root()
    with _hide_ambient_environ_startup_pths():
        result = subprocess.run(
            [sys.executable, "-S", "-c", script, str(repo)],
            check=False,
            capture_output=True,
            text=True,
            env=_child_env_without_startup_hooks(repo),
        )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "plant_before_import=0"


_COPIED_DATA_ENTRY_REPORT = """
    import hermes_environ_startup
    from agent.durable_jobs.preflight import (
        _native_env_name_present,
        _process_environ_dict,
        _secret_ref_present,
        _trusted_startup_pins,
    )
    ready = hermes_environ_startup.trusted_startup_ready()
    pins_ready, env, _posix = _trusted_startup_pins()
    data = _process_environ_dict()
    present = _secret_ref_present("CURSOR_API_KEY")
    native = _native_env_name_present("CURSOR_API_KEY")
    sys.stdout.write("accepted=" + ("1" if data is new_data else "0"))
    sys.stdout.write(" present=" + ("1" if present else "0"))
    sys.stdout.write(" env_is_new=" + ("1" if env is new else "0"))
    sys.stdout.write(" none=" + ("1" if data is None else "0"))
    sys.stdout.write(" native=" + ("1" if native else "0"))
    sys.stdout.write(" ready=" + ("1" if ready or pins_ready else "0"))
"""


def _copied_data_entry_script(
    body: str,
    *,
    win32: bool,
    posix_triple: bool,
    early_imports: str = "",
) -> str:
    """Pre-import exact ``os._Environ`` whose ``_data`` is a mapping copy.

    Seeds only synthetic home/temp names from a tempfile so Windows
    ``Path.home()`` works. Does not read, log, or compare secret values.
    ``os.putenv`` installs ``CURSOR_API_KEY`` so native presence is not
    vacuous. Host imports that crash under a ``win32`` spoof run first.
    """
    if win32 and posix_triple:
        raise AssertionError("win32 copied-_data and posix triple-rebind are distinct cases")
    prelude = """
import asyncio
import os
import shutil
import sys
import tempfile
sys.path.insert(0, sys.argv[1])
import hermes_logging
"""
    if posix_triple:
        prelude += "import posix\n"
    prelude += early_imports
    setup = """
home = tempfile.mkdtemp()
old = os.environ
encodekey = object.__getattribute__(old, "encodekey")
decodekey = object.__getattribute__(old, "decodekey")
encodevalue = object.__getattribute__(old, "encodevalue")
decodevalue = object.__getattribute__(old, "decodevalue")
old_storage = object.__getattribute__(old, "__dict__")
old_data = dict.__getitem__(old_storage, "_data")
new_data = dict.copy(old_data)
for _home_name in (
    "HOME",
    "USERPROFILE",
    "LOCALAPPDATA",
    "TEMP",
    "TMP",
    "TMPDIR",
    "HERMES_HOME",
):
    dict.__setitem__(new_data, encodekey(_home_name), encodevalue(home))
new = os._Environ(new_data, encodekey, decodekey, encodevalue, decodevalue)
os.environ = new
"""
    if posix_triple:
        setup += """
old_b = object.__getattribute__(os, "environb")
b_encodekey = object.__getattribute__(old_b, "encodekey")
b_decodekey = object.__getattribute__(old_b, "decodekey")
b_encodevalue = object.__getattribute__(old_b, "encodevalue")
b_decodevalue = object.__getattribute__(old_b, "decodevalue")
new_b = os._Environ(new_data, b_encodekey, b_decodekey, b_encodevalue, b_decodevalue)
os.environb = new_b
posix.environ = new_data
"""
    setup += "del old\n"
    if win32:
        setup += """
try:
    object.__delattr__(os, "environb")
except AttributeError:
    pass
object.__setattr__(sys, "platform", "win32")
"""
    setup += """
os.putenv("CURSOR_API_KEY", "1")
os.putenv("SLACK_BOT_TOKEN", "1")
try:
"""
    return prelude + setup + body + """
finally:
    shutil.rmtree(home, ignore_errors=True)
"""


def _assert_copied_data_entry_rejected(
    body: str,
    *,
    win32: bool,
    posix_triple: bool,
    early_imports: str = "",
) -> None:
    script = _copied_data_entry_script(
        body,
        win32=win32,
        posix_triple=posix_triple,
        early_imports=early_imports,
    )
    repo = _repo_root()
    with _hide_ambient_environ_startup_pths():
        result = subprocess.run(
            [sys.executable, "-c", script, str(repo)],
            check=False,
            capture_output=True,
            text=True,
            env=_child_env_with_worktree_startup(repo),
        )
    assert result.returncode == 0, result.stderr
    native = "0" if win32 and sys.platform != "win32" else "1"
    assert result.stdout.startswith(
        f"accepted=0 present=0 env_is_new=0 none=1 native={native}"
    ), (win32, posix_triple, result.stdout, result.stderr)


def test_copied_data_win32_environ_rejected_for_gateway_preflight():
    body = "    _capture_trusted_environ_startup()\n" + _COPIED_DATA_ENTRY_REPORT
    _assert_copied_data_entry_rejected(
        body,
        win32=True,
        posix_triple=False,
        early_imports="from gateway.run import _capture_trusted_environ_startup\n",
    )


@pytest.mark.skipif(sys.platform == "win32", reason="posix.environ triple-rebind is POSIX-only")
def test_copied_data_posix_triple_rebind_rejected_for_gateway_preflight():
    body = "    _capture_trusted_environ_startup()\n" + _COPIED_DATA_ENTRY_REPORT
    _assert_copied_data_entry_rejected(
        body,
        win32=False,
        posix_triple=True,
        early_imports="from gateway.run import _capture_trusted_environ_startup\n",
    )
