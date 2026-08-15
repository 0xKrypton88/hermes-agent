"""ENG-50: production transport binding seam without activation.

The Gateway startup path must bind only approved concrete
``CursorCloudInjectedTransport`` / ``SlackInjectedTransport`` instances.
Request callables come from an injectable provider-client seam — never from
invented HTTP/SDK clients, duck types, or config flags. Secret material is
reference names only.

No live Slack/Cursor/network. PostgreSQL is not imported.
"""

from __future__ import annotations

import os
import socket
import subprocess
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


def _matching_identity(**overrides) -> dict:
    identity = {
        "workspace_id": CONFIG_WORKSPACE,
        "repository_identity": CONFIG_REPO,
    }
    identity.update(overrides)
    return identity


def _owner_with_matching_identity():
    owner = type("Owner", (), {})()
    owner.__dict__["_durable_job_runtime_identity"] = _matching_identity()
    return owner


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
    """Raise if attach/preflight retrieves a credential value."""
    names = _SECRET_VALUE_NAMES | frozenset(extra_names)
    original_get = os.environ.get
    original_getenv = os.getenv
    original_getitem = os._Environ.__getitem__

    def _deny_get(key, default=None):
        if key in names:
            raise AssertionError("preflight/attach must not retrieve secret values")
        return original_get(key, default)

    def _deny_getenv(key, default=None):
        if key in names:
            raise AssertionError("preflight/attach must not retrieve secret values")
        return original_getenv(key, default)

    def _deny_getitem(self, key):
        if key in names:
            raise AssertionError("preflight/attach must not retrieve secret values")
        return original_getitem(self, key)

    monkeypatch.setattr(os.environ, "get", _deny_get)
    monkeypatch.setattr(os, "getenv", _deny_getenv)
    monkeypatch.setattr(os._Environ, "__getitem__", _deny_getitem)


def _deny_environ_mapping_api(*_a, **_k):
    raise AssertionError("must not use overridable environ mapping APIs")


def _install_preflight_os_environ(monkeypatch, environ, *, getenv=None):
    """Replace only preflight's os.environ view; leave process os.environ intact."""
    import agent.durable_jobs.preflight as preflight

    real_os = preflight.os
    getenv_fn = _deny_environ_mapping_api if getenv is None else getenv

    class _OsView:
        def __getattr__(self, name):
            if name == "environ":
                return environ
            if name == "getenv":
                return getenv_fn
            return getattr(real_os, name)

    monkeypatch.setattr(preflight, "os", _OsView())


def _install_overridable_environ_traps(monkeypatch):
    """Fail if preflight uses overridable os.environ mapping APIs."""
    _install_preflight_os_environ(monkeypatch, _AdversarialEnviron())


def _replace_os_module_environ(stale):
    """Point posix/nt.environ at a snapshot that is not os.environ._data.

    Windows CPython keeps ``nt.environ`` as an exact ``dict`` that is not
    synchronized with ``monkeypatch.setenv`` / ``os.environ``. POSIX tests
    reproduce that split by rebinding the OS-module attribute only.
    """
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


class _AdversarialEnviron:
    """Proxy whose mapping/equality/hash hooks fail if invoked."""

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

    def __eq__(self, other):
        raise AssertionError("environ.__eq__ must not run")

    def __hash__(self):
        raise AssertionError("environ.__hash__ must not run")


class _HookProbe:
    def __init__(self, probes, label, *, truthy=True):
        self._probes = probes
        self._label = label
        self._truthy = truthy

    def __bool__(self):
        self._probes.append(f"{self._label}.__bool__")
        return self._truthy

    def __eq__(self, other):
        self._probes.append(f"{self._label}.__eq__")
        raise AssertionError(f"{self._label}.__eq__ must not run")

    def __hash__(self):
        self._probes.append(f"{self._label}.__hash__")
        raise AssertionError(f"{self._label}.__hash__ must not run")


class _EvilStr(str):
    def __eq__(self, other):
        raise AssertionError("str-subclass __eq__ must not run")

    def __hash__(self):
        raise AssertionError("str-subclass __hash__ must not run")


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


def _child_inherits_env_name(name: str) -> bool:
    """Return whether a child process inherits this environment *name*."""
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
        owner=_owner_with_matching_identity(),
        cursor_request=_idle_request(calls),
    )
    slack_only = bind_production_transports(
        _complete(tmp_path),
        owner=_owner_with_matching_identity(),
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
        owner=_owner_with_matching_identity(),
        cursor_transport=DuckCursor(),
        slack_transport=SlackInjectedTransport(
            request=_idle_request(calls), secret_ref="SLACK_BOT_TOKEN"
        ),
        cursor_request=_idle_request(calls),
        slack_request=_idle_request(calls),
    )
    bound_subclass = bind_production_transports(
        _complete(tmp_path),
        owner=_owner_with_matching_identity(),
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
        owner=_owner_with_matching_identity(),
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
        owner=_owner_with_matching_identity(),
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
        owner=_owner_with_matching_identity(),
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
        owner=_owner_with_matching_identity(),
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
        owner=_owner_with_matching_identity(),
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


def test_missing_runtime_identity_does_not_bind_directly(tmp_path, monkeypatch):
    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    calls: list = []
    owner = type("Owner", (), {})()
    owner._durable_job_cursor_request = _idle_request(calls)
    owner._durable_job_slack_request = _idle_request(calls)
    assert "_durable_job_runtime_identity" not in vars(owner)
    bound = bind_production_transports(_complete(tmp_path), owner=owner)
    assert bound == {}
    assert calls == []


def test_missing_runtime_identity_without_owner_does_not_bind(tmp_path, monkeypatch):
    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    calls: list = []
    bound = bind_production_transports(
        _complete(tmp_path),
        cursor_request=_idle_request(calls),
        slack_request=_idle_request(calls),
    )
    assert bound == {}
    assert calls == []


def test_padded_runtime_identity_does_not_bind(tmp_path, monkeypatch):
    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    calls: list = []
    owner = type("Owner", (), {})()
    owner._durable_job_runtime_identity = _matching_identity(workspace_id=" T1 ")
    owner._durable_job_cursor_request = _idle_request(calls)
    owner._durable_job_slack_request = _idle_request(calls)
    bound = bind_production_transports(_complete(tmp_path), owner=owner)
    assert bound == {}
    assert calls == []


def test_owner_seam_properties_are_not_executed_during_bind_or_preflight(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.preflight import preflight_durable_jobs

    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    probes: list = []
    calls: list = []
    request = _idle_request(calls)
    identity = _matching_identity()

    class Owner:
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

    owner = Owner()
    raw = _complete(tmp_path)
    bound = bind_production_transports(raw, owner=owner)
    assert bound == {}
    report = preflight_durable_jobs(raw, **bound)
    assert report.runtime_ready is False
    assert report.dispatch_allowed is False
    assert probes == []
    assert calls == []


def test_owner_seam_class_attributes_are_not_read_during_bind(tmp_path, monkeypatch):
    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    calls: list = []
    request = _idle_request(calls)

    class Owner:
        _durable_job_runtime_identity = _matching_identity()
        _durable_job_cursor_request = request
        _durable_job_slack_request = request

    owner = Owner()
    assert "_durable_job_runtime_identity" not in vars(owner)
    assert "_durable_job_cursor_request" not in vars(owner)
    bound = bind_production_transports(_complete(tmp_path), owner=owner)
    assert bound == {}
    assert calls == []


def test_owner_seam_data_descriptors_are_not_executed_during_bind(
    tmp_path, monkeypatch
):
    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    probes: list = []
    calls: list = []
    request = _idle_request(calls)

    class Owner:
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

    owner = Owner()
    bound = bind_production_transports(_complete(tmp_path), owner=owner)
    assert bound == {}
    assert probes == []
    assert calls == []


def test_owner_without_concrete_instance_storage_is_denied(tmp_path, monkeypatch):
    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    calls: list = []
    request = _idle_request(calls)

    class Slotted:
        __slots__ = (
            "_durable_job_runtime_identity",
            "_durable_job_cursor_request",
            "_durable_job_slack_request",
        )

        def __init__(self):
            self._durable_job_runtime_identity = _matching_identity()
            self._durable_job_cursor_request = request
            self._durable_job_slack_request = request

    owner = Slotted()
    with pytest.raises(TypeError):
        vars(owner)
    bound = bind_production_transports(_complete(tmp_path), owner=owner)
    assert bound == {}
    assert calls == []


def test_concrete_instance_storage_is_used_without_executing_class_descriptors(
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
    probes: list = []
    calls: list = []
    request = _idle_request(calls)

    class Owner:
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

    owner = Owner()
    storage = object.__getattribute__(owner, "__dict__")
    storage["_durable_job_runtime_identity"] = _matching_identity()
    storage["_durable_job_cursor_request"] = request
    storage["_durable_job_slack_request"] = request
    raw = _complete(tmp_path)
    bound = bind_production_transports(raw, owner=owner)
    assert type(bound.get("cursor_transport")) is CursorCloudInjectedTransport
    assert type(bound.get("slack_transport")) is SlackInjectedTransport
    report = preflight_durable_jobs(raw, **bound)
    assert report.runtime_ready is True
    assert report.dispatch_allowed is False
    assert probes == []
    assert calls == []


def test_preflight_reports_secret_ref_presence_without_reading_values(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.preflight import preflight_durable_jobs

    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    _install_secret_value_traps(monkeypatch)
    calls: list = []
    raw = _complete(tmp_path)
    report = preflight_durable_jobs(
        raw,
        cursor_transport=CursorCloudInjectedTransport(
            request=_idle_request(calls), secret_ref="CURSOR_API_KEY"
        ),
        slack_transport=SlackInjectedTransport(
            request=_idle_request(calls), secret_ref="SLACK_BOT_TOKEN"
        ),
    )
    assert report.secret_refs_present is True
    assert report.runtime_ready is True
    assert report.dispatch_allowed is False
    assert "secret_refs_missing" not in report.reasons
    assert calls == []
    _assert_no_secrets(report)


def test_preflight_reports_secret_ref_absence_without_reading_values(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.preflight import preflight_durable_jobs

    monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    _install_secret_value_traps(monkeypatch)
    report = preflight_durable_jobs(_complete(tmp_path))
    assert report.secret_refs_present is False
    assert report.runtime_ready is False
    assert report.dispatch_allowed is False
    assert "secret_refs_missing" in report.reasons
    _assert_no_secrets(report)


def test_bind_and_preflight_do_not_read_secret_values(tmp_path, monkeypatch):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.preflight import preflight_durable_jobs

    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    _install_secret_value_traps(monkeypatch)
    calls: list = []
    raw = _complete(tmp_path)
    bound = bind_production_transports(
        raw,
        owner=_owner_with_matching_identity(),
        cursor_request=_idle_request(calls),
        slack_request=_idle_request(calls),
    )
    report = preflight_durable_jobs(raw, **bound)
    assert type(bound.get("cursor_transport")) is CursorCloudInjectedTransport
    assert type(bound.get("slack_transport")) is SlackInjectedTransport
    assert report.secret_refs_present is True
    assert report.runtime_ready is True
    assert report.dispatch_allowed is False
    assert calls == []
    _assert_no_secrets(bound)
    _assert_no_secrets(report)


def test_owner_metaclass_hooks_are_not_executed_during_bind(tmp_path, monkeypatch):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.preflight import preflight_durable_jobs

    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
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

    class Owner(metaclass=RecordingMeta):
        pass

    owner = Owner()
    storage = object.__getattribute__(owner, "__dict__")
    storage["_durable_job_runtime_identity"] = _matching_identity()
    storage["_durable_job_cursor_request"] = request
    storage["_durable_job_slack_request"] = request
    raw = _complete(tmp_path)
    armed["on"] = True
    bound = bind_production_transports(raw, owner=owner)
    report = preflight_durable_jobs(raw, **bound)
    assert type(bound.get("cursor_transport")) is CursorCloudInjectedTransport
    assert type(bound.get("slack_transport")) is SlackInjectedTransport
    assert report.runtime_ready is True
    assert report.dispatch_allowed is False
    assert probes == []
    assert calls == []
    _assert_no_secrets(bound)
    _assert_no_secrets(report)


def test_owner_metaclass_does_not_revive_class_attribute_or_property_seams(
    tmp_path, monkeypatch
):
    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    probes: list = []
    calls: list = []
    request = _idle_request(calls)
    identity = _matching_identity()
    armed = {"on": False}

    class RecordingMeta(type):
        def __getattribute__(cls, name):
            if armed["on"] and name in ("__mro__", "__dict__"):
                probes.append(name)
                raise AssertionError(f"owner metaclass must not supply {name}")
            return type.__getattribute__(cls, name)

    class Owner(metaclass=RecordingMeta):
        _durable_job_runtime_identity = identity
        _durable_job_cursor_request = request
        _durable_job_slack_request = request

        @property
        def _durable_job_cursor_transport(self):
            probes.append("cursor_transport")
            raise AssertionError("cursor transport property must not run")

        @property
        def _durable_job_slack_transport(self):
            probes.append("slack_transport")
            raise AssertionError("slack transport property must not run")

    owner = Owner()
    assert "_durable_job_runtime_identity" not in vars(owner)
    raw = _complete(tmp_path)
    armed["on"] = True
    bound = bind_production_transports(raw, owner=owner)
    assert bound == {}
    assert probes == []
    assert calls == []


def test_preflight_presence_survives_adversarial_environ_proxy(tmp_path, monkeypatch):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.preflight import preflight_durable_jobs

    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    _install_preflight_os_environ(monkeypatch, _AdversarialEnviron())
    calls: list = []
    report = preflight_durable_jobs(
        _complete(tmp_path),
        cursor_transport=CursorCloudInjectedTransport(
            request=_idle_request(calls), secret_ref="CURSOR_API_KEY"
        ),
        slack_transport=SlackInjectedTransport(
            request=_idle_request(calls), secret_ref="SLACK_BOT_TOKEN"
        ),
    )
    assert report.secret_refs_present is True
    assert report.runtime_ready is True
    assert report.dispatch_allowed is False
    assert calls == []
    _assert_no_secrets(report)


def test_preflight_presence_does_not_use_overridable_environ_mapping_apis(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.preflight import preflight_durable_jobs

    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    _install_overridable_environ_traps(monkeypatch)
    calls: list = []
    report = preflight_durable_jobs(
        _complete(tmp_path),
        cursor_transport=CursorCloudInjectedTransport(
            request=_idle_request(calls), secret_ref="CURSOR_API_KEY"
        ),
        slack_transport=SlackInjectedTransport(
            request=_idle_request(calls), secret_ref="SLACK_BOT_TOKEN"
        ),
    )
    assert report.secret_refs_present is True
    assert report.runtime_ready is True
    assert report.dispatch_allowed is False
    assert "secret_refs_missing" not in report.reasons
    assert calls == []
    _assert_no_secrets(report)


def test_preflight_absence_ignores_lying_environ_mapping(tmp_path, monkeypatch):
    from agent.durable_jobs.preflight import preflight_durable_jobs

    class _LyingEnviron:
        def keys(self):
            return ["CURSOR_API_KEY", "SLACK_BOT_TOKEN"]

        def get(self, *_a, **_k):
            raise AssertionError("lying environ.get must not run")

        def items(self):
            raise AssertionError("lying environ.items must not run")

        def values(self):
            raise AssertionError("lying environ.values must not run")

        def __iter__(self):
            return iter(("CURSOR_API_KEY", "SLACK_BOT_TOKEN"))

        def __contains__(self, key):
            return key in ("CURSOR_API_KEY", "SLACK_BOT_TOKEN")

        def __getitem__(self, key):
            raise AssertionError("lying environ.__getitem__ must not run")

    monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    _install_preflight_os_environ(monkeypatch, _LyingEnviron())
    report = preflight_durable_jobs(_complete(tmp_path))
    assert report.secret_refs_present is False
    assert report.runtime_ready is False
    assert report.dispatch_allowed is False
    assert "secret_refs_missing" in report.reasons
    _assert_no_secrets(report)


def test_secret_ref_presence_follows_live_os_environ_not_stale_os_module_dict(
    monkeypatch,
):
    from agent.durable_jobs.preflight import _secret_ref_present

    monkeypatch.setenv("CURSOR_API_KEY", "x")
    stale = {}
    replaced = _replace_os_module_environ(stale)
    try:
        assert _secret_ref_present("CURSOR_API_KEY") is True
    finally:
        _restore_os_module_environ(replaced)


def test_secret_ref_presence_ignores_name_only_in_stale_os_module_environ(
    monkeypatch,
):
    from agent.durable_jobs.preflight import _secret_ref_present

    monkeypatch.delenv("ONLY_IN_STALE_OS_MODULE", raising=False)
    stale = {"ONLY_IN_STALE_OS_MODULE": b"stale"}
    replaced = _replace_os_module_environ(stale)
    try:
        assert _secret_ref_present("ONLY_IN_STALE_OS_MODULE") is False
    finally:
        _restore_os_module_environ(replaced)


def test_preflight_presence_follows_live_os_environ_not_stale_os_module_dict(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.preflight import preflight_durable_jobs

    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    stale = {}
    replaced = _replace_os_module_environ(stale)
    try:
        calls: list = []
        report = preflight_durable_jobs(
            _complete(tmp_path),
            cursor_transport=CursorCloudInjectedTransport(
                request=_idle_request(calls), secret_ref="CURSOR_API_KEY"
            ),
            slack_transport=SlackInjectedTransport(
                request=_idle_request(calls), secret_ref="SLACK_BOT_TOKEN"
            ),
        )
        assert report.secret_refs_present is True
        assert report.runtime_ready is True
        assert report.dispatch_allowed is False
        assert "secret_refs_missing" not in report.reasons
        assert calls == []
        _assert_no_secrets(report)
    finally:
        _restore_os_module_environ(replaced)


def test_preflight_absence_when_names_only_in_stale_os_module_environ(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.preflight import preflight_durable_jobs

    monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    stale = {"CURSOR_API_KEY": b"stale", "SLACK_BOT_TOKEN": b"stale"}
    replaced = _replace_os_module_environ(stale)
    try:
        report = preflight_durable_jobs(_complete(tmp_path))
        assert report.secret_refs_present is False
        assert report.runtime_ready is False
        assert report.dispatch_allowed is False
        assert "secret_refs_missing" in report.reasons
        _assert_no_secrets(report)
    finally:
        _restore_os_module_environ(replaced)


def test_secret_ref_present_ignores_user_controlled_eq_and_hash_hooks():
    from agent.durable_jobs.preflight import _secret_ref_present

    probes: list = []
    assert _secret_ref_present(_HookProbe(probes, "ref")) is False
    assert probes == []
    assert _secret_ref_present(_EvilStr("CURSOR_API_KEY")) is False


def test_bind_and_preflight_do_not_use_overridable_environ_mapping_apis(
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
    _install_overridable_environ_traps(monkeypatch)
    calls: list = []
    raw = _complete(tmp_path)
    bound = bind_production_transports(
        raw,
        owner=_owner_with_matching_identity(),
        cursor_request=_idle_request(calls),
        slack_request=_idle_request(calls),
    )
    report = preflight_durable_jobs(raw, **bound)
    assert type(bound.get("cursor_transport")) is CursorCloudInjectedTransport
    assert type(bound.get("slack_transport")) is SlackInjectedTransport
    assert report.secret_refs_present is True
    assert report.runtime_ready is True
    assert report.dispatch_allowed is False
    assert calls == []
    _assert_no_secrets(bound)
    _assert_no_secrets(report)


def test_owner_dict_descriptor_metaclass_eq_is_not_compared_to_getset(
    tmp_path, monkeypatch
):
    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    probes: list = []
    calls: list = []

    class Owner:
        __dict__ = _evil_dict_descriptor(probes)

    owner = Owner()
    bound = bind_production_transports(
        _complete(tmp_path),
        owner=owner,
        cursor_request=_idle_request(calls),
        slack_request=_idle_request(calls),
    )
    assert bound == {}
    assert probes == []
    assert calls == []


def test_owner_attr_does_not_eq_hash_colliding_instance_dict_key():
    from agent.durable_jobs.production_binding import (
        OWNER_RUNTIME_IDENTITY_ATTR,
        _owner_attr,
    )

    probes: list = []
    owner = type("Owner", (), {})()
    storage = object.__getattribute__(owner, "__dict__")
    assert type(storage) is dict
    key = _ArmedCollidingKey(OWNER_RUNTIME_IDENTITY_ATTR, probes, "owner_key")
    storage[key] = _matching_identity()
    key.arm()
    assert _owner_attr(owner, OWNER_RUNTIME_IDENTITY_ATTR) is None
    assert probes == []


def test_runtime_identity_does_not_eq_hash_colliding_identity_dict_key():
    from agent.durable_jobs.production_binding import _runtime_identity

    probes: list = []
    owner = type("Owner", (), {})()
    storage = object.__getattribute__(owner, "__dict__")
    identity = {}
    key = _ArmedCollidingKey("workspace_id", probes, "id_key")
    identity[key] = CONFIG_WORKSPACE
    identity["repository_identity"] = CONFIG_REPO
    storage["_durable_job_runtime_identity"] = identity
    key.arm()
    assert _runtime_identity(owner) is None
    assert probes == []


def test_bind_does_not_eq_hash_colliding_owner_dict_key(tmp_path, monkeypatch):
    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    probes: list = []
    calls: list = []
    owner = type("Owner", (), {})()
    storage = object.__getattribute__(owner, "__dict__")
    assert type(storage) is dict
    key = _ArmedCollidingKey("_durable_job_runtime_identity", probes, "owner_key")
    storage[key] = _matching_identity()
    storage["_durable_job_cursor_request"] = _idle_request(calls)
    storage["_durable_job_slack_request"] = _idle_request(calls)
    key.arm()
    bound = bind_production_transports(_complete(tmp_path), owner=owner)
    assert bound == {}
    assert probes == []
    assert calls == []


def test_bind_does_not_eq_hash_colliding_runtime_identity_dict_key(
    tmp_path, monkeypatch
):
    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    probes: list = []
    calls: list = []
    owner = type("Owner", (), {})()
    storage = object.__getattribute__(owner, "__dict__")
    identity = {}
    key = _ArmedCollidingKey("workspace_id", probes, "id_key")
    identity[key] = CONFIG_WORKSPACE
    identity["repository_identity"] = CONFIG_REPO
    storage["_durable_job_runtime_identity"] = identity
    storage["_durable_job_cursor_request"] = _idle_request(calls)
    storage["_durable_job_slack_request"] = _idle_request(calls)
    key.arm()
    bound = bind_production_transports(_complete(tmp_path), owner=owner)
    assert bound == {}
    assert probes == []
    assert calls == []


def test_replaced_environ_data_dict_is_rejected(monkeypatch):
    from agent.durable_jobs.preflight import (
        _process_environ_dict,
        _secret_ref_present,
    )

    fake_name = "HERMES_ENG50_V6_FAKE_REF"
    real_name = "HERMES_ENG50_V6_REAL_REF"
    monkeypatch.setenv(real_name, "x")
    replacement = {fake_name: object()}
    storage, original = _replace_environ_data(replacement)
    try:
        data = _process_environ_dict()
        assert data is not replacement
        assert data is None
        assert _secret_ref_present(fake_name) is False
        assert _secret_ref_present(real_name) is False
    finally:
        dict.__setitem__(storage, "_data", original)
    assert _secret_ref_present(real_name) is True
    assert _secret_ref_present(fake_name) is False


def test_preimport_fake_os_environ_boundary_fails_closed():
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


def test_secret_ref_presence_does_not_eq_hostile_environ_data_key(monkeypatch):
    from agent.durable_jobs.preflight import _process_environ_dict, _secret_ref_present

    hostile_name = "HERMES_ENG50_V6_HOSTILE_REF"
    live_name = "HERMES_ENG50_V6_HOSTILE_LIVE"
    monkeypatch.delenv(hostile_name, raising=False)
    monkeypatch.setenv(live_name, "x")
    data = _process_environ_dict()
    assert type(data) is dict
    probes: list = []
    hostile = _ArmedCollidingKey(hostile_name, probes, "env_key")
    live_hostile = _ArmedCollidingKey(live_name, probes, "live_key")
    dict.__setitem__(data, hostile, object())
    dict.__setitem__(data, live_hostile, object())
    try:
        hostile.arm()
        live_hostile.arm()
        assert _secret_ref_present(hostile_name) is False
        assert _secret_ref_present(live_name) is True
        assert probes == []
    finally:
        hostile._armed = False
        live_hostile._armed = False
        dict.__delitem__(data, hostile)
        dict.__delitem__(data, live_hostile)


def test_secret_ref_presence_follows_child_inherited_env_not_backing_cache():
    from agent.durable_jobs.preflight import _secret_ref_present

    putenv_name = "HERMES_ENG50_V6_PUTENV_ONLY"
    backing_name = "HERMES_ENG50_V6_BACKING_ONLY"
    os.putenv(putenv_name, "1")
    try:
        assert _child_inherits_env_name(putenv_name) is True
        assert _secret_ref_present(putenv_name) is True
    finally:
        os.unsetenv(putenv_name)
    assert _secret_ref_present(putenv_name) is False
    assert _child_inherits_env_name(putenv_name) is False

    data, backing_key = _insert_backing_only_name(backing_name)
    try:
        assert _child_inherits_env_name(backing_name) is False
        assert _secret_ref_present(backing_name) is False
    finally:
        dict.__delitem__(data, backing_key)


def test_approved_transport_rejects_str_subclass_secret_ref_without_hooks():
    from agent.durable_jobs.injected_transports import CursorCloudInjectedTransport
    from agent.durable_jobs.production_binding import _approved_transport

    probes: list = []
    transport = CursorCloudInjectedTransport(
        request=_idle_request([]), secret_ref="CURSOR_API_KEY"
    )
    storage = object.__getattribute__(transport, "__dict__")
    dict.__setitem__(
        storage, "_secret_ref", _EvilSecretRefStr("CURSOR_API_KEY", probes, "ref")
    )
    assert (
        _approved_transport(
            transport, CursorCloudInjectedTransport, "CURSOR_API_KEY"
        )
        is False
    )
    assert probes == []


def test_preflight_rejects_str_subclass_secret_ref_without_hooks(tmp_path, monkeypatch):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.preflight import (
        _concrete_injected_transport,
        _transport_secret_ref,
        preflight_durable_jobs,
    )

    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    probes: list = []
    calls: list = []
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
    dict.__setitem__(
        object.__getattribute__(slack, "__dict__"),
        "_secret_ref",
        _EvilSecretRefStr("SLACK_BOT_TOKEN", probes, "slack"),
    )
    assert _concrete_injected_transport(cursor, CursorCloudInjectedTransport) is False
    assert _transport_secret_ref(cursor) is None
    report = preflight_durable_jobs(
        _complete(tmp_path), cursor_transport=cursor, slack_transport=slack
    )
    assert report.transport_capability is False
    assert report.runtime_ready is False
    assert report.dispatch_allowed is False
    assert probes == []
    assert calls == []
    _assert_no_secrets(report)


def test_bind_rejects_str_subclass_secret_ref_without_hooks(tmp_path, monkeypatch):
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )

    bind_production_transports = _require_binding()
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_TOKEN)
    monkeypatch.setenv("SLACK_BOT_TOKEN", SLACK_TOKEN)
    probes: list = []
    calls: list = []
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
    bound = bind_production_transports(
        _complete(tmp_path),
        owner=_owner_with_matching_identity(),
        cursor_transport=cursor,
        slack_transport=slack,
    )
    assert bound == {}
    assert probes == []
    assert calls == []


def test_bind_rejects_replaced_environ_data_dict(tmp_path, monkeypatch):
    bind_production_transports = _require_binding()
    monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    calls: list = []
    replacement = {"CURSOR_API_KEY": "x", "SLACK_BOT_TOKEN": "x"}
    storage, original = _replace_environ_data(replacement)
    try:
        bound = bind_production_transports(
            _complete(tmp_path),
            owner=_owner_with_matching_identity(),
            cursor_request=_idle_request(calls),
            slack_request=_idle_request(calls),
        )
        assert bound == {}
        assert calls == []
    finally:
        dict.__setitem__(storage, "_data", original)
