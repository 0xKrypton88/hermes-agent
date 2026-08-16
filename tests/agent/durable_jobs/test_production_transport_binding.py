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
import site
import socket
import subprocess
import sys
from contextlib import contextmanager
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


@contextmanager
def _hide_ambient_environ_startup_pths():
    """Hide site-packages ``hermes_environ_startup.pth`` for a child process.

    Proves startup provenance does not depend on a prior ``setup.py``
    write into the ambient interpreter. Restores parked files afterwards.
    """
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
    """Build a real ``os._Environ`` that is not the startup singleton."""
    old = os.environ
    encodekey = object.__getattribute__(old, "encodekey")
    decodekey = object.__getattribute__(old, "decodekey")
    encodevalue = object.__getattribute__(old, "encodevalue")
    decodevalue = object.__getattribute__(old, "decodevalue")
    if data is None:
        data = {}
    return os._Environ(data, encodekey, decodekey, encodevalue, decodevalue), old


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
    dict.__setitem__(data, hostile, object())
    try:
        hostile.arm()
        assert _secret_ref_present(hostile_name) is False
        assert _secret_ref_present(live_name) is True
        assert probes == []
    finally:
        hostile._armed = False
        dict.__delitem__(data, hostile)


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


def test_preflight_instance_attr_does_not_run_dict_descriptor_or_metaclass_eq():
    from agent.durable_jobs.preflight import _instance_attr

    probes: list = []

    class Holder:
        __dict__ = _evil_dict_descriptor(probes)

    holder = Holder()
    assert _instance_attr(holder, "_secret_ref") is None
    assert probes == []


def test_preflight_ignores_transport_secret_ref_data_descriptor():
    from agent.durable_jobs.injected_transports import CursorCloudInjectedTransport
    from agent.durable_jobs.preflight import (
        _concrete_injected_transport,
        _transport_secret_ref,
    )

    probes: list = []
    transport = CursorCloudInjectedTransport(
        request=_idle_request([]), secret_ref="CURSOR_API_KEY"
    )
    _drop_instance_name(transport, "_secret_ref")
    type.__setattr__(
        CursorCloudInjectedTransport,
        "_secret_ref",
        _RecordingDataDescriptor(probes, "secret_ref", "CURSOR_API_KEY"),
    )
    try:
        assert (
            _concrete_injected_transport(transport, CursorCloudInjectedTransport)
            is False
        )
        assert _transport_secret_ref(transport) is None
        assert probes == []
    finally:
        type.__delattr__(CursorCloudInjectedTransport, "_secret_ref")


def test_preflight_ignores_transport_request_data_descriptor():
    from agent.durable_jobs.injected_transports import CursorCloudInjectedTransport
    from agent.durable_jobs.preflight import _concrete_injected_transport

    probes: list = []
    calls: list = []
    transport = CursorCloudInjectedTransport(
        request=_idle_request(calls), secret_ref="CURSOR_API_KEY"
    )
    _drop_instance_name(transport, "_request")
    type.__setattr__(
        CursorCloudInjectedTransport,
        "_request",
        _RecordingDataDescriptor(probes, "request", _idle_request(calls)),
    )
    try:
        assert (
            _concrete_injected_transport(transport, CursorCloudInjectedTransport)
            is False
        )
        assert probes == []
        assert calls == []
    finally:
        type.__delattr__(CursorCloudInjectedTransport, "_request")


def test_approved_transport_ignores_transport_secret_ref_data_descriptor():
    from agent.durable_jobs.injected_transports import CursorCloudInjectedTransport
    from agent.durable_jobs.production_binding import _approved_transport

    probes: list = []
    transport = CursorCloudInjectedTransport(
        request=_idle_request([]), secret_ref="CURSOR_API_KEY"
    )
    _drop_instance_name(transport, "_secret_ref")
    type.__setattr__(
        CursorCloudInjectedTransport,
        "_secret_ref",
        _RecordingDataDescriptor(probes, "secret_ref", "CURSOR_API_KEY"),
    )
    try:
        assert (
            _approved_transport(
                transport, CursorCloudInjectedTransport, "CURSOR_API_KEY"
            )
            is False
        )
        assert probes == []
    finally:
        type.__delattr__(CursorCloudInjectedTransport, "_secret_ref")


def test_bind_rejects_transport_secret_ref_data_descriptor(tmp_path, monkeypatch):
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
    _drop_instance_name(cursor, "_secret_ref")
    type.__setattr__(
        CursorCloudInjectedTransport,
        "_secret_ref",
        _RecordingDataDescriptor(probes, "secret_ref", "CURSOR_API_KEY"),
    )
    try:
        bound = bind_production_transports(
            _complete(tmp_path),
            owner=_owner_with_matching_identity(),
            cursor_transport=cursor,
            slack_transport=slack,
        )
        assert bound == {}
        assert probes == []
        assert calls == []
    finally:
        type.__delattr__(CursorCloudInjectedTransport, "_secret_ref")


def test_preflight_transport_colliding_instance_dict_key_hooks_are_not_executed():
    from agent.durable_jobs.injected_transports import CursorCloudInjectedTransport
    from agent.durable_jobs.preflight import (
        _concrete_injected_transport,
        _transport_secret_ref,
    )

    probes: list = []
    transport = CursorCloudInjectedTransport(
        request=_idle_request([]), secret_ref="CURSOR_API_KEY"
    )
    storage = _drop_instance_name(transport, "_secret_ref")
    key = _ArmedCollidingKey("_secret_ref", probes, "transport_key")
    dict.__setitem__(storage, key, "CURSOR_API_KEY")
    key.arm()
    assert _concrete_injected_transport(transport, CursorCloudInjectedTransport) is False
    assert _transport_secret_ref(transport) is None
    assert probes == []


def test_approved_transport_transport_colliding_key_hooks_are_not_executed():
    from agent.durable_jobs.injected_transports import CursorCloudInjectedTransport
    from agent.durable_jobs.production_binding import _approved_transport

    probes: list = []
    transport = CursorCloudInjectedTransport(
        request=_idle_request([]), secret_ref="CURSOR_API_KEY"
    )
    storage = _drop_instance_name(transport, "_secret_ref")
    key = _ArmedCollidingKey("_secret_ref", probes, "transport_key")
    dict.__setitem__(storage, key, "CURSOR_API_KEY")
    key.arm()
    assert (
        _approved_transport(transport, CursorCloudInjectedTransport, "CURSOR_API_KEY")
        is False
    )
    assert probes == []


def test_bind_rejects_transport_colliding_instance_dict_key(tmp_path, monkeypatch):
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
    storage = _drop_instance_name(cursor, "_secret_ref")
    key = _ArmedCollidingKey("_secret_ref", probes, "transport_key")
    dict.__setitem__(storage, key, "CURSOR_API_KEY")
    key.arm()
    bound = bind_production_transports(
        _complete(tmp_path),
        owner=_owner_with_matching_identity(),
        cursor_transport=cursor,
        slack_transport=slack,
    )
    assert bound == {}
    assert probes == []
    assert calls == []


def test_win32_envvar_not_found_is_not_overridden_by_stale_wgetenv(monkeypatch):
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
    assert preflight._native_env_name_present("CURSOR_API_KEY") is False
    assert preflight._secret_ref_present("CURSOR_API_KEY") is False
    assert ("wgetenv", "CURSOR_API_KEY") not in calls
    for item in calls:
        if type(item) is tuple and tuple.__len__(item) == 4:
            assert tuple.__getitem__(item, 2) is None
            assert tuple.__getitem__(item, 3) == 0


def test_win32_empty_env_name_is_present_without_value_read(monkeypatch):
    import agent.durable_jobs.preflight as preflight

    calls: list = []

    def get_var(name, buf, size):
        calls.append(("get_var", name, buf, size))
        return 1

    def wgetenv(name):
        calls.append(("wgetenv", name))
        return 0x1234

    class _Ctypes:
        def get_last_error(self):
            calls.append("get_last_error")
            return 0

    monkeypatch.setattr(
        preflight,
        "_NATIVE_ENV_NAME_PROBE",
        ("win32", get_var, wgetenv, _Ctypes()),
    )
    assert preflight._native_env_name_present("HERMES_ENG50_V7_EMPTY") is True
    assert ("wgetenv", "HERMES_ENG50_V7_EMPTY") not in calls
    assert calls[0] == ("get_var", "HERMES_ENG50_V7_EMPTY", None, 0)


def test_win32_present_env_name_does_not_read_value(monkeypatch):
    import agent.durable_jobs.preflight as preflight

    calls: list = []

    def get_var(name, buf, size):
        calls.append(("get_var", name, buf, size))
        return 8

    def wgetenv(name):
        calls.append(("wgetenv", name))
        raise AssertionError("stale CRT _wgetenv must not run when Win32 is present")

    class _Ctypes:
        def get_last_error(self):
            raise AssertionError("GetLastError must not run after a present size")

    monkeypatch.setattr(
        preflight,
        "_NATIVE_ENV_NAME_PROBE",
        ("win32", get_var, wgetenv, _Ctypes()),
    )
    assert preflight._native_env_name_present("HERMES_ENG50_V7_PRESENT") is True
    assert calls == [("get_var", "HERMES_ENG50_V7_PRESENT", None, 0)]


def test_postimport_genuine_os_environ_replacement_fails_closed(monkeypatch):
    from agent.durable_jobs.preflight import (
        _process_environ_dict,
        _secret_ref_present,
    )

    live_name = "HERMES_ENG50_V7_POSTIMPORT_LIVE"
    monkeypatch.setenv(live_name, "x")
    original = os.environ
    original_storage = object.__getattribute__(original, "__dict__")
    original_data = dict.__getitem__(original_storage, "_data")
    replacement, _ = _genuine_os_environ_replacement(original_data)
    assert replacement is not original
    os.environ = replacement
    try:
        data = _process_environ_dict()
        assert data is not original_data
        assert data is None
        assert _secret_ref_present(live_name) is False
    finally:
        os.environ = original


def test_preimport_genuine_os_environ_replacement_fails_closed():
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


def test_preimport_double_environ_and_environb_replacement_fails_closed():
    script = r"""
import os
import shutil
import sys
import tempfile
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
new_data = {}
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
from agent.durable_jobs.production_binding import bind_production_transports
data = _process_environ_dict()
present = _secret_ref_present("CURSOR_API_KEY")
td = tempfile.mkdtemp()
owner = type("Owner", (), {})()
object.__getattribute__(owner, "__dict__")["_durable_job_runtime_identity"] = {
    "workspace_id": "T1",
    "repository_identity": "github.com/example/repo",
}
def _idle(*, operation, secret_ref, payload):
    raise AssertionError("bind/preflight must not call the provider")
object.__getattribute__(owner, "__dict__")["_durable_job_cursor_request"] = _idle
object.__getattribute__(owner, "__dict__")["_durable_job_slack_request"] = _idle
bound = bind_production_transports(
    {
        "durable_jobs": {
            "enabled": True,
            "dispatch_enabled": False,
            "backend": "sqlite",
            "sqlite_path": td + "/jobs.sqlite",
            "checkpoint_sqlite_path": td + "/checkpoints.sqlite",
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
    },
    owner=owner,
)
sys.stdout.write(
    "double_preimport_replacement_accepted="
    + ("1" if data is new_data else "0")
)
sys.stdout.write(" present=" + ("1" if present else "0"))
sys.stdout.write(" none=" + ("1" if data is None else "0"))
sys.stdout.write(" bound=" + ("1" if bound else "0"))
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
        == "double_preimport_replacement_accepted=0 present=0 none=1 bound=0 probes=0"
    )


@pytest.mark.skipif(sys.platform == "win32", reason="posix.environ triple-rebind is POSIX-only")
def test_preimport_posix_triple_environ_rebind_fails_closed():
    script = r"""
import os
import posix
import shutil
import sys
import tempfile
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
new_data = {}
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
from agent.durable_jobs.production_binding import bind_production_transports
data = _process_environ_dict()
present = _secret_ref_present("CURSOR_API_KEY")
td = tempfile.mkdtemp()
try:
    owner = type("Owner", (), {})()
    object.__getattribute__(owner, "__dict__")["_durable_job_runtime_identity"] = {
        "workspace_id": "T1",
        "repository_identity": "github.com/example/repo",
    }
    def _idle(*, operation, secret_ref, payload):
        raise AssertionError("bind/preflight must not call the provider")
    object.__getattribute__(owner, "__dict__")["_durable_job_cursor_request"] = _idle
    object.__getattribute__(owner, "__dict__")["_durable_job_slack_request"] = _idle
    bound = bind_production_transports(
        {
            "durable_jobs": {
                "enabled": True,
                "dispatch_enabled": False,
                "backend": "sqlite",
                "sqlite_path": td + "/jobs.sqlite",
                "checkpoint_sqlite_path": td + "/checkpoints.sqlite",
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
        },
        owner=owner,
    )
    sys.stdout.write("accepted=" + ("1" if data is new_data else "0"))
    sys.stdout.write(" present=" + ("1" if present else "0"))
    sys.stdout.write(" none=" + ("1" if data is None else "0"))
    sys.stdout.write(" bound=" + ("1" if bound else "0"))
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
    assert result.stdout == "accepted=0 present=0 none=1 bound=0 probes=0"


def test_preimport_win32_late_constants_import_does_not_self_attest():
    script = r"""
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
import hermes_constants
hostile.arm()
from agent.durable_jobs.preflight import _process_environ_dict, _secret_ref_present
data = _process_environ_dict()
present = _secret_ref_present("CURSOR_API_KEY")
sys.stdout.write("accepted=" + ("1" if data is new_data else "0"))
sys.stdout.write(" present=" + ("1" if present else "0"))
sys.stdout.write(" none=" + ("1" if data is None else "0"))
sys.stdout.write(" probes=" + ("1" if probes else "0"))
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(_repo_root())],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "accepted=0 present=0 none=1 probes=0"


def test_startup_module_import_does_not_self_attest_current_environ():
    script = r"""
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
import hermes_environ_startup
from agent.durable_jobs.preflight import _process_environ_dict, _secret_ref_present
ready = hermes_environ_startup.trusted_startup_ready()
data = _process_environ_dict()
present = _secret_ref_present("CURSOR_API_KEY")
sys.stdout.write("ready=" + ("1" if ready else "0"))
sys.stdout.write(" accepted=" + ("1" if data is new_data else "0"))
sys.stdout.write(" present=" + ("1" if present else "0"))
sys.stdout.write(" none=" + ("1" if data is None else "0"))
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(_repo_root())],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "ready=0 accepted=0 present=0 none=1"


def test_cli_entry_captures_trusted_startup_before_preflight():
    script = r"""
import os
import shutil
import sys
import tempfile
sys.path.insert(0, sys.argv[1])
home = tempfile.mkdtemp()
os.environ["HERMES_HOME"] = home
try:
    import hermes_cli.main
    import hermes_environ_startup
    from agent.durable_jobs.preflight import _process_environ_dict
    ready = hermes_environ_startup.trusted_startup_ready()
    data = _process_environ_dict()
    sys.stdout.write("trusted=" + ("1" if ready else "0"))
    sys.stdout.write(" accepted=" + ("1" if data is not None else "0"))
finally:
    shutil.rmtree(home, ignore_errors=True)
"""
    with _hide_ambient_environ_startup_pths():
        result = subprocess.run(
            [sys.executable, "-c", script, str(_repo_root())],
            check=False,
            capture_output=True,
            text=True,
        )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "trusted=1 accepted=1"


def test_setup_module_import_does_not_write_site_pth(tmp_path):
    script = r"""
import importlib.util
import site
import sys
from pathlib import Path
fake = Path(sys.argv[1])
root = Path(sys.argv[2])
site.getsitepackages = lambda: [str(fake)]
import setuptools
setuptools.setup = lambda *a, **k: None
spec = importlib.util.spec_from_file_location(
    "hermes_setup_under_test",
    root / "setup.py",
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
wrote = (fake / "hermes_environ_startup.pth").is_file()
sys.stdout.write("wrote=" + ("1" if wrote else "0"))
"""
    fake = tmp_path / "site-packages"
    fake.mkdir()
    result = subprocess.run(
        [sys.executable, "-c", script, str(fake), str(_repo_root())],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "wrote=0"


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


def _copied_data_entry_script(body: str, *, win32: bool, posix_triple: bool) -> str:
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


def _assert_copied_data_entry_rejected(body: str, *, win32: bool, posix_triple: bool) -> None:
    script = _copied_data_entry_script(body, win32=win32, posix_triple=posix_triple)
    with _hide_ambient_environ_startup_pths():
        result = subprocess.run(
            [sys.executable, "-c", script, str(_repo_root())],
            check=False,
            capture_output=True,
            text=True,
        )
    assert result.returncode == 0, result.stderr
    # Fail-closed: the copied mapping must not become the process dict or
    # the startup pin. ``present`` is gated on that dict. ``native`` proves
    # ``os.putenv("CURSOR_API_KEY")`` actually installed the name on POSIX.
    # Linux cannot bind GetEnvironmentVariableW under a win32 spoof, so
    # native stays 0 there; a real Windows process would report native=1
    # with the same none=0 hole this contract forbids.
    native = "0" if win32 and sys.platform != "win32" else "1"
    assert result.stdout.startswith(
        f"accepted=0 present=0 env_is_new=0 none=1 native={native}"
    ), (win32, posix_triple, result.stdout, result.stderr)


def test_copied_data_win32_environ_rejected_for_hermes_cli_main():
    body = "    import hermes_cli.main\n" + _COPIED_DATA_ENTRY_REPORT
    _assert_copied_data_entry_rejected(body, win32=True, posix_triple=False)


def test_copied_data_win32_environ_rejected_for_run_agent():
    body = "    import run_agent\n" + _COPIED_DATA_ENTRY_REPORT
    _assert_copied_data_entry_rejected(body, win32=True, posix_triple=False)


@pytest.mark.skipif(sys.platform == "win32", reason="posix.environ triple-rebind is POSIX-only")
def test_copied_data_posix_triple_rebind_rejected_for_hermes_cli_main():
    body = "    import hermes_cli.main\n" + _COPIED_DATA_ENTRY_REPORT
    _assert_copied_data_entry_rejected(body, win32=False, posix_triple=True)


@pytest.mark.skipif(sys.platform == "win32", reason="posix.environ triple-rebind is POSIX-only")
def test_copied_data_posix_triple_rebind_rejected_for_run_agent():
    body = "    import run_agent\n" + _COPIED_DATA_ENTRY_REPORT
    _assert_copied_data_entry_rejected(body, win32=False, posix_triple=True)
