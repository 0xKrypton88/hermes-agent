"""Capability/preflight for the durable-job lane.

No sockets, no Slack/Cursor clients, no psycopg import on the SQLite
default path. Status never includes DSN, token, or other secret values.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping, Optional

from agent.durable_jobs.config import (
    ADAPTER_MODE_INJECTED,
    BACKEND_POSTGRESQL,
    DurableJobsConfig,
    DurableJobsConfigError,
    load_durable_jobs_config,
)
from agent.durable_jobs.injected_transports import (
    CursorCloudInjectedTransport,
    SlackInjectedTransport,
)
from agent.durable_jobs.redaction import redact_secret_text


@dataclass(frozen=True)
class DurableJobsPreflight:
    constructible: bool
    dispatch_allowed: bool
    runtime_ready: bool
    reasons: tuple[str, ...]
    backend: Optional[str]
    cursor_adapter_mode: Optional[str]
    slack_adapter_mode: Optional[str]
    secret_refs_configured: bool
    secret_refs_present: bool
    transport_capability: bool = False

    def __repr__(self) -> str:
        return redact_secret_text(
            "DurableJobsPreflight("
            f"constructible={self.constructible!r}, "
            f"dispatch_allowed={self.dispatch_allowed!r}, "
            f"runtime_ready={self.runtime_ready!r}, "
            f"reasons={self.reasons!r}, "
            f"backend={self.backend!r}, "
            f"cursor_adapter_mode={self.cursor_adapter_mode!r}, "
            f"slack_adapter_mode={self.slack_adapter_mode!r}, "
            f"secret_refs_configured={self.secret_refs_configured!r}, "
            f"secret_refs_present={self.secret_refs_present!r}, "
            f"transport_capability={self.transport_capability!r})"
        )


_TYPE_DICT_DESCRIPTOR = type.__dict__["__dict__"]
_TYPE_MRO_DESCRIPTOR = type.__dict__["__mro__"]
_GETSET_DESCRIPTOR_TYPE = type(_TYPE_DICT_DESCRIPTOR)
_MISSING = object()
_WIN32_ENVVAR_NOT_FOUND = 203


def _trusted_instance_dict_descriptor_types() -> tuple[type, ...]:
    found: list[type] = []

    class _Plain:
        pass

    plain = _Plain.__dict__.get("__dict__")
    if plain is not None:
        found.append(type(plain))

    class _SlottedDict:
        __slots__ = ("__dict__",)

    slotted = _SlottedDict.__dict__.get("__dict__")
    if slotted is not None:
        slotted_type = type(slotted)
        already = False
        for existing in found:
            if slotted_type is existing:
                already = True
                break
        if not already:
            found.append(slotted_type)
    return tuple(found)


_TRUSTED_INSTANCE_DICT_DESCRIPTOR_TYPES = _trusted_instance_dict_descriptor_types()
_MODULE_TYPE = type(sys)
_MODULE_TYPE_NAMESPACE = _TYPE_DICT_DESCRIPTOR.__get__(_MODULE_TYPE, type)
_MODULE_DICT_DESCRIPTOR = (
    _MODULE_TYPE_NAMESPACE.get("__dict__")
    if type(_MODULE_TYPE_NAMESPACE) is MappingProxyType
    else None
)


def _is_trusted_instance_dict_descriptor(descriptor: Any) -> bool:
    descr_type = type(descriptor)
    for trusted in _TRUSTED_INSTANCE_DICT_DESCRIPTOR_TYPES:
        if descr_type is trusted:
            return True
    return False


def _is_stdlib_os_environ_type(environ_type: Any) -> bool:
    """True when ``environ_type`` is CPython's stdlib ``os._Environ``."""
    try:
        os_file = object.__getattribute__(os, "__file__")
        setitem = object.__getattribute__(environ_type, "__setitem__")
    except AttributeError:
        return False
    if type(os_file) is not str:
        return False
    if type(setitem) is not type(lambda: None):
        return False
    try:
        code = object.__getattribute__(setitem, "__code__")
        qualname = object.__getattribute__(setitem, "__qualname__")
        module = object.__getattribute__(setitem, "__module__")
        filename = object.__getattribute__(code, "co_filename")
    except AttributeError:
        return False
    if type(qualname) is not str or type(module) is not str or type(filename) is not str:
        return False
    if not str.__eq__(qualname, "_Environ.__setitem__"):
        return False
    if not str.__eq__(module, "os"):
        return False
    if str.__eq__(filename, "<frozen os>"):
        return True
    return str.__eq__(filename, os_file)


def _exact_str_dict_value(storage: Any, name: str):
    """Return a builtin-dict value for an exact-str key, or ``_MISSING``.

    Walks ``dict.items`` so untrusted keys are never hashed or compared.
    Any non-exact-str key fails closed. Values are not inspected.
    """
    if type(storage) is not dict or type(name) is not str:
        return _MISSING
    found = False
    value = None
    try:
        items = dict.items(storage)
    except Exception:
        return _MISSING
    for pair in items:
        if type(pair) is not tuple or tuple.__len__(pair) != 2:
            return _MISSING
        key = tuple.__getitem__(pair, 0)
        item = tuple.__getitem__(pair, 1)
        if type(key) is not str:
            return _MISSING
        if str.__eq__(key, name):
            if found:
                return _MISSING
            found = True
            value = item
    if not found:
        return _MISSING
    return value


def _type_namespace(cls: Any):
    try:
        namespace = _TYPE_DICT_DESCRIPTOR.__get__(cls, type)
    except Exception:
        return None
    if type(namespace) is not MappingProxyType:
        return None
    return namespace


def _sys_platform():
    try:
        platform = object.__getattribute__(sys, "platform")
    except AttributeError:
        return None
    if type(platform) is not str:
        return None
    return platform


def _stdlib_os_module():
    try:
        modules = object.__getattribute__(sys, "modules")
    except AttributeError:
        return None
    if type(modules) is not dict:
        return None
    module = _exact_str_dict_value(modules, "os")
    if module is _MISSING:
        return None
    return module


_PIN_SEAL_ATTR = "__hermes_trusted_environ_pin__"


def _stdlib_os_environ_parts():
    """Return ``(os_storage, environ, environ_type, descriptor, data)``."""
    module = _stdlib_os_module()
    storage = _module_storage(module)
    if storage is None:
        return None, None, None, None, None
    environ_type = _exact_str_dict_value(storage, "_Environ")
    environ = _exact_str_dict_value(storage, "environ")
    if environ_type is _MISSING or environ is _MISSING:
        return None, None, None, None, None
    if type(environ) is not environ_type:
        return None, None, None, None, None
    if not _is_stdlib_os_environ_type(environ_type):
        return None, None, None, None, None
    namespace = _type_namespace(environ_type)
    if namespace is None:
        return None, None, None, None, None
    try:
        descriptor = namespace.get("__dict__")
    except Exception:
        return None, None, None, None, None
    if descriptor is None or type(descriptor) is not _GETSET_DESCRIPTOR_TYPE:
        return None, None, None, None, None
    data = _environ_data_from_instance(environ, environ_type, descriptor)
    if data is _MISSING:
        return None, None, None, None, None
    return storage, environ, environ_type, descriptor, data


def _environ_instance_from_descriptor(environ, environ_type, descriptor):
    if type(environ) is not environ_type:
        return None
    if type(descriptor) is not _GETSET_DESCRIPTOR_TYPE:
        return None
    try:
        storage = descriptor.__get__(environ, environ_type)
    except Exception:
        return None
    if type(storage) is not dict:
        return None
    return storage


def _read_trusted_startup_pins():
    """Live-read the process-bound pin seal. Not a mintable module global."""
    os_storage, environ, environ_type, descriptor, _data = _stdlib_os_environ_parts()
    _ = os_storage
    if environ is None:
        return False, None, None
    instance = _environ_instance_from_descriptor(environ, environ_type, descriptor)
    if instance is None:
        return False, None, None
    payload = _exact_str_dict_value(instance, _PIN_SEAL_ATTR)
    if payload is _MISSING or type(payload) is not tuple or tuple.__len__(payload) != 2:
        return False, None, None
    pinned_environ = tuple.__getitem__(payload, 0)
    pinned_posix = tuple.__getitem__(payload, 1)
    if pinned_environ is not environ:
        return False, None, None
    return True, pinned_environ, pinned_posix


# Sticky fail-closed after the first missing pin. Not the trust root:
# a True result is always re-checked against the process-bound seal.
_PIN_DENIED = []


def _trusted_startup_pins():
    """Return ``(ready, pinned_os_environ, pinned_posix_environ)``.

    Provenance is a write-once seal on the genuine ``os.environ`` instance
    dict. Does not import ``hermes_environ_startup`` and does not call
    capture. A ``types.ModuleType`` injection or mutated module globals
    is not provenance.

    The first missing pin is sticky so a later ``sys.modules`` /
    module-global mutation cannot take the result from false to true.
    A cached True is never trusted on its own; ready still requires the
    live process-bound seal.
    """
    if _PIN_DENIED:
        return False, None, None
    result = _read_trusted_startup_pins()
    if result[0] is not True:
        _PIN_DENIED.append(True)
        return False, None, None
    return result


def _module_storage(module: Any):
    if module is None:
        return None
    module_type = type(module)
    namespace = _type_namespace(module_type)
    if namespace is None:
        return None
    try:
        descriptor = namespace.get("__dict__")
    except Exception:
        return None
    if descriptor is None or descriptor is not _MODULE_DICT_DESCRIPTOR:
        return None
    try:
        storage = descriptor.__get__(module, module_type)
    except Exception:
        return None
    if type(storage) is not dict:
        return None
    return storage


def _concrete_instance_storage(obj: Any):
    """Return builtin instance dict storage, or None when unsafe.

    Walks the real type MRO and class namespaces through builtin ``type``
    descriptors. Accepts only trusted instance-dict descriptor type
    identities (``is``) and exact ``dict`` storage. Does not execute
    custom ``__dict__`` descriptors, properties, or metaclass ``__eq__``.
    """
    if obj is None:
        return None
    cls = type(obj)
    try:
        mro = _TYPE_MRO_DESCRIPTOR.__get__(cls, type)
    except Exception:
        return None
    if type(mro) is not tuple:
        return None
    descriptor = None
    for base in mro:
        namespace = _type_namespace(base)
        if namespace is None:
            return None
        try:
            found = namespace.get("__dict__")
        except Exception:
            return None
        if found is not None:
            descriptor = found
            break
    if descriptor is None or not _is_trusted_instance_dict_descriptor(descriptor):
        return None
    try:
        storage = descriptor.__get__(obj, cls)
    except Exception:
        return None
    if type(storage) is not dict:
        return None
    return storage


def _environ_data_from_instance(environ: Any, environ_type: Any, descriptor: Any):
    if type(environ) is not environ_type:
        return _MISSING
    if type(descriptor) is not _GETSET_DESCRIPTOR_TYPE:
        return _MISSING
    try:
        storage = descriptor.__get__(environ, environ_type)
    except Exception:
        return _MISSING
    if type(storage) is not dict:
        return _MISSING
    data = _exact_str_dict_value(storage, "_data")
    if data is _MISSING or type(data) is not dict:
        return _MISSING
    return data


def _capture_os_environ_boundary():
    """Pin ``os.environ`` only when a prior trusted startup pin exists.

    A replaced ``os.environ`` / ``os.environb`` / ``posix.environ``
    mapping is never provenance. This function does not import
    ``hermes_environ_startup`` and does not call capture. Missing or
    unready pins fail closed on every platform.

    POSIX: ``_data`` must be the posix mapping pinned at trusted
    bootstrap (``is``), not the live ``posix.environ`` attribute.
    ``environb`` must share that same pinned object.

    Windows: ``GetEnvironmentVariableW`` is the presence authority.
    ``environb`` must stay absent. ``nt.environ`` is a copy, not
    ``_data``, and is never a presence oracle. Provenance is
    ``os.environ is`` the pinned startup singleton.
    """
    storage, environ, environ_type, descriptor, data = _stdlib_os_environ_parts()
    if environ is None:
        return None, None, None, None, None
    platform = _sys_platform()
    if platform is None:
        return None, None, None, None, None
    ready, pinned_environ, pinned_posix = _trusted_startup_pins()
    if not ready or environ is not pinned_environ:
        return None, None, None, None, None
    if str.__eq__(platform, "win32"):
        environb = _exact_str_dict_value(storage, "environb")
        if environb is not _MISSING:
            return None, None, None, None, None
        return environ, environ_type, descriptor, data, None
    if pinned_posix is None or type(pinned_posix) is not dict:
        return None, None, None, None, None
    if data is not pinned_posix:
        return None, None, None, None, None
    environb = _exact_str_dict_value(storage, "environb")
    if environb is _MISSING or type(environb) is not environ_type:
        return None, None, None, None, None
    sibling = _environ_data_from_instance(environb, environ_type, descriptor)
    if sibling is _MISSING or sibling is not pinned_posix:
        return None, None, None, None, None
    return environ, environ_type, descriptor, data, environb


(
    _CAPTURED_OS_ENVIRON,
    _CAPTURED_OS_ENVIRON_TYPE,
    _CAPTURED_OS_ENVIRON_DICT,
    _CAPTURED_OS_ENVIRON_DATA,
    _CAPTURED_OS_ENVIRONB,
) = _capture_os_environ_boundary()


def _bind_native_env_name_probe():
    """Bind a C-level name-presence probe. Never wraps credential values."""
    try:
        import ctypes

        platform = object.__getattribute__(sys, "platform")
    except Exception:
        return None
    if type(platform) is not str:
        return None
    if str.__eq__(platform, "win32"):
        try:
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            get_var = kernel32.GetEnvironmentVariableW
            get_var.argtypes = [
                ctypes.c_wchar_p,
                ctypes.c_wchar_p,
                ctypes.c_uint32,
            ]
            get_var.restype = ctypes.c_uint32
            return ("win32", get_var, None, ctypes)
        except Exception:
            return None
    try:
        libc = ctypes.CDLL(None)
        getenv = libc.getenv
        getenv.argtypes = [ctypes.c_char_p]
        getenv.restype = ctypes.c_void_p
        return ("posix", getenv, None, None)
    except Exception:
        return None


_NATIVE_ENV_NAME_PROBE = _bind_native_env_name_probe()


def _native_env_name_present(name: str) -> bool | None:
    """True/False for child-inherited name presence; None fail-closed.

    POSIX uses libc ``getenv`` with a void pointer so the value is never
    dereferenced. Windows uses ``GetEnvironmentVariableW`` size-only as
    the sole authority. ``ERROR_ENVVAR_NOT_FOUND`` is absent and is
    never overridden by a stale CRT ``_wgetenv`` cache. Empty names are
    present (nonzero size) without copying the value. Values are never
    copied, logged, or compared. ``posix.environ`` / ``nt.environ`` are
    unused.
    """
    if type(name) is not str or str.__len__(name) == 0:
        return None
    probe = _NATIVE_ENV_NAME_PROBE
    if probe is None or type(probe) is not tuple:
        return None
    kind = tuple.__getitem__(probe, 0)
    if type(kind) is not str:
        return None
    if str.__eq__(kind, "win32"):
        get_var = tuple.__getitem__(probe, 1)
        ctypes_mod = tuple.__getitem__(probe, 3)
        try:
            size = get_var(name, None, 0)
        except Exception:
            return None
        if size != 0:
            return True
        try:
            err = ctypes_mod.get_last_error()
        except Exception:
            return False
        if err == _WIN32_ENVVAR_NOT_FOUND:
            return False
        return False
    getenv = tuple.__getitem__(probe, 1)
    try:
        encoded = str.encode(name, "ascii")
    except UnicodeEncodeError:
        return False
    try:
        ptr = getenv(encoded)
    except Exception:
        return None
    return ptr is not None


def _process_environ_dict() -> dict | None:
    """Return the pinned startup ``os.environ._data`` dict, or None.

    Proves the current ``os.environ`` binding is still the captured
    singleton and that its ``_data`` object is still the captured
    dict. Capture required a prior trusted startup pin; this function
    does not import or call capture, and does not re-read live
    ``posix.environ`` / ``nt.environ``. A pin that appears only after
    this module was imported cannot revive a failed capture.
    Windows requires ``environb`` stay absent; presence stays on
    ``GetEnvironmentVariableW``.
    """
    environ = _CAPTURED_OS_ENVIRON
    environ_type = _CAPTURED_OS_ENVIRON_TYPE
    descriptor = _CAPTURED_OS_ENVIRON_DICT
    captured = _CAPTURED_OS_ENVIRON_DATA
    environb = _CAPTURED_OS_ENVIRONB
    if (
        environ is None
        or environ_type is None
        or descriptor is None
        or captured is None
    ):
        return None
    if type(environ) is not environ_type:
        return None
    if type(captured) is not dict:
        return None
    platform = _sys_platform()
    if platform is None:
        return None
    module_storage = _module_storage(_stdlib_os_module())
    if module_storage is None:
        return None
    current = _exact_str_dict_value(module_storage, "environ")
    if current is _MISSING or current is not environ:
        return None
    data = _environ_data_from_instance(environ, environ_type, descriptor)
    if data is _MISSING or data is not captured:
        return None
    current_data = _environ_data_from_instance(current, environ_type, descriptor)
    if current_data is _MISSING or current_data is not captured:
        return None
    ready, pinned_environ, pinned_posix = _trusted_startup_pins()
    if not ready or environ is not pinned_environ or current is not pinned_environ:
        return None
    if str.__eq__(platform, "win32"):
        if environb is not None:
            return None
        current_b = _exact_str_dict_value(module_storage, "environb")
        if current_b is not _MISSING:
            return None
        return captured
    if pinned_posix is None or captured is not pinned_posix:
        return None
    if environb is None or type(environb) is not environ_type:
        return None
    current_b = _exact_str_dict_value(module_storage, "environb")
    if current_b is _MISSING or current_b is not environb:
        return None
    sibling = _environ_data_from_instance(environb, environ_type, descriptor)
    if sibling is _MISSING or sibling is not captured:
        return None
    return captured


def _secret_ref_present(ref: Optional[str]) -> bool:
    """True when the child-inherited process environment has this *name*.

    Accepts only an exact ``str`` name. Requires an intact captured
    ``os._Environ`` boundary, then probes native name presence without
    retrieving, resolving, stringifying, logging, or comparing credential
    values. Never calls overridable ``os.environ`` mapping APIs or user
    equality/hash hooks, and never consults ``posix.environ`` /
    ``nt.environ`` or a replaced ``_data`` cache.
    """
    if type(ref) is not str or str.__len__(ref) == 0:
        return False
    if _process_environ_dict() is None:
        return False
    native = _native_env_name_present(ref)
    if native is None:
        return False
    return native is True


def _storage_reasons(cfg: DurableJobsConfig) -> list[str]:
    reasons: list[str] = []
    if cfg.resolved_backend == BACKEND_POSTGRESQL:
        reasons.append("lane_ledgers_require_sqlite")
        return reasons
    if not cfg.sqlite_lane_storage_ready():
        reasons.append("sqlite_storage_incomplete")
        return reasons
    sqlite_path = cfg.sqlite_path
    checkpoint = cfg.checkpoint_sqlite_path
    if sqlite_path is not None and sqlite_path.name == "state.db":
        reasons.append("refuses_hermes_state_db")
    if (
        sqlite_path is not None
        and checkpoint is not None
        and sqlite_path.resolve() == checkpoint.resolve()
    ):
        reasons.append("sqlite_paths_must_be_distinct")
    return reasons


def _instance_attr(transport: Any, name: str) -> Any:
    """Read a seam from verified builtin instance-dict storage only.

    Data descriptors, properties, metaclass hooks, and colliding
    untrusted keys are never executed. Missing or unsafe storage
    fails closed.
    """
    if type(name) is not str:
        return None
    storage = _concrete_instance_storage(transport)
    if storage is None:
        return None
    value = _exact_str_dict_value(storage, name)
    if value is _MISSING:
        return None
    return value


def _concrete_injected_transport(transport: Any, expected_cls: type) -> bool:
    """Exact concrete type plus a real callable request operation.

    Subclasses and overridable helpers are not a supported readiness contract.
    """
    if type(transport) is not expected_cls:
        return False
    request = _instance_attr(transport, "_request")
    secret_ref = _instance_attr(transport, "_secret_ref")
    if type(secret_ref) is not str:
        return False
    return callable(request) and str.__len__(str.strip(secret_ref)) != 0


def _injected_transport_capability(
    cfg: DurableJobsConfig,
    cursor_transport: Any,
    slack_transport: Any,
) -> bool:
    if cfg.cursor_adapter_mode == ADAPTER_MODE_INJECTED:
        if not _concrete_injected_transport(
            cursor_transport, CursorCloudInjectedTransport
        ):
            return False
    if cfg.slack_adapter_mode == ADAPTER_MODE_INJECTED:
        if not _concrete_injected_transport(
            slack_transport, SlackInjectedTransport
        ):
            return False
    return True


def _transport_secret_ref(transport: Any) -> Optional[str]:
    if type(transport) not in (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    ):
        return None
    raw = _instance_attr(transport, "_secret_ref")
    if type(raw) is not str:
        return None
    text = str.strip(raw)
    if type(text) is not str or str.__len__(text) == 0:
        return None
    return text


def _injected_secret_ref_binding_reason(
    cfg: DurableJobsConfig,
    cursor_transport: Any,
    slack_transport: Any,
) -> Optional[str]:
    """Fail-closed reason when an injected transport is not bound to config refs.

    Reasons never include secret values or raw tokens.
    """
    if cfg.cursor_adapter_mode == ADAPTER_MODE_INJECTED:
        declared = _transport_secret_ref(cursor_transport)
        expected = cfg.cursor_secret_ref
        if (
            declared is None
            or type(expected) is not str
            or not str.__eq__(declared, expected)
        ):
            return "transport_secret_ref_mismatch"
        if not _secret_ref_present(declared):
            return "secret_refs_missing"
    if cfg.slack_adapter_mode == ADAPTER_MODE_INJECTED:
        declared = _transport_secret_ref(slack_transport)
        expected = cfg.slack_secret_ref
        if (
            declared is None
            or type(expected) is not str
            or not str.__eq__(declared, expected)
        ):
            return "transport_secret_ref_mismatch"
        if not _secret_ref_present(declared):
            return "secret_refs_missing"
    return None


def preflight_durable_jobs(
    raw: Mapping[str, Any] | None,
    *,
    cursor_transport: Any = None,
    slack_transport: Any = None,
) -> DurableJobsPreflight:
    """Validate active config without external effects.

    ``dispatch_allowed`` is true only when config dispatch gates and a
    complete verified production runtime (secrets, transport capability,
    and secret-ref bindings) are all present. Constructible-but-incomplete
    configs stay closed.
    """
    try:
        cfg = load_durable_jobs_config(raw)
    except DurableJobsConfigError as exc:
        _ = exc
        return DurableJobsPreflight(
            constructible=False,
            dispatch_allowed=False,
            runtime_ready=False,
            reasons=("invalid_config",),
            backend=None,
            cursor_adapter_mode=None,
            slack_adapter_mode=None,
            secret_refs_configured=False,
            secret_refs_present=False,
            transport_capability=False,
        )

    reasons: list[str] = []
    if not cfg.enabled:
        reasons.append("disabled")
    reasons.extend(_storage_reasons(cfg))
    if not cfg.adapter_modes_explicit():
        reasons.append("adapter_modes_not_explicit")
    if not cfg.bindings_complete():
        reasons.append("bindings_incomplete")

    secret_refs_configured = bool(cfg.cursor_secret_ref and cfg.slack_secret_ref)
    secret_refs_present = False
    if cfg.cursor_adapter_mode == ADAPTER_MODE_INJECTED or cfg.slack_adapter_mode == (
        ADAPTER_MODE_INJECTED
    ):
        cursor_ok = (
            cfg.cursor_adapter_mode != ADAPTER_MODE_INJECTED
            or _secret_ref_present(cfg.cursor_secret_ref)
        )
        slack_ok = (
            cfg.slack_adapter_mode != ADAPTER_MODE_INJECTED
            or _secret_ref_present(cfg.slack_secret_ref)
        )
        secret_refs_present = bool(cursor_ok and slack_ok)
    else:
        secret_refs_present = True

    constructible = (
        cfg.enabled
        and cfg.sqlite_lane_storage_ready()
        and cfg.adapter_modes_explicit()
        and cfg.bindings_complete()
        and "refuses_hermes_state_db" not in reasons
        and "sqlite_paths_must_be_distinct" not in reasons
    )
    if constructible and not secret_refs_present:
        reasons.append("secret_refs_missing")
    transport_capability = _injected_transport_capability(
        cfg, cursor_transport, slack_transport
    )
    binding_reason = (
        _injected_secret_ref_binding_reason(
            cfg, cursor_transport, slack_transport
        )
        if transport_capability
        else None
    )
    if constructible and secret_refs_present and not transport_capability:
        reasons.append("transport_capability_missing")
    if constructible and transport_capability and binding_reason:
        if binding_reason not in reasons:
            reasons.append(binding_reason)
    runtime_ready = (
        constructible
        and secret_refs_present
        and transport_capability
        and binding_reason is None
    )
    return DurableJobsPreflight(
        constructible=constructible,
        dispatch_allowed=bool(cfg.dispatch_allowed and runtime_ready),
        runtime_ready=runtime_ready,
        reasons=tuple(reasons),
        backend=cfg.resolved_backend,
        cursor_adapter_mode=cfg.cursor_adapter_mode,
        slack_adapter_mode=cfg.slack_adapter_mode,
        secret_refs_configured=secret_refs_configured,
        secret_refs_present=secret_refs_present,
        transport_capability=transport_capability,
    )
