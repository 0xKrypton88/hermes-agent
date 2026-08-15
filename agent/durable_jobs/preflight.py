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
_GETSET_DESCRIPTOR_TYPE = type(_TYPE_DICT_DESCRIPTOR)
_MISSING = object()
_WIN32_ENVVAR_NOT_FOUND = 203


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


def _capture_os_environ_boundary():
    """Capture the real CPython ``os._Environ`` instance and backing dict."""
    try:
        environ_type = object.__getattribute__(os, "_Environ")
        environ = object.__getattribute__(os, "environ")
    except AttributeError:
        return None, None, None, None
    if type(environ) is not environ_type:
        return None, None, None, None
    if not _is_stdlib_os_environ_type(environ_type):
        return None, None, None, None
    try:
        namespace = _TYPE_DICT_DESCRIPTOR.__get__(environ_type, type)
    except Exception:
        return None, None, None, None
    if type(namespace) is not MappingProxyType:
        return None, None, None, None
    try:
        descriptor = namespace.get("__dict__")
    except Exception:
        return None, None, None, None
    if descriptor is None or type(descriptor) is not _GETSET_DESCRIPTOR_TYPE:
        return None, None, None, None
    try:
        storage = descriptor.__get__(environ, environ_type)
    except Exception:
        return None, None, None, None
    if type(storage) is not dict:
        return None, None, None, None
    data = _exact_str_dict_value(storage, "_data")
    if data is _MISSING or type(data) is not dict:
        return None, None, None, None
    return environ, environ_type, descriptor, data


(
    _CAPTURED_OS_ENVIRON,
    _CAPTURED_OS_ENVIRON_TYPE,
    _CAPTURED_OS_ENVIRON_DICT,
    _CAPTURED_OS_ENVIRON_DATA,
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
            wgetenv = None
            try:
                msvcrt = ctypes.CDLL("msvcrt")
                wgetenv = msvcrt._wgetenv
                wgetenv.argtypes = [ctypes.c_wchar_p]
                wgetenv.restype = ctypes.c_void_p
            except Exception:
                wgetenv = None
            return ("win32", get_var, wgetenv, ctypes)
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
    dereferenced. Windows uses ``GetEnvironmentVariableW`` size-only
    (and CRT ``_wgetenv`` as a pointer check). Values are never copied,
    logged, or compared. ``posix.environ`` / ``nt.environ`` are unused.
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
        wgetenv = tuple.__getitem__(probe, 2)
        ctypes_mod = tuple.__getitem__(probe, 3)
        try:
            size = get_var(name, None, 0)
        except Exception:
            size = 0
        if size != 0:
            return True
        try:
            err = ctypes_mod.get_last_error()
        except Exception:
            err = None
        if wgetenv is not None:
            try:
                ptr = wgetenv(name)
            except Exception:
                ptr = None
            if ptr is not None:
                return True
        if err == _WIN32_ENVVAR_NOT_FOUND or err == 0:
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
    """Return the captured CPython ``os._Environ._data`` dict, or None.

    The backing dict object captured at import must still be present
    under the authentic ``os._Environ`` instance. A replaced exact
    ``dict`` fails closed. This helper is a tamper check only — name
    presence uses the native child-inherited environment, never this
    cache and never ``posix.environ`` / ``nt.environ``.
    """
    environ = _CAPTURED_OS_ENVIRON
    environ_type = _CAPTURED_OS_ENVIRON_TYPE
    descriptor = _CAPTURED_OS_ENVIRON_DICT
    captured = _CAPTURED_OS_ENVIRON_DATA
    if (
        environ is None
        or environ_type is None
        or descriptor is None
        or captured is None
    ):
        return None
    if type(environ) is not environ_type:
        return None
    if type(descriptor) is not _GETSET_DESCRIPTOR_TYPE:
        return None
    if type(captured) is not dict:
        return None
    try:
        storage = descriptor.__get__(environ, environ_type)
    except Exception:
        return None
    if type(storage) is not dict:
        return None
    data = _exact_str_dict_value(storage, "_data")
    if data is _MISSING or type(data) is not dict:
        return None
    if data is not captured:
        return None
    return data


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
    try:
        return object.__getattribute__(transport, name)
    except AttributeError:
        return None


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
    """Validate active config without external effects."""
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
        dispatch_allowed=bool(cfg.dispatch_allowed and constructible),
        runtime_ready=runtime_ready,
        reasons=tuple(reasons),
        backend=cfg.resolved_backend,
        cursor_adapter_mode=cfg.cursor_adapter_mode,
        slack_adapter_mode=cfg.slack_adapter_mode,
        secret_refs_configured=secret_refs_configured,
        secret_refs_present=secret_refs_present,
        transport_capability=transport_capability,
    )
