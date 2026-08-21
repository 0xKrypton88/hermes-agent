"""Lifecycle-owned production transport binding for Durable Job Lane.

Binds only the approved concrete transports
(``CursorCloudInjectedTransport``, ``SlackInjectedTransport``). Request
callables must already exist on the lifecycle owner or be passed in —
this module never constructs an HTTP/SDK client from config flags and
never reads credential values.

If the repository has no truthful provider request/client for Cursor or
Slack, the owner-owned request-port attributes are the injectable
dependency seam. Missing, invalid, or mismatched
``_durable_job_runtime_identity`` fails closed. Owner seam names are
read only from concrete instance ``__dict__`` storage — never from
properties, descriptors, or class attributes. Missing, wrong-typed,
secret-ref-mismatched, or identity-mismatched sources fail closed
(empty attach kwargs).
"""

from __future__ import annotations

from collections.abc import Mapping as MappingABC
from types import MappingProxyType
from typing import Any, Mapping, Optional

from agent.durable_jobs.config import (
    ADAPTER_MODE_INJECTED,
    DurableJobsConfig,
    DurableJobsConfigError,
    load_durable_jobs_config,
)
from agent.durable_jobs.preflight import _secret_ref_present
from agent.durable_jobs.injected_transports import (
    CursorCloudInjectedTransport,
    SlackInjectedTransport,
)

OWNER_CURSOR_REQUEST_ATTR = "_durable_job_cursor_request"
OWNER_SLACK_REQUEST_ATTR = "_durable_job_slack_request"
OWNER_CURSOR_TRANSPORT_ATTR = "_durable_job_cursor_transport"
OWNER_SLACK_TRANSPORT_ATTR = "_durable_job_slack_transport"
OWNER_RUNTIME_IDENTITY_ATTR = "_durable_job_runtime_identity"


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
_TYPE_MRO_DESCRIPTOR = type.__dict__["__mro__"]
_TYPE_DICT_DESCRIPTOR = type.__dict__["__dict__"]


def _is_trusted_instance_dict_descriptor(descriptor: Any) -> bool:
    descr_type = type(descriptor)
    for trusted in _TRUSTED_INSTANCE_DICT_DESCRIPTOR_TYPES:
        if descr_type is trusted:
            return True
    return False


def _load_raw_config(raw_config: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if raw_config is not None:
        return raw_config
    try:
        from hermes_cli.config import load_config

        loaded = load_config()
        if isinstance(loaded, MappingABC):
            return loaded
    except Exception:
        pass
    return {}


def _concrete_instance_storage(owner: Any) -> dict[str, Any] | None:
    """Return the owner's instance dict, or None when storage is unsafe.

    Walks the real type MRO and class namespaces through builtin ``type``
    descriptors so custom metaclass ``__getattribute__`` / ``__eq__`` /
    ``__hash__`` hooks cannot observe or intercept the lookup. Accepts
    only trusted builtin instance-dict descriptor type identities
    (``is``, never ``==`` / ``in`` / ``isinstance``) and exact ``dict``
    storage. Custom ``__dict__`` properties/descriptors and objects
    without a builtin instance dict (including slotted objects) are
    denied. Seam names are never resolved through getattr/class lookup.
    """
    if owner is None:
        return None
    cls = type(owner)
    try:
        mro = _TYPE_MRO_DESCRIPTOR.__get__(cls, type)
    except Exception:
        return None
    if type(mro) is not tuple:
        return None
    descriptor = None
    for base in mro:
        try:
            namespace = _TYPE_DICT_DESCRIPTOR.__get__(base, type)
        except Exception:
            return None
        if type(namespace) is not MappingProxyType:
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
        storage = descriptor.__get__(owner, cls)
    except Exception:
        return None
    if type(storage) is not dict:
        return None
    return storage


def _exact_str_item_pairs(storage: Any) -> list[tuple[str, Any]] | None:
    """Return ``(exact-str key, value)`` pairs, or None if storage is unsafe.

    Walks a builtin ``dict`` through ``dict.items`` so untrusted keys are
    never hashed, membership-tested, or compared. Any non-exact-str key
    fails closed. Values are not inspected, stringified, or compared.
    """
    if type(storage) is not dict:
        return None
    pairs: list[tuple[str, Any]] = []
    try:
        items = dict.items(storage)
    except Exception:
        return None
    for pair in items:
        if type(pair) is not tuple or tuple.__len__(pair) != 2:
            return None
        key = tuple.__getitem__(pair, 0)
        value = tuple.__getitem__(pair, 1)
        if type(key) is not str:
            return None
        pairs.append((key, value))
    return pairs


def _exact_str_pair_value(pairs: list[tuple[str, Any]] | None, name: str) -> Any:
    if pairs is None or type(name) is not str:
        return None
    matched = None
    seen = False
    for key, value in pairs:
        if str.__eq__(key, name):
            if seen:
                return None
            matched = value
            seen = True
    if not seen:
        return None
    return matched


def _owner_attr(owner: Any, name: str) -> Any:
    storage = _concrete_instance_storage(owner)
    return _exact_str_pair_value(_exact_str_item_pairs(storage), name)


def _instance_attr(transport: Any, name: str) -> Any:
    return _owner_attr(transport, name)


def _is_request_port(value: Any) -> bool:
    if value is None or not callable(value):
        return False
    if type(value) in (CursorCloudInjectedTransport, SlackInjectedTransport):
        return False
    return True


def _approved_transport(transport: Any, expected_cls: type, expected_ref: Optional[str]) -> bool:
    if expected_ref is None or type(transport) is not expected_cls:
        return False
    request = _instance_attr(transport, "_request")
    secret_ref = _instance_attr(transport, "_secret_ref")
    if type(expected_ref) is not str or type(secret_ref) is not str:
        return False
    return callable(request) and str.__eq__(secret_ref, expected_ref)


def _runtime_identity(owner: Any) -> Optional[tuple[str, str]]:
    raw = _owner_attr(owner, OWNER_RUNTIME_IDENTITY_ATTR)
    if type(raw) is not dict:
        return None
    pairs = _exact_str_item_pairs(raw)
    if pairs is None:
        return None
    workspace = None
    repository = None
    seen_workspace = False
    seen_repository = False
    for key, value in pairs:
        if str.__eq__(key, "workspace_id"):
            if seen_workspace:
                return None
            workspace = value
            seen_workspace = True
        elif str.__eq__(key, "repository_identity"):
            if seen_repository:
                return None
            repository = value
            seen_repository = True
        else:
            return None
    if not seen_workspace or not seen_repository:
        return None
    if type(workspace) is not str or type(repository) is not str:
        return None
    if not str.__eq__(workspace, str.strip(workspace)):
        return None
    if not str.__eq__(repository, str.strip(repository)):
        return None
    if str.__len__(workspace) == 0 or str.__len__(repository) == 0:
        return None
    return (workspace, repository)


def _identity_matches(cfg: DurableJobsConfig, owner: Any) -> bool:
    runtime = _runtime_identity(owner)
    if runtime is None:
        return False
    binding = cfg.identity_binding
    if binding is None:
        return False
    return runtime == (binding.workspace_id, binding.repository_identity)


def bind_production_transports(
    raw_config: Mapping[str, Any] | None = None,
    *,
    owner: Any = None,
    cursor_request: Any = None,
    slack_request: Any = None,
    cursor_transport: Any = None,
    slack_transport: Any = None,
) -> dict[str, Any]:
    """Return attach kwargs, or ``{}`` when production binding is unbound.

    Never constructs a network client, never reads secret values, and never
    invents a request callable because flags are on.
    """
    try:
        cfg = load_durable_jobs_config(_load_raw_config(raw_config))
    except DurableJobsConfigError:
        return {}

    if not cfg.enabled:
        return {}
    if (
        cfg.cursor_adapter_mode != ADAPTER_MODE_INJECTED
        or cfg.slack_adapter_mode != ADAPTER_MODE_INJECTED
    ):
        return {}
    if not _identity_matches(cfg, owner):
        return {}
    if (
        type(cfg.cursor_secret_ref) is not str
        or type(cfg.slack_secret_ref) is not str
        or not _secret_ref_present(cfg.cursor_secret_ref)
        or not _secret_ref_present(cfg.slack_secret_ref)
    ):
        return {}

    if cursor_transport is None:
        cursor_transport = _owner_attr(owner, OWNER_CURSOR_TRANSPORT_ATTR)
    if slack_transport is None:
        slack_transport = _owner_attr(owner, OWNER_SLACK_TRANSPORT_ATTR)
    if cursor_request is None:
        cursor_request = _owner_attr(owner, OWNER_CURSOR_REQUEST_ATTR)
    if slack_request is None:
        slack_request = _owner_attr(owner, OWNER_SLACK_REQUEST_ATTR)

    cursor_offered = cursor_transport is not None
    slack_offered = slack_transport is not None
    if cursor_offered or slack_offered:
        if not (
            cursor_offered
            and slack_offered
            and _approved_transport(
                cursor_transport,
                CursorCloudInjectedTransport,
                cfg.cursor_secret_ref,
            )
            and _approved_transport(
                slack_transport,
                SlackInjectedTransport,
                cfg.slack_secret_ref,
            )
        ):
            return {}
        return {
            "cursor_transport": cursor_transport,
            "slack_transport": slack_transport,
        }

    if not (
        _is_request_port(cursor_request)
        and _is_request_port(slack_request)
        and cfg.cursor_secret_ref
        and cfg.slack_secret_ref
    ):
        return {}

    try:
        return {
            "cursor_transport": CursorCloudInjectedTransport(
                request=cursor_request, secret_ref=cfg.cursor_secret_ref
            ),
            "slack_transport": SlackInjectedTransport(
                request=slack_request, secret_ref=cfg.slack_secret_ref
            ),
        }
    except (TypeError, ValueError, DurableJobsConfigError):
        return {}


def production_attach_kwargs(
    *,
    owner: Any = None,
    raw_config: Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Lifecycle helper: bind production transports for Gateway attach."""
    return bind_production_transports(raw_config, owner=owner, **kwargs)
