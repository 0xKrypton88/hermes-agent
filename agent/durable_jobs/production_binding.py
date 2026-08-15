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
from agent.durable_jobs.injected_transports import (
    CursorCloudInjectedTransport,
    SlackInjectedTransport,
)

OWNER_CURSOR_REQUEST_ATTR = "_durable_job_cursor_request"
OWNER_SLACK_REQUEST_ATTR = "_durable_job_slack_request"
OWNER_CURSOR_TRANSPORT_ATTR = "_durable_job_cursor_transport"
OWNER_SLACK_TRANSPORT_ATTR = "_durable_job_slack_transport"
OWNER_RUNTIME_IDENTITY_ATTR = "_durable_job_runtime_identity"


def _builtin_instance_dict_descriptor_types() -> frozenset[type]:
    found: set[type] = set()

    class _Plain:
        pass

    plain = _Plain.__dict__.get("__dict__")
    if plain is not None:
        found.add(type(plain))

    class _SlottedDict:
        __slots__ = ("__dict__",)

    slotted = _SlottedDict.__dict__.get("__dict__")
    if slotted is not None:
        found.add(type(slotted))
    return frozenset(found)


_INSTANCE_DICT_DESCRIPTOR_TYPES = _builtin_instance_dict_descriptor_types()
_TYPE_MRO_DESCRIPTOR = type.__dict__["__mro__"]
_TYPE_DICT_DESCRIPTOR = type.__dict__["__dict__"]


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
    descriptors so custom metaclass ``__getattribute__`` hooks cannot
    observe or intercept the lookup. Accepts only builtin instance-dict
    descriptors and exact ``dict`` storage. Custom ``__dict__``
    properties/descriptors and objects without a builtin instance dict
    (including slotted objects) are denied. Seam names are never
    resolved through getattr/class lookup.
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
    if descriptor is None or type(descriptor) not in _INSTANCE_DICT_DESCRIPTOR_TYPES:
        return None
    try:
        storage = descriptor.__get__(owner, cls)
    except Exception:
        return None
    if type(storage) is not dict:
        return None
    return storage


def _owner_attr(owner: Any, name: str) -> Any:
    storage = _concrete_instance_storage(owner)
    if storage is None:
        return None
    return storage.get(name)


def _instance_attr(transport: Any, name: str) -> Any:
    try:
        return object.__getattribute__(transport, name)
    except AttributeError:
        return None


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
    return callable(request) and secret_ref == expected_ref


def _runtime_identity(owner: Any) -> Optional[tuple[str, str]]:
    raw = _owner_attr(owner, OWNER_RUNTIME_IDENTITY_ATTR)
    if type(raw) is not dict:
        return None
    workspace = raw.get("workspace_id")
    repository = raw.get("repository_identity")
    if type(workspace) is not str or type(repository) is not str:
        return None
    if workspace != workspace.strip() or repository != repository.strip():
        return None
    if not workspace or not repository:
        return None
    if set(raw) != {"workspace_id", "repository_identity"}:
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
