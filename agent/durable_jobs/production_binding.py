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
from agent.durable_jobs.request_ports import (
    CursorCloudInjectedRequestPort,
    SlackInjectedRequestPort,
)

OWNER_CURSOR_REQUEST_ATTR = "_durable_job_cursor_request"
OWNER_SLACK_REQUEST_ATTR = "_durable_job_slack_request"
OWNER_CURSOR_TRANSPORT_ATTR = "_durable_job_cursor_transport"
OWNER_SLACK_TRANSPORT_ATTR = "_durable_job_slack_transport"
OWNER_RUNTIME_IDENTITY_ATTR = "_durable_job_runtime_identity"
OWNER_CURSOR_CLIENT_ATTR = "_durable_job_cursor_client"
OWNER_SLACK_CLIENT_ATTR = "_durable_job_slack_client"
OWNER_SLACK_CHANNEL_ATTR = "_durable_job_slack_channel_id"
OWNER_SLACK_THREAD_ATTR = "_durable_job_slack_root_thread_ts"
OWNER_CREDENTIAL_RESOLVER_ATTR = "_durable_job_credential_resolver"


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


def _is_request_port(value: Any, expected_cls: type) -> bool:
    """Accept only an approved request-bound capability for its provider."""
    return type(value) is expected_cls and callable(value)


def _request_binding_matches(
    request: Any,
    expected_cls: type,
    expected: tuple[tuple[str, str], ...],
) -> bool:
    """Compare exact request-port bindings through builtin instance storage."""
    if not _is_request_port(request, expected_cls):
        return False
    pairs = _exact_str_item_pairs(_concrete_instance_storage(request))
    if pairs is None:
        return False
    for name, wanted in expected:
        actual = _exact_str_pair_value(pairs, name)
        if type(actual) is not str or type(wanted) is not str:
            return False
        if not str.__eq__(actual, wanted):
            return False
    return True


def _approved_transport(
    transport: Any,
    expected_cls: type,
    expected_request_cls: type,
    expected: tuple[tuple[str, str], ...] | str,
) -> bool:
    # Keep the legacy internal secret-ref-only probe compatible. Production
    # binding always supplies the full identity tuple below, so this does not
    # relax its workspace/repository/channel/thread checks.
    if type(expected) is str:
        expected = (("_secret_ref", expected),)
    elif type(expected) is not tuple:
        return False
    if type(transport) is not expected_cls:
        return False
    request = _instance_attr(transport, "_request")
    for name, wanted in expected:
        actual = _instance_attr(transport, name)
        if type(actual) is not str or not str.__eq__(actual, wanted):
            return False
    return _request_binding_matches(request, expected_request_cls, expected)


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


def _exact_owner_str(owner: Any, name: str, offered: Any = None) -> str | None:
    """Return a strip-stable exact str from kwargs or instance storage."""
    value = offered if offered is not None else _owner_attr(owner, name)
    if type(value) is not str:
        return None
    if not str.__eq__(value, str.strip(value)) or str.__len__(value) == 0:
        return None
    return value


def _wrap_injected_clients(
    cfg: DurableJobsConfig,
    owner: Any,
    *,
    cursor_client: Any = None,
    slack_client: Any = None,
    slack_channel_id: Any = None,
    slack_root_thread_ts: Any = None,
    credential_resolver: Any = None,
) -> Optional[tuple[Any, Any]]:
    """Wrap already-injected clients from instance storage or explicit kwargs.

    Never constructs HTTP/SDK clients and never reads owner seams through
    getattr, properties, descriptors, or class attributes.
    """
    if cursor_client is None:
        cursor_client = _owner_attr(owner, OWNER_CURSOR_CLIENT_ATTR)
    if slack_client is None:
        slack_client = _owner_attr(owner, OWNER_SLACK_CLIENT_ATTR)
    channel_id = _exact_owner_str(
        owner, OWNER_SLACK_CHANNEL_ATTR, slack_channel_id
    )
    thread_ts = _exact_owner_str(
        owner, OWNER_SLACK_THREAD_ATTR, slack_root_thread_ts
    )
    if credential_resolver is None:
        credential_resolver = _owner_attr(owner, OWNER_CREDENTIAL_RESOLVER_ATTR)
    if cursor_client is None or slack_client is None:
        return None
    if channel_id is None or thread_ts is None:
        return None
    if credential_resolver is not None and not callable(credential_resolver):
        return None
    binding = cfg.identity_binding
    if binding is None:
        return None
    if type(cfg.cursor_secret_ref) is not str or type(cfg.slack_secret_ref) is not str:
        return None
    try:
        return (
            CursorCloudInjectedRequestPort(
                client=cursor_client,
                secret_ref=cfg.cursor_secret_ref,
                workspace_id=binding.workspace_id,
                repository_identity=binding.repository_identity,
                credential_resolver=credential_resolver,
            ),
            SlackInjectedRequestPort(
                client=slack_client,
                secret_ref=cfg.slack_secret_ref,
                workspace_id=binding.workspace_id,
                channel_id=channel_id,
                repository_identity=binding.repository_identity,
                root_thread_ts=thread_ts,
                credential_resolver=credential_resolver,
            ),
        )
    except (TypeError, ValueError, DurableJobsConfigError):
        return None


def bind_production_transports(
    raw_config: Mapping[str, Any] | None = None,
    *,
    owner: Any = None,
    cursor_request: Any = None,
    slack_request: Any = None,
    cursor_transport: Any = None,
    slack_transport: Any = None,
    cursor_client: Any = None,
    slack_client: Any = None,
    slack_channel_id: Any = None,
    slack_root_thread_ts: Any = None,
    credential_resolver: Any = None,
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
        or not cfg.cursor_secret_ref.strip()
        or not cfg.slack_secret_ref.strip()
    ):
        return {}
    binding = cfg.identity_binding
    if binding is None:
        return {}
    channel_id = _exact_owner_str(owner, OWNER_SLACK_CHANNEL_ATTR, slack_channel_id)
    thread_ts = _exact_owner_str(owner, OWNER_SLACK_THREAD_ATTR, slack_root_thread_ts)
    if channel_id is None or thread_ts is None:
        return {}
    cursor_expected = (
        ("_workspace_id", binding.workspace_id),
        ("_repository_identity", binding.repository_identity),
        ("_secret_ref", cfg.cursor_secret_ref),
    )
    slack_expected = (
        ("_workspace_id", binding.workspace_id),
        ("_repository_identity", binding.repository_identity),
        ("_secret_ref", cfg.slack_secret_ref),
        ("_channel_id", channel_id),
        ("_root_thread_ts", thread_ts),
    )

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
                CursorCloudInjectedRequestPort,
                cursor_expected,
            )
            and _approved_transport(
                slack_transport,
                SlackInjectedTransport,
                SlackInjectedRequestPort,
                slack_expected,
            )
        ):
            return {}
        return {
            "cursor_transport": cursor_transport,
            "slack_transport": slack_transport,
        }

    if not (
        _request_binding_matches(
            cursor_request, CursorCloudInjectedRequestPort, cursor_expected
        )
        and _request_binding_matches(
            slack_request, SlackInjectedRequestPort, slack_expected
        )
        and cfg.cursor_secret_ref
        and cfg.slack_secret_ref
    ):
        wrapped = _wrap_injected_clients(
            cfg,
            owner,
            cursor_client=cursor_client,
            slack_client=slack_client,
            slack_channel_id=channel_id,
            slack_root_thread_ts=thread_ts,
            credential_resolver=credential_resolver,
        )
        if wrapped is None:
            return {}
        cursor_request, slack_request = wrapped

    try:
        return {
            "cursor_transport": CursorCloudInjectedTransport(
                request=cursor_request,
                secret_ref=cfg.cursor_secret_ref,
                workspace_id=binding.workspace_id,
                repository_identity=binding.repository_identity,
            ),
            "slack_transport": SlackInjectedTransport(
                request=slack_request,
                secret_ref=cfg.slack_secret_ref,
                workspace_id=binding.workspace_id,
                repository_identity=binding.repository_identity,
                channel_id=channel_id,
                root_thread_ts=thread_ts,
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
    """Lifecycle helper: bind transports and mandatory datastore authority."""
    authority_check = _owner_attr(owner, "durable_job_writer_authority_check")
    if not callable(authority_check):
        connection_provider = _owner_attr(
            owner, "durable_job_writer_authority_connection_provider"
        )
        if not callable(connection_provider):
            return {}
        try:
            from agent.durable_jobs.config import load_durable_jobs_config
            from agent.durable_jobs.writer_authority import (
                AuthorityTarget,
                DatastoreWriterAuthorityCheck,
            )

            config = load_durable_jobs_config(raw_config or {})
            if not config.writer_id or config.writer_authority_epoch <= 0:
                return {}
            authority_check = DatastoreWriterAuthorityCheck.from_connection_provider(
                connection_provider,
                expected=AuthorityTarget(
                    config.postgres_storage_id, config.postgres_environment_id
                ),
                requested_mode="new",
                writer_id=config.writer_id,
                minimum_epoch=config.writer_authority_epoch,
            )
        except (TypeError, ValueError, DurableJobsConfigError):
            return {}
    if not callable(getattr(authority_check, "effect_lease", None)):
        return {}
    bound = bind_production_transports(raw_config, owner=owner, **kwargs)
    if not bound:
        return {}
    bound["writer_authority_check"] = authority_check
    return bound
