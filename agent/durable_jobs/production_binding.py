"""Lifecycle-owned production transport binding for Durable Job Lane.

Binds only the approved concrete transports
(``CursorCloudInjectedTransport``, ``SlackInjectedTransport``). Request
callables must already exist on the lifecycle owner or be passed in —
this module never constructs an HTTP/SDK client from config flags and
never reads credential values.

If the repository has no truthful provider request/client for Cursor or
Slack, the owner-owned request-port attributes are the injectable
dependency seam. Missing, wrong-typed, secret-ref-mismatched, or
identity-mismatched sources fail closed (empty attach kwargs).
"""

from __future__ import annotations

from collections.abc import Mapping as MappingABC
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
OWNER_CURSOR_CLIENT_ATTR = "_durable_job_cursor_client"
OWNER_SLACK_CLIENT_ATTR = "_durable_job_slack_client"
OWNER_SLACK_CHANNEL_ATTR = "_durable_job_slack_channel_id"
OWNER_SLACK_THREAD_ATTR = "_durable_job_slack_root_thread_ts"
OWNER_CREDENTIAL_RESOLVER_ATTR = "_durable_job_credential_resolver"


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


def _owner_attr(owner: Any, name: str) -> Any:
    if owner is None:
        return None
    return getattr(owner, name, None)


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


def _runtime_identity(owner: Any) -> Optional[tuple[str, str]] | bool:
    raw = _owner_attr(owner, OWNER_RUNTIME_IDENTITY_ATTR)
    if raw is None:
        return None
    if not isinstance(raw, MappingABC):
        return False
    workspace = raw.get("workspace_id")
    repository = raw.get("repository_identity")
    if not isinstance(workspace, str) or not workspace.strip():
        return False
    if not isinstance(repository, str) or not repository.strip():
        return False
    if set(raw) - {"workspace_id", "repository_identity"}:
        return False
    return (workspace.strip(), repository.strip())


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
    """Wrap already-injected clients. Never constructs HTTP/SDK clients."""
    if cursor_client is None:
        cursor_client = _owner_attr(owner, OWNER_CURSOR_CLIENT_ATTR)
    if slack_client is None:
        slack_client = _owner_attr(owner, OWNER_SLACK_CLIENT_ATTR)
    if slack_channel_id is None:
        slack_channel_id = _owner_attr(owner, OWNER_SLACK_CHANNEL_ATTR)
    if slack_root_thread_ts is None:
        slack_root_thread_ts = _owner_attr(owner, OWNER_SLACK_THREAD_ATTR)
    if credential_resolver is None:
        credential_resolver = _owner_attr(owner, OWNER_CREDENTIAL_RESOLVER_ATTR)
    if cursor_client is None or slack_client is None:
        return None
    binding = cfg.identity_binding
    if binding is None:
        return None
    if not isinstance(slack_channel_id, str) or not slack_channel_id.strip():
        return None
    if not isinstance(slack_root_thread_ts, str) or not slack_root_thread_ts.strip():
        return None
    if not cfg.cursor_secret_ref or not cfg.slack_secret_ref:
        return None
    from agent.durable_jobs.request_ports import (
        CursorCloudInjectedRequestPort,
        SlackInjectedRequestPort,
    )

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
                channel_id=slack_channel_id,
                repository_identity=binding.repository_identity,
                root_thread_ts=slack_root_thread_ts,
                credential_resolver=credential_resolver,
            ),
        )
    except (TypeError, ValueError, DurableJobsConfigError):
        return None


def _identity_matches(cfg: DurableJobsConfig, owner: Any) -> bool:
    runtime = _runtime_identity(owner)
    if runtime is None:
        return True
    if runtime is False:
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
        wrapped = _wrap_injected_clients(
            cfg,
            owner,
            cursor_client=cursor_client,
            slack_client=slack_client,
            slack_channel_id=slack_channel_id,
            slack_root_thread_ts=slack_root_thread_ts,
            credential_resolver=credential_resolver,
        )
        if wrapped is None:
            return {}
        cursor_request, slack_request = wrapped

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
