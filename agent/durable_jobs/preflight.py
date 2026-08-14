"""Capability/preflight for the durable-job lane.

No sockets, no Slack/Cursor clients, no psycopg import on the SQLite
default path. Status never includes DSN, token, or other secret values.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
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


def _secret_ref_present(ref: Optional[str]) -> bool:
    if not ref:
        return False
    value = os.environ.get(ref)
    return bool(value)


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
    return callable(request) and isinstance(secret_ref, str) and bool(
        secret_ref.strip()
    )


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
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    return text or None


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
        if declared is None or declared != cfg.cursor_secret_ref:
            return "transport_secret_ref_mismatch"
        if not _secret_ref_present(declared):
            return "secret_refs_missing"
    if cfg.slack_adapter_mode == ADAPTER_MODE_INJECTED:
        declared = _transport_secret_ref(slack_transport)
        if declared is None or declared != cfg.slack_secret_ref:
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
