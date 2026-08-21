"""Fail-closed local preflight for the ENG-13 Linear Issue→Go receipt gate.

This module is deliberately side-effect free: it evaluates a supplied parsed
configuration only. It does not load secrets, bind ports, create a webhook,
dispatch an agent, or mutate Linear.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from gateway.platforms.webhook import _is_loopback_host


RECEIPT_MODE = "linear_issue_go"
REQUIRED_EVENT = "Issue"


@dataclass(frozen=True)
class LinearGoLaunchGateResult:
    """Configuration-only result for the bounded local preflight."""

    status: str  # BLOCKED | LOCAL_READY
    listener_ready: bool
    external_activation_ready: bool
    blockers: tuple[str, ...]


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _nonempty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _route_has_allowlist(route: Mapping[str, Any]) -> bool:
    state_ids = route.get("allowed_state_ids")
    return isinstance(state_ids, list) and any(
        _nonempty_string(item) for item in state_ids
    )


def assess_linear_go_launch_gate(config: Mapping[str, Any]) -> LinearGoLaunchGateResult:
    """Assess whether a config is safe for a *local-only* receipt listener.

    `LOCAL_READY` does not authorize or perform external activation. The caller
    must separately approve and execute any Gateway start/restart, tailnet
    routing, Linear webhook registration, or state transition.
    """
    platforms = _mapping(_mapping(config).get("platforms"))
    webhook = _mapping(platforms.get("webhook"))
    extra = _mapping(webhook.get("extra"))
    routes = _mapping(extra.get("routes"))
    blockers: list[str] = []

    if webhook.get("enabled") is not True:
        blockers.append("webhook_platform_disabled")

    host = extra.get("host", webhook.get("host"))
    if not _is_loopback_host(host if isinstance(host, str) else None):
        blockers.append("non_loopback_host")

    receipt_routes = [
        route
        for route in routes.values()
        if _mapping(route).get("receipt_only") == RECEIPT_MODE
    ]
    if len(receipt_routes) != 1:
        blockers.append("linear_go_route_count_not_one")

    # If no valid mode was found, report the relevant route's missing contract
    # independently so an operator can repair it without trial activation.
    candidate = _mapping(next(iter(routes.values()), {}))
    route = _mapping(receipt_routes[0]) if len(receipt_routes) == 1 else candidate
    if route.get("receipt_only") != RECEIPT_MODE:
        blockers.append("missing_receipt_only_mode")
    if not _route_has_allowlist(route):
        blockers.append("missing_allowed_state_ids")
    if not _nonempty_string(route.get("secret", extra.get("secret"))):
        blockers.append("missing_route_secret")
    events = route.get("events")
    if not isinstance(events, list) or REQUIRED_EVENT not in events:
        blockers.append("missing_issue_event_filter")

    blockers_tuple = tuple(dict.fromkeys(blockers))
    return LinearGoLaunchGateResult(
        status="LOCAL_READY" if not blockers_tuple else "BLOCKED",
        listener_ready=not blockers_tuple,
        # This guard is intentionally never true: external actions are a
        # distinct explicit decision boundary outside of a config preflight.
        external_activation_ready=False,
        blockers=blockers_tuple,
    )
