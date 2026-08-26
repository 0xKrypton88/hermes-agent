"""Explicit offline runtime ingress for the durable session-handoff pilot.

This module owns no configuration lookup, scheduler, client construction, or
external transport.  A test/shadow caller must inject the already-authorized
durable lane and every projection port, then attach the runtime to one agent.
Absent that attachment the universal turn boundary does not import this module.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from agent.durable_jobs.session_handoff import (
    SemanticWaypoint,
    SessionHandoff,
    SessionHandoffConfig,
)


class SessionHandoffRuntimeDisabled(RuntimeError):
    """The strict, literal offline shadow/test gate was not open."""


@dataclass(frozen=True)
class SessionHandoffIngress:
    """One request-bound handoff presented at the real turn ingress."""

    job_id: str
    parent_session_id: str
    handoff: SessionHandoff
    provider: str
    model: str
    used_tokens: int
    context_tokens: int
    manual_resume: bool = False


@dataclass(frozen=True)
class SessionHandoffRuntime:
    """Injected controller for the offline shadow/test product boundary."""

    lane: Any
    linear: Any
    slack: Any
    sessions: Any
    request: SessionHandoffIngress
    waypoint_policy: Callable[[Any, Any, SessionHandoffIngress], SemanticWaypoint]
    enabled: bool = False
    mode: str = "off"

    def __post_init__(self) -> None:
        if type(self.enabled) is not bool:
            raise SessionHandoffRuntimeDisabled("enabled must be a literal bool")
        if self.enabled is not True or self.mode != "offline_shadow_test":
            raise SessionHandoffRuntimeDisabled(
                "session handoff runtime requires enabled=True and "
                "mode='offline_shadow_test'"
            )
        if type(self.request.manual_resume) is not bool:
            raise ValueError("manual_resume must be a literal bool")

    def ingress(self, agent: Any, user_message: Any) -> Any:
        """Evaluate the safe waypoint and resume through the canonical lane."""
        waypoint = self.waypoint_policy(agent, user_message, self.request)
        if not isinstance(waypoint, SemanticWaypoint):
            raise TypeError("waypoint policy must return SemanticWaypoint")
        handoff_config = SessionHandoffConfig(
            policies=SessionHandoffConfig.default().policies,
            enabled=True,
            shadow=False,
        )
        pressure = handoff_config.evaluate(
            self.request.provider,
            self.request.model,
            self.request.used_tokens,
            context_tokens=self.request.context_tokens,
        )
        return self.lane.resume_session_handoff(
            job_id=self.request.job_id,
            parent_session_id=self.request.parent_session_id,
            handoff=self.request.handoff,
            waypoint=waypoint,
            pressure=pressure,
            linear=self.linear,
            slack=self.slack,
            sessions=self.sessions,
            handoff_config=handoff_config,
            manual_resume=self.request.manual_resume,
        )


def attach_session_handoff_runtime(
    agent: Any, runtime: SessionHandoffRuntime, *, enabled: bool = False
) -> None:
    """Attach only an already gated runtime; never infer enablement from config."""
    if type(enabled) is not bool or enabled is not True:
        raise SessionHandoffRuntimeDisabled("attachment requires enabled=True")
    if not isinstance(runtime, SessionHandoffRuntime):
        raise TypeError("runtime must be SessionHandoffRuntime")
    active_session_id = str(getattr(agent, "session_id", "") or "")
    if not active_session_id:
        raise ValueError("session handoff attachment requires an active session_id")
    if runtime.request.parent_session_id != active_session_id:
        raise ValueError(
            "request.parent_session_id does not match the active agent session_id"
        )
    agent._session_handoff_runtime = runtime
    agent._session_handoff_authority = {
        "parent_session_id": active_session_id,
        "consumed": False,
        "turn_id": None,
    }


def discard_attached_session_handoff_ingress(agent: Any, *, turn_id: str) -> None:
    """Consume a denied turn's one-shot authority without running any effect."""
    authority = getattr(agent, "_session_handoff_authority", None)
    if isinstance(authority, dict) and not authority.get("consumed"):
        authority["consumed"] = True
        authority["turn_id"] = str(turn_id or "")


def run_attached_session_handoff_ingress(
    agent: Any, user_message: Any, *, turn_id: str
) -> Any:
    """Run once for the authorized current turn and retain its observation."""
    runtime = getattr(agent, "_session_handoff_runtime", None)
    if runtime is None:
        return None
    if not isinstance(runtime, SessionHandoffRuntime):
        raise TypeError("attached session handoff runtime has an invalid type")
    authority = getattr(agent, "_session_handoff_authority", None)
    if not isinstance(authority, dict):
        raise RuntimeError("session handoff current-turn authority is missing")
    if authority.get("consumed"):
        raise RuntimeError(
            "session handoff current-turn authority was already consumed"
        )
    active_session_id = str(getattr(agent, "session_id", "") or "")
    if (
        not active_session_id
        or runtime.request.parent_session_id != active_session_id
        or authority.get("parent_session_id") != active_session_id
    ):
        raise ValueError(
            "request.parent_session_id does not match the active agent session_id"
        )
    current_turn_id = str(getattr(agent, "_current_turn_id", "") or "")
    if not turn_id or current_turn_id != str(turn_id):
        raise RuntimeError("session handoff authority is not for the current turn")
    # Consume before entering the lane. A retry is a new explicitly attached
    # request, never an accidental replay on a later ordinary message.
    authority["consumed"] = True
    authority["turn_id"] = current_turn_id
    result = runtime.ingress(agent, user_message)
    agent._last_session_handoff_result = result
    return result
