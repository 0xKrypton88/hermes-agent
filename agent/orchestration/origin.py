"""Trusted TurnOrigin for Adaptive Orchestrator V1.1.

Activation identity is carried server-side from authenticated gateway /
session metadata. Prompt text and client-supplied trust flags are never
authoritative.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class TurnOrigin:
    """Server-side turn identity used for orchestration activation.

    ``trusted=True`` is only valid when constructed from authenticated
    normalized gateway metadata / session source — never from prompt text
    or client-supplied flags.
    """

    platform: str
    workspace_id: Optional[str] = None
    channel_id: Optional[str] = None
    user_id: Optional[str] = None
    trusted: bool = False
    chat_type: Optional[str] = None
    session_key: Optional[str] = None
    thread_id: Optional[str] = None

    def redacted_dimensions(self) -> dict[str, Optional[str]]:
        """ID-safe origin dimensions for local telemetry (no display names)."""
        return {
            "origin_platform": self.platform or None,
            "origin_workspace_id": self.workspace_id or None,
            "origin_channel_id": self.channel_id or None,
            "origin_user_id": self.user_id or None,
        }


def _platform_value(platform: Any) -> str:
    if platform is None:
        return ""
    if hasattr(platform, "value"):
        return str(platform.value).strip().lower()
    return str(platform).strip().lower()


def turn_origin_from_session_source(
    source: Any,
    *,
    session_key: Optional[str] = None,
) -> TurnOrigin:
    """Build a trusted TurnOrigin from an authenticated SessionSource."""
    if source is None:
        return TurnOrigin(platform="", trusted=False, session_key=session_key)

    platform = _platform_value(getattr(source, "platform", None))
    workspace_id = getattr(source, "scope_id", None) or getattr(source, "guild_id", None)
    channel_id = getattr(source, "chat_id", None)
    user_id = getattr(source, "user_id", None)
    chat_type = getattr(source, "chat_type", None)
    thread_id = getattr(source, "thread_id", None)

    # Trust requires authenticated platform + at least one durable ID dimension.
    has_identity = bool(platform) and bool(
        (workspace_id and str(workspace_id).strip())
        or (channel_id and str(channel_id).strip())
        or (user_id and str(user_id).strip())
    )
    return TurnOrigin(
        platform=platform,
        workspace_id=str(workspace_id).strip() if workspace_id else None,
        channel_id=str(channel_id).strip() if channel_id else None,
        user_id=str(user_id).strip() if user_id else None,
        trusted=bool(has_identity),
        chat_type=str(chat_type).strip() if chat_type else None,
        session_key=session_key,
        thread_id=str(thread_id).strip() if thread_id else None,
    )


def turn_origin_from_agent(
    agent: Any,
    *,
    prompt_hint: Optional[str] = None,
) -> TurnOrigin:
    """Derive TurnOrigin from agent attrs stamped by the gateway.

    ``prompt_hint`` is accepted only to prove it cannot forge trust — it is
    never parsed for activation identity.
    """
    del prompt_hint  # Explicit non-signal: never infer activation from text.

    if agent is None:
        return TurnOrigin(platform="", trusted=False)

    # Prefer an already-stamped trusted origin object when present.
    existing = getattr(agent, "_turn_origin", None)
    if isinstance(existing, TurnOrigin):
        return existing

    platform = _platform_value(getattr(agent, "platform", None))
    workspace_id = getattr(agent, "_scope_id", None)
    channel_id = getattr(agent, "_chat_id", None)
    user_id = getattr(agent, "_user_id", None)
    chat_type = getattr(agent, "_chat_type", None)
    thread_id = getattr(agent, "_thread_id", None)
    session_key = getattr(agent, "_gateway_session_key", None)

    # Only server-stamped trust bit (set by gateway / conversation seam).
    # Missing/false → untrusted even if IDs happen to be present on the agent
    # from an unauthenticated surface.
    stamped = getattr(agent, "_turn_origin_trusted", None)
    if stamped is None:
        # Gateway-stamped identity: platform + user/chat/scope present from
        # authenticated messaging surfaces. CLI/desktop/api/cron without an
        # explicit stamp remain untrusted for activation elevation.
        messaging = platform in {
            "slack",
            "telegram",
            "discord",
            "whatsapp",
            "signal",
            "matrix",
            "mattermost",
            "feishu",
            "homeassistant",
            "email",
            "sms",
        }
        stamped = bool(
            messaging
            and (
                (workspace_id and str(workspace_id).strip())
                or (channel_id and str(channel_id).strip())
                or (user_id and str(user_id).strip())
            )
        )

    has_identity = bool(platform) and bool(
        (workspace_id and str(workspace_id).strip())
        or (channel_id and str(channel_id).strip())
        or (user_id and str(user_id).strip())
    )
    trusted = bool(stamped) and has_identity

    return TurnOrigin(
        platform=platform,
        workspace_id=str(workspace_id).strip() if workspace_id else None,
        channel_id=str(channel_id).strip() if channel_id else None,
        user_id=str(user_id).strip() if user_id else None,
        trusted=trusted,
        chat_type=str(chat_type).strip() if chat_type else None,
        session_key=str(session_key).strip() if session_key else None,
        thread_id=str(thread_id).strip() if thread_id else None,
    )
