"""Redact secrets in durable-job event payloads. Preserve correlation ids."""

from __future__ import annotations

import re
from typing import Any

_KEEP_KEYS = frozenset(
    {
        "job_id",
        "idempotency_key",
        "authorization_idempotency_key",
        "decision_idempotency_key",
        "provider_idempotency_key",
        "client_msg_id",
        "outbound_client_msg_id",
        "correlation_id",
        "evidence_id",
        "enqueue_id",
        "inbound_id",
        "decision_id",
        "action_id",
        "provider_run_id",
    }
)

_REDACT_EXACT = frozenset(
    {
        "token",
        "owner_token",
        "claim_owner_token",
        "effect_inflight_token",
        "prompt",
        "authorization",
        "api_key",
        "secret",
        "password",
        "bearer",
    }
)

_REDACT_SUBSTR = (
    "token",
    "prompt",
    "secret",
    "password",
    "authorization",
    "api_key",
)

REDACTED = "[REDACTED]"

_URI_PASSWORD_RE = re.compile(
    r"((?:postgres(?:ql)?)://[^:/?#\s]+:)(.+)(@[^/?#]+)",
    re.IGNORECASE,
)
_KV_QUOTED_PASSWORD_RE = re.compile(
    r"(password\s*=\s*)(['\"])(.*?)\2",
    re.IGNORECASE | re.DOTALL,
)
_KV_UNQUOTED_PASSWORD_RE = re.compile(
    r"(password\s*=\s*)([^\s'\"]+)",
    re.IGNORECASE,
)


def redact_secret_text(text: str) -> str:
    """Redact DSN passwords and URI userinfo. Never raise on odd input."""
    if not text:
        return text
    redacted = _URI_PASSWORD_RE.sub(r"\1[REDACTED]\3", text)
    redacted = _KV_QUOTED_PASSWORD_RE.sub(r"\1\2[REDACTED]\2", redacted)
    redacted = _KV_UNQUOTED_PASSWORD_RE.sub(r"\1[REDACTED]", redacted)
    return redacted


def _key_should_redact(key: str) -> bool:
    lowered = str(key).strip().lower()
    if lowered in _KEEP_KEYS or "idempotency" in lowered:
        return False
    if lowered in _REDACT_EXACT:
        return True
    if "dsn" in lowered:
        return True
    return any(part in lowered for part in _REDACT_SUBSTR)


def redact_payload(payload: Any) -> Any:
    """Return a copy with token/prompt secrets replaced. Ids are kept."""
    if isinstance(payload, dict):
        redacted = {}
        for key, value in payload.items():
            if _key_should_redact(str(key)):
                redacted[key] = REDACTED
            else:
                redacted[key] = redact_payload(value)
        return redacted
    if isinstance(payload, list):
        return [redact_payload(item) for item in payload]
    if isinstance(payload, str):
        return redact_secret_text(payload)
    return payload
