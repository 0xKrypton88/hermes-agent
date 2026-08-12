"""Fail-closed explicit-Go launch *planning* (plan-only, never dispatches).

Accepts a normalized Issue→Go transition plus verified Ready-review provenance
and returns an immutable, non-dispatched ``LaunchIntent`` record suitable for
caller-owned durable storage.

This module is deliberately pure:

- no network I/O, process spawning, remote agent bridges, or provider APIs
- no shared mutable state; idempotency uses caller-supplied seen-key sets
- never arms, deploys, restarts, trades, or mutates provider state
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import AbstractSet, Any, Mapping, Optional, Union

GO_TARGET_STATE = "Go"
READY_DECISION = "READY_FOR_GO"
_SHA256_LOWER = re.compile(r"^[0-9a-f]{64}$")
# Delivery/idempotency identities must be unambiguous in the composite intent key
# (colon-delimited) and must not silently normalize padded or spaced values.
_GO_EVENT_KEY_CANONICAL = re.compile(r"^[A-Za-z0-9._-]+$")


@dataclass(frozen=True)
class NormalizedGoTransition:
    """Caller-normalized explicit Go transition (already parsed upstream)."""

    issue_id: str
    issue_identifier: str
    target_state: str
    previous_state: Optional[str]
    go_event_key: str


@dataclass(frozen=True)
class ReadyReviewProvenance:
    """Verified Ready-review provenance bound to the same canonical issue."""

    issue_id: str
    review_key: str
    source_digest: str
    decision: str
    starts_agent_work: bool


@dataclass(frozen=True)
class LaunchIntent:
    """Immutable non-dispatched launch plan record."""

    issue_id: str
    issue_identifier: str
    review_key: str
    source_digest: str
    go_event_key: str
    idempotency_key: str
    dispatched: bool = False


@dataclass(frozen=True)
class LaunchPlanResult:
    """Outcome of launch planning; ``intent`` is set only on success."""

    ok: bool
    intent: Optional[LaunchIntent] = None
    reason_codes: tuple[str, ...] = ()


def build_go_launch_idempotency_key(
    *,
    issue_id: str,
    review_key: str,
    source_digest: str,
    go_event_key: str,
) -> str:
    """Deterministic idempotency key for an explicit-Go launch intent."""
    return f"go_launch:{issue_id}:{review_key}:{source_digest}:{go_event_key}"


def _as_optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _as_required_identity(value: Any) -> Optional[str]:
    """Non-blank string identity; rejects non-strings and whitespace-only."""
    return _as_optional_str(value)


def _as_canonical_go_event_key(value: Any) -> tuple[Optional[str], Optional[str]]:
    """Return (key, reason_code). reason_code is set when the key is unusable."""
    if not isinstance(value, str):
        return None, "blank_go_event_key"
    if not value or not value.strip():
        return None, "blank_go_event_key"
    if value != value.strip() or any(ch.isspace() for ch in value):
        return None, "noncanonical_go_event_key"
    if not _GO_EVENT_KEY_CANONICAL.fullmatch(value):
        return None, "noncanonical_go_event_key"
    return value, None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _coerce_transition(
    transition: Union[NormalizedGoTransition, Mapping[str, Any], None],
) -> Optional[NormalizedGoTransition]:
    if isinstance(transition, NormalizedGoTransition):
        return transition
    if not isinstance(transition, Mapping):
        return None
    data = _mapping(transition)
    return NormalizedGoTransition(
        issue_id=data.get("issue_id", ""),  # type: ignore[arg-type]
        issue_identifier=data.get("issue_identifier", ""),  # type: ignore[arg-type]
        target_state=data.get("target_state", ""),  # type: ignore[arg-type]
        previous_state=data.get("previous_state"),  # type: ignore[arg-type]
        go_event_key=data.get("go_event_key", ""),  # type: ignore[arg-type]
    )


def _coerce_provenance(
    provenance: Union[ReadyReviewProvenance, Mapping[str, Any], None],
) -> Optional[ReadyReviewProvenance]:
    if provenance is None:
        return None
    if isinstance(provenance, ReadyReviewProvenance):
        return provenance
    if not isinstance(provenance, Mapping):
        return None
    data = _mapping(provenance)
    return ReadyReviewProvenance(
        issue_id=data.get("issue_id", ""),  # type: ignore[arg-type]
        review_key=data.get("review_key", ""),  # type: ignore[arg-type]
        source_digest=data.get("source_digest", ""),  # type: ignore[arg-type]
        decision=data.get("decision", ""),  # type: ignore[arg-type]
        starts_agent_work=bool(data.get("starts_agent_work", True)),
    )


def plan_explicit_go_launch(
    transition: Union[NormalizedGoTransition, Mapping[str, Any], None],
    provenance: Union[ReadyReviewProvenance, Mapping[str, Any], None],
    *,
    seen_delivery_keys: AbstractSet[str] = frozenset(),
    seen_intent_keys: AbstractSet[str] = frozenset(),
) -> LaunchPlanResult:
    """Plan a non-dispatched LaunchIntent or fail closed with reason codes.

    Idempotency is entirely caller-owned: pass previously persisted delivery /
    event keys and intent keys. Duplicate membership yields no intent.
    """
    reasons: list[str] = []

    coerced = _coerce_transition(transition)
    if coerced is None:
        reasons.append("missing_go_transition")
        return LaunchPlanResult(ok=False, intent=None, reason_codes=tuple(reasons))

    issue_id = _as_optional_str(coerced.issue_id)
    if issue_id is None:
        reasons.append("blank_issue_id")

    target = _as_optional_str(coerced.target_state)
    if target is None:
        reasons.append("missing_go_target_state")
    elif target != GO_TARGET_STATE:
        reasons.append("non_go_target_state")

    previous = _as_optional_str(coerced.previous_state)
    if previous is None:
        reasons.append("missing_state_transition")
    elif target == GO_TARGET_STATE and previous == GO_TARGET_STATE:
        reasons.append("noop_duplicate_go_transition")

    issue_identifier = _as_required_identity(coerced.issue_identifier)
    if issue_identifier is None:
        reasons.append("blank_issue_identifier")

    go_event_key, go_event_reason = _as_canonical_go_event_key(coerced.go_event_key)
    if go_event_reason is not None:
        reasons.append(go_event_reason)

    ready = _coerce_provenance(provenance)
    if ready is None:
        reasons.append("missing_ready_provenance")
    else:
        ready_issue = _as_optional_str(ready.issue_id)
        if issue_id is not None and ready_issue != issue_id:
            reasons.append("ready_provenance_issue_mismatch")
        review_key = _as_optional_str(ready.review_key)
        if review_key is None:
            reasons.append("blank_review_key")
        digest = ready.source_digest if isinstance(ready.source_digest, str) else ""
        if not _SHA256_LOWER.fullmatch(digest):
            reasons.append("invalid_source_digest")
        decision = _as_optional_str(ready.decision)
        if decision != READY_DECISION:
            reasons.append("ready_decision_not_ready_for_go")
        if ready.starts_agent_work is not False:
            reasons.append("ready_starts_agent_work")

    if reasons:
        return LaunchPlanResult(
            ok=False,
            intent=None,
            reason_codes=tuple(dict.fromkeys(reasons)),
        )

    assert issue_id is not None
    assert issue_identifier is not None
    assert go_event_key is not None
    assert ready is not None
    review_key = _as_optional_str(ready.review_key)
    assert review_key is not None
    digest = ready.source_digest

    if go_event_key in seen_delivery_keys:
        return LaunchPlanResult(
            ok=False,
            intent=None,
            reason_codes=("duplicate_delivery_key",),
        )

    idempotency_key = build_go_launch_idempotency_key(
        issue_id=issue_id,
        review_key=review_key,
        source_digest=digest,
        go_event_key=go_event_key,
    )
    if idempotency_key in seen_intent_keys:
        return LaunchPlanResult(
            ok=False,
            intent=None,
            reason_codes=("duplicate_intent_key",),
        )

    intent = LaunchIntent(
        issue_id=issue_id,
        issue_identifier=issue_identifier,
        review_key=review_key,
        source_digest=digest,
        go_event_key=go_event_key,
        idempotency_key=idempotency_key,
        dispatched=False,
    )
    return LaunchPlanResult(ok=True, intent=intent, reason_codes=())
