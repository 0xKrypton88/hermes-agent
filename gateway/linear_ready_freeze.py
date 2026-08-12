"""Pure Ready evaluation / source freeze (local-only, never starts work).

Accepts explicitly supplied source and review inputs, validates canonical issue
identities, freezes the source package, and returns a deterministic
``READY_FOR_GO`` receipt only when every required field is present.

This module is deliberately side-effect free:

- no network I/O, Linear API clients, webhook registration, or listener lifecycle
- no Cursor / LangGraph / subprocess / ``handle_message`` dispatch
- ``starts_agent_work`` is always ``False``
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence, Union

DECISION_READY_FOR_GO = "READY_FOR_GO"
DECISION_BLOCKED = "BLOCKED"

REASON_MISSING_ISSUE_ID = "missing_issue_id"
REASON_MISSING_ISSUE_IDENTIFIER = "missing_issue_identifier"
REASON_MISSING_ISSUE_TITLE = "missing_issue_title"
REASON_MISSING_ISSUE_DESCRIPTION = "missing_issue_description"
REASON_MISSING_ACCEPTANCE_CRITERIA = "missing_acceptance_criteria"
REASON_MISSING_REPOSITORY_BINDING = "missing_repository_binding"
REASON_MISSING_TARGET_REF = "missing_target_ref"
REASON_UNRESOLVED_REQUIRED_INPUTS = "unresolved_required_inputs"
REASON_INVALID_SOURCE_INPUT = "invalid_source_input"
REASON_ISSUE_IDENTITY_MISMATCH = "issue_identity_mismatch"
REASON_NONCANONICAL_ISSUE_ID = "noncanonical_issue_id"
REASON_NONCANONICAL_ISSUE_IDENTIFIER = "noncanonical_issue_identifier"
REASON_NONCANONICAL_TEAM_KEY = "noncanonical_team_key"
REASON_CROSS_TEAM_MISMATCH = "cross_team_mismatch"
REASON_UNKNOWN_TEAM_KEY = "unknown_team_key"

_SHA256_LOWER = re.compile(r"^[0-9a-f]{64}$")
# Strict allowlist for local identities: no whitespace/padding; fail closed on
# anything outside this charset so mismatched/noncanonical values never freeze.
_CANONICAL_IDENTITY = re.compile(r"^[A-Za-z0-9._:-]+$")


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_nonempty_str(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _as_canonical_identity(value: Any) -> tuple[Optional[str], Optional[str]]:
    """Return (identity, reason_code) for allowlisted non-blank identities.

    Rejects non-strings, blank/whitespace-only values, surrounding or internal
    whitespace, and any character outside ``[A-Za-z0-9._:-]``.
    """
    if not isinstance(value, str):
        return None, "noncanonical"
    if not value or not value.strip():
        return None, "blank"
    if value != value.strip() or any(ch.isspace() for ch in value):
        return None, "noncanonical"
    if not _CANONICAL_IDENTITY.fullmatch(value):
        return None, "noncanonical"
    return value, None


def _normalize_criteria(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        text = value.strip()
        return (text,) if text else ()
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        items: list[str] = []
        for item in value:
            text = _as_nonempty_str(item)
            if text is not None:
                items.append(text)
        return tuple(items)
    return ()


@dataclass(frozen=True)
class ReadySourceInput:
    """Caller-supplied issue source fields to freeze."""

    issue_id: str
    issue_identifier: str
    title: str
    description: str
    acceptance_criteria: Union[str, Sequence[str]]
    repository: str
    target_ref: str
    unresolved_required_inputs: bool = False
    team_key: Optional[str] = None
    allowed_team_keys: Optional[Sequence[str]] = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ReadySourceInput":
        data = _mapping(value)
        identifier = data.get("issue_identifier", data.get("identifier", ""))
        allowed = data.get("allowed_team_keys")
        return cls(
            issue_id=str(data.get("issue_id", "") or ""),
            issue_identifier=str(identifier or ""),
            title=str(data.get("title", "") or ""),
            description=str(data.get("description", "") or ""),
            acceptance_criteria=data.get("acceptance_criteria", ()),
            repository=str(data.get("repository", "") or ""),
            target_ref=str(data.get("target_ref", "") or ""),
            unresolved_required_inputs=bool(
                data.get("unresolved_required_inputs", False)
            ),
            team_key=data.get("team_key"),  # type: ignore[arg-type]
            allowed_team_keys=(
                allowed
                if isinstance(allowed, Sequence)
                and not isinstance(allowed, (str, bytes, bytearray))
                else None
            ),
        )


@dataclass(frozen=True)
class ReadyReviewInput:
    """Optional review-side identity checks supplied by the caller."""

    issue_id: Optional[str] = None
    issue_identifier: Optional[str] = None
    review_key: Optional[str] = None
    team_key: Optional[str] = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ReadyReviewInput":
        data = _mapping(value)
        identifier = data.get("issue_identifier", data.get("identifier"))
        return cls(
            issue_id=data.get("issue_id"),  # type: ignore[arg-type]
            issue_identifier=identifier,  # type: ignore[arg-type]
            review_key=data.get("review_key"),  # type: ignore[arg-type]
            team_key=data.get("team_key"),  # type: ignore[arg-type]
        )


@dataclass(frozen=True)
class FrozenReadySource:
    """Immutable frozen source package used for digests and persistence."""

    issue_id: str
    issue_identifier: str
    title: str
    description: str
    acceptance_criteria: tuple[str, ...]
    repository: str
    target_ref: str
    team_key: str = ""

    def to_canonical_dict(self) -> dict[str, Any]:
        payload = {
            "acceptance_criteria": list(self.acceptance_criteria),
            "description": self.description,
            "issue_id": self.issue_id,
            "issue_identifier": self.issue_identifier,
            "repository": self.repository,
            "target_ref": self.target_ref,
            "title": self.title,
        }
        # Only include team_key when present so digests stay stable for
        # team-less local fixtures while still binding cross-team checks.
        if self.team_key:
            payload["team_key"] = self.team_key
        return payload


@dataclass(frozen=True)
class ReadyFreezeReceipt:
    """Deterministic Ready freeze outcome; never authorizes starting work."""

    decision: str  # READY_FOR_GO | BLOCKED
    reasons: tuple[str, ...]
    review_key: str
    source_digest: str
    frozen_source: FrozenReadySource
    starts_agent_work: bool = False


def freeze_source_package(source: ReadySourceInput) -> FrozenReadySource:
    """Normalize and freeze source fields into an immutable package."""
    issue_id, _issue_id_reason = _as_canonical_identity(source.issue_id)
    issue_identifier, _issue_identifier_reason = _as_canonical_identity(
        source.issue_identifier
    )
    team_key = ""
    if source.team_key is not None and str(source.team_key).strip() != "":
        team_canon, _team_reason = _as_canonical_identity(source.team_key)
        team_key = team_canon or ""
    return FrozenReadySource(
        # Noncanonical identities are cleared so they never enter a READY digest.
        issue_id=issue_id or "",
        issue_identifier=issue_identifier or "",
        title=(_as_nonempty_str(source.title) or "").strip(),
        description=(_as_nonempty_str(source.description) or "").strip(),
        acceptance_criteria=_normalize_criteria(source.acceptance_criteria),
        repository=(_as_nonempty_str(source.repository) or "").strip(),
        target_ref=(_as_nonempty_str(source.target_ref) or "").strip(),
        team_key=team_key,
    )


def digest_frozen_source(package: FrozenReadySource) -> str:
    """Return lowercase SHA-256 hex of the canonical frozen-source JSON."""
    payload = json.dumps(
        package.to_canonical_dict(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_ready_review_key(issue_id: str, source_digest: str) -> str:
    """Stable review identity: ``{issue_id}:{source_digest}``."""
    issue = (_as_nonempty_str(issue_id) or "").strip() or "missing-issue-id"
    digest = (_as_nonempty_str(source_digest) or "").strip() or "missing-digest"
    return f"{issue}:{digest}"


def _collect_absence_reasons(
    package: FrozenReadySource,
    *,
    unresolved_required_inputs: bool,
    source: ReadySourceInput,
) -> list[str]:
    reasons: list[str] = []
    issue_id, issue_id_reason = _as_canonical_identity(source.issue_id)
    if issue_id_reason == "noncanonical":
        reasons.append(REASON_NONCANONICAL_ISSUE_ID)
    elif issue_id_reason == "blank" or not package.issue_id:
        reasons.append(REASON_MISSING_ISSUE_ID)

    issue_identifier, issue_identifier_reason = _as_canonical_identity(
        source.issue_identifier
    )
    if issue_identifier_reason == "noncanonical":
        reasons.append(REASON_NONCANONICAL_ISSUE_IDENTIFIER)
    elif issue_identifier_reason == "blank" or not package.issue_identifier:
        reasons.append(REASON_MISSING_ISSUE_IDENTIFIER)

    if not package.title:
        reasons.append(REASON_MISSING_ISSUE_TITLE)
    if not package.description:
        reasons.append(REASON_MISSING_ISSUE_DESCRIPTION)
    if not package.acceptance_criteria:
        reasons.append(REASON_MISSING_ACCEPTANCE_CRITERIA)
    if not package.repository:
        reasons.append(REASON_MISSING_REPOSITORY_BINDING)
    if not package.target_ref:
        reasons.append(REASON_MISSING_TARGET_REF)
    if unresolved_required_inputs:
        reasons.append(REASON_UNRESOLVED_REQUIRED_INPUTS)

    if source.team_key is not None and str(source.team_key).strip() != "":
        team_key, team_reason = _as_canonical_identity(source.team_key)
        if team_reason == "noncanonical" or team_key is None:
            reasons.append(REASON_NONCANONICAL_TEAM_KEY)
        else:
            allowed_raw = source.allowed_team_keys
            if allowed_raw is not None:
                allowed: set[str] = set()
                for item in allowed_raw:
                    canon, reason = _as_canonical_identity(item)
                    if canon is not None and reason is None:
                        allowed.add(canon)
                if team_key not in allowed:
                    reasons.append(REASON_UNKNOWN_TEAM_KEY)
    return reasons


def _coerce_source(
    source: Union[ReadySourceInput, Mapping[str, Any], None],
) -> Optional[ReadySourceInput]:
    if isinstance(source, ReadySourceInput):
        return source
    if isinstance(source, Mapping):
        return ReadySourceInput.from_mapping(source)
    return None


def _coerce_review(
    review: Union[ReadyReviewInput, Mapping[str, Any], None],
) -> Optional[ReadyReviewInput]:
    if review is None:
        return None
    if isinstance(review, ReadyReviewInput):
        return review
    if isinstance(review, Mapping):
        return ReadyReviewInput.from_mapping(review)
    return None


def evaluate_ready_freeze(
    source: Union[ReadySourceInput, Mapping[str, Any], None],
    review: Union[ReadyReviewInput, Mapping[str, Any], None] = None,
) -> ReadyFreezeReceipt:
    """Freeze + evaluate Ready; return ``READY_FOR_GO`` only when complete.

    Never starts agent/coding work. Invalid source input fails closed as
    ``BLOCKED`` with ``invalid_source_input``. Unknown/mismatched/noncanonical
    identities fail closed via allowlist reason codes.
    """
    coerced = _coerce_source(source)
    if coerced is None:
        empty = FrozenReadySource(
            issue_id="",
            issue_identifier="",
            title="",
            description="",
            acceptance_criteria=(),
            repository="",
            target_ref="",
            team_key="",
        )
        digest = digest_frozen_source(empty)
        return ReadyFreezeReceipt(
            decision=DECISION_BLOCKED,
            reasons=(REASON_INVALID_SOURCE_INPUT,),
            review_key=build_ready_review_key("missing-issue-id", digest),
            source_digest=digest,
            frozen_source=empty,
            starts_agent_work=False,
        )

    package = freeze_source_package(coerced)
    digest = digest_frozen_source(package)
    assert _SHA256_LOWER.fullmatch(digest)
    review_key = build_ready_review_key(package.issue_id or "missing-issue-id", digest)
    reasons = _collect_absence_reasons(
        package,
        unresolved_required_inputs=bool(coerced.unresolved_required_inputs),
        source=coerced,
    )

    review_input = _coerce_review(review)
    if review_input is not None:
        expected_issue = _as_nonempty_str(review_input.issue_id)
        if expected_issue is not None:
            expected_issue_canon, expected_issue_reason = _as_canonical_identity(
                review_input.issue_id
            )
            if expected_issue_reason == "noncanonical":
                reasons.append(REASON_NONCANONICAL_ISSUE_ID)
            elif package.issue_id and expected_issue_canon != package.issue_id:
                reasons.append(REASON_ISSUE_IDENTITY_MISMATCH)
        expected_identifier = _as_nonempty_str(review_input.issue_identifier)
        if expected_identifier is not None:
            expected_id_canon, expected_id_reason = _as_canonical_identity(
                review_input.issue_identifier
            )
            if expected_id_reason == "noncanonical":
                reasons.append(REASON_NONCANONICAL_ISSUE_IDENTIFIER)
            elif (
                package.issue_identifier
                and expected_id_canon != package.issue_identifier
            ):
                reasons.append(REASON_ISSUE_IDENTITY_MISMATCH)
        expected_key = _as_nonempty_str(review_input.review_key)
        if expected_key is not None and expected_key != review_key:
            # Caller-supplied review_key that does not match the frozen digest
            # is treated as an identity mismatch (tamper / stale review).
            reasons.append(REASON_ISSUE_IDENTITY_MISMATCH)
        if review_input.team_key is not None and str(review_input.team_key).strip():
            review_team, review_team_reason = _as_canonical_identity(
                review_input.team_key
            )
            if review_team_reason == "noncanonical" or review_team is None:
                reasons.append(REASON_NONCANONICAL_TEAM_KEY)
            elif package.team_key and review_team != package.team_key:
                reasons.append(REASON_CROSS_TEAM_MISMATCH)
            elif not package.team_key and review_team:
                reasons.append(REASON_CROSS_TEAM_MISMATCH)

    # Preserve first-seen order while collapsing duplicate mismatch codes.
    reasons_tuple = tuple(dict.fromkeys(reasons))
    if reasons_tuple:
        return ReadyFreezeReceipt(
            decision=DECISION_BLOCKED,
            reasons=reasons_tuple,
            review_key=review_key,
            source_digest=digest,
            frozen_source=package,
            starts_agent_work=False,
        )

    return ReadyFreezeReceipt(
        decision=DECISION_READY_FOR_GO,
        reasons=(),
        review_key=review_key,
        source_digest=digest,
        frozen_source=package,
        starts_agent_work=False,
    )
