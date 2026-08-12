"""Fail-closed Linear Ready review gate (ENG-14 vertical slice).

Side-effect free: evaluates a normalized issue snapshot and parsed policy only.
It does not call Linear APIs, persist state, bind webhooks, dispatch agents, or
perform Go. Ready review never starts coding/agent work.

Intended later sequence (mutation layer is out of scope here):

1. Ready event arrives with a normalized issue snapshot.
2. This gate builds a source-package snapshot, digests it, and decides.
3. A later adapter may post **one** Linear comment and transition the issue to
   Ready-for-Go **or** Blocked.
4. Go dispatch remains a separate, explicitly out-of-scope control plane.

Idempotency is modeled at the pure boundary via ``review_key`` =
``{issue_id}:{source_package_digest}``. Duplicate Ready deliveries with the
same key must not emit a second comment/transition request.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, AbstractSet, Mapping, Optional, Protocol, Sequence, Union


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

_CHECKED_REQUIREMENTS = (
    "canonical issue id",
    "issue identifier",
    "issue title",
    "issue description/context",
    "acceptance criteria",
    "repository binding",
    "target ref / branch policy",
    "no unresolved required-input blockers",
)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_nonempty_str(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


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
class LinearIssueSnapshot:
    """Normalized issue fields required for Ready source-package review."""

    issue_id: str
    identifier: str
    title: str
    description: str
    acceptance_criteria: Union[str, Sequence[str]]
    repository: str
    target_ref: str
    unresolved_required_inputs: bool = False

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "LinearIssueSnapshot":
        data = _mapping(value)
        return cls(
            issue_id=str(data.get("issue_id", "") or ""),
            identifier=str(data.get("identifier", "") or ""),
            title=str(data.get("title", "") or ""),
            description=str(data.get("description", "") or ""),
            acceptance_criteria=data.get("acceptance_criteria", ()),
            repository=str(data.get("repository", "") or ""),
            target_ref=str(data.get("target_ref", "") or ""),
            unresolved_required_inputs=bool(data.get("unresolved_required_inputs", False)),
        )


@dataclass(frozen=True)
class LinearReadyReviewPolicy:
    """Parsed policy for Ready review and later comment/transition intent."""

    ready_for_go_state_id: str = ""
    blocked_state_id: str = ""

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "LinearReadyReviewPolicy":
        data = _mapping(value)
        return cls(
            ready_for_go_state_id=str(data.get("ready_for_go_state_id", "") or ""),
            blocked_state_id=str(data.get("blocked_state_id", "") or ""),
        )


@dataclass(frozen=True)
class SourcePackageSnapshot:
    """Immutable normalized source package suitable for checkpointing."""

    issue_id: str
    identifier: str
    title: str
    description: str
    acceptance_criteria: tuple[str, ...]
    repository: str
    target_ref: str

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "acceptance_criteria": list(self.acceptance_criteria),
            "description": self.description,
            "identifier": self.identifier,
            "issue_id": self.issue_id,
            "repository": self.repository,
            "target_ref": self.target_ref,
            "title": self.title,
        }


@dataclass(frozen=True)
class LinearMutationIntent:
    """Narrow mutation boundary: one comment + one state transition.

    No provider API client is included or allowed in this slice. A later
    adapter may consume this intent without expanding the surface.
    """

    issue_id: str
    comment_body: str
    target_state_id: str
    review_key: str


class LinearMutationPort(Protocol):
    """Adapter protocol for a later Linear mutation layer (comment + transition)."""

    def apply_comment_and_transition(self, intent: LinearMutationIntent) -> None:
        """Apply exactly one comment and one state transition for ``intent``."""


@dataclass(frozen=True)
class LinearReadyReviewDecision:
    """Deterministic Ready-gate decision object."""

    decision: str  # READY_FOR_GO | BLOCKED
    reasons: tuple[str, ...]
    source_package: Optional[SourcePackageSnapshot]
    source_package_digest: str
    review_key: str
    comment_body: str
    starts_agent_work: bool = False


def build_source_package(snapshot: LinearIssueSnapshot) -> SourcePackageSnapshot:
    """Build the canonical source package from a normalized issue snapshot."""
    return SourcePackageSnapshot(
        issue_id=(_as_nonempty_str(snapshot.issue_id) or "").strip(),
        identifier=(_as_nonempty_str(snapshot.identifier) or "").strip(),
        title=(_as_nonempty_str(snapshot.title) or "").strip(),
        description=(_as_nonempty_str(snapshot.description) or "").strip(),
        acceptance_criteria=_normalize_criteria(snapshot.acceptance_criteria),
        repository=(_as_nonempty_str(snapshot.repository) or "").strip(),
        target_ref=(_as_nonempty_str(snapshot.target_ref) or "").strip(),
    )


def digest_source_package(package: SourcePackageSnapshot) -> str:
    """Return the SHA-256 hex digest of the canonical source-package JSON."""
    payload = json.dumps(
        package.to_canonical_dict(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_review_key(issue_id: str, source_package_digest: str) -> str:
    """Stable idempotency key: issue identity + source-package digest."""
    issue = (_as_nonempty_str(issue_id) or "").strip() or "missing-issue-id"
    digest = (_as_nonempty_str(source_package_digest) or "").strip() or "missing-digest"
    return f"{issue}:{digest}"


def _collect_absence_reasons(
    package: SourcePackageSnapshot,
    *,
    unresolved_required_inputs: bool,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if not package.issue_id:
        reasons.append(REASON_MISSING_ISSUE_ID)
    if not package.identifier:
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
    return tuple(reasons)


def _format_checked_summary() -> str:
    return "; ".join(_CHECKED_REQUIREMENTS)


def _comment_ready(digest: str, review_key: str) -> str:
    return (
        "Hermes Ready review: READY_FOR_GO.\n"
        f"Source-package digest: {digest}\n"
        f"Review key: {review_key}\n"
        f"Checked: {_format_checked_summary()}.\n"
        "Ready review does not start coding or agent work. Go remains a separate gate."
    )


def _comment_blocked(reasons: tuple[str, ...], digest: str, review_key: str) -> str:
    missing = ", ".join(reasons) if reasons else "unknown_gap"
    return (
        "Hermes Ready review: BLOCKED.\n"
        f"Missing before returning to Ready: {missing}.\n"
        f"Source-package digest: {digest}\n"
        f"Review key: {review_key}\n"
        "Ready review does not start coding or agent work. Add the missing "
        "information, then move the issue back to Ready for another review."
    )


def _coerce_snapshot(
    snapshot: Union[LinearIssueSnapshot, Mapping[str, Any]],
) -> LinearIssueSnapshot:
    if isinstance(snapshot, LinearIssueSnapshot):
        return snapshot
    return LinearIssueSnapshot.from_mapping(_mapping(snapshot))


def _coerce_policy(
    policy: Union[LinearReadyReviewPolicy, Mapping[str, Any], None],
) -> LinearReadyReviewPolicy:
    if isinstance(policy, LinearReadyReviewPolicy):
        return policy
    return LinearReadyReviewPolicy.from_mapping(_mapping(policy))


def assess_linear_ready_review(
    snapshot: Union[LinearIssueSnapshot, Mapping[str, Any]],
    policy: Union[LinearReadyReviewPolicy, Mapping[str, Any], None] = None,
) -> LinearReadyReviewDecision:
    """Assess whether a normalized issue is Ready-for-Go capable.

    Fail-closed: any missing source-package requirement yields ``BLOCKED``.
    Never starts agent/coding work and never contacts Linear.
    """
    # policy is accepted for boundary compatibility with later wiring; the
    # adequacy checks themselves are snapshot-driven in this slice.
    _coerce_policy(policy)
    issue = _coerce_snapshot(snapshot)
    package = build_source_package(issue)
    digest = digest_source_package(package)
    review_key = build_review_key(package.issue_id or issue.issue_id, digest)
    reasons = _collect_absence_reasons(
        package,
        unresolved_required_inputs=bool(issue.unresolved_required_inputs),
    )

    if reasons:
        return LinearReadyReviewDecision(
            decision=DECISION_BLOCKED,
            reasons=reasons,
            source_package=package,
            source_package_digest=digest,
            review_key=review_key,
            comment_body=_comment_blocked(reasons, digest, review_key),
            starts_agent_work=False,
        )

    return LinearReadyReviewDecision(
        decision=DECISION_READY_FOR_GO,
        reasons=(),
        source_package=package,
        source_package_digest=digest,
        review_key=review_key,
        comment_body=_comment_ready(digest, review_key),
        starts_agent_work=False,
    )


def should_emit_review(
    review_key: str,
    *,
    seen_review_keys: AbstractSet[str],
) -> bool:
    """Return True only when this review_key has not already been emitted."""
    key = (_as_nonempty_str(review_key) or "").strip()
    if not key:
        return False
    return key not in seen_review_keys


def plan_linear_mutation(
    decision: LinearReadyReviewDecision,
    policy: Union[LinearReadyReviewPolicy, Mapping[str, Any], None] = None,
    *,
    seen_review_keys: Optional[AbstractSet[str]] = None,
) -> Optional[LinearMutationIntent]:
    """Build a comment+transition intent, or None when fail-closed / idempotent.

    Returns ``None`` (no mutation) when:
    - the review_key was already emitted;
    - the decision has no canonical issue_id;
    - the policy lacks the destination state id for this decision.

    Never emits an intent with an empty ``issue_id`` or ``target_state_id``.
    This is the narrow adapter boundary for a later Linear mutation layer.
    It does not perform provider calls.
    """
    seen = seen_review_keys if seen_review_keys is not None else set()
    if not should_emit_review(decision.review_key, seen_review_keys=seen):
        return None

    issue_id = ""
    if decision.source_package is not None:
        issue_id = (_as_nonempty_str(decision.source_package.issue_id) or "").strip()
    if not issue_id:
        return None

    parsed = _coerce_policy(policy)
    if decision.decision == DECISION_READY_FOR_GO:
        target_state_id = _as_nonempty_str(parsed.ready_for_go_state_id)
    else:
        target_state_id = _as_nonempty_str(parsed.blocked_state_id)
    if target_state_id is None:
        return None

    return LinearMutationIntent(
        issue_id=issue_id,
        comment_body=decision.comment_body,
        target_state_id=target_state_id,
        review_key=decision.review_key,
    )
