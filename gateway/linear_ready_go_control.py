"""Local-only Linear Ready/Go control plane (fail-closed, never dispatches).

Orchestrates:

1. Pure Ready freeze evaluation (``evaluate_ready_freeze``)
2. Durable persistence of Ready provenance
3. Go transition → exactly one non-dispatched ``LaunchIntent`` when provenance
   matches and the Go transition is valid

Safety contract:

- no Cursor / LangGraph / subprocess / network / Linear API / ``handle_message``
- no webhook registration or listener lifecycle
- ``LaunchIntent.dispatched`` is always ``False``
- existing receipt-only webhook intake remains untouched
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Union

from gateway.linear_go_launch_plan import (
    LaunchIntent,
    NormalizedGoTransition,
    ReadyReviewProvenance,
    plan_explicit_go_launch,
)
from gateway.linear_ready_freeze import (
    DECISION_READY_FOR_GO,
    ReadyFreezeReceipt,
    ReadyReviewInput,
    ReadySourceInput,
    evaluate_ready_freeze,
)
from gateway.linear_ready_go_store import (
    LaunchIntentRecord,
    LinearReadyGoStore,
    ReadyReviewRecord,
)

# Match Ready freeze identity + digest contracts (fail closed on padding / charset).
_CANONICAL_IDENTITY = re.compile(r"^[A-Za-z0-9._:-]+$")
_SHA256_LOWER = re.compile(r"^[0-9a-f]{64}$")


def _exact_canonical_identity(value: Any) -> tuple[Optional[str], Optional[str]]:
    """Return ``(identity, reason)`` with **no** str() coercion or whitespace trim.

    ``reason`` is ``\"missing\"`` for None / exact empty string, else
    ``\"noncanonical\"`` for non-strings, padding, internal whitespace, or
    charset violations. Accepted values are returned unchanged.
    """
    if value is None:
        return None, "missing"
    if not isinstance(value, str):
        return None, "noncanonical"
    if value == "":
        return None, "missing"
    if value != value.strip() or any(ch.isspace() for ch in value):
        return None, "noncanonical"
    if not _CANONICAL_IDENTITY.fullmatch(value):
        return None, "noncanonical"
    return value, None


def _canonical_go_selector(value: Any) -> tuple[Optional[str], Optional[str]]:
    """Return ``(canonical_value, reason_code)`` for a required Go selector.

    Blank / omitted → ``missing_*`` (caller supplies the specific code).
    Non-string, surrounding/internal whitespace, or charset mismatch →
    ``noncanonical``. Digests must additionally be lowercase SHA-256 hex.
    """
    if value is None:
        return None, "missing"
    if not isinstance(value, str):
        return None, "noncanonical"
    if not value or not value.strip():
        return None, "missing"
    if value != value.strip() or any(ch.isspace() for ch in value):
        return None, "noncanonical"
    return value, None


def _validate_go_provenance_selectors(
    review_key: Any,
    source_digest: Any,
) -> tuple[Optional[str], Optional[str], tuple[str, ...]]:
    """Require both Go selectors to be canonical and nonempty.

    Returns ``(canonical_review_key, canonical_source_digest, reason_codes)``.
    When ``reason_codes`` is non-empty both selectors are rejected (fail closed).
    """
    codes: list[str] = []
    key, key_reason = _canonical_go_selector(review_key)
    if key_reason == "missing":
        codes.append("missing_review_key")
        key = None
    elif key_reason == "noncanonical" or key is None or not _CANONICAL_IDENTITY.fullmatch(
        key
    ):
        codes.append("noncanonical_review_key")
        key = None

    digest, digest_reason = _canonical_go_selector(source_digest)
    if digest_reason == "missing":
        codes.append("missing_source_digest")
        digest = None
    elif (
        digest_reason == "noncanonical"
        or digest is None
        or not _SHA256_LOWER.fullmatch(digest)
    ):
        codes.append("noncanonical_source_digest")
        digest = None

    if codes:
        return None, None, tuple(dict.fromkeys(codes))
    assert key is not None and digest is not None
    return key, digest, ()


def _extract_go_transition_identities(
    transition: Union[NormalizedGoTransition, Mapping[str, Any]],
) -> tuple[Any, Any]:
    """Return raw ``(issue_id, issue_identifier)`` without coercion."""
    if isinstance(transition, NormalizedGoTransition):
        return transition.issue_id, transition.issue_identifier
    return transition.get("issue_id"), transition.get("issue_identifier")


def _validate_go_transition_identities(
    issue_id: Any,
    issue_identifier: Any,
) -> tuple[Optional[str], Optional[str], tuple[str, ...]]:
    """Require exact canonical Go transition identities (no trim / str())."""
    codes: list[str] = []
    canon_id, id_reason = _exact_canonical_identity(issue_id)
    if id_reason == "missing":
        codes.append("missing_issue_id")
    elif id_reason == "noncanonical" or canon_id is None:
        codes.append("noncanonical_issue_id")
        canon_id = None

    canon_ident, ident_reason = _exact_canonical_identity(issue_identifier)
    if ident_reason == "missing":
        codes.append("missing_issue_identifier")
    elif ident_reason == "noncanonical" or canon_ident is None:
        codes.append("noncanonical_issue_identifier")
        canon_ident = None

    if codes:
        return None, None, tuple(dict.fromkeys(codes))
    assert canon_id is not None and canon_ident is not None
    return canon_id, canon_ident, ()


def _validate_go_team_key(team_key: Any) -> tuple[Optional[str], tuple[str, ...]]:
    """Validate optional Go ``team_key`` without trimming or str() coercion.

    ``None`` / exact ``\"\"`` → no team constraint. Any other value must be an
    exact canonical identity string; padded / non-string / noncanonical reject.
    """
    if team_key is None or team_key == "":
        return None, ()
    canon, reason = _exact_canonical_identity(team_key)
    if reason is not None or canon is None:
        return None, ("noncanonical_team_key",)
    return canon, ()


@dataclass(frozen=True)
class ReadyControlResult:
    """Outcome of Ready freeze + optional durable record."""

    ok: bool
    receipt: ReadyFreezeReceipt
    status: str  # "ready" | "blocked" | "duplicate"
    record: Optional[ReadyReviewRecord] = None
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class GoControlResult:
    """Outcome of Go transition → non-dispatched LaunchIntent creation."""

    ok: bool
    status: str  # "created" | "duplicate" | "rejected"
    intent: Optional[LaunchIntent] = None
    record: Optional[LaunchIntentRecord] = None
    reason_codes: tuple[str, ...] = ()
    reason: str = ""


class LinearReadyGoControlPlane:
    """Durable Ready/Go control plane rooted at a profile-local store."""

    def __init__(self, store: Optional[LinearReadyGoStore] = None):
        self._store = store if store is not None else LinearReadyGoStore()

    @property
    def store(self) -> LinearReadyGoStore:
        return self._store

    def process_ready(
        self,
        source: Union[ReadySourceInput, Mapping[str, Any], None],
        review: Union[ReadyReviewInput, Mapping[str, Any], None] = None,
        *,
        persist_blocked: bool = False,
    ) -> ReadyControlResult:
        """Freeze source, and persist Ready provenance when ``READY_FOR_GO``.

        Duplicate ``review_key`` deliveries are safe no-ops (``status=duplicate``).
        Blocked outcomes are not persisted unless ``persist_blocked=True``.
        Storage failures fail closed with ``storage_failure``.
        """
        receipt = evaluate_ready_freeze(source, review)
        if receipt.decision != DECISION_READY_FOR_GO:
            if not persist_blocked:
                return ReadyControlResult(
                    ok=False,
                    receipt=receipt,
                    status="blocked",
                    record=None,
                    reason_codes=receipt.reasons,
                )
            try:
                insert = self._store.record_ready_review(
                    issue_id=receipt.frozen_source.issue_id or "missing-issue-id",
                    issue_identifier=(
                        receipt.frozen_source.issue_identifier or "missing-identifier"
                    ),
                    review_key=receipt.review_key,
                    source_digest=receipt.source_digest,
                    decision=receipt.decision,
                    frozen_source=receipt.frozen_source,
                    starts_agent_work=False,
                )
            except (sqlite3.Error, OSError, ValueError) as exc:
                _ = exc
                return ReadyControlResult(
                    ok=False,
                    receipt=receipt,
                    status="blocked",
                    record=None,
                    reason_codes=tuple(
                        dict.fromkeys((*receipt.reasons, "storage_failure"))
                    ),
                )
            return ReadyControlResult(
                ok=False,
                receipt=receipt,
                status=insert.status if insert.status == "duplicate" else "blocked",
                record=insert.record,
                reason_codes=receipt.reasons,
            )

        try:
            insert = self._store.record_ready_review(
                issue_id=receipt.frozen_source.issue_id,
                issue_identifier=receipt.frozen_source.issue_identifier,
                review_key=receipt.review_key,
                source_digest=receipt.source_digest,
                decision=receipt.decision,
                frozen_source=receipt.frozen_source,
                starts_agent_work=False,
            )
        except (sqlite3.Error, OSError, ValueError) as exc:
            _ = exc
            return ReadyControlResult(
                ok=False,
                receipt=receipt,
                status="blocked",
                record=None,
                reason_codes=("storage_failure",),
            )
        return ReadyControlResult(
            ok=True,
            receipt=receipt,
            status=insert.status,
            record=insert.record,
            reason_codes=(),
        )

    def process_go(
        self,
        transition: Union[NormalizedGoTransition, Mapping[str, Any], None],
        *,
        review_key: Optional[str] = None,
        source_digest: Optional[str] = None,
        team_key: Optional[str] = None,
    ) -> GoControlResult:
        """Create exactly one non-dispatched LaunchIntent when provenance matches.

        Both ``review_key`` and ``source_digest`` are mandatory, canonical, and
        nonempty. Transition ``issue_id`` / ``issue_identifier`` must be exact
        canonical strings (no str() coercion or whitespace trim) and must equal
        the same persisted ``READY_FOR_GO`` row. Optional ``team_key`` must be
        exact-canonical when present/nonempty and must equal the frozen Ready
        team key without trimming. There is no latest-READY fallback.
        """
        if transition is None:
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=("missing_go_transition",),
                reason="missing_go_transition",
            )

        if isinstance(transition, NormalizedGoTransition):
            pass
        elif isinstance(transition, Mapping):
            pass
        else:
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=("missing_go_transition",),
                reason="missing_go_transition",
            )

        raw_issue_id, raw_issue_identifier = _extract_go_transition_identities(
            transition
        )
        issue_id, issue_identifier, identity_codes = _validate_go_transition_identities(
            raw_issue_id, raw_issue_identifier
        )
        if identity_codes:
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=identity_codes,
                reason=identity_codes[0],
            )
        assert issue_id is not None and issue_identifier is not None

        canon_key, canon_digest, selector_codes = _validate_go_provenance_selectors(
            review_key, source_digest
        )
        if selector_codes:
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=selector_codes,
                reason=selector_codes[0],
            )

        go_team, team_codes = _validate_go_team_key(team_key)
        if team_codes:
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=team_codes,
                reason=team_codes[0],
            )

        try:
            provenance_row = self._store.get_ready_provenance(
                issue_id=issue_id,
                review_key=canon_key,
                source_digest=canon_digest,
                require_ready_for_go=True,
            )
        except (sqlite3.Error, OSError) as exc:
            _ = exc
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=("storage_failure",),
                reason="storage_failure",
            )

        if provenance_row is None:
            # Explain why the exact keyed+digest READY_FOR_GO lookup missed.
            # Never fall back to "latest READY for issue".
            try:
                keyed = self._store.get_ready_review_by_key(canon_key or "")
            except (sqlite3.Error, OSError) as exc:
                _ = exc
                return GoControlResult(
                    ok=False,
                    status="rejected",
                    reason_codes=("storage_failure",),
                    reason="storage_failure",
                )
            if keyed is None:
                codes = ("stale_ready_provenance",)
                # Prefer missing when this issue has no Ready rows at all.
                try:
                    any_for_issue = self._store.get_ready_provenance(
                        issue_id=issue_id,
                        review_key=None,
                        source_digest=None,
                        require_ready_for_go=False,
                    )
                except (sqlite3.Error, OSError) as exc:
                    _ = exc
                    return GoControlResult(
                        ok=False,
                        status="rejected",
                        reason_codes=("storage_failure",),
                        reason="storage_failure",
                    )
                if any_for_issue is None:
                    codes = ("missing_ready_provenance",)
            elif keyed.issue_id != issue_id:
                codes = ("stale_ready_provenance",)
            elif keyed.source_digest != canon_digest:
                codes = ("ready_provenance_digest_mismatch",)
            elif keyed.decision != DECISION_READY_FOR_GO:
                codes = ("ready_decision_not_ready_for_go",)
            else:
                codes = ("stale_ready_provenance",)
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=codes,
                reason=codes[0],
            )

        # Exact same-record match (defense in depth after keyed store lookup).
        if provenance_row.source_digest != canon_digest:
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=("ready_provenance_digest_mismatch",),
                reason="ready_provenance_digest_mismatch",
            )
        if provenance_row.review_key != canon_key:
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=("stale_ready_provenance",),
                reason="stale_ready_provenance",
            )
        # Exact identity equality against persisted Ready provenance — no
        # coercion or trimming on either side.
        if (
            provenance_row.issue_id != issue_id
            or provenance_row.issue_identifier != issue_identifier
        ):
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=("issue_identity_mismatch",),
                reason="issue_identity_mismatch",
            )
        if provenance_row.starts_agent_work:
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=("ready_starts_agent_work",),
                reason="ready_starts_agent_work",
            )

        if go_team is not None:
            try:
                frozen = json.loads(provenance_row.frozen_source_json)
            except (TypeError, ValueError):
                frozen = {}
            frozen_team: Any = None
            if isinstance(frozen, Mapping):
                frozen_team = frozen.get("team_key")
            # Exact match only — never str()/strip() frozen or supplied team keys.
            if not isinstance(frozen_team, str) or frozen_team != go_team:
                return GoControlResult(
                    ok=False,
                    status="rejected",
                    reason_codes=("cross_team_mismatch",),
                    reason="cross_team_mismatch",
                )

        provenance = ReadyReviewProvenance(
            issue_id=provenance_row.issue_id,
            review_key=provenance_row.review_key,
            source_digest=provenance_row.source_digest,
            decision=provenance_row.decision,
            starts_agent_work=False,
        )

        try:
            seen_delivery = self._store.list_delivery_keys()
            seen_intents = self._store.list_intent_keys()
        except (sqlite3.Error, OSError) as exc:
            _ = exc
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=("storage_failure",),
                reason="storage_failure",
            )

        plan = plan_explicit_go_launch(
            transition,
            provenance,
            seen_delivery_keys=seen_delivery,
            seen_intent_keys=seen_intents,
        )
        if not plan.ok or plan.intent is None:
            codes = plan.reason_codes or ("go_plan_rejected",)
            # Duplicate detection at the pure planner maps to duplicate status.
            if codes == ("duplicate_delivery_key",) or codes == ("duplicate_intent_key",):
                existing = None
                if "duplicate_delivery_key" in codes and isinstance(transition, Mapping):
                    existing = self._store.get_launch_intent_by_event_key(
                        str(transition.get("go_event_key") or "")
                    )
                elif "duplicate_delivery_key" in codes and isinstance(
                    transition, NormalizedGoTransition
                ):
                    existing = self._store.get_launch_intent_by_event_key(
                        transition.go_event_key
                    )
                return GoControlResult(
                    ok=False,
                    status="duplicate",
                    intent=None,
                    record=existing,
                    reason_codes=codes,
                    reason=codes[0],
                )
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=codes,
                reason=codes[0],
            )

        intent = plan.intent
        assert intent.dispatched is False
        # Intent identities must remain exact matches to the validated transition.
        if (
            intent.issue_id != issue_id
            or intent.issue_identifier != issue_identifier
        ):
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=("issue_identity_mismatch",),
                reason="issue_identity_mismatch",
            )
        try:
            insert = self._store.record_launch_intent(
                issue_id=intent.issue_id,
                issue_identifier=intent.issue_identifier,
                review_key=intent.review_key,
                source_digest=intent.source_digest,
                go_event_key=intent.go_event_key,
                idempotency_key=intent.idempotency_key,
                dispatched=False,
            )
        except ValueError as exc:
            code = str(exc) or "ready_provenance_mismatch"
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=(code,),
                reason=code,
            )
        except (sqlite3.Error, OSError) as exc:
            _ = exc
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=("storage_failure",),
                reason="storage_failure",
            )

        if insert.status == "duplicate":
            return GoControlResult(
                ok=False,
                status="duplicate",
                intent=None,
                record=insert.record,
                reason_codes=(insert.reason or "duplicate_intent_key",),
                reason=insert.reason or "duplicate_intent_key",
            )

        assert insert.record.dispatched is False
        return GoControlResult(
            ok=True,
            status="created",
            intent=intent,
            record=insert.record,
            reason_codes=(),
            reason="",
        )


def process_ready(
    source: Union[ReadySourceInput, Mapping[str, Any], None],
    review: Union[ReadyReviewInput, Mapping[str, Any], None] = None,
    *,
    store: Optional[LinearReadyGoStore] = None,
    persist_blocked: bool = False,
) -> ReadyControlResult:
    """Module-level Ready freeze + persist entry point."""
    return LinearReadyGoControlPlane(store=store).process_ready(
        source, review, persist_blocked=persist_blocked
    )


def process_go(
    transition: Union[NormalizedGoTransition, Mapping[str, Any], None],
    *,
    store: Optional[LinearReadyGoStore] = None,
    review_key: Optional[str] = None,
    source_digest: Optional[str] = None,
    team_key: Optional[str] = None,
) -> GoControlResult:
    """Module-level Go → non-dispatched LaunchIntent entry point."""
    return LinearReadyGoControlPlane(store=store).process_go(
        transition,
        review_key=review_key,
        source_digest=source_digest,
        team_key=team_key,
    )
