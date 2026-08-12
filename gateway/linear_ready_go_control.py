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

        Loads persisted Ready provenance for the transition's ``issue_id`` (optionally
        constrained by ``review_key`` / ``source_digest``), plans via the pure
        ``plan_explicit_go_launch`` helper, and persists the intent. Duplicate
        delivery/intent keys return ``status=duplicate`` with an explainable reason.
        Cross-team Go attempts fail closed when ``team_key`` mismatches frozen
        Ready provenance. Storage failures fail closed with ``storage_failure``.
        """
        if transition is None:
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=("missing_go_transition",),
                reason="missing_go_transition",
            )

        if isinstance(transition, NormalizedGoTransition):
            issue_id = transition.issue_id
        elif isinstance(transition, Mapping):
            issue_id = transition.get("issue_id", "")
        else:
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=("missing_go_transition",),
                reason="missing_go_transition",
            )

        try:
            provenance_row = self._store.get_ready_provenance(
                issue_id=str(issue_id or ""),
                review_key=review_key,
                source_digest=source_digest,
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
            # Distinguish missing vs wrong decision / tamper when a row exists
            # without the READY_FOR_GO requirement.
            try:
                any_row = self._store.get_ready_provenance(
                    issue_id=str(issue_id or ""),
                    review_key=review_key,
                    source_digest=source_digest,
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
            if any_row is None:
                # Stale keyed lookup: a review_key was supplied but does not match
                # any persisted Ready row for this issue.
                if review_key or source_digest:
                    codes = ("stale_ready_provenance",)
                else:
                    codes = ("missing_ready_provenance",)
            elif any_row.decision != DECISION_READY_FOR_GO:
                codes = ("ready_decision_not_ready_for_go",)
            elif source_digest and any_row.source_digest != source_digest:
                codes = ("ready_provenance_digest_mismatch",)
            elif review_key and any_row.review_key != review_key:
                codes = ("stale_ready_provenance",)
            else:
                codes = ("missing_ready_provenance",)
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=codes,
                reason=codes[0],
            )

        if source_digest and provenance_row.source_digest != source_digest:
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=("ready_provenance_digest_mismatch",),
                reason="ready_provenance_digest_mismatch",
            )
        if review_key and provenance_row.review_key != review_key:
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=("stale_ready_provenance",),
                reason="stale_ready_provenance",
            )
        if provenance_row.starts_agent_work:
            return GoControlResult(
                ok=False,
                status="rejected",
                reason_codes=("ready_starts_agent_work",),
                reason="ready_starts_agent_work",
            )

        if team_key is not None and str(team_key).strip():
            try:
                frozen = json.loads(provenance_row.frozen_source_json)
            except (TypeError, ValueError):
                frozen = {}
            frozen_team = ""
            if isinstance(frozen, Mapping):
                frozen_team = str(frozen.get("team_key") or "").strip()
            if not frozen_team or frozen_team != str(team_key).strip():
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
