"""Verifier + bounded escalation ladder for Adaptive Orchestrator V1.

Outcomes: RETURN | RETRY | ESCALATE | ASK_USER | REQUIRE_APPROVAL | BLOCK.
Default ladder: LUNA low → TERRA medium → TERRA high → SOL high → at most one
SOL max/alternative when configured → ASK_USER/BLOCK.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

from agent.orchestration.config import OrchestrationConfig
from agent.orchestration.contracts import (
    ModelFamily,
    ReasoningEffort,
    RuleId,
    VerificationOutcome,
    VerificationResult,
)


DEFAULT_LADDER: Tuple[Tuple[ModelFamily, ReasoningEffort], ...] = (
    (ModelFamily.LUNA, ReasoningEffort.LOW),
    (ModelFamily.TERRA, ReasoningEffort.MEDIUM),
    (ModelFamily.TERRA, ReasoningEffort.HIGH),
    (ModelFamily.SOL, ReasoningEffort.HIGH),
    (ModelFamily.SOL, ReasoningEffort.MAX),
)


@dataclass(frozen=True)
class AttemptRecord:
    family: ModelFamily
    reasoning: ReasoningEffort
    success: bool
    schema_ok: bool
    failure_signature: Optional[str] = None
    prompt_fingerprint: Optional[str] = None
    cost_usd: float = 0.0
    duration_s: float = 0.0
    requires_approval: bool = False


def next_ladder_step(
    family: ModelFamily,
    reasoning: ReasoningEffort,
    *,
    allow_sol_max: bool = True,
) -> Tuple[Optional[ModelFamily], Optional[ReasoningEffort], str]:
    """Return the next ladder step or exhausted sentinel."""
    current = (family, reasoning)
    try:
        idx = DEFAULT_LADDER.index(current)
    except ValueError:
        # Snap to nearest later step by family order
        for i, step in enumerate(DEFAULT_LADDER):
            if step[0] == family and _effort_rank(step[1]) > _effort_rank(reasoning):
                idx = i - 1
                break
        else:
            for i, step in enumerate(DEFAULT_LADDER):
                if _family_rank(step[0]) > _family_rank(family):
                    return step[0], step[1], f"LADDER_SNAP_TO_{step[0].value}_{step[1].value.upper()}"
            return None, None, "LADDER_EXHAUSTED_ASK_USER"

    nxt = idx + 1
    if nxt >= len(DEFAULT_LADDER):
        return None, None, "LADDER_EXHAUSTED_ASK_USER"

    fam, effort = DEFAULT_LADDER[nxt]
    if fam is ModelFamily.SOL and effort is ReasoningEffort.MAX and not allow_sol_max:
        return None, None, "LADDER_EXHAUSTED_BLOCK"

    code = f"LADDER_{family.value}_{reasoning.value.upper()}_TO_{fam.value}_{effort.value.upper()}"
    return fam, effort, code


def _effort_rank(effort: ReasoningEffort) -> int:
    order = {
        ReasoningEffort.LOW: 0,
        ReasoningEffort.MEDIUM: 1,
        ReasoningEffort.HIGH: 2,
        ReasoningEffort.MAX: 3,
    }
    return order[effort]


def _family_rank(family: ModelFamily) -> int:
    order = {ModelFamily.LUNA: 0, ModelFamily.TERRA: 1, ModelFamily.SOL: 2}
    return order[family]


def _looping(current: AttemptRecord, history: Sequence[AttemptRecord]) -> bool:
    if not current.failure_signature:
        return False
    for prev in history:
        if (
            prev.family == current.family
            and prev.reasoning == current.reasoning
            and prev.prompt_fingerprint == current.prompt_fingerprint
            and prev.failure_signature == current.failure_signature
            and not prev.success
        ):
            return True
    return False


def _terra_failures(history: Sequence[AttemptRecord], current: AttemptRecord) -> int:
    count = 0
    for rec in list(history) + [current]:
        if rec.family is ModelFamily.TERRA and not rec.success:
            count += 1
    return count


def verify_attempt(
    current: AttemptRecord,
    *,
    history: Sequence[AttemptRecord],
    cfg: OrchestrationConfig,
    ask_user_pending: bool = False,
    approval_denied: bool = False,
    require_approval: bool = False,
) -> VerificationResult:
    """Decide the next verification outcome for an attempt."""
    if current.success and current.schema_ok:
        return VerificationResult(
            outcome=VerificationOutcome.RETURN,
            reason_code="OK",
            evidence=("attempt succeeded",),
        )

    if approval_denied:
        return VerificationResult(
            outcome=VerificationOutcome.BLOCK,
            reason_code=RuleId.R_APPROVAL_DENIED.value,
            evidence=("approval denied",),
        )

    if require_approval:
        return VerificationResult(
            outcome=VerificationOutcome.REQUIRE_APPROVAL,
            reason_code=RuleId.R_SIDE_EFFECT_APPROVAL.value,
            evidence=("approval required",),
        )

    if ask_user_pending:
        return VerificationResult(
            outcome=VerificationOutcome.ASK_USER,
            reason_code=RuleId.R_BLOCKER_UNKNOWN.value,
            evidence=("blocker unknown",),
        )

    attempts_used = len(history) + 1
    total_cost = sum(h.cost_usd for h in history) + current.cost_usd
    total_duration = sum(h.duration_s for h in history) + current.duration_s

    if attempts_used >= cfg.budgets.max_attempts:
        return VerificationResult(
            outcome=VerificationOutcome.BLOCK,
            reason_code=RuleId.R_MAX_ATTEMPTS.value,
            evidence=(f"attempts={attempts_used}",),
        )
    if total_cost > cfg.budgets.max_cost_usd:
        return VerificationResult(
            outcome=VerificationOutcome.BLOCK,
            reason_code=RuleId.R_MAX_COST.value,
            evidence=(f"cost={total_cost}",),
        )
    if total_duration > cfg.budgets.max_duration_s:
        return VerificationResult(
            outcome=VerificationOutcome.BLOCK,
            reason_code=RuleId.R_MAX_DURATION.value,
            evidence=(f"duration={total_duration}",),
        )

    if _looping(current, history):
        # Identical route/prompt/failure — escalate or stop, never retry same
        nxt_fam, nxt_eff, code = next_ladder_step(
            current.family,
            current.reasoning,
            allow_sol_max=cfg.reasoning_capabilities.get("max", True),
        )
        if nxt_fam is None:
            return VerificationResult(
                outcome=VerificationOutcome.ASK_USER,
                reason_code=RuleId.R_LOOP_GUARD.value,
                evidence=(code,),
            )
        return VerificationResult(
            outcome=VerificationOutcome.ESCALATE,
            reason_code=RuleId.R_LOOP_GUARD.value,
            evidence=(code,),
            next_family=nxt_fam,
            next_reasoning=nxt_eff,
            strategy_change="route_escalation_after_loop",
        )

    if not current.schema_ok and cfg.verification.schema_retry_once:
        # Schema failure may retry only with a strategy/prompt change
        already_schema_retried = any(
            (not h.schema_ok) and h.family == current.family and h.reasoning == current.reasoning
            for h in history
        )
        if not already_schema_retried:
            return VerificationResult(
                outcome=VerificationOutcome.RETRY,
                reason_code=RuleId.R_SCHEMA_RETRY.value,
                evidence=("schema validation failed",),
                strategy_change="tighten_output_schema_instructions",
                next_family=current.family,
                next_reasoning=current.reasoning,
            )

    # Scenario E: repeated TERRA failure → SOL
    if current.family is ModelFamily.TERRA and _terra_failures(history, current) >= 2:
        return VerificationResult(
            outcome=VerificationOutcome.ESCALATE,
            reason_code=RuleId.R_ESCALATE_TERRA_TO_SOL.value,
            evidence=(f"terra_failures={_terra_failures(history, current)}",),
            next_family=ModelFamily.SOL,
            next_reasoning=ReasoningEffort.HIGH,
            strategy_change="escalate_to_sol",
        )

    nxt_fam, nxt_eff, code = next_ladder_step(
        current.family,
        current.reasoning,
        allow_sol_max=cfg.reasoning_capabilities.get("max", True),
    )
    if nxt_fam is None:
        return VerificationResult(
            outcome=VerificationOutcome.ASK_USER,
            reason_code=code,
            evidence=("ladder exhausted",),
        )

    return VerificationResult(
        outcome=VerificationOutcome.ESCALATE,
        reason_code=code,
        evidence=("escalate_along_ladder",),
        next_family=nxt_fam,
        next_reasoning=nxt_eff,
        strategy_change="ladder_step",
    )
