"""WP7 — verification outcomes + bounded escalation ladder."""

from __future__ import annotations

from agent.orchestration.config import load_orchestration_config
from agent.orchestration.contracts import (
    ModelFamily,
    ReasoningEffort,
    RuleId,
    VerificationOutcome,
)


def test_verification_outcomes_enum_and_return_path():
    from agent.orchestration.verifier import verify_attempt, AttemptRecord

    result = verify_attempt(
        AttemptRecord(
            family=ModelFamily.TERRA,
            reasoning=ReasoningEffort.MEDIUM,
            success=True,
            schema_ok=True,
            failure_signature=None,
        ),
        history=(),
        cfg=load_orchestration_config({}),
        ask_user_pending=False,
        approval_denied=False,
    )
    assert result.outcome is VerificationOutcome.RETURN
    assert result.reason_code == "OK"


def test_scenario_e_repeated_terra_failure_escalates_to_sol():
    from agent.orchestration.verifier import verify_attempt, AttemptRecord

    history = (
        AttemptRecord(
            family=ModelFamily.TERRA,
            reasoning=ReasoningEffort.MEDIUM,
            success=False,
            schema_ok=True,
            failure_signature="tool_error:x",
        ),
        AttemptRecord(
            family=ModelFamily.TERRA,
            reasoning=ReasoningEffort.HIGH,
            success=False,
            schema_ok=True,
            failure_signature="tool_error:x2",
        ),
    )
    current = AttemptRecord(
        family=ModelFamily.TERRA,
        reasoning=ReasoningEffort.HIGH,
        success=False,
        schema_ok=True,
        failure_signature="tool_error:x3",
    )
    result = verify_attempt(
        current,
        history=history,
        cfg=load_orchestration_config({}),
    )
    assert result.outcome is VerificationOutcome.ESCALATE
    assert result.next_family is ModelFamily.SOL
    assert RuleId.R_ESCALATE_TERRA_TO_SOL.value in result.reason_code or result.reason_code == RuleId.R_ESCALATE_TERRA_TO_SOL.value


def test_schema_failure_retries_only_with_strategy_change():
    from agent.orchestration.verifier import verify_attempt, AttemptRecord

    current = AttemptRecord(
        family=ModelFamily.LUNA,
        reasoning=ReasoningEffort.LOW,
        success=False,
        schema_ok=False,
        failure_signature="schema:missing_status",
        prompt_fingerprint="pf-1",
    )
    result = verify_attempt(
        current,
        history=(),
        cfg=load_orchestration_config({}),
    )
    assert result.outcome is VerificationOutcome.RETRY
    assert result.strategy_change
    assert result.reason_code == RuleId.R_SCHEMA_RETRY.value

    # Identical route/prompt/failure must not loop
    loop = verify_attempt(
        current,
        history=(current,),
        cfg=load_orchestration_config({}),
    )
    assert loop.outcome in (VerificationOutcome.ESCALATE, VerificationOutcome.BLOCK, VerificationOutcome.ASK_USER)
    assert loop.reason_code == RuleId.R_LOOP_GUARD.value or RuleId.R_LOOP_GUARD.value in (loop.reason_code,)


def test_approval_denial_and_blocker_unknown_and_budgets():
    from agent.orchestration.verifier import verify_attempt, AttemptRecord

    denied = verify_attempt(
        AttemptRecord(
            family=ModelFamily.TERRA,
            reasoning=ReasoningEffort.MEDIUM,
            success=False,
            schema_ok=True,
            failure_signature="approval_denied",
        ),
        history=(),
        cfg=load_orchestration_config({}),
        approval_denied=True,
    )
    assert denied.outcome is VerificationOutcome.BLOCK
    assert denied.reason_code == RuleId.R_APPROVAL_DENIED.value

    ask = verify_attempt(
        AttemptRecord(
            family=ModelFamily.TERRA,
            reasoning=ReasoningEffort.MEDIUM,
            success=False,
            schema_ok=True,
            failure_signature="unknown",
        ),
        history=(),
        cfg=load_orchestration_config({}),
        ask_user_pending=True,
    )
    assert ask.outcome is VerificationOutcome.ASK_USER
    assert ask.reason_code == RuleId.R_BLOCKER_UNKNOWN.value

    cfg = load_orchestration_config(
        {"orchestration": {"budgets": {"max_attempts": 2, "max_cost_usd": 0.01, "max_duration_s": 1}}}
    )
    history = (
        AttemptRecord(
            family=ModelFamily.LUNA,
            reasoning=ReasoningEffort.LOW,
            success=False,
            schema_ok=True,
            failure_signature="a",
            cost_usd=0.02,
            duration_s=2,
        ),
    )
    budgeted = verify_attempt(
        AttemptRecord(
            family=ModelFamily.TERRA,
            reasoning=ReasoningEffort.MEDIUM,
            success=False,
            schema_ok=True,
            failure_signature="b",
            cost_usd=0.02,
            duration_s=2,
        ),
        history=history,
        cfg=cfg,
    )
    assert budgeted.outcome in (VerificationOutcome.BLOCK, VerificationOutcome.ASK_USER)
    assert budgeted.reason_code in {
        RuleId.R_MAX_ATTEMPTS.value,
        RuleId.R_MAX_COST.value,
        RuleId.R_MAX_DURATION.value,
    }


def test_default_bounded_ladder_transitions_have_stable_reason_codes():
    from agent.orchestration.verifier import next_ladder_step, DEFAULT_LADDER

    assert DEFAULT_LADDER[0] == (ModelFamily.LUNA, ReasoningEffort.LOW)
    assert DEFAULT_LADDER[1] == (ModelFamily.TERRA, ReasoningEffort.MEDIUM)
    assert DEFAULT_LADDER[2] == (ModelFamily.TERRA, ReasoningEffort.HIGH)
    assert DEFAULT_LADDER[3] == (ModelFamily.SOL, ReasoningEffort.HIGH)

    step = next_ladder_step(ModelFamily.LUNA, ReasoningEffort.LOW)
    assert step == (ModelFamily.TERRA, ReasoningEffort.MEDIUM, "LADDER_LUNA_LOW_TO_TERRA_MEDIUM")

    step2 = next_ladder_step(ModelFamily.TERRA, ReasoningEffort.MEDIUM)
    assert step2[0] is ModelFamily.TERRA and step2[1] is ReasoningEffort.HIGH

    step3 = next_ladder_step(ModelFamily.TERRA, ReasoningEffort.HIGH)
    assert step3[0] is ModelFamily.SOL

    # At most one SOL max/alternative then ASK_USER/BLOCK
    final = next_ladder_step(ModelFamily.SOL, ReasoningEffort.MAX, allow_sol_max=True)
    assert final[0] is None
    assert final[2] in ("LADDER_EXHAUSTED_ASK_USER", "LADDER_EXHAUSTED_BLOCK")
