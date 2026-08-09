"""WP2 — deterministic LUNA/TERRA/SOL routing scenarios."""

from __future__ import annotations

from agent.orchestration.config import load_orchestration_config
from agent.orchestration.contracts import (
    CapabilityClass,
    ImpactLevel,
    ModelFamily,
    Provenance,
    ReasoningEffort,
    RuleId,
    SideEffectClass,
    TaskSpec,
)


def _route(spec: TaskSpec, cfg=None):
    from agent.orchestration.router import route_task

    return route_task(spec, cfg or load_orchestration_config({}))


def test_scenario_a_low_complexity_routes_luna_low():
    spec = TaskSpec(
        objective="Reformat this JSON",
        provenance=Provenance.EXPLICIT,
        complexity="low",
        impact=ImpactLevel.LOW,
        capabilities=(CapabilityClass.READ,),
        side_effects=(SideEffectClass.NONE,),
    )
    decision = _route(spec)
    assert decision.family is ModelFamily.LUNA
    assert decision.reasoning is ReasoningEffort.LOW
    assert RuleId.R_LOW_COMPLEXITY.value in decision.rule_ids
    assert decision.requires_approval is False


def test_scenario_b_normal_research_routes_terra_medium():
    spec = TaskSpec(
        objective="Research and implement a small feature",
        provenance=Provenance.INFERRED,
        complexity="moderate",
        impact=ImpactLevel.MODERATE,
        capabilities=(CapabilityClass.READ, CapabilityClass.WRITE, CapabilityClass.EXECUTE),
        side_effects=(SideEffectClass.WRITE,),
    )
    decision = _route(spec)
    assert decision.family is ModelFamily.TERRA
    assert decision.reasoning is ReasoningEffort.MEDIUM
    assert RuleId.R_NORMAL_MULTI_STEP.value in decision.rule_ids


def test_scenario_c_high_consequence_routes_sol_with_verification():
    spec = TaskSpec(
        objective="Rotate production credentials safely",
        provenance=Provenance.EXPLICIT,
        complexity="high",
        impact=ImpactLevel.CRITICAL,
        capabilities=(CapabilityClass.READ, CapabilityClass.WRITE, CapabilityClass.EXECUTE),
        side_effects=(SideEffectClass.WRITE,),
    )
    decision = _route(spec)
    assert decision.family is ModelFamily.SOL
    assert decision.reasoning in (ReasoningEffort.HIGH, ReasoningEffort.MAX)
    assert decision.requires_independent_verification is True
    assert RuleId.R_HIGH_CONSEQUENCE.value in decision.rule_ids


def test_scenario_d_destructive_or_financial_requires_approval():
    destructive = TaskSpec(
        objective="Delete old backups",
        provenance=Provenance.EXPLICIT,
        complexity="moderate",
        impact=ImpactLevel.HIGH,
        side_effects=(SideEffectClass.DESTRUCTIVE,),
        capabilities=(CapabilityClass.WRITE,),
    )
    financial = TaskSpec(
        objective="Submit a payment transfer",
        provenance=Provenance.EXPLICIT,
        complexity="moderate",
        impact=ImpactLevel.HIGH,
        side_effects=(SideEffectClass.FINANCIAL,),
        capabilities=(CapabilityClass.WRITE,),
    )
    d1 = _route(destructive)
    d2 = _route(financial)
    assert d1.requires_approval is True
    assert d2.requires_approval is True
    assert RuleId.R_SIDE_EFFECT_APPROVAL.value in d1.rule_ids
    assert RuleId.R_SIDE_EFFECT_APPROVAL.value in d2.rule_ids


def test_capability_mismatch_escalates_or_blocks():
    # LUNA default toolsets lack terminal/execute — mismatch should escalate.
    spec = TaskSpec(
        objective="Run a shell build",
        provenance=Provenance.EXPLICIT,
        complexity="low",
        impact=ImpactLevel.LOW,
        capabilities=(CapabilityClass.EXECUTE,),
        side_effects=(SideEffectClass.NONE,),
    )
    decision = _route(spec)
    assert decision.family in (ModelFamily.TERRA, ModelFamily.SOL) or decision.blocked
    assert RuleId.R_CAPABILITY_MISMATCH.value in decision.rule_ids


def test_unsupported_reasoning_produces_explicit_fallback_reason():
    cfg = load_orchestration_config(
        {
            "orchestration": {
                "reasoning_capabilities": {
                    "low": True,
                    "medium": True,
                    "high": False,
                    "max": False,
                }
            }
        }
    )
    spec = TaskSpec(
        objective="Audit security posture",
        provenance=Provenance.EXPLICIT,
        complexity="high",
        impact=ImpactLevel.CRITICAL,
        capabilities=(CapabilityClass.READ,),
        side_effects=(SideEffectClass.NONE,),
    )
    decision = _route(spec, cfg)
    assert decision.family is ModelFamily.SOL
    assert decision.reasoning is ReasoningEffort.MEDIUM  # clamped
    assert decision.fallback_reason
    assert RuleId.R_REASONING_FALLBACK.value in decision.rule_ids


def test_same_taskspec_and_config_yield_reproducible_decision():
    spec = TaskSpec(
        objective="Refactor module X",
        provenance=Provenance.INFERRED,
        complexity="moderate",
        impact=ImpactLevel.MODERATE,
        capabilities=(CapabilityClass.READ, CapabilityClass.WRITE),
        side_effects=(SideEffectClass.WRITE,),
    )
    a = _route(spec)
    b = _route(spec)
    assert a == b
    assert a.rule_ids == b.rule_ids
