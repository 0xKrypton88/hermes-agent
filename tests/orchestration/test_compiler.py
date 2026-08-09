"""WP4 — worker brief compiler + planner trigger gates."""

from __future__ import annotations

from agent.orchestration.config import load_orchestration_config
from agent.orchestration.contracts import (
    CapabilityClass,
    ImpactLevel,
    ModelFamily,
    Provenance,
    ReasoningEffort,
    RoutingDecision,
    SideEffectClass,
    TaskSpec,
)
from agent.orchestration.router import route_task


def _compile(spec: TaskSpec, decision: RoutingDecision | None = None):
    from agent.orchestration.compiler import compile_worker_brief

    cfg = load_orchestration_config({})
    decision = decision or route_task(spec, cfg)
    return compile_worker_brief(spec, decision, cfg), decision


def test_luna_brief_is_minimal_strict_no_speculation_or_secrets():
    spec = TaskSpec(
        objective="Format JSON",
        provenance=Provenance.EXPLICIT,
        complexity="low",
        impact=ImpactLevel.LOW,
        capabilities=(CapabilityClass.READ,),
        side_effects=(SideEffectClass.NONE,),
        success_criteria=("valid JSON object",),
        constraints=("no network",),
    )
    compiled, decision = _compile(spec)
    assert decision.family is ModelFamily.LUNA
    brief = compiled.brief.lower()
    assert "objective" in brief
    assert "success criteria" in brief
    assert "output schema" in brief
    assert "do not speculate" in brief or "no speculation" in brief
    # Must forbid private CoT / secret echo — never *request* them.
    assert "do not request private" in brief or "no private" in brief
    assert "share your private" not in brief
    assert "api_key" not in brief
    assert "password" not in brief
    assert compiled.include_planner is False


def test_terra_brief_includes_tools_and_definition_of_done():
    spec = TaskSpec(
        objective="Implement helper and tests",
        provenance=Provenance.INFERRED,
        complexity="moderate",
        impact=ImpactLevel.MODERATE,
        capabilities=(CapabilityClass.READ, CapabilityClass.WRITE, CapabilityClass.EXECUTE),
        side_effects=(SideEffectClass.WRITE,),
        success_criteria=("tests pass",),
        non_goals=("rewrite unrelated modules",),
    )
    compiled, decision = _compile(spec)
    assert decision.family is ModelFamily.TERRA
    brief = compiled.brief.lower()
    assert "definition of done" in brief or "success criteria" in brief
    assert "allowed capabilities" in brief
    assert "approval boundary" in brief
    assert "non-goals" in brief


def test_sol_brief_includes_risk_evidence_and_prior_failure_contract():
    spec = TaskSpec(
        objective="Harden production auth",
        provenance=Provenance.EXPLICIT,
        complexity="high",
        impact=ImpactLevel.CRITICAL,
        capabilities=(CapabilityClass.READ, CapabilityClass.WRITE),
        side_effects=(SideEffectClass.WRITE,),
        unknowns=("rollback_window",),
    )
    decision = RoutingDecision(
        family=ModelFamily.SOL,
        reasoning=ReasoningEffort.HIGH,
        rule_ids=("R_HIGH_CONSEQUENCE",),
        requires_approval=False,
        requires_independent_verification=True,
        route_reason="test",
    )
    compiled, _ = _compile(spec, decision)
    brief = compiled.brief.lower()
    assert "evidence" in brief
    assert "risk" in brief
    assert "prior failure" in brief or "prior-failure" in brief
    assert "independent verification" in brief


def test_planner_only_for_concrete_triggers():
    from agent.orchestration.planner import should_include_planner

    low = TaskSpec(
        objective="typo fix",
        provenance=Provenance.EXPLICIT,
        complexity="low",
        impact=ImpactLevel.LOW,
    )
    assert should_include_planner(low, prior_failures=0) is False

    high = TaskSpec(
        objective="redesign multi-service dependency graph",
        provenance=Provenance.EXPLICIT,
        complexity="high",
        impact=ImpactLevel.HIGH,
    )
    assert should_include_planner(high, prior_failures=0) is True

    hyp = TaskSpec(
        objective="test the hypothesis that cache invalidation causes the outage",
        provenance=Provenance.INFERRED,
        complexity="moderate",
        impact=ImpactLevel.MODERATE,
    )
    assert should_include_planner(hyp, prior_failures=0) is True

    failed = TaskSpec(
        objective="retry previous coding task",
        provenance=Provenance.INFERRED,
        complexity="moderate",
        impact=ImpactLevel.MODERATE,
    )
    assert should_include_planner(failed, prior_failures=2) is True
