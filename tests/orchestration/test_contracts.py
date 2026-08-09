"""WP1 — Adaptive Orchestrator core contracts.

Behavioral contracts for versioned immutable TaskSpec / routing / execution
types. No secret-bearing fields; schema/policy/prompt versions exist from day one.
"""

from __future__ import annotations

import dataclasses

import pytest


def test_task_spec_is_versioned_immutable_and_has_provenance():
    from agent.orchestration.contracts import (
        SCHEMA_VERSION,
        CapabilityClass,
        ImpactLevel,
        Provenance,
        SideEffectClass,
        TaskSpec,
    )

    spec = TaskSpec(
        objective="Summarize the README",
        provenance=Provenance.EXPLICIT,
        constraints=("no network",),
        success_criteria=("return a short summary",),
        capabilities=(CapabilityClass.READ,),
        impact=ImpactLevel.LOW,
        side_effects=(SideEffectClass.NONE,),
        autonomy_boundary="read_only",
    )

    assert SCHEMA_VERSION
    assert spec.schema_version == SCHEMA_VERSION
    assert spec.policy_version
    assert spec.prompt_version
    assert spec.provenance is Provenance.EXPLICIT
    assert dataclasses.is_dataclass(spec)
    with pytest.raises(dataclasses.FrozenInstanceError):
        spec.objective = "mutated"  # type: ignore[misc]


def test_contracts_expose_routing_execution_and_trace_types():
    from agent.orchestration.contracts import (
        CompiledTask,
        ExecutionPlan,
        ExecutionTrace,
        ModelFamily,
        ReasoningEffort,
        RoutingDecision,
        VerificationResult,
        WorkerRunRequest,
        VerificationOutcome,
    )

    decision = RoutingDecision(
        family=ModelFamily.LUNA,
        reasoning=ReasoningEffort.LOW,
        rule_ids=("R_LOW_COMPLEXITY",),
        requires_approval=False,
        requires_independent_verification=False,
        route_reason="low complexity deterministic transform",
    )
    assert decision.family is ModelFamily.LUNA
    assert decision.reasoning is ReasoningEffort.LOW

    plan = ExecutionPlan(
        attempts_budget=3,
        cost_budget_usd=1.0,
        duration_budget_s=120,
        escalation_ladder=("LUNA:low", "TERRA:medium"),
    )
    compiled = CompiledTask(
        brief="objective: Summarize the README",
        family=ModelFamily.LUNA,
        reasoning=ReasoningEffort.LOW,
        toolsets=("file",),
        output_schema={"type": "object"},
    )
    worker = WorkerRunRequest(
        goal="Summarize the README",
        context=compiled.brief,
        toolsets=compiled.toolsets,
        family=ModelFamily.LUNA,
        reasoning=ReasoningEffort.LOW,
        timeout_seconds=60,
        correlation_id="corr-1",
    )
    verification = VerificationResult(
        outcome=VerificationOutcome.RETURN,
        reason_code="OK",
        evidence=("summary present",),
    )
    trace = ExecutionTrace(
        correlation_id="corr-1",
        session_id="sess-1",
        task_id="task-1",
        mode="shadow",
        family=ModelFamily.LUNA,
        reasoning=ReasoningEffort.LOW,
        rule_ids=("R_LOW_COMPLEXITY",),
        schema_version="1",
        policy_version="1",
        prompt_version="1",
    )

    assert plan.attempts_budget == 3
    assert compiled.family is ModelFamily.LUNA
    assert worker.toolsets == ("file",)
    assert verification.outcome is VerificationOutcome.RETURN
    assert trace.mode == "shadow"
    for obj in (decision, plan, compiled, worker, verification, trace):
        assert dataclasses.is_dataclass(obj)
        assert obj.__dataclass_params__.frozen  # type: ignore[attr-defined]


def test_contracts_have_no_secret_bearing_field_names():
    from agent.orchestration import contracts as c

    forbidden = {"api_key", "password", "token", "secret", "credential", "private_key"}
    for name, cls in vars(c).items():
        if not dataclasses.is_dataclass(cls):
            continue
        field_names = {f.name.lower() for f in dataclasses.fields(cls)}
        assert not (field_names & forbidden), f"{name} has secret-bearing fields"
