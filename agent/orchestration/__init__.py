"""Adaptive Orchestrator V1 — feature-flagged control plane.

Isolated worker routing for top-level turns. Parent prompt cache, history,
tool schemas, and model selection remain immutable mid-session.
"""

from __future__ import annotations

from agent.orchestration.contracts import (
    SCHEMA_VERSION,
    POLICY_VERSION,
    PROMPT_VERSION,
    AutonomyBoundary,
    CapabilityClass,
    CompiledTask,
    ExecutionPlan,
    ExecutionTrace,
    ImpactLevel,
    ModelFamily,
    Provenance,
    ReasoningEffort,
    RoutingDecision,
    SideEffectClass,
    TaskSpec,
    VerificationOutcome,
    VerificationResult,
    WorkerRunRequest,
)
from agent.orchestration.origin import TurnOrigin, turn_origin_from_agent, turn_origin_from_session_source
from agent.orchestration.service import OrchestrationTurnResult, maybe_orchestrate_turn

__all__ = [
    "SCHEMA_VERSION",
    "POLICY_VERSION",
    "PROMPT_VERSION",
    "AutonomyBoundary",
    "CapabilityClass",
    "CompiledTask",
    "ExecutionPlan",
    "ExecutionTrace",
    "ImpactLevel",
    "ModelFamily",
    "Provenance",
    "ReasoningEffort",
    "RoutingDecision",
    "SideEffectClass",
    "TaskSpec",
    "VerificationOutcome",
    "VerificationResult",
    "WorkerRunRequest",
    "TurnOrigin",
    "turn_origin_from_agent",
    "turn_origin_from_session_source",
    "OrchestrationTurnResult",
    "maybe_orchestrate_turn",
]
