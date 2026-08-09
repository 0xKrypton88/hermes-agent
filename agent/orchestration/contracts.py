"""Versioned immutable contracts for Adaptive Orchestrator V1.

No secret-bearing fields. Schema / policy / prompt versions exist from day one.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Optional, Tuple


SCHEMA_VERSION = "orch.task_spec.v1"
POLICY_VERSION = "orch.policy.v1"
PROMPT_VERSION = "orch.prompt.v1"


class Provenance(str, Enum):
    EXPLICIT = "explicit"
    INFERRED = "inferred"
    UNKNOWN = "unknown"


class CapabilityClass(str, Enum):
    READ = "read"
    WRITE = "write"
    EXECUTE = "execute"
    NETWORK = "network"
    BROWSER = "browser"
    DELEGATE = "delegate"


class ImpactLevel(str, Enum):
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    CRITICAL = "critical"


class SideEffectClass(str, Enum):
    NONE = "none"
    READ = "read"
    WRITE = "write"
    DESTRUCTIVE = "destructive"
    FINANCIAL = "financial"
    EXTERNAL = "external"


class AutonomyBoundary(str, Enum):
    READ_ONLY = "read_only"
    WRITE_WITH_POLICY = "write_with_policy"
    APPROVAL_REQUIRED = "approval_required"
    BLOCKED = "blocked"


class ModelFamily(str, Enum):
    LUNA = "LUNA"
    TERRA = "TERRA"
    SOL = "SOL"


class ReasoningEffort(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    MAX = "max"


class VerificationOutcome(str, Enum):
    RETURN = "RETURN"
    RETRY = "RETRY"
    ESCALATE = "ESCALATE"
    ASK_USER = "ASK_USER"
    REQUIRE_APPROVAL = "REQUIRE_APPROVAL"
    BLOCK = "BLOCK"


# Stable rule / reason codes used across router, verifier, and traces.
class RuleId(str, Enum):
    R_LOW_COMPLEXITY = "R_LOW_COMPLEXITY"
    R_NORMAL_MULTI_STEP = "R_NORMAL_MULTI_STEP"
    R_HIGH_CONSEQUENCE = "R_HIGH_CONSEQUENCE"
    R_SIDE_EFFECT_APPROVAL = "R_SIDE_EFFECT_APPROVAL"
    R_CAPABILITY_MISMATCH = "R_CAPABILITY_MISMATCH"
    R_REASONING_FALLBACK = "R_REASONING_FALLBACK"
    R_EXPLICIT_FACTS_WIN = "R_EXPLICIT_FACTS_WIN"
    R_BLOCKER_UNKNOWN = "R_BLOCKER_UNKNOWN"
    R_ESCALATE_TERRA_TO_SOL = "R_ESCALATE_TERRA_TO_SOL"
    R_MAX_ATTEMPTS = "R_MAX_ATTEMPTS"
    R_MAX_COST = "R_MAX_COST"
    R_MAX_DURATION = "R_MAX_DURATION"
    R_LOOP_GUARD = "R_LOOP_GUARD"
    R_APPROVAL_DENIED = "R_APPROVAL_DENIED"
    R_SCHEMA_RETRY = "R_SCHEMA_RETRY"
    R_WORKER_RECURSION_GUARD = "R_WORKER_RECURSION_GUARD"
    R_MODE_OFF = "R_MODE_OFF"
    R_MODE_SHADOW = "R_MODE_SHADOW"
    R_MODE_ACTIVE = "R_MODE_ACTIVE"


@dataclass(frozen=True)
class InferredFact:
    """Machine-separated inferred fact with rationale + confidence."""

    key: str
    value: Any
    rationale: str
    confidence: float


@dataclass(frozen=True)
class TaskSpec:
    """Versioned task specification produced by intake/merge."""

    objective: str
    provenance: Provenance
    constraints: Tuple[str, ...] = ()
    success_criteria: Tuple[str, ...] = ()
    capabilities: Tuple[CapabilityClass, ...] = ()
    impact: ImpactLevel = ImpactLevel.LOW
    side_effects: Tuple[SideEffectClass, ...] = (SideEffectClass.NONE,)
    autonomy_boundary: str = AutonomyBoundary.READ_ONLY.value
    unknowns: Tuple[str, ...] = ()
    assumptions: Tuple[str, ...] = ()
    non_goals: Tuple[str, ...] = ()
    complexity: str = "low"  # low | moderate | high | critical
    blocker_unknowns: Tuple[str, ...] = ()
    explicit_facts: Mapping[str, Any] = field(default_factory=dict)
    inferred_facts: Tuple[InferredFact, ...] = ()
    schema_version: str = SCHEMA_VERSION
    policy_version: str = POLICY_VERSION
    prompt_version: str = PROMPT_VERSION


@dataclass(frozen=True)
class RoutingDecision:
    family: ModelFamily
    reasoning: ReasoningEffort
    rule_ids: Tuple[str, ...]
    requires_approval: bool
    requires_independent_verification: bool
    route_reason: str
    blocked: bool = False
    fallback_reason: Optional[str] = None
    concrete_provider: Optional[str] = None
    concrete_model_alias: Optional[str] = None  # family alias key, not hard-coded model


@dataclass(frozen=True)
class CompiledTask:
    brief: str
    family: ModelFamily
    reasoning: ReasoningEffort
    toolsets: Tuple[str, ...]
    output_schema: Mapping[str, Any] = field(default_factory=dict)
    include_planner: bool = False
    planner_reason: Optional[str] = None


@dataclass(frozen=True)
class ExecutionPlan:
    attempts_budget: int
    cost_budget_usd: float
    duration_budget_s: int
    escalation_ladder: Tuple[str, ...]
    verification_required: bool = False


@dataclass(frozen=True)
class WorkerRunRequest:
    """Typed adapter intent for ``tools.delegate_tool._build_child_agent``."""

    goal: str
    context: str
    toolsets: Tuple[str, ...]
    family: ModelFamily
    reasoning: ReasoningEffort
    timeout_seconds: int
    correlation_id: str
    provider_alias: Optional[str] = None  # config family → provider mapping key
    model_alias: Optional[str] = None
    max_iterations: int = 50
    role: str = "leaf"
    parent_session_id: Optional[str] = None
    parent_turn_id: Optional[str] = None
    task_id: Optional[str] = None
    # Side-effect classes the compiled/requested task may exercise without
    # host approval. Destructive/financial/external stay approval-gated.
    allowed_side_effects: Tuple[str, ...] = ()


@dataclass(frozen=True)
class VerificationResult:
    outcome: VerificationOutcome
    reason_code: str
    evidence: Tuple[str, ...] = ()
    next_family: Optional[ModelFamily] = None
    next_reasoning: Optional[ReasoningEffort] = None
    strategy_change: Optional[str] = None


@dataclass(frozen=True)
class ExecutionTrace:
    correlation_id: str
    session_id: str
    task_id: str
    mode: str
    family: ModelFamily
    reasoning: ReasoningEffort
    rule_ids: Tuple[str, ...]
    schema_version: str
    policy_version: str
    prompt_version: str
    attempt: int = 1
    worker_id: Optional[str] = None
    concrete_provider: Optional[str] = None
    concrete_model: Optional[str] = None
    allowed_capabilities: Tuple[str, ...] = ()
    used_tools: Tuple[str, ...] = ()
    approval_outcome: Optional[str] = None
    latency_ms: Optional[int] = None
    input_tokens: int = 0
    output_tokens: int = 0
    estimated_cost_usd: Optional[float] = None
    verification_outcome: Optional[str] = None
    escalation_reason: Optional[str] = None
    error_class: Optional[str] = None
    feedback: Optional[str] = None
    # V1.1 trusted-origin / activation dimensions (ID-safe; no raw prompts).
    origin_platform: Optional[str] = None
    origin_workspace_id: Optional[str] = None
    origin_channel_id: Optional[str] = None
    origin_user_id: Optional[str] = None
    effective_mode: Optional[str] = None
    activation_rule_id: Optional[str] = None
    legacy_parent_executed: bool = False
