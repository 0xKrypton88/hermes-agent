"""Deterministic TaskSpec → LUNA/TERRA/SOL routing.

Same TaskSpec + config yields a reproducible RoutingDecision with stable
rule IDs. Family aliases come from config; concrete models are not hard-coded.
"""

from __future__ import annotations

from typing import List, Optional, Sequence, Set, Tuple

from agent.orchestration.config import OrchestrationConfig, resolve_family_model
from agent.orchestration.contracts import (
    CapabilityClass,
    ImpactLevel,
    ModelFamily,
    ReasoningEffort,
    RoutingDecision,
    RuleId,
    SideEffectClass,
    TaskSpec,
)


# Capability classes each family can satisfy via its default toolsets.
_FAMILY_CAPABILITIES: dict[ModelFamily, frozenset[CapabilityClass]] = {
    ModelFamily.LUNA: frozenset(
        {CapabilityClass.READ, CapabilityClass.NETWORK}
    ),
    ModelFamily.TERRA: frozenset(
        {
            CapabilityClass.READ,
            CapabilityClass.WRITE,
            CapabilityClass.EXECUTE,
            CapabilityClass.NETWORK,
            CapabilityClass.BROWSER,
        }
    ),
    ModelFamily.SOL: frozenset(
        {
            CapabilityClass.READ,
            CapabilityClass.WRITE,
            CapabilityClass.EXECUTE,
            CapabilityClass.NETWORK,
            CapabilityClass.BROWSER,
            CapabilityClass.DELEGATE,
        }
    ),
}

_FAMILY_ORDER = (ModelFamily.LUNA, ModelFamily.TERRA, ModelFamily.SOL)
_REASONING_ORDER = (
    ReasoningEffort.LOW,
    ReasoningEffort.MEDIUM,
    ReasoningEffort.HIGH,
    ReasoningEffort.MAX,
)


def _approval_required(spec: TaskSpec, cfg: OrchestrationConfig) -> bool:
    effects = set(spec.side_effects)
    if cfg.approval.require_for_destructive and SideEffectClass.DESTRUCTIVE in effects:
        return True
    if cfg.approval.require_for_financial and SideEffectClass.FINANCIAL in effects:
        return True
    return False


def _base_family_and_reasoning(
    spec: TaskSpec,
) -> Tuple[ModelFamily, ReasoningEffort, List[str], bool]:
    """Pick the initial family/reasoning from complexity + impact."""
    rules: List[str] = []
    impact = spec.impact
    complexity = (spec.complexity or "low").lower()

    high_consequence = impact in (ImpactLevel.HIGH, ImpactLevel.CRITICAL) or complexity in (
        "high",
        "critical",
    )
    # Security / production / financial high consequence → SOL
    if impact is ImpactLevel.CRITICAL or complexity == "critical":
        rules.append(RuleId.R_HIGH_CONSEQUENCE.value)
        reasoning = (
            ReasoningEffort.MAX if impact is ImpactLevel.CRITICAL else ReasoningEffort.HIGH
        )
        return ModelFamily.SOL, reasoning, rules, True

    if high_consequence and (
        "security" in spec.objective.lower()
        or "production" in spec.objective.lower()
        or SideEffectClass.FINANCIAL in spec.side_effects
        or impact is ImpactLevel.HIGH
    ):
        rules.append(RuleId.R_HIGH_CONSEQUENCE.value)
        return ModelFamily.SOL, ReasoningEffort.HIGH, rules, True

    if complexity in ("moderate", "high") or impact is ImpactLevel.MODERATE:
        rules.append(RuleId.R_NORMAL_MULTI_STEP.value)
        return ModelFamily.TERRA, ReasoningEffort.MEDIUM, rules, False

    rules.append(RuleId.R_LOW_COMPLEXITY.value)
    return ModelFamily.LUNA, ReasoningEffort.LOW, rules, False


def _family_supports(
    family: ModelFamily, needed: Sequence[CapabilityClass]
) -> bool:
    have = _FAMILY_CAPABILITIES[family]
    return set(needed).issubset(have)


def _next_compatible(
    start: ModelFamily, needed: Sequence[CapabilityClass]
) -> Optional[ModelFamily]:
    start_idx = _FAMILY_ORDER.index(start)
    for fam in _FAMILY_ORDER[start_idx:]:
        if _family_supports(fam, needed):
            return fam
    # Also try earlier families if start was somehow too high but mismatched
    for fam in _FAMILY_ORDER:
        if _family_supports(fam, needed):
            return fam
    return None


def _clamp_reasoning(
    desired: ReasoningEffort,
    cfg: OrchestrationConfig,
) -> Tuple[ReasoningEffort, Optional[str], bool]:
    """Clamp to a provider-compatible effort; return (effort, reason, used_fallback)."""
    caps = cfg.reasoning_capabilities
    if caps.get(desired.value, True):
        return desired, None, False

    # Walk downward then upward for an enabled effort.
    idx = _REASONING_ORDER.index(desired)
    for effort in reversed(_REASONING_ORDER[:idx]):
        if caps.get(effort.value, True):
            return (
                effort,
                f"reasoning {desired.value} unsupported; clamped to {effort.value}",
                True,
            )
    for effort in _REASONING_ORDER[idx + 1 :]:
        if caps.get(effort.value, True):
            return (
                effort,
                f"reasoning {desired.value} unsupported; clamped to {effort.value}",
                True,
            )
    # Nothing enabled — keep desired but mark fallback (blocked path handled by caller)
    return (
        desired,
        f"reasoning {desired.value} unsupported; no compatible alternative",
        True,
    )


def route_task(spec: TaskSpec, cfg: OrchestrationConfig) -> RoutingDecision:
    """Deterministically route a TaskSpec to a family + reasoning effort."""
    family, reasoning, rules, needs_verification = _base_family_and_reasoning(spec)

    needed = tuple(spec.capabilities or ())
    if needed and not _family_supports(family, needed):
        rules.append(RuleId.R_CAPABILITY_MISMATCH.value)
        compatible = _next_compatible(family, needed)
        if compatible is None:
            provider_alias, model_alias = resolve_family_model(cfg, family.value)
            return RoutingDecision(
                family=family,
                reasoning=reasoning,
                rule_ids=tuple(rules),
                requires_approval=_approval_required(spec, cfg),
                requires_independent_verification=needs_verification,
                route_reason="capability mismatch; no compatible family",
                blocked=True,
                concrete_provider=provider_alias,
                concrete_model_alias=model_alias,
            )
        family = compatible

    requires_approval = _approval_required(spec, cfg)
    if requires_approval:
        rules.append(RuleId.R_SIDE_EFFECT_APPROVAL.value)

    reasoning, fallback_reason, used_fallback = _clamp_reasoning(reasoning, cfg)
    if used_fallback:
        rules.append(RuleId.R_REASONING_FALLBACK.value)

    if family is ModelFamily.SOL and cfg.verification.independent_for_sol:
        needs_verification = True

    provider_alias, model_alias = resolve_family_model(cfg, family.value)

    reason_parts = [
        f"family={family.value}",
        f"reasoning={reasoning.value}",
        f"complexity={spec.complexity}",
        f"impact={spec.impact.value}",
    ]
    if requires_approval:
        reason_parts.append("approval_required")
    if needs_verification:
        reason_parts.append("independent_verification")
    if fallback_reason:
        reason_parts.append(fallback_reason)

    return RoutingDecision(
        family=family,
        reasoning=reasoning,
        rule_ids=tuple(dict.fromkeys(rules)),  # stable unique order
        requires_approval=requires_approval,
        requires_independent_verification=needs_verification,
        route_reason="; ".join(reason_parts),
        blocked=False,
        fallback_reason=fallback_reason,
        concrete_provider=provider_alias,
        concrete_model_alias=model_alias,
    )
