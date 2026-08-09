"""Planner inclusion gates for Adaptive Orchestrator V1.

Planner is only included for concrete triggers — never by default for LUNA
or simple TERRA turns.
"""

from __future__ import annotations

from agent.orchestration.contracts import ImpactLevel, TaskSpec


_PLANNER_KEYWORDS = (
    "hypothesis",
    "dependency",
    "system-wide",
    "multi-service",
    "architecture",
    "redesign",
)


def should_include_planner(spec: TaskSpec, *, prior_failures: int = 0) -> bool:
    """Return True only for concrete planner triggers."""
    complexity = (spec.complexity or "low").lower()
    if complexity in ("high", "critical"):
        return True
    if spec.impact in (ImpactLevel.HIGH, ImpactLevel.CRITICAL):
        return True
    if prior_failures and prior_failures > 0:
        return True

    objective = (spec.objective or "").lower()
    if any(k in objective for k in _PLANNER_KEYWORDS):
        return True

    # Dependency / system breadth via capabilities + unknowns volume
    if len(spec.unknowns) >= 3 and complexity == "moderate":
        return True

    return False


def planner_reason(spec: TaskSpec, *, prior_failures: int = 0) -> str:
    if not should_include_planner(spec, prior_failures=prior_failures):
        return ""
    complexity = (spec.complexity or "low").lower()
    if complexity in ("high", "critical") or spec.impact in (
        ImpactLevel.HIGH,
        ImpactLevel.CRITICAL,
    ):
        return "high_consequence_or_complexity"
    if prior_failures:
        return "prior_failures"
    return "dependency_or_hypothesis"
