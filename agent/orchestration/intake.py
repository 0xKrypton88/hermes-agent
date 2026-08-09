"""Intake / classifier adapter and merge rules.

Explicit facts and hard risk rules always win. The classifier cannot lower
risk, overwrite explicit facts, approve side effects, or hide blocker unknowns.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from agent.orchestration.contracts import (
    CapabilityClass,
    ImpactLevel,
    InferredFact,
    Provenance,
    SideEffectClass,
    TaskSpec,
)


_IMPACT_RANK = {
    ImpactLevel.LOW: 0,
    ImpactLevel.MODERATE: 1,
    ImpactLevel.HIGH: 2,
    ImpactLevel.CRITICAL: 3,
}

_COMPLEXITY_RANK = {"low": 0, "moderate": 1, "high": 2, "critical": 3}

_VALID_COMPLEXITY = frozenset(_COMPLEXITY_RANK)
_VALID_IMPACT = frozenset(i.value for i in ImpactLevel)
_VALID_SIDE_EFFECTS = frozenset(s.value for s in SideEffectClass)
_VALID_CAPABILITIES = frozenset(c.value for c in CapabilityClass)

_CONFIDENCE_FLOOR = 0.45


@dataclass(frozen=True)
class ClassifierOutput:
    complexity: str
    impact: str
    side_effects: Tuple[str, ...]
    confidence: float
    capabilities: Tuple[str, ...] = ()
    unknowns: Tuple[str, ...] = ()
    blocker_unknowns: Tuple[str, ...] = ()
    objective: Optional[str] = None
    success_criteria: Tuple[str, ...] = ()
    constraints: Tuple[str, ...] = ()


@dataclass(frozen=True)
class IntakeResult:
    task_spec: TaskSpec
    used_fallback: bool
    ask_user: bool
    classifier_approved_side_effects: bool = False
    merge_notes: Tuple[str, ...] = ()


def parse_classifier_output(raw: Any) -> ClassifierOutput:
    """Validate structured classifier output; raise ValueError if malformed."""
    if not isinstance(raw, Mapping):
        raise ValueError("classifier output must be a mapping")

    complexity = raw.get("complexity")
    impact = raw.get("impact", "low")
    confidence = raw.get("confidence")
    side_effects = raw.get("side_effects", ["none"])
    capabilities = raw.get("capabilities", [])
    unknowns = raw.get("unknowns", [])
    blocker_unknowns = raw.get("blocker_unknowns", [])

    if not isinstance(complexity, str) or complexity not in _VALID_COMPLEXITY:
        raise ValueError(f"invalid complexity: {complexity!r}")
    if not isinstance(impact, str) or impact not in _VALID_IMPACT:
        raise ValueError(f"invalid impact: {impact!r}")
    if not isinstance(confidence, (int, float)) or isinstance(confidence, bool):
        raise ValueError(f"invalid confidence: {confidence!r}")
    if not isinstance(side_effects, (list, tuple)):
        raise ValueError("side_effects must be a list")
    for se in side_effects:
        if se not in _VALID_SIDE_EFFECTS:
            raise ValueError(f"invalid side_effect: {se!r}")
    if not isinstance(capabilities, (list, tuple)):
        raise ValueError("capabilities must be a list")
    for cap in capabilities:
        if cap not in _VALID_CAPABILITIES:
            raise ValueError(f"invalid capability: {cap!r}")

    return ClassifierOutput(
        complexity=complexity,
        impact=impact,
        side_effects=tuple(side_effects) or (SideEffectClass.NONE.value,),
        confidence=float(confidence),
        capabilities=tuple(capabilities),
        unknowns=tuple(unknowns or ()),
        blocker_unknowns=tuple(blocker_unknowns or ()),
        objective=raw.get("objective") if isinstance(raw.get("objective"), str) else None,
        success_criteria=tuple(raw.get("success_criteria") or ()),
        constraints=tuple(raw.get("constraints") or ()),
    )


def _heuristic_fallback(user_text: str) -> ClassifierOutput:
    """English + Swedish fail-safe heuristic (used only when structured path fails)."""
    text = (user_text or "").lower()
    complexity = "low"
    impact = "low"
    side_effects: List[str] = [SideEffectClass.NONE.value]
    capabilities: List[str] = [CapabilityClass.READ.value]

    if any(
        k in text
        for k in (
            "implement",
            "refactor",
            "research",
            "multi-step",
            "feature",
            "troubleshoot",
            "debug",
            "implementera",
            "refaktorera",
            "felsök",
            "utred",
            "undersök",
            "funktion",
        )
    ):
        complexity = "moderate"
        impact = "moderate"
        side_effects = [SideEffectClass.WRITE.value]
        capabilities = [
            CapabilityClass.READ.value,
            CapabilityClass.WRITE.value,
            CapabilityClass.EXECUTE.value,
        ]
    if any(
        k in text
        for k in (
            "security",
            "production",
            "credential",
            "payment",
            "deploy",
            "säkerhet",
            "produktion",
            "betalning",
            "driftsätt",
            "legitimation",
        )
    ):
        complexity = "high"
        impact = "high"
    if any(
        k in text
        for k in ("delete", "drop", "destroy", "wipe", "radera", "förstör", "töm")
    ):
        side_effects = [SideEffectClass.DESTRUCTIVE.value]
        impact = "high"
    if any(
        k in text
        for k in (
            "payment",
            "transfer",
            "trade",
            "order",
            "betalning",
            "överföring",
            "handel",
            "orderläggning",
        )
    ):
        side_effects = list(dict.fromkeys(side_effects + [SideEffectClass.FINANCIAL.value]))
        impact = "high"

    return ClassifierOutput(
        complexity=complexity,
        impact=impact,
        side_effects=tuple(side_effects),
        confidence=0.0,
        capabilities=tuple(capabilities),
        unknowns=(),
        blocker_unknowns=(),
    )


def _parse_impact(value: str) -> ImpactLevel:
    return ImpactLevel(value)


def _max_impact(a: ImpactLevel, b: ImpactLevel) -> ImpactLevel:
    return a if _IMPACT_RANK[a] >= _IMPACT_RANK[b] else b


def _max_complexity(a: str, b: str) -> str:
    return a if _COMPLEXITY_RANK.get(a, 0) >= _COMPLEXITY_RANK.get(b, 0) else b


def _parse_side_effects(values: Sequence[str]) -> Tuple[SideEffectClass, ...]:
    out: List[SideEffectClass] = []
    for v in values:
        try:
            se = SideEffectClass(v)
        except ValueError:
            continue
        if se not in out:
            out.append(se)
    if not out:
        out.append(SideEffectClass.NONE)
    # Drop NONE if any real effect present
    if len(out) > 1 and SideEffectClass.NONE in out:
        out = [s for s in out if s is not SideEffectClass.NONE]
    return tuple(out)


def _parse_capabilities(values: Sequence[str]) -> Tuple[CapabilityClass, ...]:
    out: List[CapabilityClass] = []
    for v in values:
        try:
            cap = CapabilityClass(v)
        except ValueError:
            continue
        if cap not in out:
            out.append(cap)
    return tuple(out)


def _parse_inferred_facts(
    classifier_raw: Optional[Mapping[str, Any]],
    *,
    user_text: str,
) -> Tuple[InferredFact, ...]:
    """Extract inferred facts without duplicating raw prompts/secrets."""
    if not isinstance(classifier_raw, Mapping):
        return ()
    raw_items = classifier_raw.get("inferred_facts") or ()
    if not isinstance(raw_items, (list, tuple)):
        return ()
    out: List[InferredFact] = []
    prompt = (user_text or "").strip()
    for item in raw_items:
        if not isinstance(item, Mapping):
            continue
        key = str(item.get("key") or "").strip()
        if not key:
            continue
        value = item.get("value")
        rationale = str(item.get("rationale") or "").strip()
        try:
            confidence = float(item.get("confidence", 0.0))
        except (TypeError, ValueError):
            confidence = 0.0
        confidence = max(0.0, min(1.0, confidence))
        # Never store raw prompt duplication in inferred bags.
        if prompt and (
            (isinstance(value, str) and value.strip() == prompt)
            or rationale.strip() == prompt
        ):
            continue
        out.append(
            InferredFact(
                key=key[:128],
                value=value,
                rationale=rationale[:500],
                confidence=confidence,
            )
        )
    return tuple(out)


def merge_intake(
    user_text: str,
    classifier_raw: Any,
    explicit_facts: Optional[Mapping[str, Any]] = None,
) -> IntakeResult:
    """Merge classifier output with explicit facts; explicit/hard risk always win."""
    explicit_facts = dict(explicit_facts or {})
    notes: List[str] = []
    used_fallback = False
    classifier_approved = False

    if isinstance(classifier_raw, Mapping) and classifier_raw.get("approval_suggested"):
        # Classifier suggestions to approve side effects are ignored.
        classifier_approved = False
        notes.append("ignored classifier approval_suggested")

    try:
        if classifier_raw is None:
            raise ValueError("no classifier output")
        parsed = parse_classifier_output(classifier_raw)
        if parsed.confidence < _CONFIDENCE_FLOOR:
            raise ValueError("low confidence")
    except (ValueError, TypeError) as exc:
        notes.append(f"classifier fallback: {exc}")
        parsed = _heuristic_fallback(user_text)
        used_fallback = True

    complexity = parsed.complexity
    impact = _parse_impact(parsed.impact)
    side_effects = _parse_side_effects(parsed.side_effects)
    capabilities = _parse_capabilities(parsed.capabilities)
    unknowns = list(parsed.unknowns)
    blocker_unknowns = list(parsed.blocker_unknowns)

    # Explicit facts always win / can only raise risk
    if "complexity" in explicit_facts:
        complexity = _max_complexity(complexity, str(explicit_facts["complexity"]))
        notes.append("explicit complexity applied")
    if "impact" in explicit_facts:
        impact = _max_impact(impact, _parse_impact(str(explicit_facts["impact"])))
        notes.append("explicit impact applied")
    if "side_effects" in explicit_facts:
        explicit_se = _parse_side_effects(list(explicit_facts["side_effects"]))
        # Union — classifier cannot remove explicit side effects
        merged_se = list(side_effects)
        for se in explicit_se:
            if se not in merged_se:
                merged_se.append(se)
        side_effects = _parse_side_effects([s.value for s in merged_se])
        notes.append("explicit side_effects applied")
    if "capabilities" in explicit_facts:
        capabilities = _parse_capabilities(list(explicit_facts["capabilities"]))
    if "unknowns" in explicit_facts:
        for u in explicit_facts["unknowns"]:
            if u not in unknowns:
                unknowns.append(str(u))
    if "blocker_unknowns" in explicit_facts:
        for u in explicit_facts["blocker_unknowns"]:
            if u not in blocker_unknowns:
                blocker_unknowns.append(str(u))
        notes.append("explicit blocker_unknowns applied")

    # Classifier cannot lower risk below what heuristics / explicit imply
    # (already enforced via max_*). Also strip NONE when destructive/financial.
    if any(s in side_effects for s in (SideEffectClass.DESTRUCTIVE, SideEffectClass.FINANCIAL)):
        side_effects = tuple(
            s for s in side_effects if s is not SideEffectClass.NONE
        ) or (SideEffectClass.DESTRUCTIVE,)

    ask_user = bool(blocker_unknowns)
    provenance = Provenance.EXPLICIT if explicit_facts else (
        Provenance.UNKNOWN if used_fallback and parsed.confidence <= 0 else Provenance.INFERRED
    )
    if used_fallback and not explicit_facts:
        provenance = Provenance.INFERRED

    objective = (
        explicit_facts.get("objective")
        or parsed.objective
        or (user_text or "").strip()
        or "unspecified objective"
    )

    autonomy = "read_only"
    if any(s in side_effects for s in (SideEffectClass.DESTRUCTIVE, SideEffectClass.FINANCIAL)):
        autonomy = "approval_required"
    elif SideEffectClass.WRITE in side_effects:
        autonomy = "write_with_policy"

    inferred_facts = _parse_inferred_facts(
        classifier_raw if isinstance(classifier_raw, Mapping) else None,
        user_text=user_text,
    )

    spec = TaskSpec(
        objective=str(objective)[:2000],
        provenance=provenance,
        constraints=tuple(explicit_facts.get("constraints") or parsed.constraints),
        success_criteria=tuple(
            explicit_facts.get("success_criteria") or parsed.success_criteria
        ),
        capabilities=capabilities,
        impact=impact,
        side_effects=side_effects,
        autonomy_boundary=autonomy,
        unknowns=tuple(unknowns),
        assumptions=tuple(explicit_facts.get("assumptions") or ()),
        non_goals=tuple(explicit_facts.get("non_goals") or ()),
        complexity=complexity,
        blocker_unknowns=tuple(blocker_unknowns),
        explicit_facts=dict(explicit_facts),
        inferred_facts=inferred_facts,
    )

    return IntakeResult(
        task_spec=spec,
        used_fallback=used_fallback,
        ask_user=ask_user,
        classifier_approved_side_effects=classifier_approved,
        merge_notes=tuple(notes),
    )
