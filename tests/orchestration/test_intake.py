"""WP3 — classifier adapter + merge where explicit facts / hard risk win."""

from __future__ import annotations

import pytest

from agent.orchestration.contracts import (
    ImpactLevel,
    Provenance,
    SideEffectClass,
)


def test_malformed_classifier_output_falls_back_deterministically():
    from agent.orchestration.intake import ClassifierOutput, merge_intake

    # Malformed / low-confidence → deterministic inferred fallback
    result = merge_intake(
        user_text="please summarize this note",
        classifier_raw={"complexity": "???","confidence": 0.1},
        explicit_facts={},
    )
    assert result.task_spec.provenance in (Provenance.INFERRED, Provenance.UNKNOWN)
    assert result.task_spec.complexity in ("low", "moderate")
    assert result.used_fallback is True


def test_explicit_facts_and_hard_risk_always_win():
    from agent.orchestration.intake import merge_intake

    result = merge_intake(
        user_text="clean up temporary files",
        classifier_raw={
            "complexity": "low",
            "impact": "low",
            "side_effects": ["none"],
            "confidence": 0.99,
            # Classifier tries to approve / lower risk — must not stick
            "approval_suggested": True,
            "impact_override": "low",
        },
        explicit_facts={
            "impact": "critical",
            "side_effects": ["destructive", "financial"],
            "complexity": "high",
        },
    )
    spec = result.task_spec
    assert spec.impact is ImpactLevel.CRITICAL
    assert SideEffectClass.DESTRUCTIVE in spec.side_effects
    assert SideEffectClass.FINANCIAL in spec.side_effects
    assert spec.complexity == "high"
    # Classifier cannot approve side effects
    assert result.classifier_approved_side_effects is False


def test_classifier_cannot_lower_risk_or_hide_blocker_unknowns():
    from agent.orchestration.intake import merge_intake

    result = merge_intake(
        user_text="deploy the payment service",
        classifier_raw={
            "complexity": "low",
            "impact": "low",
            "side_effects": ["none"],
            "confidence": 0.95,
            "unknowns": [],
        },
        explicit_facts={
            "impact": "high",
            "blocker_unknowns": ["target_environment"],
        },
    )
    spec = result.task_spec
    assert spec.impact is ImpactLevel.HIGH  # not lowered to low
    assert "target_environment" in spec.blocker_unknowns
    assert result.ask_user is True


def test_non_blocking_unknown_does_not_halt():
    from agent.orchestration.intake import merge_intake

    result = merge_intake(
        user_text="refactor helper module",
        classifier_raw={
            "complexity": "moderate",
            "impact": "moderate",
            "side_effects": ["write"],
            "confidence": 0.8,
            "unknowns": ["preferred_style"],
            "blocker_unknowns": [],
        },
        explicit_facts={},
    )
    assert "preferred_style" in result.task_spec.unknowns
    assert result.ask_user is False
    assert result.task_spec.blocker_unknowns == ()


def test_structured_classifier_schema_validation():
    from agent.orchestration.intake import ClassifierOutput, parse_classifier_output

    good = parse_classifier_output(
        {
            "complexity": "moderate",
            "impact": "moderate",
            "side_effects": ["write"],
            "confidence": 0.77,
            "capabilities": ["read", "write"],
            "unknowns": [],
        }
    )
    assert isinstance(good, ClassifierOutput)
    assert good.complexity == "moderate"

    with pytest.raises(ValueError):
        parse_classifier_output({"complexity": 123, "confidence": "nope"})


def test_explicit_inferred_and_unknown_facts_remain_separate_through_merge():
    """Machine-separated provenance: explicit wins; inferred carries rationale/confidence."""
    from agent.orchestration.intake import merge_intake
    from agent.orchestration.compiler import compile_worker_brief
    from agent.orchestration.router import route_task
    from agent.orchestration.config import load_orchestration_config

    result = merge_intake(
        user_text="refactor helper module carefully",
        classifier_raw={
            "complexity": "moderate",
            "impact": "low",
            "side_effects": ["write"],
            "confidence": 0.81,
            "capabilities": ["read", "write"],
            "unknowns": ["preferred_style"],
            "inferred_facts": [
                {
                    "key": "likely_language",
                    "value": "python",
                    "rationale": "path hints suggest python sources",
                    "confidence": 0.72,
                }
            ],
            # Classifier tries to lower explicit impact — must not win
            "objective": "classifier objective must not clobber explicit",
        },
        explicit_facts={
            "impact": "high",
            "objective": "explicit objective wins",
            "side_effects": ["write"],
        },
    )
    spec = result.task_spec
    assert spec.explicit_facts["objective"] == "explicit objective wins"
    assert spec.objective == "explicit objective wins"
    assert spec.impact is ImpactLevel.HIGH
    assert "preferred_style" in spec.unknowns
    assert hasattr(spec, "inferred_facts")
    inferred = list(spec.inferred_facts)
    assert inferred, "inferred_facts must remain machine-separated"
    fact = inferred[0]
    value = getattr(fact, "value", None) or (fact.get("value") if isinstance(fact, dict) else None)
    rationale = getattr(fact, "rationale", None) or (
        fact.get("rationale") if isinstance(fact, dict) else None
    )
    confidence = getattr(fact, "confidence", None) or (
        fact.get("confidence") if isinstance(fact, dict) else None
    )
    assert value == "python"
    assert rationale and (
        "python" in rationale.lower() or "path" in rationale.lower()
    )
    assert isinstance(confidence, float) and 0.0 <= confidence <= 1.0
    # No secret/raw prompt duplication into inferred bag
    blob = str(inferred)
    assert "refactor helper module carefully" not in blob

    cfg = load_orchestration_config(
        {"orchestration": {"enabled": True, "mode": "shadow"}}
    )
    decision = route_task(spec, cfg)
    compiled = compile_worker_brief(spec, decision, cfg)
    brief = compiled.brief
    assert "explicit objective wins" in brief
    assert "preferred_style" in brief or "Unknowns" in brief
    # Inferred facts may appear with rationale, but not raw user prompt dump
    assert "classifier objective must not clobber explicit" not in brief
