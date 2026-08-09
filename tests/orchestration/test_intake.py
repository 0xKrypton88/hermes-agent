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
