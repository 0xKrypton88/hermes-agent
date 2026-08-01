from __future__ import annotations

from agent.adaptive_reasoning import (
    AUTO_MODEL,
    AUTO_PROVIDER,
    POLICY_VERSION,
    decide_adaptive_reasoning,
)


def _decide(prompt: str, **kwargs):
    return decide_adaptive_reasoning(prompt=prompt, config={"enabled": True}, **kwargs)


def test_auto_identity_is_fixed_and_micro_work_is_low() -> None:
    decision = _decide("Fix this typo in the README wording.")

    assert decision.provider == AUTO_PROVIDER == "openai-codex"
    assert decision.model == AUTO_MODEL == "gpt-5.6-sol"
    assert decision.effort == "low"
    assert decision.work_class == "micro"
    assert decision.reason_code == "micro_change"
    assert decision.policy_version == POLICY_VERSION


def test_normal_feature_is_medium() -> None:
    decision = _decide("Implement a new export button with focused tests.")

    assert decision.effort == "medium"
    assert decision.work_class == "normal"
    assert decision.reason_code == "normal_feature"


def test_ambiguous_request_defaults_to_medium() -> None:
    decision = _decide("Can you take a look at this?")

    assert decision.effort == "medium"
    assert decision.work_class == "ambiguous"
    assert decision.reason_code == "default_medium"


def test_unknown_root_cause_is_high() -> None:
    decision = _decide("The failure is intermittent and the root cause is unclear; diagnose it.")

    assert decision.effort == "high"
    assert decision.work_class == "unknown_root_cause"
    assert decision.reason_code == "unknown_root_cause"


def test_sensitive_mutations_are_high() -> None:
    cases = [
        ("Migrate the persisted session state to the new schema.", "migration"),
        ("Change OAuth authorization and credential refresh behavior.", "auth"),
        ("Patch the running production service configuration.", "live_mutation"),
        ("Modify live trading order placement logic.", "trading_mutation"),
    ]

    for prompt, work_class in cases:
        decision = _decide(prompt)
        assert decision.effort == "high", prompt
        assert decision.work_class == work_class, prompt


def test_swedish_coding_requests_follow_the_same_routing_contract() -> None:
    cases = [
        ("Fixa stavfelet i README.", "low"),
        ("Implementera en ny exportknapp med fokuserade tester.", "medium"),
        ("Felet är intermittent och rotorsaken är okänd; felsök det.", "high"),
        ("Ändra orderläggningen i live trading.", "high"),
        ("Migrera sessionstillståndet till det nya schemat.", "high"),
        ("Ändra OAuth-behörigheter och autentisering.", "high"),
        ("Patcha den körande produktionstjänsten.", "high"),
    ]

    for prompt, effort in cases:
        assert _decide(prompt).effort == effort, prompt


def test_project_name_and_prompt_length_alone_never_trigger_high() -> None:
    named = _decide("In QuantCore, explain the color palette.", cwd="D:/QuantCore/live-trading")
    long = _decide("Please summarize this ordinary note. " + ("background " * 2000))

    assert named.effort == "medium"
    assert long.effort == "medium"


def test_followups_escalate_only_within_low_high_bounds() -> None:
    held = _decide("Fix a typo.", history=[{"role": "user", "content": "Earlier"}], current_floor="medium")
    raised = _decide("The root cause is unknown; investigate.", current_floor="medium")
    capped = decide_adaptive_reasoning(
        prompt="Change authentication state migration.",
        current_floor="low",
        config={"enabled": True, "min_effort": "low", "max_effort": "medium", "followup_policy": "escalate_only"},
    )

    assert held.effort == "medium"
    assert held.reason_code == "followup_floor"
    assert raised.effort == "high"
    assert capped.effort == "medium"


def test_manual_override_is_explicit_and_not_auto_escalated() -> None:
    decision = decide_adaptive_reasoning(
        prompt="Migrate authentication state.",
        current_floor="high",
        manual_override="low",
        config={"enabled": True},
    )

    assert decision.effort == "low"
    assert decision.work_class == "manual"
    assert decision.reason_code == "manual_override"


def test_untrusted_config_cannot_change_auto_identity_or_effort_vocabulary() -> None:
    decision = decide_adaptive_reasoning(
        prompt="Migrate authentication state.",
        config={
            "enabled": True,
            "provider": "openrouter",
            "model": "spark",
            "default_effort": "xhigh",
            "min_effort": "none",
            "max_effort": "xhigh",
        },
    )

    assert decision.provider == "openai-codex"
    assert decision.model == "gpt-5.6-sol"
    assert decision.effort in {"low", "medium", "high"}


def test_upstream_config_is_disabled_and_narrowly_fixed() -> None:
    from hermes_cli.config import DEFAULT_CONFIG

    assert DEFAULT_CONFIG["agent"]["adaptive_reasoning"] == {
        "enabled": False,
        "provider": "openai-codex",
        "model": "gpt-5.6-sol",
        "default_effort": "medium",
        "min_effort": "low",
        "max_effort": "high",
        "followup_policy": "escalate_only",
    }
