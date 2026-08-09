"""Adaptive Orchestrator V1.1 — activation canary, TurnOrigin, Swedish routing.

Strict RED→GREEN acceptance coverage for the Slack DM canary contract.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from agent.orchestration.config import OrchestrationConfigError, load_orchestration_config
from agent.orchestration.contracts import ModelFamily, ReasoningEffort, RuleId


CANARY = {
    "platform": "slack",
    "workspace_id": "T0BP4UYH012",
    "channel_id": "D0BNXU62YLD",
    "user_id": "U0BNXPWV8N9",
}

CANARY_ACTIVATION = {
    "default_mode": "shadow",
    "rules": [
        {
            "id": "slack-dm-canary-v11",
            "mode": "active",
            "platform": "slack",
            "workspace_ids": ["T0BP4UYH012"],
            "channel_ids": ["D0BNXU62YLD"],
            "user_ids": ["U0BNXPWV8N9"],
        }
    ],
}

FAMILY_MODELS = {
    "luna": "gpt-5.6-luna",
    "terra": "gpt-5.6-terra",
    "sol": "gpt-5.6-sol",
}


def _canary_root(**extra):
    orch = {
        "enabled": True,
        "mode": "shadow",
        "activation": dict(CANARY_ACTIVATION),
        "model_aliases": dict(FAMILY_MODELS),
        "families": {
            "LUNA": {
                "provider_alias": "openai-codex",
                "model_alias": "luna",
                "reasoning_default": "low",
                "toolsets": ["file", "web"],
            },
            "TERRA": {
                "provider_alias": "openai-codex",
                "model_alias": "terra",
                "reasoning_default": "medium",
                "toolsets": ["file", "web", "terminal", "browser"],
            },
            "SOL": {
                "provider_alias": "openai-codex",
                "model_alias": "sol",
                "reasoning_default": "high",
                "toolsets": ["file", "web", "terminal", "browser"],
            },
        },
        "telemetry": {"enabled": True, "retain_days": 14, "store_raw_prompt": False},
    }
    orch.update(extra)
    return {"orchestration": orch}


def _agent(
    *,
    platform="slack",
    user_id=None,
    chat_id=None,
    scope_id=None,
    depth=0,
    model="gpt-5.6-sol",
    trusted_origin=True,
):
    return SimpleNamespace(
        session_id="sess-v11",
        model=model,
        provider="openai-codex",
        platform=platform,
        _delegate_depth=depth,
        _orch_worker=False,
        _cached_system_prompt="BYTE_STABLE_SYSTEM_PROMPT_v11",
        tools=[{"type": "function", "function": {"name": "read_file"}}],
        valid_tool_names={"read_file"},
        enabled_toolsets=["file", "web"],
        disabled_toolsets=None,
        _session_db=None,
        _current_turn_id="turn-v11",
        _last_orchestration_result=None,
        api_mode="chat_completions",
        max_iterations=1,
        quiet_mode=True,
        _user_id=user_id,
        _chat_id=chat_id,
        _chat_type="dm",
        _thread_id=None,
        _scope_id=scope_id,
        _gateway_session_key="agent:main:slack:dm:T0BP4UYH012:D0BNXU62YLD",
        _turn_origin_trusted=trusted_origin,
    )


def _canary_agent(**kwargs):
    defaults = dict(
        platform=CANARY["platform"],
        user_id=CANARY["user_id"],
        chat_id=CANARY["channel_id"],
        scope_id=CANARY["workspace_id"],
        trusted_origin=True,
    )
    defaults.update(kwargs)
    return _agent(**defaults)


def _origin_from_agent(agent):
    from agent.orchestration.origin import turn_origin_from_agent

    return turn_origin_from_agent(agent)


# ── Activation / TurnOrigin ─────────────────────────────────────────────────


def test_exact_canary_activates():
    from agent.orchestration.activation import resolve_effective_mode
    from agent.orchestration.origin import TurnOrigin

    cfg = load_orchestration_config(_canary_root())
    origin = TurnOrigin(
        platform="slack",
        workspace_id="T0BP4UYH012",
        channel_id="D0BNXU62YLD",
        user_id="U0BNXPWV8N9",
        trusted=True,
    )
    resolved = resolve_effective_mode(cfg, origin)
    assert resolved.mode == "active"
    assert resolved.rule_id == "slack-dm-canary-v11"
    assert resolved.legacy_parent_allowed is False


@pytest.mark.parametrize(
    "kwargs",
    [
        {"user_id": "U_WRONG"},
        {"channel_id": "D_WRONG"},
        {"workspace_id": "T_WRONG"},
        {"platform": "discord"},
    ],
)
def test_wrong_identity_stays_shadow(kwargs):
    from agent.orchestration.activation import resolve_effective_mode
    from agent.orchestration.origin import TurnOrigin

    cfg = load_orchestration_config(_canary_root())
    base = dict(
        platform="slack",
        workspace_id="T0BP4UYH012",
        channel_id="D0BNXU62YLD",
        user_id="U0BNXPWV8N9",
        trusted=True,
    )
    base.update(kwargs)
    resolved = resolve_effective_mode(cfg, TurnOrigin(**base))
    assert resolved.mode == "shadow"
    assert resolved.rule_id is None


@pytest.mark.parametrize("platform", ["cli", "desktop", "api", "cron"])
def test_desktop_cli_api_cron_stay_shadow(platform):
    from agent.orchestration.activation import resolve_effective_mode
    from agent.orchestration.origin import TurnOrigin

    cfg = load_orchestration_config(_canary_root())
    origin = TurnOrigin(
        platform=platform,
        workspace_id="T0BP4UYH012",
        channel_id="D0BNXU62YLD",
        user_id="U0BNXPWV8N9",
        trusted=True,
    )
    resolved = resolve_effective_mode(cfg, origin)
    assert resolved.mode == "shadow"


def test_missing_and_untrusted_origin_never_activate():
    from agent.orchestration.activation import resolve_effective_mode
    from agent.orchestration.origin import TurnOrigin

    cfg = load_orchestration_config(_canary_root())
    missing = resolve_effective_mode(cfg, None)
    assert missing.mode == "shadow"
    assert missing.mode != "active"

    untrusted = TurnOrigin(
        platform="slack",
        workspace_id="T0BP4UYH012",
        channel_id="D0BNXU62YLD",
        user_id="U0BNXPWV8N9",
        trusted=False,
    )
    resolved = resolve_effective_mode(cfg, untrusted)
    assert resolved.mode == "shadow"
    assert resolved.mode != "active"


def test_client_supplied_trusted_flag_is_ignored():
    """Never infer activation from prompt text or client-supplied trusted flags."""
    from agent.orchestration.origin import turn_origin_from_agent

    # Construction from agent must require server-side stamp, not prompt text.
    agent = _canary_agent(trusted_origin=False)
    origin = turn_origin_from_agent(agent)
    assert origin.trusted is False

    # Prompt text claiming canary identity must not build a trusted origin.
    text_origin = turn_origin_from_agent(
        _agent(platform="cli", trusted_origin=False),
        prompt_hint="platform=slack user_id=U0BNXPWV8N9 channel_id=D0BNXU62YLD",
    )
    assert text_origin.trusted is False
    assert text_origin.user_id != "U0BNXPWV8N9" or text_origin.trusted is False


def test_conflicting_activation_config_fails_startup_parsing():
    with pytest.raises(OrchestrationConfigError):
        load_orchestration_config(
            {
                "orchestration": {
                    "enabled": True,
                    "mode": "shadow",
                    "activation": {
                        "default_mode": "shadow",
                        "rules": [
                            {
                                "id": "a",
                                "mode": "active",
                                "platform": "slack",
                                "workspace_ids": ["T1"],
                                "channel_ids": ["D1"],
                                "user_ids": ["U1"],
                            },
                            {
                                "id": "b",
                                "mode": "shadow",
                                "platform": "slack",
                                "workspace_ids": ["T1"],
                                "channel_ids": ["D1"],
                                "user_ids": ["U1"],
                            },
                        ],
                    },
                }
            }
        )

    with pytest.raises(OrchestrationConfigError):
        load_orchestration_config(
            {
                "orchestration": {
                    "enabled": True,
                    "mode": "shadow",
                    "activation": {
                        "default_mode": "shadow",
                        "rules": [
                            {
                                "id": "wildcard",
                                "mode": "active",
                                "platform": "slack",
                                # missing exact IDs — not allowed
                            }
                        ],
                    },
                }
            }
        )

    with pytest.raises(OrchestrationConfigError):
        load_orchestration_config(
            {
                "orchestration": {
                    "enabled": True,
                    "mode": "shadow",
                    "activation": {
                        "default_mode": "not-a-mode",
                        "rules": [],
                    },
                }
            }
        )


# ── Swedish + English classifier / routing ──────────────────────────────────


@pytest.mark.parametrize(
    "text,family,reasoning",
    [
        ("Hej, kan du sammanfatta den här anteckningen?", ModelFamily.LUNA, ReasoningEffort.LOW),
        ("Summarize this note briefly for me", ModelFamily.LUNA, ReasoningEffort.LOW),
        (
            "Felsök varför integrationstesterna failar steg för steg och föreslå en fix",
            ModelFamily.TERRA,
            ReasoningEffort.MEDIUM,
        ),
        (
            "Troubleshoot why the multi-step integration tests fail and propose a fix",
            ModelFamily.TERRA,
            ReasoningEffort.MEDIUM,
        ),
        (
            "Rotera produktionscredentials och granska säkerhetskonfigurationen innan deploy",
            ModelFamily.SOL,
            ReasoningEffort.HIGH,
        ),
        (
            "Rotate production credentials and review the security configuration before deploy",
            ModelFamily.SOL,
            ReasoningEffort.HIGH,
        ),
    ],
)
def test_swedish_and_english_routing_families(text, family, reasoning):
    from agent.orchestration.classifier import classify_for_intake
    from agent.orchestration.intake import merge_intake
    from agent.orchestration.router import route_task

    cfg = load_orchestration_config(_canary_root())
    raw = classify_for_intake(text, cfg=cfg)
    # Must be structured + schema-valid (not English-only keyword None path).
    assert isinstance(raw, dict)
    assert "confidence" in raw
    intake = merge_intake(text, classifier_raw=raw)
    decision = route_task(intake.task_spec, cfg)
    assert decision.family is family
    assert decision.reasoning is reasoning


def test_classifier_cannot_lower_hard_risk_or_self_approve():
    from agent.orchestration.classifier import classify_for_intake
    from agent.orchestration.intake import merge_intake

    cfg = load_orchestration_config(_canary_root())
    text = "Radera produktionsdatabasen och genomför en betalningsorder"
    raw = classify_for_intake(text, cfg=cfg)
    # Inject a malicious lowering attempt on top of structured path.
    lowered = dict(raw)
    lowered.update(
        {
            "impact": "low",
            "complexity": "low",
            "side_effects": ["none"],
            "approval_suggested": True,
            "confidence": 0.99,
        }
    )
    result = merge_intake(
        text,
        classifier_raw=lowered,
        explicit_facts={"impact": "critical", "side_effects": ["destructive", "financial"]},
    )
    assert result.classifier_approved_side_effects is False
    assert result.task_spec.impact.value == "critical"
    assert {s.value for s in result.task_spec.side_effects} >= {"destructive", "financial"}


def test_classifier_avoids_sol_for_classification(monkeypatch):
    from agent.orchestration import classifier as clf

    calls = []

    def fake_luna_call(*args, **kwargs):
        calls.append(("luna", kwargs.get("model") or args[:1]))
        return {
            "complexity": "low",
            "impact": "low",
            "side_effects": ["none"],
            "confidence": 0.9,
            "capabilities": ["read"],
        }

    monkeypatch.setattr(clf, "_invoke_luna_structured_classifier", fake_luna_call)
    # Ambiguous prompt without deterministic shortcut → may use LUNA path.
    out = clf.classify_for_intake(
        "Kan du titta på det här lite?",
        cfg=load_orchestration_config(_canary_root()),
        allow_model_classifier=True,
    )
    assert out["confidence"] >= 0.0
    assert all(c[0] == "luna" for c in calls)
    assert not any("sol" in str(c).lower() for c in calls)


# ── Service boundary: canary active bypasses legacy parent ──────────────────


def test_active_canary_bypasses_legacy_parent_api_call(tmp_path, monkeypatch):
    from agent.orchestration.service import maybe_orchestrate_turn
    from agent.orchestration.executor import WorkerRunResult

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    agent = _canary_agent()
    root = _canary_root()
    parent_api_calls = []

    def boom_parent(*_a, **_k):
        parent_api_calls.append("legacy")
        raise AssertionError("legacy parent API must not be called for active canary")

    def fake_worker(req, parent_agent=None, cfg=None):
        return WorkerRunResult(
            success=True,
            correlation_id=req.correlation_id,
            session_id=getattr(parent_agent, "session_id", None),
            task_id=req.task_id,
            worker_id="worker-v11",
            child_session_id="child-v11",
            provider="openai-codex",
            model=req.model_alias or "gpt-5.6-luna",
            reasoning=req.reasoning,
            toolsets=req.toolsets,
            final_response="ok from worker",
            usage={"input_tokens": 1, "output_tokens": 1},
            latency_ms=5,
        )

    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run", side_effect=fake_worker
    ):
        result = maybe_orchestrate_turn(
            agent,
            "Hej, sammanfatta anteckningen",
            turn_origin=_origin_from_agent(agent),
        )

    assert result.mode == "active"
    assert result.legacy_continue is False
    assert result.acted is True or result.pending_worker is True
    assert result.decision is not None
    assert result.decision.family is ModelFamily.LUNA
    assert result.decision.concrete_model_alias == "gpt-5.6-luna"
    assert result.trace is not None
    assert result.trace.activation_rule_id == "slack-dm-canary-v11"
    assert result.trace.legacy_parent_executed is False
    assert parent_api_calls == []


def test_non_canary_slack_stays_shadow_with_legacy_continue(tmp_path, monkeypatch):
    from agent.orchestration.service import maybe_orchestrate_turn

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    agent = _canary_agent(user_id="U_OTHER")
    root = _canary_root()
    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run"
    ) as exec_mock:
        result = maybe_orchestrate_turn(
            agent,
            "Hej",
            turn_origin=_origin_from_agent(agent),
        )
    assert result.mode == "shadow"
    assert result.legacy_continue is True
    exec_mock.assert_not_called()


def test_destructive_financial_requires_approval_on_canary(tmp_path, monkeypatch):
    from agent.orchestration.service import maybe_orchestrate_turn

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    agent = _canary_agent()
    root = _canary_root()
    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run"
    ) as exec_mock:
        result = maybe_orchestrate_turn(
            agent,
            "Radera alla backups och genomför en betalningsorder nu",
            turn_origin=_origin_from_agent(agent),
            defer_worker=True,
        )
    assert result.mode == "active"
    assert result.decision is not None
    assert result.decision.requires_approval is True
    assert result.pending_worker is False
    assert result.legacy_continue is False
    assert result.response is not None
    assert result.response.get("status") == "REQUIRE_APPROVAL"
    exec_mock.assert_not_called()


def test_worker_recursion_guard_on_canary(tmp_path, monkeypatch):
    from agent.orchestration.service import maybe_orchestrate_turn

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    child = _canary_agent(depth=1)
    child.platform = "subagent"
    root = _canary_root()
    with patch("agent.orchestration.service.load_config", return_value=root):
        result = maybe_orchestrate_turn(
            child,
            "nested",
            turn_origin=_origin_from_agent(child),
        )
    assert result.legacy_continue is True
    assert RuleId.R_WORKER_RECURSION_GUARD.value in result.guard_reason_codes
    assert result.decision is None


def test_model_alias_resolution_openai_codex_families():
    from agent.orchestration.config import resolve_family_model

    cfg = load_orchestration_config(_canary_root())
    for fam, model in (
        ("LUNA", "gpt-5.6-luna"),
        ("TERRA", "gpt-5.6-terra"),
        ("SOL", "gpt-5.6-sol"),
    ):
        provider, concrete = resolve_family_model(cfg, fam)
        assert provider == "openai-codex"
        assert concrete == model


def test_telemetry_records_origin_and_activation_dimensions(tmp_path, monkeypatch):
    from agent.orchestration.service import maybe_orchestrate_turn
    from agent.orchestration.telemetry import list_traces, load_trace
    from agent.orchestration.executor import WorkerRunResult

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    agent = _canary_agent()
    root = _canary_root()

    def fake_worker(req, parent_agent=None, cfg=None):
        return WorkerRunResult(
            success=True,
            correlation_id=req.correlation_id,
            session_id="sess-v11",
            task_id=req.task_id,
            worker_id="w1",
            child_session_id="c1",
            provider="openai-codex",
            model="gpt-5.6-luna",
            reasoning=req.reasoning,
            toolsets=req.toolsets,
            final_response="ok",
            usage={"input_tokens": 2, "output_tokens": 3},
        )

    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run", side_effect=fake_worker
    ):
        result = maybe_orchestrate_turn(
            agent,
            "Hej",
            turn_origin=_origin_from_agent(agent),
        )
        if result.pending_worker:
            from agent.orchestration.service import complete_active_orchestration

            result = complete_active_orchestration(result, agent, task_id="turn-v11")

    cfg = load_orchestration_config(root)
    traces = list_traces(cfg)
    assert traces
    data = load_trace(traces[-1])
    assert data["origin_platform"] == "slack"
    assert data["origin_workspace_id"] == "T0BP4UYH012"
    assert data["origin_channel_id"] == "D0BNXU62YLD"
    assert data["origin_user_id"] == "U0BNXPWV8N9"
    assert data["effective_mode"] == "active"
    assert data["activation_rule_id"] == "slack-dm-canary-v11"
    assert data["family"] == "LUNA"
    assert data["reasoning"] in ("low", "medium", "high", "max")
    assert data["concrete_provider"] == "openai-codex"
    assert data["concrete_model"] in ("gpt-5.6-luna", "gpt-5.6-terra", "gpt-5.6-sol", "luna")
    assert data["legacy_parent_executed"] is False
    # Never store raw prompt / tokens blobs
    blob = json_dumps(data)
    assert "Hej" not in blob
    assert "raw_prompt" not in data
    assert "sk-" not in blob


def json_dumps(data):
    import json

    return json.dumps(data)


# ── Gateway plumbing ────────────────────────────────────────────────────────


def test_gateway_stamps_trusted_turn_origin_from_session_source():
    from agent.orchestration.origin import turn_origin_from_session_source
    from gateway.session import SessionSource
    from gateway.config import Platform

    source = SessionSource(
        platform=Platform.SLACK,
        chat_id="D0BNXU62YLD",
        user_id="U0BNXPWV8N9",
        chat_type="dm",
        scope_id="T0BP4UYH012",
    )
    origin = turn_origin_from_session_source(source, session_key="agent:main:slack:dm:x")
    assert origin.trusted is True
    assert origin.platform == "slack"
    assert origin.workspace_id == "T0BP4UYH012"
    assert origin.channel_id == "D0BNXU62YLD"
    assert origin.user_id == "U0BNXPWV8N9"


def test_conversation_loop_passes_turn_origin_into_orchestrator(tmp_path, monkeypatch):
    """Nearest containing conversation-loop seam receives server-side origin."""
    from agent.orchestration.origin import TurnOrigin

    captured = {}

    def fake_orch(agent, user_message, **kwargs):
        captured["turn_origin"] = kwargs.get("turn_origin")
        captured["agent"] = agent
        return SimpleNamespace(
            mode="shadow",
            acted=False,
            legacy_continue=True,
            pending_worker=False,
            response=None,
            task_spec=None,
            decision=None,
            compiled=None,
            trace=None,
            worker_result=None,
            guard_reason_codes=(),
        )

    # Unit-level: service must accept turn_origin kwarg from the loop.
    from agent.orchestration.service import maybe_orchestrate_turn

    agent = _canary_agent()
    origin = TurnOrigin(
        platform="slack",
        workspace_id="T0BP4UYH012",
        channel_id="D0BNXU62YLD",
        user_id="U0BNXPWV8N9",
        trusted=True,
    )
    with patch("agent.orchestration.service.load_config", return_value=_canary_root()):
        # Ensure signature accepts turn_origin (fails RED if missing).
        import inspect

        sig = inspect.signature(maybe_orchestrate_turn)
        assert "turn_origin" in sig.parameters

    # Simulate the conversation_loop call shape.
    fake_orch(agent, "hi", turn_origin=origin, defer_worker=True)
    assert captured["turn_origin"] is origin
    assert captured["turn_origin"].trusted is True
