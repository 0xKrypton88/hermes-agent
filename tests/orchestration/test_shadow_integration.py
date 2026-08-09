"""WP9 — universal seam integration: off / shadow / recursion guard."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from agent.orchestration.config import load_orchestration_config
from agent.orchestration.contracts import ModelFamily, RuleId


def _fake_agent(*, depth=0, platform="cli"):
    agent = SimpleNamespace(
        session_id="sess-int-1",
        model="parent-model",
        provider="openrouter",
        platform=platform,
        _delegate_depth=depth,
        _cached_system_prompt="BYTE_STABLE_SYSTEM_PROMPT_v1",
        tools=[{"type": "function", "function": {"name": "read_file"}}],
        valid_tool_names={"read_file"},
        enabled_toolsets=["file", "web"],
        disabled_toolsets=None,
        _session_db=None,
        _current_turn_id="turn-int",
        _last_orchestration_result=None,
        api_mode="chat_completions",
        max_iterations=1,
        quiet_mode=True,
    )
    return agent


def test_off_mode_is_exact_legacy_noop(tmp_path, monkeypatch):
    from agent.orchestration.service import maybe_orchestrate_turn
    from agent.orchestration.telemetry import list_traces

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    agent = _fake_agent()
    prompt_before = agent._cached_system_prompt
    tools_before = list(agent.tools)

    with patch("agent.orchestration.service.load_config", return_value={}):
        result = maybe_orchestrate_turn(agent, "hello world")

    assert result.mode == "off"
    assert result.acted is False
    assert result.legacy_continue is True
    assert result.task_spec is None
    assert result.decision is None
    assert result.worker_result is None
    assert agent._cached_system_prompt == prompt_before
    assert agent.tools == tools_before
    cfg = load_orchestration_config({})
    assert list_traces(cfg) == []


def test_shadow_preserves_legacy_and_records_decision_without_workers(
    tmp_path, monkeypatch
):
    from agent.orchestration.service import maybe_orchestrate_turn
    from agent.orchestration.telemetry import list_traces
    from agent.orchestration.executor import execute_worker_run

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    agent = _fake_agent()
    prompt_before = agent._cached_system_prompt
    tools_before = list(agent.tools)
    model_before = agent.model

    root = {
        "orchestration": {
            "enabled": True,
            "mode": "shadow",
            "telemetry": {"enabled": True, "retain_days": 14},
        }
    }

    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run", wraps=execute_worker_run
    ) as exec_mock:
        result = maybe_orchestrate_turn(
            agent, "Research and implement a small helper"
        )

    assert result.mode == "shadow"
    assert result.acted is False
    assert result.legacy_continue is True
    assert result.task_spec is not None
    assert result.decision is not None
    assert result.decision.family in (ModelFamily.TERRA, ModelFamily.LUNA, ModelFamily.SOL)
    assert result.worker_result is None
    exec_mock.assert_not_called()

    assert agent._cached_system_prompt == prompt_before
    assert agent.tools == tools_before
    assert agent.model == model_before
    assert RuleId.R_MODE_SHADOW.value in (result.trace.rule_ids if result.trace else ())

    cfg = load_orchestration_config(root)
    traces = list_traces(cfg)
    assert len(traces) >= 1


def test_worker_recursion_guard_skips_orchestration(tmp_path, monkeypatch):
    from agent.orchestration.service import maybe_orchestrate_turn

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    child = _fake_agent(depth=1, platform="subagent")
    root = {
        "orchestration": {
            "enabled": True,
            "mode": "shadow",
            "telemetry": {"enabled": True},
        }
    }
    with patch("agent.orchestration.service.load_config", return_value=root):
        result = maybe_orchestrate_turn(child, "nested work")

    assert result.mode == "shadow"
    assert result.acted is False
    assert result.legacy_continue is True
    assert result.task_spec is None
    assert result.decision is None
    assert RuleId.R_WORKER_RECURSION_GUARD.value in result.guard_reason_codes


def test_active_mode_uses_fakes_only_and_is_non_default(tmp_path, monkeypatch):
    from agent.orchestration.service import maybe_orchestrate_turn
    from agent.orchestration.executor import WorkerRunResult
    from agent.orchestration.contracts import ReasoningEffort

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()

    default = load_orchestration_config({})
    assert default.mode == "off"
    assert default.enabled is False

    agent = _fake_agent()
    prompt_before = agent._cached_system_prompt
    root = {
        "orchestration": {
            "enabled": True,
            "mode": "active",
            "telemetry": {"enabled": True},
        }
    }
    fake_result = WorkerRunResult(
        success=True,
        correlation_id="c",
        session_id=agent.session_id,
        task_id="t",
        worker_id="sa-1",
        child_session_id="child",
        provider="openrouter",
        model="resolved",
        reasoning=ReasoningEffort.MEDIUM,
        toolsets=("file", "web"),
        final_response="worker done",
        usage={"input_tokens": 1, "output_tokens": 1},
    )

    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run", return_value=fake_result
    ) as exec_mock:
        result = maybe_orchestrate_turn(agent, "Implement a feature with tests")

    assert result.mode == "active"
    assert result.acted is True
    assert result.legacy_continue is False
    assert result.response["final_response"] == "worker done"
    exec_mock.assert_called_once()
    assert agent._cached_system_prompt == prompt_before


def test_conversation_loop_calls_orchestrator_before_build_turn_context(
    tmp_path, monkeypatch
):
    """Universal seam: hook runs immediately before build_turn_context."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()

    order = []

    def fake_orch(agent, user_message, **kwargs):
        order.append("orch")
        from agent.orchestration.service import OrchestrationTurnResult

        return OrchestrationTurnResult(
            mode="off",
            acted=False,
            legacy_continue=True,
            task_spec=None,
            decision=None,
            compiled=None,
            trace=None,
            worker_result=None,
            response=None,
            guard_reason_codes=(),
        )

    def fake_btc(*args, **kwargs):
        order.append("build_turn_context")
        raise RuntimeError("stop-after-prologue")

    agent = MagicMock()
    agent.api_mode = "chat_completions"
    agent._delegate_depth = 0
    agent.platform = "cli"
    agent.session_id = "s"
    agent._try_refresh_env_client_credentials = MagicMock()
    agent._last_compaction_in_place = False
    agent.max_compression_attempts = 3
    agent._drain_pending_redirect = MagicMock(return_value=None)

    with patch(
        "agent.orchestration.service.maybe_orchestrate_turn", side_effect=fake_orch
    ), patch(
        "agent.conversation_loop.build_turn_context", side_effect=fake_btc
    ):
        from agent import conversation_loop

        with pytest.raises(RuntimeError, match="stop-after-prologue"):
            conversation_loop.run_conversation(agent, "hi there")

    assert order == ["orch", "build_turn_context"]


def test_active_mode_blocks_required_approval(tmp_path, monkeypatch):
    """Destructive/financial routing must not launch a worker before approval."""
    from agent.orchestration.service import maybe_orchestrate_turn
    from agent.orchestration.contracts import RuleId, VerificationOutcome
    from agent.orchestration.executor import execute_worker_run

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    agent = _fake_agent()
    root = {
        "orchestration": {
            "enabled": True,
            "mode": "active",
            "telemetry": {"enabled": True},
            "approval": {
                "require_for_destructive": True,
                "require_for_financial": True,
            },
        }
    }

    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run", wraps=execute_worker_run
    ) as exec_mock:
        result = maybe_orchestrate_turn(
            agent,
            "Please delete production backups and transfer payment",
            explicit_facts={
                "side_effects": ["destructive", "financial"],
                "impact": "critical",
                "complexity": "high",
            },
        )

    assert result.mode == "active"
    assert result.decision is not None
    assert result.decision.requires_approval is True
    exec_mock.assert_not_called()
    assert result.worker_result is None
    codes = set(result.guard_reason_codes or ())
    if result.trace and result.trace.rule_ids:
        codes.update(result.trace.rule_ids)
    assert RuleId.R_SIDE_EFFECT_APPROVAL.value in codes or (
        result.response
        and str(result.response.get("orchestration", {}).get("status", "")).upper()
        in {"REQUIRE_APPROVAL", "BLOCKED"}
    ) or (
        result.trace
        and result.trace.verification_outcome
        in {VerificationOutcome.REQUIRE_APPROVAL.value, "REQUIRE_APPROVAL"}
    )
    assert result.legacy_continue is False
    assert result.acted is True
    assert isinstance(result.response, dict)
    assert result.response.get("completed") is False
    assert str(result.response.get("status", "")).upper() == "REQUIRE_APPROVAL"


def test_active_mode_preserves_parent_turn_lifecycle_and_correlates_worker_usage(
    tmp_path, monkeypatch
):
    """Active workers must not skip build_turn_context / parent persistence."""
    from agent.orchestration.executor import WorkerRunResult
    from agent.orchestration.contracts import ReasoningEffort
    from agent.turn_context import TurnContext

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()

    order = []
    parent_prompt = "BYTE_STABLE_SYSTEM_PROMPT_v1"
    parent_tools = [{"type": "function", "function": {"name": "read_file"}}]
    fake_worker = WorkerRunResult(
        success=True,
        correlation_id="corr-life",
        session_id="sess-int-1",
        task_id="task-life",
        worker_id="sa-life",
        child_session_id="child-life",
        provider="openrouter",
        model="parent-model",
        reasoning=ReasoningEffort.MEDIUM,
        toolsets=("file", "web"),
        final_response="worker lifecycle ok",
        usage={"input_tokens": 9, "output_tokens": 4},
        latency_ms=42,
    )

    def fake_btc(agent, user_message, *args, **kwargs):
        order.append("build_turn_context")
        agent._current_turn_id = "turn-life-1"
        messages = [
            {"role": "system", "content": parent_prompt},
            {"role": "user", "content": str(user_message)},
        ]
        return TurnContext(
            user_message=str(user_message),
            original_user_message=str(user_message),
            messages=messages,
            conversation_history=[],
            active_system_prompt=parent_prompt,
            effective_task_id="task-life",
            turn_id="turn-life-1",
            current_turn_user_idx=1,
            should_review_memory=False,
            plugin_user_context="",
            ext_prefetch_cache="",
        )

    agent = MagicMock()
    agent.api_mode = "chat_completions"
    agent._delegate_depth = 0
    agent._orch_worker = False
    agent.platform = "cli"
    agent.session_id = "sess-int-1"
    agent.model = "parent-model"
    agent.provider = "openrouter"
    agent._cached_system_prompt = parent_prompt
    agent.tools = list(parent_tools)
    agent.valid_tool_names = {"read_file"}
    agent.enabled_toolsets = ["file", "web"]
    agent.disabled_toolsets = None
    agent._try_refresh_env_client_credentials = MagicMock()
    agent._last_compaction_in_place = False
    agent.max_compression_attempts = 3
    agent._drain_pending_redirect = MagicMock(return_value=None)
    agent.max_iterations = 1
    agent.quiet_mode = True
    agent._session_db = None
    agent._current_turn_id = None
    agent.iteration_budget = SimpleNamespace(remaining=10)

    root = {
        "orchestration": {
            "enabled": True,
            "mode": "active",
            "telemetry": {"enabled": True},
        }
    }

    real_orch = None

    def tracking_orch(agent_arg, user_message, **kwargs):
        order.append("orch")
        return real_orch(agent_arg, user_message, **kwargs)

    from agent.orchestration import service as orch_service

    real_orch = orch_service.maybe_orchestrate_turn

    with patch.object(
        orch_service, "load_config", return_value=root
    ), patch.object(
        orch_service, "execute_worker_run", return_value=fake_worker
    ), patch(
        "agent.orchestration.service.maybe_orchestrate_turn", side_effect=tracking_orch
    ), patch(
        "agent.conversation_loop.build_turn_context", side_effect=fake_btc
    ), patch(
        "agent.conversation_loop.finalize_turn",
        side_effect=lambda *a, **kw: {
            "final_response": kw.get("final_response") or fake_worker.final_response,
            "messages": list(kw.get("messages") or []),
            "completed": True,
            "api_calls": kw.get("api_call_count", 0),
            "orchestration": {
                "correlation_id": "corr-life",
                "worker_id": "sa-life",
                "mode": "active",
            },
        },
    ):
        from agent import conversation_loop

        result = conversation_loop.run_conversation(agent, "Implement a helper")

    assert order[:2] == ["orch", "build_turn_context"], order
    assert agent._cached_system_prompt == parent_prompt
    assert agent.tools == parent_tools
    assert agent.model == "parent-model"
    assert isinstance(result, dict)
    assert result.get("final_response") == "worker lifecycle ok"
    messages = result.get("messages") or []
    assert messages, "parent turn must persist/return non-empty messages"
    roles = [m.get("role") for m in messages]
    assert "system" in roles or roles[0] == "user"
    assert "user" in roles
    assert "assistant" in roles


def test_active_worker_installs_policy_context_and_cannot_mint_user_approval(
    tmp_path, monkeypatch
):
    """PolicyContext must be active inside workers; spoofed actor=user is rejected."""
    from agent.orchestration.service import maybe_orchestrate_turn
    from agent.orchestration.tool_policy import (
        get_active_policy_context,
        canonical_action_digest,
    )
    from agent.orchestration.executor import WorkerRunResult
    from agent.orchestration.contracts import ReasoningEffort

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    agent = _fake_agent()
    agent._current_turn_id = "turn-policy"
    seen = {"policy_in_worker": False, "spoof_blocked": False}

    def fake_exec(req, *, parent_agent, cfg, **kwargs):
        ctx = get_active_policy_context()
        seen["policy_in_worker"] = bool(ctx is not None and ctx.is_worker)
        if ctx is not None:
            try:
                ctx.approval_store.approve(
                    session_id=ctx.session_id,
                    turn_id=ctx.turn_id,
                    tool_call_id="tc-spoof",
                    tool_name="terminal",
                    action_digest=canonical_action_digest(
                        "terminal", {"cmd": "rm -rf /"}
                    ),
                    actor="user",
                )
            except PermissionError:
                seen["spoof_blocked"] = True
        return WorkerRunResult(
            success=True,
            correlation_id=req.correlation_id,
            session_id=agent.session_id,
            task_id=req.task_id,
            worker_id="sa-p",
            child_session_id="child-p",
            provider="openrouter",
            model="m",
            reasoning=ReasoningEffort.LOW,
            toolsets=tuple(req.toolsets),
            final_response="ok",
        )

    root = {
        "orchestration": {
            "enabled": True,
            "mode": "active",
            "telemetry": {"enabled": False},
        }
    }
    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run", side_effect=fake_exec
    ):
        result = maybe_orchestrate_turn(agent, "Summarize README")

    assert seen["policy_in_worker"] is True
    assert seen["spoof_blocked"] is True
    assert result.mode == "active"


def test_active_service_escalates_terra_failures_to_sol_and_records_trace(
    tmp_path, monkeypatch
):
    """Scenario E via service: repeated TERRA failure must reach SOL."""
    from agent.orchestration.service import maybe_orchestrate_turn
    from agent.orchestration.executor import WorkerRunResult
    from agent.orchestration.contracts import (
        ModelFamily,
        ReasoningEffort,
        VerificationOutcome,
    )
    from agent.orchestration.telemetry import list_traces, load_trace
    from agent.orchestration.config import load_orchestration_config

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    agent = _fake_agent()
    families_seen = []

    def failing_then_sol(req, *, parent_agent, cfg, **kwargs):
        families_seen.append(req.family)
        ok = req.family is ModelFamily.SOL
        return WorkerRunResult(
            success=ok,
            correlation_id=req.correlation_id,
            session_id=agent.session_id,
            task_id=req.task_id,
            worker_id=f"sa-{req.family.value}",
            child_session_id=f"child-{req.family.value}",
            provider="openrouter",
            model=f"model-{req.family.value}",
            reasoning=req.reasoning,
            toolsets=tuple(req.toolsets),
            final_response="sol ok" if ok else None,
            usage={"input_tokens": 2, "output_tokens": 1},
            error_class=None if ok else "tool_error",
            latency_ms=10,
        )

    root = {
        "orchestration": {
            "enabled": True,
            "mode": "active",
            "telemetry": {"enabled": True, "retain_days": 14},
            "budgets": {"max_attempts": 5, "max_cost_usd": 5.0, "max_duration_s": 600},
        }
    }
    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run", side_effect=failing_then_sol
    ):
        result = maybe_orchestrate_turn(
            agent,
            "Implement a multi-step feature with tests",
            explicit_facts={
                "complexity": "moderate",
                "impact": "moderate",
                "side_effects": ["write"],
                "capabilities": ["read", "write", "execute"],
            },
        )

    assert ModelFamily.TERRA in families_seen
    assert ModelFamily.SOL in families_seen
    assert families_seen.index(ModelFamily.SOL) > families_seen.index(ModelFamily.TERRA)
    assert result.worker_result is not None
    assert result.worker_result.success is True
    assert result.trace is not None
    assert result.trace.verification_outcome in {
        "RETURN",
        VerificationOutcome.RETURN.value,
    }
    # Final correlated trace must include non-zero usage / worker ids
    assert result.trace.worker_id
    assert (result.trace.input_tokens or 0) + (result.trace.output_tokens or 0) > 0
    assert result.trace.concrete_model
    cfg = load_orchestration_config(root)
    traces = list_traces(cfg)
    assert traces
    final = load_trace(max(traces, key=lambda p: p.stat().st_mtime))
    assert final.get("worker_id")
    assert (final.get("input_tokens") or 0) + (final.get("output_tokens") or 0) > 0


@pytest.mark.parametrize(
    "worker_output",
    [
        {"summary": "Human-ready answer", "evidence": ["tests passed"], "status": "ok"},
        '{"summary":"Human-ready answer","evidence":["tests passed"],"status":"ok"}',
    ],
)
def test_active_mode_normalizes_valid_worker_envelope_to_summary(
    tmp_path, monkeypatch, worker_output
):
    from agent.orchestration.service import maybe_orchestrate_turn
    from agent.orchestration.executor import WorkerRunResult
    from agent.orchestration.contracts import ReasoningEffort

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    agent = _fake_agent()
    fake_result = WorkerRunResult(
        success=True,
        correlation_id="c-envelope",
        session_id=agent.session_id,
        task_id="t-envelope",
        worker_id="sa-envelope",
        child_session_id="child-envelope",
        provider="openrouter",
        model="resolved",
        reasoning=ReasoningEffort.LOW,
        toolsets=("file",),
        final_response=worker_output,
    )
    root = {"orchestration": {"enabled": True, "mode": "active"}}

    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run", return_value=fake_result
    ):
        result = maybe_orchestrate_turn(agent, "Summarize this note")

    assert result.response["final_response"] == "Human-ready answer"


@pytest.mark.parametrize(
    "worker_output",
    [
        "ordinary worker prose",
        '{"unrelated":"json"}',
        '{"summary":"missing required envelope fields"}',
    ],
)
def test_active_mode_leaves_prose_and_invalid_json_unchanged(
    tmp_path, monkeypatch, worker_output
):
    from agent.orchestration.service import maybe_orchestrate_turn
    from agent.orchestration.executor import WorkerRunResult
    from agent.orchestration.contracts import ReasoningEffort

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    agent = _fake_agent()
    fake_result = WorkerRunResult(
        success=True,
        correlation_id="c-plain",
        session_id=agent.session_id,
        task_id="t-plain",
        worker_id="sa-plain",
        child_session_id="child-plain",
        provider="openrouter",
        model="resolved",
        reasoning=ReasoningEffort.LOW,
        toolsets=("file",),
        final_response=worker_output,
    )

    with patch(
        "agent.orchestration.service.load_config",
        return_value={"orchestration": {"enabled": True, "mode": "active"}},
    ), patch("agent.orchestration.service.execute_worker_run", return_value=fake_result):
        result = maybe_orchestrate_turn(agent, "Summarize this note")

    assert result.response["final_response"] == worker_output
