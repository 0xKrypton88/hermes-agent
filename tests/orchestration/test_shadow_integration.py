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
