"""Second-repair RED→GREEN coverage for Adaptive Orchestrator V1 safety/lifecycle.

These tests exercise real conversation-loop / service / executor / policy seams.
Legacy model/tool paths are set to fail if incorrectly reached.
"""

from __future__ import annotations

import json
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from agent.orchestration.config import load_orchestration_config
from agent.orchestration.contracts import (
    CapabilityClass,
    ModelFamily,
    ReasoningEffort,
    SideEffectClass,
    VerificationOutcome,
)
from agent.turn_context import TurnContext


def _active_root(**extra):
    root = {
        "orchestration": {
            "enabled": True,
            "mode": "active",
            "telemetry": {"enabled": True, "retain_days": 14},
            "approval": {
                "require_for_destructive": True,
                "require_for_financial": True,
            },
            "budgets": {"max_attempts": 4, "max_cost_usd": 5.0, "max_duration_s": 600},
        }
    }
    root["orchestration"].update(extra)
    return root


def _loop_agent():
    agent = MagicMock()
    agent.api_mode = "chat_completions"
    agent._delegate_depth = 0
    agent._orch_worker = False
    agent.platform = "cli"
    agent.session_id = "sess-repair-1"
    agent.model = "parent-model"
    agent.provider = "openrouter"
    agent._cached_system_prompt = "BYTE_STABLE_SYSTEM_PROMPT_v1"
    agent.tools = [{"type": "function", "function": {"name": "read_file"}}]
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
    agent._active_children = []
    return agent


def _prologue_btc(agent, user_message, *args, **kwargs):
    agent._current_turn_id = "turn-repair-1"
    messages = [
        {"role": "system", "content": "BYTE_STABLE_SYSTEM_PROMPT_v1"},
        {"role": "user", "content": str(user_message)},
    ]
    return TurnContext(
        user_message=str(user_message),
        original_user_message=str(user_message),
        messages=messages,
        conversation_history=[],
        active_system_prompt="BYTE_STABLE_SYSTEM_PROMPT_v1",
        effective_task_id="task-repair",
        turn_id="turn-repair-1",
        current_turn_user_idx=1,
        should_review_memory=False,
        plugin_user_context="",
        ext_prefetch_cache="",
    )


def test_conversation_loop_require_approval_prologue_finalize_no_worker_or_legacy(
    tmp_path, monkeypatch
):
    """REQUIRE_APPROVAL does prologue/finalize but never worker or legacy model."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()

    order = []
    agent = _loop_agent()

    def tracking_btc(*a, **kw):
        order.append("prologue")
        return _prologue_btc(*a, **kw)

    def boom_worker(*a, **kw):
        order.append("worker")
        raise AssertionError("worker must not launch for REQUIRE_APPROVAL")

    def boom_legacy(*a, **kw):
        order.append("legacy_model")
        raise AssertionError("legacy model must not run for REQUIRE_APPROVAL")

    finalized = {}

    def fake_finalize(agent_arg, **kw):
        order.append("finalize")
        finalized.update(kw)
        return {
            "final_response": kw.get("final_response"),
            "messages": list(kw.get("messages") or []),
            "completed": False,
            "status": "REQUIRE_APPROVAL",
            "api_calls": kw.get("api_call_count", 0),
        }

    root = _active_root()
    # Poison the API client path if the loop incorrectly reaches legacy.
    agent.client = MagicMock()
    agent.client.chat = MagicMock()
    agent.client.chat.completions = MagicMock()
    agent.client.chat.completions.create = MagicMock(side_effect=boom_legacy)
    agent._api_call = MagicMock(side_effect=boom_legacy)

    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run", side_effect=boom_worker
    ), patch(
        "agent.conversation_loop.build_turn_context", side_effect=tracking_btc
    ), patch(
        "agent.conversation_loop.finalize_turn", side_effect=fake_finalize
    ):
        from agent import conversation_loop
        from agent.orchestration import service as orch_service

        with patch.object(orch_service, "load_config", return_value=root):
            result = conversation_loop.run_conversation(
                agent,
                "Please delete production backups and transfer payment",
            )

    assert "prologue" in order
    assert "finalize" in order
    assert "worker" not in order
    assert "legacy_model" not in order
    assert isinstance(result, dict)
    status = str(
        result.get("status")
        or (result.get("orchestration") or {}).get("status")
        or ""
    ).upper()
    assert "REQUIRE_APPROVAL" in status or "APPROVAL" in (
        str(result.get("final_response") or "").upper()
    )
    assert finalized.get("api_call_count", 0) == 0
    assert agent._cached_system_prompt == "BYTE_STABLE_SYSTEM_PROMPT_v1"


def test_blocked_ask_user_active_decision_never_falls_through_to_legacy(
    tmp_path, monkeypatch
):
    """blocked/ASK_USER active decision never falls through to legacy."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()

    order = []
    agent = _loop_agent()

    def tracking_btc(*a, **kw):
        order.append("prologue")
        return _prologue_btc(*a, **kw)

    def boom_worker(*a, **kw):
        order.append("worker")
        raise AssertionError("worker must not launch for ASK_USER/blocked")

    def boom_legacy(*a, **kw):
        order.append("legacy_model")
        raise AssertionError("legacy model must not run for ASK_USER/blocked")

    def fake_finalize(agent_arg, **kw):
        order.append("finalize")
        return {
            "final_response": kw.get("final_response"),
            "messages": list(kw.get("messages") or []),
            "completed": False,
            "status": "ASK_USER",
            "api_calls": 0,
        }

    root = _active_root()
    from agent.orchestration.service import maybe_orchestrate_turn
    from agent.orchestration import service as orch_service

    real_orch = orch_service.maybe_orchestrate_turn

    def orch_with_blockers(agent_arg, user_message, **kwargs):
        kwargs["explicit_facts"] = {"blocker_unknowns": ["which_environment"]}
        return real_orch(agent_arg, user_message, **kwargs)

    with patch.object(orch_service, "load_config", return_value=root), patch(
        "agent.orchestration.service.load_config", return_value=root
    ), patch(
        "agent.orchestration.service.execute_worker_run", side_effect=boom_worker
    ), patch(
        "agent.orchestration.service.maybe_orchestrate_turn",
        side_effect=orch_with_blockers,
    ), patch(
        "agent.conversation_loop.build_turn_context", side_effect=tracking_btc
    ), patch(
        "agent.conversation_loop.finalize_turn", side_effect=fake_finalize
    ):
        agent.client = MagicMock()
        agent.client.chat = MagicMock()
        agent.client.chat.completions = MagicMock()
        agent.client.chat.completions.create = MagicMock(side_effect=boom_legacy)
        agent._api_call = MagicMock(side_effect=boom_legacy)

        from agent import conversation_loop

        result = conversation_loop.run_conversation(agent, "ambiguous request")

    assert "prologue" in order
    assert "finalize" in order
    assert "worker" not in order
    assert "legacy_model" not in order
    assert isinstance(result, dict)
    assert str(result.get("status", "")).upper() in {"ASK_USER", "BLOCKED", "BLOCK"}

    agent2 = SimpleNamespace(
        session_id="s2",
        model="m",
        provider="p",
        platform="cli",
        _delegate_depth=0,
        _orch_worker=False,
        _cached_system_prompt="SYS",
        tools=[],
        _session_db=None,
        _current_turn_id="t2",
        _last_orchestration_result=None,
    )
    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run", side_effect=boom_worker
    ):
        decision = maybe_orchestrate_turn(
            agent2,
            "ambiguous request",
            explicit_facts={"blocker_unknowns": ["which_environment"]},
            defer_worker=True,
        )

    assert decision.legacy_continue is False
    assert decision.pending_worker is False
    assert isinstance(decision.response, dict)
    assert decision.acted is True
    status = str(
        decision.response.get("status")
        or decision.response.get("orchestration", {}).get("status")
        or ""
    ).upper()
    assert status in {"ASK_USER", "BLOCKED", "BLOCK"}


def test_active_completion_exception_fails_closed_no_legacy_model(
    tmp_path, monkeypatch
):
    """active completion exception fails closed, no legacy model."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()

    order = []
    agent = _loop_agent()

    def tracking_btc(*a, **kw):
        order.append("prologue")
        return _prologue_btc(*a, **kw)

    def boom_complete(*a, **kw):
        order.append("complete")
        raise RuntimeError("worker/service/policy exploded")

    def boom_legacy(*a, **kw):
        order.append("legacy_model")
        raise AssertionError("legacy model must not run after active failure")

    def fake_finalize(agent_arg, **kw):
        order.append("finalize")
        return {
            "final_response": kw.get("final_response"),
            "messages": list(kw.get("messages") or []),
            "completed": False,
            "failed": True,
            "status": "BLOCKED",
            "api_calls": 0,
        }

    root = _active_root()
    from agent.orchestration import service as orch_service

    with patch.object(orch_service, "load_config", return_value=root), patch(
        "agent.conversation_loop.build_turn_context", side_effect=tracking_btc
    ), patch(
        "agent.orchestration.service.complete_active_orchestration",
        side_effect=boom_complete,
    ), patch(
        "agent.conversation_loop.finalize_turn", side_effect=fake_finalize
    ):
        agent.client = MagicMock()
        agent.client.chat = MagicMock()
        agent.client.chat.completions = MagicMock()
        agent.client.chat.completions.create = MagicMock(side_effect=boom_legacy)
        agent._api_call = MagicMock(side_effect=boom_legacy)

        from agent import conversation_loop

        result = conversation_loop.run_conversation(
            agent, "Implement a helper with tests"
        )

    assert "prologue" in order
    assert "complete" in order
    assert "finalize" in order
    assert "legacy_model" not in order
    assert isinstance(result, dict)
    assert result.get("completed") is False or result.get("failed") is True


def test_read_only_worker_denies_write_while_write_capable_can_write(
    tmp_path, monkeypatch
):
    """read-only worker denies WRITE while write-capable task can WRITE."""
    from tools.registry import ToolRegistry
    from agent.orchestration.tool_policy import (
        ApprovalStore,
        PolicyContext,
        ToolRiskMeta,
        enforce_tool_policy,
        allowed_side_effects_for_task,
    )
    from agent.orchestration.contracts import TaskSpec, Provenance, AutonomyBoundary
    from agent.orchestration.service import maybe_orchestrate_turn
    from agent.orchestration.executor import WorkerRunResult
    from agent.orchestration.tool_policy import get_active_policy_context

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()

    read_spec = TaskSpec(
        objective="Summarize README",
        provenance=Provenance.EXPLICIT,
        capabilities=(CapabilityClass.READ,),
        side_effects=(SideEffectClass.READ,),
        autonomy_boundary=AutonomyBoundary.READ_ONLY.value,
    )
    write_spec = TaskSpec(
        objective="Implement helper",
        provenance=Provenance.EXPLICIT,
        capabilities=(CapabilityClass.READ, CapabilityClass.WRITE),
        side_effects=(SideEffectClass.WRITE,),
        autonomy_boundary=AutonomyBoundary.WRITE_WITH_POLICY.value,
    )
    read_allowed = allowed_side_effects_for_task(read_spec)
    write_allowed = allowed_side_effects_for_task(write_spec)
    assert SideEffectClass.WRITE not in read_allowed
    assert SideEffectClass.WRITE in write_allowed
    assert SideEffectClass.DESTRUCTIVE not in write_allowed
    assert SideEffectClass.FINANCIAL not in write_allowed

    reg = ToolRegistry()
    reg.register(
        name="write_file",
        toolset="file",
        schema={"name": "write_file", "description": "w", "parameters": {"type": "object"}},
        handler=lambda args, **kw: json.dumps({"ok": True}),
        risk_metadata=ToolRiskMeta(side_effect=SideEffectClass.WRITE, risk_level="moderate"),
    )
    store = ApprovalStore()
    read_ctx = PolicyContext(
        session_id="s",
        turn_id="t",
        tool_call_id="c",
        is_worker=True,
        allowed_side_effects=read_allowed,
        approval_store=store,
    )
    write_ctx = PolicyContext(
        session_id="s",
        turn_id="t",
        tool_call_id="c",
        is_worker=True,
        allowed_side_effects=write_allowed,
        approval_store=store,
    )
    denied = enforce_tool_policy(
        reg, "write_file", {"path": "x.py", "content": "x"}, read_ctx, tool_call_id="c"
    )
    allowed = enforce_tool_policy(
        reg, "write_file", {"path": "x.py", "content": "x"}, write_ctx, tool_call_id="c"
    )
    assert denied.allowed is False
    assert denied.reason_code == "WRITE_NOT_ALLOWED"
    assert allowed.allowed is True

    # Propagation into the active worker loop / thread context
    seen = {"allowed": None}

    def capture_exec(req, *, parent_agent, cfg, **kwargs):
        ctx = get_active_policy_context()
        seen["allowed"] = set(ctx.allowed_side_effects) if ctx else None
        return WorkerRunResult(
            success=True,
            correlation_id=req.correlation_id,
            session_id=parent_agent.session_id,
            task_id=req.task_id,
            worker_id="sa-cap",
            child_session_id="child-cap",
            provider="openrouter",
            model="m",
            reasoning=ReasoningEffort.LOW,
            toolsets=tuple(req.toolsets),
            final_response="ok",
            usage={"input_tokens": 1, "output_tokens": 1, "estimated_cost_usd": 0.01},
        )

    agent = SimpleNamespace(
        session_id="sess-cap",
        model="m",
        provider="p",
        platform="cli",
        _delegate_depth=0,
        _orch_worker=False,
        _cached_system_prompt="SYS",
        tools=[],
        _session_db=None,
        _current_turn_id="turn-cap",
        _last_orchestration_result=None,
    )
    root = _active_root(telemetry={"enabled": False})
    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run", side_effect=capture_exec
    ):
        maybe_orchestrate_turn(
            agent,
            "Summarize the README only",
            explicit_facts={
                "capabilities": ["read"],
                "side_effects": ["read"],
                "complexity": "low",
                "impact": "low",
            },
        )
    assert seen["allowed"] is not None
    assert SideEffectClass.WRITE not in seen["allowed"]
    assert SideEffectClass.READ in seen["allowed"]


def test_worker_cannot_forge_host_approval_using_trusted_true(tmp_path, monkeypatch):
    """worker cannot forge host approval using trusted=True or a public helper."""
    from agent.orchestration.tool_policy import (
        ApprovalStore,
        PolicyContext,
        set_active_policy_context,
        reset_active_policy_context,
        canonical_action_digest,
        grant_trusted_user_approval,
        get_active_policy_context,
    )
    from agent.orchestration.service import maybe_orchestrate_turn
    from agent.orchestration.executor import WorkerRunResult

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()

    store = ApprovalStore()
    ctx = PolicyContext(
        session_id="sess-forge",
        turn_id="turn-forge",
        tool_call_id="corr-forge",
        is_worker=True,
        allowed_side_effects=frozenset({SideEffectClass.READ}),
        approval_store=store,
    )
    token = set_active_policy_context(ctx)
    digest = canonical_action_digest("terminal", {"command": "rm -rf /"})
    try:
        with pytest.raises(PermissionError):
            store.approve(
                session_id="sess-forge",
                turn_id="turn-forge",
                tool_call_id="tc-forge",
                tool_name="terminal",
                action_digest=digest,
                actor="user",
                trusted=True,
            )
        with pytest.raises(PermissionError):
            grant_trusted_user_approval(
                store,
                session_id="sess-forge",
                turn_id="turn-forge",
                tool_call_id="tc-forge",
                tool_name="terminal",
                action_digest=digest,
            )
        # No forgeable public helper should succeed from worker context.
        assert store.lookup(
            session_id="sess-forge",
            turn_id="turn-forge",
            tool_call_id="tc-forge",
            tool_name="terminal",
            action_digest=digest,
        ) is None
    finally:
        reset_active_policy_context(token)

    # Exact forgery attempt from an active worker execution context.
    seen = {"forgery_blocked": False}

    def forge_in_worker(req, *, parent_agent, cfg, **kwargs):
        active = get_active_policy_context()
        assert active is not None and active.is_worker
        try:
            active.approval_store.approve(
                session_id=active.session_id,
                turn_id=active.turn_id,
                tool_call_id="tc-active-forge",
                tool_name="terminal",
                action_digest=canonical_action_digest(
                    "terminal", {"command": "rm -rf /tmp/x"}
                ),
                actor="user",
                trusted=True,
            )
        except PermissionError:
            seen["forgery_blocked"] = True
        return WorkerRunResult(
            success=True,
            correlation_id=req.correlation_id,
            session_id=parent_agent.session_id,
            task_id=req.task_id,
            worker_id="sa-forge",
            child_session_id="child-forge",
            provider="openrouter",
            model="m",
            reasoning=ReasoningEffort.LOW,
            toolsets=tuple(req.toolsets),
            final_response="ok",
        )

    agent = SimpleNamespace(
        session_id="sess-forge-2",
        model="m",
        provider="p",
        platform="cli",
        _delegate_depth=0,
        _orch_worker=False,
        _cached_system_prompt="SYS",
        tools=[],
        _session_db=None,
        _current_turn_id="turn-forge-2",
        _last_orchestration_result=None,
        _orch_approval_store=ApprovalStore(),
    )
    root = _active_root(telemetry={"enabled": False})
    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run", side_effect=forge_in_worker
    ):
        maybe_orchestrate_turn(agent, "Summarize README")
    assert seen["forgery_blocked"] is True


def test_total_trace_cost_aggregates_all_attempts(tmp_path, monkeypatch):
    """total trace cost aggregates all attempts."""
    from agent.orchestration.service import maybe_orchestrate_turn
    from agent.orchestration.executor import WorkerRunResult

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()

    agent = SimpleNamespace(
        session_id="sess-cost",
        model="m",
        provider="p",
        platform="cli",
        _delegate_depth=0,
        _orch_worker=False,
        _cached_system_prompt="SYS",
        tools=[],
        _session_db=None,
        _current_turn_id="turn-cost",
        _last_orchestration_result=None,
    )
    costs = [0.10, 0.20, 0.05]
    idx = {"i": 0}

    def escalating(req, *, parent_agent, cfg, **kwargs):
        i = idx["i"]
        idx["i"] += 1
        cost = costs[min(i, len(costs) - 1)]
        ok = req.family is ModelFamily.SOL
        return WorkerRunResult(
            success=ok,
            correlation_id=req.correlation_id,
            session_id=agent.session_id,
            task_id=req.task_id,
            worker_id=f"sa-{req.family.value}-{i}",
            child_session_id=f"child-{i}",
            provider="openrouter",
            model=f"model-{req.family.value}",
            reasoning=req.reasoning,
            toolsets=tuple(req.toolsets),
            final_response="done" if ok else None,
            usage={
                "input_tokens": 10,
                "output_tokens": 5,
                "estimated_cost_usd": cost,
            },
            error_class=None if ok else "tool_error",
            latency_ms=11,
        )

    root = _active_root()
    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run", side_effect=escalating
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

    assert result.trace is not None
    # Aggregate across retries/escalations — not merely the last attempt.
    assert result.trace.estimated_cost_usd == pytest.approx(sum(costs[: idx["i"]]))
    assert (result.trace.input_tokens or 0) == 10 * idx["i"]
    assert (result.trace.output_tokens or 0) == 5 * idx["i"]
    assert (result.trace.latency_ms or 0) == 11 * idx["i"]


def test_timeout_cancel_owns_interrupt_close_registry_cleanup_bounded_return(
    tmp_path, monkeypatch
):
    """timeout/cancel owns interrupt/close/registry cleanup with bounded return."""
    import tools.delegate_tool as delegate_tool
    from agent.orchestration.executor import execute_worker_run
    from agent.orchestration.contracts import WorkerRunRequest

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()

    parent = SimpleNamespace(
        session_id="parent-to",
        model="parent-model",
        provider="openrouter",
        base_url="https://x",
        api_key="sk",
        api_mode="chat_completions",
        enabled_toolsets=["file"],
        disabled_toolsets=None,
        valid_tool_names={"read_file"},
        tools=[],
        _cached_system_prompt="SYS",
        _delegate_depth=0,
        _session_db=None,
        _current_turn_id="turn-to",
        _active_children=[],
        _active_children_lock=None,
        platform="cli",
    )
    req = WorkerRunRequest(
        goal="hang forever",
        context="brief",
        toolsets=("file",),
        family=ModelFamily.TERRA,
        reasoning=ReasoningEffort.MEDIUM,
        timeout_seconds=1,
        correlation_id="corr-to-cleanup",
        parent_session_id=parent.session_id,
        parent_turn_id="turn-to",
        task_id="task-to",
    )

    events = {"interrupt": 0, "close": 0}
    fake_child = MagicMock()
    fake_child.session_id = "child-to"
    fake_child._subagent_id = "sa-to-cleanup"
    fake_child.enabled_toolsets = ["file"]
    fake_child.model = "m"
    fake_child.provider = "p"
    fake_child.platform = "subagent"
    fake_child._delegate_depth = 1

    def interrupt():
        events["interrupt"] += 1
        # Child ignores cancellation for the purpose of this probe.

    def close():
        events["close"] += 1

    fake_child.interrupt.side_effect = interrupt
    fake_child.close.side_effect = close

    def hang_lifecycle(*a, **kw):
        # Register like real lifecycle ownership, then ignore cancel.
        delegate_tool._register_subagent(
            {
                "subagent_id": fake_child._subagent_id,
                "agent": fake_child,
                "parent_session_id": parent.session_id,
            }
        )
        parent._active_children.append(fake_child)
        time.sleep(5)
        return {"status": "completed", "summary": "late"}

    cfg = load_orchestration_config({"orchestration": {"enabled": True, "mode": "active"}})
    started = time.monotonic()
    with patch(
        "agent.orchestration.executor.resolve_runtime_provider",
        return_value={
            "provider": "openrouter",
            "api_mode": "chat_completions",
            "base_url": "https://x",
            "api_key": "sk",
            "source": "test",
            "requested_provider": "openrouter",
        },
    ), patch(
        "agent.orchestration.executor._build_child_preserving_parent_tools",
        return_value=fake_child,
    ), patch(
        "agent.orchestration.executor._run_child_lifecycle",
        side_effect=hang_lifecycle,
    ), patch(
        "agent.orchestration.executor._DEFAULT_POLL_S",
        0.05,
    ):
        result = execute_worker_run(req, parent_agent=parent, cfg=cfg)
    elapsed = time.monotonic() - started

    assert elapsed < 2.5, f"caller return not bounded: {elapsed:.2f}s"
    assert result.success is False
    assert result.timed_out is True or result.error_class == "timeout"
    assert events["interrupt"] >= 1
    assert events["close"] >= 1
    assert fake_child not in parent._active_children
    active_ids = {
        r.get("subagent_id") for r in delegate_tool.list_active_subagents()
    }
    assert "sa-to-cleanup" not in active_ids


def test_structured_destructive_api_financial_classification_and_false_positive_guard():
    """structured destructive/API/financial action classification and false-positive guard."""
    from tools.registry import ToolRegistry
    from agent.orchestration.tool_policy import (
        ToolRiskMeta,
        normalize_action_risk,
        attach_default_risk_metadata,
        get_tool_risk_meta,
        enforce_tool_policy,
        ApprovalStore,
        PolicyContext,
    )

    reg = ToolRegistry()
    reg.register(
        name="terminal",
        toolset="terminal",
        schema={"name": "terminal", "description": "shell", "parameters": {"type": "object"}},
        handler=lambda args, **kw: json.dumps({"ok": True}),
        risk_metadata=ToolRiskMeta(side_effect=SideEffectClass.WRITE, risk_level="high"),
    )
    reg.register(
        name="http_request",
        toolset="mcp",
        schema={"name": "http_request", "description": "http", "parameters": {"type": "object"}},
        handler=lambda args, **kw: json.dumps({"ok": True}),
        risk_metadata=ToolRiskMeta(side_effect=SideEffectClass.EXTERNAL, risk_level="moderate"),
    )
    reg.register(
        name="broker_place_order",
        toolset="external",
        schema={"name": "broker_place_order", "description": "order", "parameters": {"type": "object"}},
        handler=lambda args, **kw: json.dumps({"ok": True}),
        risk_metadata=ToolRiskMeta(
            side_effect=SideEffectClass.FINANCIAL, risk_level="critical"
        ),
    )
    reg.register(
        name="read_file",
        toolset="file",
        schema={"name": "read_file", "description": "read", "parameters": {"type": "object"}},
        handler=lambda args, **kw: json.dumps({"ok": True}),
        risk_metadata=ToolRiskMeta(side_effect=SideEffectClass.READ, risk_level="low"),
    )
    attach_default_risk_metadata(reg)

    hard = normalize_action_risk(
        "terminal",
        {"command": "git reset --hard HEAD~1"},
        get_tool_risk_meta(reg, "terminal"),
    )
    clean = normalize_action_risk(
        "terminal",
        {"command": "git clean -fd"},
        get_tool_risk_meta(reg, "terminal"),
    )
    assert hard.side_effect is SideEffectClass.DESTRUCTIVE
    assert clean.side_effect is SideEffectClass.DESTRUCTIVE

    api_del = normalize_action_risk(
        "http_request",
        {"method": "DELETE", "url": "https://api.example/v1/resource/1"},
        get_tool_risk_meta(reg, "http_request"),
    )
    assert api_del.side_effect is SideEffectClass.DESTRUCTIVE

    fin = normalize_action_risk(
        "broker_place_order",
        {"symbol": "SYNTH", "qty": 1, "side": "buy"},
        get_tool_risk_meta(reg, "broker_place_order"),
    )
    assert fin.side_effect is SideEffectClass.FINANCIAL

    # False-positive guard: harmless read path containing token "trade"
    trade_read = normalize_action_risk(
        "read_file",
        {"path": "/workspace/docs/trade_notes.md"},
        get_tool_risk_meta(reg, "read_file"),
    )
    assert trade_read.side_effect is SideEffectClass.READ

    store = ApprovalStore()
    ctx = PolicyContext(
        session_id="s",
        turn_id="t",
        tool_call_id="c",
        is_worker=True,
        allowed_side_effects=frozenset(
            {SideEffectClass.NONE, SideEffectClass.READ, SideEffectClass.WRITE}
        ),
        approval_store=store,
    )
    blocked = enforce_tool_policy(
        reg,
        "terminal",
        {"command": "git reset --hard"},
        ctx,
        tool_call_id="c1",
    )
    assert blocked.allowed is False
    assert blocked.requires_approval is True
    read_ok = enforce_tool_policy(
        reg,
        "read_file",
        {"path": "/workspace/docs/trade_notes.md"},
        ctx,
        tool_call_id="c2",
    )
    assert read_ok.allowed is True
