"""WP5 — WorkerRunRequest adapter over ``_build_child_agent`` (fakes only)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from agent.orchestration.config import load_orchestration_config
from agent.orchestration.contracts import (
    ModelFamily,
    ReasoningEffort,
    WorkerRunRequest,
)


def _parent(*, system_prompt="SYSTEM_BYTES", tools=None, depth=0):
    tools = tools or [{"type": "function", "function": {"name": "read_file"}}]
    return SimpleNamespace(
        session_id="parent-sess",
        model="parent-model",
        provider="openrouter",
        base_url="https://openrouter.ai/api/v1",
        api_key="sk-parent",
        api_mode="chat_completions",
        enabled_toolsets=["file", "web", "terminal", "browser"],
        disabled_toolsets=None,
        valid_tool_names={"read_file", "web_search", "terminal"},
        tools=list(tools),
        _cached_system_prompt=system_prompt,
        _delegate_depth=depth,
        _session_db=None,
        _current_turn_id="turn-1",
        _active_children=[],
        _active_children_lock=None,
        max_tokens=4096,
        reasoning_config=None,
        prefill_messages=None,
        fallback_model=None,
        request_overrides={},
        providers_allowed=None,
        providers_ignored=None,
        providers_order=None,
        provider_sort=None,
        provider_require_parameters=None,
        provider_data_collection=None,
        openrouter_min_coding_score=None,
        _print_fn=None,
        platform="cli",
    )


def test_executor_builds_child_via_delegate_with_resolved_runtime_and_static_toolsets():
    from agent.orchestration.executor import execute_worker_run

    parent = _parent()
    parent_prompt_before = parent._cached_system_prompt
    parent_tools_before = list(parent.tools)
    parent_model_before = parent.model

    req = WorkerRunRequest(
        goal="Summarize README",
        context="Worker brief text",
        toolsets=("file", "web"),
        family=ModelFamily.LUNA,
        reasoning=ReasoningEffort.LOW,
        timeout_seconds=90,
        correlation_id="corr-abc",
        provider_alias="openrouter",
        model_alias="terra-alias",
        parent_session_id=parent.session_id,
        parent_turn_id="turn-1",
        task_id="task-9",
        max_iterations=12,
    )

    fake_child = MagicMock()
    fake_child.session_id = "child-sess"
    fake_child._subagent_id = "sa-0-deadbeef"
    fake_child.enabled_toolsets = ["file", "web"]
    fake_child.model = "resolved-model"
    fake_child.provider = "openrouter"
    fake_child.platform = "subagent"
    fake_child._delegate_depth = 1
    fake_child.run_conversation.return_value = {
        "final_response": "ok",
        "messages": [],
        "usage": {"input_tokens": 11, "output_tokens": 7},
    }

    cfg = load_orchestration_config(
        {
            "orchestration": {
                "model_aliases": {"terra-alias": "google/gemini-flash"},
                "enabled": True,
                "mode": "active",
            }
        }
    )

    with patch(
        "agent.orchestration.executor.resolve_runtime_provider",
        return_value={
            "provider": "openrouter",
            "api_mode": "chat_completions",
            "base_url": "https://openrouter.ai/api/v1",
            "api_key": "sk-resolved",
            "source": "test",
            "requested_provider": "openrouter",
        },
    ) as resolve_mock, patch(
        "agent.orchestration.executor._build_child_preserving_parent_tools",
        return_value=fake_child,
    ) as build_mock, patch(
        "agent.orchestration.executor._run_child_lifecycle",
        return_value={
            "status": "completed",
            "summary": "ok",
            "final_response": "ok",
            "usage": {"input_tokens": 11, "output_tokens": 7},
        },
    ):
        result = execute_worker_run(req, parent_agent=parent, cfg=cfg)

    assert result.success is True
    assert result.worker_id == "sa-0-deadbeef"
    assert result.correlation_id == "corr-abc"
    assert result.provider == "openrouter"
    assert result.model == "resolved-model"
    assert result.reasoning == ReasoningEffort.LOW
    assert tuple(result.toolsets) == ("file", "web")
    assert result.usage["input_tokens"] == 11

    resolve_mock.assert_called()
    build_kwargs = build_mock.call_args.kwargs
    assert build_kwargs["goal"] == "Summarize README"
    assert build_kwargs["context"] == "Worker brief text"
    assert build_kwargs["toolsets"] == ["file", "web"]
    assert build_kwargs["override_provider"] == "openrouter"
    assert build_kwargs["override_api_key"] == "sk-resolved"
    assert build_kwargs["max_iterations"] == 12
    assert build_kwargs["role"] == "leaf"

    # Parent immutability
    assert parent._cached_system_prompt == parent_prompt_before
    assert parent.tools == parent_tools_before
    assert parent.model == parent_model_before


def test_executor_honors_timeout_and_cancellation_without_duplicate_transport():
    from agent.orchestration.executor import execute_worker_run, WorkerCancelled

    parent = _parent()
    req = WorkerRunRequest(
        goal="long job",
        context="brief",
        toolsets=("file",),
        family=ModelFamily.TERRA,
        reasoning=ReasoningEffort.MEDIUM,
        timeout_seconds=1,
        correlation_id="corr-cancel",
    )

    fake_child = MagicMock()
    fake_child.session_id = "child-2"
    fake_child._subagent_id = "sa-1-cafe"
    fake_child.enabled_toolsets = ["file"]
    fake_child.model = "m"
    fake_child.provider = "p"
    fake_child.platform = "subagent"
    fake_child._delegate_depth = 1

    def _hang(**_kw):
        import time

        time.sleep(5)
        return {"final_response": "late"}

    fake_child.run_conversation.side_effect = _hang

    cancel = {"flag": False}

    def cancel_check():
        return cancel["flag"]

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
        return_value={"status": "completed", "summary": "late"},
    ), patch(
        "agent.orchestration.executor._DEFAULT_POLL_S",
        0.05,
    ):
        # Force immediate cancel via short timeout path
        result = execute_worker_run(
            req,
            parent_agent=parent,
            cfg=load_orchestration_config({}),
            cancel_check=lambda: True,
        )

    assert result.success is False
    assert result.cancelled is True
    assert result.error_class in ("cancelled", "timeout", WorkerCancelled.__name__)


def test_executor_correlates_to_session_and_task():
    from agent.orchestration.executor import execute_worker_run

    parent = _parent()
    req = WorkerRunRequest(
        goal="g",
        context="c",
        toolsets=("file",),
        family=ModelFamily.LUNA,
        reasoning=ReasoningEffort.LOW,
        timeout_seconds=30,
        correlation_id="corr-42",
        parent_session_id="parent-sess",
        task_id="task-42",
    )
    fake_child = MagicMock()
    fake_child.session_id = "child-x"
    fake_child._subagent_id = "sa-9"
    fake_child.enabled_toolsets = ["file"]
    fake_child.model = "m"
    fake_child.provider = "openrouter"
    fake_child.platform = "subagent"
    fake_child._delegate_depth = 1
    fake_child.run_conversation.return_value = {
        "final_response": "done",
        "usage": {},
    }

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
        return_value={
            "status": "completed",
            "summary": "done",
            "final_response": "done",
            "usage": {},
        },
    ):
        result = execute_worker_run(
            req, parent_agent=parent, cfg=load_orchestration_config({})
        )

    assert result.session_id == "parent-sess"
    assert result.task_id == "task-42"
    assert result.correlation_id == "corr-42"
    assert result.child_session_id == "child-x"


def test_default_family_alias_inherits_parent_model():
    """Empty/unmapped family aliases must not become literal model IDs."""
    import agent.orchestration.executor as executor_mod
    from agent.orchestration.executor import execute_worker_run

    parent = _parent()
    req = WorkerRunRequest(
        goal="quick task",
        context="brief",
        toolsets=("file",),
        family=ModelFamily.LUNA,
        reasoning=ReasoningEffort.LOW,
        timeout_seconds=30,
        correlation_id="corr-alias",
        provider_alias="delegation",
        model_alias="luna",
        parent_session_id=parent.session_id,
    )

    fake_child = MagicMock()
    fake_child.session_id = "child-alias"
    fake_child._subagent_id = "sa-alias"
    fake_child.enabled_toolsets = ["file"]
    fake_child.model = "parent-model"
    fake_child.provider = "openrouter"
    fake_child.platform = "subagent"
    fake_child._delegate_depth = 1
    fake_child.run_conversation.return_value = {
        "final_response": "ok",
        "usage": {},
    }

    cfg = load_orchestration_config(
        {
            "orchestration": {
                "enabled": True,
                "mode": "active",
                "model_aliases": {"luna": "", "terra": "", "sol": ""},
            }
        }
    )

    build_kwargs = {}

    def capture_build(**kwargs):
        build_kwargs.update(kwargs)
        return fake_child

    # Wire patch targets that the repaired executor must expose/use.
    if not hasattr(executor_mod, "_build_child_preserving_parent_tools"):
        executor_mod._build_child_preserving_parent_tools = executor_mod._build_child_agent
    if not hasattr(executor_mod, "_run_child_lifecycle"):
        def _legacy_lifecycle(task_index, goal, child=None, parent_agent=None, **kw):
            return child.run_conversation(
                user_message=goal, system_message=None, conversation_history=None
            )

        executor_mod._run_child_lifecycle = _legacy_lifecycle

    with patch(
        "agent.orchestration.executor.resolve_runtime_provider",
        return_value={
            "provider": "openrouter",
            "api_mode": "chat_completions",
            "base_url": "https://openrouter.ai/api/v1",
            "api_key": "sk-resolved",
            "source": "test",
            "requested_provider": None,
        },
    ) as resolve_mock, patch(
        "agent.orchestration.executor._build_child_agent",
        side_effect=capture_build,
    ), patch(
        "agent.orchestration.executor._build_child_preserving_parent_tools",
        side_effect=capture_build,
    ), patch(
        "agent.orchestration.executor._run_child_lifecycle",
        side_effect=lambda *a, **kw: {
            "status": "completed",
            "summary": "ok",
            "final_response": "ok",
            "usage": {},
        },
    ):
        result = execute_worker_run(req, parent_agent=parent, cfg=cfg)

    resolve_kwargs = resolve_mock.call_args.kwargs
    assert resolve_kwargs.get("target_model") not in {"luna", "terra", "sol"}
    model_arg = build_kwargs.get("model")
    assert model_arg not in {"luna", "terra", "sol"}
    assert model_arg in (None, "", parent.model, "parent-model")
    assert result.model not in {"luna", "terra", "sol"}


def test_executor_owns_complete_child_cleanup_and_parent_tool_preservation():
    """Executor must reuse delegate lifecycle cleanup on all exit paths."""
    import agent.orchestration.executor as executor_mod
    from agent.orchestration.executor import execute_worker_run
    import model_tools

    parent = _parent()
    parent._active_children = []
    parent_tools_before = [{"type": "function", "function": {"name": "read_file"}}]
    parent.tools = list(parent_tools_before)
    saved_names = ["read_file", "web_search"]
    model_tools._last_resolved_tool_names = list(saved_names)

    req = WorkerRunRequest(
        goal="cleanup contract",
        context="brief",
        toolsets=("file",),
        family=ModelFamily.LUNA,
        reasoning=ReasoningEffort.LOW,
        timeout_seconds=30,
        correlation_id="corr-clean",
    )

    events = []

    fake_child = MagicMock()
    fake_child.session_id = "child-clean"
    fake_child._subagent_id = "sa-clean"
    fake_child.enabled_toolsets = ["file"]
    fake_child.model = "parent-model"
    fake_child.provider = "openrouter"
    fake_child.platform = "subagent"
    fake_child._delegate_depth = 1
    fake_child._delegate_saved_tool_names = list(saved_names)

    def close_side_effect():
        events.append("close")

    fake_child.close.side_effect = close_side_effect

    def lifecycle(task_index, goal, child=None, parent_agent=None, **kw):
        events.append("lifecycle")
        if parent_agent is not None and child in getattr(parent_agent, "_active_children", []):
            parent_agent._active_children.remove(child)
        model_tools._last_resolved_tool_names = list(
            getattr(child, "_delegate_saved_tool_names", saved_names)
        )
        if hasattr(child, "close"):
            child.close()
        events.append("cleanup_done")
        return {
            "status": "completed",
            "summary": "ok",
            "final_response": "ok",
            "usage": {"input_tokens": 1, "output_tokens": 1},
        }

    cfg = load_orchestration_config({"orchestration": {"enabled": True, "mode": "active"}})
    if not hasattr(executor_mod, "_build_child_preserving_parent_tools"):
        executor_mod._build_child_preserving_parent_tools = executor_mod._build_child_agent
    if not hasattr(executor_mod, "_run_child_lifecycle"):
        executor_mod._run_child_lifecycle = lifecycle

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
    ) as build_mock, patch(
        "agent.orchestration.executor._run_child_lifecycle",
        side_effect=lifecycle,
    ) as life_mock:
        result = execute_worker_run(req, parent_agent=parent, cfg=cfg)

    assert build_mock.called, "must build via parent-tool-preserving seam"
    assert life_mock.called, "must run via lifecycle-owned seam"
    assert "lifecycle" in events and "cleanup_done" in events
    assert events.count("close") == 1
    assert model_tools._last_resolved_tool_names == saved_names
    assert parent.tools == parent_tools_before
    assert result.success is True

    events.clear()

    def boom(*a, **kw):
        events.append("lifecycle")
        raise RuntimeError("child exploded")

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
        side_effect=boom,
    ):
        result2 = execute_worker_run(req, parent_agent=parent, cfg=cfg)

    assert result2.success is False
    assert model_tools._last_resolved_tool_names == saved_names
    assert parent.tools == parent_tools_before


def test_executor_timeout_returns_within_deadline():
    """Timeout must return within deadline + small tolerance; no orphan wait."""
    import time
    import agent.orchestration.executor as executor_mod
    from agent.orchestration.executor import execute_worker_run

    parent = _parent()
    parent._active_children = []
    req = WorkerRunRequest(
        goal="hang",
        context="brief",
        toolsets=("file",),
        family=ModelFamily.TERRA,
        reasoning=ReasoningEffort.MEDIUM,
        timeout_seconds=1,
        correlation_id="corr-deadline",
    )

    fake_child = MagicMock()
    fake_child.session_id = "child-deadline"
    fake_child._subagent_id = "sa-deadline"
    fake_child.enabled_toolsets = ["file"]
    fake_child.model = "m"
    fake_child.provider = "p"
    fake_child.platform = "subagent"
    fake_child._delegate_depth = 1
    interrupted = {"count": 0}
    cleaned = {"count": 0}

    def interrupt():
        interrupted["count"] += 1

    fake_child.interrupt.side_effect = interrupt

    def hang_run(**_kw):
        try:
            time.sleep(5)
            return {"final_response": "late", "usage": {}}
        finally:
            cleaned["count"] += 1

    fake_child.run_conversation.side_effect = hang_run

    def hang_lifecycle(*a, **kw):
        try:
            time.sleep(5)
            return {"status": "completed", "summary": "late"}
        finally:
            cleaned["count"] += 1

    if not hasattr(executor_mod, "_build_child_preserving_parent_tools"):
        executor_mod._build_child_preserving_parent_tools = executor_mod._build_child_agent
    if not hasattr(executor_mod, "_run_child_lifecycle"):
        executor_mod._run_child_lifecycle = hang_lifecycle

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
        "agent.orchestration.executor._build_child_agent",
        return_value=fake_child,
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
        result = execute_worker_run(
            req, parent_agent=parent, cfg=load_orchestration_config({})
        )
    elapsed = time.monotonic() - started

    assert elapsed < 2.5, f"returned too late: {elapsed:.2f}s"
    assert result.timed_out is True or result.error_class == "timeout"
    assert result.success is False
    assert interrupted["count"] >= 1 or cleaned["count"] >= 1


def test_explicit_concrete_model_survives_shared_credential_lease_binding():
    """A shared credential lease must not reset an orchestrated child to SOL."""
    from agent.orchestration.executor import execute_worker_run

    parent = _parent()
    parent.model = "gpt-5.6-sol"
    req = WorkerRunRequest(
        goal="canary smoke: hej",
        context="brief",
        toolsets=("file",),
        family=ModelFamily.TERRA,
        reasoning=ReasoningEffort.MEDIUM,
        timeout_seconds=30,
        correlation_id="corr-model-intent",
        provider_alias="openai-codex",
        model_alias="gpt-5.6-terra",
    )

    fake_pool = MagicMock()
    fake_pool.acquire_lease.return_value = "cred-1"
    fake_pool.current.return_value = SimpleNamespace(id="cred-1")
    fake_child = MagicMock()
    fake_child.session_id = "child-model-intent"
    fake_child._subagent_id = None
    fake_child._parent_subagent_id = None
    fake_child._delegate_depth = 1
    fake_child._delegate_role = "leaf"
    fake_child._delegate_saved_tool_names = []
    fake_child._credential_pool = fake_pool
    fake_child.enabled_toolsets = ["file"]
    fake_child.model = "gpt-5.6-terra"
    fake_child.provider = "openai-codex"
    fake_child.platform = "subagent"
    fake_child.tool_progress_callback = None
    fake_child._delegate_output_schema = None
    seen_models = []

    def swap_credential(_entry):
        fake_child.model = "gpt-5.6-sol"

    def run_conversation(**_kwargs):
        seen_models.append(fake_child.model)
        return {
            "final_response": "done",
            "messages": [],
            "usage": {"input_tokens": 1, "output_tokens": 1},
            "api_calls": 1,
            "completed": True,
        }

    fake_child._swap_credential.side_effect = swap_credential
    fake_child.run_conversation.side_effect = run_conversation
    fake_child.get_activity_summary.return_value = {"api_call_count": 1}
    cfg = load_orchestration_config(
        {
            "orchestration": {
                "enabled": True,
                "mode": "active",
                "model_aliases": {
                    "luna": "gpt-5.6-luna",
                    "terra": "gpt-5.6-terra",
                    "sol": "gpt-5.6-sol",
                },
            }
        }
    )

    with patch(
        "agent.orchestration.executor.resolve_runtime_provider",
        return_value={
            "provider": "openai-codex",
            "api_mode": "codex_responses",
            "base_url": "https://chatgpt.com/backend-api/codex",
            "api_key": "token",
            "source": "test",
            "requested_provider": "openai-codex",
        },
    ) as resolve_mock, patch(
        "agent.orchestration.executor._build_child_preserving_parent_tools",
        return_value=fake_child,
    ) as build_mock, patch("tools.delegate_tool._get_child_timeout", return_value=None):
        result = execute_worker_run(req, parent_agent=parent, cfg=cfg)

    assert resolve_mock.call_args.kwargs["target_model"] == "gpt-5.6-terra"
    assert build_mock.call_args.kwargs["model"] == "gpt-5.6-terra"
    assert seen_models == ["gpt-5.6-terra"]
    assert result.model == "gpt-5.6-terra"
    fake_pool.release_lease.assert_called_once_with("cred-1")
