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
        "agent.orchestration.executor._build_child_agent",
        return_value=fake_child,
    ) as build_mock:
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
        "agent.orchestration.executor._build_child_agent",
        return_value=fake_child,
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
        "agent.orchestration.executor._build_child_agent",
        return_value=fake_child,
    ):
        result = execute_worker_run(
            req, parent_agent=parent, cfg=load_orchestration_config({})
        )

    assert result.session_id == "parent-sess"
    assert result.task_id == "task-42"
    assert result.correlation_id == "corr-42"
    assert result.child_session_id == "child-x"
