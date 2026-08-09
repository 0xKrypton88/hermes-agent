"""RED→GREEN: messaging Terra/Sol active routing must be non-blocking.

Acceptance coverage:
A) Blocked Terra/Sol child does not block active messaging return; parent
   retains no active child ownership.
B) Durable completion carries exact origin routing identity + normalized
   human result.
C) Luna remains normal synchronous dialogue (no raw worker JSON).
D) Concrete gpt-5.6-terra / gpt-5.6-sol intent survives async path.
E) Unsupported delivery, unresolved approval, and async capacity rejection
   are non-blocking fail-safe (no silent sync Terra/Sol fallback).
"""

from __future__ import annotations

import json
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from agent.orchestration.contracts import ModelFamily
from agent.orchestration.origin import TurnOrigin
from tools import async_delegation as ad
from tools.process_registry import process_registry


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


@pytest.fixture(autouse=True)
def _clean_async_state():
    ad._reset_for_tests()
    while not process_registry.completion_queue.empty():
        process_registry.completion_queue.get_nowait()
    yield
    deadline = time.monotonic() + 2.0
    while ad.active_count() and time.monotonic() < deadline:
        time.sleep(0.02)
    ad._reset_for_tests()
    while not process_registry.completion_queue.empty():
        process_registry.completion_queue.get_nowait()


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


def _canary_agent(**kwargs):
    defaults = dict(
        platform=CANARY["platform"],
        user_id=CANARY["user_id"],
        chat_id=CANARY["channel_id"],
        scope_id=CANARY["workspace_id"],
        depth=0,
        model="gpt-5.6-luna",
        trusted_origin=True,
    )
    defaults.update(kwargs)
    return SimpleNamespace(
        session_id="sess-async-orch",
        model=defaults["model"],
        provider="openai-codex",
        platform=defaults["platform"],
        _delegate_depth=defaults["depth"],
        _orch_worker=False,
        _cached_system_prompt="BYTE_STABLE_SYSTEM_PROMPT",
        tools=[{"type": "function", "function": {"name": "read_file"}}],
        valid_tool_names={"read_file"},
        enabled_toolsets=["file", "web", "terminal", "browser"],
        disabled_toolsets=None,
        _session_db=None,
        _current_turn_id="turn-async-orch",
        _last_orchestration_result=None,
        api_mode="chat_completions",
        max_iterations=1,
        quiet_mode=True,
        _user_id=defaults["user_id"],
        _chat_id=defaults["chat_id"],
        _chat_type="dm",
        _thread_id="thread-origin-1",
        _scope_id=defaults["scope_id"],
        _gateway_session_key=(
            f"agent:main:slack:dm:{CANARY['workspace_id']}:{CANARY['channel_id']}"
        ),
        _turn_origin_trusted=defaults["trusted_origin"],
        _active_children=[],
        _active_children_lock=threading.Lock(),
    )


def _origin_from_agent(agent) -> TurnOrigin:
    return TurnOrigin(
        platform=str(agent.platform),
        workspace_id=str(agent._scope_id),
        channel_id=str(agent._chat_id),
        user_id=str(agent._user_id),
        trusted=True,
        chat_type="dm",
        session_key=str(agent._gateway_session_key),
        thread_id=str(agent._thread_id),
    )


def _drain_for(delegation_id, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not process_registry.completion_queue.empty():
            evt = process_registry.completion_queue.get_nowait()
            if evt.get("delegation_id") == delegation_id:
                return evt
            continue
        time.sleep(0.02)
    return None


def _complete_active(agent, user_message, *, root, explicit_facts=None):
    from agent.orchestration.service import (
        complete_active_orchestration,
        maybe_orchestrate_turn,
    )

    plan = maybe_orchestrate_turn(
        agent,
        user_message,
        turn_origin=_origin_from_agent(agent),
        defer_worker=True,
        explicit_facts=explicit_facts,
        task_id="turn-async-orch",
    )
    if plan.pending_worker:
        return complete_active_orchestration(
            plan,
            agent,
            task_id="turn-async-orch",
            messages=[{"role": "user", "content": user_message}],
        )
    return plan


# ── A: blocked Terra/Sol child must not block messaging return ──────────────


def test_messaging_terra_blocked_child_returns_ack_without_active_child(
    tmp_path, monkeypatch
):
    from agent.orchestration.executor import WorkerRunResult

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    agent = _canary_agent()
    root = _canary_root()
    gate = threading.Event()
    entered = threading.Event()

    def slow_worker(req, parent_agent=None, cfg=None):
        entered.set()
        # Simulate attaching a child the way delegate_tool does.
        child = SimpleNamespace(session_id="child-terra-slow", model=req.model_alias)
        if parent_agent is not None and hasattr(parent_agent, "_active_children"):
            parent_agent._active_children.append(child)
        gate.wait(timeout=60)
        if parent_agent is not None and hasattr(parent_agent, "_active_children"):
            try:
                parent_agent._active_children.remove(child)
            except ValueError:
                pass
        return WorkerRunResult(
            success=True,
            correlation_id=req.correlation_id,
            session_id=getattr(parent_agent, "session_id", None),
            task_id=req.task_id,
            worker_id="worker-terra",
            child_session_id="child-terra-slow",
            provider="openai-codex",
            model="gpt-5.6-terra",
            reasoning=req.reasoning,
            toolsets=req.toolsets,
            final_response=json.dumps(
                {
                    "summary": "Terra finished the multi-step work.",
                    "evidence": ["step-1"],
                    "status": "ok",
                }
            ),
            usage={"input_tokens": 3, "output_tokens": 4},
            latency_ms=10,
        )

    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run", side_effect=slow_worker
    ), patch(
        "gateway.session_context.async_delivery_supported", return_value=True
    ), patch(
        "tools.approval.get_current_session_key",
        return_value=agent._gateway_session_key,
    ):
        t0 = time.monotonic()
        result = _complete_active(
            agent,
            "Implement a multi-step feature with tests",
            root=root,
        )
        elapsed = time.monotonic() - t0

        assert result.mode == "active"
        assert result.decision is not None
        assert result.decision.family is ModelFamily.TERRA
        assert result.pending_worker is False
        assert result.acted is True
        assert result.legacy_continue is False
        assert isinstance(result.response, dict)
        ack = result.response.get("final_response") or ""
        assert ack
        assert "{" not in ack or "summary" not in ack
        assert result.response.get("completed") is True or result.response.get(
            "orchestration", {}
        ).get("status") in {"dispatched", "async_dispatched", "ok"}
        # Non-blocking: returned while child still gated.
        assert elapsed < 2.0, f"foreground blocked {elapsed:.2f}s on Terra child"
        assert agent._active_children == []
        orch = result.response.get("orchestration") or {}
        delegation_id = orch.get("delegation_id")
        assert delegation_id
        # Hold patches until the durable runner finishes — the child executes
        # on the async rail after the foreground ack returns.
        deadline = time.monotonic() + 2.0
        while not entered.is_set() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert entered.is_set()
        assert agent._active_children == []
        gate.set()
        evt = _drain_for(delegation_id, timeout=5.0)
        assert evt is not None
        assert evt.get("status") in {"completed", "success", "ok"}


# ── B: durable completion carries origin routing + human result ─────────────


def test_messaging_terra_completion_preserves_origin_and_human_summary(
    tmp_path, monkeypatch
):
    from agent.orchestration.executor import WorkerRunResult

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    agent = _canary_agent()
    root = _canary_root()

    def fast_worker(req, parent_agent=None, cfg=None):
        return WorkerRunResult(
            success=True,
            correlation_id=req.correlation_id,
            session_id=getattr(parent_agent, "session_id", None),
            task_id=req.task_id,
            worker_id="worker-terra",
            child_session_id="child-terra",
            provider="openai-codex",
            model="gpt-5.6-terra",
            reasoning=req.reasoning,
            toolsets=req.toolsets,
            final_response=json.dumps(
                {
                    "summary": "Done: feature implemented and tests green.",
                    "evidence": ["tests passed"],
                    "status": "ok",
                }
            ),
            usage={"input_tokens": 1, "output_tokens": 2},
            latency_ms=5,
        )

    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run", side_effect=fast_worker
    ), patch(
        "gateway.session_context.async_delivery_supported", return_value=True
    ), patch(
        "tools.approval.get_current_session_key",
        return_value=agent._gateway_session_key,
    ):
        result = _complete_active(
            agent,
            "Implement a multi-step feature with tests",
            root=root,
        )
        orch = result.response["orchestration"]
        delegation_id = orch["delegation_id"]
        evt = _drain_for(delegation_id, timeout=5.0)

    assert evt is not None
    assert evt["type"] == "async_delegation"
    assert evt["session_key"] == agent._gateway_session_key
    assert evt["parent_session_id"] == agent.session_id
    assert evt.get("status") in {"completed", "success", "ok"}
    summary = evt.get("summary") or ""
    assert "Done: feature implemented and tests green." in summary
    assert "evidence" not in summary
    # Raw envelope keys must not leak as the user-facing summary body.
    assert not summary.strip().startswith("{")


# ── C: Luna remains normal synchronous dialogue ─────────────────────────────


def test_messaging_luna_stays_sync_human_dialogue(tmp_path, monkeypatch):
    from agent.orchestration.executor import WorkerRunResult

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    agent = _canary_agent()
    root = _canary_root()
    worker_calls = []

    def luna_worker(req, parent_agent=None, cfg=None):
        worker_calls.append(req.family)
        return WorkerRunResult(
            success=True,
            correlation_id=req.correlation_id,
            session_id=getattr(parent_agent, "session_id", None),
            task_id=req.task_id,
            worker_id="worker-luna",
            child_session_id="child-luna",
            provider="openai-codex",
            model="gpt-5.6-luna",
            reasoning=req.reasoning,
            toolsets=req.toolsets,
            final_response=json.dumps(
                {
                    "summary": "Hej! Anteckningen handlar om lunch.",
                    "evidence": ["note.txt"],
                    "status": "ok",
                }
            ),
            usage={"input_tokens": 1, "output_tokens": 1},
            latency_ms=3,
        )

    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run", side_effect=luna_worker
    ), patch(
        "tools.async_delegation.dispatch_async_delegation"
    ) as dispatch_mock:
        result = _complete_active(agent, "Hej, sammanfatta anteckningen", root=root)

    assert result.decision is not None
    assert result.decision.family is ModelFamily.LUNA
    assert worker_calls == [ModelFamily.LUNA]
    dispatch_mock.assert_not_called()
    assert result.pending_worker is False
    assert result.acted is True
    text = result.response.get("final_response") or ""
    assert text == "Hej! Anteckningen handlar om lunch."
    assert "evidence" not in text
    assert '"status"' not in text


# ── D: concrete model survives async path ───────────────────────────────────


@pytest.mark.parametrize(
    "family,message,expected_model",
    [
        (
            ModelFamily.TERRA,
            "Implement a multi-step feature with tests",
            "gpt-5.6-terra",
        ),
        (
            ModelFamily.SOL,
            "Investigate a production security issue thoroughly",
            "gpt-5.6-sol",
        ),
    ],
)
def test_messaging_concrete_model_survives_async_dispatch(
    tmp_path, monkeypatch, family, message, expected_model
):
    from agent.orchestration.executor import WorkerRunResult

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    agent = _canary_agent()
    root = _canary_root()
    seen_models = []

    def capture_worker(req, parent_agent=None, cfg=None):
        seen_models.append(req.model_alias)
        return WorkerRunResult(
            success=True,
            correlation_id=req.correlation_id,
            session_id=getattr(parent_agent, "session_id", None),
            task_id=req.task_id,
            worker_id="worker-x",
            child_session_id="child-x",
            provider="openai-codex",
            model=expected_model,
            reasoning=req.reasoning,
            toolsets=req.toolsets,
            final_response="ok",
            usage={"input_tokens": 1, "output_tokens": 1},
            latency_ms=2,
        )

    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run", side_effect=capture_worker
    ), patch(
        "gateway.session_context.async_delivery_supported", return_value=True
    ), patch(
        "tools.approval.get_current_session_key",
        return_value=agent._gateway_session_key,
    ):
        result = _complete_active(agent, message, root=root)
        assert result.decision is not None
        assert result.decision.family is family
        assert result.decision.concrete_model_alias == expected_model
        orch = result.response["orchestration"]
        assert orch.get("delegation_id")
        assert orch.get("concrete_model") == expected_model
        deadline = time.monotonic() + 2.0
        while not seen_models and time.monotonic() < deadline:
            time.sleep(0.02)
        evt = _drain_for(orch["delegation_id"], timeout=5.0)

    assert evt is not None
    assert evt.get("model") == expected_model
    assert seen_models
    assert expected_model in {
        FAMILY_MODELS.get(str(m), str(m)) for m in seen_models
    } or expected_model in {str(m) for m in seen_models}


# ── E: fail-safe paths (unsupported / approval / capacity) ──────────────────


def test_messaging_terra_unsupported_delivery_fails_closed_non_blocking(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    agent = _canary_agent()
    root = _canary_root()

    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run"
    ) as exec_mock, patch(
        "gateway.session_context.async_delivery_supported", return_value=False
    ), patch(
        "tools.async_delegation.dispatch_async_delegation"
    ) as dispatch_mock:
        t0 = time.monotonic()
        result = _complete_active(
            agent,
            "Implement a multi-step feature with tests",
            root=root,
        )
        elapsed = time.monotonic() - t0

    assert elapsed < 2.0
    assert result.acted is True
    assert result.pending_worker is False
    assert result.legacy_continue is False
    exec_mock.assert_not_called()
    dispatch_mock.assert_not_called()
    status = str(
        result.response.get("status")
        or (result.response.get("orchestration") or {}).get("status")
        or ""
    ).upper()
    assert status in {"BLOCKED", "UNAVAILABLE", "UNSUPPORTED"}
    assert result.response.get("completed") is False
    assert agent._active_children == []


def test_messaging_approval_not_async_dispatched(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    agent = _canary_agent()
    root = _canary_root()

    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run"
    ) as exec_mock, patch(
        "tools.async_delegation.dispatch_async_delegation"
    ) as dispatch_mock:
        result = _complete_active(
            agent,
            "Radera alla backups och genomför en betalningsorder nu",
            root=root,
        )

    assert result.decision is not None
    assert result.decision.requires_approval is True
    assert result.pending_worker is False
    assert result.response.get("status") == "REQUIRE_APPROVAL"
    exec_mock.assert_not_called()
    dispatch_mock.assert_not_called()
    assert agent._active_children == []


def test_messaging_terra_capacity_rejection_no_sync_fallback(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()
    agent = _canary_agent()
    root = _canary_root()

    with patch("agent.orchestration.service.load_config", return_value=root), patch(
        "agent.orchestration.service.execute_worker_run"
    ) as exec_mock, patch(
        "gateway.session_context.async_delivery_supported", return_value=True
    ), patch(
        "tools.approval.get_current_session_key",
        return_value=agent._gateway_session_key,
    ), patch(
        "tools.async_delegation.dispatch_async_delegation",
        return_value={
            "status": "rejected",
            "error": "Async delegation capacity reached (3 running).",
        },
    ):
        t0 = time.monotonic()
        result = _complete_active(
            agent,
            "Implement a multi-step feature with tests",
            root=root,
        )
        elapsed = time.monotonic() - t0

    assert elapsed < 2.0
    assert result.decision.family is ModelFamily.TERRA
    # Must NOT fall back to blocking synchronous Terra.
    exec_mock.assert_not_called()
    assert result.acted is True
    assert result.pending_worker is False
    assert result.legacy_continue is False
    status = str(
        result.response.get("status")
        or (result.response.get("orchestration") or {}).get("status")
        or ""
    ).upper()
    assert status in {"BUSY", "UNAVAILABLE", "CAPACITY", "REJECTED", "BLOCKED"}
    assert result.response.get("completed") is False
    ack = (result.response.get("final_response") or "").lower()
    assert any(tok in ack for tok in ("busy", "capacity", "unavailable", "try again"))
    assert agent._active_children == []
