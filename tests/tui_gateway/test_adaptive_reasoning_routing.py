"""Gateway contract tests for narrow GPT-5.6 adaptive reasoning routing."""

from __future__ import annotations

import json
import os
import tempfile
import threading
from types import SimpleNamespace
from unittest.mock import patch

import pytest

# The canonical runner uses env -i. Give module import an isolated home before
# tui_gateway.server resolves its profile paths on Windows.
os.environ.setdefault("HERMES_HOME", os.path.join(tempfile.gettempdir(), "hermes-adaptive-tests"))

import tui_gateway.server as server


_ENABLED = {
    "agent": {
        "adaptive_reasoning": {
            "enabled": True,
            "provider": "openai-codex",
            "model": "gpt-5.6-sol",
            "default_effort": "medium",
            "min_effort": "low",
            "max_effort": "high",
            "followup_policy": "escalate_only",
        }
    }
}


def _create(params: dict, cfg: dict) -> tuple[dict, dict]:
    with patch.object(server, "_load_cfg", return_value=cfg), \
            patch.object(server, "_new_session_key", return_value="stored-1"), \
            patch.object(server, "_completion_cwd", return_value="D:/work"), \
            patch.object(server, "_resolve_session_source", return_value="desktop"), \
            patch.object(server, "_profile_home", return_value=None), \
            patch.object(server, "_claim_active_session_slot", return_value=(None, None)), \
            patch.object(server, "_register_session_cwd"), \
            patch.object(server, "_schedule_agent_build"), \
            patch.object(server, "_schedule_session_cap_enforcement"), \
            patch.object(server, "_git_branch_for_cwd", return_value="main"), \
            patch.object(server, "_project_info_for_cwd", return_value=None):
        response = server._methods["session.create"]("rid", params)
    sid = response["result"]["session_id"]
    return response, server._sessions.pop(sid)


def test_auto_create_fixes_route_and_keeps_effort_undecided() -> None:
    response, session = _create(
        {"reasoning_mode": "auto", "reasoning_effort": "high"}, _ENABLED
    )

    assert session["reasoning_mode"] == "auto"
    assert session["reasoning_floor"] is None
    assert session["adaptive_reasoning_decision"] is None
    assert session["create_reasoning_override"] is None
    assert session["model_override"] == {
        "model": "gpt-5.6-sol",
        "provider": "openai-codex",
    }
    assert response["result"]["info"]["reasoning_mode"] == "auto"
    assert response["result"]["info"]["reasoning_effort"] == ""


def test_disabled_auto_preserves_legacy_route() -> None:
    response, session = _create({"reasoning_mode": "auto"}, {"agent": {}})

    assert session["reasoning_mode"] == "inherit"
    assert session["model_override"] is None
    assert session["create_reasoning_override"] is None
    assert response["result"]["info"]["reasoning_mode"] == "inherit"


def test_invalid_reasoning_mode_is_rejected() -> None:
    with patch.object(server, "_load_cfg", return_value=_ENABLED):
        response = server._methods["session.create"](
            "rid", {"reasoning_mode": "delegated"}
        )

    assert response["error"]["code"] == 4002


def test_each_auto_turn_applies_bounded_effort_and_escalates_only() -> None:
    agent = SimpleNamespace(
        model="gpt-5.6-sol",
        provider="openai-codex",
        reasoning_config=None,
        service_tier=None,
        session_id="stored-1",
    )
    session = {
        "agent": agent,
        "session_key": "stored-1",
        "reasoning_mode": "auto",
        "reasoning_floor": None,
        "adaptive_reasoning_decision": None,
        "history": [],
    }

    with patch.object(server, "_load_cfg", return_value=_ENABLED), \
            patch.object(server, "_persist_live_session_runtime"), \
            patch.object(server, "_emit"):
        first = server._prepare_adaptive_reasoning_turn(
            "live-1", session, "Fix this README typo.", []
        )
        second = server._prepare_adaptive_reasoning_turn(
            "live-1", session, "Authentication migration root cause is unknown.", []
        )
        third = server._prepare_adaptive_reasoning_turn(
            "live-1", session, "Fix another typo.", []
        )

    assert first.effort == "low"
    assert second.effort == "high"
    assert third.effort == "high"
    assert session["reasoning_floor"] == "high"
    assert session["create_reasoning_override"] == {"enabled": True, "effort": "high"}
    assert agent.reasoning_config == {"enabled": True, "effort": "high"}
    assert agent.service_tier is None


def test_auto_refuses_a_non_fixed_runtime_instead_of_routing_elsewhere() -> None:
    agent = SimpleNamespace(
        model="spark",
        provider="openrouter",
        reasoning_config=None,
        service_tier="priority",
    )
    session = {
        "agent": agent,
        "reasoning_mode": "auto",
        "reasoning_floor": None,
        "history": [],
    }

    with patch.object(server, "_load_cfg", return_value=_ENABLED):
        with pytest.raises(RuntimeError, match="fixed runtime"):
            server._prepare_adaptive_reasoning_turn("live-1", session, "Do work", [])


def test_adaptive_metadata_round_trips_through_model_config() -> None:
    decision = {
        "provider": "openai-codex",
        "model": "gpt-5.6-sol",
        "effort": "high",
        "work_class": "auth",
        "reason_code": "auth_mutation",
        "policy_version": "gpt56-adaptive-v1",
    }
    session = {
        "reasoning_mode": "auto",
        "reasoning_floor": "high",
        "adaptive_reasoning_decision": decision,
    }
    agent = SimpleNamespace(
        model="gpt-5.6-sol",
        provider="openai-codex",
        base_url="",
        api_mode="codex_app_server",
        reasoning_config={"enabled": True, "effort": "high"},
        service_tier=None,
    )

    model_config = server._runtime_model_config(agent, {}, session)
    restored = server._stored_session_runtime_overrides(
        {"model": "gpt-5.6-sol", "model_config": json.dumps(model_config)}
    )

    assert model_config["adaptive_reasoning"] == {
        "mode": "auto",
        "floor": "high",
        "decision": {
            "effort": "high",
            "work_class": "auth",
            "reason_code": "auth_mutation",
            "policy_version": "gpt56-adaptive-v1",
        },
    }
    assert restored["adaptive_reasoning_state"] == model_config["adaptive_reasoning"]


def test_session_info_exposes_adaptive_source_and_reason() -> None:
    agent = SimpleNamespace(
        reasoning_config={"enabled": True, "effort": "high"},
        service_tier=None,
        model="gpt-5.6-sol",
        provider="openai-codex",
        session_id="stored-1",
        tools=[],
    )
    session = {
        "agent": agent,
        "session_key": "stored-1",
        "cwd": "D:/work",
        "reasoning_mode": "auto",
        "reasoning_floor": "high",
        "adaptive_reasoning_decision": {
            "reason_code": "auth_mutation",
            "policy_version": "gpt56-adaptive-v1",
        },
    }

    with patch.object(server, "_load_cfg", return_value={}), \
            patch.object(server, "_display_session_cwd", return_value="D:/work"), \
            patch.object(server, "_git_branch_for_cwd", return_value="main"), \
            patch.object(server, "_project_info_for_cwd", return_value=None), \
            patch.object(server, "_probe_credentials", return_value=None):
        info = server._session_info(agent, session)

    assert info["reasoning_mode"] == "auto"
    assert info["reasoning_effort"] == "high"
    assert info["reasoning_reason"] == "auth_mutation"
    assert info["adaptive_policy_version"] == "gpt56-adaptive-v1"


def test_config_set_auto_enables_the_fixed_live_session_route() -> None:
    agent = SimpleNamespace(
        model="gpt-5.6-sol",
        provider="openai-codex",
        reasoning_config={"enabled": True, "effort": "high"},
        service_tier="priority",
    )
    session = {
        "agent": agent,
        "reasoning_mode": "manual",
        "reasoning_floor": "high",
        "adaptive_reasoning_decision": {"reason_code": "manual"},
        "create_reasoning_override": {"enabled": True, "effort": "high"},
    }
    server._sessions["live-auto"] = session
    try:
        with patch.object(server, "_load_cfg", return_value=_ENABLED), \
                patch.object(server, "_persist_live_session_runtime"), \
                patch.object(server, "_session_info", return_value={"reasoning_mode": "auto"}), \
                patch.object(server, "_emit"):
            response = server._methods["config.set"](
                "rid",
                {"key": "reasoning", "session_id": "live-auto", "value": "auto"},
            )
    finally:
        server._sessions.pop("live-auto", None)

    assert response["result"]["value"] == "auto"
    assert session["reasoning_mode"] == "auto"
    assert session["reasoning_floor"] is None
    assert session["adaptive_reasoning_decision"] is None
    assert "create_reasoning_override" not in session
    assert agent.reasoning_config is None
    assert agent.service_tier is None
