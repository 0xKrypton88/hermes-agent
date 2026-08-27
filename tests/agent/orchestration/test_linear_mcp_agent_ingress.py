from __future__ import annotations

import json

import pytest

from agent.orchestration.linear_mcp_projection import (
    LinearMCPProjectionBinding,
    LinearMCPProjectionConfig,
)
from agent.orchestration.production_handoff_composition import (
    LiveAdapterUnavailable,
    LiveEffectAuthority,
    ProductionRequestAuthority,
)
from run_agent import AIAgent


def _binding(*, enabled=True, mode="live", session_id="session-128"):
    return LinearMCPProjectionBinding(
        LinearMCPProjectionConfig(enabled, mode),
        ProductionRequestAuthority("request-128", session_id, True),
        LiveEffectAuthority("request-128", session_id, "go-128", True),
    )


def _fake_agent():
    agent = AIAgent.__new__(AIAgent)
    agent.session_id = "session-128"
    agent.calls = []
    agent.comments = []

    def invoke(name, arguments, task_id, *args, **kwargs):
        agent.calls.append((name, dict(arguments), task_id))
        if name == "mcp__linear__list_comments":
            payload = {
                "comments": list(agent.comments),
                "hasNextPage": False,
                "cursor": None,
            }
        elif name == "mcp__linear__save_comment":
            agent.comments.append({"body": arguments["body"]})
            payload = {"id": "comment-1"}
        else:  # pragma: no cover - makes any widened ingress fail loudly
            raise AssertionError(name)
        return json.dumps({"result": json.dumps(payload)})

    agent._invoke_tool = invoke
    return agent


def test_agent_ingress_routes_projection_through_request_bound_invoke_tool():
    agent = _fake_agent()
    owner = agent.attach_linear_mcp_projection(_binding())

    owner.upsert_handoff(
        issue="ENG-128", canonical="provider bytes", idempotency_key="a" * 64
    )
    assert owner.read_handoff(issue="ENG-128", idempotency_key="a" * 64) == "provider bytes"
    assert [call[0] for call in agent.calls] == [
        "mcp__linear__list_comments",
        "mcp__linear__save_comment",
        "mcp__linear__list_comments",
        "mcp__linear__list_comments",
    ]
    assert {call[2] for call in agent.calls} == {"request-128"}


def test_agent_ingress_is_default_off_and_zero_touch():
    agent = _fake_agent()

    with pytest.raises(LiveAdapterUnavailable, match="default-off"):
        agent.attach_linear_mcp_projection(
            LinearMCPProjectionBinding(
                LinearMCPProjectionConfig(),
                ProductionRequestAuthority("request-128", "session-128", True),
                LiveEffectAuthority("request-128", "session-128", "go-128", True),
            )
        )
    assert agent.calls == []
    assert not hasattr(agent, "_linear_mcp_projection_owner")


def test_agent_ingress_rejects_wrong_session_and_duplicate_attachment_without_calls():
    agent = _fake_agent()
    with pytest.raises(LiveAdapterUnavailable, match="active session"):
        agent.attach_linear_mcp_projection(_binding(session_id="other-session"))
    assert agent.calls == []

    owner = agent.attach_linear_mcp_projection(_binding())
    assert owner.started
    with pytest.raises(LiveAdapterUnavailable, match="already attached"):
        agent.attach_linear_mcp_projection(_binding())
    assert agent.calls == []


def test_agent_ingress_rejects_session_drift_before_invoke_tool():
    agent = _fake_agent()
    owner = agent.attach_linear_mcp_projection(_binding())
    agent.session_id = "rotated-session"

    with pytest.raises(LiveAdapterUnavailable, match="escaped its request binding"):
        owner.read_handoff(issue="ENG-128", idempotency_key="a" * 64)
    assert agent.calls == []
