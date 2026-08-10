"""Focused tests for the Cursor Cloud bridge contract."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from tools import cursor_cloud_bridge as bridge


@pytest.fixture
def hermes_home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("CURSOR_API_KEY", "test-cursor-key")
    return home


def _mock_response(status_code: int, payload: dict | None = None, text: str = ""):
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text or (json.dumps(payload) if payload is not None else "")
    if payload is None:
        resp.json.side_effect = ValueError("no json")
    else:
        resp.json.return_value = payload
    return resp


# ---------------------------------------------------------------------------
# Request payload contracts
# ---------------------------------------------------------------------------

def test_named_environment_create_payload_uses_cloud_env_type_and_name():
    payload = bridge.build_create_agent_payload(
        prompt="Implement the bridge",
        repository_url="https://github.com/org/repo",
        starting_ref="main",
        environment_name="hermes-coding",
        name="Bridge job",
    )
    assert payload == {
        "prompt": {"text": "Implement the bridge"},
        "name": "Bridge job",
        "env": {"type": "cloud", "name": "hermes-coding"},
    }
    assert "repos" not in payload


def test_github_ref_create_payload_preserves_repository_url_and_starting_ref():
    payload = bridge.build_create_agent_payload(
        prompt="Fix the bug",
        repository_url="https://github.com/org/repo",
        starting_ref="fix/cursor-feedback-delivery-92dd",
    )
    assert payload["repos"] == [
        {
            "url": "https://github.com/org/repo",
            "startingRef": "fix/cursor-feedback-delivery-92dd",
        }
    ]
    assert "env" not in payload


def test_map_create_agent_result_never_hides_url():
    mapped = bridge.map_create_agent_result(
        {
            "agent": {
                "id": "bc-11111111-1111-1111-1111-111111111111",
                "status": "ACTIVE",
                "url": "https://cursor.com/agents/bc-11111111-1111-1111-1111-111111111111",
                "latestRunId": "run-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            },
            "run": {
                "id": "run-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "status": "CREATING",
            },
        }
    )
    assert mapped["cursor_agent_id"] == "bc-11111111-1111-1111-1111-111111111111"
    assert mapped["cursor_run_id"] == "run-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    assert (
        mapped["cursor_agent_url"]
        == "https://cursor.com/agents/bc-11111111-1111-1111-1111-111111111111"
    )
    assert mapped["path"] == "create_agent"


def test_create_new_agent_with_named_environment_persists_ids_and_url(hermes_home):
    create_payload_resp = {
        "agent": {
            "id": "bc-22222222-2222-2222-2222-222222222222",
            "status": "ACTIVE",
            "url": "https://cursor.com/agents/bc-22222222-2222-2222-2222-222222222222",
            "latestRunId": "run-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        },
        "run": {
            "id": "run-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "status": "CREATING",
        },
    }
    client = MagicMock()
    client.create_agent.return_value = _mock_response(200, create_payload_resp)

    raw = bridge.cursor_cloud_dispatch(
        "Ship the feature",
        environment_name="prod-env",
        hermes_job_id="job-1",
        hermes_session_id="sess-1",
        hermes_thread_id="thread-1",
        client=client,
    )
    result = json.loads(raw)

    assert result["success"] is True
    assert result["path"] == "create_agent"
    assert result["cursor_agent_id"] == "bc-22222222-2222-2222-2222-222222222222"
    assert result["cursor_run_id"] == "run-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    assert (
        result["cursor_agent_url"]
        == "https://cursor.com/agents/bc-22222222-2222-2222-2222-222222222222"
    )
    assert result["request_payload"]["env"] == {"type": "cloud", "name": "prod-env"}
    client.create_agent.assert_called_once()
    sent = client.create_agent.call_args.args[0]
    assert sent["env"] == {"type": "cloud", "name": "prod-env"}

    record = bridge.get_dispatch_record(result["dispatch_id"])
    assert record is not None
    assert record["hermes_job_id"] == "job-1"
    assert record["hermes_session_id"] == "sess-1"
    assert record["hermes_thread_id"] == "thread-1"
    assert record["cursor_agent_id"] == result["cursor_agent_id"]
    assert record["cursor_run_id"] == result["cursor_run_id"]
    assert record["cursor_agent_url"] == result["cursor_agent_url"]
    assert record["phase"] == bridge.PHASE_RUNNING
    assert record["next_action"] == bridge.NEXT_POLL


# ---------------------------------------------------------------------------
# Existing-agent follow-up + agent_busy
# ---------------------------------------------------------------------------

def test_existing_agent_followup_gets_then_posts_run(hermes_home):
    agent_id = "bc-33333333-3333-3333-3333-333333333333"
    client = MagicMock()
    client.get_agent.return_value = _mock_response(
        200,
        {
            "id": agent_id,
            "status": "ACTIVE",
            "url": f"https://cursor.com/agents/{agent_id}",
            "latestRunId": "run-old",
        },
    )
    # Busy probe: previous run already finished.
    client.get_run.return_value = _mock_response(
        200,
        {"id": "run-old", "agentId": agent_id, "status": "FINISHED"},
    )
    client.create_run.return_value = _mock_response(
        200,
        {
            "run": {
                "id": "run-cccccccc-cccc-cccc-cccc-cccccccccccc",
                "agentId": agent_id,
                "status": "CREATING",
            }
        },
    )

    raw = bridge.cursor_cloud_dispatch(
        "Continue the work",
        existing_agent_id=agent_id,
        hermes_job_id="job-continue",
        client=client,
    )
    result = json.loads(raw)

    assert result["success"] is True
    assert result["path"] == "existing_agent"
    assert result["cursor_agent_id"] == agent_id
    assert result["cursor_run_id"] == "run-cccccccc-cccc-cccc-cccc-cccccccccccc"
    assert result["cursor_agent_url"] == f"https://cursor.com/agents/{agent_id}"
    client.get_agent.assert_called_once_with(agent_id)
    client.create_run.assert_called_once()
    assert client.create_run.call_args.args[0] == agent_id
    assert client.create_run.call_args.args[1] == {
        "prompt": {"text": "Continue the work"}
    }


def test_agent_busy_returns_typed_non_user_actionable_state(hermes_home):
    agent_id = "bc-44444444-4444-4444-4444-444444444444"
    client = MagicMock()
    client.get_agent.return_value = _mock_response(
        200,
        {
            "id": agent_id,
            "status": "ACTIVE",
            "url": f"https://cursor.com/agents/{agent_id}",
            "latestRunId": "run-active",
        },
    )
    client.get_run.return_value = _mock_response(
        200,
        {"id": "run-active", "agentId": agent_id, "status": "RUNNING"},
    )

    raw = bridge.cursor_cloud_dispatch(
        "Try again while busy",
        existing_agent_id=agent_id,
        client=client,
    )
    result = json.loads(raw)

    assert result["success"] is False
    assert result["error_type"] == "agent_busy"
    assert result["user_actionable"] is False
    assert result["manual_action_required"] is False
    assert result["next_action"] == bridge.NEXT_WAIT
    assert result["cursor_agent_url"] == f"https://cursor.com/agents/{agent_id}"
    # Must not instruct the operator to click Cursor UI.
    blob = json.dumps(result).lower()
    assert "click" not in blob
    assert "open cursor" not in blob
    assert "dashboard" not in blob
    client.create_run.assert_not_called()


def test_agent_busy_from_create_run_409(hermes_home):
    agent_id = "bc-55555555-5555-5555-5555-555555555555"
    client = MagicMock()
    client.get_agent.return_value = _mock_response(
        200,
        {
            "id": agent_id,
            "status": "ACTIVE",
            "url": f"https://cursor.com/agents/{agent_id}",
            "latestRunId": "",
        },
    )
    client.create_run.return_value = _mock_response(
        409,
        {"error": {"code": "agent_busy", "message": "agent_busy"}},
    )

    raw = bridge.cursor_cloud_dispatch(
        "Follow up",
        existing_agent_id=agent_id,
        client=client,
    )
    result = json.loads(raw)
    assert result["error_type"] == "agent_busy"
    assert result["user_actionable"] is False
    assert result["manual_action_required"] is False
    assert "click" not in result["error"].lower()


# ---------------------------------------------------------------------------
# No-manual-action error contract
# ---------------------------------------------------------------------------

def test_missing_api_key_is_typed_technical_block(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    with patch.object(bridge, "_get_cursor_api_key", return_value=""):
        raw = bridge.cursor_cloud_dispatch(
            "Anything",
            repository_url="https://github.com/org/repo",
        )
    result = json.loads(raw)
    assert result["error_type"] == "bridge_unavailable"
    assert result["user_actionable"] is False
    assert result["manual_action_required"] is False
    lowered = result["error"].lower()
    assert "click" not in lowered
    assert "open cursor" not in lowered
    assert "dashboard" not in lowered


def test_local_origin_is_typed_technical_block_not_ui_instruction(hermes_home):
    raw = bridge.cursor_cloud_dispatch(
        "Work locally",
        repository_url="/home/ubuntu/project",
    )
    result = json.loads(raw)
    assert result["error_type"] == "local_origin"
    assert result["user_actionable"] is False
    assert result["manual_action_required"] is False
    blob = json.dumps(result).lower()
    assert "click" not in blob
    assert "open the agents window" not in blob


def test_preflight_error_strips_manual_ui_copy(hermes_home):
    client = MagicMock()
    client.create_agent.return_value = _mock_response(
        500,
        {
            "error": {
                "code": "internal",
                "message": "Open Cursor dashboard and click Start Agent manually",
            }
        },
    )
    raw = bridge.cursor_cloud_dispatch(
        "Fail preflight",
        environment_name="env-x",
        client=client,
    )
    result = json.loads(raw)
    assert result["error_type"] == "preflight_error"
    assert result["user_actionable"] is False
    assert result["manual_action_required"] is False
    lowered = result["error"].lower()
    assert "click" not in lowered
    assert "dashboard" not in lowered


# ---------------------------------------------------------------------------
# Same-job completion resume
# ---------------------------------------------------------------------------

def test_completion_resumes_same_job_not_fresh_chat(hermes_home):
    record = bridge.create_dispatch_record(
        hermes_job_id="job-9",
        hermes_session_id="sess-9",
        hermes_thread_id="thread-9",
        prompt_text="original",
    )
    bridge.update_dispatch_record(
        record["dispatch_id"],
        cursor_agent_id="bc-99999999-9999-9999-9999-999999999999",
        cursor_run_id="run-99999999-9999-9999-9999-999999999999",
        cursor_agent_url="https://cursor.com/agents/bc-99999999-9999-9999-9999-999999999999",
        phase=bridge.PHASE_RUNNING,
        next_action=bridge.NEXT_POLL,
        status="running",
    )
    record = bridge.get_dispatch_record(record["dispatch_id"])

    with patch("tools.process_registry.process_registry") as pr:
        event = bridge.publish_completion_for_resume(record, result="done")
        pr.completion_queue.put.assert_called_once()
        queued = pr.completion_queue.put.call_args.args[0]

    assert event["resume_same_job"] is True
    assert event["fresh_chat"] is False
    assert queued["type"] == "cursor_cloud"
    assert queued["hermes_job_id"] == "job-9"
    assert queued["cursor_agent_url"].startswith("https://cursor.com/agents/")

    text = bridge.format_cursor_cloud_completion(queued)
    assert "Resume the SAME job" in text
    assert "fresh chat" in text.lower()
    assert "job-9" in text
    assert queued["cursor_agent_url"] in text


def test_status_finished_publishes_same_job_resume(hermes_home):
    record = bridge.create_dispatch_record(
        hermes_job_id="job-finish",
        hermes_session_id="sess-finish",
    )
    agent_id = "bc-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    run_id = "run-ffffffff-ffff-ffff-ffff-ffffffffffff"
    bridge.update_dispatch_record(
        record["dispatch_id"],
        cursor_agent_id=agent_id,
        cursor_run_id=run_id,
        cursor_agent_url=f"https://cursor.com/agents/{agent_id}",
        status="running",
        phase=bridge.PHASE_RUNNING,
        next_action=bridge.NEXT_POLL,
    )
    client = MagicMock()
    client.get_run.return_value = _mock_response(
        200,
        {
            "id": run_id,
            "agentId": agent_id,
            "status": "FINISHED",
            "result": "all good",
        },
    )

    with patch.object(bridge, "publish_completion_for_resume") as pub:
        pub.side_effect = lambda rec, result=None: {
            "type": "cursor_cloud",
            "resume_same_job": True,
            "fresh_chat": False,
            "result": result,
        }
        raw = bridge.cursor_cloud_status(
            dispatch_id=record["dispatch_id"],
            client=client,
        )
    result = json.loads(raw)
    assert result["success"] is True
    assert result["phase"] == bridge.PHASE_COMPLETED
    assert result["next_action"] == bridge.NEXT_RESUME
    assert result["resume_same_job"] is True
    assert result["fresh_chat"] is False
    assert result["cursor_agent_url"] == f"https://cursor.com/agents/{agent_id}"
    pub.assert_called_once()


def test_technical_block_contract_fields():
    raw = bridge.technical_block("preflight_error", "something failed", status_code=502)
    result = json.loads(raw)
    assert result["error"] == "something failed"
    assert result["error_type"] == "preflight_error"
    assert result["user_actionable"] is False
    assert result["manual_action_required"] is False
    assert result["success"] is False
    assert result["status_code"] == 502
