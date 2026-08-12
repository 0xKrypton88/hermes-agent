"""RED→GREEN tests for explicit-Go launch planning (plan-only).

The planner accepts a normalized Go transition plus Ready-review provenance and
returns a non-dispatched LaunchIntent. It must never dispatch work or touch
network/Linear/Cursor/LangGraph/subprocess/handle_message.
"""

from __future__ import annotations

import ast
import hashlib
import importlib
import inspect
from pathlib import Path

import pytest

from gateway.linear_go_launch_plan import (
    LaunchIntent,
    LaunchPlanResult,
    NormalizedGoTransition,
    ReadyReviewProvenance,
    plan_explicit_go_launch,
)


ISSUE_ID = "issue-uuid-go-001"
ISSUE_IDENTIFIER = "ENG-14"
REVIEW_KEY = "ready-review-key-001"
GO_EVENT_KEY = "svix_msg_go_delivery_1"
SOURCE_DIGEST = hashlib.sha256(b"frozen-ready-review-source").hexdigest()

FORBIDDEN_IMPORT_ROOTS = frozenset(
    {
        "subprocess",
        "socket",
        "httpx",
        "requests",
        "aiohttp",
        "urllib",
        "langgraph",
        "cursor",
        "linear",
    }
)
FORBIDDEN_NAME_FRAGMENTS = frozenset(
    {
        "handle_message",
        "subprocess",
        "Popen",
        "langgraph",
        "Cursor",
        "LinearClient",
    }
)


def _valid_transition(**overrides) -> NormalizedGoTransition:
    data = dict(
        issue_id=ISSUE_ID,
        issue_identifier=ISSUE_IDENTIFIER,
        target_state="Go",
        previous_state="Ready",
        go_event_key=GO_EVENT_KEY,
    )
    data.update(overrides)
    return NormalizedGoTransition(**data)


def _valid_provenance(**overrides) -> ReadyReviewProvenance:
    data = dict(
        issue_id=ISSUE_ID,
        review_key=REVIEW_KEY,
        source_digest=SOURCE_DIGEST,
        decision="READY_FOR_GO",
        starts_agent_work=False,
    )
    data.update(overrides)
    return ReadyReviewProvenance(**data)


def test_success_returns_non_dispatched_immutable_launch_intent():
    result = plan_explicit_go_launch(
        _valid_transition(),
        _valid_provenance(),
        seen_delivery_keys=frozenset(),
        seen_intent_keys=frozenset(),
    )

    assert isinstance(result, LaunchPlanResult)
    assert result.ok is True
    assert result.reason_codes == ()
    intent = result.intent
    assert isinstance(intent, LaunchIntent)
    assert intent.issue_id == ISSUE_ID
    assert intent.issue_identifier == ISSUE_IDENTIFIER
    assert intent.review_key == REVIEW_KEY
    assert intent.source_digest == SOURCE_DIGEST
    assert intent.go_event_key == GO_EVENT_KEY
    assert intent.dispatched is False
    assert intent.idempotency_key
    assert intent.idempotency_key == (
        f"go_launch:{ISSUE_ID}:{REVIEW_KEY}:{SOURCE_DIGEST}:{GO_EVENT_KEY}"
    )
    with pytest.raises(Exception):
        intent.dispatched = True  # type: ignore[misc]


@pytest.mark.parametrize(
    ("transition_overrides", "provenance", "expected_code"),
    [
        ({"target_state": "In Progress"}, _valid_provenance(), "non_go_target_state"),
        ({"target_state": ""}, _valid_provenance(), "missing_go_target_state"),
        ({"target_state": "   "}, _valid_provenance(), "missing_go_target_state"),
        ({"issue_id": ""}, _valid_provenance(), "blank_issue_id"),
        ({"issue_id": "   "}, _valid_provenance(), "blank_issue_id"),
        (
            {"previous_state": None},
            _valid_provenance(),
            "missing_state_transition",
        ),
        (
            {"previous_state": ""},
            _valid_provenance(),
            "missing_state_transition",
        ),
        (
            {"previous_state": "Go"},
            _valid_provenance(),
            "noop_duplicate_go_transition",
        ),
        (
            {},
            None,
            "missing_ready_provenance",
        ),
    ],
    ids=[
        "non_go_target",
        "empty_go_target",
        "blank_go_target",
        "empty_issue_id",
        "blank_issue_id",
        "absent_previous_state",
        "blank_previous_state",
        "duplicate_noop_go",
        "missing_provenance",
    ],
)
def test_fail_closed_categories(transition_overrides, provenance, expected_code):
    result = plan_explicit_go_launch(
        _valid_transition(**transition_overrides),
        provenance,
    )

    assert result.ok is False
    assert result.intent is None
    assert expected_code in result.reason_codes


@pytest.mark.parametrize(
    ("prov_overrides", "expected_code"),
    [
        ({"issue_id": "other-issue"}, "ready_provenance_issue_mismatch"),
        ({"review_key": ""}, "blank_review_key"),
        ({"review_key": "   "}, "blank_review_key"),
        ({"source_digest": "ABC123"}, "invalid_source_digest"),
        ({"source_digest": "a" * 63}, "invalid_source_digest"),
        ({"source_digest": "A" * 64}, "invalid_source_digest"),
        ({"source_digest": "g" * 64}, "invalid_source_digest"),
        ({"decision": "NOT_READY"}, "ready_decision_not_ready_for_go"),
        ({"starts_agent_work": True}, "ready_starts_agent_work"),
    ],
    ids=[
        "issue_mismatch",
        "empty_review_key",
        "blank_review_key",
        "short_digest",
        "wrong_len_digest",
        "uppercase_digest",
        "non_hex_digest",
        "wrong_decision",
        "starts_agent_work_true",
    ],
)
def test_provenance_mismatch_fail_closed(prov_overrides, expected_code):
    result = plan_explicit_go_launch(
        _valid_transition(),
        _valid_provenance(**prov_overrides),
    )
    assert result.ok is False
    assert result.intent is None
    assert expected_code in result.reason_codes


def test_duplicate_delivery_key_returns_no_intent():
    intent_key = f"go_launch:{ISSUE_ID}:{REVIEW_KEY}:{SOURCE_DIGEST}:{GO_EVENT_KEY}"
    result = plan_explicit_go_launch(
        _valid_transition(),
        _valid_provenance(),
        seen_delivery_keys=frozenset({GO_EVENT_KEY}),
        seen_intent_keys=frozenset(),
    )
    assert result.ok is False
    assert result.intent is None
    assert "duplicate_delivery_key" in result.reason_codes
    assert intent_key  # deterministic form documented above


def test_duplicate_intent_key_returns_no_intent():
    intent_key = f"go_launch:{ISSUE_ID}:{REVIEW_KEY}:{SOURCE_DIGEST}:{GO_EVENT_KEY}"
    result = plan_explicit_go_launch(
        _valid_transition(),
        _valid_provenance(),
        seen_delivery_keys=frozenset(),
        seen_intent_keys=frozenset({intent_key}),
    )
    assert result.ok is False
    assert result.intent is None
    assert "duplicate_intent_key" in result.reason_codes


def test_module_has_no_execution_imports_or_calls():
    module_path = (
        Path(__file__).resolve().parents[2] / "gateway" / "linear_go_launch_plan.py"
    )
    tree = ast.parse(module_path.read_text(encoding="utf-8"))

    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
        elif isinstance(node, ast.Call):
            name = ast.unparse(node.func)
            for fragment in FORBIDDEN_NAME_FRAGMENTS:
                assert fragment not in name, f"forbidden call: {name}"

    assert imported.isdisjoint(FORBIDDEN_IMPORT_ROOTS), imported
    assert imported <= {"__future__", "re", "dataclasses", "typing"}

    mod = importlib.import_module("gateway.linear_go_launch_plan")
    # Loaded module must expose the pure planner and not bind execution helpers.
    assert hasattr(mod, "plan_explicit_go_launch")
    assert not hasattr(mod, "handle_message")
    assert not any(
        name.startswith(("subprocess", "aiohttp", "langgraph", "httpx"))
        for name in vars(mod)
    )
    assert inspect.isfunction(mod.plan_explicit_go_launch)
