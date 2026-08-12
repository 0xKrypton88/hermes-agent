"""Tests for the fail-closed Linear Ready review gate (ENG-14 slice).

Pure logic only: no Linear API calls, no persistence, no webhook binding, and
no agent/Go dispatch. Ready review must never start coding work.
"""

from __future__ import annotations

import hashlib
import json
import re

from gateway.linear_ready_review_gate import (
    DECISION_BLOCKED,
    DECISION_READY_FOR_GO,
    LinearIssueSnapshot,
    LinearMutationIntent,
    LinearReadyReviewPolicy,
    assess_linear_ready_review,
    build_review_key,
    plan_linear_mutation,
    should_emit_review,
)


def _complete_snapshot(**overrides) -> LinearIssueSnapshot:
    data = {
        "issue_id": "issue-uuid-ready-001",
        "identifier": "ENG-14",
        "title": "Linear Ready review gate",
        "description": "Normalize the Ready source package before any Go.",
        "acceptance_criteria": (
            "Gate returns READY_FOR_GO only when the source package is complete.",
            "Blocked decisions list every missing requirement.",
        ),
        "repository": "https://github.com/0xKrypton88/hermes-agent.git",
        "target_ref": "integration/eng-14-ci-closure",
        "unresolved_required_inputs": False,
    }
    data.update(overrides)
    return LinearIssueSnapshot(**data)


def _policy(**overrides) -> LinearReadyReviewPolicy:
    data = {
        "ready_for_go_state_id": "state-ready-for-go",
        "blocked_state_id": "state-blocked",
    }
    data.update(overrides)
    return LinearReadyReviewPolicy(**data)


def test_ready_gate_passes_with_complete_source_package():
    snapshot = _complete_snapshot()
    decision = assess_linear_ready_review(snapshot, _policy())

    assert decision.decision == DECISION_READY_FOR_GO
    assert decision.reasons == ()
    assert decision.starts_agent_work is False
    assert decision.source_package is not None
    assert decision.source_package_digest
    assert re.fullmatch(r"[0-9a-f]{64}", decision.source_package_digest)
    assert decision.review_key == build_review_key(
        snapshot.issue_id, decision.source_package_digest
    )
    assert "READY_FOR_GO" in decision.comment_body
    assert decision.source_package_digest in decision.comment_body
    assert "does not start" in decision.comment_body.lower()
    assert "coding" in decision.comment_body.lower() or "agent" in decision.comment_body.lower()


def test_ready_gate_digest_is_sha256_of_canonical_source_package():
    snapshot = _complete_snapshot()
    decision = assess_linear_ready_review(snapshot, _policy())
    assert decision.source_package is not None

    canonical = decision.source_package.to_canonical_dict()
    expected = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
            "utf-8"
        )
    ).hexdigest()
    assert decision.source_package_digest == expected


def test_ready_gate_is_deterministic_for_identical_inputs():
    snapshot = _complete_snapshot()
    policy = _policy()
    first = assess_linear_ready_review(snapshot, policy)
    second = assess_linear_ready_review(snapshot, policy)

    assert first == second
    assert first.comment_body == second.comment_body
    assert first.source_package_digest == second.source_package_digest
    assert first.review_key == second.review_key


def test_ready_gate_blocks_with_individually_named_missing_reasons():
    snapshot = LinearIssueSnapshot(
        issue_id="issue-uuid-incomplete",
        identifier="",
        title="",
        description="",
        acceptance_criteria=(),
        repository="",
        target_ref="",
        unresolved_required_inputs=True,
    )
    decision = assess_linear_ready_review(snapshot, _policy())

    assert decision.decision == DECISION_BLOCKED
    assert decision.starts_agent_work is False
    assert decision.reasons == (
        "missing_issue_identifier",
        "missing_issue_title",
        "missing_issue_description",
        "missing_acceptance_criteria",
        "missing_repository_binding",
        "missing_target_ref",
        "unresolved_required_inputs",
    )
    for reason in decision.reasons:
        assert reason in decision.comment_body
    assert "Ready" in decision.comment_body
    assert "does not start" in decision.comment_body.lower()


def test_ready_gate_reports_only_present_gaps():
    snapshot = _complete_snapshot(target_ref="  ", repository="")
    decision = assess_linear_ready_review(snapshot, _policy())

    assert decision.decision == DECISION_BLOCKED
    assert decision.reasons == (
        "missing_repository_binding",
        "missing_target_ref",
    )
    assert "missing_acceptance_criteria" not in decision.reasons


def test_ready_gate_blocks_when_acceptance_criteria_blank_strings():
    snapshot = _complete_snapshot(acceptance_criteria=("  ", ""))
    decision = assess_linear_ready_review(snapshot, _policy())

    assert decision.decision == DECISION_BLOCKED
    assert "missing_acceptance_criteria" in decision.reasons


def test_ready_gate_blocks_when_canonical_issue_id_missing():
    snapshot = _complete_snapshot(issue_id="  ")
    decision = assess_linear_ready_review(snapshot, _policy())

    assert decision.decision == DECISION_BLOCKED
    assert decision.decision != DECISION_READY_FOR_GO
    assert "missing_issue_id" in decision.reasons
    assert "missing_issue_id" in decision.comment_body
    assert plan_linear_mutation(decision, _policy()) is None


def test_mutation_plan_is_comment_and_transition_only():
    snapshot = _complete_snapshot()
    policy = _policy()
    decision = assess_linear_ready_review(snapshot, policy)
    intent = plan_linear_mutation(decision, policy)

    assert isinstance(intent, LinearMutationIntent)
    assert intent.issue_id == snapshot.issue_id
    assert intent.comment_body == decision.comment_body
    assert intent.target_state_id == policy.ready_for_go_state_id
    assert intent.review_key == decision.review_key
    # Boundary is intentional data only — no provider client fields.
    assert set(intent.__dataclass_fields__) == {
        "issue_id",
        "comment_body",
        "target_state_id",
        "review_key",
    }


def test_mutation_plan_for_blocked_uses_blocked_state():
    snapshot = _complete_snapshot(description="")
    policy = _policy()
    decision = assess_linear_ready_review(snapshot, policy)
    intent = plan_linear_mutation(decision, policy)

    assert decision.decision == DECISION_BLOCKED
    assert intent is not None
    assert intent.target_state_id == policy.blocked_state_id


def test_mutation_plan_fail_closed_without_ready_for_go_state_id():
    snapshot = _complete_snapshot()
    policy = _policy(ready_for_go_state_id="")
    decision = assess_linear_ready_review(snapshot, policy)

    assert decision.decision == DECISION_READY_FOR_GO
    assert plan_linear_mutation(decision, policy) is None


def test_mutation_plan_fail_closed_without_blocked_state_id():
    snapshot = _complete_snapshot(description="")
    policy = _policy(blocked_state_id="   ")
    decision = assess_linear_ready_review(snapshot, policy)

    assert decision.decision == DECISION_BLOCKED
    assert plan_linear_mutation(decision, policy) is None


def test_duplicate_ready_review_key_suppresses_second_mutation():
    snapshot = _complete_snapshot()
    policy = _policy()
    first = assess_linear_ready_review(snapshot, policy)
    second = assess_linear_ready_review(snapshot, policy)

    assert first.review_key == second.review_key
    assert should_emit_review(first.review_key, seen_review_keys=set()) is True
    assert (
        should_emit_review(second.review_key, seen_review_keys={first.review_key})
        is False
    )

    first_intent = plan_linear_mutation(first, policy)
    assert first_intent is not None
    # Boundary-level idempotency: same key => no second comment/transition request.
    assert (
        plan_linear_mutation(
            second,
            policy,
            seen_review_keys={first.review_key},
        )
        is None
    )


def test_policy_mapping_input_is_accepted():
    snapshot = _complete_snapshot()
    decision = assess_linear_ready_review(
        snapshot,
        {
            "ready_for_go_state_id": "state-ready-for-go",
            "blocked_state_id": "state-blocked",
        },
    )
    assert decision.decision == DECISION_READY_FOR_GO
