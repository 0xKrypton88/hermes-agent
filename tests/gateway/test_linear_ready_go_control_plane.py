"""RED→GREEN tests for the durable Linear Ready/Go control plane.

Covers freeze evaluation, profile-local persistence across reconstruction,
duplicate/race-safe inserts, mismatched/missing/tampered provenance, invalid
input, and static no-execution boundaries. Never dispatches work.
"""

from __future__ import annotations

import ast
import hashlib
import importlib
import inspect
import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from gateway.linear_go_launch_plan import LaunchIntent, NormalizedGoTransition
from gateway.linear_ready_freeze import (
    DECISION_BLOCKED,
    DECISION_READY_FOR_GO,
    ReadyFreezeReceipt,
    ReadySourceInput,
    digest_frozen_source,
    evaluate_ready_freeze,
    freeze_source_package,
)
from gateway.linear_ready_go_control import (
    GoControlResult,
    LinearReadyGoControlPlane,
    process_go,
    process_ready,
)
from gateway.linear_ready_go_store import LinearReadyGoStore


ISSUE_ID = "issue-uuid-ready-go-001"
ISSUE_IDENTIFIER = "ENG-14"
GO_EVENT_KEY = "svix_msg_go_delivery_plane_1"

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

CONTROL_PLANE_MODULES = (
    "gateway/linear_ready_freeze.py",
    "gateway/linear_ready_go_store.py",
    "gateway/linear_ready_go_control.py",
)

ACTIVATION_GATED_MODULES = (
    "gateway.linear_ready_go_live_ids",
    "gateway.linear_ready_go_adapter",
    "gateway.linear_ready_go_pilot",
)


def _complete_source(**overrides) -> ReadySourceInput:
    data = dict(
        issue_id=ISSUE_ID,
        issue_identifier=ISSUE_IDENTIFIER,
        title="Durable Ready/Go control plane",
        description="Freeze Ready provenance and plan non-dispatched Go intents.",
        acceptance_criteria=(
            "READY_FOR_GO only when the frozen source package is complete.",
            "Go creates exactly one LaunchIntent(dispatched=False).",
        ),
        repository="https://github.com/0xKrypton88/hermes-agent.git",
        target_ref="cursor/durable-ready-go-plane-52df",
        unresolved_required_inputs=False,
    )
    data.update(overrides)
    return ReadySourceInput(**data)


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


def _store(tmp_path: Path) -> LinearReadyGoStore:
    return LinearReadyGoStore(db_path=tmp_path / "linear_ready_go.db")


def test_freeze_ready_for_go_receipt_never_starts_work():
    receipt = evaluate_ready_freeze(_complete_source())
    assert isinstance(receipt, ReadyFreezeReceipt)
    assert receipt.decision == DECISION_READY_FOR_GO
    assert receipt.reasons == ()
    assert receipt.starts_agent_work is False
    assert receipt.source_digest == digest_frozen_source(receipt.frozen_source)
    assert receipt.review_key == f"{ISSUE_ID}:{receipt.source_digest}"


def test_freeze_digest_is_deterministic_sha256():
    source = _complete_source()
    first = evaluate_ready_freeze(source)
    second = evaluate_ready_freeze(source)
    package = freeze_source_package(source)
    expected = hashlib.sha256(
        json.dumps(
            package.to_canonical_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()
    assert first.source_digest == expected
    assert first == second


def test_freeze_blocks_missing_and_invalid_input():
    blocked = evaluate_ready_freeze(
        _complete_source(
            issue_id="  ",
            title="",
            acceptance_criteria=(),
            unresolved_required_inputs=True,
        )
    )
    assert blocked.decision == DECISION_BLOCKED
    assert blocked.starts_agent_work is False
    assert "missing_issue_id" in blocked.reasons
    assert "missing_issue_title" in blocked.reasons
    assert "missing_acceptance_criteria" in blocked.reasons
    assert "unresolved_required_inputs" in blocked.reasons

    invalid = evaluate_ready_freeze(None)
    assert invalid.decision == DECISION_BLOCKED
    assert "invalid_source_input" in invalid.reasons


def test_freeze_rejects_review_identity_mismatch():
    receipt = evaluate_ready_freeze(
        _complete_source(),
        {"issue_id": "other-issue", "issue_identifier": ISSUE_IDENTIFIER},
    )
    assert receipt.decision == DECISION_BLOCKED
    assert "issue_identity_mismatch" in receipt.reasons


def test_freeze_rejects_noncanonical_and_unknown_team_identities():
    padded = evaluate_ready_freeze(_complete_source(issue_id=" issue-uuid-ready-go-001 "))
    assert padded.decision == DECISION_BLOCKED
    assert "noncanonical_issue_id" in padded.reasons

    weird = evaluate_ready_freeze(_complete_source(issue_identifier="ENG 14"))
    assert weird.decision == DECISION_BLOCKED
    assert "noncanonical_issue_identifier" in weird.reasons

    unknown_team = evaluate_ready_freeze(
        _complete_source(team_key="OPS", allowed_team_keys=("ENG",))
    )
    assert unknown_team.decision == DECISION_BLOCKED
    assert "unknown_team_key" in unknown_team.reasons

    cross = evaluate_ready_freeze(
        _complete_source(team_key="ENG", allowed_team_keys=("ENG",)),
        {"team_key": "OPS"},
    )
    assert cross.decision == DECISION_BLOCKED
    assert "cross_team_mismatch" in cross.reasons


def test_ready_persistence_survives_store_reconstruction(tmp_path):
    store = _store(tmp_path)
    plane = LinearReadyGoControlPlane(store=store)
    result = plane.process_ready(_complete_source())
    assert result.ok is True
    assert result.status == "created"
    assert result.record is not None
    assert result.record.decision == DECISION_READY_FOR_GO
    assert result.record.starts_agent_work is False

    rebuilt = LinearReadyGoStore(db_path=store.db_path)
    loaded = rebuilt.get_ready_provenance(issue_id=ISSUE_ID)
    assert loaded is not None
    assert loaded.review_key == result.record.review_key
    assert loaded.source_digest == result.record.source_digest
    assert loaded.decision == DECISION_READY_FOR_GO
    assert rebuilt.count_ready_reviews() == 1


def test_duplicate_ready_delivery_is_noop(tmp_path):
    store = _store(tmp_path)
    first = process_ready(_complete_source(), store=store)
    second = process_ready(_complete_source(), store=store)
    assert first.status == "created"
    assert second.status == "duplicate"
    assert second.record is not None
    assert second.record.review_key == first.record.review_key
    assert store.count_ready_reviews() == 1


def test_go_creates_exactly_one_non_dispatched_intent(tmp_path):
    store = _store(tmp_path)
    ready = process_ready(_complete_source(), store=store)
    assert ready.ok is True

    go = process_go(
        _valid_transition(),
        store=store,
        review_key=ready.receipt.review_key,
        source_digest=ready.receipt.source_digest,
    )
    assert isinstance(go, GoControlResult)
    assert go.ok is True
    assert go.status == "created"
    assert isinstance(go.intent, LaunchIntent)
    assert go.intent.dispatched is False
    assert go.record is not None
    assert go.record.dispatched is False
    assert store.count_launch_intents() == 1

    rebuilt = LinearReadyGoStore(db_path=store.db_path)
    loaded = rebuilt.get_launch_intent_by_event_key(GO_EVENT_KEY)
    assert loaded is not None
    assert loaded.dispatched is False
    assert loaded.source_digest == ready.receipt.source_digest


def test_duplicate_go_delivery_and_intent_are_safe_noops(tmp_path):
    store = _store(tmp_path)
    ready = process_ready(_complete_source(), store=store)
    first = process_go(
        _valid_transition(),
        store=store,
        review_key=ready.receipt.review_key,
        source_digest=ready.receipt.source_digest,
    )
    assert first.ok is True

    dup_delivery = process_go(
        _valid_transition(),
        store=store,
        review_key=ready.receipt.review_key,
        source_digest=ready.receipt.source_digest,
    )
    assert dup_delivery.ok is False
    assert dup_delivery.status == "duplicate"
    assert "duplicate_delivery_key" in dup_delivery.reason_codes
    assert store.count_launch_intents() == 1

    # Store-level duplicate_intent_key: same idempotency_key, different event key.
    dup_intent = store.record_launch_intent(
        issue_id=first.intent.issue_id,
        issue_identifier=first.intent.issue_identifier,
        review_key=first.intent.review_key,
        source_digest=first.intent.source_digest,
        go_event_key="svix_msg_go_delivery_plane_2",
        idempotency_key=first.intent.idempotency_key,
        dispatched=False,
    )
    assert dup_intent.status == "duplicate"
    assert dup_intent.reason == "duplicate_intent_key"
    assert store.count_launch_intents() == 1


def test_go_fails_closed_without_ready_provenance(tmp_path):
    store = _store(tmp_path)
    # Selectors required even when no Ready row exists — use syntactically valid ones.
    orphan_digest = "c" * 64
    result = process_go(
        _valid_transition(),
        store=store,
        review_key=f"{ISSUE_ID}:{orphan_digest}",
        source_digest=orphan_digest,
    )
    assert result.ok is False
    assert result.status == "rejected"
    assert "missing_ready_provenance" in result.reason_codes
    assert store.count_launch_intents() == 0


def test_go_requires_exact_review_key_and_source_digest(tmp_path):
    """Go must never authorize from latest READY when selectors are omitted."""
    store = _store(tmp_path)
    ready = process_ready(_complete_source(), store=store)
    assert ready.ok is True
    assert store.count_ready_reviews() == 1

    omitted_both = process_go(_valid_transition(), store=store)
    assert omitted_both.ok is False
    assert omitted_both.status == "rejected"
    assert "missing_review_key" in omitted_both.reason_codes
    assert "missing_source_digest" in omitted_both.reason_codes
    assert store.count_launch_intents() == 0

    omitted_key = process_go(
        _valid_transition(go_event_key="svix_msg_omit_key"),
        store=store,
        source_digest=ready.receipt.source_digest,
    )
    assert omitted_key.ok is False
    assert omitted_key.status == "rejected"
    assert "missing_review_key" in omitted_key.reason_codes
    assert store.count_launch_intents() == 0

    omitted_digest = process_go(
        _valid_transition(go_event_key="svix_msg_omit_digest"),
        store=store,
        review_key=ready.receipt.review_key,
    )
    assert omitted_digest.ok is False
    assert omitted_digest.status == "rejected"
    assert "missing_source_digest" in omitted_digest.reason_codes
    assert store.count_launch_intents() == 0

    blank_both = process_go(
        _valid_transition(go_event_key="svix_msg_blank_selectors"),
        store=store,
        review_key="   ",
        source_digest="",
    )
    assert blank_both.ok is False
    assert "missing_review_key" in blank_both.reason_codes
    assert "missing_source_digest" in blank_both.reason_codes
    assert store.count_launch_intents() == 0

    wrong_key = process_go(
        _valid_transition(go_event_key="svix_msg_wrong_key"),
        store=store,
        review_key=f"{ISSUE_ID}:{'d' * 64}",
        source_digest=ready.receipt.source_digest,
    )
    assert wrong_key.ok is False
    assert wrong_key.status == "rejected"
    assert "stale_ready_provenance" in wrong_key.reason_codes
    assert store.count_launch_intents() == 0

    wrong_digest = process_go(
        _valid_transition(go_event_key="svix_msg_wrong_digest"),
        store=store,
        review_key=ready.receipt.review_key,
        source_digest="e" * 64,
    )
    assert wrong_digest.ok is False
    assert wrong_digest.status == "rejected"
    assert "ready_provenance_digest_mismatch" in wrong_digest.reason_codes
    assert store.count_launch_intents() == 0


def test_go_fails_closed_on_digest_mismatch_and_tamper(tmp_path):
    store = _store(tmp_path)
    ready = process_ready(_complete_source(), store=store)
    bad_digest = "a" * 64
    result = process_go(
        _valid_transition(),
        store=store,
        review_key=ready.receipt.review_key,
        source_digest=bad_digest,
    )
    assert result.ok is False
    assert result.status == "rejected"
    assert (
        "ready_provenance_digest_mismatch" in result.reason_codes
        or "missing_ready_provenance" in result.reason_codes
        or "stale_ready_provenance" in result.reason_codes
    )
    assert store.count_launch_intents() == 0

    # Tamper: blocked review cannot authorize Go.
    blocked_source = _complete_source(
        title="",
        description="incomplete",
    )
    blocked = evaluate_ready_freeze(blocked_source)
    assert blocked.decision == DECISION_BLOCKED
    store.record_ready_review(
        issue_id=ISSUE_ID,
        issue_identifier=ISSUE_IDENTIFIER,
        review_key=f"{ISSUE_ID}-blocked-key",
        source_digest=blocked.source_digest,
        decision=DECISION_BLOCKED,
        frozen_source=blocked.frozen_source,
    )
    # Looking up without requiring ready-for-go still won't authorize process_go.
    rejected = process_go(
        _valid_transition(go_event_key="svix_msg_blocked_attempt"),
        store=store,
        review_key=f"{ISSUE_ID}-blocked-key",
        source_digest=blocked.source_digest,
    )
    assert rejected.ok is False
    assert "ready_decision_not_ready_for_go" in rejected.reason_codes
    assert store.count_launch_intents() == 0


def test_go_fails_closed_on_stale_provenance(tmp_path):
    store = _store(tmp_path)
    ready = process_ready(_complete_source(), store=store)
    stale = process_go(
        _valid_transition(go_event_key="svix_msg_stale"),
        store=store,
        review_key=f"{ISSUE_ID}:{'b' * 64}",
        source_digest=ready.receipt.source_digest,
    )
    assert stale.ok is False
    assert stale.status == "rejected"
    assert "stale_ready_provenance" in stale.reason_codes
    assert store.count_launch_intents() == 0


def test_go_rejects_invalid_and_unknown_transitions(tmp_path):
    store = _store(tmp_path)
    ready = process_ready(_complete_source(), store=store)
    result = process_go(
        _valid_transition(target_state="In Progress", go_event_key="svix_msg_bad"),
        store=store,
        review_key=ready.receipt.review_key,
        source_digest=ready.receipt.source_digest,
    )
    assert result.ok is False
    assert result.status == "rejected"
    assert "non_go_target_state" in result.reason_codes

    unknown = process_go(
        _valid_transition(target_state="TotallyUnknown", go_event_key="svix_msg_unknown"),
        store=store,
        review_key=ready.receipt.review_key,
        source_digest=ready.receipt.source_digest,
    )
    assert unknown.ok is False
    assert "non_go_target_state" in unknown.reason_codes

    malformed = process_go(None, store=store)
    assert malformed.ok is False
    assert "missing_go_transition" in malformed.reason_codes
    assert store.count_launch_intents() == 0


def test_go_rejects_cross_team_transition(tmp_path):
    store = _store(tmp_path)
    ready = process_ready(
        _complete_source(team_key="ENG", allowed_team_keys=("ENG",)),
        store=store,
    )
    assert ready.ok is True
    rejected = process_go(
        _valid_transition(go_event_key="svix_msg_cross_team"),
        store=store,
        review_key=ready.receipt.review_key,
        source_digest=ready.receipt.source_digest,
        team_key="OPS",
    )
    assert rejected.ok is False
    assert "cross_team_mismatch" in rejected.reason_codes
    assert store.count_launch_intents() == 0


def test_store_rejects_dispatched_true(tmp_path):
    store = _store(tmp_path)
    ready = process_ready(_complete_source(), store=store)
    with pytest.raises(ValueError, match="dispatched"):
        store.record_launch_intent(
            issue_id=ISSUE_ID,
            issue_identifier=ISSUE_IDENTIFIER,
            review_key=ready.receipt.review_key,
            source_digest=ready.receipt.source_digest,
            go_event_key=GO_EVENT_KEY,
            idempotency_key="go_launch:x",
            dispatched=True,
        )


def test_concurrent_duplicate_go_inserts_are_race_safe(tmp_path):
    store = _store(tmp_path)
    ready = process_ready(_complete_source(), store=store)
    barrier = threading.Barrier(8)
    results: list[GoControlResult] = []
    lock = threading.Lock()

    def _worker():
        barrier.wait(timeout=5)
        outcome = process_go(
            _valid_transition(),
            store=store,
            review_key=ready.receipt.review_key,
            source_digest=ready.receipt.source_digest,
        )
        with lock:
            results.append(outcome)

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(_worker) for _ in range(8)]
        for fut in futures:
            fut.result(timeout=10)

    created = [r for r in results if r.status == "created"]
    duplicates = [r for r in results if r.status == "duplicate"]
    assert len(created) == 1
    assert len(duplicates) == 7
    assert store.count_launch_intents() == 1
    assert all(r.record is None or r.record.dispatched is False for r in results)


@pytest.mark.parametrize("relpath", CONTROL_PLANE_MODULES)
def test_control_plane_modules_have_no_execution_imports_or_calls(relpath):
    module_path = Path(__file__).resolve().parents[2] / relpath
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
                assert fragment not in name, f"forbidden call in {relpath}: {name}"

    assert imported.isdisjoint(FORBIDDEN_IMPORT_ROOTS), (relpath, imported)

    # Loaded modules must not bind execution helpers.
    mod_name = relpath.replace("/", ".").removesuffix(".py")
    mod = importlib.import_module(mod_name)
    assert not hasattr(mod, "handle_message")
    assert not any(
        name.startswith(("subprocess", "aiohttp", "langgraph", "httpx"))
        for name in vars(mod)
    )


def test_freeze_module_exports_pure_evaluator():
    mod = importlib.import_module("gateway.linear_ready_freeze")
    assert inspect.isfunction(mod.evaluate_ready_freeze)
    assert mod.evaluate_ready_freeze.__module__ == "gateway.linear_ready_freeze"


def test_control_plane_does_not_touch_webhook_receipt_path(tmp_path, monkeypatch):
    """Ready/Go plane must not invoke webhook receipt intake or handle_message."""
    calls: list[str] = []

    def _boom(*_a, **_k):
        calls.append("handle_message")
        raise AssertionError("handle_message must not be called")

    monkeypatch.setattr(
        "gateway.platforms.webhook.WebhookAdapter.handle_message",
        _boom,
        raising=False,
    )
    store = _store(tmp_path)
    ready = process_ready(_complete_source(), store=store)
    go = process_go(
        _valid_transition(),
        store=store,
        review_key=ready.receipt.review_key,
        source_digest=ready.receipt.source_digest,
    )
    assert ready.ok and go.ok
    assert calls == []


def test_storage_failure_fails_closed(tmp_path, monkeypatch):
    store = _store(tmp_path)

    def _boom(*_a, **_k):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(store, "record_ready_review", _boom)
    ready = process_ready(_complete_source(), store=store)
    assert ready.ok is False
    assert "storage_failure" in ready.reason_codes

    # Restore a working store, then break Go persistence.
    good = _store(tmp_path / "go-fail")
    ready_ok = process_ready(_complete_source(), store=good)
    assert ready_ok.ok is True
    monkeypatch.setattr(
        good,
        "record_launch_intent",
        lambda **_kwargs: (_ for _ in ()).throw(
            sqlite3.OperationalError("disk I/O error")
        ),
    )
    go = process_go(
        _valid_transition(go_event_key="svix_msg_storage_fail"),
        store=good,
        review_key=ready_ok.receipt.review_key,
        source_digest=ready_ok.receipt.source_digest,
    )
    assert go.ok is False
    assert go.status == "rejected"
    assert "storage_failure" in go.reason_codes
    assert good.count_launch_intents() == 0


def test_activation_gated_packages_remain_unimplemented():
    live_ids = importlib.import_module("gateway.linear_ready_go_live_ids")
    adapter = importlib.import_module("gateway.linear_ready_go_adapter")
    pilot = importlib.import_module("gateway.linear_ready_go_pilot")
    with pytest.raises(live_ids.ActivationGatedError):
        live_ids.bind_live_linear_ids()
    with pytest.raises(adapter.ActivationGatedError):
        adapter.connect_linear_adapter()
    with pytest.raises(adapter.ActivationGatedError):
        adapter.apply_ready_mutation()
    with pytest.raises(pilot.ActivationGatedError):
        pilot.start_pilot_dispatch()
    with pytest.raises(pilot.ActivationGatedError):
        pilot.arm_pilot_listener()

    # Control-plane modules must not import activation-gated packages.
    forbidden_modules = set(ACTIVATION_GATED_MODULES)
    for relpath in CONTROL_PLANE_MODULES:
        module_path = Path(__file__).resolve().parents[2] / relpath
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                assert node.module not in forbidden_modules
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert alias.name not in forbidden_modules
