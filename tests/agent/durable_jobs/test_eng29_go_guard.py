"""ENG-29 mandatory Go guard (isolated, default-off).

Local policy-contract evidence only. No Slack/live authorization, no
gateway wiring, no deploy/restart adapters.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pytest

MANDATORY_CATEGORIES = (
    "scope_change",
    "missing_prerequisites",
    "unresolved_provider_ambiguity",
    "deploy",
    "restart",
    "cutover",
    "production_migration",
    "external_promotion_release",
    "financial_action",
)

TUPLE_MISMATCH_FIELDS = (
    "source_package_id",
    "source_package_version",
    "candidate_sha",
    "candidate_id",
    "candidate_version",
    "target_environment",
    "target_action",
    "actor_id",
    "policy_version",
    "matrix_version",
)


def _db(tmp_path: Path) -> Path:
    return tmp_path / "pilot_jobs.sqlite"


def _make_job(tmp_path: Path, *, idempotency_key: str = "idem-eng29"):
    from agent.durable_jobs.store import DurableJobStore

    store = DurableJobStore(sqlite_path=_db(tmp_path))
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="ENG-29 go guard",
        repository_identity="github.com/example/repo",
        frozen_baseline_sha="sha-eng29-test",
        idempotency_key=idempotency_key,
    )
    return store, job


def _bind_and_policy(
    store,
    job,
    *,
    actor: str = "U-alice",
    policy_version: str = "pol-1",
    root_thread_ts: str = "111.222",
):
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.slack_contract import SlackBindingLedger

    SlackBindingLedger(sqlite_path=store.sqlite_path).bind(
        job_id=job.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts=root_thread_ts,
        candidate_id="cand-1",
        candidate_version="v1",
    )
    decisions = DecisionLedger(sqlite_path=store.sqlite_path)
    decisions.set_policy(
        job_id=job.job_id,
        policy_version=policy_version,
        allowed_actors=(actor,),
        expires_at="2099-01-01T00:00:00+00:00",
    )
    return decisions


@dataclass
class FakeRun:
    run_id: str
    idempotency_key: str


@dataclass
class FakeCreateResult:
    kind: str
    run: Optional[FakeRun] = None


class CountingProvider:
    def __init__(self, *, create_kind: str = "accepted") -> None:
        self.create_kind = create_kind
        self.create_calls: list[str] = []
        self.lookup_calls: list[str] = []

    def create_run(self, *, idempotency_key: str, job_id: str) -> FakeCreateResult:
        self.create_calls.append(idempotency_key)
        if self.create_kind != "accepted":
            return FakeCreateResult(kind=self.create_kind)
        return FakeCreateResult(kind="accepted", run=FakeRun("run-1", idempotency_key))

    def lookup_runs(self, *, idempotency_key: str) -> list[FakeRun]:
        self.lookup_calls.append(idempotency_key)
        return []


@dataclass
class FakePostResult:
    kind: str
    message_ts: Optional[str] = None


class CountingPort:
    def __init__(self, *, post_kind: str = "accepted") -> None:
        self.post_kind = post_kind
        self.posts: list[str] = []
        self.lookup_calls: list[str] = []

    def post_root(self, **kwargs) -> FakePostResult:
        self.posts.append(kwargs["client_msg_id"])
        if self.post_kind != "accepted":
            return FakePostResult(kind=self.post_kind)
        return FakePostResult(kind="accepted", message_ts="42.0")

    def lookup_by_client_msg_id(self, client_msg_id: str) -> list:
        self.lookup_calls.append(client_msg_id)
        return []


def _provider_kwargs(job):
    return dict(
        job_id=job.job_id,
        action_id="create_run",
        origin_platform=job.origin_platform,
        origin_chat_id=job.origin_chat_id,
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )


def _payload_identity(job, *, candidate_id: str = "cand-1", candidate_version: str = "v1"):
    return dict(
        source_package_id=job.repository_identity,
        source_package_version=candidate_version,
        candidate_sha=job.frozen_baseline_sha,
        candidate_id=candidate_id,
        candidate_version=candidate_version,
        target_environment=job.origin_platform,
    )


def _default_tuple_kwargs(job, **overrides):
    from agent.durable_jobs.eng29 import (
        MATRIX_VERSION,
        PROVIDER_CREATE_TARGET_ACTION,
    )

    ident = _payload_identity(job)
    base = dict(
        job_id=job.job_id,
        source_package_id=ident["source_package_id"],
        source_package_version=ident["source_package_version"],
        candidate_sha=ident["candidate_sha"],
        candidate_id=ident["candidate_id"],
        candidate_version=ident["candidate_version"],
        target_environment=ident["target_environment"],
        target_action=PROVIDER_CREATE_TARGET_ACTION,
        authorized_actor="U-alice",
        expires_at="2099-01-01T00:00:00+00:00",
        policy_version="pol-1",
        matrix_version=MATRIX_VERSION,
        authorization_idempotency_key=f"tuple:{job.job_id}:default",
    )
    base.update(overrides)
    return base


def _guard_kwargs(job, **overrides):
    from agent.durable_jobs.eng29 import (
        MATRIX_VERSION,
        PROVIDER_CREATE_TARGET_ACTION,
    )

    ident = _payload_identity(job)
    base = dict(
        job_id=job.job_id,
        source_package_id=ident["source_package_id"],
        source_package_version=ident["source_package_version"],
        candidate_sha=ident["candidate_sha"],
        candidate_id=ident["candidate_id"],
        candidate_version=ident["candidate_version"],
        target_environment=ident["target_environment"],
        target_action=PROVIDER_CREATE_TARGET_ACTION,
        actor_id="U-alice",
        policy_version="pol-1",
        matrix_version=MATRIX_VERSION,
        now_iso="2026-01-01T00:00:00+00:00",
    )
    base.update(overrides)
    return base


def _seed_authorized_go_despite_binding(store, job, *, target_action: str):
    """Tuple + accepted Go for cand-1/v1 even when the Slack binding differs.

    DecisionLedger correctly refuses to record that Go against a mismatched
    binding. This helper only plants the authorized tuple/Go so the adapter
    guard can be shown not to replay it onto a different binding candidate.
    """
    import sqlite3
    import uuid

    from agent.durable_jobs.eng29 import register_authorization_tuple

    tuple_kwargs = _default_tuple_kwargs(
        job,
        target_action=target_action,
        candidate_id="cand-1",
        candidate_version="v1",
        authorization_idempotency_key=f"tuple:{job.job_id}:{target_action}",
    )
    registered = register_authorization_tuple(
        sqlite_path=store.sqlite_path, **tuple_kwargs
    )
    assert registered.ok is True
    conn = sqlite3.connect(store.sqlite_path)
    try:
        conn.execute(
            """
            INSERT INTO job_decisions(
                decision_id, job_id, decision_type, candidate_id,
                candidate_version, actor_id, policy_version,
                decision_idempotency_key, status, reason_codes_json,
                created_at, source_package_id, source_package_version,
                candidate_sha, target_environment, target_action,
                matrix_version
            ) VALUES (?, ?, 'go', 'cand-1', 'v1', ?, ?, ?, 'accepted', '[]',
                      ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"dd_{uuid.uuid4().hex}",
                job.job_id,
                tuple_kwargs["authorized_actor"],
                tuple_kwargs["policy_version"],
                f"go:{job.job_id}:{target_action}",
                "2026-01-01T00:00:00+00:00",
                tuple_kwargs["source_package_id"],
                tuple_kwargs["source_package_version"],
                tuple_kwargs["candidate_sha"],
                tuple_kwargs["target_environment"],
                target_action,
                tuple_kwargs["matrix_version"],
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _register_and_go(store, job, decisions, *, target_action: str, **tuple_overrides):
    from agent.durable_jobs.eng29 import register_authorization_tuple

    tuple_kwargs = _default_tuple_kwargs(
        job,
        target_action=target_action,
        authorization_idempotency_key=f"tuple:{job.job_id}:{target_action}",
        **tuple_overrides,
    )
    registered = register_authorization_tuple(
        sqlite_path=store.sqlite_path, **tuple_kwargs
    )
    assert registered.ok is True
    go = decisions.record_decision(
        job_id=job.job_id,
        decision_type="go",
        candidate_id=tuple_kwargs["candidate_id"],
        candidate_version=tuple_kwargs["candidate_version"],
        actor_id=tuple_kwargs["authorized_actor"],
        policy_version=tuple_kwargs["policy_version"],
        decision_idempotency_key=f"go:{job.job_id}:{target_action}",
        source_package_id=tuple_kwargs["source_package_id"],
        source_package_version=tuple_kwargs["source_package_version"],
        candidate_sha=tuple_kwargs["candidate_sha"],
        target_environment=tuple_kwargs["target_environment"],
        target_action=tuple_kwargs["target_action"],
        matrix_version=tuple_kwargs["matrix_version"],
    )
    assert go.ok is True
    return registered, go


def _restore_live_policy(
    store,
    job,
    *,
    actor: str = "U-alice",
    policy_version: str = "pol-1",
    expires_at: str = "2099-01-01T00:00:00+00:00",
):
    from agent.durable_jobs.decisions import DecisionLedger

    DecisionLedger(sqlite_path=store.sqlite_path).set_policy(
        job_id=job.job_id,
        policy_version=policy_version,
        allowed_actors=(actor,),
        expires_at=expires_at,
    )


def _apply_live_policy_defect(store, job, defect: str) -> None:
    import sqlite3

    from agent.durable_jobs.decisions import DecisionLedger

    decisions = DecisionLedger(sqlite_path=store.sqlite_path)
    if defect == "absent":
        conn = sqlite3.connect(store.sqlite_path)
        try:
            conn.execute(
                "DELETE FROM job_authz_policies WHERE job_id = ?", (job.job_id,)
            )
            conn.commit()
        finally:
            conn.close()
        return
    if defect == "expired":
        decisions.set_policy(
            job_id=job.job_id,
            policy_version="pol-1",
            allowed_actors=("U-alice",),
            expires_at="2020-01-01T00:00:00+00:00",
        )
        return
    if defect == "revoked":
        decisions.set_policy(
            job_id=job.job_id,
            policy_version="pol-1",
            allowed_actors=(),
            expires_at="2099-01-01T00:00:00+00:00",
        )
        return
    if defect == "inactive":
        conn = sqlite3.connect(store.sqlite_path)
        try:
            conn.execute(
                """
                UPDATE job_authz_policies
                   SET allowed_actors_json = '[""]'
                 WHERE job_id = ?
                """,
                (job.job_id,),
            )
            conn.commit()
        finally:
            conn.close()
        return
    if defect == "actor_mismatch":
        decisions.set_policy(
            job_id=job.job_id,
            policy_version="pol-1",
            allowed_actors=("U-bob",),
            expires_at="2099-01-01T00:00:00+00:00",
        )
        return
    if defect == "policy_version_mismatch":
        decisions.set_policy(
            job_id=job.job_id,
            policy_version="pol-2",
            allowed_actors=("U-alice",),
            expires_at="2099-01-01T00:00:00+00:00",
        )
        return

    conn = sqlite3.connect(store.sqlite_path)
    try:
        if defect == "malformed":
            conn.execute(
                """
                UPDATE job_authz_policies
                   SET allowed_actors_json = 'not-json'
                 WHERE job_id = ?
                """,
                (job.job_id,),
            )
        elif defect == "missing_expires_at":
            conn.execute(
                "UPDATE job_authz_policies SET expires_at = NULL WHERE job_id = ?",
                (job.job_id,),
            )
        elif defect == "malformed_expires_at":
            conn.execute(
                """
                UPDATE job_authz_policies
                   SET expires_at = 'not-a-timestamp'
                 WHERE job_id = ?
                """,
                (job.job_id,),
            )
        else:
            raise AssertionError(f"unknown live policy defect: {defect}")
        conn.commit()
    finally:
        conn.close()


def _effect_event_types(store, job_id: str) -> list[str]:
    return [
        event["event_type"]
        for event in store.list_events(job_id)
        if event["event_type"].startswith(("provider_effect_", "slack_root_"))
    ]


# ---------------------------------------------------------------------------
# Adapter zero-effect without Go (RED on 5a1b6ff uses existing APIs only)
# ---------------------------------------------------------------------------


def test_provider_create_without_go_is_zero_claim_and_zero_call_out(tmp_path):
    from agent.durable_jobs.effects import ProviderEffectLedger, reconcile_cursor_create

    store, job = _make_job(tmp_path, idempotency_key="idem-eng29-nogo-provider")
    _bind_and_policy(store, job)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    provider = CountingProvider()
    caught: list[BaseException] = []
    try:
        reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    except BaseException as exc:  # noqa: BLE001
        caught.append(exc)

    assert provider.create_calls == [], "provider create executed without accepted Go"
    assert provider.lookup_calls == [], "provider lookup executed without accepted Go"
    assert ledger.get_claim(job.job_id, "create_run") is None
    from agent.durable_jobs.eng29 import AuthorizationDenied

    assert caught and isinstance(caught[0], AuthorizationDenied)


def test_slack_post_without_go_is_zero_claim_and_zero_call_out(tmp_path):
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-eng29-nogo-slack")
    _bind_and_policy(store, job)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    port = CountingPort()
    caught: list[BaseException] = []
    try:
        deliver_slack_root(ledger, port, job_id=job.job_id)
    except BaseException as exc:  # noqa: BLE001
        caught.append(exc)

    assert port.posts == [], "slack post executed without accepted Go"
    assert port.lookup_calls == [], "slack lookup executed without accepted Go"
    persisted = ledger.get_binding(job.job_id)
    assert persisted is not None
    assert persisted.status is SlackRootStatus.BOUND
    from agent.durable_jobs.eng29 import AuthorizationDenied

    assert caught and isinstance(caught[0], AuthorizationDenied)


# ---------------------------------------------------------------------------
# Classifier / matrix
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("category", MANDATORY_CATEGORIES)
def test_mandatory_categories_require_go(category):
    from agent.durable_jobs.eng29 import classify_target_action

    classified = classify_target_action(category)
    assert classified.require_go is True
    assert classified.category == category


@pytest.mark.parametrize("action", ("", "not_a_real_action", "unknown", "something_else"))
def test_unknown_unclassified_actions_default_deny_require_go(action):
    from agent.durable_jobs.eng29 import classify_target_action

    classified = classify_target_action(action)
    assert classified.require_go is True
    assert classified.category == "unknown"


def test_adapter_target_actions_are_explicit_and_require_go():
    from agent.durable_jobs.eng29 import (
        PROVIDER_CREATE_TARGET_ACTION,
        SLACK_POST_ROOT_TARGET_ACTION,
        classify_target_action,
    )

    provider = classify_target_action(PROVIDER_CREATE_TARGET_ACTION)
    slack = classify_target_action(SLACK_POST_ROOT_TARGET_ACTION)
    assert provider.require_go is True
    assert slack.require_go is True
    assert provider.action == PROVIDER_CREATE_TARGET_ACTION
    assert slack.action == SLACK_POST_ROOT_TARGET_ACTION


def test_matrix_version_is_pinned():
    from agent.durable_jobs.eng29 import MATRIX_VERSION

    assert MATRIX_VERSION == "eng29-matrix-v1"


# ---------------------------------------------------------------------------
# Guard: exact Go success + mismatches + expiry + hold/cancel
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("category", MANDATORY_CATEGORIES)
def test_guard_no_go_denies_every_mandatory_category(tmp_path, category):
    from agent.durable_jobs.eng29 import evaluate_authorization

    store, job = _make_job(tmp_path, idempotency_key=f"idem-eng29-nogo-{category}")
    _bind_and_policy(store, job)
    result = evaluate_authorization(
        sqlite_path=store.sqlite_path,
        **_guard_kwargs(job, target_action=category),
    )
    assert result.ok is False
    assert "no_go" in result.reason_codes or "unauthorized" in result.reason_codes


@pytest.mark.parametrize("category", MANDATORY_CATEGORIES)
def test_guard_exact_matching_unexpired_go_allows_category(tmp_path, category):
    from agent.durable_jobs.eng29 import evaluate_authorization

    store, job = _make_job(tmp_path, idempotency_key=f"idem-eng29-go-{category}")
    decisions = _bind_and_policy(store, job)
    extra = {}
    if category == "missing_prerequisites":
        extra["prerequisites_satisfied"] = True
    if category == "unresolved_provider_ambiguity":
        extra["provider_ambiguity_resolved"] = True
    _register_and_go(store, job, decisions, target_action=category, **extra)
    result = evaluate_authorization(
        sqlite_path=store.sqlite_path,
        **_guard_kwargs(job, target_action=category),
    )
    assert result.ok is True
    assert result.reason_codes == ()


@pytest.mark.parametrize("field", TUPLE_MISMATCH_FIELDS)
def test_guard_rejects_every_tuple_field_mismatch(tmp_path, field):
    from agent.durable_jobs.eng29 import (
        MATRIX_VERSION,
        PROVIDER_CREATE_TARGET_ACTION,
        evaluate_authorization,
    )

    store, job = _make_job(tmp_path, idempotency_key=f"idem-eng29-mismatch-{field}")
    decisions = _bind_and_policy(store, job)
    _register_and_go(
        store, job, decisions, target_action=PROVIDER_CREATE_TARGET_ACTION
    )
    wrong = {
        "source_package_id": "other-package",
        "source_package_version": "other-ver",
        "candidate_sha": "sha-other",
        "candidate_id": "cand-NOT-AUTHORIZED",
        "candidate_version": "v999",
        "target_environment": "prod",
        "target_action": "deploy",
        "actor_id": "U-eve",
        "policy_version": "pol-other",
        "matrix_version": MATRIX_VERSION + "-other",
    }
    result = evaluate_authorization(
        sqlite_path=store.sqlite_path,
        **_guard_kwargs(job, **{field: wrong[field]}),
    )
    assert result.ok is False
    assert "mismatch" in result.reason_codes or "unauthorized" in result.reason_codes


def test_guard_rejects_expired_tuple(tmp_path):
    from agent.durable_jobs.eng29 import (
        PROVIDER_CREATE_TARGET_ACTION,
        evaluate_authorization,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-eng29-expired")
    decisions = _bind_and_policy(store, job)
    _register_and_go(
        store,
        job,
        decisions,
        target_action=PROVIDER_CREATE_TARGET_ACTION,
        expires_at="2026-01-01T00:00:00+00:00",
    )
    result = evaluate_authorization(
        sqlite_path=store.sqlite_path,
        **_guard_kwargs(job, now_iso="2026-01-02T00:00:00+00:00"),
    )
    assert result.ok is False
    assert "expired" in result.reason_codes


def test_guard_hold_fail_closed(tmp_path):
    from agent.durable_jobs.eng29 import (
        PROVIDER_CREATE_TARGET_ACTION,
        evaluate_authorization,
        register_authorization_tuple,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-eng29-hold")
    decisions = _bind_and_policy(store, job)
    tuple_kwargs = _default_tuple_kwargs(
        job, target_action=PROVIDER_CREATE_TARGET_ACTION
    )
    assert register_authorization_tuple(
        sqlite_path=store.sqlite_path, **tuple_kwargs
    ).ok
    hold = decisions.record_decision(
        job_id=job.job_id,
        decision_type="hold",
        candidate_id="cand-1",
        candidate_version="v1",
        actor_id="U-alice",
        policy_version="pol-1",
        decision_idempotency_key="k-hold",
        source_package_id=tuple_kwargs["source_package_id"],
        source_package_version=tuple_kwargs["source_package_version"],
        candidate_sha=tuple_kwargs["candidate_sha"],
        target_environment=tuple_kwargs["target_environment"],
        target_action=tuple_kwargs["target_action"],
        matrix_version=tuple_kwargs["matrix_version"],
    )
    assert hold.ok is True
    result = evaluate_authorization(
        sqlite_path=store.sqlite_path, **_guard_kwargs(job),
    )
    assert result.ok is False
    assert "hold" in result.reason_codes


def test_guard_cancel_fail_closed_and_later_go_rejected(tmp_path):
    from agent.durable_jobs.eng29 import (
        PROVIDER_CREATE_TARGET_ACTION,
        evaluate_authorization,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-eng29-cancel")
    decisions = _bind_and_policy(store, job)
    _register_and_go(
        store, job, decisions, target_action=PROVIDER_CREATE_TARGET_ACTION
    )
    canceled = decisions.record_decision(
        job_id=job.job_id,
        decision_type="cancel",
        candidate_id="cand-1",
        candidate_version="v1",
        actor_id="U-alice",
        policy_version="pol-1",
        decision_idempotency_key="k-cancel",
    )
    assert canceled.ok is True
    result = evaluate_authorization(
        sqlite_path=store.sqlite_path, **_guard_kwargs(job),
    )
    assert result.ok is False
    assert "canceled" in result.reason_codes
    go_after = decisions.record_decision(
        job_id=job.job_id,
        decision_type="go",
        candidate_id="cand-1",
        candidate_version="v1",
        actor_id="U-alice",
        policy_version="pol-1",
        decision_idempotency_key="k-go-after-cancel",
    )
    assert go_after.ok is False
    assert "canceled" in go_after.reason_codes


# ---------------------------------------------------------------------------
# Tuple registration: replay, immutability, flags, reopen
# ---------------------------------------------------------------------------


def test_tuple_replay_conflict_and_exact_duplicate(tmp_path):
    from agent.durable_jobs.eng29 import register_authorization_tuple

    store, job = _make_job(tmp_path, idempotency_key="idem-eng29-replay")
    _bind_and_policy(store, job)
    first = register_authorization_tuple(
        sqlite_path=store.sqlite_path, **_default_tuple_kwargs(job)
    )
    assert first.ok is True
    dup = register_authorization_tuple(
        sqlite_path=store.sqlite_path, **_default_tuple_kwargs(job)
    )
    assert dup.ok is True
    assert dup.status == "duplicate"
    conflict = register_authorization_tuple(
        sqlite_path=store.sqlite_path,
        **_default_tuple_kwargs(job, candidate_sha="sha-other"),
    )
    assert conflict.ok is False
    assert "replayed" in conflict.reason_codes


def test_immutable_policy_and_matrix_re_registration_rejected(tmp_path):
    from agent.durable_jobs.eng29 import (
        MATRIX_VERSION,
        register_authorization_tuple,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-eng29-immutable")
    _bind_and_policy(store, job)
    assert register_authorization_tuple(
        sqlite_path=store.sqlite_path, **_default_tuple_kwargs(job)
    ).ok
    policy_change = register_authorization_tuple(
        sqlite_path=store.sqlite_path,
        **_default_tuple_kwargs(
            job,
            policy_version="pol-2",
            authorization_idempotency_key=f"tuple:{job.job_id}:pol2",
        ),
    )
    assert policy_change.ok is False
    matrix_change = register_authorization_tuple(
        sqlite_path=store.sqlite_path,
        **_default_tuple_kwargs(
            job,
            matrix_version=MATRIX_VERSION + "-2",
            authorization_idempotency_key=f"tuple:{job.job_id}:mx2",
        ),
    )
    assert matrix_change.ok is False
    unknown_matrix = register_authorization_tuple(
        sqlite_path=store.sqlite_path,
        **_default_tuple_kwargs(
            job,
            target_action="deploy",
            matrix_version="not-the-pinned-matrix",
            authorization_idempotency_key=f"tuple:{job.job_id}:bad-mx",
        ),
    )
    assert unknown_matrix.ok is False


def test_missing_prerequisites_fail_closed_until_flag_set(tmp_path):
    from agent.durable_jobs.eng29 import evaluate_authorization

    store, job = _make_job(tmp_path, idempotency_key="idem-eng29-prereq")
    decisions = _bind_and_policy(store, job)
    _register_and_go(
        store,
        job,
        decisions,
        target_action="missing_prerequisites",
        prerequisites_satisfied=False,
    )
    denied = evaluate_authorization(
        sqlite_path=store.sqlite_path,
        **_guard_kwargs(job, target_action="missing_prerequisites"),
    )
    assert denied.ok is False
    assert "missing_prerequisites" in denied.reason_codes

    store2, job2 = _make_job(tmp_path, idempotency_key="idem-eng29-prereq-ok")
    decisions2 = _bind_and_policy(store2, job2, root_thread_ts="111.223")
    _register_and_go(
        store2,
        job2,
        decisions2,
        target_action="missing_prerequisites",
        prerequisites_satisfied=True,
    )
    allowed = evaluate_authorization(
        sqlite_path=store2.sqlite_path,
        **_guard_kwargs(job2, target_action="missing_prerequisites"),
    )
    assert allowed.ok is True


def test_unresolved_provider_ambiguity_fail_closed_until_resolved(tmp_path):
    from agent.durable_jobs.eng29 import evaluate_authorization

    store, job = _make_job(tmp_path, idempotency_key="idem-eng29-ambig")
    decisions = _bind_and_policy(store, job)
    _register_and_go(
        store,
        job,
        decisions,
        target_action="unresolved_provider_ambiguity",
        provider_ambiguity_resolved=False,
    )
    denied = evaluate_authorization(
        sqlite_path=store.sqlite_path,
        **_guard_kwargs(job, target_action="unresolved_provider_ambiguity"),
    )
    assert denied.ok is False
    assert "unresolved_provider_ambiguity" in denied.reason_codes

    store2, job2 = _make_job(tmp_path, idempotency_key="idem-eng29-ambig-ok")
    decisions2 = _bind_and_policy(store2, job2, root_thread_ts="111.223")
    _register_and_go(
        store2,
        job2,
        decisions2,
        target_action="unresolved_provider_ambiguity",
        provider_ambiguity_resolved=True,
    )
    allowed = evaluate_authorization(
        sqlite_path=store2.sqlite_path,
        **_guard_kwargs(
            job2, target_action="unresolved_provider_ambiguity"
        ),
    )
    assert allowed.ok is True


def test_authorization_tuple_and_go_reopen_persistence(tmp_path):
    from agent.durable_jobs.decisions import DecisionLedger, DecisionType
    from agent.durable_jobs.eng29 import (
        PROVIDER_CREATE_TARGET_ACTION,
        evaluate_authorization,
        get_authorization_tuple,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-eng29-reopen")
    decisions = _bind_and_policy(store, job)
    _register_and_go(
        store, job, decisions, target_action=PROVIDER_CREATE_TARGET_ACTION
    )
    reopened_tuple = get_authorization_tuple(
        store.sqlite_path, job.job_id, PROVIDER_CREATE_TARGET_ACTION
    )
    assert reopened_tuple is not None
    assert reopened_tuple.target_action == PROVIDER_CREATE_TARGET_ACTION
    reopened_decisions = DecisionLedger(sqlite_path=store.sqlite_path)
    latest = reopened_decisions.latest_accepted(job.job_id)
    assert latest is not None
    assert latest.decision_type is DecisionType.GO
    result = evaluate_authorization(
        sqlite_path=store.sqlite_path, **_guard_kwargs(job),
    )
    assert result.ok is True


# ---------------------------------------------------------------------------
# Exact Go allows adapter call-out; mismatch still zero-effect
# ---------------------------------------------------------------------------


def test_provider_create_with_exact_go_succeeds(tmp_path):
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        reconcile_cursor_create,
    )
    from agent.durable_jobs.eng29 import PROVIDER_CREATE_TARGET_ACTION

    store, job = _make_job(tmp_path, idempotency_key="idem-eng29-provider-go")
    decisions = _bind_and_policy(store, job)
    _register_and_go(
        store, job, decisions, target_action=PROVIDER_CREATE_TARGET_ACTION
    )
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    provider = CountingProvider()
    result = reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    assert result.status is EffectStatus.ACCEPTED
    assert provider.create_calls
    assert ledger.get_claim(job.job_id, "create_run") is not None


def test_slack_post_with_exact_go_succeeds(tmp_path):
    from agent.durable_jobs.eng29 import SLACK_POST_ROOT_TARGET_ACTION
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-eng29-slack-go")
    decisions = _bind_and_policy(store, job)
    _register_and_go(
        store, job, decisions, target_action=SLACK_POST_ROOT_TARGET_ACTION
    )
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    port = CountingPort()
    result = deliver_slack_root(ledger, port, job_id=job.job_id)
    assert result.status is SlackRootStatus.DELIVERED
    assert port.posts


def test_provider_create_rejects_unauthorized_candidate_identity(tmp_path):
    """Cross-candidate replay: default Go must not authorize a different candidate."""
    from agent.durable_jobs.effects import ProviderEffectLedger, reconcile_cursor_create
    from agent.durable_jobs.eng29 import AuthorizationDenied
    from tests.agent.durable_jobs.authz_fixtures import (
        install_default_adapter_authorization,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-eng29-cand-replay")
    _bind_and_policy(store, job)
    install_default_adapter_authorization(store.sqlite_path, job.job_id)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    provider = CountingProvider()
    caught: list[BaseException] = []
    try:
        reconcile_cursor_create(
            ledger,
            provider,
            **{
                **_provider_kwargs(job),
                "candidate_id": "cand-NOT-AUTHORIZED",
                "candidate_version": "v999",
            },
        )
    except BaseException as exc:  # noqa: BLE001
        caught.append(exc)

    assert provider.create_calls == [], (
        "provider create executed for unauthorized candidate"
    )
    assert provider.lookup_calls == [], (
        "provider lookup executed for unauthorized candidate"
    )
    assert ledger.get_claim(job.job_id, "create_run") is None
    assert caught and isinstance(caught[0], AuthorizationDenied)


def test_provider_create_rejects_blank_candidate_identity(tmp_path):
    """Missing candidate/version must default-deny before claim or create."""
    from agent.durable_jobs.effects import ProviderEffectLedger, reconcile_cursor_create
    from agent.durable_jobs.eng29 import (
        AuthorizationDenied,
        PROVIDER_CREATE_TARGET_ACTION,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-eng29-cand-blank")
    decisions = _bind_and_policy(store, job)
    _register_and_go(
        store, job, decisions, target_action=PROVIDER_CREATE_TARGET_ACTION
    )
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    provider = CountingProvider()
    caught: list[BaseException] = []
    try:
        reconcile_cursor_create(
            ledger,
            provider,
            **{
                **_provider_kwargs(job),
                "candidate_id": "",
                "candidate_version": "",
            },
        )
    except BaseException as exc:  # noqa: BLE001
        caught.append(exc)

    assert provider.create_calls == []
    assert provider.lookup_calls == []
    assert ledger.get_claim(job.job_id, "create_run") is None
    assert caught and isinstance(caught[0], AuthorizationDenied)


def test_slack_post_rejects_unauthorized_binding_candidate(tmp_path):
    """Slack effect identity is the binding candidate/version; mismatch fail-closed."""
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.eng29 import (
        AuthorizationDenied,
        SLACK_POST_ROOT_TARGET_ACTION,
    )
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-eng29-slack-cand-mismatch")
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    ledger.bind(
        job_id=job.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        candidate_id="cand-NOT-AUTHORIZED",
        candidate_version="v999",
    )
    decisions = DecisionLedger(sqlite_path=store.sqlite_path)
    decisions.set_policy(
        job_id=job.job_id,
        policy_version="pol-1",
        allowed_actors=("U-alice",),
        expires_at="2099-01-01T00:00:00+00:00",
    )
    _seed_authorized_go_despite_binding(
        store, job, target_action=SLACK_POST_ROOT_TARGET_ACTION
    )
    port = CountingPort()
    caught: list[BaseException] = []
    try:
        deliver_slack_root(ledger, port, job_id=job.job_id)
    except BaseException as exc:  # noqa: BLE001
        caught.append(exc)

    assert port.posts == [], "slack post executed for unauthorized binding candidate"
    assert port.lookup_calls == [], (
        "slack lookup executed for unauthorized binding candidate"
    )
    persisted = ledger.get_binding(job.job_id)
    assert persisted is not None
    assert persisted.status is SlackRootStatus.BOUND
    assert caught and isinstance(caught[0], AuthorizationDenied)


# ---------------------------------------------------------------------------
# Live policy revalidation: expiry/revocation under the same policy version
# ---------------------------------------------------------------------------

LIVE_POLICY_DEFECTS = (
    ("absent", "unauthorized"),
    ("expired", "expired"),
    ("revoked", "unauthorized"),
    ("inactive", "unauthorized"),
    ("malformed", "unauthorized"),
    ("actor_mismatch", "unauthorized"),
    ("policy_version_mismatch", "mismatch"),
    ("missing_expires_at", "expired"),
    ("malformed_expires_at", "expired"),
)


@pytest.mark.parametrize("defect,reason", LIVE_POLICY_DEFECTS)
def test_guard_fail_closed_on_current_live_policy_defects(tmp_path, defect, reason):
    """Unexpired tuple + accepted Go must not survive a dead live policy."""
    from agent.durable_jobs.eng29 import (
        PROVIDER_CREATE_TARGET_ACTION,
        evaluate_authorization,
    )

    store, job = _make_job(tmp_path, idempotency_key=f"idem-eng29-live-{defect}")
    decisions = _bind_and_policy(store, job)
    _register_and_go(
        store, job, decisions, target_action=PROVIDER_CREATE_TARGET_ACTION
    )
    green = evaluate_authorization(
        sqlite_path=store.sqlite_path, **_guard_kwargs(job)
    )
    assert green.ok is True

    _apply_live_policy_defect(store, job, defect)
    denied = evaluate_authorization(
        sqlite_path=store.sqlite_path, **_guard_kwargs(job)
    )
    assert denied.ok is False
    assert reason in denied.reason_codes

    _restore_live_policy(store, job)
    restored = evaluate_authorization(
        sqlite_path=store.sqlite_path, **_guard_kwargs(job)
    )
    assert restored.ok is True
    assert restored.reason_codes == ()


def test_guard_live_policy_expiry_does_not_use_unexpired_tuple(tmp_path):
    """RED→GREEN: same policy version, unexpired tuple, expired live policy."""
    from agent.durable_jobs.eng29 import (
        PROVIDER_CREATE_TARGET_ACTION,
        evaluate_authorization,
        get_authorization_tuple,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-eng29-live-expiry-tuple")
    decisions = _bind_and_policy(store, job)
    _register_and_go(
        store, job, decisions, target_action=PROVIDER_CREATE_TARGET_ACTION
    )
    tup = get_authorization_tuple(
        store.sqlite_path, job.job_id, PROVIDER_CREATE_TARGET_ACTION
    )
    assert tup is not None
    assert tup.expires_at == "2099-01-01T00:00:00+00:00"
    assert tup.policy_version == "pol-1"

    _apply_live_policy_defect(store, job, "expired")
    denied = evaluate_authorization(
        sqlite_path=store.sqlite_path,
        **_guard_kwargs(job, now_iso="2026-01-01T00:00:00+00:00"),
    )
    assert denied.ok is False
    assert "expired" in denied.reason_codes

    _restore_live_policy(store, job)
    allowed = evaluate_authorization(
        sqlite_path=store.sqlite_path,
        **_guard_kwargs(job, now_iso="2026-01-01T00:00:00+00:00"),
    )
    assert allowed.ok is True


@pytest.mark.parametrize("defect", ("expired", "revoked"))
def test_provider_create_zero_claim_and_zero_calls_when_live_policy_dead(
    tmp_path, defect
):
    from agent.durable_jobs.effects import ProviderEffectLedger, reconcile_cursor_create
    from agent.durable_jobs.eng29 import (
        AuthorizationDenied,
        PROVIDER_CREATE_TARGET_ACTION,
    )

    store, job = _make_job(
        tmp_path, idempotency_key=f"idem-eng29-provider-live-{defect}"
    )
    decisions = _bind_and_policy(store, job)
    _register_and_go(
        store, job, decisions, target_action=PROVIDER_CREATE_TARGET_ACTION
    )
    _apply_live_policy_defect(store, job, defect)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    provider = CountingProvider()
    events_before = _effect_event_types(store, job.job_id)
    caught: list[BaseException] = []
    try:
        reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    except BaseException as exc:  # noqa: BLE001
        caught.append(exc)

    assert provider.create_calls == []
    assert provider.lookup_calls == []
    assert ledger.get_claim(job.job_id, "create_run") is None
    assert _effect_event_types(store, job.job_id) == events_before
    assert caught and isinstance(caught[0], AuthorizationDenied)

    _restore_live_policy(store, job)
    result = reconcile_cursor_create(ledger, CountingProvider(), **_provider_kwargs(job))
    from agent.durable_jobs.effects import EffectStatus

    assert result.status is EffectStatus.ACCEPTED


@pytest.mark.parametrize("defect", ("expired", "revoked"))
def test_slack_post_zero_claim_and_zero_calls_when_live_policy_dead(
    tmp_path, defect
):
    from agent.durable_jobs.eng29 import (
        AuthorizationDenied,
        SLACK_POST_ROOT_TARGET_ACTION,
    )
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(
        tmp_path, idempotency_key=f"idem-eng29-slack-live-{defect}"
    )
    decisions = _bind_and_policy(store, job)
    _register_and_go(
        store, job, decisions, target_action=SLACK_POST_ROOT_TARGET_ACTION
    )
    _apply_live_policy_defect(store, job, defect)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    port = CountingPort()
    events_before = _effect_event_types(store, job.job_id)
    caught: list[BaseException] = []
    try:
        deliver_slack_root(ledger, port, job_id=job.job_id)
    except BaseException as exc:  # noqa: BLE001
        caught.append(exc)

    assert port.posts == []
    assert port.lookup_calls == []
    persisted = ledger.get_binding(job.job_id)
    assert persisted is not None
    assert persisted.status is SlackRootStatus.BOUND
    assert _effect_event_types(store, job.job_id) == events_before
    assert caught and isinstance(caught[0], AuthorizationDenied)

    _restore_live_policy(store, job)
    delivered = deliver_slack_root(ledger, CountingPort(), job_id=job.job_id)
    assert delivered.status is SlackRootStatus.DELIVERED


@pytest.mark.parametrize("defect", ("expired", "revoked"))
def test_provider_stale_takeover_zero_mutation_when_live_policy_dead(
    tmp_path, defect
):
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS, FrozenClock
    from agent.durable_jobs.effects import (
        ProviderEffectLedger,
        reconcile_cursor_create,
    )
    from agent.durable_jobs.eng29 import (
        AuthorizationDenied,
        PROVIDER_CREATE_TARGET_ACTION,
    )

    store, job = _make_job(
        tmp_path, idempotency_key=f"idem-eng29-provider-takeover-{defect}"
    )
    decisions = _bind_and_policy(store, job)
    _register_and_go(
        store, job, decisions, target_action=PROVIDER_CREATE_TARGET_ACTION
    )
    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    first = ledger.claim_effect(**_provider_kwargs(job))
    assert first.won is True
    generation = first.claim.claim_generation
    owner = first.claim.claim_owner_token
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    _apply_live_policy_defect(store, job, defect)
    events_before = _effect_event_types(store, job.job_id)
    provider = CountingProvider()
    caught: list[BaseException] = []
    try:
        ledger.takeover_stale_claim(job.job_id, "create_run")
    except BaseException as exc:  # noqa: BLE001
        caught.append(exc)
    assert caught and isinstance(caught[0], AuthorizationDenied)

    persisted = ledger.get_claim(job.job_id, "create_run")
    assert persisted is not None
    assert persisted.claim_generation == generation
    assert persisted.claim_owner_token == owner
    assert "provider_effect_claim_taken" not in _effect_event_types(store, job.job_id)
    assert _effect_event_types(store, job.job_id) == events_before

    caught.clear()
    try:
        reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    except BaseException as exc:  # noqa: BLE001
        caught.append(exc)
    assert provider.create_calls == []
    assert provider.lookup_calls == []
    assert caught and isinstance(caught[0], AuthorizationDenied)
    after = ledger.get_claim(job.job_id, "create_run")
    assert after is not None
    assert after.claim_generation == generation
    assert after.claim_owner_token == owner


@pytest.mark.parametrize("defect", ("expired", "revoked"))
def test_slack_stale_takeover_zero_mutation_when_live_policy_dead(
    tmp_path, defect
):
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS, FrozenClock
    from agent.durable_jobs.eng29 import (
        AuthorizationDenied,
        SLACK_POST_ROOT_TARGET_ACTION,
    )
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(
        tmp_path, idempotency_key=f"idem-eng29-slack-takeover-{defect}"
    )
    decisions = _bind_and_policy(store, job)
    _register_and_go(
        store, job, decisions, target_action=SLACK_POST_ROOT_TARGET_ACTION
    )
    clock = FrozenClock()
    ledger = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    first = ledger.claim_delivery(job.job_id)
    assert first.won is True
    generation = first.binding.claim_generation
    owner = first.binding.claim_owner_token
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    _apply_live_policy_defect(store, job, defect)
    events_before = _effect_event_types(store, job.job_id)
    port = CountingPort()
    caught: list[BaseException] = []
    try:
        ledger.takeover_stale_delivery(job.job_id)
    except BaseException as exc:  # noqa: BLE001
        caught.append(exc)
    assert caught and isinstance(caught[0], AuthorizationDenied)

    persisted = ledger.get_binding(job.job_id)
    assert persisted is not None
    assert persisted.status is SlackRootStatus.CLAIMED
    assert persisted.claim_generation == generation
    assert persisted.claim_owner_token == owner
    assert "slack_root_claim_taken" not in _effect_event_types(store, job.job_id)
    assert _effect_event_types(store, job.job_id) == events_before

    caught.clear()
    try:
        deliver_slack_root(ledger, port, job_id=job.job_id)
    except BaseException as exc:  # noqa: BLE001
        caught.append(exc)
    assert port.posts == []
    assert port.lookup_calls == []
    assert caught and isinstance(caught[0], AuthorizationDenied)
    after = ledger.get_binding(job.job_id)
    assert after is not None
    assert after.claim_generation == generation
    assert after.claim_owner_token == owner


@pytest.mark.parametrize("defect", ("expired", "revoked"))
def test_provider_recovery_lookup_zero_calls_when_live_policy_dead(
    tmp_path, defect
):
    from agent.durable_jobs.clock import FrozenClock
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        reconcile_cursor_create,
    )
    from agent.durable_jobs.eng29 import (
        AuthorizationDenied,
        PROVIDER_CREATE_TARGET_ACTION,
    )

    store, job = _make_job(
        tmp_path, idempotency_key=f"idem-eng29-provider-lookup-{defect}"
    )
    decisions = _bind_and_policy(store, job)
    _register_and_go(
        store, job, decisions, target_action=PROVIDER_CREATE_TARGET_ACTION
    )
    clock = FrozenClock()
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path, now_fn=clock)
    recovering = reconcile_cursor_create(
        ledger,
        CountingProvider(create_kind="lost_response"),
        **_provider_kwargs(job),
    )
    assert recovering.status is EffectStatus.RECOVERING
    _apply_live_policy_defect(store, job, defect)
    generation = recovering.claim_generation
    owner = recovering.claim_owner_token
    events_before = _effect_event_types(store, job.job_id)
    provider = CountingProvider()
    caught: list[BaseException] = []
    try:
        reconcile_cursor_create(
            ledger,
            provider,
            owner_token=owner,
            **_provider_kwargs(job),
        )
    except BaseException as exc:  # noqa: BLE001
        caught.append(exc)

    assert provider.create_calls == []
    assert provider.lookup_calls == []
    assert caught and isinstance(caught[0], AuthorizationDenied)
    after = ledger.get_claim(job.job_id, "create_run")
    assert after is not None
    assert after.status is EffectStatus.RECOVERING
    assert after.claim_generation == generation
    assert after.claim_owner_token == owner
    assert _effect_event_types(store, job.job_id) == events_before


@pytest.mark.parametrize("defect", ("expired", "revoked"))
def test_slack_recovery_lookup_zero_calls_when_live_policy_dead(tmp_path, defect):
    from agent.durable_jobs.clock import FrozenClock
    from agent.durable_jobs.eng29 import (
        AuthorizationDenied,
        SLACK_POST_ROOT_TARGET_ACTION,
    )
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(
        tmp_path, idempotency_key=f"idem-eng29-slack-lookup-{defect}"
    )
    decisions = _bind_and_policy(store, job)
    _register_and_go(
        store, job, decisions, target_action=SLACK_POST_ROOT_TARGET_ACTION
    )
    clock = FrozenClock()
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path, now_fn=clock)
    recovering = deliver_slack_root(
        ledger, CountingPort(post_kind="lost_response"), job_id=job.job_id
    )
    assert recovering.status is SlackRootStatus.RECOVERING
    _apply_live_policy_defect(store, job, defect)
    generation = recovering.claim_generation
    owner = recovering.claim_owner_token
    events_before = _effect_event_types(store, job.job_id)
    port = CountingPort()
    caught: list[BaseException] = []
    try:
        deliver_slack_root(
            ledger, port, job_id=job.job_id, owner_token=owner
        )
    except BaseException as exc:  # noqa: BLE001
        caught.append(exc)

    assert port.posts == []
    assert port.lookup_calls == []
    assert caught and isinstance(caught[0], AuthorizationDenied)
    after = ledger.get_binding(job.job_id)
    assert after is not None
    assert after.status is SlackRootStatus.RECOVERING
    assert after.claim_generation == generation
    assert after.claim_owner_token == owner
    assert _effect_event_types(store, job.job_id) == events_before


def test_production_modules_expose_no_auto_grant_even_when_pytest_is_spoofed(
    tmp_path, monkeypatch
):
    """Spoofing PYTEST_CURRENT_TEST / pytest must not mint a production Go helper."""
    import inspect
    import sys

    import agent.durable_jobs.decisions as decisions_mod
    import agent.durable_jobs.effects as effects_mod
    import agent.durable_jobs.eng29 as eng29_mod
    import agent.durable_jobs.slack_contract as slack_mod
    from agent.durable_jobs.effects import (
        ProviderEffectLedger,
        reconcile_cursor_create,
    )
    from agent.durable_jobs.eng29 import AuthorizationDenied
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    monkeypatch.setenv("PYTEST_CURRENT_TEST", "spoofed::test_fake")
    assert "pytest" in sys.modules

    forbidden_needles = ("install_default", "auto_grant", "auto_auth")
    for mod in (eng29_mod, effects_mod, slack_mod, decisions_mod):
        assert not hasattr(mod, "install_default_adapter_authorization")
        assert not hasattr(mod, "_in_test_runtime")
        for name, _obj in inspect.getmembers(mod):
            lowered = name.lower()
            if any(needle in lowered for needle in forbidden_needles):
                raise AssertionError(
                    f"{mod.__name__}.{name} looks like a production auto-grant path"
                )

    store, job = _make_job(
        tmp_path, idempotency_key="idem-eng29-spoof-no-auto-grant"
    )
    _bind_and_policy(store, job)
    provider = CountingProvider()
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    caught: list[BaseException] = []
    try:
        reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    except BaseException as exc:  # noqa: BLE001
        caught.append(exc)
    assert provider.create_calls == []
    assert provider.lookup_calls == []
    assert ledger.get_claim(job.job_id, "create_run") is None
    assert caught and isinstance(caught[0], AuthorizationDenied)

    slack = SlackBindingLedger(sqlite_path=store.sqlite_path)
    port = CountingPort()
    caught.clear()
    try:
        deliver_slack_root(slack, port, job_id=job.job_id)
    except BaseException as exc:  # noqa: BLE001
        caught.append(exc)
    assert port.posts == []
    assert port.lookup_calls == []
    persisted = slack.get_binding(job.job_id)
    assert persisted is not None
    assert persisted.status is SlackRootStatus.BOUND
    assert caught and isinstance(caught[0], AuthorizationDenied)


# ---------------------------------------------------------------------------
# Strict allowed_actors_json parsing
# ---------------------------------------------------------------------------

VALID_ALLOWED_ACTORS = (
    ('["U-alice"]', ("U-alice",)),
    ('["  U-alice  "]', ("U-alice",)),
    ('["U-alice", "U-bob"]', ("U-alice", "U-bob")),
    ('["U-alice", "  U-bob  "]', ("U-alice", "U-bob")),
    ("[]", ()),
)

INVALID_ALLOWED_ACTORS = (
    1,
    1.5,
    True,
    False,
    None,
    ["U-alice"],
    {"U-alice": True},
    "",
    "   ",
    "1",
    "1.5",
    "true",
    "false",
    "null",
    '"U-alice"',
    '{"U-alice": true}',
    "[1]",
    "[1.5]",
    "[true]",
    "[false]",
    "[null]",
    "[{}]",
    "[[]]",
    "not-json",
    '[""]',
    '["  "]',
    '["U-alice", 1]',
    '["U-alice", true]',
    '["U-alice", null]',
    '["U-alice", ""]',
    '["U-alice", "  "]',
)


@pytest.mark.parametrize("raw,expected", VALID_ALLOWED_ACTORS)
def test_parse_allowed_actors_accepts_strict_string_lists(raw, expected):
    from agent.durable_jobs.eng29 import parse_allowed_actors

    assert parse_allowed_actors(raw) == expected


@pytest.mark.parametrize("raw", INVALID_ALLOWED_ACTORS)
def test_parse_allowed_actors_rejects_malformed_elements(raw):
    from agent.durable_jobs.eng29 import parse_allowed_actors

    assert parse_allowed_actors(raw) is None


def test_live_policy_numeric_actor_is_never_stringified(tmp_path):
    import sqlite3

    from agent.durable_jobs.effects import ProviderEffectLedger
    from agent.durable_jobs.eng29 import (
        AuthorizationDenied,
        PROVIDER_CREATE_TARGET_ACTION,
        evaluate_authorization,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-eng29-actor-numeric")
    decisions = _bind_and_policy(store, job)
    _register_and_go(
        store, job, decisions, target_action=PROVIDER_CREATE_TARGET_ACTION
    )
    conn = sqlite3.connect(store.sqlite_path)
    try:
        conn.execute(
            """
            UPDATE job_authz_policies
               SET allowed_actors_json = '[1]'
             WHERE job_id = ?
            """,
            (job.job_id,),
        )
        conn.commit()
    finally:
        conn.close()

    denied = evaluate_authorization(
        sqlite_path=store.sqlite_path, **_guard_kwargs(job)
    )
    assert denied.ok is False
    assert "unauthorized" in denied.reason_codes
    as_string = evaluate_authorization(
        sqlite_path=store.sqlite_path, **_guard_kwargs(job, actor_id="1")
    )
    assert as_string.ok is False
    assert "unauthorized" in as_string.reason_codes or "mismatch" in as_string.reason_codes

    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    with pytest.raises(AuthorizationDenied):
        ledger.claim_effect(**_provider_kwargs(job))
    assert ledger.get_claim(job.job_id, "create_run") is None


def test_live_policy_whitespace_actor_matches_stripped_request(tmp_path):
    import sqlite3

    from agent.durable_jobs.eng29 import (
        PROVIDER_CREATE_TARGET_ACTION,
        evaluate_authorization,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-eng29-actor-strip")
    decisions = _bind_and_policy(store, job)
    _register_and_go(
        store, job, decisions, target_action=PROVIDER_CREATE_TARGET_ACTION
    )
    conn = sqlite3.connect(store.sqlite_path)
    try:
        conn.execute(
            """
            UPDATE job_authz_policies
               SET allowed_actors_json = ?
             WHERE job_id = ?
            """,
            ('["  U-alice  "]', job.job_id),
        )
        conn.commit()
    finally:
        conn.close()
    allowed = evaluate_authorization(
        sqlite_path=store.sqlite_path, **_guard_kwargs(job)
    )
    assert allowed.ok is True


def test_evaluate_authorization_on_conn_sees_uncommitted_policy_delete(tmp_path):
    import sqlite3

    from agent.durable_jobs.eng29 import (
        PROVIDER_CREATE_TARGET_ACTION,
        evaluate_authorization,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-eng29-conn-snapshot")
    decisions = _bind_and_policy(store, job)
    kwargs = _guard_kwargs(job)
    _register_and_go(
        store, job, decisions, target_action=PROVIDER_CREATE_TARGET_ACTION
    )
    assert evaluate_authorization(sqlite_path=store.sqlite_path, **kwargs).ok is True

    conn = sqlite3.connect(store.sqlite_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.isolation_level = "IMMEDIATE"
    try:
        conn.execute(
            "DELETE FROM job_authz_policies WHERE job_id = ?", (job.job_id,)
        )
        denied = evaluate_authorization(conn=conn, **kwargs)
        assert denied.ok is False
        assert "unauthorized" in denied.reason_codes
        probe = sqlite3.connect(store.sqlite_path, timeout=2)
        probe.row_factory = sqlite3.Row
        try:
            still_committed = evaluate_authorization(conn=probe, **kwargs)
            assert still_committed.ok is True
        finally:
            probe.close()
        conn.rollback()
    finally:
        conn.close()
    assert evaluate_authorization(sqlite_path=store.sqlite_path, **kwargs).ok is True


# ---------------------------------------------------------------------------
# Two-connection TOCTOU: validation and mutation share the write lock
# ---------------------------------------------------------------------------

_TOCTOU_CASES = (
    ("provider", "claim"),
    ("provider", "takeover"),
    ("slack", "claim"),
    ("slack", "takeover"),
)
_POLICY_MUTATIONS = ("delete", "revoke", "change")


def _attempt_concurrent_policy_write(
    sqlite_path, job_id: str, kind: str, *, timeout_s: float = 0.25
):
    import sqlite3

    conn = sqlite3.connect(str(sqlite_path), timeout=timeout_s)
    try:
        conn.isolation_level = None
        conn.execute(f"PRAGMA busy_timeout = {int(timeout_s * 1000)}")
        conn.execute("BEGIN IMMEDIATE")
        if kind == "delete":
            conn.execute(
                "DELETE FROM job_authz_policies WHERE job_id = ?", (job_id,)
            )
        elif kind == "revoke":
            conn.execute(
                """
                UPDATE job_authz_policies
                   SET allowed_actors_json = '[]'
                 WHERE job_id = ?
                """,
                (job_id,),
            )
        elif kind == "change":
            conn.execute(
                """
                UPDATE job_authz_policies
                   SET allowed_actors_json = ?
                 WHERE job_id = ?
                """,
                ('["U-intruder"]', job_id),
            )
        else:
            raise AssertionError(f"unknown policy mutation {kind}")
        conn.execute("COMMIT")
        return None
    except sqlite3.OperationalError as exc:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        return exc
    finally:
        conn.close()


def _live_policy_actors_json(sqlite_path, job_id: str):
    import sqlite3

    conn = sqlite3.connect(str(sqlite_path))
    try:
        row = conn.execute(
            "SELECT allowed_actors_json FROM job_authz_policies WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        return None if row is None else row[0]
    finally:
        conn.close()


def _prepare_toctou_path(tmp_path, *, adapter: str, phase: str, suffix: str):
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS, FrozenClock
    from agent.durable_jobs.effects import ProviderEffectLedger
    from agent.durable_jobs.eng29 import (
        PROVIDER_CREATE_TARGET_ACTION,
        SLACK_POST_ROOT_TARGET_ACTION,
    )
    from agent.durable_jobs.slack_contract import SlackBindingLedger

    store, job = _make_job(
        tmp_path, idempotency_key=f"idem-eng29-toctou-{suffix}"
    )
    target = (
        PROVIDER_CREATE_TARGET_ACTION
        if adapter == "provider"
        else SLACK_POST_ROOT_TARGET_ACTION
    )
    decisions = _bind_and_policy(store, job)
    _register_and_go(store, job, decisions, target_action=target)
    clock = FrozenClock()
    if adapter == "provider":
        ledger = ProviderEffectLedger(
            sqlite_path=store.sqlite_path,
            now_fn=clock,
            lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
        )
        if phase == "takeover":
            first = ledger.claim_effect(**_provider_kwargs(job))
            assert first.won is True
            clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)

        def run():
            if phase == "claim":
                return ledger.claim_effect(**_provider_kwargs(job))
            return ledger.takeover_stale_claim(job.job_id, "create_run")

        won_event = (
            "provider_effect_claimed"
            if phase == "claim"
            else "provider_effect_claim_taken"
        )
    else:
        ledger = SlackBindingLedger(
            sqlite_path=store.sqlite_path,
            now_fn=clock,
            lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
        )
        if phase == "takeover":
            first = ledger.claim_delivery(job.job_id)
            assert first.won is True
            clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)

        def run():
            if phase == "claim":
                return ledger.claim_delivery(job.job_id)
            return ledger.takeover_stale_delivery(job.job_id)

        won_event = (
            "slack_root_claimed" if phase == "claim" else "slack_root_claim_taken"
        )
    return store, job, ledger, run, won_event


def _pause_after_in_transaction_go(monkeypatch):
    import threading

    from agent.durable_jobs import eng29 as eng29_mod

    entered = threading.Event()
    release = threading.Event()

    def seam():
        entered.set()
        if not release.wait(timeout=5):
            raise TimeoutError("in-transaction Go seam wait exceeded")

    monkeypatch.setattr(eng29_mod, "after_in_transaction_adapter_go", seam)
    return entered, release


@pytest.mark.parametrize("adapter,phase", _TOCTOU_CASES)
@pytest.mark.parametrize("mutation", _POLICY_MUTATIONS)
def test_in_transaction_go_blocks_concurrent_policy_mutation(
    tmp_path, monkeypatch, adapter, phase, mutation
):
    import threading

    store, job, ledger, run, won_event = _prepare_toctou_path(
        tmp_path,
        adapter=adapter,
        phase=phase,
        suffix=f"{adapter}-{phase}-{mutation}",
    )
    entered, release = _pause_after_in_transaction_go(monkeypatch)
    outcome: list[tuple[str, object]] = []

    def worker():
        try:
            outcome.append(("ok", run()))
        except Exception as exc:  # noqa: BLE001
            outcome.append(("err", exc))

    thread = threading.Thread(target=worker, name="eng29-toctou-worker")
    thread.start()
    try:
        assert entered.wait(timeout=5), "worker never reached in-transaction seam"
        busy = _attempt_concurrent_policy_write(
            store.sqlite_path, job.job_id, mutation, timeout_s=0.25
        )
        assert busy is not None, "concurrent policy write committed during claim txn"
        text = str(busy).lower()
        assert "locked" in text or "busy" in text
        actors_during = _live_policy_actors_json(store.sqlite_path, job.job_id)
        assert actors_during is not None
        assert "U-alice" in actors_during
        assert "U-intruder" not in actors_during
    finally:
        release.set()
        thread.join(timeout=5)
    assert not thread.is_alive(), "worker deadlocked after seam release"
    assert outcome and outcome[0][0] == "ok", outcome
    result = outcome[0][1]
    assert result.won is True
    assert won_event in _effect_event_types(store, job.job_id)
    after_busy = _attempt_concurrent_policy_write(
        store.sqlite_path, job.job_id, mutation, timeout_s=2.0
    )
    assert after_busy is None


@pytest.mark.parametrize("adapter,phase", _TOCTOU_CASES)
def test_in_transaction_go_rollback_persists_no_mutation_or_event(
    tmp_path, monkeypatch, adapter, phase
):
    from agent.durable_jobs import eng29 as eng29_mod

    store, job, ledger, run, won_event = _prepare_toctou_path(
        tmp_path,
        adapter=adapter,
        phase=phase,
        suffix=f"{adapter}-{phase}-rollback",
    )
    events_before = _effect_event_types(store, job.job_id)

    def boom():
        raise RuntimeError("injected post-validation failure")

    monkeypatch.setattr(eng29_mod, "after_in_transaction_adapter_go", boom)
    with pytest.raises(RuntimeError, match="injected post-validation failure"):
        run()

    assert won_event not in _effect_event_types(store, job.job_id)
    assert _effect_event_types(store, job.job_id) == events_before
    if adapter == "provider":
        claim = ledger.get_claim(job.job_id, "create_run")
        if phase == "claim":
            assert claim is None
        else:
            assert claim is not None
            assert claim.claim_generation == 1
    else:
        binding = ledger.get_binding(job.job_id)
        assert binding is not None
        if phase == "claim":
            from agent.durable_jobs.slack_contract import SlackRootStatus

            assert binding.status is SlackRootStatus.BOUND
            assert binding.claim_generation == 0
        else:
            assert binding.claim_generation == 1


@pytest.mark.parametrize("adapter,phase", _TOCTOU_CASES)
def test_policy_revoke_before_claim_or_takeover_is_denied(
    tmp_path, adapter, phase
):
    import sqlite3

    from agent.durable_jobs.eng29 import AuthorizationDenied

    store, job, ledger, run, won_event = _prepare_toctou_path(
        tmp_path,
        adapter=adapter,
        phase=phase,
        suffix=f"{adapter}-{phase}-revoke-first",
    )
    events_before = _effect_event_types(store, job.job_id)
    conn = sqlite3.connect(store.sqlite_path)
    try:
        conn.execute(
            "DELETE FROM job_authz_policies WHERE job_id = ?", (job.job_id,)
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(AuthorizationDenied):
        run()
    assert won_event not in _effect_event_types(store, job.job_id)
    assert _effect_event_types(store, job.job_id) == events_before
    if adapter == "provider" and phase == "claim":
        assert ledger.get_claim(job.job_id, "create_run") is None
    if adapter == "slack" and phase == "claim":
        from agent.durable_jobs.slack_contract import SlackRootStatus

        binding = ledger.get_binding(job.job_id)
        assert binding is not None
        assert binding.status is SlackRootStatus.BOUND


# ---------------------------------------------------------------------------
# Post-lock authorization clock: expiry during BEGIN IMMEDIATE wait
# ---------------------------------------------------------------------------


def _prepare_lock_wait_expiry(tmp_path, *, adapter: str, phase: str):
    from agent.durable_jobs.clock import (
        DEFAULT_CLAIM_LEASE_SECONDS,
        FrozenClock,
        add_seconds_iso,
    )
    from agent.durable_jobs.effects import ProviderEffectLedger
    from agent.durable_jobs.eng29 import (
        PROVIDER_CREATE_TARGET_ACTION,
        SLACK_POST_ROOT_TARGET_ACTION,
    )
    from agent.durable_jobs.slack_contract import SlackBindingLedger

    store, job = _make_job(
        tmp_path, idempotency_key=f"idem-eng29-lockwait-{adapter}-{phase}"
    )
    clock = FrozenClock()
    t0 = clock()
    target = (
        PROVIDER_CREATE_TARGET_ACTION
        if adapter == "provider"
        else SLACK_POST_ROOT_TARGET_ACTION
    )
    if phase == "claim":
        expires_at = add_seconds_iso(t0, 10)
        expire_advance = 15
    else:
        expires_at = add_seconds_iso(t0, 45)
        expire_advance = 20
    decisions = _bind_and_policy(store, job)
    _register_and_go(store, job, decisions, target_action=target)
    import sqlite3

    conn = sqlite3.connect(store.sqlite_path)
    try:
        conn.execute(
            """
            UPDATE job_authz_policies
               SET expires_at = ?
             WHERE job_id = ?
            """,
            (expires_at, job.job_id),
        )
        conn.execute(
            """
            UPDATE job_authorization_tuples
               SET expires_at = ?
             WHERE job_id = ? AND target_action = ?
            """,
            (expires_at, job.job_id, target),
        )
        conn.commit()
    finally:
        conn.close()
    if adapter == "provider":
        ledger = ProviderEffectLedger(
            sqlite_path=store.sqlite_path,
            now_fn=clock,
            lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
        )
        if phase == "takeover":
            first = ledger.claim_effect(**_provider_kwargs(job))
            assert first.won is True
            clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)

        def run():
            if phase == "claim":
                return ledger.claim_effect(**_provider_kwargs(job))
            return ledger.takeover_stale_claim(job.job_id, "create_run")

        won_event = (
            "provider_effect_claimed"
            if phase == "claim"
            else "provider_effect_claim_taken"
        )
    else:
        ledger = SlackBindingLedger(
            sqlite_path=store.sqlite_path,
            now_fn=clock,
            lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
        )
        if phase == "takeover":
            first = ledger.claim_delivery(job.job_id)
            assert first.won is True
            clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)

        def run():
            if phase == "claim":
                return ledger.claim_delivery(job.job_id)
            return ledger.takeover_stale_delivery(job.job_id)

        won_event = (
            "slack_root_claimed" if phase == "claim" else "slack_root_claim_taken"
        )
    return store, job, ledger, run, won_event, clock, expires_at, expire_advance


@pytest.mark.parametrize("adapter,phase", _TOCTOU_CASES)
def test_policy_tuple_expiry_during_begin_immediate_wait_is_denied(
    tmp_path, monkeypatch, adapter, phase
):
    """Connection A holds the write lock; B blocks on BEGIN IMMEDIATE.

    Live policy/tuple expire while B waits. After A releases, B must deny
    with zero claim/takeover mutation or event. Pre-lock timing is proven
    by the production ``before_begin_immediate`` seam firing while the
    frozen clock is still unexpired.
    """
    import sqlite3
    import threading

    from agent.durable_jobs import eng29 as eng29_mod
    from agent.durable_jobs.clock import parse_iso
    from agent.durable_jobs.eng29 import AuthorizationDenied

    store, job, ledger, run, won_event, clock, expires_at, expire_advance = (
        _prepare_lock_wait_expiry(tmp_path, adapter=adapter, phase=phase)
    )
    events_before = _effect_event_types(store, job.job_id)
    entered = threading.Event()

    def seam():
        entered.set()

    monkeypatch.setattr(eng29_mod, "before_begin_immediate", seam)

    holder = sqlite3.connect(str(store.sqlite_path), timeout=5)
    holder.isolation_level = None
    holder.execute("BEGIN IMMEDIATE")
    outcome: list[tuple[str, object]] = []

    def worker():
        try:
            outcome.append(("ok", run()))
        except Exception as exc:  # noqa: BLE001
            outcome.append(("err", exc))

    thread = threading.Thread(target=worker, name="eng29-lockwait-worker")
    try:
        thread.start()
        assert entered.wait(timeout=5), "worker never reached pre-lock seam"
        assert parse_iso(clock()) < parse_iso(expires_at)
        assert outcome == []
        clock.advance(expire_advance)
        assert parse_iso(clock()) >= parse_iso(expires_at)
        assert outcome == []
    finally:
        try:
            holder.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        holder.close()
        thread.join(timeout=5)
    assert not thread.is_alive(), "worker deadlocked after lock holder released"
    assert outcome and outcome[0][0] == "err", outcome
    assert isinstance(outcome[0][1], AuthorizationDenied)
    assert won_event not in _effect_event_types(store, job.job_id)
    assert _effect_event_types(store, job.job_id) == events_before
    if adapter == "provider":
        claim = ledger.get_claim(job.job_id, "create_run")
        if phase == "claim":
            assert claim is None
        else:
            assert claim is not None
            assert claim.claim_generation == 1
    else:
        from agent.durable_jobs.slack_contract import SlackRootStatus

        binding = ledger.get_binding(job.job_id)
        assert binding is not None
        if phase == "claim":
            assert binding.status is SlackRootStatus.BOUND
            assert binding.claim_generation == 0
        else:
            assert binding.claim_generation == 1


# ---------------------------------------------------------------------------
# Schema v7: pre-v7 ENG-29 tuples migrate to blank candidate identity
# ---------------------------------------------------------------------------


_PRE_V7_ENG29_SCHEMA = """
CREATE TABLE durable_jobs_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE durable_jobs (
    job_id TEXT PRIMARY KEY,
    phase TEXT NOT NULL,
    origin_platform TEXT NOT NULL,
    origin_chat_id TEXT NOT NULL,
    origin_root_thread_id TEXT NOT NULL,
    objective TEXT NOT NULL,
    repository_identity TEXT NOT NULL,
    frozen_baseline_sha TEXT NOT NULL DEFAULT '',
    idempotency_key TEXT NOT NULL UNIQUE,
    next_action TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE durable_job_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    idempotency_key TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(job_id, event_type, idempotency_key)
);
CREATE TABLE job_authz_policies (
    job_id TEXT PRIMARY KEY,
    policy_version TEXT NOT NULL,
    allowed_actors_json TEXT NOT NULL,
    expires_at TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (job_id) REFERENCES durable_jobs(job_id)
);
CREATE TABLE job_decisions (
    decision_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    decision_type TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    candidate_version TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    decision_idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    reason_codes_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    source_package_id TEXT,
    source_package_version TEXT,
    candidate_sha TEXT,
    target_environment TEXT,
    target_action TEXT,
    matrix_version TEXT,
    CHECK (decision_type IN ('go', 'hold', 'cancel')),
    CHECK (status IN ('accepted', 'duplicate', 'rejected')),
    FOREIGN KEY (job_id) REFERENCES durable_jobs(job_id)
);
CREATE TABLE job_authorization_tuples (
    job_id TEXT NOT NULL,
    target_action TEXT NOT NULL,
    source_package_id TEXT NOT NULL,
    source_package_version TEXT NOT NULL,
    candidate_sha TEXT NOT NULL,
    target_environment TEXT NOT NULL,
    authorized_actor TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    matrix_version TEXT NOT NULL,
    authorization_idempotency_key TEXT NOT NULL UNIQUE,
    prerequisites_satisfied INTEGER NOT NULL DEFAULT 0,
    provider_ambiguity_resolved INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    PRIMARY KEY (job_id, target_action),
    FOREIGN KEY (job_id) REFERENCES durable_jobs(job_id)
);
CREATE TABLE slack_job_bindings (
    job_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    root_thread_ts TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    candidate_version TEXT NOT NULL,
    outbound_client_msg_id TEXT NOT NULL UNIQUE,
    delivered_message_ts TEXT,
    status TEXT NOT NULL,
    unknown_reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def test_schema_v7_migrates_pre_v7_tuple_blank_candidate_fail_closed(tmp_path):
    """Pre-v7 tuples gain blank candidate identity and cannot authorize effects."""
    import sqlite3

    from agent.durable_jobs.effects import ProviderEffectLedger, reconcile_cursor_create
    from agent.durable_jobs.eng29 import (
        MATRIX_VERSION,
        PROVIDER_CREATE_TARGET_ACTION,
        AuthorizationDenied,
        evaluate_authorization,
        get_authorization_tuple,
        register_authorization_tuple,
    )
    from agent.durable_jobs.store import SCHEMA_VERSION, DurableJobStore

    path = _db(tmp_path)
    now = "2026-01-01T00:00:00+00:00"
    job_id = "dj_prev7"
    conn = sqlite3.connect(path)
    try:
        conn.executescript(_PRE_V7_ENG29_SCHEMA)
        conn.execute(
            "INSERT INTO durable_jobs_meta(key, value) VALUES('schema_version', '6')"
        )
        conn.execute(
            """
            INSERT INTO durable_jobs(
                job_id, phase, origin_platform, origin_chat_id,
                origin_root_thread_id, objective, repository_identity,
                frozen_baseline_sha, idempotency_key, next_action,
                created_at, updated_at
            ) VALUES (?, 'INTAKE', 'slack', 'C123', '111.222', 'v6 tuple',
                      'github.com/example/repo', 'sha-eng29-test', 'idem-prev7',
                      'freeze_baseline', ?, ?)
            """,
            (job_id, now, now),
        )
        conn.execute(
            """
            INSERT INTO job_authz_policies(
                job_id, policy_version, allowed_actors_json, expires_at, created_at
            ) VALUES (?, 'pol-1', ?, '2099-01-01T00:00:00+00:00', ?)
            """,
            (job_id, '["U-alice"]', now),
        )
        conn.execute(
            """
            INSERT INTO slack_job_bindings(
                job_id, workspace_id, channel_id, root_thread_ts,
                candidate_id, candidate_version, outbound_client_msg_id,
                delivered_message_ts, status, unknown_reason, created_at, updated_at
            ) VALUES (?, 'T1', 'C123', '111.222', 'cand-1', 'v1',
                      'msg-prev7', NULL, 'bound', NULL, ?, ?)
            """,
            (job_id, now, now),
        )
        conn.execute(
            """
            INSERT INTO job_authorization_tuples(
                job_id, target_action, source_package_id, source_package_version,
                candidate_sha, target_environment, authorized_actor, expires_at,
                policy_version, matrix_version, authorization_idempotency_key,
                prerequisites_satisfied, provider_ambiguity_resolved, created_at
            ) VALUES (?, ?, 'github.com/example/repo', 'v1', 'sha-eng29-test',
                      'slack', 'U-alice', '2099-01-01T00:00:00+00:00', 'pol-1',
                      ?, 'tuple:dj_prev7:create', 1, 1, ?)
            """,
            (job_id, PROVIDER_CREATE_TARGET_ACTION, MATRIX_VERSION, now),
        )
        conn.execute(
            """
            INSERT INTO job_decisions(
                decision_id, job_id, decision_type, candidate_id,
                candidate_version, actor_id, policy_version,
                decision_idempotency_key, status, reason_codes_json,
                created_at, source_package_id, source_package_version,
                candidate_sha, target_environment, target_action, matrix_version
            ) VALUES ('dd_prev7', ?, 'go', 'cand-1', 'v1', 'U-alice', 'pol-1',
                      'go:dj_prev7:create', 'accepted', '[]', ?,
                      'github.com/example/repo', 'v1', 'sha-eng29-test',
                      'slack', ?, ?)
            """,
            (job_id, now, PROVIDER_CREATE_TARGET_ACTION, MATRIX_VERSION),
        )
        conn.commit()
    finally:
        conn.close()

    probe = sqlite3.connect(path)
    try:
        before_cols = {
            row[1]
            for row in probe.execute(
                "PRAGMA table_info(job_authorization_tuples)"
            ).fetchall()
        }
    finally:
        probe.close()
    assert "candidate_id" not in before_cols
    assert "candidate_version" not in before_cols

    DurableJobStore(sqlite_path=path)
    assert SCHEMA_VERSION >= 7
    probe = sqlite3.connect(path)
    try:
        after_cols = {
            row[1]
            for row in probe.execute(
                "PRAGMA table_info(job_authorization_tuples)"
            ).fetchall()
        }
    finally:
        probe.close()
    assert "candidate_id" in after_cols
    assert "candidate_version" in after_cols

    migrated = get_authorization_tuple(
        path, job_id, PROVIDER_CREATE_TARGET_ACTION
    )
    assert migrated is not None
    assert migrated.candidate_id == ""
    assert migrated.candidate_version == ""

    from types import SimpleNamespace

    job = SimpleNamespace(
        job_id=job_id,
        repository_identity="github.com/example/repo",
        frozen_baseline_sha="sha-eng29-test",
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
    )
    blank = evaluate_authorization(
        sqlite_path=path,
        **_guard_kwargs(job, candidate_id="", candidate_version=""),
    )
    assert blank.ok is False
    assert "unauthorized" in blank.reason_codes

    replay = evaluate_authorization(sqlite_path=path, **_guard_kwargs(job))
    assert replay.ok is False
    assert "mismatch" in replay.reason_codes or "unauthorized" in replay.reason_codes

    overwritten = register_authorization_tuple(
        sqlite_path=path,
        **_default_tuple_kwargs(
            job,
            target_action=PROVIDER_CREATE_TARGET_ACTION,
            authorization_idempotency_key="tuple:dj_prev7:reauth",
        ),
    )
    assert overwritten.ok is False

    ledger = ProviderEffectLedger(sqlite_path=path)
    provider = CountingProvider()
    caught: list[BaseException] = []
    try:
        reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    except BaseException as exc:  # noqa: BLE001
        caught.append(exc)
    assert provider.create_calls == []
    assert provider.lookup_calls == []
    assert ledger.get_claim(job_id, "create_run") is None
    assert caught and isinstance(caught[0], AuthorizationDenied)

    store2, job2 = _make_job(tmp_path, idempotency_key="idem-eng29-v7-reauth")
    decisions2 = _bind_and_policy(store2, job2, root_thread_ts="111.223")
    _register_and_go(
        store2, job2, decisions2, target_action=PROVIDER_CREATE_TARGET_ACTION
    )
    allowed = evaluate_authorization(
        sqlite_path=store2.sqlite_path, **_guard_kwargs(job2)
    )
    assert allowed.ok is True
