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
    def __init__(self) -> None:
        self.create_calls: list[str] = []
        self.lookup_calls: list[str] = []

    def create_run(self, *, idempotency_key: str, job_id: str) -> FakeCreateResult:
        self.create_calls.append(idempotency_key)
        return FakeCreateResult(kind="accepted", run=FakeRun("run-1", idempotency_key))

    def lookup_runs(self, *, idempotency_key: str) -> list[FakeRun]:
        self.lookup_calls.append(idempotency_key)
        return []


@dataclass
class FakePostResult:
    kind: str
    message_ts: Optional[str] = None


class CountingPort:
    def __init__(self) -> None:
        self.posts: list[str] = []
        self.lookup_calls: list[str] = []

    def post_root(self, **kwargs) -> FakePostResult:
        self.posts.append(kwargs["client_msg_id"])
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


def _default_tuple_kwargs(job_id: str, **overrides):
    from agent.durable_jobs.eng29 import (
        DEFAULT_CANDIDATE_SHA,
        DEFAULT_SOURCE_PACKAGE_ID,
        DEFAULT_SOURCE_PACKAGE_VERSION,
        DEFAULT_TARGET_ENVIRONMENT,
        MATRIX_VERSION,
        PROVIDER_CREATE_TARGET_ACTION,
    )

    base = dict(
        job_id=job_id,
        source_package_id=DEFAULT_SOURCE_PACKAGE_ID,
        source_package_version=DEFAULT_SOURCE_PACKAGE_VERSION,
        candidate_sha=DEFAULT_CANDIDATE_SHA,
        target_environment=DEFAULT_TARGET_ENVIRONMENT,
        target_action=PROVIDER_CREATE_TARGET_ACTION,
        authorized_actor="U-alice",
        expires_at="2099-01-01T00:00:00+00:00",
        policy_version="pol-1",
        matrix_version=MATRIX_VERSION,
        authorization_idempotency_key=f"tuple:{job_id}:default",
    )
    base.update(overrides)
    return base


def _guard_kwargs(job_id: str, **overrides):
    from agent.durable_jobs.eng29 import (
        DEFAULT_CANDIDATE_SHA,
        DEFAULT_SOURCE_PACKAGE_ID,
        DEFAULT_SOURCE_PACKAGE_VERSION,
        DEFAULT_TARGET_ENVIRONMENT,
        MATRIX_VERSION,
        PROVIDER_CREATE_TARGET_ACTION,
    )

    base = dict(
        job_id=job_id,
        source_package_id=DEFAULT_SOURCE_PACKAGE_ID,
        source_package_version=DEFAULT_SOURCE_PACKAGE_VERSION,
        candidate_sha=DEFAULT_CANDIDATE_SHA,
        target_environment=DEFAULT_TARGET_ENVIRONMENT,
        target_action=PROVIDER_CREATE_TARGET_ACTION,
        actor_id="U-alice",
        policy_version="pol-1",
        matrix_version=MATRIX_VERSION,
        now_iso="2026-01-01T00:00:00+00:00",
    )
    base.update(overrides)
    return base


def _register_and_go(store, job, decisions, *, target_action: str, **tuple_overrides):
    from agent.durable_jobs.eng29 import register_authorization_tuple

    tuple_kwargs = _default_tuple_kwargs(
        job.job_id,
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
        candidate_id="cand-1",
        candidate_version="v1",
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
        **_guard_kwargs(job.job_id, target_action=category),
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
        **_guard_kwargs(job.job_id, target_action=category),
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
        "target_environment": "prod",
        "target_action": "deploy",
        "actor_id": "U-eve",
        "policy_version": "pol-other",
        "matrix_version": MATRIX_VERSION + "-other",
    }
    result = evaluate_authorization(
        sqlite_path=store.sqlite_path,
        **_guard_kwargs(job.job_id, **{field: wrong[field]}),
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
        **_guard_kwargs(job.job_id, now_iso="2026-01-02T00:00:00+00:00"),
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
        job.job_id, target_action=PROVIDER_CREATE_TARGET_ACTION
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
        sqlite_path=store.sqlite_path, **_guard_kwargs(job.job_id)
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
        sqlite_path=store.sqlite_path, **_guard_kwargs(job.job_id)
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
        sqlite_path=store.sqlite_path, **_default_tuple_kwargs(job.job_id)
    )
    assert first.ok is True
    dup = register_authorization_tuple(
        sqlite_path=store.sqlite_path, **_default_tuple_kwargs(job.job_id)
    )
    assert dup.ok is True
    assert dup.status == "duplicate"
    conflict = register_authorization_tuple(
        sqlite_path=store.sqlite_path,
        **_default_tuple_kwargs(job.job_id, candidate_sha="sha-other"),
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
        sqlite_path=store.sqlite_path, **_default_tuple_kwargs(job.job_id)
    ).ok
    policy_change = register_authorization_tuple(
        sqlite_path=store.sqlite_path,
        **_default_tuple_kwargs(
            job.job_id,
            policy_version="pol-2",
            authorization_idempotency_key=f"tuple:{job.job_id}:pol2",
        ),
    )
    assert policy_change.ok is False
    matrix_change = register_authorization_tuple(
        sqlite_path=store.sqlite_path,
        **_default_tuple_kwargs(
            job.job_id,
            matrix_version=MATRIX_VERSION + "-2",
            authorization_idempotency_key=f"tuple:{job.job_id}:mx2",
        ),
    )
    assert matrix_change.ok is False
    unknown_matrix = register_authorization_tuple(
        sqlite_path=store.sqlite_path,
        **_default_tuple_kwargs(
            job.job_id,
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
        **_guard_kwargs(job.job_id, target_action="missing_prerequisites"),
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
        **_guard_kwargs(job2.job_id, target_action="missing_prerequisites"),
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
        **_guard_kwargs(job.job_id, target_action="unresolved_provider_ambiguity"),
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
            job2.job_id, target_action="unresolved_provider_ambiguity"
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
        sqlite_path=store.sqlite_path, **_guard_kwargs(job.job_id)
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
