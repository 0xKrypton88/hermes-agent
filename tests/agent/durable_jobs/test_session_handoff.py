from __future__ import annotations


def test_default_policy_arms_only_target_model_and_hard_threshold_is_configurable():
    from agent.durable_jobs.session_handoff import HandoffPolicy, SessionHandoffConfig

    config = SessionHandoffConfig.default_shadow()

    target = config.evaluate(
        provider="openai-codex",
        model="gpt-5.6-sol",
        used_tokens=45,
        context_tokens=100,
    )
    other = config.evaluate(
        provider="openai-codex",
        model="gpt-5.5",
        used_tokens=99,
        context_tokens=100,
    )
    custom = SessionHandoffConfig(
        policies={
            ("provider-x", "model-y"): HandoffPolicy(
                soft_arm_ratio=0.30,
                hard_precompression_ratio=0.60,
            )
        }
    ).evaluate(
        provider="provider-x",
        model="model-y",
        used_tokens=61,
        context_tokens=100,
    )

    assert target.armed is True
    assert target.hard is False
    assert other.armed is False
    assert custom.armed is True
    assert custom.hard is True


def _handoff():
    from agent.durable_jobs.session_handoff import SessionHandoff

    return SessionHandoff(
        handoff_id="ho_123",
        idempotency_key="handoff:ENG-122:d13dee",
        project="Hermes",
        issue="ENG-122",
        goal="Continue session handoff implementation",
        verified=("baseline SHA verified",),
        pending=("focused tests",),
        remaining=("broader checks",),
        blockers=(),
        user_action="none",
        repository="github.com/nous/hermes",
        worktree="D:/Hermes/worktrees/hermes-eng122-session-handoff",
        branch="codex/eng-122-session-handoff",
        exact_sha="d13deeeb05b8f5c1221dbd0131536ff81102b2ea",
        diff_fingerprint="sha256:abc",
        test_evidence=("selector: 1 passed",),
        risk_gates=("single_writer", "project_go", "approval_required"),
        forbidden_actions=("commit", "push", "deploy", "activate"),
        resume_pointer="durable-job://dj_1/handoffs/ho_123",
        next_action="Run focused selector",
    )


def test_handoff_is_canonical_versioned_and_excludes_prompt_or_reasoning_fields():
    import json

    handoff = _handoff()
    canonical = handoff.canonical_json()
    decoded = json.loads(canonical)

    assert handoff.schema == "hermes.session-handoff"
    assert handoff.version == 1
    assert decoded["handoff_id"] == "ho_123"
    assert decoded["resume_pointer"] == "durable-job://dj_1/handoffs/ho_123"
    assert decoded["next_action"] == "Run focused selector"
    assert canonical == handoff.canonical_json()
    assert "prompt" not in canonical.lower()
    assert "reasoning" not in canonical.lower()


def test_handoff_redacts_secret_text_before_persisting_canonical_payload():
    import json
    from dataclasses import replace

    handoff = replace(
        _handoff(),
        goal="Inspect password=hunter2 without persisting it",
        next_action="Check password=super-secret before continuing",
    )

    canonical = handoff.canonical_json()
    decoded = json.loads(canonical)

    assert "hunter2" not in canonical
    assert "super-secret" not in canonical
    assert "[REDACTED]" in decoded["goal"]
    assert "[REDACTED]" in decoded["next_action"]


def _lane(tmp_path):
    from agent.durable_jobs.config import DurableJobsConfig, DurableJobsIdentityBinding
    from agent.durable_jobs.lane import DurableLaneService
    from agent.durable_jobs.store import DurableJobStore

    store = DurableJobStore(tmp_path / "jobs.sqlite")
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C1",
        origin_root_thread_id="100.1",
        objective="ENG-122",
        repository_identity="github.com/nous/hermes",
        frozen_baseline_sha="d13deeeb05b8f5c1221dbd0131536ff81102b2ea",
        idempotency_key="job:ENG-122",
    )
    lane = DurableLaneService(
        DurableJobsConfig(
            enabled=True,
            dispatch_enabled=False,
            backend="sqlite",
            sqlite_path=tmp_path / "jobs.sqlite",
            checkpoint_sqlite_path=tmp_path / "checkpoints.sqlite",
            identity_binding=DurableJobsIdentityBinding(
                workspace_id="T1",
                repository_identity="github.com/nous/hermes",
            ),
        ),
        store=store,
    )
    lane.bind_slack(
        job_id=job.job_id,
        workspace_id="T1",
        channel_id="C1",
        root_thread_ts="100.1",
        candidate_id="eng-122",
        candidate_version="d13dee",
    )
    return lane, job


def _armed():
    from agent.durable_jobs.session_handoff import SessionHandoffConfig

    return SessionHandoffConfig.default().evaluate(
        "openai-codex", "gpt-5.6-sol", used_tokens=450, max_tokens=1000
    )


def _enabled_handoff_config():
    from dataclasses import replace

    from agent.durable_jobs.session_handoff import SessionHandoffConfig

    return replace(SessionHandoffConfig.default(), enabled=True, shadow=False)


def _complete_ledger_effect(ledger, job_id, handoff_id, effect_name, receipt=None):
    owner = f"setup-{effect_name.lower()}"
    claim = ledger.claim_effect(
        job_id,
        handoff_id,
        effect_name,
        owner_token=owner,
    )
    assert claim.acquired
    return ledger.complete_effect(
        job_id,
        handoff_id,
        effect_name,
        owner_token=owner,
        receipt=receipt,
    )


def test_enabled_shadow_mode_refuses_all_external_effects(tmp_path):
    from dataclasses import replace

    import pytest

    from agent.durable_jobs.lane import PilotDisabledError
    from agent.durable_jobs.session_handoff import (
        SemanticWaypoint,
        SessionHandoffConfig,
    )

    lane, job = _lane(tmp_path)
    linear, slack, sessions = _Linear(), _Slack(), _Sessions()

    with pytest.raises(PilotDisabledError):
        lane.resume_session_handoff(
            job_id=job.job_id,
            parent_session_id="parent-1",
            handoff=_handoff(),
            waypoint=SemanticWaypoint(verified=True),
            pressure=_armed(),
            linear=linear,
            slack=slack,
            sessions=sessions,
            handoff_config=replace(SessionHandoffConfig.default(), enabled=True),
        )

    assert not linear.effects and not slack.effects and not sessions.children


class _Linear:
    def __init__(self):
        self.effects = {}
        self.value = None

    def upsert_handoff(self, *, issue, canonical, idempotency_key):
        self.effects.setdefault(idempotency_key, f"linear:{issue}")
        self.value = canonical
        return self.effects[idempotency_key]

    def read_handoff(self, *, issue):
        return self.value


class _Slack:
    def __init__(self):
        self.effects = {}
        self.pointers = {}

    def post_handoff_receipt(self, *, handoff_id, resume_pointer, idempotency_key):
        self.effects.setdefault(idempotency_key, f"slack:{handoff_id}")
        self.pointers.setdefault(idempotency_key, resume_pointer)
        return self.effects[idempotency_key]


class _Sessions:
    def __init__(self):
        self.children = {}
        self.injections = {}
        self.turns = {}

    def find_or_create_child(self, *, parent_session_id, handoff_id, idempotency_key):
        self.children.setdefault(idempotency_key, "child-1")
        return self.children[idempotency_key]

    def inject_handoff(self, *, child_session_id, canonical, idempotency_key):
        self.injections.setdefault(idempotency_key, canonical)

    def start_first_turn(self, *, child_session_id, next_action, idempotency_key):
        self.turns.setdefault(idempotency_key, next_action)


def test_safe_waypoint_runs_ordered_durable_handoff_exactly_once(tmp_path):
    from agent.durable_jobs.session_handoff import SemanticWaypoint

    lane, job = _lane(tmp_path)
    linear, slack, sessions = _Linear(), _Slack(), _Sessions()
    kwargs = dict(
        job_id=job.job_id,
        parent_session_id="parent-1",
        handoff=_handoff(),
        waypoint=SemanticWaypoint(verified=True),
        pressure=_armed(),
        linear=linear,
        slack=slack,
        sessions=sessions,
        handoff_config=_enabled_handoff_config(),
    )

    first = lane.resume_session_handoff(**kwargs)
    replay = lane.resume_session_handoff(**kwargs)

    assert first.stage == "COMPLETE"
    assert replay == first
    assert first.child_session_id == "child-1"
    assert len(linear.effects) == 1
    assert len(slack.effects) == 1
    assert len(sessions.children) == 1
    assert len(sessions.injections) == 1
    assert len(sessions.turns) == 1


def test_hard_pressure_never_overrides_an_unsafe_boundary(tmp_path):
    import pytest

    from agent.durable_jobs.session_handoff import (
        SemanticWaypoint,
        SessionHandoffConfig,
        UnsafeHandoffWaypoint,
    )

    lane, job = _lane(tmp_path)
    linear, slack, sessions = _Linear(), _Slack(), _Sessions()
    hard = SessionHandoffConfig.default().evaluate(
        "openai-codex", "gpt-5.6-sol", used_tokens=900, max_tokens=1000
    )
    with pytest.raises(UnsafeHandoffWaypoint):
        lane.resume_session_handoff(
            job_id=job.job_id,
            parent_session_id="parent-1",
            handoff=_handoff(),
            waypoint=SemanticWaypoint(verified=True, external_mutation_active=True),
            pressure=hard,
            linear=linear,
            slack=slack,
            sessions=sessions,
            handoff_config=_enabled_handoff_config(),
        )
    assert not linear.effects and not slack.effects and not sessions.children


class _CrashAfterChild(_Sessions):
    def __init__(self):
        super().__init__()
        self.crash_once = True

    def find_or_create_child(self, *, parent_session_id, handoff_id, idempotency_key):
        child = super().find_or_create_child(
            parent_session_id=parent_session_id,
            handoff_id=handoff_id,
            idempotency_key=idempotency_key,
        )
        if self.crash_once:
            self.crash_once = False
            raise ConnectionError("secret raw provider failure must not persist")
        return child


def test_crash_is_explicit_fail_closed_and_manual_resume_deduplicates_effects(tmp_path):
    import pytest

    from agent.durable_jobs.session_handoff import (
        EffectReconciliationRequired,
        ManualResumeRequired,
        SemanticWaypoint,
        SessionHandoffLedger,
    )

    lane, job = _lane(tmp_path)
    linear, slack, sessions = _Linear(), _Slack(), _CrashAfterChild()
    kwargs = dict(
        job_id=job.job_id,
        parent_session_id="parent-1",
        handoff=_handoff(),
        waypoint=SemanticWaypoint(verified=True),
        pressure=_armed(),
        linear=linear,
        slack=slack,
        sessions=sessions,
        handoff_config=_enabled_handoff_config(),
    )

    with pytest.raises(ConnectionError):
        lane.resume_session_handoff(**kwargs)
    failed = SessionHandoffLedger(tmp_path / "jobs.sqlite").get(job.job_id, "ho_123")
    assert failed.stage == "FAILED_CLOSED"
    assert failed.checkpoint_stage == "SLACK_RECEIPTED"
    assert failed.failure_reason == "ConnectionError"
    assert "manual_resume=True" in failed.manual_resume_action
    assert "secret" not in failed.failure_reason

    with pytest.raises(ManualResumeRequired):
        lane.resume_session_handoff(**kwargs)
    with pytest.raises(EffectReconciliationRequired):
        lane.resume_session_handoff(**kwargs, manual_resume=True)
    still_failed = SessionHandoffLedger(tmp_path / "jobs.sqlite").get(
        job.job_id, "ho_123"
    )
    assert still_failed.stage == "FAILED_CLOSED"

    failed_claim = SessionHandoffLedger(tmp_path / "jobs.sqlite").get_effect(
        job.job_id, "ho_123", "CHILD_CREATE"
    )
    lane.reconcile_session_handoff_effect(
        job_id=job.job_id,
        handoff_id="ho_123",
        effect_name="CHILD_CREATE",
        outcome="APPLIED",
        receipt="child-1",
        expected_owner_token=failed_claim.owner_token,
        expected_generation=failed_claim.generation,
        dead_owner_verified=True,
        handoff_config=_enabled_handoff_config(),
    )
    reconciled = SessionHandoffLedger(tmp_path / "jobs.sqlite").get(
        job.job_id, "ho_123"
    )
    assert reconciled.stage == "FAILED_CLOSED"
    assert reconciled.checkpoint_stage == "CHILD_CREATED"

    complete = lane.resume_session_handoff(**kwargs, manual_resume=True)

    assert complete.stage == "COMPLETE"
    assert len(linear.effects) == 1
    assert len(slack.effects) == 1
    assert len(sessions.children) == 1
    assert len(sessions.injections) == 1
    assert len(sessions.turns) == 1


def test_shipped_disabled_handoff_config_blocks_every_effect(tmp_path):
    import pytest

    from agent.durable_jobs.service import PilotDisabledError
    from agent.durable_jobs.session_handoff import (
        SemanticWaypoint,
        SessionHandoffConfig,
    )

    lane, job = _lane(tmp_path)
    linear, slack, sessions = _Linear(), _Slack(), _Sessions()

    with pytest.raises(PilotDisabledError):
        lane.resume_session_handoff(
            job_id=job.job_id,
            parent_session_id="parent-1",
            handoff=_handoff(),
            waypoint=SemanticWaypoint(verified=True),
            pressure=_armed(),
            linear=linear,
            slack=slack,
            sessions=sessions,
            handoff_config=SessionHandoffConfig.default(),
        )

    assert not linear.effects and not slack.effects and not sessions.children


def test_manual_resume_rejects_parent_session_drift(tmp_path):
    import pytest

    from agent.durable_jobs.session_handoff import (
        HandoffIdentityMismatch,
        SemanticWaypoint,
    )

    lane, job = _lane(tmp_path)
    linear, slack, sessions = _Linear(), _Slack(), _CrashAfterChild()
    kwargs = dict(
        job_id=job.job_id,
        handoff=_handoff(),
        waypoint=SemanticWaypoint(verified=True),
        pressure=_armed(),
        linear=linear,
        slack=slack,
        sessions=sessions,
        handoff_config=_enabled_handoff_config(),
    )

    with pytest.raises(ConnectionError):
        lane.resume_session_handoff(**kwargs, parent_session_id="parent-1")
    with pytest.raises(HandoffIdentityMismatch):
        lane.resume_session_handoff(
            **kwargs, parent_session_id="parent-2", manual_resume=True
        )


def test_direct_advance_cannot_bypass_effect_claims(tmp_path):
    import pytest

    from agent.durable_jobs.session_handoff import SessionHandoffLedger

    lane, job = _lane(tmp_path)
    ledger = SessionHandoffLedger(tmp_path / "jobs.sqlite")
    ledger.stage(job.job_id, "parent-1", _handoff())
    ledger.claim_effect(
        job.job_id, "ho_123", "LINEAR_UPSERT", owner_token="linear-owner"
    )
    ledger.complete_effect(
        job.job_id,
        "ho_123",
        "LINEAR_UPSERT",
        owner_token="linear-owner",
        receipt="linear:ENG-122",
    )

    with pytest.raises(ValueError):
        ledger.advance(
            job.job_id, "ho_123", "CHILD_CREATED", child_session_id="child-1"
        )
    state = ledger.get(job.job_id, "ho_123")

    assert state is not None
    assert state.stage == "LINEAR_VERIFIED"
    assert state.checkpoint_stage == "LINEAR_VERIFIED"
    assert state.child_session_id is None


def test_failed_closed_checkpoint_cannot_advance_without_manual_resume(tmp_path):
    import pytest

    from agent.durable_jobs.session_handoff import SessionHandoffLedger

    _, job = _lane(tmp_path)
    ledger = SessionHandoffLedger(tmp_path / "jobs.sqlite")
    ledger.stage(job.job_id, "parent-1", _handoff())
    ledger.claim_effect(
        job.job_id, "ho_123", "LINEAR_UPSERT", owner_token="linear-owner"
    )
    ledger.complete_effect(
        job.job_id,
        "ho_123",
        "LINEAR_UPSERT",
        owner_token="linear-owner",
        receipt="linear:ENG-122",
    )
    failed = ledger.fail_closed(job.job_id, "ho_123", "ConnectionError")

    with pytest.raises(ValueError):
        ledger.advance(job.job_id, "ho_123", "SLACK_RECEIPTED")
    stale = ledger.get(job.job_id, "ho_123")

    assert failed.stage == "FAILED_CLOSED"
    assert stale is not None
    assert stale.stage == "FAILED_CLOSED"
    assert stale.checkpoint_stage == "LINEAR_VERIFIED"
    assert stale.failure_reason == "ConnectionError"
    assert "manual_resume=True" in stale.manual_resume_action


def test_concurrent_caller_cannot_steal_inflight_effect(tmp_path):
    import threading

    import pytest

    from agent.durable_jobs.session_handoff import (
        EffectReconciliationRequired,
        SemanticWaypoint,
        SessionHandoffLedger,
    )

    release_failure = threading.Event()
    failer_inside = threading.Event()

    class RacingSessions(_Sessions):
        def start_first_turn(self, **kwargs):
            if threading.current_thread().name == "failer":
                failer_inside.set()
                assert release_failure.wait(5)
                raise ConnectionError("late failure")
            return super().start_first_turn(**kwargs)

    lane, job = _lane(tmp_path)
    linear, slack, sessions = _Linear(), _Slack(), RacingSessions()
    kwargs = dict(
        job_id=job.job_id,
        parent_session_id="parent-1",
        handoff=_handoff(),
        waypoint=SemanticWaypoint(verified=True),
        pressure=_armed(),
        linear=linear,
        slack=slack,
        sessions=sessions,
        handoff_config=_enabled_handoff_config(),
    )
    failures = []

    def fail_late():
        try:
            lane.resume_session_handoff(**kwargs)
        except BaseException as exc:
            failures.append(exc)

    thread = threading.Thread(target=fail_late, name="failer")
    thread.start()
    assert failer_inside.wait(5)
    with pytest.raises(EffectReconciliationRequired):
        lane.resume_session_handoff(**kwargs)
    assert not release_failure.is_set()

    release_failure.set()
    thread.join(5)
    assert not thread.is_alive()
    assert len(failures) == 1 and isinstance(failures[0], ConnectionError)
    failed = SessionHandoffLedger(tmp_path / "jobs.sqlite").get(job.job_id, "ho_123")
    assert failed is not None
    assert failed.stage == "FAILED_CLOSED"
    assert failed.checkpoint_stage == "HANDOFF_INJECTED"

    with pytest.raises(EffectReconciliationRequired):
        lane.resume_session_handoff(**kwargs, manual_resume=True)
    failed_claim = SessionHandoffLedger(tmp_path / "jobs.sqlite").get_effect(
        job.job_id, "ho_123", "FIRST_TURN_START"
    )
    lane.reconcile_session_handoff_effect(
        job_id=job.job_id,
        handoff_id="ho_123",
        effect_name="FIRST_TURN_START",
        outcome="NOT_APPLIED",
        expected_owner_token=failed_claim.owner_token,
        expected_generation=failed_claim.generation,
        dead_owner_verified=True,
        handoff_config=_enabled_handoff_config(),
    )
    complete = lane.resume_session_handoff(**kwargs, manual_resume=True)
    assert complete.stage == "COMPLETE"
    assert len(sessions.turns) == 1


def test_control_fields_are_secret_safe_at_every_external_sink(tmp_path):
    from dataclasses import replace

    from agent.durable_jobs.session_handoff import SemanticWaypoint

    lane, job = _lane(tmp_path)
    linear, slack, sessions = _Linear(), _Slack(), _Sessions()
    handoff = replace(
        _handoff(),
        resume_pointer="durable-job://dj_1/handoffs/ho_123?password=resume-secret",
        next_action="connect password=turn-secret",
    )

    lane.resume_session_handoff(
        job_id=job.job_id,
        parent_session_id="parent-1",
        handoff=handoff,
        waypoint=SemanticWaypoint(verified=True),
        pressure=_armed(),
        linear=linear,
        slack=slack,
        sessions=sessions,
        handoff_config=_enabled_handoff_config(),
    )

    projected = " ".join((*slack.pointers.values(), *sessions.turns.values()))
    assert "resume-secret" not in projected
    assert "turn-secret" not in projected
    assert "[REDACTED]" in projected


def test_handoff_identity_must_match_durable_job_before_effects(tmp_path):
    from dataclasses import replace

    import pytest

    from agent.durable_jobs.session_handoff import (
        HandoffIdentityMismatch,
        SemanticWaypoint,
    )

    lane, job = _lane(tmp_path)
    linear, slack, sessions = _Linear(), _Slack(), _Sessions()

    for handoff in (
        replace(_handoff(), repository="github.com/other/repository"),
        replace(_handoff(), exact_sha="0" * 40),
    ):
        with pytest.raises(HandoffIdentityMismatch):
            lane.resume_session_handoff(
                job_id=job.job_id,
                parent_session_id="parent-1",
                handoff=handoff,
                waypoint=SemanticWaypoint(verified=True),
                pressure=_armed(),
                linear=linear,
                slack=slack,
                sessions=sessions,
                handoff_config=_enabled_handoff_config(),
            )

    assert not linear.effects and not slack.effects and not sessions.children


def test_secret_control_identifiers_are_rejected_before_persistence_or_effects(
    tmp_path,
):
    from dataclasses import replace
    import sqlite3

    import pytest

    from agent.durable_jobs.session_handoff import (
        HandoffIdentityMismatch,
        SemanticWaypoint,
    )

    lane, job = _lane(tmp_path)
    linear, slack, sessions = _Linear(), _Slack(), _Sessions()

    for handoff in (
        replace(_handoff(), idempotency_key="password=idempotency-secret"),
        replace(_handoff(), issue="password=issue-secret"),
    ):
        with pytest.raises(HandoffIdentityMismatch):
            lane.resume_session_handoff(
                job_id=job.job_id,
                parent_session_id="parent-1",
                handoff=handoff,
                waypoint=SemanticWaypoint(verified=True),
                pressure=_armed(),
                linear=linear,
                slack=slack,
                sessions=sessions,
                handoff_config=_enabled_handoff_config(),
            )

    with sqlite3.connect(tmp_path / "jobs.sqlite") as conn:
        count = conn.execute("SELECT COUNT(*) FROM session_handoffs").fetchone()[0]
    assert count == 0
    assert not linear.effects and not slack.effects and not sessions.children


def test_missing_frozen_baseline_sha_fails_closed_before_effects(tmp_path):
    import sqlite3

    import pytest

    from agent.durable_jobs.session_handoff import (
        HandoffIdentityMismatch,
        SemanticWaypoint,
    )

    lane, job = _lane(tmp_path)
    with sqlite3.connect(tmp_path / "jobs.sqlite") as conn:
        conn.execute(
            "UPDATE durable_jobs SET frozen_baseline_sha='' WHERE job_id=?",
            (job.job_id,),
        )
    linear, slack, sessions = _Linear(), _Slack(), _Sessions()

    with pytest.raises(HandoffIdentityMismatch):
        lane.resume_session_handoff(
            job_id=job.job_id,
            parent_session_id="parent-1",
            handoff=_handoff(),
            waypoint=SemanticWaypoint(verified=True),
            pressure=_armed(),
            linear=linear,
            slack=slack,
            sessions=sessions,
            handoff_config=_enabled_handoff_config(),
        )

    assert not linear.effects and not slack.effects and not sessions.children


def test_effect_claim_is_durable_and_exclusive(tmp_path):
    from agent.durable_jobs.session_handoff import SessionHandoffLedger

    _, job = _lane(tmp_path)
    ledger = SessionHandoffLedger(tmp_path / "jobs.sqlite")
    ledger.stage(job.job_id, "parent-1", _handoff())

    first = ledger.claim_effect(
        job.job_id,
        "ho_123",
        "LINEAR_UPSERT",
        owner_token="owner-a",
    )
    second = ledger.claim_effect(
        job.job_id,
        "ho_123",
        "LINEAR_UPSERT",
        owner_token="owner-b",
    )

    assert first.acquired is True
    assert first.status == "IN_FLIGHT"
    assert first.owner_token == "owner-a"
    assert second.acquired is False
    assert second.status == "IN_FLIGHT"
    assert second.owner_token == "owner-a"


def test_effect_claim_has_one_sqlite_winner_under_concurrency(tmp_path):
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from agent.durable_jobs.session_handoff import SessionHandoffLedger

    _lane_service, job = _lane(tmp_path)
    ledger_path = tmp_path / "jobs.sqlite"
    ledger = SessionHandoffLedger(ledger_path)
    ledger.stage(job.job_id, "parent-1", _handoff())
    workers = 24
    barrier = threading.Barrier(workers)

    def compete(index):
        barrier.wait()
        return SessionHandoffLedger(ledger_path).claim_effect(
            job.job_id,
            "ho_123",
            "LINEAR_UPSERT",
            owner_token=f"owner-{index}",
        )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        claims = list(pool.map(compete, range(workers)))

    winners = [claim for claim in claims if claim.acquired]
    assert len(winners) == 1
    persisted = ledger.get_effect(job.job_id, "ho_123", "LINEAR_UPSERT")
    assert persisted is not None
    assert persisted.status == "IN_FLIGHT"
    assert persisted.owner_token == winners[0].owner_token


def test_effect_completion_atomically_advances_handoff(tmp_path):
    from agent.durable_jobs.session_handoff import SessionHandoffLedger

    _, job = _lane(tmp_path)
    ledger = SessionHandoffLedger(tmp_path / "jobs.sqlite")
    ledger.stage(job.job_id, "parent-1", _handoff())
    ledger.claim_effect(
        job.job_id,
        "ho_123",
        "LINEAR_UPSERT",
        owner_token="owner-a",
    )

    state = ledger.complete_effect(
        job.job_id,
        "ho_123",
        "LINEAR_UPSERT",
        owner_token="owner-a",
        receipt="linear:ENG-122",
    )
    claim = ledger.get_effect(job.job_id, "ho_123", "LINEAR_UPSERT")

    assert state.stage == "LINEAR_VERIFIED"
    assert state.linear_receipt == "linear:ENG-122"
    assert claim is not None
    assert claim.status == "APPLIED"
    assert claim.owner_token is None


def test_effect_completion_cannot_skip_stages_or_persist_secret_receipts(tmp_path):
    import pytest

    from agent.durable_jobs.session_handoff import (
        EffectOwnershipLost,
        SessionHandoffLedger,
    )

    _lane_service, job = _lane(tmp_path)
    ledger = SessionHandoffLedger(tmp_path / "jobs.sqlite")
    ledger.stage(job.job_id, "parent-1", _handoff())
    ledger.claim_effect(
        job.job_id,
        "ho_123",
        "CHILD_CREATE",
        owner_token="child-owner",
    )
    with pytest.raises(EffectOwnershipLost):
        ledger.complete_effect(
            job.job_id,
            "ho_123",
            "CHILD_CREATE",
            owner_token="child-owner",
            receipt="child-1",
        )

    ledger.claim_effect(
        job.job_id,
        "ho_123",
        "LINEAR_UPSERT",
        owner_token="linear-owner",
    )
    with pytest.raises(ValueError):
        ledger.complete_effect(
            job.job_id,
            "ho_123",
            "LINEAR_UPSERT",
            owner_token="linear-owner",
            receipt="token=must-not-persist",
        )
    state = ledger.get(job.job_id, "ho_123")
    assert state is not None
    assert state.stage == "STAGED"
    assert state.linear_receipt is None


def test_manual_applied_reconciliation_requires_persisted_verification_evidence(
    tmp_path,
):
    import pytest

    from agent.durable_jobs.session_handoff import (
        ProjectionVerificationError,
        SessionHandoffLedger,
    )

    _lane_service, job = _lane(tmp_path)
    ledger = SessionHandoffLedger(tmp_path / "jobs.sqlite")
    ledger.stage(job.job_id, "parent-1", _handoff())
    _complete_ledger_effect(
        ledger, job.job_id, "ho_123", "LINEAR_UPSERT", "linear:ENG-122"
    )
    _complete_ledger_effect(
        ledger, job.job_id, "ho_123", "SLACK_RECEIPT", "slack:ho_123"
    )
    _complete_ledger_effect(ledger, job.job_id, "ho_123", "CHILD_CREATE", "child-1")
    _complete_ledger_effect(ledger, job.job_id, "ho_123", "HANDOFF_INJECT")
    dead_claim = ledger.claim_effect(
        job.job_id,
        "ho_123",
        "FIRST_TURN_START",
        owner_token="dead-owner",
    )

    with pytest.raises(ProjectionVerificationError):
        ledger.reconcile_effect(
            job_id=job.job_id,
            handoff_id="ho_123",
            effect_name="FIRST_TURN_START",
            outcome="APPLIED",
            expected_owner_token="dead-owner",
            expected_generation=dead_claim.generation,
            dead_owner_verified=True,
        )

    reconciled = ledger.reconcile_effect(
        job_id=job.job_id,
        handoff_id="ho_123",
        effect_name="FIRST_TURN_START",
        outcome="APPLIED",
        receipt="operator:verified:first-turn",
        expected_owner_token="dead-owner",
        expected_generation=dead_claim.generation,
        dead_owner_verified=True,
    )
    assert reconciled.stage == "FIRST_TURN_STARTED"


def test_orphaned_effect_claim_fences_replay_before_external_effects(tmp_path):
    import pytest

    from agent.durable_jobs.session_handoff import (
        EffectReconciliationRequired,
        SemanticWaypoint,
        SessionHandoffLedger,
    )

    lane, job = _lane(tmp_path)
    ledger = SessionHandoffLedger(tmp_path / "jobs.sqlite")
    ledger.stage(job.job_id, "parent-1", _handoff())
    ledger.claim_effect(
        job.job_id,
        "ho_123",
        "LINEAR_UPSERT",
        owner_token="dead-owner",
    )
    linear, slack, sessions = _Linear(), _Slack(), _Sessions()

    with pytest.raises(EffectReconciliationRequired):
        lane.resume_session_handoff(
            job_id=job.job_id,
            parent_session_id="parent-1",
            handoff=_handoff(),
            waypoint=SemanticWaypoint(verified=True),
            pressure=_armed(),
            linear=linear,
            slack=slack,
            sessions=sessions,
            handoff_config=_enabled_handoff_config(),
        )

    assert not linear.effects and not slack.effects and not sessions.children


def test_hard_process_exit_leaves_fence_and_requires_verified_reconciliation(tmp_path):
    import subprocess
    import sys

    import pytest

    from agent.durable_jobs.session_handoff import (
        EffectReconciliationRequired,
        SemanticWaypoint,
        SessionHandoffLedger,
    )

    lane, job = _lane(tmp_path)
    handoff = _handoff()
    ledger_path = tmp_path / "jobs.sqlite"
    ledger = SessionHandoffLedger(ledger_path)
    ledger.stage(job.job_id, "parent-1", handoff)
    _complete_ledger_effect(
        ledger, job.job_id, handoff.handoff_id, "LINEAR_UPSERT", "linear:ENG-122"
    )
    _complete_ledger_effect(
        ledger, job.job_id, handoff.handoff_id, "SLACK_RECEIPT", "slack:ho_123"
    )
    effect_marker = tmp_path / "external-child-effect.txt"

    crash_script = (
        "import os; from pathlib import Path; "
        "from agent.durable_jobs.session_handoff import SessionHandoffLedger; "
        f"ledger=SessionHandoffLedger({str(ledger_path)!r}); "
        f"claim=ledger.claim_effect({job.job_id!r}, {handoff.handoff_id!r}, "
        "'CHILD_CREATE', owner_token='crashed-process-owner'); "
        f"assert claim.acquired; Path({str(effect_marker)!r}).write_text("
        "'child-external-1', encoding='utf-8'); os._exit(91)"
    )
    crashed = subprocess.run([sys.executable, "-c", crash_script], check=False)
    assert crashed.returncode == 91
    assert effect_marker.read_text(encoding="utf-8") == "child-external-1"
    claim = ledger.get_effect(job.job_id, handoff.handoff_id, "CHILD_CREATE")
    assert claim is not None and claim.status == "IN_FLIGHT"

    linear, slack, sessions = _Linear(), _Slack(), _Sessions()
    kwargs = dict(
        job_id=job.job_id,
        parent_session_id="parent-1",
        handoff=handoff,
        waypoint=SemanticWaypoint(verified=True),
        pressure=_armed(),
        linear=linear,
        slack=slack,
        sessions=sessions,
        handoff_config=_enabled_handoff_config(),
    )
    with pytest.raises(EffectReconciliationRequired):
        lane.resume_session_handoff(**kwargs)
    assert not sessions.children

    lane.reconcile_session_handoff_effect(
        job_id=job.job_id,
        handoff_id=handoff.handoff_id,
        effect_name="CHILD_CREATE",
        outcome="APPLIED",
        receipt="child-external-1",
        expected_owner_token="crashed-process-owner",
        expected_generation=claim.generation,
        dead_owner_verified=True,
        handoff_config=_enabled_handoff_config(),
    )
    complete = lane.resume_session_handoff(**kwargs)
    assert complete.stage == "COMPLETE"
    assert complete.child_session_id == "child-external-1"
    assert not sessions.children


def test_manual_resume_requires_an_actual_boolean(tmp_path):
    import pytest

    from agent.durable_jobs.session_handoff import (
        SemanticWaypoint,
        SessionHandoffLedger,
    )

    lane, job = _lane(tmp_path)
    ledger = SessionHandoffLedger(tmp_path / "jobs.sqlite")
    ledger.stage(job.job_id, "parent-1", _handoff())
    ledger.fail_closed(job.job_id, "ho_123", "ConnectionError")
    linear, slack, sessions = _Linear(), _Slack(), _Sessions()

    for invalid in ("false", "true", 1, object()):
        with pytest.raises((TypeError, ValueError)):
            lane.resume_session_handoff(
                job_id=job.job_id,
                parent_session_id="parent-1",
                handoff=_handoff(),
                waypoint=SemanticWaypoint(verified=True),
                pressure=_armed(),
                linear=linear,
                slack=slack,
                sessions=sessions,
                handoff_config=_enabled_handoff_config(),
                manual_resume=invalid,
            )
    assert ledger.get(job.job_id, "ho_123").stage == "FAILED_CLOSED"
    assert not linear.effects and not slack.effects and not sessions.children


def test_reconciliation_requires_exact_dead_owner_witness(tmp_path):
    import pytest

    from agent.durable_jobs.session_handoff import (
        EffectOwnershipLost,
        SessionHandoffLedger,
    )

    _, job = _lane(tmp_path)
    ledger = SessionHandoffLedger(tmp_path / "jobs.sqlite")
    ledger.stage(job.job_id, "parent-1", _handoff())
    claim = ledger.claim_effect(
        job.job_id, "ho_123", "LINEAR_UPSERT", owner_token="live-owner"
    )

    for owner, generation, witness in (
        ("live-owner", claim.generation, False),
        ("wrong-owner", claim.generation, True),
        ("live-owner", claim.generation + 1, True),
    ):
        with pytest.raises(EffectOwnershipLost):
            ledger.reconcile_effect(
                job_id=job.job_id,
                handoff_id="ho_123",
                effect_name="LINEAR_UPSERT",
                outcome="NOT_APPLIED",
                expected_owner_token=owner,
                expected_generation=generation,
                dead_owner_verified=witness,
            )
    persisted = ledger.get_effect(job.job_id, "ho_123", "LINEAR_UPSERT")
    assert persisted.status == "IN_FLIGHT"
    assert persisted.owner_token == "live-owner"


def test_stale_owner_cannot_fail_close_after_reconciliation_and_reassignment(tmp_path):
    from agent.durable_jobs.session_handoff import SessionHandoffLedger

    _, job = _lane(tmp_path)
    ledger = SessionHandoffLedger(tmp_path / "jobs.sqlite")
    ledger.stage(job.job_id, "parent-1", _handoff())
    _complete_ledger_effect(
        ledger, job.job_id, "ho_123", "LINEAR_UPSERT", "linear:ENG-122"
    )
    _complete_ledger_effect(
        ledger, job.job_id, "ho_123", "SLACK_RECEIPT", "slack:ho_123"
    )
    _complete_ledger_effect(ledger, job.job_id, "ho_123", "CHILD_CREATE", "child-1")
    _complete_ledger_effect(ledger, job.job_id, "ho_123", "HANDOFF_INJECT")
    old = ledger.claim_effect(
        job.job_id, "ho_123", "FIRST_TURN_START", owner_token="owner-a"
    )
    ledger.reconcile_effect(
        job_id=job.job_id,
        handoff_id="ho_123",
        effect_name="FIRST_TURN_START",
        outcome="NOT_APPLIED",
        expected_owner_token="owner-a",
        expected_generation=old.generation,
        dead_owner_verified=True,
    )
    current = ledger.claim_effect(
        job.job_id, "ho_123", "FIRST_TURN_START", owner_token="owner-b"
    )

    stale_result = ledger.fail_closed(
        job.job_id,
        "ho_123",
        "ConnectionError",
        effect_name="FIRST_TURN_START",
        expected_owner_token="owner-a",
        expected_generation=old.generation,
    )
    completed = ledger.complete_effect(
        job.job_id,
        "ho_123",
        "FIRST_TURN_START",
        owner_token="owner-b",
        receipt=None,
    )

    assert stale_result.stage == "HANDOFF_INJECTED"
    assert current.generation == old.generation + 1
    assert completed.stage == "FIRST_TURN_STARTED"


def test_token_shaped_receipts_are_rejected_before_persistence(tmp_path):
    import sqlite3

    import pytest

    from agent.durable_jobs.session_handoff import SessionHandoffLedger

    _, job = _lane(tmp_path)
    ledger = SessionHandoffLedger(tmp_path / "jobs.sqlite")
    ledger.stage(job.job_id, "parent-1", _handoff())
    ledger.claim_effect(job.job_id, "ho_123", "LINEAR_UPSERT", owner_token="owner-a")
    secret = "ghp_abcdefghijklmnopqrstuvwxyz0123456789"
    with pytest.raises(ValueError):
        ledger.complete_effect(
            job.job_id,
            "ho_123",
            "LINEAR_UPSERT",
            owner_token="owner-a",
            receipt=secret,
        )
    with sqlite3.connect(tmp_path / "jobs.sqlite") as conn:
        persisted = conn.execute(
            "SELECT linear_receipt FROM session_handoffs WHERE job_id=? AND handoff_id=?",
            (job.job_id, "ho_123"),
        ).fetchone()[0]
    assert persisted is None


def test_reconciliation_is_gated_and_revalidates_frozen_identity(tmp_path):
    from dataclasses import replace
    import sqlite3

    import pytest

    from agent.durable_jobs.lane import PilotDisabledError
    from agent.durable_jobs.session_handoff import (
        HandoffIdentityMismatch,
        SessionHandoffLedger,
    )

    lane, job = _lane(tmp_path)
    ledger = SessionHandoffLedger(tmp_path / "jobs.sqlite")
    ledger.stage(job.job_id, "parent-1", _handoff())
    claim = ledger.claim_effect(
        job.job_id, "ho_123", "LINEAR_UPSERT", owner_token="dead-owner"
    )
    kwargs = dict(
        job_id=job.job_id,
        handoff_id="ho_123",
        effect_name="LINEAR_UPSERT",
        outcome="NOT_APPLIED",
        expected_owner_token="dead-owner",
        expected_generation=claim.generation,
        dead_owner_verified=True,
    )
    with pytest.raises(PilotDisabledError):
        lane.reconcile_session_handoff_effect(
            **kwargs, handoff_config=replace(_enabled_handoff_config(), shadow=True)
        )
    with sqlite3.connect(tmp_path / "jobs.sqlite") as conn:
        conn.execute(
            "UPDATE durable_jobs SET frozen_baseline_sha=? WHERE job_id=?",
            ("b" * 40, job.job_id),
        )
    with pytest.raises(HandoffIdentityMismatch):
        lane.reconcile_session_handoff_effect(
            **kwargs, handoff_config=_enabled_handoff_config()
        )
    assert (
        ledger.get_effect(job.job_id, "ho_123", "LINEAR_UPSERT").status == "IN_FLIGHT"
    )


def test_effect_schema_migrates_reconciliation_columns_idempotently(tmp_path):
    import sqlite3

    from agent.durable_jobs.session_handoff import SessionHandoffLedger

    path = tmp_path / "upgrade.sqlite"
    with sqlite3.connect(path) as conn:
        conn.execute(
            """CREATE TABLE session_handoff_effects (
                job_id TEXT NOT NULL,
                handoff_id TEXT NOT NULL,
                effect_name TEXT NOT NULL,
                status TEXT NOT NULL,
                owner_token TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(job_id, handoff_id, effect_name)
            )"""
        )
    SessionHandoffLedger(path)
    SessionHandoffLedger(path)
    with sqlite3.connect(path) as conn:
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(session_handoff_effects)")
        }
    assert {"generation", "reconciliation_receipt"} <= columns
