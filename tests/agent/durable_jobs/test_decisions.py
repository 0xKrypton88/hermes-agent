"""ENG-27 — durable Go/Hold/Cancel decision contract (isolated, default-off).

Fail closed. Cancel is terminal. No Slack routing fork and no live authorization
bypass. Policy is an explicit ledger, not Slack history.
"""

from __future__ import annotations

from pathlib import Path

import pytest


def _db(tmp_path: Path) -> Path:
    return tmp_path / "pilot_jobs.sqlite"


def _ready_job(tmp_path: Path, *, idempotency_key: str = "idem-dec"):
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.slack_contract import SlackBindingLedger
    from agent.durable_jobs.store import DurableJobStore

    store = DurableJobStore(sqlite_path=_db(tmp_path))
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="ENG-27 decisions",
        repository_identity="github.com/example/repo",
        idempotency_key=idempotency_key,
    )
    slack = SlackBindingLedger(sqlite_path=store.sqlite_path)
    slack.bind(
        job_id=job.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    decisions = DecisionLedger(sqlite_path=store.sqlite_path)
    decisions.set_policy(
        job_id=job.job_id,
        policy_version="pol-1",
        allowed_actors=("U-alice",),
        expires_at="2099-01-01T00:00:00+00:00",
    )
    return store, job, decisions


def _decision_kwargs(job_id: str, **overrides):
    base = dict(
        job_id=job_id,
        decision_type="go",
        candidate_id="cand-1",
        candidate_version="v1",
        actor_id="U-alice",
        policy_version="pol-1",
        decision_idempotency_key="dec-1",
    )
    base.update(overrides)
    return base


def test_go_hold_cancel_idempotent_on_same_decision_key(tmp_path):
    from agent.durable_jobs.decisions import DecisionType

    store, job, ledger = _ready_job(tmp_path)
    first = ledger.record_decision(**_decision_kwargs(job.job_id, decision_type="go"))
    second = ledger.record_decision(**_decision_kwargs(job.job_id, decision_type="go"))
    assert first.ok is True
    assert first.status == "accepted"
    assert first.record is not None
    assert first.record.decision_type is DecisionType.GO
    assert second.ok is True
    assert second.status == "duplicate"
    assert second.record is not None
    assert second.record.decision_idempotency_key == first.record.decision_idempotency_key
    assert ledger.count_decisions(job.job_id) == 1


def test_decision_fail_closed_on_unauthorized_mismatch_expired_replayed(tmp_path):
    store, job, ledger = _ready_job(tmp_path)

    unauthorized = ledger.record_decision(
        **_decision_kwargs(job.job_id, actor_id="U-eve", decision_idempotency_key="k-unauth")
    )
    assert unauthorized.ok is False
    assert unauthorized.status == "rejected"
    assert "unauthorized" in unauthorized.reason_codes

    mismatch_policy = ledger.record_decision(
        **_decision_kwargs(
            job.job_id, policy_version="pol-other", decision_idempotency_key="k-pol"
        )
    )
    assert mismatch_policy.ok is False
    assert "mismatch" in mismatch_policy.reason_codes

    mismatch_candidate = ledger.record_decision(
        **_decision_kwargs(
            job.job_id, candidate_version="v9", decision_idempotency_key="k-cand"
        )
    )
    assert mismatch_candidate.ok is False
    assert "mismatch" in mismatch_candidate.reason_codes

    from agent.durable_jobs.decisions import DecisionLedger

    expired_ledger = DecisionLedger(
        sqlite_path=store.sqlite_path,
        now_fn=lambda: "2100-01-01T00:00:00+00:00",
    )
    expired = expired_ledger.record_decision(
        **_decision_kwargs(job.job_id, decision_idempotency_key="k-exp")
    )
    assert expired.ok is False
    assert "expired" in expired.reason_codes

    accepted = ledger.record_decision(
        **_decision_kwargs(job.job_id, decision_type="go", decision_idempotency_key="k-replay")
    )
    assert accepted.ok is True
    replayed = ledger.record_decision(
        **_decision_kwargs(
            job.job_id, decision_type="hold", decision_idempotency_key="k-replay"
        )
    )
    assert replayed.ok is False
    assert "replayed" in replayed.reason_codes


def test_cancel_is_terminal_and_not_weakened_by_later_go_or_hold(tmp_path):
    store, job, ledger = _ready_job(tmp_path)
    cancel = ledger.record_decision(
        **_decision_kwargs(
            job.job_id, decision_type="cancel", decision_idempotency_key="k-cancel"
        )
    )
    assert cancel.ok is True
    assert cancel.status == "accepted"

    go_after = ledger.record_decision(
        **_decision_kwargs(job.job_id, decision_type="go", decision_idempotency_key="k-go-after")
    )
    assert go_after.ok is False
    assert "canceled" in go_after.reason_codes

    hold_after = ledger.record_decision(
        **_decision_kwargs(
            job.job_id, decision_type="hold", decision_idempotency_key="k-hold-after"
        )
    )
    assert hold_after.ok is False
    assert "canceled" in hold_after.reason_codes

    cancel_again = ledger.record_decision(
        **_decision_kwargs(
            job.job_id, decision_type="cancel", decision_idempotency_key="k-cancel"
        )
    )
    assert cancel_again.ok is True
    assert cancel_again.status == "duplicate"
    assert ledger.is_canceled(job.job_id) is True


def test_pre_cancel_go_replay_after_accepted_cancel_is_rejected_as_canceled(tmp_path):
    store, job, ledger = _ready_job(tmp_path)
    go = ledger.record_decision(
        **_decision_kwargs(job.job_id, decision_type="go", decision_idempotency_key="k-go")
    )
    assert go.ok is True
    cancel = ledger.record_decision(
        **_decision_kwargs(
            job.job_id, decision_type="cancel", decision_idempotency_key="k-cancel"
        )
    )
    assert cancel.ok is True

    replay_go = ledger.record_decision(
        **_decision_kwargs(job.job_id, decision_type="go", decision_idempotency_key="k-go")
    )
    assert replay_go.ok is False
    assert replay_go.status == "rejected"
    assert "canceled" in replay_go.reason_codes
    assert ledger.is_canceled(job.job_id) is True


def test_pre_cancel_hold_replay_after_accepted_cancel_is_rejected_as_canceled(tmp_path):
    store, job, ledger = _ready_job(tmp_path)
    hold = ledger.record_decision(
        **_decision_kwargs(
            job.job_id, decision_type="hold", decision_idempotency_key="k-hold"
        )
    )
    assert hold.ok is True
    cancel = ledger.record_decision(
        **_decision_kwargs(
            job.job_id, decision_type="cancel", decision_idempotency_key="k-cancel"
        )
    )
    assert cancel.ok is True

    replay_hold = ledger.record_decision(
        **_decision_kwargs(
            job.job_id, decision_type="hold", decision_idempotency_key="k-hold"
        )
    )
    assert replay_hold.ok is False
    assert replay_hold.status == "rejected"
    assert "canceled" in replay_hold.reason_codes

    cancel_replay = ledger.record_decision(
        **_decision_kwargs(
            job.job_id, decision_type="cancel", decision_idempotency_key="k-cancel"
        )
    )
    assert cancel_replay.ok is True
    assert cancel_replay.status == "duplicate"
    assert ledger.is_canceled(job.job_id) is True


def test_decisions_survive_store_recreation(tmp_path):
    from agent.durable_jobs.decisions import DecisionLedger, DecisionType

    store, job, ledger = _ready_job(tmp_path)
    ledger.record_decision(**_decision_kwargs(job.job_id, decision_type="hold"))
    reopened = DecisionLedger(sqlite_path=store.sqlite_path)
    latest = reopened.latest_accepted(job.job_id)
    assert latest is not None
    assert latest.decision_type is DecisionType.HOLD
    assert latest.actor_id == "U-alice"
    assert latest.policy_version == "pol-1"


def test_decision_without_policy_or_binding_is_unauthorized(tmp_path):
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.store import DurableJobStore

    store = DurableJobStore(sqlite_path=_db(tmp_path))
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="no policy",
        repository_identity="repo",
        idempotency_key="idem-nopolicy",
    )
    ledger = DecisionLedger(sqlite_path=store.sqlite_path)
    result = ledger.record_decision(**_decision_kwargs(job.job_id))
    assert result.ok is False
    assert "unauthorized" in result.reason_codes


def test_decision_paths_rejected_when_pilot_disabled(tmp_path):
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.lane import DurableLaneService
    from agent.durable_jobs.service import PilotDisabledError

    cfg = load_durable_jobs_config(
        {
            "durable_jobs": {
                "enabled": False,
                "dispatch_enabled": False,
                "sqlite_path": str(_db(tmp_path)),
                "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
            }
        }
    )
    lane = DurableLaneService(config=cfg)
    with pytest.raises(PilotDisabledError):
        lane.record_decision(**_decision_kwargs("dj_nope"))
    with pytest.raises(PilotDisabledError):
        lane.set_job_policy(
            job_id="dj_nope",
            policy_version="pol-1",
            allowed_actors=("U-alice",),
        )


MALFORMED_ALLOWED_ACTORS = (
    "U-alice",
    b"U-alice",
    bytearray(b"U-alice"),
    None,
    1,
    True,
    {"U-alice": True},
    (1,),
    (True,),
    (None,),
    (object(),),
    ({"U-alice": True},),
    (["U-alice"],),
    (("U-alice",),),
    (b"U-alice",),
    ("",),
    ("   ",),
    ("U-alice", 1),
    ("U-alice", ""),
)


def test_set_policy_accepts_stripped_string_sequence(tmp_path):
    import sqlite3

    store, job, ledger = _ready_job(tmp_path, idempotency_key="idem-dec-strip")
    policy = ledger.set_policy(
        job_id=job.job_id,
        policy_version="pol-2",
        allowed_actors=("  U-alice  ", "U-bob"),
        expires_at="2099-01-01T00:00:00+00:00",
    )
    assert policy.allowed_actors == ("U-alice", "U-bob")
    conn = sqlite3.connect(store.sqlite_path)
    try:
        (raw,) = conn.execute(
            "SELECT allowed_actors_json FROM job_authz_policies WHERE job_id = ?",
            (job.job_id,),
        ).fetchone()
    finally:
        conn.close()
    assert "  U-alice  " not in raw
    assert "U-alice" in raw


@pytest.mark.parametrize("allowed_actors", MALFORMED_ALLOWED_ACTORS)
def test_set_policy_rejects_malformed_actors_with_zero_mutation(
    tmp_path, allowed_actors
):
    import sqlite3

    from agent.durable_jobs.decisions import (
        DecisionLedger,
        InvalidAllowedActorsError,
    )
    from agent.durable_jobs.store import DurableJobStore

    store = DurableJobStore(sqlite_path=_db(tmp_path))
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="ENG-27 malformed actors",
        repository_identity="github.com/example/repo",
        idempotency_key="idem-dec-malformed",
    )
    ledger = DecisionLedger(sqlite_path=store.sqlite_path)
    events_before = [
        event["event_type"]
        for event in store.list_events(job.job_id)
        if event["event_type"] == "job_authz_policy_set"
    ]
    with pytest.raises(InvalidAllowedActorsError):
        ledger.set_policy(
            job_id=job.job_id,
            policy_version="pol-1",
            allowed_actors=allowed_actors,
            expires_at="2099-01-01T00:00:00+00:00",
        )
    conn = sqlite3.connect(store.sqlite_path)
    try:
        row = conn.execute(
            "SELECT 1 FROM job_authz_policies WHERE job_id = ?",
            (job.job_id,),
        ).fetchone()
    finally:
        conn.close()
    assert row is None
    events_after = [
        event["event_type"]
        for event in store.list_events(job.job_id)
        if event["event_type"] == "job_authz_policy_set"
    ]
    assert events_after == events_before


def test_set_policy_malformed_update_does_not_clobber_existing(tmp_path):
    store, job, ledger = _ready_job(tmp_path, idempotency_key="idem-dec-noclobber")
    from agent.durable_jobs.decisions import InvalidAllowedActorsError

    with pytest.raises(InvalidAllowedActorsError):
        ledger.set_policy(
            job_id=job.job_id,
            policy_version="pol-evil",
            allowed_actors=(1,),
            expires_at="2020-01-01T00:00:00+00:00",
        )
    import sqlite3

    conn = sqlite3.connect(store.sqlite_path)
    try:
        row = conn.execute(
            """
            SELECT policy_version, allowed_actors_json, expires_at
              FROM job_authz_policies WHERE job_id = ?
            """,
            (job.job_id,),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] == "pol-1"
    assert "U-alice" in row[1]
    assert row[2] == "2099-01-01T00:00:00+00:00"
    assert all(
        "pol-evil" not in (event.get("payload_json") or "")
        for event in store.list_events(job.job_id)
    )
