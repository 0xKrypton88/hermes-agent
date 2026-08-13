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
