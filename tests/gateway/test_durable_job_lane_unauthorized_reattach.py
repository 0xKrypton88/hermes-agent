"""Unauthorized/unready reattach must retire the owner lane (ENG-36 P1).

A valid/runtime-ready attach followed by reattach with enabled=False,
invalid config, or config-read failure must not leave the previous handle
in ``_LANES``. Ingress (``consume_slack_action_if_active``) must become
inactive for that owner. Double valid attach and distinct-owner attach
keep their existing semantics.

No live Slack/Cursor/network. No Gateway start.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.agent.durable_jobs.package2_support import (
    attach_runtime_ready_lane,
    runtime_ready_transport_kwargs,
)


def _complete(tmp_path: Path, **overrides) -> dict:
    section = {
        "enabled": True,
        "dispatch_enabled": False,
        "backend": "sqlite",
        "sqlite_path": str(tmp_path / "jobs.sqlite"),
        "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
        "cursor_adapter_mode": "injected",
        "slack_adapter_mode": "injected",
        "cursor_secret_ref": "CURSOR_API_KEY",
        "slack_secret_ref": "SLACK_BOT_TOKEN",
        "policy_version": "pol-1",
        "identity_binding": {
            "workspace_id": "T1",
            "repository_identity": "github.com/example/repo",
        },
    }
    section.update(overrides)
    return {"durable_jobs": section}


@pytest.fixture(autouse=True)
def _reset_lane_seam():
    from gateway.durable_job_lane import detach_durable_job_lane

    detach_durable_job_lane()
    yield
    detach_durable_job_lane()


def _seed_bound_job(handle, *, idempotency_key: str):
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.slack_contract import SlackBindingLedger

    store = handle.lane._require_sqlite_path()
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="reattach-ingress",
        repository_identity="github.com/example/repo",
        idempotency_key=idempotency_key,
    )
    SlackBindingLedger(sqlite_path=store.sqlite_path).bind(
        job_id=job.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    DecisionLedger(sqlite_path=store.sqlite_path).set_policy(
        job_id=job.job_id,
        policy_version="pol-1",
        allowed_actors=("U-alice",),
        expires_at="2099-01-01T00:00:00+00:00",
    )
    return job, store


def _go_action(job, *, decision_idempotency_key: str) -> tuple[dict, dict]:
    body = {
        "team": {"id": "T1"},
        "user": {"id": "U-alice"},
        "channel": {"id": "C123"},
        "message": {"thread_ts": "111.222", "ts": "111.222"},
    }
    action = {
        "action_id": "hermes_durable_go",
        "value": json.dumps(
            {
                "job_id": job.job_id,
                "decision_idempotency_key": decision_idempotency_key,
                "policy_version": "pol-1",
                "candidate_id": "cand-1",
                "candidate_version": "v1",
            }
        ),
    }
    return body, action


def _owner_entry(owner):
    from gateway.durable_job_lane import _LANES, _owner_key

    return _LANES.get(_owner_key(owner))


def _assert_owner_lane_retired(owner, previous_handle) -> None:
    from gateway.durable_job_lane import _LANES

    assert getattr(owner, "_durable_job_lane", None) is None
    assert _owner_entry(owner) is None
    assert previous_handle not in _LANES.values()
    assert previous_handle.lane._closed is True
    assert previous_handle.lane._store is None


def _assert_ingress_inactive(job) -> None:
    from gateway.durable_job_lane import consume_slack_action_if_active

    body, action = _go_action(job, decision_idempotency_key="dec-retired")
    result = consume_slack_action_if_active(body, action)
    assert result is None


def test_runner_reattach_disabled_retires_owner_lane_and_ingress(
    tmp_path, monkeypatch
):
    from gateway.durable_job_lane import (
        attach_to_gateway_runner,
        consume_slack_action_if_active,
    )

    runner = SimpleNamespace(_durable_job_lane=None)
    first = attach_to_gateway_runner(
        runner,
        raw_config=_complete(tmp_path),
        **runtime_ready_transport_kwargs(monkeypatch),
    )
    assert first is not None
    assert _owner_entry(runner) is first
    job, store = _seed_bound_job(first, idempotency_key="idem-disabled")
    body, action = _go_action(job, decision_idempotency_key="dec-before")
    before = consume_slack_action_if_active(body, action)
    assert before is not None
    assert before.ok is True

    result = attach_to_gateway_runner(
        runner,
        raw_config=_complete(tmp_path, enabled=False),
        **runtime_ready_transport_kwargs(monkeypatch),
    )
    assert result is None
    _assert_owner_lane_retired(runner, first)
    _assert_ingress_inactive(job)
    conn = sqlite3.connect(store.sqlite_path)
    try:
        (decisions,) = conn.execute(
            "SELECT COUNT(*) FROM job_decisions"
        ).fetchone()
    finally:
        conn.close()
    assert int(decisions) == 1


def test_runner_reattach_invalid_config_retires_owner_lane(
    tmp_path, monkeypatch
):
    from gateway.durable_job_lane import attach_to_gateway_runner

    runner = SimpleNamespace(_durable_job_lane=None)
    first = attach_to_gateway_runner(
        runner,
        raw_config=_complete(tmp_path),
        **runtime_ready_transport_kwargs(monkeypatch),
    )
    assert first is not None
    job, _store = _seed_bound_job(first, idempotency_key="idem-invalid")

    result = attach_to_gateway_runner(
        runner,
        raw_config={"durable_jobs": "not-a-mapping"},
        **runtime_ready_transport_kwargs(monkeypatch),
    )
    assert result is None
    _assert_owner_lane_retired(runner, first)
    _assert_ingress_inactive(job)


def test_runner_reattach_config_read_failure_retires_owner_lane(
    tmp_path, monkeypatch
):
    from gateway.durable_job_lane import attach_to_gateway_runner

    runner = SimpleNamespace(_durable_job_lane=None)
    first = attach_to_gateway_runner(
        runner,
        raw_config=_complete(tmp_path),
        **runtime_ready_transport_kwargs(monkeypatch),
    )
    assert first is not None
    job, _store = _seed_bound_job(first, idempotency_key="idem-read-fail")

    def _boom():
        raise RuntimeError("config read failed")

    monkeypatch.setattr("hermes_cli.config.load_config", _boom)
    result = attach_to_gateway_runner(runner)
    assert result is None
    _assert_owner_lane_retired(runner, first)
    _assert_ingress_inactive(job)


def test_attach_durable_job_lane_unready_reattach_retires_owner(
    tmp_path, monkeypatch
):
    from gateway.durable_job_lane import attach_durable_job_lane

    runner = SimpleNamespace(_durable_job_lane=None)
    first = attach_runtime_ready_lane(
        raw_config=_complete(tmp_path),
        monkeypatch=monkeypatch,
        owner=runner,
    )
    assert first is not None
    runner._durable_job_lane = first
    job, _store = _seed_bound_job(first, idempotency_key="idem-direct")

    result = attach_durable_job_lane(
        raw_config=_complete(tmp_path, enabled=False),
        owner=runner,
        **runtime_ready_transport_kwargs(monkeypatch),
    )
    assert result is None
    _assert_owner_lane_retired(runner, first)
    _assert_ingress_inactive(job)


def test_double_valid_attach_still_rejects_without_retiring(
    tmp_path, monkeypatch
):
    from gateway.durable_job_lane import (
        DurableJobLaneAlreadyAttached,
        attach_durable_job_lane,
        consume_slack_action_if_active,
    )

    runner = SimpleNamespace(_durable_job_lane=None)
    first = attach_runtime_ready_lane(
        raw_config=_complete(tmp_path),
        monkeypatch=monkeypatch,
        owner=runner,
    )
    assert first is not None
    job, _store = _seed_bound_job(first, idempotency_key="idem-double")

    with pytest.raises(DurableJobLaneAlreadyAttached):
        attach_durable_job_lane(
            raw_config=_complete(tmp_path),
            owner=runner,
            **runtime_ready_transport_kwargs(monkeypatch),
        )
    assert _owner_entry(runner) is first
    assert first.lane._closed is False
    body, action = _go_action(job, decision_idempotency_key="dec-double")
    result = consume_slack_action_if_active(body, action)
    assert result is not None
    assert result.ok is True


def test_unauthorized_reattach_does_not_retire_sibling_owner(
    tmp_path, monkeypatch
):
    from gateway.durable_job_lane import (
        attach_to_gateway_runner,
        consume_slack_action_if_active,
    )

    owner_a = SimpleNamespace(_durable_job_lane=None)
    owner_b = SimpleNamespace(_durable_job_lane=None)
    lane_a = attach_to_gateway_runner(
        owner_a,
        raw_config=_complete(tmp_path / "a"),
        **runtime_ready_transport_kwargs(monkeypatch),
    )
    lane_b = attach_to_gateway_runner(
        owner_b,
        raw_config=_complete(tmp_path / "b"),
        **runtime_ready_transport_kwargs(monkeypatch),
    )
    assert lane_a is not None
    assert lane_b is not None
    job, _store = _seed_bound_job(lane_b, idempotency_key="idem-sibling")

    result = attach_to_gateway_runner(
        owner_a,
        raw_config=_complete(tmp_path / "a", enabled=False),
        **runtime_ready_transport_kwargs(monkeypatch),
    )
    assert result is None
    _assert_owner_lane_retired(owner_a, lane_a)
    assert _owner_entry(owner_b) is lane_b
    assert owner_b._durable_job_lane is lane_b
    assert lane_b.lane._closed is False
    body, action = _go_action(job, decision_idempotency_key="dec-sibling")
    live = consume_slack_action_if_active(body, action)
    assert live is not None
    assert live.ok is True
