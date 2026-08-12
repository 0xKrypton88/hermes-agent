"""ENG-3 Package 1 — recovery, outbox, and LangGraph phase flow (no dispatch)."""

from __future__ import annotations

from pathlib import Path

import pytest


def _cfg(tmp_path: Path, *, enabled: bool = True):
    from agent.durable_jobs.config import load_durable_jobs_config

    return load_durable_jobs_config(
        {
            "durable_jobs": {
                "enabled": enabled,
                "dispatch_enabled": False,
                "sqlite_path": str(tmp_path / "jobs.sqlite"),
                "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
            }
        }
    )


def test_reopen_recovers_nonterminal_job_phase_and_correlation(tmp_path):
    from agent.durable_jobs.models import JobPhase
    from agent.durable_jobs.service import DurableJobService
    from agent.durable_jobs.store import DurableJobStore

    cfg = _cfg(tmp_path)
    service = DurableJobService(config=cfg)
    created = service.create_and_advance(
        origin_platform="slack",
        origin_chat_id="C9",
        origin_root_thread_id="rt-9",
        objective="recover me",
        repository_identity="github.com/example/repo",
        idempotency_key="idem-recover",
        frozen_baseline_sha="deadbeef",
    )
    assert created.phase is JobPhase.AWAIT_DISPATCH
    job_id = created.job_id

    # Simulate process restart: new store/service on the same disposable DB.
    recovered_store = DurableJobStore(sqlite_path=cfg.sqlite_path)
    recovered = recovered_store.recover_job(job_id)
    assert recovered is not None
    assert recovered.job_id == job_id
    assert recovered.phase is JobPhase.AWAIT_DISPATCH
    assert recovered.origin_platform == "slack"
    assert recovered.origin_chat_id == "C9"
    assert recovered.origin_root_thread_id == "rt-9"
    assert recovered.objective == "recover me"
    assert recovered.frozen_baseline_sha == "deadbeef"


def test_outbox_intent_is_append_only_and_idempotent(tmp_path):
    from agent.durable_jobs.store import DurableJobStore

    cfg = _cfg(tmp_path)
    store = DurableJobStore(sqlite_path=cfg.sqlite_path)
    job = store.create_job(
        origin_platform="cli",
        origin_chat_id="local",
        origin_root_thread_id="root",
        objective="outbox",
        repository_identity="repo",
        idempotency_key="idem-outbox",
    )
    first = store.append_intent(
        job.job_id,
        event_type="dispatch_requested",
        payload={"note": "would dispatch"},
        idempotency_key="intent-1",
    )
    second = store.append_intent(
        job.job_id,
        event_type="dispatch_requested",
        payload={"note": "retry after crash"},
        idempotency_key="intent-1",
    )
    assert first is True
    assert second is False
    events = store.list_events(job.job_id)
    dispatch_events = [e for e in events if e["event_type"] == "dispatch_requested"]
    assert len(dispatch_events) == 1


def test_langgraph_flow_intake_to_await_dispatch_uses_separate_checkpointer_db(tmp_path):
    from agent.durable_jobs.models import JobPhase
    from agent.durable_jobs.service import DurableJobService

    cfg = _cfg(tmp_path)
    service = DurableJobService(config=cfg)
    job = service.create_and_advance(
        origin_platform="cli",
        origin_chat_id="local",
        origin_root_thread_id="root-flow",
        objective="run graph",
        repository_identity="repo",
        idempotency_key="idem-flow",
        frozen_baseline_sha="baseline-sha-1",
    )
    assert job.phase is JobPhase.AWAIT_DISPATCH
    assert job.frozen_baseline_sha == "baseline-sha-1"
    assert job.next_action == "reject_dispatch_until_enabled"

    assert cfg.sqlite_path is not None
    assert cfg.checkpoint_sqlite_path is not None
    assert cfg.sqlite_path.exists()
    assert cfg.checkpoint_sqlite_path.exists()
    assert cfg.sqlite_path.resolve() != cfg.checkpoint_sqlite_path.resolve()


def test_disabled_pilot_create_is_rejected(tmp_path):
    from agent.durable_jobs.service import DurableJobService, PilotDisabledError

    cfg = _cfg(tmp_path, enabled=False)
    service = DurableJobService(config=cfg)
    with pytest.raises(PilotDisabledError):
        service.create_and_advance(
            origin_platform="cli",
            origin_chat_id="local",
            origin_root_thread_id="root",
            objective="nope",
            repository_identity="repo",
            idempotency_key="idem-disabled",
            frozen_baseline_sha="x",
        )
