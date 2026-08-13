"""ENG-3 Package 1 — durable application job store (distinct from checkpointer)."""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest


def _db(tmp_path: Path) -> Path:
    return tmp_path / "pilot_jobs.sqlite"


def test_create_job_assigns_opaque_job_id_and_persists_correlation(tmp_path):
    from agent.durable_jobs.models import JobPhase
    from agent.durable_jobs.store import DurableJobStore

    store = DurableJobStore(sqlite_path=_db(tmp_path))
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="thread-root-1",
        objective="Implement ENG-3 pilot",
        repository_identity="github.com/0xKrypton88/hermes-agent",
        frozen_baseline_sha="",
        idempotency_key="idem-create-1",
    )

    assert job.job_id
    assert not re.fullmatch(r"\d+\.\d+", job.job_id), "job_id must not be a Slack timestamp"
    assert job.phase is JobPhase.INTAKE
    assert job.origin_platform == "slack"
    assert job.origin_chat_id == "C123"
    assert job.origin_root_thread_id == "thread-root-1"
    assert job.objective == "Implement ENG-3 pilot"
    assert job.repository_identity == "github.com/0xKrypton88/hermes-agent"
    assert job.idempotency_key == "idem-create-1"
    assert job.created_at
    assert job.updated_at

    loaded = store.get_job(job.job_id)
    assert loaded is not None
    assert loaded.job_id == job.job_id
    assert loaded.origin_root_thread_id == "thread-root-1"


def test_duplicate_idempotency_key_adopts_original_without_second_record(tmp_path):
    from agent.durable_jobs.store import DurableJobStore

    store = DurableJobStore(sqlite_path=_db(tmp_path))
    first = store.create_job(
        origin_platform="cli",
        origin_chat_id="local",
        origin_root_thread_id="root-a",
        objective="same work",
        repository_identity="repo",
        frozen_baseline_sha="",
        idempotency_key="idem-dup",
    )
    second = store.create_job(
        origin_platform="cli",
        origin_chat_id="local",
        origin_root_thread_id="root-a",
        objective="same work",
        repository_identity="repo",
        frozen_baseline_sha="",
        idempotency_key="idem-dup",
    )
    assert second.job_id == first.job_id
    assert store.count_jobs() == 1


def test_valid_phase_transitions_and_invalid_rejected(tmp_path):
    from agent.durable_jobs.models import InvalidPhaseTransition, JobPhase
    from agent.durable_jobs.store import DurableJobStore

    store = DurableJobStore(sqlite_path=_db(tmp_path))
    job = store.create_job(
        origin_platform="cli",
        origin_chat_id="local",
        origin_root_thread_id="root-b",
        objective="phase walk",
        repository_identity="repo",
        frozen_baseline_sha="",
        idempotency_key="idem-phase",
    )
    store.transition_phase(job.job_id, JobPhase.FREEZE_BASELINE, frozen_baseline_sha="abc123")
    mid = store.get_job(job.job_id)
    assert mid is not None
    assert mid.phase is JobPhase.FREEZE_BASELINE
    assert mid.frozen_baseline_sha == "abc123"

    store.transition_phase(job.job_id, JobPhase.AWAIT_DISPATCH)
    end = store.get_job(job.job_id)
    assert end is not None
    assert end.phase is JobPhase.AWAIT_DISPATCH

    with pytest.raises(InvalidPhaseTransition):
        store.transition_phase(job.job_id, JobPhase.INTAKE)

    with pytest.raises(InvalidPhaseTransition):
        # skip FREEZE_BASELINE from a fresh job
        other = store.create_job(
            origin_platform="cli",
            origin_chat_id="local",
            origin_root_thread_id="root-c",
            objective="bad jump",
            repository_identity="repo",
            frozen_baseline_sha="",
            idempotency_key="idem-bad-jump",
        )
        store.transition_phase(other.job_id, JobPhase.AWAIT_DISPATCH)


def test_application_store_schema_is_local_and_isolated_from_checkpointer_tables(tmp_path):
    from agent.durable_jobs.store import SCHEMA_VERSION, DurableJobStore

    path = _db(tmp_path)
    DurableJobStore(sqlite_path=path)
    conn = sqlite3.connect(path)
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        conn.close()

    assert "durable_jobs" in tables
    assert "durable_job_events" in tables
    assert "provider_effect_claims" in tables
    assert "provider_job_mappings" in tables
    assert "slack_job_bindings" in tables
    assert "job_authz_policies" in tables
    assert "job_decisions" in tables
    assert "checkpoints" not in tables
    assert "checkpoint_blobs" not in tables
    assert SCHEMA_VERSION >= 1


def test_stale_phase_transition_rejected_without_diverging_audit_history(
    tmp_path, monkeypatch
):
    """Compare-and-swap: a stale INTAKE observation must not clobber AWAIT_DISPATCH."""
    from agent.durable_jobs.models import DurableJob, InvalidPhaseTransition, JobPhase
    from agent.durable_jobs.store import DurableJobStore

    store = DurableJobStore(sqlite_path=_db(tmp_path))
    created = store.create_job(
        origin_platform="cli",
        origin_chat_id="local",
        origin_root_thread_id="root-cas",
        objective="cas race",
        repository_identity="repo",
        frozen_baseline_sha="",
        idempotency_key="idem-cas",
    )
    store.transition_phase(
        created.job_id, JobPhase.FREEZE_BASELINE, frozen_baseline_sha="sha-cas"
    )
    store.transition_phase(created.job_id, JobPhase.AWAIT_DISPATCH)

    before = store.get_job(created.job_id)
    assert before is not None
    assert before.phase is JobPhase.AWAIT_DISPATCH
    events_before = store.list_events(created.job_id)

    # Deterministic stale read: first row→job mapping claims INTAKE while DB is AWAIT.
    real_row_to_job = DurableJobStore._row_to_job
    flipped = {"done": False}

    def stale_row_to_job(row):
        job = real_row_to_job(row)
        if not flipped["done"] and job.job_id == created.job_id:
            flipped["done"] = True
            return DurableJob(
                job_id=job.job_id,
                phase=JobPhase.INTAKE,
                origin_platform=job.origin_platform,
                origin_chat_id=job.origin_chat_id,
                origin_root_thread_id=job.origin_root_thread_id,
                objective=job.objective,
                repository_identity=job.repository_identity,
                frozen_baseline_sha=job.frozen_baseline_sha,
                idempotency_key=job.idempotency_key,
                next_action=job.next_action,
                created_at=job.created_at,
                updated_at=job.updated_at,
            )
        return job

    monkeypatch.setattr(
        DurableJobStore, "_row_to_job", staticmethod(stale_row_to_job)
    )

    with pytest.raises(InvalidPhaseTransition):
        store.transition_phase(
            created.job_id,
            JobPhase.FREEZE_BASELINE,
            frozen_baseline_sha="sha-stale",
        )

    after = store.get_job(created.job_id)
    assert after is not None
    assert after.phase is JobPhase.AWAIT_DISPATCH
    assert after.frozen_baseline_sha == "sha-cas"
    assert store.list_events(created.job_id) == events_before
