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
    assert "checkpoints" not in tables
    assert "checkpoint_blobs" not in tables
    assert SCHEMA_VERSION >= 1
