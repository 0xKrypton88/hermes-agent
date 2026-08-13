"""ENG-25 live PostgreSQL integration (skip only when the test DSN is missing).

These tests must not pass via mocks. A disposable DSN is required:
``HERMES_DURABLE_JOBS_PG_TEST_DSN``. Absence skips with an explicit
missing-test-DSN reason.
"""

from __future__ import annotations

import os
import uuid
from multiprocessing import Process, Queue

import pytest

_TEST_DSN_ENV = "HERMES_DURABLE_JOBS_PG_TEST_DSN"
_MISSING_DSN_REASON = (
    "missing-test-DSN: HERMES_DURABLE_JOBS_PG_TEST_DSN is unset"
)

pytestmark = pytest.mark.skipif(
    not os.environ.get(_TEST_DSN_ENV),
    reason=_MISSING_DSN_REASON,
)


def _dsn() -> str:
    value = os.environ.get(_TEST_DSN_ENV, "").strip()
    if not value:
        pytest.skip(_MISSING_DSN_REASON)
    return value


def _schema_pair() -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:12]
    return f"djapp{suffix}", f"djckpt{suffix}"


def _drop_schema(dsn: str, schema: str) -> None:
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.fixture
def pg_schemas():
    dsn = _dsn()
    app_schema, ckpt_schema = _schema_pair()
    yield dsn, app_schema, ckpt_schema
    for schema in (app_schema, ckpt_schema):
        try:
            _drop_schema(dsn, schema)
        except Exception:
            pass


def _job_kwargs(idempotency_key: str) -> dict:
    return {
        "origin_platform": "cli",
        "origin_chat_id": "local",
        "origin_root_thread_id": "root-pg",
        "objective": "pg integration",
        "repository_identity": "repo",
        "frozen_baseline_sha": "",
        "idempotency_key": idempotency_key,
    }


def test_initial_application_schema_and_marker(pg_schemas):
    from agent.durable_jobs.postgres_store import PostgresDurableJobStore
    from agent.durable_jobs.store import SCHEMA_VERSION

    dsn, app_schema, _ckpt = pg_schemas
    store = PostgresDurableJobStore(dsn=dsn, schema=app_schema)
    job = store.create_job(**_job_kwargs("idem-pg-init"))
    assert job.job_id
    loaded = store.get_job(job.job_id)
    assert loaded is not None
    assert loaded.idempotency_key == "idem-pg-init"

    import psycopg

    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            f"SELECT value FROM {app_schema}.durable_jobs_meta WHERE key = %s",
            ("schema_version",),
        ).fetchone()
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = %s",
                (app_schema,),
            ).fetchall()
        }
    assert row is not None
    assert int(row[0]) == SCHEMA_VERSION
    assert "durable_jobs" in tables
    assert "durable_job_events" in tables
    assert "checkpoints" not in tables
    assert "checkpoint_blobs" not in tables


def _create_in_process(dsn: str, schema: str, key: str, queue: Queue) -> None:
    from agent.durable_jobs.postgres_store import PostgresDurableJobStore

    store = PostgresDurableJobStore(dsn=dsn, schema=schema)
    job = store.create_job(**_job_kwargs(key))
    queue.put((job.job_id, store.count_jobs()))


def test_duplicate_idempotent_create_under_two_independent_processes(pg_schemas):
    from agent.durable_jobs.postgres_store import PostgresDurableJobStore

    dsn, app_schema, _ckpt = pg_schemas
    # Initialize schema in the parent so both children see a writable store.
    PostgresDurableJobStore(dsn=dsn, schema=app_schema)
    key = f"idem-pg-race-{uuid.uuid4().hex[:8]}"
    queue: Queue = Queue()
    workers = [
        Process(target=_create_in_process, args=(dsn, app_schema, key, queue))
        for _ in range(2)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=30)
        assert worker.exitcode == 0

    first = queue.get(timeout=5)
    second = queue.get(timeout=5)
    assert first[0] == second[0]
    store = PostgresDurableJobStore(dsn=dsn, schema=app_schema)
    assert store.count_jobs() == 1
    adopted = store.get_job_by_idempotency_key(key)
    assert adopted is not None
    assert adopted.job_id == first[0]


def test_stale_phase_transition_rejected_without_audit_divergence(pg_schemas):
    from agent.durable_jobs.models import InvalidPhaseTransition, JobPhase
    from agent.durable_jobs.postgres_store import PostgresDurableJobStore

    dsn, app_schema, _ckpt = pg_schemas
    store = PostgresDurableJobStore(dsn=dsn, schema=app_schema)
    created = store.create_job(**_job_kwargs("idem-pg-stale"))
    store.transition_phase(
        created.job_id, JobPhase.FREEZE_BASELINE, frozen_baseline_sha="sha-pg"
    )
    store.transition_phase(created.job_id, JobPhase.AWAIT_DISPATCH)
    before = store.list_events(created.job_id)

    other = PostgresDurableJobStore(dsn=dsn, schema=app_schema)
    with pytest.raises(InvalidPhaseTransition):
        other.transition_phase(
            created.job_id,
            JobPhase.FREEZE_BASELINE,
            frozen_baseline_sha="sha-stale",
        )

    after = store.get_job(created.job_id)
    assert after is not None
    assert after.phase is JobPhase.AWAIT_DISPATCH
    assert after.frozen_baseline_sha == "sha-pg"
    assert store.list_events(created.job_id) == before


def test_application_and_checkpointer_schemas_are_separate(pg_schemas):
    from agent.durable_jobs.postgres_checkpointer import open_postgres_checkpointer
    from agent.durable_jobs.postgres_store import PostgresDurableJobStore

    dsn, app_schema, ckpt_schema = pg_schemas
    store = PostgresDurableJobStore(dsn=dsn, schema=app_schema)
    store.create_job(**_job_kwargs("idem-pg-sep"))

    saver, conn = open_postgres_checkpointer(dsn=dsn, schema=ckpt_schema)
    try:
        assert type(saver).__name__ == "PostgresSaver"
        assert "Memory" not in type(saver).__name__
        saver.setup()
    finally:
        conn.close()

    import psycopg

    with psycopg.connect(dsn) as probe:
        app_tables = {
            r[0]
            for r in probe.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = %s",
                (app_schema,),
            ).fetchall()
        }
        ckpt_tables = {
            r[0]
            for r in probe.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = %s",
                (ckpt_schema,),
            ).fetchall()
        }
    assert "durable_jobs" in app_tables
    assert "checkpoints" not in app_tables
    assert "checkpoints" in ckpt_tables or "checkpoint_migrations" in ckpt_tables
    assert "durable_jobs" not in ckpt_tables


def test_missing_schema_marker_on_existing_schema_fails_closed(pg_schemas):
    from agent.durable_jobs.postgres_store import PostgresDurableJobStore
    from agent.durable_jobs.store import UnknownSchemaError

    dsn, app_schema, _ckpt = pg_schemas
    store = PostgresDurableJobStore(dsn=dsn, schema=app_schema)
    job = store.create_job(**_job_kwargs("idem-pg-marker"))

    import psycopg

    with psycopg.connect(dsn) as conn:
        conn.execute(
            f"DELETE FROM {app_schema}.durable_jobs_meta WHERE key = %s",
            ("schema_version",),
        )
        conn.commit()
        remaining = conn.execute(
            f"SELECT 1 FROM {app_schema}.durable_jobs_meta WHERE key = %s",
            ("schema_version",),
        ).fetchone()
        count_before = conn.execute(
            f"SELECT COUNT(*) FROM {app_schema}.durable_jobs"
        ).fetchone()[0]
    assert remaining is None

    with pytest.raises(UnknownSchemaError):
        PostgresDurableJobStore(dsn=dsn, schema=app_schema)

    with psycopg.connect(dsn) as conn:
        marker = conn.execute(
            f"SELECT value FROM {app_schema}.durable_jobs_meta WHERE key = %s",
            ("schema_version",),
        ).fetchone()
        count_after = conn.execute(
            f"SELECT COUNT(*) FROM {app_schema}.durable_jobs WHERE job_id = %s",
            (job.job_id,),
        ).fetchone()[0]
    assert marker is None
    assert count_after == 1
    assert count_before == 1
