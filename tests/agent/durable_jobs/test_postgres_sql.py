"""ENG-25 — SQL / transaction contract for the PostgreSQL application store.

These tests exercise helpers and recorded SQL. They do not claim live
PostgreSQL integration and do not mock an integration suite green.
"""

from __future__ import annotations

import pytest


def test_row_lock_cas_and_advisory_lock_sql_are_transactional():
    from agent.durable_jobs.postgres_store import (
        ADVISORY_LOCK_SQL,
        JOB_ROW_LOCK_SQL_TEMPLATE,
        PHASE_CAS_SQL_TEMPLATE,
        transition_sql_batch,
    )

    assert "pg_advisory_xact_lock" in ADVISORY_LOCK_SQL
    assert "FOR UPDATE" in JOB_ROW_LOCK_SQL_TEMPLATE
    assert "WHERE" in PHASE_CAS_SQL_TEMPLATE
    assert "phase" in PHASE_CAS_SQL_TEMPLATE.lower()

    statements = transition_sql_batch("durable_jobs_app")
    joined = "\n".join(statements).upper()
    assert "PG_ADVISORY_XACT_LOCK" in joined
    assert "FOR UPDATE" in joined
    assert "UPDATE" in joined
    assert "INSERT" in joined
    assert "DURABLE_JOBS_APP.DURABLE_JOBS" in joined
    assert "DURABLE_JOBS_APP.DURABLE_JOB_EVENTS" in joined
    assert "PUBLIC." not in joined


def test_create_job_sql_inserts_job_and_event_against_qualified_schema():
    from agent.durable_jobs.postgres_store import create_job_sql_batch

    statements = create_job_sql_batch("durable_jobs_app")
    joined = "\n".join(statements).upper()
    assert "INSERT INTO DURABLE_JOBS_APP.DURABLE_JOBS" in joined
    assert "INSERT INTO DURABLE_JOBS_APP.DURABLE_JOB_EVENTS" in joined
    assert "IDEMPOTENCY_KEY" in joined


def test_schema_marker_check_is_required_before_writes():
    from agent.durable_jobs.postgres_store import (
        UnknownSchemaError,
        fail_closed_for_schema_marker,
    )

    class _Conn:
        def __init__(self, version, tables):
            self.version = version
            self.tables = tables
            self.executed = []

        def execute(self, sql, params=None):
            self.executed.append(sql)
            raise AssertionError("fail-closed marker check must not mutate")

    with pytest.raises(UnknownSchemaError):
        fail_closed_for_schema_marker(
            _Conn(version=None, tables={"durable_jobs"}),
            schema="durable_jobs_app",
            preexisting=True,
        )

    with pytest.raises(UnknownSchemaError):
        fail_closed_for_schema_marker(
            _Conn(version="not-a-schema", tables={"durable_jobs"}),
            schema="durable_jobs_app",
            preexisting=True,
        )

    with pytest.raises(UnknownSchemaError):
        fail_closed_for_schema_marker(
            _Conn(version="9999", tables={"durable_jobs"}),
            schema="durable_jobs_app",
            preexisting=True,
        )


def test_postgres_store_repr_does_not_include_dsn():
    from agent.durable_jobs.postgres_store import PostgresDurableJobStore

    store = PostgresDurableJobStore.__new__(PostgresDurableJobStore)
    store._schema = "durable_jobs_app"
    store._dsn = "postgresql://hermes:supersecret@localhost:5432/durable_jobs"
    dumped = repr(store)
    assert "supersecret" not in dumped
    assert "postgresql://hermes:" not in dumped
    assert "durable_jobs_app" in dumped


def test_checkpointer_seam_binds_checkpoint_schema_and_skips_memory_saver(monkeypatch):
    from agent.durable_jobs import postgres_checkpointer as pc

    assert not hasattr(pc, "MemorySaver")

    recorded = {"sql": [], "saver": None}

    class _Cursor:
        def execute(self, sql, params=None):
            recorded["sql"].append(str(sql))
            return self

        def fetchone(self):
            return None

        def fetchall(self):
            return []

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class _Conn:
        autocommit = True

        def execute(self, sql, params=None):
            recorded["sql"].append(str(sql))
            return _Cursor()

        def cursor(self, **kwargs):
            return _Cursor()

        def close(self):
            recorded["closed"] = True

    def _connect(*_a, **_k):
        return _Conn()

    class _Saver:
        def __init__(self, conn):
            recorded["saver"] = type(self).__name__
            recorded["conn"] = conn

        def setup(self):
            recorded["setup"] = True

    monkeypatch.setattr(pc, "_connect_postgres", _connect)
    monkeypatch.setattr(pc, "PostgresSaver", _Saver)

    saver, conn = pc.open_postgres_checkpointer(
        dsn="postgresql://hermes:supersecret@localhost:5432/durable_jobs",
        schema="durable_jobs_ckpt",
    )
    assert recorded["saver"] == "_Saver"
    assert recorded.get("setup") is True
    joined = "\n".join(recorded["sql"]).lower()
    assert "durable_jobs_ckpt" in joined
    assert "search_path" in joined
    assert "durable_jobs_app" not in joined
    assert "supersecret" not in joined
    conn.close()


def test_checkpointer_factory_does_not_use_sqlite_or_memory_for_postgresql(
    monkeypatch,
):
    from agent.durable_jobs.backend import open_langgraph_checkpointer
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.graph import open_checkpointer as open_sqlite_checkpointer

    def _boom_sqlite(*_a, **_k):
        raise AssertionError("PostgreSQL backend must not open SqliteSaver")

    monkeypatch.setattr(
        "agent.durable_jobs.backend.open_sqlite_checkpointer",
        _boom_sqlite,
        raising=False,
    )
    monkeypatch.setattr(
        "agent.durable_jobs.graph.open_checkpointer",
        _boom_sqlite,
    )

    opened = {}

    def _fake_pg(*, dsn, schema):
        opened["schema"] = schema
        opened["dsn_leaked"] = "supersecret" in str(dsn)
        return ("pg-saver", "pg-conn")

    monkeypatch.setattr(
        "agent.durable_jobs.backend.open_postgres_checkpointer",
        _fake_pg,
    )

    cfg = load_durable_jobs_config(
        {
            "durable_jobs": {
                "backend": "postgresql",
                "postgres_dsn": "postgresql://hermes:supersecret@localhost:5432/durable_jobs",
                "postgres_schema": "durable_jobs_app",
                "checkpoint_postgres_dsn": "postgresql://hermes:supersecret@localhost:5432/durable_jobs",
                "checkpoint_postgres_schema": "durable_jobs_ckpt",
                "postgres_storage_id": "durable_app",
                "checkpoint_postgres_storage_id": "durable_ckpt",
                "postgres_environment_id": "test",
            }
        }
    )
    monkeypatch.setattr(
        "agent.durable_jobs.postgres_identity.verify_configured_target_identities",
        lambda _config: None,
    )
    saver, conn = open_langgraph_checkpointer(cfg)
    assert saver == "pg-saver"
    assert conn == "pg-conn"
    assert opened["schema"] == "durable_jobs_ckpt"
    # Factory may receive the DSN internally; it must not be the sqlite seam.
    assert open_sqlite_checkpointer is not None
