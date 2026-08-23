"""ENG-25 — fail-closed schema isolation that aliases cannot bypass."""

from __future__ import annotations

import pytest
from types import SimpleNamespace

SECRET = "supersecret"
APP_DSN_LOCALHOST = f"postgresql://hermes:{SECRET}@localhost:5432/durable_jobs"
APP_DSN_LOOPBACK = f"postgresql://hermes:{SECRET}@127.0.0.1:5432/durable_jobs"
APP_DSN_V6 = f"postgresql://hermes:{SECRET}@[::1]:5432/durable_jobs"
OTHER_DB_DSN = f"postgresql://hermes:{SECRET}@127.0.0.1:5432/other_jobs"


def _pg(**overrides):
    section = {
        "backend": "postgresql",
        "postgres_dsn": APP_DSN_LOCALHOST,
        "postgres_schema": "durable_jobs_app",
        "checkpoint_postgres_dsn": OTHER_DB_DSN,
        "checkpoint_postgres_schema": "durable_jobs_ckpt",
        "postgres_storage_id": "durable_app",
        "checkpoint_postgres_storage_id": "durable_ckpt",
        "postgres_environment_id": "test",
    }
    section.update(overrides)
    return {"durable_jobs": section}


def test_loopback_host_aliases_same_database_schema_rejected():
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config(
            _pg(
                postgres_dsn=APP_DSN_LOCALHOST,
                checkpoint_postgres_dsn=APP_DSN_LOOPBACK,
                postgres_schema="shared_jobs",
                checkpoint_postgres_schema="shared_jobs",
            )
        )
    msg = str(exc.value).lower()
    assert "distinct" in msg or "identity" in msg or "schema" in msg
    assert SECRET not in str(exc.value)


def test_ipv6_loopback_alias_same_database_schema_rejected():
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config(
            _pg(
                postgres_dsn=APP_DSN_LOOPBACK,
                checkpoint_postgres_dsn=APP_DSN_V6,
                postgres_schema="shared_jobs",
                checkpoint_postgres_schema="shared_jobs",
            )
        )
    assert SECRET not in str(exc.value)


def test_identical_explicit_storage_ids_rejected_even_when_dsns_differ():
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config(
            _pg(
                postgres_storage_id="same-store",
                checkpoint_postgres_storage_id="same-store",
            )
        )
    msg = str(exc.value).lower()
    assert "storage" in msg or "identity" in msg
    assert SECRET not in str(exc.value)


def test_missing_storage_ids_rejected():
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    section = _pg()["durable_jobs"]
    section.pop("postgres_storage_id")
    section.pop("checkpoint_postgres_storage_id")
    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config({"durable_jobs": section})
    msg = str(exc.value)
    assert "storage_id" in msg
    assert SECRET not in msg


def test_live_identity_tuple_conflicts_on_system_db_schema_not_host_text():
    from agent.durable_jobs.postgres_identity import (
        PostgresStorageIdentity,
        identities_share_schema,
    )

    left = PostgresStorageIdentity(
        system_identifier=111, database="durable_jobs", schema="app"
    )
    aliased = PostgresStorageIdentity(
        system_identifier=111, database="durable_jobs", schema="app"
    )
    other_cluster = PostgresStorageIdentity(
        system_identifier=222, database="durable_jobs", schema="app"
    )
    other_schema = PostgresStorageIdentity(
        system_identifier=111, database="durable_jobs", schema="ckpt"
    )
    assert identities_share_schema(left, aliased) is True
    assert identities_share_schema(left, other_cluster) is False
    assert identities_share_schema(left, other_schema) is False


def test_assert_distinct_live_identities_fail_closed():
    from agent.durable_jobs.config import DurableJobsConfigError
    from agent.durable_jobs.postgres_identity import (
        PostgresStorageIdentity,
        assert_distinct_live_identities,
    )

    shared = PostgresStorageIdentity(1, "db", "schema")
    with pytest.raises(DurableJobsConfigError) as exc:
        assert_distinct_live_identities(shared, shared)
    assert SECRET not in str(exc.value)
    assert_distinct_live_identities(
        PostgresStorageIdentity(1, "db", "app"),
        PostgresStorageIdentity(1, "db", "ckpt"),
    )

def test_production_target_identity_verification_reads_both_declared_stores(monkeypatch):
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.postgres_identity import (
        configured_target_identities,
        verify_configured_target_identities,
    )

    config = load_durable_jobs_config(_pg())
    app, checkpoint = configured_target_identities(config)
    rows = [list(app.as_markers().items()), list(checkpoint.as_markers().items())]
    connections = []

    class Cursor:
        def __init__(self, result):
            self.result = result

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, statement):
            assert "durable_target_identity" in statement

        def fetchall(self):
            return self.result

    class Connection:
        def __init__(self, result):
            self.result = result
            self.closed = False

        def cursor(self):
            return Cursor(self.result)

        def close(self):
            self.closed = True

    def connect(_dsn, *, autocommit):
        assert autocommit is True
        connection = Connection(rows[len(connections)])
        connections.append(connection)
        return connection

    monkeypatch.setitem(__import__("sys").modules, "psycopg", SimpleNamespace(connect=connect))
    assert verify_configured_target_identities(config) == (app, checkpoint)
    assert len(connections) == 2
    assert all(connection.closed for connection in connections)


def test_backend_refuses_store_setup_before_persisted_identity_proof(monkeypatch):
    from agent.durable_jobs import backend, postgres_identity
    from agent.durable_jobs.config import load_durable_jobs_config

    config = load_durable_jobs_config(_pg())
    calls = []

    def deny(_config):
        calls.append("verify")
        raise postgres_identity.TargetIdentityError("identity mismatch")

    monkeypatch.setattr(postgres_identity, "verify_configured_target_identities", deny)
    monkeypatch.setattr(
        backend,
        "PostgresDurableJobStore",
        lambda **_kwargs: pytest.fail("store setup must not run before identity proof"),
    )
    with pytest.raises(postgres_identity.TargetIdentityError, match="identity mismatch"):
        backend.open_application_store(config)
    assert calls == ["verify"]
