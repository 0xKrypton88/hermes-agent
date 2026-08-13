"""ENG-25 — fail-closed schema isolation that aliases cannot bypass."""

from __future__ import annotations

import pytest

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
