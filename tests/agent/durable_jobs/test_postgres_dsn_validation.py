"""ENG-25 — structural PostgreSQL DSN validation at config load."""

from __future__ import annotations

import pytest

SECRET = "supersecret"
VALID = f"postgresql://hermes:{SECRET}@127.0.0.1:5432/durable_jobs"
VALID_CKPT = f"postgresql://hermes:{SECRET}@127.0.0.1:5432/other_jobs"


def _base(**overrides):
    section = {
        "backend": "postgresql",
        "postgres_dsn": VALID,
        "postgres_schema": "durable_jobs_app",
        "checkpoint_postgres_dsn": VALID_CKPT,
        "checkpoint_postgres_schema": "durable_jobs_ckpt",
        "postgres_storage_id": "durable_app",
        "checkpoint_postgres_storage_id": "durable_ckpt",
    }
    section.update(overrides)
    return {"durable_jobs": section}


@pytest.mark.parametrize(
    "bad",
    [
        "mysql://hermes:supersecret@127.0.0.1:3306/durable_jobs",
        "abc",
        "not-a-postgres-dsn",
        "postgresql://",
        "postgresql://127.0.0.1",
        "postgresql://127.0.0.1:5432/",
        "postgres://hermes@127.0.0.1",
        "http://127.0.0.1:5432/durable_jobs",
    ],
)
def test_malformed_and_wrong_protocol_application_dsns_rejected(bad):
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config(_base(postgres_dsn=bad))
    msg = str(exc.value).lower()
    assert "dsn" in msg or "postgres" in msg or "protocol" in msg
    assert SECRET not in str(exc.value)
    if "supersecret" in bad:
        assert "supersecret" not in str(exc.value)


@pytest.mark.parametrize(
    "bad",
    [
        "mysql://hermes:supersecret@127.0.0.1:3306/other_jobs",
        "abc",
        "postgresql://",
        "postgresql://127.0.0.1:5432/",
    ],
)
def test_malformed_checkpoint_dsns_rejected(bad):
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config(_base(checkpoint_postgres_dsn=bad))
    assert SECRET not in str(exc.value)


def test_libpq_keyword_form_with_database_is_accepted():
    from agent.durable_jobs.config import load_durable_jobs_config

    cfg = load_durable_jobs_config(
        _base(
            postgres_dsn=f"host=127.0.0.1 port=5432 dbname=durable_jobs user=hermes password={SECRET}",
            checkpoint_postgres_dsn="host=127.0.0.1 port=5432 dbname=other_jobs user=hermes",
        )
    )
    assert cfg.resolved_backend == "postgresql"
    assert SECRET not in repr(cfg)


def test_libpq_form_without_database_rejected():
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config(
            _base(postgres_dsn="host=127.0.0.1 port=5432 user=hermes")
        )
    msg = str(exc.value).lower()
    assert "dbname" in msg or "database" in msg or "dsn" in msg
