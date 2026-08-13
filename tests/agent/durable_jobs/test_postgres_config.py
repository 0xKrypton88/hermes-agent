"""ENG-25 — PostgreSQL backend selection, default-deny, and secret hygiene."""

from __future__ import annotations

from pathlib import Path

import pytest


SECRET_DSN = "postgresql://hermes:supersecret@localhost:5432/durable_jobs"
CHECKPOINT_DSN = "postgresql://hermes:supersecret@localhost:5432/durable_jobs"
OTHER_DSN = "postgresql://hermes:supersecret@localhost:5432/other_jobs"


def _sqlite_section(tmp_path: Path) -> dict:
    return {
        "sqlite_path": str(tmp_path / "jobs.sqlite"),
        "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
    }


def _pg_section() -> dict:
    return {
        "backend": "postgresql",
        "postgres_dsn": SECRET_DSN,
        "postgres_schema": "durable_jobs_app",
        "checkpoint_postgres_dsn": CHECKPOINT_DSN,
        "checkpoint_postgres_schema": "durable_jobs_ckpt",
    }


def test_defaults_remain_disabled_without_backend_or_postgres_fields():
    from agent.durable_jobs.config import (
        DEFAULT_DURABLE_JOBS_CONFIG,
        load_durable_jobs_config,
    )

    for key in (
        "backend",
        "postgres_dsn",
        "postgres_schema",
        "checkpoint_postgres_dsn",
        "checkpoint_postgres_schema",
    ):
        assert DEFAULT_DURABLE_JOBS_CONFIG.get(key) in (None, "", False)

    cfg = load_durable_jobs_config({})
    assert cfg.enabled is False
    assert cfg.dispatch_enabled is False
    assert cfg.dispatch_allowed is False
    assert cfg.backend is None
    assert cfg.resolved_backend is None
    assert cfg.postgres_dsn is None
    assert cfg.postgres_schema is None
    assert cfg.checkpoint_postgres_dsn is None
    assert cfg.checkpoint_postgres_schema is None
    assert cfg.sqlite_path is None
    assert cfg.checkpoint_sqlite_path is None


def test_sqlite_paths_without_explicit_backend_keep_sqlite_dev_behavior(tmp_path):
    from agent.durable_jobs.config import load_durable_jobs_config

    cfg = load_durable_jobs_config({"durable_jobs": {**_sqlite_section(tmp_path)}})
    assert cfg.backend is None
    assert cfg.resolved_backend == "sqlite"
    assert cfg.postgres_dsn is None


def test_explicit_sqlite_backend_is_accepted(tmp_path):
    from agent.durable_jobs.config import load_durable_jobs_config

    cfg = load_durable_jobs_config(
        {"durable_jobs": {"backend": "sqlite", **_sqlite_section(tmp_path)}}
    )
    assert cfg.backend == "sqlite"
    assert cfg.resolved_backend == "sqlite"


def test_explicit_postgresql_backend_requires_both_dsns_and_schemas():
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config({"durable_jobs": {"backend": "postgresql"}})
    msg = str(exc.value)
    assert "postgres_dsn" in msg or "DSN" in msg
    assert "supersecret" not in msg
    assert SECRET_DSN not in msg


@pytest.mark.parametrize(
    "missing",
    [
        "postgres_dsn",
        "postgres_schema",
        "checkpoint_postgres_dsn",
        "checkpoint_postgres_schema",
    ],
)
def test_postgresql_backend_rejects_missing_dsn_or_schema(missing):
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    section = _pg_section()
    section[missing] = None
    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config({"durable_jobs": section})
    msg = str(exc.value)
    assert missing in msg
    assert "supersecret" not in msg


def test_mixed_sqlite_paths_and_postgres_dsn_rejected(tmp_path):
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    section = {**_sqlite_section(tmp_path), **_pg_section()}
    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config({"durable_jobs": section})
    msg = str(exc.value).lower()
    assert "mixed" in msg or "ambiguous" in msg
    assert "supersecret" not in str(exc.value)


def test_postgresql_backend_with_sqlite_paths_is_rejected(tmp_path):
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config(
            {
                "durable_jobs": {
                    "backend": "postgresql",
                    **_pg_section(),
                    **_sqlite_section(tmp_path),
                }
            }
        )
    assert "supersecret" not in str(exc.value)


def test_sqlite_backend_with_postgres_dsn_is_rejected(tmp_path):
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config(
            {
                "durable_jobs": {
                    "backend": "sqlite",
                    **_sqlite_section(tmp_path),
                    "postgres_dsn": SECRET_DSN,
                }
            }
        )
    assert "supersecret" not in str(exc.value)


def test_postgres_fields_without_explicit_backend_are_rejected():
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    section = _pg_section()
    section.pop("backend")
    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config({"durable_jobs": section})
    msg = str(exc.value).lower()
    assert "backend" in msg
    assert "supersecret" not in str(exc.value)


def test_identical_application_and_checkpointer_schema_identity_rejected():
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config(
            {
                "durable_jobs": {
                    "backend": "postgresql",
                    "postgres_dsn": SECRET_DSN,
                    "postgres_schema": "shared_schema",
                    "checkpoint_postgres_dsn": CHECKPOINT_DSN,
                    "checkpoint_postgres_schema": "shared_schema",
                }
            }
        )
    msg = str(exc.value).lower()
    assert "schema" in msg
    assert "supersecret" not in str(exc.value)


def test_same_schema_name_on_distinct_databases_is_allowed():
    from agent.durable_jobs.config import load_durable_jobs_config

    cfg = load_durable_jobs_config(
        {
            "durable_jobs": {
                "backend": "postgresql",
                "postgres_dsn": SECRET_DSN,
                "postgres_schema": "durable_jobs",
                "checkpoint_postgres_dsn": OTHER_DSN,
                "checkpoint_postgres_schema": "durable_jobs",
            }
        }
    )
    assert cfg.resolved_backend == "postgresql"
    assert cfg.postgres_schema == "durable_jobs"
    assert cfg.checkpoint_postgres_schema == "durable_jobs"


@pytest.mark.parametrize(
    "schema",
    [
        "app;drop",
        "app-schema",
        '"quoted"',
        "AppSchema",
        "pg_toast",
        "schema.nested",
        "select",
        "1leadingdigit",
        "_hidden",
        "a" * 64,
    ],
)
def test_unsafe_schema_identifiers_rejected(schema):
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config(
            {
                "durable_jobs": {
                    "backend": "postgresql",
                    "postgres_dsn": SECRET_DSN,
                    "postgres_schema": schema,
                    "checkpoint_postgres_dsn": SECRET_DSN,
                    "checkpoint_postgres_schema": "durable_jobs_ckpt",
                }
            }
        )
    assert "schema" in str(exc.value).lower()
    assert "supersecret" not in str(exc.value)


@pytest.mark.parametrize("schema", ["public", "PUBLIC", "", "information_schema"])
def test_unqualified_or_default_schema_identifiers_rejected(schema):
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config(
            {
                "durable_jobs": {
                    "backend": "postgresql",
                    "postgres_dsn": SECRET_DSN,
                    "postgres_schema": schema or None,
                    "checkpoint_postgres_dsn": SECRET_DSN,
                    "checkpoint_postgres_schema": "durable_jobs_ckpt",
                }
            }
        )
    assert "supersecret" not in str(exc.value)


def test_in_memory_sqlite_persistence_rejected(tmp_path):
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config(
            {
                "durable_jobs": {
                    "backend": "sqlite",
                    "sqlite_path": ":memory:",
                    "checkpoint_sqlite_path": str(tmp_path / "ckpt.sqlite"),
                }
            }
        )
    msg = str(exc.value).lower()
    assert "memory" in msg


def test_in_memory_checkpoint_sqlite_rejected(tmp_path):
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config(
            {
                "durable_jobs": {
                    "sqlite_path": str(tmp_path / "jobs.sqlite"),
                    "checkpoint_sqlite_path": "file:memdb1?mode=memory&cache=shared",
                }
            }
        )
    msg = str(exc.value).lower()
    assert "memory" in msg


def test_in_memory_postgres_dsn_rejected():
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config(
            {
                "durable_jobs": {
                    "backend": "postgresql",
                    "postgres_dsn": "postgresql:///:memory:",
                    "postgres_schema": "durable_jobs_app",
                    "checkpoint_postgres_dsn": SECRET_DSN,
                    "checkpoint_postgres_schema": "durable_jobs_ckpt",
                }
            }
        )
    msg = str(exc.value).lower()
    assert "memory" in msg
    assert "supersecret" not in str(exc.value)


def test_unknown_backend_rejected():
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config({"durable_jobs": {"backend": "postgres"}})
    msg = str(exc.value)
    assert "backend" in msg.lower()
    assert "postgresql" in msg.lower() or "sqlite" in msg.lower()


def test_config_repr_and_errors_redact_dsns():
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    cfg = load_durable_jobs_config({"durable_jobs": _pg_section()})
    dumped = repr(cfg)
    assert "supersecret" not in dumped
    assert "postgresql://hermes:supersecret@" not in dumped
    assert cfg.postgres_dsn == SECRET_DSN

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config(
            {
                "durable_jobs": {
                    **_pg_section(),
                    "postgres_schema": "public",
                }
            }
        )
    assert "supersecret" not in str(exc.value)
    assert SECRET_DSN not in str(exc.value)


def test_open_application_store_postgresql_does_not_construct_sqlite(monkeypatch):
    from agent.durable_jobs.backend import open_application_store
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.store import DurableJobStore

    def _boom(*_a, **_k):
        raise AssertionError("SQLite DurableJobStore must not be constructed")

    monkeypatch.setattr(DurableJobStore, "__init__", _boom)
    cfg = load_durable_jobs_config({"durable_jobs": _pg_section()})

    created = {}

    class _FakePgStore:
        def __init__(self, **kwargs):
            created.update(kwargs)

    monkeypatch.setattr(
        "agent.durable_jobs.backend.PostgresDurableJobStore",
        _FakePgStore,
    )
    store = open_application_store(cfg)
    assert isinstance(store, _FakePgStore)
    assert created.get("schema") == "durable_jobs_app"
    assert "supersecret" not in repr(store)


def test_open_application_store_sqlite_still_available(tmp_path):
    from agent.durable_jobs.backend import open_application_store
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.store import DurableJobStore

    cfg = load_durable_jobs_config(
        {"durable_jobs": {"backend": "sqlite", **_sqlite_section(tmp_path)}}
    )
    store = open_application_store(cfg)
    assert isinstance(store, DurableJobStore)


def test_lane_postgresql_does_not_fall_back_to_sqlite(tmp_path, monkeypatch):
    from agent.durable_jobs.config import (
        DurableJobsConfigError,
        load_durable_jobs_config,
    )
    from agent.durable_jobs.lane import DurableLaneService
    from agent.durable_jobs.store import DurableJobStore

    def _boom(*_a, **_k):
        raise AssertionError("lane must not fall back to SQLite DurableJobStore")

    monkeypatch.setattr(DurableJobStore, "__init__", _boom)
    cfg = load_durable_jobs_config(
        {"durable_jobs": {"enabled": True, **_pg_section()}}
    )
    lane = DurableLaneService(config=cfg)
    with pytest.raises(DurableJobsConfigError) as exc:
        lane.set_job_policy(
            job_id="dj_unused",
            policy_version="v1",
            allowed_actors=["alice"],
        )
    assert "sqlite" in str(exc.value).lower() or "fall back" in str(exc.value).lower()
    assert "supersecret" not in str(exc.value)


def test_dispatch_remains_hard_disabled_for_postgresql_config(monkeypatch):
    """Hard-disabled dispatch must raise before any store or adapter effect.

    The fixture uses a non-live redacted DSN on purpose. Connecting would hang
    on some platforms (Windows TCP timeout). Fail closed with zero I/O.
    """
    import time

    from agent.durable_jobs import backend as dj_backend
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.postgres_store import PostgresDurableJobStore
    from agent.durable_jobs.service import DispatchDisabledError, DurableJobService

    effects: list[str] = []

    def _record(name: str):
        def _fn(*_a, **_k):
            effects.append(name)
            raise AssertionError(f"hard-disabled dispatch must not call {name}")

        return _fn

    monkeypatch.setattr(dj_backend, "open_application_store", _record("open_application_store"))
    monkeypatch.setattr(
        PostgresDurableJobStore, "__init__", _record("PostgresDurableJobStore.__init__")
    )
    monkeypatch.setattr(PostgresDurableJobStore, "get_job", _record("PostgresDurableJobStore.get_job"))
    monkeypatch.setattr(
        PostgresDurableJobStore, "append_intent", _record("PostgresDurableJobStore.append_intent")
    )
    monkeypatch.setattr(
        PostgresDurableJobStore, "create_job", _record("PostgresDurableJobStore.create_job")
    )
    monkeypatch.setattr("psycopg.connect", _record("psycopg.connect"))

    class SentinelStore:
        def get_job(self, job_id: str):
            effects.append("sentinel.get_job")
            return object()

        def append_intent(self, *args, **kwargs):
            effects.append("sentinel.append_intent")
            return True

        def create_job(self, *args, **kwargs):
            effects.append("sentinel.create_job")
            return object()

    class FakeDispatch:
        def dispatch(self, job_id: str) -> None:
            effects.append(f"adapter.dispatch:{job_id}")

    cfg = load_durable_jobs_config(
        {"durable_jobs": {"enabled": True, "dispatch_enabled": True, **_pg_section()}}
    )
    adapter = FakeDispatch()

    injected = DurableJobService(
        config=cfg, dispatch_adapter=adapter, store=SentinelStore()
    )
    uninjected = DurableJobService(config=cfg, dispatch_adapter=adapter)

    started = time.monotonic()
    with pytest.raises(DispatchDisabledError):
        injected.attempt_dispatch("job-pg")
    with pytest.raises(DispatchDisabledError):
        uninjected.attempt_dispatch("job-pg")
    elapsed = time.monotonic() - started

    assert elapsed < 2.0, f"dispatch must fail closed promptly, took {elapsed:.3f}s"
    assert effects == []
    assert "supersecret" not in repr(cfg)
