"""Persistence backend factory for durable jobs.

SQLite remains the explicit dev/test path. PostgreSQL is selected only
when config.backend is postgresql. There is no silent SQLite fallback.
"""

from __future__ import annotations

from typing import Any

from agent.durable_jobs.config import (
    BACKEND_POSTGRESQL,
    BACKEND_SQLITE,
    DurableJobsConfig,
    DurableJobsConfigError,
)
from agent.durable_jobs.postgres_checkpointer import open_postgres_checkpointer
from agent.durable_jobs.postgres_store import PostgresDurableJobStore
from agent.durable_jobs.store import DurableJobStore


def open_application_store(config: DurableJobsConfig) -> Any:
    backend = config.resolved_backend
    if backend == BACKEND_POSTGRESQL:
        if not config.postgres_dsn or not config.postgres_schema:
            raise DurableJobsConfigError(
                "durable_jobs.postgres_dsn and postgres_schema are required"
            )
        from agent.durable_jobs.postgres_identity import (
            verify_configured_target_identities,
        )

        verify_configured_target_identities(config)
        return PostgresDurableJobStore(
            dsn=config.postgres_dsn,
            schema=config.postgres_schema,
        )
    if backend == BACKEND_SQLITE:
        if config.sqlite_path is None:
            raise DurableJobsConfigError(
                "durable_jobs.sqlite_path must be set explicitly "
                "(disposable / test path); refusing default Hermes state.db"
            )
        return DurableJobStore(sqlite_path=config.sqlite_path)
    raise DurableJobsConfigError(
        "durable_jobs persistence backend is not selected; "
        "set backend to 'sqlite' or 'postgresql'"
    )


def open_langgraph_checkpointer(config: DurableJobsConfig) -> tuple[Any, Any]:
    backend = config.resolved_backend
    if backend == BACKEND_POSTGRESQL:
        if (
            not config.checkpoint_postgres_dsn
            or not config.checkpoint_postgres_schema
        ):
            raise DurableJobsConfigError(
                "durable_jobs.checkpoint_postgres_dsn and "
                "checkpoint_postgres_schema are required"
            )
        from agent.durable_jobs.postgres_identity import (
            verify_configured_target_identities,
        )

        verify_configured_target_identities(config)
        return open_postgres_checkpointer(
            dsn=config.checkpoint_postgres_dsn,
            schema=config.checkpoint_postgres_schema,
        )
    if backend == BACKEND_SQLITE:
        from agent.durable_jobs.graph import open_checkpointer as open_sqlite_checkpointer

        if config.checkpoint_sqlite_path is None:
            raise DurableJobsConfigError(
                "durable_jobs.checkpoint_sqlite_path must be set explicitly "
                "and must remain distinct from sqlite_path"
            )
        return open_sqlite_checkpointer(config.checkpoint_sqlite_path)
    raise DurableJobsConfigError(
        "durable_jobs checkpointer backend is not selected; "
        "set backend to 'sqlite' or 'postgresql'"
    )
