"""LangGraph PostgreSQL checkpointer seam (ENG-25).

Uses the separately configured checkpointer DSN + schema. Never MemorySaver.
Never the application job schema.
"""

from __future__ import annotations

from typing import Any

from agent.durable_jobs.config import DurableJobsConfigError, validate_schema_identifier

PostgresSaver = None  # tests patch this; production imports on first open


def _connect_postgres(dsn: str) -> Any:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise DurableJobsConfigError(
            "PostgreSQL backend requires the langgraph-durable-postgres extra"
        ) from exc
    return psycopg.connect(dsn, autocommit=True, row_factory=dict_row)


def open_postgres_checkpointer(*, dsn: str, schema: str) -> tuple[Any, Any]:
    """Open a LangGraph PostgresSaver bound to ``schema`` via search_path."""
    qualified = validate_schema_identifier(schema, "checkpoint_postgres_schema")
    saver_cls = PostgresSaver
    if saver_cls is None:
        try:
            from langgraph.checkpoint.postgres import PostgresSaver as saver_cls
        except ImportError as exc:
            raise DurableJobsConfigError(
                "PostgreSQL checkpointer requires the langgraph-durable-postgres extra"
            ) from exc
    conn = _connect_postgres(dsn)
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified}")
    conn.execute(f"SET search_path TO {qualified}")
    saver = saver_cls(conn)
    saver.setup()
    return saver, conn
