"""LangGraph PostgreSQL checkpointer seam (ENG-25).

Uses the separately configured checkpointer DSN + schema. Never MemorySaver.
Never the application job schema. Refuses empty/foreign/unmarked schemas.
"""

from __future__ import annotations

from typing import Any

from agent.durable_jobs.config import DurableJobsConfigError, validate_schema_identifier
from agent.durable_jobs.postgres_domain import (
    CHECKPOINTER_DOMAIN,
    DOMAIN_META_KEY,
    OWNER_META_KEY,
    SchemaOccupancy,
    classify_schema_occupancy,
    require_owned_or_vacant,
)

PostgresSaver = None  # tests patch this; production imports on first open

_CHECKPOINT_META = "durable_checkpoint_meta"


def _connect_postgres(dsn: str) -> Any:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise DurableJobsConfigError(
            "PostgreSQL backend requires the langgraph-durable-postgres extra"
        ) from exc
    return psycopg.connect(dsn, autocommit=True, row_factory=dict_row)


def _scalar(row: Any, key: str, index: int = 0) -> Any:
    if row is None:
        return None
    if isinstance(row, dict):
        if key in row:
            return row[key]
        return next(iter(row.values()))
    return row[index]


def _ensure_checkpointer_schema(conn: Any, schema: str) -> None:
    nsp = conn.execute(
        """
        SELECT r.rolname
          FROM pg_namespace n
          JOIN pg_roles r ON r.oid = n.nspowner
         WHERE n.nspname = %s
        """,
        (schema,),
    ).fetchone()
    current = conn.execute("SELECT current_user").fetchone()
    current_role = str(_scalar(current, "current_user", 0) or "")
    owner_role = None if nsp is None else str(_scalar(nsp, "rolname", 0))
    tables_rows = conn.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = %s",
        (schema,),
    ).fetchall()
    tables = set()
    for row in tables_rows or ():
        if isinstance(row, dict):
            tables.add(str(row.get("tablename") or next(iter(row.values()))))
        else:
            tables.add(str(row[0]))
    markers: dict[str, str] = {}
    if _CHECKPOINT_META in tables:
        meta_rows = conn.execute(
            f"SELECT key, value FROM {schema}.{_CHECKPOINT_META}"
        ).fetchall()
        for row in meta_rows or ():
            if isinstance(row, dict):
                markers[str(row["key"])] = str(row["value"])
            else:
                markers[str(row[0])] = str(row[1])
    occupancy = classify_schema_occupancy(
        schema_exists=nsp is not None,
        table_names=frozenset(tables),
        markers=markers,
        owner_role=owner_role,
        current_role=current_role,
        expected_domain=CHECKPOINTER_DOMAIN,
    )
    require_owned_or_vacant(occupancy, schema=schema)
    if occupancy is SchemaOccupancy.VACANT:
        conn.execute(f"CREATE SCHEMA {schema} AUTHORIZATION CURRENT_USER")
        conn.execute(
            f"CREATE TABLE {schema}.{_CHECKPOINT_META} ("
            "key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        for key, value in (
            (DOMAIN_META_KEY, CHECKPOINTER_DOMAIN),
            (OWNER_META_KEY, current_role),
        ):
            conn.execute(
                f"INSERT INTO {schema}.{_CHECKPOINT_META}(key, value) "
                "VALUES (%s, %s)",
                (key, value),
            )


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
    _ensure_checkpointer_schema(conn, qualified)
    conn.execute(f"SET search_path TO {qualified}")
    saver = saver_cls(conn)
    saver.setup()
    return saver, conn
