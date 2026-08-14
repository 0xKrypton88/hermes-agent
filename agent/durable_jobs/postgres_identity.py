"""PostgreSQL DSN parsing, logical storage identity, and live cluster identity.

Config-time identity is fail-closed against loopback aliases
(localhost / 127.0.0.1 / ::1 / empty default) that target the same
database+schema. That check is *not* DNS. Authoritative isolation is
the live probe ``(pg_control_system().system_identifier, current_database(),
schema)`` plus required distinct ``postgres_storage_id`` values.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional
from urllib.parse import unquote


_ALLOWED_SCHEMES = frozenset({"postgres", "postgresql"})
_LOOPBACK_HOSTS = frozenset(
    {"localhost", "127.0.0.1", "0.0.0.0", "::1", "[::1]", "0:0:0:0:0:0:0:1"}
)


@dataclass(frozen=True)
class PostgresStorageIdentity:
    system_identifier: int
    database: str
    schema: str


def identities_share_schema(
    left: PostgresStorageIdentity, right: PostgresStorageIdentity
) -> bool:
    return (
        left.system_identifier == right.system_identifier
        and left.database == right.database
        and left.schema == right.schema
    )


def assert_distinct_live_identities(
    left: PostgresStorageIdentity, right: PostgresStorageIdentity
) -> None:
    if identities_share_schema(left, right):
        raise _cfg_error(
            "application and checkpointer PostgreSQL schema identity must be "
            "distinct (live system_identifier + database + schema)"
        )


def _cfg_error(message: str) -> Exception:
    from agent.durable_jobs.config import DurableJobsConfigError

    return DurableJobsConfigError(message)


def validate_postgres_dsn(dsn: str, field: str) -> str:
    """Accept documented PostgreSQL URI or libpq keyword forms only."""
    text = (dsn or "").strip()
    if not text:
        raise _cfg_error(f"durable_jobs.{field} is required")
    lowered = text.lower()
    if "://" in text.split()[0]:
        scheme = text.split("://", 1)[0].lower()
        if scheme not in _ALLOWED_SCHEMES:
            raise _cfg_error(
                f"durable_jobs.{field} must use a postgresql:// or postgres:// scheme"
            )
        host, port, database = _uri_target(text)
    else:
        if any(token in lowered.split()[0] for token in ("mysql://", "http://", "https://")):
            raise _cfg_error(
                f"durable_jobs.{field} must use a postgresql:// or postgres:// scheme"
            )
        if "=" not in text:
            raise _cfg_error(
                f"durable_jobs.{field} is not a usable PostgreSQL DSN"
            )
        host, port, database = _libpq_target(text)
    if not database:
        raise _cfg_error(
            f"durable_jobs.{field} must include a usable database/dbname"
        )
    if not host:
        raise _cfg_error(
            f"durable_jobs.{field} must include a usable connection target"
        )
    del port
    return dsn


def config_schema_identity(dsn: str, schema: str) -> tuple[str, int, str, str]:
    """Logical identity used at config load. Loopback aliases collapse."""
    if "://" in dsn.split()[0]:
        host, port, database = _uri_target(dsn)
    else:
        host, port, database = _libpq_target(dsn)
    return (_host_class(host), port, database, schema)


def probe_live_storage_identity(dsn: str, schema: str) -> PostgresStorageIdentity:
    try:
        import psycopg
    except ImportError as exc:
        raise _cfg_error(
            "PostgreSQL backend requires the langgraph-durable-postgres extra"
        ) from exc
    conn = psycopg.connect(dsn)
    try:
        sys_row = conn.execute(
            "SELECT system_identifier FROM pg_control_system()"
        ).fetchone()
        db_row = conn.execute("SELECT current_database()").fetchone()
    finally:
        conn.close()
    if sys_row is None or db_row is None:
        raise _cfg_error(
            "PostgreSQL live storage identity probe returned no rows"
        )
    system_identifier = int(sys_row[0] if not isinstance(sys_row, Mapping) else next(iter(sys_row.values())))
    database = str(db_row[0] if not isinstance(db_row, Mapping) else next(iter(db_row.values())))
    return PostgresStorageIdentity(
        system_identifier=system_identifier,
        database=database.lower(),
        schema=schema,
    )


def verify_postgres_storage_isolation(config: Any) -> None:
    """Authoritative live check. Call before graph work, never from dispatch."""
    app_dsn = getattr(config, "postgres_dsn", None)
    ckpt_dsn = getattr(config, "checkpoint_postgres_dsn", None)
    app_schema = getattr(config, "postgres_schema", None)
    ckpt_schema = getattr(config, "checkpoint_postgres_schema", None)
    if not (app_dsn and ckpt_dsn and app_schema and ckpt_schema):
        raise _cfg_error(
            "PostgreSQL application and checkpointer DSN/schema are required "
            "for live isolation verification"
        )
    assert_distinct_live_identities(
        probe_live_storage_identity(app_dsn, app_schema),
        probe_live_storage_identity(ckpt_dsn, ckpt_schema),
    )


def _host_class(host: str) -> str:
    raw = (host or "").strip().lower()
    stripped = raw.strip("[]")
    if stripped in _LOOPBACK_HOSTS or stripped == "":
        return "loopback-or-default"
    return stripped


def _uri_target(dsn: str) -> tuple[str, int, str]:
    if "://" not in dsn:
        raise _cfg_error("PostgreSQL URI DSN is malformed")
    _scheme, rest = dsn.split("://", 1)
    cut = len(rest)
    for sep in "/?":
        idx = rest.find(sep)
        if idx >= 0:
            cut = min(cut, idx)
    authority = rest[:cut]
    remainder = rest[cut:]
    if "@" in authority:
        _userinfo, hostport = authority.rsplit("@", 1)
    else:
        hostport = authority
    host = hostport
    port = 5432
    if hostport.startswith("["):
        end = hostport.find("]")
        host = hostport[: end + 1] if end >= 0 else hostport
        after = hostport[end + 1 :] if end >= 0 else ""
        if after.startswith(":"):
            try:
                port = int(after[1:])
            except ValueError:
                port = 5432
    elif ":" in hostport:
        host, port_raw = hostport.rsplit(":", 1)
        try:
            port = int(port_raw)
        except ValueError:
            port = 5432
    query_host = ""
    if remainder.startswith("?"):
        query = remainder[1:]
    elif "?" in remainder:
        path, query = remainder.split("?", 1)
        remainder = path
    else:
        query = ""
    if remainder.startswith("/"):
        remainder = remainder[1:]
    database = unquote(remainder.split("/")[0].split("?")[0])
    for part in query.replace(";", "&").split("&"):
        if part.lower().startswith("host="):
            query_host = unquote(part.split("=", 1)[1])
    host = query_host or host
    return (host, port, database)


def _libpq_target(dsn: str) -> tuple[str, int, str]:
    kv: dict[str, str] = {}
    token = ""
    quote = ""
    key: Optional[str] = None
    for char in dsn.replace(";", " ") + " ":
        if quote:
            if char == quote:
                quote = ""
            else:
                token += char
            continue
        if char in {"'", '"'}:
            quote = char
            continue
        if char.isspace():
            if key is not None:
                kv[key] = token
                key = None
                token = ""
            elif token:
                token = ""
            continue
        if char == "=" and key is None:
            key = token.strip().lower()
            token = ""
            continue
        token += char
    host = kv.get("host") or kv.get("hostaddr") or ""
    try:
        port = int(kv.get("port") or "5432")
    except ValueError:
        port = 5432
    database = kv.get("dbname") or kv.get("database") or ""
    return (host, port, database)
