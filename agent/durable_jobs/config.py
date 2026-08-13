"""Feature config for the ENG-3 durable-jobs pilot.

Disabled by default. Persistence backend is explicit: SQLite remains the
dev/test path; PostgreSQL is opt-in and never inferred from DSN fields
alone. Package 1 dispatch is **hard-disabled** in the service layer (not
merely gated by these flags). ``dispatch_enabled`` is retained for
forward-compat config shape only and cannot enable adapter invocation.
"""

from __future__ import annotations

import re
from collections.abc import Mapping as MappingABC
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional
from urllib.parse import unquote, urlparse

from agent.durable_jobs.redaction import redact_secret_text


BACKEND_SQLITE = "sqlite"
BACKEND_POSTGRESQL = "postgresql"
_ALLOWED_BACKENDS = frozenset({BACKEND_SQLITE, BACKEND_POSTGRESQL})

_SAFE_SCHEMA_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_UNQUALIFIED_SCHEMAS = frozenset(
    {"public", "information_schema", "pg_catalog", "pg_toast"}
)
_RESERVED_SCHEMA_NAMES = frozenset(
    {
        "select",
        "table",
        "user",
        "check",
        "order",
        "group",
        "primary",
        "foreign",
        "schema",
        "database",
        "index",
        "constraint",
    }
)
_POSTGRES_KEYS = (
    "postgres_dsn",
    "postgres_schema",
    "checkpoint_postgres_dsn",
    "checkpoint_postgres_schema",
)

DEFAULT_DURABLE_JOBS_CONFIG: dict[str, Any] = {
    "enabled": False,
    "dispatch_enabled": False,
    "backend": None,
    "sqlite_path": None,
    "checkpoint_sqlite_path": None,
    "postgres_dsn": None,
    "postgres_schema": None,
    "checkpoint_postgres_dsn": None,
    "checkpoint_postgres_schema": None,
}


class DurableJobsConfigError(ValueError):
    """Raised when durable_jobs config is invalid."""

    def __init__(self, message: str) -> None:
        super().__init__(redact_secret_text(message))


def _redact_dsn_value(dsn: Optional[str]) -> str:
    if dsn is None:
        return "None"
    return "'[REDACTED]'"


@dataclass(frozen=True)
class DurableJobsConfig:
    enabled: bool
    dispatch_enabled: bool
    sqlite_path: Optional[Path]
    checkpoint_sqlite_path: Optional[Path]
    backend: Optional[str] = None
    postgres_dsn: Optional[str] = None
    postgres_schema: Optional[str] = None
    checkpoint_postgres_dsn: Optional[str] = None
    checkpoint_postgres_schema: Optional[str] = None

    @property
    def dispatch_allowed(self) -> bool:
        """Always False in Package 1 — dispatch is hard-disabled.

        Flags alone must never authorize external adapter invocation.
        """
        return False

    @property
    def resolved_backend(self) -> Optional[str]:
        if self.backend is not None:
            return self.backend
        if self.sqlite_path is not None or self.checkpoint_sqlite_path is not None:
            return BACKEND_SQLITE
        return None

    def __repr__(self) -> str:
        return (
            "DurableJobsConfig("
            f"enabled={self.enabled!r}, "
            f"dispatch_enabled={self.dispatch_enabled!r}, "
            f"backend={self.backend!r}, "
            f"sqlite_path={self.sqlite_path!r}, "
            f"checkpoint_sqlite_path={self.checkpoint_sqlite_path!r}, "
            f"postgres_dsn={_redact_dsn_value(self.postgres_dsn)}, "
            f"postgres_schema={self.postgres_schema!r}, "
            f"checkpoint_postgres_dsn={_redact_dsn_value(self.checkpoint_postgres_dsn)}, "
            f"checkpoint_postgres_schema={self.checkpoint_postgres_schema!r})"
        )


def _require_bool(field: str, value: Any) -> bool:
    if not isinstance(value, bool):
        raise DurableJobsConfigError(
            f"durable_jobs.{field} must be a boolean "
            f"(got {type(value).__name__!r}); "
            "string/int values like 'false'/'0' are rejected"
        )
    return value


def _require_mapping(label: str, value: Any) -> Mapping[str, Any]:
    # Use collections.abc.Mapping so typed Mapping aliases still pass isinstance.
    if not isinstance(value, MappingABC):
        raise DurableJobsConfigError(
            f"{label} must be a mapping (got {type(value).__name__!r})"
        )
    return value


def _optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, Path):
        text = str(value)
    elif isinstance(value, str):
        text = value
    else:
        raise DurableJobsConfigError(
            f"path/DSN values must be strings (got {type(value).__name__!r})"
        )
    stripped = text.strip()
    return stripped if stripped else None


def _is_in_memory_sqlite(path: Optional[str]) -> bool:
    if path is None:
        return False
    lowered = path.strip().lower()
    if lowered in {":memory:", "file::memory:"}:
        return True
    if "mode=memory" in lowered:
        return True
    return False


def _is_in_memory_dsn(dsn: Optional[str]) -> bool:
    if dsn is None:
        return False
    lowered = dsn.strip().lower()
    return ":memory:" in lowered or "mode=memory" in lowered


def _field_present(section: Mapping[str, Any], key: str) -> bool:
    if key not in section:
        return False
    return _optional_text(section.get(key)) is not None


def _parse_backend(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise DurableJobsConfigError(
            "durable_jobs.backend must be 'sqlite' or 'postgresql'"
        )
    if value not in _ALLOWED_BACKENDS:
        raise DurableJobsConfigError(
            "durable_jobs.backend must be 'sqlite' or 'postgresql'"
        )
    return value


def validate_schema_identifier(value: Any, field: str) -> str:
    text = _optional_text(value)
    if text is None:
        raise DurableJobsConfigError(
            f"durable_jobs.{field} must be an explicit schema name"
        )
    lowered = text.lower()
    if lowered in _UNQUALIFIED_SCHEMAS or lowered.startswith("pg_"):
        raise DurableJobsConfigError(
            f"durable_jobs.{field} must be a qualified application schema "
            "(not public/system)"
        )
    if lowered in _RESERVED_SCHEMA_NAMES:
        raise DurableJobsConfigError(
            f"durable_jobs.{field} is not a safe unquoted schema identifier"
        )
    if not _SAFE_SCHEMA_RE.fullmatch(text) or text != lowered:
        raise DurableJobsConfigError(
            f"durable_jobs.{field} is not a safe unquoted schema identifier"
        )
    return text


def _database_identity(dsn: str) -> tuple[str, int, str]:
    parsed = urlparse(dsn)
    scheme = (parsed.scheme or "").lower()
    if scheme in {"postgres", "postgresql"}:
        host = (parsed.hostname or "").lower()
        port = parsed.port or 5432
        db = unquote((parsed.path or "").lstrip("/")).split("?")[0].lower()
        return (host, port, db)
    # libpq keyword/value form
    kv: dict[str, str] = {}
    for part in dsn.replace(";", " ").split():
        if "=" not in part:
            continue
        key, raw = part.split("=", 1)
        kv[key.strip().lower()] = unquote(raw.strip())
    host = (kv.get("host") or kv.get("hostaddr") or "").lower()
    try:
        port = int(kv.get("port") or "5432")
    except ValueError:
        port = 5432
    db = (kv.get("dbname") or kv.get("database") or "").lower()
    return (host, port, db)


def _schema_identity(dsn: str, schema: str) -> tuple[str, int, str, str]:
    host, port, db = _database_identity(dsn)
    return (host, port, db, schema)


def load_durable_jobs_config(raw: Mapping[str, Any] | None) -> DurableJobsConfig:
    """Load durable_jobs config.

    ``None`` means "use defaults". Any other non-mapping root is rejected.
    When the ``durable_jobs`` key is present it must itself be a mapping —
    strings/lists/None/bools are not silently ignored.
    """
    if raw is None:
        root: Mapping[str, Any] = {}
    else:
        root = _require_mapping("durable_jobs config root", raw)

    if "durable_jobs" in root:
        section = _require_mapping("durable_jobs", root["durable_jobs"])
    else:
        section = {}

    merged = {**DEFAULT_DURABLE_JOBS_CONFIG, **dict(section)}
    enabled = _require_bool("enabled", merged.get("enabled", False))
    dispatch_enabled = _require_bool(
        "dispatch_enabled", merged.get("dispatch_enabled", False)
    )
    backend = _parse_backend(merged.get("backend"))

    sqlite_raw = _optional_text(merged.get("sqlite_path"))
    checkpoint_sqlite_raw = _optional_text(merged.get("checkpoint_sqlite_path"))
    postgres_dsn = _optional_text(merged.get("postgres_dsn"))
    postgres_schema_raw = merged.get("postgres_schema")
    checkpoint_postgres_dsn = _optional_text(merged.get("checkpoint_postgres_dsn"))
    checkpoint_postgres_schema_raw = merged.get("checkpoint_postgres_schema")

    sqlite_present = bool(sqlite_raw or checkpoint_sqlite_raw)
    postgres_present = any(
        _field_present(merged, key) for key in _POSTGRES_KEYS
    )

    if sqlite_present and postgres_present:
        raise DurableJobsConfigError(
            "durable_jobs config is mixed/ambiguous: SQLite paths and "
            "PostgreSQL DSN/schema fields cannot be combined"
        )
    if backend == BACKEND_POSTGRESQL and sqlite_present:
        raise DurableJobsConfigError(
            "durable_jobs config is mixed/ambiguous: backend=postgresql "
            "cannot include sqlite_path/checkpoint_sqlite_path"
        )
    if backend == BACKEND_SQLITE and postgres_present:
        raise DurableJobsConfigError(
            "durable_jobs config is mixed/ambiguous: backend=sqlite "
            "cannot include PostgreSQL DSN/schema fields"
        )
    if postgres_present and backend is None:
        raise DurableJobsConfigError(
            "durable_jobs.backend must be set explicitly to 'postgresql' "
            "when PostgreSQL DSN/schema fields are present"
        )

    if sqlite_raw and _is_in_memory_sqlite(sqlite_raw):
        raise DurableJobsConfigError(
            "durable_jobs.sqlite_path rejects in-memory persistence"
        )
    if checkpoint_sqlite_raw and _is_in_memory_sqlite(checkpoint_sqlite_raw):
        raise DurableJobsConfigError(
            "durable_jobs.checkpoint_sqlite_path rejects in-memory persistence"
        )

    postgres_schema: Optional[str] = None
    checkpoint_postgres_schema: Optional[str] = None
    if backend == BACKEND_POSTGRESQL:
        if postgres_dsn is None:
            raise DurableJobsConfigError("durable_jobs.postgres_dsn is required")
        if checkpoint_postgres_dsn is None:
            raise DurableJobsConfigError(
                "durable_jobs.checkpoint_postgres_dsn is required"
            )
        if _is_in_memory_dsn(postgres_dsn) or _is_in_memory_dsn(
            checkpoint_postgres_dsn
        ):
            raise DurableJobsConfigError(
                "durable_jobs PostgreSQL DSNs reject in-memory persistence"
            )
        postgres_schema = validate_schema_identifier(
            postgres_schema_raw, "postgres_schema"
        )
        checkpoint_postgres_schema = validate_schema_identifier(
            checkpoint_postgres_schema_raw, "checkpoint_postgres_schema"
        )
        app_identity = _schema_identity(postgres_dsn, postgres_schema)
        ckpt_identity = _schema_identity(
            checkpoint_postgres_dsn, checkpoint_postgres_schema
        )
        if app_identity == ckpt_identity:
            raise DurableJobsConfigError(
                "application and checkpointer PostgreSQL schema identity "
                "must be distinct"
            )

    return DurableJobsConfig(
        enabled=enabled,
        dispatch_enabled=dispatch_enabled,
        sqlite_path=Path(sqlite_raw) if sqlite_raw else None,
        checkpoint_sqlite_path=(
            Path(checkpoint_sqlite_raw) if checkpoint_sqlite_raw else None
        ),
        backend=backend,
        postgres_dsn=postgres_dsn,
        postgres_schema=postgres_schema,
        checkpoint_postgres_dsn=checkpoint_postgres_dsn,
        checkpoint_postgres_schema=checkpoint_postgres_schema,
    )
