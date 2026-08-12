"""Feature config for the ENG-3 durable-jobs pilot.

Disabled by default. SQLite paths must be supplied explicitly by tests/config —
never touch the production Hermes state DB. This pilot is single-process /
dev-only SQLite; production durable store remains PostgreSQL-first and is NOT
implemented here.

Package 1 dispatch is **hard-disabled** in the service layer (not merely
gated by these flags). ``dispatch_enabled`` is retained for forward-compat
config shape only and cannot enable adapter invocation in Package 1.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional


DEFAULT_DURABLE_JOBS_CONFIG: dict[str, Any] = {
    "enabled": False,
    "dispatch_enabled": False,
    "sqlite_path": None,
    "checkpoint_sqlite_path": None,
}


class DurableJobsConfigError(ValueError):
    """Raised when durable_jobs config is invalid."""


@dataclass(frozen=True)
class DurableJobsConfig:
    enabled: bool
    dispatch_enabled: bool
    sqlite_path: Optional[Path]
    checkpoint_sqlite_path: Optional[Path]

    @property
    def dispatch_allowed(self) -> bool:
        """Always False in Package 1 — dispatch is hard-disabled.

        Flags alone must never authorize external adapter invocation.
        """
        return False


def _require_bool(field: str, value: Any) -> bool:
    if not isinstance(value, bool):
        raise DurableJobsConfigError(
            f"durable_jobs.{field} must be a boolean "
            f"(got {type(value).__name__!r}); "
            "string/int values like 'false'/'0' are rejected"
        )
    return value


def load_durable_jobs_config(raw: Mapping[str, Any] | None) -> DurableJobsConfig:
    section: Mapping[str, Any] = {}
    if isinstance(raw, Mapping):
        maybe = raw.get("durable_jobs")
        if isinstance(maybe, Mapping):
            section = maybe
    merged = {**DEFAULT_DURABLE_JOBS_CONFIG, **dict(section)}
    sqlite = merged.get("sqlite_path")
    checkpoint = merged.get("checkpoint_sqlite_path")
    return DurableJobsConfig(
        enabled=_require_bool("enabled", merged.get("enabled", False)),
        dispatch_enabled=_require_bool(
            "dispatch_enabled", merged.get("dispatch_enabled", False)
        ),
        sqlite_path=Path(sqlite) if sqlite else None,
        checkpoint_sqlite_path=Path(checkpoint) if checkpoint else None,
    )
