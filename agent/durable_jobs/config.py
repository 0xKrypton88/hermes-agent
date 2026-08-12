"""Feature config for the ENG-3 durable-jobs pilot.

Disabled by default. SQLite paths must be supplied explicitly by tests/config —
never touch the production Hermes state DB. This pilot is single-process /
dev-only SQLite; production durable store remains PostgreSQL-first and is NOT
implemented here.
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
        """Dispatch requires both the pilot and the dispatch flag."""
        return self.enabled and self.dispatch_enabled


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
        enabled=bool(merged.get("enabled", False)),
        dispatch_enabled=bool(merged.get("dispatch_enabled", False)),
        sqlite_path=Path(sqlite) if sqlite else None,
        checkpoint_sqlite_path=Path(checkpoint) if checkpoint else None,
    )
