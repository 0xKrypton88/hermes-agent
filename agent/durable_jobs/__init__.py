"""LangGraph durable-job pilot (ENG-3 Package 1 + ENG-26/ENG-27 slices).

Isolated, disabled-by-default module. No gateway / Slack / Cursor wiring.
Dispatch is hard-disabled (never invokes adapters). Provider and Slack
effects use injected fakes only, after an explicit durable claim/binding.

SQLite usage here is disposable, explicit-path, single-process, and
dev/test-only. Production durable store remains PostgreSQL-first and is not
provisioned by this package. This SQLite path does not satisfy ENG-25.
"""

from __future__ import annotations

from agent.durable_jobs.config import (
    DEFAULT_DURABLE_JOBS_CONFIG,
    DurableJobsConfig,
    load_durable_jobs_config,
)
from agent.durable_jobs.models import JobPhase
from agent.durable_jobs.store import SCHEMA_VERSION

__all__ = [
    "SCHEMA_VERSION",
    "DEFAULT_DURABLE_JOBS_CONFIG",
    "DurableJobsConfig",
    "JobPhase",
    "load_durable_jobs_config",
]
