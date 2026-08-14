"""LangGraph durable-job pilot (ENG-3 Package 1 + ENG-26/ENG-27 + ENG-25 slice).

Isolated, disabled-by-default module. No gateway / Slack / Cursor wiring.
Dispatch is hard-disabled (never invokes adapters). Provider and Slack
effects use injected fakes only, after an explicit durable claim/binding
with persisted owner token, lease fencing, owner-fenced heartbeat, and
bounded recovering state after empty lookup.

SQLite usage here is disposable, explicit-path, single-process, and
dev/test-only. PostgreSQL persistence is an opt-in extra
(`[langgraph-durable-postgres]`) selected only by an explicit backend;
it does not silently fall back to SQLite.
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
