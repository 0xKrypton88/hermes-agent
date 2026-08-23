"""LangGraph durable-job pilot (Package 1 + ENG-26/27/25 + Package 2 coupling).

Isolated, disabled-by-default module. Package 2 adds one Gateway lifecycle
seam that constructs the lane only when explicit validated gates pass.
Default remains enabled=false / dispatch off. Live Slack/Cursor clients are
never minted from flags; transports must be injected. Dispatch is still
hard-disabled (never invokes adapters).
"""

from __future__ import annotations

from agent.durable_jobs import legacy_migration, writer_authority
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
    "legacy_migration",
    "load_durable_jobs_config",
    "writer_authority",
]
