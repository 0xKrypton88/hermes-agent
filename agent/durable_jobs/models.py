"""Domain models for the ENG-3 durable-jobs pilot."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, FrozenSet, Tuple


class JobPhase(str, Enum):
    INTAKE = "INTAKE"
    FREEZE_BASELINE = "FREEZE_BASELINE"
    AWAIT_DISPATCH = "AWAIT_DISPATCH"


# Deterministic Package-1 flow only. No actual dispatch terminal beyond await.
ALLOWED_TRANSITIONS: Dict[JobPhase, FrozenSet[JobPhase]] = {
    JobPhase.INTAKE: frozenset({JobPhase.FREEZE_BASELINE}),
    JobPhase.FREEZE_BASELINE: frozenset({JobPhase.AWAIT_DISPATCH}),
    JobPhase.AWAIT_DISPATCH: frozenset(),
}

NONTERMINAL_PHASES: FrozenSet[JobPhase] = frozenset(
    {JobPhase.INTAKE, JobPhase.FREEZE_BASELINE, JobPhase.AWAIT_DISPATCH}
)

DEFAULT_NEXT_ACTION: Dict[JobPhase, str] = {
    JobPhase.INTAKE: "freeze_baseline",
    JobPhase.FREEZE_BASELINE: "await_dispatch",
    JobPhase.AWAIT_DISPATCH: "reject_dispatch_until_enabled",
}


class InvalidPhaseTransition(ValueError):
    """Raised when a phase transition is not allowed."""


@dataclass(frozen=True)
class DurableJob:
    job_id: str
    phase: JobPhase
    origin_platform: str
    origin_chat_id: str
    origin_root_thread_id: str
    objective: str
    repository_identity: str
    frozen_baseline_sha: str
    idempotency_key: str
    next_action: str
    created_at: str
    updated_at: str

    def as_correlation(self) -> Tuple[str, str, str]:
        return (
            self.origin_platform,
            self.origin_chat_id,
            self.origin_root_thread_id,
        )
