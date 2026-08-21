"""Restart executes only the persisted permitted next_action.

Does not re-analyze the job or jump phases. Package 1 dispatch remains
hard-disabled. SQLite is not PostgreSQL.
"""

from __future__ import annotations

from agent.durable_jobs.models import DEFAULT_NEXT_ACTION, DurableJob, JobPhase
from agent.durable_jobs.store import DurableJobStore


class RestartDenied(RuntimeError):
    """Persisted next_action is not permitted for the recovered phase."""


def permitted_next_action(phase: JobPhase) -> str:
    return DEFAULT_NEXT_ACTION[phase]


def execute_persisted_next_action(
    store: DurableJobStore,
    job_id: str,
    *,
    frozen_baseline_sha: str = "",
) -> DurableJob:
    """Run exactly the persisted permitted next_action for a nonterminal job."""
    job = store.recover_job(job_id)
    if job is None:
        raise KeyError(f"unknown job_id: {job_id}")
    expected = permitted_next_action(job.phase)
    if job.next_action != expected:
        raise RestartDenied(
            f"persisted next_action {job.next_action!r} is not permitted "
            f"for phase {job.phase.value} (expected {expected!r})"
        )
    if job.phase is JobPhase.INTAKE:
        return store.transition_phase(
            job_id,
            JobPhase.FREEZE_BASELINE,
            frozen_baseline_sha=frozen_baseline_sha or job.frozen_baseline_sha,
        )
    if job.phase is JobPhase.FREEZE_BASELINE:
        return store.transition_phase(job_id, JobPhase.AWAIT_DISPATCH)
    from agent.durable_jobs.service import DispatchDisabledError

    raise DispatchDisabledError(
        "persisted next_action is package1_hard_disabled_dispatch; "
        "Package 1 never invokes adapters"
    )
