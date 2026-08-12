"""Durable job service facade for the ENG-3 Package 1 pilot.

Boundaries (Package 1):
- No gateway wiring, Slack action wiring, or Cursor/cloud provider calls.
- External systems are represented only by injected Protocol fakes for later
  packages; Package 1 never invokes them.
- Dispatch is **hard-disabled** (not merely configuration-gated):
  ``attempt_dispatch`` always raises and never calls any adapter.
- Application job store DB is distinct from the LangGraph checkpointer DB.
"""

from __future__ import annotations

from typing import Optional, Protocol

from agent.durable_jobs.config import DurableJobsConfig, DurableJobsConfigError
from agent.durable_jobs.graph import run_pilot_graph
from agent.durable_jobs.models import DurableJob, JobPhase
from agent.durable_jobs.store import DurableJobStore


class DispatchDisabledError(RuntimeError):
    """Package 1 hard-disables all external dispatch."""


class PilotDisabledError(RuntimeError):
    """Pilot feature flag is off — safe no-op rejection."""


class DispatchAdapter(Protocol):
    """Injected-only seam reserved for later packages.

    Package 1 stores the reference for API compatibility but never calls it.
    """

    def dispatch(self, job_id: str) -> None: ...


class DurableJobService:
    def __init__(
        self,
        config: DurableJobsConfig,
        dispatch_adapter: Optional[DispatchAdapter] = None,
        store: Optional[DurableJobStore] = None,
    ) -> None:
        self.config = config
        # Retained for constructor compatibility / later packages only.
        # Package 1 must never invoke this adapter.
        self._dispatch_adapter = dispatch_adapter
        self._store = store

    def _require_store(self) -> DurableJobStore:
        if self._store is not None:
            return self._store
        if self.config.sqlite_path is None:
            raise DurableJobsConfigError(
                "durable_jobs.sqlite_path must be set explicitly "
                "(disposable / test path); refusing default Hermes state.db"
            )
        self._store = DurableJobStore(sqlite_path=self.config.sqlite_path)
        return self._store

    def create_and_advance(
        self,
        *,
        origin_platform: str,
        origin_chat_id: str,
        origin_root_thread_id: str,
        objective: str,
        repository_identity: str,
        idempotency_key: str,
        frozen_baseline_sha: str = "",
    ) -> DurableJob:
        if not self.config.enabled:
            raise PilotDisabledError(
                "durable_jobs.enabled is False; Package 1 pilot is a no-op"
            )
        if self.config.checkpoint_sqlite_path is None:
            raise DurableJobsConfigError(
                "durable_jobs.checkpoint_sqlite_path must be set explicitly "
                "and must remain distinct from sqlite_path"
            )
        store = self._require_store()
        assert self.config.sqlite_path is not None
        if self.config.sqlite_path.resolve() == self.config.checkpoint_sqlite_path.resolve():
            raise DurableJobsConfigError(
                "application job store and LangGraph checkpointer must use "
                "distinct sqlite paths"
            )

        job = store.create_job(
            origin_platform=origin_platform,
            origin_chat_id=origin_chat_id,
            origin_root_thread_id=origin_root_thread_id,
            objective=objective,
            repository_identity=repository_identity,
            frozen_baseline_sha=frozen_baseline_sha,
            idempotency_key=idempotency_key,
        )
        # Idempotent re-entry: if already advanced, return as-is.
        if job.phase is JobPhase.AWAIT_DISPATCH:
            return job

        run_pilot_graph(
            store=store,
            checkpoint_sqlite_path=self.config.checkpoint_sqlite_path,
            job_id=job.job_id,
            frozen_baseline_sha=frozen_baseline_sha or job.frozen_baseline_sha,
        )
        advanced = store.get_job(job.job_id)
        assert advanced is not None
        return advanced

    def attempt_dispatch(self, job_id: str) -> dict:
        """Hard-disabled in Package 1 — never invokes any adapter.

        Records a rejected intent when a job exists, then always raises.
        Config flags and injected adapters cannot enable dispatch here.
        """
        if self.config.sqlite_path is not None:
            try:
                store = self._require_store()
                if store.get_job(job_id) is not None:
                    store.append_intent(
                        job_id,
                        event_type="dispatch_requested",
                        payload={"rejected": True, "package": 1, "hard_disabled": True},
                        idempotency_key=f"dispatch_requested:{job_id}",
                    )
            except Exception:
                # Intent recording must not enable dispatch; ignore store misses.
                pass

        # Fail closed: no path may call self._dispatch_adapter.dispatch(...).
        raise DispatchDisabledError(
            "Package 1 hard-disables external dispatch; "
            "adapters are never invoked regardless of enabled/dispatch_enabled"
        )
