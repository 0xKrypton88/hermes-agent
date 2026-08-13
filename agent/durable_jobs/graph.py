"""LangGraph phase graph for the ENG-3 Package 1 pilot.

Deterministic flow only: INTAKE -> FREEZE_BASELINE -> AWAIT_DISPATCH.
No actual dispatch node. Persistence uses a SqliteSaver on a *separate*
checkpoint DB path from the application job store.

Later: swap SqliteSaver for a PostgreSQL checkpointer in production; keep the
application job store on its own PostgreSQL schema (see postgres_checkpointer.py).
"""

from __future__ import annotations

import sqlite3
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import Any

from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph

from agent.durable_jobs.models import JobPhase
from agent.durable_jobs.store import DurableJobStore


@dataclass
class PilotGraphState:
    job_id: str
    phase: str = ""
    frozen_baseline_sha: str = ""
    objective: str = ""


def _build_graph(store: DurableJobStore):
    def intake_node(state: PilotGraphState) -> dict[str, Any]:
        job_id = state.job_id
        job = store.get_job(job_id)
        if job is None:
            raise KeyError(job_id)
        return {
            "job_id": job_id,
            "phase": job.phase.value,
            "frozen_baseline_sha": job.frozen_baseline_sha,
            "objective": job.objective,
        }

    def freeze_baseline_node(state: PilotGraphState) -> dict[str, Any]:
        job_id = state.job_id
        sha = state.frozen_baseline_sha or ""
        job = store.get_job(job_id)
        if job is None:
            raise KeyError(job_id)
        if job.phase is JobPhase.INTAKE:
            job = store.transition_phase(
                job_id,
                JobPhase.FREEZE_BASELINE,
                frozen_baseline_sha=sha,
            )
        return {
            "job_id": job_id,
            "phase": job.phase.value,
            "frozen_baseline_sha": job.frozen_baseline_sha,
            "objective": job.objective,
        }

    def await_dispatch_node(state: PilotGraphState) -> dict[str, Any]:
        job_id = state.job_id
        job = store.get_job(job_id)
        if job is None:
            raise KeyError(job_id)
        if job.phase is JobPhase.FREEZE_BASELINE:
            job = store.transition_phase(job_id, JobPhase.AWAIT_DISPATCH)
        # Terminal for Package 1 — no dispatch side effects.
        store.append_intent(
            job_id,
            event_type="await_dispatch_reached",
            payload={"next_action": job.next_action},
            idempotency_key="await_dispatch_reached",
        )
        return {
            "job_id": job_id,
            "phase": job.phase.value,
            "frozen_baseline_sha": job.frozen_baseline_sha,
            "objective": job.objective,
        }

    builder = StateGraph(PilotGraphState)
    builder.add_node("intake", intake_node)
    builder.add_node("freeze_baseline", freeze_baseline_node)
    builder.add_node("await_dispatch", await_dispatch_node)
    builder.add_edge(START, "intake")
    builder.add_edge("intake", "freeze_baseline")
    builder.add_edge("freeze_baseline", "await_dispatch")
    builder.add_edge("await_dispatch", END)
    return builder


def open_checkpointer(checkpoint_sqlite_path: Path) -> tuple[SqliteSaver, sqlite3.Connection]:
    """Open a LangGraph SqliteSaver on an explicit disposable path."""
    path = Path(checkpoint_sqlite_path)
    if path.parent and str(path.parent) not in ("", "."):
        path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    return SqliteSaver(conn), conn


def run_pilot_graph(
    *,
    store: DurableJobStore,
    checkpoint_sqlite_path: Path,
    job_id: str,
    frozen_baseline_sha: str,
) -> dict[str, Any]:
    """Advance a job through the pilot graph using LangGraph persistence."""
    checkpointer, conn = open_checkpointer(checkpoint_sqlite_path)
    try:
        graph = _build_graph(store).compile(checkpointer=checkpointer)
        result = graph.invoke(
            PilotGraphState(
                job_id=job_id,
                frozen_baseline_sha=frozen_baseline_sha,
            ),
            config={"configurable": {"thread_id": job_id}},
        )
        if is_dataclass(result) and not isinstance(result, type):
            return asdict(result)
        if isinstance(result, dict):
            return dict(result)
        return {"job_id": job_id, "result": result}
    finally:
        conn.close()


def run_pilot_graph_postgres(
    *,
    store: DurableJobStore,
    checkpoint_dsn: str,
    checkpoint_schema: str,
    job_id: str,
    frozen_baseline_sha: str,
) -> dict[str, Any]:
    """Advance a job through the pilot graph using PostgreSQL checkpoints."""
    from agent.durable_jobs.postgres_checkpointer import open_postgres_checkpointer

    checkpointer, conn = open_postgres_checkpointer(
        dsn=checkpoint_dsn, schema=checkpoint_schema
    )
    try:
        graph = _build_graph(store).compile(checkpointer=checkpointer)
        result = graph.invoke(
            PilotGraphState(
                job_id=job_id,
                frozen_baseline_sha=frozen_baseline_sha,
            ),
            config={"configurable": {"thread_id": job_id}},
        )
        if is_dataclass(result) and not isinstance(result, type):
            return asdict(result)
        if isinstance(result, dict):
            return dict(result)
        return {"job_id": job_id, "result": result}
    finally:
        conn.close()
