"""Durable application job store for the ENG-3 Package 1 pilot.

This SQLite store is:
- Isolated / disposable — path must be supplied explicitly (tests/config).
- Single-process / dev-only — NOT a production durable store.
- Distinct from LangGraph checkpointer state (separate DB path + tables).

Schema/migrations are local to this pilot (SCHEMA_VERSION). Production
durable-store decision is PostgreSQL-first and remains unimplemented.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from agent.durable_jobs.models import (
    ALLOWED_TRANSITIONS,
    DEFAULT_NEXT_ACTION,
    DurableJob,
    InvalidPhaseTransition,
    JobPhase,
)

SCHEMA_VERSION = 1

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS durable_jobs_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS durable_jobs (
    job_id TEXT PRIMARY KEY,
    phase TEXT NOT NULL,
    origin_platform TEXT NOT NULL,
    origin_chat_id TEXT NOT NULL,
    origin_root_thread_id TEXT NOT NULL,
    objective TEXT NOT NULL,
    repository_identity TEXT NOT NULL,
    frozen_baseline_sha TEXT NOT NULL DEFAULT '',
    idempotency_key TEXT NOT NULL UNIQUE,
    next_action TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS durable_job_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    idempotency_key TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(job_id, event_type, idempotency_key)
);
"""


def _utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _new_job_id() -> str:
    # Opaque UUID — intentionally not a Slack message timestamp.
    return f"dj_{uuid.uuid4().hex}"


class DurableJobStore:
    def __init__(self, sqlite_path: Path) -> None:
        self.sqlite_path = Path(sqlite_path)
        if self.sqlite_path.parent and str(self.sqlite_path.parent) not in ("", "."):
            self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.sqlite_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(_SCHEMA_SQL)
            conn.execute(
                "INSERT INTO durable_jobs_meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                ("schema_version", str(SCHEMA_VERSION)),
            )
            conn.commit()

    def create_job(
        self,
        *,
        origin_platform: str,
        origin_chat_id: str,
        origin_root_thread_id: str,
        objective: str,
        repository_identity: str,
        frozen_baseline_sha: str = "",
        idempotency_key: str,
    ) -> DurableJob:
        existing = self.get_job_by_idempotency_key(idempotency_key)
        if existing is not None:
            return existing

        now = _utcnow()
        job = DurableJob(
            job_id=_new_job_id(),
            phase=JobPhase.INTAKE,
            origin_platform=origin_platform,
            origin_chat_id=origin_chat_id,
            origin_root_thread_id=origin_root_thread_id,
            objective=objective,
            repository_identity=repository_identity,
            frozen_baseline_sha=frozen_baseline_sha or "",
            idempotency_key=idempotency_key,
            next_action=DEFAULT_NEXT_ACTION[JobPhase.INTAKE],
            created_at=now,
            updated_at=now,
        )
        with self._connect() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO durable_jobs(
                        job_id, phase, origin_platform, origin_chat_id,
                        origin_root_thread_id, objective, repository_identity,
                        frozen_baseline_sha, idempotency_key, next_action,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job.job_id,
                        job.phase.value,
                        job.origin_platform,
                        job.origin_chat_id,
                        job.origin_root_thread_id,
                        job.objective,
                        job.repository_identity,
                        job.frozen_baseline_sha,
                        job.idempotency_key,
                        job.next_action,
                        job.created_at,
                        job.updated_at,
                    ),
                )
                self._append_event(
                    conn,
                    job_id=job.job_id,
                    event_type="job_created",
                    payload={"phase": job.phase.value},
                    idempotency_key=f"create:{job.idempotency_key}",
                )
                conn.commit()
            except sqlite3.IntegrityError:
                conn.rollback()
                adopted = self.get_job_by_idempotency_key(idempotency_key)
                if adopted is None:
                    raise
                return adopted
        return job

    def get_job(self, job_id: str) -> Optional[DurableJob]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM durable_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        return self._row_to_job(row) if row else None

    def get_job_by_idempotency_key(self, idempotency_key: str) -> Optional[DurableJob]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM durable_jobs WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
        return self._row_to_job(row) if row else None

    def count_jobs(self) -> int:
        with self._connect() as conn:
            (count,) = conn.execute("SELECT COUNT(*) FROM durable_jobs").fetchone()
        return int(count)

    def transition_phase(
        self,
        job_id: str,
        new_phase: JobPhase,
        *,
        frozen_baseline_sha: Optional[str] = None,
    ) -> DurableJob:
        """Atomically transition phase with compare-and-swap.

        Read + validate + UPDATE ... WHERE phase=<observed> + event append run
        in one IMMEDIATE transaction so a concurrent writer cannot lose updates
        or diverge audit history from durable state.
        """
        now = _utcnow()
        with self._connect() as conn:
            # Single connection transaction: SELECT + CAS UPDATE + event.
            # Raising before context exit rolls back so state and audit stay aligned.
            row = conn.execute(
                "SELECT * FROM durable_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown job_id: {job_id}")
            job = self._row_to_job(row)
            allowed = ALLOWED_TRANSITIONS.get(job.phase, frozenset())
            if new_phase not in allowed:
                raise InvalidPhaseTransition(
                    f"cannot transition {job.phase.value} -> {new_phase.value}"
                )
            sha = (
                frozen_baseline_sha
                if frozen_baseline_sha is not None
                else job.frozen_baseline_sha
            )
            next_action = DEFAULT_NEXT_ACTION[new_phase]
            cur = conn.execute(
                """
                UPDATE durable_jobs
                   SET phase = ?, frozen_baseline_sha = ?, next_action = ?,
                       updated_at = ?
                 WHERE job_id = ? AND phase = ?
                """,
                (
                    new_phase.value,
                    sha,
                    next_action,
                    now,
                    job_id,
                    job.phase.value,
                ),
            )
            if cur.rowcount != 1:
                raise InvalidPhaseTransition(
                    f"stale phase for {job_id}: concurrent update rejected "
                    f"(observed {job.phase.value} -> {new_phase.value})"
                )
            inserted = self._append_event(
                conn,
                job_id=job_id,
                event_type="phase_transition",
                payload={
                    "from": job.phase.value,
                    "to": new_phase.value,
                    "frozen_baseline_sha": sha,
                },
                idempotency_key=f"phase:{job.phase.value}->{new_phase.value}",
            )
            if not inserted:
                raise InvalidPhaseTransition(
                    f"duplicate phase transition event for {job_id}: "
                    f"{job.phase.value} -> {new_phase.value}"
                )
            updated_row = conn.execute(
                "SELECT * FROM durable_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        assert updated_row is not None
        return self._row_to_job(updated_row)

    def append_intent(
        self,
        job_id: str,
        *,
        event_type: str,
        payload: Optional[dict[str, Any]] = None,
        idempotency_key: Optional[str] = None,
    ) -> bool:
        """Record an append-only outbox/intent event.

        Returns True if a new row was inserted, False if the idempotency key
        already existed (crash-safe recovery seam).
        """
        if self.get_job(job_id) is None:
            raise KeyError(f"unknown job_id: {job_id}")
        with self._connect() as conn:
            inserted = self._append_event(
                conn,
                job_id=job_id,
                event_type=event_type,
                payload=payload or {},
                idempotency_key=idempotency_key,
            )
            conn.commit()
        return inserted

    def list_events(self, job_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT event_id, job_id, event_type, payload_json,
                       idempotency_key, created_at
                  FROM durable_job_events
                 WHERE job_id = ?
                 ORDER BY event_id ASC
                """,
                (job_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def recover_job(self, job_id: str) -> Optional[DurableJob]:
        """Re-open path: load a nonterminal job by id with phase + correlation."""
        job = self.get_job(job_id)
        if job is None:
            return None
        return job

    @staticmethod
    def _append_event(
        conn: sqlite3.Connection,
        *,
        job_id: str,
        event_type: str,
        payload: dict[str, Any],
        idempotency_key: Optional[str],
    ) -> bool:
        try:
            conn.execute(
                """
                INSERT INTO durable_job_events(
                    job_id, event_type, payload_json, idempotency_key, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    event_type,
                    json.dumps(payload, sort_keys=True),
                    idempotency_key,
                    _utcnow(),
                ),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> DurableJob:
        return DurableJob(
            job_id=row["job_id"],
            phase=JobPhase(row["phase"]),
            origin_platform=row["origin_platform"],
            origin_chat_id=row["origin_chat_id"],
            origin_root_thread_id=row["origin_root_thread_id"],
            objective=row["objective"],
            repository_identity=row["repository_identity"],
            frozen_baseline_sha=row["frozen_baseline_sha"],
            idempotency_key=row["idempotency_key"],
            next_action=row["next_action"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
