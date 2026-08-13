"""ENG-28 local coordinators: terminal evidence, resume enqueue, inbound ACK.

Isolated SQLite. Not a live Slack/Cursor control plane. Not PostgreSQL.
"""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Protocol, Union

from agent.durable_jobs.decisions import DecisionLedger
from agent.durable_jobs.effects import EffectStatus, ProviderEffectLedger
from agent.durable_jobs.slack_contract import SlackBindingLedger, SlackRootStatus
from agent.durable_jobs.store import DurableJobStore

SqlitePath = Union[str, Path]


class TerminalEvidenceRequired(RuntimeError):
    """Resume/complete requires durable matching terminal evidence."""


class ResumeEnqueueError(RuntimeError):
    """Local resume mark is forbidden without an accepted enqueue row."""


@dataclass(frozen=True)
class TerminalEvidence:
    evidence_id: str
    job_id: str
    kind: str
    correlation_id: str
    source_status: str
    idempotency_key: str
    created_at: str


@dataclass(frozen=True)
class ResumeEnqueue:
    enqueue_id: str
    job_id: str
    evidence_id: str
    idempotency_key: str
    status: str
    local_marked: bool


@dataclass(frozen=True)
class InboundActionResult:
    ok: bool
    ack_status: str
    inbound_id: Optional[str] = None
    decision_id: Optional[str] = None


class InboundAckPort(Protocol):
    def ack(self, *, inbound_id: str, job_id: str) -> str: ...


def after_evidence_rows_before_commit() -> None:
    """Test seam after evidence rows, before COMMIT. Production no-op."""
    return None


def resume_idempotency_key(job_id: str, evidence_id: str) -> str:
    return f"resume:{job_id}:{evidence_id}"


def _connect(sqlite_path: SqlitePath) -> sqlite3.Connection:
    path = Path(sqlite_path)
    DurableJobStore(sqlite_path=path)
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.isolation_level = "IMMEDIATE"
    return conn


def _utcnow(conn: sqlite3.Connection) -> str:
    from agent.durable_jobs.clock import utcnow_iso

    return utcnow_iso()


def commit_terminal_evidence(
    sqlite_path: SqlitePath,
    *,
    job_id: str,
    kind: str,
    correlation_id: str,
) -> TerminalEvidence:
    """Persist terminal evidence only when durable status matches."""
    path = Path(sqlite_path)
    jobs = DurableJobStore(sqlite_path=path)
    if jobs.get_job(job_id) is None:
        raise TerminalEvidenceRequired(f"unknown job {job_id}")
    if not str(correlation_id or "").strip():
        raise TerminalEvidenceRequired("correlation_id required")

    source_status = ""
    if kind == "provider_run":
        claim = ProviderEffectLedger(sqlite_path=path).get_claim(job_id, "create_run")
        if (
            claim is None
            or claim.status not in (EffectStatus.ACCEPTED, EffectStatus.ADOPTED)
            or claim.provider_run_id != correlation_id
        ):
            raise TerminalEvidenceRequired(
                "provider_run evidence requires matching ACCEPTED/ADOPTED run"
            )
        source_status = claim.status.value
    elif kind == "slack_root":
        binding = SlackBindingLedger(sqlite_path=path).get_binding(job_id)
        if (
            binding is None
            or binding.status not in (SlackRootStatus.DELIVERED, SlackRootStatus.ADOPTED)
            or binding.delivered_message_ts != correlation_id
        ):
            raise TerminalEvidenceRequired(
                "slack_root evidence requires matching DELIVERED/ADOPTED ts"
            )
        source_status = binding.status.value
    else:
        raise TerminalEvidenceRequired(f"unknown evidence kind {kind!r}")

    evidence_id = f"ev_{uuid.uuid4().hex}"
    idempotency_key = f"terminal:{job_id}:{kind}:{correlation_id}"
    with _connect(path) as conn:
        existing = conn.execute(
            """
            SELECT * FROM job_terminal_evidence
             WHERE idempotency_key = ?
            """,
            (idempotency_key,),
        ).fetchone()
        if existing is not None:
            return _row_to_evidence(existing)
        now = _utcnow(conn)
        conn.execute(
            """
            INSERT INTO job_terminal_evidence(
                evidence_id, job_id, kind, correlation_id, source_status,
                idempotency_key, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                evidence_id,
                job_id,
                kind,
                correlation_id,
                source_status,
                idempotency_key,
                now,
            ),
        )
        DurableJobStore._append_event(
            conn,
            job_id=job_id,
            event_type="terminal_evidence_committed",
            payload={
                "kind": kind,
                "correlation_id": correlation_id,
                "source_status": source_status,
            },
            idempotency_key=f"terminal_evidence_committed:{idempotency_key}",
        )
        after_evidence_rows_before_commit()
        row = conn.execute(
            "SELECT * FROM job_terminal_evidence WHERE evidence_id = ?",
            (evidence_id,),
        ).fetchone()
    assert row is not None
    return _row_to_evidence(row)


def latest_terminal_evidence(
    sqlite_path: SqlitePath, job_id: str
) -> Optional[TerminalEvidence]:
    with _connect(sqlite_path) as conn:
        row = conn.execute(
            """
            SELECT * FROM job_terminal_evidence
             WHERE job_id = ?
             ORDER BY created_at DESC, evidence_id DESC
             LIMIT 1
            """,
            (job_id,),
        ).fetchone()
    return _row_to_evidence(row) if row else None


def enqueue_resume(sqlite_path: SqlitePath, *, job_id: str) -> ResumeEnqueue:
    evidence = latest_terminal_evidence(sqlite_path, job_id)
    if evidence is None:
        raise TerminalEvidenceRequired(
            f"resume enqueue requires terminal evidence for {job_id}"
        )
    key = resume_idempotency_key(job_id, evidence.evidence_id)
    enqueue_id = f"rq_{uuid.uuid4().hex}"
    with _connect(sqlite_path) as conn:
        existing = conn.execute(
            "SELECT * FROM job_resume_enqueues WHERE idempotency_key = ?",
            (key,),
        ).fetchone()
        if existing is not None:
            if int(existing["local_marked"] or 0) != 1:
                conn.execute(
                    """
                    UPDATE job_resume_enqueues
                       SET local_marked = 1, status = 'accepted', updated_at = ?
                     WHERE idempotency_key = ? AND status = 'accepted'
                    """,
                    (_utcnow(conn), key),
                )
                existing = conn.execute(
                    "SELECT * FROM job_resume_enqueues WHERE idempotency_key = ?",
                    (key,),
                ).fetchone()
            return _row_to_enqueue(existing)
        now = _utcnow(conn)
        try:
            conn.execute(
                """
                INSERT INTO job_resume_enqueues(
                    enqueue_id, job_id, evidence_id, idempotency_key, status,
                    local_marked, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'accepted', 0, ?, ?)
                """,
                (enqueue_id, job_id, evidence.evidence_id, key, now, now),
            )
        except sqlite3.IntegrityError:
            raced = conn.execute(
                "SELECT * FROM job_resume_enqueues WHERE idempotency_key = ?",
                (key,),
            ).fetchone()
            if raced is None:
                raise
            return _row_to_enqueue(raced)
        DurableJobStore._append_event(
            conn,
            job_id=job_id,
            event_type="resume_enqueue_accepted",
            payload={"idempotency_key": key, "evidence_id": evidence.evidence_id},
            idempotency_key=f"resume_enqueue_accepted:{key}",
        )
        conn.execute(
            """
            UPDATE job_resume_enqueues
               SET local_marked = 1, updated_at = ?
             WHERE enqueue_id = ? AND status = 'accepted'
            """,
            (now, enqueue_id),
        )
        row = conn.execute(
            "SELECT * FROM job_resume_enqueues WHERE enqueue_id = ?",
            (enqueue_id,),
        ).fetchone()
    assert row is not None
    return _row_to_enqueue(row)


def mark_resume_local(sqlite_path: SqlitePath, *, job_id: str) -> ResumeEnqueue:
    with _connect(sqlite_path) as conn:
        row = conn.execute(
            """
            SELECT * FROM job_resume_enqueues
             WHERE job_id = ? AND status = 'accepted'
             ORDER BY created_at DESC
             LIMIT 1
            """,
            (job_id,),
        ).fetchone()
        if row is None:
            raise ResumeEnqueueError(
                f"cannot local-mark resume for {job_id} without accepted enqueue"
            )
        conn.execute(
            """
            UPDATE job_resume_enqueues
               SET local_marked = 1, updated_at = ?
             WHERE enqueue_id = ? AND status = 'accepted'
            """,
            (_utcnow(conn), row["enqueue_id"]),
        )
        row = conn.execute(
            "SELECT * FROM job_resume_enqueues WHERE enqueue_id = ?",
            (row["enqueue_id"],),
        ).fetchone()
    assert row is not None
    return _row_to_enqueue(row)


def consume_inbound_action(
    sqlite_path: SqlitePath,
    ack_port: InboundAckPort,
    *,
    job_id: str,
    workspace_id: str,
    channel_id: str,
    root_thread_ts: str,
    actor_id: str,
    decision_type: str,
    decision_idempotency_key: str,
    policy_version: str,
    candidate_id: str,
    candidate_version: str,
) -> InboundActionResult:
    """Persist the decision first, then ACK. Never ACK-before-commit."""
    path = Path(sqlite_path)
    DurableJobStore(sqlite_path=path)
    binding = SlackBindingLedger(sqlite_path=path).get_binding(job_id)
    occupant = SlackBindingLedger(sqlite_path=path).get_by_root(
        workspace_id, channel_id, root_thread_ts
    )
    if (
        binding is None
        or occupant is None
        or occupant.job_id != job_id
        or binding.workspace_id != workspace_id
        or binding.channel_id != channel_id
        or binding.root_thread_ts != root_thread_ts
        or binding.candidate_id != candidate_id
        or binding.candidate_version != candidate_version
    ):
        return InboundActionResult(ok=False, ack_status="rejected")

    with _connect(path) as conn:
        existing = conn.execute(
            """
            SELECT * FROM job_inbound_actions
             WHERE decision_idempotency_key = ?
            """,
            (decision_idempotency_key,),
        ).fetchone()
        inbound_id = existing["inbound_id"] if existing else f"in_{uuid.uuid4().hex}"
        decision_id = existing["decision_id"] if existing else None
        ack_status = existing["ack_status"] if existing else None
        if existing is None:
            conn.execute(
                """
                INSERT INTO job_inbound_actions(
                    inbound_id, job_id, workspace_id, channel_id, root_thread_ts,
                    actor_id, decision_type, decision_idempotency_key, decision_id,
                    ack_status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, 'pending', ?, ?)
                """,
                (
                    inbound_id,
                    job_id,
                    workspace_id,
                    channel_id,
                    root_thread_ts,
                    actor_id,
                    decision_type,
                    decision_idempotency_key,
                    _utcnow(conn),
                    _utcnow(conn),
                ),
            )

    if ack_status == "acked":
        return InboundActionResult(
            ok=True,
            ack_status="acked",
            inbound_id=inbound_id,
            decision_id=decision_id,
        )

    if decision_id is None:
        recorded = DecisionLedger(sqlite_path=path).record_decision(
            job_id=job_id,
            decision_type=decision_type,
            candidate_id=candidate_id,
            candidate_version=candidate_version,
            actor_id=actor_id,
            policy_version=policy_version,
            decision_idempotency_key=decision_idempotency_key,
        )
        if not recorded.ok:
            with _connect(path) as conn:
                conn.execute(
                    """
                    UPDATE job_inbound_actions
                       SET ack_status = 'rejected', updated_at = ?
                     WHERE inbound_id = ?
                    """,
                    (_utcnow(conn), inbound_id),
                )
            return InboundActionResult(
                ok=False,
                ack_status="rejected",
                inbound_id=inbound_id,
            )
        decision_id = recorded.record.decision_id if recorded.record else None
        with _connect(path) as conn:
            conn.execute(
                """
                UPDATE job_inbound_actions
                   SET decision_id = ?, updated_at = ?
                 WHERE inbound_id = ?
                """,
                (decision_id, _utcnow(conn), inbound_id),
            )

    try:
        ack_port.ack(inbound_id=inbound_id, job_id=job_id)
    except Exception:
        return InboundActionResult(
            ok=True,
            ack_status="pending",
            inbound_id=inbound_id,
            decision_id=decision_id,
        )

    with _connect(path) as conn:
        conn.execute(
            """
            UPDATE job_inbound_actions
               SET ack_status = 'acked', updated_at = ?
             WHERE inbound_id = ? AND decision_id IS NOT NULL
            """,
            (_utcnow(conn), inbound_id),
        )
    return InboundActionResult(
        ok=True,
        ack_status="acked",
        inbound_id=inbound_id,
        decision_id=decision_id,
    )


def _row_to_evidence(row: sqlite3.Row) -> TerminalEvidence:
    return TerminalEvidence(
        evidence_id=row["evidence_id"],
        job_id=row["job_id"],
        kind=row["kind"],
        correlation_id=row["correlation_id"],
        source_status=row["source_status"],
        idempotency_key=row["idempotency_key"],
        created_at=row["created_at"],
    )


def _row_to_enqueue(row: sqlite3.Row) -> ResumeEnqueue:
    return ResumeEnqueue(
        enqueue_id=row["enqueue_id"],
        job_id=row["job_id"],
        evidence_id=row["evidence_id"],
        idempotency_key=row["idempotency_key"],
        status=row["status"],
        local_marked=bool(int(row["local_marked"] or 0)),
    )
