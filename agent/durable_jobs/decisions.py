"""ENG-27 durable Go/Hold/Cancel records (isolated, default-off).

Decisions are bound to the exact job, Slack candidate/version, actor, policy
version, and decision idempotency key. Fail closed on mismatch, unauthorized,
expired, or replayed keys. Cancel is terminal and must not be weakened.

SQLite here is disposable, explicit-path, single-process, and dev/test-only.
This module does not change gateway Slack routing or Linear Ready/Go policy.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Callable, Optional, Sequence

from agent.durable_jobs.store import DurableJobStore


class DecisionType(str, Enum):
    GO = "go"
    HOLD = "hold"
    CANCEL = "cancel"


def _utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_dt(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text)


@dataclass(frozen=True)
class JobAuthzPolicy:
    job_id: str
    policy_version: str
    allowed_actors: tuple[str, ...]
    expires_at: Optional[str]
    created_at: str


@dataclass(frozen=True)
class JobDecision:
    decision_id: str
    job_id: str
    decision_type: DecisionType
    candidate_id: str
    candidate_version: str
    actor_id: str
    policy_version: str
    decision_idempotency_key: str
    status: str
    reason_codes: tuple[str, ...]
    created_at: str
    source_package_id: Optional[str] = None
    source_package_version: Optional[str] = None
    candidate_sha: Optional[str] = None
    target_environment: Optional[str] = None
    target_action: Optional[str] = None
    matrix_version: Optional[str] = None


@dataclass(frozen=True)
class DecisionResult:
    ok: bool
    status: str
    record: Optional[JobDecision]
    reason_codes: tuple[str, ...] = ()


class JobCanceledError(RuntimeError):
    """Accepted Cancel is terminal for new provider/Slack call-outs.

    SQLite can refuse a new claim, inflight witness, recovery lookup, or
    the last SELECT immediately before an adapter call when Cancel is
    already committed. It cannot abort a Cursor/Slack RPC that has already
    begun after that SELECT — adapters do not expose cancellation of an
    outstanding request. In that race the RPC may complete, but
    accepted/adopted bind must not overwrite Cancel, and later Go stays
    rejected.
    """


_CANCEL_FENCE_TABLES = frozenset(
    {"provider_effect_claims", "slack_job_bindings"}
)


def sql_reject_authoritative_cancel(table: str) -> str:
    """Correlated predicate for a success UPDATE-CAS on ``table``.

    The Cancel row must be visible in the *same* SQLite write transaction as
    the UPDATE. A prior ``_job_is_canceled()`` on another connection is not
    a fence. This is single-file SQLite only; it does not claim PostgreSQL
    or distributed isolation.
    """
    if table not in _CANCEL_FENCE_TABLES:
        raise ValueError(f"unknown cancel-fence table: {table}")
    return (
        " AND NOT EXISTS ("
        " SELECT 1 FROM job_decisions"
        f" WHERE job_id = {table}.job_id"
        " AND decision_type = 'cancel'"
        " AND status IN ('accepted', 'duplicate'))"
    )


class DecisionLedger:
    def __init__(
        self,
        sqlite_path: Path,
        now_fn: Optional[Callable[[], str]] = None,
    ) -> None:
        self.sqlite_path = Path(sqlite_path)
        self._jobs = DurableJobStore(sqlite_path=self.sqlite_path)
        self._now_fn = now_fn or _utcnow

    def _now(self) -> str:
        return self._now_fn()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.sqlite_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.isolation_level = "IMMEDIATE"
        return conn

    def set_policy(
        self,
        *,
        job_id: str,
        policy_version: str,
        allowed_actors: Sequence[str],
        expires_at: Optional[str] = None,
    ) -> JobAuthzPolicy:
        job = self._jobs.get_job(job_id)
        if job is None:
            raise KeyError(f"unknown job_id: {job_id}")
        actors = tuple(str(a) for a in allowed_actors)
        now = self._now()
        payload = json.dumps(list(actors), sort_keys=True)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO job_authz_policies(
                    job_id, policy_version, allowed_actors_json, expires_at,
                    created_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    policy_version = excluded.policy_version,
                    allowed_actors_json = excluded.allowed_actors_json,
                    expires_at = excluded.expires_at
                """,
                (job_id, policy_version, payload, expires_at, now),
            )
            DurableJobStore._append_event(
                conn,
                job_id=job_id,
                event_type="job_authz_policy_set",
                payload={
                    "policy_version": policy_version,
                    "allowed_actors": list(actors),
                    "expires_at": expires_at,
                },
                idempotency_key=f"job_authz_policy_set:{job_id}:{policy_version}",
            )
            row = conn.execute(
                "SELECT * FROM job_authz_policies WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        assert row is not None
        return self._row_to_policy(row)

    def record_decision(
        self,
        *,
        job_id: str,
        decision_type: str,
        candidate_id: str,
        candidate_version: str,
        actor_id: str,
        policy_version: str,
        decision_idempotency_key: str,
        source_package_id: Optional[str] = None,
        source_package_version: Optional[str] = None,
        candidate_sha: Optional[str] = None,
        target_environment: Optional[str] = None,
        target_action: Optional[str] = None,
        matrix_version: Optional[str] = None,
    ) -> DecisionResult:
        try:
            dtype = DecisionType(str(decision_type).lower())
        except ValueError:
            return DecisionResult(
                ok=False,
                status="rejected",
                record=None,
                reason_codes=("mismatch",),
            )

        with self._connect() as conn:
            existing = conn.execute(
                """
                SELECT * FROM job_decisions
                 WHERE decision_idempotency_key = ?
                """,
                (decision_idempotency_key,),
            ).fetchone()
            if existing is not None:
                record = self._row_to_decision(existing)
                same = (
                    record.job_id == job_id
                    and record.decision_type is dtype
                    and record.candidate_id == candidate_id
                    and record.candidate_version == candidate_version
                    and record.actor_id == actor_id
                    and record.policy_version == policy_version
                    and record.source_package_id == source_package_id
                    and record.source_package_version == source_package_version
                    and record.candidate_sha == candidate_sha
                    and record.target_environment == target_environment
                    and record.target_action == target_action
                    and record.matrix_version == matrix_version
                )
                if same and record.status in ("accepted", "duplicate"):
                    if (
                        dtype is not DecisionType.CANCEL
                        and self._canceled_row(conn, job_id) is not None
                    ):
                        return DecisionResult(
                            ok=False,
                            status="rejected",
                            record=record,
                            reason_codes=("canceled",),
                        )
                    return DecisionResult(
                        ok=True,
                        status="duplicate",
                        record=record,
                        reason_codes=(),
                    )
                return DecisionResult(
                    ok=False,
                    status="rejected",
                    record=record,
                    reason_codes=("replayed",),
                )

            reasons = self._authorization_reasons(
                conn,
                job_id=job_id,
                dtype=dtype,
                candidate_id=candidate_id,
                candidate_version=candidate_version,
                actor_id=actor_id,
                policy_version=policy_version,
            )
            job_row = conn.execute(
                "SELECT 1 FROM durable_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if job_row is None:
                return DecisionResult(
                    ok=False,
                    status="rejected",
                    record=None,
                    reason_codes=reasons or ("unauthorized",),
                )
            status = "rejected" if reasons else "accepted"
            now = self._now()
            decision_id = f"dd_{uuid.uuid4().hex}"
            conn.execute(
                """
                INSERT INTO job_decisions(
                    decision_id, job_id, decision_type, candidate_id,
                    candidate_version, actor_id, policy_version,
                    decision_idempotency_key, status, reason_codes_json,
                    created_at, source_package_id, source_package_version,
                    candidate_sha, target_environment, target_action,
                    matrix_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision_id,
                    job_id,
                    dtype.value,
                    candidate_id,
                    candidate_version,
                    actor_id,
                    policy_version,
                    decision_idempotency_key,
                    status,
                    json.dumps(list(reasons), sort_keys=True),
                    now,
                    source_package_id,
                    source_package_version,
                    candidate_sha,
                    target_environment,
                    target_action,
                    matrix_version,
                ),
            )
            DurableJobStore._append_event(
                conn,
                job_id=job_id,
                event_type="job_decision_recorded",
                payload={
                    "decision_type": dtype.value,
                    "status": status,
                    "actor_id": actor_id,
                    "policy_version": policy_version,
                    "decision_idempotency_key": decision_idempotency_key,
                    "reason_codes": list(reasons),
                },
                idempotency_key=f"job_decision_recorded:{decision_idempotency_key}",
            )
            row = conn.execute(
                "SELECT * FROM job_decisions WHERE decision_id = ?",
                (decision_id,),
            ).fetchone()
        assert row is not None
        record = self._row_to_decision(row)
        return DecisionResult(
            ok=status == "accepted",
            status=status,
            record=record,
            reason_codes=reasons,
        )

    def latest_accepted(self, job_id: str) -> Optional[JobDecision]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM job_decisions
                 WHERE job_id = ? AND status = 'accepted'
                 ORDER BY created_at DESC, decision_id DESC
                 LIMIT 1
                """,
                (job_id,),
            ).fetchone()
        return self._row_to_decision(row) if row else None

    def is_canceled(self, job_id: str) -> bool:
        with self._connect() as conn:
            return self._canceled_row(conn, job_id) is not None

    def count_decisions(self, job_id: str) -> int:
        with self._connect() as conn:
            (count,) = conn.execute(
                """
                SELECT COUNT(*) FROM job_decisions
                 WHERE job_id = ? AND status IN ('accepted', 'duplicate')
                """,
                (job_id,),
            ).fetchone()
        return int(count)

    def _authorization_reasons(
        self,
        conn: sqlite3.Connection,
        *,
        job_id: str,
        dtype: DecisionType,
        candidate_id: str,
        candidate_version: str,
        actor_id: str,
        policy_version: str,
    ) -> tuple[str, ...]:
        reasons: list[str] = []
        job_row = conn.execute(
            "SELECT job_id FROM durable_jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        if job_row is None:
            return ("unauthorized",)

        policy_row = conn.execute(
            "SELECT * FROM job_authz_policies WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        binding_row = conn.execute(
            "SELECT * FROM slack_job_bindings WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        if policy_row is None or binding_row is None:
            reasons.append("unauthorized")
            return tuple(dict.fromkeys(reasons))

        policy = self._row_to_policy(policy_row)
        if actor_id not in policy.allowed_actors:
            reasons.append("unauthorized")
        if (
            policy.policy_version != policy_version
            or binding_row["candidate_id"] != candidate_id
            or binding_row["candidate_version"] != candidate_version
            or binding_row["job_id"] != job_id
        ):
            reasons.append("mismatch")
        if policy.expires_at:
            try:
                if _parse_dt(self._now()) >= _parse_dt(policy.expires_at):
                    reasons.append("expired")
            except ValueError:
                reasons.append("expired")

        canceled = self._canceled_row(conn, job_id)
        if canceled is not None and dtype is not DecisionType.CANCEL:
            reasons.append("canceled")
        return tuple(dict.fromkeys(reasons))

    @staticmethod
    def _canceled_row(conn: sqlite3.Connection, job_id: str) -> Optional[sqlite3.Row]:
        return conn.execute(
            """
            SELECT 1 FROM job_decisions
             WHERE job_id = ? AND decision_type = 'cancel'
               AND status IN ('accepted', 'duplicate')
             LIMIT 1
            """,
            (job_id,),
        ).fetchone()

    @staticmethod
    def _row_to_policy(row: sqlite3.Row) -> JobAuthzPolicy:
        actors = tuple(json.loads(row["allowed_actors_json"]))
        return JobAuthzPolicy(
            job_id=row["job_id"],
            policy_version=row["policy_version"],
            allowed_actors=actors,
            expires_at=row["expires_at"],
            created_at=row["created_at"],
        )

    @staticmethod
    def _row_to_decision(row: sqlite3.Row) -> JobDecision:
        keys = set(row.keys())

        def _opt(name: str) -> Optional[str]:
            if name not in keys:
                return None
            value = row[name]
            return None if value is None else str(value)

        return JobDecision(
            decision_id=row["decision_id"],
            job_id=row["job_id"],
            decision_type=DecisionType(row["decision_type"]),
            candidate_id=row["candidate_id"],
            candidate_version=row["candidate_version"],
            actor_id=row["actor_id"],
            policy_version=row["policy_version"],
            decision_idempotency_key=row["decision_idempotency_key"],
            status=row["status"],
            reason_codes=tuple(json.loads(row["reason_codes_json"] or "[]")),
            created_at=row["created_at"],
            source_package_id=_opt("source_package_id"),
            source_package_version=_opt("source_package_version"),
            candidate_sha=_opt("candidate_sha"),
            target_environment=_opt("target_environment"),
            target_action=_opt("target_action"),
            matrix_version=_opt("matrix_version"),
        )


def job_is_canceled_on_conn(conn: sqlite3.Connection, job_id: str) -> bool:
    """True when an accepted/duplicate Cancel is visible on this connection."""
    return DecisionLedger._canceled_row(conn, job_id) is not None


def raise_if_job_canceled(sqlite_path: Path, job_id: str, *, action: str) -> None:
    """Fail closed using the latest committed Cancel visible on a new snapshot."""
    if DecisionLedger(sqlite_path=sqlite_path).is_canceled(job_id):
        raise JobCanceledError(f"job {job_id} is canceled; refusing {action}")
