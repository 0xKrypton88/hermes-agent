"""Offline durable continuation leases for session handoffs.

This module is intentionally unwired: it owns no scheduler, gateway, provider,
or live-adapter integration. Callers must explicitly enqueue and drive records.

The store makes verified, persisted effects immutable and fences stale lease
owners. Only a SHA-256 digest of safe, offline receipt bytes may be persisted;
payloads, provider responses, credentials, and secrets are forbidden. It does not
make an external delivery exactly-once: an eventual adapter
must durably bind an idempotency key to each effect before attempting delivery
and reuse that key when reconciling or retrying a crash-window attempt. The store
also cannot prove that a caller-supplied digest came from the adapter's
authoritative receipt bytes; defining and binding that digest source remains an
adapter integration responsibility.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import sqlite3
from typing import Callable

from agent.durable_jobs.clock import add_seconds_iso, claim_is_expired, utcnow_iso


class ContinuationLeaseLost(RuntimeError):
    """The caller's owner token/generation no longer owns the continuation."""


class DeliveryVerificationFailed(RuntimeError):
    """Persisted delivery receipt digest is absent or does not match."""


@dataclass(frozen=True)
class ContinuationRecord:
    job_id: str
    handoff_id: str
    request_id: str | None
    session_id: str | None
    checkpoint_stage: str
    next_action: str
    due_at: str
    owner_token: str | None
    owner_generation: int
    lease_expires_at: str | None
    heartbeat_at: str | None
    verification_state: str
    manual_resume_reason: str | None
    manual_resume_operator_reason: str | None
    wake_state: str


_DDL = """
CREATE TABLE IF NOT EXISTS session_handoff_continuations (
    job_id TEXT NOT NULL,
    handoff_id TEXT NOT NULL,
    request_id TEXT,
    session_id TEXT,
    checkpoint_stage TEXT NOT NULL,
    next_action TEXT NOT NULL,
    due_at TEXT NOT NULL,
    owner_token TEXT,
    owner_generation INTEGER NOT NULL DEFAULT 0,
    lease_expires_at TEXT,
    heartbeat_at TEXT,
    verification_state TEXT NOT NULL DEFAULT 'PENDING',
    manual_resume_reason TEXT,
    manual_resume_operator_reason TEXT,
    wake_state TEXT NOT NULL DEFAULT 'DUE',
    PRIMARY KEY (job_id, handoff_id)
);
CREATE TABLE IF NOT EXISTS session_handoff_continuation_effects (
    job_id TEXT NOT NULL,
    handoff_id TEXT NOT NULL,
    effect_name TEXT NOT NULL CHECK (effect_name = 'handoff_delivery'),
    receipt_sha256 TEXT NOT NULL CHECK (
      length(receipt_sha256) = 64
      AND receipt_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    verified_at TEXT NOT NULL,
    owner_generation INTEGER NOT NULL,
    PRIMARY KEY (job_id, handoff_id, effect_name),
    FOREIGN KEY (job_id, handoff_id)
      REFERENCES session_handoff_continuations(job_id, handoff_id)
);
"""


_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


def _require_receipt_sha256(value: str, *, name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase 64-hex SHA-256 digest")


class ContinuationStore:
    """Explicit, offline SQLite store for durable handoff continuations."""

    def __init__(
        self, path: str | Path, *, now_fn: Callable[[], str] = utcnow_iso
    ) -> None:
        self.path = Path(path)
        self._now_fn = now_fn
        with self._connect() as connection:
            connection.executescript(_DDL)
            self._migrate_scope_columns(connection)

    @staticmethod
    def _migrate_scope_columns(connection: sqlite3.Connection) -> None:
        """Add authoritative scope without assigning identity to legacy rows."""
        columns = {
            row["name"]
            for row in connection.execute(
                "PRAGMA table_info(session_handoff_continuations)"
            )
        }
        for name in ("request_id", "session_id"):
            if name not in columns:
                connection.execute(
                    f"ALTER TABLE session_handoff_continuations ADD COLUMN {name} TEXT"
                )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    @staticmethod
    def _record(row: sqlite3.Row) -> ContinuationRecord:
        return ContinuationRecord(**dict(row))

    def _select_record(
        self, connection: sqlite3.Connection, job_id: str, handoff_id: str
    ) -> ContinuationRecord:
        """Snapshot a continuation while the caller's transaction still owns it."""
        row = connection.execute(
            "SELECT * FROM session_handoff_continuations WHERE job_id=? AND handoff_id=?",
            (job_id, handoff_id),
        ).fetchone()
        if row is None:
            raise KeyError((job_id, handoff_id))
        return self._record(row)

    def enqueue(
        self,
        *,
        job_id: str,
        handoff_id: str,
        checkpoint_stage: str,
        next_action: str,
        due_at: str | None = None,
        request_id: str | None = None,
        session_id: str | None = None,
    ) -> ContinuationRecord:
        if not all((job_id, handoff_id, checkpoint_stage, next_action)):
            raise ValueError(
                "continuation identity, checkpoint, and next_action are required"
            )
        if (request_id is None) != (session_id is None):
            raise ValueError("continuation request and session scope must be provided together")
        if request_id is not None and (not request_id or not session_id):
            raise ValueError("continuation request and session scope must be nonempty")
        due = due_at or self._now_fn()
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO session_handoff_continuations "
                "(job_id,handoff_id,request_id,session_id,checkpoint_stage,next_action,due_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (job_id, handoff_id, request_id, session_id, checkpoint_stage, next_action, due),
            )
            result = self._select_record(connection, job_id, handoff_id)
        return result

    def get(self, job_id: str, handoff_id: str) -> ContinuationRecord:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM session_handoff_continuations WHERE job_id=? AND handoff_id=?",
                (job_id, handoff_id),
            ).fetchone()
        if row is None:
            raise KeyError((job_id, handoff_id))
        return self._record(row)

    def claim_due(
        self, *, owner_token: str, lease_seconds: float
    ) -> ContinuationRecord | None:
        if not owner_token or lease_seconds <= 0:
            raise ValueError("owner token and positive lease are required")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            now = self._now_fn()
            expires = add_seconds_iso(now, lease_seconds)
            rows = connection.execute(
                "SELECT * FROM session_handoff_continuations "
                "WHERE request_id IS NULL AND session_id IS NULL "
                "AND wake_state='DUE' AND verification_state!='MANUAL_RESUME' AND due_at<=? "
                "ORDER BY due_at,job_id,handoff_id",
                (now,),
            ).fetchall()
            candidate = next(
                (row for row in rows if claim_is_expired(row["lease_expires_at"], now)),
                None,
            )
            if candidate is None:
                connection.commit()
                return None
            generation = int(candidate["owner_generation"]) + 1
            changed = connection.execute(
                "UPDATE session_handoff_continuations SET owner_token=?,owner_generation=?,"
                "lease_expires_at=?,heartbeat_at=? WHERE job_id=? AND handoff_id=? "
                "AND owner_generation=?",
                (
                    owner_token,
                    generation,
                    expires,
                    now,
                    candidate["job_id"],
                    candidate["handoff_id"],
                    candidate["owner_generation"],
                ),
            ).rowcount
            if changed != 1:
                connection.rollback()
                return None
            result = self._select_record(
                connection, candidate["job_id"], candidate["handoff_id"]
            )
            connection.commit()
        return result

    def claim_due_scoped(
        self,
        *,
        request_id: str,
        session_id: str,
        owner_token: str,
        lease_seconds: float,
    ) -> ContinuationRecord | None:
        """Atomically claim only a record durably bound to the exact authority."""
        if not request_id or not session_id:
            raise ValueError("nonempty request and session scope are required")
        if not owner_token or lease_seconds <= 0:
            raise ValueError("owner token and positive lease are required")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            now = self._now_fn()
            expires = add_seconds_iso(now, lease_seconds)
            rows = connection.execute(
                "SELECT * FROM session_handoff_continuations "
                "WHERE request_id=? AND session_id=? AND wake_state='DUE' "
                "AND verification_state!='MANUAL_RESUME' AND due_at<=? "
                "ORDER BY due_at,job_id,handoff_id",
                (request_id, session_id, now),
            ).fetchall()
            candidate = next(
                (row for row in rows if claim_is_expired(row["lease_expires_at"], now)),
                None,
            )
            if candidate is None:
                connection.commit()
                return None
            generation = int(candidate["owner_generation"]) + 1
            changed = connection.execute(
                "UPDATE session_handoff_continuations SET owner_token=?,owner_generation=?,"
                "lease_expires_at=?,heartbeat_at=? WHERE job_id=? AND handoff_id=? "
                "AND request_id=? AND session_id=? AND owner_generation=?",
                (
                    owner_token,
                    generation,
                    expires,
                    now,
                    candidate["job_id"],
                    candidate["handoff_id"],
                    request_id,
                    session_id,
                    candidate["owner_generation"],
                ),
            ).rowcount
            if changed != 1:
                connection.rollback()
                return None
            result = self._select_record(
                connection, candidate["job_id"], candidate["handoff_id"]
            )
            connection.commit()
        return result

    def _assert_owned(
        self,
        connection: sqlite3.Connection,
        claim: ContinuationRecord,
        *,
        now: str | None = None,
    ) -> None:
        row = connection.execute(
            "SELECT owner_token,owner_generation,lease_expires_at,wake_state "
            "FROM session_handoff_continuations WHERE job_id=? AND handoff_id=?",
            (claim.job_id, claim.handoff_id),
        ).fetchone()
        observed_at = now if now is not None else self._now_fn()
        if (
            row is None
            or row["owner_token"] != claim.owner_token
            or row["owner_generation"] != claim.owner_generation
            or row["wake_state"] != "DUE"
            or claim_is_expired(row["lease_expires_at"], observed_at)
        ):
            raise ContinuationLeaseLost("continuation lease is stale or fenced")

    def renew(
        self, claim: ContinuationRecord, *, lease_seconds: float
    ) -> ContinuationRecord:
        if lease_seconds <= 0:
            raise ValueError("positive lease is required")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            now = self._now_fn()
            self._assert_owned(connection, claim, now=now)
            changed = connection.execute(
                "UPDATE session_handoff_continuations SET lease_expires_at=?,heartbeat_at=? "
                "WHERE job_id=? AND handoff_id=? AND owner_token=? AND owner_generation=?",
                (
                    add_seconds_iso(now, lease_seconds),
                    now,
                    claim.job_id,
                    claim.handoff_id,
                    claim.owner_token,
                    claim.owner_generation,
                ),
            ).rowcount
            if changed != 1:
                raise ContinuationLeaseLost("continuation renewal CAS lost")
            result = self._select_record(connection, claim.job_id, claim.handoff_id)
        return result

    def record_verified_effect(
        self, claim: ContinuationRecord, *, effect_name: str, receipt_sha256: str
    ) -> ContinuationRecord:
        """Persist an immutable digest, never delivery content or external responses."""
        if effect_name != "handoff_delivery":
            raise ValueError("only the handoff_delivery effect can be verified")
        _require_receipt_sha256(receipt_sha256, name="receipt_sha256")
        now = self._now_fn()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._assert_owned(connection, claim)
            existing = connection.execute(
                "SELECT receipt_sha256 FROM session_handoff_continuation_effects "
                "WHERE job_id=? AND handoff_id=? AND effect_name=?",
                (claim.job_id, claim.handoff_id, effect_name),
            ).fetchone()
            if existing is not None and existing["receipt_sha256"] != receipt_sha256:
                raise DeliveryVerificationFailed(
                    "verified effect receipt digest is immutable"
                )
            connection.execute(
                "INSERT INTO session_handoff_continuation_effects "
                "(job_id,handoff_id,effect_name,receipt_sha256,verified_at,owner_generation) "
                "VALUES (?,?,?,?,?,?) ON CONFLICT(job_id,handoff_id,effect_name) DO NOTHING",
                (
                    claim.job_id,
                    claim.handoff_id,
                    effect_name,
                    receipt_sha256,
                    now,
                    claim.owner_generation,
                ),
            )
            changed = connection.execute(
                "UPDATE session_handoff_continuations SET "
                "checkpoint_stage='VERIFY_HANDOFF_DELIVERY',"
                "next_action='verify_handoff_delivery',"
                "verification_state='RECEIPT_DIGEST_PERSISTED' WHERE job_id=? AND handoff_id=? "
                "AND owner_token=? AND owner_generation=?",
                (
                    claim.job_id,
                    claim.handoff_id,
                    claim.owner_token,
                    claim.owner_generation,
                ),
            ).rowcount
            if changed != 1:
                raise ContinuationLeaseLost("continuation effect CAS lost")
            result = self._select_record(connection, claim.job_id, claim.handoff_id)
        return result

    def effect_is_verified(
        self, claim: ContinuationRecord, *, effect_name: str
    ) -> bool:
        with self._connect() as connection:
            return (
                connection.execute(
                    "SELECT 1 FROM session_handoff_continuation_effects "
                    "WHERE job_id=? AND handoff_id=? AND effect_name=?",
                    (claim.job_id, claim.handoff_id, effect_name),
                ).fetchone()
                is not None
            )

    def complete_delivery(
        self, claim: ContinuationRecord, *, observed_receipt_sha256: str
    ) -> ContinuationRecord:
        """Complete after comparing digests of safe, offline receipt bytes only."""
        _require_receipt_sha256(observed_receipt_sha256, name="observed_receipt_sha256")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._assert_owned(connection, claim)
            effect = connection.execute(
                "SELECT receipt_sha256 FROM session_handoff_continuation_effects "
                "WHERE job_id=? AND handoff_id=? AND effect_name='handoff_delivery'",
                (claim.job_id, claim.handoff_id),
            ).fetchone()
            if effect is None:
                raise DeliveryVerificationFailed(
                    "persisted delivery receipt digest is required"
                )
            if effect["receipt_sha256"] != observed_receipt_sha256:
                connection.execute(
                    "UPDATE session_handoff_continuations SET verification_state='MANUAL_RESUME',"
                    "manual_resume_reason='delivery_receipt_digest_mismatch',wake_state='MANUAL_RESUME',"
                    "owner_token=NULL,lease_expires_at=NULL WHERE job_id=? AND handoff_id=? "
                    "AND owner_token=? AND owner_generation=?",
                    (
                        claim.job_id,
                        claim.handoff_id,
                        claim.owner_token,
                        claim.owner_generation,
                    ),
                )
                connection.commit()
                raise DeliveryVerificationFailed(
                    "delivery receipt digest mismatch; manual resume required"
                )
            changed = connection.execute(
                "UPDATE session_handoff_continuations SET checkpoint_stage='COMPLETE',"
                "verification_state='VERIFIED',wake_state='COMPLETE',next_action='complete',"
                "owner_token=NULL,lease_expires_at=NULL "
                "WHERE job_id=? AND handoff_id=? AND owner_token=? AND owner_generation=?",
                (
                    claim.job_id,
                    claim.handoff_id,
                    claim.owner_token,
                    claim.owner_generation,
                ),
            ).rowcount
            if changed != 1:
                raise ContinuationLeaseLost("continuation completion CAS lost")
            result = self._select_record(connection, claim.job_id, claim.handoff_id)
        return result

    def resume_after_manual_verification(
        self,
        *,
        job_id: str,
        handoff_id: str,
        operator_reason: str,
        confirmed_receipt_sha256: str,
    ) -> ContinuationRecord:
        """Explicitly re-arm a blocked delivery after an operator verifies its digest.

        The immutable receipt evidence and original failure reason remain intact.
        Payloads, provider responses, credentials, and secrets must never be passed.
        """
        reason = operator_reason.strip() if isinstance(operator_reason, str) else ""
        if not reason:
            raise ValueError("a nonempty operator reason is required")
        _require_receipt_sha256(
            confirmed_receipt_sha256, name="confirmed_receipt_sha256"
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            continuation = connection.execute(
                "SELECT verification_state,wake_state FROM "
                "session_handoff_continuations WHERE job_id=? AND handoff_id=?",
                (job_id, handoff_id),
            ).fetchone()
            if continuation is None:
                raise KeyError((job_id, handoff_id))
            if (
                continuation["verification_state"] != "MANUAL_RESUME"
                or continuation["wake_state"] != "MANUAL_RESUME"
            ):
                raise DeliveryVerificationFailed(
                    "continuation is not blocked for manual resume"
                )
            effect = connection.execute(
                "SELECT receipt_sha256 FROM session_handoff_continuation_effects "
                "WHERE job_id=? AND handoff_id=? AND effect_name='handoff_delivery'",
                (job_id, handoff_id),
            ).fetchone()
            if effect is None or effect["receipt_sha256"] != confirmed_receipt_sha256:
                raise DeliveryVerificationFailed(
                    "confirmed receipt digest does not match immutable evidence"
                )
            changed = connection.execute(
                "UPDATE session_handoff_continuations SET "
                "checkpoint_stage='VERIFY_HANDOFF_DELIVERY',"
                "next_action='verify_handoff_delivery',due_at=?,"
                "verification_state='RECEIPT_DIGEST_PERSISTED',wake_state='DUE',"
                "manual_resume_operator_reason=?,owner_token=NULL,"
                "lease_expires_at=NULL,heartbeat_at=NULL "
                "WHERE job_id=? AND handoff_id=?",
                (self._now_fn(), reason, job_id, handoff_id),
            ).rowcount
            if changed != 1:
                raise DeliveryVerificationFailed("manual resume state changed")
            result = self._select_record(connection, job_id, handoff_id)
        return result
