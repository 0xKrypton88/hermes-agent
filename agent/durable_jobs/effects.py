"""ENG-26 Cursor provider reconciliation (isolated, default-off).

Authorities live in this ledger, not LangGraph context:
- immutable job/action identity
- atomic effect claim before any provider call
- persisted claim owner token + lease timestamps
- atomic CAS/fencing so only the owner (or a stale-lease takeover) may complete
- stable provider idempotency key
- job_id ↔ langgraph thread_id (same opaque id) ↔ provider run mapping
- frozen origin Slack + candidate/version snapshot

A non-owner that encounters an unexpired CLAIMED/RECOVERING lease must poll
and must not lookup, create, adopt, increment recovery attempts, or
terminalize. The live owner renews the persisted lease (owner-fenced
heartbeat) so a long create_run cannot be stolen. Renew CAS requires the
lease still unexpired; False/exception is observable and does not cancel
an in-flight create. Only an expired/legacy-null lease may be taken over
(CLAIMED or RECOVERING), minting a new owner token; recovery looks up by
the stable idempotency key and never blindly creates. Empty lookup stays
RECOVERING until ``recovery_deadline``, then typed UNKNOWN **only if** the
persisted in-flight witness has also expired (a live create_run still
renews that witness in SQLite after claim takeover). A persisted
foreign token is never caller authority. Cancel is terminal: a
pre-existing accepted Cancel refuses claim, inflight, create_run, and
recovery lookup. Accepted/adopted bind cannot overwrite Cancel: the
success UPDATE-CAS includes an authoritative Cancel predicate in the
same SQLite transaction. SQLite cannot abort an RPC that already began
after the last Cancel SELECT, and this fence is not PostgreSQL-safe.

SQLite here is disposable, explicit-path, single-process, and dev/test-only.
It does not satisfy ENG-25 production PostgreSQL acceptance.
Live Cursor/provider dispatch is never constructed here.
"""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional, Protocol

from agent.durable_jobs.clock import (
    DEFAULT_CLAIM_LEASE_SECONDS,
    DEFAULT_RECOVERY_MAX_ATTEMPTS,
    DEFAULT_RECOVERY_WINDOW_SECONDS,
    add_seconds_iso,
    claim_is_expired,
    utcnow_iso,
)
from agent.durable_jobs.claim_protocol import (
    caller_holds_live_lease,
    inflight_witness_blocks_unknown,
    owner_lease_heartbeat,
    recovery_bound_exceeded,
)
from agent.durable_jobs.store import DurableJobStore


class EffectStatus(str, Enum):
    CLAIMED = "claimed"
    RECOVERING = "recovering"
    ACCEPTED = "accepted"
    ADOPTED = "adopted"
    UNKNOWN = "unknown"


class UnknownReason(str, Enum):
    EMPTY_LOOKUP = "empty_lookup"
    AMBIGUOUS_LOOKUP = "ambiguous_lookup"
    AMBIGUOUS_RESPONSE = "ambiguous_response"


def provider_idempotency_key(job_id: str, action_id: str) -> str:
    """Stable Cursor-provider idempotency key derived only from job/action identity."""
    return f"cursor:{job_id}:{action_id}"


@dataclass(frozen=True)
class ProviderEffectClaim:
    job_id: str
    action_id: str
    provider_idempotency_key: str
    status: EffectStatus
    provider_run_id: Optional[str]
    langgraph_thread_id: str
    origin_platform: str
    origin_chat_id: str
    origin_root_thread_id: str
    candidate_id: str
    candidate_version: str
    unknown_reason: Optional[str]
    claim_owner_token: Optional[str]
    claim_leased_at: Optional[str]
    claim_expires_at: Optional[str]
    claim_generation: int
    recovery_attempt_count: int
    recovery_started_at: Optional[str]
    recovery_deadline: Optional[str]
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class ClaimResult:
    claim: ProviderEffectClaim
    won: bool
    owner_token: Optional[str] = None


@dataclass(frozen=True)
class JobProviderMapping:
    job_id: str
    langgraph_thread_id: str
    provider_run_id: Optional[str]
    origin_platform: str
    origin_chat_id: str
    origin_root_thread_id: str
    candidate_id: str
    candidate_version: str


class CursorProviderPort(Protocol):
    """Injected-only Cursor create/lookup seam. No live implementation ships here."""

    def create_run(self, *, idempotency_key: str, job_id: str) -> Any: ...

    def lookup_runs(self, *, idempotency_key: str) -> list[Any]: ...


class ProviderEffectLedger:
    """Application-store authority for provider effect claims and mappings."""

    def __init__(
        self,
        sqlite_path: Path,
        now_fn: Optional[Callable[[], str]] = None,
        lease_seconds: int = DEFAULT_CLAIM_LEASE_SECONDS,
        recovery_max_attempts: int = DEFAULT_RECOVERY_MAX_ATTEMPTS,
        recovery_window_seconds: int = DEFAULT_RECOVERY_WINDOW_SECONDS,
    ) -> None:
        self.sqlite_path = Path(sqlite_path)
        # Ensure Package 1 job tables exist before ENG-26 FKs.
        self._jobs = DurableJobStore(sqlite_path=self.sqlite_path)
        self._now_fn = now_fn or utcnow_iso
        self._lease_seconds = int(lease_seconds)
        self._recovery_max_attempts = int(recovery_max_attempts)
        self._recovery_window_seconds = int(recovery_window_seconds)

    def _now(self) -> str:
        return self._now_fn()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.sqlite_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.isolation_level = "IMMEDIATE"
        return conn

    def claim_effect(
        self,
        *,
        job_id: str,
        action_id: str,
        origin_platform: str,
        origin_chat_id: str,
        origin_root_thread_id: str,
        candidate_id: str,
        candidate_version: str,
    ) -> ClaimResult:
        job = self._jobs.get_job(job_id)
        if job is None:
            raise KeyError(f"unknown job_id: {job_id}")
        now = self._now()
        key = provider_idempotency_key(job_id, action_id)
        snapshot = (
            origin_platform,
            origin_chat_id,
            origin_root_thread_id,
            candidate_id,
            candidate_version,
        )
        owner_token = uuid.uuid4().hex
        expires_at = add_seconds_iso(now, self._lease_seconds)
        if self.get_claim(job_id, action_id) is None:
            from agent.durable_jobs.eng29 import (
                PROVIDER_CREATE_TARGET_ACTION,
                raise_unless_adapter_go,
            )

            raise_unless_adapter_go(
                self.sqlite_path,
                job_id=job_id,
                target_action=PROVIDER_CREATE_TARGET_ACTION,
                now_iso=now,
                action="provider claim",
            )
        with self._connect() as conn:
            existing = conn.execute(
                """
                SELECT * FROM provider_effect_claims
                 WHERE job_id = ? AND action_id = ?
                """,
                (job_id, action_id),
            ).fetchone()
            if existing is not None:
                self._reject_mapping_mismatch(conn, job_id, snapshot)
                return ClaimResult(claim=self._row_to_claim(existing), won=False)

            from agent.durable_jobs.decisions import (
                JobCanceledError,
                job_is_canceled_on_conn,
            )

            if job_is_canceled_on_conn(conn, job_id):
                raise JobCanceledError(
                    f"job {job_id} is canceled; refusing provider claim {action_id}"
                )

            mapping = conn.execute(
                "SELECT * FROM provider_job_mappings WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if mapping is not None:
                self._reject_mapping_mismatch(conn, job_id, snapshot, row=mapping)
            try:
                conn.execute(
                    """
                    INSERT INTO provider_effect_claims(
                        job_id, action_id, provider_idempotency_key, status,
                        provider_run_id, langgraph_thread_id, origin_platform,
                        origin_chat_id, origin_root_thread_id, candidate_id,
                        candidate_version, unknown_reason, claim_owner_token,
                        claim_leased_at, claim_expires_at, claim_generation,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, NULL,
                              ?, ?, ?, 1, ?, ?)
                    """,
                    (
                        job_id,
                        action_id,
                        key,
                        EffectStatus.CLAIMED.value,
                        job_id,
                        origin_platform,
                        origin_chat_id,
                        origin_root_thread_id,
                        candidate_id,
                        candidate_version,
                        owner_token,
                        now,
                        expires_at,
                        now,
                        now,
                    ),
                )
                if mapping is None:
                    conn.execute(
                        """
                        INSERT INTO provider_job_mappings(
                            job_id, langgraph_thread_id, provider_run_id,
                            origin_platform, origin_chat_id, origin_root_thread_id,
                            candidate_id, candidate_version, created_at, updated_at
                        ) VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            job_id,
                            job_id,
                            origin_platform,
                            origin_chat_id,
                            origin_root_thread_id,
                            candidate_id,
                            candidate_version,
                            now,
                            now,
                        ),
                    )
                DurableJobStore._append_event(
                    conn,
                    job_id=job_id,
                    event_type="provider_effect_claimed",
                    payload={
                        "action_id": action_id,
                        "provider_idempotency_key": key,
                        "claim_generation": 1,
                    },
                    idempotency_key=f"provider_effect_claimed:{job_id}:{action_id}",
                )
            except sqlite3.IntegrityError:
                adopted = conn.execute(
                    """
                    SELECT * FROM provider_effect_claims
                     WHERE job_id = ? AND action_id = ?
                    """,
                    (job_id, action_id),
                ).fetchone()
                if adopted is None:
                    raise
                return ClaimResult(claim=self._row_to_claim(adopted), won=False)
            row = conn.execute(
                """
                SELECT * FROM provider_effect_claims
                 WHERE job_id = ? AND action_id = ?
                """,
                (job_id, action_id),
            ).fetchone()
        assert row is not None
        return ClaimResult(
            claim=self._row_to_claim(row), won=True, owner_token=owner_token
        )

    def takeover_stale_claim(self, job_id: str, action_id: str) -> ClaimResult:
        """Atomically take an expired/legacy CLAIMED or RECOVERING row.

        Mints a new owner token. Unexpired → poll. RECOVERING keeps its
        recovery window; the new owner starts attempt bookkeeping at 0.
        """
        now = self._now()
        owner_token = uuid.uuid4().hex
        expires_at = add_seconds_iso(now, self._lease_seconds)
        with self._connect() as conn:
            current = conn.execute(
                """
                SELECT * FROM provider_effect_claims
                 WHERE job_id = ? AND action_id = ?
                """,
                (job_id, action_id),
            ).fetchone()
            if current is None:
                raise KeyError(f"unknown effect claim: {job_id}/{action_id}")
            claim = self._row_to_claim(current)
            if claim.status not in (EffectStatus.CLAIMED, EffectStatus.RECOVERING):
                return ClaimResult(claim=claim, won=False)
            from agent.durable_jobs.decisions import job_is_canceled_on_conn

            if job_is_canceled_on_conn(conn, job_id):
                return ClaimResult(claim=claim, won=False)
            if not claim_is_expired(claim.claim_expires_at, now):
                return ClaimResult(claim=claim, won=False)
            cur = conn.execute(
                """
                UPDATE provider_effect_claims
                   SET claim_owner_token = ?, claim_leased_at = ?,
                       claim_expires_at = ?,
                       claim_generation = claim_generation + 1,
                       recovery_attempt_count = CASE
                           WHEN status = ? THEN 0
                           ELSE recovery_attempt_count
                       END,
                       updated_at = ?
                 WHERE job_id = ? AND action_id = ? AND status IN (?, ?)
                   AND claim_generation = ?
                   AND (claim_expires_at IS NULL OR claim_expires_at <= ?)
                """,
                (
                    owner_token,
                    now,
                    expires_at,
                    EffectStatus.RECOVERING.value,
                    now,
                    job_id,
                    action_id,
                    EffectStatus.CLAIMED.value,
                    EffectStatus.RECOVERING.value,
                    claim.claim_generation,
                    now,
                ),
            )
            if cur.rowcount != 1:
                row = conn.execute(
                    """
                    SELECT * FROM provider_effect_claims
                     WHERE job_id = ? AND action_id = ?
                    """,
                    (job_id, action_id),
                ).fetchone()
                assert row is not None
                return ClaimResult(claim=self._row_to_claim(row), won=False)
            DurableJobStore._append_event(
                conn,
                job_id=job_id,
                event_type="provider_effect_claim_taken",
                payload={
                    "action_id": action_id,
                    "claim_generation": claim.claim_generation + 1,
                },
                idempotency_key=(
                    f"provider_effect_claim_taken:{job_id}:{action_id}:"
                    f"{claim.claim_generation + 1}"
                ),
            )
            row = conn.execute(
                """
                SELECT * FROM provider_effect_claims
                 WHERE job_id = ? AND action_id = ?
                """,
                (job_id, action_id),
            ).fetchone()
        assert row is not None
        return ClaimResult(
            claim=self._row_to_claim(row), won=True, owner_token=owner_token
        )

    def renew_claim(self, job_id: str, action_id: str, *, owner_token: str) -> bool:
        """Owner-fenced lease renewal. Requires an unexpired matching lease.

        Late renewal after ``claim_expires_at`` (or legacy NULL expiry) returns
        False even if nobody has taken over yet.
        """
        now = self._now()
        expires_at = add_seconds_iso(now, self._lease_seconds)
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE provider_effect_claims
                   SET claim_leased_at = ?, claim_expires_at = ?, updated_at = ?
                 WHERE job_id = ? AND action_id = ?
                   AND claim_owner_token = ?
                   AND status IN (?, ?)
                   AND claim_expires_at IS NOT NULL
                   AND claim_expires_at > ?
                """,
                (
                    now,
                    expires_at,
                    now,
                    job_id,
                    action_id,
                    owner_token,
                    EffectStatus.CLAIMED.value,
                    EffectStatus.RECOVERING.value,
                    now,
                ),
            )
            return cur.rowcount == 1

    def begin_inflight(self, job_id: str, action_id: str, *, owner_token: str) -> bool:
        """Persist that this owner has an outstanding create_run.

        Survives takeover of the claim lease. Cleared when the RPC returns.
        A crash leaves ``effect_inflight_until`` to expire on its own.
        """
        now = self._now()
        until = add_seconds_iso(now, self._lease_seconds)
        with self._connect() as conn:
            from agent.durable_jobs.decisions import job_is_canceled_on_conn

            if job_is_canceled_on_conn(conn, job_id):
                return False
            cur = conn.execute(
                """
                UPDATE provider_effect_claims
                   SET effect_inflight_token = ?, effect_inflight_until = ?,
                       updated_at = ?
                 WHERE job_id = ? AND action_id = ?
                   AND claim_owner_token = ?
                   AND status IN (?, ?)
                """,
                (
                    owner_token,
                    until,
                    now,
                    job_id,
                    action_id,
                    owner_token,
                    EffectStatus.CLAIMED.value,
                    EffectStatus.RECOVERING.value,
                ),
            )
            return cur.rowcount == 1

    def renew_inflight(self, job_id: str, action_id: str, *, owner_token: str) -> bool:
        """Renew the in-flight witness. Late renew is allowed while token matches.

        Claim-lease takeover does not steal this slot. One-shot FrozenClock
        jumps still keep a live RPC from being UNKNOWN'd.
        """
        now = self._now()
        until = add_seconds_iso(now, self._lease_seconds)
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE provider_effect_claims
                   SET effect_inflight_until = ?, updated_at = ?
                 WHERE job_id = ? AND action_id = ?
                   AND effect_inflight_token = ?
                   AND status IN (?, ?)
                """,
                (
                    until,
                    now,
                    job_id,
                    action_id,
                    owner_token,
                    EffectStatus.CLAIMED.value,
                    EffectStatus.RECOVERING.value,
                ),
            )
            return cur.rowcount == 1

    def clear_inflight(self, job_id: str, action_id: str, *, owner_token: str) -> None:
        now = self._now()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE provider_effect_claims
                   SET effect_inflight_token = NULL, effect_inflight_until = NULL,
                       updated_at = ?
                 WHERE job_id = ? AND action_id = ?
                   AND effect_inflight_token = ?
                """,
                (now, job_id, action_id, owner_token),
            )

    def _job_is_canceled(self, job_id: str) -> bool:
        from agent.durable_jobs.decisions import DecisionLedger

        return DecisionLedger(sqlite_path=self.sqlite_path).is_canceled(job_id)

    def get_claim(self, job_id: str, action_id: str) -> Optional[ProviderEffectClaim]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM provider_effect_claims
                 WHERE job_id = ? AND action_id = ?
                """,
                (job_id, action_id),
            ).fetchone()
        return self._row_to_claim(row) if row else None

    def get_mapping(self, job_id: str) -> Optional[JobProviderMapping]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM provider_job_mappings WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        return self._row_to_mapping(row) if row else None

    def count_claims(self) -> int:
        with self._connect() as conn:
            (count,) = conn.execute(
                "SELECT COUNT(*) FROM provider_effect_claims"
            ).fetchone()
        return int(count)

    def mark_accepted(
        self, job_id: str, action_id: str, provider_run_id: str, *, owner_token: str
    ) -> ProviderEffectClaim:
        return self._complete_claim(
            job_id,
            action_id,
            provider_run_id=provider_run_id,
            status=EffectStatus.ACCEPTED,
            event_type="provider_effect_accepted",
            owner_token=owner_token,
        )

    def adopt_run(
        self, job_id: str, action_id: str, provider_run_id: str, *, owner_token: str
    ) -> ProviderEffectClaim:
        return self._complete_claim(
            job_id,
            action_id,
            provider_run_id=provider_run_id,
            status=EffectStatus.ADOPTED,
            event_type="provider_effect_adopted",
            owner_token=owner_token,
        )

    def mark_unknown(
        self, job_id: str, action_id: str, reason: str, *, owner_token: str
    ) -> ProviderEffectClaim:
        return self._complete_claim(
            job_id,
            action_id,
            provider_run_id=None,
            status=EffectStatus.UNKNOWN,
            event_type="provider_effect_unknown",
            unknown_reason=reason,
            owner_token=owner_token,
        )

    def note_empty_lookup(
        self, job_id: str, action_id: str, *, owner_token: str
    ) -> ProviderEffectClaim:
        """Persist bounded RECOVERING on empty lookup; UNKNOWN only at the bound."""
        now = self._now()
        deadline = add_seconds_iso(now, self._recovery_window_seconds)
        expires_at = add_seconds_iso(now, self._lease_seconds)
        with self._connect() as conn:
            current = conn.execute(
                """
                SELECT * FROM provider_effect_claims
                 WHERE job_id = ? AND action_id = ?
                """,
                (job_id, action_id),
            ).fetchone()
            if current is None:
                raise KeyError(f"unknown effect claim: {job_id}/{action_id}")
            claim = self._row_to_claim(current)
            if claim.status not in (EffectStatus.CLAIMED, EffectStatus.RECOVERING):
                return claim
            cur = conn.execute(
                """
                UPDATE provider_effect_claims
                   SET status = ?,
                       recovery_attempt_count = recovery_attempt_count + 1,
                       recovery_started_at = COALESCE(recovery_started_at, ?),
                       recovery_deadline = COALESCE(recovery_deadline, ?),
                       claim_leased_at = ?,
                       claim_expires_at = ?,
                       updated_at = ?
                 WHERE job_id = ? AND action_id = ?
                   AND status IN (?, ?)
                   AND claim_owner_token = ?
                """,
                (
                    EffectStatus.RECOVERING.value,
                    now,
                    deadline,
                    now,
                    expires_at,
                    now,
                    job_id,
                    action_id,
                    EffectStatus.CLAIMED.value,
                    EffectStatus.RECOVERING.value,
                    owner_token,
                ),
            )
            if cur.rowcount != 1:
                row = conn.execute(
                    """
                    SELECT * FROM provider_effect_claims
                     WHERE job_id = ? AND action_id = ?
                    """,
                    (job_id, action_id),
                ).fetchone()
                assert row is not None
                return self._row_to_claim(row)
            DurableJobStore._append_event(
                conn,
                job_id=job_id,
                event_type="provider_effect_recovering",
                payload={
                    "action_id": action_id,
                    "claim_generation": claim.claim_generation,
                },
                idempotency_key=(
                    f"provider_effect_recovering:{job_id}:{action_id}:"
                    f"{claim.claim_generation}"
                ),
            )
            row = conn.execute(
                """
                SELECT * FROM provider_effect_claims
                 WHERE job_id = ? AND action_id = ?
                """,
                (job_id, action_id),
            ).fetchone()
        assert row is not None
        updated = self._row_to_claim(row)
        if recovery_bound_exceeded(
            attempt_count=updated.recovery_attempt_count,
            deadline=updated.recovery_deadline,
            now_iso=now,
            max_attempts=self._recovery_max_attempts,
        ):
            return self.mark_unknown(
                job_id,
                action_id,
                UnknownReason.EMPTY_LOOKUP.value,
                owner_token=owner_token,
            )
        return updated

    def _complete_claim(
        self,
        job_id: str,
        action_id: str,
        *,
        provider_run_id: Optional[str],
        status: EffectStatus,
        event_type: str,
        owner_token: str,
        unknown_reason: Optional[str] = None,
    ) -> ProviderEffectClaim:
        now = self._now()
        # Fast path only. The success UPDATE-CAS Cancel predicate below is
        # the fence; a separate-connection pre-check is not sufficient.
        if status in (EffectStatus.ACCEPTED, EffectStatus.ADOPTED) and self._job_is_canceled(
            job_id
        ):
            blocked = self.get_claim(job_id, action_id)
            if blocked is None:
                raise KeyError(f"unknown effect claim: {job_id}/{action_id}")
            return blocked
        with self._connect() as conn:
            current = conn.execute(
                """
                SELECT * FROM provider_effect_claims
                 WHERE job_id = ? AND action_id = ?
                """,
                (job_id, action_id),
            ).fetchone()
            if current is None:
                raise KeyError(f"unknown effect claim: {job_id}/{action_id}")
            claim = self._row_to_claim(current)
            if claim.status not in (EffectStatus.CLAIMED, EffectStatus.RECOVERING):
                return claim
            inflight_until = None
            inflight_token = None
            if "effect_inflight_until" in current.keys():
                inflight_until = current["effect_inflight_until"]
            if "effect_inflight_token" in current.keys():
                inflight_token = current["effect_inflight_token"]
            if (
                status is EffectStatus.UNKNOWN
                and inflight_token != owner_token
                and inflight_witness_blocks_unknown(
                    inflight_until=inflight_until, now_iso=now
                )
            ):
                return claim
            unknown_inflight_sql = ""
            unknown_inflight_args: tuple = ()
            if status is EffectStatus.UNKNOWN:
                unknown_inflight_sql = (
                    " AND (effect_inflight_until IS NULL"
                    " OR effect_inflight_until <= ?"
                    " OR effect_inflight_token = ?)"
                )
                unknown_inflight_args = (now, owner_token)
            cancel_sql = ""
            if status in (EffectStatus.ACCEPTED, EffectStatus.ADOPTED):
                from agent.durable_jobs.decisions import (
                    sql_reject_authoritative_cancel,
                )

                cancel_sql = sql_reject_authoritative_cancel(
                    "provider_effect_claims"
                )
            cur = conn.execute(
                f"""
                UPDATE provider_effect_claims
                   SET status = ?, provider_run_id = ?, unknown_reason = ?,
                       effect_inflight_token = NULL, effect_inflight_until = NULL,
                       updated_at = ?
                 WHERE job_id = ? AND action_id = ?
                   AND status IN (?, ?)
                   AND claim_owner_token = ?
                   {unknown_inflight_sql}
                   {cancel_sql}
                """,
                (
                    status.value,
                    provider_run_id,
                    unknown_reason,
                    now,
                    job_id,
                    action_id,
                    EffectStatus.CLAIMED.value,
                    EffectStatus.RECOVERING.value,
                    owner_token,
                    *unknown_inflight_args,
                ),
            )
            if cur.rowcount != 1:
                row = conn.execute(
                    """
                    SELECT * FROM provider_effect_claims
                     WHERE job_id = ? AND action_id = ?
                    """,
                    (job_id, action_id),
                ).fetchone()
                assert row is not None
                return self._row_to_claim(row)
            if provider_run_id is not None:
                conn.execute(
                    """
                    UPDATE provider_job_mappings
                       SET provider_run_id = ?, updated_at = ?
                     WHERE job_id = ?
                    """,
                    (provider_run_id, now, job_id),
                )
            DurableJobStore._append_event(
                conn,
                job_id=job_id,
                event_type=event_type,
                payload={
                    "action_id": action_id,
                    "status": status.value,
                    "provider_run_id": provider_run_id,
                    "unknown_reason": unknown_reason,
                },
                idempotency_key=f"{event_type}:{job_id}:{action_id}",
            )
            row = conn.execute(
                """
                SELECT * FROM provider_effect_claims
                 WHERE job_id = ? AND action_id = ?
                """,
                (job_id, action_id),
            ).fetchone()
        assert row is not None
        return self._row_to_claim(row)

    def bind_observed_run(
        self,
        job_id: str,
        action_id: str,
        provider_run_id: str,
        *,
        status: EffectStatus,
    ) -> ProviderEffectClaim:
        """Bind a uniquely observed provider run without a caller owner token.

        In-flight ``create_run`` cannot be canceled. If the original owner is
        fenced after heartbeat loss, this CAS still completes while the row is
        CLAIMED/RECOVERING. Already-terminal rows (including UNKNOWN) are
        unchanged — terminal events fire only when this CAS wins.
        """
        if status not in (EffectStatus.ACCEPTED, EffectStatus.ADOPTED):
            raise ValueError(f"bind_observed_run status must be accepted/adopted, got {status}")
        event_type = (
            "provider_effect_accepted"
            if status is EffectStatus.ACCEPTED
            else "provider_effect_adopted"
        )
        now = self._now()
        # Fast path only. The success UPDATE-CAS Cancel predicate below is
        # the fence; a separate-connection pre-check is not sufficient.
        if self._job_is_canceled(job_id):
            blocked = self.get_claim(job_id, action_id)
            if blocked is None:
                raise KeyError(f"unknown effect claim: {job_id}/{action_id}")
            return blocked
        with self._connect() as conn:
            current = conn.execute(
                """
                SELECT * FROM provider_effect_claims
                 WHERE job_id = ? AND action_id = ?
                """,
                (job_id, action_id),
            ).fetchone()
            if current is None:
                raise KeyError(f"unknown effect claim: {job_id}/{action_id}")
            claim = self._row_to_claim(current)
            if claim.status not in (EffectStatus.CLAIMED, EffectStatus.RECOVERING):
                return claim
            from agent.durable_jobs.decisions import sql_reject_authoritative_cancel

            cur = conn.execute(
                f"""
                UPDATE provider_effect_claims
                   SET status = ?, provider_run_id = ?, unknown_reason = NULL,
                       effect_inflight_token = NULL, effect_inflight_until = NULL,
                       updated_at = ?
                 WHERE job_id = ? AND action_id = ?
                   AND status IN (?, ?)
                   AND (provider_run_id IS NULL OR provider_run_id = ?)
                   {sql_reject_authoritative_cancel("provider_effect_claims")}
                """,
                (
                    status.value,
                    provider_run_id,
                    now,
                    job_id,
                    action_id,
                    EffectStatus.CLAIMED.value,
                    EffectStatus.RECOVERING.value,
                    provider_run_id,
                ),
            )
            if cur.rowcount != 1:
                row = conn.execute(
                    """
                    SELECT * FROM provider_effect_claims
                     WHERE job_id = ? AND action_id = ?
                    """,
                    (job_id, action_id),
                ).fetchone()
                assert row is not None
                return self._row_to_claim(row)
            conn.execute(
                """
                UPDATE provider_job_mappings
                   SET provider_run_id = ?, updated_at = ?
                 WHERE job_id = ?
                """,
                (provider_run_id, now, job_id),
            )
            DurableJobStore._append_event(
                conn,
                job_id=job_id,
                event_type=event_type,
                payload={
                    "action_id": action_id,
                    "status": status.value,
                    "provider_run_id": provider_run_id,
                    "unknown_reason": None,
                },
                idempotency_key=f"{event_type}:{job_id}:{action_id}",
            )
            row = conn.execute(
                """
                SELECT * FROM provider_effect_claims
                 WHERE job_id = ? AND action_id = ?
                """,
                (job_id, action_id),
            ).fetchone()
        assert row is not None
        return self._row_to_claim(row)

    @staticmethod
    def _reject_mapping_mismatch(
        conn: sqlite3.Connection,
        job_id: str,
        snapshot: tuple[str, str, str, str, str],
        row: Optional[sqlite3.Row] = None,
    ) -> None:
        mapping = row or conn.execute(
            "SELECT * FROM provider_job_mappings WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        if mapping is None:
            return
        existing = (
            mapping["origin_platform"],
            mapping["origin_chat_id"],
            mapping["origin_root_thread_id"],
            mapping["candidate_id"],
            mapping["candidate_version"],
        )
        if existing != snapshot:
            raise ValueError(
                f"immutable mapping mismatch for {job_id}: "
                f"{existing!r} != {snapshot!r}"
            )

    @staticmethod
    def _row_to_claim(row: sqlite3.Row) -> ProviderEffectClaim:
        keys = set(row.keys())
        generation = 0
        if "claim_generation" in keys:
            generation = int(row["claim_generation"] or 0)
        return ProviderEffectClaim(
            job_id=row["job_id"],
            action_id=row["action_id"],
            provider_idempotency_key=row["provider_idempotency_key"],
            status=EffectStatus(row["status"]),
            provider_run_id=row["provider_run_id"],
            langgraph_thread_id=row["langgraph_thread_id"],
            origin_platform=row["origin_platform"],
            origin_chat_id=row["origin_chat_id"],
            origin_root_thread_id=row["origin_root_thread_id"],
            candidate_id=row["candidate_id"],
            candidate_version=row["candidate_version"],
            unknown_reason=row["unknown_reason"],
            claim_owner_token=(
                row["claim_owner_token"] if "claim_owner_token" in keys else None
            ),
            claim_leased_at=(
                row["claim_leased_at"] if "claim_leased_at" in keys else None
            ),
            claim_expires_at=(
                row["claim_expires_at"] if "claim_expires_at" in keys else None
            ),
            claim_generation=generation,
            recovery_attempt_count=int(row["recovery_attempt_count"] or 0)
            if "recovery_attempt_count" in keys
            else 0,
            recovery_started_at=(
                row["recovery_started_at"] if "recovery_started_at" in keys else None
            ),
            recovery_deadline=(
                row["recovery_deadline"] if "recovery_deadline" in keys else None
            ),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_mapping(row: sqlite3.Row) -> JobProviderMapping:
        return JobProviderMapping(
            job_id=row["job_id"],
            langgraph_thread_id=row["langgraph_thread_id"],
            provider_run_id=row["provider_run_id"],
            origin_platform=row["origin_platform"],
            origin_chat_id=row["origin_chat_id"],
            origin_root_thread_id=row["origin_root_thread_id"],
            candidate_id=row["candidate_id"],
            candidate_version=row["candidate_version"],
        )


def reconcile_cursor_create(
    ledger: ProviderEffectLedger,
    provider: CursorProviderPort,
    *,
    job_id: str,
    action_id: str,
    origin_platform: str,
    origin_chat_id: str,
    origin_root_thread_id: str,
    candidate_id: str,
    candidate_version: str,
    owner_token: Optional[str] = None,
) -> ProviderEffectClaim:
    """Claim first, then reconcile a fake provider create. Never blindly redispatch.

    UNKNOWN / ACCEPTED / ADOPTED claims do not call ``create_run`` again.
    A live foreign CLAIMED/RECOVERING is polled — no lookup, create, adopt,
    attempt increment, or UNKNOWN. The winner renews the persisted lease
    while ``create_run`` is in flight; renew False/exception is observable
    and does not cancel that call. Only an expired CLAIMED/RECOVERING may
    be taken over (new token); recovery looks up by the stable idempotency
    key.     Empty lookup stays RECOVERING until ``recovery_deadline`` **and**
    the persisted in-flight witness has expired. A pre-existing accepted
    Cancel refuses claim/inflight/create_run/lookup; Cancel after RPC
    begin stays terminal and cannot be overwritten by accepted/adopted
    bind. SQLite cannot abort an outstanding adapter RPC.
    """
    origin_platform, origin_chat_id, origin_root_thread_id = (
        _authoritative_origin_from_slack_binding(
            ledger,
            job_id,
            origin_platform=origin_platform,
            origin_chat_id=origin_chat_id,
            origin_root_thread_id=origin_root_thread_id,
        )
    )
    claimed = ledger.claim_effect(
        job_id=job_id,
        action_id=action_id,
        origin_platform=origin_platform,
        origin_chat_id=origin_chat_id,
        origin_root_thread_id=origin_root_thread_id,
        candidate_id=candidate_id,
        candidate_version=candidate_version,
    )
    claim = claimed.claim
    if claim.status in (
        EffectStatus.ACCEPTED,
        EffectStatus.ADOPTED,
        EffectStatus.UNKNOWN,
    ):
        return claim
    if claim.status is EffectStatus.RECOVERING:
        return _recover_or_poll_provider(
            ledger, provider, claim, caller_token=owner_token
        )
    if not claimed.won:
        if claim.status is EffectStatus.CLAIMED:
            if ledger._job_is_canceled(job_id):
                return claim
            taken = ledger.takeover_stale_claim(job_id, action_id)
            if not taken.won:
                return taken.claim
            return _recover_claimed_provider(
                ledger, provider, taken.claim, owner_token=taken.owner_token
            )
        return claim

    won_token = claimed.owner_token
    if not won_token:
        return claim

    def _renew_owner() -> bool:
        inflight_ok = ledger.renew_inflight(
            job_id, action_id, owner_token=won_token
        )
        claim_ok = ledger.renew_claim(
            job_id, action_id, owner_token=won_token
        )
        return bool(inflight_ok or claim_ok)

    from agent.durable_jobs.decisions import raise_if_job_canceled

    if not ledger.begin_inflight(job_id, action_id, owner_token=won_token):
        raise_if_job_canceled(
            ledger.sqlite_path, job_id, action="provider create_run"
        )
        current = ledger.get_claim(job_id, action_id)
        return current if current is not None else claim

    try:
        # Latest committed-Cancel SELECT before the adapter call. If Cancel
        # commits after this check, create_run may still execute; adapters
        # cannot abort an outstanding RPC. Bind stays fail-closed.
        raise_if_job_canceled(
            ledger.sqlite_path, job_id, action="provider create_run"
        )
        from agent.durable_jobs.eng29 import (
            PROVIDER_CREATE_TARGET_ACTION,
            raise_unless_adapter_go,
        )

        raise_unless_adapter_go(
            ledger.sqlite_path,
            job_id=job_id,
            target_action=PROVIDER_CREATE_TARGET_ACTION,
            now_iso=ledger._now(),
            action="provider create_run",
        )
        with owner_lease_heartbeat(
            renew_fn=_renew_owner,
            now_fn=ledger._now_fn,
            lease_seconds=ledger._lease_seconds,
        ):
            result = provider.create_run(
                idempotency_key=claim.provider_idempotency_key,
                job_id=job_id,
            )
        kind = getattr(result, "kind", None)
        run = getattr(result, "run", None)
        if kind == "accepted" and run is not None:
            return _finish_observed_provider_run(
                ledger,
                claim,
                run.run_id,
                owner_token=won_token,
                status=EffectStatus.ACCEPTED,
            )
        if kind == "ambiguous_response":
            unknown = ledger.mark_unknown(
                job_id,
                action_id,
                UnknownReason.AMBIGUOUS_RESPONSE.value,
                owner_token=won_token,
            )
            if unknown.status is EffectStatus.UNKNOWN:
                return unknown
            current = ledger.get_claim(job_id, action_id)
            return current if current is not None else unknown
        return _recover_claimed_provider(
            ledger, provider, claim, owner_token=won_token
        )
    finally:
        ledger.clear_inflight(job_id, action_id, owner_token=won_token)


def _authoritative_origin_from_slack_binding(
    ledger: ProviderEffectLedger,
    job_id: str,
    *,
    origin_platform: str,
    origin_chat_id: str,
    origin_root_thread_id: str,
) -> tuple[str, str, str]:
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        resolve_provider_origin,
    )

    binding = SlackBindingLedger(sqlite_path=ledger.sqlite_path).get_binding(job_id)
    if binding is None:
        return origin_platform, origin_chat_id, origin_root_thread_id
    return resolve_provider_origin(
        binding,
        origin_platform=origin_platform,
        origin_chat_id=origin_chat_id,
        origin_root_thread_id=origin_root_thread_id,
    )


def _recover_or_poll_provider(
    ledger: ProviderEffectLedger,
    provider: CursorProviderPort,
    claim: ProviderEffectClaim,
    *,
    caller_token: Optional[str],
) -> ProviderEffectClaim:
    """RECOVERING: matching live owner looks up; anyone else polls or takeovers."""
    if ledger._job_is_canceled(claim.job_id):
        current = ledger.get_claim(claim.job_id, claim.action_id)
        return current if current is not None else claim
    if caller_holds_live_lease(
        caller_token=caller_token,
        persisted_token=claim.claim_owner_token,
        expires_at=claim.claim_expires_at,
        now_iso=ledger._now(),
        status=claim.status.value,
        live_statuses=(EffectStatus.RECOVERING.value,),
    ):
        assert caller_token is not None
        return _recover_claimed_provider(
            ledger, provider, claim, owner_token=caller_token
        )
    taken = ledger.takeover_stale_claim(claim.job_id, claim.action_id)
    if not taken.won:
        return taken.claim
    return _recover_claimed_provider(
        ledger, provider, taken.claim, owner_token=taken.owner_token
    )


def _finish_observed_provider_run(
    ledger: ProviderEffectLedger,
    claim: ProviderEffectClaim,
    provider_run_id: str,
    *,
    owner_token: str,
    status: EffectStatus,
) -> ProviderEffectClaim:
    if status is EffectStatus.ACCEPTED:
        completed = ledger.mark_accepted(
            claim.job_id,
            claim.action_id,
            provider_run_id=provider_run_id,
            owner_token=owner_token,
        )
    else:
        completed = ledger.adopt_run(
            claim.job_id,
            claim.action_id,
            provider_run_id=provider_run_id,
            owner_token=owner_token,
        )
    if (
        completed.status in (EffectStatus.ACCEPTED, EffectStatus.ADOPTED)
        and completed.provider_run_id == provider_run_id
    ):
        return completed
    return ledger.bind_observed_run(
        claim.job_id, claim.action_id, provider_run_id, status=status
    )


def _recover_claimed_provider(
    ledger: ProviderEffectLedger,
    provider: CursorProviderPort,
    claim: ProviderEffectClaim,
    *,
    owner_token: Optional[str],
) -> ProviderEffectClaim:
    if not owner_token:
        return claim
    if ledger._job_is_canceled(claim.job_id):
        current = ledger.get_claim(claim.job_id, claim.action_id)
        return current if current is not None else claim
    from agent.durable_jobs.eng29 import (
        PROVIDER_CREATE_TARGET_ACTION,
        raise_unless_adapter_go,
    )

    raise_unless_adapter_go(
        ledger.sqlite_path,
        job_id=claim.job_id,
        target_action=PROVIDER_CREATE_TARGET_ACTION,
        now_iso=ledger._now(),
        action="provider lookup",
    )
    matches = list(
        provider.lookup_runs(idempotency_key=claim.provider_idempotency_key)
    )
    if len(matches) == 1:
        return _finish_observed_provider_run(
            ledger,
            claim,
            matches[0].run_id,
            owner_token=owner_token,
            status=EffectStatus.ADOPTED,
        )
    if len(matches) == 0:
        return ledger.note_empty_lookup(
            claim.job_id, claim.action_id, owner_token=owner_token
        )
    unknown = ledger.mark_unknown(
        claim.job_id,
        claim.action_id,
        UnknownReason.AMBIGUOUS_LOOKUP.value,
        owner_token=owner_token,
    )
    if unknown.status is EffectStatus.UNKNOWN:
        return unknown
    current = ledger.get_claim(claim.job_id, claim.action_id)
    return current if current is not None else unknown
