"""ENG-26 Cursor provider reconciliation (isolated, default-off).

Authorities live in this ledger, not LangGraph context:
- immutable job/action identity
- atomic effect claim before any provider call
- persisted claim owner token + lease timestamps
- atomic CAS/fencing so only the owner (or a stale-lease takeover) may complete
- stable provider idempotency key
- job_id ↔ langgraph thread_id (same opaque id) ↔ provider run mapping
- frozen origin Slack + candidate/version snapshot

A non-owner that encounters an unexpired CLAIMED must poll and must not
lookup, create, adopt, or terminalize. Only an expired/legacy-null lease may
be taken over; recovery looks up by the stable idempotency key and never
blindly creates.

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
    add_seconds_iso,
    claim_is_expired,
    utcnow_iso,
)
from agent.durable_jobs.store import DurableJobStore


class EffectStatus(str, Enum):
    CLAIMED = "claimed"
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
    ) -> None:
        self.sqlite_path = Path(sqlite_path)
        # Ensure Package 1 job tables exist before ENG-26 FKs.
        self._jobs = DurableJobStore(sqlite_path=self.sqlite_path)
        self._now_fn = now_fn or utcnow_iso
        self._lease_seconds = int(lease_seconds)

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
        """Atomically take an expired/legacy CLAIMED row. Unexpired → poll."""
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
            if claim.status is not EffectStatus.CLAIMED:
                return ClaimResult(claim=claim, won=False)
            if not claim_is_expired(claim.claim_expires_at, now):
                return ClaimResult(claim=claim, won=False)
            cur = conn.execute(
                """
                UPDATE provider_effect_claims
                   SET claim_owner_token = ?, claim_leased_at = ?,
                       claim_expires_at = ?,
                       claim_generation = claim_generation + 1,
                       updated_at = ?
                 WHERE job_id = ? AND action_id = ? AND status = ?
                   AND claim_generation = ?
                   AND (claim_expires_at IS NULL OR claim_expires_at <= ?)
                """,
                (
                    owner_token,
                    now,
                    expires_at,
                    now,
                    job_id,
                    action_id,
                    EffectStatus.CLAIMED.value,
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
            if claim.status is not EffectStatus.CLAIMED:
                return claim
            cur = conn.execute(
                """
                UPDATE provider_effect_claims
                   SET status = ?, provider_run_id = ?, unknown_reason = ?,
                       updated_at = ?
                 WHERE job_id = ? AND action_id = ? AND status = ?
                   AND claim_owner_token = ?
                """,
                (
                    status.value,
                    provider_run_id,
                    unknown_reason,
                    now,
                    job_id,
                    action_id,
                    EffectStatus.CLAIMED.value,
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
) -> ProviderEffectClaim:
    """Claim first, then reconcile a fake provider create. Never blindly redispatch.

    UNKNOWN / ACCEPTED / ADOPTED claims do not call ``create_run`` again.
    A live foreign CLAIMED is polled — no lookup, create, adopt, or UNKNOWN.
    Only an expired CLAIMED may be taken over; recovery looks up by the
    stable idempotency key and never blindly calls ``create_run``.
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
    if not claimed.won:
        if claim.status is EffectStatus.CLAIMED:
            taken = ledger.takeover_stale_claim(job_id, action_id)
            if not taken.won:
                return taken.claim
            return _recover_claimed_provider(
                ledger, provider, taken.claim, owner_token=taken.owner_token
            )
        return claim

    owner_token = claimed.owner_token
    if not owner_token:
        return claim
    result = provider.create_run(
        idempotency_key=claim.provider_idempotency_key,
        job_id=job_id,
    )
    kind = getattr(result, "kind", None)
    run = getattr(result, "run", None)
    if kind == "accepted" and run is not None:
        return ledger.mark_accepted(
            job_id, action_id, provider_run_id=run.run_id, owner_token=owner_token
        )
    if kind == "ambiguous_response":
        return ledger.mark_unknown(
            job_id,
            action_id,
            UnknownReason.AMBIGUOUS_RESPONSE.value,
            owner_token=owner_token,
        )
    return _recover_claimed_provider(
        ledger, provider, claim, owner_token=owner_token
    )


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


def _recover_claimed_provider(
    ledger: ProviderEffectLedger,
    provider: CursorProviderPort,
    claim: ProviderEffectClaim,
    *,
    owner_token: Optional[str],
) -> ProviderEffectClaim:
    if not owner_token:
        return claim
    matches = list(
        provider.lookup_runs(idempotency_key=claim.provider_idempotency_key)
    )
    if len(matches) == 1:
        return ledger.adopt_run(
            claim.job_id,
            claim.action_id,
            provider_run_id=matches[0].run_id,
            owner_token=owner_token,
        )
    if len(matches) == 0:
        return ledger.mark_unknown(
            claim.job_id,
            claim.action_id,
            UnknownReason.EMPTY_LOOKUP.value,
            owner_token=owner_token,
        )
    return ledger.mark_unknown(
        claim.job_id,
        claim.action_id,
        UnknownReason.AMBIGUOUS_LOOKUP.value,
        owner_token=owner_token,
    )
