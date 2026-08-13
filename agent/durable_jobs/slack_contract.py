"""ENG-27 Slack job-thread binding contract (isolated, default-off).

Binding authority is this ledger — not Slack history or LangGraph context.
A job is bound to workspace/channel/root-thread/candidate/version *before*
any outbound effect. Rebind and cross-job/cross-binding resume fail closed.

Delivery claims persist an owner token and lease. A non-owner that sees an
unexpired CLAIMED/RECOVERING lease must poll and must not lookup, post,
adopt, increment recovery attempts, or terminalize. The live owner renews
the persisted lease while ``post_root`` is in flight; renew False/exception
is observable and does not cancel that call. Only an expired/legacy-null
lease may be taken over (CLAIMED or RECOVERING), minting a new owner token.
Recovery looks up by the stable ``client_msg_id`` and never blindly
reposts. Empty lookup stays RECOVERING until ``recovery_deadline``. A
persisted foreign token is never caller authority. The previous owner is
fenced from mark_delivered after takeover. Cancel is terminal: a
pre-existing accepted Cancel refuses claim, inflight, post_root, and
recovery lookup. Delivered/adopted bind cannot overwrite Cancel: the
success UPDATE-CAS includes an authoritative Cancel predicate in the
same SQLite transaction. SQLite cannot abort an RPC that already began
after the last Cancel SELECT, and this fence is not PostgreSQL-safe.

SQLite here is disposable, explicit-path, single-process, and dev/test-only.
No live Slack API client is constructed.
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


class SlackRootStatus(str, Enum):
    BOUND = "bound"
    CLAIMED = "claimed"
    RECOVERING = "recovering"
    DELIVERED = "delivered"
    ADOPTED = "adopted"
    UNKNOWN = "unknown"


class SlackUnknownReason(str, Enum):
    EMPTY_LOOKUP = "empty_lookup"
    AMBIGUOUS_LOOKUP = "ambiguous_lookup"
    AMBIGUOUS_RESPONSE = "ambiguous_response"


class BindingConflict(ValueError):
    """Rebind or cross-job/cross-binding resume rejected."""


class BindingRequiredError(RuntimeError):
    """An effect was attempted before the immutable Slack binding existed."""


class OriginMismatchError(ValueError):
    """Supplied provider origin does not match the durable Slack binding."""


def stable_outbound_client_msg_id(job_id: str) -> str:
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL, f"hermes.durable_jobs.slack.root:{job_id}"
        )
    )


def origin_from_slack_binding(binding: "SlackJobBinding") -> tuple[str, str, str]:
    """Authoritative provider origin: Slack platform + bound channel/root."""
    return ("slack", binding.channel_id, binding.root_thread_ts)


def resolve_provider_origin(
    binding: "SlackJobBinding",
    *,
    origin_platform: str,
    origin_chat_id: str,
    origin_root_thread_id: str,
) -> tuple[str, str, str]:
    """Derive origin from the Slack binding; reject a supplied mismatch."""
    derived = origin_from_slack_binding(binding)
    supplied = (origin_platform, origin_chat_id, origin_root_thread_id)
    if supplied != derived:
        raise OriginMismatchError(
            f"supplied origin {supplied!r} does not match Slack binding {derived!r}"
        )
    return derived


@dataclass(frozen=True)
class SlackJobBinding:
    job_id: str
    workspace_id: str
    channel_id: str
    root_thread_ts: str
    candidate_id: str
    candidate_version: str
    outbound_client_msg_id: str
    delivered_message_ts: Optional[str]
    status: SlackRootStatus
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
class DeliveryClaimResult:
    binding: SlackJobBinding
    won: bool
    owner_token: Optional[str] = None


class SlackMessagePort(Protocol):
    """Injected-only Slack post/lookup seam. No live implementation ships here."""

    def post_root(
        self,
        *,
        client_msg_id: str,
        workspace_id: str,
        channel_id: str,
        root_thread_ts: str,
        job_id: str,
    ) -> Any: ...

    def lookup_by_client_msg_id(self, client_msg_id: str) -> list[Any]: ...


class SlackBindingLedger:
    def __init__(
        self,
        sqlite_path: Path,
        now_fn: Optional[Callable[[], str]] = None,
        lease_seconds: int = DEFAULT_CLAIM_LEASE_SECONDS,
        recovery_max_attempts: int = DEFAULT_RECOVERY_MAX_ATTEMPTS,
        recovery_window_seconds: int = DEFAULT_RECOVERY_WINDOW_SECONDS,
    ) -> None:
        self.sqlite_path = Path(sqlite_path)
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

    def bind(
        self,
        *,
        job_id: str,
        workspace_id: str,
        channel_id: str,
        root_thread_ts: str,
        candidate_id: str,
        candidate_version: str,
    ) -> SlackJobBinding:
        job = self._jobs.get_job(job_id)
        if job is None:
            raise KeyError(f"unknown job_id: {job_id}")
        snapshot = (
            workspace_id,
            channel_id,
            root_thread_ts,
            candidate_id,
            candidate_version,
        )
        now = self._now()
        client_msg_id = stable_outbound_client_msg_id(job_id)
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if existing is not None:
                binding = self._row_to_binding(existing)
                if (
                    binding.workspace_id,
                    binding.channel_id,
                    binding.root_thread_ts,
                    binding.candidate_id,
                    binding.candidate_version,
                ) != snapshot:
                    raise BindingConflict(
                        f"job {job_id} already bound; rebind rejected"
                    )
                return binding
            occupant = conn.execute(
                """
                SELECT * FROM slack_job_bindings
                 WHERE workspace_id = ? AND channel_id = ? AND root_thread_ts = ?
                """,
                (workspace_id, channel_id, root_thread_ts),
            ).fetchone()
            if occupant is not None and occupant["job_id"] != job_id:
                raise BindingConflict(
                    f"root {workspace_id}/{channel_id}/{root_thread_ts} "
                    f"already bound to {occupant['job_id']}"
                )
            try:
                conn.execute(
                    """
                    INSERT INTO slack_job_bindings(
                        job_id, workspace_id, channel_id, root_thread_ts,
                        candidate_id, candidate_version, outbound_client_msg_id,
                        delivered_message_ts, status, unknown_reason,
                        claim_owner_token, claim_leased_at, claim_expires_at,
                        claim_generation, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL,
                              NULL, NULL, NULL, 0, ?, ?)
                    """,
                    (
                        job_id,
                        workspace_id,
                        channel_id,
                        root_thread_ts,
                        candidate_id,
                        candidate_version,
                        client_msg_id,
                        SlackRootStatus.BOUND.value,
                        now,
                        now,
                    ),
                )
                DurableJobStore._append_event(
                    conn,
                    job_id=job_id,
                    event_type="slack_binding_created",
                    payload={
                        "workspace_id": workspace_id,
                        "channel_id": channel_id,
                        "root_thread_ts": root_thread_ts,
                        "candidate_id": candidate_id,
                        "candidate_version": candidate_version,
                        "outbound_client_msg_id": client_msg_id,
                    },
                    idempotency_key=f"slack_binding_created:{job_id}",
                )
            except sqlite3.IntegrityError:
                adopted = conn.execute(
                    "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                    (job_id,),
                ).fetchone()
                if adopted is None:
                    occupant = conn.execute(
                        """
                        SELECT * FROM slack_job_bindings
                         WHERE workspace_id = ? AND channel_id = ?
                           AND root_thread_ts = ?
                        """,
                        (workspace_id, channel_id, root_thread_ts),
                    ).fetchone()
                    if occupant is not None:
                        raise BindingConflict(
                            f"root already bound to {occupant['job_id']}"
                        )
                    raise
                binding = self._row_to_binding(adopted)
                if (
                    binding.workspace_id,
                    binding.channel_id,
                    binding.root_thread_ts,
                    binding.candidate_id,
                    binding.candidate_version,
                ) != snapshot:
                    raise BindingConflict(
                        f"job {job_id} already bound; rebind rejected"
                    )
                return binding
            row = conn.execute(
                "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        assert row is not None
        return self._row_to_binding(row)

    def resume(
        self,
        *,
        job_id: str,
        workspace_id: str,
        channel_id: str,
        root_thread_ts: str,
        candidate_id: str,
        candidate_version: str,
    ) -> SlackJobBinding:
        binding = self.get_binding(job_id)
        if binding is None:
            raise BindingRequiredError(f"no Slack binding for {job_id}")
        if (
            binding.workspace_id != workspace_id
            or binding.channel_id != channel_id
            or binding.root_thread_ts != root_thread_ts
            or binding.candidate_id != candidate_id
            or binding.candidate_version != candidate_version
        ):
            raise BindingConflict(
                f"cross-binding resume rejected for {job_id}"
            )
        occupant = self.get_by_root(workspace_id, channel_id, root_thread_ts)
        if occupant is not None and occupant.job_id != job_id:
            raise BindingConflict(
                f"cross-job resume rejected: root owned by {occupant.job_id}"
            )
        return binding

    def get_binding(self, job_id: str) -> Optional[SlackJobBinding]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        return self._row_to_binding(row) if row else None

    def get_by_root(
        self, workspace_id: str, channel_id: str, root_thread_ts: str
    ) -> Optional[SlackJobBinding]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM slack_job_bindings
                 WHERE workspace_id = ? AND channel_id = ? AND root_thread_ts = ?
                """,
                (workspace_id, channel_id, root_thread_ts),
            ).fetchone()
        return self._row_to_binding(row) if row else None

    def count_bindings(self) -> int:
        with self._connect() as conn:
            (count,) = conn.execute(
                "SELECT COUNT(*) FROM slack_job_bindings"
            ).fetchone()
        return int(count)

    def claim_delivery(self, job_id: str) -> DeliveryClaimResult:
        """CAS BOUND → CLAIMED with owner token + lease. Concurrent losers must not post."""
        now = self._now()
        owner_token = uuid.uuid4().hex
        expires_at = add_seconds_iso(now, self._lease_seconds)
        peeked = self.get_binding(job_id)
        if peeked is None:
            raise BindingRequiredError(f"no Slack binding for {job_id}")
        if peeked.status is not SlackRootStatus.BOUND:
            return DeliveryClaimResult(binding=peeked, won=False)
        from agent.durable_jobs.eng29 import (
            SLACK_POST_ROOT_TARGET_ACTION,
            raise_unless_adapter_go,
        )

        raise_unless_adapter_go(
            self.sqlite_path,
            job_id=job_id,
            target_action=SLACK_POST_ROOT_TARGET_ACTION,
            candidate_id=peeked.candidate_id,
            candidate_version=peeked.candidate_version,
            now_iso=now,
            action="slack delivery claim",
        )
        with self._connect() as conn:
            current = conn.execute(
                "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if current is None:
                raise BindingRequiredError(f"no Slack binding for {job_id}")
            binding = self._row_to_binding(current)
            if binding.status is not SlackRootStatus.BOUND:
                return DeliveryClaimResult(binding=binding, won=False)
            from agent.durable_jobs.decisions import (
                JobCanceledError,
                job_is_canceled_on_conn,
            )

            if job_is_canceled_on_conn(conn, job_id):
                raise JobCanceledError(
                    f"job {job_id} is canceled; refusing Slack delivery claim"
                )
            cur = conn.execute(
                """
                UPDATE slack_job_bindings
                   SET status = ?, claim_owner_token = ?, claim_leased_at = ?,
                       claim_expires_at = ?,
                       claim_generation = claim_generation + 1,
                       updated_at = ?
                 WHERE job_id = ? AND status = ?
                """,
                (
                    SlackRootStatus.CLAIMED.value,
                    owner_token,
                    now,
                    expires_at,
                    now,
                    job_id,
                    SlackRootStatus.BOUND.value,
                ),
            )
            if cur.rowcount != 1:
                row = conn.execute(
                    "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                    (job_id,),
                ).fetchone()
                assert row is not None
                return DeliveryClaimResult(
                    binding=self._row_to_binding(row), won=False
                )
            DurableJobStore._append_event(
                conn,
                job_id=job_id,
                event_type="slack_root_claimed",
                payload={
                    "outbound_client_msg_id": binding.outbound_client_msg_id,
                    "status": SlackRootStatus.CLAIMED.value,
                    "claim_generation": binding.claim_generation + 1,
                },
                idempotency_key=f"slack_root_claimed:{job_id}",
            )
            row = conn.execute(
                "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        assert row is not None
        return DeliveryClaimResult(
            binding=self._row_to_binding(row), won=True, owner_token=owner_token
        )

    def takeover_stale_delivery(self, job_id: str) -> DeliveryClaimResult:
        """Atomically take an expired/legacy CLAIMED or RECOVERING delivery.

        Mints a new owner token. Unexpired → poll. RECOVERING keeps its
        recovery window; the new owner starts attempt bookkeeping at 0.
        """
        now = self._now()
        owner_token = uuid.uuid4().hex
        expires_at = add_seconds_iso(now, self._lease_seconds)
        with self._connect() as conn:
            current = conn.execute(
                "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if current is None:
                raise BindingRequiredError(f"no Slack binding for {job_id}")
            binding = self._row_to_binding(current)
            if binding.status not in (
                SlackRootStatus.CLAIMED,
                SlackRootStatus.RECOVERING,
            ):
                return DeliveryClaimResult(binding=binding, won=False)
            from agent.durable_jobs.decisions import job_is_canceled_on_conn

            if job_is_canceled_on_conn(conn, job_id):
                return DeliveryClaimResult(binding=binding, won=False)
            if not claim_is_expired(binding.claim_expires_at, now):
                return DeliveryClaimResult(binding=binding, won=False)
            from agent.durable_jobs.eng29 import (
                SLACK_POST_ROOT_TARGET_ACTION,
                raise_unless_adapter_go,
            )

            raise_unless_adapter_go(
                self.sqlite_path,
                job_id=job_id,
                target_action=SLACK_POST_ROOT_TARGET_ACTION,
                candidate_id=binding.candidate_id,
                candidate_version=binding.candidate_version,
                now_iso=now,
                action="slack stale takeover",
            )
            cur = conn.execute(
                """
                UPDATE slack_job_bindings
                   SET claim_owner_token = ?, claim_leased_at = ?,
                       claim_expires_at = ?,
                       claim_generation = claim_generation + 1,
                       recovery_attempt_count = CASE
                           WHEN status = ? THEN 0
                           ELSE recovery_attempt_count
                       END,
                       updated_at = ?
                 WHERE job_id = ? AND status IN (?, ?)
                   AND claim_generation = ?
                   AND (claim_expires_at IS NULL OR claim_expires_at <= ?)
                """,
                (
                    owner_token,
                    now,
                    expires_at,
                    SlackRootStatus.RECOVERING.value,
                    now,
                    job_id,
                    SlackRootStatus.CLAIMED.value,
                    SlackRootStatus.RECOVERING.value,
                    binding.claim_generation,
                    now,
                ),
            )
            if cur.rowcount != 1:
                row = conn.execute(
                    "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                    (job_id,),
                ).fetchone()
                assert row is not None
                return DeliveryClaimResult(
                    binding=self._row_to_binding(row), won=False
                )
            DurableJobStore._append_event(
                conn,
                job_id=job_id,
                event_type="slack_root_claim_taken",
                payload={
                    "outbound_client_msg_id": binding.outbound_client_msg_id,
                    "claim_generation": binding.claim_generation + 1,
                },
                idempotency_key=(
                    f"slack_root_claim_taken:{job_id}:{binding.claim_generation + 1}"
                ),
            )
            row = conn.execute(
                "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        assert row is not None
        return DeliveryClaimResult(
            binding=self._row_to_binding(row), won=True, owner_token=owner_token
        )

    def renew_delivery(self, job_id: str, *, owner_token: str) -> bool:
        """Owner-fenced lease renewal. Requires an unexpired matching lease.

        Late renewal after ``claim_expires_at`` (or legacy NULL expiry) returns
        False even if nobody has taken over yet.
        """
        now = self._now()
        expires_at = add_seconds_iso(now, self._lease_seconds)
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE slack_job_bindings
                   SET claim_leased_at = ?, claim_expires_at = ?, updated_at = ?
                 WHERE job_id = ? AND claim_owner_token = ?
                   AND status IN (?, ?)
                   AND claim_expires_at IS NOT NULL
                   AND claim_expires_at > ?
                """,
                (
                    now,
                    expires_at,
                    now,
                    job_id,
                    owner_token,
                    SlackRootStatus.CLAIMED.value,
                    SlackRootStatus.RECOVERING.value,
                    now,
                ),
            )
            return cur.rowcount == 1

    def begin_inflight(self, job_id: str, *, owner_token: str) -> bool:
        """Persist that this owner has an outstanding post_root."""
        now = self._now()
        until = add_seconds_iso(now, self._lease_seconds)
        with self._connect() as conn:
            from agent.durable_jobs.decisions import job_is_canceled_on_conn

            if job_is_canceled_on_conn(conn, job_id):
                return False
            cur = conn.execute(
                """
                UPDATE slack_job_bindings
                   SET effect_inflight_token = ?, effect_inflight_until = ?,
                       updated_at = ?
                 WHERE job_id = ? AND claim_owner_token = ?
                   AND status IN (?, ?)
                """,
                (
                    owner_token,
                    until,
                    now,
                    job_id,
                    owner_token,
                    SlackRootStatus.CLAIMED.value,
                    SlackRootStatus.RECOVERING.value,
                ),
            )
            return cur.rowcount == 1

    def renew_inflight(self, job_id: str, *, owner_token: str) -> bool:
        """Renew the in-flight witness. Late renew is allowed while token matches."""
        now = self._now()
        until = add_seconds_iso(now, self._lease_seconds)
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE slack_job_bindings
                   SET effect_inflight_until = ?, updated_at = ?
                 WHERE job_id = ? AND effect_inflight_token = ?
                   AND status IN (?, ?)
                """,
                (
                    until,
                    now,
                    job_id,
                    owner_token,
                    SlackRootStatus.CLAIMED.value,
                    SlackRootStatus.RECOVERING.value,
                ),
            )
            return cur.rowcount == 1

    def clear_inflight(self, job_id: str, *, owner_token: str) -> None:
        now = self._now()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE slack_job_bindings
                   SET effect_inflight_token = NULL, effect_inflight_until = NULL,
                       updated_at = ?
                 WHERE job_id = ? AND effect_inflight_token = ?
                """,
                (now, job_id, owner_token),
            )

    def _job_is_canceled(self, job_id: str) -> bool:
        from agent.durable_jobs.decisions import DecisionLedger

        return DecisionLedger(sqlite_path=self.sqlite_path).is_canceled(job_id)

    def mark_delivered(
        self, job_id: str, message_ts: str, *, owner_token: str
    ) -> SlackJobBinding:
        return self._complete_delivery(
            job_id,
            message_ts=message_ts,
            status=SlackRootStatus.DELIVERED,
            event_type="slack_root_delivered",
            owner_token=owner_token,
        )

    def adopt_delivery(
        self, job_id: str, message_ts: str, *, owner_token: str
    ) -> SlackJobBinding:
        return self._complete_delivery(
            job_id,
            message_ts=message_ts,
            status=SlackRootStatus.ADOPTED,
            event_type="slack_root_adopted",
            owner_token=owner_token,
        )

    def mark_unknown(
        self, job_id: str, reason: str, *, owner_token: str
    ) -> SlackJobBinding:
        return self._complete_delivery(
            job_id,
            message_ts=None,
            status=SlackRootStatus.UNKNOWN,
            event_type="slack_root_unknown",
            unknown_reason=reason,
            owner_token=owner_token,
        )

    def note_empty_lookup(
        self, job_id: str, *, owner_token: str
    ) -> SlackJobBinding:
        """Persist bounded RECOVERING on empty lookup; UNKNOWN only at the bound."""
        now = self._now()
        deadline = add_seconds_iso(now, self._recovery_window_seconds)
        expires_at = add_seconds_iso(now, self._lease_seconds)
        with self._connect() as conn:
            current = conn.execute(
                "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if current is None:
                raise BindingRequiredError(f"no Slack binding for {job_id}")
            binding = self._row_to_binding(current)
            if binding.status not in (
                SlackRootStatus.CLAIMED,
                SlackRootStatus.RECOVERING,
            ):
                return binding
            cur = conn.execute(
                """
                UPDATE slack_job_bindings
                   SET status = ?,
                       recovery_attempt_count = recovery_attempt_count + 1,
                       recovery_started_at = COALESCE(recovery_started_at, ?),
                       recovery_deadline = COALESCE(recovery_deadline, ?),
                       claim_leased_at = ?,
                       claim_expires_at = ?,
                       updated_at = ?
                 WHERE job_id = ? AND status IN (?, ?)
                   AND claim_owner_token = ?
                """,
                (
                    SlackRootStatus.RECOVERING.value,
                    now,
                    deadline,
                    now,
                    expires_at,
                    now,
                    job_id,
                    SlackRootStatus.CLAIMED.value,
                    SlackRootStatus.RECOVERING.value,
                    owner_token,
                ),
            )
            if cur.rowcount != 1:
                row = conn.execute(
                    "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                    (job_id,),
                ).fetchone()
                assert row is not None
                return self._row_to_binding(row)
            DurableJobStore._append_event(
                conn,
                job_id=job_id,
                event_type="slack_root_recovering",
                payload={
                    "outbound_client_msg_id": binding.outbound_client_msg_id,
                    "claim_generation": binding.claim_generation,
                },
                idempotency_key=(
                    f"slack_root_recovering:{job_id}:{binding.claim_generation}"
                ),
            )
            row = conn.execute(
                "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        assert row is not None
        updated = self._row_to_binding(row)
        if recovery_bound_exceeded(
            attempt_count=updated.recovery_attempt_count,
            deadline=updated.recovery_deadline,
            now_iso=now,
            max_attempts=self._recovery_max_attempts,
        ):
            return self.mark_unknown(
                job_id,
                SlackUnknownReason.EMPTY_LOOKUP.value,
                owner_token=owner_token,
            )
        return updated

    def _complete_delivery(
        self,
        job_id: str,
        *,
        message_ts: Optional[str],
        status: SlackRootStatus,
        event_type: str,
        owner_token: str,
        unknown_reason: Optional[str] = None,
    ) -> SlackJobBinding:
        now = self._now()
        # Fast path only. The success UPDATE-CAS Cancel predicate below is
        # the fence; a separate-connection pre-check is not sufficient.
        if status in (
            SlackRootStatus.DELIVERED,
            SlackRootStatus.ADOPTED,
        ) and self._job_is_canceled(job_id):
            blocked = self.get_binding(job_id)
            if blocked is None:
                raise BindingRequiredError(f"no Slack binding for {job_id}")
            return blocked
        with self._connect() as conn:
            current = conn.execute(
                "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if current is None:
                raise BindingRequiredError(f"no Slack binding for {job_id}")
            binding = self._row_to_binding(current)
            if binding.status in (
                SlackRootStatus.DELIVERED,
                SlackRootStatus.ADOPTED,
                SlackRootStatus.UNKNOWN,
            ):
                return binding
            if binding.status not in (
                SlackRootStatus.CLAIMED,
                SlackRootStatus.RECOVERING,
            ):
                return binding
            inflight_until = None
            inflight_token = None
            if "effect_inflight_until" in current.keys():
                inflight_until = current["effect_inflight_until"]
            if "effect_inflight_token" in current.keys():
                inflight_token = current["effect_inflight_token"]
            if (
                status is SlackRootStatus.UNKNOWN
                and inflight_token != owner_token
                and inflight_witness_blocks_unknown(
                    inflight_until=inflight_until, now_iso=now
                )
            ):
                return binding
            unknown_inflight_sql = ""
            unknown_inflight_args: tuple = ()
            if status is SlackRootStatus.UNKNOWN:
                unknown_inflight_sql = (
                    " AND (effect_inflight_until IS NULL"
                    " OR effect_inflight_until <= ?"
                    " OR effect_inflight_token = ?)"
                )
                unknown_inflight_args = (now, owner_token)
            cancel_sql = ""
            if status in (SlackRootStatus.DELIVERED, SlackRootStatus.ADOPTED):
                from agent.durable_jobs.decisions import (
                    sql_reject_authoritative_cancel,
                )

                cancel_sql = sql_reject_authoritative_cancel("slack_job_bindings")
            cur = conn.execute(
                f"""
                UPDATE slack_job_bindings
                   SET status = ?, delivered_message_ts = ?, unknown_reason = ?,
                       effect_inflight_token = NULL, effect_inflight_until = NULL,
                       updated_at = ?
                 WHERE job_id = ? AND status IN (?, ?) AND claim_owner_token = ?
                   {unknown_inflight_sql}
                   {cancel_sql}
                """,
                (
                    status.value,
                    message_ts,
                    unknown_reason,
                    now,
                    job_id,
                    SlackRootStatus.CLAIMED.value,
                    SlackRootStatus.RECOVERING.value,
                    owner_token,
                    *unknown_inflight_args,
                ),
            )
            if cur.rowcount != 1:
                row = conn.execute(
                    "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                    (job_id,),
                ).fetchone()
                assert row is not None
                return self._row_to_binding(row)
            DurableJobStore._append_event(
                conn,
                job_id=job_id,
                event_type=event_type,
                payload={
                    "status": status.value,
                    "delivered_message_ts": message_ts,
                    "unknown_reason": unknown_reason,
                    "outbound_client_msg_id": binding.outbound_client_msg_id,
                },
                idempotency_key=f"{event_type}:{job_id}",
            )
            row = conn.execute(
                "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        assert row is not None
        return self._row_to_binding(row)

    def bind_observed_delivery(
        self,
        job_id: str,
        message_ts: str,
        *,
        status: SlackRootStatus,
    ) -> SlackJobBinding:
        """Bind a uniquely observed Slack message without a caller owner token.

        In-flight ``post_root`` cannot be canceled. If the original owner is
        fenced after heartbeat loss, this CAS still completes while the row is
        CLAIMED/RECOVERING. Already-terminal rows are unchanged.
        """
        if status not in (SlackRootStatus.DELIVERED, SlackRootStatus.ADOPTED):
            raise ValueError(
                f"bind_observed_delivery status must be delivered/adopted, got {status}"
            )
        event_type = (
            "slack_root_delivered"
            if status is SlackRootStatus.DELIVERED
            else "slack_root_adopted"
        )
        now = self._now()
        # Fast path only. The success UPDATE-CAS Cancel predicate below is
        # the fence; a separate-connection pre-check is not sufficient.
        if self._job_is_canceled(job_id):
            blocked = self.get_binding(job_id)
            if blocked is None:
                raise BindingRequiredError(f"no Slack binding for {job_id}")
            return blocked
        with self._connect() as conn:
            current = conn.execute(
                "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if current is None:
                raise BindingRequiredError(f"no Slack binding for {job_id}")
            binding = self._row_to_binding(current)
            if binding.status not in (
                SlackRootStatus.CLAIMED,
                SlackRootStatus.RECOVERING,
            ):
                return binding
            from agent.durable_jobs.decisions import sql_reject_authoritative_cancel

            cur = conn.execute(
                f"""
                UPDATE slack_job_bindings
                   SET status = ?, delivered_message_ts = ?, unknown_reason = NULL,
                       effect_inflight_token = NULL, effect_inflight_until = NULL,
                       updated_at = ?
                 WHERE job_id = ? AND status IN (?, ?)
                   AND (delivered_message_ts IS NULL OR delivered_message_ts = ?)
                   {sql_reject_authoritative_cancel("slack_job_bindings")}
                """,
                (
                    status.value,
                    message_ts,
                    now,
                    job_id,
                    SlackRootStatus.CLAIMED.value,
                    SlackRootStatus.RECOVERING.value,
                    message_ts,
                ),
            )
            if cur.rowcount != 1:
                row = conn.execute(
                    "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                    (job_id,),
                ).fetchone()
                assert row is not None
                return self._row_to_binding(row)
            DurableJobStore._append_event(
                conn,
                job_id=job_id,
                event_type=event_type,
                payload={
                    "status": status.value,
                    "delivered_message_ts": message_ts,
                    "unknown_reason": None,
                    "outbound_client_msg_id": binding.outbound_client_msg_id,
                },
                idempotency_key=f"{event_type}:{job_id}",
            )
            row = conn.execute(
                "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        assert row is not None
        return self._row_to_binding(row)

    @staticmethod
    def _row_to_binding(row: sqlite3.Row) -> SlackJobBinding:
        keys = set(row.keys())
        generation = 0
        if "claim_generation" in keys:
            generation = int(row["claim_generation"] or 0)
        return SlackJobBinding(
            job_id=row["job_id"],
            workspace_id=row["workspace_id"],
            channel_id=row["channel_id"],
            root_thread_ts=row["root_thread_ts"],
            candidate_id=row["candidate_id"],
            candidate_version=row["candidate_version"],
            outbound_client_msg_id=row["outbound_client_msg_id"],
            delivered_message_ts=row["delivered_message_ts"],
            status=SlackRootStatus(row["status"]),
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


def deliver_slack_root(
    ledger: SlackBindingLedger,
    slack_port: SlackMessagePort,
    *,
    job_id: str,
    owner_token: Optional[str] = None,
) -> SlackJobBinding:
    """Post at most one logical root. Binding + client_msg_id must already exist.

    Atomic CLAIMED CAS happens before ``post_root``. A live foreign
    CLAIMED/RECOVERING is polled — concurrent losers do not post, lookup,
    increment attempts, or terminalize. The winner renews the persisted
    lease while ``post_root`` is in flight; renew False/exception is
    observable and does not cancel that call. An expired CLAIMED/RECOVERING
    may be taken over (new token); recovery looks up by the stable
    ``client_msg_id`` and never blindly reposts. Empty lookup stays
    RECOVERING until ``recovery_deadline``. A pre-existing accepted
    Cancel refuses claim/inflight/post_root/lookup; Cancel after RPC
    begin stays terminal and cannot be overwritten by delivered/adopted
    bind. SQLite cannot abort an outstanding adapter RPC.
    """
    binding = ledger.get_binding(job_id)
    if binding is None:
        raise BindingRequiredError(f"no Slack binding for {job_id}")
    if binding.status in (
        SlackRootStatus.DELIVERED,
        SlackRootStatus.ADOPTED,
        SlackRootStatus.UNKNOWN,
    ):
        return binding
    if binding.status is SlackRootStatus.RECOVERING:
        return _recover_or_poll_slack(
            ledger, slack_port, binding, caller_token=owner_token
        )
    if binding.status is SlackRootStatus.CLAIMED:
        if ledger._job_is_canceled(job_id):
            return binding
        taken = ledger.takeover_stale_delivery(job_id)
        if not taken.won:
            return taken.binding
        return _lookup_slack_root(
            ledger, slack_port, taken.binding, owner_token=taken.owner_token
        )

    claimed = ledger.claim_delivery(job_id)
    if not claimed.won:
        return claimed.binding

    binding = claimed.binding
    won_token = claimed.owner_token
    if not won_token:
        return binding

    def _renew_owner() -> bool:
        inflight_ok = ledger.renew_inflight(job_id, owner_token=won_token)
        claim_ok = ledger.renew_delivery(job_id, owner_token=won_token)
        return bool(inflight_ok or claim_ok)

    from agent.durable_jobs.decisions import raise_if_job_canceled

    if not ledger.begin_inflight(job_id, owner_token=won_token):
        raise_if_job_canceled(
            ledger.sqlite_path, job_id, action="slack post_root"
        )
        current = ledger.get_binding(job_id)
        return current if current is not None else binding

    try:
        # Latest committed-Cancel SELECT before the adapter call. If Cancel
        # commits after this check, post_root may still execute; adapters
        # cannot abort an outstanding RPC. Bind stays fail-closed.
        raise_if_job_canceled(
            ledger.sqlite_path, job_id, action="slack post_root"
        )
        from agent.durable_jobs.eng29 import (
            SLACK_POST_ROOT_TARGET_ACTION,
            raise_unless_adapter_go,
        )

        raise_unless_adapter_go(
            ledger.sqlite_path,
            job_id=job_id,
            target_action=SLACK_POST_ROOT_TARGET_ACTION,
            candidate_id=binding.candidate_id,
            candidate_version=binding.candidate_version,
            now_iso=ledger._now(),
            action="slack post_root",
        )
        with owner_lease_heartbeat(
            renew_fn=_renew_owner,
            now_fn=ledger._now_fn,
            lease_seconds=ledger._lease_seconds,
        ):
            result = slack_port.post_root(
                client_msg_id=binding.outbound_client_msg_id,
                workspace_id=binding.workspace_id,
                channel_id=binding.channel_id,
                root_thread_ts=binding.root_thread_ts,
                job_id=job_id,
            )
        kind = getattr(result, "kind", None)
        message_ts = getattr(result, "message_ts", None)
        if kind == "accepted" and message_ts:
            return _finish_observed_slack_delivery(
                ledger,
                job_id,
                message_ts,
                owner_token=won_token,
                status=SlackRootStatus.DELIVERED,
            )
        if kind == "ambiguous_response":
            unknown = ledger.mark_unknown(
                job_id,
                SlackUnknownReason.AMBIGUOUS_RESPONSE.value,
                owner_token=won_token,
            )
            if unknown.status is SlackRootStatus.UNKNOWN:
                return unknown
            current = ledger.get_binding(job_id)
            return current if current is not None else unknown
        return _lookup_slack_root(
            ledger, slack_port, binding, owner_token=won_token
        )
    finally:
        ledger.clear_inflight(job_id, owner_token=won_token)


def _recover_or_poll_slack(
    ledger: SlackBindingLedger,
    slack_port: SlackMessagePort,
    binding: SlackJobBinding,
    *,
    caller_token: Optional[str],
) -> SlackJobBinding:
    if ledger._job_is_canceled(binding.job_id):
        current = ledger.get_binding(binding.job_id)
        return current if current is not None else binding
    if caller_holds_live_lease(
        caller_token=caller_token,
        persisted_token=binding.claim_owner_token,
        expires_at=binding.claim_expires_at,
        now_iso=ledger._now(),
        status=binding.status.value,
        live_statuses=(SlackRootStatus.RECOVERING.value,),
    ):
        assert caller_token is not None
        return _lookup_slack_root(
            ledger, slack_port, binding, owner_token=caller_token
        )
    taken = ledger.takeover_stale_delivery(binding.job_id)
    if not taken.won:
        return taken.binding
    return _lookup_slack_root(
        ledger, slack_port, taken.binding, owner_token=taken.owner_token
    )


def _finish_observed_slack_delivery(
    ledger: SlackBindingLedger,
    job_id: str,
    message_ts: str,
    *,
    owner_token: str,
    status: SlackRootStatus,
) -> SlackJobBinding:
    if status is SlackRootStatus.DELIVERED:
        completed = ledger.mark_delivered(
            job_id, message_ts, owner_token=owner_token
        )
    else:
        completed = ledger.adopt_delivery(
            job_id, message_ts, owner_token=owner_token
        )
    if (
        completed.status in (SlackRootStatus.DELIVERED, SlackRootStatus.ADOPTED)
        and completed.delivered_message_ts == message_ts
    ):
        return completed
    return ledger.bind_observed_delivery(job_id, message_ts, status=status)


def _lookup_slack_root(
    ledger: SlackBindingLedger,
    slack_port: SlackMessagePort,
    binding: SlackJobBinding,
    *,
    owner_token: Optional[str],
) -> SlackJobBinding:
    if not owner_token:
        return binding
    if ledger._job_is_canceled(binding.job_id):
        current = ledger.get_binding(binding.job_id)
        return current if current is not None else binding
    from agent.durable_jobs.eng29 import (
        SLACK_POST_ROOT_TARGET_ACTION,
        raise_unless_adapter_go,
    )

    raise_unless_adapter_go(
        ledger.sqlite_path,
        job_id=binding.job_id,
        target_action=SLACK_POST_ROOT_TARGET_ACTION,
        candidate_id=binding.candidate_id,
        candidate_version=binding.candidate_version,
        now_iso=ledger._now(),
        action="slack lookup",
    )
    matches = list(
        slack_port.lookup_by_client_msg_id(binding.outbound_client_msg_id)
    )
    if len(matches) == 1:
        return _finish_observed_slack_delivery(
            ledger,
            binding.job_id,
            matches[0].message_ts,
            owner_token=owner_token,
            status=SlackRootStatus.ADOPTED,
        )
    if len(matches) == 0:
        return ledger.note_empty_lookup(
            binding.job_id, owner_token=owner_token
        )
    unknown = ledger.mark_unknown(
        binding.job_id,
        SlackUnknownReason.AMBIGUOUS_LOOKUP.value,
        owner_token=owner_token,
    )
    if unknown.status is SlackRootStatus.UNKNOWN:
        return unknown
    current = ledger.get_binding(binding.job_id)
    return current if current is not None else unknown
