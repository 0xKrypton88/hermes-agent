"""ENG-27 Slack job-thread binding contract (isolated, default-off).

Binding authority is this ledger — not Slack history or LangGraph context.
A job is bound to workspace/channel/root-thread/candidate/version *before*
any outbound effect. Rebind and cross-job/cross-binding resume fail closed.

SQLite here is disposable, explicit-path, single-process, and dev/test-only.
No live Slack API client is constructed.
"""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Protocol

from agent.durable_jobs.store import DurableJobStore


class SlackRootStatus(str, Enum):
    BOUND = "bound"
    CLAIMED = "claimed"
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


def _utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


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
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class DeliveryClaimResult:
    binding: SlackJobBinding
    won: bool


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
    def __init__(self, sqlite_path: Path) -> None:
        self.sqlite_path = Path(sqlite_path)
        self._jobs = DurableJobStore(sqlite_path=self.sqlite_path)

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
        now = _utcnow()
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
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, ?, ?)
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
        """CAS BOUND → CLAIMED. Concurrent losers must not post."""
        now = _utcnow()
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
            cur = conn.execute(
                """
                UPDATE slack_job_bindings
                   SET status = ?, updated_at = ?
                 WHERE job_id = ? AND status = ?
                """,
                (
                    SlackRootStatus.CLAIMED.value,
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
                },
                idempotency_key=f"slack_root_claimed:{job_id}",
            )
            row = conn.execute(
                "SELECT * FROM slack_job_bindings WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        assert row is not None
        return DeliveryClaimResult(binding=self._row_to_binding(row), won=True)

    def mark_delivered(self, job_id: str, message_ts: str) -> SlackJobBinding:
        return self._complete_delivery(
            job_id,
            message_ts=message_ts,
            status=SlackRootStatus.DELIVERED,
            event_type="slack_root_delivered",
        )

    def adopt_delivery(self, job_id: str, message_ts: str) -> SlackJobBinding:
        return self._complete_delivery(
            job_id,
            message_ts=message_ts,
            status=SlackRootStatus.ADOPTED,
            event_type="slack_root_adopted",
        )

    def mark_unknown(self, job_id: str, reason: str) -> SlackJobBinding:
        return self._complete_delivery(
            job_id,
            message_ts=None,
            status=SlackRootStatus.UNKNOWN,
            event_type="slack_root_unknown",
            unknown_reason=reason,
        )

    def _complete_delivery(
        self,
        job_id: str,
        *,
        message_ts: Optional[str],
        status: SlackRootStatus,
        event_type: str,
        unknown_reason: Optional[str] = None,
    ) -> SlackJobBinding:
        now = _utcnow()
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
            if binding.status is not SlackRootStatus.CLAIMED:
                return binding
            conn.execute(
                """
                UPDATE slack_job_bindings
                   SET status = ?, delivered_message_ts = ?, unknown_reason = ?,
                       updated_at = ?
                 WHERE job_id = ? AND status = ?
                """,
                (
                    status.value,
                    message_ts,
                    unknown_reason,
                    now,
                    job_id,
                    SlackRootStatus.CLAIMED.value,
                ),
            )
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

    @staticmethod
    def _row_to_binding(row: sqlite3.Row) -> SlackJobBinding:
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
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


def deliver_slack_root(
    ledger: SlackBindingLedger,
    slack_port: SlackMessagePort,
    *,
    job_id: str,
) -> SlackJobBinding:
    """Post at most one logical root. Binding + client_msg_id must already exist.

    Atomic CLAIMED CAS happens before ``post_root``. Concurrent losers do not
    post. An existing CLAIMED row after restart looks up by the stable
    ``client_msg_id`` and never blindly reposts.
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
    if binding.status is SlackRootStatus.CLAIMED:
        return _lookup_slack_root(ledger, slack_port, binding)

    claimed = ledger.claim_delivery(job_id)
    if not claimed.won:
        return claimed.binding

    binding = claimed.binding
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
        return ledger.mark_delivered(job_id, message_ts)
    if kind == "ambiguous_response":
        return ledger.mark_unknown(
            job_id, SlackUnknownReason.AMBIGUOUS_RESPONSE.value
        )
    return _lookup_slack_root(ledger, slack_port, binding)


def _lookup_slack_root(
    ledger: SlackBindingLedger,
    slack_port: SlackMessagePort,
    binding: SlackJobBinding,
) -> SlackJobBinding:
    matches = list(
        slack_port.lookup_by_client_msg_id(binding.outbound_client_msg_id)
    )
    if len(matches) == 1:
        return ledger.adopt_delivery(binding.job_id, matches[0].message_ts)
    if len(matches) == 0:
        return ledger.mark_unknown(
            binding.job_id, SlackUnknownReason.EMPTY_LOOKUP.value
        )
    return ledger.mark_unknown(
        binding.job_id, SlackUnknownReason.AMBIGUOUS_LOOKUP.value
    )
