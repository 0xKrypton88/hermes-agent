"""Profile-local durable receipt ledger for receipt-only webhook intake.

Records constrained identifiers for allowlisted Linear Issue→Go transitions.
This is intentionally receipt-only: no job creation, agent dispatch, or
provider mutation happens here. Idempotency is transactional on
``(provider, delivery_id)``.
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Sequence

from hermes_constants import get_hermes_home
from hermes_time import now as _hermes_now

logger = logging.getLogger(__name__)

PROVIDER_LINEAR = "linear"
MODE_LINEAR_ISSUE_GO = "linear_issue_go"

_lock = threading.RLock()


def default_receipts_db_path() -> Path:
    return get_hermes_home().resolve() / "webhook_receipts.db"


def payload_sha256(raw_body: bytes) -> str:
    return hashlib.sha256(raw_body).hexdigest()


def build_linear_issue_go_job_key(*, issue_id: str, state_id: str) -> str:
    return f"linear_issue_go:{issue_id}:{state_id}"


@dataclass(frozen=True)
class ReceiptRecord:
    receipt_id: str
    provider: str
    delivery_id: str
    issue_id: str
    issue_identifier: str
    state_id: str
    job_key: str
    payload_hash: str
    created_at: str


@dataclass(frozen=True)
class ReceiptInsertResult:
    status: str  # "created" | "duplicate" | "conflict"
    receipt: ReceiptRecord


@dataclass(frozen=True)
class LinearIssueGoValidation:
    ok: bool
    error: Optional[str] = None
    issue_id: Optional[str] = None
    issue_identifier: Optional[str] = None
    state_id: Optional[str] = None
    job_key: Optional[str] = None


def _as_nonempty_str(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def validate_linear_issue_go_payload(
    payload: Any,
    *,
    allowed_state_ids: Sequence[Any],
) -> LinearIssueGoValidation:
    """Conservatively validate a Linear Issue update for receipt-only intake."""
    if not isinstance(payload, dict):
        return LinearIssueGoValidation(ok=False, error="Invalid payload")

    whitelist = []
    for item in allowed_state_ids or ():
        state_id = _as_nonempty_str(item)
        if state_id is not None:
            whitelist.append(state_id)
    if not whitelist:
        return LinearIssueGoValidation(
            ok=False, error="No allowlisted state IDs configured"
        )

    event_type = payload.get("type")
    if event_type != "Issue":
        return LinearIssueGoValidation(ok=False, error="Unsupported event type")

    action = payload.get("action")
    if action != "update":
        return LinearIssueGoValidation(ok=False, error="Unsupported action")

    data = payload.get("data")
    if not isinstance(data, dict):
        return LinearIssueGoValidation(ok=False, error="Missing issue data")

    issue_id = _as_nonempty_str(data.get("id"))
    if issue_id is None:
        return LinearIssueGoValidation(ok=False, error="Missing issue id")

    issue_identifier = _as_nonempty_str(data.get("identifier"))
    if issue_identifier is None:
        return LinearIssueGoValidation(ok=False, error="Missing issue identifier")

    state_id = _as_nonempty_str(data.get("stateId"))
    if state_id is None and isinstance(data.get("state"), dict):
        state_id = _as_nonempty_str(data["state"].get("id"))
    if state_id is None:
        return LinearIssueGoValidation(ok=False, error="Missing state id")

    if state_id not in whitelist:
        return LinearIssueGoValidation(ok=False, error="State id not allowlisted")

    updated_from = payload.get("updatedFrom")
    previous_state_id = (
        _as_nonempty_str(updated_from.get("stateId"))
        if isinstance(updated_from, dict)
        else None
    )
    if previous_state_id is None or previous_state_id == state_id:
        return LinearIssueGoValidation(
            ok=False, error="Missing state transition"
        )

    return LinearIssueGoValidation(
        ok=True,
        issue_id=issue_id,
        issue_identifier=issue_identifier,
        state_id=state_id,
        job_key=build_linear_issue_go_job_key(
            issue_id=issue_id, state_id=state_id
        ),
    )


class WebhookReceiptStore:
    """SQLite-backed, profile-local receipt store with unique delivery keys."""

    def __init__(self, db_path: Optional[Path] = None):
        self._db_path = Path(db_path) if db_path is not None else default_receipts_db_path()

    @property
    def db_path(self) -> Path:
        return self._db_path

    def _connect(self) -> sqlite3.Connection:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        return sqlite3.connect(self._db_path, timeout=5)

    def _initialize_schema(self, conn: sqlite3.Connection) -> None:
        from hermes_state import apply_wal_with_fallback

        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        apply_wal_with_fallback(conn, db_label="webhook_receipts.db")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS webhook_receipts (
                 receipt_id TEXT PRIMARY KEY,
                 provider TEXT NOT NULL,
                 delivery_id TEXT NOT NULL,
                 issue_id TEXT NOT NULL,
                 issue_identifier TEXT NOT NULL,
                 state_id TEXT NOT NULL,
                 job_key TEXT NOT NULL,
                 payload_hash TEXT NOT NULL,
                 created_at TEXT NOT NULL,
                 UNIQUE(provider, delivery_id)
               )"""
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_webhook_receipts_job_created "
            "ON webhook_receipts(job_key, created_at DESC)"
        )

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with _lock:
            conn = self._connect()
            try:
                self._initialize_schema(conn)
                with conn:
                    yield conn
            finally:
                conn.close()

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> ReceiptRecord:
        return ReceiptRecord(
            receipt_id=row["receipt_id"],
            provider=row["provider"],
            delivery_id=row["delivery_id"],
            issue_id=row["issue_id"],
            issue_identifier=row["issue_identifier"],
            state_id=row["state_id"],
            job_key=row["job_key"],
            payload_hash=row["payload_hash"],
            created_at=row["created_at"],
        )

    def record_linear_issue_go(
        self,
        *,
        delivery_id: str,
        issue_id: str,
        issue_identifier: str,
        state_id: str,
        job_key: str,
        payload_hash: str,
    ) -> ReceiptInsertResult:
        """Insert a Linear Issue→Go receipt; duplicates return the existing row."""
        delivery_id = _as_nonempty_str(delivery_id) or ""
        issue_id = _as_nonempty_str(issue_id) or ""
        issue_identifier = _as_nonempty_str(issue_identifier) or ""
        state_id = _as_nonempty_str(state_id) or ""
        job_key = _as_nonempty_str(job_key) or ""
        payload_hash = _as_nonempty_str(payload_hash) or ""
        if not all(
            (delivery_id, issue_id, issue_identifier, state_id, job_key, payload_hash)
        ):
            raise ValueError("Receipt fields must be non-empty strings")

        receipt_id = uuid.uuid4().hex
        created_at = _hermes_now().isoformat()
        with self._transaction() as conn:
            try:
                conn.execute(
                    """INSERT INTO webhook_receipts (
                         receipt_id, provider, delivery_id, issue_id,
                         issue_identifier, state_id, job_key, payload_hash,
                         created_at
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        receipt_id,
                        PROVIDER_LINEAR,
                        delivery_id,
                        issue_id,
                        issue_identifier,
                        state_id,
                        job_key,
                        payload_hash,
                        created_at,
                    ),
                )
            except sqlite3.IntegrityError:
                row = conn.execute(
                    """SELECT * FROM webhook_receipts
                       WHERE provider=? AND delivery_id=?""",
                    (PROVIDER_LINEAR, delivery_id),
                ).fetchone()
                if row is None:
                    raise
                existing = self._row_to_record(row)
                if (
                    existing.issue_id != issue_id
                    or existing.issue_identifier != issue_identifier
                    or existing.state_id != state_id
                    or existing.job_key != job_key
                    or existing.payload_hash != payload_hash
                ):
                    return ReceiptInsertResult(
                        status="conflict",
                        receipt=existing,
                    )
                return ReceiptInsertResult(
                    status="duplicate",
                    receipt=existing,
                )
            row = conn.execute(
                "SELECT * FROM webhook_receipts WHERE receipt_id=?",
                (receipt_id,),
            ).fetchone()
        return ReceiptInsertResult(
            status="created",
            receipt=self._row_to_record(row),
        )

    def count(self) -> int:
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM webhook_receipts"
            ).fetchone()
        return int(row["n"]) if row is not None else 0


def parse_allowed_state_ids(route_config: Dict[str, Any]) -> list[str]:
    """Read the explicit immutable state-id whitelist from route config."""
    raw = route_config.get("allowed_state_ids")
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for item in raw:
        state_id = _as_nonempty_str(item)
        if state_id is not None:
            out.append(state_id)
    return out
