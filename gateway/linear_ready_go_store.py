"""Profile-local durable storage for Ready reviews and non-dispatched LaunchIntents.

SQLite + WAL (with DELETE fallback) + FULL synchronous + process RLock, matching
``gateway.webhook_receipts.WebhookReceiptStore``.

This store never dispatches work, never mutates Linear, and never sets
``dispatched=True``. Integrity constraints keep Ready provenance and Go intents
idempotent under duplicate delivery.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional, Union

from hermes_constants import get_hermes_home
from hermes_time import now as _hermes_now

_lock = threading.RLock()

DECISION_READY_FOR_GO = "READY_FOR_GO"
DECISION_BLOCKED = "BLOCKED"


def default_ready_go_db_path() -> Path:
    return get_hermes_home().resolve() / "linear_ready_go.db"


@dataclass(frozen=True)
class ReadyReviewRecord:
    review_id: str
    issue_id: str
    issue_identifier: str
    review_key: str
    source_digest: str
    decision: str
    starts_agent_work: bool
    frozen_source_json: str
    created_at: str


@dataclass(frozen=True)
class LaunchIntentRecord:
    intent_id: str
    issue_id: str
    issue_identifier: str
    review_key: str
    source_digest: str
    go_event_key: str
    idempotency_key: str
    dispatched: bool
    created_at: str


@dataclass(frozen=True)
class ReadyReviewInsertResult:
    status: str  # "created" | "duplicate"
    record: ReadyReviewRecord


@dataclass(frozen=True)
class LaunchIntentInsertResult:
    status: str  # "created" | "duplicate"
    record: LaunchIntentRecord
    reason: str = ""


def _as_nonempty_str(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


class LinearReadyGoStore:
    """Durable Ready-review + LaunchIntent ledger (profile-local)."""

    def __init__(self, db_path: Optional[Path] = None):
        self._db_path = (
            Path(db_path) if db_path is not None else default_ready_go_db_path()
        )

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
        apply_wal_with_fallback(conn, db_label="linear_ready_go.db")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS ready_reviews (
                 review_id TEXT PRIMARY KEY,
                 issue_id TEXT NOT NULL,
                 issue_identifier TEXT NOT NULL,
                 review_key TEXT NOT NULL,
                 source_digest TEXT NOT NULL,
                 decision TEXT NOT NULL,
                 starts_agent_work INTEGER NOT NULL
                     CHECK (starts_agent_work = 0),
                 frozen_source_json TEXT NOT NULL,
                 created_at TEXT NOT NULL,
                 UNIQUE(review_key),
                 UNIQUE(issue_id, source_digest),
                 CHECK (decision IN ('READY_FOR_GO', 'BLOCKED')),
                 CHECK (length(source_digest) = 64),
                 CHECK (source_digest NOT GLOB '*[^0-9a-f]*')
               )"""
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ready_reviews_issue_created "
            "ON ready_reviews(issue_id, created_at DESC)"
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS launch_intents (
                 intent_id TEXT PRIMARY KEY,
                 issue_id TEXT NOT NULL,
                 issue_identifier TEXT NOT NULL,
                 review_key TEXT NOT NULL,
                 source_digest TEXT NOT NULL,
                 go_event_key TEXT NOT NULL,
                 idempotency_key TEXT NOT NULL,
                 dispatched INTEGER NOT NULL CHECK (dispatched = 0),
                 created_at TEXT NOT NULL,
                 UNIQUE(go_event_key),
                 UNIQUE(idempotency_key),
                 CHECK (length(source_digest) = 64),
                 CHECK (source_digest NOT GLOB '*[^0-9a-f]*'),
                 FOREIGN KEY (review_key)
                     REFERENCES ready_reviews(review_key)
               )"""
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_launch_intents_issue_created "
            "ON launch_intents(issue_id, created_at DESC)"
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
    def _row_to_ready(row: sqlite3.Row) -> ReadyReviewRecord:
        return ReadyReviewRecord(
            review_id=row["review_id"],
            issue_id=row["issue_id"],
            issue_identifier=row["issue_identifier"],
            review_key=row["review_key"],
            source_digest=row["source_digest"],
            decision=row["decision"],
            starts_agent_work=bool(row["starts_agent_work"]),
            frozen_source_json=row["frozen_source_json"],
            created_at=row["created_at"],
        )

    @staticmethod
    def _row_to_intent(row: sqlite3.Row) -> LaunchIntentRecord:
        return LaunchIntentRecord(
            intent_id=row["intent_id"],
            issue_id=row["issue_id"],
            issue_identifier=row["issue_identifier"],
            review_key=row["review_key"],
            source_digest=row["source_digest"],
            go_event_key=row["go_event_key"],
            idempotency_key=row["idempotency_key"],
            dispatched=bool(row["dispatched"]),
            created_at=row["created_at"],
        )

    def record_ready_review(
        self,
        *,
        issue_id: str,
        issue_identifier: str,
        review_key: str,
        source_digest: str,
        decision: str,
        frozen_source: Union[Mapping[str, Any], Any],
        starts_agent_work: bool = False,
    ) -> ReadyReviewInsertResult:
        """Insert a Ready review; duplicate ``review_key`` returns existing row."""
        issue_id = _as_nonempty_str(issue_id) or ""
        issue_identifier = _as_nonempty_str(issue_identifier) or ""
        review_key = _as_nonempty_str(review_key) or ""
        source_digest = _as_nonempty_str(source_digest) or ""
        decision = _as_nonempty_str(decision) or ""
        if not all((issue_id, issue_identifier, review_key, source_digest, decision)):
            raise ValueError("Ready review fields must be non-empty strings")
        if decision not in (DECISION_READY_FOR_GO, DECISION_BLOCKED):
            raise ValueError("decision must be READY_FOR_GO or BLOCKED")
        if starts_agent_work:
            raise ValueError("starts_agent_work must be False")

        if hasattr(frozen_source, "to_canonical_dict"):
            payload = frozen_source.to_canonical_dict()  # type: ignore[attr-defined]
        elif isinstance(frozen_source, dict):
            payload = frozen_source
        else:
            raise ValueError("frozen_source must be a mapping or FrozenReadySource")
        frozen_source_json = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )

        review_id = uuid.uuid4().hex
        created_at = _hermes_now().isoformat()
        with self._transaction() as conn:
            try:
                conn.execute(
                    """INSERT INTO ready_reviews (
                         review_id, issue_id, issue_identifier, review_key,
                         source_digest, decision, starts_agent_work,
                         frozen_source_json, created_at
                       ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)""",
                    (
                        review_id,
                        issue_id,
                        issue_identifier,
                        review_key,
                        source_digest,
                        decision,
                        frozen_source_json,
                        created_at,
                    ),
                )
            except sqlite3.IntegrityError:
                row = conn.execute(
                    "SELECT * FROM ready_reviews WHERE review_key=?",
                    (review_key,),
                ).fetchone()
                if row is None:
                    # Unique(issue_id, source_digest) collision with different key.
                    row = conn.execute(
                        """SELECT * FROM ready_reviews
                           WHERE issue_id=? AND source_digest=?""",
                        (issue_id, source_digest),
                    ).fetchone()
                if row is None:
                    raise
                return ReadyReviewInsertResult(
                    status="duplicate",
                    record=self._row_to_ready(row),
                )
            row = conn.execute(
                "SELECT * FROM ready_reviews WHERE review_id=?",
                (review_id,),
            ).fetchone()
        return ReadyReviewInsertResult(
            status="created",
            record=self._row_to_ready(row),
        )

    def get_ready_review_by_key(self, review_key: str) -> Optional[ReadyReviewRecord]:
        key = _as_nonempty_str(review_key)
        if key is None:
            return None
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT * FROM ready_reviews WHERE review_key=?",
                (key,),
            ).fetchone()
        return self._row_to_ready(row) if row is not None else None

    def get_ready_provenance(
        self,
        *,
        issue_id: str,
        review_key: Optional[str] = None,
        source_digest: Optional[str] = None,
        require_ready_for_go: bool = True,
    ) -> Optional[ReadyReviewRecord]:
        """Load Ready provenance for an issue, optionally keyed/digested."""
        issue = _as_nonempty_str(issue_id)
        if issue is None:
            return None
        key = _as_nonempty_str(review_key)
        digest = _as_nonempty_str(source_digest)
        with self._transaction() as conn:
            if key is not None:
                row = conn.execute(
                    "SELECT * FROM ready_reviews WHERE review_key=?",
                    (key,),
                ).fetchone()
            elif digest is not None:
                row = conn.execute(
                    """SELECT * FROM ready_reviews
                       WHERE issue_id=? AND source_digest=?
                       ORDER BY created_at DESC LIMIT 1""",
                    (issue, digest),
                ).fetchone()
            else:
                clauses = ["issue_id=?"]
                params: list[Any] = [issue]
                if require_ready_for_go:
                    clauses.append("decision=?")
                    params.append(DECISION_READY_FOR_GO)
                sql = (
                    f"SELECT * FROM ready_reviews WHERE {' AND '.join(clauses)} "
                    "ORDER BY created_at DESC LIMIT 1"
                )
                row = conn.execute(sql, params).fetchone()
        if row is None:
            return None
        record = self._row_to_ready(row)
        if record.issue_id != issue:
            return None
        if digest is not None and record.source_digest != digest:
            return None
        if require_ready_for_go and record.decision != DECISION_READY_FOR_GO:
            return None
        return record

    def list_delivery_keys(self) -> frozenset[str]:
        with self._transaction() as conn:
            rows = conn.execute(
                "SELECT go_event_key FROM launch_intents"
            ).fetchall()
        return frozenset(str(r["go_event_key"]) for r in rows)

    def list_intent_keys(self) -> frozenset[str]:
        with self._transaction() as conn:
            rows = conn.execute(
                "SELECT idempotency_key FROM launch_intents"
            ).fetchall()
        return frozenset(str(r["idempotency_key"]) for r in rows)

    def get_launch_intent_by_event_key(
        self, go_event_key: str
    ) -> Optional[LaunchIntentRecord]:
        key = _as_nonempty_str(go_event_key)
        if key is None:
            return None
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT * FROM launch_intents WHERE go_event_key=?",
                (key,),
            ).fetchone()
        return self._row_to_intent(row) if row is not None else None

    def get_launch_intent_by_idempotency_key(
        self, idempotency_key: str
    ) -> Optional[LaunchIntentRecord]:
        key = _as_nonempty_str(idempotency_key)
        if key is None:
            return None
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT * FROM launch_intents WHERE idempotency_key=?",
                (key,),
            ).fetchone()
        return self._row_to_intent(row) if row is not None else None

    def record_launch_intent(
        self,
        *,
        issue_id: str,
        issue_identifier: str,
        review_key: str,
        source_digest: str,
        go_event_key: str,
        idempotency_key: str,
        dispatched: bool = False,
    ) -> LaunchIntentInsertResult:
        """Insert a non-dispatched LaunchIntent; duplicates are no-ops."""
        issue_id = _as_nonempty_str(issue_id) or ""
        issue_identifier = _as_nonempty_str(issue_identifier) or ""
        review_key = _as_nonempty_str(review_key) or ""
        source_digest = _as_nonempty_str(source_digest) or ""
        go_event_key = _as_nonempty_str(go_event_key) or ""
        idempotency_key = _as_nonempty_str(idempotency_key) or ""
        if not all(
            (
                issue_id,
                issue_identifier,
                review_key,
                source_digest,
                go_event_key,
                idempotency_key,
            )
        ):
            raise ValueError("LaunchIntent fields must be non-empty strings")
        if dispatched:
            raise ValueError("dispatched must be False")

        intent_id = uuid.uuid4().hex
        created_at = _hermes_now().isoformat()
        with self._transaction() as conn:
            # Fail closed if Ready provenance is missing for this review_key.
            ready = conn.execute(
                "SELECT * FROM ready_reviews WHERE review_key=?",
                (review_key,),
            ).fetchone()
            if ready is None:
                raise ValueError("missing_ready_provenance")
            ready_rec = self._row_to_ready(ready)
            if ready_rec.decision != DECISION_READY_FOR_GO:
                raise ValueError("ready_decision_not_ready_for_go")
            if ready_rec.source_digest != source_digest:
                raise ValueError("ready_provenance_digest_mismatch")
            if ready_rec.issue_id != issue_id:
                raise ValueError("ready_provenance_issue_mismatch")
            if ready_rec.issue_identifier != issue_identifier:
                raise ValueError("ready_provenance_issue_identifier_mismatch")

            try:
                conn.execute(
                    """INSERT INTO launch_intents (
                         intent_id, issue_id, issue_identifier, review_key,
                         source_digest, go_event_key, idempotency_key,
                         dispatched, created_at
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)""",
                    (
                        intent_id,
                        issue_id,
                        issue_identifier,
                        review_key,
                        source_digest,
                        go_event_key,
                        idempotency_key,
                        created_at,
                    ),
                )
            except sqlite3.IntegrityError:
                by_event = conn.execute(
                    "SELECT * FROM launch_intents WHERE go_event_key=?",
                    (go_event_key,),
                ).fetchone()
                if by_event is not None:
                    return LaunchIntentInsertResult(
                        status="duplicate",
                        record=self._row_to_intent(by_event),
                        reason="duplicate_delivery_key",
                    )
                by_intent = conn.execute(
                    "SELECT * FROM launch_intents WHERE idempotency_key=?",
                    (idempotency_key,),
                ).fetchone()
                if by_intent is not None:
                    return LaunchIntentInsertResult(
                        status="duplicate",
                        record=self._row_to_intent(by_intent),
                        reason="duplicate_intent_key",
                    )
                raise
            row = conn.execute(
                "SELECT * FROM launch_intents WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
        return LaunchIntentInsertResult(
            status="created",
            record=self._row_to_intent(row),
            reason="",
        )

    def count_ready_reviews(self) -> int:
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM ready_reviews"
            ).fetchone()
        return int(row["n"]) if row is not None else 0

    def count_launch_intents(self) -> int:
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM launch_intents"
            ).fetchone()
        return int(row["n"]) if row is not None else 0
