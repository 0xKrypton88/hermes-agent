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
from agent.durable_jobs.redaction import redact_payload

SCHEMA_VERSION = 9

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

CREATE TABLE IF NOT EXISTS provider_effect_claims (
    job_id TEXT NOT NULL,
    action_id TEXT NOT NULL,
    provider_idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    provider_run_id TEXT,
    langgraph_thread_id TEXT NOT NULL,
    origin_platform TEXT NOT NULL,
    origin_chat_id TEXT NOT NULL,
    origin_root_thread_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    candidate_version TEXT NOT NULL,
    unknown_reason TEXT,
    claim_owner_token TEXT,
    claim_leased_at TEXT,
    claim_expires_at TEXT,
    claim_generation INTEGER NOT NULL DEFAULT 0,
    recovery_attempt_count INTEGER NOT NULL DEFAULT 0,
    recovery_started_at TEXT,
    recovery_deadline TEXT,
    effect_inflight_token TEXT,
    effect_inflight_until TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (job_id, action_id),
    CHECK (langgraph_thread_id = job_id),
    CHECK (status IN ('claimed', 'accepted', 'adopted', 'unknown', 'recovering')),
    FOREIGN KEY (job_id) REFERENCES durable_jobs(job_id)
);

CREATE TABLE IF NOT EXISTS provider_job_mappings (
    job_id TEXT PRIMARY KEY,
    langgraph_thread_id TEXT NOT NULL,
    provider_run_id TEXT,
    origin_platform TEXT NOT NULL,
    origin_chat_id TEXT NOT NULL,
    origin_root_thread_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    candidate_version TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (langgraph_thread_id = job_id),
    FOREIGN KEY (job_id) REFERENCES durable_jobs(job_id)
);

CREATE TABLE IF NOT EXISTS slack_job_bindings (
    job_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    root_thread_ts TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    candidate_version TEXT NOT NULL,
    outbound_client_msg_id TEXT NOT NULL UNIQUE,
    delivered_message_ts TEXT,
    status TEXT NOT NULL,
    unknown_reason TEXT,
    claim_owner_token TEXT,
    claim_leased_at TEXT,
    claim_expires_at TEXT,
    claim_generation INTEGER NOT NULL DEFAULT 0,
    recovery_attempt_count INTEGER NOT NULL DEFAULT 0,
    recovery_started_at TEXT,
    recovery_deadline TEXT,
    effect_inflight_token TEXT,
    effect_inflight_until TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (workspace_id, channel_id, root_thread_ts),
    CHECK (status IN ('bound', 'claimed', 'delivered', 'adopted', 'unknown', 'recovering')),
    FOREIGN KEY (job_id) REFERENCES durable_jobs(job_id)
);

CREATE TABLE IF NOT EXISTS job_authz_policies (
    job_id TEXT PRIMARY KEY,
    policy_version TEXT NOT NULL,
    allowed_actors_json TEXT NOT NULL,
    expires_at TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (job_id) REFERENCES durable_jobs(job_id)
);

CREATE TABLE IF NOT EXISTS job_decisions (
    decision_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    decision_type TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    candidate_version TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    decision_idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    reason_codes_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    source_package_id TEXT,
    source_package_version TEXT,
    candidate_sha TEXT,
    target_environment TEXT,
    target_action TEXT,
    matrix_version TEXT,
    CHECK (decision_type IN ('go', 'hold', 'cancel')),
    CHECK (status IN ('accepted', 'duplicate', 'rejected')),
    FOREIGN KEY (job_id) REFERENCES durable_jobs(job_id)
);

CREATE TABLE IF NOT EXISTS job_authorization_tuples (
    job_id TEXT NOT NULL,
    target_action TEXT NOT NULL,
    source_package_id TEXT NOT NULL,
    source_package_version TEXT NOT NULL,
    candidate_sha TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    candidate_version TEXT NOT NULL,
    target_environment TEXT NOT NULL,
    authorized_actor TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    matrix_version TEXT NOT NULL,
    authorization_idempotency_key TEXT NOT NULL UNIQUE,
    prerequisites_satisfied INTEGER NOT NULL DEFAULT 0,
    provider_ambiguity_resolved INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    PRIMARY KEY (job_id, target_action),
    FOREIGN KEY (job_id) REFERENCES durable_jobs(job_id)
);

CREATE TABLE IF NOT EXISTS job_terminal_evidence (
    evidence_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    source_status TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    CHECK (kind IN ('provider_run', 'slack_root')),
    FOREIGN KEY (job_id) REFERENCES durable_jobs(job_id)
);

CREATE TABLE IF NOT EXISTS job_resume_enqueues (
    enqueue_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    evidence_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    local_marked INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (status IN ('queued', 'accepted', 'failed')),
    CHECK (local_marked = 0 OR status = 'accepted'),
    FOREIGN KEY (job_id) REFERENCES durable_jobs(job_id),
    FOREIGN KEY (evidence_id) REFERENCES job_terminal_evidence(evidence_id)
);

CREATE TABLE IF NOT EXISTS job_inbound_actions (
    inbound_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    root_thread_ts TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    decision_type TEXT NOT NULL,
    policy_version TEXT NOT NULL DEFAULT '',
    candidate_id TEXT NOT NULL DEFAULT '',
    candidate_version TEXT NOT NULL DEFAULT '',
    decision_idempotency_key TEXT NOT NULL UNIQUE,
    decision_id TEXT,
    ack_status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (ack_status IN ('pending', 'acked', 'rejected')),
    FOREIGN KEY (job_id) REFERENCES durable_jobs(job_id)
);

CREATE TABLE IF NOT EXISTS retired_idempotency_keys (
    idempotency_key TEXT PRIMARY KEY,
    origin TEXT NOT NULL,
    retired_at TEXT NOT NULL
);
"""


_CLAIM_LEASE_TABLES = ("provider_effect_claims", "slack_job_bindings")
_CLAIM_LEASE_COLUMNS = (
    ("claim_owner_token", "TEXT"),
    ("claim_leased_at", "TEXT"),
    ("claim_expires_at", "TEXT"),
    ("claim_generation", "INTEGER NOT NULL DEFAULT 0"),
)
_CLAIM_RECOVERY_COLUMNS = (
    ("recovery_attempt_count", "INTEGER NOT NULL DEFAULT 0"),
    ("recovery_started_at", "TEXT"),
    ("recovery_deadline", "TEXT"),
)
_CLAIM_INFLIGHT_COLUMNS = (
    ("effect_inflight_token", "TEXT"),
    ("effect_inflight_until", "TEXT"),
)
_DECISION_AUTHZ_COLUMNS = (
    ("source_package_id", "TEXT"),
    ("source_package_version", "TEXT"),
    ("candidate_sha", "TEXT"),
    ("target_environment", "TEXT"),
    ("target_action", "TEXT"),
    ("matrix_version", "TEXT"),
)
_INBOUND_TUPLE_COLUMNS = (
    ("policy_version", "TEXT NOT NULL DEFAULT ''"),
    ("candidate_id", "TEXT NOT NULL DEFAULT ''"),
    ("candidate_version", "TEXT NOT NULL DEFAULT ''"),
)
_TUPLE_CANDIDATE_COLUMNS = (
    ("candidate_id", "TEXT NOT NULL DEFAULT ''"),
    ("candidate_version", "TEXT NOT NULL DEFAULT ''"),
)


class UnknownSchemaError(ValueError):
    """Refuse writes when schema_version is missing on a pre-existing DB, newer, or unparseable."""


def _utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def after_job_rows_before_commit() -> None:
    """Crash-injection instrumentation after job+event rows, before COMMIT.

    Production is a no-op. This cannot authorize, grant Go, or bypass
    ENG-29. Crash injection must leave zero job rows and zero events:
    the write transaction is still uncommitted.
    """
    return None


_DURABLE_JOBS_TABLE_NAMES = (
    "durable_jobs_meta",
    "durable_jobs",
    "durable_job_events",
    "provider_effect_claims",
    "provider_job_mappings",
    "slack_job_bindings",
    "job_authz_policies",
    "job_decisions",
    "job_authorization_tuples",
    "job_terminal_evidence",
    "job_resume_enqueues",
    "job_inbound_actions",
    "retired_idempotency_keys",
)


def _preexisting_durable_jobs_schema(conn: sqlite3.Connection) -> bool:
    names = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }
    return bool(names.intersection(_DURABLE_JOBS_TABLE_NAMES))


def _read_schema_version(conn: sqlite3.Connection) -> Optional[str]:
    try:
        row = conn.execute(
            "SELECT value FROM durable_jobs_meta WHERE key = 'schema_version'"
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None:
        return None
    return str(row[0]) if row[0] is not None else None


def _parse_schema_version(raw: str) -> Optional[int]:
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return None
    if value < 1:
        return None
    return value


def _ensure_claim_lease_columns(conn: sqlite3.Connection) -> None:
    """Evolve candidate-created v2 DBs; no-op when columns already exist."""
    for table in _CLAIM_LEASE_TABLES:
        existing = {
            row[1] for row in conn.execute(f"PRAGMA table_info({table})")
        }
        if not existing:
            continue
        for name, decl in _CLAIM_LEASE_COLUMNS:
            if name not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")


def _table_ddl(conn: sqlite3.Connection, table: str) -> str:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row[0] if row else ""


def _ensure_recovery_protocol(conn: sqlite3.Connection) -> None:
    """Add recovery columns and widen status CHECK to include recovering."""
    for table in _CLAIM_LEASE_TABLES:
        existing = {
            row[1] for row in conn.execute(f"PRAGMA table_info({table})")
        }
        if not existing:
            continue
        for name, decl in _CLAIM_RECOVERY_COLUMNS:
            if name not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")
    _rebuild_status_check_for_recovering(conn)


def _ensure_inflight_witness(conn: sqlite3.Connection) -> None:
    """Persist in-flight create/post liveness; no-op when columns exist."""
    for table in _CLAIM_LEASE_TABLES:
        existing = {
            row[1] for row in conn.execute(f"PRAGMA table_info({table})")
        }
        if not existing:
            continue
        for name, decl in _CLAIM_INFLIGHT_COLUMNS:
            if name not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")


def _ensure_eng29_authz(conn: sqlite3.Connection) -> None:
    """Disposable-dev ENG-29 tuple table + decision replay columns."""
    existing = {
        row[1] for row in conn.execute("PRAGMA table_info(job_decisions)")
    }
    if existing:
        for name, decl in _DECISION_AUTHZ_COLUMNS:
            if name not in existing:
                conn.execute(
                    f"ALTER TABLE job_decisions ADD COLUMN {name} {decl}"
                )
    tuple_cols = {
        row[1]
        for row in conn.execute("PRAGMA table_info(job_authorization_tuples)")
    }
    if tuple_cols:
        for name, decl in _TUPLE_CANDIDATE_COLUMNS:
            if name not in tuple_cols:
                conn.execute(
                    f"ALTER TABLE job_authorization_tuples ADD COLUMN {name} {decl}"
                )


def _ensure_inbound_tuple_columns(conn: sqlite3.Connection) -> None:
    """Persist inbound idempotency tuple identity on v8 DBs."""
    existing = {
        row[1] for row in conn.execute("PRAGMA table_info(job_inbound_actions)")
    }
    if not existing:
        return
    for name, decl in _INBOUND_TUPLE_COLUMNS:
        if name not in existing:
            conn.execute(
                f"ALTER TABLE job_inbound_actions ADD COLUMN {name} {decl}"
            )


def _rebuild_status_check_for_recovering(conn: sqlite3.Connection) -> None:
    for table in _CLAIM_LEASE_TABLES:
        ddl = _table_ddl(conn, table)
        if not ddl or "recovering" in ddl.lower():
            continue
        _rebuild_claim_table(conn, table)


def _rebuild_claim_table(conn: sqlite3.Connection, table: str) -> None:
    """Recreate a claim table so CHECK allows 'recovering'."""
    legacy = f"{table}__legacy_v4"
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        conn.execute(f"ALTER TABLE {table} RENAME TO {legacy}")
        create_sql = _claim_table_create_sql(table)
        conn.execute(create_sql)
        dest_cols = [
            row[1] for row in conn.execute(f"PRAGMA table_info({table})")
        ]
        src_cols = {
            row[1] for row in conn.execute(f"PRAGMA table_info({legacy})")
        }
        select_parts = []
        for col in dest_cols:
            if col in src_cols:
                select_parts.append(col)
            elif col == "recovery_attempt_count":
                select_parts.append("0")
            else:
                select_parts.append("NULL")
        conn.execute(
            f"INSERT INTO {table} ({', '.join(dest_cols)}) "
            f"SELECT {', '.join(select_parts)} FROM {legacy}"
        )
        conn.execute(f"DROP TABLE {legacy}")
    finally:
        conn.execute("PRAGMA foreign_keys = ON")


def _claim_table_create_sql(table: str) -> str:
    marker = f"CREATE TABLE IF NOT EXISTS {table} ("
    start = _SCHEMA_SQL.find(marker)
    if start < 0:
        raise RuntimeError(f"missing schema for {table}")
    depth = 0
    for index, char in enumerate(_SCHEMA_SQL[start:]):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                # Drop IF NOT EXISTS so a rebuild after RENAME still creates.
                return "CREATE TABLE " + _SCHEMA_SQL[start + len("CREATE TABLE IF NOT EXISTS "): start + index + 1]
    raise RuntimeError(f"unterminated schema for {table}")


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
        conn = sqlite3.connect(self.sqlite_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            # Inspect before any DDL. A missing-table SELECT can abort the
            # implicit txn; roll it back so fail-closed raises cleanly and a
            # truly empty DB can still initialize.
            preexisting = _preexisting_durable_jobs_schema(conn)
            existing = _read_schema_version(conn)
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            if existing is None:
                if preexisting:
                    raise UnknownSchemaError(
                        "missing durable-jobs schema_version on a pre-existing "
                        f"database; refusing writes (local SCHEMA_VERSION={SCHEMA_VERSION})"
                    )
            else:
                parsed = _parse_schema_version(existing)
                if parsed is None or parsed > SCHEMA_VERSION:
                    raise UnknownSchemaError(
                        f"unknown durable-jobs schema_version {existing!r}; "
                        f"refusing writes (local SCHEMA_VERSION={SCHEMA_VERSION})"
                    )
            conn.executescript(_SCHEMA_SQL)
            _ensure_claim_lease_columns(conn)
            _ensure_recovery_protocol(conn)
            _ensure_inflight_witness(conn)
            _ensure_eng29_authz(conn)
            _ensure_inbound_tuple_columns(conn)
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
                after_job_rows_before_commit()
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

    def retire_idempotency_key(self, idempotency_key: str, *, origin: str) -> None:
        now = _utcnow()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO retired_idempotency_keys(idempotency_key, origin, retired_at)
                VALUES (?, ?, ?)
                ON CONFLICT(idempotency_key) DO NOTHING
                """,
                (idempotency_key, origin, now),
            )

    def is_idempotency_key_retired(self, idempotency_key: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM retired_idempotency_keys WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
        return row is not None

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
                    json.dumps(redact_payload(payload), sort_keys=True),
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
