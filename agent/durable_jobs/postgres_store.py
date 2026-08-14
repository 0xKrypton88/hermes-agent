"""PostgreSQL application store for the ENG-25 durable-jobs slice.

Implements the DurableJobStore public contract used by Package 1 job
lifecycle (create, CAS phase transition, outbox events, recovery).
Application tables live in a dedicated schema that is never shared with
the LangGraph checkpointer. There is no SQLite fallback.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from agent.durable_jobs.config import DurableJobsConfigError, validate_schema_identifier
from agent.durable_jobs.models import (
    ALLOWED_TRANSITIONS,
    DEFAULT_NEXT_ACTION,
    DurableJob,
    InvalidPhaseTransition,
    JobPhase,
)
from agent.durable_jobs.postgres_advance import (
    AdvanceClaimDecision,
    AdvanceClaimError,
    AdvanceClaimResult,
    AdvanceClaimView,
    decide_advance_claim,
)
from agent.durable_jobs.postgres_domain import (
    APPLICATION_DOMAIN,
    DOMAIN_META_KEY,
    OWNER_META_KEY,
    SchemaOccupancy,
    classify_schema_occupancy,
    require_owned_or_vacant,
)
from agent.durable_jobs.redaction import redact_payload
from agent.durable_jobs.store import (
    SCHEMA_VERSION,
    UnknownSchemaError,
    _new_job_id,
    _parse_schema_version,
    _utcnow,
    after_job_rows_before_commit,
)

_ADVISORY_CLASSID = 872451
_APP_TABLES = (
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

ADVISORY_LOCK_SQL = "SELECT pg_advisory_xact_lock(%s, hashtext(%s))"
JOB_ROW_LOCK_SQL_TEMPLATE = (
    "SELECT * FROM {schema}.durable_jobs WHERE job_id = %s FOR UPDATE"
)
PHASE_CAS_SQL_TEMPLATE = (
    "UPDATE {schema}.durable_jobs "
    "SET phase = %s, frozen_baseline_sha = %s, next_action = %s, updated_at = %s "
    "WHERE job_id = %s AND phase = %s"
)
INSERT_JOB_SQL_TEMPLATE = (
    "INSERT INTO {schema}.durable_jobs("
    "job_id, phase, origin_platform, origin_chat_id, origin_root_thread_id, "
    "objective, repository_identity, frozen_baseline_sha, idempotency_key, "
    "next_action, created_at, updated_at"
    ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
)
APPEND_EVENT_SQL_TEMPLATE = (
    "INSERT INTO {schema}.durable_job_events("
    "job_id, event_type, payload_json, idempotency_key, created_at"
    ") VALUES (%s, %s, %s, %s, %s)"
)


def transition_sql_batch(schema: str) -> list[str]:
    return [
        ADVISORY_LOCK_SQL,
        JOB_ROW_LOCK_SQL_TEMPLATE.format(schema=schema),
        PHASE_CAS_SQL_TEMPLATE.format(schema=schema),
        APPEND_EVENT_SQL_TEMPLATE.format(schema=schema),
    ]


def create_job_sql_batch(schema: str) -> list[str]:
    return [
        INSERT_JOB_SQL_TEMPLATE.format(schema=schema),
        APPEND_EVENT_SQL_TEMPLATE.format(schema=schema),
    ]


def fail_closed_for_schema_marker(conn: Any, schema: str, preexisting: bool) -> None:
    """Refuse writes when the schema marker is missing/future/unparseable.

    The probe connection must not be used for application mutation. Test
    fakes expose ``version`` so this helper can fail closed without
    executing SQL.
    """
    del schema  # identity is the caller's qualified schema; not interpolated
    version = getattr(conn, "version", None)
    if version is None:
        if preexisting:
            raise UnknownSchemaError(
                "missing durable-jobs schema_version on a pre-existing "
                f"database; refusing writes (local SCHEMA_VERSION={SCHEMA_VERSION})"
            )
        return
    parsed = _parse_schema_version(str(version))
    if parsed is None or parsed != SCHEMA_VERSION:
        raise UnknownSchemaError(
            f"unknown durable-jobs schema_version {version!r}; "
            f"refusing writes (local SCHEMA_VERSION={SCHEMA_VERSION})"
        )


def _require_psycopg():
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise DurableJobsConfigError(
            "PostgreSQL backend requires the langgraph-durable-postgres extra"
        ) from exc
    return psycopg, dict_row


def _application_ddl(schema: str) -> str:
    q = schema
    return f"""
CREATE TABLE IF NOT EXISTS {q}.durable_jobs_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {q}.durable_jobs (
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

CREATE TABLE IF NOT EXISTS {q}.durable_job_events (
    event_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    job_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{{}}',
    idempotency_key TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(job_id, event_type, idempotency_key)
);

CREATE TABLE IF NOT EXISTS {q}.provider_effect_claims (
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
    FOREIGN KEY (job_id) REFERENCES {q}.durable_jobs(job_id)
);

CREATE TABLE IF NOT EXISTS {q}.provider_job_mappings (
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
    FOREIGN KEY (job_id) REFERENCES {q}.durable_jobs(job_id)
);

CREATE TABLE IF NOT EXISTS {q}.slack_job_bindings (
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
    FOREIGN KEY (job_id) REFERENCES {q}.durable_jobs(job_id)
);

CREATE TABLE IF NOT EXISTS {q}.job_authz_policies (
    job_id TEXT PRIMARY KEY,
    policy_version TEXT NOT NULL,
    allowed_actors_json TEXT NOT NULL,
    expires_at TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (job_id) REFERENCES {q}.durable_jobs(job_id)
);

CREATE TABLE IF NOT EXISTS {q}.job_decisions (
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
    FOREIGN KEY (job_id) REFERENCES {q}.durable_jobs(job_id)
);

CREATE TABLE IF NOT EXISTS {q}.job_authorization_tuples (
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
    FOREIGN KEY (job_id) REFERENCES {q}.durable_jobs(job_id)
);

CREATE TABLE IF NOT EXISTS {q}.job_terminal_evidence (
    evidence_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    source_status TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    CHECK (kind IN ('provider_run', 'slack_root')),
    FOREIGN KEY (job_id) REFERENCES {q}.durable_jobs(job_id)
);

CREATE TABLE IF NOT EXISTS {q}.job_resume_enqueues (
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
    FOREIGN KEY (job_id) REFERENCES {q}.durable_jobs(job_id),
    FOREIGN KEY (evidence_id) REFERENCES {q}.job_terminal_evidence(evidence_id)
);

CREATE TABLE IF NOT EXISTS {q}.job_inbound_actions (
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
    FOREIGN KEY (job_id) REFERENCES {q}.durable_jobs(job_id)
);

CREATE TABLE IF NOT EXISTS {q}.retired_idempotency_keys (
    idempotency_key TEXT PRIMARY KEY,
    origin TEXT NOT NULL,
    retired_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {q}.durable_job_advance_claims (
    job_id TEXT PRIMARY KEY,
    owner_token TEXT NOT NULL,
    status TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 0,
    leased_until TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (status IN ('claimed', 'completed')),
    FOREIGN KEY (job_id) REFERENCES {q}.durable_jobs(job_id)
);
"""


class PostgresDurableJobStore:
    def __init__(
        self,
        dsn: str,
        schema: str,
        *,
        connect: Optional[Callable[[], Any]] = None,
    ) -> None:
        self._dsn = dsn
        self._schema = validate_schema_identifier(schema, "postgres_schema")
        self._connect_fn = connect
        self._init_schema()

    def __repr__(self) -> str:
        return f"PostgresDurableJobStore(schema={self._schema!r})"

    def _connect(self):
        if self._connect_fn is not None:
            return self._connect_fn()
        psycopg, dict_row = _require_psycopg()
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def _preexisting(self, conn: Any) -> bool:
        rows = conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = %s",
            (self._schema,),
        ).fetchall()
        names = set()
        for row in rows:
            if isinstance(row, dict):
                names.add(str(row.get("tablename") or next(iter(row.values()))))
            else:
                names.add(str(row[0]))
        return bool(names.intersection(_APP_TABLES))

    def _read_schema_version(self, conn: Any) -> Optional[str]:
        try:
            row = conn.execute(
                f"SELECT value FROM {self._schema}.durable_jobs_meta "
                "WHERE key = %s",
                ("schema_version",),
            ).fetchone()
        except Exception:
            return None
        if row is None:
            return None
        if isinstance(row, dict):
            value = row.get("value")
        else:
            value = row[0]
        return str(value) if value is not None else None

    def _scalar(self, row: Any, key: str, index: int = 0) -> Any:
        if row is None:
            return None
        if isinstance(row, dict):
            return row.get(key, row.get(list(row.keys())[index] if row else None))
        return row[index]

    def _table_names(self, conn: Any) -> frozenset[str]:
        rows = conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = %s",
            (self._schema,),
        ).fetchall()
        names = set()
        for row in rows:
            if isinstance(row, dict):
                names.add(str(row.get("tablename") or next(iter(row.values()))))
            else:
                names.add(str(row[0]))
        return frozenset(names)

    def _read_markers(self, conn: Any, tables: frozenset[str]) -> dict[str, str]:
        if "durable_jobs_meta" not in tables:
            return {}
        rows = conn.execute(
            f"SELECT key, value FROM {self._schema}.durable_jobs_meta"
        ).fetchall()
        markers: dict[str, str] = {}
        for row in rows:
            if isinstance(row, dict):
                markers[str(row["key"])] = str(row["value"])
            else:
                markers[str(row[0])] = str(row[1])
        return markers

    def _current_role(self, conn: Any) -> str:
        row = conn.execute("SELECT current_user").fetchone()
        return str(self._scalar(row, "current_user", 0))

    def _namespace_owner(self, conn: Any) -> Optional[str]:
        row = conn.execute(
            """
            SELECT r.rolname
              FROM pg_namespace n
              JOIN pg_roles r ON r.oid = n.nspowner
             WHERE n.nspname = %s
            """,
            (self._schema,),
        ).fetchone()
        if row is None:
            return None
        return str(self._scalar(row, "rolname", 0))

    def _write_application_markers(self, conn: Any, owner_role: str) -> None:
        for key, value in (
            ("schema_version", str(SCHEMA_VERSION)),
            (DOMAIN_META_KEY, APPLICATION_DOMAIN),
            (OWNER_META_KEY, owner_role),
        ):
            conn.execute(
                f"INSERT INTO {self._schema}.durable_jobs_meta(key, value) "
                "VALUES (%s, %s) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                (key, value),
            )

    def _apply_ddl(self, conn: Any) -> None:
        for statement in _application_ddl(self._schema).split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(stmt)

    def _init_schema(self) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "SELECT pg_advisory_xact_lock(%s, %s)",
                (_ADVISORY_CLASSID, 1),
            )
            owner_role = self._namespace_owner(conn)
            current_role = self._current_role(conn)
            tables = self._table_names(conn)
            markers = self._read_markers(conn, tables)
            occupancy = classify_schema_occupancy(
                schema_exists=owner_role is not None,
                table_names=tables,
                markers=markers,
                owner_role=owner_role,
                current_role=current_role,
                expected_domain=APPLICATION_DOMAIN,
            )
            require_owned_or_vacant(occupancy, schema=self._schema)
            if occupancy is SchemaOccupancy.VACANT:
                conn.execute(
                    f"CREATE SCHEMA {self._schema} AUTHORIZATION CURRENT_USER"
                )
                self._apply_ddl(conn)
                self._write_application_markers(conn, current_role)
            else:
                probe = type("Probe", (), {"version": markers.get("schema_version")})()
                fail_closed_for_schema_marker(
                    probe, schema=self._schema, preexisting=True
                )
                self._apply_ddl(conn)
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()

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

        psycopg, _dict_row = _require_psycopg()
        from psycopg.errors import UniqueViolation

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
        conn = self._connect()
        try:
            conn.execute(
                INSERT_JOB_SQL_TEMPLATE.format(schema=self._schema),
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
        except UniqueViolation:
            try:
                conn.rollback()
            except Exception:
                pass
            adopted = self.get_job_by_idempotency_key(idempotency_key)
            if adopted is None:
                raise
            return adopted
        finally:
            conn.close()
        return job

    def get_job(self, job_id: str) -> Optional[DurableJob]:
        conn = self._connect()
        try:
            row = conn.execute(
                f"SELECT * FROM {self._schema}.durable_jobs WHERE job_id = %s",
                (job_id,),
            ).fetchone()
        finally:
            conn.close()
        return self._row_to_job(row) if row else None

    def get_job_by_idempotency_key(self, idempotency_key: str) -> Optional[DurableJob]:
        conn = self._connect()
        try:
            row = conn.execute(
                f"SELECT * FROM {self._schema}.durable_jobs "
                "WHERE idempotency_key = %s",
                (idempotency_key,),
            ).fetchone()
        finally:
            conn.close()
        return self._row_to_job(row) if row else None

    def count_jobs(self) -> int:
        conn = self._connect()
        try:
            row = conn.execute(
                f"SELECT COUNT(*) AS n FROM {self._schema}.durable_jobs"
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            return 0
        if isinstance(row, dict):
            return int(row["n"])
        return int(row[0])

    def transition_phase(
        self,
        job_id: str,
        new_phase: JobPhase,
        *,
        frozen_baseline_sha: Optional[str] = None,
    ) -> DurableJob:
        now = _utcnow()
        conn = self._connect()
        try:
            conn.execute(ADVISORY_LOCK_SQL, (_ADVISORY_CLASSID, job_id))
            row = conn.execute(
                JOB_ROW_LOCK_SQL_TEMPLATE.format(schema=self._schema),
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
                PHASE_CAS_SQL_TEMPLATE.format(schema=self._schema),
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
                f"SELECT * FROM {self._schema}.durable_jobs WHERE job_id = %s",
                (job_id,),
            ).fetchone()
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()
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
        if self.get_job(job_id) is None:
            raise KeyError(f"unknown job_id: {job_id}")
        conn = self._connect()
        try:
            inserted = self._append_event(
                conn,
                job_id=job_id,
                event_type=event_type,
                payload=payload or {},
                idempotency_key=idempotency_key,
            )
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()
        return inserted

    def list_events(self, job_id: str) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            rows = conn.execute(
                f"""
                SELECT event_id, job_id, event_type, payload_json,
                       idempotency_key, created_at
                  FROM {self._schema}.durable_job_events
                 WHERE job_id = %s
                 ORDER BY event_id ASC
                """,
                (job_id,),
            ).fetchall()
        finally:
            conn.close()
        events = []
        for row in rows:
            item = dict(row)
            if "event_id" in item:
                item["event_id"] = int(item["event_id"])
            events.append(item)
        return events

    def recover_job(self, job_id: str) -> Optional[DurableJob]:
        return self.get_job(job_id)

    def retire_idempotency_key(self, idempotency_key: str, *, origin: str) -> None:
        now = _utcnow()
        conn = self._connect()
        try:
            conn.execute(
                f"""
                INSERT INTO {self._schema}.retired_idempotency_keys(
                    idempotency_key, origin, retired_at
                ) VALUES (%s, %s, %s)
                ON CONFLICT (idempotency_key) DO NOTHING
                """,
                (idempotency_key, origin, now),
            )
            conn.commit()
        finally:
            conn.close()

    def is_idempotency_key_retired(self, idempotency_key: str) -> bool:
        conn = self._connect()
        try:
            row = conn.execute(
                f"SELECT 1 FROM {self._schema}.retired_idempotency_keys "
                "WHERE idempotency_key = %s",
                (idempotency_key,),
            ).fetchone()
        finally:
            conn.close()
        return row is not None

    def _parse_lease(self, raw: str) -> datetime:
        text = str(raw).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    def _claim_view(self, row: Any) -> AdvanceClaimView:
        getter = row.__getitem__
        return AdvanceClaimView(
            owner_token=str(getter("owner_token")),
            status=str(getter("status")),
            generation=int(getter("generation")),
            leased_until=self._parse_lease(str(getter("leased_until"))),
        )

    def claim_advance(
        self,
        job_id: str,
        *,
        owner_token: str,
        lease_seconds: float = 30.0,
    ) -> AdvanceClaimResult:
        now = datetime.now(timezone.utc)
        leased_until = (now + timedelta(seconds=lease_seconds)).replace(
            microsecond=0
        ).isoformat()
        stamp = _utcnow()
        conn = self._connect()
        try:
            conn.execute(ADVISORY_LOCK_SQL, (_ADVISORY_CLASSID, job_id))
            row = conn.execute(
                JOB_ROW_LOCK_SQL_TEMPLATE.format(schema=self._schema),
                (job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown job_id: {job_id}")
            job = self._row_to_job(row)
            claim_row = conn.execute(
                f"SELECT * FROM {self._schema}.durable_job_advance_claims "
                "WHERE job_id = %s FOR UPDATE",
                (job_id,),
            ).fetchone()
            existing = self._claim_view(claim_row) if claim_row else None
            decision = decide_advance_claim(
                job_phase=job.phase.value,
                existing=existing,
                owner_token=owner_token,
                now=now,
            )
            generation = existing.generation if existing is not None else 0
            if decision is AdvanceClaimDecision.WIN:
                generation = 1
                conn.execute(
                    f"""
                    INSERT INTO {self._schema}.durable_job_advance_claims(
                        job_id, owner_token, status, generation, leased_until,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        job_id,
                        owner_token,
                        "claimed",
                        generation,
                        leased_until,
                        stamp,
                        stamp,
                    ),
                )
            elif decision in {
                AdvanceClaimDecision.TAKEOVER,
                AdvanceClaimDecision.REENTER,
            }:
                if decision is AdvanceClaimDecision.TAKEOVER:
                    generation = generation + 1
                conn.execute(
                    f"""
                    UPDATE {self._schema}.durable_job_advance_claims
                       SET owner_token = %s, status = %s, generation = %s,
                           leased_until = %s, updated_at = %s
                     WHERE job_id = %s
                    """,
                    (
                        owner_token,
                        "claimed",
                        generation,
                        leased_until,
                        stamp,
                        job_id,
                    ),
                )
            conn.commit()
            return AdvanceClaimResult(decision=decision, generation=generation)
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()

    def complete_advance(self, job_id: str, *, owner_token: str) -> None:
        stamp = _utcnow()
        conn = self._connect()
        try:
            conn.execute(ADVISORY_LOCK_SQL, (_ADVISORY_CLASSID, job_id))
            cur = conn.execute(
                f"""
                UPDATE {self._schema}.durable_job_advance_claims
                   SET status = %s, updated_at = %s
                 WHERE job_id = %s AND owner_token = %s AND status = %s
                """,
                ("completed", stamp, job_id, owner_token, "claimed"),
            )
            if cur.rowcount != 1:
                raise AdvanceClaimError(
                    f"stale or missing advance owner for {job_id}"
                )
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()

    def _append_event(
        self,
        conn: Any,
        *,
        job_id: str,
        event_type: str,
        payload: dict[str, Any],
        idempotency_key: Optional[str],
    ) -> bool:
        psycopg, _dict_row = _require_psycopg()
        from psycopg.errors import UniqueViolation

        try:
            conn.execute(
                APPEND_EVENT_SQL_TEMPLATE.format(schema=self._schema),
                (
                    job_id,
                    event_type,
                    json.dumps(redact_payload(payload), sort_keys=True),
                    idempotency_key,
                    _utcnow(),
                ),
            )
            return True
        except UniqueViolation:
            return False

    @staticmethod
    def _row_to_job(row: Any) -> DurableJob:
        getter = row.__getitem__
        return DurableJob(
            job_id=getter("job_id"),
            phase=JobPhase(getter("phase")),
            origin_platform=getter("origin_platform"),
            origin_chat_id=getter("origin_chat_id"),
            origin_root_thread_id=getter("origin_root_thread_id"),
            objective=getter("objective"),
            repository_identity=getter("repository_identity"),
            frozen_baseline_sha=getter("frozen_baseline_sha"),
            idempotency_key=getter("idempotency_key"),
            next_action=getter("next_action"),
            created_at=getter("created_at"),
            updated_at=getter("updated_at"),
        )
