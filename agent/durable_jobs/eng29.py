"""ENG-29 mandatory Go guard (isolated, default-off SQLite).

Local policy-contract evidence only. This module is not Slack authorization,
not live provider authorization, not gateway ingress, and not a PostgreSQL
or production control plane. No deploy/restart/cutover adapters are
implemented — those categories are enforced by the classifier and guard.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union

from agent.durable_jobs.clock import parse_iso, utcnow_iso
from agent.durable_jobs.decisions import DecisionType, JobCanceledError
from agent.durable_jobs.store import DurableJobStore

MATRIX_VERSION = "eng29-matrix-v1"

DEFAULT_SOURCE_PACKAGE_ID = "hermes-agent-durable-jobs"
DEFAULT_SOURCE_PACKAGE_VERSION = "eng29-v1"
DEFAULT_TARGET_ENVIRONMENT = "durable-jobs-dev"
DEFAULT_CANDIDATE_SHA = "sha-eng29-test"

PROVIDER_CREATE_TARGET_ACTION = "cursor.create_run"
SLACK_POST_ROOT_TARGET_ACTION = "slack.post_root"

MANDATORY_GO_CATEGORIES = frozenset(
    {
        "scope_change",
        "missing_prerequisites",
        "unresolved_provider_ambiguity",
        "deploy",
        "restart",
        "cutover",
        "production_migration",
        "external_promotion_release",
        "financial_action",
    }
)

ADAPTER_TARGET_ACTIONS = frozenset(
    {PROVIDER_CREATE_TARGET_ACTION, SLACK_POST_ROOT_TARGET_ACTION}
)

ACTION_MATRIX: dict[str, dict[str, object]] = {
    category: {"require_go": True, "adapter": None}
    for category in sorted(MANDATORY_GO_CATEGORIES)
}
ACTION_MATRIX[PROVIDER_CREATE_TARGET_ACTION] = {
    "require_go": True,
    "adapter": "injected fake create_run",
}
ACTION_MATRIX[SLACK_POST_ROOT_TARGET_ACTION] = {
    "require_go": True,
    "adapter": "injected fake post_root",
}

SqlitePath = Union[str, Path]


@dataclass(frozen=True)
class Classification:
    action: str
    category: str
    require_go: bool


@dataclass(frozen=True)
class AuthorizationTuple:
    job_id: str
    source_package_id: str
    source_package_version: str
    candidate_sha: str
    candidate_id: str
    candidate_version: str
    target_environment: str
    target_action: str
    authorized_actor: str
    expires_at: str
    policy_version: str
    matrix_version: str
    authorization_idempotency_key: str
    prerequisites_satisfied: bool
    provider_ambiguity_resolved: bool
    created_at: str


@dataclass(frozen=True)
class AuthorizationTupleResult:
    ok: bool
    status: str
    record: Optional[AuthorizationTuple] = None
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class GuardResult:
    ok: bool
    reason_codes: tuple[str, ...] = ()


class AuthorizationDenied(Exception):
    """Fail-closed ENG-29 guard. Not a live Slack/provider authorization."""

    def __init__(
        self, reason_codes: tuple[str, ...] = (), message: str = ""
    ) -> None:
        self.reason_codes = reason_codes
        super().__init__(
            message or ",".join(reason_codes) or "authorization denied"
        )


def classify_target_action(action: Optional[str]) -> Classification:
    raw = "" if action is None else str(action).strip()
    if not raw:
        return Classification(
            action="" if action is None else str(action),
            category="unknown",
            require_go=True,
        )
    if raw in MANDATORY_GO_CATEGORIES or raw in ADAPTER_TARGET_ACTIONS:
        return Classification(action=raw, category=raw, require_go=True)
    return Classification(action=raw, category="unknown", require_go=True)


def _ensure_store(sqlite_path: SqlitePath) -> Path:
    path = Path(sqlite_path)
    DurableJobStore(sqlite_path=path)
    return path


def _connect(sqlite_path: Path, *, immediate: bool = False) -> sqlite3.Connection:
    conn = sqlite3.connect(sqlite_path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    if immediate:
        conn.isolation_level = "IMMEDIATE"
    return conn


def _row_text(row: sqlite3.Row, key: str, *, default: str = "") -> str:
    if key not in row.keys():
        return default
    value = row[key]
    if value is None:
        return default
    return str(value)


def _row_to_tuple(row: sqlite3.Row) -> AuthorizationTuple:
    return AuthorizationTuple(
        job_id=row["job_id"],
        source_package_id=row["source_package_id"],
        source_package_version=row["source_package_version"],
        candidate_sha=row["candidate_sha"],
        candidate_id=_row_text(row, "candidate_id"),
        candidate_version=_row_text(row, "candidate_version"),
        target_environment=row["target_environment"],
        target_action=row["target_action"],
        authorized_actor=row["authorized_actor"],
        expires_at=row["expires_at"],
        policy_version=row["policy_version"],
        matrix_version=row["matrix_version"],
        authorization_idempotency_key=row["authorization_idempotency_key"],
        prerequisites_satisfied=bool(row["prerequisites_satisfied"]),
        provider_ambiguity_resolved=bool(row["provider_ambiguity_resolved"]),
        created_at=row["created_at"],
    )


def _tuple_payload_equal(
    record: AuthorizationTuple,
    *,
    job_id: str,
    source_package_id: str,
    source_package_version: str,
    candidate_sha: str,
    candidate_id: str,
    candidate_version: str,
    target_environment: str,
    target_action: str,
    authorized_actor: str,
    expires_at: str,
    policy_version: str,
    matrix_version: str,
    authorization_idempotency_key: str,
    prerequisites_satisfied: bool,
    provider_ambiguity_resolved: bool,
) -> bool:
    return (
        record.job_id == job_id
        and record.source_package_id == source_package_id
        and record.source_package_version == source_package_version
        and record.candidate_sha == candidate_sha
        and record.candidate_id == candidate_id
        and record.candidate_version == candidate_version
        and record.target_environment == target_environment
        and record.target_action == target_action
        and record.authorized_actor == authorized_actor
        and record.expires_at == expires_at
        and record.policy_version == policy_version
        and record.matrix_version == matrix_version
        and record.authorization_idempotency_key == authorization_idempotency_key
        and record.prerequisites_satisfied is bool(prerequisites_satisfied)
        and record.provider_ambiguity_resolved is bool(provider_ambiguity_resolved)
    )


def _fetch_tuple_by_job_action(
    conn: sqlite3.Connection, job_id: str, target_action: str
) -> Optional[AuthorizationTuple]:
    row = conn.execute(
        """
        SELECT * FROM job_authorization_tuples
         WHERE job_id = ? AND target_action = ?
        """,
        (job_id, target_action),
    ).fetchone()
    return _row_to_tuple(row) if row else None


def _fetch_tuple_by_key(
    conn: sqlite3.Connection, authorization_idempotency_key: str
) -> Optional[AuthorizationTuple]:
    row = conn.execute(
        """
        SELECT * FROM job_authorization_tuples
         WHERE authorization_idempotency_key = ?
        """,
        (authorization_idempotency_key,),
    ).fetchone()
    return _row_to_tuple(row) if row else None


def get_authorization_tuple(
    sqlite_path: SqlitePath, job_id: str, target_action: str
) -> Optional[AuthorizationTuple]:
    path = _ensure_store(sqlite_path)
    with _connect(path) as conn:
        return _fetch_tuple_by_job_action(conn, job_id, target_action)


def register_authorization_tuple(
    sqlite_path: SqlitePath,
    *,
    job_id: str,
    source_package_id: str,
    source_package_version: str,
    candidate_sha: str,
    candidate_id: str,
    candidate_version: str,
    target_environment: str,
    target_action: str,
    authorized_actor: str,
    expires_at: str,
    policy_version: str,
    matrix_version: str,
    authorization_idempotency_key: str,
    prerequisites_satisfied: bool = False,
    provider_ambiguity_resolved: bool = False,
) -> AuthorizationTupleResult:
    """Insert an immutable authorization tuple. Never upsert-changes authority."""
    path = _ensure_store(sqlite_path)
    if matrix_version != MATRIX_VERSION:
        return AuthorizationTupleResult(
            ok=False,
            status="rejected",
            reason_codes=("mismatch",),
        )

    payload = dict(
        job_id=job_id,
        source_package_id=source_package_id,
        source_package_version=source_package_version,
        candidate_sha=candidate_sha,
        candidate_id=candidate_id,
        candidate_version=candidate_version,
        target_environment=target_environment,
        target_action=target_action,
        authorized_actor=authorized_actor,
        expires_at=expires_at,
        policy_version=policy_version,
        matrix_version=matrix_version,
        authorization_idempotency_key=authorization_idempotency_key,
        prerequisites_satisfied=bool(prerequisites_satisfied),
        provider_ambiguity_resolved=bool(provider_ambiguity_resolved),
    )

    def _conflict(existing: AuthorizationTuple) -> AuthorizationTupleResult:
        if _tuple_payload_equal(existing, **payload):
            return AuthorizationTupleResult(
                ok=True,
                status="duplicate",
                record=existing,
            )
        if (
            existing.policy_version != policy_version
            or existing.matrix_version != matrix_version
        ):
            return AuthorizationTupleResult(
                ok=False,
                status="rejected",
                record=existing,
                reason_codes=("mismatch",),
            )
        return AuthorizationTupleResult(
            ok=False,
            status="rejected",
            record=existing,
            reason_codes=("replayed",),
        )

    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    with _connect(path, immediate=True) as conn:
        job_row = conn.execute(
            "SELECT 1 FROM durable_jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        if job_row is None:
            return AuthorizationTupleResult(
                ok=False,
                status="rejected",
                reason_codes=("unauthorized",),
            )
        existing_ja = _fetch_tuple_by_job_action(conn, job_id, target_action)
        if existing_ja is not None:
            return _conflict(existing_ja)
        existing_key = _fetch_tuple_by_key(conn, authorization_idempotency_key)
        if existing_key is not None:
            return _conflict(existing_key)
        try:
            conn.execute(
                """
                INSERT INTO job_authorization_tuples(
                    job_id, source_package_id, source_package_version,
                    candidate_sha, candidate_id, candidate_version,
                    target_environment, target_action,
                    authorized_actor, expires_at, policy_version,
                    matrix_version, authorization_idempotency_key,
                    prerequisites_satisfied, provider_ambiguity_resolved,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    source_package_id,
                    source_package_version,
                    candidate_sha,
                    candidate_id,
                    candidate_version,
                    target_environment,
                    target_action,
                    authorized_actor,
                    expires_at,
                    policy_version,
                    matrix_version,
                    authorization_idempotency_key,
                    1 if prerequisites_satisfied else 0,
                    1 if provider_ambiguity_resolved else 0,
                    now,
                ),
            )
            DurableJobStore._append_event(
                conn,
                job_id=job_id,
                event_type="job_authorization_tuple_registered",
                payload={
                    "target_action": target_action,
                    "matrix_version": matrix_version,
                    "policy_version": policy_version,
                    "authorization_idempotency_key": authorization_idempotency_key,
                },
                idempotency_key=(
                    f"job_authorization_tuple_registered:"
                    f"{authorization_idempotency_key}"
                ),
            )
        except sqlite3.IntegrityError:
            raced = _fetch_tuple_by_job_action(
                conn, job_id, target_action
            ) or _fetch_tuple_by_key(conn, authorization_idempotency_key)
            if raced is None:
                raise
            return _conflict(raced)
        row = conn.execute(
            """
            SELECT * FROM job_authorization_tuples
             WHERE job_id = ? AND target_action = ?
            """,
            (job_id, target_action),
        ).fetchone()
    assert row is not None
    record = _row_to_tuple(row)
    return AuthorizationTupleResult(
        ok=True, status="accepted", record=record
    )


def _optional_text(row: sqlite3.Row, key: str) -> Optional[str]:
    if key not in row.keys():
        return None
    value = row[key]
    if value is None:
        return None
    return str(value)


def _required_identity_missing(
    *,
    job_id: str,
    source_package_id: str,
    source_package_version: str,
    target_environment: str,
    target_action: str,
    actor_id: str,
    policy_version: str,
    matrix_version: str,
    candidate_id: str,
    candidate_version: str,
) -> bool:
    return any(
        not str(value).strip()
        for value in (
            job_id,
            source_package_id,
            source_package_version,
            target_environment,
            target_action,
            actor_id,
            policy_version,
            matrix_version,
            candidate_id,
            candidate_version,
        )
    )


def evaluate_authorization(
    sqlite_path: SqlitePath,
    *,
    job_id: str,
    source_package_id: str,
    source_package_version: str,
    candidate_sha: str,
    candidate_id: str,
    candidate_version: str,
    target_environment: str,
    target_action: str,
    actor_id: str,
    policy_version: str,
    matrix_version: str,
    now_iso: Optional[str] = None,
) -> GuardResult:
    """Exact matching, unexpired ACCEPTED Go. Fail closed otherwise."""
    if _required_identity_missing(
        job_id=job_id,
        source_package_id=source_package_id,
        source_package_version=source_package_version,
        target_environment=target_environment,
        target_action=target_action,
        actor_id=actor_id,
        policy_version=policy_version,
        matrix_version=matrix_version,
        candidate_id=candidate_id,
        candidate_version=candidate_version,
    ):
        return GuardResult(ok=False, reason_codes=("unauthorized",))

    path = _ensure_store(sqlite_path)
    now = now_iso or utcnow_iso()
    classified = classify_target_action(target_action)

    with _connect(path) as conn:
        job_row = conn.execute(
            "SELECT 1 FROM durable_jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        if job_row is None:
            return GuardResult(ok=False, reason_codes=("unauthorized",))

        canceled = conn.execute(
            """
            SELECT 1 FROM job_decisions
             WHERE job_id = ? AND decision_type = 'cancel'
               AND status IN ('accepted', 'duplicate')
             LIMIT 1
            """,
            (job_id,),
        ).fetchone()
        if canceled is not None:
            return GuardResult(ok=False, reason_codes=("canceled",))

        latest = conn.execute(
            """
            SELECT * FROM job_decisions
             WHERE job_id = ? AND status = 'accepted'
             ORDER BY created_at DESC, decision_id DESC
             LIMIT 1
            """,
            (job_id,),
        ).fetchone()
        if latest is not None and latest["decision_type"] == DecisionType.HOLD.value:
            return GuardResult(ok=False, reason_codes=("hold",))

        tup = _fetch_tuple_by_job_action(conn, job_id, target_action)
        if tup is None:
            any_tuple = conn.execute(
                """
                SELECT 1 FROM job_authorization_tuples
                 WHERE job_id = ? LIMIT 1
                """,
                (job_id,),
            ).fetchone()
            if any_tuple is not None:
                return GuardResult(ok=False, reason_codes=("mismatch",))
            return GuardResult(ok=False, reason_codes=("no_go",))

        mismatches = (
            tup.source_package_id != source_package_id,
            tup.source_package_version != source_package_version,
            tup.candidate_sha != candidate_sha,
            tup.candidate_id != candidate_id,
            tup.candidate_version != candidate_version,
            tup.target_environment != target_environment,
            tup.target_action != target_action,
            tup.authorized_actor != actor_id,
            tup.policy_version != policy_version,
            tup.matrix_version != matrix_version,
        )
        if any(mismatches):
            return GuardResult(ok=False, reason_codes=("mismatch",))

        try:
            if parse_iso(tup.expires_at) <= parse_iso(now):
                return GuardResult(ok=False, reason_codes=("expired",))
        except ValueError:
            return GuardResult(ok=False, reason_codes=("expired",))

        if classified.category == "missing_prerequisites" and not (
            tup.prerequisites_satisfied
        ):
            return GuardResult(
                ok=False, reason_codes=("missing_prerequisites",)
            )
        if classified.category == "unresolved_provider_ambiguity" and not (
            tup.provider_ambiguity_resolved
        ):
            return GuardResult(
                ok=False, reason_codes=("unresolved_provider_ambiguity",)
            )

        go_rows = conn.execute(
            """
            SELECT * FROM job_decisions
             WHERE job_id = ? AND decision_type = 'go' AND status = 'accepted'
             ORDER BY created_at DESC, decision_id DESC
            """,
            (job_id,),
        ).fetchall()
        matching_go = False
        for row in go_rows:
            if (
                row["actor_id"] == actor_id
                and row["policy_version"] == policy_version
                and _optional_text(row, "source_package_id") == source_package_id
                and _optional_text(row, "source_package_version")
                == source_package_version
                and _optional_text(row, "candidate_sha") == candidate_sha
                and _optional_text(row, "candidate_id") == candidate_id
                and _optional_text(row, "candidate_version") == candidate_version
                and _optional_text(row, "target_environment")
                == target_environment
                and _optional_text(row, "target_action") == target_action
                and _optional_text(row, "matrix_version") == matrix_version
            ):
                matching_go = True
                break
        if not matching_go:
            return GuardResult(ok=False, reason_codes=("no_go",))

    _ = classified.require_go  # unknown/mandatory always require Go
    return GuardResult(ok=True, reason_codes=())


def raise_unless_authorized_go(
    sqlite_path: SqlitePath,
    *,
    job_id: str,
    source_package_id: str,
    source_package_version: str,
    candidate_sha: str,
    candidate_id: str,
    candidate_version: str,
    target_environment: str,
    target_action: str,
    actor_id: str,
    policy_version: str,
    matrix_version: str,
    now_iso: Optional[str] = None,
    action: str = "effect",
) -> GuardResult:
    result = evaluate_authorization(
        sqlite_path,
        job_id=job_id,
        source_package_id=source_package_id,
        source_package_version=source_package_version,
        candidate_sha=candidate_sha,
        candidate_id=candidate_id,
        candidate_version=candidate_version,
        target_environment=target_environment,
        target_action=target_action,
        actor_id=actor_id,
        policy_version=policy_version,
        matrix_version=matrix_version,
        now_iso=now_iso,
    )
    if result.ok:
        return result
    if "canceled" in result.reason_codes:
        raise JobCanceledError(
            f"job {job_id} is canceled; refusing {action}"
        )
    raise AuthorizationDenied(result.reason_codes)


def _live_policy_actor(
    conn: sqlite3.Connection, job_id: str
) -> tuple[str, str]:
    row = conn.execute(
        """
        SELECT policy_version, allowed_actors_json
          FROM job_authz_policies
         WHERE job_id = ?
        """,
        (job_id,),
    ).fetchone()
    if row is None:
        return "", ""
    policy_version = str(row["policy_version"] or "")
    try:
        actors = json.loads(row["allowed_actors_json"] or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        return "", policy_version
    if not isinstance(actors, list) or not actors:
        return "", policy_version
    return str(actors[0] or ""), policy_version


def raise_unless_adapter_go(
    sqlite_path: SqlitePath,
    *,
    job_id: str,
    target_action: str,
    candidate_id: str,
    candidate_version: str,
    now_iso: Optional[str] = None,
    action: str = "adapter effect",
) -> GuardResult:
    """Bind provider create / Slack post_root to the effect's authorization tuple.

    Identity is derived from the job row, live policy, and the caller-supplied
    candidate/version. Hardcoded DEFAULT_* values and the stored tuple actor
    are never used as the requested identity.
    """
    denied = GuardResult(ok=False, reason_codes=("unauthorized",))
    if not (
        str(job_id).strip()
        and str(target_action).strip()
        and str(candidate_id).strip()
        and str(candidate_version).strip()
    ):
        raise AuthorizationDenied(denied.reason_codes)

    path = _ensure_store(sqlite_path)
    with _connect(path) as conn:
        job_row = conn.execute(
            "SELECT * FROM durable_jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        if job_row is None:
            raise AuthorizationDenied(denied.reason_codes)
        source_package_id = str(job_row["repository_identity"] or "")
        candidate_sha = (
            ""
            if job_row["frozen_baseline_sha"] is None
            else str(job_row["frozen_baseline_sha"])
        )
        target_environment = str(job_row["origin_platform"] or "")
        actor_id, policy_version = _live_policy_actor(conn, job_id)

    return raise_unless_authorized_go(
        path,
        job_id=job_id,
        source_package_id=source_package_id,
        source_package_version=str(candidate_version).strip(),
        candidate_sha=candidate_sha,
        candidate_id=str(candidate_id).strip(),
        candidate_version=str(candidate_version).strip(),
        target_environment=target_environment,
        target_action=str(target_action).strip(),
        actor_id=actor_id,
        policy_version=policy_version,
        matrix_version=MATRIX_VERSION,
        now_iso=now_iso,
        action=action,
    )


def install_default_adapter_authorization(
    sqlite_path: SqlitePath, job_id: str
) -> None:
    """Grant exact Go for provider create + Slack post_root from job+binding.

    Used by existing effect/fencing fixtures so they keep exercising claim
    fencing after ENG-29. Derives the immutable tuple from the job row and
    Slack binding (or binds cand-1/v1 when none exists). ENG-29 no-Go tests
    must not call this. Production adapter paths never call this helper.
    """
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.slack_contract import BindingConflict, SlackBindingLedger

    path = _ensure_store(sqlite_path)
    jobs = DurableJobStore(sqlite_path=path)
    job = jobs.get_job(job_id)
    if job is None:
        return

    slack = SlackBindingLedger(sqlite_path=path)
    bound = slack.get_binding(job_id)
    if bound is None:
        try:
            slack.bind(
                job_id=job_id,
                workspace_id="T1",
                channel_id="C123",
                root_thread_ts="111.222",
                candidate_id="cand-1",
                candidate_version="v1",
            )
        except BindingConflict:
            return
        bound = slack.get_binding(job_id)
    if bound is None:
        return

    decisions = DecisionLedger(sqlite_path=path)
    with _connect(path) as conn:
        policy_row = conn.execute(
            "SELECT 1 FROM job_authz_policies WHERE job_id = ?",
            (job_id,),
        ).fetchone()
    if policy_row is None:
        decisions.set_policy(
            job_id=job_id,
            policy_version="pol-1",
            allowed_actors=("U-alice",),
            expires_at="2099-01-01T00:00:00+00:00",
        )

    with _connect(path) as conn:
        actor_id, policy_version = _live_policy_actor(conn, job_id)
    if not str(actor_id).strip() or not str(policy_version).strip():
        return

    expires_at = "2099-01-01T00:00:00+00:00"
    for target_action in (
        PROVIDER_CREATE_TARGET_ACTION,
        SLACK_POST_ROOT_TARGET_ACTION,
    ):
        register_authorization_tuple(
            path,
            job_id=job_id,
            source_package_id=job.repository_identity,
            source_package_version=bound.candidate_version,
            candidate_sha=job.frozen_baseline_sha,
            candidate_id=bound.candidate_id,
            candidate_version=bound.candidate_version,
            target_environment=job.origin_platform,
            target_action=target_action,
            authorized_actor=actor_id,
            expires_at=expires_at,
            policy_version=policy_version,
            matrix_version=MATRIX_VERSION,
            authorization_idempotency_key=f"tuple:{job_id}:{target_action}",
            prerequisites_satisfied=True,
            provider_ambiguity_resolved=True,
        )
        decisions.record_decision(
            job_id=job_id,
            decision_type="go",
            candidate_id=bound.candidate_id,
            candidate_version=bound.candidate_version,
            actor_id=actor_id,
            policy_version=policy_version,
            decision_idempotency_key=f"go:{job_id}:{target_action}",
            source_package_id=job.repository_identity,
            source_package_version=bound.candidate_version,
            candidate_sha=job.frozen_baseline_sha,
            target_environment=job.origin_platform,
            target_action=target_action,
            matrix_version=MATRIX_VERSION,
        )
