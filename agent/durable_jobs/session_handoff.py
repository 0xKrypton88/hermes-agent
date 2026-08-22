"""Offline/shadow session handoff primitives for the existing durable-job lane.

This module never constructs provider, Linear, Slack, or session clients. All
projection/session effects are injected and ``attempt_dispatch`` remains outside
this path and hard-disabled.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
import re
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Mapping, Protocol


from agent.durable_jobs.redaction import redact_secret_text


def _redact_handoff_value(value: Any) -> Any:
    """Redact secret-bearing text recursively while preserving canonical shape."""
    if isinstance(value, dict):
        return {key: _redact_handoff_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_redact_handoff_value(item) for item in value]
    if isinstance(value, str):
        return redact_secret_text(value)
    return value


@dataclass(frozen=True)
class HandoffPolicy:
    soft_arm_ratio: float
    hard_precompression_ratio: float

    def __post_init__(self) -> None:
        if not 0 < self.soft_arm_ratio < self.hard_precompression_ratio <= 1:
            raise ValueError("handoff thresholds must satisfy 0 < soft < hard <= 1")


@dataclass(frozen=True)
class HandoffPressure:
    armed: bool
    hard: bool
    ratio: float


@dataclass(frozen=True)
class SessionHandoffConfig:
    policies: Mapping[tuple[str, str], HandoffPolicy]
    enabled: bool = False
    shadow: bool = True

    def __post_init__(self) -> None:
        if type(self.enabled) is not bool or type(self.shadow) is not bool:
            raise TypeError("session handoff enabled/shadow must be exact booleans")

    @classmethod
    def default_shadow(cls) -> "SessionHandoffConfig":
        return cls(
            policies={
                ("openai-codex", "gpt-5.6-sol"): HandoffPolicy(
                    soft_arm_ratio=0.45,
                    hard_precompression_ratio=0.80,
                )
            }
        )

    @classmethod
    def default(cls) -> "SessionHandoffConfig":
        """Return the shipped inactive/shadow policy."""
        return cls.default_shadow()

    def evaluate(
        self,
        provider: str,
        model: str,
        used_tokens: int,
        context_tokens: int | None = None,
        max_tokens: int | None = None,
    ) -> HandoffPressure:
        if context_tokens is not None and max_tokens is not None:
            raise ValueError("provide either context_tokens or max_tokens, not both")
        context_tokens = context_tokens if context_tokens is not None else max_tokens
        if context_tokens is None:
            raise ValueError("context_tokens is required")
        if used_tokens < 0 or context_tokens <= 0:
            raise ValueError("token counts must be non-negative with positive context")
        ratio = used_tokens / context_tokens
        policy = self.policies.get((provider, model))
        if policy is None:
            return HandoffPressure(armed=False, hard=False, ratio=ratio)
        return HandoffPressure(
            armed=ratio >= policy.soft_arm_ratio,
            hard=ratio >= policy.hard_precompression_ratio,
            ratio=ratio,
        )


@dataclass(frozen=True)
class SessionHandoff:
    handoff_id: str
    idempotency_key: str
    project: str
    issue: str
    goal: str
    verified: tuple[str, ...]
    pending: tuple[str, ...]
    remaining: tuple[str, ...]
    blockers: tuple[str, ...]
    user_action: str
    repository: str
    worktree: str
    branch: str
    exact_sha: str
    diff_fingerprint: str
    test_evidence: tuple[str, ...]
    risk_gates: tuple[str, ...]
    forbidden_actions: tuple[str, ...]
    resume_pointer: str
    next_action: str
    schema: str = "hermes.session-handoff"
    version: int = 1

    def canonical_json(self) -> str:
        return json.dumps(
            _redact_handoff_value(asdict(self)),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )


@dataclass(frozen=True)
class SemanticWaypoint:
    verified: bool
    tool_active: bool = False
    external_mutation_active: bool = False
    commit_active: bool = False
    push_active: bool = False
    deploy_active: bool = False
    authority_boundary_active: bool = False

    @property
    def safe(self) -> bool:
        return self.verified and not any((
            self.tool_active,
            self.external_mutation_active,
            self.commit_active,
            self.push_active,
            self.deploy_active,
            self.authority_boundary_active,
        ))


class UnsafeHandoffWaypoint(RuntimeError):
    pass


class HandoffNotArmed(RuntimeError):
    pass


class ManualResumeRequired(RuntimeError):
    pass


class HandoffIdentityMismatch(RuntimeError):
    pass


class ProjectionVerificationError(RuntimeError):
    pass


class EffectReconciliationRequired(RuntimeError):
    pass


class EffectOwnershipLost(RuntimeError):
    pass


class _EffectOwnerGuard:
    """OS-backed witness that no live coordinator owns this effect."""

    def __init__(
        self,
        lock_path: Path,
        *,
        job_id: str,
        handoff_id: str,
        effect_name: str,
    ) -> None:
        self.lock_path = lock_path
        self.job_id = job_id
        self.handoff_id = handoff_id
        self.effect_name = effect_name
        self._handle: Any | None = None

    @property
    def held(self) -> bool:
        return self._handle is not None

    def acquire(self) -> "_EffectOwnerGuard":
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = open(self.lock_path, "a+b")
        try:
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise EffectOwnershipLost("effect owner is still live") from exc
        self._handle = handle
        return self

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
            self._handle = None

    def __enter__(self) -> "_EffectOwnerGuard":
        return self.acquire()

    def __exit__(self, *_exc: object) -> None:
        self.release()


class LinearHandoffProjection(Protocol):
    def upsert_handoff(
        self, *, issue: str, canonical: str, idempotency_key: str
    ) -> str: ...

    def read_handoff(self, *, issue: str) -> str: ...


class SlackHandoffProjection(Protocol):
    def post_handoff_receipt(
        self, *, handoff_id: str, resume_pointer: str, idempotency_key: str
    ) -> str: ...


class ChildSessionPort(Protocol):
    def find_or_create_child(
        self,
        *,
        parent_session_id: str,
        handoff_id: str,
        idempotency_key: str,
    ) -> str: ...

    def inject_handoff(
        self, *, child_session_id: str, canonical: str, idempotency_key: str
    ) -> None: ...

    def start_first_turn(
        self, *, child_session_id: str, next_action: str, idempotency_key: str
    ) -> None: ...


@dataclass(frozen=True)
class HandoffState:
    job_id: str
    handoff_id: str
    stage: str
    checkpoint_stage: str
    canonical_hash: str
    linear_receipt: str | None
    slack_receipt: str | None
    child_session_id: str | None
    failure_reason: str | None
    manual_resume_action: str | None


@dataclass(frozen=True)
class EffectClaim:
    job_id: str
    handoff_id: str
    effect_name: str
    status: str
    owner_token: str | None
    generation: int
    acquired: bool


_STAGES = (
    "STAGED",
    "LINEAR_VERIFIED",
    "SLACK_RECEIPTED",
    "CHILD_CREATED",
    "HANDOFF_INJECTED",
    "FIRST_TURN_STARTED",
    "COMPLETE",
)
_STAGE_INDEX = {stage: index for index, stage in enumerate(_STAGES)}
_EFFECT_NAMES = {
    "LINEAR_UPSERT",
    "SLACK_RECEIPT",
    "CHILD_CREATE",
    "HANDOFF_INJECT",
    "FIRST_TURN_START",
}
_EFFECT_TARGETS = {
    "LINEAR_UPSERT": ("LINEAR_VERIFIED", "linear_receipt"),
    "SLACK_RECEIPT": ("SLACK_RECEIPTED", "slack_receipt"),
    "CHILD_CREATE": ("CHILD_CREATED", "child_session_id"),
    "HANDOFF_INJECT": ("HANDOFF_INJECTED", None),
    "FIRST_TURN_START": ("FIRST_TURN_STARTED", None),
}
_MANUAL_RESUME = "Call resume_session_handoff(..., manual_resume=True) after fixing the injected boundary"


def _validate_receipt(receipt: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9._:/-]{1,512}", receipt):
        raise ValueError("invalid effect receipt")
    if redact_secret_text(receipt) != receipt:
        raise ValueError("effect receipt must not contain secret-bearing material")


class SessionHandoffLedger:
    """Canonical state in the existing durable lane's SQLite database."""

    def __init__(self, sqlite_path: Any) -> None:
        self.sqlite_path = sqlite_path
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS session_handoffs (
                    job_id TEXT NOT NULL,
                    handoff_id TEXT NOT NULL,
                    parent_session_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    canonical_json TEXT NOT NULL,
                    canonical_hash TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    checkpoint_stage TEXT NOT NULL,
                    linear_receipt TEXT,
                    slack_receipt TEXT,
                    child_session_id TEXT,
                    failure_reason TEXT,
                    manual_resume_action TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(job_id, handoff_id),
                    FOREIGN KEY(job_id) REFERENCES durable_jobs(job_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS session_handoff_effects (
                    job_id TEXT NOT NULL,
                    handoff_id TEXT NOT NULL,
                    effect_name TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('IN_FLIGHT', 'RETRYABLE', 'APPLIED')),
                    owner_token TEXT,
                    generation INTEGER NOT NULL DEFAULT 1,
                    reconciliation_receipt TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(job_id, handoff_id, effect_name),
                    FOREIGN KEY(job_id, handoff_id)
                        REFERENCES session_handoffs(job_id, handoff_id)
                )
                """
            )
            columns = {
                str(row[1])
                for row in conn.execute("PRAGMA table_info(session_handoff_effects)")
            }
            if "generation" not in columns:
                conn.execute(
                    "ALTER TABLE session_handoff_effects "
                    "ADD COLUMN generation INTEGER NOT NULL DEFAULT 1"
                )
            if "reconciliation_receipt" not in columns:
                conn.execute(
                    "ALTER TABLE session_handoff_effects "
                    "ADD COLUMN reconciliation_receipt TEXT"
                )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.sqlite_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    def effect_owner_guard(
        self, job_id: str, handoff_id: str, effect_name: str
    ) -> _EffectOwnerGuard:
        database = Path(self.sqlite_path)
        stat = database.stat()
        if stat.st_ino == 0:
            raise RuntimeError("database file identity is unavailable")
        database_identity = f"{stat.st_dev}:{stat.st_ino}".encode("ascii")
        database_digest = hashlib.sha256(database_identity).hexdigest()
        effect_identity = "\0".join((job_id, handoff_id, effect_name)).encode("utf-8")
        effect_digest = hashlib.sha256(effect_identity).hexdigest()
        lock_path = (
            Path(tempfile.gettempdir())
            / "hermes-session-handoff-effect-locks"
            / database_digest
            / f"{effect_digest}.lock"
        )
        return _EffectOwnerGuard(
            lock_path,
            job_id=job_id,
            handoff_id=handoff_id,
            effect_name=effect_name,
        )

    @staticmethod
    def _hash(canonical: str) -> str:
        return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def stage(
        self, job_id: str, parent_session_id: str, handoff: SessionHandoff
    ) -> HandoffState:
        canonical = handoff.canonical_json()
        digest = self._hash(canonical)
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO session_handoffs(
                    job_id, handoff_id, parent_session_id, idempotency_key, canonical_json,
                    canonical_hash, stage, checkpoint_stage, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'STAGED', 'STAGED', ?)
                ON CONFLICT(idempotency_key) DO NOTHING
                """,
                (
                    job_id,
                    handoff.handoff_id,
                    parent_session_id,
                    handoff.idempotency_key,
                    canonical,
                    digest,
                    now,
                ),
            )
        state = self.get(job_id, handoff.handoff_id)
        with self._connect() as conn:
            identity = conn.execute(
                "SELECT parent_session_id FROM session_handoffs WHERE job_id=? AND handoff_id=?",
                (job_id, handoff.handoff_id),
            ).fetchone()
        if state is None or state.canonical_hash != digest:
            raise ProjectionVerificationError(
                "idempotency key reused for different handoff"
            )
        if identity is None or identity[0] != parent_session_id:
            raise HandoffIdentityMismatch(
                "handoff cannot resume under a different parent session"
            )
        return state

    def canonical(self, job_id: str, handoff_id: str) -> str:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT canonical_json FROM session_handoffs WHERE job_id=? AND handoff_id=?",
                (job_id, handoff_id),
            ).fetchone()
        if row is None:
            raise KeyError(handoff_id)
        return str(row[0])

    def claim_effect(
        self,
        job_id: str,
        handoff_id: str,
        effect_name: str,
        *,
        owner_token: str,
    ) -> EffectClaim:
        if effect_name not in _EFFECT_NAMES:
            raise ValueError("unsupported handoff effect")
        if not re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", owner_token):
            raise ValueError("invalid effect owner token")
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """
                INSERT INTO session_handoff_effects(
                    job_id, handoff_id, effect_name, status, owner_token,
                    created_at, updated_at
                ) VALUES (?, ?, ?, 'IN_FLIGHT', ?, ?, ?)
                ON CONFLICT(job_id, handoff_id, effect_name) DO NOTHING
                """,
                (job_id, handoff_id, effect_name, owner_token, now, now),
            )
            acquired = cursor.rowcount == 1
            if not acquired:
                retry = conn.execute(
                    """
                    UPDATE session_handoff_effects
                       SET status='IN_FLIGHT', owner_token=?, generation=generation+1,
                           updated_at=?
                     WHERE job_id=? AND handoff_id=? AND effect_name=?
                       AND status='RETRYABLE'
                    """,
                    (owner_token, now, job_id, handoff_id, effect_name),
                )
                acquired = retry.rowcount == 1
            row = conn.execute(
                """
                SELECT status, owner_token, generation
                  FROM session_handoff_effects
                 WHERE job_id=? AND handoff_id=? AND effect_name=?
                """,
                (job_id, handoff_id, effect_name),
            ).fetchone()
        if row is None:
            raise KeyError(handoff_id)
        return EffectClaim(
            job_id=job_id,
            handoff_id=handoff_id,
            effect_name=effect_name,
            status=str(row["status"]),
            owner_token=row["owner_token"],
            generation=int(row["generation"]),
            acquired=acquired,
        )

    def get_effect(
        self, job_id: str, handoff_id: str, effect_name: str
    ) -> EffectClaim | None:
        if effect_name not in _EFFECT_NAMES:
            raise ValueError("unsupported handoff effect")
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT status, owner_token, generation
                  FROM session_handoff_effects
                 WHERE job_id=? AND handoff_id=? AND effect_name=?
                """,
                (job_id, handoff_id, effect_name),
            ).fetchone()
        if row is None:
            return None
        return EffectClaim(
            job_id=job_id,
            handoff_id=handoff_id,
            effect_name=effect_name,
            status=str(row["status"]),
            owner_token=row["owner_token"],
            generation=int(row["generation"]),
            acquired=False,
        )

    def complete_effect(
        self,
        job_id: str,
        handoff_id: str,
        effect_name: str,
        *,
        owner_token: str,
        expected_generation: int,
        receipt: str | None = None,
    ) -> HandoffState:
        if effect_name not in _EFFECT_TARGETS:
            raise ValueError("unsupported handoff effect")
        target_stage, receipt_field = _EFFECT_TARGETS[effect_name]
        if receipt_field is not None and not receipt:
            raise ProjectionVerificationError("effect receipt is required")
        if receipt_field is None and receipt is not None:
            raise ValueError("effect does not accept a receipt")
        if receipt is not None:
            _validate_receipt(receipt)
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            claim = conn.execute(
                """
                SELECT status, owner_token, generation
                  FROM session_handoff_effects
                 WHERE job_id=? AND handoff_id=? AND effect_name=?
                """,
                (job_id, handoff_id, effect_name),
            ).fetchone()
            if (
                claim is None
                or claim["status"] != "IN_FLIGHT"
                or claim["owner_token"] != owner_token
                or int(claim["generation"]) != expected_generation
            ):
                raise EffectOwnershipLost("effect claim is not owned by this caller")
            handoff = conn.execute(
                """
                SELECT stage, checkpoint_stage
                  FROM session_handoffs
                 WHERE job_id=? AND handoff_id=?
                """,
                (job_id, handoff_id),
            ).fetchone()
            if handoff is None:
                raise KeyError(handoff_id)
            if handoff["stage"] == "FAILED_CLOSED":
                raise EffectOwnershipLost("handoff became fail-closed before commit")
            checkpoint = str(handoff["checkpoint_stage"])
            if (
                handoff["stage"] != checkpoint
                or _STAGE_INDEX[target_stage] != _STAGE_INDEX[checkpoint] + 1
            ):
                raise EffectOwnershipLost(
                    "effect completion would skip or regress a stage"
                )
            assignments = [
                "stage=?",
                "checkpoint_stage=?",
                "failure_reason=NULL",
                "manual_resume_action=NULL",
                "updated_at=?",
            ]
            params: list[Any] = [target_stage, target_stage, now]
            if receipt_field is not None:
                assignments.append(f"{receipt_field}=?")
                params.append(receipt)
            params.extend((job_id, handoff_id))
            conn.execute(
                f"UPDATE session_handoffs SET {', '.join(assignments)} "
                "WHERE job_id=? AND handoff_id=?",
                params,
            )
            conn.execute(
                """
                UPDATE session_handoff_effects
                   SET status='APPLIED', owner_token=NULL, updated_at=?
                 WHERE job_id=? AND handoff_id=? AND effect_name=?
                """,
                (now, job_id, handoff_id, effect_name),
            )
        return self.get(job_id, handoff_id)  # type: ignore[return-value]

    def reconcile_effect(
        self,
        *,
        job_id: str,
        handoff_id: str,
        effect_name: str,
        outcome: str,
        receipt: str | None = None,
        expected_owner_token: str,
        expected_generation: int,
        dead_owner_verified: bool,
        owner_guard: _EffectOwnerGuard,
    ) -> HandoffState:
        """Resolve an ambiguous effect only after an operator verifies its outcome."""
        if effect_name not in _EFFECT_TARGETS:
            raise ValueError("unsupported handoff effect")
        if outcome not in {"APPLIED", "NOT_APPLIED"}:
            raise ValueError("effect outcome must be APPLIED or NOT_APPLIED")
        if dead_owner_verified is not True:
            raise EffectOwnershipLost(
                "reconciliation requires an explicit dead-owner witness"
            )
        if not expected_owner_token or expected_generation < 1:
            raise ValueError("exact effect owner and generation are required")
        if (
            not owner_guard.held
            or owner_guard.job_id != job_id
            or owner_guard.handoff_id != handoff_id
            or owner_guard.effect_name != effect_name
        ):
            raise EffectOwnershipLost(
                "reconciliation requires an acquired effect-owner guard"
            )
        target_stage, receipt_field = _EFFECT_TARGETS[effect_name]
        if outcome == "APPLIED" and not receipt:
            raise ProjectionVerificationError(
                "verified applied effect requires reconciliation evidence"
            )
        if outcome == "NOT_APPLIED" and receipt is not None:
            raise ValueError("receipt is not valid for this reconciliation outcome")
        if receipt is not None:
            _validate_receipt(receipt)

        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            claim = conn.execute(
                """
                SELECT status, owner_token, generation
                  FROM session_handoff_effects
                 WHERE job_id=? AND handoff_id=? AND effect_name=?
                """,
                (job_id, handoff_id, effect_name),
            ).fetchone()
            if (
                claim is None
                or claim["status"] != "IN_FLIGHT"
                or claim["owner_token"] != expected_owner_token
                or int(claim["generation"]) != expected_generation
            ):
                raise EffectOwnershipLost(
                    "effect does not match the witnessed in-flight claim"
                )
            handoff = conn.execute(
                """
                SELECT stage, checkpoint_stage
                  FROM session_handoffs
                 WHERE job_id=? AND handoff_id=?
                """,
                (job_id, handoff_id),
            ).fetchone()
            if handoff is None:
                raise KeyError(handoff_id)

            if outcome == "NOT_APPLIED":
                conn.execute(
                    """
                    UPDATE session_handoff_effects
                       SET status='RETRYABLE', owner_token=NULL, updated_at=?
                     WHERE job_id=? AND handoff_id=? AND effect_name=?
                    """,
                    (now, job_id, handoff_id, effect_name),
                )
            else:
                checkpoint = str(handoff["checkpoint_stage"])
                if _STAGE_INDEX[target_stage] != _STAGE_INDEX[checkpoint] + 1:
                    raise EffectOwnershipLost(
                        "reconciliation would skip or regress a stage"
                    )
                assignments = ["checkpoint_stage=?", "updated_at=?"]
                params: list[Any] = [target_stage, now]
                if handoff["stage"] != "FAILED_CLOSED":
                    assignments.append("stage=?")
                    params.append(target_stage)
                if receipt_field is not None:
                    assignments.append(f"{receipt_field}=?")
                    params.append(receipt)
                params.extend((job_id, handoff_id))
                conn.execute(
                    f"UPDATE session_handoffs SET {', '.join(assignments)} "
                    "WHERE job_id=? AND handoff_id=?",
                    params,
                )
                conn.execute(
                    """
                    UPDATE session_handoff_effects
                       SET status='APPLIED', owner_token=NULL,
                           reconciliation_receipt=?, updated_at=?
                     WHERE job_id=? AND handoff_id=? AND effect_name=?
                    """,
                    (receipt, now, job_id, handoff_id, effect_name),
                )
        return self.get(job_id, handoff_id)  # type: ignore[return-value]

    def get(self, job_id: str, handoff_id: str) -> HandoffState | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM session_handoffs WHERE job_id=? AND handoff_id=?",
                (job_id, handoff_id),
            ).fetchone()
        if row is None:
            return None
        return HandoffState(
            job_id=row["job_id"],
            handoff_id=row["handoff_id"],
            stage=row["stage"],
            checkpoint_stage=row["checkpoint_stage"],
            canonical_hash=row["canonical_hash"],
            linear_receipt=row["linear_receipt"],
            slack_receipt=row["slack_receipt"],
            child_session_id=row["child_session_id"],
            failure_reason=row["failure_reason"],
            manual_resume_action=row["manual_resume_action"],
        )

    def advance(
        self, job_id: str, handoff_id: str, stage: str, **values: Any
    ) -> HandoffState:
        if stage != "COMPLETE" or values:
            raise ValueError(
                "effect stages require complete_effect or reconcile_effect"
            )
        assignments = [
            "stage=?",
            "checkpoint_stage=?",
            "failure_reason=NULL",
            "manual_resume_action=NULL",
            "updated_at=?",
        ]
        params: list[Any] = [
            stage,
            stage,
            datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        ]
        eligible = ("FIRST_TURN_STARTED", "COMPLETE")
        params.extend((job_id, handoff_id, *eligible))
        with self._connect() as conn:
            cursor = conn.execute(
                f"UPDATE session_handoffs SET {', '.join(assignments)} "
                f"WHERE job_id=? AND handoff_id=? "
                "AND stage <> 'FAILED_CLOSED' "
                f"AND checkpoint_stage IN ({','.join('?' for _ in eligible)})",
                params,
            )
            if cursor.rowcount == 0:
                exists = conn.execute(
                    "SELECT 1 FROM session_handoffs WHERE job_id=? AND handoff_id=?",
                    (job_id, handoff_id),
                ).fetchone()
                if exists is None:
                    raise KeyError(handoff_id)
        return self.get(job_id, handoff_id)  # type: ignore[return-value]

    def resume_failed(self, job_id: str, handoff_id: str) -> HandoffState:
        """Explicitly release the persisted fail-closed fence for manual resume."""
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            unresolved = conn.execute(
                """
                SELECT effect_name
                  FROM session_handoff_effects
                 WHERE job_id=? AND handoff_id=? AND status='IN_FLIGHT'
                 LIMIT 1
                """,
                (job_id, handoff_id),
            ).fetchone()
            if unresolved is not None:
                raise EffectReconciliationRequired(
                    f"effect {unresolved['effect_name']} requires explicit reconciliation"
                )
            cursor = conn.execute(
                """
                UPDATE session_handoffs
                   SET stage=checkpoint_stage, failure_reason=NULL,
                       manual_resume_action=NULL, updated_at=?
                 WHERE job_id=? AND handoff_id=? AND stage='FAILED_CLOSED'
                """,
                (
                    datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                    job_id,
                    handoff_id,
                ),
            )
            if cursor.rowcount == 0:
                exists = conn.execute(
                    "SELECT 1 FROM session_handoffs WHERE job_id=? AND handoff_id=?",
                    (job_id, handoff_id),
                ).fetchone()
                if exists is None:
                    raise KeyError(handoff_id)
        return self.get(job_id, handoff_id)  # type: ignore[return-value]

    def fail_closed(
        self,
        job_id: str,
        handoff_id: str,
        reason: str,
        *,
        effect_name: str | None = None,
        expected_owner_token: str | None = None,
        expected_generation: int | None = None,
    ) -> HandoffState:
        # Persist only exception type, never raw provider text/prompt/reasoning/secrets.
        safe_reason = reason[:128]
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if effect_name is not None:
                claim = conn.execute(
                    """
                    SELECT status, owner_token, generation
                      FROM session_handoff_effects
                     WHERE job_id=? AND handoff_id=? AND effect_name=?
                    """,
                    (job_id, handoff_id, effect_name),
                ).fetchone()
                if (
                    claim is None
                    or claim["status"] != "IN_FLIGHT"
                    or claim["owner_token"] != expected_owner_token
                    or int(claim["generation"]) != expected_generation
                ):
                    return self.get(job_id, handoff_id)  # type: ignore[return-value]
            elif (
                conn.execute(
                    """
                SELECT 1 FROM session_handoff_effects
                 WHERE job_id=? AND handoff_id=? AND status='IN_FLIGHT' LIMIT 1
                """,
                    (job_id, handoff_id),
                ).fetchone()
                is not None
            ):
                return self.get(job_id, handoff_id)  # type: ignore[return-value]
            conn.execute(
                """
                UPDATE session_handoffs
                   SET stage='FAILED_CLOSED', failure_reason=?, manual_resume_action=?, updated_at=?
                 WHERE job_id=? AND handoff_id=? AND checkpoint_stage <> 'COMPLETE'
                """,
                (
                    safe_reason,
                    _MANUAL_RESUME,
                    datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                    job_id,
                    handoff_id,
                ),
            )
        return self.get(job_id, handoff_id)  # type: ignore[return-value]
