"""Deterministic, offline-only ENG-118 legacy adoption ledger.

This module inventories a caller-attested frozen SQLite snapshot.  It never
creates schema-9 jobs or replayable effects; every source row is preserved in
an immutable provenance ledger and unsafe state is explicitly quarantined.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote

FORMAT_VERSION = 1
_UNSAFE_STATUSES = frozenset({"running", "unknown"})
_LOCK_TABLES = frozenset({"compression_locks", "session_turn_leases"})

_LEGACY_TABLE_ALLOWLIST = frozenset({"async_delegations", "compression_locks", "delivery_obligations", "gateway_hygiene_state", "gateway_routing", "messages", "schema_version", "session_model_usage", "session_turn_leases", "sessions", "state_meta", "system_prompts"})
_SECRET_COLUMN_FRAGMENTS = ("token", "_token", "api_key", "password", "secret", "credential", "private_key", "dsn")
_EXPORT_COLUMNS = {
    'async_delegations': frozenset(['payload', 'session_id', 'status', 'delegation_id', 'origin_session', 'origin_ui_session_id', 'parent_session_id', 'state', 'dispatched_at', 'completed_at', 'updated_at', 'event_json', 'result_json', 'delivery_state', 'delivery_attempts', 'delivered_at', 'owner_pid', 'owner_started_at', 'task_json', 'delivery_claim', 'delivery_claimed_at', 'origin_session_id']),
    'compression_locks': frozenset(['session_id', 'holder', 'acquired_at', 'expires_at']),
    'delivery_obligations': frozenset(['obligation_id', 'session_key', 'platform', 'chat_id', 'thread_id', 'content', 'state', 'attempts', 'created_at', 'updated_at', 'owner_pid', 'owner_started_at', 'last_error']),
    'gateway_hygiene_state': frozenset(['session_key', 'failure_streak']),
    'gateway_routing': frozenset(['scope', 'session_key', 'entry_json', 'updated_at']),
    'messages': frozenset(['body', 'id', 'session_id', 'role', 'content', 'tool_call_id', 'tool_calls', 'tool_name', 'timestamp', 'token_count', 'finish_reason', 'reasoning', 'reasoning_content', 'reasoning_details', 'codex_reasoning_items', 'codex_message_items', 'platform_message_id', 'observed', 'active', 'compacted', 'effect_disposition', 'api_content', 'display_kind', 'display_metadata']),
    'schema_version': frozenset(['version']),
    'session_model_usage': frozenset(['session_id', 'model', 'billing_provider', 'billing_base_url', 'billing_mode', 'task', 'api_call_count', 'input_tokens', 'output_tokens', 'cache_read_tokens', 'cache_write_tokens', 'reasoning_tokens', 'estimated_cost_usd', 'actual_cost_usd', 'cost_status', 'cost_source', 'first_seen', 'last_seen']),
    'session_turn_leases': frozenset(['conversation_id', 'holder', 'acquired_at', 'expires_at']),
    'sessions': frozenset(['id', 'source', 'user_id', 'session_key', 'chat_id', 'chat_type', 'thread_id', 'display_name', 'origin_json', 'expiry_finalized', 'model', 'model_config', 'system_prompt', 'parent_session_id', 'started_at', 'ended_at', 'end_reason', 'message_count', 'tool_call_count', 'input_tokens', 'output_tokens', 'cache_read_tokens', 'cache_write_tokens', 'reasoning_tokens', 'cwd', 'git_branch', 'git_repo_root', 'billing_provider', 'billing_base_url', 'billing_mode', 'estimated_cost_usd', 'actual_cost_usd', 'cost_status', 'cost_source', 'pricing_version', 'title', 'api_call_count', 'handoff_state', 'handoff_platform', 'handoff_error', 'compression_failure_cooldown_until', 'compression_failure_error', 'rewind_count', 'archived', 'compression_fallback_streak', 'profile_name', 'pinned', 'compression_ineffective_count', 'title_source', 'title_meta', 'system_prompt_hash', 'last_activity_at', 'last_activity_description', 'last_activity_provenance', 'last_read_at', 'git_metadata_generation', 'hidden']),
    'state_meta': frozenset(['key', 'value']),
    'system_prompts': frozenset(['hash', 'prompt']),
}


class LegacyMigrationError(RuntimeError):
    """Fail-closed offline migration contract violation."""


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha(value: str | bytes) -> str:
    data = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha256(data).hexdigest()


def _normalize(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"$bytes_sha256": _sha(value), "$length": len(value)}
    if isinstance(value, str) and value[:1] in {"{", "["}:
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            pass
    return value



_CREDENTIAL_TOKEN_RE = re.compile(
    r"(?i)\b(?:sk-[A-Za-z0-9_-]{8,}|gh[pousr]_[A-Za-z0-9]{8,}|xox[baprs]-[A-Za-z0-9-]{8,})\b"
)
_CREDENTIAL_ASSIGNMENT_RE = re.compile(
    r"(?i)((?:api[_-]?key|access[_-]?token|refresh[_-]?token|password|secret|authorization)\s*[:=]\s*(?:bearer\s+)?)([^\s\"',;}]{8,})"
)
_PRIVATE_KEY_RE = re.compile(
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.DOTALL
)

def _redacted_marker(value: str) -> str:
    return f"<redacted:sha256={_sha(value)}>"

def _redact_free_text(value: str) -> str:
    value = _PRIVATE_KEY_RE.sub(lambda match: _redacted_marker(match.group(0)), value)
    value = _CREDENTIAL_TOKEN_RE.sub(lambda match: _redacted_marker(match.group(0)), value)
    return _CREDENTIAL_ASSIGNMENT_RE.sub(
        lambda match: match.group(1) + _redacted_marker(match.group(2)), value
    )


_SECRET_JSON_KEYS = frozenset(
    {
        "authorization",
        "accesstoken",
        "refreshtoken",
        "privatekey",
        "clientsecret",
        "apikey",
        "password",
        "secret",
        "token",
        "credential",
        "dsn",
    }
)


def _is_secret_json_key(key: object) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
    return normalized in _SECRET_JSON_KEYS or any(
        normalized.endswith(suffix)
        for suffix in (
            "accesstoken",
            "refreshtoken",
            "privatekey",
            "clientsecret",
            "apikey",
            "password",
        )
    )


def _redact_structured_secret(value: Any) -> str:
    return _redacted_marker(_json(_normalize(value)))


def _sanitize_export_value(value: Any) -> Any:
    if isinstance(value, str):
        return _redact_free_text(value)
    if isinstance(value, list):
        return [_sanitize_export_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): (
                _redact_structured_secret(item)
                if _is_secret_json_key(key)
                else _sanitize_export_value(item)
            )
            for key, item in value.items()
        }
    return value

@dataclass(frozen=True)
class FrozenSQLiteSnapshot:
    path: Path
    file_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", Path(self.path).resolve())
        if len(self.file_sha256) != 64:
            raise LegacyMigrationError("snapshot SHA-256 must contain 64 hex characters")
        try:
            int(self.file_sha256, 16)
        except ValueError as exc:
            raise LegacyMigrationError("snapshot SHA-256 is not hexadecimal") from exc


@dataclass(frozen=True)
class AdoptionEntry:
    migration_key: str
    source_table: str
    source_pk_json: str
    row_sha256: str
    canonical_row_json: str
    target_kind: str


@dataclass(frozen=True)
class ReconciliationBlocker:
    migration_key: str
    reason: str


@dataclass(frozen=True)
class AdoptionPlan:
    snapshot_sha256: str
    population_sha256: str
    table_sha256: Mapping[str, str]
    table_counts: Mapping[str, int]
    entries: tuple[AdoptionEntry, ...]
    blockers: tuple[ReconciliationBlocker, ...]
    source_snapshot: FrozenSQLiteSnapshot

    def manifest_json(self) -> str:
        payload = {
            "format_version": FORMAT_VERSION,
            "snapshot_sha256": self.snapshot_sha256,
            "population_sha256": self.population_sha256,
            "tables": [
                {
                    "name": name,
                    "row_count": self.table_counts[name],
                    "population_sha256": self.table_sha256[name],
                }
                for name in sorted(self.table_sha256)
            ],
            "entries": [entry.__dict__ for entry in self.entries],
        }
        return _json(payload)

    def reconciliation_json(self) -> str:
        return _json(
            {
                "format_version": FORMAT_VERSION,
                "blocking": bool(self.blockers),
                "items": [item.__dict__ for item in self.blockers],
            }
        )

    def with_replaced_row_sha(self, migration_key: str, row_sha256: str) -> AdoptionPlan:
        entries = tuple(
            replace(entry, row_sha256=row_sha256)
            if entry.migration_key == migration_key
            else entry
            for entry in self.entries
        )
        return replace(self, entries=entries)


@dataclass(frozen=True)
class ApplyResult:
    total_count: int
    inserted_count: int
    duplicate_count: int


@dataclass(frozen=True)
class VerificationResult:
    verified: bool
    expected_count: int
    actual_count: int


def _snapshot_digest(snapshot: FrozenSQLiteSnapshot) -> str:
    if not snapshot.path.is_file():
        raise LegacyMigrationError("frozen snapshot does not exist")
    for suffix in ("-wal", "-shm", "-journal"):
        if Path(str(snapshot.path) + suffix).exists():
            raise LegacyMigrationError("snapshot has mutable SQLite sidecar files")
    return _sha(snapshot.path.read_bytes())


def _open_read_only(snapshot: FrozenSQLiteSnapshot) -> sqlite3.Connection:
    before = _snapshot_digest(snapshot)
    if before != snapshot.file_sha256.lower():
        raise LegacyMigrationError("snapshot digest does not match frozen attestation")
    uri = f"file:{quote(snapshot.path.as_posix())}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def _table_names(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT name
        FROM pragma_table_list
        WHERE schema = 'main' AND type = 'table' AND name NOT LIKE 'sqlite_%'
        """
    ).fetchall()
    return sorted(str(row[0]) for row in rows if str(row[0]) in _LEGACY_TABLE_ALLOWLIST)


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _pk_columns(conn: sqlite3.Connection, table: str) -> tuple[str, ...]:
    rows = conn.execute(f"PRAGMA table_info({_quoted(table)})").fetchall()
    primary = sorted(
        ((int(row[5]), str(row[1])) for row in rows if int(row[5]) > 0),
        key=lambda item: item[0],
    )
    return tuple(name for _, name in primary)


def _blocker_reasons(
    *, table: str, row: Mapping[str, Any], session_ids: frozenset[str]
) -> tuple[str, ...]:
    reasons: list[str] = []
    status = str(row.get("status") or "").lower()
    if status in _UNSAFE_STATUSES:
        reasons.append(f"unsafe_status:{status}")
    if table in _LOCK_TABLES:
        reasons.append(f"unresolved_{table.removesuffix('s')}")
    session_ref = row.get("session_id")
    if table != "sessions" and session_ref and str(session_ref) not in session_ids:
        reasons.append("missing_session_reference")
    if table == "sessions":
        parent = row.get("parent_session_id")
        if parent and str(parent) not in session_ids:
            reasons.append("missing_parent_session_reference")
    return tuple(reasons)


def plan_legacy_adoption(snapshot: FrozenSQLiteSnapshot) -> AdoptionPlan:
    """Inventory a frozen snapshot without writing it or fabricating target jobs."""

    conn = _open_read_only(snapshot)
    try:
        tables = _table_names(conn)
        session_ids = frozenset()
        if "sessions" in tables:
            session_ids = frozenset(
                str(row[0]) for row in conn.execute("SELECT id FROM sessions").fetchall()
            )
        entries: list[AdoptionEntry] = []
        blockers: list[ReconciliationBlocker] = []
        table_hashes: dict[str, str] = {}
        table_counts: dict[str, int] = {}
        for table in tables:
            columns = {
                str(row[1])
                for row in conn.execute(
                    f"PRAGMA table_info({_quoted(table)})"
                ).fetchall()
            }
            allowed_columns = _EXPORT_COLUMNS[table]
            unknown_columns = sorted(columns - allowed_columns)
            if unknown_columns:
                raise LegacyMigrationError(
                    f"columns are not export-allowlisted: {table}.{','.join(unknown_columns)}"
                )
            forbidden = sorted(
                column for column in columns
                if any(
                    (fragment == column.lower() if fragment == "token" else fragment in column.lower())
                    for fragment in _SECRET_COLUMN_FRAGMENTS
                )
            )
            if forbidden:
                raise LegacyMigrationError(
                    f"credential-bearing columns are not migratable: {table}.{','.join(forbidden)}"
                )
            primary = _pk_columns(conn, table)
            projection = "*"
            if not primary:
                if "$rowid" in columns:
                    raise LegacyMigrationError(
                        f"source table {table!r} has ambiguous $rowid column"
                    )
                primary = ("$rowid",)
                projection = 'rowid AS "$rowid", *'
            rows = conn.execute(
                f"SELECT {projection} FROM {_quoted(table)}"
            ).fetchall()
            table_entries: list[AdoptionEntry] = []
            for raw in rows:
                row = {key: _normalize(raw[key]) for key in sorted(raw.keys())}
                pk = {key: row[key] for key in primary}
                sanitized_row = {
                    str(name): _sanitize_export_value(value) for name, value in row.items()
                }
                canonical = _json(sanitized_row)
                key = _sha(_json({"source_table": table, "primary_key": pk}))
                reasons = _blocker_reasons(table=table, row=row, session_ids=session_ids)
                entry = AdoptionEntry(
                    migration_key=key,
                    source_table=table,
                    source_pk_json=_json(pk),
                    row_sha256=_sha(canonical),
                    canonical_row_json=canonical,
                    target_kind="quarantine" if reasons else "adoption_ledger",
                )
                table_entries.append(entry)
                blockers.extend(ReconciliationBlocker(key, reason) for reason in reasons)
            table_entries.sort(key=lambda item: item.migration_key)
            entries.extend(table_entries)
            table_counts[table] = len(table_entries)
            table_hashes[table] = _sha(_json([item.row_sha256 for item in table_entries]))
        entries.sort(key=lambda item: (item.source_table, item.migration_key))
        blockers.sort(key=lambda item: (item.migration_key, item.reason))
        population = _sha(_json({name: table_hashes[name] for name in sorted(table_hashes)}))
    finally:
        conn.close()
    if _snapshot_digest(snapshot) != snapshot.file_sha256.lower():
        raise LegacyMigrationError("snapshot changed during read-only inventory")
    return AdoptionPlan(
        snapshot_sha256=snapshot.file_sha256.lower(),
        population_sha256=population,
        table_sha256=table_hashes,
        table_counts=table_counts,
        entries=tuple(entries),
        blockers=tuple(blockers),
        source_snapshot=snapshot,
    )


def _plan_matches_source_snapshot(plan: AdoptionPlan) -> bool:
    """Re-inventory source evidence instead of trusting plan-owned hashes."""

    try:
        expected = plan_legacy_adoption(plan.source_snapshot)
    except (OSError, sqlite3.Error, LegacyMigrationError):
        return False
    return (
        plan.snapshot_sha256 == expected.snapshot_sha256
        and plan.population_sha256 == expected.population_sha256
        and plan.table_sha256 == expected.table_sha256
        and plan.table_counts == expected.table_counts
        and plan.entries == expected.entries
        and plan.blockers == expected.blockers
    )


def _ensure_ledger(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS eng118_adoption_ledger(
        migration_key TEXT PRIMARY KEY,
        source_table TEXT NOT NULL,
        source_pk_json TEXT NOT NULL,
        row_sha256 TEXT NOT NULL,
        canonical_row_json TEXT NOT NULL,
        target_kind TEXT NOT NULL,
        disposition TEXT,
        snapshot_sha256 TEXT NOT NULL,
        population_sha256 TEXT NOT NULL
        )"""
    )
    conn.execute(
        """CREATE TRIGGER IF NOT EXISTS eng118_adoption_ledger_no_update
        BEFORE UPDATE ON eng118_adoption_ledger
        BEGIN SELECT RAISE(ABORT, 'ENG-118 adoption ledger is immutable'); END"""
    )
    conn.execute(
        """CREATE TRIGGER IF NOT EXISTS eng118_adoption_ledger_no_delete
        BEFORE DELETE ON eng118_adoption_ledger
        BEGIN SELECT RAISE(ABORT, 'ENG-118 adoption ledger is immutable'); END"""
    )


def apply_legacy_adoption(
    plan: AdoptionPlan,
    ledger_path: Path,
    *,
    dispositions: Mapping[str, str],
) -> ApplyResult:
    """Append immutable rows; exact duplicates are no-ops, divergence is fatal."""

    if not _plan_matches_source_snapshot(plan):
        raise LegacyMigrationError(
            "divergent adoption plan does not match independently recomputed source snapshot"
        )
    unresolved = {
        item.migration_key
        for item in plan.blockers
        if not str(dispositions.get(item.migration_key) or "").strip()
    }
    if unresolved:
        raise LegacyMigrationError("explicit disposition required for every blocker")
    conn = sqlite3.connect(Path(ledger_path))
    inserted = duplicates = 0
    try:
        conn.execute("BEGIN IMMEDIATE")
        _ensure_ledger(conn)
        for entry in plan.entries:
            existing = conn.execute(
                "SELECT source_table, source_pk_json, row_sha256, canonical_row_json, "
                "target_kind, disposition, snapshot_sha256, population_sha256 "
                "FROM eng118_adoption_ledger WHERE migration_key = ?",
                (entry.migration_key,),
            ).fetchone()
            disposition = dispositions.get(entry.migration_key)
            intended = (
                entry.source_table,
                entry.source_pk_json,
                entry.row_sha256,
                entry.canonical_row_json,
                entry.target_kind,
                disposition,
                plan.snapshot_sha256,
                plan.population_sha256,
            )
            if existing is not None:
                if tuple(existing) != intended:
                    raise LegacyMigrationError(
                        "divergent payload for existing migration key"
                    )
                duplicates += 1
                continue
            conn.execute(
                "INSERT INTO eng118_adoption_ledger VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (entry.migration_key, *intended),
            )
            inserted += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return ApplyResult(len(plan.entries), inserted, duplicates)


def verify_legacy_adoption(
    plan: AdoptionPlan,
    ledger_path: Path,
    *,
    dispositions: Mapping[str, str],
) -> VerificationResult:
    if not _plan_matches_source_snapshot(plan):
        return VerificationResult(False, len(plan.entries), 0)
    path = Path(ledger_path)
    if not path.is_file():
        return VerificationResult(False, len(plan.entries), 0)
    blocker_keys = {item.migration_key for item in plan.blockers}
    if any(not str(dispositions.get(key) or "").strip() for key in blocker_keys):
        return VerificationResult(False, len(plan.entries), 0)
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
        try:
            rows = conn.execute(
                "SELECT migration_key, source_table, source_pk_json, row_sha256, "
                "canonical_row_json, target_kind, disposition, snapshot_sha256, "
                "population_sha256 FROM eng118_adoption_ledger ORDER BY migration_key"
            ).fetchall()
        finally:
            conn.close()
    except sqlite3.Error:
        return VerificationResult(False, len(plan.entries), 0)
    actual = {key: tuple(values) for key, *values in rows}
    expected = {
        item.migration_key: (
            item.source_table,
            item.source_pk_json,
            item.row_sha256,
            item.canonical_row_json,
            item.target_kind,
            dispositions.get(item.migration_key),
            plan.snapshot_sha256,
            plan.population_sha256,
        )
        for item in plan.entries
    }
    return VerificationResult(actual == expected, len(expected), len(actual))
