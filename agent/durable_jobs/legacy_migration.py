"""Deterministic, offline-only ENG-118 legacy adoption ledger.

This module inventories a caller-attested frozen SQLite snapshot.  It never
creates schema-9 jobs or replayable effects; every source row is preserved in
an immutable provenance ledger and unsafe state is explicitly quarantined.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote

FORMAT_VERSION = 1
_UNSAFE_STATUSES = frozenset({"running", "unknown"})
_LOCK_TABLES = frozenset({"compression_locks", "session_turn_leases"})

_LEGACY_TABLE_ALLOWLIST = frozenset({"async_delegations", "compression_locks", "delivery_obligations", "gateway_hygiene_state", "gateway_routing", "messages", "schema_version", "session_model_usage", "session_turn_leases", "sessions", "state_meta", "system_prompts"})
_SECRET_COLUMN_FRAGMENTS = ("token", "api_key", "password", "secret", "credential", "private_key", "dsn")


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
            forbidden = sorted(
                column for column in columns
                if any(fragment in column.lower() for fragment in _SECRET_COLUMN_FRAGMENTS)
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
                canonical = _json(row)
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
