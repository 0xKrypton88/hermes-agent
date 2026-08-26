"""Disposable-only acceptance APIs for ENG-118 and ENG-122.

Nothing in this module is wired into a runtime, gateway, or production store.
Targets must be newly initialized beneath a caller-declared disposable root.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Mapping

from agent.durable_jobs.legacy_migration import (
    AdoptionPlan,
    FrozenSQLiteSnapshot,
    LegacyMigrationError,
    verify_legacy_adoption,
)


class OfflineAcceptanceError(RuntimeError):
    """A disposable-only acceptance boundary was not satisfied."""


@dataclass(frozen=True)
class MaterializationResult:
    batch_sha256: str
    total_count: int
    inserted_count: int
    duplicate_count: int


@dataclass(frozen=True)
class MaterializationReadback:
    verified: bool
    batch_sha256: str
    expected_count: int
    actual_count: int


@dataclass(frozen=True)
class RollbackResult:
    batch_sha256: str
    removed_count: int


_DDL = """
CREATE TABLE eng118_disposable_target (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  format_version INTEGER NOT NULL CHECK(format_version=1),
  live_effects INTEGER NOT NULL CHECK(live_effects=0),
  root_sha256 TEXT NOT NULL
);
CREATE TABLE eng118_adopted_sessions (
  migration_key TEXT PRIMARY KEY, source_pk_json TEXT NOT NULL,
  row_sha256 TEXT NOT NULL, canonical_row_json TEXT NOT NULL,
  batch_sha256 TEXT NOT NULL
);
CREATE TABLE eng118_adopted_messages (
  migration_key TEXT PRIMARY KEY, source_pk_json TEXT NOT NULL,
  row_sha256 TEXT NOT NULL, canonical_row_json TEXT NOT NULL,
  batch_sha256 TEXT NOT NULL
);
CREATE TABLE eng118_materialization_journal (
  batch_sha256 TEXT NOT NULL, migration_key TEXT NOT NULL,
  target_table TEXT NOT NULL, inserted INTEGER NOT NULL CHECK(inserted IN (0,1)),
  PRIMARY KEY(batch_sha256,migration_key)
);
"""

_TARGET_TABLES = {
    "sessions": "eng118_adopted_sessions",
    "messages": "eng118_adopted_messages",
}


def _sha(data: str | bytes) -> str:
    return hashlib.sha256(data.encode() if isinstance(data, str) else data).hexdigest()


def _resolved_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def initialize_disposable_application(path: Path, *, disposable_root: Path) -> None:
    """Create a fresh, offline-only application target below ``disposable_root``."""
    target = Path(path).resolve()
    root = Path(disposable_root).resolve()
    if not root.is_dir() or not _resolved_within(target, root) or target.exists():
        raise OfflineAcceptanceError(
            "target must be a new file beneath an existing disposable root"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(target)
    try:
        connection.executescript(_DDL)
        connection.execute(
            "INSERT INTO eng118_disposable_target VALUES(1,1,0,?)",
            (_sha(str(root)),),
        )
        connection.commit()
    except Exception:
        connection.close()
        target.unlink(missing_ok=True)
        raise
    finally:
        connection.close()


def _connect_target(path: Path, root: Path) -> sqlite3.Connection:
    target = Path(path).resolve()
    root = Path(root).resolve()
    if not _resolved_within(target, root):
        raise OfflineAcceptanceError("application target is outside disposable root")
    connection = sqlite3.connect(target)
    connection.row_factory = sqlite3.Row
    try:
        marker = connection.execute(
            "SELECT format_version,live_effects,root_sha256 FROM eng118_disposable_target "
            "WHERE singleton=1"
        ).fetchone()
    except sqlite3.Error as exc:
        connection.close()
        raise OfflineAcceptanceError("application target is not disposable") from exc
    if marker is None or tuple(marker) != (1, 0, _sha(str(root))):
        connection.close()
        raise OfflineAcceptanceError("disposable target marker does not match root")
    return connection


def _verified_ledger(
    plan: AdoptionPlan,
    ledger_path: Path,
    dispositions: Mapping[str, str],
    expected_source_snapshot: FrozenSQLiteSnapshot,
) -> None:
    verification = verify_legacy_adoption(
        plan,
        ledger_path,
        dispositions=dispositions,
        expected_source_snapshot=expected_source_snapshot,
    )
    if not verification.verified:
        raise LegacyMigrationError("immutable adoption ledger failed readback")


def materialize_disposable_adoption(
    plan: AdoptionPlan,
    ledger_path: Path,
    application_path: Path,
    *,
    disposable_root: Path,
    dispositions: Mapping[str, str],
    expected_source_snapshot: FrozenSQLiteSnapshot,
) -> MaterializationResult:
    """Materialize supported ledger rows atomically into disposable tables."""
    _verified_ledger(plan, ledger_path, dispositions, expected_source_snapshot)
    supported = tuple(entry for entry in plan.entries if entry.source_table in _TARGET_TABLES)
    batch = _sha(json.dumps(
        [entry.migration_key for entry in supported], separators=(",", ":")
    ))
    connection = _connect_target(application_path, disposable_root)
    inserted = duplicates = 0
    try:
        connection.execute("BEGIN IMMEDIATE")
        for entry in supported:
            table = _TARGET_TABLES[entry.source_table]
            existing = connection.execute(
                f"SELECT source_pk_json,row_sha256,canonical_row_json,batch_sha256 FROM {table} "
                "WHERE migration_key=?", (entry.migration_key,),
            ).fetchone()
            expected = (
                entry.source_pk_json, entry.row_sha256, entry.canonical_row_json, batch
            )
            if existing is None:
                connection.execute(
                    f"INSERT INTO {table} VALUES(?,?,?,?,?)",
                    (entry.migration_key, *expected),
                )
                inserted += 1
                was_inserted = 1
            elif tuple(existing) == expected:
                duplicates += 1
                was_inserted = 0
            else:
                raise OfflineAcceptanceError("materialized identity diverges from ledger")
            prior = connection.execute(
                "SELECT target_table FROM eng118_materialization_journal "
                "WHERE batch_sha256=? AND migration_key=?", (batch, entry.migration_key),
            ).fetchone()
            if prior is not None and prior[0] != table:
                raise OfflineAcceptanceError("materialization journal diverges")
            connection.execute(
                "INSERT INTO eng118_materialization_journal VALUES(?,?,?,?) "
                "ON CONFLICT(batch_sha256,migration_key) DO NOTHING",
                (batch, entry.migration_key, table, was_inserted),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return MaterializationResult(batch, len(supported), inserted, duplicates)


def readback_disposable_adoption(
    plan: AdoptionPlan, application_path: Path, *, disposable_root: Path
) -> MaterializationReadback:
    supported = tuple(entry for entry in plan.entries if entry.source_table in _TARGET_TABLES)
    batch = _sha(json.dumps(
        [entry.migration_key for entry in supported], separators=(",", ":")
    ))
    connection = _connect_target(application_path, disposable_root)
    actual = 0
    verified = True
    try:
        for entry in supported:
            table = _TARGET_TABLES[entry.source_table]
            row = connection.execute(
                f"SELECT source_pk_json,row_sha256,canonical_row_json,batch_sha256 FROM {table} "
                "WHERE migration_key=?", (entry.migration_key,),
            ).fetchone()
            expected = (entry.source_pk_json, entry.row_sha256, entry.canonical_row_json, batch)
            if row is not None:
                actual += 1
            verified = verified and row is not None and tuple(row) == expected
    finally:
        connection.close()
    return MaterializationReadback(verified and actual == len(supported), batch, len(supported), actual)


def rollback_disposable_adoption(
    application_path: Path, *, disposable_root: Path, batch_sha256: str
) -> RollbackResult:
    """Remove only rows first inserted by one materialization batch."""
    connection = _connect_target(application_path, disposable_root)
    removed = 0
    try:
        connection.execute("BEGIN IMMEDIATE")
        rows = connection.execute(
            "SELECT migration_key,target_table,inserted FROM eng118_materialization_journal "
            "WHERE batch_sha256=? ORDER BY migration_key", (batch_sha256,),
        ).fetchall()
        if not rows:
            raise OfflineAcceptanceError("unknown materialization batch")
        for row in rows:
            if row["target_table"] not in _TARGET_TABLES.values():
                raise OfflineAcceptanceError("rollback journal target is invalid")
            if row["inserted"]:
                removed += connection.execute(
                    f"DELETE FROM {row['target_table']} WHERE migration_key=? AND batch_sha256=?",
                    (row["migration_key"], batch_sha256),
                ).rowcount
        connection.execute(
            "DELETE FROM eng118_materialization_journal WHERE batch_sha256=?",
            (batch_sha256,),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return RollbackResult(batch_sha256, removed)
