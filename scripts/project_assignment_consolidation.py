#!/usr/bin/env python3
"""Dry-run consolidation report for session→project assignments.

The script never mutates data. It inspects a state DB and projects DB and computes
what assignment rows are stale with respect to canonical compression roots.

Usage:
  python scripts/project_assignment_consolidation.py
  python scripts/project_assignment_consolidation.py --state-db /tmp/state.db --projects-db /tmp/projects.db
  python scripts/project_assignment_consolidation.py --json

The output is intentionally conservative:
- only assignments not already keyed to the compression-root are proposed for cleanup,
- moves are only proposed when the root has no assignment row,
- conflicts are reported when both child and root rows exist with different effective
  assignments.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


@dataclass(frozen=True)
class Assignment:
    session_id: str
    project_id: Optional[str]
    source: str
    locked: bool
    confidence: Optional[float]
    reason: Optional[str]
    updated_at: int


@dataclass(frozen=True)
class PlanItem:
    action: str
    session_id: str
    target_root_session_id: str
    source: str
    root_has_assignment: bool
    project_id: Optional[str]
    root_project_id: Optional[str]
    conflict: Optional[str]


@dataclass(frozen=True)
class Report:
    move_count: int
    conflict_count: int
    redundant_count: int
    missing_root_count: int
    items: list[PlanItem]


def _default_hermes_home() -> Path:
    try:
        from hermes_constants import get_hermes_home

        return Path(get_hermes_home())
    except Exception:
        import os

        return Path(os.environ.get("HERMES_HOME", Path.home() / ".hermes")).expanduser()


def _resolve_path(path: Optional[str], *, default_name: str) -> Path:
    if path:
        return Path(path).expanduser()
    return _default_hermes_home() / default_name


def _connect_readonly(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"database not found: {db_path}")
    uri = f"file:{db_path.as_posix()}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def _safe_parse_json(raw: Any) -> dict[str, Any]:
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if not isinstance(raw, str):
        return {}
    try:
        value = json.loads(raw)
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _is_branch_like(model_config: Any) -> bool:
    cfg = _safe_parse_json(model_config)
    return bool(cfg.get("_branched_from") or cfg.get("_delegate_from"))


def _session_rows_by_id(state_db: Path) -> dict[str, dict[str, Any]]:
    with _connect_readonly(state_db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, parent_session_id, end_reason, model_config FROM sessions"
        ).fetchall()
    return {
        row["id"]: {
            "parent_session_id": row["parent_session_id"],
            "end_reason": row["end_reason"],
            "model_config": row["model_config"],
        }
        for row in rows
        if row["id"]
    }


def _resolve_root_session(session_id: str, rows_by_id: dict[str, dict[str, Any]]) -> str:
    current = str(session_id or "").strip()
    seen: set[str] = set()
    if not current:
        return current

    while current and current not in seen:
        seen.add(current)
        row = rows_by_id.get(current)
        if not row:
            break
        if _is_branch_like(row.get("model_config")):
            break

        parent = str(row.get("parent_session_id") or "").strip()
        if not parent:
            break
        parent_row = rows_by_id.get(parent)
        if not parent_row or parent_row.get("end_reason") != "compression":
            break
        current = parent
    return current


def _project_rows(projects_db: Path) -> dict[str, dict[str, str]]:
    try:
        with _connect_readonly(projects_db) as conn:
            conn.row_factory = sqlite3.Row
            projects = conn.execute("SELECT id, slug, name FROM projects").fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            return {}
        raise
    return {
        row["id"]: {
            "slug": row["slug"],
            "name": row["name"],
        }
        for row in projects
    }


def _assignment_rows(projects_db: Path) -> list[Assignment]:
    try:
        with _connect_readonly(projects_db) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT session_id, project_id, source, locked, confidence, reason, updated_at "
                "FROM session_project_assignments ORDER BY session_id"
            ).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            return []
        raise

    output: list[Assignment] = []
    for row in rows:
        output.append(
            Assignment(
                session_id=row["session_id"],
                project_id=row["project_id"],
                source=row["source"],
                locked=bool(row["locked"]),
                confidence=float(row["confidence"]) if row["confidence"] is not None else None,
                reason=row["reason"],
                updated_at=int(row["updated_at"]),
            )
        )
    return output


def _assignment_key(assign: Optional[Assignment]) -> tuple[str, Optional[str], bool]:
    if assign is None:
        return ("", None, False)
    return (assign.source, assign.project_id, assign.locked)


def _human_project_label(projects: dict[str, dict[str, str]], project_id: Optional[str]) -> str:
    if project_id is None:
        return "[home]"
    info = projects.get(project_id)
    if info:
        return f"{info['slug']} ({project_id})"
    return project_id


def _build_report(
    state_db: Path,
    projects_db: Path,
) -> Report:
    session_rows = _session_rows_by_id(state_db)
    assignment_rows = _assignment_rows(projects_db)
    by_session = {row.session_id: row for row in assignment_rows}

    items: list[PlanItem] = []
    move_count = 0
    conflict_count = 0
    redundant_count = 0
    missing_root_count = 0

    for assignment in assignment_rows:
        root_id = _resolve_root_session(assignment.session_id, session_rows)

        if assignment.session_id not in session_rows:
            missing_root_count += 1
            items.append(
                PlanItem(
                    action="missing_session",
                    session_id=assignment.session_id,
                    target_root_session_id=assignment.session_id,
                    source=assignment.source,
                    root_has_assignment=False,
                    project_id=assignment.project_id,
                    root_project_id=None,
                    conflict="session row missing in state DB",
                )
            )
            continue

        if root_id == assignment.session_id:
            continue

        root_assignment = by_session.get(root_id)
        if root_assignment is None:
            move_count += 1
            items.append(
                PlanItem(
                    action="move",
                    session_id=assignment.session_id,
                    target_root_session_id=root_id,
                    source=assignment.source,
                    root_has_assignment=False,
                    project_id=assignment.project_id,
                    root_project_id=None,
                    conflict=None,
                )
            )
            continue

        if _assignment_key(root_assignment) == _assignment_key(assignment):
            redundant_count += 1
            items.append(
                PlanItem(
                    action="redundant",
                    session_id=assignment.session_id,
                    target_root_session_id=root_id,
                    source=assignment.source,
                    root_has_assignment=True,
                    project_id=assignment.project_id,
                    root_project_id=root_assignment.project_id,
                    conflict=None,
                )
            )
            continue

        conflict_count += 1
        items.append(
            PlanItem(
                action="conflict",
                session_id=assignment.session_id,
                target_root_session_id=root_id,
                source=assignment.source,
                root_has_assignment=True,
                project_id=assignment.project_id,
                root_project_id=root_assignment.project_id,
                conflict=(
                    "child and root assignments differ for the same compression lineage; "
                    "manual review needed before apply"
                ),
            )
        )

    return Report(
        move_count=move_count,
        conflict_count=conflict_count,
        redundant_count=redundant_count,
        missing_root_count=missing_root_count,
        items=items,
    )


def _emit_human(report: Report, projects: dict[str, dict[str, str]]) -> str:
    rows = [
        f"Project assignment consolidation dry-run",
        f"moves: {report.move_count}",
        f"conflicts: {report.conflict_count}",
        f"redundant: {report.redundant_count}",
        f"missing session rows: {report.missing_root_count}",
    ]
    if not report.items:
        rows.append("No non-root rows require consolidation.")
        return "\n".join(rows)

    buckets = {
        "move": [],
        "conflict": [],
        "redundant": [],
        "missing_session": [],
    }
    for item in report.items:
        buckets.setdefault(item.action, []).append(item)

    if buckets["move"]:
        rows.append("\nMoves (safe for dry-run preview):")
        for item in buckets["move"]:
            rows.append(
                f"  - {item.session_id} -> {item.target_root_session_id} "
                f"project={_human_project_label(projects, item.project_id)}"
            )
    if buckets["redundant"]:
        rows.append("\nRedundant siblings (same effective assignment at root):")
        for item in buckets["redundant"]:
            rows.append(
                f"  - {item.session_id} -> {item.target_root_session_id} "
                f"project={_human_project_label(projects, item.project_id)}"
            )
    if buckets["conflict"]:
        rows.append("\nConflicts (root and child differ):")
        for item in buckets["conflict"]:
            rows.append(
                f"  - {item.session_id} -> {item.target_root_session_id} "
                f"child={_human_project_label(projects, item.project_id)} root="
                f"{_human_project_label(projects, item.root_project_id)} ({item.conflict})"
            )
    if buckets["missing_session"]:
        rows.append("\nAssignments without a state session row:")
        for item in buckets["missing_session"]:
            rows.append(f"  - {item.session_id}: {item.conflict}")

    return "\n".join(rows)


def _emit_json(report: Report) -> str:
    return json.dumps(
        {
            "move_count": report.move_count,
            "conflict_count": report.conflict_count,
            "redundant_count": report.redundant_count,
            "missing_root_count": report.missing_root_count,
            "items": [item.__dict__ for item in report.items],
        },
        indent=2,
        sort_keys=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-db", type=str, default=None, help="Path to state.db")
    parser.add_argument("--projects-db", type=str, default=None, help="Path to projects.db")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    args = parser.parse_args()

    state_db = _resolve_path(args.state_db, default_name="state.db")
    projects_db = _resolve_path(args.projects_db, default_name="projects.db")

    report = _build_report(state_db=state_db, projects_db=projects_db)
    projects = _project_rows(projects_db)

    if args.json:
        print(_emit_json(report))
        return 0

    print(_emit_human(report, projects))
    print("\nRead-only dry-run complete — no mutation was performed.")
    if report.conflict_count > 0:
        print("Manual confirmation required before any apply operation.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
