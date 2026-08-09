from __future__ import annotations

import sqlite3
from pathlib import Path

from scripts import project_assignment_consolidation as pac


def _init_state_db(path: Path) -> None:
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE sessions (id TEXT PRIMARY KEY, parent_session_id TEXT, end_reason TEXT, model_config TEXT)")


def _init_projects_db(path: Path) -> None:
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE projects (id TEXT PRIMARY KEY, slug TEXT, name TEXT)")
        db.execute("CREATE TABLE session_project_assignments (session_id TEXT PRIMARY KEY, project_id TEXT, source TEXT, locked INTEGER, confidence REAL, reason TEXT, updated_at INTEGER)")
        db.execute("INSERT INTO projects VALUES ('p-hermes','hermes-agent','Hermes Agent')")
        db.execute("INSERT INTO projects VALUES ('p-mcc','mission-control','Mission Control')")


def _write_session(db_path: Path, session_id: str, parent: str | None, end_reason: str | None, model_config: str | None) -> None:
    with sqlite3.connect(db_path) as db:
        db.execute(
            "INSERT INTO sessions VALUES (?, ?, ?, ?)",
            (session_id, parent, end_reason, model_config),
        )


def _write_assignment(
    db_path: Path,
    session_id: str,
    project_id: str,
    source: str = "auto",
    locked: int = 0,
    confidence: float | None = None,
) -> None:
    with sqlite3.connect(db_path) as db:
        db.execute(
            "INSERT INTO session_project_assignments VALUES (?, ?, ?, ?, ?, NULL, 1)",
            (session_id, project_id, source, locked, confidence),
        )


def test_move_candidate_for_non_root_without_root_assignment(tmp_path: Path) -> None:
    state_db = tmp_path / "state.db"
    projects_db = tmp_path / "projects.db"
    _init_state_db(state_db)
    _init_projects_db(projects_db)

    # r_root ended by compression, so r_child is continuation in a chain.
    _write_session(state_db, "r_root", None, "compression", None)
    _write_session(state_db, "r_child", "r_root", "compression", None)

    _write_assignment(projects_db, "r_child", "p-hermes", source="auto", locked=0, confidence=0.6)

    report = pac._build_report(state_db=state_db, projects_db=projects_db)

    assert report.move_count == 1
    assert report.conflict_count == 0
    assert report.redundant_count == 0
    item = report.items[0]
    assert item.action == "move"
    assert item.session_id == "r_child"
    assert item.target_root_session_id == "r_root"
    assert item.project_id == "p-hermes"


def test_redundant_candidate_when_root_and_child_match(tmp_path: Path) -> None:
    state_db = tmp_path / "state.db"
    projects_db = tmp_path / "projects.db"
    _init_state_db(state_db)
    _init_projects_db(projects_db)

    _write_session(state_db, "r_root", None, "compression", None)
    _write_session(state_db, "r_child", "r_root", "compression", None)

    _write_assignment(projects_db, "r_root", "p-hermes", source="auto", locked=0, confidence=0.96)
    _write_assignment(projects_db, "r_child", "p-hermes", source="auto", locked=0, confidence=0.45)

    report = pac._build_report(state_db=state_db, projects_db=projects_db)

    assert report.move_count == 0
    assert report.conflict_count == 0
    assert report.redundant_count == 1
    item = report.items[0]
    assert item.action == "redundant"
    assert item.root_has_assignment is True


def test_conflict_when_root_has_different_assignment(tmp_path: Path) -> None:
    state_db = tmp_path / "state.db"
    projects_db = tmp_path / "projects.db"
    _init_state_db(state_db)
    _init_projects_db(projects_db)

    _write_session(state_db, "r_root", None, "compression", None)
    _write_session(state_db, "r_child", "r_root", "compression", None)

    _write_assignment(projects_db, "r_root", "p-hermes", source="auto", locked=0, confidence=0.96)
    _write_assignment(projects_db, "r_child", "p-mcc", source="auto", locked=0, confidence=0.45)

    report = pac._build_report(state_db=state_db, projects_db=projects_db)

    assert report.move_count == 0
    assert report.conflict_count == 1
    assert report.redundant_count == 0
    item = report.items[0]
    assert item.action == "conflict"
    assert item.project_id == "p-mcc"
    assert item.root_project_id == "p-hermes"
