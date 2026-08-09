from __future__ import annotations

from pathlib import Path

import pytest

from hermes_cli.projects_db import (
    assign_session_project,
    clear_session_project,
    connect,
    create_project,
    delete_project,
    get_project,
    get_session_project,
    is_session_routing_enabled,
    observe_auto_project,
    read_session_projects,
    set_session_routing_enabled,
    update_project,
)


def _open(tmp_path: Path):
    return connect(tmp_path / "projects.db")


def test_project_aliases_round_trip_and_update(tmp_path: Path) -> None:
    with _open(tmp_path) as conn:
        project_id = create_project(
            conn,
            name="Mission Control",
            aliases=["MCC", " mission-control ", "mcc"],
        )
        project = get_project(conn, project_id)

        assert project is not None
        assert project.aliases == ["MCC", "mission-control"]

        assert update_project(conn, project_id, aliases=["MCC", "Mission Control Center"]) is True
        updated = get_project(conn, project_id)

        assert updated is not None
        assert updated.aliases == ["MCC", "Mission Control Center"]


def test_manual_assignment_is_locked_and_survives_reopen(tmp_path: Path) -> None:
    db_path = tmp_path / "projects.db"

    with connect(db_path) as conn:
        project_id = create_project(conn, name="QuantCore")
        assignment = assign_session_project(conn, "session-1", project_id, source="manual")

        assert assignment.project_id == project_id
        assert assignment.source == "manual"
        assert assignment.locked is True

    with connect(db_path) as conn:
        restored = get_session_project(conn, "session-1")

        assert restored is not None
        assert restored.project_id == project_id
        assert restored.locked is True


def test_auto_assignment_cannot_override_a_manual_lock(tmp_path: Path) -> None:
    with _open(tmp_path) as conn:
        manual_id = create_project(conn, name="Mission Control")
        automatic_id = create_project(conn, name="Hermes Agent")
        assign_session_project(conn, "session-1", manual_id, source="manual")

        observed = observe_auto_project(
            conn,
            "session-1",
            automatic_id,
            confidence=0.99,
            reason="title prefix",
            required_observations=1,
        )

        assert observed is not None
        assert observed.project_id == manual_id
        assert observed.source == "manual"


def test_auto_assignment_waits_for_a_stable_candidate_before_moving(tmp_path: Path) -> None:
    with _open(tmp_path) as conn:
        mission_control_id = create_project(conn, name="Mission Control")
        quantcore_id = create_project(conn, name="QuantCore")
        assign_session_project(
            conn,
            "session-1",
            mission_control_id,
            source="auto",
            confidence=0.88,
            reason="initial focus",
        )

        first = observe_auto_project(
            conn,
            "session-1",
            quantcore_id,
            confidence=0.82,
            reason="message mention",
            required_observations=2,
        )
        assert first is not None
        assert first.project_id == mission_control_id

        second = observe_auto_project(
            conn,
            "session-1",
            quantcore_id,
            confidence=0.84,
            reason="message mention",
            required_observations=2,
        )
        assert second is not None
        assert second.project_id == quantcore_id
        assert second.source == "auto"
        assert second.locked is False


def test_strong_initial_observation_assigns_immediately(tmp_path: Path) -> None:
    with _open(tmp_path) as conn:
        project_id = create_project(conn, name="Hermes Agent")

        assignment = observe_auto_project(
            conn,
            "session-1",
            project_id,
            confidence=0.95,
            reason="title prefix",
            required_observations=2,
            immediate_confidence=0.9,
        )

        assert assignment is not None
        assert assignment.project_id == project_id
        assert assignment.source == "auto"


def test_clear_returns_session_to_automatic_routing(tmp_path: Path) -> None:
    with _open(tmp_path) as conn:
        project_id = create_project(conn, name="Norna Agency")
        assign_session_project(conn, "session-1", project_id, source="manual")

        assert clear_session_project(conn, "session-1") is True
        assert get_session_project(conn, "session-1") is None


def test_deleting_a_project_preserves_a_manual_lock_as_no_project(tmp_path: Path) -> None:
    with _open(tmp_path) as conn:
        project_id = create_project(conn, name="Hermes Agent")
        assign_session_project(conn, "session-1", project_id, source="manual")

        assert delete_project(conn, project_id) is True
        assignment = get_session_project(conn, "session-1")
        assert assignment is not None
        assert assignment.project_id is None
        assert assignment.locked is True


def test_locked_no_project_is_a_durable_manual_assignment(tmp_path: Path) -> None:
    with _open(tmp_path) as conn:
        assignment = assign_session_project(conn, "session-home", None, source="manual")

        assert assignment.project_id is None
        assert assignment.source == "manual"
        assert assignment.locked is True
        assert get_session_project(conn, "session-home") == assignment


def test_automatic_assignment_requires_a_real_project(tmp_path: Path) -> None:
    with _open(tmp_path) as conn:
        with pytest.raises(ValueError, match="automatic assignment requires"):
            assign_session_project(conn, "session-home", None, source="auto")


def test_session_routing_is_opt_in_and_persisted(tmp_path: Path) -> None:
    db_path = tmp_path / "projects.db"

    with connect(db_path) as conn:
        assert is_session_routing_enabled(conn) is False
        set_session_routing_enabled(conn, True)
        assert is_session_routing_enabled(conn) is True

    with connect(db_path) as conn:
        assert is_session_routing_enabled(conn) is True


def test_read_session_projects_is_readonly_and_does_not_create_missing_db(tmp_path: Path) -> None:
    missing = tmp_path / "missing" / "projects.db"
    assert read_session_projects(missing) == {}
    assert not missing.exists()

    db_path = tmp_path / "projects.db"
    with connect(db_path) as conn:
        project_id = create_project(conn, name="Hermes Agent")
        assign_session_project(conn, "session-1", project_id, source="manual")

    rows = read_session_projects(db_path)
    assert rows["session-1"].project_id == project_id
    assert rows["session-1"].locked is True
