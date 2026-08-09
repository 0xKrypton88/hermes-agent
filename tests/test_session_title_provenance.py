"""Behavior tests for durable session-title provenance (derived/llm/user)."""

from hermes_state import SessionDB


def test_manual_title_is_durable_lock_and_clear_unlocks(tmp_path):
    path = tmp_path / "state.db"
    db = SessionDB(path)
    db.create_session("session-1", "cli")

    assert db.set_auto_title("session-1", "HERMES - Session Names", source="llm") is True
    assert db.get_session("session-1")["title_source"] == "llm"
    db.close()

    db = SessionDB(path)
    assert db.set_session_title("session-1", "My permanent name") is True
    assert db.get_session("session-1")["title_source"] == "user"
    assert db.set_auto_title("session-1", "HERMES - Dashboard", source="llm") is False
    db.close()

    db = SessionDB(path)
    assert db.get_session_title("session-1") == "My permanent name"
    assert db.set_auto_title("session-1", "HERMES - Dashboard", source="llm") is False
    assert db.set_session_title("session-1", "") is True
    cleared = db.get_session("session-1")
    assert cleared["title"] is None
    assert cleared["title_source"] is None
    assert cleared["title_meta"] is None
    assert db.set_auto_title("session-1", "HERMES - Dashboard", source="llm") is True
    assert db.get_session("session-1")["title_source"] == "llm"
    db.close()


def test_derived_upgrades_to_llm_and_same_rank_noop_without_meta(tmp_path):
    db = SessionDB(tmp_path / "state.db")
    db.create_session("session-1", "cli")

    assert db.set_auto_title("session-1", "fix the flaky auth test", source="derived") is True
    assert db.set_auto_title("session-1", "Fix flaky auth test", source="llm") is True
    # Same-rank llm write without meta is a no-op (rename-itself guard).
    assert db.set_auto_title("session-1", "Totally Different", source="llm") is False
    row = db.get_session("session-1")
    assert row["title"] == "Fix flaky auth test"
    assert row["title_source"] == "llm"
    db.close()


def test_auto_title_with_meta_can_refresh_same_rank_llm(tmp_path):
    db = SessionDB(tmp_path / "state.db")
    db.create_session("session-1", "cli")

    assert db.set_auto_title_with_meta(
        "session-1",
        "MCC - Stats",
        {"project": "MCC", "area": "Stats", "project_owned": True},
    ) is True
    assert db.set_auto_title_with_meta(
        "session-1",
        "MCC - Dashboard",
        {"project": "MCC", "area": "Dashboard", "project_owned": True},
    ) is True
    assert db.set_auto_title_with_meta(
        "session-1",
        "MCC - Dashboard",
        {"project": "MCC", "area": "Dashboard", "project_owned": True},
    ) is False
    row = db.get_session("session-1")
    assert row["title"] == "MCC - Dashboard"
    assert row["title_source"] == "llm"
    assert db.get_session_title_meta("session-1")["area"] == "Dashboard"
    db.close()


def test_legacy_non_null_title_without_source_fails_closed(tmp_path):
    path = tmp_path / "state.db"
    db = SessionDB(path)
    db.create_session("legacy", "cli")
    db._execute_write(
        lambda conn: (
            conn.execute(
                "UPDATE sessions SET title = 'Legacy title', title_source = NULL WHERE id = 'legacy'"
            ).rowcount
        )
    )
    db.close()

    db = SessionDB(path)
    legacy = db.get_session("legacy")
    assert legacy["title"] == "Legacy title"
    assert legacy["title_source"] is None
    assert db.set_auto_title("legacy", "HERMES - Session Names", source="llm") is False
    assert db.set_auto_title_with_meta(
        "legacy", "HERMES - Session Names", {"project": "HERMES", "area": "Session Names"}
    ) is False
    assert db.get_session_title("legacy") == "Legacy title"
    db.close()


def test_compression_continuation_inherits_exact_title_and_provenance(tmp_path):
    db = SessionDB(tmp_path / "state.db")
    db.create_session("parent", "cli")
    assert db.set_auto_title("parent", "QUANTCORE - Dynamic DCA", source="llm") is True
    db.end_session("parent", "compression")
    db.create_session("child", "cli", parent_session_id="parent")

    assert db.inherit_session_title("parent", "child") is True
    parent = db.get_session("parent")
    child = db.get_session("child")
    assert parent["title"] is None
    assert parent["title_source"] is None
    assert child["title"] == "QUANTCORE - Dynamic DCA"
    assert child["title_source"] == "llm"
    # Inherited llm stays auto-writable via meta-aware refresh.
    assert db.set_auto_title_with_meta(
        "child",
        "QUANTCORE - Risk Controls",
        {"project": "QUANTCORE", "area": "Risk Controls", "project_owned": True},
    ) is True
    db.close()


def test_export_import_preserves_title_provenance(tmp_path):
    source = SessionDB(tmp_path / "source.db")
    source.create_session("portable", "cli")
    source.set_auto_title("portable", "NORNA - Hemsidan", source="llm")
    exported = source.export_session("portable")
    source.close()

    target = SessionDB(tmp_path / "target.db")
    result = target.import_sessions([exported])
    assert result["ok"] is True
    assert target.get_session_title("portable") == "NORNA - Hemsidan"
    assert target.get_session_title_source("portable") == "llm"
    target.close()
