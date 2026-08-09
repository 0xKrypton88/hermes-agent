"""Behavioral contract for neutral session-title metadata.

Covers launch-owned PROJECT/AREA seeding, verified executor/model suffixes,
parent-vs-child ownership, adaptive AREA retitle with PROJECT retention,
compression/export durability, and sync one-shot seed persistence.
"""

from __future__ import annotations

from unittest.mock import patch

from hermes_state import SessionDB


def _create(db: SessionDB, session_id: str = "session-1") -> None:
    db.create_session(session_id, "cli")


class TestLaunchSeed:
    def test_parent_deterministic_seed_is_auto_without_suffix(self, tmp_path):
        from agent.session_title_meta import seed_launch_title

        db = SessionDB(tmp_path / "state.db")
        _create(db)

        title = seed_launch_title(db, "session-1", project="HERMES", area="Session Names")
        row = db.get_session("session-1")
        assert title == "HERMES - Session Names"
        assert row["title"] == "HERMES - Session Names"
        assert row["title_source"] == "auto"
        meta = db.get_session_title_meta("session-1")
        assert meta["project"] == "HERMES"
        assert meta["area"] == "Session Names"
        assert meta.get("project_owned") is True
        assert not meta.get("executor")
        assert not meta.get("model")
        db.close()

    def test_direct_launch_has_no_executor_suffix(self, tmp_path):
        from agent.session_title_meta import seed_launch_title

        db = SessionDB(tmp_path / "state.db")
        _create(db)
        title = seed_launch_title(
            db,
            "session-1",
            project="MCC",
            area="Agent Sessions",
            # Launch path must ignore stray executor/model attempts.
            executor="Cursor",
            model="Grok 4.5",
        )
        assert title == "MCC - Agent Sessions"
        assert "·" not in title
        db.close()


class TestVerifiedDispatch:
    def test_verified_executor_and_grok_suffix(self, tmp_path):
        from agent.session_title_meta import (
            apply_verified_session_title_metadata,
            seed_launch_title,
        )

        db = SessionDB(tmp_path / "state.db")
        _create(db)
        seed_launch_title(db, "session-1", project="QuantCore", area="BTC Scalper")

        title = apply_verified_session_title_metadata(
            db,
            "session-1",
            verified={"executor": "Cursor", "model": "Grok 4.5"},
        )
        assert title == "QuantCore - BTC Scalper · Cursor Grok 4.5"
        meta = db.get_session_title_meta("session-1")
        assert meta["executor"] == "Cursor"
        assert meta["model"] == "Grok 4.5"
        assert meta["project"] == "QuantCore"
        db.close()

    def test_actual_verified_metadata_wins_over_requested(self, tmp_path):
        from agent.session_title_meta import (
            apply_verified_session_title_metadata,
            seed_launch_title,
        )

        db = SessionDB(tmp_path / "state.db")
        _create(db)
        seed_launch_title(db, "session-1", project="HERMES", area="Titles")

        title = apply_verified_session_title_metadata(
            db,
            "session-1",
            verified={"executor": "Cursor", "model": "Grok 4.5"},
            requested={"executor": "Codex", "model": "Fable 5", "project": "OTHER", "area": "Hack"},
        )
        assert title == "HERMES - Titles · Cursor Grok 4.5"
        meta = db.get_session_title_meta("session-1")
        assert meta["executor"] == "Cursor"
        assert meta["model"] == "Grok 4.5"
        assert meta["project"] == "HERMES"
        assert meta["area"] == "Titles"
        db.close()

    def test_fable_5_appears_only_when_verified_envelope_reports_it(self, tmp_path):
        """Fable 5 is written only from verified; requested Fable 5 stays out."""
        from agent.session_title_meta import (
            apply_verified_session_title_metadata,
            seed_launch_title,
        )

        db = SessionDB(tmp_path / "state.db")
        _create(db)
        seed_launch_title(db, "session-1", project="HERMES", area="Codex Work")

        # Requested Fable 5 must not win over verified Grok.
        title = apply_verified_session_title_metadata(
            db,
            "session-1",
            verified={"executor": "Cursor", "model": "Grok 4.5"},
            requested={"executor": "Codex", "model": "Fable 5"},
        )
        assert title == "HERMES - Codex Work · Cursor Grok 4.5"
        assert "Fable 5" not in title
        assert db.get_session_title_meta("session-1")["model"] == "Grok 4.5"

        # Fable 5 appears only when the verified envelope actually reports it.
        title = apply_verified_session_title_metadata(
            db,
            "session-1",
            verified={"executor": "Codex", "model": "Fable 5"},
            requested={"executor": "Cursor", "model": "Grok 4.5"},
        )
        assert title == "HERMES - Codex Work · Codex Fable 5"
        assert db.get_session_title_meta("session-1")["model"] == "Fable 5"
        db.close()

    def test_child_project_cannot_replace_parent(self, tmp_path):
        from agent.session_title_meta import (
            apply_verified_session_title_metadata,
            seed_launch_title,
        )

        db = SessionDB(tmp_path / "state.db")
        _create(db)
        seed_launch_title(db, "session-1", project="PARENT", area="Launch Area")

        title = apply_verified_session_title_metadata(
            db,
            "session-1",
            verified={
                "project": "CHILD",
                "area": "Dispatch Area",
                "executor": "Cursor",
            },
        )
        assert title == "PARENT - Launch Area · Cursor"
        meta = db.get_session_title_meta("session-1")
        assert meta["project"] == "PARENT"
        assert meta["area"] == "Launch Area"
        db.close()

    def test_suffix_update_outside_adaptive_cadence(self, tmp_path):
        from agent.session_title_meta import (
            apply_verified_session_title_metadata,
            seed_launch_title,
        )
        from agent.title_generator import maybe_auto_title

        db = SessionDB(tmp_path / "state.db")
        _create(db)
        seed_launch_title(db, "session-1", project="MCC", area="Stats")

        # Cadence turn 2 is ineligible for adaptive retitle; suffix update must
        # still land immediately from verified dispatch metadata.
        history = [
            {"role": "user", "content": "one"},
            {"role": "assistant", "content": "a1"},
            {"role": "user", "content": "two"},
            {"role": "assistant", "content": "a2"},
        ]
        cfg = {
            "auxiliary": {
                "title_generation": {"style": "project_area", "mode": "adaptive"}
            }
        }
        with patch("hermes_cli.config.load_config_readonly", return_value=cfg), patch(
            "agent.title_generator.threading.Thread"
        ) as thread_cls:
            maybe_auto_title(db, "session-1", "two", "a2", history)
            assert thread_cls.call_count == 0

        title = apply_verified_session_title_metadata(
            db,
            "session-1",
            verified={"executor": "Cursor", "model": "Grok 4.5"},
        )
        assert title == "MCC - Stats · Cursor Grok 4.5"
        assert db.get_session_title_source("session-1") == "auto"
        db.close()


class TestLocksAndAdaptive:
    def test_manual_lock_blocks_seed_and_dispatch(self, tmp_path):
        from agent.session_title_meta import (
            apply_verified_session_title_metadata,
            seed_launch_title,
        )

        db = SessionDB(tmp_path / "state.db")
        _create(db)
        db.set_session_title("session-1", "My permanent name")

        assert seed_launch_title(db, "session-1", project="HERMES", area="Dashboard") is None
        assert (
            apply_verified_session_title_metadata(
                db, "session-1", verified={"executor": "Cursor"}
            )
            is None
        )
        assert db.get_session_title("session-1") == "My permanent name"
        db.close()

    def test_legacy_lock_blocks_dispatch_without_schema_default_model(self, tmp_path):
        from agent.session_title_meta import apply_verified_session_title_metadata

        db = SessionDB(tmp_path / "state.db")
        _create(db, "legacy")
        db._execute_write(
            lambda conn: conn.execute(
                "UPDATE sessions SET title = 'Legacy title', title_source = NULL "
                "WHERE id = 'legacy'"
            ).rowcount
        )

        assert (
            apply_verified_session_title_metadata(
                db,
                "legacy",
                # Model alone is never enough; also proves no schema-default model
                # is invented when the verified executor is missing.
                verified={"model": "Grok 4.5"},
            )
            is None
        )
        assert (
            apply_verified_session_title_metadata(
                db, "legacy", verified={"executor": "Cursor", "model": "Grok 4.5"}
            )
            is None
        )
        assert db.get_session_title("legacy") == "Legacy title"
        db.close()

    def test_model_without_verified_executor_is_rejected(self, tmp_path):
        from agent.session_title_meta import (
            apply_verified_session_title_metadata,
            seed_launch_title,
        )

        db = SessionDB(tmp_path / "state.db")
        _create(db)
        seed_launch_title(db, "session-1", project="HERMES", area="Titles")

        assert (
            apply_verified_session_title_metadata(
                db, "session-1", verified={"model": "Grok 4.5"}
            )
            is None
        )
        assert db.get_session_title("session-1") == "HERMES - Titles"
        assert not db.get_session_title_meta("session-1").get("model")
        db.close()

    def test_adaptive_eligible_retitle_keeps_launch_project(self, tmp_path):
        from agent.session_title_meta import seed_launch_title
        from agent.title_generator import auto_title_session

        db = SessionDB(tmp_path / "state.db")
        _create(db)
        seed_launch_title(db, "session-1", project="HERMES", area="Session Names")

        with patch(
            "agent.title_generator.generate_title",
            return_value="OTHER - Dashboard",
        ):
            auto_title_session(
                db,
                "session-1",
                "Now focus on the dashboard",
                "Building dashboard",
                title_context="First intent: session names\nRecent: dashboard",
                adaptive=True,
            )

        assert db.get_session_title("session-1") == "HERMES - Dashboard"
        meta = db.get_session_title_meta("session-1")
        assert meta["project"] == "HERMES"
        assert meta["area"] == "Dashboard"
        assert meta.get("project_owned") is True
        db.close()

    def test_adaptive_retitle_keeps_verified_executor_model_suffix(self, tmp_path):
        from agent.session_title_meta import (
            apply_verified_session_title_metadata,
            seed_launch_title,
        )
        from agent.title_generator import auto_title_session

        db = SessionDB(tmp_path / "state.db")
        _create(db)
        seed_launch_title(db, "session-1", project="QuantCore", area="BTC Scalper")
        apply_verified_session_title_metadata(
            db,
            "session-1",
            verified={"executor": "Cursor", "model": "Grok 4.5"},
        )

        with patch(
            "agent.title_generator.generate_title",
            return_value="OTHER - Risk Controls · Codex Fable 5",
        ):
            auto_title_session(
                db,
                "session-1",
                "Shift to risk controls",
                "Working risk controls",
                title_context="Recent: risk controls",
                adaptive=True,
            )

        assert (
            db.get_session_title("session-1")
            == "QuantCore - Risk Controls · Cursor Grok 4.5"
        )
        meta = db.get_session_title_meta("session-1")
        assert meta["project"] == "QuantCore"
        assert meta["area"] == "Risk Controls"
        assert meta["executor"] == "Cursor"
        assert meta["model"] == "Grok 4.5"
        db.close()


class TestDurability:
    def test_compression_continuation_inherits_title_and_meta(self, tmp_path):
        from agent.session_title_meta import (
            apply_verified_session_title_metadata,
            seed_launch_title,
        )

        db = SessionDB(tmp_path / "state.db")
        _create(db, "parent")
        seed_launch_title(db, "parent", project="QUANTCORE", area="Dynamic DCA")
        apply_verified_session_title_metadata(
            db, "parent", verified={"executor": "Cursor", "model": "Grok 4.5"}
        )
        db.end_session("parent", "compression")
        db.create_session("child", "cli", parent_session_id="parent")

        assert db.inherit_session_title("parent", "child") is True
        child = db.get_session("child")
        assert child["title"] == "QUANTCORE - Dynamic DCA · Cursor Grok 4.5"
        assert child["title_source"] == "auto"
        meta = db.get_session_title_meta("child")
        assert meta["project"] == "QUANTCORE"
        assert meta["area"] == "Dynamic DCA"
        assert meta["executor"] == "Cursor"
        assert meta["model"] == "Grok 4.5"
        assert db.get_session("parent")["title"] is None
        assert db.get_session_title_meta("parent") is None
        db.close()

    def test_export_import_preserves_title_meta(self, tmp_path):
        from agent.session_title_meta import (
            apply_verified_session_title_metadata,
            seed_launch_title,
        )

        source = SessionDB(tmp_path / "source.db")
        source.create_session("portable", "cli")
        seed_launch_title(source, "portable", project="NORNA", area="Hemsidan")
        apply_verified_session_title_metadata(
            source, "portable", verified={"executor": "Cursor"}
        )
        exported = source.export_session("portable")
        source.close()

        target = SessionDB(tmp_path / "target.db")
        result = target.import_sessions([exported])
        assert result["ok"] is True
        assert target.get_session_title("portable") == "NORNA - Hemsidan · Cursor"
        assert target.get_session_title_source("portable") == "auto"
        meta = target.get_session_title_meta("portable")
        assert meta["project"] == "NORNA"
        assert meta["executor"] == "Cursor"
        target.close()


class TestOneShotSeedPersistence:
    def test_oneshot_returns_only_after_seed_persistence(self, tmp_path, monkeypatch):
        """Deterministic launch seed must be written before the seed call returns.

        One-shot ``hermes chat -Q -q`` exits the process after the turn; the
        seed therefore cannot rely on the async LLM title daemon.
        """
        from agent.session_title_meta import seed_launch_title_from_env

        monkeypatch.setenv("HERMES_TITLE_PROJECT", "MCC")
        monkeypatch.setenv("HERMES_TITLE_AREA", "Agent Sessions")
        monkeypatch.delenv("HERMES_TITLE_EXECUTOR", raising=False)
        monkeypatch.delenv("HERMES_TITLE_MODEL", raising=False)

        db = SessionDB(tmp_path / "state.db")
        _create(db, "oneshot-1")

        done = {"returned": False, "title_at_return": None}

        def _observe():
            # Simulate the one-shot exit boundary: after seed returns, the
            # process may exit immediately — the DB row must already hold the
            # auto title.
            done["returned"] = True
            done["title_at_return"] = db.get_session_title("oneshot-1")

        title = seed_launch_title_from_env(db, "oneshot-1")
        _observe()

        assert title == "MCC - Agent Sessions"
        assert done["returned"] is True
        assert done["title_at_return"] == "MCC - Agent Sessions"
        assert db.get_session_title_source("oneshot-1") == "auto"
        # Re-open to prove durability across connection close (process exit).
        db.close()
        db2 = SessionDB(tmp_path / "state.db")
        assert db2.get_session_title("oneshot-1") == "MCC - Agent Sessions"
        assert db2.get_session_title_meta("oneshot-1")["project"] == "MCC"
        db2.close()

    def test_ensure_db_session_seeds_synchronously(self, tmp_path, monkeypatch):
        from run_agent import AIAgent

        monkeypatch.setenv("HERMES_TITLE_PROJECT", "HERMES")
        monkeypatch.setenv("HERMES_TITLE_AREA", "Balance Codex")
        monkeypatch.setenv("HERMES_HOME", str(tmp_path / "home"))
        (tmp_path / "home").mkdir()

        db = SessionDB(tmp_path / "state.db")
        agent = AIAgent.__new__(AIAgent)
        agent._persist_disabled = False
        agent._session_db_created = False
        agent._session_db = db
        agent.session_id = "cli-oneshot"
        agent.platform = "cli"
        agent.model = "test-model"
        agent._session_init_model_config = {}
        agent._cached_system_prompt = None
        agent._parent_session_id = None

        with patch("run_agent._session_source_for_agent", return_value="cli"), patch(
            "run_agent._launch_cwd_for_session", return_value=str(tmp_path)
        ):
            agent._ensure_db_session()

        assert agent._session_db_created is True
        assert db.get_session_title("cli-oneshot") == "HERMES - Balance Codex"
        assert db.get_session_title_source("cli-oneshot") == "auto"
        db.close()


class TestPluginAndChatFlags:
    def test_plugin_context_applies_only_verified_suffix(self, tmp_path):
        from agent.session_title_meta import seed_launch_title
        from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest

        db = SessionDB(tmp_path / "state.db")
        _create(db)
        seed_launch_title(db, "session-1", project="HERMES", area="Plugins")

        manager = PluginManager()
        manager._discovered = True
        ctx = PluginContext(PluginManifest(name="title-probe"), manager)
        title = ctx.apply_session_title_metadata(
            db,
            "session-1",
            verified={"executor": "Codex", "model": "Fable 5"},
            requested={
                "executor": "Cursor",
                "model": "Grok 4.5",
                "project": "CHILD",
                "area": "Hack",
            },
        )
        assert title == "HERMES - Plugins · Codex Fable 5"
        meta = db.get_session_title_meta("session-1")
        assert meta["project"] == "HERMES"
        assert meta["executor"] == "Codex"
        assert meta["model"] == "Fable 5"
        db.close()

    def test_chat_title_project_area_flags_wire_env_transport(self, monkeypatch):
        from agent.session_title_meta import apply_launch_title_env
        from hermes_cli._parser import build_top_level_parser

        monkeypatch.delenv("HERMES_TITLE_PROJECT", raising=False)
        monkeypatch.delenv("HERMES_TITLE_AREA", raising=False)

        parser, _subparsers, _chat = build_top_level_parser()
        args = parser.parse_args(
            [
                "chat",
                "--title-project",
                "HERMES",
                "--title-area",
                "Dashboard",
                "-q",
                "hello",
            ]
        )
        assert args.title_project == "HERMES"
        assert args.title_area == "Dashboard"

        apply_launch_title_env(
            title_project=args.title_project,
            title_area=args.title_area,
        )
        import os

        assert os.environ["HERMES_TITLE_PROJECT"] == "HERMES"
        assert os.environ["HERMES_TITLE_AREA"] == "Dashboard"

    def test_partial_or_missing_launch_flags_cannot_reuse_stale_env(
        self, tmp_path, monkeypatch
    ):
        import os

        from agent.session_title_meta import (
            apply_launch_title_env,
            resolve_launch_title_args,
            seed_launch_title_from_env,
        )

        # Stale shell leftovers from a previous launch.
        monkeypatch.setenv("HERMES_TITLE_PROJECT", "STALE")
        monkeypatch.setenv("HERMES_TITLE_AREA", "Old Area")

        # Partial flag must not mix with stale env for the missing half.
        assert resolve_launch_title_args(title_project="MCC", title_area=None) == (
            None,
            None,
        )
        apply_launch_title_env(title_project="MCC", title_area=None)
        assert "HERMES_TITLE_PROJECT" not in os.environ
        assert "HERMES_TITLE_AREA" not in os.environ

        # Incomplete env pair alone must also fail closed (no half-seed).
        monkeypatch.setenv("HERMES_TITLE_PROJECT", "ONLY-PROJECT")
        monkeypatch.delenv("HERMES_TITLE_AREA", raising=False)
        assert resolve_launch_title_args() == (None, None)
        apply_launch_title_env()
        assert "HERMES_TITLE_PROJECT" not in os.environ
        assert "HERMES_TITLE_AREA" not in os.environ

        db = SessionDB(tmp_path / "state.db")
        db.create_session("no-seed", "cli")
        assert seed_launch_title_from_env(db, "no-seed") is None
        assert db.get_session_title("no-seed") is None
        db.close()
