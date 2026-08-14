"""Tests for plugin-registered Slack Block Kit action handlers.

Covers:
* ``PluginContext.register_slack_action_handler`` validation + queuing
* ``PluginManager.get_slack_action_handlers`` accessor
* ``SlackAdapter.connect`` wiring those handlers into the AsyncApp
* Defensive wrapping: a plugin handler that raises does NOT take down
  the gateway and Slack still gets an ack.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Ensure the repo root is importable when this test runs directly
# ---------------------------------------------------------------------------
_repo = str(Path(__file__).resolve().parents[2])
if _repo not in sys.path:
    sys.path.insert(0, _repo)


# ---------------------------------------------------------------------------
# Mock slack-bolt so SlackAdapter can be imported even without the package
# ---------------------------------------------------------------------------

def _ensure_slack_mock() -> None:
    if "slack_bolt" in sys.modules and hasattr(sys.modules["slack_bolt"], "__file__"):
        return
    slack_bolt = MagicMock()
    slack_bolt.async_app.AsyncApp = MagicMock
    slack_bolt.adapter.socket_mode.async_handler.AsyncSocketModeHandler = MagicMock

    slack_sdk = MagicMock()
    slack_sdk.web.async_client.AsyncWebClient = MagicMock

    for name, mod in [
        ("slack_bolt", slack_bolt),
        ("slack_bolt.async_app", slack_bolt.async_app),
        ("slack_bolt.adapter", slack_bolt.adapter),
        ("slack_bolt.adapter.socket_mode", slack_bolt.adapter.socket_mode),
        ("slack_bolt.adapter.socket_mode.async_handler",
         slack_bolt.adapter.socket_mode.async_handler),
        ("slack_sdk", slack_sdk),
        ("slack_sdk.web", slack_sdk.web),
        ("slack_sdk.web.async_client", slack_sdk.web.async_client),
    ]:
        sys.modules.setdefault(name, mod)
    sys.modules.setdefault("aiohttp", MagicMock())


_ensure_slack_mock()

import plugins.platforms.slack.adapter as _slack_mod  # noqa: E402
_slack_mod.SLACK_AVAILABLE = True

from gateway.config import PlatformConfig  # noqa: E402
from plugins.platforms.slack.adapter import SlackAdapter  # noqa: E402

from hermes_cli.plugins import (  # noqa: E402
    PluginContext,
    PluginManager,
    PluginManifest,
)


# ---------------------------------------------------------------------------
# PluginContext.register_slack_action_handler — input validation + queuing
# ---------------------------------------------------------------------------

def _make_ctx(name: str = "test_plugin") -> tuple[PluginManager, PluginContext]:
    """Build a fresh PluginManager + PluginContext bound to it."""
    mgr = PluginManager()
    manifest = PluginManifest(
        name=name,
        version="0.1.0",
        description="test",
    )
    ctx = PluginContext(manifest=manifest, manager=mgr)
    return mgr, ctx


class TestRegisterSlackActionHandlerAPI:
    """Behaviour of ctx.register_slack_action_handler()."""

    def test_string_action_id_is_queued(self):
        mgr, ctx = _make_ctx()

        async def cb(ack, body, action):  # pragma: no cover - never called here
            await ack()

        ctx.register_slack_action_handler("inbox_sweep_approve", cb)

        handlers = mgr.get_slack_action_handlers()
        assert len(handlers) == 1
        action_id, callback, plugin_name = handlers[0]
        assert action_id == "inbox_sweep_approve"
        assert callback is cb
        assert plugin_name == "test_plugin"

    def test_regex_action_id_is_accepted(self):
        """slack_bolt accepts re.Pattern matchers — so should the plugin API."""
        import re as _re
        mgr, ctx = _make_ctx()

        async def cb(ack, body, action):  # pragma: no cover
            await ack()

        pat = _re.compile(r"^inbox_sweep_.*$")
        ctx.register_slack_action_handler(pat, cb)
        handlers = mgr.get_slack_action_handlers()
        assert handlers[0][0] is pat

    def test_constraint_dict_action_id_is_accepted(self):
        """slack_bolt also accepts {"action_id": ..., "block_id": ...} dicts."""
        mgr, ctx = _make_ctx()

        async def cb(ack, body, action):  # pragma: no cover
            await ack()

        constraint = {"action_id": "approve", "block_id": "row_3"}
        ctx.register_slack_action_handler(constraint, cb)
        handlers = mgr.get_slack_action_handlers()
        assert handlers[0][0] == constraint


# ---------------------------------------------------------------------------
# SlackAdapter.connect wires plugin-registered handlers into AsyncApp
# ---------------------------------------------------------------------------


def _connect_with_recording_app(
    adapter: SlackAdapter,
    *,
    plugin_handlers: list,
) -> tuple[bool, list]:
    """Run adapter.connect() with mocks and return (result, registered_actions).

    Captures every action_id passed to ``app.action()`` so tests can
    assert that built-in handlers AND plugin-supplied handlers were
    wired up.
    """
    registered_actions: list = []  # list of (action_id, callback)

    def mock_action(action_id):
        def decorator(fn):
            registered_actions.append((action_id, fn))
            return fn
        return decorator

    def mock_event(_event_type):
        def decorator(fn):
            return fn
        return decorator

    def mock_command(_cmd):
        def decorator(fn):
            return fn
        return decorator

    mock_app = MagicMock()
    mock_app.event = mock_event
    mock_app.command = mock_command
    mock_app.action = mock_action
    mock_app.client = AsyncMock()

    mock_web_client = AsyncMock()
    mock_web_client.auth_test = AsyncMock(return_value={
        "user_id": "U_BOT",
        "user": "testbot",
        "team_id": "T_FAKE",
        "team": "FakeTeam",
    })

    fake_mgr = MagicMock()
    fake_mgr.get_slack_action_handlers.return_value = plugin_handlers

    with patch.object(_slack_mod, "AsyncApp", return_value=mock_app), \
         patch.object(_slack_mod, "AsyncWebClient", return_value=mock_web_client), \
         patch.object(_slack_mod, "AsyncSocketModeHandler", return_value=MagicMock()), \
         patch.dict(os.environ, {"SLACK_APP_TOKEN": "xapp-fake"}), \
         patch("gateway.status.acquire_scoped_lock", return_value=(True, None)), \
         patch("gateway.status.release_scoped_lock"), \
         patch("hermes_cli.plugins.get_plugin_manager", return_value=fake_mgr), \
         patch("asyncio.create_task"):
        result = asyncio.run(adapter.connect())

    return result, registered_actions


class TestSlackAdapterPluginActionWiring:
    """connect() must register plugin-supplied action handlers on AsyncApp."""


    def test_no_plugin_handlers_does_not_break_connect(self):
        """An empty plugin handler list is the common case — must be a no-op."""
        config = PlatformConfig(enabled=True, token="xoxb-fake")
        adapter = SlackAdapter(config)

        result, registered = _connect_with_recording_app(
            adapter, plugin_handlers=[],
        )
        assert result is True
        # Built-ins still wired
        action_ids = [aid for aid, _cb in registered]
        assert "hermes_approve_once" in action_ids


    def test_durable_job_actions_reuse_existing_slack_action_ingress(self):
        """Package 2 couples durable Go/Hold/Cancel onto the built-in action path."""
        from gateway.durable_job_lane import DURABLE_SLACK_ACTION_IDS

        config = PlatformConfig(enabled=True, token="xoxb-fake")
        adapter = SlackAdapter(config)
        result, registered = _connect_with_recording_app(
            adapter, plugin_handlers=[],
        )
        assert result is True
        action_ids = [aid for aid, _cb in registered]
        for action_id in DURABLE_SLACK_ACTION_IDS:
            assert action_id in action_ids


    def test_plugin_loader_failure_does_not_break_connect(self):
        """If get_plugin_manager() blows up, connect() must still succeed.

        Defensive belt-and-suspenders: the gateway should not refuse to
        start because the plugin layer is unhealthy.
        """
        config = PlatformConfig(enabled=True, token="xoxb-fake")
        adapter = SlackAdapter(config)

        registered_actions: list = []

        def mock_action(action_id):
            def decorator(fn):
                registered_actions.append((action_id, fn))
                return fn
            return decorator

        def _noop(_):
            def decorator(fn): return fn
            return decorator

        mock_app = MagicMock()
        mock_app.event = _noop
        mock_app.command = _noop
        mock_app.action = mock_action
        mock_app.client = AsyncMock()

        mock_web_client = AsyncMock()
        mock_web_client.auth_test = AsyncMock(return_value={
            "user_id": "U_BOT",
            "user": "testbot",
            "team_id": "T_FAKE",
            "team": "FakeTeam",
        })

        with patch.object(_slack_mod, "AsyncApp", return_value=mock_app), \
             patch.object(_slack_mod, "AsyncWebClient", return_value=mock_web_client), \
             patch.object(_slack_mod, "AsyncSocketModeHandler", return_value=MagicMock()), \
             patch.dict(os.environ, {"SLACK_APP_TOKEN": "xapp-fake"}), \
             patch("gateway.status.acquire_scoped_lock", return_value=(True, None)), \
             patch("gateway.status.release_scoped_lock"), \
             patch("hermes_cli.plugins.get_plugin_manager",
                   side_effect=RuntimeError("plugins broken")), \
             patch("asyncio.create_task"):
            result = asyncio.run(adapter.connect())

        assert result is True
        # Built-ins still wired even when plugin loader failed.
        action_ids = [aid for aid, _cb in registered_actions]
        assert "hermes_approve_once" in action_ids


def _durable_complete(tmp_path: Path, **overrides) -> dict:
    section = {
        "enabled": True,
        "dispatch_enabled": False,
        "backend": "sqlite",
        "sqlite_path": str(tmp_path / "jobs.sqlite"),
        "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
        "cursor_adapter_mode": "injected",
        "slack_adapter_mode": "injected",
        "cursor_secret_ref": "CURSOR_API_KEY",
        "slack_secret_ref": "SLACK_BOT_TOKEN",
        "policy_version": "eng29-matrix-v1",
        "identity_binding": {
            "workspace_id": "T1",
            "repository_identity": "github.com/example/repo",
        },
    }
    section.update(overrides)
    return {"durable_jobs": section}


def _seed_durable_job(handle, *, repository_identity: str = "github.com/example/repo"):
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.slack_contract import SlackBindingLedger

    store = handle.lane._require_sqlite_path()
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="slack-ingress",
        repository_identity=repository_identity,
        idempotency_key="idem-slack-ingress",
    )
    SlackBindingLedger(sqlite_path=store.sqlite_path).bind(
        job_id=job.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    DecisionLedger(sqlite_path=store.sqlite_path).set_policy(
        job_id=job.job_id,
        policy_version="pol-1",
        allowed_actors=("U-alice",),
        expires_at="2099-01-01T00:00:00+00:00",
    )
    return job, store


def _inbound_count(sqlite_path: Path) -> int:
    import sqlite3

    conn = sqlite3.connect(sqlite_path)
    try:
        (n,) = conn.execute("SELECT COUNT(*) FROM job_inbound_actions").fetchone()
        return int(n)
    finally:
        conn.close()


def _decision_count(sqlite_path: Path) -> int:
    import sqlite3

    conn = sqlite3.connect(sqlite_path)
    try:
        (n,) = conn.execute("SELECT COUNT(*) FROM job_decisions").fetchone()
        return int(n)
    finally:
        conn.close()


class TestDurableJobSlackActionIngress:
    """Production Slack action ingress: persist before ACK, fail-closed retry."""

    def test_ack_happens_only_after_durable_persist(self, tmp_path, monkeypatch):
        from gateway.durable_job_lane import detach_durable_job_lane
        from tests.agent.durable_jobs.package2_support import attach_runtime_ready_lane

        detach_durable_job_lane()
        handle = attach_runtime_ready_lane(
            raw_config=_durable_complete(tmp_path), monkeypatch=monkeypatch
        )
        assert handle is not None
        job, store = _seed_durable_job(handle)
        inbound_at_ack: list[int] = []

        async def ack():
            inbound_at_ack.append(_inbound_count(store.sqlite_path))

        adapter = SlackAdapter(PlatformConfig(enabled=True, token="xoxb-fake"))
        body = {
            "team": {"id": "T1"},
            "user": {"id": "U-alice"},
            "channel": {"id": "C123"},
            "message": {"thread_ts": "111.222", "ts": "111.222"},
        }
        action = {
            "action_id": "hermes_durable_go",
            "value": __import__("json").dumps(
                {
                    "job_id": job.job_id,
                    "decision_idempotency_key": "dec-slack-ack",
                    "policy_version": "pol-1",
                    "candidate_id": "cand-1",
                    "candidate_version": "v1",
                }
            ),
        }
        try:
            asyncio.run(adapter._handle_durable_job_action(ack, body, action))
            assert inbound_at_ack == [1]
            assert _inbound_count(store.sqlite_path) == 1
        finally:
            detach_durable_job_lane()

    def test_retryable_failure_does_not_ack(self, tmp_path, monkeypatch):
        from gateway.durable_job_lane import detach_durable_job_lane
        from tests.agent.durable_jobs.package2_support import attach_runtime_ready_lane

        detach_durable_job_lane()
        handle = attach_runtime_ready_lane(
            raw_config=_durable_complete(tmp_path), monkeypatch=monkeypatch
        )
        assert handle is not None
        job, store = _seed_durable_job(handle)
        handle.shutdown()
        acked: list[str] = []

        async def ack():
            acked.append("ack")

        adapter = SlackAdapter(PlatformConfig(enabled=True, token="xoxb-fake"))
        body = {
            "team": {"id": "T1"},
            "user": {"id": "U-alice"},
            "channel": {"id": "C123"},
            "message": {"thread_ts": "111.222", "ts": "111.222"},
        }
        action = {
            "action_id": "hermes_durable_go",
            "value": __import__("json").dumps(
                {
                    "job_id": job.job_id,
                    "decision_idempotency_key": "dec-slack-retry",
                    "policy_version": "pol-1",
                    "candidate_id": "cand-1",
                    "candidate_version": "v1",
                }
            ),
        }
        try:
            asyncio.run(adapter._handle_durable_job_action(ack, body, action))
            assert acked == []
            assert _inbound_count(store.sqlite_path) == 0
        finally:
            detach_durable_job_lane()

    def test_cross_repo_decision_does_not_persist_or_success_ack(self, tmp_path, monkeypatch):
        from gateway.durable_job_lane import detach_durable_job_lane
        from tests.agent.durable_jobs.package2_support import attach_runtime_ready_lane

        detach_durable_job_lane()
        handle = attach_runtime_ready_lane(
            raw_config=_durable_complete(tmp_path), monkeypatch=monkeypatch
        )
        assert handle is not None
        job, store = _seed_durable_job(
            handle, repository_identity="github.com/evil/other"
        )
        inbound_at_ack: list[int] = []
        decisions_at_ack: list[int] = []

        async def ack():
            inbound_at_ack.append(_inbound_count(store.sqlite_path))
            decisions_at_ack.append(_decision_count(store.sqlite_path))

        adapter = SlackAdapter(PlatformConfig(enabled=True, token="xoxb-fake"))
        body = {
            "team": {"id": "T1"},
            "user": {"id": "U-alice"},
            "channel": {"id": "C123"},
            "message": {"thread_ts": "111.222", "ts": "111.222"},
        }
        action = {
            "action_id": "hermes_durable_go",
            "value": __import__("json").dumps(
                {
                    "job_id": job.job_id,
                    "decision_idempotency_key": "dec-slack-cross-repo",
                    "policy_version": "pol-1",
                    "candidate_id": "cand-1",
                    "candidate_version": "v1",
                }
            ),
        }
        try:
            asyncio.run(adapter._handle_durable_job_action(ack, body, action))
            assert inbound_at_ack == [0]
            assert decisions_at_ack == [0]
            assert _inbound_count(store.sqlite_path) == 0
        finally:
            detach_durable_job_lane()
