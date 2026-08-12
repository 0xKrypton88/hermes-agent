"""Lifecycle-scoped gateway delivery regressions for terminal completions.

The gateway contract here is deliberately narrower than exactly-once: one live
GatewayRunner suppresses concurrent/replayed copies after successful adapter
injection, failed injection remains retryable, and durable async-delegation
state (when available) is acknowledged through its authoritative SQLite API.
"""

import asyncio
import json
import queue
from collections import OrderedDict
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import Platform
from gateway.run import GatewayRunner
from gateway.session import SessionSource
from tools.process_registry import ProcessRegistry, ProcessSession


@pytest.fixture(autouse=True)
def isolated_registry(tmp_path, monkeypatch):
    """Any current/future durable compatibility path must stay in tmp state."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    import tools.process_registry as pr_module

    monkeypatch.setattr(pr_module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    registry = pr_module.ProcessRegistry()
    monkeypatch.setattr(pr_module, "process_registry", registry)
    return registry


def _runner(adapter, *, origins=None, platform=Platform.TELEGRAM, session_db=None):
    runner = object.__new__(GatewayRunner)
    runner._running = True
    runner.adapters = {platform: adapter}
    runner.session_store = SimpleNamespace(
        _ensure_loaded=lambda: None,
        _entries=origins or {},
    )
    runner._session_source_cache = {}
    runner._completion_delivery_lock = __import__("threading").Lock()
    runner._completion_deliveries_inflight = set()
    runner._completion_deliveries_delivered = OrderedDict()
    runner._completion_delivery_retention = 2048
    # Live parent sessions so #65838 pre-flight does not force retry/drop when
    # tests stamp parent_session_id (Cursor/Slack origin lineage).
    runner._session_db = session_db or SimpleNamespace(
        get_session=AsyncMock(return_value={"ended_at": None}),
        get_compression_tip=AsyncMock(return_value=None),
    )
    return runner


def _async_event(
    delegation_id="deleg_duplicate",
    *,
    session_key="agent:main:telegram:dm:12345:678",
):
    return {
        "type": "async_delegation",
        "delegation_id": delegation_id,
        "session_key": session_key,
        "goal": "Investigate flaky test",
        "status": "completed",
        "summary": "Found it",
        "api_calls": 1,
        "duration_seconds": 12.0,
        "dispatched_at": 1000.0,
        "completed_at": 1012.0,
        # PR #62479 stamps these on gateway-owned events. They must not
        # change the producer identity used for queue replay.
        "origin_profile": "default",
        "origin_hermes_home": "/tmp/hermes-default",
    }


def _completion_event(*, started_at, session_id="proc_reused"):
    return {
        "type": "completion",
        "session_id": session_id,
        "session_key": "agent:main:telegram:dm:123",
        "platform": "telegram",
        "chat_type": "dm",
        "chat_id": "123",
        "started_at": started_at,
        "command": "echo done",
        "exit_code": 0,
        "completion_reason": "exited",
        "output": "done\n",
    }


def _stop_after_sleeps(monkeypatch, runner, count):
    sleep_calls = 0

    async def _bounded_sleep(_delay):
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls >= count:
            runner._running = False

    monkeypatch.setattr(asyncio, "sleep", _bounded_sleep)


def test_duplicate_async_queue_replay_injects_once(monkeypatch, isolated_registry):
    """Byte-identical queue replays produce one turn in one gateway lifecycle."""
    isolated = queue.Queue()
    monkeypatch.setattr(isolated_registry, "completion_queue", isolated)
    isolated.put(dict(_async_event()))
    isolated.put(dict(_async_event()))

    adapter = SimpleNamespace(handle_message=AsyncMock())
    runner = _runner(adapter)
    _stop_after_sleeps(monkeypatch, runner, count=2)

    asyncio.run(runner._async_delegation_watcher(interval=0))

    adapter.handle_message.assert_awaited_once()


def test_unroutable_async_event_is_not_requeued_forever(
    monkeypatch, isolated_registry,
):
    isolated = queue.Queue()
    monkeypatch.setattr(isolated_registry, "completion_queue", isolated)
    event = _async_event("deleg_desktop_or_cli")
    event["session_key"] = "20260711_unparseable_ui_session"
    isolated.put(event)

    adapter = SimpleNamespace(handle_message=AsyncMock())
    runner = _runner(adapter)
    _stop_after_sleeps(monkeypatch, runner, count=2)

    asyncio.run(runner._async_delegation_watcher(interval=0))

    adapter.handle_message.assert_not_awaited()
    assert isolated.empty()


def test_concurrent_claims_share_the_same_narrow_delivery_seam():
    """Concurrent consumers in one runner cannot both enter the adapter."""
    entered = asyncio.Event()
    release = asyncio.Event()

    async def _blocked_injection(_event):
        entered.set()
        await release.wait()

    adapter = SimpleNamespace(handle_message=AsyncMock(side_effect=_blocked_injection))
    runner = _runner(adapter)
    event = _async_event()
    text = "completion"

    async def _exercise():
        first = asyncio.create_task(runner._deliver_completion_notification(text, dict(event)))
        await entered.wait()
        second = asyncio.create_task(runner._deliver_completion_notification(text, dict(event)))
        await asyncio.sleep(0)
        release.set()
        return await asyncio.gather(first, second)

    assert sorted(asyncio.run(_exercise()), key=str) == [None, True]
    adapter.handle_message.assert_awaited_once()


def test_failed_async_injection_is_retried_and_only_success_is_acked(
    monkeypatch, isolated_registry,
):
    isolated = queue.Queue()
    monkeypatch.setattr(isolated_registry, "completion_queue", isolated)
    isolated.put(_async_event())

    adapter = SimpleNamespace(
        handle_message=AsyncMock(side_effect=[RuntimeError("temporary"), None])
    )
    runner = _runner(adapter)
    _stop_after_sleeps(monkeypatch, runner, count=3)

    from tools import async_delegation

    acknowledgements = []
    monkeypatch.setattr(
        async_delegation,
        "complete_completion_delivery",
        lambda delegation_id, _claim_id: acknowledgements.append(delegation_id) or True,
        raising=False,
    )

    asyncio.run(runner._async_delegation_watcher(interval=0))

    assert adapter.handle_message.await_count == 2
    assert acknowledgements == ["deleg_duplicate"]


def _persist_pending_completion(event):
    from tools import async_delegation

    async_delegation._persist_dispatch({
        "delegation_id": event["delegation_id"],
        "session_key": event["session_key"],
        "origin_ui_session_id": "",
        "parent_session_id": event.get("parent_session_id"),
        "dispatched_at": event["dispatched_at"],
    })
    async_delegation._persist_completion(event, {
        "status": "completed",
        "summary": event["summary"],
    })


def test_watcher_discovers_cross_process_pending_after_startup(
    monkeypatch, isolated_registry,
):
    """A durable pending row written after watcher start must be delivered.

    Reproduces the cross-process Cursor Cloud completion gap: producer
    process A persists ``state=completed, delivery_state=pending`` into the
    shared state.db and only enqueues its own in-memory queue. The gateway
    watcher starts with an empty queue and must still discover that row once
    and pass it to ``_deliver_completion_notification`` without a restart.
    """
    isolated = queue.Queue()
    monkeypatch.setattr(isolated_registry, "completion_queue", isolated)
    assert isolated.empty()

    event = _async_event("deleg_cross_process")
    adapter = SimpleNamespace(handle_message=AsyncMock())
    runner = _runner(adapter)

    delivered_ids = []
    original_deliver = GatewayRunner._deliver_completion_notification

    async def _tracking_deliver(self, synth_text, evt):
        delivered_ids.append(str(evt.get("delegation_id") or ""))
        return await original_deliver(self, synth_text, evt)

    monkeypatch.setattr(
        GatewayRunner, "_deliver_completion_notification", _tracking_deliver,
    )

    sleep_calls = 0

    async def _bounded_sleep(_delay):
        nonlocal sleep_calls
        sleep_calls += 1
        # After the watcher's settle sleep, inject a durable row as if another
        # process completed — the in-memory queue stays empty until discovery.
        if sleep_calls == 1:
            assert isolated.empty()
            _persist_pending_completion(event)
        if sleep_calls >= 4:
            runner._running = False

    monkeypatch.setattr(asyncio, "sleep", _bounded_sleep)

    asyncio.run(
        runner._async_delegation_watcher(interval=0, durable_scan_interval=0)
    )

    assert delivered_ids == ["deleg_cross_process"]
    adapter.handle_message.assert_awaited_once()
    from tools import async_delegation

    row = async_delegation.get_durable_delegation("deleg_cross_process")
    assert row is not None
    assert row["delivery_state"] == "delivered"


def test_watcher_does_not_duplicate_already_queued_or_terminal_rows(
    monkeypatch, isolated_registry,
):
    """Already-queued, delivered, and actively-claimed rows are not re-fired."""
    from tools import async_delegation

    isolated = queue.Queue()
    monkeypatch.setattr(isolated_registry, "completion_queue", isolated)

    queued = _async_event("deleg_already_queued")
    delivered = _async_event("deleg_already_delivered")
    claimed = _async_event("deleg_already_claimed")
    fresh = _async_event("deleg_fresh_pending")

    _persist_pending_completion(queued)
    _persist_pending_completion(delivered)
    _persist_pending_completion(claimed)
    _persist_pending_completion(fresh)

    assert async_delegation.mark_completion_delivered("deleg_already_delivered")
    assert async_delegation.claim_completion_delivery(
        "deleg_already_claimed", "other-process-claim",
    )

    # Simulate the local queue already holding the in-process copy.
    isolated.put(dict(queued))

    adapter = SimpleNamespace(handle_message=AsyncMock())
    runner = _runner(adapter)

    delivered_ids = []
    original_deliver = GatewayRunner._deliver_completion_notification

    async def _tracking_deliver(self, synth_text, evt):
        did = str(evt.get("delegation_id") or "")
        delivered_ids.append(did)
        return await original_deliver(self, synth_text, evt)

    monkeypatch.setattr(
        GatewayRunner, "_deliver_completion_notification", _tracking_deliver,
    )
    _stop_after_sleeps(monkeypatch, runner, count=5)

    asyncio.run(
        runner._async_delegation_watcher(interval=0, durable_scan_interval=0)
    )

    assert delivered_ids.count("deleg_already_queued") == 1
    assert "deleg_already_delivered" not in delivered_ids
    assert "deleg_already_claimed" not in delivered_ids
    assert delivered_ids.count("deleg_fresh_pending") == 1
    assert adapter.handle_message.await_count == 2


def test_lost_wakeup_then_reconcile_delivers_once_to_slack_origin(
    monkeypatch, isolated_registry,
):
    """Process-boundary: durable pending + empty queue → one Slack origin send.

    Simulates the confirmed Cursor last-mile defect: producer persists
    ``completed + delivery_state=pending`` and the local queue wakeup is
    dropped (or a new gateway process boots with an empty queue). Gateway
    rediscovery must deliver exactly once to the same origin session/thread.
    """
    from tools import async_delegation

    isolated = queue.Queue()
    monkeypatch.setattr(isolated_registry, "completion_queue", isolated)

    slack_key = "agent:main:slack:thread:C09ORIGIN:1718000000.000100"
    event = _async_event(
        "cursor_hrun-bef93c72-d30a-46ff-bb8e-d2a6ac1651a6",
        session_key=slack_key,
    )
    event["parent_session_id"] = "sess-slack-origin"
    event["summary"] = "Cursor run finished"

    # Handoff without wakeup — authoritative outbox only.
    handoff = async_delegation.handoff_cursor_run_completion(
        run_id="hrun-bef93c72-d30a-46ff-bb8e-d2a6ac1651a6",
        session_key=slack_key,
        summary=event["summary"],
        parent_session_id=event["parent_session_id"],
        enqueue_wakeup=False,
    )
    assert handoff["handoff_state"] == "accepted"
    assert handoff["delivery_state"] == "pending"
    assert isolated.empty()

    adapter = SimpleNamespace(handle_message=AsyncMock())
    # Fresh runner = process recreate; in-memory delivery sets start empty.
    runner = _runner(adapter, platform=Platform.SLACK)
    _stop_after_sleeps(monkeypatch, runner, count=4)

    asyncio.run(
        runner._async_delegation_watcher(interval=0, durable_scan_interval=0)
    )

    adapter.handle_message.assert_awaited_once()
    delivered = adapter.handle_message.await_args.args[0]
    assert delivered.source.platform == Platform.SLACK
    assert delivered.source.chat_id == "C09ORIGIN"
    assert str(delivered.source.thread_id) == "1718000000.000100"

    row = async_delegation.get_durable_delegation(handoff["delegation_id"])
    assert row is not None
    assert row["delivery_state"] == "delivered"
    assert row["origin_session"] == slack_key

    # Second drain / restarted watcher must not re-send.
    runner2 = _runner(adapter, platform=Platform.SLACK)
    _stop_after_sleeps(monkeypatch, runner2, count=3)
    asyncio.run(
        runner2._async_delegation_watcher(interval=0, durable_scan_interval=0)
    )
    assert adapter.handle_message.await_count == 1


def test_double_drain_claims_once_and_send_failure_retries(
    monkeypatch, isolated_registry,
):
    """Concurrent drainers share one claim; failed send stays pending/retryable."""
    from tools import async_delegation

    isolated = queue.Queue()
    monkeypatch.setattr(isolated_registry, "completion_queue", isolated)

    slack_key = "agent:main:slack:dm:U09USER"
    event = _async_event("deleg_double_drain", session_key=slack_key)
    _persist_pending_completion(event)
    # Drop any accidental wakeup — outbox is the source of truth.
    while not isolated.empty():
        isolated.get_nowait()

    attempts = {"n": 0}

    async def _flaky_send(msg):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("slack 503")
        return None

    adapter = SimpleNamespace(handle_message=AsyncMock(side_effect=_flaky_send))
    runner = _runner(adapter, platform=Platform.SLACK)

    # Concurrent drains against the same durable identity.
    async def _race():
        text = "completion"
        first = asyncio.create_task(
            runner._deliver_completion_notification(text, dict(event))
        )
        second = asyncio.create_task(
            runner._deliver_completion_notification(text, dict(event))
        )
        return await asyncio.gather(first, second)

    results = sorted(asyncio.run(_race()), key=str)
    # One caller owns the claim path; the other is suppressed (None) or also
    # sees the released-retry outcome. Never two successful True acks.
    assert results.count(True) <= 1
    assert True not in results or results == [None, True] or results == [False, True]

    row = async_delegation.get_durable_delegation("deleg_double_drain")
    assert row is not None
    if row["delivery_state"] == "delivered":
        # First send failed then a winner retried successfully inside race.
        assert attempts["n"] >= 1
    else:
        assert row["delivery_state"] == "pending"
        # Explicit retry after release must succeed exactly once more.
        assert asyncio.run(
            runner._deliver_completion_notification("completion", dict(event))
        ) is True
        row = async_delegation.get_durable_delegation("deleg_double_drain")
        assert row["delivery_state"] == "delivered"

    assert adapter.handle_message.await_count >= 1
    final = async_delegation.get_durable_delegation("deleg_double_drain")
    assert final["delivery_state"] == "delivered"
    # No second success after ack.
    assert asyncio.run(
        runner._deliver_completion_notification("completion", dict(event))
    ) is None


def test_explicit_kill_returns_output_before_consuming_notification(monkeypatch):
    import tools.process_registry as pr_module

    registry = ProcessRegistry()
    session = ProcessSession(
        id="proc_kill_consumed",
        command="sleep 999",
        task_id="task",
        started_at=1.0,
        output_buffer="important terminal output\n",
        notify_on_complete=True,
    )
    session.process = MagicMock()
    session.process.pid = 4242
    registry._running[session.id] = session
    monkeypatch.setattr(registry, "_terminate_host_pid", lambda *_a, **_kw: None)
    monkeypatch.setattr(registry, "_write_checkpoint", lambda: None)
    monkeypatch.setattr(pr_module, "process_registry", registry)

    result = registry.kill_process(session.id)
    assert result["status"] == "killed"
    assert result["output"] == "important terminal output\n"
    assert registry.is_completion_consumed(session.id)

    adapter = SimpleNamespace(handle_message=AsyncMock())
    runner = _runner(adapter)

    async def _instant_sleep(*_a, **_kw):
        pass

    monkeypatch.setattr(asyncio, "sleep", _instant_sleep)
    asyncio.run(runner._run_process_watcher({
        "session_id": session.id,
        "check_interval": 0,
        "session_key": "agent:main:telegram:dm:123",
        "platform": "telegram",
        "chat_type": "dm",
        "chat_id": "123",
        "notify_on_complete": True,
    }))

    adapter.handle_message.assert_not_awaited()


def test_process_tool_redacts_explicit_kill_output(monkeypatch):
    from tools import process_registry as pr_module

    registry = ProcessRegistry()
    session = ProcessSession(
        id="proc_kill_redacted",
        command="printenv",
        task_id="task",
        started_at=1.0,
        output_buffer="PRIVATE_TOKEN=opaque-value\n",
        exited=True,
        exit_code=0,
    )
    registry._finished[session.id] = session
    monkeypatch.setattr(pr_module, "process_registry", registry)

    def _redact(result):
        assert result["output"] == "PRIVATE_TOKEN=opaque-value\n"
        result["output"] = "PRIVATE_TOKEN=<redacted>\n"
        return result

    monkeypatch.setattr(pr_module, "_redact_process_result", _redact)

    result = json.loads(pr_module._handle_process({
        "action": "kill",
        "session_id": session.id,
    }))
    assert result["output"] == "PRIVATE_TOKEN=<redacted>\n"


def test_autonomous_completion_redacts_real_command_and_output_secrets(monkeypatch):
    import agent.redact as redact_module
    import tools.process_registry as pr_module

    secret = "abc123randomopaquetokenvalue999"
    registry = ProcessRegistry()
    session = ProcessSession(
        id="proc_autonomous_redaction",
        command=f"printenv MY_SERVICE_TOKEN={secret}",
        task_id="task",
        started_at=1234.5,
        output_buffer=f"MY_SERVICE_TOKEN={secret}\nHOME=/home/user\n",
        exited=True,
        exit_code=0,
        notify_on_complete=True,
    )
    registry._finished[session.id] = session
    monkeypatch.setattr(pr_module, "process_registry", registry)
    monkeypatch.setattr(redact_module, "_REDACT_ENABLED", True)

    adapter = SimpleNamespace(handle_message=AsyncMock())
    runner = _runner(adapter)

    async def _instant_sleep(*_a, **_kw):
        pass

    monkeypatch.setattr(asyncio, "sleep", _instant_sleep)
    asyncio.run(runner._run_process_watcher({
        "session_id": session.id,
        "check_interval": 0,
        "session_key": "agent:main:telegram:dm:123",
        "platform": "telegram",
        "chat_type": "dm",
        "chat_id": "123",
        "notify_on_complete": True,
    }))

    delivered = adapter.handle_message.await_args.args[0]
    assert secret not in delivered.text
    assert "HOME=/home/user" in delivered.text
