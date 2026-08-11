"""RED→GREEN / regression tests for durable Slack Job Threads.

Covers:
- durable job row created first in CREATING_THREAD with idempotency key
- Slack root ts persisted and indexed as (platform, chat_id, thread_ts) ↔ job_id
- first status posted as a reply with the same thread_ts
- idempotent retry / restart never opens a second root
- pending root without mapping is recoverable (never silently orphaned)
- status / fortsätt / pausa / hjälp agenten / completion route via mapping
- legacy unmapped threads fall through
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest


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
        (
            "slack_bolt.adapter.socket_mode.async_handler",
            slack_bolt.adapter.socket_mode.async_handler,
        ),
        ("slack_sdk", slack_sdk),
        ("slack_sdk.web", slack_sdk.web),
        ("slack_sdk.web.async_client", slack_sdk.web.async_client),
    ]:
        sys.modules.setdefault(name, mod)
    sys.modules.setdefault("aiohttp", MagicMock())


_ensure_slack_mock()

import plugins.platforms.slack.adapter as _slack_mod  # noqa: E402

_slack_mod.SLACK_AVAILABLE = True

from gateway.config import Platform, PlatformConfig  # noqa: E402
from gateway.job_threads import (  # noqa: E402
    ACTION_CONTINUE,
    ACTION_HELP,
    ACTION_PAUSE,
    ACTION_STATUS,
    PHASE_ACTIVE,
    PHASE_COMPLETED,
    PHASE_CREATING_THREAD,
    PHASE_PAUSED,
    PHASE_PENDING_RECOVERY,
    PHASE_THREAD_READY,
    JobThreadStore,
    classify_job_control_text,
    ensure_slack_job_thread,
    post_job_update,
    reset_default_store,
    route_inbound_job_command,
)
from gateway.platforms.base import MessageEvent  # noqa: E402
from gateway.session import SessionSource  # noqa: E402
from plugins.platforms.slack.adapter import SlackAdapter  # noqa: E402


@pytest.fixture
def store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> JobThreadStore:
    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    reset_default_store()
    path = hermes_home / "job_threads.json"
    s = JobThreadStore(path)
    yield s
    reset_default_store()


class FakeSlackAdapter:
    """Minimal Slack adapter stand-in for job-thread create/send."""

    def __init__(self, root_ts: str = "1710000000.000100"):
        self.root_ts = root_ts
        self.create_calls: List[Dict[str, Any]] = []
        self.send_calls: List[Dict[str, Any]] = []
        self._next_ts_suffix = 0
        self._roots_by_operation: Dict[str, str] = {}
        self._lose_next_ts = False

    async def create_handoff_thread(self, parent_chat_id: str, name: str) -> Optional[str]:
        self.create_calls.append(
            {"via": "handoff", "chat_id": parent_chat_id, "name": name}
        )
        self._next_ts_suffix += 1
        return f"{self.root_ts[:-1]}{self._next_ts_suffix}"

    async def create_job_root_thread(
        self,
        parent_chat_id: str,
        name: str,
        *,
        job_id: str = "",
        operation_id: str = "",
    ) -> Optional[str]:
        self.create_calls.append(
            {
                "via": "job_root",
                "chat_id": parent_chat_id,
                "name": name,
                "job_id": job_id,
                "operation_id": operation_id,
            }
        )
        op = str(operation_id or "").strip()
        if op and op in self._roots_by_operation:
            # Slack client_msg_id idempotency: same operation → same ts.
            return self._roots_by_operation[op]

        self._next_ts_suffix += 1
        if self._next_ts_suffix == 1:
            ts = self.root_ts
        else:
            ts = f"{self.root_ts[:-1]}{self._next_ts_suffix}"
        if op:
            self._roots_by_operation[op] = ts
        if self._lose_next_ts:
            # Simulate: Slack accepted the post but client lost the ts.
            self._lose_next_ts = False
            return None
        return ts

    async def find_job_root_by_operation_id(
        self, parent_chat_id: str, operation_id: str
    ) -> Optional[str]:
        return self._roots_by_operation.get(str(operation_id or "").strip())

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.send_calls.append(
            {
                "chat_id": chat_id,
                "content": content,
                "reply_to": reply_to,
                "metadata": dict(metadata or {}),
            }
        )
        result = MagicMock()
        result.success = True
        return result


def test_create_job_record_first_in_creating_thread(store: JobThreadStore):
    job = store.create_job_creating_thread(
        idempotency_key="ik-1",
        platform="slack",
        chat_id="D123",
        objective="Ship durable threads",
    )
    assert job["phase"] == PHASE_CREATING_THREAD
    assert job["idempotency_key"] == "ik-1"
    assert job["chat_id"] == "D123"
    assert job["root_thread_ts"] is None
    assert job["next_action"] == "create_slack_root"
    # Persisted — reload from disk.
    reloaded = JobThreadStore(store.path)
    again = reloaded.get_by_idempotency("ik-1")
    assert again is not None
    assert again["job_id"] == job["job_id"]
    assert again["phase"] == PHASE_CREATING_THREAD


@pytest.mark.asyncio
async def test_ensure_persists_root_ts_and_posts_status_in_same_thread(
    store: JobThreadStore,
):
    adapter = FakeSlackAdapter(root_ts="1710000000.000111")
    job = await ensure_slack_job_thread(
        adapter,
        idempotency_key="ik-persist",
        objective="Persist root mapping",
        chat_id="C999",
        store=store,
        initial_status="Job started in thread",
    )
    assert job["root_thread_ts"] == "1710000000.000111"
    assert job["phase"] == PHASE_ACTIVE
    assert job["initial_status_posted"] is True
    assert len(adapter.create_calls) == 1
    assert adapter.create_calls[0]["chat_id"] == "C999"
    assert len(adapter.send_calls) == 1
    send = adapter.send_calls[0]
    assert send["chat_id"] == "C999"
    assert send["reply_to"] == "1710000000.000111"
    assert send["metadata"]["thread_ts"] == "1710000000.000111"
    assert send["content"] == "Job started in thread"

    mapped = store.find_by_thread("slack", "C999", "1710000000.000111")
    assert mapped is not None
    assert mapped["job_id"] == job["job_id"]


@pytest.mark.asyncio
async def test_followup_status_posts_to_same_persisted_thread(store: JobThreadStore):
    adapter = FakeSlackAdapter(root_ts="1710000000.000222")
    job = await ensure_slack_job_thread(
        adapter,
        idempotency_key="ik-follow",
        objective="Follow-up status",
        chat_id="D777",
        store=store,
    )
    root = job["root_thread_ts"]
    await post_job_update(adapter, job["job_id"], "Halfway done", store=store)
    assert len(adapter.send_calls) == 2
    assert adapter.send_calls[1]["reply_to"] == root
    assert adapter.send_calls[1]["metadata"]["thread_id"] == root


@pytest.mark.asyncio
async def test_idempotent_retry_does_not_create_second_root(store: JobThreadStore):
    adapter = FakeSlackAdapter(root_ts="1710000000.000333")
    first = await ensure_slack_job_thread(
        adapter,
        idempotency_key="ik-once",
        objective="Only one root",
        chat_id="D555",
        store=store,
    )
    second = await ensure_slack_job_thread(
        adapter,
        idempotency_key="ik-once",
        objective="Only one root",
        chat_id="D555",
        store=store,
    )
    assert first["job_id"] == second["job_id"]
    assert first["root_thread_ts"] == second["root_thread_ts"]
    assert len(adapter.create_calls) == 1
    # First status once; retry must not spam another seed/status pair.
    assert len(adapter.send_calls) == 1


@pytest.mark.asyncio
async def test_restart_reconciliation_promotes_pending_root(store: JobThreadStore):
    """A root recorded as pending (crash before full bind) is recoverable."""
    job = store.create_job_creating_thread(
        idempotency_key="ik-pending",
        platform="slack",
        chat_id="D42",
        objective="Recover me",
    )
    store.mark_create_attempt(job["job_id"])
    store.register_pending_root(job["job_id"], root_thread_ts="1710000000.000444")
    # Simulate process restart: new store instance on same file.
    restarted = JobThreadStore(store.path)
    recovered = restarted.reconcile_incomplete_creates()
    assert recovered
    bound = restarted.get_job(job["job_id"])
    assert bound is not None
    assert bound["root_thread_ts"] == "1710000000.000444"
    assert bound["phase"] == PHASE_THREAD_READY
    mapped = restarted.find_by_thread("slack", "D42", "1710000000.000444")
    assert mapped is not None
    assert mapped["job_id"] == job["job_id"]


@pytest.mark.asyncio
async def test_restart_after_create_attempt_without_root_does_not_open_second(
    store: JobThreadStore,
):
    job = store.create_job_creating_thread(
        idempotency_key="ik-orphan-guard",
        platform="slack",
        chat_id="D88",
        objective="No second root",
    )
    claimed = store.mark_create_attempt(job["job_id"])
    op_id = claimed["create_operation_id"]
    assert op_id
    # Restart reconciliation: create was attempted, no local root → PENDING_RECOVERY
    restarted = JobThreadStore(store.path)
    restarted.reconcile_incomplete_creates()
    guarded = restarted.get_job(job["job_id"])
    assert guarded is not None
    assert guarded["phase"] == PHASE_PENDING_RECOVERY
    assert guarded["create_operation_id"] == op_id

    adapter = FakeSlackAdapter(root_ts="1710000000.000999")
    result = await ensure_slack_job_thread(
        adapter,
        idempotency_key="ik-orphan-guard",
        objective="No second root",
        chat_id="D88",
        store=restarted,
    )
    # Same durable operation may be idempotently retried / recovered — but a
    # second create_operation_id / second root must never be minted.
    assert result["create_operation_id"] == op_id
    ops = {c.get("operation_id") for c in adapter.create_calls}
    assert ops == {op_id}
    assert len(adapter._roots_by_operation) <= 1


@pytest.mark.asyncio
async def test_refuses_arbitrary_destination_chat(store: JobThreadStore):
    adapter = FakeSlackAdapter()
    await ensure_slack_job_thread(
        adapter,
        idempotency_key="ik-dest",
        objective="Stay in DM",
        chat_id="D_HOME",
        store=store,
    )
    with pytest.raises(ValueError, match="refusing create"):
        await ensure_slack_job_thread(
            adapter,
            idempotency_key="ik-dest",
            objective="Stay in DM",
            chat_id="C_OTHER",
            store=store,
        )


def test_command_classification_swedish_and_english():
    assert classify_job_control_text("status") == ACTION_STATUS
    assert classify_job_control_text("/status") == ACTION_STATUS
    assert classify_job_control_text("fortsätt") == ACTION_CONTINUE
    assert classify_job_control_text("continue") == ACTION_CONTINUE
    assert classify_job_control_text("pausa") == ACTION_PAUSE
    assert classify_job_control_text("pause") == ACTION_PAUSE
    assert classify_job_control_text("hjälp agenten") == ACTION_HELP
    assert classify_job_control_text("help agent") == ACTION_HELP
    assert classify_job_control_text("klar") == "completion"
    assert classify_job_control_text("please keep going on the task") is None


def test_routing_uses_durable_mapping_not_heuristics(store: JobThreadStore):
    job = store.create_job_creating_thread(
        idempotency_key="ik-route",
        platform="slack",
        chat_id="D9",
        objective="Route me",
    )
    store.bind_root_thread(job["job_id"], root_thread_ts="1710000000.000555")

    hit = route_inbound_job_command(
        platform="slack",
        chat_id="D9",
        thread_ts="1710000000.000555",
        text="fortsätt",
        store=store,
    )
    assert hit is not None
    assert hit.job_id == job["job_id"]
    assert hit.action == ACTION_CONTINUE

    # Legacy / unmapped thread — no durable mapping → fallback.
    miss = route_inbound_job_command(
        platform="slack",
        chat_id="D9",
        thread_ts="1710000000.000000",
        text="fortsätt",
        store=store,
    )
    assert miss is None


@pytest.mark.asyncio
async def test_gateway_mixin_routes_pause_via_mapping(
    store: JobThreadStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    hermes_home = tmp_path / ".hermes"
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    reset_default_store()
    # Point default store at our temp file.
    from gateway import job_threads as jt

    monkeypatch.setattr(jt, "_DEFAULT_STORE", store)

    job = store.create_job_creating_thread(
        idempotency_key="ik-gw",
        platform="slack",
        chat_id="D1",
        objective="Gateway route",
    )
    store.bind_root_thread(
        job["job_id"],
        root_thread_ts="1710000000.000666",
        phase=PHASE_ACTIVE,
        next_action="await_work",
    )

    from gateway.slash_commands import GatewaySlashCommandsMixin

    class _Runner(GatewaySlashCommandsMixin):
        def __init__(self):
            self.adapters = {}
            self.async_session_store = MagicMock()

    runner = _Runner()
    event = MessageEvent(
        text="pausa",
        source=SessionSource(
            platform=Platform.SLACK,
            chat_id="D1",
            user_id="U1",
            thread_id="1710000000.000666",
        ),
    )
    reply = await runner._maybe_handle_job_thread_command(event)
    assert reply is not None
    assert "paused" in reply.lower()
    updated = store.get_job(job["job_id"])
    assert updated is not None
    assert updated["phase"] == PHASE_PAUSED

    # Legacy thread: same text, no mapping → None (fall through).
    legacy = MessageEvent(
        text="pausa",
        source=SessionSource(
            platform=Platform.SLACK,
            chat_id="D1",
            user_id="U1",
            thread_id="1710000000.000001",
        ),
    )
    assert await runner._maybe_handle_job_thread_command(legacy) is None


@pytest.mark.asyncio
async def test_completion_routes_and_marks_completed(store: JobThreadStore):
    job = store.create_job_creating_thread(
        idempotency_key="ik-done",
        platform="slack",
        chat_id="D2",
        objective="Finish",
    )
    store.bind_root_thread(
        job["job_id"],
        root_thread_ts="1710000000.000777",
        phase=PHASE_ACTIVE,
    )
    from gateway.slash_commands import GatewaySlashCommandsMixin
    from gateway import job_threads as jt

    jt._DEFAULT_STORE = store

    class _Runner(GatewaySlashCommandsMixin):
        def __init__(self):
            self.adapters = {}
            self.async_session_store = MagicMock()

    reply = await _Runner()._maybe_handle_job_thread_command(
        MessageEvent(
            text="klar",
            source=SessionSource(
                platform=Platform.SLACK,
                chat_id="D2",
                user_id="U2",
                thread_id="1710000000.000777",
            ),
        )
    )
    assert reply is not None
    assert "completed" in reply.lower()
    assert store.get_job(job["job_id"])["phase"] == PHASE_COMPLETED


@pytest.mark.asyncio
async def test_slack_adapter_create_job_root_posts_seed_in_authorized_channel():
    """Adapter helper posts into the given chat_id only (no channel admin)."""
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="xoxb-test"))
    adapter._app = MagicMock()
    client = AsyncMock()
    client.chat_postMessage = AsyncMock(
        return_value={"ok": True, "ts": "1710000000.000888"}
    )
    adapter._get_client = MagicMock(return_value=client)

    ts = await adapter.create_job_root_thread(
        "D_AUTH", "Build it", job_id="abc123", operation_id="op-xyz"
    )
    assert ts == "1710000000.000888"
    client.chat_postMessage.assert_awaited_once()
    kwargs = client.chat_postMessage.await_args.kwargs
    assert kwargs["channel"] == "D_AUTH"
    assert "abc123" in kwargs["text"]
    assert kwargs["client_msg_id"] == "op-xyz"
    assert "hermes_job_op=op-xyz" in kwargs["text"]


@pytest.mark.asyncio
async def test_slack_success_lost_ts_retry_no_second_root(store: JobThreadStore):
    """RED→GREEN: Slack accepts root but client loses ts; retry must not open #2."""
    adapter = FakeSlackAdapter(root_ts="1710000000.000321")
    adapter._lose_next_ts = True

    first = await ensure_slack_job_thread(
        adapter,
        idempotency_key="ik-lost-ts",
        objective="Crash window",
        chat_id="D_LOST",
        store=store,
    )
    # In-line find recovers the accepted root from the same operation id.
    assert first.get("root_thread_ts") == "1710000000.000321"
    assert len(adapter._roots_by_operation) == 1
    assert len({c["operation_id"] for c in adapter.create_calls}) == 1

    # Restart / retry with a fresh store handle — still one root, one op.
    restarted = JobThreadStore(store.path)
    adapter._lose_next_ts = False
    second = await ensure_slack_job_thread(
        adapter,
        idempotency_key="ik-lost-ts",
        objective="Crash window",
        chat_id="D_LOST",
        store=restarted,
    )
    assert second["root_thread_ts"] == first["root_thread_ts"]
    assert len(adapter._roots_by_operation) == 1
    # No new create_operation_id minted (same op reused at most for idempotent retry).
    ops = [c["operation_id"] for c in adapter.create_calls if c.get("operation_id")]
    assert len(set(ops)) == 1


@pytest.mark.asyncio
async def test_lost_ts_without_find_is_fail_closed_then_idempotent_retry(
    store: JobThreadStore,
):
    """When ts is unknown and history scan is unavailable, no second root."""
    posts: List[str] = []
    remote: Dict[str, str] = {}

    async def flaky_create(chat_id: str, name: str, operation_id: str = "") -> Optional[str]:
        posts.append(operation_id)
        if operation_id not in remote:
            remote[operation_id] = "1710000000.000654"
            return None  # accepted remotely, ts lost, no find API
        return remote[operation_id]

    class _BareAdapter:
        async def send(self, chat_id, content, reply_to=None, metadata=None):
            result = MagicMock()
            result.success = True
            return result

    first = await ensure_slack_job_thread(
        _BareAdapter(),
        idempotency_key="ik-fail-closed",
        objective="No find",
        chat_id="D_FC",
        store=store,
        create_root=flaky_create,
    )
    assert first["phase"] == PHASE_PENDING_RECOVERY
    assert first.get("root_thread_ts") in (None, "")
    assert len(posts) == 1
    assert len(remote) == 1

    # Retry: same durable operation id, idempotent create returns the one root.
    second = await ensure_slack_job_thread(
        _BareAdapter(),
        idempotency_key="ik-fail-closed",
        objective="No find",
        chat_id="D_FC",
        store=JobThreadStore(store.path),
        create_root=flaky_create,
    )
    assert second["root_thread_ts"] == "1710000000.000654"
    assert len(remote) == 1
    assert len(set(posts)) == 1


@pytest.mark.asyncio
async def test_parallel_ensure_single_root(store: JobThreadStore):
    """Concurrent ensure calls with the same idempotency key open one root."""
    import asyncio

    path = store.path
    roots: Dict[str, str] = {}
    create_count = {"n": 0}
    lock = asyncio.Lock()

    async def slow_create(chat_id: str, name: str, operation_id: str = "") -> Optional[str]:
        async with lock:
            create_count["n"] += 1
            op = operation_id or f"anon-{create_count['n']}"
            if op in roots:
                return roots[op]
            await asyncio.sleep(0.05)
            ts = f"1710000000.{create_count['n']:06d}"
            roots[op] = ts
            return ts

    class _BareAdapter:
        async def send(self, chat_id, content, reply_to=None, metadata=None):
            result = MagicMock()
            result.success = True
            return result

    async def _run() -> Dict[str, Any]:
        return await ensure_slack_job_thread(
            _BareAdapter(),
            idempotency_key="ik-parallel",
            objective="One root only",
            chat_id="D_PAR",
            store=JobThreadStore(path),
            create_root=slow_create,
        )

    r1, r2 = await asyncio.gather(_run(), _run())
    assert r1["job_id"] == r2["job_id"]
    assert r1["root_thread_ts"] == r2["root_thread_ts"]
    assert len(roots) == 1


@pytest.mark.asyncio
async def test_initial_status_only_after_bind_exactly_once(store: JobThreadStore):
    """Initial status posts only after durable bind, and only once across retry."""
    order: List[str] = []
    bound_before_status = {"ok": False}

    async def create_root(chat_id: str, name: str, operation_id: str = "") -> str:
        order.append("create")
        return "1710000000.000777"

    class _TrackingAdapter:
        def __init__(self):
            self.send_calls: List[Dict[str, Any]] = []

        async def send(self, chat_id, content, reply_to=None, metadata=None):
            # Root must already be durably bound before status.
            snap = JobThreadStore(store.path).get_by_idempotency("ik-status-once")
            bound_before_status["ok"] = bool(
                snap and snap.get("root_thread_ts") == "1710000000.000777"
            )
            order.append("status")
            self.send_calls.append(
                {"reply_to": reply_to, "content": content, "metadata": metadata}
            )
            result = MagicMock()
            result.success = True
            return result

    adapter = _TrackingAdapter()
    first = await ensure_slack_job_thread(
        adapter,
        idempotency_key="ik-status-once",
        objective="Status once",
        chat_id="D_ST",
        store=store,
        create_root=create_root,
        initial_status="hello-once",
    )
    assert first["initial_status_posted"] is True
    assert bound_before_status["ok"] is True
    assert order == ["create", "status"]
    assert len(adapter.send_calls) == 1
    assert adapter.send_calls[0]["reply_to"] == "1710000000.000777"

    second = await ensure_slack_job_thread(
        adapter,
        idempotency_key="ik-status-once",
        objective="Status once",
        chat_id="D_ST",
        store=JobThreadStore(store.path),
        create_root=create_root,
        initial_status="hello-once",
    )
    assert second["root_thread_ts"] == first["root_thread_ts"]
    assert order.count("status") == 1
    assert len(adapter.send_calls) == 1


@pytest.mark.asyncio
async def test_create_operation_id_persisted_before_slack_call(store: JobThreadStore):
    """Durable create-intent token exists on disk before the Slack API returns."""
    seen_op_before_return = {"op": None}

    async def create_root(chat_id: str, name: str, operation_id: str = "") -> str:
        on_disk = JobThreadStore(store.path).get_by_idempotency("ik-claim")
        assert on_disk is not None
        assert on_disk.get("create_operation_id") == operation_id
        assert on_disk.get("phase") == PHASE_CREATING_THREAD
        seen_op_before_return["op"] = operation_id
        return "1710000000.000111"

    class _BareAdapter:
        async def send(self, chat_id, content, reply_to=None, metadata=None):
            result = MagicMock()
            result.success = True
            return result

    job = await ensure_slack_job_thread(
        _BareAdapter(),
        idempotency_key="ik-claim",
        objective="Claim first",
        chat_id="D_CLAIM",
        store=store,
        create_root=create_root,
    )
    assert seen_op_before_return["op"]
    assert job["create_operation_id"] == seen_op_before_return["op"]
