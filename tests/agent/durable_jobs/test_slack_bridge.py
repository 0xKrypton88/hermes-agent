"""ENG-31 — inactive/fail-closed Slack client bridge + durable action ingress.

Deterministic injected transport only. No live Slack API, gateway wiring,
tokens, dispatch enablement, or production datastore.
"""

from __future__ import annotations

import socket
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest


def _db(tmp_path: Path) -> Path:
    return tmp_path / "pilot_jobs.sqlite"


def _make_job(tmp_path: Path, *, idempotency_key: str = "idem-eng31", authorize: bool = True):
    from agent.durable_jobs.store import DurableJobStore

    store = DurableJobStore(sqlite_path=_db(tmp_path))
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="ENG-31 bridge",
        repository_identity="github.com/example/repo",
        idempotency_key=idempotency_key,
    )
    if authorize:
        from tests.agent.durable_jobs.authz_fixtures import (
            install_default_adapter_authorization,
        )

        install_default_adapter_authorization(store.sqlite_path, job.job_id)
    return store, job


def _bind_kwargs(job_id: str, **overrides):
    base = dict(
        job_id=job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    base.update(overrides)
    return base


def _inbound_kwargs(job, **overrides):
    base = dict(
        job_id=job.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        actor_id="U-alice",
        decision_type="go",
        decision_idempotency_key="dec-eng31",
        policy_version="pol-1",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    base.update(overrides)
    return base


def _bind_policy(store, job) -> None:
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.slack_contract import SlackBindingLedger

    SlackBindingLedger(sqlite_path=store.sqlite_path).bind(**_bind_kwargs(job.job_id))
    DecisionLedger(sqlite_path=store.sqlite_path).set_policy(
        job_id=job.job_id,
        policy_version="pol-1",
        allowed_actors=("U-alice",),
        expires_at="2099-01-01T00:00:00+00:00",
    )


def _lane(tmp_path: Path, store=None, *, enabled: bool = True):
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.lane import DurableLaneService

    sqlite_path = str(store.sqlite_path) if store is not None else str(_db(tmp_path))
    cfg = load_durable_jobs_config(
        {
            "durable_jobs": {
                "enabled": enabled,
                "dispatch_enabled": False,
                "sqlite_path": sqlite_path,
                "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
            }
        }
    )
    return DurableLaneService(config=cfg, store=store)


def _official_post(*, ts: str, client_msg_id: str, channel: str = "C123", team: str = "T1"):
    return {
        "ok": True,
        "channel": channel,
        "ts": ts,
        "message": {
            "type": "message",
            "ts": ts,
            "thread_ts": "111.222",
            "client_msg_id": client_msg_id,
            "team": team,
            "text": "durable root",
        },
    }


def _official_lookup_message(
    *, ts: str, client_msg_id: str, channel: str = "C123", team: str = "T1"
):
    return {
        "type": "message",
        "ts": ts,
        "thread_ts": "111.222",
        "client_msg_id": client_msg_id,
        "channel": channel,
        "team": team,
        "text": "durable root",
    }


@dataclass
class MemorySlackTransport:
    """In-memory Slack stand-in. No sockets, no SDK."""

    post_payload: Any = None
    lookups: Any = field(default_factory=list)
    post_calls: list = field(default_factory=list)
    lookup_calls: list = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def post_root(
        self,
        *,
        client_msg_id: str,
        workspace_id: str,
        channel_id: str,
        root_thread_ts: str,
        job_id: str,
    ) -> Any:
        with self._lock:
            self.post_calls.append(
                {
                    "client_msg_id": client_msg_id,
                    "workspace_id": workspace_id,
                    "channel_id": channel_id,
                    "root_thread_ts": root_thread_ts,
                    "job_id": job_id,
                }
            )
            payload = self.post_payload
            if callable(payload) and not isinstance(payload, type):
                return payload(
                    client_msg_id=client_msg_id,
                    workspace_id=workspace_id,
                    channel_id=channel_id,
                    root_thread_ts=root_thread_ts,
                    job_id=job_id,
                )
            if isinstance(payload, BaseException):
                raise payload
            return payload

    def lookup_by_client_msg_id(self, client_msg_id: str) -> Any:
        with self._lock:
            self.lookup_calls.append(client_msg_id)
            if isinstance(self.lookups, BaseException):
                raise self.lookups
            if isinstance(self.lookups, dict):
                return self.lookups
            return list(self.lookups)


# ---------------------------------------------------------------------------
# Slice 1 — injected seam, isolation, disabled-by-default
# ---------------------------------------------------------------------------


def test_adapter_seam_requires_injected_transport_and_preserves_null_isolation():
    from agent.durable_jobs.adapters import NullSlackPort, SlackMessagePort
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.slack_bridge import SlackClientBridge, adapter_from_config
    import agent.durable_jobs.adapters as adapters
    import agent.durable_jobs.slack_bridge as slack_bridge

    assert hasattr(adapters, "NullSlackPort")
    assert not hasattr(adapters, "SlackClientBridge")
    assert not hasattr(adapters, "SlackDispatchAdapter")
    assert not hasattr(slack_bridge, "SlackHttpClient")
    assert not hasattr(slack_bridge, "LiveSlackTransport")
    assert not hasattr(slack_bridge, "SlackSdkClient")

    null = NullSlackPort()
    with pytest.raises(RuntimeError, match="refuses post_root"):
        null.post_root(
            client_msg_id="cmid",
            workspace_id="T1",
            channel_id="C123",
            root_thread_ts="111.222",
            job_id="job",
        )
    with pytest.raises(RuntimeError, match="refuses lookup"):
        null.lookup_by_client_msg_id("cmid")

    with pytest.raises((TypeError, RuntimeError)):
        SlackClientBridge()

    cfg = load_durable_jobs_config(
        {
            "durable_jobs": {
                "enabled": True,
                "dispatch_enabled": True,
            }
        }
    )
    assert cfg.dispatch_allowed is False
    default = adapter_from_config(cfg)
    assert isinstance(default, NullSlackPort)

    adapter = SlackClientBridge(transport=_FakeTransport())
    assert callable(getattr(adapter, "post_root", None))
    assert callable(getattr(adapter, "lookup_by_client_msg_id", None))
    assert callable(getattr(SlackMessagePort, "post_root", None))
    assert callable(getattr(SlackMessagePort, "lookup_by_client_msg_id", None))


class _FakeTransport:
    def post_root(self, **_kwargs):
        raise AssertionError("transport must not be called in isolation test")

    def lookup_by_client_msg_id(self, client_msg_id: str):
        raise AssertionError("transport must not be called in isolation test")


def test_disabled_paths_reject_before_store_or_adapter(tmp_path, monkeypatch):
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.lane import DurableLaneService
    from agent.durable_jobs.service import PilotDisabledError
    from agent.durable_jobs.slack_bridge import SlackClientBridge
    from tests.agent.durable_jobs.eng28_support import RecordingAckPort

    constructed: list[str] = []

    def _boom_store(*_a, **_k):
        constructed.append("store")
        raise AssertionError("disabled path must not construct DurableJobStore")

    def _boom_bridge(*_a, **_k):
        constructed.append("bridge")
        raise AssertionError("disabled path must not construct SlackClientBridge")

    monkeypatch.setattr("agent.durable_jobs.store.DurableJobStore.__init__", _boom_store)
    monkeypatch.setattr("agent.durable_jobs.lane.DurableJobStore.__init__", _boom_store)
    monkeypatch.setattr(SlackClientBridge, "__init__", _boom_bridge)

    cfg = load_durable_jobs_config(
        {
            "durable_jobs": {
                "enabled": False,
                "dispatch_enabled": True,
                "sqlite_path": str(_db(tmp_path)),
                "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
            }
        }
    )
    assert cfg.dispatch_allowed is False
    lane = DurableLaneService(config=cfg)
    ack = RecordingAckPort()
    with pytest.raises(PilotDisabledError):
        lane.bind_slack(**_bind_kwargs("dj_nope"))
    with pytest.raises(PilotDisabledError):
        lane.deliver_slack_root(job_id="dj_nope", slack_port=object())
    with pytest.raises(PilotDisabledError):
        lane.consume_inbound_action(ack, **_inbound_kwargs(type("J", (), {"job_id": "dj_nope"})()))
    assert ack.acks == []
    assert constructed == []


def test_adapter_from_config_cannot_mint_live_client_when_dispatch_flag_true():
    from agent.durable_jobs.adapters import NullSlackPort
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.slack_bridge import adapter_from_config

    cfg = load_durable_jobs_config(
        {"durable_jobs": {"enabled": True, "dispatch_enabled": True}}
    )
    assert cfg.dispatch_allowed is False
    assert isinstance(adapter_from_config(cfg), NullSlackPort)
    assert isinstance(adapter_from_config(cfg, transport=None), NullSlackPort)
    from agent.durable_jobs.slack_bridge import SlackClientBridge

    bridged = adapter_from_config(cfg, transport=_FakeTransport())
    assert isinstance(bridged, SlackClientBridge)


def test_bridge_import_does_not_load_live_slack_or_gateway_modules():
    import sys

    from agent.durable_jobs import slack_bridge

    for name in (
        "slack_sdk",
        "slack_bolt",
        "plugins.platforms.slack.adapter",
        "gateway.platforms.slack",
    ):
        assert name not in sys.modules
    assert not hasattr(slack_bridge, "WebClient")
    assert not hasattr(slack_bridge, "AsyncApp")


# ---------------------------------------------------------------------------
# Slice 2 — production-shaped post/lookup + immutable client_msg_id
# ---------------------------------------------------------------------------


def test_official_post_preserves_stable_client_msg_id_and_binding(tmp_path):
    from agent.durable_jobs.slack_bridge import SlackClientBridge, SlackPostKind
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
        stable_outbound_client_msg_id,
    )

    store, job = _make_job(tmp_path)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    bound = ledger.bind(**_bind_kwargs(job.job_id))
    assert bound.outbound_client_msg_id == stable_outbound_client_msg_id(job.job_id)
    transport = MemorySlackTransport(
        post_payload=_official_post(ts="10.1", client_msg_id=bound.outbound_client_msg_id)
    )
    adapter = SlackClientBridge(transport=transport)
    delivered = deliver_slack_root(ledger, adapter, job_id=job.job_id)
    assert delivered.status is SlackRootStatus.DELIVERED
    assert delivered.delivered_message_ts == "10.1"
    assert delivered.outbound_client_msg_id == bound.outbound_client_msg_id
    assert delivered.workspace_id == "T1"
    assert delivered.channel_id == "C123"
    assert delivered.root_thread_ts == "111.222"
    assert len(transport.post_calls) == 1
    assert transport.post_calls[0]["client_msg_id"] == bound.outbound_client_msg_id
    assert transport.post_calls[0]["workspace_id"] == "T1"
    assert transport.post_calls[0]["channel_id"] == "C123"
    assert transport.post_calls[0]["root_thread_ts"] == "111.222"
    assert transport.post_calls[0]["job_id"] == job.job_id

    direct = adapter.post_root(
        client_msg_id=bound.outbound_client_msg_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        job_id=job.job_id,
    )
    assert direct.kind is SlackPostKind.ACCEPTED
    assert direct.message_ts == "10.1"


def test_foreign_or_mismatched_post_identity_is_not_accepted(tmp_path):
    from agent.durable_jobs.slack_bridge import SlackClientBridge, SlackPostKind
    from agent.durable_jobs.slack_contract import SlackBindingLedger, stable_outbound_client_msg_id

    store, job = _make_job(tmp_path)
    cmid = stable_outbound_client_msg_id(job.job_id)
    adapter = SlackClientBridge(
        transport=MemorySlackTransport(
            post_payload=_official_post(ts="10.1", client_msg_id="other-client-msg")
        )
    )
    result = adapter.post_root(
        client_msg_id=cmid,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        job_id=job.job_id,
    )
    assert result.kind is not SlackPostKind.ACCEPTED
    assert result.message_ts is None

    foreign_channel = SlackClientBridge(
        transport=MemorySlackTransport(
            post_payload=_official_post(ts="10.1", client_msg_id=cmid, channel="C999")
        )
    )
    mismatched = foreign_channel.post_root(
        client_msg_id=cmid,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        job_id=job.job_id,
    )
    assert mismatched.kind is not SlackPostKind.ACCEPTED


def test_immutable_binding_cannot_be_rebound_through_bridge_path(tmp_path):
    from agent.durable_jobs.slack_bridge import SlackClientBridge
    from agent.durable_jobs.slack_contract import (
        BindingConflict,
        SlackBindingLedger,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    bound = ledger.bind(**_bind_kwargs(job.job_id))
    transport = MemorySlackTransport(
        post_payload=_official_post(ts="10.1", client_msg_id=bound.outbound_client_msg_id)
    )
    deliver_slack_root(ledger, SlackClientBridge(transport=transport), job_id=job.job_id)
    with pytest.raises(BindingConflict):
        ledger.bind(**_bind_kwargs(job.job_id, root_thread_ts="333.444"))
    with pytest.raises(BindingConflict):
        ledger.bind(**_bind_kwargs(job.job_id, candidate_version="v2"))
    with pytest.raises(BindingConflict):
        ledger.bind(**_bind_kwargs(job.job_id, workspace_id="T-other"))
    frozen = ledger.get_binding(job.job_id)
    assert frozen is not None
    assert frozen.root_thread_ts == "111.222"
    assert frozen.candidate_version == "v1"
    assert frozen.workspace_id == "T1"
    assert frozen.outbound_client_msg_id == bound.outbound_client_msg_id


# ---------------------------------------------------------------------------
# Slice 3 — duplicate/concurrent root delivery; loser has zero transport calls
# ---------------------------------------------------------------------------


def test_duplicate_root_delivery_posts_once(tmp_path):
    from agent.durable_jobs.slack_bridge import SlackClientBridge
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    bound = ledger.bind(**_bind_kwargs(job.job_id))
    transport = MemorySlackTransport(
        post_payload=_official_post(ts="42.1", client_msg_id=bound.outbound_client_msg_id)
    )
    adapter = SlackClientBridge(transport=transport)
    first = deliver_slack_root(ledger, adapter, job_id=job.job_id)
    second = deliver_slack_root(ledger, adapter, job_id=job.job_id)
    assert first.status is SlackRootStatus.DELIVERED
    assert second.status is SlackRootStatus.DELIVERED
    assert second.delivered_message_ts == "42.1"
    assert len(transport.post_calls) == 1
    assert transport.lookup_calls == []


def test_concurrent_root_delivery_loser_makes_zero_transport_calls(tmp_path):
    from agent.durable_jobs.slack_bridge import SlackClientBridge
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    bound = ledger.bind(**_bind_kwargs(job.job_id))
    transport = MemorySlackTransport(
        post_payload=_official_post(ts="42.1", client_msg_id=bound.outbound_client_msg_id)
    )
    adapter = SlackClientBridge(transport=transport)
    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def worker() -> None:
        barrier.wait()
        try:
            deliver_slack_root(ledger, adapter, job_id=job.job_id)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert len(transport.post_calls) == 1
    assert transport.lookup_calls == []
    loaded = ledger.get_binding(job.job_id)
    assert loaded is not None
    assert loaded.status is SlackRootStatus.DELIVERED
    assert loaded.delivered_message_ts == "42.1"


# ---------------------------------------------------------------------------
# Slice 4 — accepted-lost lookup/adopt, restart/takeover, ambiguous/foreign
# ---------------------------------------------------------------------------


def test_accepted_lost_response_unique_lookup_adopts_without_repost(tmp_path):
    from agent.durable_jobs.slack_bridge import SlackClientBridge
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    bound = ledger.bind(**_bind_kwargs(job.job_id))
    cmid = bound.outbound_client_msg_id
    transport = MemorySlackTransport(
        post_payload={"ok": False, "error": "timeout"},
        lookups={
            "ok": True,
            "messages": [
                _official_lookup_message(ts="10.1", client_msg_id=cmid),
            ],
        },
    )
    adapter = SlackClientBridge(transport=transport)
    first = deliver_slack_root(ledger, adapter, job_id=job.job_id)
    assert first.status is SlackRootStatus.ADOPTED
    assert first.delivered_message_ts == "10.1"
    second = deliver_slack_root(ledger, adapter, job_id=job.job_id)
    assert second.status is SlackRootStatus.ADOPTED
    assert len(transport.post_calls) == 1
    assert transport.lookup_calls == [cmid]


def test_restart_takeover_looks_up_without_repost(tmp_path):
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS, FrozenClock
    from agent.durable_jobs.slack_bridge import SlackClientBridge
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path)
    clock = FrozenClock()
    ledger = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    bound = ledger.bind(**_bind_kwargs(job.job_id))
    cmid = bound.outbound_client_msg_id
    inner = SlackClientBridge(
        transport=MemorySlackTransport(
            post_payload=_official_post(ts="10.1", client_msg_id=cmid)
        )
    )

    class _CrashAfterAccepted:
        def __init__(self, port):
            self._port = port

        def post_root(self, **kwargs):
            result = self._port.post_root(**kwargs)
            raise RuntimeError("simulated crash after accepted Slack post")

        def lookup_by_client_msg_id(self, client_msg_id: str):
            return self._port.lookup_by_client_msg_id(client_msg_id)

    with pytest.raises(RuntimeError, match="simulated crash"):
        deliver_slack_root(ledger, _CrashAfterAccepted(inner), job_id=job.job_id)
    assert len(inner._transport.post_calls) == 1
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)

    recover = MemorySlackTransport(
        post_payload={"kind": "lost_response"},
        lookups={
            "ok": True,
            "messages": {"matches": [_official_lookup_message(ts="10.1", client_msg_id=cmid)]},
        },
    )
    reopened = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    adopted = deliver_slack_root(
        reopened, SlackClientBridge(transport=recover), job_id=job.job_id
    )
    assert adopted.status is SlackRootStatus.ADOPTED
    assert adopted.delivered_message_ts == "10.1"
    assert recover.post_calls == []
    assert recover.lookup_calls == [cmid]


def test_ambiguous_multiple_lookup_matches_fail_closed_without_repost(tmp_path):
    from agent.durable_jobs.slack_bridge import SlackClientBridge
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        SlackUnknownReason,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    bound = ledger.bind(**_bind_kwargs(job.job_id))
    cmid = bound.outbound_client_msg_id
    transport = MemorySlackTransport(
        post_payload={"kind": "lost_response"},
        lookups={
            "ok": True,
            "messages": [
                _official_lookup_message(ts="10.1", client_msg_id=cmid),
                _official_lookup_message(ts="10.2", client_msg_id=cmid),
            ],
        },
    )
    adapter = SlackClientBridge(transport=transport)
    first = deliver_slack_root(ledger, adapter, job_id=job.job_id)
    assert first.status is SlackRootStatus.UNKNOWN
    assert first.unknown_reason == SlackUnknownReason.REMOTE_DELIVERY_AMBIGUOUS.value
    second = deliver_slack_root(ledger, adapter, job_id=job.job_id)
    assert second.status is SlackRootStatus.UNKNOWN
    assert len(transport.post_calls) == 1


def test_foreign_malformed_and_cross_boundary_lookup_is_not_adopted(tmp_path):
    from agent.durable_jobs.slack_bridge import SlackClientBridge, parse_lookup_posts
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    bound = ledger.bind(**_bind_kwargs(job.job_id))
    cmid = bound.outbound_client_msg_id

    malformed = parse_lookup_posts(
        [{"text": "no ts", "client_msg_id": cmid}],
        expected_client_msg_id=cmid,
    )
    assert malformed == []

    foreign = parse_lookup_posts(
        [_official_lookup_message(ts="10.1", client_msg_id="foreign-id")],
        expected_client_msg_id=cmid,
    )
    assert all(post.client_msg_id != cmid for post in foreign) or foreign == []

    transport = MemorySlackTransport(
        post_payload={"kind": "lost_response"},
        lookups={
            "ok": True,
            "messages": [
                {"text": "garbled"},
                _official_lookup_message(ts="10.1", client_msg_id="foreign-id"),
                _official_lookup_message(ts="10.9", client_msg_id=cmid, channel="C999"),
            ],
        },
    )
    result = deliver_slack_root(
        ledger, SlackClientBridge(transport=transport), job_id=job.job_id
    )
    assert result.status is not SlackRootStatus.ADOPTED
    assert result.status is not SlackRootStatus.DELIVERED
    assert result.delivered_message_ts is None
    assert len(transport.post_calls) == 1


def test_empty_lookup_stays_recovering_without_repost(tmp_path):
    from agent.durable_jobs.slack_bridge import SlackClientBridge
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    bound = ledger.bind(**_bind_kwargs(job.job_id))
    transport = MemorySlackTransport(
        post_payload={"kind": "lost_response"},
        lookups={"ok": True, "messages": []},
    )
    recovering = deliver_slack_root(
        ledger, SlackClientBridge(transport=transport), job_id=job.job_id
    )
    assert recovering.status is SlackRootStatus.RECOVERING
    assert recovering.delivered_message_ts is None
    assert len(transport.post_calls) == 1
    retry = deliver_slack_root(
        ledger, SlackClientBridge(transport=transport), job_id=job.job_id
    )
    assert retry.status is SlackRootStatus.RECOVERING
    assert len(transport.post_calls) == 1


def test_typed_sdk_foreign_record_is_not_adoptable(tmp_path):
    from types import SimpleNamespace

    from agent.durable_jobs.slack_bridge import SlackClientBridge, parse_lookup_posts
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
        stable_outbound_client_msg_id,
    )

    store, job = _make_job(tmp_path)
    cmid = stable_outbound_client_msg_id(job.job_id)
    foreign = SimpleNamespace(
        ts="10.1",
        client_msg_id="other-id",
        channel="C123",
        thread_ts="111.222",
        team="T1",
    )
    assert parse_lookup_posts([foreign], expected_client_msg_id=cmid) == [] or all(
        getattr(item, "client_msg_id", None) != cmid
        for item in parse_lookup_posts([foreign], expected_client_msg_id=cmid)
    )

    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    ledger.bind(**_bind_kwargs(job.job_id))
    transport = MemorySlackTransport(
        post_payload={"kind": "lost_response"},
        lookups=[foreign],
    )
    result = deliver_slack_root(
        ledger, SlackClientBridge(transport=transport), job_id=job.job_id
    )
    assert result.status is not SlackRootStatus.ADOPTED
    assert result.delivered_message_ts is None


# ---------------------------------------------------------------------------
# Slice 5 — durable Go/Pause/Cancel ingress through lane/coordinator
# ---------------------------------------------------------------------------


def test_lane_go_pause_cancel_ingress_validates_and_maps_pause_to_hold(tmp_path):
    from agent.durable_jobs.decisions import DecisionLedger, DecisionType
    from tests.agent.durable_jobs.eng28_support import RecordingAckPort

    store, job = _make_job(tmp_path, authorize=False)
    _bind_policy(store, job)
    lane = _lane(tmp_path, store)
    ack = RecordingAckPort()

    pause = lane.consume_inbound_action(
        ack, **_inbound_kwargs(job, decision_type="pause", decision_idempotency_key="k-pause")
    )
    assert pause.ok is True
    assert pause.ack_status == "acked"
    latest = DecisionLedger(sqlite_path=store.sqlite_path).latest_accepted(job.job_id)
    assert latest is not None
    assert latest.decision_type is DecisionType.HOLD
    assert latest.actor_id == "U-alice"
    assert len(ack.acks) == 1

    go = lane.consume_inbound_action(
        ack, **_inbound_kwargs(job, decision_type="go", decision_idempotency_key="k-go")
    )
    assert go.ok is True
    cancel = lane.consume_inbound_action(
        ack,
        **_inbound_kwargs(job, decision_type="cancel", decision_idempotency_key="k-cancel"),
    )
    assert cancel.ok is True
    after = DecisionLedger(sqlite_path=store.sqlite_path).latest_accepted(job.job_id)
    assert after is not None
    assert after.decision_type is DecisionType.CANCEL


def test_unauthorized_foreign_cross_boundary_and_stale_actions_are_rejected(
    tmp_path,
):
    from agent.durable_jobs.decisions import DecisionLedger
    from tests.agent.durable_jobs.eng28_support import RecordingAckPort

    store, job = _make_job(tmp_path, authorize=False, idempotency_key="idem-eng31-authz")
    _bind_policy(store, job)
    lane = _lane(tmp_path, store)
    ack = RecordingAckPort()

    unauthorized = lane.consume_inbound_action(
        ack, **_inbound_kwargs(job, actor_id="U-mallory", decision_idempotency_key="k-unauth")
    )
    assert unauthorized.ok is False
    assert unauthorized.ack_status == "rejected"

    cross_ws = lane.consume_inbound_action(
        ack,
        **_inbound_kwargs(
            job, workspace_id="T-other", decision_idempotency_key="k-ws"
        ),
    )
    assert cross_ws.ok is False
    assert cross_ws.ack_status == "rejected"

    cross_root = lane.consume_inbound_action(
        ack,
        **_inbound_kwargs(
            job, root_thread_ts="999.000", decision_idempotency_key="k-root"
        ),
    )
    assert cross_root.ok is False

    stale_candidate = lane.consume_inbound_action(
        ack,
        **_inbound_kwargs(
            job, candidate_version="v9", decision_idempotency_key="k-cand"
        ),
    )
    assert stale_candidate.ok is False

    missing = lane.consume_inbound_action(
        ack, **_inbound_kwargs(job, job_id="", decision_idempotency_key="k-missing")
    )
    assert missing.ok is False
    assert missing.ack_status == "rejected"

    malformed = lane.consume_inbound_action(
        ack, **_inbound_kwargs(job, decision_type="deploy", decision_idempotency_key="k-mal")
    )
    assert malformed.ok is False

    other = store.create_job(
        origin_platform="slack",
        origin_chat_id="C999",
        origin_root_thread_id="999.000",
        objective="other",
        repository_identity="github.com/example/repo",
        idempotency_key="idem-eng31-other",
    )
    foreign_job = lane.consume_inbound_action(
        ack,
        **_inbound_kwargs(
            job, job_id=other.job_id, decision_idempotency_key="k-foreign-job"
        ),
    )
    assert foreign_job.ok is False
    assert ack.acks == []
    assert DecisionLedger(sqlite_path=store.sqlite_path).count_decisions(job.job_id) == 0
    assert DecisionLedger(sqlite_path=store.sqlite_path).count_decisions(other.job_id) == 0


def test_duplicate_click_replay_is_idempotent(tmp_path):
    from agent.durable_jobs.decisions import DecisionLedger
    from tests.agent.durable_jobs.eng28_support import RecordingAckPort

    store, job = _make_job(tmp_path, authorize=False)
    _bind_policy(store, job)
    lane = _lane(tmp_path, store)
    ack = RecordingAckPort()
    kwargs = _inbound_kwargs(job, decision_type="pause", decision_idempotency_key="k-replay")
    first = lane.consume_inbound_action(ack, **kwargs)
    second = lane.consume_inbound_action(ack, **kwargs)
    assert first.ok is True
    assert second.ok is True
    assert first.inbound_id == second.inbound_id
    assert first.decision_id == second.decision_id
    assert DecisionLedger(sqlite_path=store.sqlite_path).count_decisions(job.job_id) == 1
    assert len(ack.acks) == 1


def test_restart_between_ack_and_resolution_reacks_without_second_decision(tmp_path):
    from agent.durable_jobs.decisions import DecisionLedger
    from tests.agent.durable_jobs.eng28_support import RecordingAckPort

    store, job = _make_job(tmp_path, authorize=False)
    _bind_policy(store, job)
    lane = _lane(tmp_path, store)
    lost = RecordingAckPort(fail_once=True)
    kwargs = _inbound_kwargs(job, decision_type="go", decision_idempotency_key="k-reack")
    first = lane.consume_inbound_action(lost, **kwargs)
    assert first.ok is True
    assert first.ack_status == "pending"
    assert lost.acks == []
    assert DecisionLedger(sqlite_path=store.sqlite_path).count_decisions(job.job_id) == 1

    retry_port = RecordingAckPort()
    second = lane.consume_inbound_action(retry_port, **kwargs)
    assert second.ok is True
    assert second.ack_status == "acked"
    assert second.decision_id == first.decision_id
    assert len(retry_port.acks) == 1
    assert DecisionLedger(sqlite_path=store.sqlite_path).count_decisions(job.job_id) == 1


def test_final_cancel_is_terminal_against_late_go_pause_and_stale_completion(
    tmp_path,
):
    from agent.durable_jobs.decisions import DecisionLedger, DecisionType, JobCanceledError
    from agent.durable_jobs.slack_bridge import SlackClientBridge
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )
    from tests.agent.durable_jobs.eng28_support import RecordingAckPort

    store, job = _make_job(tmp_path, authorize=False)
    _bind_policy(store, job)
    lane = _lane(tmp_path, store)
    ack = RecordingAckPort()
    cancel = lane.consume_inbound_action(
        ack,
        **_inbound_kwargs(job, decision_type="cancel", decision_idempotency_key="k-term"),
    )
    assert cancel.ok is True
    late_go = lane.consume_inbound_action(
        ack, **_inbound_kwargs(job, decision_type="go", decision_idempotency_key="k-late-go")
    )
    late_pause = lane.consume_inbound_action(
        ack,
        **_inbound_kwargs(
            job, decision_type="pause", decision_idempotency_key="k-late-pause"
        ),
    )
    assert late_go.ok is False
    assert late_pause.ok is False
    latest = DecisionLedger(sqlite_path=store.sqlite_path).latest_accepted(job.job_id)
    assert latest is not None
    assert latest.decision_type is DecisionType.CANCEL

    replay_cancel = lane.consume_inbound_action(
        ack,
        **_inbound_kwargs(job, decision_type="cancel", decision_idempotency_key="k-term"),
    )
    assert replay_cancel.ok is True
    assert replay_cancel.decision_id == cancel.decision_id

    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    bound = ledger.get_binding(job.job_id)
    assert bound is not None
    transport = MemorySlackTransport(
        post_payload=_official_post(ts="99.1", client_msg_id=bound.outbound_client_msg_id)
    )
    caught: list[BaseException] = []
    try:
        deliver_slack_root(
            ledger, SlackClientBridge(transport=transport), job_id=job.job_id
        )
    except BaseException as exc:  # noqa: BLE001
        caught.append(exc)
    assert transport.post_calls == []
    assert transport.lookup_calls == []
    assert caught
    assert all(isinstance(exc, JobCanceledError) for exc in caught)
    still = ledger.get_binding(job.job_id)
    assert still is not None
    assert still.status is SlackRootStatus.BOUND
    assert still.delivered_message_ts is None


def test_missing_fields_reject_before_store_on_enabled_lane(tmp_path, monkeypatch):
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.lane import DurableLaneService
    from tests.agent.durable_jobs.eng28_support import RecordingAckPort

    store, job = _make_job(tmp_path, authorize=False)
    _bind_policy(store, job)
    cfg = load_durable_jobs_config(
        {
            "durable_jobs": {
                "enabled": True,
                "dispatch_enabled": False,
                "sqlite_path": str(store.sqlite_path),
                "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
            }
        }
    )
    lane = DurableLaneService(config=cfg)
    ack = RecordingAckPort()
    constructed: list[str] = []

    import agent.durable_jobs.store as store_mod

    real_init = store_mod.DurableJobStore.__init__

    def _wrap(self, *a, **k):
        constructed.append("store")
        return real_init(self, *a, **k)

    monkeypatch.setattr(store_mod.DurableJobStore, "__init__", _wrap)
    monkeypatch.setattr("agent.durable_jobs.lane.DurableJobStore.__init__", _wrap)

    result = lane.consume_inbound_action(
        ack, **_inbound_kwargs(job, actor_id="   ", decision_idempotency_key="k-blank")
    )
    assert result.ok is False
    assert result.ack_status == "rejected"
    assert ack.acks == []
    assert constructed == []


# ---------------------------------------------------------------------------
# Isolation / redaction / no sockets
# ---------------------------------------------------------------------------


def test_bridge_methods_open_no_network_sockets(monkeypatch):
    from agent.durable_jobs.slack_bridge import SlackClientBridge

    def _deny(*_args, **_kwargs):
        raise AssertionError("network socket open attempted in ENG-31 bridge")

    monkeypatch.setattr(socket.socket, "connect", _deny)
    monkeypatch.setattr(socket.socket, "connect_ex", _deny)

    transport = MemorySlackTransport(
        post_payload=_official_post(ts="1.1", client_msg_id="cmid-1"),
        lookups={"ok": True, "messages": [_official_lookup_message(ts="1.1", client_msg_id="cmid-1")]},
    )
    adapter = SlackClientBridge(transport=transport)
    posted = adapter.post_root(
        client_msg_id="cmid-1",
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        job_id="job",
    )
    assert posted.message_ts == "1.1"
    looked = adapter.lookup_by_client_msg_id("cmid-1")
    assert looked


def test_slack_errors_are_secret_safe_and_redacted():
    from agent.durable_jobs.slack_bridge import (
        SlackClientBridge,
        SlackPostKind,
        redact_slack_error,
    )

    secret = "xoxb-live-slack-token-value"
    dsn = "postgresql://hermes:p@ssword@127.0.0.1:5432/durable_jobs"
    transport = MemorySlackTransport(
        post_payload=RuntimeError(f"auth failed token={secret} dsn={dsn}"),
        lookups=RuntimeError(f"lookup bearer={secret}"),
    )
    adapter = SlackClientBridge(transport=transport)
    posted = adapter.post_root(
        client_msg_id="cmid",
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        job_id="job",
    )
    assert posted.kind is SlackPostKind.UNKNOWN
    assert posted.error is not None
    assert secret not in posted.error
    assert "p@ssword" not in posted.error
    looked = adapter.lookup_by_client_msg_id("cmid")
    blob = str(looked)
    assert secret not in blob
    assert "p@ssword" not in redact_slack_error(f"dsn={dsn} token={secret}")
