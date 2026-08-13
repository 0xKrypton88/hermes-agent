"""Claim-owner lease / fencing contract (isolated, default-off).

Proves the fresh live-winner race: a losing caller that sees another worker's
unexpired CLAIMED must poll and must not lookup / post / create / adopt /
terminalize. Stale-lease takeover and old-owner fencing are also covered.

Deterministic injected clock only — no sleeps for lease correctness.
Same-process reopen / manual CLAIMED rows are unit evidence, not crash
evidence. Fresh-process death coverage lives in
``test_claim_restart_subprocess.py``.
"""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

import pytest


def _db(tmp_path: Path) -> Path:
    return tmp_path / "pilot_jobs.sqlite"


def _make_job(tmp_path: Path, *, idempotency_key: str = "idem-lease"):
    from agent.durable_jobs.store import DurableJobStore

    store = DurableJobStore(sqlite_path=_db(tmp_path))
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="claim-lease slice",
        repository_identity="github.com/example/repo",
        idempotency_key=idempotency_key,
    )
    return store, job


def _provider_kwargs(job):
    return dict(
        job_id=job.job_id,
        action_id="create_run",
        origin_platform=job.origin_platform,
        origin_chat_id=job.origin_chat_id,
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )


def _bind_kwargs(job_id: str):
    return dict(
        job_id=job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        candidate_id="cand-1",
        candidate_version="v1",
    )


class FrozenClock:
    """Deterministic clock used as ``now_fn``. Advance explicitly — no sleeps."""

    def __init__(self, start: Optional[datetime] = None) -> None:
        self._now = start or datetime(2026, 1, 1, tzinfo=timezone.utc)

    def __call__(self) -> str:
        return self._now.replace(microsecond=0).isoformat()

    def advance(self, seconds: float) -> None:
        self._now = self._now + timedelta(seconds=seconds)


@dataclass
class FakeRun:
    run_id: str
    idempotency_key: str


@dataclass
class FakeCreateResult:
    kind: str
    run: Optional[FakeRun] = None


class FakeCursorProvider:
    def __init__(
        self,
        create_result: FakeCreateResult,
        lookups: Optional[List[FakeRun]] = None,
    ) -> None:
        self.create_result = create_result
        self.lookups = list(lookups or [])
        self.create_calls: list[dict] = []
        self.lookup_calls: list[str] = []

    def create_run(self, *, idempotency_key: str, job_id: str) -> FakeCreateResult:
        self.create_calls.append(
            {"idempotency_key": idempotency_key, "job_id": job_id}
        )
        return self.create_result

    def lookup_runs(self, *, idempotency_key: str) -> list[FakeRun]:
        self.lookup_calls.append(idempotency_key)
        return list(self.lookups)


@dataclass
class FakePosted:
    message_ts: str
    client_msg_id: str


@dataclass
class FakePostResult:
    kind: str
    message_ts: Optional[str] = None


class FakeSlackPort:
    def __init__(
        self,
        post_result: FakePostResult,
        lookups: Optional[List[FakePosted]] = None,
    ) -> None:
        self.post_result = post_result
        self.lookups = list(lookups or [])
        self.posts: list[dict] = []
        self.lookup_calls: list[str] = []
        self._lock = threading.Lock()

    def post_root(
        self,
        *,
        client_msg_id: str,
        workspace_id: str,
        channel_id: str,
        root_thread_ts: str,
        job_id: str,
    ) -> FakePostResult:
        with self._lock:
            self.posts.append(
                {
                    "client_msg_id": client_msg_id,
                    "workspace_id": workspace_id,
                    "channel_id": channel_id,
                    "root_thread_ts": root_thread_ts,
                    "job_id": job_id,
                }
            )
        return self.post_result

    def lookup_by_client_msg_id(self, client_msg_id: str) -> list[FakePosted]:
        self.lookup_calls.append(client_msg_id)
        return list(self.lookups)


def _table_columns(path: Path, table: str) -> set[str]:
    conn = sqlite3.connect(path)
    try:
        return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Fresh live-winner race (the reproduced blocker)
# ---------------------------------------------------------------------------


def test_provider_fresh_live_claim_loser_must_not_lookup_or_terminalize(tmp_path):
    """Loser sees a live winner's fresh CLAIMED before create_run starts.

    Must return/poll CLAIMED. Must not lookup, create, adopt, or UNKNOWN.
    """
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    kwargs = _provider_kwargs(job)
    winner = ledger.claim_effect(**kwargs)
    assert winner.won is True
    assert winner.claim.status is EffectStatus.CLAIMED

    provider = FakeCursorProvider(FakeCreateResult(kind="lost_response"), lookups=[])
    loser = reconcile_cursor_create(ledger, provider, **kwargs)

    assert loser.status is EffectStatus.CLAIMED
    assert loser.unknown_reason is None
    assert loser.provider_run_id is None
    assert provider.lookup_calls == []
    assert provider.create_calls == []
    persisted = ledger.get_claim(job.job_id, "create_run")
    assert persisted is not None
    assert persisted.status is EffectStatus.CLAIMED


def test_slack_fresh_live_claim_loser_must_not_lookup_or_terminalize(tmp_path):
    """Loser sees a live winner's fresh CLAIMED before post_root starts.

    Must return/poll CLAIMED. Must not lookup, post, adopt, or UNKNOWN.
    """
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    ledger.bind(**_bind_kwargs(job.job_id))
    winner = ledger.claim_delivery(job.job_id)
    assert winner.won is True
    assert winner.binding.status is SlackRootStatus.CLAIMED

    port = FakeSlackPort(FakePostResult(kind="lost_response"), lookups=[])
    loser = deliver_slack_root(ledger, port, job_id=job.job_id)

    assert loser.status is SlackRootStatus.CLAIMED
    assert loser.unknown_reason is None
    assert loser.delivered_message_ts is None
    assert port.lookup_calls == []
    assert port.posts == []
    persisted = ledger.get_binding(job.job_id)
    assert persisted is not None
    assert persisted.status is SlackRootStatus.CLAIMED


def test_slack_loser_during_live_post_must_not_empty_lookup_unknown(tmp_path):
    """Threaded: loser runs while winner is inside post_root (side effect live)."""
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    ledger.bind(**_bind_kwargs(job.job_id))

    started = threading.Event()
    release = threading.Event()

    class GatePort(FakeSlackPort):
        def post_root(self, **kwargs) -> FakePostResult:
            started.set()
            assert release.wait(5.0), "winner was not released"
            return super().post_root(**kwargs)

    winner_port = GatePort(FakePostResult(kind="accepted", message_ts="42.1"))
    loser_port = FakeSlackPort(FakePostResult(kind="lost_response"), lookups=[])
    errors: list[BaseException] = []

    def winner() -> None:
        try:
            deliver_slack_root(ledger, winner_port, job_id=job.job_id)
        except BaseException as exc:  # noqa: BLE001 — surface into parent
            errors.append(exc)

    thread = threading.Thread(target=winner)
    thread.start()
    assert started.wait(5.0), "winner never reached post_root"
    loser = deliver_slack_root(ledger, loser_port, job_id=job.job_id)
    assert loser.status is SlackRootStatus.CLAIMED
    assert loser_port.lookup_calls == []
    assert loser_port.posts == []
    release.set()
    thread.join(timeout=5.0)
    assert errors == []
    loaded = ledger.get_binding(job.job_id)
    assert loaded is not None
    assert loaded.status is SlackRootStatus.DELIVERED
    assert loaded.delivered_message_ts == "42.1"
    assert len(winner_port.posts) == 1


def test_provider_reopen_of_fresh_claimed_is_unit_evidence_not_recovery(tmp_path):
    """Same-process reopen of an unexpired CLAIMED is not crash evidence.

    Must not lookup or terminalize. Recovery requires an expired lease.
    """
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path)
    ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    kwargs = _provider_kwargs(job)
    ledger.claim_effect(**kwargs)
    provider = FakeCursorProvider(FakeCreateResult(kind="lost_response"), lookups=[])
    reopened = ProviderEffectLedger(sqlite_path=store.sqlite_path)
    result = reconcile_cursor_create(reopened, provider, **kwargs)
    assert result.status is EffectStatus.CLAIMED
    assert provider.lookup_calls == []
    assert provider.create_calls == []


# ---------------------------------------------------------------------------
# Stale lease takeover after restart (unit evidence + API)
# ---------------------------------------------------------------------------


def test_provider_stale_lease_takeover_looks_up_by_key_never_creates(tmp_path):
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        provider_idempotency_key,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path)
    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    kwargs = _provider_kwargs(job)
    first = ledger.claim_effect(**kwargs)
    assert first.won is True
    original_token = first.owner_token
    assert original_token
    assert first.claim.claim_generation >= 1

    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    key = provider_idempotency_key(job.job_id, "create_run")
    provider = FakeCursorProvider(
        FakeCreateResult(kind="lost_response"),
        lookups=[FakeRun("run-unique", key)],
    )
    reopened = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    adopted = reconcile_cursor_create(reopened, provider, **kwargs)
    assert adopted.status is EffectStatus.ADOPTED
    assert adopted.provider_run_id == "run-unique"
    assert adopted.claim_owner_token != original_token
    assert adopted.claim_generation > first.claim.claim_generation
    assert provider.create_calls == []
    assert provider.lookup_calls == [key]


def test_slack_stale_lease_takeover_looks_up_by_client_msg_id_never_reposts(tmp_path):
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS
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
    first = ledger.claim_delivery(job.job_id)
    assert first.won is True
    original_token = first.owner_token
    assert original_token

    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    port = FakeSlackPort(
        FakePostResult(kind="lost_response"),
        lookups=[FakePosted("10.1", bound.outbound_client_msg_id)],
    )
    reopened = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    adopted = deliver_slack_root(reopened, port, job_id=job.job_id)
    assert adopted.status is SlackRootStatus.ADOPTED
    assert adopted.delivered_message_ts == "10.1"
    assert adopted.claim_owner_token != original_token
    assert adopted.claim_generation > first.binding.claim_generation
    assert port.posts == []
    assert port.lookup_calls == [bound.outbound_client_msg_id]


def test_unexpired_claim_takeover_is_rejected_without_lookup(tmp_path):
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS
    from agent.durable_jobs.effects import EffectStatus, ProviderEffectLedger

    store, job = _make_job(tmp_path)
    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    first = ledger.claim_effect(**_provider_kwargs(job))
    taken = ledger.takeover_stale_claim(job.job_id, "create_run")
    assert taken.won is False
    assert taken.claim.status is EffectStatus.CLAIMED
    assert taken.claim.claim_owner_token == first.owner_token
    assert taken.owner_token is None


# ---------------------------------------------------------------------------
# Old-owner fencing after takeover
# ---------------------------------------------------------------------------


def test_provider_old_owner_fenced_from_accepted_after_takeover(tmp_path):
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS
    from agent.durable_jobs.effects import EffectStatus, ProviderEffectLedger

    store, job = _make_job(tmp_path)
    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    first = ledger.claim_effect(**_provider_kwargs(job))
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    taken = ledger.takeover_stale_claim(job.job_id, "create_run")
    assert taken.won is True
    assert taken.owner_token != first.owner_token

    fenced = ledger.mark_accepted(
        job.job_id, "create_run", provider_run_id="run-from-old-owner",
        owner_token=first.owner_token,
    )
    assert fenced.status is EffectStatus.CLAIMED
    assert fenced.provider_run_id is None
    assert fenced.claim_owner_token == taken.owner_token

    accepted = ledger.mark_accepted(
        job.job_id, "create_run", provider_run_id="run-from-recovery",
        owner_token=taken.owner_token,
    )
    assert accepted.status is EffectStatus.ACCEPTED
    assert accepted.provider_run_id == "run-from-recovery"


def test_slack_old_owner_fenced_from_delivered_after_takeover(tmp_path):
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS
    from agent.durable_jobs.slack_contract import SlackBindingLedger, SlackRootStatus

    store, job = _make_job(tmp_path)
    clock = FrozenClock()
    ledger = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    ledger.bind(**_bind_kwargs(job.job_id))
    first = ledger.claim_delivery(job.job_id)
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    taken = ledger.takeover_stale_delivery(job.job_id)
    assert taken.won is True
    assert taken.owner_token != first.owner_token

    fenced = ledger.mark_delivered(
        job.job_id, "99.1", owner_token=first.owner_token
    )
    assert fenced.status is SlackRootStatus.CLAIMED
    assert fenced.delivered_message_ts is None
    assert fenced.claim_owner_token == taken.owner_token

    delivered = ledger.mark_delivered(
        job.job_id, "10.1", owner_token=taken.owner_token
    )
    assert delivered.status is SlackRootStatus.DELIVERED
    assert delivered.delivered_message_ts == "10.1"


# ---------------------------------------------------------------------------
# Schema evolution: fresh DB + reopen candidate-created v2 DB
# ---------------------------------------------------------------------------


_CANDIDATE_V2_SCHEMA = """
CREATE TABLE durable_jobs_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE durable_jobs (
    job_id TEXT PRIMARY KEY,
    phase TEXT NOT NULL,
    origin_platform TEXT NOT NULL,
    origin_chat_id TEXT NOT NULL,
    origin_root_thread_id TEXT NOT NULL,
    objective TEXT NOT NULL,
    repository_identity TEXT NOT NULL,
    frozen_baseline_sha TEXT NOT NULL DEFAULT '',
    idempotency_key TEXT NOT NULL UNIQUE,
    next_action TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE durable_job_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    idempotency_key TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(job_id, event_type, idempotency_key)
);
CREATE TABLE provider_effect_claims (
    job_id TEXT NOT NULL,
    action_id TEXT NOT NULL,
    provider_idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    provider_run_id TEXT,
    langgraph_thread_id TEXT NOT NULL,
    origin_platform TEXT NOT NULL,
    origin_chat_id TEXT NOT NULL,
    origin_root_thread_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    candidate_version TEXT NOT NULL,
    unknown_reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (job_id, action_id)
);
CREATE TABLE slack_job_bindings (
    job_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    root_thread_ts TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    candidate_version TEXT NOT NULL,
    outbound_client_msg_id TEXT NOT NULL UNIQUE,
    delivered_message_ts TEXT,
    status TEXT NOT NULL,
    unknown_reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def test_fresh_disposable_db_has_claim_lease_columns(tmp_path):
    from agent.durable_jobs.store import SCHEMA_VERSION, DurableJobStore

    path = _db(tmp_path)
    DurableJobStore(sqlite_path=path)
    provider_cols = _table_columns(path, "provider_effect_claims")
    slack_cols = _table_columns(path, "slack_job_bindings")
    for cols in (provider_cols, slack_cols):
        assert "claim_owner_token" in cols
        assert "claim_leased_at" in cols
        assert "claim_expires_at" in cols
        assert "claim_generation" in cols
        assert "recovery_attempt_count" in cols
        assert "recovery_started_at" in cols
        assert "recovery_deadline" in cols
    assert SCHEMA_VERSION >= 4


def test_reopen_candidate_v2_db_evolves_lease_columns_null_expiry_is_stale(tmp_path):
    """Reopening a candidate-created v2 DB must gain lease columns.

    Rows with NULL claim_expires_at are treated as stale so recovery can
    take them over. This is schema-evolution unit evidence, not a crash.
    """
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS
    from agent.durable_jobs.effects import EffectStatus, ProviderEffectLedger
    from agent.durable_jobs.store import DurableJobStore

    path = _db(tmp_path)
    now = "2026-01-01T00:00:00+00:00"
    conn = sqlite3.connect(path)
    try:
        conn.executescript(_CANDIDATE_V2_SCHEMA)
        conn.execute(
            "INSERT INTO durable_jobs_meta(key, value) VALUES('schema_version', '2')"
        )
        conn.execute(
            """
            INSERT INTO durable_jobs(
                job_id, phase, origin_platform, origin_chat_id,
                origin_root_thread_id, objective, repository_identity,
                frozen_baseline_sha, idempotency_key, next_action,
                created_at, updated_at
            ) VALUES (?, 'INTAKE', 'slack', 'C123', '111.222', 'v2',
                      'repo', '', 'idem-v2', 'freeze_baseline', ?, ?)
            """,
            ("dj_v2legacy", now, now),
        )
        conn.execute(
            """
            INSERT INTO provider_effect_claims(
                job_id, action_id, provider_idempotency_key, status,
                provider_run_id, langgraph_thread_id, origin_platform,
                origin_chat_id, origin_root_thread_id, candidate_id,
                candidate_version, unknown_reason, created_at, updated_at
            ) VALUES (?, 'create_run', 'cursor:dj_v2legacy:create_run',
                      'claimed', NULL, 'dj_v2legacy', 'slack', 'C123',
                      '111.222', 'cand-1', 'v1', NULL, ?, ?)
            """,
            ("dj_v2legacy", now, now),
        )
        conn.commit()
    finally:
        conn.close()

    before = _table_columns(path, "provider_effect_claims")
    assert "claim_owner_token" not in before

    DurableJobStore(sqlite_path=path)
    after_provider = _table_columns(path, "provider_effect_claims")
    after_slack = _table_columns(path, "slack_job_bindings")
    for cols in (after_provider, after_slack):
        assert "claim_owner_token" in cols
        assert "claim_expires_at" in cols
        assert "claim_generation" in cols

    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    loaded = ledger.get_claim("dj_v2legacy", "create_run")
    assert loaded is not None
    assert loaded.status is EffectStatus.CLAIMED
    taken = ledger.takeover_stale_claim("dj_v2legacy", "create_run")
    assert taken.won is True
    assert taken.owner_token
    assert taken.claim.claim_expires_at


def test_pilot_remains_default_off_and_state_stays_on_explicit_path(tmp_path):
    from agent.durable_jobs.config import (
        DEFAULT_DURABLE_JOBS_CONFIG,
        load_durable_jobs_config,
    )
    from agent.durable_jobs.store import DurableJobStore

    assert DEFAULT_DURABLE_JOBS_CONFIG["enabled"] is False
    cfg = load_durable_jobs_config({})
    assert cfg.enabled is False
    assert cfg.sqlite_path is None
    path = _db(tmp_path)
    DurableJobStore(sqlite_path=path)
    assert path.exists()
    home_state = Path.home() / ".hermes" / "state.db"
    assert path.resolve() != home_state.resolve()


# ---------------------------------------------------------------------------
# ENG-26/27 lease protocol: live-call expiry, heartbeat stop, delayed visibility
# ---------------------------------------------------------------------------


class _DelayedLookupProvider:
    """Lookup becomes uniquely visible only after ``visible`` is set."""

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        self.visible = False
        self.create_calls: list[str] = []
        self.lookup_calls: list[str] = []

    def create_run(self, *, idempotency_key: str, job_id: str):
        self.create_calls.append(idempotency_key)

        class _R:
            kind = "lost_response"
            run = None

        return _R()

    def lookup_runs(self, *, idempotency_key: str):
        self.lookup_calls.append(idempotency_key)
        if not self.visible:
            return []

        class _Run:
            run_id = self.run_id

        return [_Run()]


class _DelayedLookupPort:
    def __init__(self, message_ts: str) -> None:
        self.message_ts = message_ts
        self.visible = False
        self.posts: list[str] = []
        self.lookup_calls: list[str] = []

    def post_root(self, **kwargs):
        self.posts.append(kwargs["client_msg_id"])

        class _R:
            kind = "lost_response"
            message_ts = None

        return _R()

    def lookup_by_client_msg_id(self, client_msg_id: str):
        self.lookup_calls.append(client_msg_id)
        if not self.visible:
            return []

        class _Posted:
            message_ts = self.message_ts

        return [_Posted()]


def test_provider_expiry_during_live_create_does_not_unknown_while_owner_alive(tmp_path):
    """Winner paused inside create_run; clock past 30s lease; loser must poll.

    Must not takeover or terminalize UNKNOWN while the owner is still alive.
    """
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS, FrozenClock
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-live-provider")
    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    kwargs = _provider_kwargs(job)
    started = threading.Event()
    release = threading.Event()

    class GateProvider(FakeCursorProvider):
        def create_run(self, *, idempotency_key: str, job_id: str) -> FakeCreateResult:
            started.set()
            assert release.wait(5.0), "winner was not released"
            return super().create_run(idempotency_key=idempotency_key, job_id=job_id)

    winner_provider = GateProvider(
        FakeCreateResult(kind="accepted", run=FakeRun("run-live", "k"))
    )
    loser_provider = FakeCursorProvider(
        FakeCreateResult(kind="lost_response"), lookups=[]
    )
    errors: list[BaseException] = []

    def winner() -> None:
        try:
            reconcile_cursor_create(ledger, winner_provider, **kwargs)
        except BaseException as exc:  # noqa: BLE001 — surface into parent
            errors.append(exc)

    thread = threading.Thread(target=winner)
    thread.start()
    assert started.wait(5.0), "winner never reached create_run"
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    loser_ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    loser = reconcile_cursor_create(loser_ledger, loser_provider, **kwargs)
    assert loser.status is EffectStatus.CLAIMED
    assert loser.unknown_reason is None
    assert loser.provider_run_id is None
    assert loser_provider.lookup_calls == []
    assert loser_provider.create_calls == []
    persisted = ledger.get_claim(job.job_id, "create_run")
    assert persisted is not None
    assert persisted.status is EffectStatus.CLAIMED
    release.set()
    thread.join(timeout=5.0)
    assert errors == []
    done = ledger.get_claim(job.job_id, "create_run")
    assert done is not None
    assert done.status is EffectStatus.ACCEPTED
    assert done.provider_run_id == "run-live"


def test_slack_expiry_during_live_post_does_not_unknown_while_owner_alive(tmp_path):
    """Winner paused inside post_root; clock past 30s lease; loser must poll."""
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS, FrozenClock
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-live-slack")
    clock = FrozenClock()
    ledger = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    ledger.bind(**_bind_kwargs(job.job_id))
    started = threading.Event()
    release = threading.Event()

    class GatePort(FakeSlackPort):
        def post_root(self, **kwargs) -> FakePostResult:
            started.set()
            assert release.wait(5.0), "winner was not released"
            return super().post_root(**kwargs)

    winner_port = GatePort(FakePostResult(kind="accepted", message_ts="42.1"))
    loser_port = FakeSlackPort(FakePostResult(kind="lost_response"), lookups=[])
    errors: list[BaseException] = []

    def winner() -> None:
        try:
            deliver_slack_root(ledger, winner_port, job_id=job.job_id)
        except BaseException as exc:  # noqa: BLE001 — surface into parent
            errors.append(exc)

    thread = threading.Thread(target=winner)
    thread.start()
    assert started.wait(5.0), "winner never reached post_root"
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    loser_ledger = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    loser = deliver_slack_root(loser_ledger, loser_port, job_id=job.job_id)
    assert loser.status is SlackRootStatus.CLAIMED
    assert loser.unknown_reason is None
    assert loser.delivered_message_ts is None
    assert loser_port.lookup_calls == []
    assert loser_port.posts == []
    persisted = ledger.get_binding(job.job_id)
    assert persisted is not None
    assert persisted.status is SlackRootStatus.CLAIMED
    release.set()
    thread.join(timeout=5.0)
    assert errors == []
    done = ledger.get_binding(job.job_id)
    assert done is not None
    assert done.status is SlackRootStatus.DELIVERED
    assert done.delivered_message_ts == "42.1"


def test_provider_heartbeat_stop_then_expiry_allows_lookup_only_takeover(tmp_path):
    """Owner death is heartbeat stop: after expiry a second worker may take over."""
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS, FrozenClock
    from agent.durable_jobs.claim_protocol import owner_lease_heartbeat
    from agent.durable_jobs.effects import EffectStatus, ProviderEffectLedger

    store, job = _make_job(tmp_path, idempotency_key="idem-hb-stop-provider")
    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    first = ledger.claim_effect(**_provider_kwargs(job))
    assert first.won is True
    with owner_lease_heartbeat(
        renew_fn=lambda: ledger.renew_claim(
            job.job_id, "create_run", owner_token=first.owner_token
        ),
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    ):
        clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
        live = ledger.takeover_stale_claim(job.job_id, "create_run")
        assert live.won is False
        assert live.claim.claim_owner_token == first.owner_token
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    taken = ledger.takeover_stale_claim(job.job_id, "create_run")
    assert taken.won is True
    assert taken.owner_token != first.owner_token
    assert taken.claim.status is EffectStatus.CLAIMED


def test_slack_heartbeat_stop_then_expiry_allows_lookup_only_takeover(tmp_path):
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS, FrozenClock
    from agent.durable_jobs.claim_protocol import owner_lease_heartbeat
    from agent.durable_jobs.slack_contract import SlackBindingLedger, SlackRootStatus

    store, job = _make_job(tmp_path, idempotency_key="idem-hb-stop-slack")
    clock = FrozenClock()
    ledger = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    ledger.bind(**_bind_kwargs(job.job_id))
    first = ledger.claim_delivery(job.job_id)
    assert first.won is True
    with owner_lease_heartbeat(
        renew_fn=lambda: ledger.renew_delivery(
            job.job_id, owner_token=first.owner_token
        ),
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    ):
        clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
        live = ledger.takeover_stale_delivery(job.job_id)
        assert live.won is False
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    taken = ledger.takeover_stale_delivery(job.job_id)
    assert taken.won is True
    assert taken.owner_token != first.owner_token
    assert taken.binding.status is SlackRootStatus.CLAIMED


def test_provider_renew_cas_extends_lease_stale_token_rejected(tmp_path):
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS, FrozenClock
    from agent.durable_jobs.effects import ProviderEffectLedger

    store, job = _make_job(tmp_path, idempotency_key="idem-renew-cas-provider")
    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    first = ledger.claim_effect(**_provider_kwargs(job))
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS - 1)
    assert ledger.renew_claim(
        job.job_id, "create_run", owner_token=first.owner_token
    ) is True
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS - 1)
    blocked = ledger.takeover_stale_claim(job.job_id, "create_run")
    assert blocked.won is False
    clock.advance(2)
    taken = ledger.takeover_stale_claim(job.job_id, "create_run")
    assert taken.won is True
    assert ledger.renew_claim(
        job.job_id, "create_run", owner_token=first.owner_token
    ) is False
    assert ledger.renew_claim(
        job.job_id, "create_run", owner_token=taken.owner_token
    ) is True


def test_slack_renew_cas_extends_lease_stale_token_rejected(tmp_path):
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS, FrozenClock
    from agent.durable_jobs.slack_contract import SlackBindingLedger

    store, job = _make_job(tmp_path, idempotency_key="idem-renew-cas-slack")
    clock = FrozenClock()
    ledger = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    ledger.bind(**_bind_kwargs(job.job_id))
    first = ledger.claim_delivery(job.job_id)
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS - 1)
    assert ledger.renew_delivery(job.job_id, owner_token=first.owner_token) is True
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS - 1)
    blocked = ledger.takeover_stale_delivery(job.job_id)
    assert blocked.won is False
    clock.advance(2)
    taken = ledger.takeover_stale_delivery(job.job_id)
    assert taken.won is True
    assert ledger.renew_delivery(job.job_id, owner_token=first.owner_token) is False
    assert ledger.renew_delivery(job.job_id, owner_token=taken.owner_token) is True


def test_provider_delayed_visibility_after_takeover_does_not_false_negative(tmp_path):
    """Post-crash takeover: first empty lookup must not terminalize UNKNOWN."""
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS, FrozenClock
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-delay-provider")
    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    kwargs = _provider_kwargs(job)
    first = ledger.claim_effect(**kwargs)
    assert first.won is True
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    provider = _DelayedLookupProvider("run-late")
    recovered = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    empty = reconcile_cursor_create(recovered, provider, **kwargs)
    assert empty.status is EffectStatus.RECOVERING
    assert empty.unknown_reason is None
    assert empty.provider_run_id is None
    assert provider.create_calls == []
    assert len(provider.lookup_calls) == 1
    unknown_events = [
        event
        for event in store.list_events(job.job_id)
        if event["event_type"] == "provider_effect_unknown"
    ]
    assert unknown_events == []

    provider.visible = True
    adopted = reconcile_cursor_create(recovered, provider, **kwargs)
    assert adopted.status is EffectStatus.ADOPTED
    assert adopted.provider_run_id == "run-late"
    assert provider.create_calls == []
    assert len(provider.lookup_calls) == 2


def test_slack_delayed_visibility_after_takeover_does_not_false_negative(tmp_path):
    from agent.durable_jobs.clock import DEFAULT_CLAIM_LEASE_SECONDS, FrozenClock
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-delay-slack")
    clock = FrozenClock()
    ledger = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    ledger.bind(**_bind_kwargs(job.job_id))
    first = ledger.claim_delivery(job.job_id)
    assert first.won is True
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    port = _DelayedLookupPort("10.9")
    recovered = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    empty = deliver_slack_root(recovered, port, job_id=job.job_id)
    assert empty.status is SlackRootStatus.RECOVERING
    assert empty.unknown_reason is None
    assert empty.delivered_message_ts is None
    assert port.posts == []
    assert len(port.lookup_calls) == 1
    unknown_events = [
        event
        for event in store.list_events(job.job_id)
        if event["event_type"] == "slack_root_unknown"
    ]
    assert unknown_events == []

    port.visible = True
    adopted = deliver_slack_root(recovered, port, job_id=job.job_id)
    assert adopted.status is SlackRootStatus.ADOPTED
    assert adopted.delivered_message_ts == "10.9"
    assert port.posts == []
    assert len(port.lookup_calls) == 2


def test_provider_empty_recovery_bound_then_unknown_without_create(tmp_path):
    from agent.durable_jobs.clock import (
        DEFAULT_CLAIM_LEASE_SECONDS,
        DEFAULT_RECOVERY_MAX_ATTEMPTS,
        FrozenClock,
    )
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        UnknownReason,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-bound-provider")
    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    kwargs = _provider_kwargs(job)
    ledger.claim_effect(**kwargs)
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    provider = FakeCursorProvider(FakeCreateResult(kind="lost_response"), lookups=[])
    recovered = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    last = None
    for _ in range(DEFAULT_RECOVERY_MAX_ATTEMPTS - 1):
        last = reconcile_cursor_create(recovered, provider, **kwargs)
        assert last.status is EffectStatus.RECOVERING
        assert last.unknown_reason is None
    terminal = reconcile_cursor_create(recovered, provider, **kwargs)
    assert terminal.status is EffectStatus.UNKNOWN
    assert terminal.unknown_reason == UnknownReason.EMPTY_LOOKUP.value
    assert provider.create_calls == []
    unknown_events = [
        event
        for event in store.list_events(job.job_id)
        if event["event_type"] == "provider_effect_unknown"
    ]
    assert len(unknown_events) == 1


def test_slack_empty_recovery_bound_then_unknown_without_repost(tmp_path):
    from agent.durable_jobs.clock import (
        DEFAULT_CLAIM_LEASE_SECONDS,
        DEFAULT_RECOVERY_MAX_ATTEMPTS,
        FrozenClock,
    )
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        SlackUnknownReason,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-bound-slack")
    clock = FrozenClock()
    ledger = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    ledger.bind(**_bind_kwargs(job.job_id))
    ledger.claim_delivery(job.job_id)
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    port = FakeSlackPort(FakePostResult(kind="lost_response"), lookups=[])
    recovered = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    for _ in range(DEFAULT_RECOVERY_MAX_ATTEMPTS - 1):
        last = deliver_slack_root(recovered, port, job_id=job.job_id)
        assert last.status is SlackRootStatus.RECOVERING
        assert last.unknown_reason is None
    terminal = deliver_slack_root(recovered, port, job_id=job.job_id)
    assert terminal.status is SlackRootStatus.UNKNOWN
    assert terminal.unknown_reason == SlackUnknownReason.EMPTY_LOOKUP.value
    assert port.posts == []
    unknown_events = [
        event
        for event in store.list_events(job.job_id)
        if event["event_type"] == "slack_root_unknown"
    ]
    assert len(unknown_events) == 1
