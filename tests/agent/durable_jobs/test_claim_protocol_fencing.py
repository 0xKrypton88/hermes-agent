"""Adversarial lease/recovery ownership tests (isolated, default-off).

Covers independently reproduced defects:
1. Heartbeat swallows renew False/exception while create_run/post_root is
   in flight; a loser then empty-lookups to UNKNOWN although the external
   effect succeeds.
2. RECOVERING reuses the persisted owner token for any caller, so foreign
   connections can spend recovery attempts at one frozen instant.
3. renew_* succeeds after claim_expires_at if nobody has taken over yet.
4. Accepted Cancel that exists *before* reconcile/deliver still create_runs
   / posts (and recovery-looks-up). In-flight Cancel tests only fence bind
   after RPC has begun; they do not cover this call-out TOCTOU.

Deterministic FrozenClock + barriers only — no wall-clock sleeps.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from agent.durable_jobs.clock import (
    DEFAULT_CLAIM_LEASE_SECONDS,
    DEFAULT_RECOVERY_MAX_ATTEMPTS,
    DEFAULT_RECOVERY_WINDOW_SECONDS,
    FrozenClock,
)


def _db(tmp_path: Path) -> Path:
    return tmp_path / "pilot_jobs.sqlite"


def _make_job(tmp_path: Path, *, idempotency_key: str):
    from agent.durable_jobs.store import DurableJobStore

    store = DurableJobStore(sqlite_path=_db(tmp_path))
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="claim-protocol fencing",
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


@dataclass
class FakeRun:
    run_id: str
    idempotency_key: str


@dataclass
class FakeCreateResult:
    kind: str
    run: Optional[FakeRun] = None


@dataclass
class FakePosted:
    message_ts: str
    client_msg_id: str


@dataclass
class FakePostResult:
    kind: str
    message_ts: Optional[str] = None


class SwallowRenewProviderLedger:
    """renew_claim returns False and does not extend the lease."""

    def __init__(self, inner) -> None:
        self._inner = inner
        self.renew_results: list[object] = []

    def __getattr__(self, name: str):
        return getattr(self._inner, name)

    def renew_claim(self, job_id: str, action_id: str, *, owner_token: str) -> bool:
        self.renew_results.append(False)
        return False


class RaisingRenewProviderLedger:
    def __init__(self, inner) -> None:
        self._inner = inner
        self.renew_results: list[object] = []

    def __getattr__(self, name: str):
        return getattr(self._inner, name)

    def renew_claim(self, job_id: str, action_id: str, *, owner_token: str) -> bool:
        self.renew_results.append("error")
        raise RuntimeError("renew failed")


class RaisingRenewSlackLedger:
    def __init__(self, inner) -> None:
        self._inner = inner
        self.renew_results: list[object] = []

    def __getattr__(self, name: str):
        return getattr(self._inner, name)

    def renew_delivery(self, job_id: str, *, owner_token: str) -> bool:
        self.renew_results.append("error")
        raise RuntimeError("renew failed")


class SwallowRenewSlackLedger:
    def __init__(self, inner) -> None:
        self._inner = inner
        self.renew_results: list[object] = []

    def __getattr__(self, name: str):
        return getattr(self._inner, name)

    def renew_delivery(self, job_id: str, *, owner_token: str) -> bool:
        self.renew_results.append(False)
        return False


class GateCreateProvider:
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        self.started = threading.Event()
        self.release = threading.Event()
        self.create_calls: list[str] = []
        self.lookup_calls: list[str] = []
        self._posted = False

    def create_run(self, *, idempotency_key: str, job_id: str) -> FakeCreateResult:
        self.create_calls.append(idempotency_key)
        self.started.set()
        assert self.release.wait(5.0), "winner was not released"
        self._posted = True
        return FakeCreateResult(
            kind="accepted", run=FakeRun(self.run_id, idempotency_key)
        )

    def lookup_runs(self, *, idempotency_key: str) -> list[FakeRun]:
        self.lookup_calls.append(idempotency_key)
        if not self._posted:
            return []
        return [FakeRun(self.run_id, idempotency_key)]


class EmptyLookupProvider:
    def __init__(self) -> None:
        self.create_calls: list[str] = []
        self.lookup_calls: list[str] = []

    def create_run(self, *, idempotency_key: str, job_id: str) -> FakeCreateResult:
        self.create_calls.append(idempotency_key)
        return FakeCreateResult(kind="lost_response")

    def lookup_runs(self, *, idempotency_key: str) -> list[FakeRun]:
        self.lookup_calls.append(idempotency_key)
        return []


class GatePostPort:
    def __init__(self, message_ts: str) -> None:
        self.message_ts = message_ts
        self.started = threading.Event()
        self.release = threading.Event()
        self.posts: list[str] = []
        self.lookup_calls: list[str] = []
        self._posted = False

    def post_root(self, **kwargs) -> FakePostResult:
        self.posts.append(kwargs["client_msg_id"])
        self.started.set()
        assert self.release.wait(5.0), "winner was not released"
        self._posted = True
        return FakePostResult(kind="accepted", message_ts=self.message_ts)

    def lookup_by_client_msg_id(self, client_msg_id: str) -> list[FakePosted]:
        self.lookup_calls.append(client_msg_id)
        if not self._posted:
            return []
        return [FakePosted(self.message_ts, client_msg_id)]


class EmptyLookupPort:
    def __init__(self) -> None:
        self.posts: list[str] = []
        self.lookup_calls: list[str] = []

    def post_root(self, **kwargs) -> FakePostResult:
        self.posts.append(kwargs["client_msg_id"])
        return FakePostResult(kind="lost_response")

    def lookup_by_client_msg_id(self, client_msg_id: str) -> list[FakePosted]:
        self.lookup_calls.append(client_msg_id)
        return []


# ---------------------------------------------------------------------------
# Defect 1: heartbeat loss while side effect is in flight
# ---------------------------------------------------------------------------


def test_provider_heartbeat_loss_during_live_create_does_not_unknown(tmp_path):
    """Renew False is discarded today; loser UNKNOWNs while create_run succeeds."""
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-hb-loss-provider")
    clock = FrozenClock()
    inner = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    winner_ledger = SwallowRenewProviderLedger(inner)
    kwargs = _provider_kwargs(job)
    winner_provider = GateCreateProvider("run-exists")
    errors: list[BaseException] = []

    def winner() -> None:
        try:
            reconcile_cursor_create(winner_ledger, winner_provider, **kwargs)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=winner)
    thread.start()
    assert winner_provider.started.wait(5.0), "winner never reached create_run"
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    assert False in winner_ledger.renew_results

    loser_states: list[str] = []
    for _ in range(DEFAULT_RECOVERY_MAX_ATTEMPTS):
        loser_ledger = ProviderEffectLedger(
            sqlite_path=store.sqlite_path,
            now_fn=clock,
            lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
        )
        loser_provider = EmptyLookupProvider()
        state = reconcile_cursor_create(loser_ledger, loser_provider, **kwargs)
        loser_states.append(state.status.value)
        assert loser_provider.create_calls == []

    winner_provider.release.set()
    thread.join(timeout=5.0)
    assert errors == []

    persisted = inner.get_claim(job.job_id, "create_run")
    assert persisted is not None
    mapping = inner.get_mapping(job.job_id)
    unknown_events = [
        event
        for event in store.list_events(job.job_id)
        if event["event_type"] == "provider_effect_unknown"
    ]
    assert persisted.status is not EffectStatus.UNKNOWN, (
        f"loser states={loser_states} persisted={persisted.status} "
        f"run_id={persisted.provider_run_id}"
    )
    assert persisted.unknown_reason is None
    assert unknown_events == []
    assert persisted.status in (EffectStatus.ACCEPTED, EffectStatus.ADOPTED)
    assert persisted.provider_run_id == "run-exists"
    assert mapping is not None
    assert mapping.provider_run_id == "run-exists"


def test_provider_heartbeat_exception_during_live_create_is_observable(tmp_path):
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-hb-exc-provider")
    clock = FrozenClock()
    inner = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    winner_ledger = RaisingRenewProviderLedger(inner)
    kwargs = _provider_kwargs(job)
    winner_provider = GateCreateProvider("run-exists")
    errors: list[BaseException] = []

    def winner() -> None:
        try:
            reconcile_cursor_create(winner_ledger, winner_provider, **kwargs)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=winner)
    thread.start()
    assert winner_provider.started.wait(5.0)
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    assert "error" in winner_ledger.renew_results
    loser = reconcile_cursor_create(
        ProviderEffectLedger(
            sqlite_path=store.sqlite_path,
            now_fn=clock,
            lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
        ),
        EmptyLookupProvider(),
        **kwargs,
    )
    winner_provider.release.set()
    thread.join(timeout=5.0)
    assert errors == []
    persisted = inner.get_claim(job.job_id, "create_run")
    assert persisted is not None
    mapping = inner.get_mapping(job.job_id)
    assert loser.status is not EffectStatus.UNKNOWN
    assert persisted.status is not EffectStatus.UNKNOWN
    assert persisted.status in (EffectStatus.ACCEPTED, EffectStatus.ADOPTED)
    assert persisted.provider_run_id == "run-exists"
    assert mapping is not None
    assert mapping.provider_run_id == "run-exists"


def test_slack_heartbeat_loss_during_live_post_does_not_unknown(tmp_path):
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-hb-loss-slack")
    clock = FrozenClock()
    inner = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    inner.bind(**_bind_kwargs(job.job_id))
    winner_ledger = SwallowRenewSlackLedger(inner)
    winner_port = GatePostPort("42.1")
    errors: list[BaseException] = []

    def winner() -> None:
        try:
            deliver_slack_root(winner_ledger, winner_port, job_id=job.job_id)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=winner)
    thread.start()
    assert winner_port.started.wait(5.0), "winner never reached post_root"
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    assert False in winner_ledger.renew_results

    loser_states: list[str] = []
    for _ in range(DEFAULT_RECOVERY_MAX_ATTEMPTS):
        loser_ledger = SlackBindingLedger(
            sqlite_path=store.sqlite_path,
            now_fn=clock,
            lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
        )
        loser_port = EmptyLookupPort()
        state = deliver_slack_root(loser_ledger, loser_port, job_id=job.job_id)
        loser_states.append(state.status.value)
        assert loser_port.posts == []

    winner_port.release.set()
    thread.join(timeout=5.0)
    assert errors == []

    persisted = inner.get_binding(job.job_id)
    assert persisted is not None
    unknown_events = [
        event
        for event in store.list_events(job.job_id)
        if event["event_type"] == "slack_root_unknown"
    ]
    assert persisted.status is not SlackRootStatus.UNKNOWN, (
        f"loser states={loser_states} persisted={persisted.status} "
        f"ts={persisted.delivered_message_ts}"
    )
    assert persisted.unknown_reason is None
    assert unknown_events == []
    assert persisted.status in (SlackRootStatus.DELIVERED, SlackRootStatus.ADOPTED)
    assert persisted.delivered_message_ts == "42.1"


def test_slack_heartbeat_exception_during_live_post_is_observable(tmp_path):
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-hb-exc-slack")
    clock = FrozenClock()
    inner = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    inner.bind(**_bind_kwargs(job.job_id))
    winner_ledger = RaisingRenewSlackLedger(inner)
    winner_port = GatePostPort("42.1")
    errors: list[BaseException] = []

    def winner() -> None:
        try:
            deliver_slack_root(winner_ledger, winner_port, job_id=job.job_id)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=winner)
    thread.start()
    assert winner_port.started.wait(5.0)
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    assert "error" in winner_ledger.renew_results
    loser = deliver_slack_root(
        SlackBindingLedger(
            sqlite_path=store.sqlite_path,
            now_fn=clock,
            lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
        ),
        EmptyLookupPort(),
        job_id=job.job_id,
    )
    winner_port.release.set()
    thread.join(timeout=5.0)
    assert errors == []
    persisted = inner.get_binding(job.job_id)
    assert persisted is not None
    assert loser.status is not SlackRootStatus.UNKNOWN
    assert persisted.status is not SlackRootStatus.UNKNOWN
    assert persisted.status in (SlackRootStatus.DELIVERED, SlackRootStatus.ADOPTED)
    assert persisted.delivered_message_ts == "42.1"


# ---------------------------------------------------------------------------
# Defect 2: foreign callers spend another owner's recovery attempts
# ---------------------------------------------------------------------------


def test_provider_foreign_recovering_callers_do_not_spend_attempts(tmp_path):
    """Three separate connections at one frozen instant must not UNKNOWN."""
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-foreign-provider")
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
    owner = reconcile_cursor_create(ledger, EmptyLookupProvider(), **kwargs)
    assert owner.status is EffectStatus.RECOVERING
    attempts_after_owner = owner.recovery_attempt_count
    assert attempts_after_owner >= 1

    barrier = threading.Barrier(DEFAULT_RECOVERY_MAX_ATTEMPTS)
    results: List = []
    lookup_counts: list[int] = []

    def foreign() -> None:
        foreign_ledger = ProviderEffectLedger(
            sqlite_path=store.sqlite_path,
            now_fn=clock,
            lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
        )
        provider = EmptyLookupProvider()
        barrier.wait()
        results.append(reconcile_cursor_create(foreign_ledger, provider, **kwargs))
        lookup_counts.append(len(provider.lookup_calls))

    threads = [
        threading.Thread(target=foreign) for _ in range(DEFAULT_RECOVERY_MAX_ATTEMPTS)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5.0)

    assert len(results) == DEFAULT_RECOVERY_MAX_ATTEMPTS
    for state in results:
        assert state.status is EffectStatus.RECOVERING
        assert state.unknown_reason is None
    assert lookup_counts == [0] * DEFAULT_RECOVERY_MAX_ATTEMPTS
    persisted = ledger.get_claim(job.job_id, "create_run")
    assert persisted is not None
    assert persisted.status is EffectStatus.RECOVERING
    assert persisted.recovery_attempt_count == attempts_after_owner
    unknown_events = [
        event
        for event in store.list_events(job.job_id)
        if event["event_type"] == "provider_effect_unknown"
    ]
    assert unknown_events == []


def test_slack_foreign_recovering_callers_do_not_spend_attempts(tmp_path):
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-foreign-slack")
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
    owner = deliver_slack_root(ledger, EmptyLookupPort(), job_id=job.job_id)
    assert owner.status is SlackRootStatus.RECOVERING
    attempts_after_owner = owner.recovery_attempt_count

    barrier = threading.Barrier(DEFAULT_RECOVERY_MAX_ATTEMPTS)
    results: List = []
    lookup_counts: list[int] = []

    def foreign() -> None:
        foreign_ledger = SlackBindingLedger(
            sqlite_path=store.sqlite_path,
            now_fn=clock,
            lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
        )
        port = EmptyLookupPort()
        barrier.wait()
        results.append(deliver_slack_root(foreign_ledger, port, job_id=job.job_id))
        lookup_counts.append(len(port.lookup_calls))

    threads = [
        threading.Thread(target=foreign) for _ in range(DEFAULT_RECOVERY_MAX_ATTEMPTS)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5.0)

    assert len(results) == DEFAULT_RECOVERY_MAX_ATTEMPTS
    for state in results:
        assert state.status is SlackRootStatus.RECOVERING
        assert state.unknown_reason is None
    assert lookup_counts == [0] * DEFAULT_RECOVERY_MAX_ATTEMPTS
    persisted = ledger.get_binding(job.job_id)
    assert persisted is not None
    assert persisted.status is SlackRootStatus.RECOVERING
    assert persisted.recovery_attempt_count == attempts_after_owner
    unknown_events = [
        event
        for event in store.list_events(job.job_id)
        if event["event_type"] == "slack_root_unknown"
    ]
    assert unknown_events == []


# ---------------------------------------------------------------------------
# Defect 3: late renewal after expiry must return False
# ---------------------------------------------------------------------------


def test_provider_late_renew_after_expiry_returns_false_without_takeover(tmp_path):
    from agent.durable_jobs.effects import ProviderEffectLedger

    store, job = _make_job(tmp_path, idempotency_key="idem-late-renew-provider")
    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    first = ledger.claim_effect(**_provider_kwargs(job))
    assert first.won is True
    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    assert (
        ledger.renew_claim(
            job.job_id, "create_run", owner_token=first.owner_token
        )
        is False
    )
    taken = ledger.takeover_stale_claim(job.job_id, "create_run")
    assert taken.won is True
    assert taken.owner_token != first.owner_token


def test_slack_late_renew_after_expiry_returns_false_without_takeover(tmp_path):
    from agent.durable_jobs.slack_contract import SlackBindingLedger

    store, job = _make_job(tmp_path, idempotency_key="idem-late-renew-slack")
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
    assert ledger.renew_delivery(job.job_id, owner_token=first.owner_token) is False
    taken = ledger.takeover_stale_delivery(job.job_id)
    assert taken.won is True
    assert taken.owner_token != first.owner_token


# ---------------------------------------------------------------------------
# Remaining race: in-flight create/post still blocked past recovery deadline
# ---------------------------------------------------------------------------


def test_provider_inflight_past_recovery_deadline_does_not_unknown(tmp_path):
    """Lease + recovery window elapse while create_run is still blocked.

    d0de351 only delayed the old race by 90s: a foreign caller UNKNOWNs and
    the original cannot bind the run that then succeeds.
    """
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        UnknownReason,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-inflight-deadline-provider")
    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    kwargs = _provider_kwargs(job)
    winner_provider = GateCreateProvider("run-late")
    errors: list[BaseException] = []
    winner_result = []

    def winner() -> None:
        try:
            winner_result.append(
                reconcile_cursor_create(ledger, winner_provider, **kwargs)
            )
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=winner)
    thread.start()
    assert winner_provider.started.wait(5.0), "winner never reached create_run"

    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    recovering = reconcile_cursor_create(
        ProviderEffectLedger(
            sqlite_path=store.sqlite_path,
            now_fn=clock,
            lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
        ),
        EmptyLookupProvider(),
        **kwargs,
    )
    assert recovering.status is EffectStatus.RECOVERING

    clock.advance(DEFAULT_RECOVERY_WINDOW_SECONDS + 1)
    after_deadline = reconcile_cursor_create(
        ProviderEffectLedger(
            sqlite_path=store.sqlite_path,
            now_fn=clock,
            lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
        ),
        EmptyLookupProvider(),
        **kwargs,
    )
    assert after_deadline.status is not EffectStatus.UNKNOWN, (
        f"in-flight create still blocked; got {after_deadline.status} "
        f"{after_deadline.unknown_reason}"
    )
    assert after_deadline.unknown_reason is None
    unknown_events = [
        event
        for event in store.list_events(job.job_id)
        if event["event_type"] == "provider_effect_unknown"
    ]
    assert unknown_events == []

    winner_provider.release.set()
    thread.join(timeout=5.0)
    assert errors == []
    persisted = ledger.get_claim(job.job_id, "create_run")
    assert persisted is not None
    mapping = ledger.get_mapping(job.job_id)
    assert persisted.status is not EffectStatus.UNKNOWN
    assert persisted.unknown_reason != UnknownReason.EMPTY_LOOKUP.value
    assert persisted.status in (EffectStatus.ACCEPTED, EffectStatus.ADOPTED)
    assert persisted.provider_run_id == "run-late"
    assert mapping is not None
    assert mapping.provider_run_id == "run-late"
    assert winner_result
    assert winner_result[0].status in (EffectStatus.ACCEPTED, EffectStatus.ADOPTED)
    assert winner_result[0].provider_run_id == "run-late"


def test_slack_inflight_past_recovery_deadline_does_not_unknown(tmp_path):
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        SlackUnknownReason,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-inflight-deadline-slack")
    clock = FrozenClock()
    ledger = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    ledger.bind(**_bind_kwargs(job.job_id))
    winner_port = GatePostPort("42.9")
    errors: list[BaseException] = []
    winner_result = []

    def winner() -> None:
        try:
            winner_result.append(
                deliver_slack_root(ledger, winner_port, job_id=job.job_id)
            )
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=winner)
    thread.start()
    assert winner_port.started.wait(5.0), "winner never reached post_root"

    clock.advance(DEFAULT_CLAIM_LEASE_SECONDS + 1)
    recovering = deliver_slack_root(
        SlackBindingLedger(
            sqlite_path=store.sqlite_path,
            now_fn=clock,
            lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
        ),
        EmptyLookupPort(),
        job_id=job.job_id,
    )
    assert recovering.status is SlackRootStatus.RECOVERING

    clock.advance(DEFAULT_RECOVERY_WINDOW_SECONDS + 1)
    after_deadline = deliver_slack_root(
        SlackBindingLedger(
            sqlite_path=store.sqlite_path,
            now_fn=clock,
            lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
        ),
        EmptyLookupPort(),
        job_id=job.job_id,
    )
    assert after_deadline.status is not SlackRootStatus.UNKNOWN, (
        f"in-flight post still blocked; got {after_deadline.status} "
        f"{after_deadline.unknown_reason}"
    )
    assert after_deadline.unknown_reason is None
    unknown_events = [
        event
        for event in store.list_events(job.job_id)
        if event["event_type"] == "slack_root_unknown"
    ]
    assert unknown_events == []

    winner_port.release.set()
    thread.join(timeout=5.0)
    assert errors == []
    persisted = ledger.get_binding(job.job_id)
    assert persisted is not None
    assert persisted.status is not SlackRootStatus.UNKNOWN
    assert persisted.unknown_reason != SlackUnknownReason.EMPTY_LOOKUP.value
    assert persisted.status in (SlackRootStatus.DELIVERED, SlackRootStatus.ADOPTED)
    assert persisted.delivered_message_ts == "42.9"
    assert winner_result
    assert winner_result[0].status in (
        SlackRootStatus.DELIVERED,
        SlackRootStatus.ADOPTED,
    )
    assert winner_result[0].delivered_message_ts == "42.9"


def test_provider_cancel_blocks_inflight_bind_and_stays_terminal(tmp_path):
    """Cancel is terminal: in-flight completion must not overwrite it."""
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        reconcile_cursor_create,
    )
    from agent.durable_jobs.slack_contract import SlackBindingLedger

    store, job = _make_job(tmp_path, idempotency_key="idem-cancel-inflight-provider")
    SlackBindingLedger(sqlite_path=store.sqlite_path).bind(**_bind_kwargs(job.job_id))
    decisions = DecisionLedger(sqlite_path=store.sqlite_path)
    decisions.set_policy(
        job_id=job.job_id,
        policy_version="pol-1",
        allowed_actors=("U-alice",),
        expires_at="2099-01-01T00:00:00+00:00",
    )
    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    kwargs = _provider_kwargs(job)
    winner_provider = GateCreateProvider("run-canceled")
    errors: list[BaseException] = []

    def winner() -> None:
        try:
            reconcile_cursor_create(ledger, winner_provider, **kwargs)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=winner)
    thread.start()
    assert winner_provider.started.wait(5.0)
    canceled = decisions.record_decision(
        job_id=job.job_id,
        decision_type="cancel",
        candidate_id="cand-1",
        candidate_version="v1",
        actor_id="U-alice",
        policy_version="pol-1",
        decision_idempotency_key="k-cancel-inflight",
    )
    assert canceled.ok is True
    assert decisions.is_canceled(job.job_id) is True
    winner_provider.release.set()
    thread.join(timeout=5.0)
    assert errors == []
    persisted = ledger.get_claim(job.job_id, "create_run")
    assert persisted is not None
    assert persisted.status is not EffectStatus.ACCEPTED
    assert persisted.status is not EffectStatus.ADOPTED
    assert decisions.is_canceled(job.job_id) is True
    go_after = decisions.record_decision(
        job_id=job.job_id,
        decision_type="go",
        candidate_id="cand-1",
        candidate_version="v1",
        actor_id="U-alice",
        policy_version="pol-1",
        decision_idempotency_key="k-go-after-cancel",
    )
    assert go_after.ok is False
    assert "canceled" in go_after.reason_codes


def test_slack_cancel_blocks_inflight_bind_and_stays_terminal(tmp_path):
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-cancel-inflight-slack")
    decisions = DecisionLedger(sqlite_path=store.sqlite_path)
    decisions.set_policy(
        job_id=job.job_id,
        policy_version="pol-1",
        allowed_actors=("U-alice",),
        expires_at="2099-01-01T00:00:00+00:00",
    )
    clock = FrozenClock()
    ledger = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    ledger.bind(**_bind_kwargs(job.job_id))
    winner_port = GatePostPort("42.0")
    errors: list[BaseException] = []

    def winner() -> None:
        try:
            deliver_slack_root(ledger, winner_port, job_id=job.job_id)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=winner)
    thread.start()
    assert winner_port.started.wait(5.0)
    canceled = decisions.record_decision(
        job_id=job.job_id,
        decision_type="cancel",
        candidate_id="cand-1",
        candidate_version="v1",
        actor_id="U-alice",
        policy_version="pol-1",
        decision_idempotency_key="k-cancel-inflight-slack",
    )
    assert canceled.ok is True
    winner_port.release.set()
    thread.join(timeout=5.0)
    assert errors == []
    persisted = ledger.get_binding(job.job_id)
    assert persisted is not None
    assert persisted.status is not SlackRootStatus.DELIVERED
    assert persisted.status is not SlackRootStatus.ADOPTED
    assert decisions.is_canceled(job.job_id) is True
    go_after = decisions.record_decision(
        job_id=job.job_id,
        decision_type="go",
        candidate_id="cand-1",
        candidate_version="v1",
        actor_id="U-alice",
        policy_version="pol-1",
        decision_idempotency_key="k-go-after-cancel-slack",
    )
    assert go_after.ok is False
    assert "canceled" in go_after.reason_codes


def test_provider_accepted_cancel_before_reconcile_does_not_call_out(tmp_path):
    """Accepted Cancel before reconcile_cursor_create must not create or lookup."""
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        reconcile_cursor_create,
    )
    from agent.durable_jobs.slack_contract import SlackBindingLedger

    store, job = _make_job(tmp_path, idempotency_key="idem-cancel-before-provider")
    SlackBindingLedger(sqlite_path=store.sqlite_path).bind(**_bind_kwargs(job.job_id))
    decisions = DecisionLedger(sqlite_path=store.sqlite_path)
    decisions.set_policy(
        job_id=job.job_id,
        policy_version="pol-1",
        allowed_actors=("U-alice",),
        expires_at="2099-01-01T00:00:00+00:00",
    )
    canceled = decisions.record_decision(
        job_id=job.job_id,
        decision_type="cancel",
        candidate_id="cand-1",
        candidate_version="v1",
        actor_id="U-alice",
        policy_version="pol-1",
        decision_idempotency_key="k-cancel-before-provider",
    )
    assert canceled.ok is True
    assert decisions.is_canceled(job.job_id) is True

    clock = FrozenClock()
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    provider = EmptyLookupProvider()
    caught: list[BaseException] = []
    try:
        reconcile_cursor_create(ledger, provider, **_provider_kwargs(job))
    except BaseException as exc:  # noqa: BLE001
        caught.append(exc)

    assert provider.create_calls == [], "provider create executed after accepted Cancel"
    assert provider.lookup_calls == [], "provider lookup executed after accepted Cancel"
    from agent.durable_jobs.decisions import JobCanceledError

    assert caught and isinstance(caught[0], JobCanceledError)
    persisted = ledger.get_claim(job.job_id, "create_run")
    assert persisted is None or persisted.status not in (
        EffectStatus.ACCEPTED,
        EffectStatus.ADOPTED,
        EffectStatus.UNKNOWN,
    )
    assert persisted is None
    assert decisions.is_canceled(job.job_id) is True
    go_after = decisions.record_decision(
        job_id=job.job_id,
        decision_type="go",
        candidate_id="cand-1",
        candidate_version="v1",
        actor_id="U-alice",
        policy_version="pol-1",
        decision_idempotency_key="k-go-after-cancel-before-provider",
    )
    assert go_after.ok is False
    assert "canceled" in go_after.reason_codes


def test_slack_accepted_cancel_before_deliver_does_not_call_out(tmp_path):
    """Accepted Cancel before deliver_slack_root must not post or lookup."""
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-cancel-before-slack")
    decisions = DecisionLedger(sqlite_path=store.sqlite_path)
    decisions.set_policy(
        job_id=job.job_id,
        policy_version="pol-1",
        allowed_actors=("U-alice",),
        expires_at="2099-01-01T00:00:00+00:00",
    )
    clock = FrozenClock()
    ledger = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=clock,
        lease_seconds=DEFAULT_CLAIM_LEASE_SECONDS,
    )
    ledger.bind(**_bind_kwargs(job.job_id))
    canceled = decisions.record_decision(
        job_id=job.job_id,
        decision_type="cancel",
        candidate_id="cand-1",
        candidate_version="v1",
        actor_id="U-alice",
        policy_version="pol-1",
        decision_idempotency_key="k-cancel-before-slack",
    )
    assert canceled.ok is True
    assert decisions.is_canceled(job.job_id) is True

    port = EmptyLookupPort()
    caught: list[BaseException] = []
    try:
        deliver_slack_root(ledger, port, job_id=job.job_id)
    except BaseException as exc:  # noqa: BLE001
        caught.append(exc)

    assert port.posts == [], "slack post executed after accepted Cancel"
    assert port.lookup_calls == [], "slack lookup executed after accepted Cancel"
    from agent.durable_jobs.decisions import JobCanceledError

    assert caught and isinstance(caught[0], JobCanceledError)
    persisted = ledger.get_binding(job.job_id)
    assert persisted is not None
    assert persisted.status is SlackRootStatus.BOUND
    assert persisted.status is not SlackRootStatus.DELIVERED
    assert persisted.status is not SlackRootStatus.ADOPTED
    assert persisted.status is not SlackRootStatus.CLAIMED
    assert persisted.status is not SlackRootStatus.UNKNOWN
    assert decisions.is_canceled(job.job_id) is True
    go_after = decisions.record_decision(
        job_id=job.job_id,
        decision_type="go",
        candidate_id="cand-1",
        candidate_version="v1",
        actor_id="U-alice",
        policy_version="pol-1",
        decision_idempotency_key="k-go-after-cancel-before-slack",
    )
    assert go_after.ok is False
    assert "canceled" in go_after.reason_codes
