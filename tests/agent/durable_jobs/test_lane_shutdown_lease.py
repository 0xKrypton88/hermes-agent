"""DurableLaneService close/mutation-lease invariant (ENG-36 Package 2).

Linearization: a mutation lease is acquired under the same lock that
``close()`` uses to set ``_closed``. Store checkout is not a lease.
Losers return typed pending/retryable (consume) or LaneClosedError
(other writers) with zero durable writes, zero effects, and zero ACK.

No live Slack/Cursor/network. Barriers use threading.Event, not sleeps.
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from tests.agent.durable_jobs.eng28_support import RecordingAckPort, count_table


def _complete(tmp_path: Path, **overrides) -> dict:
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
        "policy_version": "pol-1",
        "identity_binding": {
            "workspace_id": "T1",
            "repository_identity": "github.com/example/repo",
        },
    }
    section.update(overrides)
    return {"durable_jobs": section}


def _seed(tmp_path: Path, *, idempotency_key: str = "idem-lease"):
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.lane import DurableLaneService
    from agent.durable_jobs.slack_contract import SlackBindingLedger
    from agent.durable_jobs.store import DurableJobStore

    cfg = load_durable_jobs_config(_complete(tmp_path))
    store = DurableJobStore(sqlite_path=cfg.sqlite_path)
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="lease-invariant",
        repository_identity="github.com/example/repo",
        idempotency_key=idempotency_key,
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
    lane = DurableLaneService(config=cfg, store=store)
    return lane, job, store


def _inbound(job, **overrides):
    payload = dict(
        job_id=job.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        actor_id="U-alice",
        decision_type="go",
        decision_idempotency_key="dec-lease",
        policy_version="pol-1",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    payload.update(overrides)
    return payload


def _assert_loser_consume(result, ack, store, lane):
    assert result.ok is False
    assert result.retryable is True
    assert result.ack_status == "pending"
    assert ack.acks == []
    assert lane._closed is True
    assert lane._store is None
    assert count_table(store.sqlite_path, "job_inbound_actions") == 0
    assert count_table(store.sqlite_path, "job_decisions") == 0


def _assert_no_reopen(lane):
    from agent.durable_jobs.lane import LaneClosedError

    with pytest.raises(LaneClosedError):
        lane._require_sqlite_path()
    assert lane._store is None


def test_A_close_before_admission_is_pending_zero_write(tmp_path):
    lane, job, store = _seed(tmp_path, idempotency_key="idem-A")
    ack = RecordingAckPort()
    lane.close()
    result = lane.consume_inbound_action(ack, **_inbound(job, decision_idempotency_key="dec-A"))
    _assert_loser_consume(result, ack, store, lane)
    _assert_no_reopen(lane)


def test_B_close_between_admission_and_store_acquisition(tmp_path):
    from agent.durable_jobs.lane import DurableLaneService

    class _CloseBeforeRequire(DurableLaneService):
        def _require_sqlite_path(self):
            DurableLaneService.close(self)
            return DurableLaneService._require_sqlite_path(self)

    base, job, store = _seed(tmp_path, idempotency_key="idem-B")
    lane = _CloseBeforeRequire(config=base.config, store=store)
    ack = RecordingAckPort()
    result = lane.consume_inbound_action(ack, **_inbound(job, decision_idempotency_key="dec-B"))
    _assert_loser_consume(result, ack, store, lane)
    _assert_no_reopen(lane)


def test_C_close_immediately_after_store_checkout_before_identity(tmp_path):
    """Verified parent gap: close after store hand-out, before repo/coordinator."""
    lane, job, store = _seed(tmp_path, idempotency_key="idem-C")
    ack = RecordingAckPort()
    original = lane._require_sqlite_path

    def _checkout_then_close():
        checked = original()
        lane.close()
        return checked

    lane._require_sqlite_path = _checkout_then_close
    result = lane.consume_inbound_action(ack, **_inbound(job, decision_idempotency_key="dec-C"))
    _assert_loser_consume(result, ack, store, lane)
    _assert_no_reopen(lane)


def test_D_close_before_identity_policy_validation(tmp_path):
    lane, job, store = _seed(tmp_path, idempotency_key="idem-D")
    ack = RecordingAckPort()
    original = lane._repository_identity_rejected

    def _close_then_validate(checked, job_id):
        lane.close()
        return original(checked, job_id)

    lane._repository_identity_rejected = _close_then_validate
    result = lane.consume_inbound_action(ack, **_inbound(job, decision_idempotency_key="dec-D"))
    _assert_loser_consume(result, ack, store, lane)
    _assert_no_reopen(lane)


def test_E_close_before_coordinator_transaction_entry(tmp_path):
    lane, job, store = _seed(tmp_path, idempotency_key="idem-E")
    ack = RecordingAckPort()
    original = lane._repository_identity_rejected

    def _validate_then_close(checked, job_id):
        rejected = original(checked, job_id)
        lane.close()
        return rejected

    lane._repository_identity_rejected = _validate_then_close
    result = lane.consume_inbound_action(ack, **_inbound(job, decision_idempotency_key="dec-E"))
    _assert_loser_consume(result, ack, store, lane)
    _assert_no_reopen(lane)


def test_F_close_during_persist_does_not_write_after_close_returns(tmp_path):
    """In-flight persist either wins the lease (write before close returns) or
    does not exist. Writing after close() has returned is forbidden.
    """
    from agent.durable_jobs import coordinator as coordinator_mod

    lane, job, store = _seed(tmp_path, idempotency_key="idem-F")
    ack = RecordingAckPort()
    close_entered = threading.Event()
    close_returned = threading.Event()
    persist_continued_after_close_return = []
    previous = coordinator_mod.after_inbound_select_before_insert

    def _barrier():
        def _closer():
            close_entered.set()
            lane.close()
            close_returned.set()

        threading.Thread(target=_closer, name="lease-F-close").start()
        assert close_entered.wait(timeout=5)
        if close_returned.is_set():
            persist_continued_after_close_return.append(True)
        return previous()

    coordinator_mod.after_inbound_select_before_insert = _barrier
    try:
        result = lane.consume_inbound_action(
            ack, **_inbound(job, decision_idempotency_key="dec-F")
        )
    finally:
        coordinator_mod.after_inbound_select_before_insert = previous
    assert close_returned.wait(timeout=5)
    assert persist_continued_after_close_return == []
    assert result.ok is True
    assert result.ack_status == "acked"
    assert len(ack.acks) == 1
    assert count_table(store.sqlite_path, "job_inbound_actions") == 1
    assert count_table(store.sqlite_path, "job_decisions") == 1
    assert lane._closed is True
    assert lane._store is None


def test_G_close_after_commit_before_ack_is_single_effect_winner(tmp_path):
    """Lease spans durable commit through ACK. close waits; no second write."""
    lane, job, store = _seed(tmp_path, idempotency_key="idem-G")
    close_entered = threading.Event()
    close_returned = threading.Event()
    ack_after_close_return = []
    inner = RecordingAckPort()

    class _Ack:
        def ack(self, *, inbound_id: str, job_id: str) -> str:
            def _closer():
                close_entered.set()
                lane.close()
                close_returned.set()

            threading.Thread(target=_closer, name="lease-G-close").start()
            assert close_entered.wait(timeout=5)
            if close_returned.is_set():
                ack_after_close_return.append(True)
            return inner.ack(inbound_id=inbound_id, job_id=job_id)

    result = lane.consume_inbound_action(
        _Ack(), **_inbound(job, decision_idempotency_key="dec-G")
    )
    assert close_returned.wait(timeout=5)
    assert ack_after_close_return == []
    assert result.ok is True
    assert result.ack_status == "acked"
    assert len(inner.acks) == 1
    assert count_table(store.sqlite_path, "job_inbound_actions") == 1
    assert count_table(store.sqlite_path, "job_decisions") == 1
    replay = RecordingAckPort()
    again = lane.consume_inbound_action(
        replay, **_inbound(job, decision_idempotency_key="dec-G")
    )
    assert again.ok is False
    assert again.retryable is True
    assert replay.acks == []
    assert count_table(store.sqlite_path, "job_inbound_actions") == 1
    assert count_table(store.sqlite_path, "job_decisions") == 1


def test_H_all_mutating_entrypoints_fail_closed_after_close(tmp_path):
    from agent.durable_jobs.lane import LaneClosedError

    lane, job, store = _seed(tmp_path, idempotency_key="idem-H")
    inbound_before = count_table(store.sqlite_path, "job_inbound_actions")
    decisions_before = count_table(store.sqlite_path, "job_decisions")
    lane.close()
    ack = RecordingAckPort()
    provider_calls: list[str] = []
    slack_calls: list[str] = []
    errors: list[str] = []

    class _Slack:
        def post_root(self, **_k):
            slack_calls.append("post_root")
            raise AssertionError("slack effect after close")

        def lookup_by_client_msg_id(self, client_msg_id: str):
            slack_calls.append("lookup")
            raise AssertionError("slack effect after close")

    class _Provider:
        def create_run(self, **_k):
            provider_calls.append("create_run")
            raise AssertionError("provider effect after close")

        def lookup_runs(self, **_k):
            provider_calls.append("lookup_runs")
            return []

    def _run(name, fn):
        try:
            fn()
        except LaneClosedError:
            errors.append(name)
        except Exception as exc:
            errors.append(f"{name}:{type(exc).__name__}")

    workers = [
        threading.Thread(
            target=_run,
            args=(
                "consume",
                lambda: lane.consume_inbound_action(
                    ack, **_inbound(job, decision_idempotency_key="dec-H")
                ),
            ),
        ),
        threading.Thread(
            target=_run,
            args=(
                "bind",
                lambda: lane.bind_slack(
                    job_id=job.job_id,
                    workspace_id="T1",
                    channel_id="C123",
                    root_thread_ts="111.222",
                    candidate_id="cand-1",
                    candidate_version="v1",
                ),
            ),
        ),
        threading.Thread(
            target=_run,
            args=(
                "deliver",
                lambda: lane.deliver_slack_root(job_id=job.job_id, slack_port=_Slack()),
            ),
        ),
        threading.Thread(
            target=_run,
            args=(
                "reconcile",
                lambda: lane.reconcile_cursor_create(
                    job_id=job.job_id,
                    action_id="create_run",
                    origin_platform="slack",
                    origin_chat_id="C123",
                    origin_root_thread_id="111.222",
                    candidate_id="cand-1",
                    candidate_version="v1",
                    provider=_Provider(),
                ),
            ),
        ),
        threading.Thread(
            target=_run,
            args=(
                "policy",
                lambda: lane.set_job_policy(
                    job_id=job.job_id,
                    policy_version="pol-2",
                    allowed_actors=("U-alice",),
                ),
            ),
        ),
        threading.Thread(
            target=_run,
            args=(
                "decision",
                lambda: lane.record_decision(
                    job_id=job.job_id,
                    decision_type="hold",
                    candidate_id="cand-1",
                    candidate_version="v1",
                    actor_id="U-alice",
                    policy_version="pol-1",
                    decision_idempotency_key="dec-H-direct",
                ),
            ),
        ),
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=5)
        assert not worker.is_alive()
    consume = lane.consume_inbound_action(
        ack, **_inbound(job, decision_idempotency_key="dec-H2")
    )
    assert consume.ok is False
    assert consume.retryable is True
    assert ack.acks == []
    assert slack_calls == []
    assert provider_calls == []
    assert count_table(store.sqlite_path, "job_inbound_actions") == inbound_before
    assert count_table(store.sqlite_path, "job_decisions") == decisions_before
    _assert_no_reopen(lane)


def test_I_repeated_close_is_idempotent(tmp_path):
    lane, job, store = _seed(tmp_path, idempotency_key="idem-I")
    lane.close()
    lane.close()
    lane.close()
    ack = RecordingAckPort()
    result = lane.consume_inbound_action(ack, **_inbound(job, decision_idempotency_key="dec-I"))
    _assert_loser_consume(result, ack, store, lane)
    _assert_no_reopen(lane)


def test_J_close_does_not_reconstruct_store(tmp_path, monkeypatch):
    from agent.durable_jobs.store import DurableJobStore

    lane, job, store = _seed(tmp_path, idempotency_key="idem-J")
    constructed: list[str] = []
    real_init = DurableJobStore.__init__

    def _wrap(self, *a, **k):
        constructed.append("store")
        return real_init(self, *a, **k)

    monkeypatch.setattr(DurableJobStore, "__init__", _wrap)
    monkeypatch.setattr("agent.durable_jobs.lane.DurableJobStore.__init__", _wrap)
    lane.close()
    constructed.clear()
    ack = RecordingAckPort()
    result = lane.consume_inbound_action(ack, **_inbound(job, decision_idempotency_key="dec-J"))
    _assert_loser_consume(result, ack, store, lane)
    assert constructed == []
    _assert_no_reopen(lane)


def test_K_cross_repo_and_missing_binding_remain_zero_write_reject(tmp_path):
    from tests.agent.durable_jobs.test_package2_gates import (
        test_lane_consume_rejects_cross_repo_job_with_zero_writes_or_ack,
        test_lane_consume_rejects_missing_identity_binding_with_zero_writes,
    )

    cross = tmp_path / "cross"
    missing = tmp_path / "missing"
    cross.mkdir()
    missing.mkdir()
    test_lane_consume_rejects_cross_repo_job_with_zero_writes_or_ack(cross)
    test_lane_consume_rejects_missing_identity_binding_with_zero_writes(missing)


def test_winner_without_close_still_persists_once(tmp_path):
    lane, job, store = _seed(tmp_path, idempotency_key="idem-winner")
    ack = RecordingAckPort()
    result = lane.consume_inbound_action(
        ack, **_inbound(job, decision_idempotency_key="dec-winner")
    )
    assert result.ok is True
    assert result.ack_status == "acked"
    assert len(ack.acks) == 1
    assert count_table(store.sqlite_path, "job_inbound_actions") == 1
    assert count_table(store.sqlite_path, "job_decisions") == 1
    assert lane._closed is False


def _checkout_then_close(lane):
    original = lane._require_sqlite_path

    def _wrapped():
        checked = original()
        lane.close()
        return checked

    lane._require_sqlite_path = _wrapped


def test_C_writers_close_after_checkout_zero_effect(tmp_path):
    """Same verified gap as C, for every non-consume mutating entrypoint."""
    from agent.durable_jobs.lane import DurableLaneService, LaneClosedError
    from agent.durable_jobs.store import DurableJobStore

    template, job, store = _seed(tmp_path, idempotency_key="idem-C-writers")
    other = DurableJobStore(sqlite_path=store.sqlite_path).create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="333.444",
        objective="unbound-writer",
        repository_identity="github.com/example/repo",
        idempotency_key="idem-C-writers-other",
    )
    bindings_before = count_table(store.sqlite_path, "slack_job_bindings")
    policies_before = count_table(store.sqlite_path, "job_authz_policies")
    decisions_before = count_table(store.sqlite_path, "job_decisions")
    effects_before = count_table(store.sqlite_path, "provider_effect_claims")
    events_before = count_table(store.sqlite_path, "durable_job_events")
    slack_calls: list[str] = []
    provider_calls: list[str] = []

    class _Slack:
        def post_root(self, **_k):
            slack_calls.append("post_root")
            raise AssertionError("slack effect after close")

        def lookup_by_client_msg_id(self, client_msg_id: str):
            slack_calls.append("lookup")
            raise AssertionError("slack effect after close")

    class _Provider:
        def create_run(self, **_k):
            provider_calls.append("create_run")
            raise AssertionError("provider effect after close")

        def lookup_runs(self, **_k):
            provider_calls.append("lookup_runs")
            return []

    def _fresh():
        lane = DurableLaneService(config=template.config, store=store)
        _checkout_then_close(lane)
        return lane

    with pytest.raises(LaneClosedError):
        _fresh().bind_slack(
            job_id=other.job_id,
            workspace_id="T1",
            channel_id="C123",
            root_thread_ts="333.444",
            candidate_id="cand-1",
            candidate_version="v1",
        )
    with pytest.raises(LaneClosedError):
        _fresh().deliver_slack_root(job_id=job.job_id, slack_port=_Slack())
    with pytest.raises(LaneClosedError):
        _fresh().reconcile_cursor_create(
            job_id=job.job_id,
            action_id="create_run",
            origin_platform="slack",
            origin_chat_id="C123",
            origin_root_thread_id="111.222",
            candidate_id="cand-1",
            candidate_version="v1",
            provider=_Provider(),
        )
    with pytest.raises(LaneClosedError):
        _fresh().set_job_policy(
            job_id=job.job_id,
            policy_version="pol-2",
            allowed_actors=("U-alice",),
        )
    with pytest.raises(LaneClosedError):
        _fresh().record_decision(
            job_id=job.job_id,
            decision_type="hold",
            candidate_id="cand-1",
            candidate_version="v1",
            actor_id="U-alice",
            policy_version="pol-1",
            decision_idempotency_key="dec-C-direct",
        )
    assert slack_calls == []
    assert provider_calls == []
    assert count_table(store.sqlite_path, "slack_job_bindings") == bindings_before
    assert count_table(store.sqlite_path, "job_authz_policies") == policies_before
    assert count_table(store.sqlite_path, "job_decisions") == decisions_before
    assert count_table(store.sqlite_path, "provider_effect_claims") == effects_before
    assert count_table(store.sqlite_path, "durable_job_events") == events_before
    closed = DurableLaneService(config=template.config, store=store)
    closed.close()
    _assert_no_reopen(closed)


def test_close_one_seam_after_checkout_and_before_lease(tmp_path):
    """Move shutdown one seam later than C and one earlier than coordinator."""
    lane, job, store = _seed(tmp_path, idempotency_key="idem-seams")
    ack = RecordingAckPort()
    original_after = lane._after_store_checkout
    original_before = lane._before_mutation_lease
    hits = {"after_checkout": 0, "before_lease": 0}

    def _after():
        hits["after_checkout"] += 1
        original_after()
        if hits["after_checkout"] == 1:
            lane.close()

    def _before():
        hits["before_lease"] += 1
        original_before()

    lane._after_store_checkout = _after
    lane._before_mutation_lease = _before
    result = lane.consume_inbound_action(
        ack, **_inbound(job, decision_idempotency_key="dec-seams")
    )
    _assert_loser_consume(result, ack, store, lane)
    assert hits["after_checkout"] == 1
    assert hits["before_lease"] == 0
    _assert_no_reopen(lane)


def test_E2_close_immediately_before_lease_acquire(tmp_path):
    """Linearization point: close at `_before_mutation_lease` is a loser."""
    lane, job, store = _seed(tmp_path, idempotency_key="idem-E2")
    ack = RecordingAckPort()

    def _close_then_lease():
        lane.close()

    lane._before_mutation_lease = _close_then_lease
    result = lane.consume_inbound_action(
        ack, **_inbound(job, decision_idempotency_key="dec-E2")
    )
    _assert_loser_consume(result, ack, store, lane)
    _assert_no_reopen(lane)


def test_gateway_shutdown_after_checkout_does_not_ack_or_write(tmp_path):
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.slack_contract import SlackBindingLedger
    from gateway.durable_job_lane import (
        attach_durable_job_lane,
        consume_slack_action_if_active,
        detach_durable_job_lane,
    )

    detach_durable_job_lane()
    try:
        handle = attach_durable_job_lane(raw_config=_complete(tmp_path))
        assert handle is not None
        lane = handle.lane
        store = lane._require_sqlite_path()
        job = store.create_job(
            origin_platform="slack",
            origin_chat_id="C123",
            origin_root_thread_id="111.222",
            objective="gw-lease",
            repository_identity="github.com/example/repo",
            idempotency_key="idem-gw-C",
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
        original = lane._require_sqlite_path

        def _checkout_then_shutdown():
            checked = original()
            handle.shutdown()
            return checked

        lane._require_sqlite_path = _checkout_then_shutdown
        result = consume_slack_action_if_active(
            {
                "team": {"id": "T1"},
                "user": {"id": "U-alice"},
                "channel": {"id": "C123"},
                "message": {"thread_ts": "111.222", "ts": "111.222"},
            },
            {
                "action_id": "hermes_durable_go",
                "value": __import__("json").dumps(
                    {
                        "job_id": job.job_id,
                        "decision_idempotency_key": "dec-gw-C",
                        "policy_version": "pol-1",
                        "candidate_id": "cand-1",
                        "candidate_version": "v1",
                    }
                ),
            },
        )
        assert result is not None
        assert result.ok is False
        assert result.retryable is True
        assert count_table(store.sqlite_path, "job_inbound_actions") == 0
        assert count_table(store.sqlite_path, "job_decisions") == 0
    finally:
        detach_durable_job_lane()
