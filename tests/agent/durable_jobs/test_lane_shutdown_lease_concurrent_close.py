"""Concurrent mutation-lease holders both calling close() must not deadlock.

Parent 3d7a08cd waits while ``_active_leases > self_held``. Two holders
(each self_held==1, active==2) both call close() and wait for each other.

Barriers use threading.Event, not sleeps. No live Slack/Cursor/network.
"""

from __future__ import annotations

import threading

import pytest

from tests.agent.durable_jobs.eng28_support import RecordingAckPort, count_table
from tests.agent.durable_jobs.test_lane_shutdown_lease import _inbound, _seed


def _add_bound_job(store, *, idempotency_key: str, root_thread_ts: str):
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.slack_contract import SlackBindingLedger

    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id=root_thread_ts,
        objective="lease-invariant",
        repository_identity="github.com/example/repo",
        idempotency_key=idempotency_key,
    )
    SlackBindingLedger(sqlite_path=store.sqlite_path).bind(
        job_id=job.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts=root_thread_ts,
        candidate_id="cand-1",
        candidate_version="v1",
    )
    DecisionLedger(sqlite_path=store.sqlite_path).set_policy(
        job_id=job.job_id,
        policy_version="pol-1",
        allowed_actors=("U-alice",),
        expires_at="2099-01-01T00:00:00+00:00",
    )
    return job


def test_two_mutation_lease_holders_calling_close_do_not_deadlock(tmp_path):
    """Two holders each call close() while the other still holds a lease."""
    from agent.durable_jobs.lane import LaneClosedError

    lane, _job, _store = _seed(tmp_path, idempotency_key="idem-two-holder-close")
    entered = [threading.Event(), threading.Event()]
    done = [threading.Event(), threading.Event()]
    raised: list[list[BaseException]] = [[], []]

    def holder(index: int) -> None:
        try:
            with lane._mutation_lease():
                entered[index].set()
                if not entered[1 - index].wait(timeout=5.0):
                    return
                try:
                    lane.close()
                except LaneClosedError as exc:
                    raised[index].append(exc)
        finally:
            done[index].set()

    workers = [
        threading.Thread(target=holder, args=(i,), name=f"two-holder-close-{i}", daemon=True)
        for i in (0, 1)
    ]
    for worker in workers:
        worker.start()
    if not entered[0].wait(timeout=5.0) or not entered[1].wait(timeout=5.0):
        pytest.fail("both threads must hold a mutation lease before close()")
    finished = done[0].wait(timeout=5.0) and done[1].wait(timeout=5.0)
    if not finished:
        pytest.fail(
            "two concurrent mutation-lease holders calling close() deadlocked"
        )
    for worker in workers:
        worker.join(timeout=1.0)
        assert not worker.is_alive()
    assert raised[0] and raised[1], "each holder close() must fail closed"
    assert all(isinstance(exc, LaneClosedError) for exc in raised[0] + raised[1])
    assert lane._closed is True
    assert lane._store is None
    assert lane._active_leases == 0


def test_two_ack_paths_calling_close_do_not_deadlock_consume(tmp_path):
    """Realistic entry: two consume/ACK callbacks both call close()."""
    lane, job_a, store = _seed(tmp_path, idempotency_key="idem-two-ack-a")
    job_b = _add_bound_job(
        store, idempotency_key="idem-two-ack-b", root_thread_ts="333.444"
    )
    inbound_before = count_table(store.sqlite_path, "job_inbound_actions")
    decisions_before = count_table(store.sqlite_path, "job_decisions")
    entered = [threading.Event(), threading.Event()]
    done = [threading.Event(), threading.Event()]
    results: list[list] = [[], []]

    def make_ack(index: int):
        class _AckClose:
            def ack(self, *, inbound_id: str, job_id: str) -> str:
                entered[index].set()
                if not entered[1 - index].wait(timeout=5.0):
                    raise RuntimeError("peer consume never reached ACK")
                lane.close()
                return f"ack:{inbound_id}"

        return _AckClose()

    jobs = (job_a, job_b)
    keys = ("dec-two-ack-a", "dec-two-ack-b")
    threads_ts = ("111.222", "333.444")

    def runner(index: int) -> None:
        try:
            results[index].append(
                lane.consume_inbound_action(
                    make_ack(index),
                    **_inbound(
                        jobs[index],
                        root_thread_ts=threads_ts[index],
                        decision_idempotency_key=keys[index],
                    ),
                )
            )
        finally:
            done[index].set()

    workers = [
        threading.Thread(target=runner, args=(i,), name=f"two-ack-close-{i}", daemon=True)
        for i in (0, 1)
    ]
    for worker in workers:
        worker.start()
    if not entered[0].wait(timeout=5.0) or not entered[1].wait(timeout=5.0):
        pytest.fail("both consume paths must reach ACK holding leases before close()")
    finished = done[0].wait(timeout=5.0) and done[1].wait(timeout=5.0)
    if not finished:
        pytest.fail(
            "two concurrent consume/ACK close() calls deadlocked waiting on each other"
        )
    for worker in workers:
        worker.join(timeout=1.0)
        assert not worker.is_alive()
    assert results[0] and results[1]
    assert results[0][0].ack_status == "pending"
    assert results[1][0].ack_status == "pending"
    assert lane._closed is True
    assert lane._store is None
    assert lane._active_leases == 0
    # Persist+decision ran under owned leases before ACK; that is winner work.
    # close() must not complete ACK (ack() never returned) or deadlock.
    assert count_table(store.sqlite_path, "job_inbound_actions") == inbound_before + 2
    assert count_table(store.sqlite_path, "job_decisions") == decisions_before + 2
    replay = RecordingAckPort()
    again = lane.consume_inbound_action(
        replay, **_inbound(job_a, decision_idempotency_key="dec-two-ack-a")
    )
    assert again.ok is False
    assert again.retryable is True
    assert replay.acks == []
    assert count_table(store.sqlite_path, "job_inbound_actions") == inbound_before + 2
    assert count_table(store.sqlite_path, "job_decisions") == decisions_before + 2
