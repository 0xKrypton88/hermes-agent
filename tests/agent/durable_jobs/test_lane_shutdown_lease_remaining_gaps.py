"""Remaining Package 2 close/mutation-lease gaps (heartbeat + reentrant close).

Linearization: lease acquire is the point vs close(); child work started
under that ownership interval (claim-lease heartbeat) is still owned until
it cannot write. After owner_lease_heartbeat returns, no child thread from
that interval may still mutate. close() must complete boundedly and must
not deadlock if invoked by a same-thread lease holder (adapter/ACK).

Barriers use threading.Event, not sleeps. No live Slack/Cursor/network.

These tests are the transplant RED pair: they fail on parent 40bb2695 and
pass after the heartbeat-join and reentrant-close fixes.
"""

from __future__ import annotations

import threading

import pytest

from tests.agent.durable_jobs.eng28_support import RecordingAckPort, count_table
from tests.agent.durable_jobs.test_lane_shutdown_lease import _inbound, _seed


# Parent owner_lease_heartbeat.__exit__ joins with timeout=1.0 then drops the
# thread. Wait past that so RED observes write-after-exit; GREEN is still
# blocked in join() until the test releases the in-flight renew.
_PARENT_HEARTBEAT_JOIN_GIVE_UP_S = 1.6
_PARENT_FROZEN_EXIT_GIVE_UP_S = 0.5


def test_owner_lease_heartbeat_blocked_renew_cannot_write_after_context_returns():
    """Gap: join(timeout=1) then drop thread lets a blocked renew write after __exit__.

    Second renew starts inside the context and stays blocked. Parent __exit__
    returns after ~1s while the daemon is still in renew_fn; releasing then
    completes the write after the context returned. Candidate waits until the
    in-flight renew finishes, so the write cannot happen after __exit__.
    """
    from agent.durable_jobs.claim_protocol import owner_lease_heartbeat

    second_entered = threading.Event()
    release_second = threading.Event()
    write_completed = threading.Event()
    exit_returned = threading.Event()
    order: list[str] = []
    calls = {"n": 0}
    thread_ref: dict[str, threading.Thread | None] = {"t": None}

    def renew_fn() -> bool:
        calls["n"] += 1
        if calls["n"] == 1:
            return True
        second_entered.set()
        release_second.wait()
        order.append("write")
        write_completed.set()
        return True

    def runner() -> None:
        with owner_lease_heartbeat(
            renew_fn=renew_fn,
            now_fn=lambda: "daemon-now",
            lease_seconds=0,
        ) as heartbeat:
            assert second_entered.wait(timeout=5.0)
            thread_ref["t"] = heartbeat._thread
        order.append("exit")
        exit_returned.set()

    worker = threading.Thread(target=runner, name="hb-exit-runner", daemon=True)
    worker.start()
    assert second_entered.wait(timeout=5.0)
    parent_gave_up = exit_returned.wait(timeout=_PARENT_HEARTBEAT_JOIN_GIVE_UP_S)
    if parent_gave_up:
        assert "write" not in order
        release_second.set()
        assert write_completed.wait(timeout=5.0)
        worker.join(timeout=5.0)
        pytest.fail(
            "blocked heartbeat renew completed after owner_lease_heartbeat "
            "__exit__ returned (join timeout abandoned the thread)"
        )
    release_second.set()
    assert write_completed.wait(timeout=5.0)
    assert exit_returned.wait(timeout=5.0)
    worker.join(timeout=5.0)
    assert not worker.is_alive()
    alive = thread_ref["t"].is_alive() if thread_ref["t"] is not None else False
    assert alive is False
    assert order == ["write", "exit"]


def test_frozen_clock_in_flight_renew_cannot_write_after_context_returns():
    """Same ownership interval: a FrozenClock tick already inside renew_fn."""
    from agent.durable_jobs.claim_protocol import owner_lease_heartbeat
    from agent.durable_jobs.clock import FrozenClock

    clock = FrozenClock()
    second_entered = threading.Event()
    release_second = threading.Event()
    write_completed = threading.Event()
    exit_returned = threading.Event()
    order: list[str] = []
    calls = {"n": 0}

    def renew_fn() -> bool:
        calls["n"] += 1
        if calls["n"] == 1:
            return True
        second_entered.set()
        release_second.wait()
        order.append("write")
        write_completed.set()
        return True

    def tick() -> None:
        clock.advance(10.0)

    def runner() -> None:
        with owner_lease_heartbeat(
            renew_fn=renew_fn,
            now_fn=clock,
            lease_seconds=30,
        ):
            ticker = threading.Thread(target=tick, name="hb-tick", daemon=True)
            ticker.start()
            assert second_entered.wait(timeout=5.0)
        order.append("exit")
        exit_returned.set()

    worker = threading.Thread(target=runner, name="hb-frozen-exit", daemon=True)
    worker.start()
    assert second_entered.wait(timeout=5.0)
    parent_gave_up = exit_returned.wait(timeout=_PARENT_FROZEN_EXIT_GIVE_UP_S)
    if parent_gave_up:
        assert "write" not in order
        release_second.set()
        assert write_completed.wait(timeout=5.0)
        worker.join(timeout=5.0)
        pytest.fail(
            "blocked FrozenClock heartbeat renew completed after "
            "owner_lease_heartbeat __exit__ returned"
        )
    release_second.set()
    assert write_completed.wait(timeout=5.0)
    assert exit_returned.wait(timeout=5.0)
    worker.join(timeout=5.0)
    assert not worker.is_alive()
    assert order == ["write", "exit"]


def test_close_from_mutation_lease_holder_does_not_deadlock(tmp_path):
    """Gap: close() waiting for _active_leases==0 deadlocks on the holder thread."""
    from agent.durable_jobs.lane import LaneClosedError

    lane, _job, _store = _seed(tmp_path, idempotency_key="idem-reentrant-close")
    done = threading.Event()
    raised: list[BaseException] = []
    closed_flag = {"v": False}

    def holder() -> None:
        try:
            with lane._mutation_lease():
                try:
                    lane.close()
                except LaneClosedError as exc:
                    raised.append(exc)
                closed_flag["v"] = lane._closed
        finally:
            done.set()

    worker = threading.Thread(target=holder, name="reentrant-close", daemon=True)
    worker.start()
    finished = done.wait(timeout=5.0)
    if not finished:
        pytest.fail(
            "close() from a mutation-lease holder deadlocked waiting on its own lease"
        )
    worker.join(timeout=1.0)
    assert not worker.is_alive()
    assert closed_flag["v"] is True
    assert raised, "close() from a lease holder must fail closed with LaneClosedError"
    assert isinstance(raised[0], LaneClosedError)
    assert lane._active_leases == 0
    assert lane._store is None


def test_ack_calling_close_does_not_deadlock_consume(tmp_path):
    """Same gap via ACK: coordinator ACK runs under the consume mutation lease."""
    lane, job, store = _seed(tmp_path, idempotency_key="idem-ack-close")
    inbound_before = count_table(store.sqlite_path, "job_inbound_actions")
    decisions_before = count_table(store.sqlite_path, "job_decisions")
    done = threading.Event()
    results: list = []

    class _AckClose:
        def ack(self, *, inbound_id: str, job_id: str) -> str:
            lane.close()
            return f"ack:{inbound_id}"

    def runner() -> None:
        try:
            results.append(
                lane.consume_inbound_action(
                    _AckClose(),
                    **_inbound(job, decision_idempotency_key="dec-ack-close"),
                )
            )
        finally:
            done.set()

    worker = threading.Thread(target=runner, name="ack-close-consume", daemon=True)
    worker.start()
    finished = done.wait(timeout=5.0)
    if not finished:
        pytest.fail(
            "consume_inbound_action deadlocked: ACK called close() on the "
            "lease-holding thread"
        )
    worker.join(timeout=1.0)
    assert not worker.is_alive()
    assert results, "consume_inbound_action must return after ACK-injected close()"
    result = results[0]
    assert result.ack_status == "pending"
    assert lane._closed is True
    assert lane._store is None
    assert lane._active_leases == 0
    # Persist+decision already ran under the owned lease before ACK; that is
    # winner work. close() must not ACK (ack() never returned) or deadlock.
    assert count_table(store.sqlite_path, "job_inbound_actions") == inbound_before + 1
    assert count_table(store.sqlite_path, "job_decisions") == decisions_before + 1
    replay = RecordingAckPort()
    again = lane.consume_inbound_action(
        replay, **_inbound(job, decision_idempotency_key="dec-ack-close")
    )
    assert again.ok is False
    assert again.retryable is True
    assert replay.acks == []
    assert count_table(store.sqlite_path, "job_inbound_actions") == inbound_before + 1
    assert count_table(store.sqlite_path, "job_decisions") == decisions_before + 1
