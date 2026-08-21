"""ENG-36: consume result after handle.shutdown() inside ACK.

Verifier counterexample: AckShutdown.ack runs handle.shutdown(); the
callback does not continue and no ACK is recorded, but
``consume_inbound_action`` still returned
``InboundActionResult(ok=True, ack_status='pending', retryable=False)``.

Frozen contract: when the lane is closed under the consume lease/callback,
the result must be ``ok=False``, ``ack_status='pending'``, ``retryable=True``.

No live Slack/Cursor/network. Barriers use threading.Event, not sleeps.
"""

from __future__ import annotations

import threading

import pytest

from tests.agent.durable_jobs.eng28_support import RecordingAckPort
from tests.agent.durable_jobs.test_handle_shutdown_lease_holder import (
    _inbound,
    _seed_handle,
)


@pytest.fixture(autouse=True)
def _reset_lane_seam():
    from gateway.durable_job_lane import detach_durable_job_lane

    detach_durable_job_lane()
    yield
    detach_durable_job_lane()


def test_handle_shutdown_inside_ack_must_not_return_accepted(tmp_path, monkeypatch):
    handle, job, _store = _seed_handle(
        tmp_path, monkeypatch, idempotency_key="idem-ack-closed-result"
    )
    events: list[str] = []
    acks: list[tuple[str, str]] = []
    done = threading.Event()
    results: list = []

    class AckShutdown:
        def ack(self, *, inbound_id: str, job_id: str) -> str:
            events.append("ack-enter")
            handle.shutdown()
            events.append("after-shutdown-return")
            acks.append((inbound_id, job_id))
            return f"ack:{inbound_id}"

    def runner() -> None:
        try:
            results.append(
                handle.lane.consume_inbound_action(
                    AckShutdown(),
                    **_inbound(job, decision_idempotency_key="dec-ack-closed"),
                )
            )
        finally:
            done.set()

    worker = threading.Thread(target=runner, name="ack-closed-result")
    worker.start()
    finished = done.wait(timeout=5.0)
    if not finished:
        pytest.fail(
            "consume deadlocked: ACK called handle.shutdown() on the "
            "lease-holding thread"
        )
    worker.join(timeout=1.0)
    assert not worker.is_alive()
    assert events == ["ack-enter"]
    assert acks == []
    assert results, "consume_inbound_action must return after ACK shutdown"
    result = results[0]
    assert result.ok is False
    assert result.ack_status == "pending"
    assert result.retryable is True
    replay = RecordingAckPort()
    again = handle.lane.consume_inbound_action(
        replay, **_inbound(job, decision_idempotency_key="dec-ack-closed")
    )
    assert again.ok is False
    assert again.retryable is True
    assert replay.acks == []


def test_ack_lane_closed_error_is_not_accepted_pending_while_store_remains(
    tmp_path, monkeypatch
):
    """Joiner-shaped ACK: LaneClosedError before close() drops the store.

    On 242fcdf0 the coordinator swallowed that into
    ``ok=True, ack_status='pending', retryable=False`` because lane remap
    only fires after ``_store is None``. A fenced holder must still be
    ``ok=False`` / ``pending`` / ``retryable=True``.
    """
    from agent.durable_jobs.coordinator import consume_inbound_action
    from agent.durable_jobs.lane import LaneClosedError

    handle, job, store = _seed_handle(
        tmp_path, monkeypatch, idempotency_key="idem-ack-joiner-fence"
    )
    events: list[str] = []

    class AckJoinerFence:
        def ack(self, *, inbound_id: str, job_id: str) -> str:
            events.append("ack-enter")
            raise LaneClosedError(
                "durable job lane is retiring; active holder must fail closed"
            )

    inbound = _inbound(job, decision_idempotency_key="dec-ack-joiner-fence")
    with pytest.raises(LaneClosedError):
        consume_inbound_action(store.sqlite_path, AckJoinerFence(), **inbound)
    assert events == ["ack-enter"]

    events.clear()
    result = handle.lane.consume_inbound_action(AckJoinerFence(), **inbound)
    assert events == ["ack-enter"]
    assert result.ok is False
    assert result.ack_status == "pending"
    assert result.retryable is True
    assert handle.lane._store is not None
    assert handle.lane._closed is False
    replay = RecordingAckPort()
    again = handle.lane.consume_inbound_action(
        replay, **_inbound(job, decision_idempotency_key="dec-ack-joiner-replay")
    )
    assert again.ok is True
    assert replay.acks
