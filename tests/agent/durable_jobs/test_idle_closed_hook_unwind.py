"""Idle-closed hook must not replace an in-flight primary exception.

On 8e5df0bc, ``_mutation_lease`` releases in ``finally``. Holder
``close()`` raises required ``LaneClosedError``, then a failing
``_after_idle_closed`` hook replaces it. Consume then misses the typed
fail-closed mapping.

When the with-body succeeds, a cleanup ``Exception`` must still
propagate. ``BaseException`` from the hook is not swallowed.

No live Slack/Cursor/network. No Gateway start.
"""

from __future__ import annotations

import pytest

from tests.agent.durable_jobs.eng28_support import RecordingAckPort
from tests.agent.durable_jobs.test_handle_shutdown_lease_holder import (
    _inbound,
    _seed_handle,
)
from tests.agent.durable_jobs.test_lane_shutdown_lease import _seed


@pytest.fixture(autouse=True)
def _reset_lane_seam():
    from gateway.durable_job_lane import detach_durable_job_lane

    detach_durable_job_lane()
    yield
    detach_durable_job_lane()


def _explode(_lane=None):
    raise RuntimeError("hook exploded")


def test_holder_close_lane_closed_survives_exploding_idle_hook(tmp_path):
    from agent.durable_jobs.lane import LaneClosedError

    lane, _job, _store = _seed(tmp_path, idempotency_key="idem-hook-lce")
    lane._after_idle_closed = _explode
    with pytest.raises(LaneClosedError):
        with lane._mutation_lease():
            lane.close()
    assert lane._closed is True
    assert lane._active_leases == 0


def test_primary_exception_survives_exploding_idle_hook(tmp_path):
    lane, _job, _store = _seed(tmp_path, idempotency_key="idem-hook-primary")
    lane._after_idle_closed = _explode
    with pytest.raises(ValueError, match="primary"):
        with lane._mutation_lease():
            with lane._lifecycle:
                lane._closed = True
            raise ValueError("primary")
    assert lane._active_leases == 0


def test_idle_cleanup_error_propagates_when_no_primary_exception(tmp_path):
    lane, _job, _store = _seed(tmp_path, idempotency_key="idem-hook-success")
    lane._after_idle_closed = _explode
    with pytest.raises(RuntimeError, match="hook exploded"):
        with lane._mutation_lease():
            with lane._lifecycle:
                lane._closed = True
    assert lane._active_leases == 0


def test_exploding_idle_hook_does_not_bypass_ack_pending_mapping(
    tmp_path, monkeypatch
):
    handle, job, _store = _seed_handle(
        tmp_path, monkeypatch, idempotency_key="idem-hook-ack"
    )
    handle.lane._after_idle_closed = _explode
    events: list[str] = []

    class AckShutdown:
        def ack(self, *, inbound_id: str, job_id: str) -> str:
            events.append("ack-enter")
            handle.shutdown()
            events.append("after-shutdown-return")
            return f"ack:{inbound_id}"

    result = handle.lane.consume_inbound_action(
        AckShutdown(),
        **_inbound(job, decision_idempotency_key="dec-hook-ack"),
    )
    assert events == ["ack-enter"]
    assert result.ok is False
    assert result.ack_status == "pending"
    assert result.retryable is True
    replay = RecordingAckPort()
    again = handle.lane.consume_inbound_action(
        replay, **_inbound(job, decision_idempotency_key="dec-hook-ack")
    )
    assert again.ok is False
    assert again.retryable is True
    assert replay.acks == []
