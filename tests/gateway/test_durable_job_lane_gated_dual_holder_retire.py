"""ENG-36 verifier-owned gated dual-holder retirement probe.

Reconstructs the exact interleaving from
``eng36-bbfe6488-dual-holder-retire-probe.py`` (not in this tree):

1. Two threads both hold ``lane._mutation_lease()`` on the same published lane.
2. ``handle.shutdown`` is monkeypatched to set ``released`` then wait for
   ``joiner_done`` before calling the real shutdown.
3. Holder A drives a disabled reattach (or detach) and becomes the
   retirement leader, entering the gated shutdown.
4. Holder B, seeing ``_RETIRING``, must not re-enter that in-flight
   ``shutdown()`` in a way that blocks the leader.

On 21d60499 the joiner re-enters the same gated ``shutdown()``, so both
wait on ``joiner_done``. The leader's gate times out; ``_shutdown_retired``
swallows the generic ``TimeoutError`` and A returns normally
(``continued``). B later gets ``LaneClosedError``. That is independently
RED even though ungated dual-holder tests stay green.

Invariant: once retirement is visible, every active holder of
disabled/invalid/config-failed reattach, detach, or stop fails closed
with ``LaneClosedError`` — no normal return/ACK and no deadlock.
"""

from __future__ import annotations

import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.agent.durable_jobs.package2_support import runtime_ready_transport_kwargs
from tests.agent.durable_jobs.test_handle_shutdown_lease_holder import _complete
from tests.agent.durable_jobs.test_reentrant_reattach_and_detach import _seed_owned


@pytest.fixture(autouse=True)
def _reset_lane_seam():
    from gateway.durable_job_lane import detach_durable_job_lane

    detach_durable_job_lane()
    yield
    detach_durable_job_lane()


def _install_gated_shutdown(handle, *, released: threading.Event, joiner_done: threading.Event):
    original_shutdown = handle.shutdown

    def gated_shutdown():
        released.set()
        if not joiner_done.wait(timeout=5.0):
            raise TimeoutError(
                "gated dual-holder retire probe: joiner never released the "
                "in-flight shutdown gate"
            )
        original_shutdown()

    handle.shutdown = gated_shutdown
    return original_shutdown


def _capture_holder_outcome(fn) -> str:
    try:
        fn()
    except Exception as exc:
        from agent.durable_jobs.lane import LaneClosedError

        if isinstance(exc, LaneClosedError):
            return "LaneClosedError"
        return f"{type(exc).__name__}:{exc}"
    return "continued"


def _run_gated_dual_holders(
    tmp_path: Path,
    monkeypatch,
    *,
    joiner,
    leader,
    idempotency_key: str,
) -> tuple[str, str]:
    """Exact gated interleaving: leader enters shutdown, then joiner runs.

    Both threads keep their mutation leases for the whole body. The joiner
    does not start until the leader has entered the gated ``shutdown()``.
    """
    owner = SimpleNamespace(_durable_job_lane=None)
    handle, _job, _store = _seed_owned(
        tmp_path, monkeypatch, owner, idempotency_key=idempotency_key
    )
    transports = runtime_ready_transport_kwargs(monkeypatch)
    a_ready = threading.Event()
    b_ready = threading.Event()
    both_holding = threading.Event()
    released = threading.Event()
    joiner_done = threading.Event()
    outcomes: dict[str, str] = {}

    _install_gated_shutdown(handle, released=released, joiner_done=joiner_done)

    def holder_a() -> None:
        with handle.lane._mutation_lease():
            a_ready.set()
            assert both_holding.wait(timeout=5.0)

            def body() -> None:
                try:
                    leader(owner, handle, tmp_path, transports)
                finally:
                    joiner_done.set()

            outcomes["A"] = _capture_holder_outcome(body)

    def holder_b() -> None:
        with handle.lane._mutation_lease():
            b_ready.set()
            assert both_holding.wait(timeout=5.0)
            assert released.wait(timeout=5.0), "leader never entered gated shutdown"

            def body() -> None:
                try:
                    joiner(owner, handle, tmp_path, transports)
                finally:
                    joiner_done.set()

            outcomes["B"] = _capture_holder_outcome(body)

    threads = [
        threading.Thread(target=holder_a, name="gated-holder-A"),
        threading.Thread(target=holder_b, name="gated-holder-B"),
    ]
    for thread in threads:
        thread.start()
    assert a_ready.wait(timeout=2.0) and b_ready.wait(timeout=2.0)
    both_holding.set()
    for thread in threads:
        thread.join(timeout=8.0)
        assert not thread.is_alive(), f"{thread.name} deadlocked in gated retirement"
    return outcomes.get("A", "missing"), outcomes.get("B", "missing")


def _disabled_reattach(owner, _handle, tmp_path, transports) -> None:
    from gateway.durable_job_lane import attach_to_gateway_runner

    attach_to_gateway_runner(
        owner,
        raw_config=_complete(tmp_path, enabled=False),
        **transports,
    )


def _detach_from_runner(owner, _handle, _tmp_path, _transports) -> None:
    from gateway.durable_job_lane import detach_from_gateway_runner

    detach_from_gateway_runner(owner)


def _noarg_detach(_owner, _handle, _tmp_path, _transports) -> None:
    from gateway.durable_job_lane import detach_durable_job_lane

    detach_durable_job_lane()


def test_gated_dual_holder_disabled_reattach_both_fail_closed(tmp_path, monkeypatch):
    a, b = _run_gated_dual_holders(
        tmp_path,
        monkeypatch,
        leader=_disabled_reattach,
        joiner=_disabled_reattach,
        idempotency_key="idem-gated-dual-reattach",
    )
    assert (a, b) == ("LaneClosedError", "LaneClosedError"), (
        "gated dual-holder retire probe: both active holders must fail closed; "
        f"got A={a!r} B={b!r}"
    )


def test_gated_leader_disabled_reattach_joiner_detach_both_fail_closed(
    tmp_path, monkeypatch
):
    a, b = _run_gated_dual_holders(
        tmp_path,
        monkeypatch,
        leader=_disabled_reattach,
        joiner=_detach_from_runner,
        idempotency_key="idem-gated-reattach-detach",
    )
    assert (a, b) == ("LaneClosedError", "LaneClosedError"), (
        "gated reattach+detach probe: both active holders must fail closed; "
        f"got A={a!r} B={b!r}"
    )


def test_gated_public_noarg_detach_while_leader_shutdown_holder_fails_closed(
    tmp_path, monkeypatch
):
    a, b = _run_gated_dual_holders(
        tmp_path,
        monkeypatch,
        leader=_noarg_detach,
        joiner=_disabled_reattach,
        idempotency_key="idem-gated-noarg-detach",
    )
    assert (a, b) == ("LaneClosedError", "LaneClosedError"), (
        "gated no-arg detach probe: both active holders must fail closed; "
        f"got A={a!r} B={b!r}"
    )


def test_gated_dual_holder_retire_stress_twenty_five(tmp_path, monkeypatch):
    for i in range(25):
        a, b = _run_gated_dual_holders(
            tmp_path / f"iter-{i}",
            monkeypatch,
            leader=_disabled_reattach,
            joiner=_disabled_reattach,
            idempotency_key=f"idem-gated-stress-{i}",
        )
        assert (a, b) == ("LaneClosedError", "LaneClosedError"), (
            f"gated dual-holder stress iteration {i}: A={a!r} B={b!r}"
        )


def test_replacement_attach_waits_until_previous_shutdown_is_complete(tmp_path, monkeypatch):
    """A new owner lane must not publish while the prior handle is still shutting down."""
    from gateway.durable_job_lane import (
        attach_to_gateway_runner,
        detach_from_gateway_runner,
    )

    owner = SimpleNamespace(_durable_job_lane=None)
    old, _job, _store = _seed_owned(
        tmp_path, monkeypatch, owner, idempotency_key="idem-replacement-fence"
    )
    transports = runtime_ready_transport_kwargs(monkeypatch)
    shutdown_entered = threading.Event()
    allow_shutdown = threading.Event()
    replacement_done = threading.Event()
    original_shutdown = old.shutdown

    def gated_shutdown():
        shutdown_entered.set()
        assert allow_shutdown.wait(timeout=5.0)
        original_shutdown()

    old.shutdown = gated_shutdown
    retire = threading.Thread(target=lambda: detach_from_gateway_runner(owner))
    retire.start()
    assert shutdown_entered.wait(timeout=2.0)

    result = {}

    def replace():
        result["handle"] = attach_to_gateway_runner(
            owner, raw_config=_complete(tmp_path), **transports
        )
        replacement_done.set()

    replacement = threading.Thread(target=replace)
    replacement.start()
    assert not replacement_done.wait(timeout=0.2)
    assert owner._durable_job_lane is None

    allow_shutdown.set()
    retire.join(timeout=5.0)
    replacement.join(timeout=5.0)
    assert not retire.is_alive() and not replacement.is_alive()
    assert result["handle"] is owner._durable_job_lane
    assert result["handle"] is not old
