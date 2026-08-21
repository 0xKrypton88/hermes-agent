"""ENG-36: per-owner attach/retire must stay registry/runner consistent.

``_retire_owner_lane`` must not clear a runner field that a concurrent
valid attach already published. Sibling owners stay isolated. No deadlock
and no global lock across shutdown I/O.

No live Slack/Cursor/network. No Gateway start. Barriers use
threading.Event, not sleeps.
"""

from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

from tests.agent.durable_jobs.package2_support import runtime_ready_transport_kwargs
from tests.gateway.test_durable_job_lane_unauthorized_reattach import _complete


@pytest.fixture(autouse=True)
def _reset_lane_seam():
    from gateway.durable_job_lane import detach_durable_job_lane

    detach_durable_job_lane()
    yield
    detach_durable_job_lane()


def _owner_entry(owner):
    from gateway.durable_job_lane import _LANES, _owner_key

    return _LANES.get(_owner_key(owner))


def test_invalid_retire_concurrent_valid_attach_keeps_registry_runner_aligned(
    tmp_path, monkeypatch
):
    from gateway.durable_job_lane import attach_to_gateway_runner

    old_dir = tmp_path / "old"
    new_dir = tmp_path / "new"
    old_dir.mkdir()
    new_dir.mkdir()
    transports = runtime_ready_transport_kwargs(monkeypatch)
    runner = SimpleNamespace(_durable_job_lane=None)
    first = attach_to_gateway_runner(
        runner, raw_config=_complete(old_dir), **transports
    )
    assert first is not None
    assert runner._durable_job_lane is first

    released = threading.Event()
    attach_finished = threading.Event()
    allow_shutdown = threading.Event()
    original_shutdown = first.shutdown
    errors: list[BaseException] = []

    def gated_shutdown() -> None:
        released.set()
        if not allow_shutdown.wait(timeout=5.0):
            raise TimeoutError("test never released prior shutdown")
        original_shutdown()

    first.shutdown = gated_shutdown

    def retire() -> None:
        try:
            attach_to_gateway_runner(
                runner,
                raw_config=_complete(old_dir, enabled=False),
                **transports,
            )
        except Exception as exc:
            errors.append(exc)

    def attach() -> None:
        try:
            if not released.wait(timeout=5.0):
                raise TimeoutError("retire never entered shutdown")
            attach_to_gateway_runner(
                runner, raw_config=_complete(new_dir), **transports
            )
        except Exception as exc:
            errors.append(exc)
        finally:
            attach_finished.set()

    workers = [
        threading.Thread(target=retire, name="retire-invalid"),
        threading.Thread(target=attach, name="attach-valid"),
    ]
    for worker in workers:
        worker.start()
    assert released.wait(timeout=2.0)
    assert not attach_finished.wait(timeout=0.2)
    assert runner._durable_job_lane is None
    allow_shutdown.set()
    for worker in workers:
        worker.join(timeout=6.0)
        assert not worker.is_alive(), "attach/retire deadlocked"
    assert errors == []

    live = _owner_entry(runner)
    assert live is not None
    assert live is not first
    assert runner._durable_job_lane is live
    assert first.lane._closed is True


def test_invalid_retire_does_not_clear_sibling_owner_runner_or_registry(
    tmp_path, monkeypatch
):
    from gateway.durable_job_lane import attach_to_gateway_runner

    a_dir = tmp_path / "a"
    b_dir = tmp_path / "b"
    new_dir = tmp_path / "new"
    a_dir.mkdir()
    b_dir.mkdir()
    new_dir.mkdir()
    transports = runtime_ready_transport_kwargs(monkeypatch)
    owner_a = SimpleNamespace(_durable_job_lane=None)
    owner_b = SimpleNamespace(_durable_job_lane=None)
    lane_a = attach_to_gateway_runner(
        owner_a, raw_config=_complete(a_dir), **transports
    )
    lane_b = attach_to_gateway_runner(
        owner_b, raw_config=_complete(b_dir), **transports
    )
    assert lane_a is not None and lane_b is not None

    released = threading.Event()
    attach_finished = threading.Event()
    allow_shutdown = threading.Event()
    original_shutdown = lane_a.shutdown

    def gated_shutdown() -> None:
        released.set()
        if not allow_shutdown.wait(timeout=5.0):
            raise TimeoutError("test never released prior shutdown")
        original_shutdown()

    lane_a.shutdown = gated_shutdown

    def retire() -> None:
        attach_to_gateway_runner(
            owner_a,
            raw_config=_complete(a_dir, enabled=False),
            **transports,
        )

    def attach() -> None:
        try:
            if not released.wait(timeout=5.0):
                raise TimeoutError("retire never entered shutdown")
            attach_to_gateway_runner(
                owner_a, raw_config=_complete(new_dir), **transports
            )
        finally:
            attach_finished.set()

    workers = [
        threading.Thread(target=retire, name="retire-a"),
        threading.Thread(target=attach, name="attach-a"),
    ]
    for worker in workers:
        worker.start()
    assert released.wait(timeout=2.0)
    assert not attach_finished.wait(timeout=0.2)
    assert owner_a._durable_job_lane is None
    assert owner_b._durable_job_lane is lane_b
    allow_shutdown.set()
    for worker in workers:
        worker.join(timeout=6.0)
        assert not worker.is_alive()

    assert owner_b._durable_job_lane is lane_b
    assert _owner_entry(owner_b) is lane_b
    assert lane_b.lane._closed is False
    live_a = _owner_entry(owner_a)
    assert live_a is not None
    assert owner_a._durable_job_lane is live_a
    assert live_a is not lane_a
