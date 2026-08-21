"""ENG-36: public handle detach must clear registry and runner field.

Verified counterexample: attach_to_gateway_runner then
``detach_durable_job_lane(handle)`` pops ``_LANES`` but leaves
``runner._durable_job_lane`` pointing at the closed handle.

A newer concurrently published lane must not be cleared by retiring the
older handle. Sibling owners stay isolated. No live Slack/Cursor/network.
No Gateway start. Barriers use threading.Event, not sleeps.
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


def test_detach_handle_clears_registry_and_runner_field(tmp_path, monkeypatch):
    from gateway.durable_job_lane import (
        attach_to_gateway_runner,
        detach_durable_job_lane,
    )

    runner = SimpleNamespace(_durable_job_lane=None)
    handle = attach_to_gateway_runner(
        runner,
        raw_config=_complete(tmp_path),
        **runtime_ready_transport_kwargs(monkeypatch),
    )
    assert handle is not None
    assert runner._durable_job_lane is handle
    assert _owner_entry(runner) is handle

    detach_durable_job_lane(handle)

    assert _owner_entry(runner) is None
    assert runner._durable_job_lane is None
    assert handle.lane._closed is True


def test_detach_handle_does_not_clear_newer_concurrent_attach(tmp_path, monkeypatch):
    from gateway.durable_job_lane import (
        attach_to_gateway_runner,
        detach_durable_job_lane,
    )

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
    original_shutdown = first.shutdown
    errors: list[BaseException] = []

    def gated_shutdown() -> None:
        released.set()
        if not attach_finished.wait(timeout=5.0):
            raise TimeoutError("valid attach did not finish during detach window")
        original_shutdown()

    first.shutdown = gated_shutdown

    def detach() -> None:
        try:
            detach_durable_job_lane(first)
        except Exception as exc:
            errors.append(exc)

    def attach() -> None:
        try:
            if not released.wait(timeout=5.0):
                raise TimeoutError("handle detach never entered shutdown")
            attach_to_gateway_runner(
                runner, raw_config=_complete(new_dir), **transports
            )
        except Exception as exc:
            errors.append(exc)
        finally:
            attach_finished.set()

    workers = [
        threading.Thread(target=detach, name="detach-handle"),
        threading.Thread(target=attach, name="attach-valid"),
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=6.0)
        assert not worker.is_alive(), "detach/attach deadlocked"
    assert errors == []

    live = _owner_entry(runner)
    assert live is not None
    assert live is not first
    assert runner._durable_job_lane is live
    assert first.lane._closed is True


def test_detach_handle_does_not_clear_sibling_owner(tmp_path, monkeypatch):
    from gateway.durable_job_lane import (
        attach_to_gateway_runner,
        detach_durable_job_lane,
    )

    transports = runtime_ready_transport_kwargs(monkeypatch)
    owner_a = SimpleNamespace(_durable_job_lane=None)
    owner_b = SimpleNamespace(_durable_job_lane=None)
    lane_a = attach_to_gateway_runner(
        owner_a, raw_config=_complete(tmp_path / "a"), **transports
    )
    lane_b = attach_to_gateway_runner(
        owner_b, raw_config=_complete(tmp_path / "b"), **transports
    )
    assert lane_a is not None and lane_b is not None

    detach_durable_job_lane(lane_a)

    assert _owner_entry(owner_a) is None
    assert owner_a._durable_job_lane is None
    assert lane_a.lane._closed is True
    assert _owner_entry(owner_b) is lane_b
    assert owner_b._durable_job_lane is lane_b
    assert lane_b.lane._closed is False


def test_holder_detach_clears_retiring_after_final_lease_release(
    tmp_path, monkeypatch
):
    """Holder-led handle detach must not leak ``_RETIRING`` after leases drain.

    On 164d81f26 ``_clear_retiring_if_idle`` runs in ``_shutdown_retired``
    finally while the holder lease is still active, so the entry stays.
    ``_release_mutation_lease`` never retries. The strong owner/handle
    refs remain, and a later no-arg detach cannot pop them.
    """
    from agent.durable_jobs.lane import LaneClosedError
    from gateway.durable_job_lane import (
        _RETIRING,
        _owner_key,
        attach_to_gateway_runner,
        detach_durable_job_lane,
    )

    transports = runtime_ready_transport_kwargs(monkeypatch)
    owner = SimpleNamespace(_durable_job_lane=None)
    sibling = SimpleNamespace(_durable_job_lane=None)
    handle = attach_to_gateway_runner(
        owner, raw_config=_complete(tmp_path / "owner"), **transports
    )
    sibling_handle = attach_to_gateway_runner(
        sibling, raw_config=_complete(tmp_path / "sibling"), **transports
    )
    assert handle is not None and sibling_handle is not None
    key = _owner_key(owner)
    sibling_key = _owner_key(sibling)
    raised: list[BaseException] = []

    with handle.lane._mutation_lease():
        try:
            detach_durable_job_lane(handle)
        except LaneClosedError as exc:
            raised.append(exc)

    assert raised and isinstance(raised[0], LaneClosedError)
    assert handle.lane._active_leases == 0
    assert handle.lane._closed is True
    assert key not in _RETIRING
    assert sibling_key not in _RETIRING
    assert owner._durable_job_lane is None
    assert _owner_entry(owner) is None
    assert sibling._durable_job_lane is sibling_handle
    assert _owner_entry(sibling) is sibling_handle
    assert sibling_handle.lane._closed is False

    newer = attach_to_gateway_runner(
        owner, raw_config=_complete(tmp_path / "newer"), **transports
    )
    assert newer is not None
    assert newer is not handle
    assert owner._durable_job_lane is newer
    assert _owner_entry(owner) is newer
    assert key not in _RETIRING
    assert sibling._durable_job_lane is sibling_handle
    assert _owner_entry(sibling) is sibling_handle

    detach_durable_job_lane(newer)
    assert owner._durable_job_lane is None
    assert _owner_entry(owner) is None
    assert key not in _RETIRING
    assert sibling._durable_job_lane is sibling_handle
    assert _owner_entry(sibling) is sibling_handle
    assert sibling_handle.lane._closed is False


def test_holder_detach_noarg_after_lease_release_does_not_keep_retiring(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.lane import LaneClosedError
    from gateway.durable_job_lane import (
        _RETIRING,
        _owner_key,
        attach_to_gateway_runner,
        detach_durable_job_lane,
    )

    owner = SimpleNamespace(_durable_job_lane=None)
    handle = attach_to_gateway_runner(
        owner,
        raw_config=_complete(tmp_path),
        **runtime_ready_transport_kwargs(monkeypatch),
    )
    assert handle is not None
    key = _owner_key(owner)

    with handle.lane._mutation_lease():
        try:
            detach_durable_job_lane(handle)
        except LaneClosedError:
            pass

    detach_durable_job_lane()
    assert key not in _RETIRING
    assert _RETIRING == {}
    assert owner._durable_job_lane is None
    assert handle.lane._closed is True
