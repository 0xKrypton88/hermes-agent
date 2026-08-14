"""ENG-36: in-flight retirement must fence every lease-holder writer.

C1: two mutation-lease holders both disabled-reattach. The first pops
``_LANES`` and blocks in shutdown waiting for the peer lease. The second
must not treat the empty registry as success and continue to ACK.

Also covers single-holder invalid/config-failed reattach, nested leases,
no-arg detach cleanup, and handle detach from an ACK callback.

No live Slack/Cursor/network. No Gateway start. Barriers use
threading.Event, not sleeps.
"""

from __future__ import annotations

import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.agent.durable_jobs.eng28_support import RecordingAckPort
from tests.agent.durable_jobs.package2_support import runtime_ready_transport_kwargs
from tests.agent.durable_jobs.test_handle_shutdown_lease_holder import (
    _complete,
    _inbound,
)
from tests.agent.durable_jobs.test_reentrant_reattach_and_detach import (
    _run_consume,
    _seed_owned,
)


@pytest.fixture(autouse=True)
def _reset_lane_seam():
    from gateway.durable_job_lane import detach_durable_job_lane

    detach_durable_job_lane()
    yield
    detach_durable_job_lane()


def _owner_entry(owner):
    from gateway.durable_job_lane import _LANES, _owner_key

    return _LANES.get(_owner_key(owner))


def _assert_detached(owner, previous) -> None:
    from gateway.durable_job_lane import _LANES

    assert _owner_entry(owner) is None
    assert getattr(owner, "_durable_job_lane", None) is None
    assert previous not in _LANES.values()
    assert previous.lane._closed is True


def test_single_holder_disabled_reattach_raises_and_does_not_continue(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.lane import LaneClosedError
    from gateway.durable_job_lane import attach_to_gateway_runner

    runner = SimpleNamespace(_durable_job_lane=None)
    handle, _job, _store = _seed_owned(
        tmp_path, monkeypatch, runner, idempotency_key="idem-single-disabled"
    )
    transports = runtime_ready_transport_kwargs(monkeypatch)
    events: list[str] = []
    raised: list[BaseException] = []

    def holder() -> None:
        with handle.lane._mutation_lease():
            events.append("lease")
            try:
                attach_to_gateway_runner(
                    runner,
                    raw_config=_complete(tmp_path, enabled=False),
                    **transports,
                )
                events.append("continued")
            except LaneClosedError as exc:
                raised.append(exc)

    worker = threading.Thread(target=holder, name="single-disabled")
    worker.start()
    worker.join(timeout=5.0)
    assert not worker.is_alive(), "single-holder disabled reattach deadlocked"
    assert events == ["lease"]
    assert raised and isinstance(raised[0], LaneClosedError)
    _assert_detached(runner, handle)


def test_single_holder_invalid_and_config_failed_reattach_raise(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.lane import LaneClosedError
    from gateway.durable_job_lane import attach_to_gateway_runner

    transports = runtime_ready_transport_kwargs(monkeypatch)
    for label, kwargs, setup in (
        (
            "invalid",
            {"raw_config": {"durable_jobs": "not-a-mapping"}},
            None,
        ),
        (
            "config-failed",
            {},
            lambda: monkeypatch.setattr(
                "hermes_cli.config.load_config",
                lambda: (_ for _ in ()).throw(RuntimeError("config read failed")),
            ),
        ),
    ):
        runner = SimpleNamespace(_durable_job_lane=None)
        handle, _job, _store = _seed_owned(
            tmp_path / label, monkeypatch, runner, idempotency_key=f"idem-{label}"
        )
        if setup is not None:
            setup()
        events: list[str] = []
        raised: list[BaseException] = []

        def holder() -> None:
            with handle.lane._mutation_lease():
                events.append("lease")
                try:
                    attach_to_gateway_runner(runner, **kwargs, **transports)
                    events.append("continued")
                except LaneClosedError as exc:
                    raised.append(exc)

        worker = threading.Thread(target=holder, name=f"single-{label}")
        worker.start()
        worker.join(timeout=5.0)
        assert not worker.is_alive(), f"{label} reattach deadlocked"
        assert events == ["lease"], label
        assert raised and isinstance(raised[0], LaneClosedError), label
        _assert_detached(runner, handle)


def test_two_holders_disabled_reattach_both_raise_lane_closed(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.lane import LaneClosedError
    from gateway.durable_job_lane import attach_to_gateway_runner

    runner = SimpleNamespace(_durable_job_lane=None)
    sibling = SimpleNamespace(_durable_job_lane=None)
    handle, _job, _store = _seed_owned(
        tmp_path / "a", monkeypatch, runner, idempotency_key="idem-dual-a"
    )
    sibling_handle = attach_to_gateway_runner(
        sibling,
        raw_config=_complete(tmp_path / "b"),
        **runtime_ready_transport_kwargs(monkeypatch),
    )
    assert sibling_handle is not None
    transports = runtime_ready_transport_kwargs(monkeypatch)
    entered = [threading.Event(), threading.Event()]
    done = [threading.Event(), threading.Event()]
    outcomes: list[str] = ["", ""]

    def holder(index: int) -> None:
        try:
            with handle.lane._mutation_lease():
                entered[index].set()
                if not entered[1 - index].wait(timeout=5.0):
                    outcomes[index] = "peer-missing"
                    return
                try:
                    attach_to_gateway_runner(
                        runner,
                        raw_config=_complete(tmp_path / "a", enabled=False),
                        **transports,
                    )
                    outcomes[index] = "continued"
                except LaneClosedError:
                    outcomes[index] = "LaneClosedError"
        finally:
            done[index].set()

    workers = [
        threading.Thread(target=holder, args=(i,), name=f"dual-disabled-{i}")
        for i in (0, 1)
    ]
    for worker in workers:
        worker.start()
    if not entered[0].wait(timeout=5.0) or not entered[1].wait(timeout=5.0):
        pytest.fail("both threads must hold a mutation lease before reattach")
    finished = done[0].wait(timeout=5.0) and done[1].wait(timeout=5.0)
    if not finished:
        pytest.fail("dual-holder disabled reattach deadlocked")
    for worker in workers:
        worker.join(timeout=1.0)
        assert not worker.is_alive()
    assert outcomes == ["LaneClosedError", "LaneClosedError"]
    _assert_detached(runner, handle)
    assert sibling._durable_job_lane is sibling_handle
    assert _owner_entry(sibling) is sibling_handle
    assert sibling_handle.lane._closed is False


def test_two_ack_holders_disabled_reattach_neither_acks(tmp_path, monkeypatch):
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.slack_contract import SlackBindingLedger
    from gateway.durable_job_lane import attach_to_gateway_runner
    from tests.agent.durable_jobs.authz_fixtures import (
        install_default_adapter_authorization,
    )

    runner = SimpleNamespace(_durable_job_lane=None)
    handle, job_a, store = _seed_owned(
        tmp_path, monkeypatch, runner, idempotency_key="idem-ack-dual-a"
    )
    job_b = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="333.444",
        objective="dual-ack-b",
        repository_identity="github.com/example/repo",
        idempotency_key="idem-ack-dual-b",
    )
    SlackBindingLedger(sqlite_path=store.sqlite_path).bind(
        job_id=job_b.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="333.444",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    DecisionLedger(sqlite_path=store.sqlite_path).set_policy(
        job_id=job_b.job_id,
        policy_version="pol-1",
        allowed_actors=("U-alice",),
        expires_at="2099-01-01T00:00:00+00:00",
    )
    install_default_adapter_authorization(store.sqlite_path, job_b.job_id)
    transports = runtime_ready_transport_kwargs(monkeypatch)
    entered = [threading.Event(), threading.Event()]
    done = [threading.Event(), threading.Event()]
    results: list[list] = [[], []]
    continued = [False, False]

    def make_ack(index: int):
        class _AckReattach:
            def ack(self, *, inbound_id: str, job_id: str) -> str:
                entered[index].set()
                if not entered[1 - index].wait(timeout=5.0):
                    raise RuntimeError("peer consume never reached ACK")
                attach_to_gateway_runner(
                    runner,
                    raw_config=_complete(tmp_path, enabled=False),
                    **transports,
                )
                continued[index] = True
                return f"ack:{inbound_id}"

        return _AckReattach()

    jobs = (job_a, job_b)
    keys = ("dec-ack-dual-a", "dec-ack-dual-b")
    threads_ts = ("111.222", "333.444")

    def runner_fn(index: int) -> None:
        try:
            kwargs = _inbound(jobs[index], decision_idempotency_key=keys[index])
            kwargs["root_thread_ts"] = threads_ts[index]
            results[index].append(
                handle.lane.consume_inbound_action(make_ack(index), **kwargs)
            )
        finally:
            done[index].set()

    workers = [
        threading.Thread(target=runner_fn, args=(i,), name=f"ack-dual-{i}")
        for i in (0, 1)
    ]
    for worker in workers:
        worker.start()
    if not entered[0].wait(timeout=5.0) or not entered[1].wait(timeout=5.0):
        pytest.fail("both consume paths must reach ACK holding leases")
    finished = done[0].wait(timeout=5.0) and done[1].wait(timeout=5.0)
    if not finished:
        pytest.fail("dual ACK disabled reattach deadlocked")
    for worker in workers:
        worker.join(timeout=1.0)
        assert not worker.is_alive()
    assert continued == [False, False]
    assert results[0] and results[1]
    assert results[0][0].ok is False
    assert results[1][0].ok is False
    assert results[0][0].ack_status == "pending"
    assert results[1][0].ack_status == "pending"
    assert results[0][0].retryable is True
    assert results[1][0].retryable is True
    _assert_detached(runner, handle)
    replay = RecordingAckPort()
    again = handle.lane.consume_inbound_action(
        replay, **_inbound(job_a, decision_idempotency_key="dec-ack-dual-a")
    )
    assert again.ok is False
    assert again.retryable is True
    assert replay.acks == []


def test_holder_detach_handle_from_ack_does_not_continue(tmp_path, monkeypatch):
    from gateway.durable_job_lane import detach_durable_job_lane

    runner = SimpleNamespace(_durable_job_lane=None)
    handle, job, _store = _seed_owned(
        tmp_path, monkeypatch, runner, idempotency_key="idem-ack-detach-handle"
    )
    events: list[str] = []
    acks: list[tuple[str, str]] = []

    class AckDetachHandle:
        def ack(self, *, inbound_id: str, job_id: str) -> str:
            events.append("ack-enter")
            detach_durable_job_lane(handle)
            events.append("after-detach-return")
            acks.append((inbound_id, job_id))
            return f"ack:{inbound_id}"

    result = _run_consume(handle, job, AckDetachHandle(), key="dec-ack-detach-handle")
    assert events == ["ack-enter"]
    assert acks == []
    assert result.ok is False
    assert result.ack_status == "pending"
    assert result.retryable is True
    _assert_detached(runner, handle)


def test_nested_lease_and_second_holder_disabled_reattach_both_fenced(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.lane import LaneClosedError
    from gateway.durable_job_lane import attach_to_gateway_runner

    runner = SimpleNamespace(_durable_job_lane=None)
    handle, _job, _store = _seed_owned(
        tmp_path, monkeypatch, runner, idempotency_key="idem-nested"
    )
    transports = runtime_ready_transport_kwargs(monkeypatch)
    entered = [threading.Event(), threading.Event()]
    done = [threading.Event(), threading.Event()]
    outcomes: list[str] = ["", ""]

    def nested_holder() -> None:
        try:
            with handle.lane._mutation_lease():
                with handle.lane._mutation_lease():
                    entered[0].set()
                    if not entered[1].wait(timeout=5.0):
                        outcomes[0] = "peer-missing"
                        return
                    try:
                        attach_to_gateway_runner(
                            runner,
                            raw_config=_complete(tmp_path, enabled=False),
                            **transports,
                        )
                        outcomes[0] = "continued"
                    except LaneClosedError:
                        outcomes[0] = "LaneClosedError"
        finally:
            done[0].set()

    def second_holder() -> None:
        try:
            with handle.lane._mutation_lease():
                entered[1].set()
                if not entered[0].wait(timeout=5.0):
                    outcomes[1] = "peer-missing"
                    return
                try:
                    attach_to_gateway_runner(
                        runner,
                        raw_config=_complete(tmp_path, enabled=False),
                        **transports,
                    )
                    outcomes[1] = "continued"
                except LaneClosedError:
                    outcomes[1] = "LaneClosedError"
        finally:
            done[1].set()

    workers = [
        threading.Thread(target=nested_holder, name="nested-holder"),
        threading.Thread(target=second_holder, name="second-holder"),
    ]
    for worker in workers:
        worker.start()
    finished = done[0].wait(timeout=5.0) and done[1].wait(timeout=5.0)
    if not finished:
        pytest.fail("nested + second holder disabled reattach deadlocked")
    for worker in workers:
        worker.join(timeout=1.0)
        assert not worker.is_alive()
    assert outcomes == ["LaneClosedError", "LaneClosedError"]
    _assert_detached(runner, handle)


def test_noarg_detach_clears_every_owner_field_and_registry(tmp_path, monkeypatch):
    from gateway.durable_job_lane import (
        _LANES,
        attach_to_gateway_runner,
        detach_durable_job_lane,
    )

    transports = runtime_ready_transport_kwargs(monkeypatch)
    owners = []
    handles = []
    for name in ("a", "b"):
        owner = SimpleNamespace(_durable_job_lane=None)
        handle = attach_to_gateway_runner(
            owner,
            raw_config=_complete(tmp_path / name),
            **transports,
        )
        assert handle is not None
        owners.append(owner)
        handles.append(handle)

    detach_durable_job_lane()

    assert _LANES == {}
    for owner, handle in zip(owners, handles):
        assert owner._durable_job_lane is None
        assert handle.lane._closed is True
