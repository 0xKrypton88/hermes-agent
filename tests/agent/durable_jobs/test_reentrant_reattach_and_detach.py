"""ENG-36: reentrant wrappers must not swallow LaneClosedError.

ACK/Slack/provider callbacks that hold a mutation lease and invoke
disabled reattach or the real Gateway detach wrapper must not continue
to ACK/accepted after holder shutdown. consume stays
ok=False / pending / retryable=True.

No live Slack/Cursor/network. No Gateway start. Barriers use
threading.Event, not sleeps.
"""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.agent.durable_jobs.eng28_support import (
    FakeCreateResult,
    FakePostResult,
    FakeRun,
    RecordingAckPort,
)
from tests.agent.durable_jobs.package2_support import runtime_ready_transport_kwargs
from tests.agent.durable_jobs.test_handle_shutdown_lease_holder import (
    _complete,
    _inbound,
)


@pytest.fixture(autouse=True)
def _reset_lane_seam():
    from gateway.durable_job_lane import detach_durable_job_lane

    detach_durable_job_lane()
    yield
    detach_durable_job_lane()


def _seed_owned(tmp_path: Path, monkeypatch, owner, *, idempotency_key: str):
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.slack_contract import SlackBindingLedger
    from gateway.durable_job_lane import attach_to_gateway_runner
    from tests.agent.durable_jobs.authz_fixtures import (
        install_default_adapter_authorization,
    )

    handle = attach_to_gateway_runner(
        owner,
        raw_config=_complete(tmp_path),
        **runtime_ready_transport_kwargs(monkeypatch),
    )
    assert handle is not None
    store = handle.lane._require_sqlite_path()
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="reentrant-wrapper",
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
    install_default_adapter_authorization(store.sqlite_path, job.job_id)
    return handle, job, store


def _run_consume(handle, job, ack_port, *, key: str, timeout: float = 5.0):
    done = threading.Event()
    results: list = []

    def runner() -> None:
        try:
            results.append(
                handle.lane.consume_inbound_action(
                    ack_port, **_inbound(job, decision_idempotency_key=key)
                )
            )
        finally:
            done.set()

    worker = threading.Thread(target=runner, name=f"consume-{key}")
    worker.start()
    finished = done.wait(timeout=timeout)
    if not finished:
        pytest.fail("consume deadlocked on reentrant holder shutdown")
    worker.join(timeout=1.0)
    assert not worker.is_alive()
    assert results, "consume_inbound_action must return"
    return results[0]


def test_disabled_reattach_from_ack_does_not_continue_or_ack(tmp_path, monkeypatch):
    from gateway.durable_job_lane import attach_to_gateway_runner

    runner = SimpleNamespace(_durable_job_lane=None)
    handle, job, _store = _seed_owned(
        tmp_path, monkeypatch, runner, idempotency_key="idem-ack-reattach"
    )
    events: list[str] = []
    acks: list[tuple[str, str]] = []
    transports = runtime_ready_transport_kwargs(monkeypatch)

    class AckReattach:
        def ack(self, *, inbound_id: str, job_id: str) -> str:
            events.append("ack-enter")
            attach_to_gateway_runner(
                runner,
                raw_config=_complete(tmp_path, enabled=False),
                **transports,
            )
            events.append("after-reattach-return")
            acks.append((inbound_id, job_id))
            return f"ack:{inbound_id}"

    result = _run_consume(handle, job, AckReattach(), key="dec-ack-reattach")
    assert events == ["ack-enter"]
    assert acks == []
    assert result.ok is False
    assert result.ack_status == "pending"
    assert result.retryable is True
    replay = RecordingAckPort()
    again = handle.lane.consume_inbound_action(
        replay, **_inbound(job, decision_idempotency_key="dec-ack-reattach")
    )
    assert again.ok is False
    assert again.retryable is True
    assert replay.acks == []


def test_disabled_reattach_from_slack_post_root_does_not_accept(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.lane import LaneClosedError
    from agent.durable_jobs.slack_contract import SlackBindingLedger, SlackRootStatus
    from gateway.durable_job_lane import attach_to_gateway_runner

    runner = SimpleNamespace(_durable_job_lane=None)
    handle, job, store = _seed_owned(
        tmp_path, monkeypatch, runner, idempotency_key="idem-slack-reattach"
    )
    events: list[str] = []
    transports = runtime_ready_transport_kwargs(monkeypatch)

    class _Slack:
        def post_root(self, **_k):
            events.append("post_root")
            attach_to_gateway_runner(
                runner,
                raw_config=_complete(tmp_path, enabled=False),
                **transports,
            )
            events.append("after-reattach-return")
            return FakePostResult(kind="accepted", message_ts="111.999")

        def lookup_by_client_msg_id(self, client_msg_id: str):
            events.append("lookup-after-reattach")
            return []

    done = threading.Event()
    raised: list[BaseException] = []
    delivered = []

    def runner_fn() -> None:
        try:
            delivered.append(
                handle.lane.deliver_slack_root(job_id=job.job_id, slack_port=_Slack())
            )
        except LaneClosedError as exc:
            raised.append(exc)
        finally:
            done.set()

    worker = threading.Thread(target=runner_fn, name="slack-reattach")
    worker.start()
    if not done.wait(timeout=5.0):
        pytest.fail("deliver_slack_root deadlocked on disabled reattach")
    worker.join(timeout=1.0)
    assert not worker.is_alive()
    assert events == ["post_root"]
    assert delivered == []
    assert raised and isinstance(raised[0], LaneClosedError)
    binding = SlackBindingLedger(sqlite_path=store.sqlite_path).get_binding(job.job_id)
    assert binding is not None
    assert binding.status not in (SlackRootStatus.DELIVERED, SlackRootStatus.ADOPTED)


def test_disabled_reattach_from_provider_create_run_does_not_accept(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.effects import EffectStatus, ProviderEffectLedger
    from agent.durable_jobs.lane import LaneClosedError
    from gateway.durable_job_lane import attach_to_gateway_runner

    runner = SimpleNamespace(_durable_job_lane=None)
    handle, job, store = _seed_owned(
        tmp_path, monkeypatch, runner, idempotency_key="idem-provider-reattach"
    )
    events: list[str] = []
    transports = runtime_ready_transport_kwargs(monkeypatch)

    class _Provider:
        def create_run(self, *, idempotency_key: str, job_id: str):
            events.append("create_run")
            attach_to_gateway_runner(
                runner,
                raw_config=_complete(tmp_path, enabled=False),
                **transports,
            )
            events.append("after-reattach-return")
            return FakeCreateResult(
                kind="accepted",
                run=FakeRun(run_id="run-after-reattach", idempotency_key=idempotency_key),
            )

        def lookup_runs(self, *, idempotency_key: str):
            events.append("lookup-after-reattach")
            return []

    done = threading.Event()
    raised: list[BaseException] = []
    claims = []

    def runner_fn() -> None:
        try:
            claims.append(
                handle.lane.reconcile_cursor_create(
                    job_id=job.job_id,
                    action_id="create_run",
                    origin_platform="slack",
                    origin_chat_id="C123",
                    origin_root_thread_id="111.222",
                    candidate_id="cand-1",
                    candidate_version="v1",
                    provider=_Provider(),
                )
            )
        except LaneClosedError as exc:
            raised.append(exc)
        finally:
            done.set()

    worker = threading.Thread(target=runner_fn, name="provider-reattach")
    worker.start()
    if not done.wait(timeout=5.0):
        pytest.fail("reconcile_cursor_create deadlocked on disabled reattach")
    worker.join(timeout=1.0)
    assert not worker.is_alive()
    assert events == ["create_run"]
    assert claims == []
    assert raised and isinstance(raised[0], LaneClosedError)
    claim = ProviderEffectLedger(sqlite_path=store.sqlite_path).get_claim(
        job.job_id, "create_run"
    )
    if claim is not None:
        assert claim.status not in (EffectStatus.ACCEPTED, EffectStatus.ADOPTED)


def test_maybe_detach_from_ack_does_not_continue_or_ack(tmp_path, monkeypatch):
    from gateway.config import GatewayConfig
    from gateway.run import GatewayRunner

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    runner = GatewayRunner(
        GatewayConfig(
            platforms={},
            sessions_dir=tmp_path / "sessions",
            loop_watchdog=False,
        )
    )
    handle, job, _store = _seed_owned(
        tmp_path, monkeypatch, runner, idempotency_key="idem-ack-detach"
    )
    events: list[str] = []
    acks: list[tuple[str, str]] = []

    class AckDetach:
        def ack(self, *, inbound_id: str, job_id: str) -> str:
            events.append("ack-enter")
            runner._maybe_detach_durable_job_lane()
            events.append("after-detach-return")
            acks.append((inbound_id, job_id))
            return f"ack:{inbound_id}"

    result = _run_consume(handle, job, AckDetach(), key="dec-ack-detach")
    assert events == ["ack-enter"]
    assert acks == []
    assert result.ok is False
    assert result.ack_status == "pending"
    assert result.retryable is True


@pytest.mark.asyncio
async def test_gateway_stop_wrapper_propagates_lane_closed(tmp_path):
    from agent.durable_jobs.lane import LaneClosedError
    from gateway.config import GatewayConfig
    from gateway.run import GatewayRunner

    runner = GatewayRunner(
        GatewayConfig(
            platforms={},
            sessions_dir=tmp_path / "sessions",
            loop_watchdog=False,
        )
    )

    async def _noop() -> None:
        return None

    already_done = asyncio.create_task(_noop())
    await already_done
    runner._stop_task = already_done

    def _boom() -> None:
        raise LaneClosedError("holder detach")

    runner._maybe_detach_durable_job_lane = _boom
    with pytest.raises(LaneClosedError):
        await asyncio.wait_for(runner.stop(), timeout=5)
