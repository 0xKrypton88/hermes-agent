"""DurableJobLaneHandle.shutdown() from a mutation-lease holder (ENG-36 P1).

``lane.close()`` from the lease-holder thread raises ``LaneClosedError``
after dropping the store. Handle ``shutdown()`` must not swallow that into
a normal return: the ACK/adapter callback must not continue, and consume
must not report ok/acked (or adapter accepted/delivered) after shutdown
has returned. No self-deadlock. Store is not reopened.

No live Slack/Cursor/network. Barriers use threading.Event, not sleeps.
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from tests.agent.durable_jobs.eng28_support import (
    FakeCreateResult,
    FakePostResult,
    FakeRun,
    RecordingAckPort,
    count_table,
)
from tests.agent.durable_jobs.package2_support import attach_runtime_ready_lane


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


@pytest.fixture(autouse=True)
def _reset_lane_seam():
    from gateway.durable_job_lane import detach_durable_job_lane

    detach_durable_job_lane()
    yield
    detach_durable_job_lane()


def _seed_handle(tmp_path, monkeypatch, *, idempotency_key: str):
    from agent.durable_jobs.decisions import DecisionLedger
    from agent.durable_jobs.slack_contract import SlackBindingLedger
    from tests.agent.durable_jobs.authz_fixtures import (
        install_default_adapter_authorization,
    )

    handle = attach_runtime_ready_lane(
        raw_config=_complete(tmp_path),
        monkeypatch=monkeypatch,
    )
    store = handle.lane._require_sqlite_path()
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="handle-shutdown-holder",
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


def _inbound(job, *, decision_idempotency_key: str) -> dict:
    return dict(
        job_id=job.job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        actor_id="U-alice",
        decision_type="go",
        decision_idempotency_key=decision_idempotency_key,
        policy_version="pol-1",
        candidate_id="cand-1",
        candidate_version="v1",
    )


def test_handle_shutdown_from_non_holder_returns_and_closes(tmp_path, monkeypatch):
    handle, _job, _store = _seed_handle(
        tmp_path, monkeypatch, idempotency_key="idem-non-holder"
    )
    handle.shutdown()
    assert handle.lane._closed is True
    assert handle.lane._store is None
    assert handle.lane._active_leases == 0


def test_handle_shutdown_on_lease_holder_raises_and_does_not_return(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.lane import LaneClosedError

    handle, _job, _store = _seed_handle(
        tmp_path, monkeypatch, idempotency_key="idem-holder-unit"
    )
    done = threading.Event()
    observed: list[str] = []

    def holder() -> None:
        try:
            with handle.lane._mutation_lease():
                try:
                    handle.shutdown()
                    observed.append("returned")
                except LaneClosedError:
                    observed.append("raised")
                assert handle.lane._store is None
        finally:
            done.set()

    worker = threading.Thread(target=holder, name="handle-shutdown-holder")
    worker.start()
    finished = done.wait(timeout=5.0)
    if not finished:
        pytest.fail("handle.shutdown() deadlocked on the lease-holder thread")
    worker.join(timeout=1.0)
    assert not worker.is_alive()
    assert observed == ["raised"]
    assert handle.lane._closed is True
    assert handle.lane._store is None
    assert handle.lane._active_leases == 0


def test_ack_calling_handle_shutdown_does_not_complete_ok_acked(
    tmp_path, monkeypatch
):
    handle, job, store = _seed_handle(
        tmp_path, monkeypatch, idempotency_key="idem-ack-handle"
    )
    inbound_before = count_table(store.sqlite_path, "job_inbound_actions")
    decisions_before = count_table(store.sqlite_path, "job_decisions")
    continued_after_shutdown: list[str] = []
    inner = RecordingAckPort()
    done = threading.Event()
    results: list = []

    class _AckShutdown:
        def ack(self, *, inbound_id: str, job_id: str) -> str:
            handle.shutdown()
            continued_after_shutdown.append("after-shutdown-return")
            return inner.ack(inbound_id=inbound_id, job_id=job_id)

    def runner() -> None:
        try:
            results.append(
                handle.lane.consume_inbound_action(
                    _AckShutdown(),
                    **_inbound(job, decision_idempotency_key="dec-ack-handle"),
                )
            )
        finally:
            done.set()

    worker = threading.Thread(target=runner, name="ack-handle-shutdown")
    worker.start()
    finished = done.wait(timeout=5.0)
    if not finished:
        pytest.fail(
            "consume deadlocked: ACK called handle.shutdown() on the "
            "lease-holding thread"
        )
    worker.join(timeout=1.0)
    assert not worker.is_alive()
    assert continued_after_shutdown == []
    assert results, "consume_inbound_action must return after ACK shutdown"
    result = results[0]
    assert result.ack_status == "pending"
    assert result.ack_status != "acked"
    assert inner.acks == []
    assert handle.lane._closed is True
    assert handle.lane._store is None
    replay = RecordingAckPort()
    again = handle.lane.consume_inbound_action(
        replay, **_inbound(job, decision_idempotency_key="dec-ack-handle")
    )
    assert again.ok is False
    assert again.retryable is True
    assert replay.acks == []
    assert count_table(store.sqlite_path, "job_inbound_actions") == inbound_before + 1
    assert count_table(store.sqlite_path, "job_decisions") == decisions_before + 1


def test_slack_post_root_calling_handle_shutdown_does_not_accept_after_return(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.lane import LaneClosedError
    from agent.durable_jobs.slack_contract import SlackBindingLedger, SlackRootStatus

    handle, job, store = _seed_handle(
        tmp_path, monkeypatch, idempotency_key="idem-slack-handle"
    )
    continued_after_shutdown: list[str] = []
    posted: list[str] = []

    class _Slack:
        def post_root(self, **_k):
            posted.append("post_root")
            handle.shutdown()
            continued_after_shutdown.append("after-shutdown-return")
            return FakePostResult(kind="accepted", message_ts="111.999")

        def lookup_by_client_msg_id(self, client_msg_id: str):
            continued_after_shutdown.append("lookup-after-shutdown")
            return []

    done = threading.Event()
    raised: list[BaseException] = []
    delivered = []

    def runner() -> None:
        try:
            delivered.append(
                handle.lane.deliver_slack_root(job_id=job.job_id, slack_port=_Slack())
            )
        except LaneClosedError as exc:
            raised.append(exc)
        finally:
            done.set()

    worker = threading.Thread(target=runner, name="slack-handle-shutdown")
    worker.start()
    finished = done.wait(timeout=5.0)
    if not finished:
        pytest.fail(
            "deliver_slack_root deadlocked: post_root called handle.shutdown()"
        )
    worker.join(timeout=1.0)
    assert not worker.is_alive()
    assert posted == ["post_root"]
    assert continued_after_shutdown == []
    assert delivered == []
    assert raised and isinstance(raised[0], LaneClosedError)
    binding = SlackBindingLedger(sqlite_path=store.sqlite_path).get_binding(job.job_id)
    assert binding is not None
    assert binding.status not in (
        SlackRootStatus.DELIVERED,
        SlackRootStatus.ADOPTED,
    )
    assert handle.lane._store is None


def test_provider_create_run_calling_handle_shutdown_does_not_accept_after_return(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.effects import EffectStatus, ProviderEffectLedger
    from agent.durable_jobs.lane import LaneClosedError

    handle, job, store = _seed_handle(
        tmp_path, monkeypatch, idempotency_key="idem-provider-handle"
    )
    continued_after_shutdown: list[str] = []
    created: list[str] = []

    class _Provider:
        def create_run(self, *, idempotency_key: str, job_id: str):
            created.append("create_run")
            handle.shutdown()
            continued_after_shutdown.append("after-shutdown-return")
            return FakeCreateResult(
                kind="accepted",
                run=FakeRun(run_id="run-after-shutdown", idempotency_key=idempotency_key),
            )

        def lookup_runs(self, *, idempotency_key: str):
            continued_after_shutdown.append("lookup-after-shutdown")
            return []

    done = threading.Event()
    raised: list[BaseException] = []
    claims = []

    def runner() -> None:
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

    worker = threading.Thread(target=runner, name="provider-handle-shutdown")
    worker.start()
    finished = done.wait(timeout=5.0)
    if not finished:
        pytest.fail(
            "reconcile_cursor_create deadlocked: create_run called "
            "handle.shutdown()"
        )
    worker.join(timeout=1.0)
    assert not worker.is_alive()
    assert created == ["create_run"]
    assert continued_after_shutdown == []
    assert claims == []
    assert raised and isinstance(raised[0], LaneClosedError)
    claim = ProviderEffectLedger(sqlite_path=store.sqlite_path).get_claim(
        job.job_id, "create_run"
    )
    if claim is not None:
        assert claim.status not in (EffectStatus.ACCEPTED, EffectStatus.ADOPTED)
    assert handle.lane._store is None
