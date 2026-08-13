"""ENG-27 — durable Slack job-thread contract (isolated, default-off).

Deterministic fakes only. No Slack API, gateway routing fork, network, or
live state.db. Binding is the authority — not Slack history.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pytest


def _db(tmp_path: Path) -> Path:
    return tmp_path / "pilot_jobs.sqlite"


def _make_job(tmp_path: Path, *, idempotency_key: str = "idem-eng27"):
    from agent.durable_jobs.store import DurableJobStore

    store = DurableJobStore(sqlite_path=_db(tmp_path))
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="ENG-27 slice",
        repository_identity="github.com/example/repo",
        idempotency_key=idempotency_key,
    )
    return store, job


def _bind_kwargs(job_id: str, **overrides):
    base = dict(
        job_id=job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    base.update(overrides)
    return base


@dataclass
class FakePosted:
    message_ts: str
    client_msg_id: str


@dataclass
class FakePostResult:
    kind: str  # accepted | lost_response | ambiguous_response
    message_ts: Optional[str] = None


class FakeSlackPort:
    def __init__(
        self,
        post_result: FakePostResult,
        lookups: Optional[List[FakePosted]] = None,
    ) -> None:
        self.post_result = post_result
        self.lookups = list(lookups or [])
        self.posts: list[dict] = []
        self.lookup_calls: list[str] = []

    def post_root(
        self,
        *,
        client_msg_id: str,
        workspace_id: str,
        channel_id: str,
        root_thread_ts: str,
        job_id: str,
    ) -> FakePostResult:
        self.posts.append(
            {
                "client_msg_id": client_msg_id,
                "workspace_id": workspace_id,
                "channel_id": channel_id,
                "root_thread_ts": root_thread_ts,
                "job_id": job_id,
            }
        )
        return self.post_result

    def lookup_by_client_msg_id(self, client_msg_id: str) -> list[FakePosted]:
        self.lookup_calls.append(client_msg_id)
        return list(self.lookups)


def test_binding_required_before_any_slack_effect(tmp_path):
    from agent.durable_jobs.slack_contract import (
        BindingRequiredError,
        SlackBindingLedger,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    port = FakeSlackPort(FakePostResult(kind="accepted", message_ts="999.1"))
    with pytest.raises(BindingRequiredError):
        deliver_slack_root(ledger, port, job_id=job.job_id)
    assert port.posts == []
    assert ledger.get_binding(job.job_id) is None


def test_duplicate_ingress_adopts_same_binding(tmp_path):
    from agent.durable_jobs.slack_contract import SlackBindingLedger

    store, job = _make_job(tmp_path)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    first = ledger.bind(**_bind_kwargs(job.job_id))
    second = ledger.bind(**_bind_kwargs(job.job_id))
    assert second.job_id == first.job_id
    assert second.outbound_client_msg_id == first.outbound_client_msg_id
    assert second.root_thread_ts == first.root_thread_ts
    assert ledger.count_bindings() == 1


def test_rebind_to_different_root_or_candidate_or_version_is_rejected(tmp_path):
    from agent.durable_jobs.slack_contract import BindingConflict, SlackBindingLedger

    store, job = _make_job(tmp_path)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    ledger.bind(**_bind_kwargs(job.job_id))
    with pytest.raises(BindingConflict):
        ledger.bind(**_bind_kwargs(job.job_id, root_thread_ts="333.444"))
    with pytest.raises(BindingConflict):
        ledger.bind(**_bind_kwargs(job.job_id, candidate_id="cand-other"))
    with pytest.raises(BindingConflict):
        ledger.bind(**_bind_kwargs(job.job_id, candidate_version="v2"))
    frozen = ledger.get_binding(job.job_id)
    assert frozen is not None
    assert frozen.root_thread_ts == "111.222"
    assert frozen.candidate_id == "cand-1"
    assert frozen.candidate_version == "v1"


def test_outbound_client_msg_id_is_stable_across_restart(tmp_path):
    from agent.durable_jobs.slack_contract import SlackBindingLedger

    store, job = _make_job(tmp_path)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    bound = ledger.bind(**_bind_kwargs(job.job_id))
    assert bound.outbound_client_msg_id
    reopened = SlackBindingLedger(sqlite_path=store.sqlite_path)
    loaded = reopened.get_binding(job.job_id)
    assert loaded is not None
    assert loaded.outbound_client_msg_id == bound.outbound_client_msg_id


def test_binding_survives_store_recreation(tmp_path):
    from agent.durable_jobs.slack_contract import SlackBindingLedger, SlackRootStatus

    store, job = _make_job(tmp_path)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    ledger.bind(**_bind_kwargs(job.job_id))
    reopened = SlackBindingLedger(sqlite_path=store.sqlite_path)
    loaded = reopened.get_binding(job.job_id)
    assert loaded is not None
    assert loaded.status is SlackRootStatus.BOUND
    assert loaded.workspace_id == "T1"
    assert loaded.channel_id == "C123"
    assert loaded.root_thread_ts == "111.222"
    assert loaded.candidate_version == "v1"


def test_lost_slack_response_unique_lookup_adopts_without_duplicate_root(tmp_path):
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    bound = ledger.bind(**_bind_kwargs(job.job_id))
    port = FakeSlackPort(
        FakePostResult(kind="lost_response"),
        lookups=[FakePosted("10.1", bound.outbound_client_msg_id)],
    )
    first = deliver_slack_root(ledger, port, job_id=job.job_id)
    assert first.status is SlackRootStatus.ADOPTED
    assert first.delivered_message_ts == "10.1"
    second = deliver_slack_root(ledger, port, job_id=job.job_id)
    assert second.status is SlackRootStatus.ADOPTED
    assert len(port.posts) == 1


def test_lost_slack_response_ambiguous_does_not_post_second_root(tmp_path):
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    bound = ledger.bind(**_bind_kwargs(job.job_id))
    cmid = bound.outbound_client_msg_id
    port = FakeSlackPort(
        FakePostResult(kind="lost_response"),
        lookups=[FakePosted("10.1", cmid), FakePosted("10.2", cmid)],
    )
    first = deliver_slack_root(ledger, port, job_id=job.job_id)
    assert first.status is SlackRootStatus.UNKNOWN
    second = deliver_slack_root(ledger, port, job_id=job.job_id)
    assert second.status is SlackRootStatus.UNKNOWN
    assert len(port.posts) == 1


def test_cross_job_root_resume_is_rejected(tmp_path):
    from agent.durable_jobs.slack_contract import BindingConflict, SlackBindingLedger

    store, job_a = _make_job(tmp_path, idempotency_key="idem-a")
    job_b = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="other job",
        repository_identity="github.com/example/repo",
        idempotency_key="idem-b",
    )
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    ledger.bind(**_bind_kwargs(job_a.job_id))
    with pytest.raises(BindingConflict):
        ledger.bind(**_bind_kwargs(job_b.job_id))
    assert ledger.get_binding(job_b.job_id) is None
    assert ledger.get_by_root("T1", "C123", "111.222").job_id == job_a.job_id


def test_cross_binding_resume_is_rejected(tmp_path):
    from agent.durable_jobs.slack_contract import BindingConflict, SlackBindingLedger

    store, job = _make_job(tmp_path)
    ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
    ledger.bind(**_bind_kwargs(job.job_id))
    with pytest.raises(BindingConflict):
        ledger.resume(
            job_id=job.job_id,
            workspace_id="T1",
            channel_id="C123",
            root_thread_ts="999.000",
            candidate_id="cand-1",
            candidate_version="v1",
        )
    loaded = ledger.get_binding(job.job_id)
    assert loaded is not None
    assert loaded.root_thread_ts == "111.222"


def test_lane_cursor_create_requires_binding_before_provider_effect(tmp_path):
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.lane import DurableLaneService
    from agent.durable_jobs.slack_contract import BindingRequiredError

    store, job = _make_job(tmp_path)
    cfg = load_durable_jobs_config(
        {
            "durable_jobs": {
                "enabled": True,
                "dispatch_enabled": False,
                "sqlite_path": str(store.sqlite_path),
                "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
            }
        }
    )
    create_calls: list[str] = []

    class FakeProvider:
        def create_run(self, *, idempotency_key: str, job_id: str):
            create_calls.append(job_id)

            class _R:
                kind = "accepted"
                run = None

            return _R()

        def lookup_runs(self, *, idempotency_key: str):
            return []

    lane = DurableLaneService(config=cfg, store=store)
    with pytest.raises(BindingRequiredError):
        lane.reconcile_cursor_create(
            job_id=job.job_id,
            action_id="create_run",
            origin_platform=job.origin_platform,
            origin_chat_id=job.origin_chat_id,
            origin_root_thread_id=job.origin_root_thread_id,
            candidate_id="cand-1",
            candidate_version="v1",
            provider=FakeProvider(),
        )
    assert create_calls == []


def test_slack_paths_rejected_when_pilot_disabled(tmp_path):
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.lane import DurableLaneService
    from agent.durable_jobs.service import PilotDisabledError

    cfg = load_durable_jobs_config(
        {
            "durable_jobs": {
                "enabled": False,
                "dispatch_enabled": False,
                "sqlite_path": str(_db(tmp_path)),
                "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
            }
        }
    )
    port = FakeSlackPort(FakePostResult(kind="accepted", message_ts="1.1"))
    lane = DurableLaneService(config=cfg)
    with pytest.raises(PilotDisabledError):
        lane.bind_slack(**_bind_kwargs("dj_nope"))
    with pytest.raises(PilotDisabledError):
        lane.deliver_slack_root(job_id="dj_nope", slack_port=port)
    assert port.posts == []
