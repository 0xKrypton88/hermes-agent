"""ENG-3 Package 1 — external adapters are injected fakes only; no live I/O."""

from __future__ import annotations

import socket

import pytest


def test_attempt_dispatch_never_calls_injected_adapter_when_disabled(tmp_path):
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.service import DispatchDisabledError, DurableJobService

    calls: list[str] = []

    class FakeDispatch:
        def dispatch(self, job_id: str) -> None:
            calls.append(job_id)

    cfg = load_durable_jobs_config(
        {
            "durable_jobs": {
                "enabled": True,
                "dispatch_enabled": False,
                "sqlite_path": str(tmp_path / "jobs.sqlite"),
                "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
            }
        }
    )
    service = DurableJobService(config=cfg, dispatch_adapter=FakeDispatch())
    with pytest.raises(DispatchDisabledError):
        service.attempt_dispatch("job-x")
    assert calls == []


def test_package1_hard_rejects_dispatch_even_when_both_flags_true_with_fake_adapter(
    tmp_path,
):
    """Package 1 has no external dispatch capability whatsoever."""
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.service import DispatchDisabledError, DurableJobService

    calls: list[str] = []

    class FakeDispatch:
        def dispatch(self, job_id: str) -> None:
            calls.append(job_id)

    cfg = load_durable_jobs_config(
        {
            "durable_jobs": {
                "enabled": True,
                "dispatch_enabled": True,
                "sqlite_path": str(tmp_path / "jobs.sqlite"),
                "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
            }
        }
    )
    assert cfg.enabled is True
    assert cfg.dispatch_enabled is True
    service = DurableJobService(config=cfg, dispatch_adapter=FakeDispatch())
    with pytest.raises(DispatchDisabledError):
        service.attempt_dispatch("job-both-flags-on")
    assert calls == []


def test_no_live_dispatch_adapter_is_exported():
    import agent.durable_jobs.adapters as adapters

    assert hasattr(adapters, "DispatchPort")
    assert hasattr(adapters, "NullDispatchAdapter")
    assert not hasattr(adapters, "SlackDispatchAdapter")
    assert not hasattr(adapters, "CursorCloudAdapter")


def test_pilot_flow_and_dispatch_rejection_open_no_network_sockets(tmp_path, monkeypatch):
    """Package 1 must be unable to reach Slack/Cursor/network during the flow."""
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.service import DispatchDisabledError, DurableJobService

    def _deny(*_args, **_kwargs):
        raise AssertionError("network socket open attempted in durable_jobs pilot")

    monkeypatch.setattr(socket.socket, "connect", _deny)
    monkeypatch.setattr(socket.socket, "connect_ex", _deny)

    cfg = load_durable_jobs_config(
        {
            "durable_jobs": {
                "enabled": True,
                "dispatch_enabled": False,
                "sqlite_path": str(tmp_path / "jobs.sqlite"),
                "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
            }
        }
    )
    service = DurableJobService(config=cfg)
    job = service.create_and_advance(
        origin_platform="cli",
        origin_chat_id="local",
        origin_root_thread_id="root",
        objective="offline only",
        repository_identity="repo",
        idempotency_key="idem-offline",
        frozen_baseline_sha="sha1",
    )
    assert job.job_id
    with pytest.raises(DispatchDisabledError):
        service.attempt_dispatch(job.job_id)
