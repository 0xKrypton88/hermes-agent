"""ENG-3 Package 1 — durable-jobs feature config is disabled by default."""

from __future__ import annotations

import pytest


def test_durable_jobs_defaults_are_disabled_and_require_explicit_sqlite_path():
    from agent.durable_jobs.config import (
        DEFAULT_DURABLE_JOBS_CONFIG,
        DurableJobsConfig,
        load_durable_jobs_config,
    )

    assert DEFAULT_DURABLE_JOBS_CONFIG["enabled"] is False
    assert DEFAULT_DURABLE_JOBS_CONFIG["dispatch_enabled"] is False
    assert DEFAULT_DURABLE_JOBS_CONFIG.get("sqlite_path") in (None, "")

    cfg = load_durable_jobs_config({})
    assert isinstance(cfg, DurableJobsConfig)
    assert cfg.enabled is False
    assert cfg.dispatch_enabled is False
    assert cfg.sqlite_path is None


def test_dispatch_attempt_rejected_when_feature_disabled(tmp_path):
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.service import (
        DispatchDisabledError,
        DurableJobService,
    )

    cfg = load_durable_jobs_config(
        {
            "durable_jobs": {
                "enabled": False,
                "dispatch_enabled": False,
                "sqlite_path": str(tmp_path / "jobs.sqlite"),
                "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
            }
        }
    )
    service = DurableJobService(config=cfg)
    with pytest.raises(DispatchDisabledError):
        service.attempt_dispatch(job_id="job-does-not-matter")


def test_dispatch_attempt_rejected_when_enabled_but_dispatch_flag_off(tmp_path):
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.service import (
        DispatchDisabledError,
        DurableJobService,
    )

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
    with pytest.raises(DispatchDisabledError):
        service.attempt_dispatch(job_id="still-blocked")
