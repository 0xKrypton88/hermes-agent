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


@pytest.mark.parametrize(
    "field,value",
    [
        ("enabled", "false"),
        ("enabled", "true"),
        ("enabled", "0"),
        ("enabled", "1"),
        ("enabled", 1),
        ("enabled", 0),
        ("dispatch_enabled", "false"),
        ("dispatch_enabled", "true"),
        ("dispatch_enabled", 1),
        ("dispatch_enabled", 0),
    ],
)
def test_non_bool_flag_values_are_rejected(field, value):
    from agent.durable_jobs.config import (
        DurableJobsConfigError,
        load_durable_jobs_config,
    )

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config({"durable_jobs": {field: value}})
    assert field in str(exc.value)


@pytest.mark.parametrize(
    "raw",
    [
        "not-a-mapping",
        ["durable_jobs"],
        42,
        True,
        object(),
    ],
)
def test_non_mapping_raw_config_is_rejected(raw):
    from agent.durable_jobs.config import (
        DurableJobsConfigError,
        load_durable_jobs_config,
    )

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config(raw)
    assert "mapping" in str(exc.value).lower()


@pytest.mark.parametrize(
    "section",
    [
        "enabled",
        ["nested"],
        1,
        True,
        None,
        object(),
    ],
)
def test_non_mapping_durable_jobs_section_is_rejected(section):
    from agent.durable_jobs.config import (
        DurableJobsConfigError,
        load_durable_jobs_config,
    )

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config({"durable_jobs": section})
    msg = str(exc.value).lower()
    assert "durable_jobs" in msg
    assert "mapping" in msg


def test_none_raw_config_loads_disabled_defaults():
    from agent.durable_jobs.config import DurableJobsConfig, load_durable_jobs_config

    cfg = load_durable_jobs_config(None)
    assert isinstance(cfg, DurableJobsConfig)
    assert cfg.enabled is False
    assert cfg.dispatch_enabled is False
    assert cfg.dispatch_allowed is False
