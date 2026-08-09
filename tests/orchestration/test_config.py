"""WP1 — orchestration config defaults, validation, and off-mode safety."""

from __future__ import annotations

import pytest


def test_orchestration_defaults_disabled_off_mode_with_family_aliases():
    from agent.orchestration.config import (
        DEFAULT_ORCHESTRATION_CONFIG,
        OrchestrationConfig,
        load_orchestration_config,
    )

    raw = DEFAULT_ORCHESTRATION_CONFIG
    assert raw["enabled"] is False
    assert raw["mode"] == "off"
    assert set(raw["families"]) >= {"LUNA", "TERRA", "SOL"}
    assert "reasoning_capabilities" in raw
    assert raw["budgets"]["max_attempts"] >= 1
    assert raw["telemetry"]["enabled"] is False

    cfg = load_orchestration_config({})
    assert isinstance(cfg, OrchestrationConfig)
    assert cfg.enabled is False
    assert cfg.mode == "off"
    assert cfg.families["LUNA"].provider_alias
    assert cfg.families["TERRA"].model_alias
    assert "low" in cfg.reasoning_capabilities
    assert cfg.budgets.max_attempts >= 1


def test_invalid_orchestration_mode_fails_clearly():
    from agent.orchestration.config import OrchestrationConfigError, load_orchestration_config

    with pytest.raises(OrchestrationConfigError) as exc:
        load_orchestration_config({"orchestration": {"mode": "live-trading"}})
    assert "mode" in str(exc.value).lower()


def test_off_and_shadow_preserve_single_model_behavior_flag():
    from agent.orchestration.config import load_orchestration_config

    off = load_orchestration_config({"orchestration": {"enabled": True, "mode": "off"}})
    shadow = load_orchestration_config({"orchestration": {"enabled": True, "mode": "shadow"}})
    active = load_orchestration_config({"orchestration": {"enabled": True, "mode": "active"}})

    assert off.preserves_legacy_execution is True
    assert shadow.preserves_legacy_execution is True
    assert active.preserves_legacy_execution is False
    assert active.mode == "active"
    # active must never be the default
    default = load_orchestration_config({})
    assert default.mode == "off"
    assert default.enabled is False


def test_default_config_includes_orchestration_section():
    from hermes_cli.config_defaults import DEFAULT_CONFIG

    orch = DEFAULT_CONFIG.get("orchestration")
    assert isinstance(orch, dict)
    assert orch.get("enabled") is False
    assert orch.get("mode") == "off"
