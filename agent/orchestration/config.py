"""Orchestration config load / validation.

Disabled by default (mode ``off``). Invalid config fails clearly.
Family aliases map through config — product logic must not hard-code
concrete model names.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Tuple


VALID_MODES = frozenset({"off", "shadow", "active"})
VALID_REASONING = frozenset({"low", "medium", "high", "max"})
FAMILY_NAMES = ("LUNA", "TERRA", "SOL")


DEFAULT_ORCHESTRATION_CONFIG: Dict[str, Any] = {
    "enabled": False,
    "mode": "off",  # off | shadow | active — active is never default
    "families": {
        # Aliases only — concrete models resolve via resolve_runtime_provider
        "LUNA": {
            "provider_alias": "delegation",
            "model_alias": "luna",
            "reasoning_default": "low",
            "toolsets": ["file", "web"],
        },
        "TERRA": {
            "provider_alias": "delegation",
            "model_alias": "terra",
            "reasoning_default": "medium",
            "toolsets": ["file", "web", "terminal", "browser"],
        },
        "SOL": {
            "provider_alias": "delegation",
            "model_alias": "sol",
            "reasoning_default": "high",
            "toolsets": ["file", "web", "terminal", "browser"],
        },
    },
    # Family alias → concrete model id resolved at runtime from this map
    # (or inheritance). Product code looks up aliases, never hard-codes names.
    "model_aliases": {
        "luna": "",
        "terra": "",
        "sol": "",
    },
    "reasoning_capabilities": {
        "low": True,
        "medium": True,
        "high": True,
        "max": True,
    },
    "budgets": {
        "max_attempts": 5,
        "max_cost_usd": 2.0,
        "max_duration_s": 600,
        "child_timeout_seconds": 300,
    },
    "verification": {
        "independent_for_sol": True,
        "schema_retry_once": True,
    },
    "telemetry": {
        "enabled": False,  # local traces only when explicitly enabled
        "retain_days": 14,
        "store_raw_prompt": False,
    },
    "approval": {
        "require_for_destructive": True,
        "require_for_financial": True,
        "workers_cannot_self_approve": True,
    },
    "schema_version": "orch.task_spec.v1",
    "policy_version": "orch.policy.v1",
    "prompt_version": "orch.prompt.v1",
}


class OrchestrationConfigError(ValueError):
    """Raised when orchestration config is invalid."""


@dataclass(frozen=True)
class FamilyAlias:
    provider_alias: str
    model_alias: str
    reasoning_default: str
    toolsets: Tuple[str, ...]


@dataclass(frozen=True)
class BudgetConfig:
    max_attempts: int
    max_cost_usd: float
    max_duration_s: int
    child_timeout_seconds: int


@dataclass(frozen=True)
class TelemetryConfig:
    enabled: bool
    retain_days: int
    store_raw_prompt: bool


@dataclass(frozen=True)
class VerificationConfig:
    independent_for_sol: bool
    schema_retry_once: bool


@dataclass(frozen=True)
class ApprovalConfig:
    require_for_destructive: bool
    require_for_financial: bool
    workers_cannot_self_approve: bool


@dataclass(frozen=True)
class OrchestrationConfig:
    enabled: bool
    mode: str
    families: Mapping[str, FamilyAlias]
    model_aliases: Mapping[str, str]
    reasoning_capabilities: Mapping[str, bool]
    budgets: BudgetConfig
    verification: VerificationConfig
    telemetry: TelemetryConfig
    approval: ApprovalConfig
    schema_version: str
    policy_version: str
    prompt_version: str

    @property
    def preserves_legacy_execution(self) -> bool:
        """off and shadow must preserve legacy single-model execution."""
        return self.mode in ("off", "shadow") or not self.enabled


def _deep_merge(base: Dict[str, Any], override: Mapping[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)  # type: ignore[arg-type]
        else:
            out[key] = value
    return out


def validate_orchestration_dict(raw: Mapping[str, Any]) -> None:
    """Validate a raw orchestration section; raise OrchestrationConfigError."""
    if not isinstance(raw, Mapping):
        raise OrchestrationConfigError("orchestration must be a mapping")

    mode = raw.get("mode", "off")
    if mode not in VALID_MODES:
        raise OrchestrationConfigError(
            f"orchestration.mode must be one of {sorted(VALID_MODES)}, got {mode!r}"
        )

    enabled = raw.get("enabled", False)
    if not isinstance(enabled, bool):
        raise OrchestrationConfigError("orchestration.enabled must be a boolean")

    families = raw.get("families", {})
    if not isinstance(families, Mapping):
        raise OrchestrationConfigError("orchestration.families must be a mapping")
    for name in FAMILY_NAMES:
        if name not in families:
            raise OrchestrationConfigError(f"orchestration.families missing required family {name}")
        fam = families[name]
        if not isinstance(fam, Mapping):
            raise OrchestrationConfigError(f"orchestration.families.{name} must be a mapping")
        for req in ("provider_alias", "model_alias", "reasoning_default"):
            if not fam.get(req):
                raise OrchestrationConfigError(
                    f"orchestration.families.{name}.{req} is required"
                )
        rd = fam.get("reasoning_default")
        if rd not in VALID_REASONING:
            raise OrchestrationConfigError(
                f"orchestration.families.{name}.reasoning_default must be one of "
                f"{sorted(VALID_REASONING)}, got {rd!r}"
            )

    reasoning = raw.get("reasoning_capabilities", {})
    if not isinstance(reasoning, Mapping):
        raise OrchestrationConfigError("orchestration.reasoning_capabilities must be a mapping")
    for level in VALID_REASONING:
        if level in reasoning and not isinstance(reasoning[level], bool):
            raise OrchestrationConfigError(
                f"orchestration.reasoning_capabilities.{level} must be a boolean"
            )

    budgets = raw.get("budgets", {})
    if not isinstance(budgets, Mapping):
        raise OrchestrationConfigError("orchestration.budgets must be a mapping")
    for key in ("max_attempts", "max_duration_s", "child_timeout_seconds"):
        if key in budgets:
            val = budgets[key]
            if not isinstance(val, int) or isinstance(val, bool) or val < 1:
                raise OrchestrationConfigError(
                    f"orchestration.budgets.{key} must be a positive integer"
                )
    if "max_cost_usd" in budgets:
        cost = budgets["max_cost_usd"]
        if not isinstance(cost, (int, float)) or isinstance(cost, bool) or cost < 0:
            raise OrchestrationConfigError(
                "orchestration.budgets.max_cost_usd must be a non-negative number"
            )

    telemetry = raw.get("telemetry", {})
    if telemetry is not None and not isinstance(telemetry, Mapping):
        raise OrchestrationConfigError("orchestration.telemetry must be a mapping")


def _parse_family(raw: Mapping[str, Any]) -> FamilyAlias:
    toolsets = tuple(raw.get("toolsets") or ())
    return FamilyAlias(
        provider_alias=str(raw["provider_alias"]),
        model_alias=str(raw["model_alias"]),
        reasoning_default=str(raw["reasoning_default"]),
        toolsets=toolsets,
    )


def load_orchestration_config(
    root_config: Optional[Mapping[str, Any]] = None,
) -> OrchestrationConfig:
    """Load and validate orchestration config from a root config mapping.

    Missing section → defaults (disabled, mode off).
    """
    root_config = root_config or {}
    section = root_config.get("orchestration")
    if section is None:
        merged = dict(DEFAULT_ORCHESTRATION_CONFIG)
    elif not isinstance(section, Mapping):
        raise OrchestrationConfigError("orchestration must be a mapping")
    else:
        merged = _deep_merge(DEFAULT_ORCHESTRATION_CONFIG, section)

    validate_orchestration_dict(merged)

    families = {
        name: _parse_family(merged["families"][name]) for name in FAMILY_NAMES
    }
    budgets_raw = merged["budgets"]
    telemetry_raw = merged.get("telemetry") or {}
    verification_raw = merged.get("verification") or {}
    approval_raw = merged.get("approval") or {}

    return OrchestrationConfig(
        enabled=bool(merged["enabled"]),
        mode=str(merged["mode"]),
        families=families,
        model_aliases={
            str(k): str(v or "") for k, v in (merged.get("model_aliases") or {}).items()
        },
        reasoning_capabilities={
            str(k): bool(v)
            for k, v in (merged.get("reasoning_capabilities") or {}).items()
        },
        budgets=BudgetConfig(
            max_attempts=int(budgets_raw["max_attempts"]),
            max_cost_usd=float(budgets_raw["max_cost_usd"]),
            max_duration_s=int(budgets_raw["max_duration_s"]),
            child_timeout_seconds=int(budgets_raw["child_timeout_seconds"]),
        ),
        verification=VerificationConfig(
            independent_for_sol=bool(verification_raw.get("independent_for_sol", True)),
            schema_retry_once=bool(verification_raw.get("schema_retry_once", True)),
        ),
        telemetry=TelemetryConfig(
            enabled=bool(telemetry_raw.get("enabled", False)),
            retain_days=int(telemetry_raw.get("retain_days", 14)),
            store_raw_prompt=bool(telemetry_raw.get("store_raw_prompt", False)),
        ),
        approval=ApprovalConfig(
            require_for_destructive=bool(
                approval_raw.get("require_for_destructive", True)
            ),
            require_for_financial=bool(approval_raw.get("require_for_financial", True)),
            workers_cannot_self_approve=bool(
                approval_raw.get("workers_cannot_self_approve", True)
            ),
        ),
        schema_version=str(merged.get("schema_version") or "orch.task_spec.v1"),
        policy_version=str(merged.get("policy_version") or "orch.policy.v1"),
        prompt_version=str(merged.get("prompt_version") or "orch.prompt.v1"),
    )


def resolve_family_model(
    cfg: OrchestrationConfig,
    family_name: str,
) -> Tuple[str, str]:
    """Return (provider_alias, model_alias_or_concrete) for a family.

    Concrete model ids come from ``model_aliases`` when set; otherwise the
    alias key is returned for ``resolve_runtime_provider`` inheritance.
    """
    fam = cfg.families[family_name]
    concrete = cfg.model_aliases.get(fam.model_alias, "") or ""
    return fam.provider_alias, concrete or fam.model_alias
