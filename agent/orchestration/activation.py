"""Exact-ID activation rules for Adaptive Orchestrator V1.1.

Global ``off`` / ``shadow`` / ``active`` remain compatible. When an
``activation`` section is present, effective mode is resolved from trusted
``TurnOrigin`` against exact-ID rules. Absent or untrusted origins can never
activate.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Mapping, Optional, Sequence, Tuple

from agent.orchestration.config import OrchestrationConfigError, VALID_MODES
from agent.orchestration.origin import TurnOrigin


@dataclass(frozen=True)
class ActivationRule:
    id: str
    mode: str
    platform: str
    workspace_ids: Tuple[str, ...]
    channel_ids: Tuple[str, ...]
    user_ids: Tuple[str, ...]


@dataclass(frozen=True)
class ActivationConfig:
    default_mode: str
    rules: Tuple[ActivationRule, ...]


@dataclass(frozen=True)
class EffectiveMode:
    mode: str
    rule_id: Optional[str]
    legacy_parent_allowed: bool
    reason: str


def _as_id_tuple(value: Any, *, field_name: str) -> Tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise OrchestrationConfigError(
            f"orchestration.activation rule {field_name} must be a list of exact IDs"
        )
    out: List[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise OrchestrationConfigError(
                f"orchestration.activation rule {field_name} entries must be non-empty strings"
            )
        out.append(item.strip())
    # Exact-ID lists must not contain duplicates within a rule.
    if len(out) != len(set(out)):
        raise OrchestrationConfigError(
            f"orchestration.activation rule {field_name} contains duplicate IDs"
        )
    return tuple(out)


def _parse_rule(raw: Mapping[str, Any], *, index: int) -> ActivationRule:
    if not isinstance(raw, Mapping):
        raise OrchestrationConfigError(
            f"orchestration.activation.rules[{index}] must be a mapping"
        )
    rule_id = raw.get("id")
    if not isinstance(rule_id, str) or not rule_id.strip():
        raise OrchestrationConfigError(
            f"orchestration.activation.rules[{index}].id is required"
        )
    mode = raw.get("mode")
    if mode not in VALID_MODES:
        raise OrchestrationConfigError(
            f"orchestration.activation.rules[{index}].mode must be one of "
            f"{sorted(VALID_MODES)}, got {mode!r}"
        )
    platform = raw.get("platform")
    if not isinstance(platform, str) or not platform.strip():
        raise OrchestrationConfigError(
            f"orchestration.activation.rules[{index}].platform is required"
        )
    workspace_ids = _as_id_tuple(raw.get("workspace_ids"), field_name="workspace_ids")
    channel_ids = _as_id_tuple(raw.get("channel_ids"), field_name="channel_ids")
    user_ids = _as_id_tuple(raw.get("user_ids"), field_name="user_ids")
    if not (workspace_ids or channel_ids or user_ids):
        raise OrchestrationConfigError(
            f"orchestration.activation.rules[{index}] must specify at least one "
            "exact ID list (workspace_ids, channel_ids, or user_ids)"
        )
    return ActivationRule(
        id=rule_id.strip(),
        mode=str(mode),
        platform=platform.strip().lower(),
        workspace_ids=workspace_ids,
        channel_ids=channel_ids,
        user_ids=user_ids,
    )


def _rule_match_key(rule: ActivationRule) -> Tuple[str, Tuple[str, ...], Tuple[str, ...], Tuple[str, ...]]:
    return (
        rule.platform,
        tuple(sorted(rule.workspace_ids)),
        tuple(sorted(rule.channel_ids)),
        tuple(sorted(rule.user_ids)),
    )


def validate_activation_dict(raw: Any) -> None:
    """Validate activation section; raise OrchestrationConfigError on contradiction."""
    if raw is None:
        return
    if not isinstance(raw, Mapping):
        raise OrchestrationConfigError("orchestration.activation must be a mapping")
    default_mode = raw.get("default_mode", "shadow")
    if default_mode not in VALID_MODES:
        raise OrchestrationConfigError(
            f"orchestration.activation.default_mode must be one of "
            f"{sorted(VALID_MODES)}, got {default_mode!r}"
        )
    rules_raw = raw.get("rules", [])
    if not isinstance(rules_raw, (list, tuple)):
        raise OrchestrationConfigError("orchestration.activation.rules must be a list")

    parsed: List[ActivationRule] = []
    seen_ids: set[str] = set()
    seen_keys: dict[
        Tuple[str, Tuple[str, ...], Tuple[str, ...], Tuple[str, ...]], str
    ] = {}
    for idx, item in enumerate(rules_raw):
        rule = _parse_rule(item, index=idx)
        if rule.id in seen_ids:
            raise OrchestrationConfigError(
                f"orchestration.activation duplicate rule id {rule.id!r}"
            )
        seen_ids.add(rule.id)
        key = _rule_match_key(rule)
        prior = seen_keys.get(key)
        if prior is not None:
            # Same exact-ID match set — contradiction regardless of mode.
            raise OrchestrationConfigError(
                "orchestration.activation rules contradict: "
                f"{prior!r} and {rule.id!r} share the same exact-ID match set"
            )
        seen_keys[key] = rule.id
        parsed.append(rule)


def parse_activation_config(raw: Any) -> Optional[ActivationConfig]:
    if raw is None:
        return None
    validate_activation_dict(raw)
    assert isinstance(raw, Mapping)
    rules = tuple(
        _parse_rule(item, index=idx)
        for idx, item in enumerate(raw.get("rules") or ())
    )
    return ActivationConfig(
        default_mode=str(raw.get("default_mode", "shadow")),
        rules=rules,
    )


def _ids_match(required: Sequence[str], actual: Optional[str]) -> bool:
    if not required:
        return True
    if actual is None or not str(actual).strip():
        return False
    return str(actual).strip() in set(required)


def rule_matches(rule: ActivationRule, origin: TurnOrigin) -> bool:
    if not origin.trusted:
        return False
    if (origin.platform or "").lower() != rule.platform:
        return False
    if not _ids_match(rule.workspace_ids, origin.workspace_id):
        return False
    if not _ids_match(rule.channel_ids, origin.channel_id):
        return False
    if not _ids_match(rule.user_ids, origin.user_id):
        return False
    # At least one configured ID dimension must have been evaluated against a
    # present origin value (already enforced by empty-list rejection + match).
    return True


def resolve_effective_mode(
    cfg: Any,
    origin: Optional[TurnOrigin],
) -> EffectiveMode:
    """Resolve effective orchestration mode for a turn.

    Rules:
    - disabled / global off → off
    - when ``activation`` is configured: missing/untrusted origin can never
      activate; exact-ID rules may elevate a trusted canary above global shadow
    - when ``activation`` is absent: preserve V1 global off/shadow/active
      compatibility (mode applies without requiring TurnOrigin)
    """
    enabled = bool(getattr(cfg, "enabled", False))
    global_mode = str(getattr(cfg, "mode", "off") or "off")
    activation: Optional[ActivationConfig] = getattr(cfg, "activation", None)

    if not enabled or global_mode == "off":
        return EffectiveMode(
            mode="off",
            rule_id=None,
            legacy_parent_allowed=True,
            reason="disabled_or_off",
        )

    # V1.1 activation plane — exact-ID rules + untrusted never activate.
    if activation is not None:
        if origin is None or not origin.trusted:
            fallback = (
                activation.default_mode
                if activation.default_mode in ("off", "shadow")
                else "shadow"
            )
            return EffectiveMode(
                mode=fallback,
                rule_id=None,
                legacy_parent_allowed=True,
                reason="untrusted_or_missing_origin",
            )

        matches = [rule for rule in activation.rules if rule_matches(rule, origin)]
        if len(matches) > 1:
            # Should be prevented by config validation; fail closed to shadow.
            return EffectiveMode(
                mode="shadow",
                rule_id=None,
                legacy_parent_allowed=True,
                reason="ambiguous_activation_match",
            )
        if len(matches) == 1:
            mode = matches[0].mode
            return EffectiveMode(
                mode=mode,
                rule_id=matches[0].id,
                legacy_parent_allowed=mode != "active",
                reason="activation_rule",
            )
        mode = activation.default_mode
        return EffectiveMode(
            mode=mode,
            rule_id=None,
            legacy_parent_allowed=mode != "active",
            reason="activation_default",
        )

    # No activation section — preserve global mode compatibility (V1).
    return EffectiveMode(
        mode=global_mode,
        rule_id=None,
        legacy_parent_allowed=global_mode != "active",
        reason="global_mode",
    )
