"""Bounded LUNA-class structured classifier for orchestration intake.

Deterministic bilingual (Swedish/English) shortcuts cover obvious risk and
routing signals. Ambiguous turns may use a bounded LUNA-family structured
call; SOL is never invoked merely to classify. Schema/confidence validation
and fail-safe fallbacks live here + ``intake.merge_intake``.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, Mapping, Optional

from agent.orchestration.config import OrchestrationConfig, resolve_family_model

logger = logging.getLogger(__name__)

_CONFIDENCE_SHORTCUT = 0.92
_CONFIDENCE_HEURISTIC = 0.55


def _contains(text: str, pattern: str) -> bool:
    return re.search(pattern, text, flags=re.IGNORECASE) is not None


def _base(
    *,
    complexity: str,
    impact: str,
    side_effects: list[str],
    capabilities: list[str],
    confidence: float,
) -> Dict[str, Any]:
    return {
        "complexity": complexity,
        "impact": impact,
        "side_effects": side_effects,
        "capabilities": capabilities,
        "confidence": confidence,
        "unknowns": [],
        "blocker_unknowns": [],
        "classifier_path": "deterministic_shortcut",
    }


def _deterministic_shortcut(user_text: str) -> Optional[Dict[str, Any]]:
    """High-confidence bilingual shortcuts. Prefer safe overeager risk."""
    text = user_text or ""

    destructive = _contains(
        text,
        r"\b(delete|drop|destroy|wipe|purge|rm\s+-rf|"
        r"radera|ta bort|förstör|töm|rensa)\b",
    )
    financial = _contains(
        text,
        r"\b(payment|transfer|trade|trading|order placement|wire|invoice|"
        r"betalning(?:sorder)?|överföring|handel|orderläggning|faktura)\b",
    )
    high_risk = _contains(
        text,
        r"\b(security|credential|production|prod\b|deploy| Sup|"
        r"säkerhet|produktions?\w*|driftsätt|legitimation|hemlighet|"
        r"api[- ]?nyckel|credential|rotations?\w*)\b",
    )
    troubleshooting = _contains(
        text,
        r"\b(troubleshoot|debug|multi-step|investigate|failing|implement|"
        r"refactor|research|feature|fix|"
        r"felsök|felsökning|utred|undersök|flerstegs|implementera|"
        r"refaktorera|undersökning|funktion|åtgärda|föreslå en fix)\b",
    )
    simple_read = _contains(
        text,
        r"\b(summarize|summary|explain|what is|show|list|read|translate|"
        r"sammanfatta|sammanfattning|förklara|vad är|visa|lista|läs|"
        r"översätt|anteckning(?:en)?)\b",
    )

    if destructive or financial or high_risk:
        side_effects = ["none"]
        if destructive:
            side_effects = ["destructive"]
        if financial:
            side_effects = list(dict.fromkeys(side_effects + ["financial"]))
            if side_effects == ["none", "financial"]:
                side_effects = ["financial"]
        if high_risk and "destructive" not in side_effects and "financial" not in side_effects:
            side_effects = ["write"]
        return _base(
            complexity="high",
            impact="high" if (destructive or financial or high_risk) else "moderate",
            side_effects=[s for s in side_effects if s != "none"] or ["write"],
            capabilities=["read", "write", "execute"],
            confidence=_CONFIDENCE_SHORTCUT,
        )

    if troubleshooting:
        return _base(
            complexity="moderate",
            impact="moderate",
            side_effects=["write"],
            capabilities=["read", "write", "execute"],
            confidence=_CONFIDENCE_SHORTCUT,
        )

    if simple_read and not troubleshooting:
        return _base(
            complexity="low",
            impact="low",
            side_effects=["none"],
            capabilities=["read"],
            confidence=_CONFIDENCE_SHORTCUT,
        )

    return None


def _multilingual_heuristic(user_text: str) -> Dict[str, Any]:
    """Fail-safe bilingual heuristic used when shortcuts/model path are unavailable."""
    text = (user_text or "").lower()
    complexity = "low"
    impact = "low"
    side_effects = ["none"]
    capabilities = ["read"]

    if any(
        k in text
        for k in (
            "implement",
            "refactor",
            "research",
            "multi-step",
            "feature",
            "troubleshoot",
            "debug",
            "felsök",
            "implementera",
            "refaktorera",
            "undersök",
            "utred",
            "funktion",
        )
    ):
        complexity = "moderate"
        impact = "moderate"
        side_effects = ["write"]
        capabilities = ["read", "write", "execute"]

    if any(
        k in text
        for k in (
            "security",
            "production",
            "credential",
            "payment",
            "deploy",
            "säkerhet",
            "produktion",
            "betalning",
            "driftsätt",
            "legitimation",
        )
    ):
        complexity = "high"
        impact = "high"

    if any(k in text for k in ("delete", "drop", "destroy", "wipe", "radera", "förstör", "töm")):
        side_effects = ["destructive"]
        impact = "high"
        complexity = "high" if complexity == "low" else complexity

    if any(
        k in text
        for k in (
            "payment",
            "transfer",
            "trade",
            "order",
            "betalning",
            "överföring",
            "handel",
            "orderläggning",
        )
    ):
        side_effects = list(dict.fromkeys([*(s for s in side_effects if s != "none"), "financial"]))
        impact = "high"
        complexity = "high" if complexity == "low" else complexity

    out = _base(
        complexity=complexity,
        impact=impact,
        side_effects=side_effects,
        capabilities=capabilities,
        confidence=_CONFIDENCE_HEURISTIC,
    )
    out["classifier_path"] = "multilingual_heuristic"
    return out


def _invoke_luna_structured_classifier(
    user_text: str,
    *,
    cfg: OrchestrationConfig,
    model: Optional[str] = None,
) -> Dict[str, Any]:
    """Bounded LUNA-family structured classify. Never calls SOL.

    Uses the auxiliary client when available. On any failure, raises so the
    caller can fall back to the multilingual heuristic.
    """
    provider_alias, model_alias = resolve_family_model(cfg, "LUNA")
    concrete = model or model_alias
    # Hard deny: never classify with SOL family models.
    concrete_l = str(concrete or "").lower()
    if "sol" in concrete_l and "luna" not in concrete_l:
        raise RuntimeError("refusing to classify with SOL-family model")

    from agent.auxiliary_client import auxiliary_chat_completion

    schema_hint = (
        "Return ONLY compact JSON with keys: complexity "
        "(low|moderate|high|critical), impact (low|moderate|high|critical), "
        "side_effects (list of none|read|write|destructive|financial|external), "
        "capabilities (list of read|write|execute|network|browser|delegate), "
        "confidence (0..1), unknowns (list), blocker_unknowns (list). "
        "Swedish and English prompts are equally valid. "
        "Never approve side effects. Prefer higher risk when uncertain."
    )
    messages = [
        {
            "role": "system",
            "content": (
                "You are a bounded task classifier for Hermes Adaptive Orchestrator. "
                + schema_hint
            ),
        },
        {"role": "user", "content": (user_text or "")[:4000]},
    ]
    # Prefer openai-codex / LUNA when configured; auxiliary resolves provider.
    result = auxiliary_chat_completion(
        messages,
        provider=provider_alias if provider_alias not in ("", "delegation", "inherit") else None,
        model=concrete if concrete and concrete not in {"luna", "terra", "sol"} else None,
        max_tokens=400,
        task="orchestration_classify",
    )
    content = ""
    if isinstance(result, Mapping):
        content = str(result.get("content") or result.get("text") or "")
    elif isinstance(result, str):
        content = result
    else:
        content = str(getattr(result, "content", "") or "")

    # Extract JSON object from the response.
    start = content.find("{")
    end = content.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("classifier response missing JSON object")
    payload = json.loads(content[start : end + 1])
    if not isinstance(payload, dict):
        raise ValueError("classifier JSON must be an object")
    payload["classifier_path"] = "luna_structured"
    payload["_classifier_model"] = concrete
    return payload


def classify_for_intake(
    user_text: str,
    *,
    cfg: Optional[OrchestrationConfig] = None,
    allow_model_classifier: bool = False,
) -> Dict[str, Any]:
    """Return structured classifier output for ``merge_intake``.

    Order:
    1. Deterministic bilingual safe shortcuts
    2. Optional bounded LUNA structured call (never SOL)
    3. Multilingual heuristic fail-safe
    """
    shortcut = _deterministic_shortcut(user_text)
    if shortcut is not None:
        return shortcut

    if allow_model_classifier and cfg is not None:
        try:
            raw = _invoke_luna_structured_classifier(user_text, cfg=cfg)
            # Basic shape check before returning — intake validates fully.
            if isinstance(raw, dict) and "complexity" in raw and "confidence" in raw:
                model_used = str(raw.get("_classifier_model") or "").lower()
                if "sol" in model_used and "luna" not in model_used:
                    raise RuntimeError("SOL used for classification")
                return raw
        except Exception:
            logger.debug("luna structured classifier failed; heuristic fallback", exc_info=True)

    return _multilingual_heuristic(user_text)
