"""Pure deterministic policy for GPT-5.6 Sol adaptive reasoning.

This module deliberately chooses reasoning effort only.  Auto mode has one
immutable route (``openai-codex`` / ``gpt-5.6-sol``); it never participates in
model fallback, fast-tier selection, or delegation routing.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping, Sequence


POLICY_VERSION = "gpt56-adaptive-v1"
AUTO_PROVIDER = "openai-codex"
AUTO_MODEL = "gpt-5.6-sol"
_ALLOWED_EFFORTS = ("low", "medium", "high")
_EFFORT_RANK = {effort: index for index, effort in enumerate(_ALLOWED_EFFORTS)}


@dataclass(frozen=True)
class AdaptiveReasoningDecision:
    provider: str
    model: str
    effort: str
    work_class: str
    reason_code: str
    policy_version: str = POLICY_VERSION


def _valid_effort(value: Any, default: str) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in _EFFORT_RANK else default


def _bounded(effort: str, minimum: str, maximum: str) -> str:
    low_rank = _EFFORT_RANK[minimum]
    high_rank = _EFFORT_RANK[maximum]
    if low_rank > high_rank:
        low_rank, high_rank = _EFFORT_RANK["low"], _EFFORT_RANK["high"]
    rank = min(max(_EFFORT_RANK[effort], low_rank), high_rank)
    return _ALLOWED_EFFORTS[rank]


def _contains(text: str, pattern: str) -> bool:
    return re.search(pattern, text, flags=re.IGNORECASE) is not None


def _classify(prompt: str, default_effort: str) -> tuple[str, str, str]:
    """Return ``(effort, work_class, reason_code)`` from prompt semantics.

    ``cwd``, project names, and prompt length are intentionally absent.  High
    effort requires a semantic risk/uncertainty signal in the actual request.
    """

    text = str(prompt or "")
    if _contains(
        text,
        r"\b(unknown root cause|root cause (?:is )?(?:unknown|unclear)|cause is unclear|"
        r"intermittent.{0,80}(?:diagnos|investigat)|not sure why.{0,80}(?:fail|break)|"
        r"rotorsak(?:en)? (?:är )?(?:okänd|oklar)|orsaken är oklar|"
        r"intermittent.{0,80}(?:felsök|utred))\b",
    ):
        return "high", "unknown_root_cause", "unknown_root_cause"

    mutation = _contains(
        text,
        r"\b(add|alter|change|cutover|deploy|edit|fix|implement|migrat\w*|modif\w*|"
        r"mutat\w*|patch|place|replace|restart|restore|rollout|rotate|update|write|"
        r"fixa|ändra|implementera|bygg|lägg till|skapa|migrera|migrering|modifiera|"
        r"patcha|placera|ersätt|starta om|återställ|rulla ut|uppdatera|skriv)\b",
    )
    if mutation and _contains(
        text,
        r"\b(trad(?:e|ing)|order placement|position sizing|exchange order|"
        r"handel|orderläggning|positionsstorlek|börsorder)\b",
    ):
        return "high", "trading_mutation", "trading_mutation"
    if mutation and _contains(
        text,
        r"\b(live|production|running service|deployed service|produktion(?:stjänst(?:en)?)?|"
        r"körande (?:tjänst|service)|driftsatt (?:tjänst|service))\b",
    ):
        return "high", "live_mutation", "live_mutation"
    if _contains(
        text,
        r"\b(migrat(?:e|es|ed|ing|ion|ions)|schema migration|data migration|"
        r"migrera|migrering(?:en|ar)?|schemamigrering|datamigrering)\b",
    ):
        return "high", "migration", "migration"
    if mutation and _contains(
        text,
        r"\b(auth(?:entication|orization)?|oauth|credential|permission|access control|api key|token refresh|"
        r"autentisering|auktorisering|behörighet(?:er)?|åtkomstkontroll|api-nyckel|tokenförnyelse)\b",
    ):
        return "high", "auth", "auth_mutation"
    if mutation and _contains(
        text,
        r"\b(persist(?:ed|ence|ent)? state|session state|runtime state|database state|state restore)\b",
    ):
        return "high", "state_mutation", "state_mutation"

    if mutation and _contains(
        text,
        r"\b(typo|spelling|wording|punctuation|comment|readme|docs?|rename|formatting|one[- ]line)\b",
    ):
        return "low", "micro", "micro_change"

    if mutation and _contains(
        text,
        r"\b(stavfel|stavning|formulering|skiljetecken|kommentar|readme|"
        r"dokumentation|byt namn|formatter(?:a|ing)|en[- ]?rad)\b",
    ):
        return "low", "micro", "micro_change"

    if _contains(
        text,
        r"\b(implement|build|add|create|refactor|feature|button|endpoint|component|workflow|test|"
        r"implementera|bygg|lägg till|skapa|refaktorera|funktion|knapp|komponent|arbetsflöde|tester?)\b",
    ):
        return "medium", "normal", "normal_feature"

    return default_effort, "ambiguous", "default_medium"


def decide_adaptive_reasoning(
    *,
    prompt: str,
    cwd: str = "",
    history: Sequence[Mapping[str, Any]] | None = None,
    current_floor: str | None = None,
    manual_override: str | None = None,
    config: Mapping[str, Any] | None = None,
) -> AdaptiveReasoningDecision:
    """Return a deterministic, bounded reasoning decision.

    ``cwd`` and ``history`` are accepted so callers can pass complete turn
    context, but neither can independently raise effort.  ``current_floor`` is
    the prior auto decision and enforces follow-up escalation-only behavior.
    An explicit manual override is returned exactly and is never auto-escalated.
    """

    del cwd, history  # Explicit non-signals in policy v1.
    cfg = config if isinstance(config, Mapping) else {}
    minimum = _valid_effort(cfg.get("min_effort"), "low")
    maximum = _valid_effort(cfg.get("max_effort"), "high")
    default_effort = _bounded(
        _valid_effort(cfg.get("default_effort"), "medium"), minimum, maximum
    )

    normalized_manual = str(manual_override or "").strip().lower()
    if normalized_manual in _EFFORT_RANK:
        return AdaptiveReasoningDecision(
            provider=AUTO_PROVIDER,
            model=AUTO_MODEL,
            effort=normalized_manual,
            work_class="manual",
            reason_code="manual_override",
        )

    effort, work_class, reason_code = _classify(prompt, default_effort)
    effort = _bounded(effort, minimum, maximum)

    floor = str(current_floor or "").strip().lower()
    followup_policy = str(cfg.get("followup_policy") or "escalate_only").strip().lower()
    if followup_policy == "escalate_only" and floor in _EFFORT_RANK:
        bounded_floor = _bounded(floor, minimum, maximum)
        if _EFFORT_RANK[bounded_floor] > _EFFORT_RANK[effort]:
            effort = bounded_floor
            reason_code = "followup_floor"

    return AdaptiveReasoningDecision(
        provider=AUTO_PROVIDER,
        model=AUTO_MODEL,
        effort=effort,
        work_class=work_class,
        reason_code=reason_code,
    )
