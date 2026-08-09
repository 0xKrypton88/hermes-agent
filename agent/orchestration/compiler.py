"""Compile worker briefs (not a new massive parent system prompt).

Family-specific contracts:
- LUNA: minimal context, strict schema, no speculation
- TERRA: normal context/tools/DoD
- SOL: risk / dependency / prior-failure / evidence contract
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from agent.orchestration.config import OrchestrationConfig
from agent.orchestration.contracts import (
    CompiledTask,
    ModelFamily,
    RoutingDecision,
    TaskSpec,
)
from agent.orchestration.planner import planner_reason, should_include_planner


_DEFAULT_OUTPUT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "summary": {"type": "string"},
        "evidence": {"type": "array", "items": {"type": "string"}},
        "status": {"type": "string", "enum": ["ok", "blocked", "needs_user"]},
    },
    "required": ["summary", "status"],
}


def compile_worker_brief(
    spec: TaskSpec,
    decision: RoutingDecision,
    cfg: OrchestrationConfig,
    *,
    prior_failures: int = 0,
    prior_failure_signatures: Optional[tuple] = None,
) -> CompiledTask:
    """Compile an ephemeral worker brief from TaskSpec + RoutingDecision."""
    family = decision.family
    fam_cfg = cfg.families[family.value]
    toolsets = tuple(fam_cfg.toolsets)
    include_planner = should_include_planner(spec, prior_failures=prior_failures)
    p_reason = planner_reason(spec, prior_failures=prior_failures) if include_planner else None

    sections = [
        f"# Worker Brief ({family.value})",
        "",
        f"## Objective\n{spec.objective}",
        f"## Provenance\n{spec.provenance.value}",
    ]

    if spec.unknowns or spec.assumptions:
        sections.append("## Unknowns / Assumptions")
        if spec.unknowns:
            sections.append("Unknowns: " + "; ".join(spec.unknowns))
        if spec.assumptions:
            sections.append("Assumptions: " + "; ".join(spec.assumptions))

    if spec.constraints or spec.non_goals:
        sections.append("## Constraints / Non-goals")
        if spec.constraints:
            sections.append("Constraints: " + "; ".join(spec.constraints))
        if spec.non_goals:
            sections.append("Non-goals: " + "; ".join(spec.non_goals))

    caps = ", ".join(c.value for c in spec.capabilities) or "read"
    sections.append("## Allowed capabilities / Approval boundary")
    sections.append(f"Allowed capabilities: {caps}")
    sections.append(f"Autonomy boundary: {spec.autonomy_boundary}")
    if decision.requires_approval:
        sections.append(
            "Approval boundary: destructive/financial actions require explicit approval; "
            "workers cannot self-approve or interact with the user."
        )
    else:
        sections.append(
            "Approval boundary: follow policy checks before write/side-effect tools."
        )

    criteria = "; ".join(spec.success_criteria) or "Satisfy the objective with verifiable evidence."
    sections.append("## Success criteria / Evidence / Output schema")
    sections.append(f"Success criteria / Definition of done: {criteria}")
    sections.append(
        "Evidence: cite concrete artifacts (paths, command outcomes, checks). "
        "Do not invent evidence."
    )
    sections.append(
        "Output schema:\n```json\n"
        + json.dumps(_DEFAULT_OUTPUT_SCHEMA, indent=2)
        + "\n```"
    )

    sections.append("## Retry / Escalation stops")
    sections.append(
        "Stop and return when success criteria are met, approval is denied, "
        "a blocker unknown remains, or budgets (attempts/cost/duration) are exhausted. "
        "Do not loop on identical failure signatures."
    )

    # Family-specific contracts
    if family is ModelFamily.LUNA:
        sections.append("## LUNA contract")
        sections.append(
            "Minimal context. Strict schema adherence. No speculation — "
            "if uncertain, return status=needs_user rather than guessing."
        )
    elif family is ModelFamily.TERRA:
        sections.append("## TERRA contract")
        sections.append(
            "Normal context and tools. Pursue the Definition of Done with the "
            f"static toolsets: {', '.join(toolsets)}."
        )
    else:  # SOL
        sections.append("## SOL contract")
        sections.append(
            "High-consequence path. Include relevant dependency and risk analysis. "
            "Honor the prior-failure contract: do not repeat identical failing strategies."
        )
        if prior_failure_signatures:
            sections.append(
                "Prior failure signatures: " + "; ".join(str(s) for s in prior_failure_signatures)
            )
        else:
            sections.append("Prior failure signatures: (none recorded)")
        if decision.requires_independent_verification:
            sections.append(
                "Independent verification required before accepting the result as final."
            )
        sections.append("Evidence contract: every material claim needs an artifact reference.")

    if include_planner:
        sections.append("## Planner")
        sections.append(
            f"Include a short plan before acting (trigger: {p_reason}). "
            "Keep the plan actionable; do not request private chain-of-thought."
        )

    sections.append("## Hard prohibitions")
    sections.append(
        "Do not request private chain-of-thought. Do not echo raw credentials or "
        "secret material. Do not mutate parent session context."
    )

    brief = "\n".join(sections)
    # Defense in depth — never emit secret-looking keys from explicit facts
    for banned in ("api_key", "password", "secret", "private_key", "token"):
        if banned in brief.lower() and banned not in (spec.objective or "").lower():
            # Strip any accidental leakage from explicit_facts serialization — we don't
            # include explicit_facts raw in the brief, so this is a soft guard.
            pass

    return CompiledTask(
        brief=brief,
        family=family,
        reasoning=decision.reasoning,
        toolsets=toolsets,
        output_schema=dict(_DEFAULT_OUTPUT_SCHEMA),
        include_planner=include_planner,
        planner_reason=p_reason,
    )
