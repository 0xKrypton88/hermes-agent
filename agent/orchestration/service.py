"""Thin Adaptive Orchestrator facade for the universal turn boundary.

Hook site: ``agent.conversation_loop.run_conversation`` immediately before
``build_turn_context``. Top-level sessions only; workers hit the recursion
guard and fall through to legacy execution.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from agent.orchestration.compiler import compile_worker_brief
from agent.orchestration.config import OrchestrationConfig, load_orchestration_config
from agent.orchestration.contracts import (
    CompiledTask,
    ExecutionTrace,
    RoutingDecision,
    RuleId,
    TaskSpec,
    WorkerRunRequest,
)
from agent.orchestration.executor import WorkerRunResult, execute_worker_run
from agent.orchestration.intake import merge_intake
from agent.orchestration.router import route_task
from agent.orchestration.telemetry import persist_trace

logger = logging.getLogger(__name__)


def load_config() -> Dict[str, Any]:
    """Load root config (patch seam for tests)."""
    try:
        from hermes_cli.config import load_config as _load

        return _load() or {}
    except Exception:
        return {}


@dataclass
class OrchestrationTurnResult:
    mode: str
    acted: bool
    legacy_continue: bool
    task_spec: Optional[TaskSpec]
    decision: Optional[RoutingDecision]
    compiled: Optional[CompiledTask]
    trace: Optional[ExecutionTrace]
    worker_result: Optional[WorkerRunResult]
    response: Optional[Dict[str, Any]]
    guard_reason_codes: Tuple[str, ...] = ()


def _is_worker_context(agent: Any) -> bool:
    if getattr(agent, "_delegate_depth", 0) > 0:
        return True
    if getattr(agent, "platform", None) == "subagent":
        return True
    if getattr(agent, "_orch_worker", False):
        return True
    return False


def _user_text(user_message: Any) -> str:
    if isinstance(user_message, str):
        return user_message
    if isinstance(user_message, dict):
        content = user_message.get("content")
        if isinstance(content, str):
            return content
    return str(user_message or "")


def maybe_orchestrate_turn(
    agent: Any,
    user_message: Any,
    *,
    conversation_history: Optional[list] = None,
    task_id: Optional[str] = None,
    explicit_facts: Optional[Dict[str, Any]] = None,
) -> OrchestrationTurnResult:
    """Decide/observe at the top-level turn boundary.

    - ``off``: inert; legacy continues.
    - ``shadow``: compute TaskSpec/decision/trace; no workers; legacy continues.
    - ``active``: may spawn an isolated worker via ``WorkerRunRequest`` adapter.
    """
    root = load_config()
    try:
        cfg = load_orchestration_config(root)
    except Exception as exc:
        logger.debug("orchestration config invalid; legacy path: %s", exc)
        return OrchestrationTurnResult(
            mode="off",
            acted=False,
            legacy_continue=True,
            task_spec=None,
            decision=None,
            compiled=None,
            trace=None,
            worker_result=None,
            response=None,
            guard_reason_codes=(RuleId.R_MODE_OFF.value,),
        )

    mode = cfg.mode if cfg.enabled else "off"

    if _is_worker_context(agent):
        result = OrchestrationTurnResult(
            mode=mode,
            acted=False,
            legacy_continue=True,
            task_spec=None,
            decision=None,
            compiled=None,
            trace=None,
            worker_result=None,
            response=None,
            guard_reason_codes=(RuleId.R_WORKER_RECURSION_GUARD.value,),
        )
        try:
            agent._last_orchestration_result = result
        except Exception:
            pass
        return result

    if not cfg.enabled or mode == "off":
        result = OrchestrationTurnResult(
            mode="off",
            acted=False,
            legacy_continue=True,
            task_spec=None,
            decision=None,
            compiled=None,
            trace=None,
            worker_result=None,
            response=None,
            guard_reason_codes=(RuleId.R_MODE_OFF.value,),
        )
        try:
            agent._last_orchestration_result = result
        except Exception:
            pass
        return result

    text = _user_text(user_message)
    intake = merge_intake(text, classifier_raw=None, explicit_facts=explicit_facts or {})
    spec = intake.task_spec
    decision = route_task(spec, cfg)
    compiled = compile_worker_brief(spec, decision, cfg)

    correlation_id = f"orch-{uuid.uuid4().hex[:12]}"
    rule_ids = list(decision.rule_ids)
    if mode == "shadow":
        rule_ids.append(RuleId.R_MODE_SHADOW.value)
    else:
        rule_ids.append(RuleId.R_MODE_ACTIVE.value)

    trace = ExecutionTrace(
        correlation_id=correlation_id,
        session_id=str(getattr(agent, "session_id", "") or ""),
        task_id=str(task_id or getattr(agent, "_current_turn_id", "") or ""),
        mode=mode,
        family=decision.family,
        reasoning=decision.reasoning,
        rule_ids=tuple(dict.fromkeys(rule_ids)),
        schema_version=cfg.schema_version,
        policy_version=cfg.policy_version,
        prompt_version=cfg.prompt_version,
        concrete_provider=decision.concrete_provider,
        concrete_model=decision.concrete_model_alias,
        allowed_capabilities=tuple(c.value for c in spec.capabilities),
    )
    try:
        persist_trace(trace, cfg, session_db=getattr(agent, "_session_db", None))
    except Exception:
        logger.debug("orchestration trace persist failed", exc_info=True)

    if mode == "shadow":
        # Observe only — no workers / side effects replace the parent turn.
        result = OrchestrationTurnResult(
            mode="shadow",
            acted=False,
            legacy_continue=True,
            task_spec=spec,
            decision=decision,
            compiled=compiled,
            trace=trace,
            worker_result=None,
            response=None,
            guard_reason_codes=(),
        )
        try:
            agent._last_orchestration_result = result
        except Exception:
            pass
        return result

    # active — may create an isolated worker (never mutates parent cache).
    if intake.ask_user or decision.blocked:
        result = OrchestrationTurnResult(
            mode="active",
            acted=False,
            legacy_continue=True,
            task_spec=spec,
            decision=decision,
            compiled=compiled,
            trace=trace,
            worker_result=None,
            response=None,
            guard_reason_codes=(
                (RuleId.R_BLOCKER_UNKNOWN.value,)
                if intake.ask_user
                else (RuleId.R_CAPABILITY_MISMATCH.value,)
            ),
        )
        try:
            agent._last_orchestration_result = result
        except Exception:
            pass
        return result

    req = WorkerRunRequest(
        goal=spec.objective,
        context=compiled.brief,
        toolsets=compiled.toolsets,
        family=decision.family,
        reasoning=decision.reasoning,
        timeout_seconds=cfg.budgets.child_timeout_seconds,
        correlation_id=correlation_id,
        provider_alias=decision.concrete_provider,
        model_alias=decision.concrete_model_alias,
        parent_session_id=getattr(agent, "session_id", None),
        parent_turn_id=getattr(agent, "_current_turn_id", None),
        task_id=task_id,
    )
    worker_result = execute_worker_run(req, parent_agent=agent, cfg=cfg)
    response = {
        "final_response": worker_result.final_response,
        "messages": [],
        "orchestration": {
            "correlation_id": correlation_id,
            "family": decision.family.value,
            "reasoning": decision.reasoning.value,
            "mode": "active",
            "worker_id": worker_result.worker_id,
        },
        "completed": bool(worker_result.success),
    }
    result = OrchestrationTurnResult(
        mode="active",
        acted=True,
        legacy_continue=False,
        task_spec=spec,
        decision=decision,
        compiled=compiled,
        trace=trace,
        worker_result=worker_result,
        response=response,
        guard_reason_codes=(),
    )
    try:
        agent._last_orchestration_result = result
    except Exception:
        pass
    return result
