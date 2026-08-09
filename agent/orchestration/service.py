"""Thin Adaptive Orchestrator facade for the universal turn boundary.

Hook site: ``agent.conversation_loop.run_conversation`` immediately before
``build_turn_context``. Top-level sessions only; workers hit the recursion
guard and fall through to legacy execution.

Active mode may defer worker execution until after the parent turn prologue
(``pending_worker``) so parent persistence / turn IDs / hooks remain intact.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, replace
from typing import Any, Dict, List, Optional, Tuple

from agent.orchestration.compiler import compile_worker_brief
from agent.orchestration.config import OrchestrationConfig, load_orchestration_config
from agent.orchestration.contracts import (
    CompiledTask,
    ExecutionTrace,
    RoutingDecision,
    RuleId,
    TaskSpec,
    VerificationOutcome,
    WorkerRunRequest,
)
from agent.orchestration.executor import WorkerRunResult, execute_worker_run
from agent.orchestration.intake import merge_intake
from agent.orchestration.router import route_task
from agent.orchestration.telemetry import persist_trace
from agent.orchestration.verifier import AttemptRecord, verify_attempt

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
    pending_worker: bool = False
    correlation_id: Optional[str] = None
    cfg_snapshot: Optional[OrchestrationConfig] = None


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


def _base_trace(
    *,
    correlation_id: str,
    agent: Any,
    task_id: Optional[str],
    mode: str,
    decision: RoutingDecision,
    spec: TaskSpec,
    cfg: OrchestrationConfig,
    rule_ids: List[str],
) -> ExecutionTrace:
    return ExecutionTrace(
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


def _require_approval_result(
    *,
    mode: str,
    spec: TaskSpec,
    decision: RoutingDecision,
    compiled: CompiledTask,
    trace: ExecutionTrace,
    cfg: OrchestrationConfig,
) -> OrchestrationTurnResult:
    trace = replace(
        trace,
        verification_outcome=VerificationOutcome.REQUIRE_APPROVAL.value,
        approval_outcome="required",
        rule_ids=tuple(
            dict.fromkeys(
                list(trace.rule_ids) + [RuleId.R_SIDE_EFFECT_APPROVAL.value]
            )
        ),
    )
    try:
        persist_trace(trace, cfg, session_db=None, record_usage=False)
    except Exception:
        logger.debug("orchestration approval trace persist failed", exc_info=True)

    response = {
        "final_response": (
            "This task requires explicit user approval before an active worker "
            "can run (destructive/financial side effects)."
        ),
        "messages": [],
        "orchestration": {
            "correlation_id": trace.correlation_id,
            "family": decision.family.value,
            "reasoning": decision.reasoning.value,
            "mode": mode,
            "status": "REQUIRE_APPROVAL",
            "worker_id": None,
        },
        "completed": False,
        "status": "REQUIRE_APPROVAL",
    }
    return OrchestrationTurnResult(
        mode=mode,
        acted=False,
        legacy_continue=True,
        task_spec=spec,
        decision=decision,
        compiled=compiled,
        trace=trace,
        worker_result=None,
        response=response,
        guard_reason_codes=(RuleId.R_SIDE_EFFECT_APPROVAL.value,),
        pending_worker=False,
        correlation_id=trace.correlation_id,
        cfg_snapshot=cfg,
    )


def _schema_ok(worker_result: WorkerRunResult) -> bool:
    # V1: treat structured non-error completions as schema-ok; missing response
    # on a claimed success is schema-not-ok.
    if worker_result.timed_out or worker_result.cancelled:
        return False
    if worker_result.error_class:
        return False
    if worker_result.success and not worker_result.final_response:
        return False
    return True


def _failure_signature(worker_result: WorkerRunResult) -> Optional[str]:
    if worker_result.success and _schema_ok(worker_result):
        return None
    parts = [
        worker_result.error_class or "failure",
        "timeout" if worker_result.timed_out else "",
        "cancelled" if worker_result.cancelled else "",
    ]
    return ":".join(p for p in parts if p)


def _run_active_worker_loop(
    *,
    agent: Any,
    spec: TaskSpec,
    decision: RoutingDecision,
    compiled: CompiledTask,
    cfg: OrchestrationConfig,
    correlation_id: str,
    task_id: Optional[str],
    trace: ExecutionTrace,
) -> OrchestrationTurnResult:
    """Execute workers with verifier / escalation / budget integration."""
    from agent.orchestration.tool_policy import (
        ApprovalStore,
        PolicyContext,
        set_active_policy_context,
        reset_active_policy_context,
    )

    family = decision.family
    reasoning = decision.reasoning
    strategy_change: Optional[str] = None
    history: List[AttemptRecord] = []
    transitions: List[str] = []
    last_worker: Optional[WorkerRunResult] = None
    final_verification: Optional[Any] = None
    total_input = 0
    total_output = 0
    total_latency = 0

    store = getattr(agent, "_orch_approval_store", None)
    if store is None:
        store = ApprovalStore()
        try:
            agent._orch_approval_store = store
        except Exception:
            pass

    turn_id = str(getattr(agent, "_current_turn_id", "") or task_id or "")
    policy_token = set_active_policy_context(
        PolicyContext(
            session_id=str(getattr(agent, "session_id", "") or ""),
            turn_id=turn_id,
            tool_call_id=correlation_id,
            is_worker=True,
            allowed_side_effects=frozenset(),  # filled per-enforce defaults in executor
            approval_store=store,
            allow_worker_self_approve=False,
        )
    )
    # Reinstall with READ/WRITE allowed; destructive/financial still gated.
    reset_active_policy_context(policy_token)
    from agent.orchestration.contracts import SideEffectClass

    policy_token = set_active_policy_context(
        PolicyContext(
            session_id=str(getattr(agent, "session_id", "") or ""),
            turn_id=turn_id,
            tool_call_id=correlation_id,
            is_worker=True,
            allowed_side_effects=frozenset(
                {
                    SideEffectClass.NONE,
                    SideEffectClass.READ,
                    SideEffectClass.WRITE,
                }
            ),
            approval_store=store,
            allow_worker_self_approve=False,
        )
    )

    try:
        attempt_no = 0
        while True:
            attempt_no += 1
            brief = compiled.brief
            if strategy_change:
                brief = brief + f"\n\n## Strategy change\n{strategy_change}\n"

            from agent.orchestration.config import resolve_family_model

            prov_alias, model_alias = resolve_family_model(cfg, family.value)
            req = WorkerRunRequest(
                goal=spec.objective,
                context=brief,
                toolsets=compiled.toolsets,
                family=family,
                reasoning=reasoning,
                timeout_seconds=cfg.budgets.child_timeout_seconds,
                correlation_id=correlation_id,
                provider_alias=prov_alias,
                model_alias=model_alias,
                parent_session_id=getattr(agent, "session_id", None),
                parent_turn_id=getattr(agent, "_current_turn_id", None),
                task_id=task_id,
            )

            worker_result = execute_worker_run(req, parent_agent=agent, cfg=cfg)
            last_worker = worker_result
            total_input += int((worker_result.usage or {}).get("input_tokens") or 0)
            total_output += int((worker_result.usage or {}).get("output_tokens") or 0)
            total_latency += int(worker_result.latency_ms or 0)

            current = AttemptRecord(
                family=family,
                reasoning=reasoning,
                success=bool(worker_result.success),
                schema_ok=_schema_ok(worker_result),
                failure_signature=_failure_signature(worker_result),
                prompt_fingerprint=strategy_change or "default",
                cost_usd=float((worker_result.usage or {}).get("estimated_cost_usd") or 0.0),
                duration_s=float((worker_result.latency_ms or 0) / 1000.0),
                requires_approval=bool(decision.requires_approval),
            )
            verification = verify_attempt(
                current,
                history=tuple(history),
                cfg=cfg,
                ask_user_pending=False,
                approval_denied=False,
                require_approval=False,
            )
            final_verification = verification
            transitions.append(
                f"{verification.outcome.value}:{verification.reason_code}"
            )
            history.append(current)

            if verification.outcome is VerificationOutcome.RETURN:
                break
            if verification.outcome in (
                VerificationOutcome.BLOCK,
                VerificationOutcome.ASK_USER,
                VerificationOutcome.REQUIRE_APPROVAL,
            ):
                break
            if verification.outcome is VerificationOutcome.RETRY:
                strategy_change = verification.strategy_change or "retry_strategy"
                continue
            if verification.outcome is VerificationOutcome.ESCALATE:
                if verification.next_family is not None:
                    family = verification.next_family
                if verification.next_reasoning is not None:
                    reasoning = verification.next_reasoning
                strategy_change = verification.strategy_change or "escalate"
                # Recompile brief for new family
                decision = replace(
                    decision,
                    family=family,
                    reasoning=reasoning,
                )
                compiled = compile_worker_brief(
                    spec,
                    decision,
                    cfg,
                    prior_failures=len([h for h in history if not h.success]),
                    prior_failure_signatures=tuple(
                        h.failure_signature for h in history if h.failure_signature
                    ),
                )
                continue
            break
    finally:
        reset_active_policy_context(policy_token)

    assert last_worker is not None
    outcome = (
        final_verification.outcome.value
        if final_verification is not None
        else ("RETURN" if last_worker.success else "BLOCK")
    )
    concrete_model = last_worker.model
    # Never persist bare family aliases as concrete model ids.
    if concrete_model in {"luna", "terra", "sol"}:
        concrete_model = getattr(agent, "model", None) or concrete_model

    final_trace = replace(
        trace,
        attempt=max(1, len(history)),
        worker_id=last_worker.worker_id,
        concrete_provider=last_worker.provider or trace.concrete_provider,
        concrete_model=concrete_model or trace.concrete_model,
        family=family,
        reasoning=reasoning,
        used_tools=tuple(last_worker.used_tools or ()),
        latency_ms=total_latency or last_worker.latency_ms,
        input_tokens=total_input,
        output_tokens=total_output,
        estimated_cost_usd=float(
            (last_worker.usage or {}).get("estimated_cost_usd") or 0.0
        )
        or None,
        verification_outcome=outcome,
        escalation_reason=";".join(transitions) if transitions else None,
        error_class=last_worker.error_class,
        approval_outcome="not_required",
    )
    try:
        persist_trace(
            final_trace,
            cfg,
            session_db=getattr(agent, "_session_db", None),
            record_usage=True,
        )
    except Exception:
        logger.debug("orchestration final trace persist failed", exc_info=True)

    success = bool(last_worker.success) and outcome == VerificationOutcome.RETURN.value
    response = {
        "final_response": last_worker.final_response,
        "messages": [],
        "orchestration": {
            "correlation_id": correlation_id,
            "family": family.value,
            "reasoning": reasoning.value,
            "mode": "active",
            "worker_id": last_worker.worker_id,
            "status": "ok" if success else outcome,
            "transitions": transitions,
        },
        "completed": success,
        "usage": {
            "input_tokens": total_input,
            "output_tokens": total_output,
        },
    }
    return OrchestrationTurnResult(
        mode="active",
        acted=True,
        legacy_continue=False,
        task_spec=spec,
        decision=replace(decision, family=family, reasoning=reasoning),
        compiled=compiled,
        trace=final_trace,
        worker_result=last_worker,
        response=response,
        guard_reason_codes=(),
        pending_worker=False,
        correlation_id=correlation_id,
        cfg_snapshot=cfg,
    )


def complete_active_orchestration(
    plan: OrchestrationTurnResult,
    agent: Any,
    *,
    task_id: Optional[str] = None,
    messages: Optional[List[Dict[str, Any]]] = None,
) -> OrchestrationTurnResult:
    """Execute a deferred active worker after parent turn prologue."""
    if plan is None or not plan.pending_worker:
        return plan
    if plan.task_spec is None or plan.decision is None or plan.compiled is None:
        return plan
    cfg = plan.cfg_snapshot or load_orchestration_config(load_config())
    correlation_id = plan.correlation_id or f"orch-{uuid.uuid4().hex[:12]}"
    trace = plan.trace or _base_trace(
        correlation_id=correlation_id,
        agent=agent,
        task_id=task_id,
        mode="active",
        decision=plan.decision,
        spec=plan.task_spec,
        cfg=cfg,
        rule_ids=list(plan.decision.rule_ids) + [RuleId.R_MODE_ACTIVE.value],
    )
    # Refresh turn id onto the trace after prologue assigned it.
    trace = replace(
        trace,
        task_id=str(task_id or getattr(agent, "_current_turn_id", "") or trace.task_id),
        session_id=str(getattr(agent, "session_id", "") or trace.session_id),
    )
    result = _run_active_worker_loop(
        agent=agent,
        spec=plan.task_spec,
        decision=plan.decision,
        compiled=plan.compiled,
        cfg=cfg,
        correlation_id=correlation_id,
        task_id=task_id,
        trace=trace,
    )
    if messages is not None and isinstance(result.response, dict):
        merged = list(messages)
        # Ensure role alternation: append assistant with worker final response.
        content = result.response.get("final_response") or ""
        if not merged or merged[-1].get("role") != "assistant":
            merged.append({"role": "assistant", "content": content})
        else:
            merged[-1] = {**merged[-1], "content": content}
        result.response = {
            **result.response,
            "messages": merged,
        }
    return result


def maybe_orchestrate_turn(
    agent: Any,
    user_message: Any,
    *,
    conversation_history: Optional[list] = None,
    task_id: Optional[str] = None,
    explicit_facts: Optional[Dict[str, Any]] = None,
    defer_worker: bool = False,
) -> OrchestrationTurnResult:
    """Decide/observe at the top-level turn boundary.

    - ``off``: inert; legacy continues.
    - ``shadow``: compute TaskSpec/decision/trace; no workers; legacy continues.
    - ``active``: may spawn an isolated worker via ``WorkerRunRequest`` adapter.
      When ``defer_worker=True``, returns ``pending_worker`` so the conversation
      loop can run ``build_turn_context`` first.
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

    trace = _base_trace(
        correlation_id=correlation_id,
        agent=agent,
        task_id=task_id,
        mode=mode,
        decision=decision,
        spec=spec,
        cfg=cfg,
        rule_ids=rule_ids,
    )

    if mode == "shadow":
        try:
            persist_trace(
                trace, cfg, session_db=getattr(agent, "_session_db", None), record_usage=False
            )
        except Exception:
            logger.debug("orchestration trace persist failed", exc_info=True)
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
            correlation_id=correlation_id,
            cfg_snapshot=cfg,
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
            correlation_id=correlation_id,
            cfg_snapshot=cfg,
        )
        try:
            agent._last_orchestration_result = result
        except Exception:
            pass
        return result

    # Approval gate BEFORE any active worker launch.
    if decision.requires_approval:
        result = _require_approval_result(
            mode="active",
            spec=spec,
            decision=decision,
            compiled=compiled,
            trace=trace,
            cfg=cfg,
        )
        try:
            agent._last_orchestration_result = result
        except Exception:
            pass
        return result

    if defer_worker:
        # Decision only — conversation loop runs build_turn_context first.
        result = OrchestrationTurnResult(
            mode="active",
            acted=False,
            legacy_continue=False,
            task_spec=spec,
            decision=decision,
            compiled=compiled,
            trace=trace,
            worker_result=None,
            response=None,
            guard_reason_codes=(),
            pending_worker=True,
            correlation_id=correlation_id,
            cfg_snapshot=cfg,
        )
        try:
            agent._last_orchestration_result = result
        except Exception:
            pass
        return result

    result = _run_active_worker_loop(
        agent=agent,
        spec=spec,
        decision=decision,
        compiled=compiled,
        cfg=cfg,
        correlation_id=correlation_id,
        task_id=task_id,
        trace=trace,
    )
    try:
        agent._last_orchestration_result = result
    except Exception:
        pass
    return result
