"""Thin worker executor adapter over ``tools.delegate_tool._build_child_agent``.

No duplicate AIAgent / LLM transport / provider layer. Provider/model intent
flows through ``resolve_runtime_provider``. Worker toolsets are static for
the run. Parent prompt/tool schemas remain untouched.
"""

from __future__ import annotations

import concurrent.futures
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from agent.orchestration.config import OrchestrationConfig, resolve_family_model
from agent.orchestration.contracts import ReasoningEffort, WorkerRunRequest

logger = logging.getLogger(__name__)

# Import seams used by tests (patch targets).
from hermes_cli.runtime_provider import resolve_runtime_provider  # noqa: E402
from tools.delegate_tool import _build_child_agent  # noqa: E402

_DEFAULT_POLL_S = 0.1


class WorkerCancelled(Exception):
    """Worker run cancelled by cancel_check or timeout."""


@dataclass
class WorkerRunResult:
    success: bool
    correlation_id: str
    session_id: Optional[str]
    task_id: Optional[str]
    worker_id: Optional[str]
    child_session_id: Optional[str]
    provider: Optional[str]
    model: Optional[str]
    reasoning: ReasoningEffort
    toolsets: Tuple[str, ...]
    final_response: Optional[str] = None
    usage: Dict[str, Any] = field(default_factory=dict)
    cancelled: bool = False
    timed_out: bool = False
    error_class: Optional[str] = None
    latency_ms: Optional[int] = None


def _resolve_overrides(
    req: WorkerRunRequest,
    cfg: OrchestrationConfig,
) -> Dict[str, Any]:
    """Resolve provider/model overrides via runtime provider + family aliases."""
    provider_alias = req.provider_alias
    model_alias = req.model_alias
    if not provider_alias or not model_alias:
        fam_provider, fam_model = resolve_family_model(cfg, req.family.value)
        provider_alias = provider_alias or fam_provider
        model_alias = model_alias or fam_model

    # Map family model aliases through config (never hard-code concrete names).
    concrete_model = cfg.model_aliases.get(model_alias, "") or ""
    target_model = concrete_model or None

    # ``delegation`` alias means inherit via empty requested provider.
    requested = None if provider_alias in ("", "delegation", "inherit") else provider_alias

    resolved = resolve_runtime_provider(
        requested=requested,
        target_model=target_model,
    )
    return {
        "resolved": resolved,
        "model": target_model or model_alias,
        "provider_alias": provider_alias,
        "model_alias": model_alias,
    }


def _reasoning_config(effort: ReasoningEffort) -> Dict[str, Any]:
    # Map orchestrator efforts onto the existing reasoning_config shape.
    return {"effort": effort.value}


def execute_worker_run(
    req: WorkerRunRequest,
    *,
    parent_agent: Any,
    cfg: OrchestrationConfig,
    cancel_check: Optional[Callable[[], bool]] = None,
    build_child: Optional[Callable[..., Any]] = None,
) -> WorkerRunResult:
    """Build and run an isolated worker via the delegation child path."""
    started = time.monotonic()
    toolsets: List[str] = list(req.toolsets)
    build = build_child or _build_child_agent

    try:
        overrides = _resolve_overrides(req, cfg)
    except Exception as exc:
        logger.debug("orchestration provider resolve failed: %s", exc, exc_info=True)
        return WorkerRunResult(
            success=False,
            correlation_id=req.correlation_id,
            session_id=req.parent_session_id or getattr(parent_agent, "session_id", None),
            task_id=req.task_id,
            worker_id=None,
            child_session_id=None,
            provider=None,
            model=None,
            reasoning=req.reasoning,
            toolsets=tuple(toolsets),
            error_class="provider_resolve_error",
            latency_ms=int((time.monotonic() - started) * 1000),
        )

    resolved = overrides["resolved"]
    model = overrides["model"]

    # Snapshot parent surfaces for immutability assertions (we never write them).
    _ = getattr(parent_agent, "_cached_system_prompt", None)
    _ = list(getattr(parent_agent, "tools", None) or [])
    _ = getattr(parent_agent, "model", None)

    child = build(
        task_index=0,
        goal=req.goal,
        context=req.context,
        toolsets=toolsets,
        model=model,
        max_iterations=req.max_iterations,
        task_count=1,
        parent_agent=parent_agent,
        override_provider=resolved.get("provider"),
        override_base_url=resolved.get("base_url"),
        override_api_key=resolved.get("api_key"),
        override_api_mode=resolved.get("api_mode"),
        role=req.role or "leaf",
    )

    # Apply reasoning intent without mutating parent.
    try:
        child.reasoning_config = _reasoning_config(req.reasoning)
    except Exception:
        pass

    # Stamp correlation on the child for telemetry (session-scoped, not process-global).
    try:
        child._orch_correlation_id = req.correlation_id
        child._orch_task_id = req.task_id
        child._orch_family = req.family.value
        child._orch_parent_turn_id = req.parent_turn_id
    except Exception:
        pass

    worker_id = getattr(child, "_subagent_id", None)
    child_session_id = getattr(child, "session_id", None)
    static_toolsets = tuple(getattr(child, "enabled_toolsets", None) or toolsets)

    if cancel_check and cancel_check():
        return WorkerRunResult(
            success=False,
            correlation_id=req.correlation_id,
            session_id=req.parent_session_id or getattr(parent_agent, "session_id", None),
            task_id=req.task_id,
            worker_id=worker_id,
            child_session_id=child_session_id,
            provider=getattr(child, "provider", None) or resolved.get("provider"),
            model=getattr(child, "model", None) or model,
            reasoning=req.reasoning,
            toolsets=static_toolsets,
            cancelled=True,
            error_class="cancelled",
            latency_ms=int((time.monotonic() - started) * 1000),
        )

    timeout = max(1, int(req.timeout_seconds or cfg.budgets.child_timeout_seconds or 1))
    final_response = None
    usage: Dict[str, Any] = {}
    error_class = None
    cancelled = False
    timed_out = False
    success = False

    def _run():
        return child.run_conversation(
            user_message=req.goal,
            system_message=None,
            conversation_history=None,
            task_id=req.task_id,
        )

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_run)
            deadline = time.monotonic() + timeout
            while True:
                if cancel_check and cancel_check():
                    cancelled = True
                    error_class = "cancelled"
                    try:
                        if hasattr(child, "interrupt"):
                            child.interrupt()
                    except Exception:
                        pass
                    future.cancel()
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    timed_out = True
                    error_class = "timeout"
                    try:
                        if hasattr(child, "interrupt"):
                            child.interrupt()
                    except Exception:
                        pass
                    future.cancel()
                    break
                try:
                    result = future.result(timeout=min(_DEFAULT_POLL_S, remaining))
                    final_response = None
                    if isinstance(result, dict):
                        final_response = result.get("final_response")
                        usage = dict(result.get("usage") or {})
                    else:
                        final_response = str(result)
                    success = True
                    break
                except concurrent.futures.TimeoutError:
                    continue
                except Exception as exc:
                    error_class = type(exc).__name__
                    logger.debug("worker run failed: %s", exc, exc_info=True)
                    break
    except Exception as exc:
        error_class = type(exc).__name__
        logger.debug("worker executor failed: %s", exc, exc_info=True)

    return WorkerRunResult(
        success=success,
        correlation_id=req.correlation_id,
        session_id=req.parent_session_id or getattr(parent_agent, "session_id", None),
        task_id=req.task_id,
        worker_id=worker_id,
        child_session_id=child_session_id,
        provider=getattr(child, "provider", None) or resolved.get("provider"),
        model=getattr(child, "model", None) or model,
        reasoning=req.reasoning,
        toolsets=static_toolsets,
        final_response=final_response,
        usage=usage,
        cancelled=cancelled,
        timed_out=timed_out,
        error_class=error_class,
        latency_ms=int((time.monotonic() - started) * 1000),
    )
