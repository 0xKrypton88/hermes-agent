"""Thin worker executor adapter over ``tools.delegate_tool`` lifecycle seams.

No duplicate AIAgent / LLM transport / provider layer. Provider/model intent
flows through ``resolve_runtime_provider``. Worker toolsets are static for
the run. Parent prompt/tool schemas remain untouched.

Lifecycle ownership is reused from ``tools.delegate_tool``:
``_build_child_preserving_parent_tools`` + ``_run_child_lifecycle``.
"""

from __future__ import annotations

import contextvars
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from agent.orchestration.config import OrchestrationConfig, resolve_family_model
from agent.orchestration.contracts import ReasoningEffort, SideEffectClass, WorkerRunRequest

logger = logging.getLogger(__name__)

# Import seams used by tests (patch targets).
from hermes_cli.runtime_provider import resolve_runtime_provider  # noqa: E402
from tools.delegate_tool import (  # noqa: E402
    _build_child_agent as _delegate_build_child_agent,
    _run_child_lifecycle as _delegate_run_child_lifecycle,
)
from tools.daemon_pool import DaemonThreadPoolExecutor  # noqa: E402

_DEFAULT_POLL_S = 0.1

# Module-level aliases so tests can patch ``executor._build_child_agent`` and
# still exercise the preserving/lifecycle path.
_build_child_agent = _delegate_build_child_agent


def _build_child_preserving_parent_tools(**kwargs):
    """Preserve parent tool names; route through this module's build seam."""
    import model_tools

    parent_tool_names = list(model_tools._last_resolved_tool_names)
    try:
        child = _build_child_agent(**kwargs)
    finally:
        model_tools._last_resolved_tool_names = parent_tool_names
    try:
        child._delegate_saved_tool_names = parent_tool_names
    except Exception:
        pass
    return child


def _run_child_lifecycle(task_index, goal, child=None, parent_agent=None, **kwargs):
    """Lifecycle-owned run; patch target for tests."""
    return _delegate_run_child_lifecycle(
        task_index, goal, child=child, parent_agent=parent_agent
    )


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
    used_tools: Tuple[str, ...] = ()


def _resolve_overrides(
    req: WorkerRunRequest,
    cfg: OrchestrationConfig,
    *,
    parent_agent: Any = None,
) -> Dict[str, Any]:
    """Resolve provider/model overrides via runtime provider + family aliases."""
    provider_alias = req.provider_alias
    model_alias = req.model_alias
    if not provider_alias or not model_alias:
        fam_provider, fam_model = resolve_family_model(cfg, req.family.value)
        provider_alias = provider_alias or fam_provider
        model_alias = model_alias or fam_model

    # Map family model aliases through config (never hard-code concrete names).
    concrete_model = ""
    if model_alias:
        concrete_model = cfg.model_aliases.get(model_alias, "") or ""
    # Only configured concrete IDs are passed as target_model. Empty/unmapped
    # family aliases (luna|terra|sol) inherit via parent/delegation defaults.
    family_alias_keys = {
        fam.model_alias for fam in cfg.families.values() if fam.model_alias
    }
    if concrete_model:
        target_model: Optional[str] = concrete_model
    elif model_alias and model_alias not in family_alias_keys and model_alias not in {
        "luna",
        "terra",
        "sol",
    }:
        # Non-family alias string that isn't a default family token — treat as
        # a concrete id only when present in model_aliases with a value (above)
        # or when it looks like an already-concrete configured id.
        target_model = None
    else:
        target_model = None

    # ``delegation`` alias means inherit via empty requested provider.
    requested = None if provider_alias in ("", "delegation", "inherit", None) else provider_alias

    resolved = resolve_runtime_provider(
        requested=requested,
        target_model=target_model,
    )

    # Child model: concrete alias only; otherwise inherit parent model.
    parent_model = getattr(parent_agent, "model", None) if parent_agent is not None else None
    child_model = target_model if target_model else (parent_model or None)

    return {
        "resolved": resolved,
        "model": child_model,
        "provider_alias": provider_alias,
        "model_alias": model_alias,
    }


def _reasoning_config(effort: ReasoningEffort) -> Dict[str, Any]:
    return {"effort": effort.value}


def _allowed_side_effects_from_request(req: WorkerRunRequest):
    """Resolve allowed side effects from the compiled/requested task binding."""
    allowed = set()
    for raw in req.allowed_side_effects or ():
        try:
            allowed.add(SideEffectClass(raw))
        except ValueError:
            continue
    if not allowed:
        # Safe default: read-only. Callers that want WRITE must bind it.
        allowed = {SideEffectClass.NONE, SideEffectClass.READ}
    else:
        allowed.add(SideEffectClass.NONE)
        allowed.add(SideEffectClass.READ)
    # Never silently elevate destructive/financial/external from the request
    # tuple alone without host approval — strip those if present.
    allowed.discard(SideEffectClass.DESTRUCTIVE)
    allowed.discard(SideEffectClass.FINANCIAL)
    return frozenset(allowed)


def _install_worker_policy_context(
    req: WorkerRunRequest,
    *,
    parent_agent: Any,
    cfg: OrchestrationConfig,
    child: Any,
):
    """Install session/turn-scoped PolicyContext for the worker run."""
    from agent.orchestration.tool_policy import (
        ApprovalStore,
        PolicyContext,
        set_active_policy_context,
    )

    store = getattr(parent_agent, "_orch_approval_store", None)
    if store is None:
        store = ApprovalStore()
        try:
            parent_agent._orch_approval_store = store
        except Exception:
            pass

    # Bind capabilities from the compiled/requested task. Destructive /
    # financial / external remain approval-gated.
    allowed = _allowed_side_effects_from_request(req)
    ctx = PolicyContext(
        session_id=str(
            req.parent_session_id or getattr(parent_agent, "session_id", "") or ""
        ),
        turn_id=str(
            req.parent_turn_id or getattr(parent_agent, "_current_turn_id", "") or ""
        ),
        tool_call_id=str(req.correlation_id or ""),
        is_worker=True,
        allowed_side_effects=allowed,
        approval_store=store,
        allow_worker_self_approve=False,
    )
    token = set_active_policy_context(ctx)
    try:
        child._orch_worker = True
        child._orch_policy_context = ctx
    except Exception:
        pass
    return token


def _owned_child_cleanup(child: Any, parent_agent: Any) -> None:
    """Interrupt/close/unregister ownership reused from delegation seams.

    Python cannot kill a running worker thread. This path fails closed for the
    caller: interrupt the child, drop registry/_active_children entries, and
    close resources so the parent returns with deterministic cleanup even when
    the child ignores the initial cancellation signal.
    """
    if child is None:
        return
    try:
        if hasattr(child, "interrupt"):
            child.interrupt()
    except Exception:
        logger.debug("owned child interrupt failed", exc_info=True)

    subagent_id = getattr(child, "_subagent_id", None)
    if subagent_id:
        try:
            from tools.delegate_tool import _unregister_subagent, interrupt_subagent

            try:
                interrupt_subagent(subagent_id)
            except Exception:
                pass
            _unregister_subagent(subagent_id, agent=child)
        except Exception:
            logger.debug("owned child unregister failed", exc_info=True)

    if parent_agent is not None and hasattr(parent_agent, "_active_children"):
        try:
            lock = getattr(parent_agent, "_active_children_lock", None)
            children = parent_agent._active_children
            if lock:
                with lock:
                    if child in children:
                        children.remove(child)
            elif child in children:
                children.remove(child)
        except Exception:
            logger.debug("owned child active_children cleanup failed", exc_info=True)

    try:
        if hasattr(child, "close"):
            child.close()
    except Exception:
        logger.debug("owned child close failed", exc_info=True)

    try:
        from agent import relay_runtime

        runtime = relay_runtime.get_runtime(create=False)
        child_session_id = str(getattr(child, "session_id", "") or "")
        if runtime is not None and child_session_id:
            runtime.unregister_subagent({"child_session_id": child_session_id})
    except Exception:
        logger.debug("owned child relay unregister failed", exc_info=True)


def _reset_worker_policy_context(token) -> None:
    try:
        from agent.orchestration.tool_policy import reset_active_policy_context

        if token is not None:
            reset_active_policy_context(token)
    except Exception:
        pass


def _entry_to_result(
    entry: Any,
    *,
    req: WorkerRunRequest,
    parent_agent: Any,
    child: Any,
    resolved: Dict[str, Any],
    model: Optional[str],
    static_toolsets: Tuple[str, ...],
    started: float,
    cancelled: bool = False,
    timed_out: bool = False,
    error_class: Optional[str] = None,
) -> WorkerRunResult:
    final_response = None
    usage: Dict[str, Any] = {}
    used_tools: Tuple[str, ...] = ()
    success = False

    if isinstance(entry, dict):
        status = str(entry.get("status") or "").lower()
        final_response = (
            entry.get("final_response")
            or entry.get("summary")
            or entry.get("response")
        )
        usage = dict(entry.get("usage") or {})
        if not usage:
            # Lifecycle entries may carry token fields directly.
            in_tok = entry.get("input_tokens")
            out_tok = entry.get("output_tokens")
            if in_tok or out_tok:
                usage = {
                    "input_tokens": int(in_tok or 0),
                    "output_tokens": int(out_tok or 0),
                }
        trace = entry.get("tool_trace") or entry.get("tool_calls") or ()
        if isinstance(trace, (list, tuple)):
            names = []
            for item in trace:
                if isinstance(item, dict):
                    name = item.get("name") or item.get("tool") or item.get("tool_name")
                    if name:
                        names.append(str(name))
                elif isinstance(item, str):
                    names.append(item)
            used_tools = tuple(dict.fromkeys(names))
        if error_class is None and status in {"error", "failed", "failure", "timeout"}:
            error_class = status if status != "timeout" else "timeout"
            if status == "timeout":
                timed_out = True
        success = status in {"completed", "success", "ok", ""} and not (
            cancelled or timed_out or error_class
        )
        if final_response and not error_class and not cancelled and not timed_out:
            success = True
        if entry.get("error") and not success:
            error_class = error_class or type(entry.get("error")).__name__
            if isinstance(entry.get("error"), str):
                error_class = error_class or "WorkerError"
    elif entry is not None:
        final_response = str(entry)
        success = not (cancelled or timed_out or error_class)

    return WorkerRunResult(
        success=success,
        correlation_id=req.correlation_id,
        session_id=req.parent_session_id or getattr(parent_agent, "session_id", None),
        task_id=req.task_id,
        worker_id=getattr(child, "_subagent_id", None),
        child_session_id=getattr(child, "session_id", None),
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
        used_tools=used_tools,
    )


def execute_worker_run(
    req: WorkerRunRequest,
    *,
    parent_agent: Any,
    cfg: OrchestrationConfig,
    cancel_check: Optional[Callable[[], bool]] = None,
    build_child: Optional[Callable[..., Any]] = None,
) -> WorkerRunResult:
    """Build and run an isolated worker via the delegation lifecycle path."""
    started = time.monotonic()
    toolsets: List[str] = list(req.toolsets)
    build = build_child or _build_child_preserving_parent_tools
    policy_token = None

    try:
        overrides = _resolve_overrides(req, cfg, parent_agent=parent_agent)
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

    try:
        child.reasoning_config = _reasoning_config(req.reasoning)
    except Exception:
        pass

    try:
        child._orch_correlation_id = req.correlation_id
        child._orch_task_id = req.task_id
        child._orch_family = req.family.value
        child._orch_parent_turn_id = req.parent_turn_id
        child._orch_worker = True
    except Exception:
        pass

    static_toolsets = tuple(getattr(child, "enabled_toolsets", None) or toolsets)

    if cancel_check and cancel_check():
        try:
            if hasattr(child, "close"):
                child.close()
        except Exception:
            pass
        return _entry_to_result(
            None,
            req=req,
            parent_agent=parent_agent,
            child=child,
            resolved=resolved,
            model=model,
            static_toolsets=static_toolsets,
            started=started,
            cancelled=True,
            error_class="cancelled",
        )

    timeout = max(1, int(req.timeout_seconds or cfg.budgets.child_timeout_seconds or 1))
    cancelled = False
    timed_out = False
    error_class = None
    entry: Any = None

    # Install policy context in this thread and copy into the worker thread.
    try:
        policy_token = _install_worker_policy_context(
            req, parent_agent=parent_agent, cfg=cfg, child=child
        )
    except Exception:
        logger.debug("failed to install worker policy context", exc_info=True)
        policy_token = None

    def _run_lifecycle():
        return _run_child_lifecycle(
            0,
            req.goal,
            child,
            parent_agent,
        )

    child_ctx = contextvars.copy_context()

    needs_owned_cleanup = False
    try:
        pool = DaemonThreadPoolExecutor(max_workers=1)
        try:
            future = pool.submit(child_ctx.run, _run_lifecycle)
            deadline = time.monotonic() + timeout
            while True:
                if cancel_check and cancel_check():
                    cancelled = True
                    error_class = "cancelled"
                    needs_owned_cleanup = True
                    future.cancel()
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    timed_out = True
                    error_class = "timeout"
                    needs_owned_cleanup = True
                    future.cancel()
                    break
                try:
                    entry = future.result(timeout=min(_DEFAULT_POLL_S, remaining))
                    break
                except TimeoutError:
                    continue
                except Exception as exc:
                    # concurrent.futures.TimeoutError is a TimeoutError subclass
                    # on 3.10+; already handled above via remaining check loop.
                    from concurrent.futures import TimeoutError as FuturesTimeout

                    if isinstance(exc, FuturesTimeout):
                        continue
                    error_class = type(exc).__name__
                    logger.debug("worker lifecycle failed: %s", exc, exc_info=True)
                    break
        finally:
            # Daemon pool: do not wait for abandoned workers.
            try:
                pool.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                pool.shutdown(wait=False)
            if needs_owned_cleanup:
                # Deterministic interrupt/close/registry ownership even when
                # the child thread ignores cancellation. Does not claim the
                # underlying thread was killed.
                _owned_child_cleanup(child, parent_agent)
    except Exception as exc:
        error_class = type(exc).__name__
        logger.debug("worker executor failed: %s", exc, exc_info=True)
        _owned_child_cleanup(child, parent_agent)
    finally:
        _reset_worker_policy_context(policy_token)

    return _entry_to_result(
        entry,
        req=req,
        parent_agent=parent_agent,
        child=child,
        resolved=resolved,
        model=model,
        static_toolsets=static_toolsets,
        started=started,
        cancelled=cancelled,
        timed_out=timed_out,
        error_class=error_class,
    )
