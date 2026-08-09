"""Local versioned orchestration traces (no outbound telemetry).

Persists redacted ExecutionTrace JSON under ``$HERMES_HOME/orchestration/traces``
and records token/cost via SessionDB ``record_auxiliary_usage`` when available.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from hermes_constants import get_hermes_home

from agent.orchestration.config import OrchestrationConfig
from agent.orchestration.contracts import ExecutionTrace

logger = logging.getLogger(__name__)

_TRACE_DIRNAME = "orchestration/traces"


def traces_dir() -> Path:
    return get_hermes_home() / _TRACE_DIRNAME


def _trace_to_redacted_dict(trace: ExecutionTrace) -> Dict[str, Any]:
    data = asdict(trace)
    # Enums → values
    data["family"] = trace.family.value if hasattr(trace.family, "value") else trace.family
    data["reasoning"] = (
        trace.reasoning.value if hasattr(trace.reasoning, "value") else trace.reasoning
    )
    data["rule_ids"] = list(trace.rule_ids)
    data["allowed_capabilities"] = list(trace.allowed_capabilities)
    data["used_tools"] = list(trace.used_tools)
    # Explicitly never persist these
    data.pop("raw_prompt", None)
    data.pop("user_text", None)
    data.pop("private_cot", None)
    data.pop("api_key", None)
    data["persisted_at"] = time.time()
    return data


def persist_trace(
    trace: ExecutionTrace,
    cfg: OrchestrationConfig,
    *,
    session_db: Any = None,
) -> Optional[Path]:
    """Persist a local redacted trace when telemetry is enabled.

    Returns the path written, or None when feature/telemetry is off.
    """
    if not cfg.enabled or not cfg.telemetry.enabled:
        return None
    if cfg.mode == "off" and not cfg.telemetry.enabled:
        return None

    directory = traces_dir()
    directory.mkdir(parents=True, exist_ok=True)
    payload = _trace_to_redacted_dict(trace)
    if cfg.telemetry.store_raw_prompt:
        # Still refuse raw prompts by default contract — flag is reserved but
        # V1 never stores prompt bodies.
        pass

    filename = f"{trace.correlation_id}-{trace.attempt}.json"
    path = directory / filename
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    if session_db is not None and (trace.input_tokens or trace.output_tokens):
        try:
            session_db.record_auxiliary_usage(
                trace.session_id,
                "orchestration",
                model=trace.concrete_model or trace.family.value,
                billing_provider=trace.concrete_provider,
                input_tokens=trace.input_tokens,
                output_tokens=trace.output_tokens,
                estimated_cost_usd=trace.estimated_cost_usd,
            )
        except Exception:
            logger.debug("orchestration aux usage record failed", exc_info=True)

    return path


def load_trace(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def list_traces(cfg: OrchestrationConfig) -> List[Path]:
    if not cfg.enabled or not cfg.telemetry.enabled:
        return []
    directory = traces_dir()
    if not directory.exists():
        return []
    return sorted(directory.glob("*.json"))


def prune_traces(cfg: OrchestrationConfig, *, now_ts: Optional[float] = None) -> int:
    """Deterministic retention prune by ``telemetry.retain_days``."""
    if not cfg.telemetry.enabled:
        return 0
    now = time.time() if now_ts is None else float(now_ts)
    cutoff = now - (cfg.telemetry.retain_days * 86400)
    removed = 0
    directory = traces_dir()
    if not directory.exists():
        return 0
    for path in directory.glob("*.json"):
        try:
            data = load_trace(path)
            persisted_at = float(data.get("persisted_at") or path.stat().st_mtime)
        except Exception:
            persisted_at = path.stat().st_mtime
        if persisted_at < cutoff:
            path.unlink(missing_ok=True)
            removed += 1
    return removed
