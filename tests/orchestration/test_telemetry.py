"""WP8 — local versioned orchestration traces + SessionDB aux usage."""

from __future__ import annotations

import json

from agent.orchestration.config import load_orchestration_config
from agent.orchestration.contracts import (
    ExecutionTrace,
    ModelFamily,
    ReasoningEffort,
)


def test_feature_off_creates_no_orchestration_activity(tmp_path, monkeypatch):
    from hermes_constants import get_hermes_home
    from agent.orchestration.telemetry import persist_trace, list_traces

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    (tmp_path / ".hermes").mkdir()

    cfg = load_orchestration_config({})  # enabled False, telemetry False
    trace = ExecutionTrace(
        correlation_id="c1",
        session_id="s1",
        task_id="t1",
        mode="off",
        family=ModelFamily.LUNA,
        reasoning=ReasoningEffort.LOW,
        rule_ids=("R_MODE_OFF",),
        schema_version="orch.task_spec.v1",
        policy_version="orch.policy.v1",
        prompt_version="orch.prompt.v1",
    )
    path = persist_trace(trace, cfg)
    assert path is None
    assert list_traces(cfg) == []


def test_trace_lifecycle_persists_redacted_local_fields(tmp_path, monkeypatch):
    from agent.orchestration.telemetry import persist_trace, load_trace, prune_traces
    from hermes_state import SessionDB

    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))

    cfg = load_orchestration_config(
        {
            "orchestration": {
                "enabled": True,
                "mode": "shadow",
                "telemetry": {"enabled": True, "retain_days": 14, "store_raw_prompt": False},
            }
        }
    )
    db = SessionDB(home / "state.db")
    db.create_session("s1", source="test")

    trace = ExecutionTrace(
        correlation_id="corr-9",
        session_id="s1",
        task_id="task-9",
        mode="shadow",
        family=ModelFamily.TERRA,
        reasoning=ReasoningEffort.MEDIUM,
        rule_ids=("R_NORMAL_MULTI_STEP",),
        schema_version="orch.task_spec.v1",
        policy_version="orch.policy.v1",
        prompt_version="orch.prompt.v1",
        attempt=1,
        worker_id="sa-1",
        concrete_provider="openrouter",
        concrete_model="alias:terra",
        allowed_capabilities=("read", "write"),
        used_tools=("read_file",),
        approval_outcome="not_required",
        latency_ms=12,
        input_tokens=3,
        output_tokens=5,
        estimated_cost_usd=0.001,
        verification_outcome="RETURN",
        escalation_reason=None,
        error_class=None,
        feedback="helpful",
    )
    path = persist_trace(trace, cfg, session_db=db)
    assert path is not None
    assert path.exists()

    loaded = load_trace(path)
    assert loaded["correlation_id"] == "corr-9"
    assert loaded["family"] == "TERRA"
    assert loaded["concrete_provider"] == "openrouter"
    assert loaded["rule_ids"] == ["R_NORMAL_MULTI_STEP"]
    assert "raw_prompt" not in loaded
    assert "api_key" not in json.dumps(loaded)
    # No duplicated full user text field
    assert "user_text" not in loaded
    assert "private_cot" not in loaded

    # Aux usage recorded under orchestration task
    with db._lock:
        rows = db._conn.execute(
            "SELECT task, input_tokens, output_tokens FROM session_model_usage WHERE session_id=?",
            ("s1",),
        ).fetchall()
    assert any(r[0] == "orchestration" for r in rows)

    # Deterministic retention
    removed = prune_traces(cfg, now_ts=10**12)  # far future → prune all
    assert removed >= 1


def test_persist_trace_writes_only_under_hermes_home(tmp_path, monkeypatch):
    """Local persistence only — traces land under HERMES_HOME, never outbound."""
    from agent.orchestration.telemetry import persist_trace, traces_dir

    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    cfg = load_orchestration_config(
        {"orchestration": {"telemetry": {"enabled": True}, "enabled": True, "mode": "shadow"}}
    )
    trace = ExecutionTrace(
        correlation_id="c-local",
        session_id="s",
        task_id="t",
        mode="shadow",
        family=ModelFamily.LUNA,
        reasoning=ReasoningEffort.LOW,
        rule_ids=("R_MODE_SHADOW",),
        schema_version="orch.task_spec.v1",
        policy_version="orch.policy.v1",
        prompt_version="orch.prompt.v1",
    )
    path = persist_trace(trace, cfg)
    assert path is not None
    assert path.is_relative_to(traces_dir())
    assert path.is_relative_to(home)
    assert not hasattr(
        __import__("agent.orchestration.telemetry", fromlist=["*"]),
        "emit_outbound",
    )
