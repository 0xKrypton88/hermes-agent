"""WP6 — registry risk metadata + pre-dispatch approval enforcement."""

from __future__ import annotations

import json

import pytest

from agent.orchestration.contracts import SideEffectClass


def test_registry_owned_risk_metadata_adapter():
    from tools.registry import ToolRegistry
    from agent.orchestration.tool_policy import (
        ToolRiskMeta,
        attach_default_risk_metadata,
        get_tool_risk_meta,
    )

    reg = ToolRegistry()

    def _handler(args, **kw):
        return json.dumps({"ok": True})

    reg.register(
        name="read_file",
        toolset="file",
        schema={"name": "read_file", "description": "read", "parameters": {"type": "object"}},
        handler=_handler,
    )
    reg.register(
        name="write_file",
        toolset="file",
        schema={"name": "write_file", "description": "write", "parameters": {"type": "object"}},
        handler=_handler,
    )
    reg.register(
        name="terminal",
        toolset="terminal",
        schema={"name": "terminal", "description": "shell", "parameters": {"type": "object"}},
        handler=_handler,
    )

    attach_default_risk_metadata(reg)
    assert get_tool_risk_meta(reg, "read_file").side_effect is SideEffectClass.READ
    assert get_tool_risk_meta(reg, "write_file").side_effect is SideEffectClass.WRITE
    assert get_tool_risk_meta(reg, "terminal").side_effect in (
        SideEffectClass.WRITE,
        SideEffectClass.DESTRUCTIVE,
        SideEffectClass.EXTERNAL,
    )
    # Explicit metadata wins
    reg.set_risk_metadata(
        "terminal",
        ToolRiskMeta(side_effect=SideEffectClass.DESTRUCTIVE, risk_level="high"),
    )
    assert get_tool_risk_meta(reg, "terminal").side_effect is SideEffectClass.DESTRUCTIVE


def test_read_autonomous_write_checked_destructive_needs_approval():
    from tools.registry import ToolRegistry
    from agent.orchestration.tool_policy import (
        ApprovalStore,
        PolicyContext,
        ToolRiskMeta,
        enforce_tool_policy,
        attach_default_risk_metadata,
    )
    from agent.orchestration.contracts import SideEffectClass

    reg = ToolRegistry()
    for name, toolset in (
        ("read_file", "file"),
        ("write_file", "file"),
        ("payment_submit", "external"),
    ):
        reg.register(
            name=name,
            toolset=toolset,
            schema={"name": name, "description": name, "parameters": {"type": "object"}},
            handler=lambda args, **kw: json.dumps({"ok": True}),
        )
    attach_default_risk_metadata(reg)
    reg.set_risk_metadata(
        "payment_submit",
        ToolRiskMeta(side_effect=SideEffectClass.FINANCIAL, risk_level="critical"),
    )

    store = ApprovalStore()
    ctx = PolicyContext(
        session_id="sess",
        turn_id="turn",
        tool_call_id="tc1",
        is_worker=True,
        allowed_side_effects=frozenset(
            {SideEffectClass.READ, SideEffectClass.WRITE, SideEffectClass.NONE}
        ),
        approval_store=store,
    )

    read = enforce_tool_policy(
        reg, "read_file", {"path": "a.txt"}, ctx, tool_call_id="tc-read"
    )
    assert read.allowed is True
    assert read.reason_code == "AUTONOMOUS_READ"

    write = enforce_tool_policy(
        reg, "write_file", {"path": "a.txt", "content": "x"}, ctx, tool_call_id="tc-write"
    )
    assert write.allowed is True
    assert write.reason_code == "WRITE_POLICY_OK"

    denied = enforce_tool_policy(
        reg,
        "payment_submit",
        {"amount": 10},
        ctx,
        tool_call_id="tc-pay",
    )
    assert denied.allowed is False
    assert denied.requires_approval is True
    assert denied.reason_code == "APPROVAL_REQUIRED"


def test_changed_action_digest_invalidates_approval_and_denial_not_bypassable():
    from tools.registry import ToolRegistry
    from agent.orchestration.tool_policy import (
        ApprovalStore,
        PolicyContext,
        ToolRiskMeta,
        enforce_tool_policy,
        canonical_action_digest,
    )
    from agent.orchestration.contracts import SideEffectClass

    reg = ToolRegistry()
    reg.register(
        name="rm_tree",
        toolset="terminal",
        schema={"name": "rm_tree", "description": "rm", "parameters": {"type": "object"}},
        handler=lambda args, **kw: json.dumps({"ok": True}),
    )
    reg.set_risk_metadata(
        "rm_tree",
        ToolRiskMeta(side_effect=SideEffectClass.DESTRUCTIVE, risk_level="high"),
    )
    store = ApprovalStore()
    ctx = PolicyContext(
        session_id="sess",
        turn_id="turn",
        tool_call_id="tc1",
        is_worker=True,
        allowed_side_effects=frozenset({SideEffectClass.DESTRUCTIVE}),
        approval_store=store,
    )
    args = {"path": "/tmp/x"}
    digest = canonical_action_digest("rm_tree", args)
    store.approve(
        session_id="sess",
        turn_id="turn",
        tool_call_id="tc1",
        tool_name="rm_tree",
        action_digest=digest,
    )

    ok = enforce_tool_policy(reg, "rm_tree", args, ctx, tool_call_id="tc1")
    assert ok.allowed is True

    # Changed action → fresh approval required
    changed = enforce_tool_policy(
        reg, "rm_tree", {"path": "/tmp/y"}, ctx, tool_call_id="tc1"
    )
    assert changed.allowed is False
    assert changed.reason_code == "APPROVAL_DIGEST_MISMATCH"

    store.deny(
        session_id="sess",
        turn_id="turn",
        tool_call_id="tc2",
        tool_name="rm_tree",
        action_digest=canonical_action_digest("rm_tree", args),
    )
    bypass = enforce_tool_policy(reg, "rm_tree", args, ctx, tool_call_id="tc2")
    assert bypass.allowed is False
    assert bypass.reason_code == "APPROVAL_DENIED"


def test_worker_cannot_self_approve():
    from tools.registry import ToolRegistry
    from agent.orchestration.tool_policy import (
        ApprovalStore,
        PolicyContext,
        ToolRiskMeta,
        enforce_tool_policy,
        canonical_action_digest,
    )
    from agent.orchestration.contracts import SideEffectClass

    reg = ToolRegistry()
    reg.register(
        name="wipe",
        toolset="terminal",
        schema={"name": "wipe", "description": "wipe", "parameters": {"type": "object"}},
        handler=lambda args, **kw: json.dumps({"ok": True}),
    )
    reg.set_risk_metadata(
        "wipe",
        ToolRiskMeta(side_effect=SideEffectClass.DESTRUCTIVE, risk_level="high"),
    )
    store = ApprovalStore()
    ctx = PolicyContext(
        session_id="sess",
        turn_id="turn",
        tool_call_id="tc9",
        is_worker=True,
        allowed_side_effects=frozenset({SideEffectClass.DESTRUCTIVE}),
        approval_store=store,
        allow_worker_self_approve=False,
    )
    args = {"target": "prod"}
    # Worker attempts to mint its own approval — must be rejected
    with pytest.raises(PermissionError):
        store.approve(
            session_id="sess",
            turn_id="turn",
            tool_call_id="tc9",
            tool_name="wipe",
            action_digest=canonical_action_digest("wipe", args),
            actor="worker",
        )
    result = enforce_tool_policy(reg, "wipe", args, ctx, tool_call_id="tc9")
    assert result.allowed is False
    assert result.requires_approval is True


def test_enforcement_runs_before_dispatch_via_handle_function_call_hook():
    """Authoritative gate is after middleware finalizes args, before dispatch."""
    from unittest.mock import patch
    import model_tools
    from tools.registry import registry
    from agent.orchestration.tool_policy import (
        PolicyContext,
        set_active_policy_context,
        reset_active_policy_context,
        ApprovalStore,
    )
    from agent.orchestration.contracts import SideEffectClass

    # Ensure a financial tool exists for this test
    if not registry.get_entry("orch_test_finance"):
        registry.register(
            name="orch_test_finance",
            toolset="testing",
            schema={
                "name": "orch_test_finance",
                "description": "synthetic finance tool",
                "parameters": {"type": "object", "properties": {}},
            },
            handler=lambda args, **kw: json.dumps({"side_effect": "executed"}),
            override=True,
        )
    registry.set_risk_metadata(
        "orch_test_finance",
        __import__("agent.orchestration.tool_policy", fromlist=["ToolRiskMeta"]).ToolRiskMeta(
            side_effect=SideEffectClass.FINANCIAL, risk_level="critical"
        ),
    )

    store = ApprovalStore()
    ctx = PolicyContext(
        session_id="s1",
        turn_id="t1",
        tool_call_id="c1",
        is_worker=True,
        allowed_side_effects=frozenset({SideEffectClass.READ}),
        approval_store=store,
    )
    token = set_active_policy_context(ctx)
    dispatched = {"count": 0}
    original_dispatch = registry.dispatch

    def wrapped_dispatch(name, args, **kw):
        dispatched["count"] += 1
        return original_dispatch(name, args, **kw)

    try:
        with patch.object(registry, "dispatch", side_effect=wrapped_dispatch):
            result = model_tools.handle_function_call(
                "orch_test_finance",
                {"amount": 5},
                session_id="s1",
                turn_id="t1",
                tool_call_id="c1",
                skip_pre_tool_call_hook=True,
                skip_tool_request_middleware=True,
                skip_tool_execution_middleware=True,
            )
        assert dispatched["count"] == 0
        payload = json.loads(result) if isinstance(result, str) else result
        err = payload.get("error") or payload.get("message") or str(payload)
        assert "approval" in err.lower() or "denied" in err.lower() or "blocked" in err.lower()
    finally:
        reset_active_policy_context(token)


def test_handle_function_call_reapproves_execution_middleware_final_args_and_fails_closed():
    """Policy digests final middleware args; evaluation errors fail closed."""
    from unittest.mock import patch
    import model_tools
    from tools.registry import registry
    from agent.orchestration.tool_policy import (
        PolicyContext,
        set_active_policy_context,
        reset_active_policy_context,
        ApprovalStore,
        ToolRiskMeta,
        canonical_action_digest,
    )
    from agent.orchestration.contracts import SideEffectClass

    if not registry.get_entry("orch_mw_final_args"):
        registry.register(
            name="orch_mw_final_args",
            toolset="testing",
            schema={
                "name": "orch_mw_final_args",
                "description": "synthetic",
                "parameters": {"type": "object", "properties": {}},
            },
            handler=lambda args, **kw: json.dumps({"executed": args}),
            override=True,
        )
    registry.set_risk_metadata(
        "orch_mw_final_args",
        ToolRiskMeta(side_effect=SideEffectClass.DESTRUCTIVE, risk_level="high"),
    )

    store = ApprovalStore()
    original_args = {"path": "/tmp/safe"}
    final_args = {"path": "/tmp/dangerous"}
    digest = canonical_action_digest("orch_mw_final_args", final_args)
    # Approval only for original args — middleware will mutate to final_args
    store.approve(
        session_id="s-mw",
        turn_id="t-mw",
        tool_call_id="c-mw",
        tool_name="orch_mw_final_args",
        action_digest=canonical_action_digest("orch_mw_final_args", original_args),
        actor="user",
    )

    ctx = PolicyContext(
        session_id="s-mw",
        turn_id="t-mw",
        tool_call_id="c-mw",
        is_worker=True,
        allowed_side_effects=frozenset({SideEffectClass.READ}),
        approval_store=store,
    )
    token = set_active_policy_context(ctx)
    dispatched = {"count": 0}
    original_dispatch = registry.dispatch

    def wrapped_dispatch(name, args, **kw):
        dispatched["count"] += 1
        return original_dispatch(name, args, **kw)

    def mw_replace(name, args, dispatch, **kw):
        # Execution middleware replaces args immediately before dispatch
        return dispatch(final_args)

    try:
        with patch.object(registry, "dispatch", side_effect=wrapped_dispatch), patch(
            "hermes_cli.middleware.run_tool_execution_middleware",
            side_effect=mw_replace,
        ):
            result = model_tools.handle_function_call(
                "orch_mw_final_args",
                original_args,
                session_id="s-mw",
                turn_id="t-mw",
                tool_call_id="c-mw",
                skip_pre_tool_call_hook=True,
                skip_tool_request_middleware=True,
                skip_tool_execution_middleware=False,
            )
        assert dispatched["count"] == 0
        payload = json.loads(result) if isinstance(result, str) else result
        err = str(payload.get("error") or payload)
        assert "approval" in err.lower() or "blocked" in err.lower() or "digest" in err.lower()

        # Fail closed: policy evaluation exception must block dispatch
        def boom(*a, **kw):
            raise RuntimeError("policy exploded")

        with patch(
            "agent.orchestration.tool_policy.check_active_policy_or_none",
            side_effect=boom,
        ), patch.object(registry, "dispatch", side_effect=wrapped_dispatch):
            result2 = model_tools.handle_function_call(
                "orch_mw_final_args",
                final_args,
                session_id="s-mw",
                turn_id="t-mw",
                tool_call_id="c-mw2",
                skip_pre_tool_call_hook=True,
                skip_tool_request_middleware=True,
                skip_tool_execution_middleware=True,
            )
        assert dispatched["count"] == 0
        payload2 = json.loads(result2) if isinstance(result2, str) else result2
        err2 = str(payload2.get("error") or payload2)
        assert "policy" in err2.lower() or "blocked" in err2.lower()
    finally:
        reset_active_policy_context(token)

    # Fresh approval for final digest would allow (sanity of digest identity)
    assert digest != canonical_action_digest("orch_mw_final_args", original_args)


def test_registry_metadata_classifies_destructive_terminal_and_financial_tools_before_dispatch():
    """Declarative registry metadata must classify risky tools in production defaults."""
    from tools.registry import ToolRegistry
    from agent.orchestration.tool_policy import (
        ToolRiskMeta,
        get_tool_risk_meta,
        attach_default_risk_metadata,
        ApprovalStore,
        PolicyContext,
        enforce_tool_policy,
        normalize_action_risk,
    )
    from agent.orchestration.contracts import SideEffectClass

    reg = ToolRegistry()
    seen_handlers = []

    def terminal_handler(args, **kw):
        seen_handlers.append(("terminal", dict(args)))
        return json.dumps({"ok": True})

    def finance_handler(args, **kw):
        seen_handlers.append(("finance", dict(args)))
        return json.dumps({"ok": True})

    # Declarative metadata at register-time (must not break callers omitting it)
    reg.register(
        name="terminal",
        toolset="terminal",
        schema={"name": "terminal", "description": "shell", "parameters": {"type": "object"}},
        handler=terminal_handler,
        risk_metadata=ToolRiskMeta(
            side_effect=SideEffectClass.WRITE, risk_level="high"
        ),
    )
    reg.register(
        name="broker_place_order",
        toolset="external",
        schema={
            "name": "broker_place_order",
            "description": "place order",
            "parameters": {"type": "object"},
        },
        handler=finance_handler,
        risk_metadata=ToolRiskMeta(
            side_effect=SideEffectClass.FINANCIAL, risk_level="critical"
        ),
    )
    # Callers omitting risk_metadata still work; defaults attach
    reg.register(
        name="browser_navigate",
        toolset="browser",
        schema={
            "name": "browser_navigate",
            "description": "nav",
            "parameters": {"type": "object"},
        },
        handler=lambda args, **kw: json.dumps({"ok": True}),
    )
    attach_default_risk_metadata(reg)

    assert get_tool_risk_meta(reg, "terminal").side_effect is SideEffectClass.WRITE
    assert get_tool_risk_meta(reg, "broker_place_order").side_effect is SideEffectClass.FINANCIAL
    assert get_tool_risk_meta(reg, "browser_navigate").side_effect is SideEffectClass.EXTERNAL

    # Action-aware normalized classification (not terminal-regex-only)
    destructive = normalize_action_risk(
        "terminal",
        {"command": "rm -rf /tmp/proj"},
        get_tool_risk_meta(reg, "terminal"),
    )
    assert destructive.side_effect is SideEffectClass.DESTRUCTIVE

    api_fin = normalize_action_risk(
        "broker_place_order",
        {"symbol": "SYNTH", "qty": 1},
        get_tool_risk_meta(reg, "broker_place_order"),
    )
    assert api_fin.side_effect is SideEffectClass.FINANCIAL

    store = ApprovalStore()
    ctx = PolicyContext(
        session_id="s",
        turn_id="t",
        tool_call_id="c1",
        is_worker=True,
        allowed_side_effects=frozenset({SideEffectClass.READ, SideEffectClass.WRITE}),
        approval_store=store,
    )
    blocked = enforce_tool_policy(
        reg,
        "terminal",
        {"command": "rm -rf /tmp/proj"},
        ctx,
        tool_call_id="c1",
    )
    assert blocked.allowed is False
    assert blocked.requires_approval is True

    fin_blocked = enforce_tool_policy(
        reg,
        "broker_place_order",
        {"symbol": "SYNTH", "qty": 1},
        ctx,
        tool_call_id="c2",
    )
    assert fin_blocked.allowed is False
    assert fin_blocked.requires_approval is True
    assert seen_handlers == []
