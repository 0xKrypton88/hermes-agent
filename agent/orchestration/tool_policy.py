"""Tool risk metadata + pre-dispatch approval enforcement.

Enforcement runs after middleware finalizes normalized arguments and before
``ToolRegistry.dispatch``. Prompt guidance is advisory; this gate is
authoritative.

Approval identity binds session + turn + tool-call + tool name + canonical
digest of the final normalized action/arguments. Workers cannot self-approve.
"""

from __future__ import annotations

import hashlib
import json
import logging
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, Mapping, Optional, Set

from agent.orchestration.contracts import SideEffectClass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ToolRiskMeta:
    side_effect: SideEffectClass
    risk_level: str = "low"  # low | moderate | high | critical


@dataclass(frozen=True)
class PolicyDecision:
    allowed: bool
    reason_code: str
    requires_approval: bool = False
    action_digest: Optional[str] = None


@dataclass
class ApprovalRecord:
    session_id: str
    turn_id: str
    tool_call_id: str
    tool_name: str
    action_digest: str
    status: str  # approved | denied | expired
    actor: str = "user"


class ApprovalStore:
    """In-memory approval binder for orchestration policy tests / workers."""

    def __init__(self) -> None:
        self._records: Dict[str, ApprovalRecord] = {}

    @staticmethod
    def _key(
        session_id: str,
        turn_id: str,
        tool_call_id: str,
        tool_name: str,
        action_digest: str,
    ) -> str:
        return "|".join(
            [session_id, turn_id, tool_call_id, tool_name, action_digest]
        )

    def approve(
        self,
        *,
        session_id: str,
        turn_id: str,
        tool_call_id: str,
        tool_name: str,
        action_digest: str,
        actor: str = "user",
        trusted: bool = False,
    ) -> None:
        """Record an approval.

        Workers cannot mint approvals under any ``actor`` string. Only the
        trusted host approval UX/hooks may pass ``trusted=True``.
        """
        if actor == "worker":
            raise PermissionError("workers cannot self-approve")
        active = get_active_policy_context()
        if active is not None and active.is_worker and not trusted:
            raise PermissionError("workers cannot mint user approvals")
        if not trusted and actor != "user":
            # Non-user actors require the trusted host path.
            raise PermissionError("untrusted approval actor")
        key = self._key(session_id, turn_id, tool_call_id, tool_name, action_digest)
        self._records[key] = ApprovalRecord(
            session_id=session_id,
            turn_id=turn_id,
            tool_call_id=tool_call_id,
            tool_name=tool_name,
            action_digest=action_digest,
            status="approved",
            actor="user" if trusted else actor,
        )

    def deny(
        self,
        *,
        session_id: str,
        turn_id: str,
        tool_call_id: str,
        tool_name: str,
        action_digest: str,
        actor: str = "user",
    ) -> None:
        key = self._key(session_id, turn_id, tool_call_id, tool_name, action_digest)
        self._records[key] = ApprovalRecord(
            session_id=session_id,
            turn_id=turn_id,
            tool_call_id=tool_call_id,
            tool_name=tool_name,
            action_digest=action_digest,
            status="denied",
            actor=actor,
        )

    def lookup(
        self,
        *,
        session_id: str,
        turn_id: str,
        tool_call_id: str,
        tool_name: str,
        action_digest: str,
    ) -> Optional[ApprovalRecord]:
        key = self._key(session_id, turn_id, tool_call_id, tool_name, action_digest)
        return self._records.get(key)

    def find_denied_for_call(
        self,
        *,
        session_id: str,
        turn_id: str,
        tool_call_id: str,
        tool_name: str,
    ) -> Optional[ApprovalRecord]:
        for rec in self._records.values():
            if (
                rec.session_id == session_id
                and rec.turn_id == turn_id
                and rec.tool_call_id == tool_call_id
                and rec.tool_name == tool_name
                and rec.status == "denied"
            ):
                return rec
        return None


@dataclass
class PolicyContext:
    session_id: str
    turn_id: str
    tool_call_id: str
    is_worker: bool
    allowed_side_effects: FrozenSet[SideEffectClass]
    approval_store: ApprovalStore
    allow_worker_self_approve: bool = False


_active_policy: ContextVar[Optional[PolicyContext]] = ContextVar(
    "orch_active_policy", default=None
)


def set_active_policy_context(ctx: Optional[PolicyContext]) -> Token:
    return _active_policy.set(ctx)


def reset_active_policy_context(token: Token) -> None:
    _active_policy.reset(token)


def get_active_policy_context() -> Optional[PolicyContext]:
    return _active_policy.get()


_DEFAULT_BY_NAME: Dict[str, ToolRiskMeta] = {
    "read_file": ToolRiskMeta(SideEffectClass.READ, "low"),
    "search_files": ToolRiskMeta(SideEffectClass.READ, "low"),
    "web_search": ToolRiskMeta(SideEffectClass.READ, "low"),
    "web_extract": ToolRiskMeta(SideEffectClass.READ, "low"),
    "write_file": ToolRiskMeta(SideEffectClass.WRITE, "moderate"),
    "patch": ToolRiskMeta(SideEffectClass.WRITE, "moderate"),
    "terminal": ToolRiskMeta(SideEffectClass.WRITE, "high"),
    "browser_navigate": ToolRiskMeta(SideEffectClass.EXTERNAL, "moderate"),
    "browser_click": ToolRiskMeta(SideEffectClass.EXTERNAL, "moderate"),
}

_DEFAULT_BY_TOOLSET: Dict[str, ToolRiskMeta] = {
    "file": ToolRiskMeta(SideEffectClass.READ, "low"),
    "web": ToolRiskMeta(SideEffectClass.READ, "low"),
    "terminal": ToolRiskMeta(SideEffectClass.WRITE, "high"),
    "browser": ToolRiskMeta(SideEffectClass.EXTERNAL, "moderate"),
    "mcp": ToolRiskMeta(SideEffectClass.EXTERNAL, "high"),
}


def attach_default_risk_metadata(registry: Any) -> None:
    """Populate registry-owned risk metadata for known tools/toolsets."""
    entries = []
    if hasattr(registry, "_snapshot_entries"):
        entries = registry._snapshot_entries()
    elif hasattr(registry, "_tools"):
        entries = list(registry._tools.values())

    for entry in entries:
        existing = registry.get_risk_metadata(entry.name) if hasattr(registry, "get_risk_metadata") else None
        if existing is not None:
            continue
        meta = _DEFAULT_BY_NAME.get(entry.name)
        if meta is None:
            meta = _DEFAULT_BY_TOOLSET.get(entry.toolset, ToolRiskMeta(SideEffectClass.EXTERNAL, "moderate"))
            # write_file in file toolset should be WRITE
            if entry.name.startswith("write") or entry.name in {"patch"}:
                meta = ToolRiskMeta(SideEffectClass.WRITE, "moderate")
            if entry.name.startswith("read") or entry.name.startswith("search"):
                meta = ToolRiskMeta(SideEffectClass.READ, "low")
        registry.set_risk_metadata(entry.name, meta)


def get_tool_risk_meta(registry: Any, name: str) -> ToolRiskMeta:
    meta = None
    if hasattr(registry, "get_risk_metadata"):
        meta = registry.get_risk_metadata(name)
    if isinstance(meta, ToolRiskMeta):
        return meta
    entry = registry.get_entry(name) if hasattr(registry, "get_entry") else None
    if entry is not None:
        return _DEFAULT_BY_NAME.get(
            name,
            _DEFAULT_BY_TOOLSET.get(
                entry.toolset, ToolRiskMeta(SideEffectClass.EXTERNAL, "moderate")
            ),
        )
    return ToolRiskMeta(SideEffectClass.EXTERNAL, "moderate")


_DESTRUCTIVE_ACTION_MARKERS = (
    "rm -",
    "rm -rf",
    "rmdir",
    "del /",
    "remove-item",
    "drop table",
    "drop database",
    "mkfs",
    "format ",
    "dd if=",
    "wipe",
    "shred ",
)
_FINANCIAL_ACTION_MARKERS = (
    "payment",
    "transfer",
    "place_order",
    "place-order",
    "broker",
    "trade",
    "withdraw",
    "wire ",
)


def normalize_action_risk(
    tool_name: str,
    args: Mapping[str, Any],
    base: ToolRiskMeta,
) -> ToolRiskMeta:
    """Action-aware normalized classification across terminal/browser/API/MCP."""
    blob_parts = [tool_name]
    for key in ("command", "cmd", "script", "code", "url", "path", "action", "method"):
        if key in args and args[key] is not None:
            blob_parts.append(str(args[key]))
    # Include tool name + common arg values for API/MCP tools.
    for key, value in list(args.items())[:12]:
        blob_parts.append(f"{key}={value}")
    blob = " ".join(blob_parts).lower()

    if any(m in blob for m in _FINANCIAL_ACTION_MARKERS) or base.side_effect is SideEffectClass.FINANCIAL:
        if base.side_effect is SideEffectClass.FINANCIAL or any(
            m in blob for m in _FINANCIAL_ACTION_MARKERS
        ):
            return ToolRiskMeta(SideEffectClass.FINANCIAL, "critical")

    if any(m in blob for m in _DESTRUCTIVE_ACTION_MARKERS):
        return ToolRiskMeta(SideEffectClass.DESTRUCTIVE, "high")

    if tool_name.startswith("browser_") or base.side_effect is SideEffectClass.EXTERNAL:
        if base.side_effect is SideEffectClass.EXTERNAL:
            return base

    return base


def canonical_action_digest(tool_name: str, args: Mapping[str, Any]) -> str:
    """Stable digest of final normalized tool name + arguments."""
    payload = {"tool": tool_name, "args": _canonicalize(args)}
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _canonicalize(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _canonicalize(value[k]) for k in sorted(value, key=str)}
    if isinstance(value, (list, tuple)):
        return [_canonicalize(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def enforce_tool_policy(
    registry: Any,
    tool_name: str,
    args: Mapping[str, Any],
    ctx: PolicyContext,
    *,
    tool_call_id: Optional[str] = None,
) -> PolicyDecision:
    """Authoritative pre-dispatch policy check."""
    call_id = tool_call_id or ctx.tool_call_id
    meta = normalize_action_risk(
        tool_name, args, get_tool_risk_meta(registry, tool_name)
    )
    digest = canonical_action_digest(tool_name, args)

    # Denial for this call identity cannot be bypassed (even if digest differs
    # after a deny was recorded for a prior digest on the same call id).
    denied = ctx.approval_store.find_denied_for_call(
        session_id=ctx.session_id,
        turn_id=ctx.turn_id,
        tool_call_id=call_id,
        tool_name=tool_name,
    )
    if denied is not None:
        return PolicyDecision(
            allowed=False,
            reason_code="APPROVAL_DENIED",
            requires_approval=True,
            action_digest=digest,
        )

    if meta.side_effect is SideEffectClass.READ or meta.side_effect is SideEffectClass.NONE:
        if meta.side_effect in ctx.allowed_side_effects or SideEffectClass.READ in ctx.allowed_side_effects:
            return PolicyDecision(
                allowed=True,
                reason_code="AUTONOMOUS_READ",
                action_digest=digest,
            )

    if meta.side_effect is SideEffectClass.WRITE:
        if SideEffectClass.WRITE in ctx.allowed_side_effects:
            return PolicyDecision(
                allowed=True,
                reason_code="WRITE_POLICY_OK",
                action_digest=digest,
            )
        return PolicyDecision(
            allowed=False,
            reason_code="WRITE_NOT_ALLOWED",
            requires_approval=False,
            action_digest=digest,
        )

    if meta.side_effect in (SideEffectClass.DESTRUCTIVE, SideEffectClass.FINANCIAL):
        rec = ctx.approval_store.lookup(
            session_id=ctx.session_id,
            turn_id=ctx.turn_id,
            tool_call_id=call_id,
            tool_name=tool_name,
            action_digest=digest,
        )
        if rec is not None and rec.status == "approved":
            if ctx.is_worker and rec.actor == "worker" and not ctx.allow_worker_self_approve:
                return PolicyDecision(
                    allowed=False,
                    reason_code="WORKER_SELF_APPROVE_FORBIDDEN",
                    requires_approval=True,
                    action_digest=digest,
                )
            return PolicyDecision(
                allowed=True,
                reason_code="APPROVAL_GRANTED",
                action_digest=digest,
            )
        if rec is not None and rec.status == "denied":
            return PolicyDecision(
                allowed=False,
                reason_code="APPROVAL_DENIED",
                requires_approval=True,
                action_digest=digest,
            )
        # Check whether any approval exists for this call with a different digest
        for existing in list(ctx.approval_store._records.values()):
            if (
                existing.session_id == ctx.session_id
                and existing.turn_id == ctx.turn_id
                and existing.tool_call_id == call_id
                and existing.tool_name == tool_name
                and existing.status == "approved"
                and existing.action_digest != digest
            ):
                return PolicyDecision(
                    allowed=False,
                    reason_code="APPROVAL_DIGEST_MISMATCH",
                    requires_approval=True,
                    action_digest=digest,
                )
        return PolicyDecision(
            allowed=False,
            reason_code="APPROVAL_REQUIRED",
            requires_approval=True,
            action_digest=digest,
        )

    # EXTERNAL / other — require membership in allowed set
    if meta.side_effect in ctx.allowed_side_effects:
        return PolicyDecision(
            allowed=True,
            reason_code="EXTERNAL_ALLOWED",
            action_digest=digest,
        )
    return PolicyDecision(
        allowed=False,
        reason_code="SIDE_EFFECT_BLOCKED",
        requires_approval=True,
        action_digest=digest,
    )


def grant_trusted_user_approval(
    store: ApprovalStore,
    *,
    session_id: str,
    turn_id: str,
    tool_call_id: str,
    tool_name: str,
    action_digest: str,
) -> None:
    """Host-only approval grant used by the existing approval UX/hooks."""
    store.approve(
        session_id=session_id,
        turn_id=turn_id,
        tool_call_id=tool_call_id,
        tool_name=tool_name,
        action_digest=action_digest,
        actor="user",
        trusted=True,
    )


def check_active_policy_or_none(
    registry: Any,
    tool_name: str,
    args: Mapping[str, Any],
    *,
    session_id: str = "",
    turn_id: str = "",
    tool_call_id: str = "",
) -> Optional[PolicyDecision]:
    """If an orchestration policy context is active, enforce it; else None."""
    ctx = get_active_policy_context()
    if ctx is None:
        return None
    # Bind call identity from the live dispatch when provided
    effective = PolicyContext(
        session_id=session_id or ctx.session_id,
        turn_id=turn_id or ctx.turn_id,
        tool_call_id=tool_call_id or ctx.tool_call_id,
        is_worker=ctx.is_worker,
        allowed_side_effects=ctx.allowed_side_effects,
        approval_store=ctx.approval_store,
        allow_worker_self_approve=ctx.allow_worker_self_approve,
    )
    return enforce_tool_policy(
        registry,
        tool_name,
        args,
        effective,
        tool_call_id=tool_call_id or ctx.tool_call_id,
    )
