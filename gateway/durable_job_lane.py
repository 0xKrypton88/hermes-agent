"""Lifecycle-owned Gateway seam for Durable Job Lane (ENG-36 Package 2).

Reads ``durable_jobs`` from active config and constructs the lane only when
explicit validated gates pass **and** runtime transport capability is bound
(``runtime_ready``). Default remains enabled=false / dispatch off.
No implicit network client and no built-in credentials.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

from agent.durable_jobs.config import (
    DurableJobsConfig,
    DurableJobsConfigError,
    load_durable_jobs_config,
)
from agent.durable_jobs.coordinator import InboundActionResult
from agent.durable_jobs.cursor_cloud import adapter_from_config as cursor_adapter_from_config
from agent.durable_jobs.lane import DurableLaneService, LaneClosedError
from agent.durable_jobs.preflight import DurableJobsPreflight, preflight_durable_jobs
from agent.durable_jobs.redaction import redact_secret_text
from agent.durable_jobs.slack_bridge import adapter_from_config as slack_adapter_from_config

logger = logging.getLogger(__name__)

DURABLE_SLACK_ACTION_IDS = (
    "hermes_durable_go",
    "hermes_durable_hold",
    "hermes_durable_pause",
    "hermes_durable_cancel",
)
_ACTION_TO_DECISION = {
    "hermes_durable_go": "go",
    "hermes_durable_hold": "hold",
    "hermes_durable_pause": "pause",
    "hermes_durable_cancel": "cancel",
}

_LOCK = threading.Lock()
_UNOWNED = 0
# Keys are id(owner). ``_LANES`` stays handle-valued for existing readers.
# ``_LANE_OWNERS`` is the reverse map so handle/no-arg detach can CAS-clear
# the correct runner field. ``_RETIRING`` keeps unpublished retirement
# state visible until leases drain. A later holder must fail closed
# without re-entering the leader's in-flight ``shutdown()``.
_LANES: dict[int, "DurableJobLaneHandle"] = {}
_LANE_OWNERS: dict[int, Any] = {}
_RETIRING: dict[int, "_RetirementState"] = {}
_OWNER_OPLOCKS: dict[int, threading.Lock] = {}


@dataclass
class _RetirementState:
    handle: "DurableJobLaneHandle"
    owner: Any
    leader_ident: int
    done: threading.Event = field(default_factory=threading.Event)

_IDENTITY_PAYLOAD_KEYS = (
    "workspace_id",
    "channel_id",
    "root_thread_ts",
    "actor_id",
    "decision_type",
)


class DurableJobLaneAlreadyAttached(RuntimeError):
    """A process may own at most one unowned constructed durable-job lane."""


class _SilentAck:
    def ack(self, *, inbound_id: str, job_id: str) -> str:
        return "acked"


@dataclass
class DurableJobLaneHandle:
    config: DurableJobsConfig
    lane: DurableLaneService
    cursor_adapter: Any
    slack_adapter: Any
    preflight: DurableJobsPreflight

    def shutdown(self) -> None:
        closer = getattr(self.lane, "close", None)
        if callable(closer):
            try:
                closer()
            except LaneClosedError:
                # close() already dropped the store. Re-raise so a
                # lease-holder ACK/adapter cannot complete after return.
                self.lane._store = None
                raise
            except Exception:
                logger.debug("durable job lane close failed", exc_info=True)
            else:
                return
        store = getattr(self.lane, "_store", None)
        if store is not None and hasattr(store, "close"):
            try:
                store.close()
            except Exception:
                logger.debug("durable job store close failed", exc_info=True)
        self.lane._store = None

    def __repr__(self) -> str:
        mode = "dispatch" if self.config.dispatch_allowed else "storage_only"
        return redact_secret_text(
            "DurableJobLaneHandle("
            f"mode={mode!r}, "
            f"dispatch_allowed={self.config.dispatch_allowed!r}, "
            f"enabled={self.config.enabled!r}, "
            f"cursor_adapter={type(self.cursor_adapter).__name__}, "
            f"slack_adapter={type(self.slack_adapter).__name__})"
        )


def _owner_key(owner: Any) -> int:
    if owner is None:
        return _UNOWNED
    return id(owner)


def _owner_oplock(key: int) -> threading.Lock:
    """Per-owner lock. Never acquire this while holding ``_LOCK``."""
    with _LOCK:
        lock = _OWNER_OPLOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _OWNER_OPLOCKS[key] = lock
        return lock


def _bind_handle_owner(handle: "DurableJobLaneHandle", owner: Any) -> None:
    """Remember the owner so handle detach can CAS-clear the runner field."""
    handle._owner = owner


def _owner_from_handle(handle: "DurableJobLaneHandle") -> Any:
    return getattr(handle, "_owner", None)


def _publish_runner_field(
    owner: Any, handle: Optional["DurableJobLaneHandle"]
) -> None:
    if owner is None:
        return
    if handle is not None:
        _bind_handle_owner(handle, owner)
    try:
        owner._durable_job_lane = handle
    except Exception:
        pass


def _bind_idle_cleanup(key: int, handle: "DurableJobLaneHandle") -> None:
    """Drop ``_RETIRING`` when the closed lane's last lease releases.

    Bound on the lane instance so ``DurableLaneService`` does not import
    this module. The hook must run outside ``_lifecycle``.
    """
    lane = getattr(handle, "lane", None)
    if lane is None:
        return

    def _after_idle_closed() -> None:
        _clear_retiring_if_idle(key, handle)

    lane._after_idle_closed = _after_idle_closed


def _unbind_idle_cleanup(handle: "DurableJobLaneHandle") -> None:
    lane = getattr(handle, "lane", None)
    if lane is None:
        return
    if "_after_idle_closed" in getattr(lane, "__dict__", {}):
        delattr(lane, "_after_idle_closed")


def _cas_clear_runner_field(
    owner: Any, expected: Optional["DurableJobLaneHandle"]
) -> None:
    if owner is None or expected is None:
        return
    try:
        if getattr(owner, "_durable_job_lane", None) is expected:
            owner._durable_job_lane = None
    except Exception:
        pass


def _cas_unpublish(
    *,
    key: int,
    owner: Any,
    expected: Optional["DurableJobLaneHandle"] = None,
) -> tuple[Optional["DurableJobLaneHandle"], Optional["_RetirementState"], bool]:
    """Owner-aware CAS unpublish. Caller holds the per-owner oplock.

    Removes the registry entry only if it is the expected handle (or the
    current handle when *expected* is None). Clears the owner field only
    if it still references that same handle. Returns
    ``(handle, state, is_leader)``. ``is_leader`` is True only for the
    thread that unpublished from ``_LANES``. A later caller seeing
    in-flight retirement is a joiner and must not re-enter ``shutdown()``.
    Never shuts down; caller must release locks first.
    """
    with _LOCK:
        current = _LANES.get(key)
        if expected is not None and current is not None and current is not expected:
            retiring = _RETIRING.get(key)
            if retiring is not None and retiring.handle is expected:
                return retiring.handle, retiring, False
            return None, None, False
        if current is not None and (expected is None or current is expected):
            _LANES.pop(key, None)
            mapped_owner = _LANE_OWNERS.pop(key, owner)
            clear_owner = owner if owner is not None else mapped_owner
            if clear_owner is None:
                clear_owner = _owner_from_handle(current)
            state = _RetirementState(
                handle=current,
                owner=clear_owner,
                leader_ident=threading.get_ident(),
            )
            _RETIRING[key] = state
            _cas_clear_runner_field(clear_owner, current)
            _bind_idle_cleanup(key, current)
            return current, state, True
        retiring = _RETIRING.get(key)
        if retiring is not None and (
            expected is None or retiring.handle is expected
        ):
            clear_owner = owner if owner is not None else _LANE_OWNERS.get(key)
            if clear_owner is None:
                clear_owner = retiring.owner or _owner_from_handle(retiring.handle)
            _cas_clear_runner_field(clear_owner, retiring.handle)
            return retiring.handle, retiring, False
        return None, None, False


def _clear_retiring_if_idle(key: int, handle: "DurableJobLaneHandle") -> None:
    lane = getattr(handle, "lane", None)
    idle = True
    if lane is not None:
        lifecycle = getattr(lane, "_lifecycle", None)
        closed = bool(getattr(lane, "_closed", False))
        active = int(getattr(lane, "_active_leases", 0) or 0)
        if lifecycle is not None:
            with lifecycle:
                closed = bool(getattr(lane, "_closed", False))
                active = int(getattr(lane, "_active_leases", 0) or 0)
        idle = closed and active == 0
    with _LOCK:
        current = _RETIRING.get(key)
        if idle and current is not None and current.handle is handle:
            del _RETIRING[key]
            _unbind_idle_cleanup(handle)


def _thread_holds_mutation_lease(handle: "DurableJobLaneHandle") -> bool:
    lane = getattr(handle, "lane", None)
    if lane is None:
        return False
    lifecycle = getattr(lane, "_lifecycle", None)
    leases = getattr(lane, "_leases_by_thread", None)
    ident = threading.get_ident()
    if lifecycle is not None:
        with lifecycle:
            held = getattr(lane, "_leases_by_thread", {}).get(ident, 0)
            return int(held or 0) > 0
    if not isinstance(leases, dict):
        return False
    return int(leases.get(ident, 0) or 0) > 0


def _shutdown_retired(key: int, handle: "DurableJobLaneHandle") -> None:
    """Close *handle* outside all registry locks, then drop idle retirement."""
    try:
        handle.shutdown()
    except LaneClosedError:
        raise
    except Exception:
        logger.debug("durable job lane retire failed", exc_info=True)
    finally:
        _clear_retiring_if_idle(key, handle)


def _drive_or_join_retirement(
    key: int,
    handle: "DurableJobLaneHandle",
    state: Optional["_RetirementState"],
    is_leader: bool,
) -> None:
    """Leader drives shutdown; holder joiners fail closed without re-entry.

    A later lease holder must not call the in-flight ``shutdown()`` — that
    circular-waits the leader. Non-holder cleanup waits for the leader or
    performs idempotent close of a leftover handle. Never holds ``_LOCK``
    or the per-owner oplock across shutdown/I/O.
    """
    if is_leader:
        try:
            _shutdown_retired(key, handle)
        finally:
            if state is not None:
                state.done.set()
        return
    if _thread_holds_mutation_lease(handle):
        raise LaneClosedError(
            "durable job lane is retiring; active holder must fail closed"
        )
    if state is not None and not state.done.is_set():
        state.done.wait(timeout=30.0)
        return
    lane = getattr(handle, "lane", None)
    if lane is not None and bool(getattr(lane, "_closed", False)):
        return
    try:
        handle.shutdown()
    except LaneClosedError:
        raise
    except Exception:
        logger.debug("durable job lane retire failed", exc_info=True)


def _retire_owner_lane(owner: Any) -> None:
    """Detach+shutdown the previously attached lane for this owner, if any.

    Uses the shared CAS-unpublish primitive. The first unpublisher is the
    retirement leader and drives ``shutdown()``. A later lease holder
    fails closed without re-entering that in-flight close. Shutdown runs
    outside the per-owner lock.
    """
    key = _owner_key(owner)
    oplock = _owner_oplock(key)
    with oplock:
        handle, state, is_leader = _cas_unpublish(
            key=key, owner=owner, expected=None
        )
    if handle is None:
        return
    _drive_or_join_retirement(key, handle, state, is_leader)


def get_active_durable_job_lane() -> Optional[DurableJobLaneHandle]:
    with _LOCK:
        handles = list(_LANES.values())
    if len(handles) == 1:
        return handles[0]
    return None


def durable_job_lane_status() -> dict[str, Any]:
    handle = get_active_durable_job_lane()
    status = {
        "attached": handle is not None,
        "enabled": bool(handle.config.enabled) if handle is not None else False,
        "mode": (
            "dispatch" if handle is not None and handle.config.dispatch_allowed
            else "storage_only" if handle is not None
            else None
        ),
        "dispatch_enabled": (
            bool(handle.config.dispatch_enabled) if handle is not None else False
        ),
        "dispatch_allowed": (
            bool(handle.config.dispatch_allowed) if handle is not None else False
        ),
        "cursor_adapter": (
            type(handle.cursor_adapter).__name__ if handle is not None else None
        ),
        "slack_adapter": (
            type(handle.slack_adapter).__name__ if handle is not None else None
        ),
        "backend": handle.config.resolved_backend if handle is not None else None,
    }
    return json.loads(redact_secret_text(json.dumps(status)))


def _load_raw_config(raw_config: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    if raw_config is not None:
        return raw_config
    try:
        from hermes_cli.config import load_config

        loaded = load_config()
        if isinstance(loaded, Mapping):
            return loaded
    except Exception:
        logger.debug("durable_jobs active config load failed", exc_info=True)
    return {}


def attach_durable_job_lane(
    *,
    raw_config: Mapping[str, Any] | None = None,
    cursor_transport: Any = None,
    slack_transport: Any = None,
    owner: Any = None,
    writer_authority_check: Any = None,
) -> Optional[DurableJobLaneHandle]:
    """Construct the lane only when runtime capability is bound. Fail closed."""
    try:
        raw = _load_raw_config(raw_config)
        report = preflight_durable_jobs(
            raw,
            cursor_transport=cursor_transport,
            slack_transport=slack_transport,
        )
        cfg = load_durable_jobs_config(raw) if report.constructible else None
    except DurableJobsConfigError:
        _retire_owner_lane(owner)
        return None
    except LaneClosedError:
        raise
    except Exception:
        logger.debug("durable job lane preflight failed", exc_info=True)
        _retire_owner_lane(owner)
        return None

    if report is None or not report.constructible or cfg is None:
        _retire_owner_lane(owner)
        return None
    postgres_storage_only = (
        cfg.resolved_backend == "postgresql" and not cfg.dispatch_enabled
    )
    if not report.runtime_ready and not postgres_storage_only:
        logger.debug(
            "durable job lane refusing attach; runtime capability unbound (%s)",
            report.reasons,
        )
        _retire_owner_lane(owner)
        return None

    if writer_authority_check is None:
        logger.debug("durable job lane refusing attach; writer authority unbound")
        _retire_owner_lane(owner)
        return None

    key = _owner_key(owner)
    oplock = _owner_oplock(key)
    with oplock:
        with _LOCK:
            if key in _LANES:
                raise DurableJobLaneAlreadyAttached(
                    "durable job lane is already attached for this owner"
                )
        # Fail before adapters or schema-owning stores are constructed.
        writer_authority_check()
        try:
            cursor_adapter = cursor_adapter_from_config(
                cfg, transport=cursor_transport
            )
            slack_adapter = slack_adapter_from_config(
                cfg, transport=slack_transport
            )
            handle = DurableJobLaneHandle(
                config=cfg,
                lane=DurableLaneService(
                    config=cfg, writer_authority_check=writer_authority_check
                ),
                cursor_adapter=cursor_adapter,
                slack_adapter=slack_adapter,
                preflight=report,
            )
        except LaneClosedError:
            raise
        except Exception:
            logger.debug("durable job lane construct failed", exc_info=True)
            return None
        with _LOCK:
            if key in _LANES:
                raise DurableJobLaneAlreadyAttached(
                    "durable job lane is already attached for this owner"
                )
            _LANES[key] = handle
            if owner is not None:
                _LANE_OWNERS[key] = owner
            previous = _RETIRING.pop(key, None)
        if previous is not None:
            previous.done.set()
            _unbind_idle_cleanup(previous.handle)
        _bind_handle_owner(handle, owner)
        _publish_runner_field(owner, handle)
        logger.info(
            "Durable job lane constructed (dispatch_allowed=%s, adapters=%s/%s)",
            cfg.dispatch_allowed,
            type(cursor_adapter).__name__,
            type(slack_adapter).__name__,
        )
        return handle


def _lookup_handle_binding(
    handle: "DurableJobLaneHandle",
) -> tuple[int, Any]:
    """Resolve owner key/object for a handle without holding oplock."""
    owner = _owner_from_handle(handle)
    if owner is not None:
        return _owner_key(owner), owner
    with _LOCK:
        for key, owned in _LANES.items():
            if owned is handle:
                return key, _LANE_OWNERS.get(key)
        for key, state in _RETIRING.items():
            if state.handle is handle:
                return key, (
                    _LANE_OWNERS.get(key)
                    or state.owner
                    or _owner_from_handle(state.handle)
                )
    return _UNOWNED, None


def detach_durable_job_lane(handle: Optional[DurableJobLaneHandle] = None) -> None:
    """Idempotent shutdown. No-arg clears every owned lane (tests).

    Every path uses CAS-unpublish: registry and runner field change only
    when they still name the exact retired handle. The first unpublisher
    drives ``shutdown()``; a later lease holder fails closed without
    re-entering that in-flight close. Shutdown runs outside locks.
    """
    holder_closed: Optional[LaneClosedError] = None
    if handle is None:
        with _LOCK:
            snapshot = list(_LANES.items())
            retiring = [(key, state.handle) for key, state in _RETIRING.items()]
            owners = dict(_LANE_OWNERS)
        seen: set[int] = set()
        to_close: list[
            tuple[int, DurableJobLaneHandle, Optional[_RetirementState], bool]
        ] = []
        for key, owned in snapshot + retiring:
            if id(owned) in seen:
                continue
            owner = owners.get(key) or _owner_from_handle(owned)
            oplock = _owner_oplock(key)
            with oplock:
                unpublished, state, is_leader = _cas_unpublish(
                    key=key, owner=owner, expected=owned
                )
            target = unpublished if unpublished is not None else owned
            if id(target) in seen:
                continue
            seen.add(id(target))
            to_close.append((key, target, state, is_leader))
        for key, owned, state, is_leader in to_close:
            try:
                _drive_or_join_retirement(key, owned, state, is_leader)
            except LaneClosedError as exc:
                holder_closed = exc
            except Exception:
                logger.debug("durable job lane shutdown failed", exc_info=True)
    else:
        key, owner = _lookup_handle_binding(handle)
        oplock = _owner_oplock(key)
        with oplock:
            _unpublished, state, is_leader = _cas_unpublish(
                key=key, owner=owner, expected=handle
            )
        try:
            _drive_or_join_retirement(key, handle, state, is_leader)
        except LaneClosedError as exc:
            holder_closed = exc
        except Exception:
            logger.debug("durable job lane shutdown failed", exc_info=True)
    if holder_closed is not None:
        raise holder_closed


def attach_to_gateway_runner(
    runner: Any,
    raw_config: Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> Optional[DurableJobLaneHandle]:
    kwargs.pop("owner", None)
    try:
        handle = attach_durable_job_lane(
            raw_config=raw_config, owner=runner, **kwargs
        )
    except DurableJobLaneAlreadyAttached:
        key = id(runner)
        oplock = _owner_oplock(key)
        with oplock:
            with _LOCK:
                handle = _LANES.get(key)
            if handle is not None:
                _publish_runner_field(runner, handle)
        return handle
    except LaneClosedError:
        raise
    except Exception:
        logger.debug("durable job lane attach failed (fail-closed)", exc_info=True)
        return None
    return handle


def detach_from_gateway_runner(runner: Any) -> None:
    _retire_owner_lane(runner)


def _text(value: Any) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return ""


def _nested_id(raw: Any, *keys: str) -> str:
    if not isinstance(raw, Mapping):
        return ""
    for key in keys:
        value = raw.get(key)
        if isinstance(value, Mapping):
            found = _text(value.get("id"))
            if found:
                return found
        found = _text(value)
        if found:
            return found
    return ""


def _resolve_lane_for_slack(workspace_id: str) -> Optional[DurableJobLaneHandle]:
    with _LOCK:
        live = list(_LANES.values())
    if not live:
        return None
    if len(live) == 1:
        return live[0]
    matches = [
        handle
        for handle in live
        if handle.config.identity_binding is not None
        and handle.config.identity_binding.workspace_id == workspace_id
    ]
    if len(matches) == 1:
        return matches[0]
    return None


def parse_slack_durable_action(
    body: Mapping[str, Any] | None, action: Mapping[str, Any] | None
) -> Optional[dict[str, str]]:
    if not isinstance(action, Mapping):
        return None
    action_id = _text(action.get("action_id"))
    if action_id not in _ACTION_TO_DECISION:
        return None
    raw_value = action.get("value")
    payload: dict[str, Any]
    if isinstance(raw_value, Mapping):
        payload = dict(raw_value)
    elif isinstance(raw_value, str) and raw_value.strip():
        try:
            loaded = json.loads(raw_value)
        except (TypeError, ValueError):
            return None
        if not isinstance(loaded, dict):
            return None
        payload = loaded
    else:
        payload = {}
    body = body if isinstance(body, Mapping) else {}
    message = body.get("message") if isinstance(body.get("message"), Mapping) else {}
    verified = {
        "workspace_id": _nested_id(body, "team", "team_id"),
        "channel_id": _nested_id(body, "channel", "channel_id"),
        "root_thread_ts": _text(message.get("thread_ts")) or _text(message.get("ts")),
        "actor_id": _nested_id(body, "user", "user_id"),
        "decision_type": _ACTION_TO_DECISION[action_id],
    }
    if not all(verified.values()):
        return None
    for key in _IDENTITY_PAYLOAD_KEYS:
        claimed = _text(payload.get(key))
        if claimed and claimed != verified[key]:
            return None
    return {
        "job_id": _text(payload.get("job_id")),
        "workspace_id": verified["workspace_id"],
        "channel_id": verified["channel_id"],
        "root_thread_ts": verified["root_thread_ts"],
        "actor_id": verified["actor_id"],
        "decision_type": verified["decision_type"],
        "decision_idempotency_key": _text(payload.get("decision_idempotency_key")),
        "policy_version": _text(payload.get("policy_version")),
        "candidate_id": _text(payload.get("candidate_id")),
        "candidate_version": _text(payload.get("candidate_version")),
    }


def consume_slack_action_if_active(
    body: Mapping[str, Any] | None,
    action: Mapping[str, Any] | None,
    *,
    ack_port: Any = None,
) -> Optional[InboundActionResult]:
    """Reuse DurableLaneService inbound ingress. No parallel Slack router."""
    with _LOCK:
        live = list(_LANES.values())
    if not live:
        return None
    parsed = parse_slack_durable_action(body, action)
    if parsed is None:
        return InboundActionResult(ok=False, ack_status="rejected")
    handle = _resolve_lane_for_slack(parsed["workspace_id"])
    if handle is None:
        return InboundActionResult(ok=False, ack_status="rejected")
    binding = handle.config.identity_binding
    if binding is None or binding.workspace_id != parsed["workspace_id"]:
        return InboundActionResult(ok=False, ack_status="rejected")
    if not binding.repository_identity:
        return InboundActionResult(ok=False, ack_status="rejected")
    if getattr(handle.lane, "_closed", False):
        return InboundActionResult(ok=False, ack_status="pending", retryable=True)
    try:
        return handle.lane.consume_inbound_action(
            ack_port if ack_port is not None else _SilentAck(),
            **parsed,
        )
    except sqlite3.OperationalError:
        return InboundActionResult(ok=False, ack_status="pending", retryable=True)
