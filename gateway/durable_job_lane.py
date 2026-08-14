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
import weakref
from dataclasses import dataclass
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
# Keys are id(owner). The owner object is not stored here — id() cannot
# recover it. Each published handle carries a weakref to its owner so the
# public handle-based detach API can CAS-clear owner._durable_job_lane
# only when that field still points at the exact retired handle.
_LANES: dict[int, "DurableJobLaneHandle"] = {}
_OWNER_OPLOCKS: dict[int, threading.Lock] = {}

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
        return redact_secret_text(
            "DurableJobLaneHandle("
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
    """Bind a weak owner ref so handle detach can CAS-clear the runner field."""
    handle._owner_ref = weakref.ref(owner) if owner is not None else None


def _owner_from_handle(handle: "DurableJobLaneHandle") -> Any:
    ref = getattr(handle, "_owner_ref", None)
    if ref is None:
        return None
    return ref()


def _pop_handle_locked(handle: "DurableJobLaneHandle") -> bool:
    """Remove ``handle`` from ``_LANES`` by identity. Caller holds ``_LOCK``."""
    popped = False
    for key, owned in list(_LANES.items()):
        if owned is handle:
            del _LANES[key]
            popped = True
    return popped


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


def _retire_owner_lane(owner: Any) -> None:
    """Detach+shutdown the previously attached lane for this owner, if any.

    Registry pop and runner-field CAS run under the per-owner lock.
    Shutdown runs outside that lock so a concurrent valid attach can
    publish without holding shutdown I/O, and a lease-holder reattach
    cannot deadlock against a waiter.
    """
    key = _owner_key(owner)
    oplock = _owner_oplock(key)
    with oplock:
        with _LOCK:
            handle = _LANES.pop(key, None)
        _cas_clear_runner_field(owner, handle)
    if handle is None:
        return
    try:
        handle.shutdown()
    except LaneClosedError:
        raise
    except Exception:
        logger.debug("durable job lane retire failed", exc_info=True)


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
    if not report.runtime_ready:
        logger.debug(
            "durable job lane refusing attach; runtime capability unbound (%s)",
            report.reasons,
        )
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
        try:
            cursor_adapter = cursor_adapter_from_config(
                cfg, transport=cursor_transport
            )
            slack_adapter = slack_adapter_from_config(
                cfg, transport=slack_transport
            )
            handle = DurableJobLaneHandle(
                config=cfg,
                lane=DurableLaneService(config=cfg),
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
        _bind_handle_owner(handle, owner)
        _publish_runner_field(owner, handle)
        logger.info(
            "Durable job lane constructed (dispatch_allowed=%s, adapters=%s/%s)",
            cfg.dispatch_allowed,
            type(cursor_adapter).__name__,
            type(slack_adapter).__name__,
        )
        return handle


def detach_durable_job_lane(handle: Optional[DurableJobLaneHandle] = None) -> None:
    """Idempotent shutdown. No-arg clears every owned lane (tests).

    Registry membership and ``owner._durable_job_lane`` stay consistent:
    the runner field is CAS-cleared only when it still points at the
    exact retired handle. Owner is recovered from the weakref bound at
    publish time. Shutdown runs outside ``_LOCK`` and per-owner oplocks.
    """
    holder_closed: Optional[LaneClosedError] = None
    if handle is None:
        with _LOCK:
            victims = list(_LANES.items())
            _LANES.clear()
            for _key, owned in victims:
                _cas_clear_runner_field(_owner_from_handle(owned), owned)
        to_close = [owned for _key, owned in victims]
    else:
        owner = _owner_from_handle(handle)
        popped = False
        if owner is not None:
            oplock = _owner_oplock(_owner_key(owner))
            with oplock:
                with _LOCK:
                    key = _owner_key(owner)
                    if _LANES.get(key) is handle:
                        _LANES.pop(key, None)
                        popped = True
                    else:
                        popped = _pop_handle_locked(handle)
                _cas_clear_runner_field(owner, handle)
        else:
            with _LOCK:
                popped = _pop_handle_locked(handle)
        to_close = [handle] if popped else []
    for owned in to_close:
        try:
            owned.shutdown()
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
    key = id(runner)
    oplock = _owner_oplock(key)
    with oplock:
        with _LOCK:
            owned = _LANES.pop(key, None)
        _cas_clear_runner_field(runner, owned)
    if owned is None:
        return
    try:
        owned.shutdown()
    except LaneClosedError:
        raise
    except Exception:
        logger.debug("durable job lane detach failed", exc_info=True)


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
