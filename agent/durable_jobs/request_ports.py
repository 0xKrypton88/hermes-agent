"""Explicitly injected Cursor Cloud and Slack request-port adapters.

These wrap an already-constructed client that matches the repository's
existing seams. They never construct HTTP/SDK clients, never read secret
*values* into logs, and never mint a client from config flags.

Credential resolution is a mandatory request-time dependency only.
Attach/preflight must not invoke it.
"""

from __future__ import annotations

import inspect
import math
import re
import threading
import time
from collections.abc import Iterator, Mapping as ABCMapping
from types import MappingProxyType
from typing import Any, Callable, Mapping, Optional

from agent.durable_jobs.config import validate_secret_ref_name
from agent.durable_jobs.cursor_cloud import (
    cursor_correlation_agent_id,
    cursor_correlation_name,
    redact_provider_error,
)
from agent.durable_jobs.lane import LaneClosedError
from agent.durable_jobs.redaction import redact_payload, redact_secret_text
from agent.durable_jobs.slack_bridge import redact_slack_error

_XOX_RE = re.compile(r"(?i)xox[abps]-[A-Za-z0-9-]+")
_CURSOR_METHODS = ("create_agent", "get_agent", "get_run")
_SLACK_METHODS = ("chat_postMessage", "conversations_replies")
# Completed claims are permanent authority against duplicate external writes.
# Bound memory by refusing new unique writes rather than silently evicting one.
_MAX_PERMANENT_WRITE_CLAIMS = 1024


class RequestPortError(Exception):
    """Fail-closed request-port error. Message is already redacted."""

    def __init__(self, message: str) -> None:
        super().__init__(_redact(message))


class RequestPortMismatch(RequestPortError):
    """Identity, secret-ref, or correlation mismatch. Zero client effect."""


class RequestPortTimeout(RequestPortError):
    """Timeout or cancellation before any client call."""


class RequestPortClosed(RequestPortError):
    """Port was closed; subsequent calls are fail-closed."""


def _redact(message: BaseException | str) -> str:
    text = redact_slack_error(message)
    text = redact_provider_error(text)
    text = redact_secret_text(text)
    return _XOX_RE.sub("[REDACTED]", text)


def _require_text(value: Any, field: str) -> str:
    if type(value) is not str or not value.strip():
        raise TypeError(f"{field} is required")
    return value.strip()


def _require_client(client: Any, methods: tuple[str, ...]) -> Any:
    if client is None:
        raise TypeError(
            "injected client is required; no HTTP/SDK client is constructed"
        )
    try:
        lookup = inspect.getattr_static(type(client), "__getattribute__")
    except BaseException:
        lookup = None
    if lookup is not object.__getattribute__:
        raise TypeError("injected client requires static client lookup")
    missing = []
    for name in methods:
        try:
            seam = inspect.getattr_static(client, name)
        except BaseException:
            missing.append(name)
            continue
        # Only concrete Python/builtin functions are approved. Properties,
        # descriptors, callable objects, and dynamic __getattr__ seams are
        # rejected without executing any client-controlled hook.
        if not (inspect.isfunction(seam) or inspect.isbuiltin(seam)):
            missing.append(name)
    if missing:
        raise TypeError(
            "injected client is missing required seam methods: "
            + ", ".join(missing)
        )
    return client


def _as_payload(payload: Any) -> dict[str, Any]:
    # Payload validation must not execute attacker-controlled Mapping hooks.
    # The public request contract requires a plain built-in dictionary; reject
    # subclasses and arbitrary Mapping implementations before membership/get.
    if type(payload) is not dict:
        raise RequestPortMismatch("payload must be a plain dictionary")
    # Even an exact built-in dict can contain attacker-controlled key objects.
    # Built-in lookup would execute their __hash__/__eq__ and could let one
    # masquerade as a bound identity key.  Inspect the native key iterator and
    # fail closed before any lookup; dict.keys itself executes no key hooks.
    if any(type(key) is not str for key in dict.keys(payload)):
        raise RequestPortMismatch("payload must use plain text keys")
    return payload


def _match_bound(payload: Mapping[str, Any], key: str, bound: str) -> None:
    if key not in payload:
        raise RequestPortMismatch(f"{key} is required")
    value = payload.get(key)
    if type(value) is not str or value.strip() != bound:
        raise RequestPortMismatch(f"{key} does not match bound identity")


def _check_deadline(
    *,
    timeout_seconds: Any = None,
    deadline: Optional[float] = None,
    cancel_event: Any = None,
) -> None:
    if deadline is not None and time.monotonic() >= deadline:
        raise RequestPortTimeout("timeout")
    if cancel_event is not None:
        invalid_cancellation = False
        try:
            cancelled = getattr(cancel_event, "is_set", lambda: False)()
        except BaseException:
            invalid_cancellation = True
            cancelled = False
        if invalid_cancellation:
            raise RequestPortTimeout("invalid cancellation hook")
        if cancelled:
            raise RequestPortTimeout("cancelled")
        # Calling the user-supplied hook is admission work and consumes budget.
        if deadline is not None and time.monotonic() >= deadline:
            raise RequestPortTimeout("timeout")
    if timeout_seconds is None:
        return
    invalid_timeout = False
    try:
        seconds = float(timeout_seconds)
    except (TypeError, ValueError):
        invalid_timeout = True
        seconds = 0.0
    if invalid_timeout:
        raise RequestPortTimeout("invalid timeout")
    if seconds <= 0:
        raise RequestPortTimeout("timeout")


def _unwrap_response(result: Any) -> Any:
    if isinstance(result, (dict, list, tuple, str, bytes)):
        return result
    try:
        json_seam = inspect.getattr_static(type(result), "json")
    except (AttributeError, TypeError):
        return result
    # Provider-controlled properties, descriptors, dynamic instance attributes,
    # and callable objects must not execute during response inspection.
    if not inspect.isfunction(json_seam):
        return result
    json_fn = json_seam.__get__(result, type(result))
    try:
        unwrapped = json_fn()
    except BaseException:
        return result
    if inspect.iscoroutine(unwrapped):
        unwrapped.close()
        raise RequestPortError(
            "async client methods are not invoked from request ports"
        )
    return unwrapped


def _client_method(client: Any, method_name: str) -> Any:
    method = getattr(client, method_name, None)
    if method is None or not callable(method):
        raise RequestPortError(f"injected client is missing {method_name}")
    if inspect.iscoroutinefunction(method):
        raise RequestPortError(
            "async client methods are not invoked from request ports"
        )
    return method


def _invoke(
    client: Any,
    method_name: str,
    *args: Any,
    _secret_value: Any = None,
    **kwargs: Any,
) -> Any:
    method = _client_method(client, method_name)
    lane_closed = False
    provider_error_message = None
    provider_base_failed = False
    try:
        result = method(*args, **kwargs)
    except LaneClosedError:
        lane_closed = True
        result = None
    except Exception as exc:
        try:
            provider_error_message = _redact(exc)
        except BaseException:
            provider_error_message = "provider request failed"
        if type(_secret_value) is str and _secret_value:
            provider_error_message = provider_error_message.replace(
                _secret_value, "[REDACTED]"
            )
        result = None
    except BaseException:
        provider_base_failed = True
        result = None
    if lane_closed:
        raise LaneClosedError("lane closed during request")
    if provider_error_message is not None:
        raise RequestPortError(provider_error_message)
    if provider_base_failed:
        # Provider BaseException objects and chains are untrusted and may carry
        # resolved credentials. Preserve no provider-controlled diagnostics.
        raise RequestPortError("provider request failed")
    if inspect.iscoroutine(result):
        result.close()
        raise RequestPortError(
            "async client methods are not invoked from request ports"
        )
    return _unwrap_response(result)


def _resolve_secret(
    resolver: Optional[Callable[[str], Any]], secret_ref: str
) -> Any:
    if resolver is None:
        raise RequestPortError("credential resolver is required")
    resolver_failed = False
    try:
        value = resolver(secret_ref)
    except BaseException:
        resolver_failed = True
        value = None
    if resolver_failed:
        raise RequestPortError("credential resolver failed")
    if value is None or (isinstance(value, str) and not value.strip()):
        raise RequestPortError("credential resolver returned empty")
    return value


def _cursor_key(payload: Mapping[str, Any]) -> str:
    key = payload.get("idempotency_key")
    if type(key) is not str or not key.strip():
        raise RequestPortMismatch("idempotency_key is required")
    return key.strip()


def _verify_cursor_correlation(payload: Mapping[str, Any], key: str) -> tuple[str, str]:
    try:
        expected_name = cursor_correlation_name(key)
        expected_id = cursor_correlation_agent_id(key)
    except ValueError as exc:
        raise RequestPortMismatch(_redact(exc)) from None
    name = payload.get("name")
    agent_id = payload.get("agentId", payload.get("agent_id"))
    if name is not None and (type(name) is not str or name.strip() != expected_name):
        raise RequestPortMismatch("cursor name does not match idempotency key")
    if agent_id is not None and (
        type(agent_id) is not str or agent_id.strip() != expected_id
    ):
        raise RequestPortMismatch("cursor agentId does not match idempotency key")
    return expected_name, expected_id


def _slack_key(payload: Mapping[str, Any]) -> str:
    for name in ("client_msg_id", "idempotency_key"):
        value = payload.get(name)
        if type(value) is str and value.strip():
            return value.strip()
    raise RequestPortMismatch("client_msg_id is required")


def _message_client_msg_id(raw: Any) -> str:
    if not isinstance(raw, Mapping):
        return ""
    value = raw.get("client_msg_id")
    if type(value) is str and value.strip():
        return value.strip()
    inner = raw.get("message")
    if isinstance(inner, Mapping):
        value = inner.get("client_msg_id")
        if type(value) is str and value.strip():
            return value.strip()
    return ""


def _filter_slack_lookup(raw: Any, expected: str) -> dict[str, Any]:
    if isinstance(raw, Mapping):
        messages = raw.get("messages")
        if isinstance(messages, (list, tuple)):
            matched = [
                item for item in messages if _message_client_msg_id(item) == expected
            ]
            out = dict(raw)
            out["messages"] = matched
            return out
        if _message_client_msg_id(raw) == expected:
            return dict(raw)
        return {"ok": True, "messages": []}
    if isinstance(raw, (list, tuple)):
        return {
            "ok": True,
            "messages": [item for item in raw if _message_client_msg_id(item) == expected],
        }
    return {"ok": True, "messages": []}


def _receipt_value(raw: Any) -> Any:
    if isinstance(raw, Mapping):
        return redact_payload(dict(raw))
    if isinstance(raw, list):
        return redact_payload(list(raw))
    if raw is None or isinstance(raw, (int, float, bool)):
        return raw
    if isinstance(raw, str):
        return _redact(raw)
    return {"type": type(raw).__name__}


class _FrozenDict(ABCMapping[str, Any]):
    """A true immutable mapping snapshot used by results and receipts."""

    __slots__ = ("_data",)

    def __init__(self, values: Mapping[str, Any]) -> None:
        self._data = MappingProxyType(dict(values))

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return repr(dict(self._data))


def _sanitize_snapshot_key(
    raw: Any, secret: Optional[str], *, allow_hooks: bool
) -> str:
    """Redact one copied mapping key without propagating hostile hooks."""
    if type(raw) is str:
        text = raw
    elif allow_hooks:
        try:
            text = str(raw)
        except BaseException:
            text = f"<{type(raw).__name__}>"
    else:
        # Caller-supplied payload keys are an authority boundary. Never
        # inspect or stringify them while recording a rejected request:
        # metaclass/type-name and __str__ hooks can leak data or raise
        # BaseException.
        text = "<non-text-key>"
    if secret:
        text = text.replace(secret, "[REDACTED]")
    return _redact(text)


def _sanitize_snapshot(
    raw: Any, secret_value: Any = None, *, allow_key_hooks: bool = True
) -> Any:
    """Recursively copy, redact the resolved value everywhere, and freeze."""
    secret = secret_value if type(secret_value) is str and secret_value else None
    if type(raw) is dict:
        return _FrozenDict(
            {
                _sanitize_snapshot_key(
                    key, secret, allow_hooks=allow_key_hooks
                ): _sanitize_snapshot(
                    value, secret, allow_key_hooks=allow_key_hooks
                )
                for key, value in dict.items(raw)
            }
        )
    if type(raw) in (list, tuple, set, frozenset):
        return tuple(
            _sanitize_snapshot(value, secret, allow_key_hooks=allow_key_hooks)
            for value in raw
        )
    if type(raw) is _FrozenDict:
        values = object.__getattribute__(raw, "_data")
        if type(values) is not MappingProxyType:
            return _FrozenDict({"type": type(raw).__name__})
        return _FrozenDict(
            {
                _sanitize_snapshot_key(
                    key, secret, allow_hooks=allow_key_hooks
                ): _sanitize_snapshot(
                    value, secret, allow_key_hooks=allow_key_hooks
                )
                for key, value in values.items()
            }
        )
    if isinstance(raw, Mapping):
        # Arbitrary Mapping implementations can execute attacker-controlled
        # iteration/items hooks. Preserve no data rather than risk surfacing a
        # resolved credential from a sanitizer exception.
        return _FrozenDict({"type": type(raw).__name__})
    if type(raw) is str:
        text = raw.replace(secret, "[REDACTED]") if secret else raw
        return _redact(text)
    if raw is None or isinstance(raw, (int, float, bool)):
        return raw
    return _FrozenDict({"type": type(raw).__name__})


class _InjectedRequestPortBase:
    def __init__(
        self,
        *,
        client: Any,
        secret_ref: str,
        workspace_id: str,
        repository_identity: str,
        methods: tuple[str, ...],
        credential_resolver: Optional[Callable[[str], Any]] = None,
    ) -> None:
        self._client = _require_client(client, methods)
        self._secret_ref = validate_secret_ref_name(secret_ref, field="secret_ref")
        self._workspace_id = _require_text(workspace_id, "workspace_id")
        self._repository_identity = _require_text(
            repository_identity, "repository_identity"
        )
        self._credential_resolver = credential_resolver
        self._closed = False
        self._lock = threading.RLock()
        self._idle = threading.Condition(self._lock)
        self._active_calls = 0
        self._receipts: list[_FrozenDict] = []
        self._claims: dict[str, dict[str, Any]] = {}
        capacity = _MAX_PERMANENT_WRITE_CLAIMS
        if type(capacity) is not int:
            raise TypeError("permanent write claim capacity must be an integer")
        if capacity <= 0:
            raise ValueError("permanent write claim capacity must be positive")
        self._write_claim_capacity = capacity

    def _reserve_write_claim(self, claim_key: str) -> tuple[dict[str, Any], bool]:
        """Atomically return an existing claim or reserve one permanent slot."""
        with self._idle:
            if self._closed:
                raise RequestPortClosed("request port is closed")
            claim = self._claims.get(claim_key)
            if claim is not None:
                return claim, False
            if len(self._claims) >= self._write_claim_capacity:
                raise RequestPortError("permanent write claim capacity exhausted")
            claim = {"event": threading.Event()}
            self._claims[claim_key] = claim
            return claim, True

    def _release_unstarted_write_claim(
        self, claim_key: Optional[str], claim: Any, owner: bool
    ) -> None:
        """Release only this caller's unresolved claim before worker admission."""
        if not owner or claim_key is None or claim is None:
            return
        with self._idle:
            if (
                self._claims.get(claim_key) is claim
                and not claim.get("worker_admitted", False)
                and not claim["event"].is_set()
            ):
                del self._claims[claim_key]
                self._idle.notify_all()

    def _admit_public_call(
        self, *, timeout_seconds: Any, cancel_event: Any
    ) -> Optional[float]:
        """Admit one public request and establish its single absolute deadline."""
        admission_started = time.monotonic()
        if timeout_seconds is None:
            deadline = None
        else:
            invalid_timeout = False
            try:
                seconds = float(timeout_seconds)
            except BaseException:
                invalid_timeout = True
                seconds = 0.0
            if invalid_timeout:
                raise RequestPortTimeout("invalid timeout")
            if not math.isfinite(seconds) or seconds <= 0:
                raise RequestPortTimeout("timeout")
            deadline = admission_started + seconds
        _check_deadline(deadline=deadline, cancel_event=cancel_event)
        with self._idle:
            if self._closed:
                raise RequestPortClosed("request port is closed")
            self._active_calls += 1
        return deadline

    def _release_public_call(self) -> None:
        with self._idle:
            self._active_calls -= 1
            self._idle.notify_all()

    def _ensure_open(self) -> None:
        with self._lock:
            if self._closed:
                raise RequestPortClosed("request port is closed")

    def close(self, timeout_seconds: float = 0.25) -> bool:
        deadline = time.monotonic() + max(0.0, float(timeout_seconds))
        with self._idle:
            self._closed = True
            while self._active_calls:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._idle.wait(remaining)
            return True

    @property
    def receipts(self) -> tuple[dict[str, Any], ...]:
        with self._lock:
            return tuple(self._receipts)

    def _record_receipt(
        self,
        *,
        operation: str,
        secret_ref: Any,
        payload: Any,
        outcome: str,
        client_invoked: bool,
        response: Any = None,
        error: BaseException | None = None,
        secret_value: Any = None,
    ) -> None:
        try:
            offered = secret_ref if type(secret_ref) is str else ""
            receipt: dict[str, Any] = {
                "operation": operation if type(operation) is str else "",
                "secret_ref": offered,
                "payload": _sanitize_snapshot(
                    payload, secret_value, allow_key_hooks=False
                ),
                "outcome": outcome,
                "client_invoked": bool(client_invoked),
            }
            if error is not None:
                text = _redact(error)
                if type(secret_value) is str and secret_value:
                    text = text.replace(secret_value, "[REDACTED]")
                receipt["error"] = text
            elif response is not None:
                receipt["response"] = _sanitize_snapshot(response, secret_value)
            with self._lock:
                self._receipts.append(_sanitize_snapshot(receipt, secret_value))
        except Exception:
            return

    def _run_sync(
        self,
        claim_key: str,
        call: Callable[[], Any],
        *,
        idempotent: bool = True,
        deadline: Optional[float],
        cancel_event: Any,
        sanitize_result: Callable[[Any], Any],
        reserved_claim: Optional[dict[str, Any]] = None,
        reserved_owner: Optional[bool] = None,
    ) -> Any:
        _check_deadline(deadline=deadline, cancel_event=cancel_event)
        with self._idle:
            if self._closed:
                raise RequestPortClosed("request port is closed")
            if idempotent and reserved_claim is not None:
                if self._claims.get(claim_key) is not reserved_claim:
                    raise RequestPortError("write claim reservation was lost")
                claim = reserved_claim
                owner = bool(reserved_owner)
                if owner:
                    claim["worker_admitted"] = True
                    self._active_calls += 1
            else:
                claim = self._claims.get(claim_key) if idempotent else None
                if claim is None:
                    claim = {"event": threading.Event()}
                    if idempotent:
                        # Completed write claims remain authoritative: silently
                        # evicting one would permit duplicate external dispatch.
                        self._claims[claim_key] = claim
                    self._active_calls += 1
                    owner = True
                else:
                    owner = False

        if owner:
            accounting = {"owned": True}

            def worker() -> None:
                try:
                    # This is the final dispatch-authority gate: admission and
                    # Thread.start() can consume the caller's remaining budget.
                    _check_deadline(deadline=deadline, cancel_event=cancel_event)
                    # Claims may outlive the credential resolved by this caller.
                    # Cache only a credential-independent frozen snapshot.
                    claim["result"] = sanitize_result(call())
                except BaseException as exc:
                    # Stored only until the waiting caller normalizes the error
                    # into a fresh, chainless domain exception.
                    claim["exception"] = exc
                finally:
                    with self._idle:
                        if accounting["owned"]:
                            self._active_calls -= 1
                            accounting["owned"] = False
                        claim["event"].set()
                        self._idle.notify_all()

            start_failed = False
            try:
                thread = threading.Thread(
                    target=worker,
                    name=f"request-port-{claim_key[:24]}",
                    daemon=True,
                )
                thread.start()
            except BaseException as exc:
                start_failed = True
                with self._idle:
                    unresolved = not claim["event"].is_set()
                    if accounting["owned"]:
                        self._active_calls -= 1
                        accounting["owned"] = False
                    if (
                        unresolved
                        and idempotent
                        and self._claims.get(claim_key) is claim
                    ):
                        del self._claims[claim_key]
                    if unresolved:
                        claim["exception"] = exc
                        claim["event"].set()
                    self._idle.notify_all()
            if start_failed:
                raise RequestPortError("request worker failed to start")

        while True:
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise RequestPortTimeout("timeout")
            wait_for = 0.005 if remaining is None else min(0.005, remaining)
            if claim["event"].wait(wait_for):
                break
            if cancel_event is not None:
                _check_deadline(deadline=None, cancel_event=cancel_event)
        # Event.wait() can return true only after its requested interval.  A
        # completion that crossed this caller's deadline is still a timeout.
        if cancel_event is not None:
            _check_deadline(deadline=None, cancel_event=cancel_event)
        _check_deadline(deadline=deadline, cancel_event=cancel_event)
        if "exception" in claim:
            exc = claim["exception"]
            if type(exc) is LaneClosedError:
                raise LaneClosedError("lane closed during request")
            if type(exc) is RequestPortError:
                message = str(exc)
            else:
                message = "provider worker failed"
            raise RequestPortError(message)
        return claim.get("result")

    def _validate_common(
        self,
        *,
        secret_ref: str,
        payload: Any,
        deadline: Optional[float] = None,
        cancel_event: Any = None,
    ) -> Mapping[str, Any]:
        _check_deadline(
            timeout_seconds=None,
            deadline=deadline,
            cancel_event=cancel_event,
        )
        try:
            offered = validate_secret_ref_name(secret_ref, field="secret_ref")
        except Exception as exc:
            raise RequestPortMismatch(_redact(exc)) from None
        if offered != self._secret_ref:
            raise RequestPortMismatch("secret_ref does not match bound reference")
        body = _as_payload(payload)
        _match_bound(body, "workspace_id", self._workspace_id)
        _match_bound(body, "repository_identity", self._repository_identity)
        return body

    def _resolve_credential(
        self,
        *,
        deadline: Optional[float] = None,
        cancel_event: Any = None,
    ) -> Any:
        # Credential resolution is untrusted request-bound work and must consume
        # the same absolute budget as provider dispatch.  Keep its worker in
        # lifecycle accounting after a caller times out, but abandon its value
        # so late completion can never become orphan dispatch authority.
        state: dict[str, Any] = {"done": False, "abandoned": False}
        with self._idle:
            if self._closed:
                raise RequestPortClosed("request port is closed")
            self._active_calls += 1

        def worker() -> None:
            value = None
            error_message = None
            try:
                value = _resolve_secret(self._credential_resolver, self._secret_ref)
            except BaseException as exc:
                if type(exc) is RequestPortError:
                    error_message = str(exc)
                else:
                    error_message = "credential resolver failed"
            finally:
                with self._idle:
                    if not state["abandoned"]:
                        if error_message is not None:
                            state["error"] = error_message
                        elif (
                            not self._closed
                            and (deadline is None or time.monotonic() < deadline)
                        ):
                            state["value"] = value
                    state["done"] = True
                    self._active_calls -= 1
                    self._idle.notify_all()

        start_failed = False
        try:
            thread = threading.Thread(
                target=worker,
                name="request-port-credential-resolver",
                daemon=True,
            )
            thread.start()
        except BaseException:
            start_failed = True
            with self._idle:
                self._active_calls -= 1
                state["abandoned"] = True
                state["done"] = True
                self._idle.notify_all()
        if start_failed:
            raise RequestPortError("request worker failed to start")

        try:
            while True:
                with self._idle:
                    if state["done"]:
                        break
                    remaining = (
                        None if deadline is None else deadline - time.monotonic()
                    )
                    if remaining is not None and remaining <= 0:
                        state["abandoned"] = True
                        raise RequestPortTimeout("timeout")
                    self._idle.wait(
                        0.005 if remaining is None else min(0.005, remaining)
                    )
                _check_deadline(deadline=deadline, cancel_event=cancel_event)
            self._ensure_open()
            _check_deadline(deadline=deadline, cancel_event=cancel_event)
        except BaseException:
            with self._idle:
                state["abandoned"] = True
                state.pop("value", None)
            raise

        if "error" in state:
            raise RequestPortError(state["error"])
        if "value" not in state:
            raise RequestPortTimeout("timeout")
        return state.pop("value")

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"secret_ref={self._secret_ref!r}, "
            f"workspace_id={self._workspace_id!r}, "
            f"repository_identity={self._repository_identity!r})"
        )


class CursorCloudInjectedRequestPort(_InjectedRequestPortBase):
    """Wraps an injected Cursor Cloud client (``create_agent`` / ``get_agent`` / ``get_run``)."""

    def __init__(
        self,
        *,
        client: Any,
        secret_ref: str,
        workspace_id: str,
        repository_identity: str,
        credential_resolver: Optional[Callable[[str], Any]] = None,
    ) -> None:
        super().__init__(
            client=client,
            secret_ref=secret_ref,
            workspace_id=workspace_id,
            repository_identity=repository_identity,
            methods=_CURSOR_METHODS,
            credential_resolver=credential_resolver,
        )

    def __call__(
        self,
        *,
        operation: str,
        secret_ref: str,
        payload: dict[str, Any],
        timeout_seconds: Any = None,
        cancel_event: Any = None,
    ) -> Any:
        client_invoked = False
        secret_value = None
        admitted = False
        write_claim_key: Optional[str] = None
        write_claim: Optional[dict[str, Any]] = None
        write_claim_owner = False
        try:
            if type(operation) is not str:
                operation = ""
                payload = {}
                raise RequestPortMismatch("operation must be plain text")
            deadline = self._admit_public_call(
                timeout_seconds=timeout_seconds,
                cancel_event=cancel_event,
            )
            admitted = True
            body = self._validate_common(
                secret_ref=secret_ref,
                payload=payload,
                deadline=deadline,
                cancel_event=cancel_event,
            )
            if operation == "create":
                key = _cursor_key(body)
                name, agent_id = _verify_cursor_correlation(body, key)
                write_claim_key = f"cursor:create:{key}"
                write_claim, write_claim_owner = self._reserve_write_claim(
                    write_claim_key
                )
                _client_method(self._client, "create_agent")
            elif operation == "lookup":
                key = _cursor_key(body)
                _verify_cursor_correlation(body, key)
                agent_id = cursor_correlation_agent_id(key)
                _client_method(self._client, "get_agent")
            elif operation == "status":
                key = _cursor_key(body)
                _name, expected_agent_id = _verify_cursor_correlation(body, key)
                agent_id = body.get("agent_id")
                run_id = body.get("run_id")
                if type(agent_id) is not str or agent_id != expected_agent_id:
                    raise RequestPortMismatch(
                        "Cursor payload agent identity does not match correlation"
                    )
                if type(run_id) is not str or not run_id.strip():
                    raise RequestPortMismatch("run_id is required")
                _client_method(self._client, "get_run")
            else:
                raise RequestPortError(f"unsupported cursor operation: {operation}")

            if operation != "create" or write_claim_owner:
                secret_value = self._resolve_credential(
                    deadline=deadline,
                    cancel_event=cancel_event,
                )
            if operation == "create":
                client_invoked = write_claim_owner
                result = self._run_sync(
                    write_claim_key,
                    lambda: _invoke(
                        self._client,
                        "create_agent",
                        {"name": name, "agentId": agent_id},
                        _secret_value=secret_value,
                    ),
                    deadline=deadline,
                    cancel_event=cancel_event,
                    sanitize_result=lambda value: _sanitize_snapshot(
                        value, secret_value
                    ),
                    reserved_claim=write_claim,
                    reserved_owner=write_claim_owner,
                )
            elif operation == "lookup":
                client_invoked = True
                result = self._run_sync(
                    f"cursor:lookup:{key}",
                    lambda: _invoke(
                        self._client,
                        "get_agent",
                        agent_id,
                        _secret_value=secret_value,
                    ),
                    idempotent=False,
                    deadline=deadline,
                    cancel_event=cancel_event,
                    sanitize_result=lambda value: _sanitize_snapshot(
                        value, secret_value
                    ),
                )
            elif operation == "status":
                client_invoked = True
                result = self._run_sync(
                    f"cursor:status:{agent_id}:{run_id}",
                    lambda: _invoke(
                        self._client,
                        "get_run",
                        expected_agent_id,
                        run_id.strip(),
                        _secret_value=secret_value,
                    ),
                    idempotent=False,
                    deadline=deadline,
                    cancel_event=cancel_event,
                    sanitize_result=lambda value: _sanitize_snapshot(
                        value, secret_value
                    ),
                )
            else:
                raise RequestPortError(f"unsupported cursor operation: {operation}")
            self._record_receipt(
                operation=operation,
                secret_ref=secret_ref,
                payload=payload,
                outcome="ok",
                client_invoked=True,
                response=result,
                secret_value=secret_value,
            )
            return _sanitize_snapshot(result, secret_value)
        except (LaneClosedError, RequestPortError, Exception) as exc:
            self._record_receipt(
                operation=operation,
                secret_ref=secret_ref,
                payload=payload,
                outcome="error",
                client_invoked=client_invoked,
                error=exc,
                secret_value=secret_value,
            )
            raise
        finally:
            self._release_unstarted_write_claim(
                write_claim_key, write_claim, write_claim_owner
            )
            if admitted:
                self._release_public_call()


class SlackInjectedRequestPort(_InjectedRequestPortBase):
    """Wraps an injected Slack client (``chat_postMessage`` / ``conversations_replies``)."""

    def __init__(
        self,
        *,
        client: Any,
        secret_ref: str,
        workspace_id: str,
        channel_id: str,
        repository_identity: str,
        root_thread_ts: str,
        credential_resolver: Optional[Callable[[str], Any]] = None,
    ) -> None:
        super().__init__(
            client=client,
            secret_ref=secret_ref,
            workspace_id=workspace_id,
            repository_identity=repository_identity,
            methods=_SLACK_METHODS,
            credential_resolver=credential_resolver,
        )
        self._channel_id = _require_text(channel_id, "channel_id")
        self._root_thread_ts = _require_text(root_thread_ts, "root_thread_ts")

    def __call__(
        self,
        *,
        operation: str,
        secret_ref: str,
        payload: dict[str, Any],
        timeout_seconds: Any = None,
        cancel_event: Any = None,
    ) -> Any:
        client_invoked = False
        secret_value = None
        admitted = False
        write_claim_key: Optional[str] = None
        write_claim: Optional[dict[str, Any]] = None
        write_claim_owner = False
        try:
            if type(operation) is not str:
                operation = ""
                payload = {}
                raise RequestPortMismatch("operation must be plain text")
            deadline = self._admit_public_call(
                timeout_seconds=timeout_seconds,
                cancel_event=cancel_event,
            )
            admitted = True
            body = self._validate_common(
                secret_ref=secret_ref,
                payload=payload,
                deadline=deadline,
                cancel_event=cancel_event,
            )
            _match_bound(body, "channel_id", self._channel_id)
            _match_bound(body, "root_thread_ts", self._root_thread_ts)
            key = _slack_key(body)
            if operation == "post_root":
                text = body.get("text")
                if type(text) is not str or not text.strip():
                    job_id = body.get("job_id")
                    if type(job_id) is not str or not job_id.strip():
                        raise RequestPortMismatch("message text or job_id is required")
                    text = job_id
                write_claim_key = f"slack:post_root:{key}"
                write_claim, write_claim_owner = self._reserve_write_claim(
                    write_claim_key
                )
                _client_method(self._client, "chat_postMessage")
            elif operation == "lookup":
                _client_method(self._client, "conversations_replies")
            else:
                raise RequestPortError(f"unsupported slack operation: {operation}")

            if operation != "post_root" or write_claim_owner:
                secret_value = self._resolve_credential(
                    deadline=deadline,
                    cancel_event=cancel_event,
                )
            if operation == "post_root":
                client_invoked = write_claim_owner
                result = self._run_sync(
                    write_claim_key,
                    lambda: _invoke(
                        self._client,
                        "chat_postMessage",
                        channel=self._channel_id,
                        thread_ts=self._root_thread_ts,
                        client_msg_id=key,
                        text=text,
                        _secret_value=secret_value,
                    ),
                    deadline=deadline,
                    cancel_event=cancel_event,
                    sanitize_result=lambda value: _sanitize_snapshot(
                        value, secret_value
                    ),
                    reserved_claim=write_claim,
                    reserved_owner=write_claim_owner,
                )
            elif operation == "lookup":
                client_invoked = True
                raw = self._run_sync(
                    f"slack:lookup:{key}",
                    lambda: _invoke(
                        self._client,
                        "conversations_replies",
                        channel=self._channel_id,
                        ts=self._root_thread_ts,
                        _secret_value=secret_value,
                    ),
                    idempotent=False,
                    deadline=deadline,
                    cancel_event=cancel_event,
                    sanitize_result=lambda value: _sanitize_snapshot(
                        value, secret_value
                    ),
                )
                result = _filter_slack_lookup(raw, key)
            else:
                raise RequestPortError(f"unsupported slack operation: {operation}")
            self._record_receipt(
                operation=operation,
                secret_ref=secret_ref,
                payload=payload,
                outcome="ok",
                client_invoked=True,
                response=result,
                secret_value=secret_value,
            )
            return _sanitize_snapshot(result, secret_value)
        except (LaneClosedError, RequestPortError, Exception) as exc:
            self._record_receipt(
                operation=operation,
                secret_ref=secret_ref,
                payload=payload,
                outcome="error",
                client_invoked=client_invoked,
                error=exc,
                secret_value=secret_value,
            )
            raise
        finally:
            self._release_unstarted_write_claim(
                write_claim_key, write_claim, write_claim_owner
            )
            if admitted:
                self._release_public_call()

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"secret_ref={self._secret_ref!r}, "
            f"workspace_id={self._workspace_id!r}, "
            f"channel_id={self._channel_id!r}, "
            f"root_thread_ts={self._root_thread_ts!r})"
        )
