"""Explicitly injected Cursor Cloud and Slack request-port adapters.

These wrap an already-constructed client that matches the repository's
existing seams. They never construct HTTP/SDK clients, never read secret
*values* into logs, and never mint a client from config flags.

Credential resolution, when provided, is a request-time dependency only.
Attach/preflight must not invoke it.
"""

from __future__ import annotations

import inspect
import re
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
    missing = [name for name in methods if not callable(getattr(client, name, None))]
    if missing:
        raise TypeError(
            "injected client is missing required seam methods: "
            + ", ".join(missing)
        )
    return client


def _as_payload(payload: Any) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        raise RequestPortMismatch("payload must be a mapping")
    return payload


def _match_bound(payload: Mapping[str, Any], key: str, bound: str) -> None:
    if key not in payload:
        return
    value = payload.get(key)
    if type(value) is not str or value.strip() != bound:
        raise RequestPortMismatch(f"{key} does not match bound identity")


def _check_deadline(
    *,
    timeout_seconds: Any = None,
    cancel_event: Any = None,
) -> None:
    if cancel_event is not None and getattr(cancel_event, "is_set", lambda: False)():
        raise RequestPortTimeout("cancelled")
    if timeout_seconds is None:
        return
    try:
        seconds = float(timeout_seconds)
    except (TypeError, ValueError) as exc:
        raise RequestPortTimeout("invalid timeout") from exc
    if seconds <= 0:
        raise RequestPortTimeout("timeout")


def _unwrap_response(result: Any) -> Any:
    json_fn = getattr(result, "json", None)
    if not callable(json_fn) or isinstance(result, (dict, list, tuple, str, bytes)):
        return result
    try:
        unwrapped = json_fn()
    except Exception:
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


def _invoke(client: Any, method_name: str, *args: Any, **kwargs: Any) -> Any:
    method = _client_method(client, method_name)
    try:
        result = method(*args, **kwargs)
    except (LaneClosedError, RequestPortError):
        raise
    except Exception as exc:
        raise RequestPortError(_redact(exc)) from None
    if inspect.iscoroutine(result):
        result.close()
        raise RequestPortError(
            "async client methods are not invoked from request ports"
        )
    return _unwrap_response(result)


def _resolve_secret(
    resolver: Optional[Callable[[str], Any]], secret_ref: str
) -> None:
    if resolver is None:
        return
    try:
        value = resolver(secret_ref)
    except RequestPortError:
        raise
    except Exception as exc:
        raise RequestPortError(_redact(exc)) from None
    if value is None or (isinstance(value, str) and not value.strip()):
        raise RequestPortError("credential resolver returned empty")
    del value


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
        if isinstance(messages, list):
            matched = [
                item for item in messages if _message_client_msg_id(item) == expected
            ]
            out = dict(raw)
            out["messages"] = matched
            return out
        if _message_client_msg_id(raw) == expected:
            return dict(raw)
        return {"ok": True, "messages": []}
    if isinstance(raw, list):
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
        self._receipts: list[dict[str, Any]] = []

    def close(self) -> None:
        self._closed = True

    @property
    def receipts(self) -> tuple[dict[str, Any], ...]:
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
    ) -> None:
        try:
            offered = secret_ref if type(secret_ref) is str else ""
            receipt: dict[str, Any] = {
                "operation": operation if type(operation) is str else "",
                "secret_ref": offered,
                "payload": _receipt_value(payload),
                "outcome": outcome,
                "client_invoked": bool(client_invoked),
            }
            if error is not None:
                receipt["error"] = _redact(error)
            elif response is not None:
                receipt["response"] = _receipt_value(response)
            self._receipts.append(receipt)
        except Exception:
            return

    def _prepare(
        self,
        *,
        secret_ref: str,
        payload: Any,
        timeout_seconds: Any = None,
        cancel_event: Any = None,
    ) -> Mapping[str, Any]:
        if self._closed:
            raise RequestPortClosed("request port is closed")
        _check_deadline(timeout_seconds=timeout_seconds, cancel_event=cancel_event)
        try:
            offered = validate_secret_ref_name(secret_ref, field="secret_ref")
        except Exception as exc:
            raise RequestPortMismatch(_redact(exc)) from None
        if offered != self._secret_ref:
            raise RequestPortMismatch("secret_ref does not match bound reference")
        body = _as_payload(payload)
        _match_bound(body, "workspace_id", self._workspace_id)
        _match_bound(body, "repository_identity", self._repository_identity)
        _resolve_secret(self._credential_resolver, offered)
        return body

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
        try:
            body = self._prepare(
                secret_ref=secret_ref,
                payload=payload,
                timeout_seconds=timeout_seconds,
                cancel_event=cancel_event,
            )
            if operation == "create":
                key = _cursor_key(body)
                name, agent_id = _verify_cursor_correlation(body, key)
                _client_method(self._client, "create_agent")
                client_invoked = True
                result = _invoke(
                    self._client,
                    "create_agent",
                    {"name": name, "agentId": agent_id},
                )
            elif operation == "lookup":
                key = _cursor_key(body)
                _verify_cursor_correlation(body, key)
                _client_method(self._client, "get_agent")
                client_invoked = True
                result = _invoke(
                    self._client,
                    "get_agent",
                    cursor_correlation_agent_id(key),
                )
            elif operation == "status":
                agent_id = body.get("agent_id") or body.get("agentId")
                run_id = body.get("run_id")
                if type(agent_id) is not str or not agent_id.strip():
                    raise RequestPortMismatch("agent_id is required")
                if type(run_id) is not str or not run_id.strip():
                    raise RequestPortMismatch("run_id is required")
                _client_method(self._client, "get_run")
                client_invoked = True
                result = _invoke(
                    self._client, "get_run", agent_id.strip(), run_id.strip()
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
            )
            return result
        except BaseException as exc:
            self._record_receipt(
                operation=operation,
                secret_ref=secret_ref,
                payload=payload,
                outcome="error",
                client_invoked=client_invoked,
                error=exc,
            )
            raise


class SlackInjectedRequestPort(_InjectedRequestPortBase):
    """Wraps an injected Slack client (``chat_postMessage`` / ``conversations_replies``)."""

    def __init__(
        self,
        *,
        client: Any,
        secret_ref: str,
        workspace_id: str,
        channel_id: str,
        repository_identity: str = "",
        root_thread_ts: str = "",
        credential_resolver: Optional[Callable[[str], Any]] = None,
    ) -> None:
        super().__init__(
            client=client,
            secret_ref=secret_ref,
            workspace_id=workspace_id,
            repository_identity=repository_identity or workspace_id,
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
        try:
            body = self._prepare(
                secret_ref=secret_ref,
                payload=payload,
                timeout_seconds=timeout_seconds,
                cancel_event=cancel_event,
            )
            _match_bound(body, "channel_id", self._channel_id)
            _match_bound(body, "root_thread_ts", self._root_thread_ts)
            key = _slack_key(body)
            if operation == "post_root":
                text = body.get("text")
                if type(text) is not str or not text:
                    job_id = body.get("job_id")
                    text = job_id if type(job_id) is str else ""
                _client_method(self._client, "chat_postMessage")
                client_invoked = True
                result = _invoke(
                    self._client,
                    "chat_postMessage",
                    channel=self._channel_id,
                    thread_ts=self._root_thread_ts,
                    client_msg_id=key,
                    text=text,
                )
            elif operation == "lookup":
                _client_method(self._client, "conversations_replies")
                client_invoked = True
                raw = _invoke(
                    self._client,
                    "conversations_replies",
                    channel=self._channel_id,
                    ts=self._root_thread_ts,
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
            )
            return result
        except BaseException as exc:
            self._record_receipt(
                operation=operation,
                secret_ref=secret_ref,
                payload=payload,
                outcome="error",
                client_invoked=client_invoked,
                error=exc,
            )
            raise

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"secret_ref={self._secret_ref!r}, "
            f"workspace_id={self._workspace_id!r}, "
            f"channel_id={self._channel_id!r}, "
            f"root_thread_ts={self._root_thread_ts!r})"
        )
