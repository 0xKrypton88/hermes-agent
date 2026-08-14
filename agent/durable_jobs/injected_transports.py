"""Production-shaped injected transports for Cursor Cloud and Slack.

Transports never construct an HTTP/SDK client and never read credential
values. Callers inject a request callable and a secret *reference name*.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Optional, Protocol

from agent.durable_jobs.cursor_cloud import redact_provider_error
from agent.durable_jobs.redaction import redact_secret_text
from agent.durable_jobs.slack_bridge import redact_slack_error

InjectedRequest = Callable[..., Any]


class InjectedRequestPort(Protocol):
    def __call__(
        self, *, operation: str, secret_ref: str, payload: dict[str, Any]
    ) -> Any: ...


_XOX_RE = re.compile(r"(?i)xox[abps]-[A-Za-z0-9-]+")


def redact_transport_error(exc: BaseException | str) -> str:
    text = redact_slack_error(exc)
    text = redact_provider_error(text)
    text = redact_secret_text(text)
    return _XOX_RE.sub("[REDACTED]", text)


class CursorCloudInjectedTransport:
    """Cursor Cloud transport coupling. Request callable is required."""

    def __init__(
        self,
        *,
        request: Optional[InjectedRequest] = None,
        secret_ref: str,
    ) -> None:
        if request is None:
            raise TypeError(
                "CursorCloudInjectedTransport requires an injected request "
                "callable; no HTTP client is constructed"
            )
        if not str(secret_ref or "").strip():
            raise TypeError("cursor secret_ref name is required")
        self._request = request
        self._secret_ref = str(secret_ref).strip()

    def create(
        self, *, idempotency_key: str, job_id: str, name: str, agent_id: str
    ) -> Any:
        try:
            return self._request(
                operation="create",
                secret_ref=self._secret_ref,
                payload={
                    "idempotency_key": idempotency_key,
                    "job_id": job_id,
                    "name": name,
                    "agentId": agent_id,
                },
            )
        except Exception as exc:
            raise RuntimeError(redact_transport_error(exc)) from None

    def lookup(self, *, idempotency_key: str) -> Any:
        try:
            return self._request(
                operation="lookup",
                secret_ref=self._secret_ref,
                payload={"idempotency_key": idempotency_key},
            )
        except Exception as exc:
            raise RuntimeError(redact_transport_error(exc)) from None

    def status(self, *, run_id: str, agent_id: str = "") -> Any:
        try:
            return self._request(
                operation="status",
                secret_ref=self._secret_ref,
                payload={"run_id": run_id, "agent_id": agent_id},
            )
        except Exception as exc:
            raise RuntimeError(redact_transport_error(exc)) from None


class SlackInjectedTransport:
    """Slack transport coupling. Request callable is required."""

    def __init__(
        self,
        *,
        request: Optional[InjectedRequest] = None,
        secret_ref: str,
    ) -> None:
        if request is None:
            raise TypeError(
                "SlackInjectedTransport requires an injected request "
                "callable; no Slack SDK client is constructed"
            )
        if not str(secret_ref or "").strip():
            raise TypeError("slack secret_ref name is required")
        self._request = request
        self._secret_ref = str(secret_ref).strip()

    def post_root(
        self,
        *,
        client_msg_id: str,
        workspace_id: str,
        channel_id: str,
        root_thread_ts: str,
        job_id: str,
    ) -> Any:
        try:
            return self._request(
                operation="post_root",
                secret_ref=self._secret_ref,
                payload={
                    "client_msg_id": client_msg_id,
                    "workspace_id": workspace_id,
                    "channel_id": channel_id,
                    "root_thread_ts": root_thread_ts,
                    "job_id": job_id,
                },
            )
        except Exception as exc:
            raise RuntimeError(redact_transport_error(exc)) from None

    def lookup_by_client_msg_id(self, client_msg_id: str) -> Any:
        try:
            return self._request(
                operation="lookup",
                secret_ref=self._secret_ref,
                payload={"client_msg_id": client_msg_id},
            )
        except Exception as exc:
            raise RuntimeError(redact_transport_error(exc)) from None
