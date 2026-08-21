"""Production-shaped injected transports for Cursor Cloud and Slack.

Transports never construct an HTTP/SDK client and never read credential
values. Callers inject a request callable and a secret *reference name*.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Optional, Protocol

from agent.durable_jobs.config import validate_secret_ref_name
from agent.durable_jobs.cursor_cloud import (
    cursor_correlation_agent_id,
    cursor_correlation_name,
)
from agent.durable_jobs.cursor_cloud import redact_provider_error
from agent.durable_jobs.lane import LaneClosedError
from agent.durable_jobs.redaction import redact_secret_text
from agent.durable_jobs.request_ports import (
    CursorCloudInjectedRequestPort,
    RequestPortError,
    SlackInjectedRequestPort,
)
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


def _invoke_injected_request(request: InjectedRequest, **kwargs: Any) -> Any:
    try:
        return request(**kwargs)
    except (LaneClosedError, RequestPortError):
        raise
    except Exception as exc:
        raise RuntimeError(redact_transport_error(exc)) from None


def _bound_request_identity(request: InjectedRequest, name: str) -> str:
    """Read bindings without invoking request properties or descriptors."""
    if type(request) not in (
        CursorCloudInjectedRequestPort,
        SlackInjectedRequestPort,
    ):
        return ""
    namespace = object.__getattribute__(request, "__dict__")
    value = dict.get(namespace, name)
    return value if type(value) is str else ""


class CursorCloudInjectedTransport:
    """Cursor Cloud transport coupling. Request callable is required."""

    def __init__(
        self,
        *,
        request: Optional[InjectedRequest] = None,
        secret_ref: str,
        workspace_id: str = "",
        repository_identity: str = "",
    ) -> None:
        if request is None:
            raise TypeError(
                "CursorCloudInjectedTransport requires an injected request "
                "callable; no HTTP client is constructed"
            )
        self._request = request
        self._secret_ref = validate_secret_ref_name(secret_ref, field="secret_ref")
        self._workspace_id = workspace_id or _bound_request_identity(
            request, "_workspace_id"
        )
        self._repository_identity = repository_identity or _bound_request_identity(
            request, "_repository_identity"
        )

    @property
    def secret_ref(self) -> str:
        return self._secret_ref

    def can_resolve_secret_ref(self) -> bool:
        """True when an injected request callable is bound to this secret ref.

        Never reads environment values or returns credential material.
        """
        return callable(self._request) and bool(self._secret_ref)

    def create(
        self, *, idempotency_key: str, job_id: str, name: str, agent_id: str
    ) -> Any:
        return _invoke_injected_request(
            self._request,
            operation="create",
            secret_ref=self._secret_ref,
            payload={
                "idempotency_key": idempotency_key,
                "job_id": job_id,
                "name": name,
                "agentId": agent_id,
                "workspace_id": self._workspace_id,
                "repository_identity": self._repository_identity,
            },
        )

    def lookup(self, *, idempotency_key: str) -> Any:
        return _invoke_injected_request(
            self._request,
            operation="lookup",
            secret_ref=self._secret_ref,
            payload={
                "idempotency_key": idempotency_key,
                "workspace_id": self._workspace_id,
                "repository_identity": self._repository_identity,
            },
        )

    def status(
        self, *, run_id: str, idempotency_key: str, agent_id: str = ""
    ) -> Any:
        cursor_correlation_name(idempotency_key)
        derived_agent_id = cursor_correlation_agent_id(idempotency_key)
        if agent_id and agent_id != derived_agent_id:
            raise ValueError("Cursor status agent id does not match idempotency key")
        return _invoke_injected_request(
            self._request,
            operation="status",
            secret_ref=self._secret_ref,
            payload={
                "idempotency_key": idempotency_key,
                "run_id": run_id,
                "agent_id": derived_agent_id,
                "workspace_id": self._workspace_id,
                "repository_identity": self._repository_identity,
            },
        )


class SlackInjectedTransport:
    """Slack transport coupling. Request callable is required."""

    def __init__(
        self,
        *,
        request: Optional[InjectedRequest] = None,
        secret_ref: str,
        workspace_id: str = "",
        repository_identity: str = "",
        channel_id: str = "",
        root_thread_ts: str = "",
    ) -> None:
        if request is None:
            raise TypeError(
                "SlackInjectedTransport requires an injected request "
                "callable; no Slack SDK client is constructed"
            )
        self._request = request
        self._secret_ref = validate_secret_ref_name(secret_ref, field="secret_ref")
        self._workspace_id = workspace_id or _bound_request_identity(
            request, "_workspace_id"
        )
        self._repository_identity = repository_identity or _bound_request_identity(
            request, "_repository_identity"
        )
        self._channel_id = channel_id or _bound_request_identity(request, "_channel_id")
        self._root_thread_ts = root_thread_ts or _bound_request_identity(
            request, "_root_thread_ts"
        )

    @property
    def secret_ref(self) -> str:
        return self._secret_ref

    def can_resolve_secret_ref(self) -> bool:
        """True when an injected request callable is bound to this secret ref.

        Never reads environment values or returns credential material.
        """
        return callable(self._request) and bool(self._secret_ref)

    def post_root(
        self,
        *,
        client_msg_id: str,
        workspace_id: str,
        channel_id: str,
        root_thread_ts: str,
        job_id: str,
    ) -> Any:
        return _invoke_injected_request(
            self._request,
            operation="post_root",
            secret_ref=self._secret_ref,
            payload={
                "client_msg_id": client_msg_id,
                "workspace_id": workspace_id,
                "repository_identity": self._repository_identity,
                "channel_id": channel_id,
                "root_thread_ts": root_thread_ts,
                "job_id": job_id,
            },
        )

    def lookup_by_client_msg_id(self, client_msg_id: str) -> Any:
        return _invoke_injected_request(
            self._request,
            operation="lookup",
            secret_ref=self._secret_ref,
            payload={
                "client_msg_id": client_msg_id,
                "workspace_id": self._workspace_id,
                "repository_identity": self._repository_identity,
                "channel_id": self._channel_id,
                "root_thread_ts": self._root_thread_ts,
            },
        )
