"""Request-owned, default-off Linear MCP handoff projection.

The owner deliberately does not discover MCP configuration or use the global MCP
registry.  Its caller is injected for one approved request, which keeps ordinary
agent construction and disabled compositions free of MCP and network effects.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any, Protocol, runtime_checkable

from agent.orchestration.production_handoff_composition import (
    LiveAdapterUnavailable,
    LiveEffectAuthority,
    ProductionCompositionDisabled,
    ProductionRequestAuthority,
)


_ISSUE_RE = re.compile(r"[A-Z][A-Z0-9]*-[1-9][0-9]*")
_KEY_RE = re.compile(r"[a-f0-9]{64}")
_SCHEMA = "hermes.linear-mcp-handoff-projection.v1"
_MARKER_START = "<!-- hermes:linear-mcp-handoff-projection:v1 -->"
_MARKER_END = "<!-- /hermes:linear-mcp-handoff-projection:v1 -->"
_COMMENT_LIMIT = 250
_MAX_COMMENT_PAGES = 100
_MAX_RESULT_BYTES = 2 * 1024 * 1024


@dataclass(frozen=True)
class LinearMCPProjectionConfig:
    """Strict opt-in gate for the live projection owner."""

    enabled: bool = False
    mode: str = "off"

    def __post_init__(self) -> None:
        if type(self.enabled) is not bool:
            raise LiveAdapterUnavailable("Linear MCP enabled must be a literal bool")
        if (self.enabled, self.mode) not in {(False, "off"), (True, "live")}:
            raise LiveAdapterUnavailable(
                "Linear MCP projection requires disabled/off or enabled/live"
            )


@runtime_checkable
class RequestBoundMCPCaller(Protocol):
    """Narrow request client supplied by the product ingress."""

    def call_tool(
        self,
        *,
        request_id: str,
        session_id: str,
        server_name: str,
        tool_name: str,
        arguments: dict[str, Any],
    ) -> Any: ...


@dataclass(frozen=True)
class LinearMCPProjectionBinding:
    """Complete opt-in product binding for one agent request."""

    config: LinearMCPProjectionConfig
    authority: ProductionRequestAuthority
    live_authority: LiveEffectAuthority


class LinearMCPProjectionOwner:
    """Own one Linear projection for exactly one authorized request lifecycle."""

    def __init__(
        self,
        config: LinearMCPProjectionConfig = LinearMCPProjectionConfig(),
        *,
        authority: ProductionRequestAuthority | None = None,
        live_authority: LiveEffectAuthority | None = None,
        caller: RequestBoundMCPCaller | None = None,
    ) -> None:
        self._config = config
        self._authority = authority
        self._live_authority = live_authority
        self._caller = caller
        self._started = False
        self._stopped = False

    @property
    def started(self) -> bool:
        return self._started

    def start(self) -> None:
        if self._stopped:
            raise LiveAdapterUnavailable("a stopped Linear MCP owner cannot restart")
        if self._started:
            raise LiveAdapterUnavailable("Linear MCP owner is already started")
        if not isinstance(self._config, LinearMCPProjectionConfig):
            raise LiveAdapterUnavailable("Linear MCP projection config is required")
        if self._config.enabled is not True or self._config.mode != "live":
            raise LiveAdapterUnavailable("Linear MCP projection is default-off")
        request = self._authority
        live = self._live_authority
        if not isinstance(request, ProductionRequestAuthority):
            raise LiveAdapterUnavailable("request-bound production authority is required")
        self._require_request_active(request)
        if (
            not isinstance(live, LiveEffectAuthority)
            or live.approved is not True
            or live.request_id != request.request_id
            or live.session_id != request.session_id
        ):
            raise LiveAdapterUnavailable(
                "Linear MCP activation is not bound to this request"
            )
        if not isinstance(self._caller, RequestBoundMCPCaller):
            raise LiveAdapterUnavailable("a request-bound MCP caller is required")
        self._started = True

    def shutdown(self) -> None:
        self._started = False
        self._stopped = True

    def _call(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        request = self._require_active()
        assert self._caller is not None
        result = self._caller.call_tool(
            request_id=request.request_id,
            session_id=request.session_id,
            server_name="linear",
            tool_name=tool_name,
            arguments=arguments,
        )
        text: Any = None
        if type(result) is str:
            if not result or len(result.encode()) > _MAX_RESULT_BYTES:
                raise ValueError("Linear MCP result JSON is empty or too large")
            try:
                envelope = json.loads(result)
            except json.JSONDecodeError as exc:
                raise ValueError("Linear MCP result is not valid JSON") from exc
            if type(envelope) is not dict or set(envelope) != {"result"}:
                raise ValueError("Linear MCP result must contain only a result field")
            text = envelope["result"]
        elif type(result) is dict:
            if set(result) - {"content", "isError"}:
                raise ValueError("Linear MCP result must be a plain MCP result envelope")
            if result.get("isError", False) is not False:
                raise ValueError("Linear MCP tool returned an error result")
            content = result.get("content")
            if type(content) is not list or len(content) != 1:
                raise ValueError("Linear MCP result requires exactly one text block")
            block = content[0]
            if type(block) is not dict or set(block) != {"type", "text"}:
                raise ValueError("Linear MCP result contains an invalid text block")
            text = block.get("text")
            if block.get("type") != "text" or type(text) is not str:
                raise ValueError("Linear MCP result contains an invalid text block")
        else:
            raise ValueError("Linear MCP result must be a JSON string or content envelope")
        if type(text) is not str:
            raise ValueError("Linear MCP result field must contain provider JSON text")
        if not text or len(text.encode()) > _MAX_RESULT_BYTES:
            raise ValueError("Linear MCP result JSON is empty or too large")
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("Linear MCP result text is not valid JSON") from exc
        if type(payload) is not dict or any(type(key) is not str for key in payload):
            raise ValueError("Linear MCP result JSON must be a text-keyed object")
        return payload

    def _require_active(self) -> ProductionRequestAuthority:
        if not self._started or self._stopped:
            raise LiveAdapterUnavailable("Linear MCP projection owner is not started")
        request = self._authority
        if not isinstance(request, ProductionRequestAuthority):
            raise LiveAdapterUnavailable("request-bound production authority is required")
        self._require_request_active(request)
        live = self._live_authority
        if (
            not isinstance(live, LiveEffectAuthority)
            or live.approved is not True
            or live.request_id != request.request_id
            or live.session_id != request.session_id
        ):
            raise LiveAdapterUnavailable("Linear MCP request authority was lost")
        return request

    @staticmethod
    def _require_request_active(request: ProductionRequestAuthority) -> None:
        try:
            request.require_active()
        except ProductionCompositionDisabled as exc:
            raise LiveAdapterUnavailable("request authority is not active") from exc

    @staticmethod
    def _validate_issue(issue: Any) -> str:
        if type(issue) is not str or _ISSUE_RE.fullmatch(issue) is None:
            raise ValueError("a safe Linear issue identifier is required")
        return issue

    @staticmethod
    def _document(canonical: Any, idempotency_key: Any) -> str:
        if type(canonical) is not str or not canonical or len(canonical.encode()) > 4096:
            raise ValueError("canonical projection must be nonempty bounded text")
        if type(idempotency_key) is not str or _KEY_RE.fullmatch(idempotency_key) is None:
            raise ValueError("a SHA-256 idempotency key is required")
        payload = json.dumps(
            {"canonical": canonical, "idempotency_key": idempotency_key, "schema": _SCHEMA},
            sort_keys=True,
            separators=(",", ":"),
        )
        return f"{_MARKER_START}\n{payload}\n{_MARKER_END}"

    @staticmethod
    def _comment_bodies(result: dict[str, Any]) -> list[str]:
        comments = result.get("comments")
        if type(comments) is not list:
            raise ValueError("Linear MCP readback requires a comments list")
        bodies: list[str] = []
        for comment in comments:
            if type(comment) is not dict:
                raise ValueError("Linear MCP readback contains an invalid comment")
            body = comment.get("body")
            if type(body) is not str:
                raise ValueError("Linear MCP readback contains an invalid comment body")
            bodies.append(body)
        return bodies

    @classmethod
    def _projection_from_comment(cls, body: str) -> dict[str, str] | None:
        prefix = f"{_MARKER_START}\n"
        suffix = f"\n{_MARKER_END}"
        if not body.startswith(prefix) or not body.endswith(suffix):
            return None
        try:
            value = json.loads(body[len(prefix) : -len(suffix)])
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("Linear MCP comment has a malformed projection envelope") from exc
        if type(value) is not dict or set(value) != {"canonical", "idempotency_key", "schema"}:
            raise ValueError("Linear MCP comment has an invalid projection schema")
        if (
            value.get("schema") != _SCHEMA
            or type(value.get("canonical")) is not str
            or not value.get("canonical")
            or type(value.get("idempotency_key")) is not str
            or _KEY_RE.fullmatch(value["idempotency_key"]) is None
        ):
            raise ValueError("Linear MCP comment has an invalid projection schema")
        return value

    def _list_projections(self, issue: str) -> list[dict[str, str]]:
        projections = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        for _ in range(_MAX_COMMENT_PAGES):
            arguments: dict[str, Any] = {
                "issueId": issue,
                "limit": _COMMENT_LIMIT,
                "orderBy": "createdAt",
            }
            if cursor is not None:
                arguments["cursor"] = cursor
            result = self._call("list_comments", arguments)
            if set(result) != {"comments", "hasNextPage", "cursor"}:
                raise ValueError("Linear MCP comments page has an invalid schema")
            for body in self._comment_bodies(result):
                projection = self._projection_from_comment(body)
                if projection is not None:
                    projections.append(projection)
            has_next_page = result.get("hasNextPage")
            next_cursor = result.get("cursor")
            if type(has_next_page) is not bool:
                raise ValueError("Linear MCP comments page has invalid pagination")
            if has_next_page is False:
                if next_cursor is not None and type(next_cursor) is not str:
                    raise ValueError("Linear MCP comments page has invalid pagination")
                return projections
            if (
                type(next_cursor) is not str
                or not next_cursor
                or len(next_cursor.encode()) > 4096
                or next_cursor in seen_cursors
            ):
                raise ValueError("Linear MCP comments page has invalid pagination")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        raise ValueError("Linear MCP comments pagination exceeded its bound")

    def upsert_handoff(
        self, *, issue: str, canonical: str, idempotency_key: str
    ) -> str:
        issue = self._validate_issue(issue)
        document = self._document(canonical, idempotency_key)
        for projection in self._list_projections(issue):
            if projection["idempotency_key"] != idempotency_key:
                continue
            if projection["canonical"] == canonical:
                return idempotency_key
            raise ValueError("idempotency key is already bound to different bytes")
        self._call("save_comment", {"issueId": issue, "body": document})
        projections = self._list_projections(issue)
        if not any(
            projection["idempotency_key"] == idempotency_key
            and projection["canonical"] == canonical
            for projection in projections
        ):
            raise ValueError("Linear MCP authoritative readback did not match")
        return idempotency_key

    def read_handoff(self, *, issue: str) -> str:
        issue = self._validate_issue(issue)
        projections = self._list_projections(issue)
        if not projections:
            raise ValueError("Linear MCP readback has no handoff projection comment")
        return projections[-1]["canonical"]
