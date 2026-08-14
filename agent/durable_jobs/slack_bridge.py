"""ENG-31 inactive/fail-closed Slack client bridge behind SlackMessagePort.

Production-shaped types and an injected-transport seam. No live HTTP client,
Slack SDK, gateway adapter, or token handling is constructed. Dispatch remains
hard-disabled (``config.dispatch_allowed`` is always False). Job/thread
identity stays in the existing Slack binding ledger — this module does not
keep a second message map.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, Protocol, Sequence

from agent.durable_jobs.adapters import NullSlackPort
from agent.durable_jobs.config import DurableJobsConfig
from agent.durable_jobs.redaction import redact_secret_text


class SlackPostKind(str, Enum):
    ACCEPTED = "accepted"
    LOST_RESPONSE = "lost_response"
    AMBIGUOUS_RESPONSE = "ambiguous_response"
    UNKNOWN = "unknown"


class SlackLookupKind(str, Enum):
    UNIQUE = "unique"
    EMPTY = "empty"
    AMBIGUOUS = "ambiguous"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class SlackPosted:
    message_ts: str
    client_msg_id: str
    workspace_id: str = ""
    channel_id: str = ""
    root_thread_ts: str = ""
    job_id: str = ""


@dataclass(frozen=True)
class SlackPostResult:
    kind: SlackPostKind
    message_ts: Optional[str] = None
    posted: Optional[SlackPosted] = None
    candidates: tuple[SlackPosted, ...] = ()
    error: Optional[str] = None


@dataclass(frozen=True)
class SlackLookupResult:
    kind: SlackLookupKind
    posted: Optional[SlackPosted] = None
    candidates: tuple[SlackPosted, ...] = ()
    error: Optional[str] = None


class SlackTransport(Protocol):
    """Injected transport. Tests supply an in-memory fake; no default client."""

    def post_root(
        self,
        *,
        client_msg_id: str,
        workspace_id: str,
        channel_id: str,
        root_thread_ts: str,
        job_id: str,
    ) -> Any: ...

    def lookup_by_client_msg_id(self, client_msg_id: str) -> Any: ...


def _record_get(raw: Any, key: str, default: Any = None) -> Any:
    if isinstance(raw, dict):
        return raw.get(key, default)
    if raw is None or isinstance(raw, (str, bytes, int, float, bool)):
        return default
    return getattr(raw, key, default)


def _text(raw: Any) -> str:
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return ""


def _channel_id(raw: Any) -> str:
    for key in ("channel_id", "channel"):
        value = _record_get(raw, key)
        if isinstance(value, dict):
            found = _text(value.get("id"))
            if found:
                return found
        found = _text(value)
        if found:
            return found
    return ""


def _workspace_id(raw: Any) -> str:
    for key in ("workspace_id", "team_id", "team"):
        value = _record_get(raw, key)
        if isinstance(value, dict):
            found = _text(value.get("id"))
            if found:
                return found
        found = _text(value)
        if found:
            return found
    return ""


def _message_ts(raw: Any) -> str:
    return _text(_record_get(raw, "message_ts")) or _text(_record_get(raw, "ts"))


def _client_msg_id(raw: Any) -> str:
    return _text(_record_get(raw, "client_msg_id"))


def _root_thread_ts(raw: Any) -> str:
    return _text(_record_get(raw, "root_thread_ts")) or _text(
        _record_get(raw, "thread_ts")
    )


def _job_id(raw: Any) -> str:
    return _text(_record_get(raw, "job_id"))


def _parse_posted(raw: Any) -> Optional[SlackPosted]:
    if raw is None:
        return None
    if isinstance(raw, SlackPosted):
        if not raw.message_ts:
            return None
        return raw
    if isinstance(raw, (str, bytes, int, float, bool)):
        return None
    inner = _record_get(raw, "message")
    ts = _message_ts(raw) or (_message_ts(inner) if inner is not None else "")
    if not ts:
        return None
    cmid = _client_msg_id(raw) or (
        _client_msg_id(inner) if inner is not None else ""
    )
    return SlackPosted(
        message_ts=ts,
        client_msg_id=cmid,
        workspace_id=_workspace_id(raw)
        or (_workspace_id(inner) if inner is not None else ""),
        channel_id=_channel_id(raw) or (_channel_id(inner) if inner is not None else ""),
        root_thread_ts=_root_thread_ts(raw)
        or (_root_thread_ts(inner) if inner is not None else ""),
        job_id=_job_id(raw) or (_job_id(inner) if inner is not None else ""),
    )


def _lookup_records(raw: Any) -> Sequence[Any]:
    if raw is None:
        return ()
    if isinstance(raw, (list, tuple)):
        return raw
    if isinstance(raw, SlackPosted):
        return (raw,)
    if isinstance(raw, dict):
        messages = raw.get("messages")
        if isinstance(messages, (list, tuple)):
            return messages
        if isinstance(messages, dict):
            matches = messages.get("matches")
            if isinstance(matches, (list, tuple)):
                return matches
        items = raw.get("items")
        if isinstance(items, (list, tuple)):
            return items
        if raw.get("message") is not None or _message_ts(raw) or _client_msg_id(raw):
            return (raw,)
        return ()
    return (raw,)


def _kind_from(raw: Any) -> Optional[SlackPostKind]:
    if isinstance(raw, SlackPostKind):
        return raw
    if raw is None:
        return None
    text = str(raw).strip().lower()
    try:
        return SlackPostKind(text)
    except ValueError:
        return None


def _identity_matches(
    posted: SlackPosted,
    *,
    client_msg_id: str,
    workspace_id: str,
    channel_id: str,
    root_thread_ts: str,
    job_id: str,
) -> bool:
    if posted.client_msg_id and posted.client_msg_id != client_msg_id:
        return False
    if posted.workspace_id and posted.workspace_id != workspace_id:
        return False
    if posted.channel_id and posted.channel_id != channel_id:
        return False
    if posted.root_thread_ts and posted.root_thread_ts != root_thread_ts:
        return False
    if posted.job_id and posted.job_id != job_id:
        return False
    return True


def parse_lookup_posts(
    raw: Any, *, expected_client_msg_id: str = ""
) -> list[SlackPosted]:
    """Well-formed posts only. Foreign client_msg_id values are not returned."""
    posts: list[SlackPosted] = []
    expected = str(expected_client_msg_id or "").strip()
    for item in _lookup_records(raw):
        parsed = _parse_posted(item)
        if parsed is None:
            continue
        if expected:
            if not parsed.client_msg_id:
                continue
            if parsed.client_msg_id != expected:
                continue
        posts.append(parsed)
    return posts


def classify_lookup(
    posts: Sequence[SlackPosted], *, expected_client_msg_id: str
) -> SlackLookupResult:
    usable = parse_lookup_posts(list(posts), expected_client_msg_id=expected_client_msg_id)
    if len(usable) == 1:
        return SlackLookupResult(kind=SlackLookupKind.UNIQUE, posted=usable[0])
    if len(usable) == 0:
        return SlackLookupResult(kind=SlackLookupKind.EMPTY)
    return SlackLookupResult(
        kind=SlackLookupKind.AMBIGUOUS, candidates=tuple(usable)
    )


def normalize_post_result(
    raw: Any,
    *,
    client_msg_id: str,
    workspace_id: str,
    channel_id: str,
    root_thread_ts: str,
    job_id: str,
) -> SlackPostResult:
    """Map a transport payload to a typed post result. Unknown shapes fail closed."""
    if isinstance(raw, SlackPostResult):
        return _fail_closed_post(
            raw,
            client_msg_id=client_msg_id,
            workspace_id=workspace_id,
            channel_id=channel_id,
            root_thread_ts=root_thread_ts,
            job_id=job_id,
        )
    if raw is None:
        return SlackPostResult(kind=SlackPostKind.LOST_RESPONSE)
    if isinstance(raw, (list, tuple)):
        posts = parse_lookup_posts(raw, expected_client_msg_id=client_msg_id)
        if len(posts) == 1:
            return _accepted_if_bound(
                posts[0],
                client_msg_id=client_msg_id,
                workspace_id=workspace_id,
                channel_id=channel_id,
                root_thread_ts=root_thread_ts,
                job_id=job_id,
            )
        if len(posts) == 0:
            return SlackPostResult(kind=SlackPostKind.LOST_RESPONSE)
        return SlackPostResult(
            kind=SlackPostKind.AMBIGUOUS_RESPONSE, candidates=tuple(posts)
        )

    kind_raw = None
    if isinstance(raw, dict):
        kind_raw = raw.get("kind")
        ok = raw.get("ok")
        if kind_raw is None and ok is False:
            return SlackPostResult(kind=SlackPostKind.LOST_RESPONSE)
        if kind_raw is None and ok is True:
            posted = _parse_posted(raw)
            if posted is None:
                return SlackPostResult(kind=SlackPostKind.LOST_RESPONSE)
            return _accepted_if_bound(
                posted,
                client_msg_id=client_msg_id,
                workspace_id=workspace_id,
                channel_id=channel_id,
                root_thread_ts=root_thread_ts,
                job_id=job_id,
            )
    elif not isinstance(raw, SlackPosted):
        kind_raw = getattr(raw, "kind", None)

    kind = _kind_from(kind_raw)
    posted = _parse_posted(raw)
    if posted is None and isinstance(raw, dict):
        posted = _parse_posted(
            {
                "message_ts": raw.get("message_ts") or raw.get("ts"),
                "client_msg_id": raw.get("client_msg_id") or client_msg_id,
                "channel": raw.get("channel"),
                "team": raw.get("team"),
                "thread_ts": raw.get("thread_ts") or raw.get("root_thread_ts"),
                "job_id": raw.get("job_id"),
            }
        )
    if posted is None:
        message_ts = getattr(raw, "message_ts", None)
        if isinstance(message_ts, str) and message_ts.strip():
            posted = SlackPosted(
                message_ts=message_ts.strip(),
                client_msg_id=_text(getattr(raw, "client_msg_id", "")) or client_msg_id,
            )

    if kind is SlackPostKind.LOST_RESPONSE:
        return SlackPostResult(kind=SlackPostKind.LOST_RESPONSE, posted=posted)
    if kind is SlackPostKind.AMBIGUOUS_RESPONSE:
        candidates = parse_lookup_posts(
            getattr(raw, "candidates", ()) or (raw.get("candidates") if isinstance(raw, dict) else ()),
            expected_client_msg_id=client_msg_id,
        )
        return SlackPostResult(
            kind=SlackPostKind.AMBIGUOUS_RESPONSE,
            posted=posted,
            candidates=tuple(candidates),
        )
    if kind is SlackPostKind.ACCEPTED or (kind is None and posted is not None):
        if posted is None:
            return SlackPostResult(kind=SlackPostKind.UNKNOWN)
        return _accepted_if_bound(
            posted,
            client_msg_id=client_msg_id,
            workspace_id=workspace_id,
            channel_id=channel_id,
            root_thread_ts=root_thread_ts,
            job_id=job_id,
        )
    if kind is SlackPostKind.UNKNOWN:
        return SlackPostResult(kind=SlackPostKind.UNKNOWN, posted=posted)
    return SlackPostResult(kind=SlackPostKind.UNKNOWN, posted=posted)


def _accepted_if_bound(
    posted: SlackPosted,
    *,
    client_msg_id: str,
    workspace_id: str,
    channel_id: str,
    root_thread_ts: str,
    job_id: str,
) -> SlackPostResult:
    if not posted.message_ts:
        return SlackPostResult(kind=SlackPostKind.UNKNOWN, posted=posted)
    if not _identity_matches(
        posted,
        client_msg_id=client_msg_id,
        workspace_id=workspace_id,
        channel_id=channel_id,
        root_thread_ts=root_thread_ts,
        job_id=job_id,
    ):
        return SlackPostResult(kind=SlackPostKind.UNKNOWN, posted=posted)
    bound = SlackPosted(
        message_ts=posted.message_ts,
        client_msg_id=posted.client_msg_id or client_msg_id,
        workspace_id=posted.workspace_id or workspace_id,
        channel_id=posted.channel_id or channel_id,
        root_thread_ts=posted.root_thread_ts or root_thread_ts,
        job_id=posted.job_id or job_id,
    )
    return SlackPostResult(
        kind=SlackPostKind.ACCEPTED, message_ts=bound.message_ts, posted=bound
    )


def _fail_closed_post(
    result: SlackPostResult,
    *,
    client_msg_id: str,
    workspace_id: str,
    channel_id: str,
    root_thread_ts: str,
    job_id: str,
) -> SlackPostResult:
    if result.kind is not SlackPostKind.ACCEPTED:
        return result
    posted = result.posted
    if posted is None and result.message_ts:
        posted = SlackPosted(
            message_ts=result.message_ts, client_msg_id=client_msg_id
        )
    if posted is None:
        return SlackPostResult(kind=SlackPostKind.UNKNOWN, error=result.error)
    return _accepted_if_bound(
        posted,
        client_msg_id=client_msg_id,
        workspace_id=workspace_id,
        channel_id=channel_id,
        root_thread_ts=root_thread_ts,
        job_id=job_id,
    )


def _fail_closed_lookup_error(expected_client_msg_id: str) -> list[SlackPosted]:
    """Unknown lookup errors must not uniquely adopt or look empty."""
    return [
        SlackPosted(
            message_ts="lookup-error-a", client_msg_id=expected_client_msg_id
        ),
        SlackPosted(
            message_ts="lookup-error-b", client_msg_id=expected_client_msg_id
        ),
    ]


class SlackClientBridge:
    """Fail-closed SlackMessagePort. Transport is dependency-injected.

    Never opens sockets itself. Never enables Package 1 dispatch.
    """

    def __init__(self, transport: SlackTransport) -> None:
        if transport is None:
            raise TypeError(
                "SlackClientBridge requires an injected transport; "
                "no live Slack client is constructed in this slice"
            )
        self._transport = transport

    def post_root(
        self,
        *,
        client_msg_id: str,
        workspace_id: str,
        channel_id: str,
        root_thread_ts: str,
        job_id: str,
    ) -> SlackPostResult:
        try:
            raw = self._transport.post_root(
                client_msg_id=client_msg_id,
                workspace_id=workspace_id,
                channel_id=channel_id,
                root_thread_ts=root_thread_ts,
                job_id=job_id,
            )
        except Exception as exc:
            return SlackPostResult(
                kind=SlackPostKind.UNKNOWN, error=redact_slack_error(exc)
            )
        return normalize_post_result(
            raw,
            client_msg_id=client_msg_id,
            workspace_id=workspace_id,
            channel_id=channel_id,
            root_thread_ts=root_thread_ts,
            job_id=job_id,
        )

    def lookup_by_client_msg_id(self, client_msg_id: str) -> list[SlackPosted]:
        expected = str(client_msg_id or "").strip()
        if not expected:
            return []
        try:
            raw = self._transport.lookup_by_client_msg_id(expected)
        except Exception:
            return _fail_closed_lookup_error(expected)
        return parse_lookup_posts(raw, expected_client_msg_id=expected)


def adapter_from_config(
    config: DurableJobsConfig,
    *,
    transport: Optional[SlackTransport] = None,
) -> Any:
    """Default factory stays fail-closed. Flags cannot mint a live client."""
    if config.dispatch_allowed:
        raise RuntimeError(
            "live Slack dispatch is not available; "
            "dispatch_allowed cannot enable a network client"
        )
    if transport is None:
        return NullSlackPort()
    return SlackClientBridge(transport)


_SECRET_KV_RE = re.compile(
    r"(?i)\b(api[_-]?key|token|bearer|password|secret|authorization|xoxb|xoxp)"
    r"\s*[:=]\s*(?:(['\"])(.*?)\2|([^\s,;]+))"
)


def redact_slack_error(exc: BaseException | str) -> str:
    text = redact_secret_text(str(exc))

    def _sub(match: re.Match[str]) -> str:
        key = match.group(1)
        quote = match.group(2)
        if quote:
            return f"{key}={quote}[REDACTED]{quote}"
        return f"{key}=[REDACTED]"

    return _SECRET_KV_RE.sub(_sub, text)
