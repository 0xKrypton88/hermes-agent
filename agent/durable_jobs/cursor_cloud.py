"""ENG-30 inactive/fail-closed Cursor Cloud adapter contract.

Production-shaped types and an injected-transport seam behind
``CursorProviderPort``. No live HTTP client is constructed. Dispatch remains
hard-disabled (``config.dispatch_allowed`` is always False). Job/action
idempotency identity is the existing durable effect ledger — this module
does not keep a second run map.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, Protocol, Sequence

from agent.durable_jobs.adapters import NullCursorProvider
from agent.durable_jobs.config import DurableJobsConfig
from agent.durable_jobs.redaction import redact_secret_text


class CursorCreateKind(str, Enum):
    ACCEPTED = "accepted"
    LOST_RESPONSE = "lost_response"
    AMBIGUOUS_RESPONSE = "ambiguous_response"
    UNKNOWN = "unknown"


class CursorRunState(str, Enum):
    CREATING = "creating"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    UNKNOWN = "unknown"
    AMBIGUOUS = "ambiguous"


class CursorStatusKind(str, Enum):
    UNIQUE = "unique"
    EMPTY = "empty"
    AMBIGUOUS = "ambiguous"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class CursorRun:
    run_id: str
    idempotency_key: str
    state: CursorRunState = CursorRunState.UNKNOWN


@dataclass(frozen=True)
class CursorCreateResult:
    kind: CursorCreateKind
    run: Optional[CursorRun] = None
    candidates: tuple[CursorRun, ...] = ()
    error: Optional[str] = None


@dataclass(frozen=True)
class CursorStatusResult:
    kind: CursorStatusKind
    run: Optional[CursorRun] = None
    candidates: tuple[CursorRun, ...] = ()
    error: Optional[str] = None


class CursorCloudTransport(Protocol):
    """Injected transport. Tests supply an in-memory fake; no default client."""

    def create(self, *, idempotency_key: str, job_id: str) -> Any: ...

    def lookup(self, *, idempotency_key: str) -> Sequence[Any]: ...

    def status(self, *, run_id: str) -> Any: ...


def _parse_run(raw: Any, *, fallback_key: str = "") -> Optional[CursorRun]:
    if raw is None:
        return None
    if isinstance(raw, CursorRun):
        return raw
    if isinstance(raw, dict):
        run_id = raw.get("run_id") or raw.get("id")
        key = raw.get("idempotency_key")
        if key is None:
            key = fallback_key
        if not run_id:
            return None
        return CursorRun(
            run_id=str(run_id),
            idempotency_key=str(key or ""),
            state=_parse_run_state(raw.get("state") or raw.get("status")),
        )
    run_id = getattr(raw, "run_id", None) or getattr(raw, "id", None)
    if not run_id:
        return None
    key = getattr(raw, "idempotency_key", None)
    if key is None:
        key = fallback_key
    state = getattr(raw, "state", None) or getattr(raw, "status", None)
    return CursorRun(
        run_id=str(run_id),
        idempotency_key=str(key or ""),
        state=_parse_run_state(state),
    )


def _parse_run_state(raw: Any) -> CursorRunState:
    if raw is None:
        return CursorRunState.UNKNOWN
    if isinstance(raw, CursorRunState):
        return raw
    text = str(raw).strip().lower()
    aliases = {
        "creating": CursorRunState.CREATING,
        "queued": CursorRunState.CREATING,
        "running": CursorRunState.RUNNING,
        "in_progress": CursorRunState.RUNNING,
        "completed": CursorRunState.COMPLETED,
        "finished": CursorRunState.COMPLETED,
        "succeeded": CursorRunState.COMPLETED,
        "failed": CursorRunState.FAILED,
        "error": CursorRunState.FAILED,
        "canceled": CursorRunState.CANCELED,
        "cancelled": CursorRunState.CANCELED,
        "unknown": CursorRunState.UNKNOWN,
        "ambiguous": CursorRunState.AMBIGUOUS,
    }
    return aliases.get(text, CursorRunState.UNKNOWN)


def _kind_from(raw: Any) -> Optional[CursorCreateKind]:
    if isinstance(raw, CursorCreateKind):
        return raw
    if raw is None:
        return None
    text = str(raw).strip().lower()
    try:
        return CursorCreateKind(text)
    except ValueError:
        return None


def normalize_create_result(raw: Any, *, expected_key: str) -> CursorCreateResult:
    """Map a transport payload to a typed create result. Unknown shapes fail closed."""
    if isinstance(raw, CursorCreateResult):
        return _fail_closed_create(raw, expected_key=expected_key)
    if raw is None:
        return CursorCreateResult(kind=CursorCreateKind.LOST_RESPONSE)
    if isinstance(raw, (list, tuple)):
        runs = [
            parsed
            for parsed in (_parse_run(item, fallback_key=expected_key) for item in raw)
            if parsed is not None
        ]
        if len(runs) == 1:
            return CursorCreateResult(kind=CursorCreateKind.ACCEPTED, run=runs[0])
        if len(runs) == 0:
            return CursorCreateResult(kind=CursorCreateKind.LOST_RESPONSE)
        return CursorCreateResult(
            kind=CursorCreateKind.AMBIGUOUS_RESPONSE, candidates=tuple(runs)
        )
    kind_raw = None
    run_raw: Any = None
    candidates_raw: Any = ()
    error_raw: Any = None
    if isinstance(raw, dict):
        kind_raw = raw.get("kind")
        run_raw = raw.get("run")
        candidates_raw = raw.get("candidates") or ()
        error_raw = raw.get("error")
    else:
        kind_raw = getattr(raw, "kind", None)
        run_raw = getattr(raw, "run", None)
        candidates_raw = getattr(raw, "candidates", ()) or ()
        error_raw = getattr(raw, "error", None)
        if kind_raw is None and run_raw is None and not candidates_raw:
            return CursorCreateResult(kind=CursorCreateKind.UNKNOWN)
    kind = _kind_from(kind_raw)
    if kind is None:
        return CursorCreateResult(
            kind=CursorCreateKind.UNKNOWN,
            error=redact_provider_error(error_raw) if error_raw else None,
        )
    run = _parse_run(run_raw, fallback_key=expected_key)
    candidates = tuple(
        parsed
        for parsed in (
            _parse_run(item, fallback_key=expected_key) for item in candidates_raw
        )
        if parsed is not None
    )
    error = redact_provider_error(error_raw) if error_raw else None
    return _fail_closed_create(
        CursorCreateResult(kind=kind, run=run, candidates=candidates, error=error),
        expected_key=expected_key,
    )


def _fail_closed_create(
    result: CursorCreateResult, *, expected_key: str
) -> CursorCreateResult:
    if result.kind is CursorCreateKind.ACCEPTED:
        if result.run is None or not result.run.run_id:
            return CursorCreateResult(
                kind=CursorCreateKind.UNKNOWN,
                error=result.error,
            )
        run_key = result.run.idempotency_key
        if (
            isinstance(run_key, str)
            and run_key.startswith("cursor:")
            and run_key != expected_key
        ):
            return CursorCreateResult(
                kind=CursorCreateKind.UNKNOWN,
                run=result.run,
                error=result.error,
            )
        return result
    if result.kind in (
        CursorCreateKind.LOST_RESPONSE,
        CursorCreateKind.AMBIGUOUS_RESPONSE,
        CursorCreateKind.UNKNOWN,
    ):
        return result
    return CursorCreateResult(kind=CursorCreateKind.UNKNOWN, error=result.error)


class CursorCloudAdapter:
    """Fail-closed Cursor Cloud adapter. Transport is dependency-injected.

    Never opens sockets itself. Never enables Package 1 dispatch.
    """

    def __init__(self, transport: CursorCloudTransport) -> None:
        if transport is None:
            raise TypeError(
                "CursorCloudAdapter requires an injected transport; "
                "no live Cursor Cloud client is constructed in this slice"
            )
        self._transport = transport

    def create_run(self, *, idempotency_key: str, job_id: str) -> CursorCreateResult:
        try:
            raw = self._transport.create(
                idempotency_key=idempotency_key, job_id=job_id
            )
        except Exception as exc:
            return CursorCreateResult(
                kind=CursorCreateKind.UNKNOWN,
                error=redact_provider_error(exc),
            )
        return normalize_create_result(raw, expected_key=idempotency_key)

    def lookup_runs(self, *, idempotency_key: str) -> list[CursorRun]:
        try:
            raw = self._transport.lookup(idempotency_key=idempotency_key)
        except Exception as exc:
            return _fail_closed_lookup_error(idempotency_key, exc)
        return parse_lookup_runs(raw, expected_key=idempotency_key)

    def status_run(self, *, run_id: str) -> CursorStatusResult:
        try:
            raw = self._transport.status(run_id=run_id)
        except Exception as exc:
            return CursorStatusResult(
                kind=CursorStatusKind.UNKNOWN,
                error=redact_provider_error(exc),
            )
        return normalize_status_result(raw, expected_run_id=run_id)

    def reconcile_create(self, ledger: Any, **kwargs: Any) -> Any:
        from agent.durable_jobs.effects import reconcile_cursor_create

        return reconcile_cursor_create(ledger, self, **kwargs)

    def reconcile_status(
        self,
        ledger: Any,
        *,
        job_id: str,
        action_id: str,
        owner_token: Optional[str] = None,
    ) -> Any:
        from agent.durable_jobs.effects import EffectStatus, reconcile_cursor_create

        claim = ledger.get_claim(job_id, action_id)
        if claim is None:
            raise KeyError(f"unknown effect claim: {job_id}/{action_id}")
        if claim.status in (
            EffectStatus.ACCEPTED,
            EffectStatus.ADOPTED,
            EffectStatus.UNKNOWN,
        ):
            if claim.provider_run_id:
                self.status_run(run_id=claim.provider_run_id)
            return claim
        return reconcile_cursor_create(
            ledger,
            self,
            job_id=claim.job_id,
            action_id=claim.action_id,
            origin_platform=claim.origin_platform,
            origin_chat_id=claim.origin_chat_id,
            origin_root_thread_id=claim.origin_root_thread_id,
            candidate_id=claim.candidate_id,
            candidate_version=claim.candidate_version,
            owner_token=owner_token,
        )


def parse_lookup_runs(raw: Any, *, expected_key: str) -> list[CursorRun]:
    if raw is None:
        return []
    items: Sequence[Any] = raw if isinstance(raw, (list, tuple)) else [raw]
    runs: list[CursorRun] = []
    for item in items:
        parsed = _parse_run(item, fallback_key=expected_key)
        if parsed is not None:
            runs.append(parsed)
    return runs


def classify_lookup(
    runs: Sequence[CursorRun], *, expected_key: str
) -> CursorStatusResult:
    usable: list[CursorRun] = []
    foreign = False
    for run in runs:
        if not run.run_id:
            continue
        if run.idempotency_key in ("", expected_key):
            usable.append(run)
        else:
            foreign = True
    if len(usable) == 1 and not foreign:
        return _status_from_known_run(usable[0])
    if len(usable) == 0 and not foreign:
        return CursorStatusResult(kind=CursorStatusKind.EMPTY)
    return CursorStatusResult(
        kind=CursorStatusKind.AMBIGUOUS, candidates=tuple(usable)
    )


def normalize_status_result(
    raw: Any, *, expected_run_id: Optional[str] = None
) -> CursorStatusResult:
    if isinstance(raw, CursorStatusResult):
        return raw
    if raw is None:
        return CursorStatusResult(kind=CursorStatusKind.UNKNOWN)
    if isinstance(raw, (list, tuple)):
        runs = parse_lookup_runs(raw, expected_key="")
        if len(runs) == 0:
            return CursorStatusResult(kind=CursorStatusKind.EMPTY)
        if len(runs) > 1:
            return CursorStatusResult(
                kind=CursorStatusKind.AMBIGUOUS, candidates=tuple(runs)
            )
        return _status_from_known_run(runs[0], expected_run_id=expected_run_id)
    run = _parse_run(raw, fallback_key="")
    if run is None:
        return CursorStatusResult(kind=CursorStatusKind.UNKNOWN)
    return _status_from_known_run(run, expected_run_id=expected_run_id)


def _status_from_known_run(
    run: CursorRun, *, expected_run_id: Optional[str] = None
) -> CursorStatusResult:
    if expected_run_id and run.run_id != expected_run_id:
        return CursorStatusResult(kind=CursorStatusKind.UNKNOWN, run=run)
    if run.state is CursorRunState.AMBIGUOUS:
        return CursorStatusResult(
            kind=CursorStatusKind.AMBIGUOUS, run=run, candidates=(run,)
        )
    if run.state is CursorRunState.UNKNOWN:
        return CursorStatusResult(kind=CursorStatusKind.UNKNOWN, run=run)
    return CursorStatusResult(kind=CursorStatusKind.UNIQUE, run=run)


def _fail_closed_lookup_error(expected_key: str, exc: BaseException) -> list[CursorRun]:
    """Unknown lookup errors must not uniquely adopt or look empty."""
    del exc  # message is redacted at status/create edges; do not embed it.
    return [
        CursorRun(run_id="lookup-error-a", idempotency_key=expected_key),
        CursorRun(run_id="lookup-error-b", idempotency_key=expected_key),
    ]


def adapter_from_config(
    config: DurableJobsConfig,
    *,
    transport: Optional[CursorCloudTransport] = None,
) -> Any:
    """Default factory stays fail-closed. Flags cannot mint a live client."""
    if config.dispatch_allowed:
        raise RuntimeError(
            "live Cursor Cloud dispatch is not available; "
            "dispatch_allowed cannot enable a network client"
        )
    if transport is None:
        return NullCursorProvider()
    return CursorCloudAdapter(transport)


_SECRET_KV_RE = re.compile(
    r"(?i)\b(api[_-]?key|token|bearer|password|secret|authorization)"
    r"\s*[:=]\s*(?:(['\"])(.*?)\2|([^\s,;]+))"
)


def redact_provider_error(exc: BaseException | str) -> str:
    text = redact_secret_text(str(exc))

    def _sub(match: re.Match[str]) -> str:
        key = match.group(1)
        quote = match.group(2)
        if quote:
            return f"{key}={quote}[REDACTED]{quote}"
        return f"{key}=[REDACTED]"

    return _SECRET_KV_RE.sub(_sub, text)
