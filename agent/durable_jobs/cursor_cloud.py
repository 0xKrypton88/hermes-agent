"""ENG-30 inactive/fail-closed Cursor Cloud adapter contract.

Production-shaped types and an injected-transport seam behind
``CursorProviderPort``. No live HTTP client is constructed. ``attempt_dispatch``
remains hard-disabled. ``dispatch_allowed`` is a config capability flag only —
it cannot mint a live client. Job/action idempotency identity is the existing
durable effect ledger — this module does not keep a second run map.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping
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
    agent_id: str = ""


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


@dataclass(frozen=True)
class CursorStatusReconcileResult:
    """Status observation against ledger identity.

    Status reconcile never rewrites a terminal ledger claim. ``ok`` is True
    only when the provider uniquely confirms the bound ``provider_run_id``.
    UNKNOWN/AMBIGUOUS/mismatch is fail-closed (``ok`` False) and must not be
    treated as success; the stored claim remains the durable identity.
    """

    claim: Any
    observation: Optional[CursorStatusResult]
    ok: bool


class CursorCloudTransport(Protocol):
    """Injected transport. Tests supply an in-memory fake; no default client.

    ``create`` must send client ``agent_id`` (official ``agentId``). Live v1
    list/get echo that value as ``id`` and do not preserve create ``name``.
    """

    def create(
        self, *, idempotency_key: str, job_id: str, name: str, agent_id: str
    ) -> Any: ...

    def lookup(self, *, idempotency_key: str) -> Sequence[Any]: ...

    def status(
        self, *, run_id: str, idempotency_key: str, agent_id: str
    ) -> Any: ...


# v0/v1 Cloud Agents ``name`` max is 100. Ledger keys must fit exactly; never truncate.
CURSOR_AGENT_NAME_MAX = 100
_CURSOR_KEY_RE = re.compile(r"^cursor:[A-Za-z0-9_.-]+:[A-Za-z0-9_.-]+$")
_CURSOR_AGENT_ID_RE = re.compile(r"^bc[-_][A-Za-z0-9-]+$")
# Official OpenAPI: POST /v1/agents agentId.
_CURSOR_CLIENT_AGENT_ID_RE = re.compile(
    r"^bc-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_CURSOR_AGENT_ID_PREFIX = "hermes-durable-jobs:cursor-agent:"
_OFFICIAL_LIST_KEYS = ("agents", "items")


def cursor_correlation_name(idempotency_key: str) -> str:
    """Create ``name`` to send (display only). Equals the ledger key or raises.

    Live v1 overwrites ``name`` with a generated display string; list/get do
    not preserve it. Recovery identity is ``cursor_correlation_agent_id``.
    Keys longer than ``CURSOR_AGENT_NAME_MAX`` are rejected fail-closed. This
    function never silently truncates (truncation would collide distinct keys).
    """
    key = str(idempotency_key).strip()
    if not key or len(key) > CURSOR_AGENT_NAME_MAX:
        raise ValueError(
            "provider idempotency key exceeds Cursor agent name limit"
        )
    return key


def cursor_correlation_agent_id(idempotency_key: str) -> str:
    """Deterministic client ``agentId`` echoed by live list/get as ``id``.

    OpenAPI pattern: ``bc-`` + UUID. uuid5 of a stable namespace+prefix plus
    the ledger key is idempotent across lost-create retries without injecting
    a synthetic ``idempotency_key`` into official payloads.
    """
    key = str(idempotency_key).strip()
    if not key:
        raise ValueError(
            "Cursor correlation agent id requires a non-empty idempotency key"
        )
    derived = uuid.uuid5(uuid.NAMESPACE_URL, _CURSOR_AGENT_ID_PREFIX + key)
    agent_id = f"bc-{derived}"
    if not _CURSOR_CLIENT_AGENT_ID_RE.fullmatch(agent_id):
        raise ValueError("derived Cursor agent id is not a valid client agentId")
    return agent_id


def cursor_correlation_prompt(idempotency_key: str, text: str = "") -> str:
    """Optional prompt prefix; list endpoints echo ``name``, not prompt text."""
    marker = cursor_correlation_name(idempotency_key)
    body = str(text or "").strip()
    return marker if not body else f"{marker}\n{body}"


def _record_get(raw: Any, key: str, default: Any = None) -> Any:
    """Read a field from a dict or typed SDK object. Same keys, both shapes."""
    if isinstance(raw, Mapping):
        return raw.get(key, default)
    if raw is None or isinstance(raw, (str, bytes, int, float, bool)):
        return default
    return getattr(raw, key, default)


def _is_mapping_like(raw: Any) -> bool:
    if isinstance(raw, Mapping):
        return True
    if raw is None or isinstance(
        raw, (str, bytes, int, float, bool, list, tuple, CursorRun)
    ):
        return False
    return hasattr(raw, "__dict__") or hasattr(raw, "__dataclass_fields__")


def _display_name(raw: Any) -> str:
    name = _record_get(raw, "name")
    return name.strip() if isinstance(name, str) else ""


def _prompt_first_line(raw: Any) -> str:
    prompt: Any = _record_get(raw, "prompt")
    if prompt is None:
        source = _record_get(raw, "source")
        if source is not None:
            prompt = _record_get(source, "prompt")
    if isinstance(prompt, Mapping):
        prompt = prompt.get("text")
    elif prompt is not None and not isinstance(prompt, str):
        nested = _record_get(prompt, "text")
        if nested is not None:
            prompt = nested
    if not isinstance(prompt, str):
        return ""
    return prompt.strip().splitlines()[0].strip() if prompt.strip() else ""


def _exact_cursor_key(text: str) -> Optional[str]:
    """Whole-field key only. Substrings and foreign markers do not match."""
    if not text or len(text) > CURSOR_AGENT_NAME_MAX:
        return None
    if _CURSOR_KEY_RE.fullmatch(text):
        return text
    return None


def _extract_preserved_correlation(
    raw: Any, *, expected_key: str = ""
) -> Optional[str]:
    """Recover the ledger key from documented preserved fields (name/prompt).

    Matching is exact on the whole ``name`` (or prompt first line). A human
    sentence that merely contains the key is not a match.
    """
    fields = (_display_name(raw), _prompt_first_line(raw))
    expected = str(expected_key).strip()
    for text in fields:
        if not text:
            continue
        if expected and text == expected:
            return expected
        extracted = _exact_cursor_key(text)
        if extracted:
            return extracted
    return None


def _looks_like_official_cursor_record(raw: Any) -> bool:
    """Official Cursor agent/run shape, whether a dict or a typed SDK object."""
    if raw is None or isinstance(raw, CursorRun):
        return False
    if isinstance(raw, (str, bytes, int, float, bool, list, tuple)):
        return False
    if _is_mapping_like(_record_get(raw, "agent")):
        return True
    rid = _record_get(raw, "id") or _record_get(raw, "run_id")
    if isinstance(rid, str) and _CURSOR_AGENT_ID_RE.match(rid.strip()):
        return True
    if _record_get(raw, "latestRunId") or _record_get(raw, "agentId"):
        return True
    if _is_mapping_like(_record_get(raw, "source")) or _is_mapping_like(
        _record_get(raw, "target")
    ):
        return True
    return False


def _provider_records(raw: Any) -> Sequence[Any]:
    """Unwrap v0 ``{agents: []}`` / v1 ``{items: []}`` list envelopes."""
    if raw is None:
        return ()
    if isinstance(raw, (list, tuple)):
        return raw
    if isinstance(raw, Mapping):
        for envelope in _OFFICIAL_LIST_KEYS:
            inner = raw.get(envelope)
            if isinstance(inner, (list, tuple)):
                return inner
        return (raw,)
    return (raw,)


def _record_id(raw: Any) -> str:
    rid = _record_get(raw, "run_id") or _record_get(raw, "id")
    if isinstance(rid, str) and rid.strip():
        return rid.strip()
    return ""


def _cursor_agent_id(raw: Any) -> str:
    """Durable Cloud Agent id (``bc-…``), never ``latestRunId``."""
    nested = _record_get(raw, "agent")
    if _is_mapping_like(nested):
        aid = _record_get(nested, "id")
        if isinstance(aid, str) and aid.strip():
            return aid.strip()
    for key in ("agentId", "agent_id"):
        aid = _record_get(raw, key)
        if isinstance(aid, str) and aid.strip():
            return aid.strip()
    rid = _record_get(raw, "id")
    if isinstance(rid, str) and rid.strip() and _CURSOR_AGENT_ID_RE.match(rid.strip()):
        return rid.strip()
    return ""


def _cursor_run_id(raw: Any) -> str:
    """Cursor run id for GET /v1/agents/{agent_id}/runs/{run_id}."""
    inner = _record_get(raw, "run")
    if _is_mapping_like(inner):
        rid = _record_get(inner, "id") or _record_get(inner, "run_id")
        if isinstance(rid, str) and rid.strip():
            return rid.strip()
    latest = _record_get(raw, "latestRunId") or _record_get(raw, "latest_run_id")
    if isinstance(latest, str) and latest.strip():
        return latest.strip()
    explicit = _record_get(raw, "run_id")
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip()
    rid = _record_get(raw, "id")
    if isinstance(rid, str) and rid.strip() and not _CURSOR_AGENT_ID_RE.match(rid.strip()):
        return rid.strip()
    return ""


def _is_v1_client_agent_id(value: str) -> bool:
    return bool(value) and bool(_CURSOR_CLIENT_AGENT_ID_RE.fullmatch(value))


def _matches_derived_agent_id(raw: Any, *, expected_key: str) -> bool:
    if not expected_key:
        return False
    record_id = _cursor_agent_id(raw)
    if not record_id:
        return False
    try:
        derived = cursor_correlation_agent_id(expected_key)
    except ValueError:
        return False
    return record_id.lower() == derived.lower()


def _is_cursor_agent_id_conflict(raw: Any) -> bool:
    """Official re-POST of the same client agentId returns 409 agent_id_conflict."""
    if not isinstance(raw, Mapping):
        return False
    chunks: list[str] = []
    for key in ("error", "code", "message", "error_code", "kind"):
        val = raw.get(key)
        if isinstance(val, Mapping):
            chunks.extend(str(part) for part in val.values() if part is not None)
        elif val is not None:
            chunks.append(str(val))
    return "agent_id_conflict" in " ".join(chunks).lower()


def _correlation_for_record(raw: Any, *, fallback_key: str) -> str:
    explicit = _record_get(raw, "idempotency_key")
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip()
    if _matches_derived_agent_id(raw, expected_key=fallback_key):
        return fallback_key
    extracted = _extract_preserved_correlation(raw, expected_key=fallback_key)
    if extracted:
        return extracted
    if _looks_like_official_cursor_record(raw):
        # Display names without an exact marker are not the ledger key.
        return _display_name(raw)
    return fallback_key


def _parse_run(raw: Any, *, fallback_key: str = "") -> Optional[CursorRun]:
    if raw is None:
        return None
    if isinstance(raw, CursorRun):
        return raw
    agent_id = _cursor_agent_id(raw)
    run_id = _cursor_run_id(raw)
    if _is_v1_client_agent_id(agent_id):
        if not run_id or run_id.lower() == agent_id.lower():
            # v1 agent identity is not a run id; refuse agent-as-run confusion.
            return None
    elif not run_id:
        run_id = agent_id or _record_id(raw)
    if not run_id:
        return None
    key = _correlation_for_record(raw, fallback_key=fallback_key)
    state = _record_get(raw, "state") or _record_get(raw, "status")
    return CursorRun(
        run_id=run_id,
        idempotency_key=str(key or ""),
        state=_parse_run_state(state),
        agent_id=agent_id,
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
        "not_yet_started": CursorRunState.CREATING,
        "running": CursorRunState.RUNNING,
        "in_progress": CursorRunState.RUNNING,
        "active": CursorRunState.RUNNING,
        "idle": CursorRunState.RUNNING,
        "completed": CursorRunState.COMPLETED,
        "finished": CursorRunState.COMPLETED,
        "succeeded": CursorRunState.COMPLETED,
        "failed": CursorRunState.FAILED,
        "error": CursorRunState.FAILED,
        "canceled": CursorRunState.CANCELED,
        "cancelled": CursorRunState.CANCELED,
        "expired": CursorRunState.CANCELED,
        "archived": CursorRunState.CANCELED,
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
    if _is_cursor_agent_id_conflict(raw):
        # Same client agentId already exists — look up rather than create again.
        return CursorCreateResult(kind=CursorCreateKind.LOST_RESPONSE)
    if isinstance(raw, (list, tuple)):
        runs = [
            parsed
            for parsed in (_parse_run(item, fallback_key=expected_key) for item in raw)
            if parsed is not None
        ]
        if len(runs) == 1:
            return _fail_closed_create(
                CursorCreateResult(kind=CursorCreateKind.ACCEPTED, run=runs[0]),
                expected_key=expected_key,
            )
        if len(runs) == 0:
            return CursorCreateResult(kind=CursorCreateKind.LOST_RESPONSE)
        return CursorCreateResult(
            kind=CursorCreateKind.AMBIGUOUS_RESPONSE, candidates=tuple(runs)
        )
    kind_raw = None
    run_raw: Any = None
    candidates_raw: Any = ()
    error_raw: Any = None
    if isinstance(raw, Mapping):
        kind_raw = raw.get("kind")
        if not kind_raw:
            official = _accepted_from_official_create(raw, expected_key=expected_key)
            if official is not None:
                return _fail_closed_create(official, expected_key=expected_key)
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
        if not isinstance(run_key, str) or not run_key.strip():
            # Missing key uses the ledger/transport expected identity.
            bound = CursorRun(
                run_id=result.run.run_id,
                idempotency_key=expected_key,
                state=result.run.state,
                agent_id=result.run.agent_id,
            )
            return CursorCreateResult(
                kind=CursorCreateKind.ACCEPTED,
                run=bound,
                candidates=result.candidates,
                error=result.error,
            )
        if run_key != expected_key:
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


def _accepted_from_official_create(
    raw: Mapping[str, Any], *, expected_key: str
) -> Optional[CursorCreateResult]:
    """Map v0 agent / v1 {agent, run} create bodies. No custom idempotency field."""
    record = raw.get("agent") if isinstance(raw.get("agent"), Mapping) else raw
    if not isinstance(record, Mapping):
        return None
    if not _looks_like_official_cursor_record(raw) and not _looks_like_official_cursor_record(
        record
    ):
        return None
    inner = raw.get("run") if isinstance(raw.get("run"), Mapping) else None
    if inner is not None:
        merged = dict(record)
        if inner.get("id") and not merged.get("latestRunId"):
            merged["latestRunId"] = inner["id"]
        if inner.get("agentId") and not merged.get("id"):
            merged["id"] = inner["agentId"]
        record = merged
    parsed = _parse_run(record, fallback_key=expected_key)
    if parsed is None or not parsed.run_id:
        return None
    return CursorCreateResult(kind=CursorCreateKind.ACCEPTED, run=parsed)


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
            name = cursor_correlation_name(idempotency_key)
            agent_id = cursor_correlation_agent_id(idempotency_key)
        except ValueError as exc:
            return CursorCreateResult(
                kind=CursorCreateKind.UNKNOWN,
                error=redact_provider_error(exc),
            )
        try:
            raw = self._transport.create(
                idempotency_key=idempotency_key,
                job_id=job_id,
                name=name,
                agent_id=agent_id,
            )
        except Exception as exc:
            return CursorCreateResult(
                kind=CursorCreateKind.UNKNOWN,
                error=redact_provider_error(exc),
            )
        return normalize_create_result(raw, expected_key=idempotency_key)

    def lookup_runs(self, *, idempotency_key: str) -> list[CursorRun]:
        try:
            cursor_correlation_name(idempotency_key)
        except ValueError:
            return []
        try:
            raw = self._transport.lookup(idempotency_key=idempotency_key)
        except Exception as exc:
            return _fail_closed_lookup_error(idempotency_key, exc)
        return parse_lookup_runs(raw, expected_key=idempotency_key)

    def status_run(
        self, *, run_id: str, idempotency_key: str, agent_id: str = ""
    ) -> CursorStatusResult:
        try:
            cursor_correlation_name(idempotency_key)
            derived_agent_id = cursor_correlation_agent_id(idempotency_key)
            if agent_id and agent_id != derived_agent_id:
                raise ValueError("Cursor status agent id does not match idempotency key")
            raw = self._transport.status(
                run_id=run_id,
                agent_id=derived_agent_id,
                idempotency_key=idempotency_key,
            )
        except Exception as exc:
            return CursorStatusResult(
                kind=CursorStatusKind.UNKNOWN,
                error=redact_provider_error(exc),
            )
        return normalize_status_result(
            raw, expected_run_id=run_id, expected_agent_id=derived_agent_id
        )

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
    ) -> CursorStatusReconcileResult:
        from agent.durable_jobs.effects import EffectStatus, reconcile_cursor_create

        claim = ledger.get_claim(job_id, action_id)
        if claim is None:
            raise KeyError(f"unknown effect claim: {job_id}/{action_id}")
        if claim.status in (
            EffectStatus.ACCEPTED,
            EffectStatus.ADOPTED,
            EffectStatus.UNKNOWN,
        ):
            observation: Optional[CursorStatusResult] = None
            if claim.provider_run_id:
                claim_key = getattr(claim, "provider_idempotency_key", "") or ""
                observation = self.status_run(
                    run_id=claim.provider_run_id,
                    idempotency_key=claim_key,
                )
            ok = _status_observation_confirms_claim(claim, observation)
            return CursorStatusReconcileResult(
                claim=claim, observation=observation, ok=ok
            )
        recovered = reconcile_cursor_create(
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
        ok = recovered.status in (EffectStatus.ACCEPTED, EffectStatus.ADOPTED)
        return CursorStatusReconcileResult(
            claim=recovered, observation=None, ok=ok
        )


def parse_lookup_runs(raw: Any, *, expected_key: str) -> list[CursorRun]:
    if raw is None:
        return []
    runs: list[CursorRun] = []
    for item in _provider_records(raw):
        parsed = _parse_run(item, fallback_key=expected_key)
        if parsed is None or not parsed.run_id:
            continue
        if expected_key and parsed.idempotency_key not in ("", expected_key):
            continue
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
    raw: Any,
    *,
    expected_run_id: Optional[str] = None,
    expected_agent_id: Optional[str] = None,
) -> CursorStatusResult:
    if isinstance(raw, CursorStatusResult):
        return _fail_closed_status(
            raw,
            expected_run_id=expected_run_id,
            expected_agent_id=expected_agent_id,
        )
    if raw is None:
        return CursorStatusResult(kind=CursorStatusKind.UNKNOWN)
    if isinstance(raw, (list, tuple)):
        runs = parse_lookup_runs(raw, expected_key="")
        if len(runs) == 0:
            return CursorStatusResult(kind=CursorStatusKind.EMPTY)
        if len(runs) > 1:
            return _fail_closed_status(
                CursorStatusResult(
                    kind=CursorStatusKind.AMBIGUOUS, candidates=tuple(runs)
                ),
                expected_run_id=expected_run_id,
                expected_agent_id=expected_agent_id,
            )
        return _status_from_known_run(
            runs[0],
            expected_run_id=expected_run_id,
            expected_agent_id=expected_agent_id,
        )
    run = _parse_run(raw, fallback_key="")
    if run is None:
        return CursorStatusResult(kind=CursorStatusKind.UNKNOWN)
    return _status_from_known_run(
        run,
        expected_run_id=expected_run_id,
        expected_agent_id=expected_agent_id,
    )


def _fail_closed_status(
    result: CursorStatusResult,
    *,
    expected_run_id: Optional[str],
    expected_agent_id: Optional[str] = None,
) -> CursorStatusResult:
    if not expected_run_id and not expected_agent_id:
        return result
    if result.kind is CursorStatusKind.UNIQUE:
        run = result.run
        if run is None:
            return CursorStatusResult(
                kind=CursorStatusKind.UNKNOWN,
                run=run,
                candidates=result.candidates,
                error=result.error,
            )
        if expected_run_id and run.run_id != expected_run_id:
            return CursorStatusResult(
                kind=CursorStatusKind.UNKNOWN,
                run=run,
                candidates=result.candidates,
                error=result.error,
            )
        if (
            expected_agent_id
            and run.agent_id
            and _is_v1_client_agent_id(run.agent_id)
            and run.agent_id.lower() != expected_agent_id.lower()
        ):
            return CursorStatusResult(
                kind=CursorStatusKind.UNKNOWN,
                run=run,
                candidates=result.candidates,
                error=result.error,
            )
        return result
    return result


def _status_observation_confirms_claim(
    claim: Any, observation: Optional[CursorStatusResult]
) -> bool:
    if observation is None or observation.kind is not CursorStatusKind.UNIQUE:
        return False
    bound = getattr(claim, "provider_run_id", None)
    run = observation.run
    if not bound or run is None:
        return False
    if run.run_id != bound:
        return False
    if run.agent_id and _is_v1_client_agent_id(run.agent_id):
        claim_key = getattr(claim, "provider_idempotency_key", "") or ""
        try:
            expected_agent = cursor_correlation_agent_id(claim_key) if claim_key else ""
        except ValueError:
            expected_agent = ""
        if expected_agent and run.agent_id.lower() != expected_agent.lower():
            return False
    return True


def _status_from_known_run(
    run: CursorRun,
    *,
    expected_run_id: Optional[str] = None,
    expected_agent_id: Optional[str] = None,
) -> CursorStatusResult:
    if expected_run_id and run.run_id != expected_run_id:
        return CursorStatusResult(kind=CursorStatusKind.UNKNOWN, run=run)
    if (
        expected_agent_id
        and run.agent_id
        and _is_v1_client_agent_id(run.agent_id)
        and run.agent_id.lower() != expected_agent_id.lower()
    ):
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
    """Factory stays fail-closed. Flags cannot mint a live client."""
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
