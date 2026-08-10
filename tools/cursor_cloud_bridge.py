"""Hermes ↔ Cursor Cloud Agents bridge.

Service-gated tools that start a NEW visible Cursor Cloud agent per job, or
explicitly continue an existing agent by ID. Durable dispatch rows correlate
Hermes job/session/thread identity to Cursor agent/run IDs and the public
agent URL so completion resumes the same job (never a fresh chat task).

Hard rules encoded here:
- New jobs call Cursor ``POST /v1/agents``; named environments pass
  ``env: {type: "cloud", name: <environment_name>}``.
- Continuation is ONLY via ``existing_agent_id`` → GET agent, then
  ``POST /v1/agents/{id}/runs``.
- ``agent_busy`` and every bridge/preflight/local-origin failure return a
  typed technical block with ``user_actionable=false`` — never a Cursor UI
  click instruction.
- Agent URLs are always returned and persisted; never hidden.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional, Tuple
from urllib.parse import urlparse

import requests

from hermes_constants import get_hermes_home
from tools.registry import registry, tool_error, tool_result

logger = logging.getLogger(__name__)

CURSOR_API_BASE = "https://api.cursor.com"
CURSOR_AGENT_URL_PREFIX = "https://cursor.com/agents/"
_ACTIVE_RUN_STATUSES = frozenset({"CREATING", "RUNNING"})
_LOCAL_ORIGIN_RE = re.compile(
    r"^(?:file:|local:|/|\./|\.\./|[A-Za-z]:\\)",
    re.IGNORECASE,
)
_DB_LOCK = threading.Lock()

# Phases / next_actions used for durable correlation and resume.
PHASE_DISPATCHED = "dispatched"
PHASE_RUNNING = "running"
PHASE_BUSY = "agent_busy"
PHASE_COMPLETED = "completed"
PHASE_ERROR = "error"
NEXT_POLL = "poll_status"
NEXT_RESUME = "resume_same_job"
NEXT_WAIT = "wait_for_active_run"
NEXT_NONE = "none"


# ---------------------------------------------------------------------------
# Credential / availability
# ---------------------------------------------------------------------------

def _get_cursor_api_key() -> str:
    try:
        from agent.secret_scope import get_secret

        key = get_secret("CURSOR_API_KEY", "") or ""
    except Exception:
        import os

        key = os.getenv("CURSOR_API_KEY", "") or ""
    return str(key).strip()


def check_cursor_cloud_bridge_available() -> bool:
    """True when a Cursor API key is configured (service gate)."""
    return bool(_get_cursor_api_key())


# ---------------------------------------------------------------------------
# Typed technical error contract (never manual Cursor UI instructions)
# ---------------------------------------------------------------------------

def technical_block(
    error_type: str,
    message: str,
    *,
    status_code: Optional[int] = None,
    **extra: Any,
) -> str:
    """Return a precise typed technical block; never user-actionable UI copy."""
    payload: Dict[str, Any] = {
        "error_type": error_type,
        "user_actionable": False,
        "manual_action_required": False,
        "success": False,
    }
    if status_code is not None:
        payload["status_code"] = status_code
    payload.update(extra)
    return tool_error(message, **payload)


def _looks_like_manual_ui_instruction(text: str) -> bool:
    lowered = (text or "").lower()
    needles = (
        "open cursor",
        "click",
        "in the ui",
        "in the dashboard",
        "manually start",
        "go to cursor.com",
        "visit https://cursor.com",
        "open the agents window",
    )
    return any(n in lowered for n in needles)


# ---------------------------------------------------------------------------
# Durable dispatch store
# ---------------------------------------------------------------------------

def _db_path():
    return get_hermes_home() / "state.db"


def _connect() -> sqlite3.Connection:
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        _initialize_schema(conn)
    except Exception:
        conn.close()
        raise
    return conn


def _initialize_schema(conn: sqlite3.Connection) -> None:
    from hermes_state import apply_wal_with_fallback

    apply_wal_with_fallback(conn, db_label="state.db (cursor_cloud_bridge)")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS cursor_cloud_dispatches (
            dispatch_id TEXT PRIMARY KEY,
            hermes_job_id TEXT NOT NULL DEFAULT '',
            hermes_session_id TEXT NOT NULL DEFAULT '',
            hermes_thread_id TEXT NOT NULL DEFAULT '',
            origin_session_key TEXT NOT NULL DEFAULT '',
            cursor_agent_id TEXT NOT NULL DEFAULT '',
            cursor_run_id TEXT NOT NULL DEFAULT '',
            cursor_agent_url TEXT NOT NULL DEFAULT '',
            environment_name TEXT NOT NULL DEFAULT '',
            repository_url TEXT NOT NULL DEFAULT '',
            starting_ref TEXT NOT NULL DEFAULT '',
            phase TEXT NOT NULL DEFAULT '',
            next_action TEXT NOT NULL DEFAULT '',
            prompt_text TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            completed_at REAL,
            result_json TEXT,
            meta_json TEXT
        )"""
    )


@contextmanager
def _transaction() -> Iterator[sqlite3.Connection]:
    with _DB_LOCK:
        conn = _connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    data = dict(row)
    if data.get("meta_json"):
        try:
            data["meta"] = json.loads(data["meta_json"])
        except (TypeError, ValueError):
            data["meta"] = {}
    else:
        data["meta"] = {}
    if data.get("result_json"):
        try:
            data["result"] = json.loads(data["result_json"])
        except (TypeError, ValueError):
            data["result"] = None
    else:
        data["result"] = None
    return data


def create_dispatch_record(
    *,
    hermes_job_id: str = "",
    hermes_session_id: str = "",
    hermes_thread_id: str = "",
    origin_session_key: str = "",
    environment_name: str = "",
    repository_url: str = "",
    starting_ref: str = "",
    prompt_text: str = "",
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    now = time.time()
    dispatch_id = f"ccd-{uuid.uuid4()}"
    record = {
        "dispatch_id": dispatch_id,
        "hermes_job_id": hermes_job_id or "",
        "hermes_session_id": hermes_session_id or "",
        "hermes_thread_id": hermes_thread_id or "",
        "origin_session_key": origin_session_key or "",
        "cursor_agent_id": "",
        "cursor_run_id": "",
        "cursor_agent_url": "",
        "environment_name": environment_name or "",
        "repository_url": repository_url or "",
        "starting_ref": starting_ref or "",
        "phase": PHASE_DISPATCHED,
        "next_action": NEXT_POLL,
        "prompt_text": prompt_text or "",
        "status": "pending",
        "created_at": now,
        "updated_at": now,
        "completed_at": None,
        "result_json": None,
        "meta_json": json.dumps(meta or {}, ensure_ascii=False),
    }
    with _transaction() as conn:
        conn.execute(
            """INSERT INTO cursor_cloud_dispatches (
                dispatch_id, hermes_job_id, hermes_session_id, hermes_thread_id,
                origin_session_key, cursor_agent_id, cursor_run_id, cursor_agent_url,
                environment_name, repository_url, starting_ref, phase, next_action,
                prompt_text, status, created_at, updated_at, completed_at,
                result_json, meta_json
            ) VALUES (
                :dispatch_id, :hermes_job_id, :hermes_session_id, :hermes_thread_id,
                :origin_session_key, :cursor_agent_id, :cursor_run_id, :cursor_agent_url,
                :environment_name, :repository_url, :starting_ref, :phase, :next_action,
                :prompt_text, :status, :created_at, :updated_at, :completed_at,
                :result_json, :meta_json
            )""",
            record,
        )
    return get_dispatch_record(dispatch_id) or record


def update_dispatch_record(dispatch_id: str, **fields: Any) -> Optional[Dict[str, Any]]:
    if not dispatch_id:
        return None
    allowed = {
        "cursor_agent_id",
        "cursor_run_id",
        "cursor_agent_url",
        "phase",
        "next_action",
        "status",
        "completed_at",
        "result_json",
        "meta_json",
        "environment_name",
        "repository_url",
        "starting_ref",
        "prompt_text",
        "hermes_job_id",
        "hermes_session_id",
        "hermes_thread_id",
        "origin_session_key",
    }
    updates = {k: v for k, v in fields.items() if k in allowed}
    if "meta" in fields and "meta_json" not in updates:
        updates["meta_json"] = json.dumps(fields["meta"] or {}, ensure_ascii=False)
    if "result" in fields and "result_json" not in updates:
        updates["result_json"] = json.dumps(fields["result"], ensure_ascii=False)
    if not updates:
        return get_dispatch_record(dispatch_id)
    updates["updated_at"] = time.time()
    assignments = ", ".join(f"{k} = :{k}" for k in updates)
    updates["dispatch_id"] = dispatch_id
    with _transaction() as conn:
        conn.execute(
            f"UPDATE cursor_cloud_dispatches SET {assignments} WHERE dispatch_id = :dispatch_id",
            updates,
        )
    return get_dispatch_record(dispatch_id)


def get_dispatch_record(dispatch_id: str) -> Optional[Dict[str, Any]]:
    if not dispatch_id:
        return None
    with _transaction() as conn:
        row = conn.execute(
            "SELECT * FROM cursor_cloud_dispatches WHERE dispatch_id = ?",
            (dispatch_id,),
        ).fetchone()
    return _row_to_dict(row) if row else None


def find_dispatch_by_cursor_ids(
    *,
    cursor_agent_id: str = "",
    cursor_run_id: str = "",
) -> Optional[Dict[str, Any]]:
    clauses = []
    params: list[Any] = []
    if cursor_agent_id:
        clauses.append("cursor_agent_id = ?")
        params.append(cursor_agent_id)
    if cursor_run_id:
        clauses.append("cursor_run_id = ?")
        params.append(cursor_run_id)
    if not clauses:
        return None
    sql = "SELECT * FROM cursor_cloud_dispatches WHERE " + " AND ".join(clauses)
    sql += " ORDER BY updated_at DESC LIMIT 1"
    with _transaction() as conn:
        row = conn.execute(sql, params).fetchone()
    return _row_to_dict(row) if row else None


# ---------------------------------------------------------------------------
# Cursor HTTP client (pure helpers — easy to unit test)
# ---------------------------------------------------------------------------

def build_create_agent_payload(
    *,
    prompt: str,
    repository_url: str = "",
    starting_ref: str = "",
    environment_name: str = "",
    name: str = "",
    model: str = "",
) -> Dict[str, Any]:
    """Build the Cursor v1 Create Agent request body.

    Named cloud environments are mutually exclusive with explicit ``repos``.
    When ``environment_name`` is set we pass ``env: {type:'cloud', name}`` and
    omit repos. Otherwise we preserve ``repository_url`` + ``starting_ref``.
    """
    payload: Dict[str, Any] = {"prompt": {"text": prompt}}
    if name:
        payload["name"] = name[:100]
    if model:
        payload["model"] = {"id": model}
    env_name = (environment_name or "").strip()
    if env_name:
        payload["env"] = {"type": "cloud", "name": env_name}
        return payload
    repo = (repository_url or "").strip()
    if repo:
        entry: Dict[str, Any] = {"url": repo}
        ref = (starting_ref or "").strip()
        if ref:
            entry["startingRef"] = ref
        payload["repos"] = [entry]
    return payload


def build_followup_run_payload(*, prompt: str) -> Dict[str, Any]:
    return {"prompt": {"text": prompt}}


def agent_url_for(agent_id: str, explicit_url: str = "") -> str:
    if explicit_url:
        return explicit_url
    if not agent_id:
        return ""
    return f"{CURSOR_AGENT_URL_PREFIX}{agent_id}"


def map_create_agent_result(api_response: Dict[str, Any]) -> Dict[str, Any]:
    """Map Cursor create-agent JSON into the bridge result shape."""
    agent = api_response.get("agent") or {}
    run = api_response.get("run") or {}
    agent_id = agent.get("id") or ""
    run_id = run.get("id") or agent.get("latestRunId") or ""
    url = agent_url_for(agent_id, agent.get("url") or "")
    return {
        "success": True,
        "path": "create_agent",
        "cursor_agent_id": agent_id,
        "cursor_run_id": run_id,
        "cursor_agent_url": url,
        "agent_status": agent.get("status"),
        "run_status": run.get("status"),
        "phase": PHASE_RUNNING,
        "next_action": NEXT_POLL,
    }


def map_followup_run_result(
    *,
    agent: Dict[str, Any],
    run_response: Dict[str, Any],
) -> Dict[str, Any]:
    run = run_response.get("run") or run_response
    agent_id = agent.get("id") or run.get("agentId") or ""
    run_id = run.get("id") or ""
    url = agent_url_for(agent_id, agent.get("url") or "")
    return {
        "success": True,
        "path": "existing_agent",
        "cursor_agent_id": agent_id,
        "cursor_run_id": run_id,
        "cursor_agent_url": url,
        "agent_status": agent.get("status"),
        "run_status": run.get("status"),
        "phase": PHASE_RUNNING,
        "next_action": NEXT_POLL,
    }


def parse_cursor_api_error(response: requests.Response) -> Tuple[str, str, Dict[str, Any]]:
    """Return (error_type, message, details) from a Cursor API error response."""
    details: Dict[str, Any] = {}
    body: Any = None
    try:
        body = response.json()
    except Exception:
        body = None
    if isinstance(body, dict):
        details["api_error"] = body
        err = body.get("error")
        if isinstance(err, dict):
            code = str(err.get("code") or err.get("type") or "").strip()
            msg = str(err.get("message") or err.get("msg") or "").strip()
        else:
            code = str(body.get("code") or body.get("error_type") or "").strip()
            msg = str(body.get("message") or body.get("error") or "").strip()
    else:
        code = ""
        msg = (response.text or "").strip()[:500]

    status = response.status_code
    code_l = code.lower()
    if status == 409 or code_l in {"agent_busy", "busy"}:
        return (
            "agent_busy",
            msg or "Cursor agent already has an active run (agent_busy).",
            details,
        )
    if status in (401, 403):
        return ("auth_error", msg or f"Cursor API authentication failed ({status}).", details)
    if status == 404:
        return ("not_found", msg or "Cursor agent or run not found.", details)
    if status >= 500:
        return ("preflight_error", msg or f"Cursor API server error ({status}).", details)
    return (
        "api_error",
        msg or f"Cursor API request failed ({status}).",
        details,
    )


class CursorCloudClient:
    """Thin Cursor Cloud Agents v1 HTTP client."""

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = CURSOR_API_BASE,
        session: Optional[requests.Session] = None,
        timeout: float = 60.0,
    ):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.timeout = timeout

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def request(self, method: str, path: str, *, json_body: Any = None) -> requests.Response:
        url = f"{self.base_url}{path}"
        return self.session.request(
            method,
            url,
            headers=self._headers(),
            json=json_body,
            timeout=self.timeout,
        )

    def create_agent(self, payload: Dict[str, Any]) -> requests.Response:
        return self.request("POST", "/v1/agents", json_body=payload)

    def get_agent(self, agent_id: str) -> requests.Response:
        return self.request("GET", f"/v1/agents/{agent_id}")

    def create_run(self, agent_id: str, payload: Dict[str, Any]) -> requests.Response:
        return self.request("POST", f"/v1/agents/{agent_id}/runs", json_body=payload)

    def get_run(self, agent_id: str, run_id: str) -> requests.Response:
        return self.request("GET", f"/v1/agents/{agent_id}/runs/{run_id}")


def _agent_has_active_run(agent: Dict[str, Any], client: CursorCloudClient) -> bool:
    """Best-effort busy probe using latestRunId + GET run status."""
    latest = agent.get("latestRunId") or ""
    agent_id = agent.get("id") or ""
    if not latest or not agent_id:
        return False
    resp = client.get_run(agent_id, latest)
    if resp.status_code != 200:
        return False
    try:
        data = resp.json()
    except Exception:
        return False
    run = data.get("run") if isinstance(data, dict) and "run" in data else data
    status = str((run or {}).get("status") or "").upper()
    return status in _ACTIVE_RUN_STATUSES


# ---------------------------------------------------------------------------
# Completion → resume same job (never a fresh chat task)
# ---------------------------------------------------------------------------

def build_completion_event(record: Dict[str, Any], *, result: Any = None) -> Dict[str, Any]:
    """Build a completion-queue event that resumes the SAME Hermes job."""
    return {
        "type": "cursor_cloud",
        "dispatch_id": record.get("dispatch_id"),
        "hermes_job_id": record.get("hermes_job_id") or "",
        "hermes_session_id": record.get("hermes_session_id") or "",
        "hermes_thread_id": record.get("hermes_thread_id") or "",
        "origin_session_key": record.get("origin_session_key") or "",
        "session_id": record.get("hermes_session_id") or record.get("origin_session_key") or "",
        "cursor_agent_id": record.get("cursor_agent_id") or "",
        "cursor_run_id": record.get("cursor_run_id") or "",
        "cursor_agent_url": record.get("cursor_agent_url") or "",
        "phase": PHASE_COMPLETED,
        "next_action": NEXT_RESUME,
        "resume_same_job": True,
        "fresh_chat": False,
        "result": result,
        "completed_at": time.time(),
    }


def publish_completion_for_resume(record: Dict[str, Any], *, result: Any = None) -> Dict[str, Any]:
    """Persist completed state and enqueue resume on the shared completion rail."""
    event = build_completion_event(record, result=result)
    update_dispatch_record(
        record["dispatch_id"],
        phase=PHASE_COMPLETED,
        next_action=NEXT_RESUME,
        status="completed",
        completed_at=event["completed_at"],
        result=result,
    )
    try:
        from tools.process_registry import process_registry

        process_registry.completion_queue.put(event)
    except Exception as exc:
        logger.warning("cursor_cloud completion enqueue failed: %s", exc)
    return event


def format_cursor_cloud_completion(evt: dict) -> str:
    """Format a cursor_cloud completion for re-injection into the SAME job."""
    dispatch_id = evt.get("dispatch_id", "unknown")
    job_id = evt.get("hermes_job_id") or ""
    agent_id = evt.get("cursor_agent_id") or ""
    run_id = evt.get("cursor_run_id") or ""
    url = evt.get("cursor_agent_url") or agent_url_for(agent_id)
    lines = [
        f"[CURSOR CLOUD JOB COMPLETE — {dispatch_id}]",
        "A Cursor Cloud agent run correlated to this Hermes job finished.",
        "Resume the SAME job using the correlation below — do not start a fresh chat task.",
        "",
        f"hermes_job_id: {job_id or '(none)'}",
        f"hermes_session_id: {evt.get('hermes_session_id') or '(none)'}",
        f"hermes_thread_id: {evt.get('hermes_thread_id') or '(none)'}",
        f"cursor_agent_id: {agent_id}",
        f"cursor_run_id: {run_id}",
        f"cursor_agent_url: {url}",
        f"phase: {evt.get('phase') or PHASE_COMPLETED}",
        f"next_action: {evt.get('next_action') or NEXT_RESUME}",
        "resume_same_job: true",
        "fresh_chat: false",
    ]
    result = evt.get("result")
    if result is not None:
        lines.append("--- RESULT ---")
        if isinstance(result, (dict, list)):
            lines.append(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            lines.append(str(result))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _validate_repository_url(repository_url: str) -> Optional[str]:
    """Return a technical_block string on invalid/local origin, else None."""
    raw = (repository_url or "").strip()
    if not raw:
        return None
    if _LOCAL_ORIGIN_RE.match(raw) or raw.startswith("~"):
        return technical_block(
            "local_origin",
            "repository_url must be a remote GitHub URL; local filesystem origins are not supported by the Cursor Cloud bridge.",
            repository_url=raw,
        )
    parsed = urlparse(raw)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return technical_block(
            "local_origin",
            "repository_url is not a valid remote http(s) GitHub URL.",
            repository_url=raw,
        )
    return None


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------

def cursor_cloud_dispatch(
    prompt: str,
    *,
    repository_url: str = "",
    starting_ref: str = "",
    environment_name: str = "",
    existing_agent_id: str = "",
    name: str = "",
    model: str = "",
    hermes_job_id: str = "",
    hermes_session_id: str = "",
    hermes_thread_id: str = "",
    origin_session_key: str = "",
    client: Optional[CursorCloudClient] = None,
) -> str:
    """Start a new Cursor Cloud agent job, or continue an existing agent."""
    api_key = _get_cursor_api_key()
    if not api_key and client is None:
        return technical_block(
            "bridge_unavailable",
            "Cursor Cloud bridge unavailable: CURSOR_API_KEY is not configured.",
        )

    prompt = (prompt or "").strip()
    if not prompt:
        return technical_block(
            "invalid_argument",
            "prompt is required.",
        )

    existing_id = (existing_agent_id or "").strip()
    env_name = (environment_name or "").strip()
    repo = (repository_url or "").strip()
    ref = (starting_ref or "").strip()

    if existing_id and (env_name or repo):
        # Continuation is identity-based; ignore create-only fields silently
        # but do not invent a second agent.
        pass

    if not existing_id:
        local_err = _validate_repository_url(repo)
        if local_err:
            return local_err
        if not env_name and not repo:
            return technical_block(
                "invalid_argument",
                "new jobs require environment_name or repository_url.",
            )

    http = client or CursorCloudClient(api_key)

    record = create_dispatch_record(
        hermes_job_id=hermes_job_id,
        hermes_session_id=hermes_session_id,
        hermes_thread_id=hermes_thread_id,
        origin_session_key=origin_session_key,
        environment_name=env_name if not existing_id else "",
        repository_url=repo if not existing_id else "",
        starting_ref=ref if not existing_id else "",
        prompt_text=prompt,
        meta={
            "existing_agent_id": existing_id,
            "name": name,
            "model": model,
        },
    )
    dispatch_id = record["dispatch_id"]

    try:
        if existing_id:
            return _dispatch_existing_agent(
                http,
                dispatch_id=dispatch_id,
                existing_agent_id=existing_id,
                prompt=prompt,
            )
        return _dispatch_new_agent(
            http,
            dispatch_id=dispatch_id,
            prompt=prompt,
            repository_url=repo,
            starting_ref=ref,
            environment_name=env_name,
            name=name,
            model=model,
        )
    except requests.Timeout:
        update_dispatch_record(
            dispatch_id,
            phase=PHASE_ERROR,
            next_action=NEXT_NONE,
            status="error",
        )
        return technical_block(
            "preflight_error",
            "Cursor Cloud API request timed out.",
            dispatch_id=dispatch_id,
        )
    except requests.RequestException as exc:
        update_dispatch_record(
            dispatch_id,
            phase=PHASE_ERROR,
            next_action=NEXT_NONE,
            status="error",
        )
        msg = f"Cursor Cloud API transport error: {exc}"
        if _looks_like_manual_ui_instruction(msg):
            msg = "Cursor Cloud API transport error."
        return technical_block(
            "preflight_error",
            msg,
            dispatch_id=dispatch_id,
        )


def _dispatch_new_agent(
    client: CursorCloudClient,
    *,
    dispatch_id: str,
    prompt: str,
    repository_url: str,
    starting_ref: str,
    environment_name: str,
    name: str,
    model: str,
) -> str:
    payload = build_create_agent_payload(
        prompt=prompt,
        repository_url=repository_url,
        starting_ref=starting_ref,
        environment_name=environment_name,
        name=name,
        model=model,
    )
    resp = client.create_agent(payload)
    if resp.status_code >= 400:
        error_type, message, details = parse_cursor_api_error(resp)
        update_dispatch_record(
            dispatch_id,
            phase=PHASE_ERROR,
            next_action=NEXT_NONE,
            status="error",
            result={"error_type": error_type, "details": details},
        )
        if _looks_like_manual_ui_instruction(message):
            message = f"Cursor Create Agent failed ({error_type})."
        return technical_block(
            error_type if error_type != "agent_busy" else "preflight_error",
            message,
            status_code=resp.status_code,
            dispatch_id=dispatch_id,
            request_path="create_agent",
        )

    try:
        api_json = resp.json()
    except Exception:
        update_dispatch_record(
            dispatch_id,
            phase=PHASE_ERROR,
            next_action=NEXT_NONE,
            status="error",
        )
        return technical_block(
            "preflight_error",
            "Cursor Create Agent returned a non-JSON response.",
            status_code=resp.status_code,
            dispatch_id=dispatch_id,
        )

    mapped = map_create_agent_result(api_json)
    if not mapped.get("cursor_agent_id") or not mapped.get("cursor_agent_url"):
        update_dispatch_record(
            dispatch_id,
            phase=PHASE_ERROR,
            next_action=NEXT_NONE,
            status="error",
        )
        return technical_block(
            "preflight_error",
            "Cursor Create Agent response missing agent id or url.",
            dispatch_id=dispatch_id,
        )

    persisted = update_dispatch_record(
        dispatch_id,
        cursor_agent_id=mapped["cursor_agent_id"],
        cursor_run_id=mapped["cursor_run_id"],
        cursor_agent_url=mapped["cursor_agent_url"],
        phase=mapped["phase"],
        next_action=mapped["next_action"],
        status="running",
    ) or {}
    return tool_result(
        {
            **mapped,
            "dispatch_id": dispatch_id,
            "hermes_job_id": persisted.get("hermes_job_id"),
            "hermes_session_id": persisted.get("hermes_session_id"),
            "hermes_thread_id": persisted.get("hermes_thread_id"),
            "resume_same_job": True,
            "request_payload": payload,
        }
    )


def _dispatch_existing_agent(
    client: CursorCloudClient,
    *,
    dispatch_id: str,
    existing_agent_id: str,
    prompt: str,
) -> str:
    get_resp = client.get_agent(existing_agent_id)
    if get_resp.status_code >= 400:
        error_type, message, details = parse_cursor_api_error(get_resp)
        update_dispatch_record(
            dispatch_id,
            phase=PHASE_ERROR,
            next_action=NEXT_NONE,
            status="error",
            cursor_agent_id=existing_agent_id,
            cursor_agent_url=agent_url_for(existing_agent_id),
            result={"error_type": error_type, "details": details},
        )
        if _looks_like_manual_ui_instruction(message):
            message = f"Cursor GET agent failed ({error_type})."
        return technical_block(
            error_type,
            message,
            status_code=get_resp.status_code,
            dispatch_id=dispatch_id,
            cursor_agent_id=existing_agent_id,
            cursor_agent_url=agent_url_for(existing_agent_id),
            request_path="get_agent",
        )

    try:
        agent = get_resp.json()
    except Exception:
        update_dispatch_record(
            dispatch_id,
            phase=PHASE_ERROR,
            next_action=NEXT_NONE,
            status="error",
            cursor_agent_id=existing_agent_id,
            cursor_agent_url=agent_url_for(existing_agent_id),
        )
        return technical_block(
            "preflight_error",
            "Cursor GET agent returned a non-JSON response.",
            status_code=get_resp.status_code,
            dispatch_id=dispatch_id,
            cursor_agent_id=existing_agent_id,
            cursor_agent_url=agent_url_for(existing_agent_id),
        )

    if not isinstance(agent, dict):
        return technical_block(
            "preflight_error",
            "Cursor GET agent returned an unexpected payload.",
            dispatch_id=dispatch_id,
            cursor_agent_id=existing_agent_id,
            cursor_agent_url=agent_url_for(existing_agent_id),
        )

    agent_url = agent_url_for(existing_agent_id, agent.get("url") or "")
    update_dispatch_record(
        dispatch_id,
        cursor_agent_id=existing_agent_id,
        cursor_agent_url=agent_url,
    )

    # Only one active run allowed — probe before POST when possible.
    try:
        if _agent_has_active_run(agent, client):
            update_dispatch_record(
                dispatch_id,
                phase=PHASE_BUSY,
                next_action=NEXT_WAIT,
                status="agent_busy",
                cursor_run_id=agent.get("latestRunId") or "",
            )
            return technical_block(
                "agent_busy",
                "Cursor agent already has an active run; only one run is allowed at a time.",
                status_code=409,
                dispatch_id=dispatch_id,
                cursor_agent_id=existing_agent_id,
                cursor_run_id=agent.get("latestRunId") or "",
                cursor_agent_url=agent_url,
                phase=PHASE_BUSY,
                next_action=NEXT_WAIT,
            )
    except requests.RequestException:
        # Fall through to POST; server remains authoritative for agent_busy.
        pass

    run_payload = build_followup_run_payload(prompt=prompt)
    run_resp = client.create_run(existing_agent_id, run_payload)
    if run_resp.status_code >= 400:
        error_type, message, details = parse_cursor_api_error(run_resp)
        if error_type == "agent_busy":
            update_dispatch_record(
                dispatch_id,
                phase=PHASE_BUSY,
                next_action=NEXT_WAIT,
                status="agent_busy",
                result={"error_type": error_type, "details": details},
            )
            return technical_block(
                "agent_busy",
                message,
                status_code=run_resp.status_code,
                dispatch_id=dispatch_id,
                cursor_agent_id=existing_agent_id,
                cursor_agent_url=agent_url,
                phase=PHASE_BUSY,
                next_action=NEXT_WAIT,
            )
        update_dispatch_record(
            dispatch_id,
            phase=PHASE_ERROR,
            next_action=NEXT_NONE,
            status="error",
            result={"error_type": error_type, "details": details},
        )
        if _looks_like_manual_ui_instruction(message):
            message = f"Cursor create-run failed ({error_type})."
        return technical_block(
            error_type,
            message,
            status_code=run_resp.status_code,
            dispatch_id=dispatch_id,
            cursor_agent_id=existing_agent_id,
            cursor_agent_url=agent_url,
            request_path="create_run",
        )

    try:
        run_json = run_resp.json()
    except Exception:
        update_dispatch_record(
            dispatch_id,
            phase=PHASE_ERROR,
            next_action=NEXT_NONE,
            status="error",
        )
        return technical_block(
            "preflight_error",
            "Cursor create-run returned a non-JSON response.",
            status_code=run_resp.status_code,
            dispatch_id=dispatch_id,
            cursor_agent_id=existing_agent_id,
            cursor_agent_url=agent_url,
        )

    mapped = map_followup_run_result(agent=agent, run_response=run_json)
    update_dispatch_record(
        dispatch_id,
        cursor_agent_id=mapped["cursor_agent_id"],
        cursor_run_id=mapped["cursor_run_id"],
        cursor_agent_url=mapped["cursor_agent_url"],
        phase=mapped["phase"],
        next_action=mapped["next_action"],
        status="running",
    )
    persisted = get_dispatch_record(dispatch_id) or {}
    return tool_result(
        {
            **mapped,
            "dispatch_id": dispatch_id,
            "hermes_job_id": persisted.get("hermes_job_id"),
            "hermes_session_id": persisted.get("hermes_session_id"),
            "hermes_thread_id": persisted.get("hermes_thread_id"),
            "resume_same_job": True,
            "request_payload": run_payload,
        }
    )


def cursor_cloud_status(
    *,
    dispatch_id: str = "",
    cursor_agent_id: str = "",
    cursor_run_id: str = "",
    publish_completion: bool = True,
    client: Optional[CursorCloudClient] = None,
) -> str:
    """Poll Cursor run status; on terminal success, resume the same Hermes job."""
    api_key = _get_cursor_api_key()
    if not api_key and client is None:
        return technical_block(
            "bridge_unavailable",
            "Cursor Cloud bridge unavailable: CURSOR_API_KEY is not configured.",
        )

    record = None
    if dispatch_id:
        record = get_dispatch_record(dispatch_id)
    if record is None and (cursor_agent_id or cursor_run_id):
        record = find_dispatch_by_cursor_ids(
            cursor_agent_id=cursor_agent_id,
            cursor_run_id=cursor_run_id,
        )
    if record is None:
        return technical_block(
            "not_found",
            "No durable Cursor Cloud dispatch record matched the given identifiers.",
            dispatch_id=dispatch_id or "",
            cursor_agent_id=cursor_agent_id or "",
            cursor_run_id=cursor_run_id or "",
        )

    agent_id = cursor_agent_id or record.get("cursor_agent_id") or ""
    run_id = cursor_run_id or record.get("cursor_run_id") or ""
    if not agent_id or not run_id:
        return technical_block(
            "preflight_error",
            "Dispatch record is missing cursor_agent_id or cursor_run_id.",
            dispatch_id=record.get("dispatch_id"),
            cursor_agent_url=record.get("cursor_agent_url") or "",
        )

    http = client or CursorCloudClient(api_key)
    try:
        resp = http.get_run(agent_id, run_id)
    except requests.Timeout:
        return technical_block(
            "preflight_error",
            "Cursor Cloud get-run timed out.",
            dispatch_id=record.get("dispatch_id"),
            cursor_agent_id=agent_id,
            cursor_run_id=run_id,
            cursor_agent_url=record.get("cursor_agent_url") or agent_url_for(agent_id),
        )
    except requests.RequestException as exc:
        return technical_block(
            "preflight_error",
            f"Cursor Cloud get-run transport error: {exc}",
            dispatch_id=record.get("dispatch_id"),
            cursor_agent_id=agent_id,
            cursor_run_id=run_id,
            cursor_agent_url=record.get("cursor_agent_url") or agent_url_for(agent_id),
        )

    if resp.status_code >= 400:
        error_type, message, details = parse_cursor_api_error(resp)
        if _looks_like_manual_ui_instruction(message):
            message = f"Cursor get-run failed ({error_type})."
        return technical_block(
            error_type,
            message,
            status_code=resp.status_code,
            dispatch_id=record.get("dispatch_id"),
            cursor_agent_id=agent_id,
            cursor_run_id=run_id,
            cursor_agent_url=record.get("cursor_agent_url") or agent_url_for(agent_id),
            details=details,
        )

    try:
        data = resp.json()
    except Exception:
        return technical_block(
            "preflight_error",
            "Cursor get-run returned a non-JSON response.",
            dispatch_id=record.get("dispatch_id"),
            cursor_agent_id=agent_id,
            cursor_run_id=run_id,
            cursor_agent_url=record.get("cursor_agent_url") or agent_url_for(agent_id),
        )

    run = data.get("run") if isinstance(data, dict) and "run" in data else data
    status = str((run or {}).get("status") or "").upper()
    url = record.get("cursor_agent_url") or agent_url_for(agent_id)
    base = {
        "success": True,
        "dispatch_id": record.get("dispatch_id"),
        "hermes_job_id": record.get("hermes_job_id"),
        "hermes_session_id": record.get("hermes_session_id"),
        "hermes_thread_id": record.get("hermes_thread_id"),
        "cursor_agent_id": agent_id,
        "cursor_run_id": run_id,
        "cursor_agent_url": url,
        "run_status": status,
        "resume_same_job": True,
        "fresh_chat": False,
    }

    if status in _ACTIVE_RUN_STATUSES:
        update_dispatch_record(
            record["dispatch_id"],
            phase=PHASE_RUNNING,
            next_action=NEXT_POLL,
            status="running",
        )
        return tool_result({**base, "phase": PHASE_RUNNING, "next_action": NEXT_POLL})

    if status == "FINISHED":
        result_text = (run or {}).get("result")
        if publish_completion and record.get("phase") != PHASE_COMPLETED:
            publish_completion_for_resume(record, result=result_text)
        else:
            update_dispatch_record(
                record["dispatch_id"],
                phase=PHASE_COMPLETED,
                next_action=NEXT_RESUME,
                status="completed",
                completed_at=time.time(),
                result=result_text,
            )
        return tool_result(
            {
                **base,
                "phase": PHASE_COMPLETED,
                "next_action": NEXT_RESUME,
                "result": result_text,
            }
        )

    # ERROR / CANCELLED / EXPIRED / unknown terminal
    update_dispatch_record(
        record["dispatch_id"],
        phase=PHASE_ERROR,
        next_action=NEXT_RESUME,
        status=status.lower() or "error",
        completed_at=time.time(),
        result=run,
    )
    return tool_result(
        {
            **base,
            "phase": PHASE_ERROR,
            "next_action": NEXT_RESUME,
            "result": run,
        }
    )


# ---------------------------------------------------------------------------
# Registry handlers + schemas
# ---------------------------------------------------------------------------

def _handle_dispatch(args: dict, **kwargs) -> str:
    return cursor_cloud_dispatch(
        prompt=str(args.get("prompt") or ""),
        repository_url=str(args.get("repository_url") or ""),
        starting_ref=str(args.get("starting_ref") or ""),
        environment_name=str(args.get("environment_name") or ""),
        existing_agent_id=str(args.get("existing_agent_id") or ""),
        name=str(args.get("name") or ""),
        model=str(args.get("model") or ""),
        hermes_job_id=str(args.get("hermes_job_id") or kwargs.get("task_id") or ""),
        hermes_session_id=str(
            args.get("hermes_session_id")
            or kwargs.get("session_id")
            or ""
        ),
        hermes_thread_id=str(args.get("hermes_thread_id") or kwargs.get("thread_id") or ""),
        origin_session_key=str(
            args.get("origin_session_key")
            or kwargs.get("gateway_session_key")
            or ""
        ),
    )


def _handle_status(args: dict, **kwargs) -> str:
    return cursor_cloud_status(
        dispatch_id=str(args.get("dispatch_id") or ""),
        cursor_agent_id=str(args.get("cursor_agent_id") or ""),
        cursor_run_id=str(args.get("cursor_run_id") or ""),
        publish_completion=bool(args.get("publish_completion", True)),
    )


CURSOR_CLOUD_DISPATCH_SCHEMA = {
    "name": "cursor_cloud_dispatch",
    "description": (
        "Start a NEW visible Cursor Cloud agent job, or continue an existing "
        "Cursor agent by ID. Pass environment_name to target a named Cursor "
        "Cloud environment, or repository_url (+ optional starting_ref) for a "
        "normal GitHub-ref job. Continuation requires existing_agent_id only. "
        "Always returns and persists cursor_agent_id, cursor_run_id, and the "
        "public cursor_agent_url. Completions resume the same Hermes job."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "prompt": {
                "type": "string",
                "description": "Task prompt for the Cursor Cloud agent run.",
            },
            "repository_url": {
                "type": "string",
                "description": (
                    "GitHub repository URL for a normal new-job create "
                    "(ignored when environment_name or existing_agent_id is set)."
                ),
            },
            "starting_ref": {
                "type": "string",
                "description": (
                    "Branch name or commit SHA starting point for repository_url jobs."
                ),
            },
            "environment_name": {
                "type": "string",
                "description": (
                    "Optional named Cursor Cloud environment. When set, create "
                    "passes env {type:'cloud', name: environment_name}."
                ),
            },
            "existing_agent_id": {
                "type": "string",
                "description": (
                    "Optional existing Cursor agent ID. When set, the bridge "
                    "GETs agent state then POSTs /v1/agents/{id}/runs."
                ),
            },
            "name": {
                "type": "string",
                "description": "Optional display name for a newly created agent.",
            },
            "model": {
                "type": "string",
                "description": "Optional Cursor model id for a newly created agent.",
            },
            "hermes_job_id": {
                "type": "string",
                "description": "Optional Hermes job id for durable correlation.",
            },
            "hermes_session_id": {
                "type": "string",
                "description": "Optional Hermes session id for durable correlation.",
            },
            "hermes_thread_id": {
                "type": "string",
                "description": "Optional Hermes thread id for durable correlation.",
            },
            "origin_session_key": {
                "type": "string",
                "description": "Optional gateway session key for resume routing.",
            },
        },
        "required": ["prompt"],
    },
}

CURSOR_CLOUD_STATUS_SCHEMA = {
    "name": "cursor_cloud_status",
    "description": (
        "Poll a Cursor Cloud dispatch/run. On FINISHED, marks the durable "
        "record complete and publishes a same-job resume event (not a fresh chat)."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "dispatch_id": {
                "type": "string",
                "description": "Hermes durable dispatch id returned by cursor_cloud_dispatch.",
            },
            "cursor_agent_id": {
                "type": "string",
                "description": "Cursor agent id (bc-...).",
            },
            "cursor_run_id": {
                "type": "string",
                "description": "Cursor run id (run-...).",
            },
            "publish_completion": {
                "type": "boolean",
                "description": "When true (default), enqueue a same-job resume event on FINISHED.",
            },
        },
        "required": [],
    },
}

registry.register(
    name="cursor_cloud_dispatch",
    toolset="cursor_cloud",
    schema=CURSOR_CLOUD_DISPATCH_SCHEMA,
    handler=_handle_dispatch,
    check_fn=check_cursor_cloud_bridge_available,
    requires_env=["CURSOR_API_KEY"],
    emoji="☁️",
)

registry.register(
    name="cursor_cloud_status",
    toolset="cursor_cloud",
    schema=CURSOR_CLOUD_STATUS_SCHEMA,
    handler=_handle_status,
    check_fn=check_cursor_cloud_bridge_available,
    requires_env=["CURSOR_API_KEY"],
    emoji="☁️",
)
