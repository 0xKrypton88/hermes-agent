"""Durable Slack job ↔ root-thread mapping.

Hermes creates a dedicated Slack root message for a durable job, persists the
exact ``(platform, chat_id, root_thread_ts) ↔ job_id`` binding, and routes later
status / continue / pause / help / completion traffic back to that same thread
even after process restart.

Design invariants
-----------------
1. A durable job row is written *before* any Slack root is created, with phase
   ``CREATING_THREAD`` and a caller-supplied idempotency key.
2. A durable ``create_operation_id`` (create-intent token) is persisted *before*
   the first Slack API call so the exact attempt is identifiable after restart.
3. Root creation is limited to the already-authorized ``chat_id`` (DM / channel
   the caller passed). No channel or membership mutations, no arbitrary
   destinations.
4. Double create / retry / restart with the same idempotency key never opens a
   second root. Unknown outcomes after a claimed create are fail-closed
   (``PENDING_RECOVERY``) unless the same operation can be reconciled.
5. Initial status is posted only after ``root_thread_ts`` is durably bound, and
   at most once (status-post claim).
6. Inbound job-control text is resolved via the durable mapping, not transcript
   heuristics. Unmapped (legacy) threads fall through unchanged.

Crash-window recovery path (documented)
---------------------------------------
Primary: Slack ``client_msg_id`` = ``create_operation_id`` makes the root post
idempotent for the same attempt; the seed text also embeds
``hermes_job_op=<create_operation_id>``.

On unknown outcome (Slack may have accepted the message but the client lost
the ``ts``):

1. Scan the authorized chat via ``find_job_root_by_operation_id`` for the
   exact operation marker (no new post).
2. If still unknown, one idempotent Slack retry with the *same*
   ``client_msg_id`` / operation id (not a new claim).
3. If still unknown → ``PENDING_RECOVERY`` with ``next_action=reconcile_root``
   and **no additional post**.

Cross-process safety: every store mutation takes a thread RLock plus an
advisory ``fcntl.flock`` on ``job_threads.json.lock``, and reloads from disk
under that lock before mutating (CAS-style against concurrent gateway
processes).
"""

from __future__ import annotations

import contextlib
import json
import logging
import re
import threading
import time
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterator, List, Optional, Sequence, Tuple

from hermes_constants import get_hermes_home
from utils import atomic_write_text

try:
    import fcntl
except ImportError:  # pragma: no cover - non-Unix
    fcntl = None
try:
    import msvcrt
except ImportError:  # pragma: no cover - non-Windows
    msvcrt = None

logger = logging.getLogger(__name__)

STORE_FILENAME = "job_threads.json"
STORE_LOCK_SUFFIX = ".lock"
_STORE_LOCK_TIMEOUT_SECONDS = 10.0

# Explicit lifecycle phases. CREATING_THREAD is required before any Slack call.
PHASE_CREATING_THREAD = "CREATING_THREAD"
PHASE_THREAD_READY = "THREAD_READY"
PHASE_ACTIVE = "ACTIVE"
PHASE_PAUSED = "PAUSED"
PHASE_COMPLETED = "COMPLETED"
PHASE_FAILED = "FAILED"
PHASE_PENDING_RECOVERY = "PENDING_RECOVERY"

TERMINAL_PHASES = frozenset({PHASE_COMPLETED, PHASE_FAILED})

# Job-control actions resolved from inbound text (EN + SV).
ACTION_STATUS = "status"
ACTION_CONTINUE = "continue"
ACTION_PAUSE = "pause"
ACTION_HELP = "help"
ACTION_COMPLETION = "completion"

_SUPPORTED_PLATFORMS = frozenset({"slack"})

# Exact marker embedded in Slack root seed text for recovery scans.
OPERATION_MARKER_PREFIX = "hermes_job_op="


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _thread_index_key(platform: str, chat_id: str, thread_ts: str) -> str:
    return f"{platform}:{chat_id}:{thread_ts}"


def format_operation_marker(operation_id: str) -> str:
    return f"{OPERATION_MARKER_PREFIX}{operation_id}"


def resolve_job_threads_store_path(path: str | Path | None = None) -> Path:
    if path is not None:
        explicit = str(path).strip()
        if explicit:
            return Path(explicit)
    return get_hermes_home() / STORE_FILENAME


@dataclass(frozen=True)
class JobCommandRoute:
    """Resolved inbound job-control command via durable mapping."""

    job_id: str
    action: str
    job: Dict[str, Any]


class JobThreadStore:
    """JSON-backed durable store for job ↔ Slack root-thread bindings."""

    def __init__(self, path: str | Path | None = None):
        self.path = resolve_job_threads_store_path(path)
        self._lock = threading.RLock()
        self._lock_state = threading.local()
        self._state: Dict[str, Any] = {
            "jobs": {},
            "by_idempotency": {},
            "by_thread": {},
            "pending_roots": {},
        }
        with self._store_lock():
            self._load_unlocked()

    def _lock_file_path(self) -> Path:
        return Path(str(self.path) + STORE_LOCK_SUFFIX)

    @contextlib.contextmanager
    def _store_lock(self) -> Iterator[None]:
        """Thread RLock + cross-process advisory flock; reload-safe nesting."""
        depth = getattr(self._lock_state, "depth", 0)
        if depth:
            self._lock_state.depth = depth + 1
            try:
                yield
            finally:
                self._lock_state.depth -= 1
            return

        with self._lock:
            self._lock_state.depth = 1
            lock_fd = None
            try:
                try:
                    self.path.parent.mkdir(parents=True, exist_ok=True)
                    lock_fd = open(self._lock_file_path(), "a+", encoding="utf-8")
                    lock_fd.seek(0)
                    if fcntl is not None:
                        deadline = time.monotonic() + _STORE_LOCK_TIMEOUT_SECONDS
                        while True:
                            try:
                                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                                break
                            except (OSError, IOError):
                                if time.monotonic() >= deadline:
                                    logger.error(
                                        "Timed out after %.0fs waiting for job_threads "
                                        "lock (%s); proceeding with in-process lock only",
                                        _STORE_LOCK_TIMEOUT_SECONDS,
                                        self._lock_file_path(),
                                    )
                                    try:
                                        lock_fd.close()
                                    except OSError:
                                        pass
                                    lock_fd = None
                                    break
                                time.sleep(0.05)
                    elif msvcrt is not None:
                        getattr(msvcrt, "locking")(
                            lock_fd.fileno(), getattr(msvcrt, "LK_LOCK"), 1
                        )
                except (OSError, IOError) as exc:
                    logger.warning(
                        "job_threads cross-process lock unavailable (%s); "
                        "proceeding with in-process lock only",
                        exc,
                    )
                    if lock_fd is not None:
                        try:
                            lock_fd.close()
                        except OSError:
                            pass
                        lock_fd = None
                try:
                    yield
                finally:
                    if lock_fd is not None:
                        try:
                            if fcntl is not None:
                                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                            elif msvcrt is not None:
                                getattr(msvcrt, "locking")(
                                    lock_fd.fileno(), getattr(msvcrt, "LK_UNLCK"), 1
                                )
                        except (OSError, IOError):
                            pass
                        finally:
                            lock_fd.close()
            finally:
                self._lock_state.depth = 0

    def _load_unlocked(self) -> None:
        if not self.path.exists():
            self._state = {
                "jobs": {},
                "by_idempotency": {},
                "by_thread": {},
                "pending_roots": {},
            }
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8") or "{}")
        except Exception:
            logger.warning(
                "job_threads: failed to load store at %s; starting empty",
                self.path,
                exc_info=True,
            )
            return
        if not isinstance(data, dict):
            return
        self._state["jobs"] = dict(data.get("jobs") or {})
        self._state["by_idempotency"] = dict(data.get("by_idempotency") or {})
        self._state["by_thread"] = dict(data.get("by_thread") or {})
        self._state["pending_roots"] = dict(data.get("pending_roots") or {})

    def _persist_unlocked(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(self._state, indent=2, sort_keys=True)
        atomic_write_text(self.path, payload + "\n", create_mode=0o600)

    def _snapshot_job_unlocked(self, job_id: str) -> Optional[Dict[str, Any]]:
        record = self._state["jobs"].get(job_id)
        return deepcopy(record) if isinstance(record, dict) else None

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._store_lock():
            self._load_unlocked()
            return self._snapshot_job_unlocked(job_id)

    def get_by_idempotency(self, idempotency_key: str) -> Optional[Dict[str, Any]]:
        key = str(idempotency_key or "").strip()
        if not key:
            return None
        with self._store_lock():
            self._load_unlocked()
            job_id = self._state["by_idempotency"].get(key)
            if not job_id:
                return None
            return self._snapshot_job_unlocked(str(job_id))

    def find_by_thread(
        self,
        platform: str,
        chat_id: str,
        thread_ts: str,
    ) -> Optional[Dict[str, Any]]:
        platform_n = str(platform or "").strip().lower()
        chat_n = str(chat_id or "").strip()
        ts_n = str(thread_ts or "").strip()
        if not (platform_n and chat_n and ts_n):
            return None
        key = _thread_index_key(platform_n, chat_n, ts_n)
        with self._store_lock():
            self._load_unlocked()
            job_id = self._state["by_thread"].get(key)
            if not job_id:
                pending = self._state["pending_roots"].get(key)
                if isinstance(pending, dict):
                    job_id = pending.get("job_id")
            if not job_id:
                return None
            return self._snapshot_job_unlocked(str(job_id))

    def create_job_creating_thread(
        self,
        *,
        idempotency_key: str,
        platform: str,
        chat_id: str,
        objective: str,
        next_action: str = "create_slack_root",
        job_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create or return the durable job row in ``CREATING_THREAD``.

        Idempotent on ``idempotency_key``: a retry returns the existing row
        without mutating a bound root. Serialized across processes via flock.
        """
        key = str(idempotency_key or "").strip()
        if not key:
            raise ValueError("idempotency_key is required")
        platform_n = str(platform or "").strip().lower()
        if platform_n not in _SUPPORTED_PLATFORMS:
            raise ValueError(f"unsupported platform for job threads: {platform_n!r}")
        chat_n = str(chat_id or "").strip()
        if not chat_n:
            raise ValueError("chat_id is required (authorized DM/channel only)")
        objective_n = str(objective or "").strip()
        if not objective_n:
            raise ValueError("objective is required")

        with self._store_lock():
            self._load_unlocked()
            existing_id = self._state["by_idempotency"].get(key)
            if existing_id:
                existing = self._snapshot_job_unlocked(str(existing_id))
                if existing is not None:
                    return existing

            new_id = str(job_id or uuid.uuid4().hex[:12])
            now = _utc_now_iso()
            record = {
                "job_id": new_id,
                "idempotency_key": key,
                "platform": platform_n,
                "chat_id": chat_n,
                "root_thread_ts": None,
                "objective": objective_n,
                "phase": PHASE_CREATING_THREAD,
                "next_action": str(next_action or "create_slack_root"),
                "create_attempts": 0,
                "create_operation_id": None,
                "initial_status_posted": False,
                "status_post_claimed": False,
                "created_at": now,
                "updated_at": now,
            }
            self._state["jobs"][new_id] = record
            self._state["by_idempotency"][key] = new_id
            self._persist_unlocked()
            return deepcopy(record)

    def claim_create_operation(self, job_id: str) -> Tuple[Dict[str, Any], Optional[str]]:
        """Persist a durable create-intent token *before* any Slack API call.

        Returns ``(job, operation_id)`` when the caller may perform the Slack
        root post for this fresh claim.

        Returns ``(job, None)`` when a prior claim already exists, a root is
        already bound, or the job is in ``PENDING_RECOVERY`` — the caller must
        **not** open a new root (reconcile the existing operation instead).
        """
        with self._store_lock():
            self._load_unlocked()
            job = self._state["jobs"].get(job_id)
            if not isinstance(job, dict):
                raise KeyError(f"unknown job_id: {job_id}")
            if job.get("root_thread_ts"):
                return deepcopy(job), None
            if job.get("phase") == PHASE_PENDING_RECOVERY:
                return deepcopy(job), None
            existing_op = str(job.get("create_operation_id") or "").strip()
            if existing_op:
                # Prior durable claim — do not mint a second operation id.
                return deepcopy(job), None
            operation_id = uuid.uuid4().hex
            job["create_operation_id"] = operation_id
            job["create_attempts"] = int(job.get("create_attempts") or 0) + 1
            job["phase"] = PHASE_CREATING_THREAD
            job["next_action"] = "await_slack_root"
            job["updated_at"] = _utc_now_iso()
            self._persist_unlocked()
            return deepcopy(job), operation_id

    def mark_create_attempt(self, job_id: str) -> Dict[str, Any]:
        """Backward-compatible alias: claim a create operation if needed."""
        job, _op = self.claim_create_operation(job_id)
        return job

    def bind_root_thread(
        self,
        job_id: str,
        *,
        root_thread_ts: str,
        phase: str = PHASE_THREAD_READY,
        next_action: str = "post_initial_status",
    ) -> Dict[str, Any]:
        """Atomically bind ``root_thread_ts`` and index the mapping."""
        ts = str(root_thread_ts or "").strip()
        if not ts:
            raise ValueError("root_thread_ts is required")
        with self._store_lock():
            self._load_unlocked()
            job = self._state["jobs"].get(job_id)
            if not isinstance(job, dict):
                raise KeyError(f"unknown job_id: {job_id}")
            platform = str(job["platform"])
            chat_id = str(job["chat_id"])
            existing_ts = str(job.get("root_thread_ts") or "").strip()
            if existing_ts and existing_ts != ts:
                raise ValueError(
                    f"job {job_id} already bound to root {existing_ts}; "
                    f"refusing to rebind to {ts}"
                )
            key = _thread_index_key(platform, chat_id, ts)
            owner = self._state["by_thread"].get(key)
            if owner and owner != job_id:
                raise ValueError(
                    f"thread {key} already mapped to job {owner}"
                )
            job["root_thread_ts"] = ts
            job["phase"] = phase
            job["next_action"] = next_action
            job["pending_root_thread_ts"] = None
            job["updated_at"] = _utc_now_iso()
            self._state["by_thread"][key] = job_id
            self._state["pending_roots"].pop(key, None)
            self._persist_unlocked()
            return deepcopy(job)

    def register_pending_root(
        self,
        job_id: str,
        *,
        root_thread_ts: str,
    ) -> Dict[str, Any]:
        """Record a root ts that must not be silently orphaned."""
        ts = str(root_thread_ts or "").strip()
        if not ts:
            raise ValueError("root_thread_ts is required")
        with self._store_lock():
            self._load_unlocked()
            job = self._state["jobs"].get(job_id)
            if not isinstance(job, dict):
                raise KeyError(f"unknown job_id: {job_id}")
            platform = str(job["platform"])
            chat_id = str(job["chat_id"])
            key = _thread_index_key(platform, chat_id, ts)
            self._state["pending_roots"][key] = {
                "job_id": job_id,
                "platform": platform,
                "chat_id": chat_id,
                "root_thread_ts": ts,
                "create_operation_id": job.get("create_operation_id"),
                "status": "pending",
                "updated_at": _utc_now_iso(),
            }
            job["pending_root_thread_ts"] = ts
            job["updated_at"] = _utc_now_iso()
            self._persist_unlocked()
            return deepcopy(job)

    def recover_pending_roots(self) -> List[Dict[str, Any]]:
        """Promote pending roots into durable mappings (restart reconciliation)."""
        recovered: List[Dict[str, Any]] = []
        with self._store_lock():
            self._load_unlocked()
            pending_items = list(self._state["pending_roots"].items())
        for _key, pending in pending_items:
            if not isinstance(pending, dict):
                continue
            job_id = str(pending.get("job_id") or "")
            ts = str(pending.get("root_thread_ts") or "")
            if not job_id or not ts:
                continue
            job = self.get_job(job_id)
            if job is None:
                continue
            if job.get("root_thread_ts"):
                with self._store_lock():
                    self._load_unlocked()
                    self._state["pending_roots"].pop(_key, None)
                    self._persist_unlocked()
                continue
            try:
                bound = self.bind_root_thread(
                    job_id,
                    root_thread_ts=ts,
                    phase=PHASE_THREAD_READY,
                    next_action="post_initial_status",
                )
                recovered.append(bound)
            except Exception:
                logger.warning(
                    "job_threads: failed to recover pending root for job %s",
                    job_id,
                    exc_info=True,
                )
        return recovered

    def mark_pending_recovery(
        self,
        job_id: str,
        *,
        reason: str = "unknown_create_outcome",
    ) -> Dict[str, Any]:
        """Mark a create-attempted job without a root as recoverable, not retriable."""
        with self._store_lock():
            self._load_unlocked()
            job = self._state["jobs"].get(job_id)
            if not isinstance(job, dict):
                raise KeyError(f"unknown job_id: {job_id}")
            if job.get("root_thread_ts"):
                return deepcopy(job)
            job["phase"] = PHASE_PENDING_RECOVERY
            job["next_action"] = "reconcile_root"
            job["recovery_reason"] = str(reason)
            job["updated_at"] = _utc_now_iso()
            self._persist_unlocked()
            return deepcopy(job)

    def reconcile_incomplete_creates(self) -> List[Dict[str, Any]]:
        """Restart reconciliation for jobs stuck in CREATING_THREAD.

        - Pending roots are promoted into durable mappings.
        - Jobs with a durable create-operation claim but no root are moved to
          ``PENDING_RECOVERY`` so a retry cannot open a second root.
        """
        recovered = self.recover_pending_roots()
        touched: List[Dict[str, Any]] = list(recovered)
        with self._store_lock():
            self._load_unlocked()
            job_ids = list(self._state["jobs"].keys())
        for job_id in job_ids:
            job = self.get_job(job_id)
            if job is None:
                continue
            if job.get("root_thread_ts"):
                continue
            pending_ts = str(job.get("pending_root_thread_ts") or "").strip()
            if pending_ts:
                try:
                    bound = self.bind_root_thread(
                        job_id,
                        root_thread_ts=pending_ts,
                        phase=PHASE_THREAD_READY,
                        next_action="post_initial_status",
                    )
                    touched.append(bound)
                    continue
                except Exception:
                    logger.warning(
                        "job_threads: pending_root promote failed for %s",
                        job_id,
                        exc_info=True,
                    )
            has_claim = bool(str(job.get("create_operation_id") or "").strip())
            if (
                job.get("phase") == PHASE_CREATING_THREAD
                and (has_claim or int(job.get("create_attempts") or 0) > 0)
            ):
                touched.append(
                    self.mark_pending_recovery(
                        job_id, reason="restart_with_unresolved_create_claim"
                    )
                )
        return touched

    def claim_initial_status_post(self, job_id: str) -> Tuple[Dict[str, Any], bool]:
        """Claim the right to post the initial status after a durable bind.

        Returns ``(job, True)`` only when ``root_thread_ts`` is bound and no
        prior status claim/post exists. A prior claim with no completion is
        fail-closed (``False``) to avoid duplicate status after crash.
        """
        with self._store_lock():
            self._load_unlocked()
            job = self._state["jobs"].get(job_id)
            if not isinstance(job, dict):
                raise KeyError(f"unknown job_id: {job_id}")
            if not str(job.get("root_thread_ts") or "").strip():
                return deepcopy(job), False
            if job.get("initial_status_posted"):
                return deepcopy(job), False
            if job.get("status_post_claimed"):
                # Prior claim — outcome unknown; do not post again.
                return deepcopy(job), False
            job["status_post_claimed"] = True
            job["next_action"] = "posting_initial_status"
            job["updated_at"] = _utc_now_iso()
            self._persist_unlocked()
            return deepcopy(job), True

    def update_job(
        self,
        job_id: str,
        *,
        phase: Optional[str] = None,
        next_action: Optional[str] = None,
        initial_status_posted: Optional[bool] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        with self._store_lock():
            self._load_unlocked()
            job = self._state["jobs"].get(job_id)
            if not isinstance(job, dict):
                raise KeyError(f"unknown job_id: {job_id}")
            if phase is not None:
                job["phase"] = str(phase)
            if next_action is not None:
                job["next_action"] = str(next_action)
            if initial_status_posted is not None:
                job["initial_status_posted"] = bool(initial_status_posted)
                if initial_status_posted:
                    job["status_post_claimed"] = True
            if extra:
                for k, v in extra.items():
                    if k in {
                        "job_id",
                        "idempotency_key",
                        "platform",
                        "chat_id",
                        "root_thread_ts",
                        "create_operation_id",
                    }:
                        continue
                    job[k] = v
            job["updated_at"] = _utc_now_iso()
            self._persist_unlocked()
            return deepcopy(job)

    def list_jobs(self) -> List[Dict[str, Any]]:
        with self._store_lock():
            self._load_unlocked()
            return [
                deepcopy(j) for j in self._state["jobs"].values() if isinstance(j, dict)
            ]


# Process-wide default store (tests inject via ``reset_default_store``).
_DEFAULT_STORE: Optional[JobThreadStore] = None
_DEFAULT_STORE_LOCK = threading.Lock()


def get_default_store(path: str | Path | None = None) -> JobThreadStore:
    global _DEFAULT_STORE
    with _DEFAULT_STORE_LOCK:
        if path is not None:
            return JobThreadStore(path)
        if _DEFAULT_STORE is None:
            _DEFAULT_STORE = JobThreadStore()
        return _DEFAULT_STORE


def reset_default_store() -> None:
    """Drop the process-default store (tests / profile switches)."""
    global _DEFAULT_STORE
    with _DEFAULT_STORE_LOCK:
        _DEFAULT_STORE = None


_COMMAND_PATTERNS: Sequence[tuple[str, re.Pattern[str]]] = (
    (ACTION_STATUS, re.compile(r"^\s*/?status\s*$", re.IGNORECASE)),
    (
        ACTION_CONTINUE,
        re.compile(
            r"^\s*/?(?:continue|resume|fortsätt|fortsatt)\s*$",
            re.IGNORECASE,
        ),
    ),
    (
        ACTION_PAUSE,
        re.compile(r"^\s*/?(?:pause|pausa)\s*$", re.IGNORECASE),
    ),
    (
        ACTION_HELP,
        re.compile(
            r"^\s*/?(?:help(?:\s+agent(?:en)?)?|hjälp(?:\s+agenten)?)\s*$",
            re.IGNORECASE,
        ),
    ),
    (
        ACTION_COMPLETION,
        re.compile(
            r"^\s*/?(?:completion|complete|done|klar)\s*$",
            re.IGNORECASE,
        ),
    ),
)


def classify_job_control_text(text: str) -> Optional[str]:
    """Return a job-control action for *text*, or ``None`` if not a control verb."""
    raw = (text or "").strip()
    if not raw:
        return None
    for action, pattern in _COMMAND_PATTERNS:
        if pattern.match(raw):
            return action
    return None


def route_inbound_job_command(
    *,
    platform: str,
    chat_id: str,
    thread_ts: Optional[str],
    text: str,
    store: Optional[JobThreadStore] = None,
) -> Optional[JobCommandRoute]:
    """Resolve inbound text via durable thread mapping (not transcript heuristics).

    Legacy / unmapped threads return ``None`` so normal gateway handling continues.
    """
    ts = str(thread_ts or "").strip()
    if not ts:
        return None
    action = classify_job_control_text(text)
    if action is None:
        return None
    store = store or get_default_store()
    job = store.find_by_thread(platform, chat_id, ts)
    if job is None:
        return None
    return JobCommandRoute(job_id=str(job["job_id"]), action=action, job=job)


async def _call_create_root(
    adapter: Any,
    chat_id: str,
    name: str,
    *,
    job_id: str = "",
    operation_id: str = "",
) -> Optional[str]:
    """Create a Slack root using the job/handoff primitive when present."""
    create = getattr(adapter, "create_job_root_thread", None)
    if callable(create):
        try:
            result = create(
                chat_id, name, job_id=job_id, operation_id=operation_id
            )
        except TypeError:
            try:
                result = create(chat_id, name, job_id=job_id)
            except TypeError:
                result = create(chat_id, name)
        if hasattr(result, "__await__"):
            return await result  # type: ignore[misc]
        return result
    create = getattr(adapter, "create_handoff_thread", None)
    if not callable(create):
        return None
    result = create(chat_id, name)
    if hasattr(result, "__await__"):
        return await result  # type: ignore[misc]
    return result


async def _find_root_by_operation(
    adapter: Any,
    chat_id: str,
    operation_id: str,
) -> Optional[str]:
    find = getattr(adapter, "find_job_root_by_operation_id", None)
    if not callable(find):
        return None
    try:
        result = find(chat_id, operation_id)
        if hasattr(result, "__await__"):
            result = await result  # type: ignore[misc]
        ts = str(result or "").strip()
        return ts or None
    except Exception:
        logger.debug(
            "job_threads: find_job_root_by_operation_id failed for %s",
            operation_id,
            exc_info=True,
        )
        return None


async def _invoke_create_root(
    *,
    adapter: Any,
    chat_id: str,
    name: str,
    job_id: str,
    operation_id: str,
    create_root: Optional[Callable[..., Awaitable[Optional[str]]]],
) -> Optional[str]:
    if create_root is not None:
        try:
            result = create_root(chat_id, name, operation_id=operation_id)
        except TypeError:
            result = create_root(chat_id, name)
        if hasattr(result, "__await__"):
            return await result  # type: ignore[misc]
        return result
    return await _call_create_root(
        adapter,
        chat_id,
        name,
        job_id=job_id,
        operation_id=operation_id,
    )


async def _send_thread_reply(
    adapter: Any,
    *,
    chat_id: str,
    thread_ts: str,
    content: str,
) -> Any:
    send = getattr(adapter, "send", None)
    if not callable(send):
        raise RuntimeError("adapter.send is required to post job status")
    metadata = {"thread_id": thread_ts, "thread_ts": thread_ts}
    result = send(chat_id, content, reply_to=thread_ts, metadata=metadata)
    if hasattr(result, "__await__"):
        return await result  # type: ignore[misc]
    return result


async def _resolve_root_for_operation(
    *,
    adapter: Any,
    chat_id: str,
    name: str,
    job_id: str,
    operation_id: str,
    create_root: Optional[Callable[..., Awaitable[Optional[str]]]],
    allow_slack_call: bool,
) -> Optional[str]:
    """Resolve a root ts for an existing create-operation without minting a new one.

    Order: history scan for operation marker → optional idempotent Slack call
    with the same ``client_msg_id`` / operation id.
    """
    found = await _find_root_by_operation(adapter, chat_id, operation_id)
    if found:
        return found
    if not allow_slack_call:
        return None
    return await _invoke_create_root(
        adapter=adapter,
        chat_id=chat_id,
        name=name,
        job_id=job_id,
        operation_id=operation_id,
        create_root=create_root,
    )


async def ensure_slack_job_thread(
    adapter: Any,
    *,
    idempotency_key: str,
    objective: str,
    chat_id: str,
    platform: str = "slack",
    initial_status: Optional[str] = None,
    store: Optional[JobThreadStore] = None,
    create_root: Optional[Callable[..., Awaitable[Optional[str]]]] = None,
) -> Dict[str, Any]:
    """Create (or reuse) a durable Slack job thread.

    Steps:
      1. Persist job in ``CREATING_THREAD`` under *idempotency_key*.
      2. Persist durable ``create_operation_id`` *before* any Slack call.
      3. Create the Slack root **once** (idempotent via ``client_msg_id``).
      4. Durably save ``job_id ↔ (platform, chat_id, root_thread_ts)``.
      5. Only then post the first status (once) as a reply on that thread.
    """
    store = store or get_default_store()
    platform_n = str(platform or "slack").strip().lower()
    chat_n = str(chat_id or "").strip()
    if platform_n != "slack":
        raise ValueError("ensure_slack_job_thread only supports platform='slack'")
    if not chat_n:
        raise ValueError("chat_id is required")

    # Restart reconciliation first so pending roots are never skipped.
    store.reconcile_incomplete_creates()

    job = store.create_job_creating_thread(
        idempotency_key=idempotency_key,
        platform=platform_n,
        chat_id=chat_n,
        objective=objective,
        next_action="create_slack_root",
    )
    job_id = str(job["job_id"])

    # Destination hard-lock: never redirect to another channel.
    if str(job.get("chat_id") or "") != chat_n:
        raise ValueError(
            f"job {job_id} is bound to chat_id={job.get('chat_id')!r}; "
            f"refusing create in {chat_n!r}"
        )

    root_ts = str(job.get("root_thread_ts") or "").strip()
    if not root_ts:
        pending_ts = str(job.get("pending_root_thread_ts") or "").strip()
        if pending_ts:
            job = store.bind_root_thread(
                job_id,
                root_thread_ts=pending_ts,
                phase=PHASE_THREAD_READY,
                next_action="post_initial_status",
            )
            root_ts = pending_ts

    if not root_ts:
        if job.get("phase") == PHASE_PENDING_RECOVERY:
            # Fail-closed for *new* roots: reuse the durable create_operation_id
            # only (marker scan + optional same-client_msg_id retry). Never mint
            # a second operation id.
            op_id = str(job.get("create_operation_id") or "").strip()
            if not op_id:
                return job
            name = f"Job {job_id} — {(objective or '')[:60]}"
            recovered = await _resolve_root_for_operation(
                adapter=adapter,
                chat_id=chat_n,
                name=name,
                job_id=job_id,
                operation_id=op_id,
                create_root=create_root,
                allow_slack_call=True,
            )
            if recovered:
                store.register_pending_root(job_id, root_thread_ts=recovered)
                job = store.bind_root_thread(
                    job_id,
                    root_thread_ts=recovered,
                    phase=PHASE_THREAD_READY,
                    next_action="post_initial_status",
                )
                root_ts = recovered
            else:
                return job

    if not root_ts:
        existing_op = str(job.get("create_operation_id") or "").strip()
        name = f"Job {job_id} — {(objective or '')[:60]}"

        if existing_op:
            # Durable claim already exists; outcome unknown. Reconcile the
            # same operation — never mint a second create_operation_id.
            created_ts = await _resolve_root_for_operation(
                adapter=adapter,
                chat_id=chat_n,
                name=name,
                job_id=job_id,
                operation_id=existing_op,
                create_root=create_root,
                allow_slack_call=True,
            )
            if not created_ts:
                return store.mark_pending_recovery(
                    job_id, reason="unresolved_create_operation"
                )
            store.register_pending_root(job_id, root_thread_ts=created_ts)
            job = store.bind_root_thread(
                job_id,
                root_thread_ts=created_ts,
                phase=PHASE_THREAD_READY,
                next_action="post_initial_status",
            )
            root_ts = created_ts
        else:
            job, operation_id = store.claim_create_operation(job_id)
            if not operation_id:
                # Lost the race to another process, or entered recovery.
                job = store.get_job(job_id) or job
                root_ts = str(job.get("root_thread_ts") or "").strip()
                if not root_ts:
                    op = str(job.get("create_operation_id") or "").strip()
                    if op:
                        created_ts = await _resolve_root_for_operation(
                            adapter=adapter,
                            chat_id=chat_n,
                            name=name,
                            job_id=job_id,
                            operation_id=op,
                            create_root=create_root,
                            allow_slack_call=True,
                        )
                        if created_ts:
                            store.register_pending_root(
                                job_id, root_thread_ts=created_ts
                            )
                            job = store.bind_root_thread(
                                job_id,
                                root_thread_ts=created_ts,
                                phase=PHASE_THREAD_READY,
                                next_action="post_initial_status",
                            )
                            root_ts = created_ts
                        else:
                            return store.mark_pending_recovery(
                                job_id, reason="lost_claim_race_unresolved"
                            )
                    else:
                        return store.mark_pending_recovery(
                            job_id, reason="claim_denied"
                        )
            else:
                created = await _invoke_create_root(
                    adapter=adapter,
                    chat_id=chat_n,
                    name=name,
                    job_id=job_id,
                    operation_id=operation_id,
                    create_root=create_root,
                )
                created_ts = str(created or "").strip()
                if not created_ts:
                    # Slack may have accepted the message but ts was lost.
                    created_ts = (
                        await _find_root_by_operation(
                            adapter, chat_n, operation_id
                        )
                        or ""
                    )
                if not created_ts:
                    return store.mark_pending_recovery(
                        job_id, reason="slack_create_ts_unknown"
                    )
                # Persist pending then bind *before* any status post.
                store.register_pending_root(job_id, root_thread_ts=created_ts)
                job = store.bind_root_thread(
                    job_id,
                    root_thread_ts=created_ts,
                    phase=PHASE_THREAD_READY,
                    next_action="post_initial_status",
                )
                root_ts = created_ts

    # Refresh after possible concurrent bind.
    job = store.get_job(job_id) or job
    root_ts = str(job.get("root_thread_ts") or "").strip()
    if not root_ts:
        return store.mark_pending_recovery(job_id, reason="missing_root_after_create")

    # First status reply — only after durable bind; claimed once.
    job, may_post = store.claim_initial_status_post(job_id)
    if may_post:
        status_text = (
            initial_status
            if initial_status is not None
            else f"Job `{job_id}` started — {job.get('objective')}"
        )
        await _send_thread_reply(
            adapter,
            chat_id=chat_n,
            thread_ts=root_ts,
            content=status_text,
        )
        job = store.update_job(
            job_id,
            phase=PHASE_ACTIVE,
            next_action="await_work",
            initial_status_posted=True,
        )
    elif job.get("initial_status_posted"):
        if job.get("phase") in {PHASE_THREAD_READY, PHASE_CREATING_THREAD}:
            job = store.update_job(
                job_id,
                phase=PHASE_ACTIVE,
                next_action="await_work",
            )
    else:
        # Status was claimed previously but never marked posted — fail-closed
        # to avoid a duplicate; advance phase so the job is usable.
        job = store.update_job(
            job_id,
            phase=PHASE_ACTIVE,
            next_action="await_work",
            initial_status_posted=True,
            extra={"status_post_assumed_done": True},
        )

    return job


async def post_job_update(
    adapter: Any,
    job_id: str,
    message: str,
    *,
    store: Optional[JobThreadStore] = None,
    phase: Optional[str] = None,
    next_action: Optional[str] = None,
) -> Dict[str, Any]:
    """Post a status/completion update into the job's durable Slack root thread."""
    store = store or get_default_store()
    job = store.get_job(job_id)
    if job is None:
        raise KeyError(f"unknown job_id: {job_id}")
    root_ts = str(job.get("root_thread_ts") or "").strip()
    if not root_ts:
        raise RuntimeError(f"job {job_id} has no bound root_thread_ts")
    chat_id = str(job["chat_id"])
    await _send_thread_reply(
        adapter,
        chat_id=chat_id,
        thread_ts=root_ts,
        content=message,
    )
    return store.update_job(
        job_id,
        phase=phase,
        next_action=next_action or job.get("next_action"),
    )


async def apply_job_control_action(
    adapter: Any,
    route: JobCommandRoute,
    *,
    store: Optional[JobThreadStore] = None,
) -> Dict[str, Any]:
    """Apply a routed job-control action and post a reply into the same thread."""
    store = store or get_default_store()
    job = store.get_job(route.job_id) or route.job
    action = route.action
    if action == ACTION_STATUS:
        msg = (
            f"Job `{job['job_id']}` — phase={job.get('phase')} "
            f"next_action={job.get('next_action')}\n"
            f"Objective: {job.get('objective')}"
        )
        return await post_job_update(
            adapter, route.job_id, msg, store=store, next_action=job.get("next_action")
        )
    if action == ACTION_PAUSE:
        msg = f"Job `{job['job_id']}` paused."
        return await post_job_update(
            adapter,
            route.job_id,
            msg,
            store=store,
            phase=PHASE_PAUSED,
            next_action="await_continue",
        )
    if action == ACTION_CONTINUE:
        msg = f"Job `{job['job_id']}` continuing."
        return await post_job_update(
            adapter,
            route.job_id,
            msg,
            store=store,
            phase=PHASE_ACTIVE,
            next_action="await_work",
        )
    if action == ACTION_HELP:
        msg = (
            f"Job `{job['job_id']}` help — commands: status, fortsätt/continue, "
            f"pausa/pause, hjälp agenten/help, klar/done."
        )
        return await post_job_update(
            adapter, route.job_id, msg, store=store, next_action=job.get("next_action")
        )
    if action == ACTION_COMPLETION:
        msg = f"Job `{job['job_id']}` completed."
        return await post_job_update(
            adapter,
            route.job_id,
            msg,
            store=store,
            phase=PHASE_COMPLETED,
            next_action="done",
        )
    raise ValueError(f"unknown job action: {action}")
