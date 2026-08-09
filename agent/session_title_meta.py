"""Neutral session-title metadata contract.

Title format: ``PROJECT - AREA`` with optional `` · EXECUTOR`` and optional
verified ``MODEL``. Core never guesses a model. Launch metadata owns
PROJECT/AREA; dispatch/plugin metadata may only add or refresh EXECUTOR/MODEL.

Authority vocabulary matches the store (``derived`` / ``llm`` / ``user``):
auto-writable ranks are ``derived`` and ``llm``; ``user`` (and legacy NULL
with a non-null title) is a lock.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

_PROJECT_OWNED_KEY = "project_owned"

# Ranks that may be replaced by launch seed / verified dispatch / adaptive
# retitle. Manual ``user`` locks and legacy NULL-with-title fail closed.
_AUTO_WRITABLE_SOURCES = frozenset({"derived", "llm"})


def _is_auto_writable_source(source: Optional[str]) -> bool:
    return source in _AUTO_WRITABLE_SOURCES


def _call_set_auto_title(session_db, session_id: str, title: str) -> bool:
    """Call ``set_auto_title`` with HEAD's required ``source=`` when present."""
    set_auto = getattr(session_db, "set_auto_title", None)
    if not callable(set_auto):
        return False
    try:
        return bool(set_auto(session_id, title, source="llm"))
    except TypeError:
        # Older stores that still accept the positional-only auto writer.
        return bool(set_auto(session_id, title))


def format_session_title(
    project: str,
    area: str,
    *,
    executor: Optional[str] = None,
    model: Optional[str] = None,
) -> str:
    """Build ``PROJECT - AREA[ · EXECUTOR[ MODEL]]`` from verified pieces."""
    project = (project or "").strip()
    area = (area or "").strip()
    if not project or not area:
        raise ValueError("project and area are required")
    title = f"{project} - {area}"
    executor = (executor or "").strip() or None
    model = (model or "").strip() or None
    if executor:
        suffix = executor
        if model:
            suffix = f"{executor} {model}"
        title = f"{title} · {suffix}"
    return title


def parse_session_title(title: str) -> Optional[dict]:
    """Parse a PROJECT-AREA title into structured pieces, or None if invalid."""
    if not title or " - " not in title:
        return None
    project, rest = title.split(" - ", 1)
    project = project.strip()
    rest = rest.strip()
    if not project or not rest:
        return None
    executor = None
    model = None
    area = rest
    if " · " in rest:
        area, suffix = (part.strip() for part in rest.split(" · ", 1))
        if not area or not suffix:
            return None
        # Suffix is "EXECUTOR" or "EXECUTOR MODEL..." — first token is executor.
        parts = suffix.split()
        executor = parts[0]
        if len(parts) > 1:
            model = " ".join(parts[1:])
    return {
        "project": project,
        "area": area,
        "executor": executor,
        "model": model,
    }


def normalize_title_meta(meta: Optional[Mapping[str, Any]]) -> dict:
    """Return a cleaned title-meta dict (empty values omitted except ownership)."""
    if not isinstance(meta, Mapping):
        return {}
    out: dict[str, Any] = {}
    for key in ("project", "area", "executor", "model"):
        value = meta.get(key)
        if isinstance(value, str):
            value = value.strip()
            if value:
                out[key] = value
    if meta.get(_PROJECT_OWNED_KEY):
        out[_PROJECT_OWNED_KEY] = True
    return out


def _title_from_meta(meta: Mapping[str, Any]) -> Optional[str]:
    project = (meta.get("project") or "").strip()
    area = (meta.get("area") or "").strip()
    if not project or not area:
        return None
    return format_session_title(
        project,
        area,
        executor=meta.get("executor"),
        model=meta.get("model"),
    )


def _verified_executor_model(
    verified: Optional[Mapping[str, Any]],
) -> tuple[Optional[str], Optional[str]]:
    """Accept only verified executor/model; never invent a model."""
    if not isinstance(verified, Mapping):
        return None, None
    executor = verified.get("executor")
    model = verified.get("model")
    executor = executor.strip() if isinstance(executor, str) else ""
    model = model.strip() if isinstance(model, str) else ""
    if not executor:
        # Model without a verified executor is unusable under the contract.
        return None, None
    return executor, (model or None)


def seed_launch_title(
    session_db,
    session_id: str,
    *,
    project: str,
    area: str,
    executor: Optional[str] = None,
    model: Optional[str] = None,
) -> Optional[str]:
    """Deterministically seed an auto title from launch-owned PROJECT/AREA.

    Launch ownership ignores executor/model arguments — direct launch titles
    have no suffix. Returns the persisted title, or None when locked/invalid.
    """
    del executor, model  # launch path never writes a suffix from args
    project = (project or "").strip()
    area = (area or "").strip()
    if not session_db or not session_id or not project or not area:
        return None

    title = format_session_title(project, area)
    meta = {
        "project": project,
        "area": area,
        _PROJECT_OWNED_KEY: True,
    }
    writer = getattr(session_db, "set_auto_title_with_meta", None)
    if callable(writer):
        if not writer(session_id, title, meta):
            return None
        return title

    # Compatibility fallback for stores that only know set_auto_title.
    if not _call_set_auto_title(session_db, session_id, title):
        return None
    set_meta = getattr(session_db, "set_session_title_meta", None)
    if callable(set_meta):
        set_meta(session_id, meta)
    return title


def seed_launch_title_from_env(session_db, session_id: str) -> Optional[str]:
    """Seed from ``HERMES_TITLE_PROJECT`` / ``HERMES_TITLE_AREA`` when both set."""
    project = os.environ.get("HERMES_TITLE_PROJECT", "").strip()
    area = os.environ.get("HERMES_TITLE_AREA", "").strip()
    if not project or not area:
        return None
    return seed_launch_title(session_db, session_id, project=project, area=area)


def apply_verified_session_title_metadata(
    session_db,
    session_id: str,
    verified: Mapping[str, Any],
    *,
    requested: Optional[Mapping[str, Any]] = None,
) -> Optional[str]:
    """Refresh EXECUTOR/MODEL from a verified envelope only.

    ``requested`` is accepted for call-site clarity but never written — core
    only trusts ``verified``. PROJECT/AREA from either mapping are ignored so
    child dispatch cannot replace launch-owned identity. Works outside the
    adaptive retitle cadence.
    """
    del requested  # explicitly unused: verified envelope wins
    if not session_db or not session_id:
        return None

    executor, model = _verified_executor_model(verified)
    if not executor:
        return None

    source_fn = getattr(session_db, "get_session_title_source", None)
    source = source_fn(session_id) if callable(source_fn) else None
    existing_title = session_db.get_session_title(session_id)
    # User lock + legacy non-null titles (NULL source) fail closed.
    if existing_title and not _is_auto_writable_source(source):
        return None

    get_meta = getattr(session_db, "get_session_title_meta", None)
    meta = normalize_title_meta(get_meta(session_id) if callable(get_meta) else None)
    if not meta.get("project") or not meta.get("area"):
        parsed = parse_session_title(existing_title or "")
        if not parsed:
            return None
        meta = normalize_title_meta({**parsed, _PROJECT_OWNED_KEY: True})

    # Dispatch may never replace PROJECT/AREA — even when verified carries them.
    meta["executor"] = executor
    if model:
        meta["model"] = model
    else:
        meta.pop("model", None)

    title = _title_from_meta(meta)
    if not title:
        return None

    writer = getattr(session_db, "set_auto_title_with_meta", None)
    if callable(writer):
        if not writer(session_id, title, meta):
            return None
        return title

    if not _call_set_auto_title(session_db, session_id, title):
        return None
    set_meta = getattr(session_db, "set_session_title_meta", None)
    if callable(set_meta):
        set_meta(session_id, meta)
    return title


def retain_launch_project_on_candidate(
    session_db,
    session_id: str,
    candidate_title: str,
) -> Optional[Tuple[str, dict]]:
    """Rewrite an adaptive candidate so launch-owned PROJECT is retained.

    AREA may change from the candidate. EXECUTOR/MODEL come from stored meta
    (dispatch-owned), not from an unverified candidate suffix.
    """
    if not candidate_title:
        return None
    get_meta = getattr(session_db, "get_session_title_meta", None)
    meta = normalize_title_meta(get_meta(session_id) if callable(get_meta) else None)
    parsed = parse_session_title(candidate_title)
    if not parsed:
        return None

    if meta.get(_PROJECT_OWNED_KEY) and meta.get("project"):
        project = meta["project"]
    else:
        project = parsed["project"]

    area = parsed["area"]
    merged = {
        "project": project,
        "area": area,
        _PROJECT_OWNED_KEY: bool(meta.get(_PROJECT_OWNED_KEY)) or False,
    }
    if meta.get("executor"):
        merged["executor"] = meta["executor"]
    if meta.get("model"):
        merged["model"] = meta["model"]
    title = _title_from_meta(merged)
    if not title:
        return None
    return title, normalize_title_meta(merged)


def apply_adaptive_title_with_meta(
    session_db,
    session_id: str,
    candidate_title: str,
) -> Optional[str]:
    """Persist an adaptive auto title while retaining launch-owned PROJECT.

    Returns the new title only when the visible title string changed. Silent
    meta backfill of an unchanged title is not surfaced to callers/callbacks.
    """
    result = retain_launch_project_on_candidate(session_db, session_id, candidate_title)
    if not result:
        return None
    title, meta = result
    if meta.get(_PROJECT_OWNED_KEY):
        meta[_PROJECT_OWNED_KEY] = True

    existing = None
    try:
        existing = session_db.get_session_title(session_id)
    except Exception:
        existing = None

    writer = getattr(session_db, "set_auto_title_with_meta", None)
    if callable(writer):
        changed = writer(session_id, title, meta)
    else:
        if not _call_set_auto_title(session_db, session_id, title):
            return None
        set_meta = getattr(session_db, "set_session_title_meta", None)
        if callable(set_meta):
            set_meta(session_id, meta)
        changed = True

    if not changed:
        return None
    # Unchanged visible title (e.g. meta-only backfill) is not a retitle event.
    if existing is not None and title == existing:
        return None
    return title


def resolve_launch_title_args(
    *,
    title_project: Optional[str] = None,
    title_area: Optional[str] = None,
) -> tuple[Optional[str], Optional[str]]:
    """Resolve a complete PROJECT/AREA pair for launch seeding.

    Explicit CLI flags never mix with env for the missing half — a partial
    flag pair fails closed as ``(None, None)`` so stale shell metadata cannot
    complete the title. When neither flag is provided, env transport is used
    only when *both* ``HERMES_TITLE_PROJECT`` and ``HERMES_TITLE_AREA`` are set.
    """
    flag_project = (title_project or "").strip() or None
    flag_area = (title_area or "").strip() or None
    if flag_project is not None or flag_area is not None:
        if flag_project and flag_area:
            return flag_project, flag_area
        return None, None

    env_project = (os.environ.get("HERMES_TITLE_PROJECT") or "").strip() or None
    env_area = (os.environ.get("HERMES_TITLE_AREA") or "").strip() or None
    if env_project and env_area:
        return env_project, env_area
    return None, None


def apply_launch_title_env(
    *,
    title_project: Optional[str] = None,
    title_area: Optional[str] = None,
) -> None:
    """Mirror a complete launch PROJECT/AREA pair into env transport.

    Incomplete or missing pairs clear both env keys so children cannot inherit
    a half-stale combination from a previous launch.
    """
    project, area = resolve_launch_title_args(
        title_project=title_project, title_area=title_area
    )
    if project and area:
        os.environ["HERMES_TITLE_PROJECT"] = project
        os.environ["HERMES_TITLE_AREA"] = area
        return
    os.environ.pop("HERMES_TITLE_PROJECT", None)
    os.environ.pop("HERMES_TITLE_AREA", None)
