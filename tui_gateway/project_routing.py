"""Deterministic, quiet project routing from multiple evidence sources.

This module deliberately does not ask the user and does not mutate state. It
ranks explicit projects from semantic evidence (title/message/activity) plus
workspace evidence. Callers persist a returned decision through projects_db,
where repeated observations and manual locks provide stability.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Optional

from hermes_cli.projects_db import Project

_WORDS_RE = re.compile(r"[^\w]+", re.UNICODE)
_TITLE_PREFIX_RE = re.compile(r"\s+(?:-|–|—|:)\s+")


@dataclass(frozen=True)
class RouteDecision:
    project_id: str
    confidence: float
    reason: str


def _words(value: object) -> str:
    return " ".join(_WORDS_RE.sub(" ", str(value or "").casefold()).split())


def _contains(text: str, phrase: str) -> bool:
    return bool(phrase) and f" {phrase} " in f" {text} "


def _path_key(value: object) -> str:
    return str(value or "").strip().replace("\\", "/").rstrip("/").casefold()


def _under(folder: str, target: str) -> bool:
    root = _path_key(folder)
    path = _path_key(target)
    return bool(root and path and (path == root or path.startswith(root + "/")))


def _aliases(project: Project) -> list[str]:
    candidates = [project.name, project.slug.replace("-", " "), *project.aliases]
    result: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        alias = _words(candidate)
        # Two-character acronyms are too noisy in natural language. A project's
        # full name remains usable; explicit aliases such as MCC are retained.
        if len(alias.replace(" ", "")) < 3 or alias in seen:
            continue
        seen.add(alias)
        result.append(alias)
    return result


def _mentions(text: str, aliases: Iterable[str]) -> bool:
    return any(_contains(text, alias) for alias in aliases)


def choose_project(
    projects: Iterable[Project],
    *,
    title: str = "",
    user_text: str = "",
    assistant_text: str = "",
    activity_text: str = "",
    cwd: str = "",
    git_repo_root: str = "",
    minimum_confidence: float = 0.62,
    minimum_margin: float = 0.18,
) -> Optional[RouteDecision]:
    """Return a high-confidence project decision, otherwise quietly return None.

    Workspace evidence can strengthen semantic evidence but cannot by itself
    create a durable assignment; cwd-only sessions already group correctly via
    the project tree and should not be frozen to that first location.
    """
    title_words = _words(title)
    prefix_words = _words(_TITLE_PREFIX_RE.split(str(title or ""), maxsplit=1)[0])
    user_words = _words(user_text)
    assistant_words = _words(assistant_text)
    activity_words = _words(activity_text)

    ranked: list[tuple[float, str, list[str], bool]] = []
    for project in projects:
        if project.archived:
            continue
        aliases = _aliases(project)
        if not aliases:
            continue

        score = 0.0
        reasons: list[str] = []
        semantic = False

        if prefix_words and prefix_words in aliases:
            score += 0.72
            reasons.append("title prefix")
            semantic = True
        elif _mentions(title_words, aliases):
            score += 0.45
            reasons.append("title")
            semantic = True

        if _mentions(user_words, aliases):
            score += 0.62
            reasons.append("message mention")
            semantic = True
        if _mentions(assistant_words, aliases):
            score += 0.24
            reasons.append("response")
            semantic = True
        if _mentions(activity_words, aliases):
            score += 0.30
            reasons.append("activity")
            semantic = True

        folders = [folder.path for folder in project.folders]
        if any(_under(folder, cwd) for folder in folders):
            score += 0.24
            reasons.append("workspace")
        if git_repo_root and any(_under(folder, git_repo_root) for folder in folders):
            score += 0.18
            reasons.append("repository")

        ranked.append((min(1.0, score), project.id, reasons, semantic))

    ranked.sort(key=lambda item: (-item[0], item[1]))
    if not ranked:
        return None

    top_score, project_id, reasons, semantic = ranked[0]
    runner_up = ranked[1][0] if len(ranked) > 1 else 0.0
    if not semantic or top_score < minimum_confidence or top_score - runner_up < minimum_margin:
        return None

    return RouteDecision(project_id=project_id, confidence=top_score, reason=", ".join(reasons))
