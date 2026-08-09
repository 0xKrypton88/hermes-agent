"""Auto-generate short session titles from the user's opening message.

Two stages, both off the critical path:

1. **Instant** — a deterministic title derived from the first user message,
   written before the model is even called. Costs nothing, cannot fail, and
   means a session is named the moment it starts instead of after the first
   turn finishes (which measured p50 151s / p90 1212s on real sessions).
2. **Upgrade** — one small-model call that replaces the derived title with a
   proper one. Runs on a cheap/fast tier, with thinking disabled and the
   response constrained to a JSON object, so there is no reasoning preamble to
   strip and nothing to parse out of prose.

Provenance (``derived`` < ``llm`` < ``user``) is enforced by the storage layer,
so stage 2 can only ever replace stage 1, and neither can replace a name the
user typed. That ordering is the industry-standard one — Codex CLI encodes the
same ``custom > ai > fallback`` precedence in its session importer.
"""

import json
import logging
import re
import threading
from typing import Callable, Optional

from agent.auxiliary_client import call_llm

logger = logging.getLogger(__name__)

# Callback signature: (task_name, exception) -> None. Used to surface
# auxiliary failures to the user through AIAgent._emit_auxiliary_failure
# so silent-drops (e.g. OpenRouter 402 exhausting the fallback chain)
# become visible instead of piling up as NULL session titles.
FailureCallback = Callable[[str, BaseException], None]
TitleCallback = Callable[[str], None]

# Validation callback: () -> bool. Called right before the LLM request in
# generate_title(). Return False to skip — e.g. the user switched models
# after this background thread captured its runtime snapshot, and sending
# the request would reload a model the runtime already evicted (#19027).
RuntimeValidator = Callable[[], bool]

# Cap on the text handed to the model. Claude Code and OpenClaw independently
# converged on the same 1000-char budget; a title needs the opening intent, not
# a pasted stack trace.
MAX_TITLE_INPUT_CHARS = 1000

# Cap on the instant derived title. Deliberately shorter than the model's
# budget: a raw sentence fragment reads worse the longer it runs. Cline and
# Codex CLI independently landed on the same ~50-char slice.
MAX_DERIVED_TITLE_CHARS = 48

_TITLE_PROMPT_TEMPLATE = (
    "You name chat sessions. Given the user's opening message, write a title "
    "that lets them find this conversation again in a list.\n\n"
    "Rules:\n"
    "- 3 to 7 words, sentence case (capitalize only the first word and proper nouns).\n"
    "- Name what the user wants DONE, not that they asked a question.\n"
    "- Keep technical terms, filenames, numbers, and error codes exact.\n"
    "- Drop filler words: the, this, my, a, an.\n"
    "- No trailing punctuation, no quotes, no tool names, no 'Title:' prefix.\n"
    "- Never answer the message. Name it.\n"
    "- Always produce something, even for a bare greeting.\n"
    "__LANGUAGE_RULE__\n"
    'Good: {"title": "Fix login button on mobile"}\n'
    'Good: {"title": "Postgres connection pool exhaustion"}\n'
    'Good: {"title": "Friendly greeting"}\n'
    'Too vague: {"title": "Code changes"}\n'
    'Too long: {"title": "Investigate and fix the issue where the login button '
    'does not respond on mobile devices"}\n\n'
    'Reply with JSON only: {"title": "..."}'
)

_LANGUAGE_RULE_MATCH_USER = "- Write the title in the same language as the user's message."
_LANGUAGE_RULE_PINNED = "- Write the title in {language}."

# JSON schema constraining the response to a single title field. Removes the
# whole class of "model answered the prompt instead of titling it" failures
# that produced titles like "<title>...</title>" and "User: Yep, that's the
# catch —" in real session history.
_TITLE_RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "session_title",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {"title": {"type": "string"}},
            "required": ["title"],
            "additionalProperties": False,
        },
    },
}

# Control-tag wrappers that surround machine-authored content inside what is
# nominally a "user" message. Titling from these is what produces a session
# named after a slash command or an injected reminder rather than the user's
# actual request. Ported from Codex CLI's RECOGNIZED_CONTROL_WRAPPERS, which
# strips them (and keeps titling) rather than refusing outright.
_CONTROL_WRAPPERS = (
    ("<command-message>", "</command-message>"),
    ("<command-name>", "</command-name>"),
    ("<command-args>", "</command-args>"),
    ("<local-command-caveat>", "</local-command-caveat>"),
    ("<local-command-stderr>", "</local-command-stderr>"),
    ("<local-command-stdout>", "</local-command-stdout>"),
    ("<task-notification>", "</task-notification>"),
    ("<system-reminder>", "</system-reminder>"),
    ("<ide_opened_file>", "</ide_opened_file>"),
    ("<ide_selection>", "</ide_selection>"),
)

# Hermes' own machine-authored openers. A compaction handoff or a resumed
# session must not be titled after the scaffolding that carried it.
_MACHINE_PREFIXES = (
    "[CONTEXT COMPACTION",
    "[Runtime note:",
    "[SYSTEM]",
)

# Opt-in PROJECT - AREA style (auxiliary.title_generation.style=project_area).
# Default style remains descriptive; adaptive mode is also opt-in.
_PROJECT_AREA_PROMPT = (
    "Generate a short session title using exactly this format: PROJECT - AREA, or "
    "PROJECT - AREA · EXECUTOR [MODEL] when an external or headless coding executor is "
    "explicitly supported by the supplied context. "
    "PROJECT is the stable product, system, or repository name; preserve canonical spelling "
    "and uppercase acronyms such as MCC or API. AREA is a concrete 1-4 word noun phrase that "
    "names the work surface and target, not an implementation action or result. Prefer durable "
    "labels that make the exact operating context obvious, for example Hermes - Balance Codex "
    "rather than Hermes - Automatisk failover, and MCC - Agent Sessions rather than Hermes - "
    "Sessionsarkivering. Add the optional executor suffix when explicitly supported and append "
    "its model only when that model is also explicit, for example QuantCore - BTC Scalper · "
    "Cursor or QuantCore - BTC Scalper · Cursor Grok 4.5. Never guess an executor or model. "
    "When the request concerns the Hermes app or framework itself — including its session "
    "titles, sidebar, agents, tools, gateway, or desktop client — use Hermes as PROJECT. "
    "Never promote a task-shape word such as Prompt, Megaprompt, Session, or Workflow to PROJECT. "
    "Use the user's language for ordinary words, but preserve established project names and "
    "English technical terms. Keep the complete title at 56 characters or fewer. "
    "Never use prompt-like prefixes such as Help with, Hjälp med, I want, Jag vill, Review, "
    "Granska, Test of, Test av, Workflows for, or Arbetsflöden för. "
    "Return ONLY the title text, with no quotes or trailing punctuation."
)

_PROJECT_AREA_PROMPT_PINNED_LANGUAGE = (
    "Generate a short session title using exactly this format: PROJECT - AREA, or "
    "PROJECT - AREA · EXECUTOR [MODEL] when an external or headless coding executor is "
    "explicitly supported by the supplied context. "
    "PROJECT is the stable product, system, or repository name; preserve canonical spelling "
    "and uppercase acronyms such as MCC or API. AREA is a concrete 1-4 word noun phrase that "
    "names the work surface and target, not an implementation action or result. Prefer durable "
    "labels that make the exact operating context obvious, for example Hermes - Balance Codex "
    "rather than Hermes - Automatisk failover, and MCC - Agent Sessions rather than Hermes - "
    "Sessionsarkivering. Add the optional executor suffix when explicitly supported and append "
    "its model only when that model is also explicit, for example QuantCore - BTC Scalper · "
    "Cursor or QuantCore - BTC Scalper · Cursor Grok 4.5. Never guess an executor or model. "
    "When the request concerns the Hermes app or framework itself — including its session "
    "titles, sidebar, agents, tools, gateway, or desktop client — use Hermes as PROJECT. "
    "Never promote a task-shape word such as Prompt, Megaprompt, Session, or Workflow to PROJECT. "
    "Write ordinary words in {language}, but preserve established project names and English "
    "technical terms. Keep the complete title at 56 characters or fewer. "
    "Never use prompt-like prefixes such as Help with, Hjälp med, I want, Jag vill, Review, "
    "Granska, Test of, Test av, Workflows for, or Arbetsflöden för. "
    "Return ONLY the title text, with no quotes or trailing punctuation."
)

_PROMPT_LIKE_TITLE_PREFIXES = (
    "help with",
    "hjälp med",
    "i want",
    "jag vill",
    "review ",
    "granska ",
    "test of",
    "test av",
    "workflows for",
    "arbetsflöden för",
)

_NON_PROJECT_WORDS = frozenset(
    {"prompt", "megaprompt", "session", "sessions", "workflow", "workflows"}
)
_HERMES_SELF_TOPIC_RE = re.compile(
    r"\b(?:session(?:en|er|erna|s)?|title|titles|titel|titlar|namngiv|sidebar|agent|agents|"
    r"tool|tools|gateway|desktop|klient|client)\b",
    re.IGNORECASE,
)
_AUTO_WRITABLE_TITLE_SOURCES = frozenset({"derived", "llm"})


def _project_is_supported(project: str, context: str) -> bool:
    """Reject task-shape words while recognizing work on Hermes itself."""
    normalized = project.strip().casefold()
    if normalized in _NON_PROJECT_WORDS:
        return False
    if normalized in {"hermes", "hermes agent"} and _HERMES_SELF_TOPIC_RE.search(context):
        return True
    return re.search(
        rf"(?<!\w){re.escape(project.strip())}(?!\w)",
        context,
        re.IGNORECASE,
    ) is not None


def _executor_is_supported(executor: str, context: str) -> bool:
    """Require the complete executor/model label to appear in supplied context."""
    normalized_executor = re.sub(
        r"[^\w.]+", " ", executor, flags=re.UNICODE
    ).strip().casefold()
    normalized_context = re.sub(
        r"[^\w.]+", " ", context, flags=re.UNICODE
    ).strip().casefold()
    return bool(normalized_executor and normalized_executor in normalized_context)


def _title_language() -> str:
    """Return configured title language, or empty string to match the user."""
    try:
        from hermes_cli.config import load_config_readonly

        return str(
            ((load_config_readonly() or {}).get("auxiliary") or {})
            .get("title_generation", {})
            .get("language", "")
        ).strip()
    except Exception:
        return ""


def _title_settings() -> tuple[str, str]:
    """Return validated (style, mode), preserving historical safe defaults.

    ``style`` defaults to ``descriptive`` (project_area is opt-in).
    ``mode`` defaults to ``initial`` (adaptive retitle is opt-in).
    """
    try:
        from hermes_cli.config import load_config_readonly

        title_config = (
            ((load_config_readonly() or {}).get("auxiliary") or {})
            .get("title_generation", {})
        ) or {}
        style = str(title_config.get("style", "descriptive")).strip().lower().replace("-", "_")
        mode = str(title_config.get("mode", "initial")).strip().lower()
    except Exception:
        return "descriptive", "initial"
    if style not in {"descriptive", "project_area"}:
        style = "descriptive"
    if mode not in {"initial", "adaptive"}:
        mode = "initial"
    return style, mode


def _title_style() -> str:
    """Compatibility helper retained for callers that only need the style."""
    return _title_settings()[0]


def _auto_title_enabled() -> bool:
    """Return whether automatic session title generation is enabled."""
    try:
        # Lazy imports, matching _title_language(): title_generator is imported
        # from agent code paths where a module-level hermes_cli import risks
        # circularity, and the read-only loader avoids config-migration writes.
        from hermes_cli.config import load_config_readonly
        from utils import is_truthy_value

        config = load_config_readonly()
        title_config = (config.get("auxiliary") or {}).get("title_generation") or {}
        return is_truthy_value(title_config.get("enabled"), default=True)
    except Exception:
        logger.debug("Failed to read title_generation.enabled", exc_info=True)
        return True


def strip_control_wrappers(text: str) -> str:
    """Remove leading machine-authored control wrappers, including nested ones.

    Loops so ``<command-message><command-name>/work</command-name></command-message>``
    reduces to the prose the user actually typed. Unlike a refusal check, this
    still yields usable text, so a slash-command turn gets a real title instead
    of staying untitled.
    """
    if not text:
        return ""
    current = text.strip()
    # Bounded: each pass must remove at least one wrapper or we stop.
    for _ in range(len(_CONTROL_WRAPPERS) * 2):
        stripped = current
        for open_tag, close_tag in _CONTROL_WRAPPERS:
            if not stripped.lower().startswith(open_tag):
                continue
            end = stripped.lower().find(close_tag)
            if end == -1:
                # Unterminated wrapper: drop the opening tag and keep the body.
                stripped = stripped[len(open_tag):].strip()
            else:
                inner = stripped[len(open_tag):end].strip()
                rest = stripped[end + len(close_tag):].strip()
                # Prefer the trailing prose when there is any; otherwise the
                # wrapper's own body is the only content we have.
                stripped = (rest or inner).strip()
            break
        if stripped == current:
            break
        current = stripped
    return current


def _summarize_user_message(user_message: str) -> str:
    """Reduce a user turn to the text worth titling.

    A ``/skill`` invocation expands into a message that embeds the whole skill
    body, so feeding it to the titler verbatim titles the session after the
    *skill's* prose — "Kick off a task in a fresh isolated git worktree" — not
    after the user's request. Reuse the canonical scaffolding parser so the
    model sees ``/work — fix the title leak`` instead, then strip any control
    wrappers left around it.
    """
    if not user_message:
        return ""
    described = None
    try:
        from agent.skill_commands import describe_skill_invocation

        described = describe_skill_invocation(user_message)
    except Exception:
        logger.debug("Skill-scaffolding summary failed; titling raw", exc_info=True)
    text = described if described is not None else user_message
    return strip_control_wrappers(text)


def is_titleable_user_message(user_message: str) -> bool:
    """Return whether *user_message* carries real user intent to title from.

    False for machine-authored openers (compaction handoffs, runtime notes) and
    for turns that reduce to nothing once control scaffolding is stripped.
    """
    if not isinstance(user_message, str) or not user_message.strip():
        return False
    for prefix in _MACHINE_PREFIXES:
        if user_message.lstrip().startswith(prefix):
            return False
    return bool(_summarize_user_message(user_message).strip())


def derive_title(user_message: str) -> Optional[str]:
    """Build an instant title from the user's message. No model, never fails.

    This is what the user sees within milliseconds of sending their first
    message. It is intentionally dumb — first meaningful line, trimmed to a
    word boundary — because its job is to beat the model to the screen, not to
    beat it on quality. The model's title replaces it moments later.
    """
    text = _summarize_user_message(user_message)
    if not text:
        return None
    # First non-empty line: a pasted log or a multi-paragraph brief still gets
    # named after its opening intent.
    line = next((ln.strip() for ln in text.splitlines() if ln.strip()), "")
    if not line:
        return None
    line = " ".join(line.split())
    if len(line) > MAX_DERIVED_TITLE_CHARS:
        cut = line[:MAX_DERIVED_TITLE_CHARS]
        # Prefer a word boundary so the title doesn't end mid-token.
        space = cut.rfind(" ")
        if space > MAX_DERIVED_TITLE_CHARS // 2:
            cut = cut[:space]
        line = cut.rstrip(" ,.;:—-") + "…"
    return line or None


def _extract_title_text(content: str) -> str:
    """Pull the title out of a model response.

    The JSON schema makes the object shape the expected case, but not every
    provider honors ``response_format``; fall back through a loose JSON scan
    and finally to first-line prose so a non-compliant provider still titles.
    """
    if not content:
        return ""
    raw = content.strip()
    # Fenced JSON from providers that wrap structured output in markdown.
    fenced = re.match(r"^```(?:json)?\s*(.*?)\s*```$", raw, re.DOTALL)
    if fenced:
        raw = fenced.group(1).strip()
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict) and isinstance(parsed.get("title"), str):
            return parsed["title"].strip()
    except (ValueError, TypeError):
        pass
    # Loose scan: a compliant object embedded in surrounding chatter.
    match = re.search(r'"title\"\s*:\s*"((?:[^"\\]|\\.)*)"', raw)
    if match:
        try:
            return json.loads(f'"{match.group(1)}"').strip()
        except ValueError:
            return match.group(1).strip()
    # Prose fallback. Reuse the canonical scrubber so reasoning-model output
    # (<think>…) can't leak into a title, then keep the first real line.
    try:
        from agent.agent_runtime_helpers import strip_think_blocks

        raw = strip_think_blocks(None, raw).strip()
    except Exception:
        logger.debug("strip_think_blocks unavailable for title output", exc_info=True)
    raw = next((ln.strip() for ln in raw.splitlines() if ln.strip()), "")
    if raw.lower().startswith("title:"):
        raw = raw[6:].strip()
    return raw.strip("\"'").strip()


def _clean_title(text: str) -> Optional[str]:
    """Normalize a model-produced title, or None when nothing usable remains."""
    title = " ".join((text or "").split())
    title = title.strip("\"'").strip()
    if title.lower().startswith("title:"):
        title = title[6:].strip()
    # Trailing sentence punctuation reads wrong in a sidebar list.
    title = title.rstrip(".!,;:")
    if not title:
        return None
    if len(title) > 80:
        title = title[:77].rstrip() + "..."
    return title


def generate_title(
    user_message: str,
    timeout: Optional[float] = None,
    failure_callback: Optional[FailureCallback] = None,
    main_runtime: dict = None,
    runtime_validator: Optional[RuntimeValidator] = None,
    current_title: Optional[str] = None,
    title_context: str = "",
) -> Optional[str]:
    """Generate a session title from the user's opening message.

    Runs on the ``title_generation`` auxiliary task, which resolves to a
    small/fast model tier. The default descriptive style disables thinking and
    constrains the response to ``{"title": "..."}``. Opt-in
    ``project_area`` style returns plain ``PROJECT - AREA`` text instead.

    Titles come from the user's message alone — every surveyed implementation
    that titles well (Claude Code, OpenCode, Cursor, OpenClaw) does the same.
    Waiting for the assistant is what made this slow, and it bought nothing:
    the user's opening message already states the intent worth naming.

    ``failure_callback`` is invoked with ``(task, exception)`` when the
    auxiliary call raises — the caller typically wires this to
    ``AIAgent._emit_auxiliary_failure`` so the user sees a warning instead
    of silently accumulating untitled sessions.

    ``runtime_validator`` is called right before the LLM request. If it
    returns False (e.g. the user's model was switched since the background
    thread captured its runtime snapshot), the call is skipped silently —
    no request is sent, so a stale title request can't reload a model the
    runtime already unloaded (#19027).
    """
    if not _auto_title_enabled():
        logger.debug("Auto-title skipped: auxiliary.title_generation.enabled=false")
        return None

    if runtime_validator is not None:
        try:
            if not runtime_validator():
                logger.debug("Title generation skipped: runtime validator returned False")
                return None
        except Exception:
            # Fail open: a broken validator must not disable titling.
            logger.debug("Title runtime validator raised; proceeding", exc_info=True)

    user_snippet = _summarize_user_message(user_message)[:MAX_TITLE_INPUT_CHARS]
    if not user_snippet.strip():
        return None

    language = _title_language()
    style, _mode = _title_settings()
    extra_body = None
    if style == "project_area":
        prompt = (
            _PROJECT_AREA_PROMPT_PINNED_LANGUAGE.format(language=language)
            if language
            else _PROJECT_AREA_PROMPT
        )
        if current_title:
            prompt += (
                " The current title is authoritative unless the dominant project or area has "
                "materially changed. If it has not materially changed, return the exact "
                "current title."
            )
        if current_title or title_context:
            payload_parts = []
            if current_title:
                payload_parts.append(f"Current title: {current_title[:100]}")
            if title_context:
                payload_parts.append(
                    f"Compact conversation context:\n{title_context[:2000]}"
                )
            payload_parts.append(f"Latest user: {user_snippet}")
            title_payload = "\n\n".join(payload_parts)
        else:
            title_payload = user_snippet
    else:
        language_rule = (
            _LANGUAGE_RULE_PINNED.format(language=language)
            if language
            else _LANGUAGE_RULE_MATCH_USER
        )
        # Placeholder substitution, not str.format: the prompt embeds literal JSON
        # braces as few-shot examples, which format() would try to interpolate.
        prompt = _TITLE_PROMPT_TEMPLATE.replace("__LANGUAGE_RULE__", language_rule)
        title_payload = user_snippet
        extra_body = {"response_format": _TITLE_RESPONSE_FORMAT}

    messages = [
        {"role": "system", "content": prompt},
        {"role": "user", "content": title_payload},
    ]

    try:
        response = call_llm(
            task="title_generation",
            messages=messages,
            # A title is a handful of tokens. The old 500-token ceiling let a
            # chatty model burn seconds generating prose we then threw away.
            max_tokens=64,
            temperature=0.3,
            timeout=timeout,
            main_runtime=main_runtime,
            extra_body=extra_body,
        )
        content = response.choices[0].message.content or ""
        if style == "project_area":
            title = _clean_title(_extract_title_text(content) or content)
            if not title:
                return None
            lowered = title.casefold()
            if lowered.startswith(_PROMPT_LIKE_TITLE_PREFIXES):
                return None
            if len(title) > 56 or title.count(" - ") != 1:
                return None
            project, area_with_executor = (
                part.strip() for part in title.split(" - ", 1)
            )
            if area_with_executor.count(" · ") > 1:
                return None
            if " · " in area_with_executor:
                area, executor = (
                    part.strip() for part in area_with_executor.split(" · ", 1)
                )
            else:
                area, executor = area_with_executor, ""
            context = (
                f"{current_title or ''}\n{title_context}\n{user_snippet}"
            )
            if (
                not project
                or not area
                or not _project_is_supported(project, context)
                or area.casefold().startswith(_PROMPT_LIKE_TITLE_PREFIXES)
                or (executor and not _executor_is_supported(executor, context))
            ):
                return None
            title = f"{project} - {area}"
            if executor:
                title += f" · {executor}"
            return title
        return _clean_title(_extract_title_text(content))
    except Exception as e:
        # Log at WARNING so this shows up in agent.log without debug mode.
        # Full detail at debug level for operators who need the stack.
        logger.warning("Title generation failed: %s", e)
        logger.debug("Title generation traceback", exc_info=True)
        if failure_callback is not None:
            try:
                failure_callback("title generation", e)
            except Exception:
                logger.debug("Title generation failure_callback raised", exc_info=True)
        return None


def _persist_session_title(session_db, session_id, title, *, source, adaptive: bool = False):
    """Persist a title at *source* authority, recovering from name collisions.

    The write goes through ``set_auto_title`` (precedence check + write in one
    transaction) so a manual ``/title`` set while generation was in flight is
    never overwritten. ``ValueError`` means the name is taken by an unrelated
    session (the unique-title index); rather than leave the session untitled
    (#50537), append a ``#N`` suffix via ``get_next_title_in_lineage``.

    Adaptive writes retain launch-owned PROJECT from title metadata when
    present, and may update AREA while preserving verified EXECUTOR/MODEL.

    Returns the title actually persisted, or None when a higher-authority
    title already held the row (nothing was written).
    """
    if adaptive:
        try:
            from agent.session_title_meta import apply_adaptive_title_with_meta

            adapted = apply_adaptive_title_with_meta(session_db, session_id, title)
            if adapted is not None:
                return adapted
        except Exception:
            logger.debug(
                "Adaptive title meta retention failed; falling back",
                exc_info=True,
            )

    auto_fn = getattr(session_db, "set_auto_title", None)

    def _set(candidate):
        if auto_fn is not None:
            try:
                ok = auto_fn(session_id, candidate, source=source)
            except TypeError:
                ok = auto_fn(session_id, candidate)
            if not ok:
                logger.debug(
                    "Skipping %s title: a higher-authority title already holds "
                    "session %s",
                    source, session_id,
                )
                return None
            return candidate
        # Older store without provenance support.
        legacy_fn = getattr(session_db, "set_auto_title_if_empty", None)
        if legacy_fn is not None:
            return candidate if legacy_fn(session_id, candidate) else None
        ok = session_db.set_session_title(session_id, candidate)
        if ok is False:
            raise RuntimeError(f"session {session_id} not found when storing title")
        return candidate

    try:
        return _set(title)
    except ValueError:
        next_title_fn = getattr(session_db, "get_next_title_in_lineage", None)
        if next_title_fn is None:
            raise
        deduped = next_title_fn(title)
        if not deduped or deduped == title:
            raise
        return _set(deduped)


def apply_instant_title(
    session_db,
    session_id: str,
    user_message: str,
    title_callback: Optional[TitleCallback] = None,
) -> Optional[str]:
    """Write the derived title synchronously. Cheap enough to run inline.

    Returns the title written, or None when nothing was written (no usable
    text, or the session already carries a title of at least ``derived``
    authority). Never raises: a titling failure must not affect the turn.
    """
    if not session_db or not session_id:
        return None
    try:
        if not is_titleable_user_message(user_message):
            return None
        title = derive_title(user_message)
        if not title:
            return None
        persisted = _persist_session_title(
            session_db, session_id, title, source="derived"
        )
        if persisted and title_callback is not None:
            try:
                title_callback(persisted)
            except Exception:
                logger.debug("Instant-title callback failed", exc_info=True)
        return persisted
    except Exception:
        logger.debug("Instant title failed", exc_info=True)
        return None


def auto_title_session(
    session_db,
    session_id: str,
    user_message: str,
    failure_callback: Optional[FailureCallback] = None,
    main_runtime: dict = None,
    title_callback: Optional[TitleCallback] = None,
    runtime_validator: Optional[RuntimeValidator] = None,
    title_context: str = "",
    adaptive: bool = False,
) -> None:
    """Generate and store the model title for a session.

    Called on a background thread. Silently skips if:
    - session_db is None
    - the session already carries an ``llm`` or ``user`` title (initial mode)
    - adaptive mode is locked by a ``user`` / legacy title
    - title generation fails
    - runtime_validator returns False (model was switched)

    Never lets an exception escape: this is a daemon-thread target, and an
    escaping exception would spray a raw traceback into the user's terminal
    via the default threading excepthook. The canonical trigger is the
    post-``hermes update`` stale-module window, where this function's lazy
    imports read NEW source from disk while already-cached modules
    (``agent.portal_tags`` etc.) are still the OLD version — the resulting
    ImportError repeats on every auto-title attempt until the long-running
    process restarts.
    """
    try:
        _auto_title_session(
            session_db,
            session_id,
            user_message,
            failure_callback=failure_callback,
            main_runtime=main_runtime,
            title_callback=title_callback,
            runtime_validator=runtime_validator,
            title_context=title_context,
            adaptive=adaptive,
        )
    except Exception as e:
        # WARNING (not debug) so operators see it in agent.log; the message
        # names the likely cause so "restart the process" is discoverable.
        logger.warning(
            "Auto-title failed (harmless; if this started after an update, "
            "restart the running Hermes process): %s",
            e,
        )
        logger.debug("Auto-title traceback", exc_info=True)
        if failure_callback is not None:
            try:
                failure_callback("title generation", e)
            except Exception:
                logger.debug("Auto-title failure_callback raised", exc_info=True)


def _auto_title_session(
    session_db,
    session_id: str,
    user_message: str,
    failure_callback: Optional[FailureCallback] = None,
    main_runtime: dict = None,
    title_callback: Optional[TitleCallback] = None,
    runtime_validator: Optional[RuntimeValidator] = None,
    title_context: str = "",
    adaptive: bool = False,
) -> None:
    """Body of :func:`auto_title_session` — see its docstring."""
    if not session_db or not session_id:
        return

    # Initial mode: skip when a title of at least LLM authority is already
    # stored (derived upgrades to llm). Adaptive mode may reconsider prior
    # derived/llm titles; user and legacy NULL-source titles fail closed.
    existing = None
    try:
        existing = session_db.get_session_title(session_id)
        source_fn = getattr(session_db, "get_session_title_source", None)
        existing_source = source_fn(session_id) if callable(source_fn) else None
        if adaptive:
            if existing and existing_source not in _AUTO_WRITABLE_TITLE_SOURCES:
                return
        elif source_fn is not None:
            if existing_source is not None and existing_source != "derived":
                return
        elif existing:
            return
    except Exception:
        return

    # This runs on a bare daemon thread spawned AFTER the turn's ambient
    # conversation context was reset, so publish it here from the session id
    # we already hold — the title-generation LLM call then carries the same
    # ``conversation=`` Portal tag as the turn it titles. Root-of-lineage for
    # consistency with the agent loop.
    from agent.aux_accounting import set_accounting_context
    from agent.portal_tags import set_conversation_context

    conversation_id = session_id
    try:
        conversation_id = session_db.get_conversation_root(session_id) or session_id
    except Exception:
        pass
    set_conversation_context(conversation_id)
    # Same for the accounting context, so the title call's token usage is
    # recorded against this session (task='title_generation', #23270).
    set_accounting_context(session_db, session_id)

    generate_kwargs = {
        "failure_callback": failure_callback,
        "main_runtime": main_runtime,
        "runtime_validator": runtime_validator,
    }
    if adaptive:
        generate_kwargs.update(
            current_title=existing,
            title_context=title_context,
        )
    title = generate_title(user_message, **generate_kwargs)
    if not title:
        return

    try:
        persisted = _persist_session_title(
            session_db, session_id, title, source="llm", adaptive=adaptive
        )
        if persisted is None:
            return
        logger.debug("Auto-generated session title: %s", persisted)
        if title_callback is not None:
            try:
                title_callback(persisted)
            except Exception:
                logger.debug("Auto-title callback failed", exc_info=True)
    except Exception as e:
        logger.debug("Failed to set auto-generated title: %s", e)


def _adaptive_turn_eligible(user_turn: int) -> bool:
    """Adaptive cadence: turns 1, 3, then every five turns (8, 13, ...)."""
    return user_turn == 1 or (user_turn >= 3 and (user_turn - 3) % 5 == 0)


def _compact_title_context(conversation_history: list) -> str:
    """Keep first intent plus at most three recent exchanges, never a transcript."""
    history = [
        message
        for message in (conversation_history or [])
        if isinstance(message, dict) and message.get("role") in {"user", "assistant"}
    ]
    user_positions = [
        index for index, message in enumerate(history) if message.get("role") == "user"
    ]
    if not user_positions:
        return ""

    first_content = str(history[user_positions[0]].get("content") or "")
    lines = [f"First intent (user): {first_content[:300]}"]
    recent_start = user_positions[max(0, len(user_positions) - 3)]
    for message in history[recent_start:][:6]:
        role = message.get("role")
        content = " ".join(str(message.get("content") or "").split())[:240]
        if content:
            lines.append(f"Recent {role}: {content}")
    return "\n".join(lines)[:2000]


def maybe_auto_title(
    session_db,
    session_id: str,
    user_message: str,
    conversation_history: Optional[list] = None,
    failure_callback: Optional[FailureCallback] = None,
    main_runtime: dict = None,
    title_callback: Optional[TitleCallback] = None,
    runtime_validator: Optional[RuntimeValidator] = None,
) -> None:
    """Title a session from its opening message: instant, then upgraded.

    Call this at the START of a turn, before the model is invoked. The derived
    title is written inline (sub-millisecond) and the model upgrade is forked
    onto a daemon thread, so nothing here is on the critical path.

    Default ``mode=initial`` only acts on the opening exchange. Opt-in
    ``mode=adaptive`` retitles on a sparse cadence while retaining launch
    PROJECT metadata. Machine-authored compaction handoffs are skipped.
    """
    if not session_db or not session_id or not user_message:
        return

    # Count user messages to detect the opening turn. ``conversation_history``
    # is the state BEFORE this turn's message is appended when called from the
    # turn prologue, and after it when called post-response, so accept both.
    # Entries are dicts; anything else means a caller passed the wrong
    # positional and titling must degrade quietly rather than raise.
    user_msg_count = sum(
        1
        for m in (conversation_history or [])
        if isinstance(m, dict) and m.get("role") == "user"
    )
    # Prologue callers pass history BEFORE appending this turn's user
    # message; post-response callers include it. Normalize so adaptive
    # cadence keys off the turn currently being titled.
    current_already_counted = any(
        isinstance(m, dict)
        and m.get("role") == "user"
        and m.get("content") == user_message
        for m in (conversation_history or [])
    )
    effective_user_turn = (
        user_msg_count if current_already_counted else user_msg_count + 1
    )
    style, mode = _title_settings()
    if mode == "adaptive":
        if not _adaptive_turn_eligible(effective_user_turn):
            return
    elif user_msg_count > 1:
        # Preserve historical initial-mode guard (prologue count 0/1 both fire).
        return

    if not is_titleable_user_message(user_message):
        return

    # Config read comes after the cheap guards so the file isn't touched on
    # every subsequent turn of a long session.
    if not _auto_title_enabled():
        logger.debug("Auto-title skipped: auxiliary.title_generation.enabled=false")
        return

    # Instant derived titles are for the default descriptive/initial path.
    # project_area and adaptive launches own their seed / cadence instead.
    if mode != "adaptive" and style != "project_area":
        apply_instant_title(session_db, session_id, user_message, title_callback)

    worker_kwargs = {
        "failure_callback": failure_callback,
        "main_runtime": main_runtime,
        "title_callback": title_callback,
        "runtime_validator": runtime_validator,
    }
    if mode == "adaptive":
        worker_kwargs.update(
            title_context=_compact_title_context(conversation_history),
            adaptive=True,
        )

    thread = threading.Thread(
        target=auto_title_session,
        args=(session_db, session_id, user_message),
        kwargs=worker_kwargs,
        daemon=True,
        name="auto-title",
    )
    thread.start()
