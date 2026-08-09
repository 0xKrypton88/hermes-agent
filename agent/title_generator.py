"""Auto-generate short session titles from the first user/assistant exchange.

Runs asynchronously after the first response is delivered so it never
adds latency to the user-facing reply.
"""

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

_TITLE_PROMPT = (
    "Generate a short, descriptive title (3-7 words) for a conversation that starts with the "
    "following exchange. The title should capture the main topic or intent. "
    "Write the title in the same language the user is writing in. "
    "Return ONLY the title text, nothing else. No quotes, no punctuation at the end, no prefixes."
)

_TITLE_PROMPT_PINNED_LANGUAGE = (
    "Generate a short, descriptive title (3-7 words) for a conversation that starts with the "
    "following exchange. The title should capture the main topic or intent. "
    "Write the title in {language}. "
    "Return ONLY the title text, nothing else. No quotes, no punctuation at the end, no prefixes."
)

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
    """Return validated (style, mode), preserving historical defaults."""
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


def _summarize_user_message(user_message: str) -> str:
    """Collapse a slash-skill-expanded turn back to what the user typed.

    A ``/skill`` invocation expands into a message that embeds the whole skill
    body, so feeding it to the titler verbatim titles the session after the
    *skill's* prose — "Kick off a task in a fresh isolated git worktree" — not
    after the user's request. Reuse the canonical scaffolding parser so the
    model sees ``/work — fix the title leak`` instead.
    """
    if not user_message:
        return ""
    try:
        from agent.skill_commands import describe_skill_invocation

        described = describe_skill_invocation(user_message)
    except Exception:
        logger.debug("Skill-scaffolding summary failed; titling raw", exc_info=True)
        return user_message
    return described if described is not None else user_message


def generate_title(
    user_message: str,
    assistant_response: str,
    timeout: Optional[float] = None,
    failure_callback: Optional[FailureCallback] = None,
    main_runtime: dict = None,
    runtime_validator: Optional[RuntimeValidator] = None,
    current_title: Optional[str] = None,
    title_context: str = "",
) -> Optional[str]:
    """Generate a session title from the first exchange.

    Uses the main runtime's model when available, falling back to the
    auxiliary LLM client (cheapest/fastest available model).
    Returns the title string or None on failure.

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

    # Truncate long messages to keep the request small
    user_snippet = _summarize_user_message(user_message)[:500]
    assistant_snippet = assistant_response[:500] if assistant_response else ""

    language = _title_language()
    style, _mode = _title_settings()
    if style == "project_area":
        prompt = (
            _PROJECT_AREA_PROMPT_PINNED_LANGUAGE.format(language=language)
            if language
            else _PROJECT_AREA_PROMPT
        )
    else:
        prompt = (
            _TITLE_PROMPT_PINNED_LANGUAGE.format(language=language)
            if language
            else _TITLE_PROMPT
        )

    if current_title:
        prompt += (
            " The current title is authoritative unless the dominant project or area has materially "
            "changed. If it has not materially changed, return the exact current title."
        )

    if current_title or title_context:
        payload_parts = []
        if current_title:
            payload_parts.append(f"Current title: {current_title[:100]}")
        if title_context:
            payload_parts.append(f"Compact conversation context:\n{title_context[:2000]}")
        payload_parts.append(f"Latest user: {user_snippet}")
        payload_parts.append(f"Latest assistant: {assistant_snippet}")
        title_payload = "\n\n".join(payload_parts)
    else:
        title_payload = f"User: {user_snippet}\n\nAssistant: {assistant_snippet}"

    messages = [
        {"role": "system", "content": prompt},
        {"role": "user", "content": title_payload},
    ]

    try:
        response = call_llm(
            task="title_generation",
            messages=messages,
            max_tokens=500,
            temperature=0.3,
            timeout=timeout,
            main_runtime=main_runtime,
        )
        content = response.choices[0].message.content or ""
        # Strip thinking/reasoning blocks that think-enabled models
        # (MiniMax M2.7, DeepSeek, etc.) emit even for simple prompts like
        # title generation. Without this the raw <think>...</think> XML
        # leaks into session titles. Reuses the canonical scrubber so all
        # tag variants (unterminated blocks, orphan closes, mixed case)
        # are handled, not just a single literal <think> pair.
        from agent.agent_runtime_helpers import strip_think_blocks
        title = strip_think_blocks(None, content).strip()
        # Clean up: remove quotes, trailing punctuation, prefixes like "Title: "
        title = title.strip('"\'')
        if title.lower().startswith("title:"):
            title = title[6:].strip()
        # A title is one line. A model that ignores "return ONLY the title" and
        # answers the prompt instead (a shell transcript, a bulleted plan) would
        # otherwise be stored verbatim and truncated mid-command. Keep the first
        # non-empty line — the closest thing to a title in that response.
        title = next((line.strip() for line in title.splitlines() if line.strip()), "")
        if style == "project_area":
            # Accept common typographic separators from otherwise-compliant
            # models, then enforce the configured contract before persistence.
            title = title.replace(" — ", " - ").replace(" – ", " - ")
            title = title.rstrip(".!?:;").strip()
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
                f"{current_title or ''}\n{title_context}\n{user_snippet}\n{assistant_snippet}"
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
        elif len(title) > 80:
            # Preserve the legacy descriptive-style limit unchanged.
            title = title[:77] + "..."
        return title if title else None
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


def _persist_session_title(session_db, session_id, title, *, adaptive: bool = False):
    """Persist a generated title, recovering from duplicate-title collisions.

    The write goes through the backward-compatible
    ``set_auto_title_if_empty`` entry point, whose provenance-aware
    implementation atomically fills an empty title or replaces a prior auto
    title. A manual ``/title`` (or a legacy title without provenance) is never
    overwritten. A plain ``set_session_title`` fallback keeps older stores
    working. Duplicate titles are retried with the lineage suffix helper.

    Adaptive writes retain launch-owned PROJECT from title metadata when
    present, and may update AREA while preserving verified EXECUTOR/MODEL.

    Returns the title actually persisted, or None when a concurrent manual
    title won the race (nothing was written).
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

    atomic_fn = None
    if adaptive:
        atomic_fn = getattr(session_db, "set_auto_title", None)
    if atomic_fn is None:
        atomic_fn = getattr(session_db, "set_auto_title_if_empty", None)

    def _set(t):
        if atomic_fn is not None:
            if not atomic_fn(session_id, t):
                # Predicate failed: a title appeared while generation was in
                # flight (manual /title wins), or the session vanished.
                logger.debug(
                    "Skipping auto-generated session title because a title "
                    "was set while generation was in flight"
                )
                return None
            return t
        ok = session_db.set_session_title(session_id, t)
        if ok is False:
            raise RuntimeError(
                f"session {session_id} not found when storing title"
            )
        return t

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


def auto_title_session(
    session_db,
    session_id: str,
    user_message: str,
    assistant_response: str,
    failure_callback: Optional[FailureCallback] = None,
    main_runtime: dict = None,
    title_callback: Optional[TitleCallback] = None,
    runtime_validator: Optional[RuntimeValidator] = None,
    title_context: str = "",
    adaptive: bool = False,
) -> None:
    """Generate and conditionally persist an initial or adaptive title.

    Called in a background thread after the first exchange completes.
    Silently skips if:
    - session_db is None
    - session already has a title (user-set or previously auto-generated)
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
            assistant_response,
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
    assistant_response: str,
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

    # Adaptive mode may reconsider only prior auto titles. Manual and legacy
    # titles fail closed; initial mode preserves the historical one-shot guard.
    try:
        existing = session_db.get_session_title(session_id)
        source_fn = getattr(session_db, "get_session_title_source", None)
        existing_source = source_fn(session_id) if callable(source_fn) else None
        if existing and (not adaptive or existing_source != "auto"):
            return
    except Exception:
        return

    # This runs on a bare daemon thread spawned AFTER the turn's ambient
    # conversation context was reset, so publish it here from the session id
    # we already hold — the title-generation LLM call then carries the same
    # ``conversation=`` Portal tag as the turn it titles. Root-of-lineage for
    # consistency with the agent loop (a no-op on first exchange, where
    # titling happens, but correct if this ever runs on a continuation).
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
    title = generate_title(user_message, assistant_response, **generate_kwargs)
    if not title:
        return

    try:
        persisted = _persist_session_title(
            session_db, session_id, title, adaptive=adaptive
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
    assistant_response: str,
    conversation_history: list,
    failure_callback: Optional[FailureCallback] = None,
    main_runtime: dict = None,
    title_callback: Optional[TitleCallback] = None,
    runtime_validator: Optional[RuntimeValidator] = None,
) -> None:
    """Dispatch title generation on the configured initial/adaptive cadence."""
    if not session_db or not session_id or not user_message or not assistant_response:
        return

    # Count user messages in history to detect first exchange.
    # conversation_history includes the exchange that just happened,
    # so for a first exchange we expect exactly 1 user message
    # (or 2 counting system). Be generous: generate on first 2 exchanges.
    user_msg_count = sum(1 for m in (conversation_history or []) if m.get("role") == "user")
    _style, mode = _title_settings()
    if mode == "adaptive":
        if not _adaptive_turn_eligible(user_msg_count):
            return
    elif user_msg_count > 2:
        return

    # Config read comes after the cheap first-exchange guard so the file
    # isn't touched on every subsequent turn of a long session.
    if not _auto_title_enabled():
        logger.debug("Auto-title skipped: auxiliary.title_generation.enabled=false")
        return

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
        args=(session_db, session_id, user_message, assistant_response),
        kwargs=worker_kwargs,
        daemon=True,
        name="auto-title",
    )
    thread.start()
