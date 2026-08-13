"""Owner-fenced claim lease heartbeat and bounded recovery helpers.

Lease liveness is persisted (owner token + expiry timestamps), never a
process-local in-flight set. Tests inject ``FrozenClock`` so renewal is
driven by ``advance()`` rather than wall-clock sleeps. Production uses a
short daemon thread gated by ``threading.Event``.

Heartbeat-loss semantics:
- ``renew_fn`` returning False or raising is recorded on this object
  (``last_ok``, ``last_error``, ``lost``). Callers must not swallow it.
- An in-flight ``create_run`` / ``post_root`` cannot be canceled. Loss of
  the lease does not retract a side effect that may still complete.
- A concurrent caller must not terminalize UNKNOWN while that in-flight
  call may still succeed; recovery looks up by the stable key only.
"""

from __future__ import annotations

import threading
from typing import Callable, Optional, Sequence

from agent.durable_jobs.clock import (
    DEFAULT_RECOVERY_MAX_ATTEMPTS,
    claim_is_expired,
)


class owner_lease_heartbeat:
    """Renew a persisted claim lease while the owner is inside a side effect.

    If ``now_fn`` exposes ``register_tick_listener`` (injected FrozenClock),
    renewal runs on clock ticks — no sleeps. Otherwise a daemon thread waits
    on an Event for ``lease_seconds / 3``.
    """

    def __init__(
        self,
        *,
        renew_fn: Callable[[], bool],
        now_fn: Callable[[], str],
        lease_seconds: int,
    ) -> None:
        self._renew_fn = renew_fn
        self._now_fn = now_fn
        self._lease_seconds = int(lease_seconds)
        self._unregister: Optional[Callable[[], None]] = None
        self._stop: Optional[threading.Event] = None
        self._thread: Optional[threading.Thread] = None
        self.last_ok: Optional[bool] = None
        self.last_error: Optional[BaseException] = None

    @property
    def lost(self) -> bool:
        return self.last_ok is False or self.last_error is not None

    def __enter__(self) -> "owner_lease_heartbeat":
        self._renew_safe()
        register = getattr(self._now_fn, "register_tick_listener", None)
        if callable(register):
            self._unregister = register(self._renew_safe)
            return self
        self._stop = threading.Event()
        interval = max(1.0, self._lease_seconds / 3.0)
        self._thread = threading.Thread(
            target=self._loop,
            args=(interval,),
            daemon=True,
            name="durable-claim-heartbeat",
        )
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> bool:
        if self._unregister is not None:
            self._unregister()
            self._unregister = None
        if self._stop is not None:
            self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            self._thread = None
        return False

    def _renew_safe(self) -> None:
        try:
            self.last_ok = bool(self._renew_fn())
            if self.last_ok:
                self.last_error = None
        except Exception as exc:
            self.last_ok = False
            self.last_error = exc

    def _loop(self, interval: float) -> None:
        assert self._stop is not None
        while not self._stop.wait(interval):
            self._renew_safe()


def caller_holds_live_lease(
    *,
    caller_token: Optional[str],
    persisted_token: Optional[str],
    expires_at: Optional[str],
    now_iso: str,
    status: str,
    live_statuses: Sequence[str],
) -> bool:
    """True when *this caller* still owns an unexpired CLAIMED/RECOVERING lease.

    A persisted foreign token is never caller authority. Missing caller token,
    mismatch, expiry (including legacy NULL), or a non-live status → False.
    """
    if not caller_token or not persisted_token:
        return False
    if caller_token != persisted_token:
        return False
    if status not in live_statuses:
        return False
    return not claim_is_expired(expires_at, now_iso)


def recovery_bound_exceeded(
    *,
    attempt_count: int,
    deadline: Optional[str],
    now_iso: str,
    max_attempts: int = DEFAULT_RECOVERY_MAX_ATTEMPTS,
) -> bool:
    """True when the recovery *window* has elapsed.

    ``attempt_count`` is owner-fenced bookkeeping, not a same-instant kill
    switch. Concurrent callers at one frozen timestamp must not UNKNOWN
    before ``recovery_deadline``.
    """
    _ = (attempt_count, max_attempts)
    if deadline is None or not str(deadline).strip():
        return False
    return claim_is_expired(deadline, now_iso)
