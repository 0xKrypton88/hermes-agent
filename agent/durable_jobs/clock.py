"""Injectable clocks and claim-lease helpers.

Lease expiry is compared against an injected ``now_fn`` so tests can advance
time deterministically. Wall-clock sleeps are not part of the contract.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

DEFAULT_CLAIM_LEASE_SECONDS = 30
DEFAULT_RECOVERY_MAX_ATTEMPTS = 3
DEFAULT_RECOVERY_WINDOW_SECONDS = 90


def to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def utcnow_iso() -> str:
    return to_iso(datetime.now(timezone.utc))


def parse_iso(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def add_seconds_iso(now_iso: str, seconds: float) -> str:
    return to_iso(parse_iso(now_iso) + timedelta(seconds=seconds))


def claim_is_expired(expires_at: Optional[str], now_iso: str) -> bool:
    """Missing/empty expiry is stale (legacy candidate-created rows)."""
    if expires_at is None or not str(expires_at).strip():
        return True
    return parse_iso(expires_at) <= parse_iso(now_iso)


class ClockWatermark:
    """Per-ledger high-watermark so a rewind cannot un-expire a lease.

    Instance-local: must not be shared across wall-clock and FrozenClock
    ledgers on the same SQLite file.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._value: Optional[str] = None

    def observe(self, now_iso: str) -> str:
        with self._lock:
            current = self._value
            try:
                if current and parse_iso(current) > parse_iso(now_iso):
                    return current
            except ValueError:
                self._value = now_iso
                return now_iso
            self._value = now_iso
            return now_iso


class FrozenClock:
    """Deterministic clock. Tests advance it explicitly — no sleeps.

    Owner heartbeats register via ``register_tick_listener`` so ``advance()``
    renews a live lease without wall-clock sleeps.
    """

    def __init__(self, start: Optional[datetime] = None) -> None:
        self._now = start or datetime(2026, 1, 1, tzinfo=timezone.utc)
        self._lock = threading.Lock()
        self._tick_listeners: list[Callable[[], None]] = []

    def now(self) -> datetime:
        with self._lock:
            return self._now

    def __call__(self) -> str:
        with self._lock:
            return to_iso(self._now)

    def register_tick_listener(
        self, callback: Callable[[], None]
    ) -> Callable[[], None]:
        with self._lock:
            self._tick_listeners.append(callback)

        def unregister() -> None:
            with self._lock:
                try:
                    self._tick_listeners.remove(callback)
                except ValueError:
                    return

        return unregister

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._now = self._now + timedelta(seconds=seconds)
            listeners = list(self._tick_listeners)
        for callback in listeners:
            callback()
