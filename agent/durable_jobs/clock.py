"""Injectable clocks and claim-lease helpers.

Lease expiry is compared against an injected ``now_fn`` so tests can advance
time deterministically. Wall-clock sleeps are not part of the contract.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

DEFAULT_CLAIM_LEASE_SECONDS = 30


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


class FrozenClock:
    """Deterministic clock. Tests advance it explicitly — no sleeps."""

    def __init__(self, start: Optional[datetime] = None) -> None:
        self._now = start or datetime(2026, 1, 1, tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self._now

    def __call__(self) -> str:
        return to_iso(self._now)

    def advance(self, seconds: float) -> None:
        self._now = self._now + timedelta(seconds=seconds)
