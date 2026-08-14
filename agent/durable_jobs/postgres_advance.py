"""Datastore-backed Package-1 graph-advance claim protocol (ENG-25)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class AdvanceClaimError(RuntimeError):
    """Stale or unauthorized graph-advance claim mutation."""


class AdvanceClaimDecision(Enum):
    WIN = "win"
    REENTER = "reenter"
    TAKEOVER = "takeover"
    LOST_LIVE_OWNER = "lost_live_owner"
    ADOPT_COMPLETED = "adopt_completed"


@dataclass(frozen=True)
class AdvanceClaimView:
    owner_token: str
    status: str
    generation: int
    leased_until: datetime


@dataclass(frozen=True)
class AdvanceClaimResult:
    decision: AdvanceClaimDecision
    generation: int = 0


def decide_advance_claim(
    *,
    job_phase: str,
    existing: Optional[AdvanceClaimView],
    owner_token: str,
    now: datetime,
) -> AdvanceClaimDecision:
    if job_phase == "await_dispatch":
        return AdvanceClaimDecision.ADOPT_COMPLETED
    if existing is None:
        return AdvanceClaimDecision.WIN
    if existing.status == "completed":
        return AdvanceClaimDecision.ADOPT_COMPLETED
    live = existing.leased_until > now
    same_owner = existing.owner_token == owner_token
    if same_owner and existing.status == "claimed":
        return AdvanceClaimDecision.REENTER
    if live and not same_owner:
        return AdvanceClaimDecision.LOST_LIVE_OWNER
    return AdvanceClaimDecision.TAKEOVER
