"""ENG-25 — datastore-backed advance claim/adoption (no process-local correctness)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from agent.durable_jobs.postgres_advance import (
    AdvanceClaimDecision,
    AdvanceClaimView,
    decide_advance_claim,
)


def _now() -> datetime:
    return datetime(2026, 8, 13, 17, 0, tzinfo=timezone.utc)


def test_missing_claim_on_intake_wins():
    decision = decide_advance_claim(
        job_phase="intake",
        existing=None,
        owner_token="t-a",
        now=_now(),
    )
    assert decision is AdvanceClaimDecision.WIN


def test_await_dispatch_is_adopted_without_running_graph():
    decision = decide_advance_claim(
        job_phase="await_dispatch",
        existing=None,
        owner_token="t-a",
        now=_now(),
    )
    assert decision is AdvanceClaimDecision.ADOPT_COMPLETED


def test_live_foreign_owner_does_not_win():
    existing = AdvanceClaimView(
        owner_token="t-winner",
        status="claimed",
        generation=1,
        leased_until=_now() + timedelta(seconds=30),
    )
    decision = decide_advance_claim(
        job_phase="intake",
        existing=existing,
        owner_token="t-loser",
        now=_now(),
    )
    assert decision is AdvanceClaimDecision.LOST_LIVE_OWNER


def test_expired_foreign_owner_is_takeover():
    existing = AdvanceClaimView(
        owner_token="t-stale",
        status="claimed",
        generation=3,
        leased_until=_now() - timedelta(seconds=1),
    )
    decision = decide_advance_claim(
        job_phase="freeze_baseline",
        existing=existing,
        owner_token="t-recovery",
        now=_now(),
    )
    assert decision is AdvanceClaimDecision.TAKEOVER


def test_completed_claim_is_adopted():
    existing = AdvanceClaimView(
        owner_token="t-winner",
        status="completed",
        generation=1,
        leased_until=_now() + timedelta(seconds=30),
    )
    decision = decide_advance_claim(
        job_phase="await_dispatch",
        existing=existing,
        owner_token="t-loser",
        now=_now(),
    )
    assert decision is AdvanceClaimDecision.ADOPT_COMPLETED


def test_same_owner_unexpired_claim_is_reentry():
    existing = AdvanceClaimView(
        owner_token="t-a",
        status="claimed",
        generation=2,
        leased_until=_now() + timedelta(seconds=10),
    )
    decision = decide_advance_claim(
        job_phase="intake",
        existing=existing,
        owner_token="t-a",
        now=_now(),
    )
    assert decision is AdvanceClaimDecision.REENTER
