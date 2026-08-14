"""Explicit test-only durable Go fixtures for ENG-29 adapter paths.

This module is the only place tests may write default provider/Slack
authorization tuples and accepted Go. Production ``agent.durable_jobs``
code must not import it or expose an equivalent auto-grant helper.
"""

from __future__ import annotations

from pathlib import Path
from typing import Union

from agent.durable_jobs.decisions import DecisionLedger
from agent.durable_jobs.eng29 import (
    MATRIX_VERSION,
    PROVIDER_CREATE_TARGET_ACTION,
    SLACK_POST_ROOT_TARGET_ACTION,
    _connect,
    _ensure_store,
    _live_policy_actor,
    register_authorization_tuple,
)
from agent.durable_jobs.slack_contract import BindingConflict, SlackBindingLedger
from agent.durable_jobs.store import DurableJobStore

SqlitePath = Union[str, Path]


def install_default_adapter_authorization(
    sqlite_path: SqlitePath, job_id: str
) -> None:
    """Grant exact Go for provider create + Slack post_root from job+binding.

    Used by effect/fencing fixtures so they keep exercising claim fencing
    after ENG-29. Derives the immutable tuple from the job row and Slack
    binding (or binds cand-1/v1 when none exists). ENG-29 no-Go tests must
    not call this.
    """
    path = _ensure_store(sqlite_path)
    jobs = DurableJobStore(sqlite_path=path)
    job = jobs.get_job(job_id)
    if job is None:
        return

    slack = SlackBindingLedger(sqlite_path=path)
    bound = slack.get_binding(job_id)
    if bound is None:
        try:
            slack.bind(
                job_id=job_id,
                workspace_id="T1",
                channel_id="C123",
                root_thread_ts="111.222",
                candidate_id="cand-1",
                candidate_version="v1",
            )
        except BindingConflict:
            return
        bound = slack.get_binding(job_id)
    if bound is None:
        return

    decisions = DecisionLedger(sqlite_path=path)
    with _connect(path) as conn:
        policy_row = conn.execute(
            "SELECT 1 FROM job_authz_policies WHERE job_id = ?",
            (job_id,),
        ).fetchone()
    if policy_row is None:
        decisions.set_policy(
            job_id=job_id,
            policy_version="pol-1",
            allowed_actors=("U-alice",),
            expires_at="2099-01-01T00:00:00+00:00",
        )

    with _connect(path) as conn:
        actor_id, policy_version = _live_policy_actor(conn, job_id)
    if not str(actor_id).strip() or not str(policy_version).strip():
        return

    expires_at = "2099-01-01T00:00:00+00:00"
    for target_action in (
        PROVIDER_CREATE_TARGET_ACTION,
        SLACK_POST_ROOT_TARGET_ACTION,
    ):
        register_authorization_tuple(
            path,
            job_id=job_id,
            source_package_id=job.repository_identity,
            source_package_version=bound.candidate_version,
            candidate_sha=job.frozen_baseline_sha,
            candidate_id=bound.candidate_id,
            candidate_version=bound.candidate_version,
            target_environment=job.origin_platform,
            target_action=target_action,
            authorized_actor=actor_id,
            expires_at=expires_at,
            policy_version=policy_version,
            matrix_version=MATRIX_VERSION,
            authorization_idempotency_key=f"tuple:{job_id}:{target_action}",
            prerequisites_satisfied=True,
            provider_ambiguity_resolved=True,
        )
        decisions.record_decision(
            job_id=job_id,
            decision_type="go",
            candidate_id=bound.candidate_id,
            candidate_version=bound.candidate_version,
            actor_id=actor_id,
            policy_version=policy_version,
            decision_idempotency_key=f"go:{job_id}:{target_action}",
            source_package_id=job.repository_identity,
            source_package_version=bound.candidate_version,
            candidate_sha=job.frozen_baseline_sha,
            target_environment=job.origin_platform,
            target_action=target_action,
            matrix_version=MATRIX_VERSION,
        )
