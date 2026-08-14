"""Isolated durable-lane facade for ENG-26/ENG-27 slices.

Default-off: every mutating entry point requires ``durable_jobs.enabled``.
No live Cursor, Slack, network, gateway, or dispatch adapters are constructed.
Binding is required before any provider or Slack effect.
"""

from __future__ import annotations

from typing import Optional, Sequence

from agent.durable_jobs.config import DurableJobsConfig, DurableJobsConfigError
from agent.durable_jobs.coordinator import (
    InboundAckPort,
    InboundActionResult,
    consume_inbound_action as consume_durable_inbound_action,
    inbound_action_shape_rejected,
)
from agent.durable_jobs.decisions import DecisionLedger, DecisionResult, JobAuthzPolicy
from agent.durable_jobs.effects import (
    CursorProviderPort,
    ProviderEffectClaim,
    ProviderEffectLedger,
    reconcile_cursor_create,
)
from agent.durable_jobs.service import PilotDisabledError
from agent.durable_jobs.slack_contract import (
    BindingRequiredError,
    SlackBindingLedger,
    SlackJobBinding,
    SlackMessagePort,
    deliver_slack_root,
    resolve_provider_origin,
)
from agent.durable_jobs.store import DurableJobStore


class DurableLaneService:
    def __init__(
        self,
        config: DurableJobsConfig,
        store: Optional[DurableJobStore] = None,
    ) -> None:
        self.config = config
        self._store = store

    def _require_enabled(self) -> None:
        if not self.config.enabled:
            raise PilotDisabledError(
                "durable_jobs.enabled is False; durable-lane slices are a no-op"
            )

    def _require_sqlite_path(self) -> DurableJobStore:
        if self.config.resolved_backend == "postgresql":
            raise DurableJobsConfigError(
                "durable-lane Slack/provider/decision ledgers do not fall back "
                "to SQLite when durable_jobs.backend is postgresql"
            )
        if self._store is not None:
            return self._store
        if self.config.sqlite_path is None:
            raise DurableJobsConfigError(
                "durable_jobs.sqlite_path must be set explicitly "
                "(disposable / test path); refusing default Hermes state.db"
            )
        self._store = DurableJobStore(sqlite_path=self.config.sqlite_path)
        return self._store

    def bind_slack(
        self,
        *,
        job_id: str,
        workspace_id: str,
        channel_id: str,
        root_thread_ts: str,
        candidate_id: str,
        candidate_version: str,
    ) -> SlackJobBinding:
        self._require_enabled()
        store = self._require_sqlite_path()
        return SlackBindingLedger(sqlite_path=store.sqlite_path).bind(
            job_id=job_id,
            workspace_id=workspace_id,
            channel_id=channel_id,
            root_thread_ts=root_thread_ts,
            candidate_id=candidate_id,
            candidate_version=candidate_version,
        )

    def deliver_slack_root(
        self, *, job_id: str, slack_port: SlackMessagePort,
        owner_token: Optional[str] = None,
    ) -> SlackJobBinding:
        self._require_enabled()
        store = self._require_sqlite_path()
        ledger = SlackBindingLedger(sqlite_path=store.sqlite_path)
        return deliver_slack_root(
            ledger, slack_port, job_id=job_id, owner_token=owner_token
        )

    def reconcile_cursor_create(
        self,
        *,
        job_id: str,
        action_id: str,
        origin_platform: str,
        origin_chat_id: str,
        origin_root_thread_id: str,
        candidate_id: str,
        candidate_version: str,
        provider: CursorProviderPort,
        owner_token: Optional[str] = None,
    ) -> ProviderEffectClaim:
        self._require_enabled()
        store = self._require_sqlite_path()
        binding = SlackBindingLedger(sqlite_path=store.sqlite_path).get_binding(job_id)
        if binding is None:
            raise BindingRequiredError(
                f"Slack binding required before provider effect for {job_id}"
            )
        if (
            binding.candidate_id != candidate_id
            or binding.candidate_version != candidate_version
        ):
            raise BindingRequiredError(
                f"provider effect candidate/version must match Slack binding for {job_id}"
            )
        origin_platform, origin_chat_id, origin_root_thread_id = (
            resolve_provider_origin(
                binding,
                origin_platform=origin_platform,
                origin_chat_id=origin_chat_id,
                origin_root_thread_id=origin_root_thread_id,
            )
        )
        ledger = ProviderEffectLedger(sqlite_path=store.sqlite_path)
        return reconcile_cursor_create(
            ledger,
            provider,
            job_id=job_id,
            action_id=action_id,
            origin_platform=origin_platform,
            origin_chat_id=origin_chat_id,
            origin_root_thread_id=origin_root_thread_id,
            candidate_id=binding.candidate_id,
            candidate_version=binding.candidate_version,
            owner_token=owner_token,
        )

    def set_job_policy(
        self,
        *,
        job_id: str,
        policy_version: str,
        allowed_actors: Sequence[str],
        expires_at: Optional[str] = None,
    ) -> JobAuthzPolicy:
        self._require_enabled()
        store = self._require_sqlite_path()
        return DecisionLedger(sqlite_path=store.sqlite_path).set_policy(
            job_id=job_id,
            policy_version=policy_version,
            allowed_actors=allowed_actors,
            expires_at=expires_at,
        )

    def record_decision(
        self,
        *,
        job_id: str,
        decision_type: str,
        candidate_id: str,
        candidate_version: str,
        actor_id: str,
        policy_version: str,
        decision_idempotency_key: str,
    ) -> DecisionResult:
        self._require_enabled()
        store = self._require_sqlite_path()
        return DecisionLedger(sqlite_path=store.sqlite_path).record_decision(
            job_id=job_id,
            decision_type=decision_type,
            candidate_id=candidate_id,
            candidate_version=candidate_version,
            actor_id=actor_id,
            policy_version=policy_version,
            decision_idempotency_key=decision_idempotency_key,
        )

    def consume_inbound_action(
        self,
        ack_port: InboundAckPort,
        *,
        job_id: str,
        workspace_id: str,
        channel_id: str,
        root_thread_ts: str,
        actor_id: str,
        decision_type: str,
        decision_idempotency_key: str,
        policy_version: str,
        candidate_id: str,
        candidate_version: str,
    ) -> InboundActionResult:
        """Durable Go/Pause/Cancel ingress. No parallel Slack router.

        Disabled and malformed identity reject before a store is constructed.
        Authorized consumption uses the existing coordinator ACK/decision lane.
        """
        self._require_enabled()
        if inbound_action_shape_rejected(
            job_id=job_id,
            workspace_id=workspace_id,
            channel_id=channel_id,
            root_thread_ts=root_thread_ts,
            actor_id=actor_id,
            decision_type=decision_type,
            decision_idempotency_key=decision_idempotency_key,
            policy_version=policy_version,
            candidate_id=candidate_id,
            candidate_version=candidate_version,
        ):
            return InboundActionResult(ok=False, ack_status="rejected")
        store = self._require_sqlite_path()
        return consume_durable_inbound_action(
            store.sqlite_path,
            ack_port,
            job_id=job_id,
            workspace_id=workspace_id,
            channel_id=channel_id,
            root_thread_ts=root_thread_ts,
            actor_id=actor_id,
            decision_type=decision_type,
            decision_idempotency_key=decision_idempotency_key,
            policy_version=policy_version,
            candidate_id=candidate_id,
            candidate_version=candidate_version,
        )
