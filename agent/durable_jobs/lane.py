"""Isolated durable-lane facade for ENG-26/ENG-27 slices.

Default-off: every mutating entry point requires ``durable_jobs.enabled``.
No live Cursor, Slack, network, gateway, or dispatch adapters are constructed.
Binding is required before any provider or Slack effect.

Close / mutation-lease invariant
--------------------------------
Linearization vs ``close()`` is ``_acquire_mutation_lease()`` under
``_lifecycle``. Store checkout is not a lease and does not authorize
writes. A caller that has not acquired a lease when ``_closed`` becomes
true is a loser: consume returns typed ``pending``/``retryable``; other
writers raise ``LaneClosedError``. Losers create no durable write,
event, decision, external effect, or ACK, and cannot reconstruct the
store.

A winner holds the lease through the first durable write and through ACK
or external effect, including ledger/store constructors that run schema
DDL and child threads started in that interval (claim-lease heartbeat).
``close()`` waits for in-flight leases held by *other* threads, then
drops the store, then returns — so after ``close()`` returns, no
non-owner remains able to write. Already-committed winner work is left
intact; a subsequent consume on the closed lane is a loser and must not
ACK again or write a second decision.

A ``close()`` call on a thread that already holds a mutation lease does
not wait for its own lease (that would deadlock with adapter/ACK
injection). Threads currently inside ``close()`` are excluded from the
wait set: two holders that both call ``close()`` must not wait for each
other. ``close()`` still sets ``_closed``, waits only for leases whose
owners are not already in ``close()``, drops the store, and raises
``LaneClosedError`` when invoked by a holder so shutdown is bounded and
fail-closed. After the lease context returns, heartbeat renews started
under that ownership interval have finished: they cannot still mutate.

Lock order: ``_lifecycle`` is never held across SQLite or adapter calls.
``close()`` waits on ``_close_idle`` (which releases ``_lifecycle``), so
it cannot deadlock with coordinator/SQLite.
"""

from __future__ import annotations

import logging
import sqlite3
import sys
import threading
from contextlib import contextmanager
from typing import Iterator, Optional, Sequence

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

logger = logging.getLogger(__name__)


class LaneClosedError(RuntimeError):
    """Closed durable lane refuses store reconstruction and mutation."""


class LaneIdentityRejected(RuntimeError):
    """Persisted job identity does not match the configured binding."""


class DurableLaneService:
    def __init__(
        self,
        config: DurableJobsConfig,
        store: Optional[DurableJobStore] = None,
    ) -> None:
        self.config = config
        self._store = store
        self._closed = False
        self._active_leases = 0
        self._leases_by_thread: dict[int, int] = {}
        # Leases held by threads currently executing close(). Those threads
        # cannot release until close() returns, so waiters must not wait on
        # them (two holder close() calls would otherwise deadlock).
        self._close_claimed_leases = 0
        self._lifecycle = threading.Lock()
        self._close_idle = threading.Condition(self._lifecycle)

    def close(self) -> None:
        """Idempotent shutdown. Waits for leases not already inside close().

        Same-thread lease holders cannot wait for their own lease. Concurrent
        holders that both call close() exclude each other from the wait set
        so neither deadlocks. Holder paths drop the store and raise
        ``LaneClosedError`` instead of hanging.
        """
        store = None
        self_held = 0
        with self._lifecycle:
            self._closed = True
            self_held = self._leases_by_thread.get(threading.get_ident(), 0)
            if self_held > 0:
                self._close_claimed_leases += self_held
                self._close_idle.notify_all()
            try:
                while self._active_leases > self._close_claimed_leases:
                    self._close_idle.wait()
                store = self._store
                self._store = None
            finally:
                if self_held > 0:
                    self._close_claimed_leases -= self_held
                    if self._close_claimed_leases < 0:
                        self._close_claimed_leases = 0
                    self._close_idle.notify_all()
        if store is not None and hasattr(store, "close"):
            try:
                store.close()
            except Exception:
                pass
        if self_held > 0:
            raise LaneClosedError(
                "close() from a mutation-lease holder is fail-closed"
            )

    def _after_admission(self) -> None:
        return None

    def _after_store_checkout(self) -> None:
        return None

    def _after_identity_validation(self) -> None:
        return None

    def _before_mutation_lease(self) -> None:
        return None

    def _after_idle_closed(self) -> None:
        """Called after the last mutation lease releases on a closed lane.

        Production is a no-op. The gateway seam may bind an instance hook
        so idle retirement state can be dropped without importing gateway
        from this module. Must not run while ``_lifecycle`` is held.
        """
        return None

    def _acquire_mutation_lease(self) -> None:
        with self._lifecycle:
            if self._closed:
                raise LaneClosedError("durable lane is closed")
            ident = threading.get_ident()
            self._leases_by_thread[ident] = self._leases_by_thread.get(ident, 0) + 1
            self._active_leases += 1

    def _release_mutation_lease(self) -> None:
        idle_closed = False
        with self._lifecycle:
            ident = threading.get_ident()
            held = self._leases_by_thread.get(ident, 0)
            if held <= 1:
                self._leases_by_thread.pop(ident, None)
            else:
                self._leases_by_thread[ident] = held - 1
            if self._active_leases > 0:
                self._active_leases -= 1
            self._close_idle.notify_all()
            idle_closed = self._closed and self._active_leases == 0
        if idle_closed:
            primary = sys.exc_info()[1]
            try:
                self._after_idle_closed()
            except Exception:
                if primary is not None:
                    logger.debug(
                        "durable lane idle-closed cleanup failed during unwind",
                        exc_info=True,
                    )
                else:
                    raise

    @contextmanager
    def _mutation_lease(self) -> Iterator[None]:
        self._acquire_mutation_lease()
        try:
            yield
        finally:
            self._release_mutation_lease()

    def _require_enabled(self) -> None:
        if not self.config.enabled:
            raise PilotDisabledError(
                "durable_jobs.enabled is False; durable-lane slices are a no-op"
            )

    def _require_sqlite_path(self) -> DurableJobStore:
        with self._lifecycle:
            if self._closed:
                raise LaneClosedError("durable lane is closed")
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
            sqlite_path = self.config.sqlite_path
        # DurableJobStore.__init__ runs schema DDL. That is a durable write, so
        # it must not hold ``_lifecycle`` and must run under a mutation lease
        # so close() cannot return until it finishes.
        self._acquire_mutation_lease()
        try:
            with self._lifecycle:
                if self._closed:
                    raise LaneClosedError("durable lane is closed")
                if self._store is not None:
                    return self._store
            store = DurableJobStore(sqlite_path=sqlite_path)
            with self._lifecycle:
                if self._store is None:
                    self._store = store
                return self._store
        finally:
            self._release_mutation_lease()

    def _repository_identity_rejected(
        self, store: DurableJobStore, job_id: str
    ) -> bool:
        binding = self.config.identity_binding
        if binding is None or not str(binding.repository_identity or "").strip():
            return True
        job = store.get_job(job_id)
        return job is None or job.repository_identity != binding.repository_identity

    def _persisted_workspace_id(
        self, store: DurableJobStore, job_id: str
    ) -> Optional[str]:
        """Return the bound workspace id, or None when no binding row exists.

        Schema/read failures propagate as ``sqlite3.OperationalError`` so
        callers can fail closed instead of treating an unreadable table as
        an unbound bootstrap.
        """
        with store._connect() as conn:
            row = conn.execute(
                "SELECT workspace_id FROM slack_job_bindings WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        if row is None:
            return None
        value = row["workspace_id"] if isinstance(row, sqlite3.Row) else row[0]
        return str(value or "").strip()

    def _identity_rejected(
        self,
        store: DurableJobStore,
        job_id: str,
        *,
        workspace_id: Optional[str] = None,
        allow_missing_workspace: bool = False,
    ) -> bool:
        if self._repository_identity_rejected(store, job_id):
            return True
        binding = self.config.identity_binding
        if binding is None:
            return True
        expected_ws = str(binding.workspace_id or "").strip()
        if not expected_ws:
            return True
        if workspace_id is not None and str(workspace_id).strip() != expected_ws:
            return True
        try:
            persisted = self._persisted_workspace_id(store, job_id)
        except sqlite3.OperationalError:
            return True
        if persisted is None:
            return not allow_missing_workspace
        if persisted != expected_ws:
            return True
        return False

    def _checkout_for_mutation(self) -> DurableJobStore:
        self._require_enabled()
        self._after_admission()
        store = self._require_sqlite_path()
        self._after_store_checkout()
        if self._closed:
            raise LaneClosedError("durable lane is closed")
        return store

    def _acquire_authorized_mutation(
        self,
        job_id: str,
        *,
        workspace_id: Optional[str] = None,
        allow_missing_workspace: bool = False,
    ) -> DurableJobStore:
        store = self._checkout_for_mutation()
        if self._identity_rejected(
            store,
            job_id,
            workspace_id=workspace_id,
            allow_missing_workspace=allow_missing_workspace,
        ):
            raise LaneIdentityRejected(
                "durable lane rejected unbound repository/workspace identity"
            )
        self._after_identity_validation()
        if self._closed:
            raise LaneClosedError("durable lane is closed")
        self._before_mutation_lease()
        return store

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
        store = self._acquire_authorized_mutation(
            job_id,
            workspace_id=workspace_id,
            allow_missing_workspace=True,
        )
        with self._mutation_lease():
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
        store = self._acquire_authorized_mutation(job_id)
        with self._mutation_lease():
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
        store = self._acquire_authorized_mutation(job_id)
        with self._mutation_lease():
            # SlackBindingLedger/ProviderEffectLedger construct DurableJobStore
            # and run schema DDL. That is a durable write and must not race close.
            binding = SlackBindingLedger(sqlite_path=store.sqlite_path).get_binding(
                job_id
            )
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
        store = self._acquire_authorized_mutation(job_id)
        with self._mutation_lease():
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
        store = self._acquire_authorized_mutation(job_id)
        with self._mutation_lease():
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
        Repository/workspace identity is fail-closed here before a mutation
        lease. Authorized consumption uses the existing coordinator
        ACK/decision lane only while holding a mutation lease.
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
        if getattr(self, "_closed", False):
            return InboundActionResult(
                ok=False, ack_status="pending", retryable=True
            )
        self._after_admission()
        try:
            store = self._require_sqlite_path()
        except LaneClosedError:
            return InboundActionResult(
                ok=False, ack_status="pending", retryable=True
            )
        self._after_store_checkout()
        if self._closed:
            return InboundActionResult(
                ok=False, ack_status="pending", retryable=True
            )
        try:
            if self._identity_rejected(
                store, job_id, workspace_id=workspace_id
            ):
                return InboundActionResult(ok=False, ack_status="rejected")
        except LaneClosedError:
            return InboundActionResult(
                ok=False, ack_status="pending", retryable=True
            )
        except sqlite3.OperationalError:
            return InboundActionResult(
                ok=False, ack_status="pending", retryable=True
            )
        self._after_identity_validation()
        if self._closed:
            return InboundActionResult(
                ok=False, ack_status="pending", retryable=True
            )
        self._before_mutation_lease()
        try:
            with self._mutation_lease():
                result = consume_durable_inbound_action(
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
                with self._lifecycle:
                    # Holder close/shutdown already dropped the store.
                    # A concurrent closer still waiting has _closed True but
                    # keeps the store until this lease releases (winner ACK).
                    shutdown_completed = self._closed and self._store is None
                if shutdown_completed:
                    return InboundActionResult(
                        ok=False, ack_status="pending", retryable=True
                    )
                return result
        except LaneClosedError:
            return InboundActionResult(
                ok=False, ack_status="pending", retryable=True
            )
        except sqlite3.OperationalError:
            return InboundActionResult(
                ok=False, ack_status="pending", retryable=True
            )
