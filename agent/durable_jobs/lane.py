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

import json
import logging
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Iterator, Optional, Sequence

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
            raise LaneClosedError("close() from a mutation-lease holder is fail-closed")

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

    def _release_mutation_lease(self, *, preserve_primary: bool = False) -> None:
        """Drop one lease. Hook Exception is suppressed only if the body failed.

        ``preserve_primary`` comes from explicit caller control flow, not
        ``sys.exc_info()`` — an already-handled outer except would otherwise
        look like a primary and swallow cleanup after a successful body.
        Hook ``BaseException`` is not caught.
        """
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
            try:
                self._after_idle_closed()
            except Exception:
                if preserve_primary:
                    logger.debug(
                        "durable lane idle-closed cleanup failed during unwind",
                        exc_info=True,
                    )
                else:
                    raise

    @contextmanager
    def _mutation_lease(self) -> Iterator[None]:
        self._acquire_mutation_lease()
        body_failed = False
        try:
            yield
        except BaseException:
            body_failed = True
            raise
        finally:
            self._release_mutation_lease(preserve_primary=body_failed)

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
        body_failed = False
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
        except BaseException:
            body_failed = True
            raise
        finally:
            self._release_mutation_lease(preserve_primary=body_failed)

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

    def resume_session_handoff(
        self,
        *,
        job_id: str,
        parent_session_id: str,
        handoff: Any,
        waypoint: Any,
        pressure: Any,
        linear: Any,
        slack: Any,
        sessions: Any,
        handoff_config: Any,
        manual_resume: bool = False,
    ) -> Any:
        """Run inactive ENG-122 handoff work under this lane's write lease."""
        from agent.durable_jobs.session_handoff import (
            EffectClaim,
            EffectOwnershipLost,
            EffectReconciliationRequired,
            HandoffIdentityMismatch,
            HandoffNotArmed,
            HandoffPressure,
            HandoffState,
            ManualResumeRequired,
            ProjectionVerificationError,
            SemanticWaypoint,
            SessionHandoff,
            SessionHandoffLedger,
            UnsafeHandoffWaypoint,
            _EffectOwnerGuard,
            _MANUAL_RESUME,
            _STAGE_INDEX,
        )
        from agent.durable_jobs.redaction import redact_secret_text
        import re
        from uuid import uuid4

        class _SessionHandoffCoordinator:
            def __init__(
                self,
                *,
                ledger: SessionHandoffLedger,
                linear: LinearHandoffProjection,
                slack: SlackHandoffProjection,
                sessions: ChildSessionPort,
            ) -> None:
                self.ledger = ledger
                self.linear = linear
                self.slack = slack
                self.sessions = sessions

            def _claim_effect(
                self, job_id: str, handoff_id: str, effect_name: str
            ) -> tuple[EffectClaim, _EffectOwnerGuard]:
                guard = self.ledger.effect_owner_guard(job_id, handoff_id, effect_name)
                try:
                    guard.acquire()
                except EffectOwnershipLost as exc:
                    raise EffectReconciliationRequired(
                        f"{effect_name} has a live effect owner"
                    ) from exc
                owner_token = uuid4().hex
                try:
                    claim = self.ledger.claim_effect(
                        job_id,
                        handoff_id,
                        effect_name,
                        owner_token=owner_token,
                    )
                    if not claim.acquired:
                        raise EffectReconciliationRequired(
                            f"{effect_name} is {claim.status}; reconcile its durable claim before resume"
                        )
                    return claim, guard
                except BaseException:
                    guard.release()
                    raise

            def _complete_effect(
                self,
                claim: EffectClaim,
                guard: _EffectOwnerGuard,
                *,
                receipt: str | None = None,
            ) -> HandoffState:
                try:
                    return self.ledger.complete_effect(
                        claim.job_id,
                        claim.handoff_id,
                        claim.effect_name,
                        owner_token=claim.owner_token or "",
                        expected_generation=claim.generation,
                        receipt=receipt,
                    )
                finally:
                    guard.release()

            def resume(
                self,
                *,
                job_id: str,
                parent_session_id: str,
                handoff: SessionHandoff,
                waypoint: SemanticWaypoint,
                pressure: HandoffPressure,
                manual_resume: bool = False,
            ) -> HandoffState:
                if type(manual_resume) is not bool:
                    raise ValueError("manual_resume must be a boolean")
                if not waypoint.safe:
                    raise UnsafeHandoffWaypoint(
                        "handoff requires a verified safe semantic waypoint"
                    )
                if not pressure.armed:
                    raise HandoffNotArmed(
                        "session handoff has not reached its configured soft threshold"
                    )
                key = handoff.idempotency_key
                if redact_secret_text(key) != key:
                    raise HandoffIdentityMismatch(
                        "handoff idempotency key must not contain secret-bearing text"
                    )
                if redact_secret_text(
                    handoff.issue
                ) != handoff.issue or not re.fullmatch(
                    r"[A-Z][A-Z0-9]*-[1-9][0-9]*", handoff.issue
                ):
                    raise HandoffIdentityMismatch(
                        "issue must be a secret-free Linear identifier"
                    )
                state = self.ledger.stage(job_id, parent_session_id, handoff)
                if state.failure_reason and not manual_resume:
                    raise ManualResumeRequired(
                        state.manual_resume_action or _MANUAL_RESUME
                    )
                if state.failure_reason:
                    state = self.ledger.resume_failed(job_id, handoff.handoff_id)
                canonical = self.ledger.canonical(job_id, handoff.handoff_id)
                canonical_payload = json.loads(canonical)
                safe_resume_pointer = str(canonical_payload["resume_pointer"])
                safe_next_action = str(canonical_payload["next_action"])
                active_claim: EffectClaim | None = None
                active_guard: _EffectOwnerGuard | None = None
                try:
                    if _STAGE_INDEX[state.stage] < _STAGE_INDEX["LINEAR_VERIFIED"]:
                        active_claim, active_guard = self._claim_effect(
                            job_id, handoff.handoff_id, "LINEAR_UPSERT"
                        )
                        receipt = self.linear.upsert_handoff(
                            issue=handoff.issue,
                            canonical=canonical,
                            idempotency_key=f"{key}:linear",
                        )
                        if self.linear.read_handoff(issue=handoff.issue) != canonical:
                            raise ProjectionVerificationError(
                                "Linear readback mismatch"
                            )
                        state = self._complete_effect(
                            active_claim, active_guard, receipt=receipt
                        )
                        active_guard = None
                    if _STAGE_INDEX[state.stage] < _STAGE_INDEX["SLACK_RECEIPTED"]:
                        active_claim, active_guard = self._claim_effect(
                            job_id, handoff.handoff_id, "SLACK_RECEIPT"
                        )
                        receipt = self.slack.post_handoff_receipt(
                            handoff_id=handoff.handoff_id,
                            resume_pointer=safe_resume_pointer,
                            idempotency_key=f"{key}:slack",
                        )
                        state = self._complete_effect(
                            active_claim, active_guard, receipt=receipt
                        )
                        active_guard = None
                    if _STAGE_INDEX[state.stage] < _STAGE_INDEX["CHILD_CREATED"]:
                        active_claim, active_guard = self._claim_effect(
                            job_id, handoff.handoff_id, "CHILD_CREATE"
                        )
                        child = self.sessions.find_or_create_child(
                            parent_session_id=parent_session_id,
                            handoff_id=handoff.handoff_id,
                            idempotency_key=f"{key}:child",
                        )
                        state = self._complete_effect(
                            active_claim, active_guard, receipt=child
                        )
                        active_guard = None
                    child_id = state.child_session_id
                    if not child_id:
                        raise ProjectionVerificationError(
                            "child session receipt missing"
                        )
                    if _STAGE_INDEX[state.stage] < _STAGE_INDEX["HANDOFF_INJECTED"]:
                        active_claim, active_guard = self._claim_effect(
                            job_id, handoff.handoff_id, "HANDOFF_INJECT"
                        )
                        self.sessions.inject_handoff(
                            child_session_id=child_id,
                            canonical=canonical,
                            idempotency_key=f"{key}:inject",
                        )
                        state = self._complete_effect(active_claim, active_guard)
                        active_guard = None
                    if _STAGE_INDEX[state.stage] < _STAGE_INDEX["FIRST_TURN_STARTED"]:
                        active_claim, active_guard = self._claim_effect(
                            job_id, handoff.handoff_id, "FIRST_TURN_START"
                        )
                        self.sessions.start_first_turn(
                            child_session_id=child_id,
                            next_action=safe_next_action,
                            idempotency_key=f"{key}:first-turn",
                        )
                        state = self._complete_effect(active_claim, active_guard)
                        active_guard = None
                    if state.stage != "COMPLETE":
                        state = self.ledger.advance(
                            job_id, handoff.handoff_id, "COMPLETE"
                        )
                    return state
                except EffectReconciliationRequired:
                    raise
                except Exception as exc:
                    if active_claim is None:
                        self.ledger.fail_closed(
                            job_id, handoff.handoff_id, type(exc).__name__
                        )
                    else:
                        self.ledger.fail_closed(
                            job_id,
                            handoff.handoff_id,
                            type(exc).__name__,
                            effect_name=active_claim.effect_name,
                            expected_owner_token=active_claim.owner_token,
                            expected_generation=active_claim.generation,
                        )
                    raise
                finally:
                    if active_guard is not None:
                        active_guard.release()

        if handoff_config.enabled is not True or handoff_config.shadow is not False:
            raise PilotDisabledError(
                "session handoff effects require enabled=True and shadow=False"
            )
        store = self._acquire_authorized_mutation(job_id)
        job = store.get_job(job_id)
        if job is None:
            raise KeyError(job_id)
        if str(handoff.repository).strip() != str(job.repository_identity).strip():
            raise HandoffIdentityMismatch(
                "handoff repository does not match the durable job"
            )
        if not job.frozen_baseline_sha or handoff.exact_sha != job.frozen_baseline_sha:
            raise HandoffIdentityMismatch(
                "handoff requires a matching non-empty durable job baseline SHA"
            )
        with self._mutation_lease():
            return _SessionHandoffCoordinator(
                ledger=SessionHandoffLedger(store.sqlite_path),
                linear=linear,
                slack=slack,
                sessions=sessions,
            ).resume(
                job_id=job_id,
                parent_session_id=parent_session_id,
                handoff=handoff,
                waypoint=waypoint,
                pressure=pressure,
                manual_resume=manual_resume,
            )

    def reconcile_session_handoff_effect(
        self,
        *,
        job_id: str,
        handoff_id: str,
        effect_name: str,
        outcome: str,
        receipt: Optional[str] = None,
        expected_owner_token: str,
        expected_generation: int,
        dead_owner_verified: bool,
        handoff_config: Any,
    ) -> Any:
        """Resolve a verified ambiguous handoff effect under the lane write lease."""
        from agent.durable_jobs.session_handoff import (
            HandoffIdentityMismatch,
            SessionHandoffLedger,
        )

        if handoff_config.enabled is not True or handoff_config.shadow is not False:
            raise PilotDisabledError(
                "session handoff reconciliation requires enabled=True and shadow=False"
            )
        with self._mutation_lease():
            store = self._acquire_authorized_mutation(job_id)
            job = store.get_job(job_id)
            if job is None:
                raise KeyError(job_id)
            ledger = SessionHandoffLedger(store.sqlite_path)
            state = ledger.get(job_id, handoff_id)
            if state is None:
                raise KeyError(handoff_id)
            canonical = json.loads(ledger.canonical(job_id, handoff_id))
            if (
                str(canonical.get("repository", "")).strip()
                != str(job.repository_identity).strip()
                or not job.frozen_baseline_sha
                or canonical.get("exact_sha") != job.frozen_baseline_sha
            ):
                raise HandoffIdentityMismatch(
                    "reconciliation requires current durable job repository and frozen SHA identity"
                )
            with ledger.effect_owner_guard(job_id, handoff_id, effect_name) as guard:
                return ledger.reconcile_effect(
                    job_id=job_id,
                    handoff_id=handoff_id,
                    effect_name=effect_name,
                    outcome=outcome,
                    receipt=receipt,
                    expected_owner_token=expected_owner_token,
                    expected_generation=expected_generation,
                    dead_owner_verified=dead_owner_verified,
                    owner_guard=guard,
                )

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
        self,
        *,
        job_id: str,
        slack_port: SlackMessagePort,
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
            return InboundActionResult(ok=False, ack_status="pending", retryable=True)
        self._after_admission()
        try:
            store = self._require_sqlite_path()
        except LaneClosedError:
            return InboundActionResult(ok=False, ack_status="pending", retryable=True)
        self._after_store_checkout()
        if self._closed:
            return InboundActionResult(ok=False, ack_status="pending", retryable=True)
        try:
            if self._identity_rejected(store, job_id, workspace_id=workspace_id):
                return InboundActionResult(ok=False, ack_status="rejected")
        except LaneClosedError:
            return InboundActionResult(ok=False, ack_status="pending", retryable=True)
        except sqlite3.OperationalError:
            return InboundActionResult(ok=False, ack_status="pending", retryable=True)
        self._after_identity_validation()
        if self._closed:
            return InboundActionResult(ok=False, ack_status="pending", retryable=True)
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
            return InboundActionResult(ok=False, ack_status="pending", retryable=True)
        except sqlite3.OperationalError:
            return InboundActionResult(ok=False, ack_status="pending", retryable=True)
