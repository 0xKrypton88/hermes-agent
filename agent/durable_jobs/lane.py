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

import inspect
import json
import logging
import os
import sqlite3
import threading
import weakref
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


def _build_handoff_operation_gate():
    """Lexically seal handoff mutation tickets inside real lane operations."""
    import agent.durable_jobs.session_handoff as handoff_module

    authority_type = handoff_module._HandoffMutationAuthority
    authority_issuer = handoff_module._MUTATION_AUTHORITY_ISSUER
    handoff_type = handoff_module.SessionHandoff
    handoff_identity_error = handoff_module.HandoffIdentityMismatch
    handoff_canonical_json = handoff_type.canonical_json
    handoff_fields = tuple(handoff_type.__dataclass_fields__)
    handoff_tuple_fields = frozenset({
        "verified",
        "pending",
        "remaining",
        "blockers",
        "test_evidence",
        "risk_gates",
        "forbidden_actions",
    })
    ledger_type = handoff_module.SessionHandoffLedger
    mutators = {
        name: getattr(ledger_type, name)
        for name in (
            "_stage",
            "_resume_failed",
            "_claim_effect",
            "_complete_effect",
            "_advance",
            "_fail_closed",
            "_reconcile_effect",
        )
    }
    seal_authority_validator = handoff_module._seal_handoff_authority_validator
    active: dict[object, tuple[weakref.ReferenceType[Any], int, str, str]] = {}
    operations: dict[int, tuple[weakref.ReferenceType[Any], int, str, str]] = {}
    authentic_instances: weakref.WeakSet[Any] = weakref.WeakSet()
    authentic_lane_type: type[Any] | None = None
    authentic_mutation_lease: Any = None
    registry_lock = threading.Lock()
    callback_threads: tuple[int, ...] = ()

    def freeze_handoff(handoff: Any) -> tuple[Any, str]:
        """Snapshot caller data before any mutation capability exists."""
        values: dict[str, Any] = {}
        for name in handoff_fields:
            try:
                value = getattr(handoff, name)
            except Exception as exc:
                raise handoff_identity_error(
                    f"handoff field {name!r} could not be read safely"
                ) from exc
            if name in handoff_tuple_fields:
                if type(value) is not tuple or any(
                    type(item) is not str for item in value
                ):
                    raise handoff_identity_error(
                        f"handoff field {name!r} must be a tuple of plain strings"
                    )
            elif name == "version":
                if type(value) is not int:
                    raise handoff_identity_error(
                        "handoff version must be a plain integer"
                    )
            elif type(value) is not str:
                raise handoff_identity_error(
                    f"handoff field {name!r} must be a plain string"
                )
            values[name] = value
        frozen = handoff_type(**values)
        return frozen, handoff_canonical_json(frozen)

    def seal_lane_type(lane_type: type[Any]) -> None:
        nonlocal authentic_lane_type, authentic_mutation_lease
        if authentic_lane_type is not None or not isinstance(lane_type, type):
            raise RuntimeError("durable lane type is already sealed")
        authentic_lane_type = lane_type
        authentic_mutation_lease = lane_type._mutation_lease

    def validate(ticket: object, sqlite_path: Any, job_id: str) -> bool:
        normalized = os.path.normcase(os.path.abspath(os.fspath(sqlite_path)))
        with registry_lock:
            record = active.get(ticket)
        if record is None:
            return False
        lane_ref, owner_thread, expected_path, expected_job = record
        lane = lane_ref()
        return (
            lane is not None
            and type(lane) is authentic_lane_type
            and lane in authentic_instances
            and owner_thread == threading.get_ident()
            and expected_path == normalized
            and expected_job == str(job_id)
        )

    def constructor_gate(constructor: Any) -> Any:
        def guarded_constructor(lane: Any, *args: Any, **kwargs: Any) -> Any:
            result = constructor(lane, *args, **kwargs)
            if authentic_lane_type is not None and type(lane) is authentic_lane_type:
                authentic_instances.add(lane)
            return result

        guarded_constructor.__name__ = constructor.__name__
        guarded_constructor.__qualname__ = constructor.__qualname__
        guarded_constructor.__doc__ = constructor.__doc__
        guarded_constructor.__module__ = constructor.__module__
        guarded_constructor.__annotations__ = constructor.__annotations__
        guarded_constructor.__signature__ = inspect.signature(constructor)  # type: ignore[attr-defined]
        return guarded_constructor

    def operation_gate(operation: Any) -> Any:
        def guarded_operation(lane: Any, *args: Any, **kwargs: Any) -> Any:
            nonlocal callback_threads
            from agent.durable_jobs.session_handoff import (
                HandoffMutationUnauthorized,
            )

            if type(lane) is not authentic_lane_type or lane not in authentic_instances:
                raise HandoffMutationUnauthorized(
                    "handoff mutation authority requires an authentic durable lane"
                )

            if authentic_mutation_lease is None:
                raise HandoffMutationUnauthorized(
                    "handoff mutation authority requires a sealed lifecycle lease"
                )

            @contextmanager
            def authorize_operation(sqlite_path: Any, job_id: str) -> Iterator[Any]:
                store = lane._acquire_authorized_mutation(job_id)
                normalized = os.path.normcase(os.path.abspath(os.fspath(sqlite_path)))
                authorized_path = os.path.normcase(
                    os.path.abspath(os.fspath(store.sqlite_path))
                )
                if normalized != authorized_path:
                    raise HandoffMutationUnauthorized(
                        "handoff mutation authority database does not match the authorized job store"
                    )
                with authentic_mutation_lease(lane):
                    # Schema creation/migration is itself narrowly authorized and finishes
                    # before any adapter or other attacker-controlled callback can run.
                    lane_ref = weakref.ref(lane)
                    path_identity = normalized
                    setup_ticket = object()
                    with registry_lock:
                        active[setup_ticket] = (
                            lane_ref,
                            threading.get_ident(),
                            path_identity,
                            job_id,
                        )
                    try:
                        setup_authority = authority_type(
                            authority_issuer,
                            setup_ticket,
                            normalized,
                            str(job_id),
                        )
                        ledger_type(normalized, mutation_authority=setup_authority)
                    finally:
                        with registry_lock:
                            active.pop(setup_ticket, None)
                    operation_token = object()
                    with registry_lock:
                        operations[id(operation_token)] = (
                            weakref.ref(lane),
                            threading.get_ident(),
                            normalized,
                            str(job_id),
                        )
                    try:
                        yield operation_token, normalized
                    finally:
                        with registry_lock:
                            operations.pop(id(operation_token), None)

            def mutate(
                operation_token: object,
                method_name: str,
                *method_args: Any,
                **method_kwargs: Any,
            ) -> Any:
                if threading.get_ident() in callback_threads:
                    raise HandoffMutationUnauthorized(
                        "handoff mutation is forbidden during an external callback"
                    )
                with registry_lock:
                    operation_record = operations.get(id(operation_token))
                if operation_record is None:
                    raise HandoffMutationUnauthorized(
                        "handoff mutation requires an active operation token"
                    )
                lane_ref, owner_thread, normalized, authorized_job_id = operation_record
                if (
                    type(operation_token) is not object
                    or lane_ref() is not lane
                    or owner_thread != threading.get_ident()
                ):
                    raise HandoffMutationUnauthorized(
                        "handoff mutation operation identity is invalid"
                    )
                mutator = mutators.get(method_name)
                if mutator is None:
                    raise HandoffMutationUnauthorized(
                        "handoff mutation method is not authorized"
                    )
                ticket = object()
                with registry_lock:
                    active[ticket] = operation_record
                try:
                    ledger = ledger_type(
                        normalized,
                        mutation_authority=authority_type(
                            authority_issuer,
                            ticket,
                            normalized,
                            authorized_job_id,
                        ),
                    )
                    return mutator(ledger, *method_args, **method_kwargs)
                finally:
                    with registry_lock:
                        active.pop(ticket, None)

            def invoke_external(
                target: Any,
                method_name: str,
                return_kind: str,
                **call_kwargs: Any,
            ) -> Any:
                """Run and freeze caller-controlled code while mutation is sealed."""
                nonlocal callback_threads
                owner_thread = threading.get_ident()
                callback_threads = (*callback_threads, owner_thread)
                try:
                    result = getattr(target, method_name)(**call_kwargs)
                    if return_kind == "str":
                        if type(result) is not str:
                            raise TypeError(
                                f"{method_name} must return an exact plain string"
                            )
                        return result
                    if return_kind == "none":
                        if result is not None:
                            raise TypeError(f"{method_name} must return None")
                        return None
                    raise TypeError("invalid sealed callback return kind")
                finally:
                    current = callback_threads
                    for index in range(len(current) - 1, -1, -1):
                        if current[index] == owner_thread:
                            callback_threads = current[:index] + current[index + 1 :]
                            break

            kwargs["_authorize_handoff_operation"] = authorize_operation
            kwargs["_mutate_handoff"] = mutate
            kwargs["_freeze_handoff"] = freeze_handoff
            kwargs["_invoke_handoff_callback"] = invoke_external
            return operation(lane, *args, **kwargs)

        guarded_operation.__name__ = operation.__name__
        guarded_operation.__qualname__ = operation.__qualname__
        guarded_operation.__doc__ = operation.__doc__
        guarded_operation.__module__ = operation.__module__
        guarded_operation.__annotations__ = operation.__annotations__
        operation_signature = inspect.signature(operation)
        guarded_operation.__signature__ = operation_signature.replace(  # type: ignore[attr-defined]
            parameters=[
                parameter
                for parameter in operation_signature.parameters.values()
                if parameter.name
                not in {
                    "_authorize_handoff_operation",
                    "_mutate_handoff",
                    "_freeze_handoff",
                    "_invoke_handoff_callback",
                }
            ]
        )
        return guarded_operation

    seal_authority_validator(validate)
    delattr(handoff_module, "_seal_handoff_authority_validator")
    return seal_lane_type, constructor_gate, operation_gate, validate


(
    _seal_handoff_lane_type,
    _handoff_lane_constructor,
    _handoff_operation,
    _validate_handoff_mutation_ticket,
) = _build_handoff_operation_gate()


class LaneClosedError(RuntimeError):
    """Closed durable lane refuses store reconstruction and mutation."""


class LaneIdentityRejected(RuntimeError):
    """Persisted job identity does not match the configured binding."""


class DurableLaneService:
    @_handoff_lane_constructor
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

    def _has_active_mutation_lease(self) -> bool:
        with self._lifecycle:
            return self._active_leases > 0

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

    @_handoff_operation
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
        _authorize_handoff_operation: Any,
        _mutate_handoff: Any,
        _freeze_handoff: Any,
        _invoke_handoff_callback: Any,
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
                self,
                mutate: Any,
                operation_token: object,
                job_id: str,
                handoff_id: str,
                effect_name: str,
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
                    claim = mutate(
                        operation_token,
                        "_claim_effect",
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
                mutate: Any,
                operation_token: object,
                *,
                receipt: str | None = None,
            ) -> HandoffState:
                try:
                    return mutate(
                        operation_token,
                        "_complete_effect",
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
                _mutate: Any,
                _operation_token: object,
                _canonical_handoff: str,
                _handoff_id: str,
                _idempotency_key: str,
                _safe_waypoint: bool,
                _pressure_armed: bool,
                _invoke_callback: Any,
            ) -> HandoffState:
                if type(manual_resume) is not bool:
                    raise ValueError("manual_resume must be a boolean")
                if not _safe_waypoint:
                    raise UnsafeHandoffWaypoint(
                        "handoff requires a verified safe semantic waypoint"
                    )
                if not _pressure_armed:
                    raise HandoffNotArmed(
                        "session handoff has not reached its configured soft threshold"
                    )
                key = _idempotency_key
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
                state = _mutate(
                    _operation_token,
                    "_stage",
                    job_id,
                    parent_session_id,
                    handoff,
                    canonical_payload=_canonical_handoff,
                    staged_handoff_id=_handoff_id,
                    staged_idempotency_key=_idempotency_key,
                )
                if state.failure_reason and not manual_resume:
                    raise ManualResumeRequired(
                        state.manual_resume_action or _MANUAL_RESUME
                    )
                if state.failure_reason:
                    state = _mutate(
                        _operation_token, "_resume_failed", job_id, handoff.handoff_id
                    )
                canonical = self.ledger.canonical(job_id, handoff.handoff_id)
                canonical_payload = json.loads(canonical)
                safe_resume_pointer = str(canonical_payload["resume_pointer"])
                safe_next_action = str(canonical_payload["next_action"])
                active_claim: EffectClaim | None = None
                active_guard: _EffectOwnerGuard | None = None
                try:
                    if _STAGE_INDEX[state.stage] < _STAGE_INDEX["LINEAR_VERIFIED"]:
                        active_claim, active_guard = self._claim_effect(
                            _mutate,
                            _operation_token,
                            job_id,
                            handoff.handoff_id,
                            "LINEAR_UPSERT",
                        )
                        receipt = _invoke_callback(
                            self.linear,
                            "upsert_handoff",
                            "str",
                            issue=handoff.issue,
                            canonical=canonical,
                            idempotency_key=f"{key}:linear",
                        )
                        if (
                            _invoke_callback(
                                self.linear,
                                "read_handoff",
                                "str",
                                issue=handoff.issue,
                            )
                            != canonical
                        ):
                            raise ProjectionVerificationError(
                                "Linear readback mismatch"
                            )
                        state = self._complete_effect(
                            active_claim,
                            active_guard,
                            _mutate,
                            _operation_token,
                            receipt=receipt,
                        )
                        active_guard = None
                    if _STAGE_INDEX[state.stage] < _STAGE_INDEX["SLACK_RECEIPTED"]:
                        active_claim, active_guard = self._claim_effect(
                            _mutate,
                            _operation_token,
                            job_id,
                            handoff.handoff_id,
                            "SLACK_RECEIPT",
                        )
                        receipt = _invoke_callback(
                            self.slack,
                            "post_handoff_receipt",
                            "str",
                            handoff_id=handoff.handoff_id,
                            resume_pointer=safe_resume_pointer,
                            idempotency_key=f"{key}:slack",
                        )
                        state = self._complete_effect(
                            active_claim,
                            active_guard,
                            _mutate,
                            _operation_token,
                            receipt=receipt,
                        )
                        active_guard = None
                    if _STAGE_INDEX[state.stage] < _STAGE_INDEX["CHILD_CREATED"]:
                        active_claim, active_guard = self._claim_effect(
                            _mutate,
                            _operation_token,
                            job_id,
                            handoff.handoff_id,
                            "CHILD_CREATE",
                        )
                        child = _invoke_callback(
                            self.sessions,
                            "find_or_create_child",
                            "str",
                            parent_session_id=parent_session_id,
                            handoff_id=handoff.handoff_id,
                            idempotency_key=f"{key}:child",
                        )
                        state = self._complete_effect(
                            active_claim,
                            active_guard,
                            _mutate,
                            _operation_token,
                            receipt=child,
                        )
                        active_guard = None
                    child_id = state.child_session_id
                    if not child_id:
                        raise ProjectionVerificationError(
                            "child session receipt missing"
                        )
                    if _STAGE_INDEX[state.stage] < _STAGE_INDEX["HANDOFF_INJECTED"]:
                        active_claim, active_guard = self._claim_effect(
                            _mutate,
                            _operation_token,
                            job_id,
                            handoff.handoff_id,
                            "HANDOFF_INJECT",
                        )
                        _invoke_callback(
                            self.sessions,
                            "inject_handoff",
                            "none",
                            child_session_id=child_id,
                            canonical=canonical,
                            idempotency_key=f"{key}:inject",
                        )
                        state = self._complete_effect(
                            active_claim, active_guard, _mutate, _operation_token
                        )
                        active_guard = None
                    if _STAGE_INDEX[state.stage] < _STAGE_INDEX["FIRST_TURN_STARTED"]:
                        active_claim, active_guard = self._claim_effect(
                            _mutate,
                            _operation_token,
                            job_id,
                            handoff.handoff_id,
                            "FIRST_TURN_START",
                        )
                        _invoke_callback(
                            self.sessions,
                            "start_first_turn",
                            "none",
                            child_session_id=child_id,
                            next_action=safe_next_action,
                            idempotency_key=f"{key}:first-turn",
                        )
                        state = self._complete_effect(
                            active_claim, active_guard, _mutate, _operation_token
                        )
                        active_guard = None
                    if state.stage != "COMPLETE":
                        state = _mutate(
                            _operation_token,
                            "_advance",
                            job_id,
                            handoff.handoff_id,
                            "COMPLETE",
                        )
                    return state
                except EffectReconciliationRequired:
                    raise
                except Exception as exc:
                    if active_claim is None:
                        _mutate(
                            _operation_token,
                            "_fail_closed",
                            job_id,
                            handoff.handoff_id,
                            type(exc).__name__,
                        )
                    else:
                        _mutate(
                            _operation_token,
                            "_fail_closed",
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

        if type(parent_session_id) is not str:
            raise HandoffIdentityMismatch(
                "parent_session_id must be an exact plain string"
            )
        handoff, canonical_handoff = _freeze_handoff(handoff)
        if type(waypoint) is not SemanticWaypoint or any(
            type(value) is not bool
            for value in (
                waypoint.verified,
                waypoint.tool_active,
                waypoint.external_mutation_active,
                waypoint.commit_active,
                waypoint.push_active,
                waypoint.deploy_active,
                waypoint.authority_boundary_active,
            )
        ):
            raise UnsafeHandoffWaypoint(
                "waypoint must contain exact plain boolean state"
            )
        safe_waypoint = waypoint.safe
        if (
            type(pressure) is not HandoffPressure
            or type(pressure.armed) is not bool
            or type(pressure.hard) is not bool
            or type(pressure.ratio) is not float
        ):
            raise HandoffNotArmed("pressure must contain exact plain scalar state")
        pressure_armed = pressure.armed
        frozen_handoff_id = handoff.handoff_id
        frozen_idempotency_key = handoff.idempotency_key
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
        authorized_job_id = str(job_id)
        with _authorize_handoff_operation(store.sqlite_path, authorized_job_id) as (
            operation_token,
            authorized_sqlite_path,
        ):
            return _SessionHandoffCoordinator(
                ledger=SessionHandoffLedger(authorized_sqlite_path),
                linear=linear,
                slack=slack,
                sessions=sessions,
            ).resume(
                job_id=authorized_job_id,
                parent_session_id=parent_session_id,
                handoff=handoff,
                waypoint=waypoint,
                pressure=pressure,
                manual_resume=manual_resume,
                _mutate=_mutate_handoff,
                _operation_token=operation_token,
                _canonical_handoff=canonical_handoff,
                _handoff_id=frozen_handoff_id,
                _idempotency_key=frozen_idempotency_key,
                _safe_waypoint=safe_waypoint,
                _pressure_armed=pressure_armed,
                _invoke_callback=_invoke_handoff_callback,
            )

    @_handoff_operation
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
        _authorize_handoff_operation: Any,
        _mutate_handoff: Any,
        _freeze_handoff: Any,
        _invoke_handoff_callback: Any,
    ) -> Any:
        """Resolve a verified ambiguous handoff effect under the lane write lease."""
        from agent.durable_jobs.session_handoff import (
            HandoffIdentityMismatch,
            SessionHandoffLedger,
        )

        exact_string_inputs = (
            handoff_id,
            effect_name,
            outcome,
            expected_owner_token,
        )
        if any(type(value) is not str for value in exact_string_inputs):
            raise HandoffIdentityMismatch(
                "reconciliation identity and outcome fields must be exact plain strings"
            )
        if receipt is not None and type(receipt) is not str:
            raise HandoffIdentityMismatch(
                "reconciliation receipt must be an exact plain string"
            )
        if (
            type(expected_generation) is not int
            or type(dead_owner_verified) is not bool
        ):
            raise HandoffIdentityMismatch(
                "reconciliation fencing fields must be exact plain scalars"
            )
        if handoff_config.enabled is not True or handoff_config.shadow is not False:
            raise PilotDisabledError(
                "session handoff reconciliation requires enabled=True and shadow=False"
            )
        store = self._acquire_authorized_mutation(job_id)
        authorized_job_id = str(job_id)
        with _authorize_handoff_operation(store.sqlite_path, authorized_job_id) as (
            operation_token,
            authorized_sqlite_path,
        ):
            job = store.get_job(authorized_job_id)
            if job is None:
                raise KeyError(authorized_job_id)
            ledger = SessionHandoffLedger(authorized_sqlite_path)
            state = ledger.get(authorized_job_id, handoff_id)
            if state is None:
                raise KeyError(handoff_id)
            canonical = json.loads(ledger.canonical(authorized_job_id, handoff_id))
            if (
                str(canonical.get("repository", "")).strip()
                != str(job.repository_identity).strip()
                or not job.frozen_baseline_sha
                or canonical.get("exact_sha") != job.frozen_baseline_sha
            ):
                raise HandoffIdentityMismatch(
                    "reconciliation requires current durable job repository and frozen SHA identity"
                )
            with ledger.effect_owner_guard(
                authorized_job_id, handoff_id, effect_name
            ) as guard:
                return _mutate_handoff(
                    operation_token,
                    "_reconcile_effect",
                    job_id=authorized_job_id,
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


# Ticket issuance remains reachable only through the two decorated operations.
_seal_handoff_lane_type(DurableLaneService)
del _seal_handoff_lane_type
del _handoff_lane_constructor
del _handoff_operation
del _build_handoff_operation_gate
