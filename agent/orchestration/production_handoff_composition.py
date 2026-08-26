"""Fail-closed, lifecycle-owned composition for production handoff preparation.

Nothing in ordinary Hermes runtime imports or constructs this module.  A product
client must inject request-bound authority, storage, and ports, then explicitly
start and drive one composition.  ``offline`` is the only implemented mode;
the live port is a typed residual contract, not a successful stub.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import Any, Protocol, runtime_checkable

from agent.durable_jobs.session_handoff_continuation import (
    ContinuationRecord,
    ContinuationStore,
)
from agent.orchestration.session_handoff_runtime import (
    SessionHandoffRuntime,
    attach_session_handoff_runtime,
)


class ProductionCompositionDisabled(RuntimeError):
    """A literal gate, lifecycle rule, or request authority was absent."""


class LiveAdapterUnavailable(ProductionCompositionDisabled):
    """Live effects were requested without the separately injected authority."""


@dataclass(frozen=True)
class ProductionHandoffConfig:
    """Strict schema.  Its effective default performs no work."""

    enabled: bool = False
    mode: str = "off"

    def __post_init__(self) -> None:
        if type(self.enabled) is not bool:
            raise ProductionCompositionDisabled("enabled must be a literal bool")
        if self.mode not in {"off", "offline", "live"}:
            raise ProductionCompositionDisabled("mode must be off, offline, or live")
        if self.enabled is False and self.mode != "off":
            raise ProductionCompositionDisabled("disabled configuration requires mode='off'")
        if self.enabled is True and self.mode == "off":
            raise ProductionCompositionDisabled("enabled configuration requires an explicit mode")


@dataclass(frozen=True)
class ProductionRequestAuthority:
    """One non-transferable client request allowed to compose offline wiring."""

    request_id: str
    session_id: str
    approved: bool
    terminal: bool = False

    def __post_init__(self) -> None:
        if not self.request_id or not self.session_id:
            raise ProductionCompositionDisabled("request authority identity is required")
        if type(self.approved) is not bool or type(self.terminal) is not bool:
            raise ProductionCompositionDisabled("authority flags must be literal bools")

    def require_active(self) -> None:
        if self.approved is not True or self.terminal is True:
            raise ProductionCompositionDisabled("request authority is not active")


@dataclass(frozen=True)
class LiveEffectAuthority:
    """Separate, request-bound capability required before a live port is reachable."""

    request_id: str
    session_id: str
    activation_id: str
    approved: bool

    def __post_init__(self) -> None:
        if not self.request_id or not self.session_id or not self.activation_id:
            raise LiveAdapterUnavailable("live activation identity is required")
        if type(self.approved) is not bool:
            raise LiveAdapterUnavailable("live approval must be a literal bool")


@runtime_checkable
class AuthoritativeReceiptPort(Protocol):
    """Adapter contract: stable dedupe, fenced write, and authoritative readback."""

    def deliver(
        self, record: ContinuationRecord, *, idempotency_key: str, fence: int
    ) -> bytes: ...

    def readback(
        self, record: ContinuationRecord, *, idempotency_key: str
    ) -> bytes: ...


@runtime_checkable
class LiveAuthoritativeReceiptPort(AuthoritativeReceiptPort, Protocol):
    """Residual live contract; implementations must bind provider receipt bytes."""


class ContinuationScheduler:
    """One explicitly driven scheduler, owned by one started composition."""

    def __init__(
        self,
        store: ContinuationStore,
        port: AuthoritativeReceiptPort,
        authority: ProductionRequestAuthority,
    ) -> None:
        self._store = store
        self._port = port
        self._authority = authority

    @staticmethod
    def idempotency_key(record: ContinuationRecord) -> str:
        return hashlib.sha256(
            f"{record.job_id}\0{record.handoff_id}\0handoff_delivery".encode()
        ).hexdigest()

    def run_once(self, *, owner_token: str, lease_seconds: float) -> ContinuationRecord | None:
        claim = self._store.claim_due_scoped(
            request_id=self._authority.request_id,
            session_id=self._authority.session_id,
            owner_token=owner_token,
            lease_seconds=lease_seconds,
        )
        if claim is None:
            return None
        key = self.idempotency_key(claim)
        if claim.next_action == "deliver_handoff":
            receipt = self._port.deliver(
                claim, idempotency_key=key, fence=claim.owner_generation
            )
            return self._store.record_verified_effect(
                claim,
                effect_name="handoff_delivery",
                receipt_sha256=self._digest(receipt),
            )
        if claim.next_action == "verify_handoff_delivery":
            receipt = self._port.readback(claim, idempotency_key=key)
            return self._store.complete_delivery(
                claim, observed_receipt_sha256=self._digest(receipt)
            )
        raise ValueError(f"unsupported continuation action: {claim.next_action}")

    @staticmethod
    def _digest(receipt: bytes) -> str:
        if not isinstance(receipt, bytes) or not receipt:
            raise ValueError("receipt port must return nonempty authoritative bytes")
        return hashlib.sha256(receipt).hexdigest()


class ProductionHandoffComposition:
    """Explicit composition root; it never discovers config, ports, or authority."""

    def __init__(
        self,
        config: ProductionHandoffConfig = ProductionHandoffConfig(),
        *,
        authority: ProductionRequestAuthority | None = None,
        store: ContinuationStore | None = None,
        offline_port: AuthoritativeReceiptPort | None = None,
        live_port: LiveAuthoritativeReceiptPort | None = None,
        live_authority: LiveEffectAuthority | None = None,
    ) -> None:
        self._config = config
        self._authority = authority
        self._store = store
        self._offline_port = offline_port
        self._live_port = live_port
        self._live_authority = live_authority
        self._scheduler: ContinuationScheduler | None = None
        self._stopped = False

    @property
    def started(self) -> bool:
        return self._scheduler is not None

    def start(self) -> None:
        if self._stopped:
            raise ProductionCompositionDisabled("a stopped composition cannot restart")
        if self._scheduler is not None:
            raise ProductionCompositionDisabled("composition already owns a scheduler")
        if self._config.enabled is not True:
            raise ProductionCompositionDisabled("production handoff composition is default-off")
        if self._authority is None:
            raise ProductionCompositionDisabled("request-bound production authority is required")
        self._authority.require_active()
        if self._store is None:
            raise ProductionCompositionDisabled("an injected continuation store is required")
        port: AuthoritativeReceiptPort | None
        if self._config.mode == "offline":
            port = self._offline_port
        else:
            port = self._authorized_live_port()
        if port is None or not isinstance(port, AuthoritativeReceiptPort):
            raise ProductionCompositionDisabled("an authoritative receipt port is required")
        self._scheduler = ContinuationScheduler(self._store, port, self._authority)

    def _authorized_live_port(self) -> LiveAuthoritativeReceiptPort:
        live = self._live_authority
        request = self._authority
        if live is None or request is None or self._live_port is None:
            raise LiveAdapterUnavailable("live port and separate activation authority are required")
        if live.approved is not True or (
            live.request_id != request.request_id or live.session_id != request.session_id
        ):
            raise LiveAdapterUnavailable("live activation is not bound to this request")
        return self._live_port

    def run_once(self, *, owner_token: str, lease_seconds: float) -> ContinuationRecord | None:
        self._require_running()
        assert self._scheduler is not None
        return self._scheduler.run_once(owner_token=owner_token, lease_seconds=lease_seconds)

    def attach_runtime(self, agent: Any, runtime: SessionHandoffRuntime) -> None:
        self._require_running()
        assert self._authority is not None
        if runtime.request.parent_session_id != self._authority.session_id:
            raise ProductionCompositionDisabled("runtime is not bound to the authorized session")
        attach_session_handoff_runtime(agent, runtime, enabled=True)

    def shutdown(self) -> None:
        self._scheduler = None
        self._stopped = True

    def _require_running(self) -> None:
        if self._scheduler is None or self._authority is None:
            raise ProductionCompositionDisabled("composition is not started")
        self._authority.require_active()
