"""Default-off disposable scheduler/adapter harness for ENG-122 acceptance."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
from pathlib import Path
import sqlite3
from typing import Callable

from agent.durable_jobs.session_handoff_continuation import (
    ContinuationRecord,
    ContinuationStore,
)


class OfflineHarnessDisabled(RuntimeError):
    """The explicit default-off harness enablement was absent."""


class ExternalPortCalled(AssertionError):
    """Acceptance attempted a forbidden gateway/provider/network operation."""


class FailIfCalledPorts:
    """Sentinel external ports; any access proves the offline boundary failed."""

    def __getattr__(self, name: str) -> Callable[..., None]:
        def fail(*args: object, **kwargs: object) -> None:
            raise ExternalPortCalled(f"external port {name!r} was called")

        return fail


@dataclass
class DisposableReceiptAdapter:
    """Disposable durable adapter whose bytes are its authoritative receipt."""

    path: Path
    receipt_factory: Callable[[ContinuationRecord, str], bytes]
    external_ports: FailIfCalledPorts = field(default_factory=FailIfCalledPorts)
    delivery_attempts: int = 0

    def __post_init__(self) -> None:
        self.path = Path(self.path)
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS offline_authoritative_receipts("
                "idempotency_key TEXT PRIMARY KEY, receipt_bytes BLOB NOT NULL)"
            )

    @staticmethod
    def idempotency_key(record: ContinuationRecord) -> str:
        return hashlib.sha256(
            f"{record.job_id}\0{record.handoff_id}\0handoff_delivery".encode()
        ).hexdigest()

    def deliver(self, record: ContinuationRecord) -> bytes:
        key = self.idempotency_key(record)
        self.delivery_attempts += 1
        receipt = self.receipt_factory(record, key)
        if not isinstance(receipt, bytes) or not receipt:
            raise ValueError("adapter receipt must be nonempty authoritative bytes")
        with sqlite3.connect(self.path) as connection:
            prior = connection.execute(
                "SELECT receipt_bytes FROM offline_authoritative_receipts "
                "WHERE idempotency_key=?", (key,),
            ).fetchone()
            if prior is not None and bytes(prior[0]) != receipt:
                raise ValueError("adapter receipt bytes changed for idempotency key")
            connection.execute(
                "INSERT INTO offline_authoritative_receipts VALUES(?,?) "
                "ON CONFLICT(idempotency_key) DO NOTHING", (key, receipt),
            )
        return receipt if prior is None else bytes(prior[0])

    def authoritative_receipt_bytes(self, record: ContinuationRecord) -> bytes:
        with sqlite3.connect(self.path) as connection:
            row = connection.execute(
                "SELECT receipt_bytes FROM offline_authoritative_receipts "
                "WHERE idempotency_key=?", (self.idempotency_key(record),),
            ).fetchone()
        if row is None:
            raise ValueError("adapter has no authoritative receipt bytes")
        return bytes(row[0])


ENG110_CRITERIA_EVIDENCE = {
    "durable_checkpoint": "ContinuationStore persists checkpoint_stage and next_action",
    "restart_reclaim": "expired leases are reclaimed with a fenced owner_generation",
    "effect_dedupe": "adapter idempotency key plus immutable effect digest prevents divergence",
    "authoritative_receipt_binding": "harness hashes bytes read from the adapter itself",
    "manual_resume": "digest mismatch blocks automatic wake until explicit verified resume",
    "external_effect_isolation": "FailIfCalledPorts raises on every external port call",
    "default_off": "run_once raises before claim or adapter access unless enabled=True",
}


class OfflineContinuationScheduler:
    """Drive at most one disposable continuation; never schedules itself."""

    def __init__(
        self,
        store: ContinuationStore,
        adapter: DisposableReceiptAdapter,
        *,
        enabled: bool = False,
    ) -> None:
        self.store = store
        self.adapter = adapter
        self.enabled = enabled

    def run_once(self, *, owner_token: str, lease_seconds: float) -> ContinuationRecord | None:
        if not self.enabled:
            raise OfflineHarnessDisabled("offline continuation harness is default-off")
        claim = self.store.claim_due(
            owner_token=owner_token, lease_seconds=lease_seconds
        )
        if claim is None:
            return None
        if claim.next_action == "deliver_handoff":
            receipt = self.adapter.deliver(claim)
            return self.store.record_verified_effect(
                claim,
                effect_name="handoff_delivery",
                receipt_sha256=hashlib.sha256(receipt).hexdigest(),
            )
        if claim.next_action == "verify_handoff_delivery":
            receipt = self.adapter.authoritative_receipt_bytes(claim)
            return self.store.complete_delivery(
                claim, observed_receipt_sha256=hashlib.sha256(receipt).hexdigest()
            )
        raise ValueError(f"unsupported offline continuation action: {claim.next_action}")
