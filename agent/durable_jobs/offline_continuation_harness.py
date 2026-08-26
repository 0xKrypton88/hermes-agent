"""Default-off disposable scheduler/adapter harness for ENG-122 acceptance."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
import sqlite3
from threading import Lock
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


_ROOT_MARKER = ".hermes-offline-receipts.json"
_INITIALIZE_LOCK = Lock()


def initialize_disposable_receipt_root(root: Path) -> None:
    """Create and attest a new directory reserved for offline receipt evidence."""
    resolved = Path(root).resolve()
    try:
        resolved.mkdir(parents=False, exist_ok=False)
        marker = resolved / _ROOT_MARKER
        marker.write_text(
            json.dumps(
                {"format_version": 1, "live_effects": False, "root": str(resolved)},
                sort_keys=True,
            ),
            encoding="utf-8",
        )
    except Exception:
        if resolved.is_dir() and not any(resolved.iterdir()):
            resolved.rmdir()
        raise


def _attested_root(root: Path) -> Path:
    resolved = Path(root).resolve()
    marker = resolved / _ROOT_MARKER
    expected = {"format_version": 1, "live_effects": False, "root": str(resolved)}
    try:
        observed = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError("receipt root is not an attested disposable root") from exc
    if observed != expected:
        raise ValueError("receipt root attestation does not match")
    return resolved


@dataclass
class DisposableReceiptAdapter:
    """Disposable durable adapter whose bytes are its authoritative receipt."""

    path: Path
    disposable_root: Path
    receipt_factory: Callable[[ContinuationRecord, str], bytes]
    external_ports: FailIfCalledPorts = field(default_factory=FailIfCalledPorts)
    delivery_attempts: int = 0

    def __post_init__(self) -> None:
        self.path = Path(self.path)
        self.disposable_root = Path(self.disposable_root)

    def _connect(self, *, initialize: bool = False) -> sqlite3.Connection:
        root = _attested_root(self.disposable_root)
        path = self.path.resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise ValueError("receipt database is outside disposable root") from exc
        with _INITIALIZE_LOCK:
            if initialize and not path.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                connection = sqlite3.connect(path, timeout=30)
                connection.execute(
                    "CREATE TABLE offline_authoritative_receipts("
                    "idempotency_key TEXT PRIMARY KEY, receipt_bytes BLOB NOT NULL)"
                )
                connection.execute(
                    "CREATE TABLE offline_receipt_target("
                    "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
                    "format_version INTEGER NOT NULL CHECK(format_version=1),"
                    "live_effects INTEGER NOT NULL CHECK(live_effects=0),"
                    "root_sha256 TEXT NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO offline_receipt_target VALUES(1,1,0,?)",
                    (hashlib.sha256(str(root).encode()).hexdigest(),),
                )
                connection.commit()
            else:
                if not path.is_file():
                    raise ValueError("receipt database is not initialized")
                connection = sqlite3.connect(
                    f"{path.as_uri()}?mode=rw", uri=True, timeout=30
                )
        connection.row_factory = sqlite3.Row
        try:
            marker = connection.execute(
                "SELECT format_version,live_effects,root_sha256 FROM offline_receipt_target "
                "WHERE singleton=1"
            ).fetchone()
        except sqlite3.Error as exc:
            connection.close()
            raise ValueError("receipt database is not disposable") from exc
        expected = (1, 0, hashlib.sha256(str(root).encode()).hexdigest())
        if marker is None or tuple(marker) != expected:
            connection.close()
            raise ValueError("receipt database marker does not match root")
        return connection

    @staticmethod
    def idempotency_key(record: ContinuationRecord) -> str:
        return hashlib.sha256(
            f"{record.job_id}\0{record.handoff_id}\0handoff_delivery".encode()
        ).hexdigest()

    def deliver(self, record: ContinuationRecord) -> bytes:
        key = self.idempotency_key(record)
        connection = self._connect(initialize=True)
        try:
            connection.execute("BEGIN IMMEDIATE")
            prior = connection.execute(
                "SELECT receipt_bytes FROM offline_authoritative_receipts "
                "WHERE idempotency_key=?",
                (key,),
            ).fetchone()
            if prior is None:
                self.delivery_attempts += 1
                receipt = self.receipt_factory(record, key)
                if not isinstance(receipt, bytes) or not receipt:
                    raise ValueError(
                        "adapter receipt must be nonempty authoritative bytes"
                    )
                connection.execute(
                    "INSERT INTO offline_authoritative_receipts VALUES(?,?)",
                    (key, receipt),
                )
            durable = connection.execute(
                "SELECT receipt_bytes FROM offline_authoritative_receipts "
                "WHERE idempotency_key=?",
                (key,),
            ).fetchone()
            if durable is None:
                raise RuntimeError("authoritative receipt insert was not durable")
            connection.commit()
            return bytes(durable[0])
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def authoritative_receipt_bytes(self, record: ContinuationRecord) -> bytes:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT receipt_bytes FROM offline_authoritative_receipts "
                "WHERE idempotency_key=?",
                (self.idempotency_key(record),),
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
        if type(enabled) is not bool:
            raise OfflineHarnessDisabled(
                "offline continuation enabled must be a literal bool"
            )
        self.enabled = enabled

    def run_once(
        self, *, owner_token: str, lease_seconds: float
    ) -> ContinuationRecord | None:
        if self.enabled is not True:
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
        raise ValueError(
            f"unsupported offline continuation action: {claim.next_action}"
        )
