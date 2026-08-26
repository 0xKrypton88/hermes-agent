from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

import pytest

from agent.durable_jobs.clock import FrozenClock
from agent.durable_jobs.session_handoff_continuation import (
    ContinuationStore,
    DeliveryVerificationFailed,
)
from agent.orchestration.production_handoff_composition import (
    AuthoritativeReceiptPort,
    LiveAdapterUnavailable,
    LiveEffectAuthority,
    ProductionCompositionDisabled,
    ProductionHandoffComposition,
    ProductionHandoffConfig,
    ProductionRequestAuthority,
)


class _FailIfCalledPort:
    def deliver(self, *args, **kwargs):
        raise AssertionError("port reached")

    def readback(self, *args, **kwargs):
        raise AssertionError("port reached")


class _OfflineReceiptPort:
    def __init__(self):
        self.receipts = {}
        self.deliveries = 0

    def deliver(self, record, *, idempotency_key, fence):
        prior = self.receipts.get(idempotency_key)
        if prior is None:
            self.deliveries += 1
            prior = f"offline:{idempotency_key}:{fence}".encode()
            self.receipts[idempotency_key] = prior
        return prior

    def readback(self, record, *, idempotency_key):
        return self.receipts[idempotency_key]


def _authority(*, terminal=False):
    return ProductionRequestAuthority("request-1", "session-1", True, terminal)


def _enqueue_authorized(store, *, job_id="job", request_id="request-1", session_id="session-1"):
    return store.enqueue(
        job_id=job_id, handoff_id="handoff", checkpoint_stage="DELIVER",
        next_action="deliver_handoff", request_id=request_id, session_id=session_id,
    )


def test_representative_client_default_off_is_zero_touch_and_schema_is_strict(tmp_path):
    store_path = tmp_path / "absent.sqlite3"
    port = _FailIfCalledPort()
    composition = ProductionHandoffComposition(offline_port=port)
    with pytest.raises(ProductionCompositionDisabled, match="default-off"):
        composition.start()
    assert not store_path.exists()
    for ambiguous in (1, "false", None, [], object()):
        with pytest.raises(ProductionCompositionDisabled, match="literal bool"):
            ProductionHandoffConfig(enabled=ambiguous)


def test_offline_client_restart_reclaim_manual_resume_and_lifecycle(tmp_path):
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    path = tmp_path / "continuations.sqlite3"
    store = ContinuationStore(path, now_fn=clock)
    _enqueue_authorized(store)
    port = _OfflineReceiptPort()
    first = ProductionHandoffComposition(
        ProductionHandoffConfig(True, "offline"), authority=_authority(),
        store=store, offline_port=port,
    )
    first.start()
    with pytest.raises(ProductionCompositionDisabled, match="already owns"):
        first.start()
    delivered = first.run_once(owner_token="process-a", lease_seconds=10)
    assert delivered is not None and delivered.next_action == "verify_handoff_delivery"
    first.shutdown()
    with pytest.raises(ProductionCompositionDisabled, match="cannot restart"):
        first.start()

    # A new lifecycle owner reopens the durable store and reclaims the expired lease.
    clock.advance(11)
    restarted = ProductionHandoffComposition(
        ProductionHandoffConfig(True, "offline"), authority=_authority(),
        store=ContinuationStore(path, now_fn=clock), offline_port=port,
    )
    restarted.start()
    completed = restarted.run_once(owner_token="process-b", lease_seconds=10)
    assert completed is not None and completed.wake_state == "COMPLETE"
    assert completed.owner_generation == delivered.owner_generation + 1
    assert port.deliveries == 1

    # Exercise the real explicit manual-resume API without inventing an effect.
    blocked_store = ContinuationStore(tmp_path / "blocked.sqlite3", now_fn=clock)
    _enqueue_authorized(blocked_store, job_id="blocked")
    blocked_port = _OfflineReceiptPort()
    blocked = ProductionHandoffComposition(
        ProductionHandoffConfig(True, "offline"), authority=_authority(),
        store=blocked_store, offline_port=blocked_port,
    )
    blocked.start()
    persisted = blocked.run_once(owner_token="owner", lease_seconds=10)
    assert persisted is not None
    blocked_port.receipts[next(iter(blocked_port.receipts))] = b"different"
    clock.advance(11)
    with pytest.raises(DeliveryVerificationFailed, match="manual resume required"):
        blocked.run_once(owner_token="verifier", lease_seconds=10)
    with sqlite3.connect(blocked_store.path) as connection:
        digest = connection.execute(
            "SELECT receipt_sha256 FROM session_handoff_continuation_effects"
        ).fetchone()[0]
    resumed = blocked_store.resume_after_manual_verification(
        job_id="blocked", handoff_id="handoff", operator_reason="receipt verified offline",
        confirmed_receipt_sha256=digest,
    )
    assert resumed.wake_state == "DUE" and resumed.manual_resume_operator_reason


def test_terminal_approval_has_zero_effects_and_live_requires_separate_authority(tmp_path):
    class ObservableStore:
        calls = 0

        def claim_due_scoped(self, **kwargs):
            self.calls += 1
            raise AssertionError("store reached")

    observable_store = ObservableStore()
    fail = _FailIfCalledPort()
    terminal = ProductionHandoffComposition(
        ProductionHandoffConfig(True, "offline"), authority=_authority(terminal=True),
        store=observable_store, offline_port=fail,
    )
    with pytest.raises(ProductionCompositionDisabled, match="not active"):
        terminal.start()
    assert observable_store.calls == 0

    store = ContinuationStore(tmp_path / "live.sqlite3")
    without_live_go = ProductionHandoffComposition(
        ProductionHandoffConfig(True, "live"), authority=_authority(),
        store=store, live_port=fail,
    )
    with pytest.raises(LiveAdapterUnavailable, match="separate activation"):
        without_live_go.start()
    mismatched = ProductionHandoffComposition(
        ProductionHandoffConfig(True, "live"), authority=_authority(), store=store,
        live_port=fail,
        live_authority=LiveEffectAuthority("other", "session-1", "go-1", True),
    )
    with pytest.raises(LiveAdapterUnavailable, match="not bound"):
        mismatched.start()


def test_receipt_port_contract_is_runtime_checkable():
    assert isinstance(_OfflineReceiptPort(), AuthoritativeReceiptPort)


@pytest.mark.parametrize("mode", ["offline", "live"])
def test_production_claims_only_exact_authority_scope(mode, tmp_path):
    store = ContinuationStore(tmp_path / f"{mode}.sqlite3")
    _enqueue_authorized(store, job_id="foreign", request_id="request-2")
    store.enqueue(
        job_id="legacy", handoff_id="handoff", checkpoint_stage="DELIVER",
        next_action="deliver_handoff",
    )
    port = _FailIfCalledPort()
    kwargs = {"offline_port": port}
    if mode == "live":
        kwargs = {
            "live_port": port,
            "live_authority": LiveEffectAuthority(
                "request-1", "session-1", "activation", True
            ),
        }
    composition = ProductionHandoffComposition(
        ProductionHandoffConfig(True, mode), authority=_authority(), store=store, **kwargs
    )
    composition.start()
    assert composition.run_once(owner_token="owner", lease_seconds=10) is None
    assert store.get("foreign", "handoff").owner_token is None
    assert store.get("legacy", "handoff").owner_token is None


def test_production_preparation_receipt_is_explicitly_offline():
    receipt = json.loads(
        Path("agent/orchestration/eng122_production_preparation_receipt.json").read_text(
            encoding="utf-8"
        )
    )
    assert receipt["schema"] == "hermes.eng122-production-preparation"
    assert receipt["activation"] is False and receipt["live_effects"] is False
    assert all(receipt["proofs"].values())
    assert len(receipt["remaining_live_go"]) == 4
