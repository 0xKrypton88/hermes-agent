from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import sqlite3

from agent.durable_jobs.clock import FrozenClock
from agent.durable_jobs.legacy_migration import (
    FrozenSQLiteSnapshot,
    apply_legacy_adoption,
    plan_legacy_adoption,
    verify_legacy_adoption,
)
from agent.durable_jobs.session_handoff_continuation import ContinuationStore
from agent.durable_jobs.writer_authority import (
    AuthorityTarget,
    WRITER_AUTHORITY_DDL,
    WriterAuthorityBinding,
    activate_writer_authority,
    load_writer_authority,
)


RECEIPT = Path("agent/durable_jobs/eng118_offline_acceptance_receipt.json")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _logical_sha(path: Path) -> str:
    connection = sqlite3.connect(path)
    try:
        statements = tuple(connection.iterdump())
    finally:
        connection.close()
    return hashlib.sha256("\n".join(statements).encode()).hexdigest()


def test_disposable_offline_acceptance_and_rollback(tmp_path):
    receipt = json.loads(RECEIPT.read_text(encoding="utf-8"))
    assert receipt["schema"] == "hermes.eng118-offline-acceptance"
    assert receipt["live_effects"] is False

    source = tmp_path / "legacy.sqlite3"
    connection = sqlite3.connect(source)
    connection.executescript(
        """
        CREATE TABLE sessions(id TEXT PRIMARY KEY, parent_session_id TEXT);
        CREATE TABLE messages(id INTEGER PRIMARY KEY, session_id TEXT, body TEXT);
        INSERT INTO sessions VALUES ('session-a', NULL);
        INSERT INTO messages VALUES (1, 'session-a', 'offline-safe');
        """
    )
    connection.commit()
    connection.close()
    snapshot = FrozenSQLiteSnapshot(path=source, file_sha256=_sha(source))
    before = plan_legacy_adoption(snapshot)
    repeated = plan_legacy_adoption(snapshot)
    assert before.manifest_json() == repeated.manifest_json()
    assert before.population_sha256 == repeated.population_sha256

    application = tmp_path / "application.sqlite3"
    connection = sqlite3.connect(application)
    connection.execute(WRITER_AUTHORITY_DDL)
    connection.execute(
        "INSERT INTO durable_writer_authority VALUES (?,?,?,?,?)",
        ("disposable", "offline", 1, "legacy-writer", "legacy"),
    )
    connection.commit()
    connection.close()
    application_backup = tmp_path / "application.before.sqlite3"
    shutil.copy2(application, application_backup)
    application_before = _logical_sha(application)

    checkpoint = tmp_path / "checkpoint.sqlite3"
    clock = FrozenClock()
    ContinuationStore(checkpoint, now_fn=clock)
    checkpoint_backup = tmp_path / "checkpoint.before.sqlite3"
    shutil.copy2(checkpoint, checkpoint_backup)
    checkpoint_before = _logical_sha(checkpoint)

    first = apply_legacy_adoption(
        before,
        application,
        dispositions={},
        expected_source_snapshot=snapshot,
    )
    second = apply_legacy_adoption(
        before,
        application,
        dispositions={},
        expected_source_snapshot=snapshot,
    )
    readback = verify_legacy_adoption(
        before,
        application,
        dispositions={},
        expected_source_snapshot=snapshot,
    )
    assert first.inserted_count == first.total_count == 2
    assert second.inserted_count == 0 and second.duplicate_count == 2
    assert readback.verified is True and readback.actual_count == 2

    connection = sqlite3.connect(application)
    activate_writer_authority(
        connection,
        WriterAuthorityBinding("disposable", "offline", 2, "new-writer", "new"),
    )
    connection.commit()
    connection.close()
    connection = sqlite3.connect(application)
    activated_authority = load_writer_authority(
        connection, AuthorityTarget("disposable", "offline")
    )
    connection.close()
    assert activated_authority == (
        WriterAuthorityBinding("disposable", "offline", 2, "new-writer", "new"),
    )

    store = ContinuationStore(checkpoint, now_fn=clock)
    store.enqueue(
        job_id="job-a",
        handoff_id="handoff-a",
        checkpoint_stage="DELIVER",
        next_action="deliver_handoff",
    )
    claimed = store.claim_due(owner_token="offline-process", lease_seconds=10)
    assert claimed is not None and claimed.next_action == "deliver_handoff"
    receipt_digest = hashlib.sha256(b"safe offline receipt").hexdigest()
    recorded = store.record_verified_effect(
        claimed,
        effect_name="handoff_delivery",
        receipt_sha256=receipt_digest,
    )
    assert recorded.checkpoint_stage == "VERIFY_HANDOFF_DELIVERY"
    assert recorded.next_action == "verify_handoff_delivery"
    persisted = ContinuationStore(checkpoint, now_fn=clock).get("job-a", "handoff-a")
    assert persisted.checkpoint_stage == "VERIFY_HANDOFF_DELIVERY"
    assert persisted.next_action == "verify_handoff_delivery"
    assert persisted.verification_state == "RECEIPT_DIGEST_PERSISTED"
    clock.advance(11)
    reopened = ContinuationStore(checkpoint, now_fn=clock)
    reclaimed = reopened.claim_due(owner_token="reopened-process", lease_seconds=10)
    assert reclaimed is not None
    assert reclaimed.owner_token == "reopened-process"
    assert reclaimed.owner_generation == claimed.owner_generation + 1
    assert reclaimed.next_action == "verify_handoff_delivery"
    completed = reopened.complete_delivery(
        reclaimed, observed_receipt_sha256=receipt_digest
    )
    assert completed.checkpoint_stage == "COMPLETE"
    assert completed.wake_state == "COMPLETE"
    assert completed.next_action == "complete"

    assert _logical_sha(application) != application_before
    assert _logical_sha(checkpoint) != checkpoint_before
    completed_after_reopen = ContinuationStore(checkpoint, now_fn=clock).get(
        "job-a", "handoff-a"
    )
    assert completed_after_reopen.checkpoint_stage == "COMPLETE"
    assert completed_after_reopen.wake_state == "COMPLETE"
    assert completed_after_reopen.next_action == "complete"
    assert completed_after_reopen.verification_state == "VERIFIED"

    shutil.copy2(application_backup, application)
    shutil.copy2(checkpoint_backup, checkpoint)
    assert _logical_sha(application) == application_before
    assert _logical_sha(checkpoint) == checkpoint_before
    connection = sqlite3.connect(application)
    authority = load_writer_authority(
        connection, AuthorityTarget("disposable", "offline")
    )
    connection.close()
    assert authority == (
        WriterAuthorityBinding("disposable", "offline", 1, "legacy-writer", "legacy"),
    )

    assert receipt["proofs"] == {
        "checkpoint_reopen_reclaims_expired_lease_and_completes": True,
        "defined_deduped_manifest_and_readback": True,
        "rollback_restores_application_checkpoint_and_authority": True,
    }
    assert receipt["unverified_gap"] == (
        "No current offline API materializes adopted rows into production application "
        "tables; acceptance proves the immutable adoption-ledger population only."
    )
