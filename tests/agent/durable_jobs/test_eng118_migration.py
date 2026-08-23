"""ENG-118 frozen migration identity contract."""

from __future__ import annotations

import hashlib
import importlib
import json
import sqlite3
from pathlib import Path

import pytest


def _identity_api():
    module = importlib.import_module("agent.durable_jobs.postgres_identity")
    return (
        getattr(module, "PersistedTargetIdentity", None),
        getattr(module, "TargetIdentityError", ValueError),
        getattr(module, "verify_persisted_target_identity", None),
    )


def test_target_identity_accepts_complete_exact_markers():
    identity_type, _, verify = _identity_api()
    assert identity_type is not None and callable(verify)
    expected = identity_type(
        storage_id="durable_app",
        environment_id="staging",
        storage_domain="hermes.durable_jobs.application",
        schema_version=9,
    )
    assert verify(expected.as_markers(), expected=expected) == expected


@pytest.mark.parametrize(
    "markers",
    [
        {},
        {"storage_id": "durable_app"},
        {
            "storage_id": "foreign",
            "environment_id": "staging",
            "storage_domain": "hermes.durable_jobs.application",
            "schema_version": "9",
        },
        {
            "storage_id": "durable_app",
            "environment_id": "staging",
            "storage_domain": "hermes.durable_jobs.application",
            "schema_version": "10",
        },
    ],
)
def test_target_identity_foreign_future_partial_or_missing_fails_closed(markers):
    identity_type, error_type, verify = _identity_api()
    assert identity_type is not None and callable(verify)
    expected = identity_type(
        storage_id="durable_app",
        environment_id="staging",
        storage_domain="hermes.durable_jobs.application",
        schema_version=9,
    )
    with pytest.raises(error_type):
        verify(markers, expected=expected)


def test_job_and_checkpoint_metadata_must_share_exact_target_identity():
    module = importlib.import_module("agent.durable_jobs.postgres_identity")
    verify_shared = getattr(module, "verify_shared_target_identities", None)
    assert callable(verify_shared)
    expected = module.PersistedTargetIdentity(
        storage_id="durable_app",
        environment_id="staging",
        storage_domain="hermes.durable_jobs",
        schema_version=9,
    )
    app = expected.as_markers()
    checkpoint = expected.as_markers()
    assert verify_shared(app, checkpoint, expected=expected) == expected
    checkpoint["storage_domain"] = "foreign"
    with pytest.raises(module.TargetIdentityError):
        verify_shared(app, checkpoint, expected=expected)


def _legacy_api():
    module = importlib.import_module("agent.durable_jobs")
    return getattr(module, "legacy_migration", None)


def _snapshot(tmp_path: Path):
    path = tmp_path / "legacy.snapshot.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE sessions(id TEXT PRIMARY KEY, parent_session_id TEXT);
        CREATE TABLE messages(id INTEGER PRIMARY KEY, session_id TEXT, body TEXT);
        CREATE TABLE async_delegations(
            delegation_id TEXT PRIMARY KEY, status TEXT, session_id TEXT, payload TEXT
        );
        CREATE TABLE compression_locks(session_id TEXT PRIMARY KEY, acquired_at TEXT);
        INSERT INTO sessions VALUES ('s1', NULL);
        INSERT INTO messages VALUES (1, 's1', 'ok'), (2, 'missing', 'orphan');
        INSERT INTO async_delegations VALUES
            ('done', 'completed', 's1', '{"b":2,"a":1}'),
            ('live', 'running', 's1', '{}'),
            ('mystery', 'unknown', 's1', '{}');
        INSERT INTO compression_locks VALUES ('s1', '2026-01-01T00:00:00Z');
        """
    )
    conn.commit()
    conn.close()
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return path, digest


def test_offline_plan_is_deterministic_and_quarantines_unsafe_rows(tmp_path):
    migration = _legacy_api()
    assert migration is not None
    path, digest = _snapshot(tmp_path)
    snapshot = migration.FrozenSQLiteSnapshot(path=path, file_sha256=digest)
    first = migration.plan_legacy_adoption(snapshot)
    second = migration.plan_legacy_adoption(snapshot)
    assert first.manifest_json() == second.manifest_json()
    assert first.population_sha256 == second.population_sha256
    assert {item.reason for item in first.blockers} >= {
        "unsafe_status:running",
        "unsafe_status:unknown",
        "unresolved_compression_lock",
        "missing_session_reference",
    }
    assert not any(item.target_kind == "durable_job" for item in first.entries)
    parsed = json.loads(first.manifest_json())
    assert parsed["format_version"] == 1


def test_apply_is_idempotent_and_divergent_migration_key_fails_closed(tmp_path):
    migration = _legacy_api()
    assert migration is not None
    path, digest = _snapshot(tmp_path)
    plan = migration.plan_legacy_adoption(
        migration.FrozenSQLiteSnapshot(path=path, file_sha256=digest)
    )
    ledger = tmp_path / "adoption-ledger.sqlite3"
    dispositions = {item.migration_key: "operator_quarantine" for item in plan.blockers}
    first = migration.apply_legacy_adoption(plan, ledger, dispositions=dispositions)
    second = migration.apply_legacy_adoption(plan, ledger, dispositions=dispositions)
    assert first.total_count == second.total_count == len(plan.entries)
    assert first.inserted_count == len(plan.entries)
    assert second.inserted_count == 0
    assert second.duplicate_count == len(plan.entries)
    assert migration.verify_legacy_adoption(plan, ledger).verified is True

    changed = plan.with_replaced_row_sha(plan.entries[0].migration_key, "0" * 64)
    with pytest.raises(migration.LegacyMigrationError, match="divergent"):
        migration.apply_legacy_adoption(changed, ledger, dispositions=dispositions)

    changed_dispositions = dict(dispositions)
    changed_dispositions[plan.blockers[0].migration_key] = "operator_discard"
    with pytest.raises(migration.LegacyMigrationError, match="divergent"):
        migration.apply_legacy_adoption(
            plan, ledger, dispositions=changed_dispositions
        )


def test_snapshot_digest_mismatch_fails_before_inventory(tmp_path):
    migration = _legacy_api()
    assert migration is not None
    path, _ = _snapshot(tmp_path)
    snapshot = migration.FrozenSQLiteSnapshot(path=path, file_sha256="0" * 64)
    with pytest.raises(migration.LegacyMigrationError, match="snapshot"):
        migration.plan_legacy_adoption(snapshot)


def _authority_api():
    package = importlib.import_module("agent.durable_jobs")
    return getattr(package, "writer_authority", None)


def test_writer_authority_explicit_activation_and_handover():
    authority = _authority_api()
    assert authority is not None
    expected = authority.AuthorityTarget("durable_app", "staging")

    # Staged code preserves only the legacy path before explicit enforcement.
    authority.assert_write_authority(
        (), expected=expected, requested_mode="legacy", writer_id="legacy-1",
        minimum_epoch=0, enforced=False,
    )
    with pytest.raises(authority.WriterAuthorityError):
        authority.assert_write_authority(
            (), expected=expected, requested_mode="new", writer_id="new-1",
            minimum_epoch=0, enforced=False,
        )

    legacy = authority.WriterAuthorityBinding(
        storage_id="durable_app", environment_id="staging", authority_epoch=4,
        writer_id="legacy-1", mode="legacy",
    )
    authority.assert_write_authority(
        (legacy,), expected=expected, requested_mode="legacy", writer_id="legacy-1",
        minimum_epoch=4, enforced=True,
    )
    new = authority.WriterAuthorityBinding(
        storage_id="durable_app", environment_id="staging", authority_epoch=5,
        writer_id="new-1", mode="new",
    )
    authority.assert_write_authority(
        (new,), expected=expected, requested_mode="new", writer_id="new-1",
        minimum_epoch=5, enforced=True,
    )
    with pytest.raises(authority.WriterAuthorityError, match="mode"):
        authority.assert_write_authority(
            (new,), expected=expected, requested_mode="legacy", writer_id="legacy-1",
            minimum_epoch=5, enforced=True,
        )


@pytest.mark.parametrize("fault", ["missing", "double", "stale", "target", "writer"])
def test_enforced_writer_authority_faults_fail_closed(fault):
    authority = _authority_api()
    assert authority is not None
    expected = authority.AuthorityTarget("durable_app", "staging")
    good = authority.WriterAuthorityBinding(
        storage_id="durable_app", environment_id="staging", authority_epoch=7,
        writer_id="new-1", mode="new",
    )
    bindings = (good,)
    writer = "new-1"
    epoch = 7
    if fault == "missing":
        bindings = ()
    elif fault == "double":
        bindings = (good, good)
    elif fault == "stale":
        epoch = 8
    elif fault == "target":
        bindings = (authority.WriterAuthorityBinding(
            storage_id="foreign", environment_id="staging", authority_epoch=7,
            writer_id="new-1", mode="new",
        ),)
    elif fault == "writer":
        writer = "new-2"
    with pytest.raises(authority.WriterAuthorityError):
        authority.assert_write_authority(
            bindings, expected=expected, requested_mode="new", writer_id=writer,
            minimum_epoch=epoch, enforced=True,
        )


def test_datastore_authority_check_reloads_binding_for_every_write():
    authority = _authority_api()
    expected = authority.AuthorityTarget("durable_app", "staging")
    rows = [authority.WriterAuthorityBinding(
        storage_id="durable_app", environment_id="staging", authority_epoch=3,
        writer_id="new-1", mode="new",
    )]
    loads = 0

    def load():
        nonlocal loads
        loads += 1
        return tuple(rows)

    check = authority.DatastoreWriterAuthorityCheck(
        load, expected, "new", "new-1", 3
    )
    assert check().authority_epoch == 3
    rows.clear()
    with pytest.raises(authority.WriterAuthorityError, match="exactly one"):
        check()
    assert loads == 2


class _FailIfCalled:
    def __getattr__(self, name):
        raise AssertionError(f"external adapter called: {name}")

    def __call__(self, *args, **kwargs):
        raise AssertionError("external adapter called")


def test_new_lane_attach_and_dispatch_recheck_authoritative_binding(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.lane import DurableLaneService
    from gateway.durable_job_lane import attach_durable_job_lane
    from tests.agent.durable_jobs.package2_support import (
        runtime_ready_transport_kwargs,
    )

    raw = {"durable_jobs": {
        "enabled": True, "dispatch_enabled": True, "backend": "sqlite",
        "sqlite_path": str(tmp_path / "jobs.sqlite"),
        "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
        "cursor_adapter_mode": "injected", "slack_adapter_mode": "injected",
        "cursor_secret_ref": "CURSOR_API_KEY", "slack_secret_ref": "SLACK_BOT_TOKEN",
        "policy_version": "eng29-matrix-v1",
        "identity_binding": {
            "workspace_id": "T1", "repository_identity": "github.com/example/repo"
        },
    }}
    failure = RuntimeError("authority denied")

    def deny():
        raise failure

    with pytest.raises(RuntimeError, match="authority denied"):
        attach_durable_job_lane(
            raw_config=raw,
            **runtime_ready_transport_kwargs(monkeypatch),
            writer_authority_check=deny,
        )
    assert not (tmp_path / "jobs.sqlite").exists()

    lane = DurableLaneService(
        load_durable_jobs_config(raw), writer_authority_check=deny
    )
    with pytest.raises(RuntimeError, match="authority denied"):
        lane.deliver_slack_root(job_id="job-1", slack_port=_FailIfCalled())
    assert not (tmp_path / "jobs.sqlite").exists()


def test_legacy_dispatch_batch_recovery_and_restore_fail_before_side_effects(monkeypatch):
    module = importlib.import_module("tools.async_delegation")
    calls = 0

    def deny():
        nonlocal calls
        calls += 1
        raise RuntimeError("legacy authority denied")

    module.configure_legacy_writer_authority_check(deny)
    monkeypatch.setattr(module, "_connect", _FailIfCalled())
    common = dict(
        context=None, toolsets=None, role="researcher", model=None,
        session_key="session", runner=_FailIfCalled(),
    )
    try:
        with pytest.raises(RuntimeError, match="legacy authority denied"):
            module.dispatch_async_delegation(goal="one", **common)
        with pytest.raises(RuntimeError, match="legacy authority denied"):
            module.dispatch_async_delegation_batch(goals=["one"], **common)
        with pytest.raises(RuntimeError, match="legacy authority denied"):
            module.recover_abandoned_delegations()
        with pytest.raises(RuntimeError, match="legacy authority denied"):
            module.restore_undelivered_completions(_FailIfCalled())
    finally:
        module.configure_legacy_writer_authority_check(None)
    assert calls == 4

def test_session_handoff_rechecks_writer_authority_before_effects(tmp_path):
    import pytest

    from agent.durable_jobs.writer_authority import WriterAuthorityError
    from tests.agent.durable_jobs.test_session_handoff import (
        _Linear,
        _Sessions,
        _Slack,
        _armed,
        _enabled_handoff_config,
        _handoff,
        _lane,
    )
    from agent.durable_jobs.session_handoff import SemanticWaypoint

    lane, job = _lane(tmp_path)
    checks: list[str] = []

    def denied_writer() -> None:
        checks.append("checked")
        raise WriterAuthorityError("writer authority lost")

    lane._writer_authority_check = denied_writer
    linear, slack, sessions = _Linear(), _Slack(), _Sessions()

    with pytest.raises(WriterAuthorityError, match="authority lost"):
        lane.resume_session_handoff(
            job_id=job.job_id,
            parent_session_id="parent-1",
            handoff=_handoff(),
            waypoint=SemanticWaypoint(verified=True),
            pressure=_armed(),
            linear=linear,
            slack=slack,
            sessions=sessions,
            handoff_config=_enabled_handoff_config(),
        )

    assert checks == ["checked"]
    assert not linear.effects
    assert not slack.effects
    assert not sessions.children
    assert not sessions.injections
    assert not sessions.turns
