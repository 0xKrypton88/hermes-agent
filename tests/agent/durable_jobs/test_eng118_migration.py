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


def test_plan_skips_virtual_shadow_tables_and_keys_rowid_tables(tmp_path):
    migration = _legacy_api()
    assert migration is not None
    path = tmp_path / "legacy-with-fts.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE schema_version(version INTEGER NOT NULL);
        INSERT INTO schema_version VALUES (9);
        CREATE VIRTUAL TABLE messages_fts USING fts5(body);
        INSERT INTO messages_fts VALUES ('indexed only');
        """
    )
    conn.commit()
    conn.close()
    digest = hashlib.sha256(path.read_bytes()).hexdigest()

    plan = migration.plan_legacy_adoption(
        migration.FrozenSQLiteSnapshot(path=path, file_sha256=digest)
    )

    assert plan.table_counts == {"schema_version": 1}
    assert {entry.source_table for entry in plan.entries} == {"schema_version"}
    assert json.loads(plan.entries[0].source_pk_json) == {"$rowid": 1}


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
    assert migration.verify_legacy_adoption(plan, ledger, dispositions=dispositions).verified is True

    changed = plan.with_replaced_row_sha(plan.entries[0].migration_key, "0" * 64)
    with pytest.raises(migration.LegacyMigrationError, match="divergent"):
        migration.apply_legacy_adoption(changed, ledger, dispositions=dispositions)

    changed_dispositions = dict(dispositions)
    changed_dispositions[plan.blockers[0].migration_key] = "operator_discard"
    with pytest.raises(migration.LegacyMigrationError, match="divergent"):
        migration.apply_legacy_adoption(
            plan, ledger, dispositions=changed_dispositions
        )


def test_verify_adoption_is_read_only_and_checks_dispositions(tmp_path):
    migration = _legacy_api()
    assert migration is not None
    path, digest = _snapshot(tmp_path)
    plan = migration.plan_legacy_adoption(
        migration.FrozenSQLiteSnapshot(path=path, file_sha256=digest)
    )
    missing = tmp_path / "missing-ledger.sqlite3"
    result = migration.verify_legacy_adoption(plan, missing, dispositions={})
    assert result.verified is False
    assert not missing.exists()

    ledger = tmp_path / "adoption-ledger.sqlite3"
    dispositions = {item.migration_key: "operator_quarantine" for item in plan.blockers}
    migration.apply_legacy_adoption(plan, ledger, dispositions=dispositions)
    wrong = dict(dispositions)
    wrong[plan.blockers[0].migration_key] = "operator_discard"
    assert migration.verify_legacy_adoption(
        plan, ledger, dispositions=wrong
    ).verified is False
    assert migration.verify_legacy_adoption(
        plan, ledger, dispositions={}
    ).verified is False


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


def test_writer_authority_activation_rejects_non_monotonic_conflict_atomically():
    authority = _authority_api()
    assert authority is not None
    conn = sqlite3.connect(":memory:")
    conn.execute(authority.WRITER_AUTHORITY_DDL)
    high = authority.WriterAuthorityBinding(
        storage_id="durable_app", environment_id="staging", authority_epoch=9,
        writer_id="new-9", mode="new",
    )
    low = authority.WriterAuthorityBinding(
        storage_id="durable_app", environment_id="staging", authority_epoch=8,
        writer_id="legacy-8", mode="legacy",
    )
    authority.activate_writer_authority(conn, high)
    with pytest.raises(authority.WriterAuthorityError, match="monotonically"):
        authority.activate_writer_authority(conn, low)
    assert conn.execute(
        "SELECT authority_epoch, writer_id, mode FROM durable_writer_authority"
    ).fetchone() == (9, "new-9", "new")
    conn.close()


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

    transport_kwargs = runtime_ready_transport_kwargs(monkeypatch)
    transport_kwargs["writer_authority_check"] = deny
    with pytest.raises(RuntimeError, match="authority denied"):
        attach_durable_job_lane(raw_config=raw, **transport_kwargs)
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


def test_legacy_completion_persistence_rechecks_writer_authority(monkeypatch):
    module = importlib.import_module("tools.async_delegation")
    calls: list[str] = []

    def deny():
        calls.append("checked")
        raise RuntimeError("legacy authority lost")

    module.configure_legacy_writer_authority_check(deny)
    monkeypatch.setattr(module, "_connect", _FailIfCalled())
    try:
        with pytest.raises(RuntimeError, match="authority lost"):
            module._persist_completion(
                {"delegation_id": "d-1", "status": "completed"},
                {"content": "done"},
            )
    finally:
        module.configure_legacy_writer_authority_check(None)
    assert calls == ["checked"]


def test_session_handoff_rechecks_writer_authority_at_effect_boundary(tmp_path):
    from agent.durable_jobs.session_handoff import SemanticWaypoint
    from agent.durable_jobs.writer_authority import WriterAuthorityError
    from tests.agent.durable_jobs.test_session_handoff import (
        _Linear, _Sessions, _Slack, _armed, _enabled_handoff_config, _handoff, _lane,
    )

    lane, job = _lane(tmp_path)
    checks = 0

    class LosingAuthority:
        def __call__(self):
            nonlocal checks
            checks += 1
            if checks == 5:
                raise WriterAuthorityError("writer authority lost before effect")

        def effect_lease(self, _effect_key):
            from contextlib import nullcontext
            return nullcontext()

    lane._writer_authority_check = LosingAuthority()
    linear, slack, sessions = _Linear(), _Slack(), _Sessions()
    with pytest.raises(WriterAuthorityError, match="before effect"):
        lane.resume_session_handoff(
            job_id=job.job_id, parent_session_id="parent-1", handoff=_handoff(),
            waypoint=SemanticWaypoint(verified=True), pressure=_armed(),
            linear=linear, slack=slack, sessions=sessions,
            handoff_config=_enabled_handoff_config(),
        )
    assert checks == 5
    assert not linear.effects
    assert not slack.effects
    assert not sessions.children
    assert not sessions.injections
    assert not sessions.turns

def test_restore_rejects_completion_without_exact_writer_metadata(monkeypatch):
    from contextlib import contextmanager
    from types import SimpleNamespace

    import tools.async_delegation as module

    binding = SimpleNamespace(writer_id="target", authority_epoch=11)
    module.configure_legacy_writer_authority_check(lambda: binding)
    monkeypatch.setattr(module, "recover_abandoned_delegations", lambda: 0)

    class Rows:
        def fetchall(self):
            return [("deleg-1", json.dumps({"type": "async_delegation"}))]

    class Connection:
        def execute(self, *_args, **_kwargs):
            return Rows()

    @contextmanager
    def transaction():
        yield Connection()

    class Queue:
        def put(self, _event):
            pytest.fail("unfenced completion must not be restored")

    monkeypatch.setattr(module, "_transaction", transaction)
    try:
        with pytest.raises(RuntimeError, match="missing writer authority metadata"):
            module.restore_undelivered_completions(Queue())
    finally:
        module.configure_legacy_writer_authority_check(None)


def test_restore_accepts_completion_from_current_writer_epoch(monkeypatch):
    from contextlib import contextmanager
    from types import SimpleNamespace

    import tools.async_delegation as module

    binding = SimpleNamespace(writer_id="target", authority_epoch=11)
    payload = {
        "type": "async_delegation",
        "delegation_id": "deleg-1",
        "writer_id": "target",
        "writer_epoch": 11,
    }
    module.configure_legacy_writer_authority_check(lambda: binding)
    monkeypatch.setattr(module, "recover_abandoned_delegations", lambda: 0)

    class Rows:
        def fetchall(self):
            return [("deleg-1", json.dumps(payload))]

    class Connection:
        def execute(self, *_args, **_kwargs):
            return Rows()

    @contextmanager
    def transaction():
        yield Connection()

    queued = []
    monkeypatch.setattr(module, "_transaction", transaction)
    try:
        assert module.restore_undelivered_completions(SimpleNamespace(put=queued.append)) == 1
    finally:
        module.configure_legacy_writer_authority_check(None)
    assert queued[0]["restored"] is True

def test_persisted_effect_lease_fences_authority_handover(tmp_path):
    import sqlite3
    from agent.durable_jobs.writer_authority import (
        AuthorityTarget, DatastoreWriterAuthorityCheck, WRITER_AUTHORITY_DDL,
        WriterAuthorityBinding, WriterAuthorityError, activate_writer_authority,
    )

    path = tmp_path / "authority.db"
    setup = sqlite3.connect(path)
    setup.execute(WRITER_AUTHORITY_DDL)
    current = WriterAuthorityBinding("storage", "env", 1, "writer-a", "new")
    activate_writer_authority(setup, current)
    setup.commit()
    setup.close()

    provider = lambda: sqlite3.connect(path)
    check = DatastoreWriterAuthorityCheck.from_connection_provider(
        provider, expected=AuthorityTarget("storage", "env"), requested_mode="new",
        writer_id="writer-a", minimum_epoch=1,
    )
    replacement = WriterAuthorityBinding("storage", "env", 2, "writer-b", "new")
    with check.effect_lease("linear:job-1"):
        connection = provider()
        with pytest.raises(WriterAuthorityError, match="live external-effect lease"):
            activate_writer_authority(connection, replacement)
        connection.close()

    connection = provider()
    activate_writer_authority(connection, replacement)
    connection.commit()
    connection.close()

def test_shadow_excludes_unclassified_secret_tables_and_rejects_secret_columns(tmp_path):
    import sqlite3
    from agent.durable_jobs import legacy_migration as migration

    unknown = tmp_path / "unknown.sqlite"
    with sqlite3.connect(unknown) as conn:
        conn.execute("CREATE TABLE oauth_tokens (id INTEGER PRIMARY KEY, refresh_token TEXT)")
        conn.execute("INSERT INTO oauth_tokens VALUES (1, 'do-not-copy')")
    snapshot = migration.FrozenSQLiteSnapshot(
        path=unknown, file_sha256=hashlib.sha256(unknown.read_bytes()).hexdigest()
    )
    plan = migration.plan_legacy_adoption(snapshot)
    assert plan.entries == ()
    assert "do-not-copy" not in repr(plan)

    classified = tmp_path / "classified.sqlite"
    with sqlite3.connect(classified) as conn:
        conn.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, api_token TEXT)")
        conn.execute("INSERT INTO messages VALUES (1, 'do-not-copy')")
    snapshot = migration.FrozenSQLiteSnapshot(
        path=classified, file_sha256=hashlib.sha256(classified.read_bytes()).hexdigest()
    )
    with pytest.raises(migration.LegacyMigrationError, match="credential-bearing columns"):
        migration.plan_legacy_adoption(snapshot)
