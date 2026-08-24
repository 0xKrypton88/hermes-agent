"""ENG-118 frozen migration identity contract."""

from __future__ import annotations

import hashlib
import importlib
import json
import sqlite3
from dataclasses import replace
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
    first = migration.apply_legacy_adoption(
        plan, ledger, dispositions=dispositions, expected_source_snapshot=plan.source_snapshot
    )
    second = migration.apply_legacy_adoption(
        plan, ledger, dispositions=dispositions, expected_source_snapshot=plan.source_snapshot
    )
    assert first.total_count == second.total_count == len(plan.entries)
    assert first.inserted_count == len(plan.entries)
    assert second.inserted_count == 0
    assert second.duplicate_count == len(plan.entries)
    assert migration.verify_legacy_adoption(
        plan, ledger, dispositions=dispositions, expected_source_snapshot=plan.source_snapshot
    ).verified is True

    changed = plan.with_replaced_row_sha(plan.entries[0].migration_key, "0" * 64)
    with pytest.raises(migration.LegacyMigrationError, match="divergent"):
        migration.apply_legacy_adoption(
            changed,
            ledger,
            dispositions=dispositions,
            expected_source_snapshot=plan.source_snapshot,
        )

    changed_dispositions = dict(dispositions)
    changed_dispositions[plan.blockers[0].migration_key] = "operator_discard"
    with pytest.raises(migration.LegacyMigrationError, match="divergent"):
        migration.apply_legacy_adoption(
            plan,
            ledger,
            dispositions=changed_dispositions,
            expected_source_snapshot=plan.source_snapshot,
        )


def test_verify_adoption_is_read_only_and_checks_dispositions(tmp_path):
    migration = _legacy_api()
    assert migration is not None
    path, digest = _snapshot(tmp_path)
    plan = migration.plan_legacy_adoption(
        migration.FrozenSQLiteSnapshot(path=path, file_sha256=digest)
    )
    missing = tmp_path / "missing-ledger.sqlite3"
    result = migration.verify_legacy_adoption(
        plan, missing, dispositions={}, expected_source_snapshot=plan.source_snapshot
    )
    assert result.verified is False
    assert not missing.exists()

    ledger = tmp_path / "adoption-ledger.sqlite3"
    dispositions = {item.migration_key: "operator_quarantine" for item in plan.blockers}
    migration.apply_legacy_adoption(
        plan, ledger, dispositions=dispositions, expected_source_snapshot=plan.source_snapshot
    )
    wrong = dict(dispositions)
    wrong[plan.blockers[0].migration_key] = "operator_discard"
    assert migration.verify_legacy_adoption(
        plan,
        ledger,
        dispositions=wrong,
        expected_source_snapshot=plan.source_snapshot,
    ).verified is False
    assert migration.verify_legacy_adoption(
        plan,
        ledger,
        dispositions={},
        expected_source_snapshot=plan.source_snapshot,
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
    import threading
    from agent.durable_jobs.writer_authority import (
        AuthorityTarget, DatastoreWriterAuthorityCheck, WRITER_AUTHORITY_DDL,
        WriterAuthorityBinding, activate_writer_authority,
    )

    path = tmp_path / "authority.db"
    setup = sqlite3.connect(path)
    setup.execute(WRITER_AUTHORITY_DDL)
    current = WriterAuthorityBinding("storage", "env", 1, "writer-a", "new")
    activate_writer_authority(setup, current)
    setup.close()

    provider = lambda: sqlite3.connect(path, timeout=5)
    check = DatastoreWriterAuthorityCheck.from_connection_provider(
        provider, expected=AuthorityTarget("storage", "env"), requested_mode="new",
        writer_id="writer-a", minimum_epoch=1,
    )
    replacement = WriterAuthorityBinding("storage", "env", 2, "writer-b", "new")
    started = threading.Event()
    done = threading.Event()
    errors = []

    def handover():
        started.set()
        connection = provider()
        try:
            activate_writer_authority(connection, replacement)
        except Exception as exc:
            errors.append(exc)
        finally:
            connection.close()
            done.set()

    with check.effect_lease("linear:job-1"):
        worker = threading.Thread(target=handover)
        worker.start()
        assert started.wait(1)
        assert done.wait(0.2) is False
    assert done.wait(2)
    worker.join(timeout=2)
    assert errors == []
    connection = provider()
    row = connection.execute(
        "SELECT authority_epoch, writer_id FROM durable_writer_authority"
    ).fetchone()
    connection.close()
    assert row == (2, "writer-b")


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
    with pytest.raises(migration.LegacyMigrationError, match="not export-allowlisted"):
        migration.plan_legacy_adoption(snapshot)

def test_shadow_redacts_credential_shaped_values_in_allowlisted_content(tmp_path):
    import hashlib

    from agent.durable_jobs import legacy_migration as migration

    source = tmp_path / "credential-content.db"
    conn = sqlite3.connect(source)
    conn.execute("CREATE TABLE messages (id TEXT PRIMARY KEY, content TEXT)")
    secret = "supersecretvalue123"
    conn.execute("INSERT INTO messages VALUES (?, ?)", ("m1", f"api_key={secret}"))
    conn.commit()
    conn.close()
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    plan = migration.plan_legacy_adoption(
        migration.FrozenSQLiteSnapshot(path=source, file_sha256=digest)
    )
    canonical = plan.entries[0].canonical_row_json
    assert secret not in canonical
    assert hashlib.sha256(secret.encode()).hexdigest() in canonical


def test_nested_secret_keys_are_redacted_before_all_exports_and_hashes(tmp_path):
    migration = _legacy_api()
    assert migration is not None
    raw_secret = "Bearer reviewer-secret-123456789"
    path = tmp_path / "legacy-nested-secrets.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        "CREATE TABLE sessions(id TEXT PRIMARY KEY, origin_json TEXT);"
        "CREATE TABLE messages(id INTEGER PRIMARY KEY, session_id TEXT, content TEXT);"
    )
    conn.execute("INSERT INTO sessions VALUES (?, ?)", ("s1", "{}"))
    payload = {
        "safe": "keep-me",
        "nested": {
            "Authorization": raw_secret,
            "children": [
                {"access_token": "child-token-secret-123456789"},
                {"private_key": "not-a-real-key-but-still-secret"},
            ],
        },
    }
    conn.execute(
        "INSERT INTO messages VALUES (?, ?, ?)",
        (1, "s1", json.dumps(payload)),
    )
    conn.commit()
    conn.close()
    snapshot = migration.FrozenSQLiteSnapshot(
        path=path, file_sha256=hashlib.sha256(path.read_bytes()).hexdigest()
    )

    plan = migration.plan_legacy_adoption(snapshot)
    all_exports = plan.manifest_json() + plan.reconciliation_json()
    assert raw_secret not in all_exports
    assert "child-token-secret-123456789" not in all_exports
    assert "not-a-real-key-but-still-secret" not in all_exports
    message = next(entry for entry in plan.entries if entry.source_table == "messages")
    canonical = json.loads(message.canonical_row_json)
    assert canonical["content"]["safe"] == "keep-me"
    assert canonical["content"]["nested"]["Authorization"].startswith(
        "<redacted:sha256="
    )
    assert canonical["content"]["nested"]["children"][0]["access_token"].startswith(
        "<redacted:sha256="
    )
    assert message.row_sha256 == hashlib.sha256(
        message.canonical_row_json.encode("utf-8")
    ).hexdigest()


def test_nested_secret_in_text_primary_key_is_redacted_before_identity_and_exports(
    tmp_path, monkeypatch
):
    migration = _legacy_api()
    assert migration is not None
    raw_secret = "fabricated-pk-secret-123456789"
    source = tmp_path / "credential-primary-key.sqlite3"
    with sqlite3.connect(source) as conn:
        conn.execute("CREATE TABLE sessions(id TEXT PRIMARY KEY, origin_json TEXT)")
        primary_key = json.dumps(
            {"tenant": "safe", "nested": {"access_token": raw_secret}}
        )
        conn.execute("INSERT INTO sessions VALUES (?, ?)", (primary_key, "{}"))

    serialized_hash_inputs = []
    original_sha = migration._sha

    def capture_sha(value):
        if isinstance(value, str):
            serialized_hash_inputs.append(value)
        return original_sha(value)

    monkeypatch.setattr(migration, "_sha", capture_sha)
    snapshot = migration.FrozenSQLiteSnapshot(
        path=source, file_sha256=hashlib.sha256(source.read_bytes()).hexdigest()
    )
    plan = migration.plan_legacy_adoption(snapshot)
    entry = plan.entries[0]
    visible_outputs = "".join(
        (
            entry.source_pk_json,
            entry.canonical_row_json,
            plan.manifest_json(),
            plan.reconciliation_json(),
        )
    )

    assert raw_secret not in visible_outputs
    # The raw value may be hashed only to construct its redaction marker. It must
    # not reach any later migration-key, row, table, or population hash input.
    assert [value for value in serialized_hash_inputs if raw_secret in value] == [
        json.dumps(raw_secret, sort_keys=True, separators=(",", ":"))
    ]
    assert json.loads(entry.source_pk_json)["id"]["nested"]["access_token"].startswith(
        "<redacted:sha256="
    )
    assert entry.migration_key == original_sha(
        json.dumps(
            {
                "source_table": "sessions",
                "primary_key": json.loads(entry.source_pk_json),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )

    collision_source = tmp_path / "redaction-collision.sqlite3"
    with sqlite3.connect(collision_source) as conn:
        conn.execute("CREATE TABLE sessions(id TEXT PRIMARY KEY)")
        conn.executemany(
            "INSERT INTO sessions VALUES (?)",
            [
                (json.dumps({"access_token": "fabricated-first-123456789"}),),
                (json.dumps({"access_token": "fabricated-second-123456789"}),),
            ],
        )
    monkeypatch.setattr(migration, "_redacted_marker", lambda _value: "<redacted>")
    collision_snapshot = migration.FrozenSQLiteSnapshot(
        path=collision_source,
        file_sha256=hashlib.sha256(collision_source.read_bytes()).hexdigest(),
    )
    with pytest.raises(migration.LegacyMigrationError, match="collide after credential redaction"):
        migration.plan_legacy_adoption(collision_snapshot)


def test_verify_recomputes_snapshot_and_rejects_self_consistent_forged_plan(tmp_path):
    migration = _legacy_api()
    assert migration is not None
    path, digest = _snapshot(tmp_path)
    plan = migration.plan_legacy_adoption(
        migration.FrozenSQLiteSnapshot(path=path, file_sha256=digest)
    )
    entry = plan.entries[0]
    forged_canonical = json.dumps(
        {"forged": True}, sort_keys=True, separators=(",", ":")
    )
    forged_entry = entry.__class__(
        migration_key=entry.migration_key,
        source_table=entry.source_table,
        source_pk_json=entry.source_pk_json,
        row_sha256=hashlib.sha256(forged_canonical.encode("utf-8")).hexdigest(),
        canonical_row_json=forged_canonical,
        target_kind=entry.target_kind,
    )
    forged_plan = replace(plan, entries=(forged_entry, *plan.entries[1:]))
    ledger = tmp_path / "forged-ledger.sqlite3"
    dispositions = {
        item.migration_key: "operator_quarantine" for item in forged_plan.blockers
    }
    with pytest.raises(
        migration.LegacyMigrationError,
        match="expected source provenance",
    ):
        migration.apply_legacy_adoption(
            forged_plan,
            ledger,
            dispositions=dispositions,
            expected_source_snapshot=plan.source_snapshot,
        )
    assert not ledger.exists()

    # Reproduce the reviewer's stronger case: both the plan and ledger agree on
    # the forged payload. Verification must still reject source-independent data.
    conn = sqlite3.connect(ledger)
    try:
        migration._ensure_ledger(conn)
        for item in forged_plan.entries:
            disposition = dispositions.get(item.migration_key)
            conn.execute(
                "INSERT INTO eng118_adoption_ledger VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    item.migration_key,
                    item.source_table,
                    item.source_pk_json,
                    item.row_sha256,
                    item.canonical_row_json,
                    item.target_kind,
                    disposition,
                    forged_plan.snapshot_sha256,
                    forged_plan.population_sha256,
                ),
            )
        conn.commit()
    finally:
        conn.close()

    assert not migration.verify_legacy_adoption(
        forged_plan,
        ledger,
        dispositions=dispositions,
        expected_source_snapshot=plan.source_snapshot,
    ).verified


def test_apply_and_verify_bind_plan_to_independently_expected_source(tmp_path):
    migration = _legacy_api()
    assert migration is not None

    def frozen(name, session_id):
        path = tmp_path / name
        with sqlite3.connect(path) as conn:
            conn.execute("CREATE TABLE sessions(id TEXT PRIMARY KEY)")
            conn.execute("INSERT INTO sessions VALUES (?)", (session_id,))
        return migration.FrozenSQLiteSnapshot(
            path=path, file_sha256=hashlib.sha256(path.read_bytes()).hexdigest()
        )

    original = frozen("original.sqlite3", "operator-approved")
    forged = frozen("forged.sqlite3", "attacker-controlled")
    forged_plan = migration.plan_legacy_adoption(forged)
    ledger = tmp_path / "forged-ledger.sqlite3"

    with pytest.raises(migration.LegacyMigrationError, match="expected source provenance"):
        migration.apply_legacy_adoption(
            forged_plan,
            ledger,
            dispositions={},
            expected_source_snapshot=original,
        )
    assert not ledger.exists()

    migration.apply_legacy_adoption(
        forged_plan,
        ledger,
        dispositions={},
        expected_source_snapshot=forged,
    )
    assert not migration.verify_legacy_adoption(
        forged_plan,
        ledger,
        dispositions={},
        expected_source_snapshot=original,
    ).verified
    assert migration.verify_legacy_adoption(
        forged_plan,
        ledger,
        dispositions={},
        expected_source_snapshot=forged,
    ).verified

    missing = tmp_path / "missing-provenance-ledger.sqlite3"
    with pytest.raises(migration.LegacyMigrationError, match="expected source provenance"):
        migration.apply_legacy_adoption(forged_plan, missing, dispositions={})
    assert not migration.verify_legacy_adoption(
        forged_plan, ledger, dispositions={}
    ).verified
