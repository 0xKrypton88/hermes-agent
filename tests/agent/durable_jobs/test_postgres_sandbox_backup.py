"""ENG-25 — backup/restore sandbox contract. Never executes a restore here."""

from __future__ import annotations

import pytest

from agent.durable_jobs.sandbox_backup import (
    DESTRUCTIVE_SANDBOX_FLAG,
    SandboxBackupError,
    paired_snapshot_scope,
    restore_acceptance_steps,
    validate_sandbox_restore_request,
)


def test_snapshot_scope_pairs_application_and_checkpointer():
    scope = paired_snapshot_scope()
    assert scope.application_objects
    assert scope.checkpointer_objects
    assert "durable_jobs" in scope.application_objects
    assert "durable_job_events" in scope.application_objects
    assert "durable_jobs_meta" in scope.application_objects
    assert "durable_checkpoint_meta" in scope.checkpointer_objects
    assert scope.quiesce_assumption
    assert "paired" in scope.consistency_rule.lower() or "same" in scope.consistency_rule.lower()


def test_restore_acceptance_names_owner_acl_clean_db_and_markers():
    steps = restore_acceptance_steps()
    joined = " ".join(steps).lower()
    assert "quiesce" in joined
    assert "owner" in joined or "acl" in joined
    assert "clean" in joined
    assert "marker" in joined
    assert "checkpoint" in joined
    assert "event" in joined
    assert "restart" in joined


def test_restore_defaults_fail_closed_without_destructive_flag():
    with pytest.raises(SandboxBackupError) as exc:
        validate_sandbox_restore_request(
            application_dsn="postgresql://ubuntu@127.0.0.1:55432/hermes_dj_sandbox_app",
            checkpoint_dsn="postgresql://ubuntu@127.0.0.1:55432/hermes_dj_sandbox_ckpt",
            application_schema="djapp",
            checkpoint_schema="djckpt",
            destructive_sandbox=False,
        )
    assert "fail-closed" in str(exc.value).lower() or "destructive" in str(exc.value).lower()
    assert DESTRUCTIVE_SANDBOX_FLAG in str(exc.value) or "destructive" in str(exc.value).lower()


def test_restore_refuses_non_loopback_or_non_sandbox_database():
    with pytest.raises(SandboxBackupError) as exc:
        validate_sandbox_restore_request(
            application_dsn="postgresql://hermes:supersecret@db.internal:5432/prod",
            checkpoint_dsn="postgresql://hermes:supersecret@db.internal:5432/prod",
            application_schema="djapp",
            checkpoint_schema="djckpt",
            destructive_sandbox=True,
        )
    msg = str(exc.value)
    assert "supersecret" not in msg
    assert "disposable" in msg.lower() or "sandbox" in msg.lower() or "loopback" in msg.lower()


def test_restore_refuses_missing_explicit_dsns_even_with_flag():
    with pytest.raises(SandboxBackupError):
        validate_sandbox_restore_request(
            application_dsn="",
            checkpoint_dsn="postgresql://ubuntu@127.0.0.1:55432/hermes_dj_sandbox_ckpt",
            application_schema="djapp",
            checkpoint_schema="djckpt",
            destructive_sandbox=True,
        )


def test_execute_restore_is_not_reached_without_gate(monkeypatch):
    from agent.durable_jobs import sandbox_backup as sb

    def _boom(*_a, **_k):
        raise AssertionError("pg_restore must not run in Cursor Cloud")

    monkeypatch.setattr(sb, "_run_pg_restore", _boom)
    with pytest.raises(SandboxBackupError):
        sb.execute_sandbox_restore(
            application_dsn="postgresql://ubuntu@127.0.0.1:55432/hermes_dj_sandbox_app",
            checkpoint_dsn="postgresql://ubuntu@127.0.0.1:55432/hermes_dj_sandbox_ckpt",
            application_schema="djapp",
            checkpoint_schema="djckpt",
            snapshot_dir="/tmp/not-used",
            destructive_sandbox=False,
        )
