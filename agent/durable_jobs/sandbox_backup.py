"""Fail-closed disposable-sandbox backup/restore tooling for ENG-25.

This module documents and gates a paired application+checkpointer restore.
It does **not** run against production, credentials, or live Hermes state.
Cursor Cloud tests must never set the destructive flag against a real DB.
"""

from __future__ import annotations

from dataclasses import dataclass

from agent.durable_jobs.redaction import redact_secret_text

DESTRUCTIVE_SANDBOX_FLAG = "--i-understand-this-destroys-disposable-data"
_SANDBOX_DB_PREFIX = "hermes_dj_sandbox_"
_LOOPBACK = frozenset({"127.0.0.1", "localhost", "::1", "[::1]"})


class SandboxBackupError(RuntimeError):
    def __init__(self, message: str) -> None:
        super().__init__(redact_secret_text(message))


@dataclass(frozen=True)
class PairedSnapshotScope:
    application_objects: tuple[str, ...]
    checkpointer_objects: tuple[str, ...]
    quiesce_assumption: str
    consistency_rule: str


def paired_snapshot_scope() -> PairedSnapshotScope:
    return PairedSnapshotScope(
        application_objects=(
            "durable_jobs_meta",
            "durable_jobs",
            "durable_job_events",
            "durable_job_advance_claims",
        ),
        checkpointer_objects=(
            "durable_checkpoint_meta",
            "checkpoints",
            "checkpoint_blobs",
            "checkpoint_writes",
            "checkpoint_migrations",
        ),
        quiesce_assumption=(
            "Both application and checkpointer connections are idle: no "
            "in-flight create_and_advance, no open transactions, and no "
            "lease-holding advance owner."
        ),
        consistency_rule=(
            "The paired snapshot is one logical restore point: application "
            "schema and checkpointer schema must be captured together and "
            "restored together onto a clean database."
        ),
    )


def restore_acceptance_steps() -> tuple[str, ...]:
    return (
        "Quiesce writers; confirm no live advance claim leases remain.",
        "Snapshot application schema objects and checkpointer schema objects together.",
        "Restore onto a clean database (drop/create sandbox DB); never onto leftover schemas.",
        "Restore schema owner and ACLs to the sandbox role that will reopen the store.",
        "Validate application domain marker, owner_role, and schema_version before writes.",
        "Validate checkpointer domain marker before PostgresSaver.setup.",
        "Validate durable_job_events and checkpoint rows for the restored job_id.",
        "Restart acceptance: reopen stores and recover_job; phase and events must match.",
    )


def validate_sandbox_restore_request(
    *,
    application_dsn: str,
    checkpoint_dsn: str,
    application_schema: str,
    checkpoint_schema: str,
    destructive_sandbox: bool,
) -> None:
    if not destructive_sandbox:
        raise SandboxBackupError(
            "fail-closed: sandbox restore requires the explicit destructive "
            f"flag {DESTRUCTIVE_SANDBOX_FLAG}"
        )
    if not application_dsn or not checkpoint_dsn:
        raise SandboxBackupError(
            "sandbox restore requires explicit disposable application and "
            "checkpointer DSNs"
        )
    if not application_schema or not checkpoint_schema:
        raise SandboxBackupError("sandbox restore requires explicit schema names")
    if application_schema == checkpoint_schema:
        raise SandboxBackupError(
            "sandbox restore refuses a shared application/checkpointer schema"
        )
    _require_disposable_dsn(application_dsn, "application")
    _require_disposable_dsn(checkpoint_dsn, "checkpointer")


def execute_sandbox_restore(
    *,
    application_dsn: str,
    checkpoint_dsn: str,
    application_schema: str,
    checkpoint_schema: str,
    snapshot_dir: str,
    destructive_sandbox: bool,
) -> None:
    validate_sandbox_restore_request(
        application_dsn=application_dsn,
        checkpoint_dsn=checkpoint_dsn,
        application_schema=application_schema,
        checkpoint_schema=checkpoint_schema,
        destructive_sandbox=destructive_sandbox,
    )
    _run_pg_restore(
        application_dsn=application_dsn,
        checkpoint_dsn=checkpoint_dsn,
        application_schema=application_schema,
        checkpoint_schema=checkpoint_schema,
        snapshot_dir=snapshot_dir,
    )


def _require_disposable_dsn(dsn: str, label: str) -> None:
    from agent.durable_jobs.postgres_identity import _libpq_target, _uri_target

    text = dsn.strip()
    try:
        if "://" in text.split()[0]:
            host, _port, database = _uri_target(text)
        else:
            host, _port, database = _libpq_target(text)
    except Exception as exc:
        raise SandboxBackupError(f"sandbox {label} DSN is not usable") from exc
    host_l = (host or "").lower().strip("[]")
    if host_l not in _LOOPBACK:
        raise SandboxBackupError(
            f"sandbox {label} DSN must target loopback disposable PostgreSQL"
        )
    if not database.lower().startswith(_SANDBOX_DB_PREFIX):
        raise SandboxBackupError(
            f"sandbox {label} DSN database must start with {_SANDBOX_DB_PREFIX}"
        )


def _run_pg_restore(**_kwargs) -> None:
    raise SandboxBackupError(
        "sandbox pg_restore is not executed in Cursor Cloud; local Hermes "
        "runs restore only under its own destructive-sandbox gate"
    )
