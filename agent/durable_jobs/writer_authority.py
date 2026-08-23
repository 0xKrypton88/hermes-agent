"""Persisted sole-writer authority contract for ENG-118.

The binding passed to the gate must be read from the datastore at the write
boundary.  Nothing in this module caches authority in process-global state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Literal, Protocol

WriterMode = Literal["legacy", "new"]


class WriterAuthorityCheck(Protocol):
    """Fresh authority assertion invoked at each durable write boundary."""

    def __call__(self) -> "WriterAuthorityBinding | None": ...


class WriterAuthorityError(RuntimeError):
    """A writer cannot prove current, exclusive datastore authority."""


@dataclass(frozen=True)
class AuthorityTarget:
    storage_id: str
    environment_id: str

    def __post_init__(self) -> None:
        if not self.storage_id or not self.environment_id:
            raise WriterAuthorityError("authority target fields must be non-empty")


@dataclass(frozen=True)
class WriterAuthorityBinding:
    storage_id: str
    environment_id: str
    authority_epoch: int
    writer_id: str
    mode: WriterMode

    def __post_init__(self) -> None:
        if not self.storage_id or not self.environment_id or not self.writer_id:
            raise WriterAuthorityError("authority binding fields must be non-empty")
        if self.authority_epoch < 1:
            raise WriterAuthorityError("authority epoch must be positive")
        if self.mode not in {"legacy", "new"}:
            raise WriterAuthorityError("authority mode must be legacy or new")


WRITER_AUTHORITY_DDL = """
CREATE TABLE IF NOT EXISTS durable_writer_authority (
    storage_id TEXT NOT NULL,
    environment_id TEXT NOT NULL,
    authority_epoch BIGINT NOT NULL CHECK (authority_epoch > 0),
    writer_id TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('legacy', 'new')),
    PRIMARY KEY (storage_id, environment_id)
)
""".strip()


def assert_write_authority(
    bindings: Iterable[WriterAuthorityBinding],
    *,
    expected: AuthorityTarget,
    requested_mode: WriterMode,
    writer_id: str,
    minimum_epoch: int,
    enforced: bool,
) -> WriterAuthorityBinding | None:
    """Authorize one write using a fresh datastore binding.

    Before explicit activation, only the existing legacy writer remains
    compatible. Once enforced, missing/double/stale/foreign bindings all fail.
    """

    materialized = tuple(bindings)
    if not enforced:
        if requested_mode == "legacy" and not materialized:
            return None
        raise WriterAuthorityError("writer authority is not explicitly activated")
    if len(materialized) != 1:
        raise WriterAuthorityError("exactly one datastore authority binding is required")
    binding = materialized[0]
    if (binding.storage_id, binding.environment_id) != (
        expected.storage_id,
        expected.environment_id,
    ):
        raise WriterAuthorityError("authority target mismatch")
    if binding.authority_epoch < minimum_epoch:
        raise WriterAuthorityError("stale authority epoch")
    if binding.mode != requested_mode:
        raise WriterAuthorityError("authority mode does not permit this writer")
    if binding.writer_id != writer_id:
        raise WriterAuthorityError("writer identity mismatch")
    return binding


@dataclass(frozen=True)
class DatastoreWriterAuthorityCheck:
    """Bind a writer to a fresh datastore loader without caching its result."""

    load_bindings: Callable[[], Iterable[WriterAuthorityBinding]]
    expected: AuthorityTarget
    requested_mode: WriterMode
    writer_id: str
    minimum_epoch: int
    enforced: bool = True

    def __call__(self) -> WriterAuthorityBinding | None:
        return assert_write_authority(
            self.load_bindings(),
            expected=self.expected,
            requested_mode=self.requested_mode,
            writer_id=self.writer_id,
            minimum_epoch=self.minimum_epoch,
            enforced=self.enforced,
        )


def load_writer_authority(connection, expected: AuthorityTarget) -> tuple[WriterAuthorityBinding, ...]:
    """Read authoritative bindings from a DB-API connection without caching."""

    rows = connection.execute(
        "SELECT storage_id, environment_id, authority_epoch, writer_id, mode "
        "FROM durable_writer_authority WHERE storage_id = ? AND environment_id = ?",
        (expected.storage_id, expected.environment_id),
    ).fetchall()
    return tuple(WriterAuthorityBinding(*row) for row in rows)


def activate_writer_authority(connection, binding: WriterAuthorityBinding) -> None:
    """Persist an explicit monotonic handover; caller owns transaction scope."""

    existing = connection.execute(
        "SELECT authority_epoch FROM durable_writer_authority "
        "WHERE storage_id = ? AND environment_id = ?",
        (binding.storage_id, binding.environment_id),
    ).fetchone()
    if existing is not None and int(existing[0]) >= binding.authority_epoch:
        raise WriterAuthorityError("authority epoch must increase monotonically")
    connection.execute(
        "INSERT INTO durable_writer_authority "
        "(storage_id, environment_id, authority_epoch, writer_id, mode) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(storage_id, environment_id) DO UPDATE SET "
        "authority_epoch=excluded.authority_epoch, writer_id=excluded.writer_id, "
        "mode=excluded.mode",
        (
            binding.storage_id,
            binding.environment_id,
            binding.authority_epoch,
            binding.writer_id,
            binding.mode,
        ),
    )
