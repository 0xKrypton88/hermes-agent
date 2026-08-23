"""Persisted sole-writer authority contract for ENG-118.

The binding passed to the gate must be read from the datastore at the write
boundary.  Nothing in this module caches authority in process-global state.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import time
from uuid import uuid4
from typing import Callable, ContextManager, Iterable, Iterator, Literal, Protocol

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


WRITER_EFFECT_LEASE_DDL = """
CREATE TABLE IF NOT EXISTS durable_writer_effect_leases (
    storage_id TEXT NOT NULL,
    environment_id TEXT NOT NULL,
    effect_key TEXT NOT NULL,
    authority_epoch BIGINT NOT NULL,
    writer_id TEXT NOT NULL,
    lease_token TEXT NOT NULL,
    expires_at DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (storage_id, environment_id, effect_key)
)
""".strip()


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
    lease_scope: Callable[[WriterAuthorityBinding, str], ContextManager[None]] | None = None

    @classmethod
    def from_connection_provider(
        cls,
        connection_provider: Callable[[], object],
        *,
        expected: AuthorityTarget,
        requested_mode: WriterMode,
        writer_id: str,
        minimum_epoch: int,
        enforced: bool = True,
    ) -> "DatastoreWriterAuthorityCheck":
        return cls(
            load_bindings=lambda: _load_bindings_from_provider(connection_provider, expected),
            expected=expected,
            requested_mode=requested_mode,
            writer_id=writer_id,
            minimum_epoch=minimum_epoch,
            enforced=enforced,
            lease_scope=lambda binding, key: datastore_writer_effect_lease(
                connection_provider, binding, key
            ),
        )

    def __call__(self) -> WriterAuthorityBinding | None:
        return assert_write_authority(
            self.load_bindings(),
            expected=self.expected,
            requested_mode=self.requested_mode,
            writer_id=self.writer_id,
            minimum_epoch=self.minimum_epoch,
            enforced=self.enforced,
        )

    @contextmanager
    def effect_lease(self, effect_key: str) -> Iterator[None]:
        """Hold a datastore-backed authority lease across one external effect."""
        binding = self()
        if binding is None or self.lease_scope is None:
            raise WriterAuthorityError(
                "datastore-backed effect lease is required for external effects"
            )
        with self.lease_scope(binding, effect_key):
            self()
            yield
            self()


def _is_postgres_connection(connection: object) -> bool:
    module = type(connection).__module__.lower()
    return "psycopg" in module or "postgres" in module


def _execute_authority_sql(connection, sql: str, params: tuple = ()):
    if _is_postgres_connection(connection):
        sql = sql.replace("?", "%s")
    return connection.execute(sql, params)


def _begin_authority_transaction(connection, target: AuthorityTarget) -> None:
    key = f"{target.storage_id}:{target.environment_id}"
    if _is_postgres_connection(connection):
        _execute_authority_sql(
            connection,
            "SELECT pg_advisory_xact_lock(hashtextextended(?, 0))",
            (key,),
        )
    else:
        if bool(getattr(connection, "in_transaction", False)):
            connection.commit()
        connection.execute("BEGIN IMMEDIATE")


@contextmanager
def datastore_writer_effect_lease(
    connection_provider: Callable[[], object],
    binding: WriterAuthorityBinding,
    effect_key: str,
    *,
    ttl_seconds: float = 300.0,
) -> Iterator[None]:
    """Hold one target lock and authority transaction across the effect."""
    if not effect_key or ttl_seconds <= 0:
        raise WriterAuthorityError("effect lease key and TTL must be valid")
    token = uuid4().hex
    target = AuthorityTarget(binding.storage_id, binding.environment_id)
    connection = connection_provider()
    committed = False
    try:
        _begin_authority_transaction(connection, target)
        _execute_authority_sql(connection, WRITER_EFFECT_LEASE_DDL)
        current = load_writer_authority(connection, target)
        assert_write_authority(
            current,
            expected=target,
            requested_mode=binding.mode,
            writer_id=binding.writer_id,
            minimum_epoch=binding.authority_epoch,
            enforced=True,
        )
        cursor = _execute_authority_sql(
            connection,
            "INSERT INTO durable_writer_effect_leases "
            "(storage_id, environment_id, effect_key, authority_epoch, writer_id, lease_token, expires_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(storage_id, environment_id, effect_key) DO UPDATE SET "
            "authority_epoch=excluded.authority_epoch, writer_id=excluded.writer_id, "
            "lease_token=excluded.lease_token, expires_at=excluded.expires_at "
            "WHERE durable_writer_effect_leases.authority_epoch = excluded.authority_epoch AND "
            "durable_writer_effect_leases.writer_id = excluded.writer_id",
            (binding.storage_id, binding.environment_id, effect_key, binding.authority_epoch, binding.writer_id, token, time.time() + ttl_seconds),
        )
        if cursor.rowcount != 1:
            raise WriterAuthorityError("effect already has a foreign authority lease")
        # The target transaction/advisory lock remains held while the effect runs.
        yield
        _execute_authority_sql(
            connection,
            "DELETE FROM durable_writer_effect_leases WHERE storage_id = ? AND environment_id = ? "
            "AND effect_key = ? AND lease_token = ?",
            (binding.storage_id, binding.environment_id, effect_key, token),
        )
        connection.commit()
        committed = True
    finally:
        if not committed:
            rollback = getattr(connection, "rollback", None)
            if callable(rollback):
                rollback()
        close = getattr(connection, "close", None)
        if callable(close):
            close()


def _load_bindings_from_provider(
    connection_provider: Callable[[], object], expected: AuthorityTarget
) -> tuple[WriterAuthorityBinding, ...]:
    connection = connection_provider()
    try:
        return load_writer_authority(connection, expected)
    finally:
        close = getattr(connection, "close", None)
        if callable(close):
            close()


def load_writer_authority(connection, expected: AuthorityTarget) -> tuple[WriterAuthorityBinding, ...]:
    """Read authoritative bindings from a DB-API connection without caching."""
    rows = _execute_authority_sql(
        connection,
        "SELECT storage_id, environment_id, authority_epoch, writer_id, mode "
        "FROM durable_writer_authority WHERE storage_id = ? AND environment_id = ?",
        (expected.storage_id, expected.environment_id),
    ).fetchall()
    return tuple(WriterAuthorityBinding(*row) for row in rows)


def activate_writer_authority(connection, binding: WriterAuthorityBinding) -> None:
    """Atomically lock the target, reject live effects, and install a newer epoch."""
    target = AuthorityTarget(binding.storage_id, binding.environment_id)
    committed = False
    try:
        _begin_authority_transaction(connection, target)
        _execute_authority_sql(connection, WRITER_EFFECT_LEASE_DDL)
        active_lease = _execute_authority_sql(
            connection,
            "SELECT 1 FROM durable_writer_effect_leases WHERE storage_id = ? AND environment_id = ? "
            "AND (authority_epoch <> ? OR writer_id <> ?) LIMIT 1",
            (binding.storage_id, binding.environment_id, binding.authority_epoch, binding.writer_id),
        ).fetchone()
        if active_lease is not None:
            raise WriterAuthorityError("authority handover is fenced by a live external-effect lease")
        cursor = _execute_authority_sql(
            connection,
            "INSERT INTO durable_writer_authority "
            "(storage_id, environment_id, authority_epoch, writer_id, mode) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(storage_id, environment_id) DO UPDATE SET "
            "authority_epoch=excluded.authority_epoch, writer_id=excluded.writer_id, mode=excluded.mode "
            "WHERE durable_writer_authority.authority_epoch < excluded.authority_epoch",
            (binding.storage_id, binding.environment_id, binding.authority_epoch, binding.writer_id, binding.mode),
        )
        if cursor.rowcount != 1:
            raise WriterAuthorityError("authority epoch must increase monotonically")
        connection.commit()
        committed = True
    finally:
        if not committed:
            rollback = getattr(connection, "rollback", None)
            if callable(rollback):
                rollback()
