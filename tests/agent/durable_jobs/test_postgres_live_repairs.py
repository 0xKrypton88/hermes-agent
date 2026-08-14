"""ENG-25 live PostgreSQL tests for isolation, domain, and advance races.

Skip only when HERMES_DURABLE_JOBS_PG_TEST_DSN is unset. Never mock green.
"""

from __future__ import annotations

import os
import time
import uuid
from multiprocessing import Process, Queue

import pytest

_TEST_DSN_ENV = "HERMES_DURABLE_JOBS_PG_TEST_DSN"
_MISSING_DSN_REASON = (
    "missing-test-DSN: HERMES_DURABLE_JOBS_PG_TEST_DSN is unset"
)

pytestmark = pytest.mark.skipif(
    not os.environ.get(_TEST_DSN_ENV),
    reason=_MISSING_DSN_REASON,
)


def _dsn() -> str:
    value = os.environ.get(_TEST_DSN_ENV, "").strip()
    if not value:
        pytest.skip(_MISSING_DSN_REASON)
    return value


def _schema_name(prefix: str) -> str:
    return f"{prefix}{uuid.uuid4().hex[:10]}"


def _drop_schema(dsn: str, schema: str) -> None:
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def _rewrite_host(dsn: str, host: str) -> str:
    from urllib.parse import urlparse, urlunparse

    parsed = urlparse(dsn)
    if parsed.scheme:
        userinfo = parsed.netloc.rsplit("@", 1)
        if len(userinfo) == 2:
            creds, _rest = userinfo
            port = f":{parsed.port}" if parsed.port else ""
            netloc = f"{creds}@{host}{port}"
        else:
            port = f":{parsed.port}" if parsed.port else ""
            netloc = f"{host}{port}"
        return urlunparse(parsed._replace(netloc=netloc))
    parts = []
    replaced = False
    for part in dsn.replace(";", " ").split():
        if part.lower().startswith("host="):
            parts.append(f"host={host}")
            replaced = True
        else:
            parts.append(part)
    if not replaced:
        parts.append(f"host={host}")
    return " ".join(parts)


@pytest.fixture
def live_dsn():
    dsn = _dsn()
    created: list[str] = []
    yield dsn, created
    for schema in created:
        try:
            _drop_schema(dsn, schema)
        except Exception:
            pass


def test_live_loopback_alias_probes_share_system_database(live_dsn):
    from agent.durable_jobs.postgres_identity import (
        identities_share_schema,
        probe_live_storage_identity,
    )

    dsn, _created = live_dsn
    schema = _schema_name("djid")
    left = probe_live_storage_identity(dsn, schema)
    right = probe_live_storage_identity(_rewrite_host(dsn, "localhost"), schema)
    assert identities_share_schema(left, right) is True
    other = probe_live_storage_identity(dsn, _schema_name("djid"))
    assert identities_share_schema(left, other) is False


def test_live_setup_rejects_aliased_same_schema_even_with_distinct_storage_ids(
    live_dsn,
):
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config
    from agent.durable_jobs.postgres_identity import (
        assert_distinct_live_identities,
        probe_live_storage_identity,
    )

    dsn, created = live_dsn
    schema = _schema_name("djshare")
    created.append(schema)
    with pytest.raises(DurableJobsConfigError):
        load_durable_jobs_config(
            {
                "durable_jobs": {
                    "backend": "postgresql",
                    "postgres_dsn": dsn,
                    "postgres_schema": schema,
                    "checkpoint_postgres_dsn": _rewrite_host(dsn, "localhost"),
                    "checkpoint_postgres_schema": schema,
                    "postgres_storage_id": "app_one",
                    "checkpoint_postgres_storage_id": "ckpt_two",
                }
            }
        )
    left = probe_live_storage_identity(dsn, schema)
    right = probe_live_storage_identity(_rewrite_host(dsn, "localhost"), schema)
    with pytest.raises(DurableJobsConfigError):
        assert_distinct_live_identities(left, right)


def test_live_empty_and_foreign_and_unrelated_schemas_fail_closed(live_dsn):
    import psycopg

    from agent.durable_jobs.config import DurableJobsConfigError
    from agent.durable_jobs.postgres_checkpointer import open_postgres_checkpointer
    from agent.durable_jobs.postgres_store import PostgresDurableJobStore

    dsn, created = live_dsn
    empty = _schema_name("djempty")
    foreign = _schema_name("djfrgn")
    unrelated = _schema_name("djunrel")
    created.extend([empty, foreign, unrelated])
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"CREATE SCHEMA {empty}")
        conn.execute(f"CREATE SCHEMA {foreign}")
        conn.execute(f"CREATE SCHEMA {unrelated}")
        conn.execute(f"CREATE TABLE {unrelated}.widgets (id int)")

    with pytest.raises((DurableJobsConfigError, Exception)) as empty_exc:
        PostgresDurableJobStore(dsn=dsn, schema=empty)
    assert "empty" in str(empty_exc.value).lower() or "foreign" in str(empty_exc.value).lower() or "unmarked" in str(empty_exc.value).lower()

    with pytest.raises((DurableJobsConfigError, Exception)):
        open_postgres_checkpointer(dsn=dsn, schema=foreign)

    with pytest.raises((DurableJobsConfigError, Exception)) as unrelated_exc:
        PostgresDurableJobStore(dsn=dsn, schema=unrelated)
    assert "unrelated" in str(unrelated_exc.value).lower() or "foreign" in str(unrelated_exc.value).lower()


def test_live_wrong_marker_and_wrong_owner_fail_closed(live_dsn):
    import psycopg

    from agent.durable_jobs.config import DurableJobsConfigError
    from agent.durable_jobs.postgres_domain import APPLICATION_DOMAIN
    from agent.durable_jobs.postgres_store import PostgresDurableJobStore
    from agent.durable_jobs.store import UnknownSchemaError

    dsn, created = live_dsn
    wrong_marker = _schema_name("djwm")
    owned = _schema_name("djown")
    created.extend([wrong_marker, owned])

    store = PostgresDurableJobStore(dsn=dsn, schema=owned)
    store.create_job(
        origin_platform="cli",
        origin_chat_id="local",
        origin_root_thread_id="root",
        objective="reopen",
        repository_identity="repo",
        idempotency_key="idem-reopen",
    )
    reopened = PostgresDurableJobStore(dsn=dsn, schema=owned)
    assert reopened.count_jobs() == 1

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"CREATE SCHEMA {wrong_marker}")
        conn.execute(
            f"CREATE TABLE {wrong_marker}.durable_jobs_meta (key text primary key, value text not null)"
        )
        conn.execute(
            f"INSERT INTO {wrong_marker}.durable_jobs_meta(key, value) VALUES (%s, %s), (%s, %s), (%s, %s)",
            (
                "schema_version",
                "9",
                "domain",
                "someone.else.product",
                "owner_role",
                "ubuntu",
            ),
        )
        conn.execute(
            f"UPDATE {owned}.durable_jobs_meta SET value = %s WHERE key = %s",
            ("not-the-owner", "owner_role"),
        )

    with pytest.raises((DurableJobsConfigError, UnknownSchemaError)):
        PostgresDurableJobStore(dsn=dsn, schema=wrong_marker)
    with pytest.raises((DurableJobsConfigError, UnknownSchemaError)):
        PostgresDurableJobStore(dsn=dsn, schema=owned)
    assert APPLICATION_DOMAIN.startswith("hermes.")


def _advance_in_process(dsn: str, app_schema: str, ckpt_schema: str, key: str, queue: Queue) -> None:
    from agent.durable_jobs.config import load_durable_jobs_config
    from agent.durable_jobs.service import DurableJobService

    cfg = load_durable_jobs_config(
        {
            "durable_jobs": {
                "enabled": True,
                "backend": "postgresql",
                "postgres_dsn": dsn,
                "postgres_schema": app_schema,
                "checkpoint_postgres_dsn": dsn,
                "checkpoint_postgres_schema": ckpt_schema,
                "postgres_storage_id": "durable_app",
                "checkpoint_postgres_storage_id": "durable_ckpt",
            }
        }
    )
    service = DurableJobService(config=cfg)
    job = service.create_and_advance(
        origin_platform="cli",
        origin_chat_id="local",
        origin_root_thread_id="root-race",
        objective="race",
        repository_identity="repo",
        idempotency_key=key,
        frozen_baseline_sha="sha-race",
    )
    queue.put((job.job_id, job.phase.value))


def test_live_multiprocess_create_and_advance_converges(live_dsn):
    from agent.durable_jobs.models import JobPhase
    from agent.durable_jobs.postgres_store import PostgresDurableJobStore

    dsn, created = live_dsn
    app_schema = _schema_name("djadv")
    ckpt_schema = _schema_name("djckpt")
    created.extend([app_schema, ckpt_schema])
    PostgresDurableJobStore(dsn=dsn, schema=app_schema)
    from agent.durable_jobs.postgres_checkpointer import open_postgres_checkpointer

    saver, conn = open_postgres_checkpointer(dsn=dsn, schema=ckpt_schema)
    conn.close()

    key = f"idem-adv-{uuid.uuid4().hex[:8]}"
    queue: Queue = Queue()
    workers = [
        Process(
            target=_advance_in_process,
            args=(dsn, app_schema, ckpt_schema, key, queue),
        )
        for _ in range(2)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=60)
        assert worker.exitcode == 0

    first = queue.get(timeout=5)
    second = queue.get(timeout=5)
    assert first[0] == second[0]
    assert first[1] == JobPhase.AWAIT_DISPATCH.value
    assert second[1] == JobPhase.AWAIT_DISPATCH.value
    store = PostgresDurableJobStore(dsn=dsn, schema=app_schema)
    assert store.count_jobs() == 1
    events = store.list_events(first[0])
    phases = [e["event_type"] for e in events if e["event_type"] == "phase_transition"]
    assert len(phases) == 2


def test_live_stale_advance_owner_is_taken_over(live_dsn):
    from datetime import datetime, timedelta, timezone

    from agent.durable_jobs.models import JobPhase
    from agent.durable_jobs.postgres_advance import AdvanceClaimDecision, AdvanceClaimError
    from agent.durable_jobs.postgres_store import PostgresDurableJobStore

    dsn, created = live_dsn
    app_schema = _schema_name("djstale")
    created.append(app_schema)
    store = PostgresDurableJobStore(dsn=dsn, schema=app_schema)
    job = store.create_job(
        origin_platform="cli",
        origin_chat_id="local",
        origin_root_thread_id="root",
        objective="stale",
        repository_identity="repo",
        idempotency_key=f"idem-stale-{uuid.uuid4().hex[:8]}",
    )
    expired = (datetime.now(timezone.utc) - timedelta(seconds=30)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    import psycopg

    with psycopg.connect(dsn) as conn:
        conn.execute(
            f"""
            INSERT INTO {app_schema}.durable_job_advance_claims(
                job_id, owner_token, status, generation, leased_until,
                created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (job.job_id, "stale-owner", "claimed", 1, expired, expired, expired),
        )
        conn.commit()

    claim = store.claim_advance(job.job_id, owner_token="recovery-owner", lease_seconds=30)
    assert claim.decision in {
        AdvanceClaimDecision.TAKEOVER,
        AdvanceClaimDecision.WIN,
    }
    with pytest.raises(AdvanceClaimError):
        store.complete_advance(job.job_id, owner_token="stale-owner")
    store.complete_advance(job.job_id, owner_token="recovery-owner")
    loaded = store.get_job(job.job_id)
    assert loaded is not None
    assert loaded.phase is JobPhase.INTAKE
