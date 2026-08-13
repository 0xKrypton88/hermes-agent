"""ENG-28 deterministic failure-injection matrix.

Real temp SQLite, stateful fakes, fresh subprocesses, FrozenClock.
No network. ENG-29 Go gating is not weakened.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import textwrap
import threading
from pathlib import Path

import pytest

from tests.agent.durable_jobs.eng28_support import (
    FakeCreateResult,
    FakePostResult,
    FakePosted,
    FakeRun,
    RecordingAckPort,
    StatefulCursorProvider,
    StatefulSlackPort,
    child_env,
    db_path,
    deny_network,
    load_matrix,
    make_job,
)


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    deny_network(monkeypatch)


def test_eng28_matrix_artifact_covers_all_22_rows():
    matrix = load_matrix()
    rows = matrix["rows"]
    assert len(rows) == 22
    ids = [int(row["id"]) for row in rows]
    assert ids == list(range(1, 23))
    for row in rows:
        assert row["selectors"], f"row {row['id']} missing selectors"
        assert row["proof_layer"] in {
            "PROVEN_LOCAL",
            "PARTIAL",
            "BLOCKED_EXTERNAL",
            "BLOCKED_MISSING_SEAM",
            "PENDING",
        }


# ---------------------------------------------------------------------------
# Row 1 — before/after immutable job/package commit
# ---------------------------------------------------------------------------


def test_row01_crash_before_job_commit_persists_nothing(tmp_path, monkeypatch):
    from agent.durable_jobs import store as store_mod
    from agent.durable_jobs.store import DurableJobStore

    def boom() -> None:
        raise RuntimeError("injected crash before job commit")

    monkeypatch.setattr(
        store_mod, "after_job_rows_before_commit", boom, raising=False
    )

    store = DurableJobStore(sqlite_path=db_path(tmp_path))
    with pytest.raises(RuntimeError, match="injected crash before job commit"):
        store.create_job(
            origin_platform="slack",
            origin_chat_id="C123",
            origin_root_thread_id="111.222",
            objective="crash before commit",
            repository_identity="github.com/example/repo",
            idempotency_key="idem-row01-before",
        )
    conn = sqlite3.connect(store.sqlite_path)
    try:
        (n,) = conn.execute("SELECT COUNT(*) FROM durable_jobs").fetchone()
        (e,) = conn.execute("SELECT COUNT(*) FROM durable_job_events").fetchone()
    finally:
        conn.close()
    assert n == 0
    assert e == 0


def test_row01_crash_after_job_commit_reopens_exactly_one(tmp_path):
    script = textwrap.dedent(
        """
        import sys
        from pathlib import Path
        from agent.durable_jobs.store import DurableJobStore
        store = DurableJobStore(sqlite_path=Path(sys.argv[1]))
        store.create_job(
            origin_platform="slack",
            origin_chat_id="C123",
            origin_root_thread_id="111.222",
            objective="crash after commit",
            repository_identity="github.com/example/repo",
            idempotency_key="idem-row01-after",
        )
        """
    )
    db = db_path(tmp_path)
    proc = subprocess.run(
        [sys.executable, "-c", script, str(db)],
        env=child_env(),
        check=True,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0
    from agent.durable_jobs.store import DurableJobStore

    store = DurableJobStore(sqlite_path=db)
    first = store.get_job_by_idempotency_key("idem-row01-after")
    assert first is not None
    second = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="crash after commit",
        repository_identity="github.com/example/repo",
        idempotency_key="idem-row01-after",
    )
    assert second.job_id == first.job_id
    assert store.count_jobs() == 1
    events = store.list_events(first.job_id)
    assert any(ev["event_type"] == "job_created" for ev in events)


def test_row01_tuple_rejected_before_job_exists(tmp_path):
    from agent.durable_jobs.eng29 import (
        MATRIX_VERSION,
        PROVIDER_CREATE_TARGET_ACTION,
        register_authorization_tuple,
    )
    from agent.durable_jobs.store import DurableJobStore

    DurableJobStore(sqlite_path=db_path(tmp_path))
    result = register_authorization_tuple(
        db_path(tmp_path),
        job_id="dj_missing",
        source_package_id="github.com/example/repo",
        source_package_version="v1",
        candidate_sha="sha-eng28",
        candidate_id="cand-1",
        candidate_version="v1",
        target_environment="slack",
        target_action=PROVIDER_CREATE_TARGET_ACTION,
        authorized_actor="U-alice",
        expires_at="2099-01-01T00:00:00+00:00",
        policy_version="pol-1",
        matrix_version=MATRIX_VERSION,
        authorization_idempotency_key="tuple:missing:create",
    )
    assert result.ok is False
    assert "unauthorized" in result.reason_codes
