"""Shared ENG-28 fakes, clocks, and temp-store helpers.

Deterministic, no network. Production auto-grant lives only in
``authz_fixtures.py``.
"""

from __future__ import annotations

import json
import os
import socket
import sqlite3
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional

from agent.durable_jobs.clock import FrozenClock
from agent.durable_jobs.store import DurableJobStore


def db_path(tmp_path: Path) -> Path:
    return tmp_path / "pilot_jobs.sqlite"


def deny_network(monkeypatch) -> None:
    def _deny(*_args, **_kwargs):
        raise AssertionError("network socket open attempted in ENG-28 matrix")

    monkeypatch.setattr(socket.socket, "connect", _deny)
    monkeypatch.setattr(socket.socket, "connect_ex", _deny)


def make_job(
    tmp_path: Path,
    *,
    idempotency_key: str = "idem-eng28",
    authorize: bool = True,
    origin_root_thread_id: str = "111.222",
):
    store = DurableJobStore(sqlite_path=db_path(tmp_path))
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id=origin_root_thread_id,
        objective="ENG-28 matrix",
        repository_identity="github.com/example/repo",
        frozen_baseline_sha="sha-eng28",
        idempotency_key=idempotency_key,
    )
    if authorize:
        from tests.agent.durable_jobs.authz_fixtures import (
            install_default_adapter_authorization,
        )

        install_default_adapter_authorization(store.sqlite_path, job.job_id)
    return store, job


@dataclass
class FakeRun:
    run_id: Optional[str]
    idempotency_key: str


@dataclass
class FakeCreateResult:
    kind: str
    run: Optional[FakeRun] = None
    candidates: tuple[FakeRun, ...] = ()


class StatefulCursorProvider:
    """Fake Cursor keyed by immutable idempotency key. No network."""

    def __init__(
        self,
        create_result: FakeCreateResult,
        lookups: Optional[List[FakeRun]] = None,
    ) -> None:
        self.create_result = create_result
        self.lookups = list(lookups or [])
        self.create_calls: list[dict] = []
        self.lookup_calls: list[str] = []
        self._lock = threading.Lock()
        self.store: dict[str, list[FakeRun]] = {}

    def create_run(self, *, idempotency_key: str, job_id: str) -> FakeCreateResult:
        with self._lock:
            self.create_calls.append(
                {"idempotency_key": idempotency_key, "job_id": job_id}
            )
            if (
                self.create_result.kind == "accepted"
                and self.create_result.run is not None
                and self.create_result.run.run_id
            ):
                self.store.setdefault(idempotency_key, []).append(
                    self.create_result.run
                )
            return self.create_result

    def lookup_runs(self, *, idempotency_key: str) -> list[FakeRun]:
        with self._lock:
            self.lookup_calls.append(idempotency_key)
            if self.lookups:
                return list(self.lookups)
            return list(self.store.get(idempotency_key, []))


@dataclass
class FakePosted:
    message_ts: str
    client_msg_id: str


@dataclass
class FakePostResult:
    kind: str
    message_ts: Optional[str] = None


class StatefulSlackPort:
    def __init__(
        self,
        post_result: FakePostResult,
        lookups: Optional[List[FakePosted]] = None,
    ) -> None:
        self.post_result = post_result
        self.lookups = list(lookups or [])
        self.posts: list[dict] = []
        self.lookup_calls: list[str] = []
        self._lock = threading.Lock()
        self.store: dict[str, list[FakePosted]] = {}

    def post_root(
        self,
        *,
        client_msg_id: str,
        workspace_id: str,
        channel_id: str,
        root_thread_ts: str,
        job_id: str,
    ) -> FakePostResult:
        with self._lock:
            self.posts.append(
                {
                    "client_msg_id": client_msg_id,
                    "workspace_id": workspace_id,
                    "channel_id": channel_id,
                    "root_thread_ts": root_thread_ts,
                    "job_id": job_id,
                }
            )
            if self.post_result.kind == "accepted" and self.post_result.message_ts:
                self.store.setdefault(client_msg_id, []).append(
                    FakePosted(self.post_result.message_ts, client_msg_id)
                )
            return self.post_result

    def lookup_by_client_msg_id(self, client_msg_id: str) -> list[FakePosted]:
        with self._lock:
            self.lookup_calls.append(client_msg_id)
            if self.lookups:
                return list(self.lookups)
            return list(self.store.get(client_msg_id, []))


@dataclass
class RecordingAckPort:
    acks: list[dict] = field(default_factory=list)
    fail_once: bool = False
    _failed: bool = False

    def ack(self, *, inbound_id: str, job_id: str) -> str:
        if self.fail_once and not self._failed:
            self._failed = True
            raise RuntimeError("injected ack lost")
        self.acks.append({"inbound_id": inbound_id, "job_id": job_id})
        return f"ack:{inbound_id}"


def child_env() -> dict[str, str]:
    env = os.environ.copy()
    repo = str(Path(__file__).resolve().parents[3])
    env["PYTHONPATH"] = os.pathsep.join([repo, env.get("PYTHONPATH", "")])
    env["HERMES_HOME"] = env.get("HERMES_HOME", "/tmp/hermes-eng28-child")
    return env


def count_table(path: Path, table: str) -> int:
    conn = sqlite3.connect(path)
    try:
        (n,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return int(n)
    finally:
        conn.close()


def load_matrix() -> dict[str, Any]:
    path = (
        Path(__file__).resolve().parents[3]
        / "agent"
        / "durable_jobs"
        / "eng28_matrix.json"
    )
    return json.loads(path.read_text(encoding="utf-8"))
