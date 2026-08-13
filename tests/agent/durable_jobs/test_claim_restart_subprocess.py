"""Fresh-process death/restart coverage for claim leases.

A child process persists CLAIMED then exits before the side effect completes.
The parent advances an injected clock past the lease (no sleeps) and recovers
by stable idempotency / client_msg_id lookup — never a blind create/repost.

This is crash evidence. Same-process reopen tests in test_claim_leases.py
and the older restart tests are unit evidence only.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path


def _child_env() -> dict[str, str]:
    env = os.environ.copy()
    repo = str(Path(__file__).resolve().parents[3])
    env["PYTHONPATH"] = os.pathsep.join([repo, env.get("PYTHONPATH", "")])
    return env


_PROVIDER_CHILD = textwrap.dedent(
    """
    import sys
    from datetime import datetime, timezone
    from pathlib import Path

    from agent.durable_jobs.clock import FrozenClock
    from agent.durable_jobs.effects import ProviderEffectLedger
    from agent.durable_jobs.store import DurableJobStore

    db = Path(sys.argv[1])
    job_id = sys.argv[2]
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    DurableJobStore(sqlite_path=db)
    ledger = ProviderEffectLedger(sqlite_path=db, now_fn=clock, lease_seconds=30)
    result = ledger.claim_effect(
        job_id=job_id,
        action_id="create_run",
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    if not result.won:
        raise SystemExit("child failed to claim provider effect")
    # Process death: persist CLAIMED, do not create_run / mark_accepted.
    """
)

_SLACK_CHILD = textwrap.dedent(
    """
    import sys
    from datetime import datetime, timezone
    from pathlib import Path

    from agent.durable_jobs.clock import FrozenClock
    from agent.durable_jobs.slack_contract import SlackBindingLedger
    from agent.durable_jobs.store import DurableJobStore

    db = Path(sys.argv[1])
    job_id = sys.argv[2]
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    DurableJobStore(sqlite_path=db)
    ledger = SlackBindingLedger(sqlite_path=db, now_fn=clock, lease_seconds=30)
    ledger.bind(
        job_id=job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    result = ledger.claim_delivery(job_id)
    if not result.won:
        raise SystemExit("child failed to claim slack delivery")
    # Process death: persist CLAIMED, do not post_root / mark_delivered.
    """
)

_PROVIDER_RECOVERING_CHILD = textwrap.dedent(
    """
    import sys
    from datetime import datetime, timezone
    from pathlib import Path

    from agent.durable_jobs.clock import FrozenClock
    from agent.durable_jobs.effects import ProviderEffectLedger
    from agent.durable_jobs.store import DurableJobStore

    db = Path(sys.argv[1])
    job_id = sys.argv[2]
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    DurableJobStore(sqlite_path=db)
    ledger = ProviderEffectLedger(sqlite_path=db, now_fn=clock, lease_seconds=30)
    result = ledger.claim_effect(
        job_id=job_id,
        action_id="create_run",
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    if not result.won:
        raise SystemExit("child failed to claim provider effect")
    ledger.note_empty_lookup(job_id, "create_run", owner_token=result.owner_token)
    """
)

_SLACK_RECOVERING_CHILD = textwrap.dedent(
    """
    import sys
    from datetime import datetime, timezone
    from pathlib import Path

    from agent.durable_jobs.clock import FrozenClock
    from agent.durable_jobs.slack_contract import SlackBindingLedger
    from agent.durable_jobs.store import DurableJobStore

    db = Path(sys.argv[1])
    job_id = sys.argv[2]
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    DurableJobStore(sqlite_path=db)
    ledger = SlackBindingLedger(sqlite_path=db, now_fn=clock, lease_seconds=30)
    ledger.bind(
        job_id=job_id,
        workspace_id="T1",
        channel_id="C123",
        root_thread_ts="111.222",
        candidate_id="cand-1",
        candidate_version="v1",
    )
    result = ledger.claim_delivery(job_id)
    if not result.won:
        raise SystemExit("child failed to claim slack delivery")
    ledger.note_empty_lookup(job_id, owner_token=result.owner_token)
    """
)


def _make_job(tmp_path: Path, *, idempotency_key: str):
    from agent.durable_jobs.store import DurableJobStore

    store = DurableJobStore(sqlite_path=tmp_path / "pilot_jobs.sqlite")
    job = store.create_job(
        origin_platform="slack",
        origin_chat_id="C123",
        origin_root_thread_id="111.222",
        objective="subprocess death",
        repository_identity="github.com/example/repo",
        idempotency_key=idempotency_key,
    )
    return store, job


def test_provider_child_process_death_then_stale_takeover_looks_up(tmp_path):
    from datetime import datetime, timedelta, timezone

    from agent.durable_jobs.clock import FrozenClock
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        provider_idempotency_key,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-sub-provider")
    completed = subprocess.run(
        [sys.executable, "-c", _PROVIDER_CHILD, str(store.sqlite_path), job.job_id],
        env=_child_env(),
        cwd=str(Path(__file__).resolve().parents[3]),
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr

    parent_clock = FrozenClock(
        datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=31)
    )
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=parent_clock,
        lease_seconds=30,
    )
    loaded = ledger.get_claim(job.job_id, "create_run")
    assert loaded is not None
    assert loaded.status is EffectStatus.CLAIMED

    key = provider_idempotency_key(job.job_id, "create_run")

    class _Provider:
        def __init__(self) -> None:
            self.create_calls: list[str] = []
            self.lookup_calls: list[str] = []

        def create_run(self, *, idempotency_key: str, job_id: str):
            self.create_calls.append(idempotency_key)

            class _R:
                kind = "lost_response"
                run = None

            return _R()

        def lookup_runs(self, *, idempotency_key: str):
            self.lookup_calls.append(idempotency_key)

            class _Run:
                run_id = "run-after-death"

            return [_Run()]

    provider = _Provider()
    adopted = reconcile_cursor_create(
        ledger,
        provider,
        job_id=job.job_id,
        action_id="create_run",
        origin_platform=job.origin_platform,
        origin_chat_id=job.origin_chat_id,
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )
    assert adopted.status is EffectStatus.ADOPTED
    assert adopted.provider_run_id == "run-after-death"
    assert provider.create_calls == []
    assert provider.lookup_calls == [key]


def test_slack_child_process_death_then_stale_takeover_looks_up(tmp_path):
    from datetime import datetime, timedelta, timezone

    from agent.durable_jobs.clock import FrozenClock
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-sub-slack")
    completed = subprocess.run(
        [sys.executable, "-c", _SLACK_CHILD, str(store.sqlite_path), job.job_id],
        env=_child_env(),
        cwd=str(Path(__file__).resolve().parents[3]),
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr

    parent_clock = FrozenClock(
        datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=31)
    )
    ledger = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=parent_clock,
        lease_seconds=30,
    )
    loaded = ledger.get_binding(job.job_id)
    assert loaded is not None
    assert loaded.status is SlackRootStatus.CLAIMED
    client_msg_id = loaded.outbound_client_msg_id

    class _Port:
        def __init__(self) -> None:
            self.posts: list[str] = []
            self.lookup_calls: list[str] = []

        def post_root(self, **kwargs):
            self.posts.append(kwargs["client_msg_id"])

            class _R:
                kind = "lost_response"
                message_ts = None

            return _R()

        def lookup_by_client_msg_id(self, client_msg_id: str):
            self.lookup_calls.append(client_msg_id)

            class _Posted:
                message_ts = "10.1"

            return [_Posted()]

    port = _Port()
    adopted = deliver_slack_root(ledger, port, job_id=job.job_id)
    assert adopted.status is SlackRootStatus.ADOPTED
    assert adopted.delivered_message_ts == "10.1"
    assert port.posts == []
    assert port.lookup_calls == [client_msg_id]


def test_provider_child_death_empty_then_delayed_visibility_adopts(tmp_path):
    """Crash/restart: first empty lookup stays non-terminal; later unique adopt."""
    from datetime import datetime, timedelta, timezone

    from agent.durable_jobs.clock import FrozenClock
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-sub-delay-provider")
    completed = subprocess.run(
        [sys.executable, "-c", _PROVIDER_CHILD, str(store.sqlite_path), job.job_id],
        env=_child_env(),
        cwd=str(Path(__file__).resolve().parents[3]),
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr

    parent_clock = FrozenClock(
        datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=31)
    )
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=parent_clock,
        lease_seconds=30,
    )

    class _Delayed:
        def __init__(self) -> None:
            self.visible = False
            self.create_calls: list[str] = []
            self.lookup_calls: list[str] = []

        def create_run(self, *, idempotency_key: str, job_id: str):
            self.create_calls.append(idempotency_key)

            class _R:
                kind = "lost_response"
                run = None

            return _R()

        def lookup_runs(self, *, idempotency_key: str):
            self.lookup_calls.append(idempotency_key)
            if not self.visible:
                return []

            class _Run:
                run_id = "run-after-delay"

            return [_Run()]

    provider = _Delayed()
    kwargs = dict(
        job_id=job.job_id,
        action_id="create_run",
        origin_platform=job.origin_platform,
        origin_chat_id=job.origin_chat_id,
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )
    empty = reconcile_cursor_create(ledger, provider, **kwargs)
    assert empty.status is EffectStatus.RECOVERING
    assert empty.unknown_reason is None
    assert provider.create_calls == []
    provider.visible = True
    adopted = reconcile_cursor_create(
        ledger, provider, owner_token=empty.claim_owner_token, **kwargs
    )
    assert adopted.status is EffectStatus.ADOPTED
    assert adopted.provider_run_id == "run-after-delay"
    assert provider.create_calls == []


def test_slack_child_death_empty_then_delayed_visibility_adopts(tmp_path):
    from datetime import datetime, timedelta, timezone

    from agent.durable_jobs.clock import FrozenClock
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-sub-delay-slack")
    completed = subprocess.run(
        [sys.executable, "-c", _SLACK_CHILD, str(store.sqlite_path), job.job_id],
        env=_child_env(),
        cwd=str(Path(__file__).resolve().parents[3]),
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr

    parent_clock = FrozenClock(
        datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=31)
    )
    ledger = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=parent_clock,
        lease_seconds=30,
    )

    class _Delayed:
        def __init__(self) -> None:
            self.visible = False
            self.posts: list[str] = []
            self.lookup_calls: list[str] = []

        def post_root(self, **kwargs):
            self.posts.append(kwargs["client_msg_id"])

            class _R:
                kind = "lost_response"
                message_ts = None

            return _R()

        def lookup_by_client_msg_id(self, client_msg_id: str):
            self.lookup_calls.append(client_msg_id)
            if not self.visible:
                return []

            class _Posted:
                message_ts = "10.8"

            return [_Posted()]

    port = _Delayed()
    empty = deliver_slack_root(ledger, port, job_id=job.job_id)
    assert empty.status is SlackRootStatus.RECOVERING
    assert empty.unknown_reason is None
    assert port.posts == []
    port.visible = True
    adopted = deliver_slack_root(
        ledger, port, job_id=job.job_id, owner_token=empty.claim_owner_token
    )
    assert adopted.status is SlackRootStatus.ADOPTED
    assert adopted.delivered_message_ts == "10.8"
    assert port.posts == []


def test_provider_recovering_child_token_is_not_caller_authority(tmp_path):
    """Crash left RECOVERING: parent must not spend the child's persisted token."""
    from datetime import datetime, timedelta, timezone

    from agent.durable_jobs.clock import FrozenClock
    from agent.durable_jobs.effects import (
        EffectStatus,
        ProviderEffectLedger,
        reconcile_cursor_create,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-sub-recovering-provider")
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            _PROVIDER_RECOVERING_CHILD,
            str(store.sqlite_path),
            job.job_id,
        ],
        env=_child_env(),
        cwd=str(Path(__file__).resolve().parents[3]),
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr

    parent_clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger = ProviderEffectLedger(
        sqlite_path=store.sqlite_path,
        now_fn=parent_clock,
        lease_seconds=30,
    )
    loaded = ledger.get_claim(job.job_id, "create_run")
    assert loaded is not None
    assert loaded.status is EffectStatus.RECOVERING
    child_token = loaded.claim_owner_token
    attempts = loaded.recovery_attempt_count
    kwargs = dict(
        job_id=job.job_id,
        action_id="create_run",
        origin_platform=job.origin_platform,
        origin_chat_id=job.origin_chat_id,
        origin_root_thread_id=job.origin_root_thread_id,
        candidate_id="cand-1",
        candidate_version="v1",
    )

    class _Empty:
        def __init__(self) -> None:
            self.create_calls: list[str] = []
            self.lookup_calls: list[str] = []

        def create_run(self, *, idempotency_key: str, job_id: str):
            self.create_calls.append(idempotency_key)

            class _R:
                kind = "lost_response"
                run = None

            return _R()

        def lookup_runs(self, *, idempotency_key: str):
            self.lookup_calls.append(idempotency_key)
            return []

    empty_provider = _Empty()
    polled = reconcile_cursor_create(ledger, empty_provider, **kwargs)
    assert polled.status is EffectStatus.RECOVERING
    assert empty_provider.lookup_calls == []
    assert empty_provider.create_calls == []
    assert polled.recovery_attempt_count == attempts
    assert polled.claim_owner_token == child_token

    parent_clock.advance(31)

    class _Found:
        def __init__(self) -> None:
            self.create_calls: list[str] = []
            self.lookup_calls: list[str] = []

        def create_run(self, *, idempotency_key: str, job_id: str):
            self.create_calls.append(idempotency_key)

            class _R:
                kind = "lost_response"
                run = None

            return _R()

        def lookup_runs(self, *, idempotency_key: str):
            self.lookup_calls.append(idempotency_key)

            class _Run:
                run_id = "run-after-recovering-death"

            return [_Run()]

    found = _Found()
    adopted = reconcile_cursor_create(ledger, found, **kwargs)
    assert adopted.status is EffectStatus.ADOPTED
    assert adopted.provider_run_id == "run-after-recovering-death"
    assert adopted.claim_owner_token != child_token
    assert found.create_calls == []
    assert found.lookup_calls != []


def test_slack_recovering_child_token_is_not_caller_authority(tmp_path):
    from datetime import datetime, timezone

    from agent.durable_jobs.clock import FrozenClock
    from agent.durable_jobs.slack_contract import (
        SlackBindingLedger,
        SlackRootStatus,
        deliver_slack_root,
    )

    store, job = _make_job(tmp_path, idempotency_key="idem-sub-recovering-slack")
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            _SLACK_RECOVERING_CHILD,
            str(store.sqlite_path),
            job.job_id,
        ],
        env=_child_env(),
        cwd=str(Path(__file__).resolve().parents[3]),
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr

    parent_clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    ledger = SlackBindingLedger(
        sqlite_path=store.sqlite_path,
        now_fn=parent_clock,
        lease_seconds=30,
    )
    loaded = ledger.get_binding(job.job_id)
    assert loaded is not None
    assert loaded.status is SlackRootStatus.RECOVERING
    child_token = loaded.claim_owner_token
    attempts = loaded.recovery_attempt_count

    class _Empty:
        def __init__(self) -> None:
            self.posts: list[str] = []
            self.lookup_calls: list[str] = []

        def post_root(self, **kwargs):
            self.posts.append(kwargs["client_msg_id"])

            class _R:
                kind = "lost_response"
                message_ts = None

            return _R()

        def lookup_by_client_msg_id(self, client_msg_id: str):
            self.lookup_calls.append(client_msg_id)
            return []

    empty_port = _Empty()
    polled = deliver_slack_root(ledger, empty_port, job_id=job.job_id)
    assert polled.status is SlackRootStatus.RECOVERING
    assert empty_port.lookup_calls == []
    assert empty_port.posts == []
    assert polled.recovery_attempt_count == attempts
    assert polled.claim_owner_token == child_token

    parent_clock.advance(31)

    class _Found:
        def __init__(self) -> None:
            self.posts: list[str] = []
            self.lookup_calls: list[str] = []

        def post_root(self, **kwargs):
            self.posts.append(kwargs["client_msg_id"])

            class _R:
                kind = "lost_response"
                message_ts = None

            return _R()

        def lookup_by_client_msg_id(self, client_msg_id: str):
            self.lookup_calls.append(client_msg_id)

            class _Posted:
                message_ts = "10.7"

            return [_Posted()]

    found = _Found()
    adopted = deliver_slack_root(ledger, found, job_id=job.job_id)
    assert adopted.status is SlackRootStatus.ADOPTED
    assert adopted.delivered_message_ts == "10.7"
    assert adopted.claim_owner_token != child_token
    assert found.posts == []
    assert found.lookup_calls != []
