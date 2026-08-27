from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


class FailIfCalledPorts:
    def __getattr__(self, name):
        raise AssertionError(f"external port {name} was called")


class DisposableDurablePorts:
    """SQLite-only projections/session boundary used by the runtime E2E."""

    def __init__(self, path: Path, *, crash_after_child_once: bool = False):
        self.path = path
        self.crash_after_child_once = crash_after_child_once
        self.external = FailIfCalledPorts()
        with sqlite3.connect(path) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS projection(
                  issue TEXT PRIMARY KEY, canonical TEXT NOT NULL, receipt TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS effects(
                  effect_key TEXT PRIMARY KEY, kind TEXT NOT NULL, value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS adapter_receipts(
                  effect_key TEXT PRIMARY KEY, receipt BLOB NOT NULL
                );
                """
            )

    def _put(self, key, kind, value):
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                "INSERT INTO effects VALUES(?,?,?) ON CONFLICT(effect_key) DO NOTHING",
                (key, kind, value),
            )
            row = connection.execute(
                "SELECT value FROM effects WHERE effect_key=?", (key,)
            ).fetchone()
        return str(row[0])

    def upsert_handoff(self, *, issue, canonical, idempotency_key):
        receipt = f"linear:{issue}"
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                "INSERT INTO projection VALUES(?,?,?) ON CONFLICT(issue) DO NOTHING",
                (issue, canonical, receipt),
            )
        return receipt

    def read_handoff(self, *, issue, idempotency_key):
        with sqlite3.connect(self.path) as connection:
            row = connection.execute(
                "SELECT canonical FROM projection WHERE issue=?", (issue,)
            ).fetchone()
        return str(row[0])

    def post_handoff_receipt(self, *, handoff_id, resume_pointer, idempotency_key):
        receipt = f"slack:{handoff_id}".encode("utf-8")
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                "INSERT INTO adapter_receipts VALUES(?,?) ON CONFLICT(effect_key) DO NOTHING",
                (idempotency_key, receipt),
            )
        return self._put(idempotency_key, "slack-shadow", receipt.decode("utf-8"))

    def authoritative_receipt_bytes(self, effect_key):
        with sqlite3.connect(self.path) as connection:
            row = connection.execute(
                "SELECT receipt FROM adapter_receipts WHERE effect_key=?", (effect_key,)
            ).fetchone()
        return bytes(row[0])

    def find_or_create_child(self, *, parent_session_id, handoff_id, idempotency_key):
        child = self._put(idempotency_key, "child-shadow", "offline-child-1")
        if self.crash_after_child_once:
            self.crash_after_child_once = False
            raise ConnectionError("offline crash window")
        return child

    def inject_handoff(self, *, child_session_id, canonical, idempotency_key):
        self._put(idempotency_key, "inject-shadow", child_session_id)

    def start_first_turn(self, *, child_session_id, next_action, idempotency_key):
        self._put(idempotency_key, "first-turn-shadow", next_action)

    def counts(self):
        with sqlite3.connect(self.path) as connection:
            return dict(
                connection.execute(
                    "SELECT kind,COUNT(*) FROM effects GROUP BY kind"
                ).fetchall()
            )


def _lane(tmp_path):
    from agent.durable_jobs.config import DurableJobsConfig, DurableJobsIdentityBinding
    from agent.durable_jobs.lane import DurableLaneService
    from agent.durable_jobs.store import DurableJobStore

    path = tmp_path / "runtime-ledger.sqlite3"
    store = DurableJobStore(path)
    job = store.create_job(
        origin_platform="offline-test",
        origin_chat_id="offline",
        origin_root_thread_id="offline-root",
        objective="ENG-122 runtime ingress",
        repository_identity="github.com/nous/hermes",
        frozen_baseline_sha="a" * 40,
        idempotency_key="job:ENG-122:runtime",
    )
    lane = DurableLaneService(
        DurableJobsConfig(
            enabled=True,
            dispatch_enabled=False,
            backend="sqlite",
            sqlite_path=path,
            checkpoint_sqlite_path=tmp_path / "runtime-checkpoints.sqlite3",
            identity_binding=DurableJobsIdentityBinding(
                workspace_id="offline-test",
                repository_identity="github.com/nous/hermes",
            ),
        ),
        store=store,
    )
    lane.bind_slack(
        job_id=job.job_id,
        workspace_id="offline-test",
        channel_id="offline",
        root_thread_ts="offline-root",
        candidate_id="eng-122-runtime",
        candidate_version="offline-v1",
    )
    return lane, job, path


def _request(job_id):
    from agent.durable_jobs.session_handoff import SessionHandoff
    from agent.orchestration.session_handoff_runtime import SessionHandoffIngress

    return SessionHandoffIngress(
        job_id=job_id,
        parent_session_id="parent-offline-1",
        handoff=SessionHandoff(
            handoff_id="handoff-runtime-1",
            idempotency_key="handoff:ENG-122:runtime-1",
            project="Hermes",
            issue="ENG-122",
            goal="Exercise offline runtime boundary",
            verified=("offline fixture",),
            pending=(),
            remaining=("production gates",),
            blockers=(),
            user_action="none",
            repository="github.com/nous/hermes",
            worktree="disposable",
            branch="offline-test",
            exact_sha="a" * 40,
            diff_fingerprint="sha256:offline",
            test_evidence=("runtime e2e",),
            risk_gates=("offline_only",),
            forbidden_actions=("network", "provider", "gateway"),
            resume_pointer=f"durable-job://{job_id}/handoffs/handoff-runtime-1",
            next_action="Continue first offline turn",
        ),
        provider="openai-codex",
        model="gpt-5.6-sol",
        used_tokens=46,
        context_tokens=100,
    )


def _runtime(lane, ports, request, policy):
    from agent.orchestration.session_handoff_runtime import SessionHandoffRuntime

    return SessionHandoffRuntime(
        lane=lane,
        linear=ports,
        slack=ports,
        sessions=ports,
        request=request,
        waypoint_policy=policy,
        enabled=True,
        mode="offline_shadow_test",
    )


def _agent():
    agent = SimpleNamespace(
        _try_refresh_env_client_credentials=MagicMock(),
        _last_compaction_in_place=False,
        _last_compression_attempt_recorded=False,
        _last_compression_attempt_in_place=None,
        _delegate_depth=0,
        platform="cli",
        session_id="parent-offline-1",
    )
    return agent


def _turn_context():
    return SimpleNamespace(
        user_message="ordinary",
        original_user_message="ordinary",
        messages=[{"role": "user", "content": "ordinary"}],
        conversation_history=[],
        active_system_prompt="system",
        effective_task_id="turn-task",
        turn_id="turn-1",
        current_turn_user_idx=0,
        should_review_memory=False,
        plugin_user_context=None,
        ext_prefetch_cache=None,
    )


def test_default_runtime_ingress_is_no_touch(tmp_path):
    from agent import conversation_loop

    sentinel = tmp_path / "must-not-open.sqlite3"
    sentinel.write_bytes(b"byte-for-byte sentinel")
    agent = _agent()

    with patch(
        "agent.orchestration.service.maybe_orchestrate_turn",
        side_effect=RuntimeError("stop before config/client work"),
    ), patch(
        "agent.conversation_loop.build_turn_context",
        side_effect=RuntimeError("stop after ingress"),
    ), pytest.raises(RuntimeError, match="stop after ingress"):
        conversation_loop.run_conversation(agent, "ordinary turn")

    assert sentinel.read_bytes() == b"byte-for-byte sentinel"
    assert not hasattr(agent, "_last_session_handoff_result")


def test_eng110_runtime_receipt_is_explicitly_offline():
    receipt = json.loads(
        Path("docs/eng-110-session-handoff-runtime-receipt.json").read_text(
            encoding="utf-8"
        )
    )
    assert receipt["schema"] == "hermes.eng110-session-handoff-runtime-offline-receipt"
    assert receipt["base_sha"] == "5786fce1a590eb46da083b991ba005adf19df214"
    assert receipt["authority"] == {
        "parent_session_bound": True,
        "current_turn_bound": True,
        "one_shot": True,
        "terminal_orchestration_zero_effects": True,
    }
    assert bytes.fromhex(receipt["authoritative_offline_adapter_receipt_hex"]) == (
        b"slack:handoff-runtime-1"
    )
    assert receipt["receipt_scope"] == "injected_disposable_adapter_only"
    assert receipt["live_effects"] is False
    assert receipt["activation_approved"] is False
    assert receipt["gate"] == {
        "enabled": True,
        "mode": "offline_shadow_test",
        "source": "injected_test_runtime_only",
    }
    assert receipt["forbidden_effects_observed"] == []


def test_real_client_attachment_runs_ingress_checkpoint_resume_and_first_turn_once(
    tmp_path,
):
    from agent import conversation_loop
    from agent.durable_jobs.session_handoff import SemanticWaypoint, SessionHandoffLedger
    from run_agent import AIAgent

    lane, job, ledger_path = _lane(tmp_path)
    ports_path = tmp_path / "offline-projections.sqlite3"
    ports = DisposableDurablePorts(ports_path)
    request = _request(job.job_id)
    policy_calls = []

    def safe_policy(agent, message, bound_request):
        policy_calls.append((agent.session_id, message, bound_request.job_id))
        return SemanticWaypoint(verified=True)

    first_agent = _agent()
    AIAgent.attach_offline_session_handoff_runtime(
        first_agent,
        _runtime(lane, ports, request, safe_policy),
        enabled=True,
    )

    def stop_after_ingress(*args, **kwargs):
        first_agent._current_turn_id = "turn-offline-1"
        return SimpleNamespace(
            user_message="continue safely",
            original_user_message="continue safely",
            messages=[{"role": "user", "content": "continue safely"}],
            conversation_history=[],
            active_system_prompt="system",
            effective_task_id="turn-offline-1",
            turn_id="turn-offline-1",
            current_turn_user_idx=0,
            should_review_memory=False,
            plugin_user_context=None,
            ext_prefetch_cache=None,
        )

    completed = SimpleNamespace(
        legacy_continue=False,
        pending_worker=False,
        acted=True,
        response={"status": "ok", "final_response": "done", "completed": True},
    )
    pending = SimpleNamespace(
        legacy_continue=False,
        pending_worker=True,
        acted=False,
        response=None,
    )

    with patch(
        "agent.orchestration.service.maybe_orchestrate_turn",
        return_value=pending,
    ), patch(
        "agent.conversation_loop.build_turn_context", side_effect=stop_after_ingress
    ), patch(
        "agent.orchestration.service.complete_active_orchestration", return_value=completed
    ), patch("agent.conversation_loop.finalize_turn", return_value=completed.response):
        conversation_loop.run_conversation(first_agent, "continue safely")

    assert first_agent._last_session_handoff_result.stage == "COMPLETE"
    assert policy_calls == [("parent-offline-1", "continue safely", job.job_id)]
    assert ports.counts() == {
        "child-shadow": 1,
        "first-turn-shadow": 1,
        "inject-shadow": 1,
        "slack-shadow": 1,
    }
    assert SessionHandoffLedger(ledger_path).get(
        job.job_id, request.handoff.handoff_id
    ).checkpoint_stage == "COMPLETE"

    restarted_agent = _agent()
    restarted_ports = DisposableDurablePorts(ports_path)
    AIAgent.attach_offline_session_handoff_runtime(
        restarted_agent,
        _runtime(lane, restarted_ports, request, safe_policy),
        enabled=True,
    )

    def restarted_context(*args, **kwargs):
        restarted_agent._current_turn_id = "turn-offline-2"
        value = stop_after_ingress(*args, **kwargs)
        value.turn_id = "turn-offline-2"
        value.effective_task_id = "turn-offline-2"
        return value

    with patch(
        "agent.orchestration.service.maybe_orchestrate_turn",
        return_value=pending,
    ), patch(
        "agent.conversation_loop.build_turn_context", side_effect=restarted_context
    ), patch(
        "agent.orchestration.service.complete_active_orchestration", return_value=completed
    ), patch("agent.conversation_loop.finalize_turn", return_value=completed.response):
        conversation_loop.run_conversation(restarted_agent, "continue safely")

    assert restarted_agent._last_session_handoff_result.stage == "COMPLETE"
    assert restarted_ports.counts() == ports.counts()


def test_runtime_crash_requires_reconciliation_and_manual_resume_without_duplicates(
    tmp_path,
):
    from agent.durable_jobs.session_handoff import (
        SemanticWaypoint,
        SessionHandoffLedger,
    )

    lane, job, ledger_path = _lane(tmp_path)
    ports_path = tmp_path / "crash-projections.sqlite3"
    ports = DisposableDurablePorts(ports_path, crash_after_child_once=True)
    request = _request(job.job_id)
    runtime = _runtime(
        lane,
        ports,
        request,
        lambda *_: SemanticWaypoint(verified=True),
    )

    with pytest.raises(ConnectionError, match="offline crash window"):
        runtime.ingress(_agent(), "crash")

    ledger = SessionHandoffLedger(ledger_path)
    state = ledger.get(job.job_id, request.handoff.handoff_id)
    claim = ledger.get_effect(job.job_id, request.handoff.handoff_id, "CHILD_CREATE")
    assert state.stage == "FAILED_CLOSED"
    assert state.checkpoint_stage == "SLACK_RECEIPTED"
    assert claim is not None and claim.status == "IN_FLIGHT"

    from agent.durable_jobs.session_handoff import SessionHandoffConfig

    enabled_config = replace(
        SessionHandoffConfig.default(), enabled=True, shadow=False
    )
    lane.reconcile_session_handoff_effect(
        job_id=job.job_id,
        handoff_id=request.handoff.handoff_id,
        effect_name="CHILD_CREATE",
        outcome="APPLIED",
        receipt="offline-child-1",
        expected_owner_token=claim.owner_token,
        expected_generation=claim.generation,
        dead_owner_verified=True,
        handoff_config=enabled_config,
    )
    resumed = _runtime(
        lane,
        DisposableDurablePorts(ports_path),
        replace(request, manual_resume=True),
        lambda *_: SemanticWaypoint(verified=True),
    ).ingress(_agent(), "manual resume")

    assert resumed.stage == "COMPLETE"
    assert DisposableDurablePorts(ports_path).counts() == {
        "child-shadow": 1,
        "first-turn-shadow": 1,
        "inject-shadow": 1,
        "slack-shadow": 1,
    }


def test_initialized_agent_offline_e2e_crosses_prologue_model_and_finalization(
    tmp_path, monkeypatch
):
    from agent.durable_jobs.session_handoff import SemanticWaypoint, SessionHandoffLedger
    from run_agent import AIAgent

    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    agent = AIAgent(
        base_url="http://127.0.0.1:9/v1",
        api_key="offline-test-key",
        provider="openai",
        model="offline-model",
        session_id="parent-offline-1",
        platform="cli",
        quiet_mode=True,
        max_iterations=1,
        skip_context_files=True,
        skip_memory=True,
        skip_background_review=True,
    )
    fake_client = MagicMock()
    fake_client.chat.completions.create.return_value = SimpleNamespace(
        model="offline-model",
        choices=[
            SimpleNamespace(
                finish_reason="stop",
                message=SimpleNamespace(
                    content="offline final",
                    tool_calls=None,
                    reasoning=None,
                    reasoning_content=None,
                ),
            )
        ],
        usage=SimpleNamespace(
            prompt_tokens=7,
            completion_tokens=2,
            total_tokens=9,
            prompt_tokens_details=None,
            completion_tokens_details=None,
        ),
    )
    agent.client = fake_client
    agent._disable_streaming = True

    lane, job, ledger_path = _lane(tmp_path)
    ports = DisposableDurablePorts(tmp_path / "initialized-agent-ports.sqlite3")
    request = _request(job.job_id)
    agent.attach_offline_session_handoff_runtime(
        _runtime(
            lane,
            ports,
            request,
            lambda *_: SemanticWaypoint(verified=True),
        ),
        enabled=True,
    )

    def forbidden(*_args, **_kwargs):
        raise AssertionError("network/socket/live service must not be called")

    with patch(
        "agent.orchestration.service.load_config",
        return_value={"orchestration": {"enabled": False, "mode": "off"}},
    ), patch("socket.create_connection", side_effect=forbidden), patch(
        "socket.socket.connect", side_effect=forbidden
    ):
        result = agent.run_conversation("continue safely offline")
        with pytest.raises(RuntimeError, match="already consumed"):
            agent.run_conversation("unrelated stale second turn")

    assert result["final_response"] == "offline final"
    assert agent._current_turn_id
    assert agent._last_session_handoff_result.stage == "COMPLETE"
    assert ports.counts() == {
        "child-shadow": 1,
        "first-turn-shadow": 1,
        "inject-shadow": 1,
        "slack-shadow": 1,
    }
    assert ports.authoritative_receipt_bytes(request.handoff.idempotency_key + ":slack") == (
        b"slack:handoff-runtime-1"
    )
    state = SessionHandoffLedger(ledger_path).get(
        job.job_id, request.handoff.handoff_id
    )
    assert state.stage == "COMPLETE"
    assert state.checkpoint_stage == "COMPLETE"
    assert fake_client.chat.completions.create.call_count == 1


def test_runtime_unsafe_waypoint_reaches_no_projection_or_child_port(tmp_path):
    from agent.durable_jobs.session_handoff import (
        SemanticWaypoint,
        SessionHandoffLedger,
        UnsafeHandoffWaypoint,
    )

    lane, job, ledger_path = _lane(tmp_path)
    request = _request(job.job_id)
    ports = FailIfCalledPorts()
    runtime = _runtime(
        lane,
        ports,
        request,
        lambda *_: SemanticWaypoint(verified=True, tool_active=True),
    )

    with pytest.raises(UnsafeHandoffWaypoint):
        runtime.ingress(_agent(), "unsafe boundary")
    assert SessionHandoffLedger(ledger_path).get(
        job.job_id, request.handoff.handoff_id
    ) is None


@pytest.mark.parametrize("status", ["REQUIRE_APPROVAL", "ASK_USER", "BLOCKED"])
def test_terminal_orchestration_has_zero_handoff_effects(tmp_path, status):
    from agent import conversation_loop
    from agent.durable_jobs.session_handoff import SemanticWaypoint, SessionHandoffLedger
    from agent.orchestration.service import OrchestrationTurnResult
    from run_agent import AIAgent

    lane, job, ledger_path = _lane(tmp_path)
    request = _request(job.job_id)
    agent = _agent()
    AIAgent.attach_offline_session_handoff_runtime(
        agent,
        _runtime(
            lane,
            FailIfCalledPorts(),
            request,
            lambda *_: SemanticWaypoint(verified=True),
        ),
        enabled=True,
    )
    terminal = OrchestrationTurnResult(
        mode="active",
        acted=True,
        legacy_continue=False,
        task_spec=None,
        decision=None,
        compiled=None,
        trace=None,
        worker_result=None,
        response={"status": status, "final_response": status, "completed": False},
        guard_reason_codes=(),
    )
    context = SimpleNamespace(
        user_message="ordinary",
        original_user_message="ordinary",
        messages=[{"role": "user", "content": "ordinary"}],
        conversation_history=[],
        active_system_prompt="system",
        effective_task_id="turn-task",
        turn_id="turn-1",
        current_turn_user_idx=0,
        should_review_memory=False,
        plugin_user_context=None,
        ext_prefetch_cache=None,
    )

    with patch("agent.orchestration.service.maybe_orchestrate_turn", return_value=terminal), patch(
        "agent.conversation_loop.build_turn_context", return_value=context
    ), patch("agent.conversation_loop.finalize_turn", return_value=terminal.response):
        result = conversation_loop.run_conversation(agent, "ordinary")

    assert result["status"] == status
    assert SessionHandoffLedger(ledger_path).get(job.job_id, request.handoff.handoff_id) is None
    assert not hasattr(agent, "_last_session_handoff_result")


@pytest.mark.parametrize("status", ["REQUIRE_APPROVAL", "ASK_USER", "BLOCKED"])
def test_pending_worker_completion_terminal_denies_handoff_before_effects(
    tmp_path, status
):
    from agent import conversation_loop
    from agent.durable_jobs.session_handoff import SemanticWaypoint, SessionHandoffLedger
    from run_agent import AIAgent

    lane, job, ledger_path = _lane(tmp_path)
    request = _request(job.job_id)
    agent = _agent()
    AIAgent.attach_offline_session_handoff_runtime(
        agent,
        _runtime(
            lane,
            FailIfCalledPorts(),
            request,
            lambda *_: SemanticWaypoint(verified=True),
        ),
        enabled=True,
    )
    pending = SimpleNamespace(
        legacy_continue=False, pending_worker=True, acted=False, response=None
    )
    terminal = SimpleNamespace(
        legacy_continue=False,
        pending_worker=False,
        acted=True,
        response={"status": status, "final_response": status, "completed": False},
    )

    with patch(
        "agent.orchestration.service.maybe_orchestrate_turn", return_value=pending
    ), patch(
        "agent.conversation_loop.build_turn_context", return_value=_turn_context()
    ), patch(
        "agent.orchestration.service.complete_active_orchestration",
        return_value=terminal,
    ) as complete, patch(
        "agent.conversation_loop.finalize_turn", return_value=terminal.response
    ):
        result = conversation_loop.run_conversation(agent, "ordinary")

    assert result["status"] == status
    complete.assert_called_once()
    assert agent._session_handoff_authority["consumed"] is True
    assert SessionHandoffLedger(ledger_path).get(
        job.job_id, request.handoff.handoff_id
    ) is None
    assert not hasattr(agent, "_last_session_handoff_result")


def test_pending_worker_completion_exception_denies_handoff_before_effects(tmp_path):
    from agent import conversation_loop
    from agent.durable_jobs.session_handoff import SemanticWaypoint, SessionHandoffLedger
    from run_agent import AIAgent

    lane, job, ledger_path = _lane(tmp_path)
    request = _request(job.job_id)
    agent = _agent()
    AIAgent.attach_offline_session_handoff_runtime(
        agent,
        _runtime(
            lane,
            FailIfCalledPorts(),
            request,
            lambda *_: SemanticWaypoint(verified=True),
        ),
        enabled=True,
    )
    pending = SimpleNamespace(
        legacy_continue=False, pending_worker=True, acted=False, response=None
    )
    blocked = SimpleNamespace(
        legacy_continue=False,
        pending_worker=False,
        acted=True,
        response={"status": "BLOCKED", "final_response": "blocked", "completed": False},
    )

    with patch(
        "agent.orchestration.service.maybe_orchestrate_turn", return_value=pending
    ), patch(
        "agent.conversation_loop.build_turn_context", return_value=_turn_context()
    ), patch(
        "agent.orchestration.service.complete_active_orchestration",
        side_effect=RuntimeError("completion exploded"),
    ), patch(
        "agent.orchestration.service._fail_closed_active_error", return_value=blocked
    ), patch(
        "agent.conversation_loop.finalize_turn", return_value=blocked.response
    ):
        result = conversation_loop.run_conversation(agent, "ordinary")

    assert result["status"] == "BLOCKED"
    assert agent._session_handoff_authority["consumed"] is True
    assert SessionHandoffLedger(ledger_path).get(
        job.job_id, request.handoff.handoff_id
    ) is None
    assert not hasattr(agent, "_last_session_handoff_result")


def test_orchestration_boundary_exception_denies_attached_handoff(tmp_path):
    from agent import conversation_loop
    from agent.durable_jobs.session_handoff import SemanticWaypoint, SessionHandoffLedger
    from agent.orchestration.session_handoff_runtime import (
        discard_attached_session_handoff_ingress,
    )
    from run_agent import AIAgent

    lane, job, ledger_path = _lane(tmp_path)
    request = _request(job.job_id)
    agent = _agent()
    AIAgent.attach_offline_session_handoff_runtime(
        agent,
        _runtime(
            lane,
            FailIfCalledPorts(),
            request,
            lambda *_: SemanticWaypoint(verified=True),
        ),
        enabled=True,
    )

    def consume_then_stop(bound_agent, *, turn_id):
        discard_attached_session_handoff_ingress(bound_agent, turn_id=turn_id)
        raise RuntimeError("stop after denied handoff boundary")

    with patch(
        "agent.orchestration.service.maybe_orchestrate_turn",
        side_effect=RuntimeError("orchestration exploded"),
    ), patch(
        "agent.conversation_loop.build_turn_context", return_value=_turn_context()
    ), patch(
        "agent.orchestration.session_handoff_runtime.discard_attached_session_handoff_ingress",
        side_effect=consume_then_stop,
    ), pytest.raises(RuntimeError, match="stop after denied handoff boundary"):
        conversation_loop.run_conversation(agent, "ordinary")

    assert agent._session_handoff_authority["consumed"] is True
    assert SessionHandoffLedger(ledger_path).get(
        job.job_id, request.handoff.handoff_id
    ) is None
    assert not hasattr(agent, "_last_session_handoff_result")


def test_parent_mismatch_and_stale_second_turn_fail_before_effects(tmp_path):
    from agent.durable_jobs.session_handoff import SemanticWaypoint, SessionHandoffLedger
    from agent.orchestration.session_handoff_runtime import run_attached_session_handoff_ingress
    from run_agent import AIAgent

    lane, job, ledger_path = _lane(tmp_path)
    request = _request(job.job_id)
    agent = _agent()
    ports = DisposableDurablePorts(tmp_path / "authority-ports.sqlite3")
    runtime = _runtime(
        lane,
        ports,
        request,
        lambda *_: SemanticWaypoint(verified=True),
    )
    AIAgent.attach_offline_session_handoff_runtime(agent, runtime, enabled=True)

    agent.session_id = "wrong-parent"
    agent._current_turn_id = "turn-1"
    with pytest.raises(ValueError, match="parent_session_id"):
        run_attached_session_handoff_ingress(agent, "first", turn_id="turn-1")
    assert SessionHandoffLedger(ledger_path).get(job.job_id, request.handoff.handoff_id) is None

    agent.session_id = request.parent_session_id
    run_attached_session_handoff_ingress(agent, "first", turn_id="turn-1")
    agent._current_turn_id = "turn-2"
    with pytest.raises(RuntimeError, match="current turn|consumed|stale"):
        run_attached_session_handoff_ingress(agent, "second", turn_id="turn-2")
    assert ports.counts() == {
        "child-shadow": 1,
        "first-turn-shadow": 1,
        "inject-shadow": 1,
        "slack-shadow": 1,
    }


@pytest.mark.parametrize("enabled", [False, 1, "true", None])
def test_runtime_and_client_require_literal_enablement(tmp_path, enabled):
    from agent.durable_jobs.session_handoff import SemanticWaypoint
    from agent.orchestration.session_handoff_runtime import (
        SessionHandoffRuntime,
        SessionHandoffRuntimeDisabled,
    )

    sentinel = tmp_path / "untouched.sqlite3"
    sentinel.write_bytes(b"untouched")
    with pytest.raises(SessionHandoffRuntimeDisabled):
        SessionHandoffRuntime(
            lane=FailIfCalledPorts(),
            linear=FailIfCalledPorts(),
            slack=FailIfCalledPorts(),
            sessions=FailIfCalledPorts(),
            request=MagicMock(manual_resume=False),
            waypoint_policy=lambda *_: SemanticWaypoint(verified=True),
            enabled=enabled,
            mode="offline_shadow_test",
        )
    assert sentinel.read_bytes() == b"untouched"
