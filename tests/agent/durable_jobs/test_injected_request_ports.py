"""ENG-58: injected Cursor Cloud / Slack request-port adapters.

These tests are the RED proof that real request ports are missing (or cannot
run isolated), then the GREEN contract once they exist.

Adapters must wrap an *injected* client that already matches the repository's
existing seams:

* Cursor Cloud: ``create_agent`` / ``get_agent`` / ``get_run``
* Slack: ``chat_postMessage`` / ``conversations_replies``

They must never construct HTTP/SDK clients from config flags or env, never
log credential values, and must fail closed (zero client calls) on identity,
secret-ref, or correlation mismatch.
"""

from __future__ import annotations

import inspect
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from agent.durable_jobs.injected_transports import (
    CursorCloudInjectedTransport,
    SlackInjectedTransport,
)
from agent.durable_jobs.lane import LaneClosedError
from agent.durable_jobs.production_binding import (
    bind_production_transports,
    production_attach_kwargs,
)
from tests.agent.durable_jobs.package2_support import bind_runtime_secret_env

_SECRET_CURSOR = "CURSOR_API_KEY"
_SECRET_SLACK = "SLACK_BOT_TOKEN"
WORKSPACE = "T1"
REPO = "github.com/example/repo"
CHANNEL = "C123"
THREAD = "111.222"
CURSOR_KEY = "cursor-idem-1"
SLACK_KEY = "slack-idem-1"
CURSOR_SECRET_VALUE = "cursor-secret-token-value"
SLACK_SECRET_VALUE = "xoxb-super-secret-token"


def _cursor_ids(key: str = CURSOR_KEY):
    from agent.durable_jobs.cursor_cloud import (
        cursor_correlation_agent_id,
        cursor_correlation_name,
    )

    return cursor_correlation_name(key), cursor_correlation_agent_id(key)


class FakeCursorCloudClient:
    """Deterministic stand-in for the existing Cursor Cloud client seam."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.agents: dict[str, dict] = {}
        self.runs: dict[tuple[str, str], dict] = {}
        self.create_error: BaseException | None = None
        self.lookup_error: BaseException | None = None

    def create_agent(self, payload):
        self.calls.append(("create_agent", payload))
        if self.create_error is not None:
            raise self.create_error
        agent_id = payload["agentId"]
        record = {
            "name": payload["name"],
            "agentId": agent_id,
            "id": agent_id,
            "latestRunId": f"run-{agent_id}",
        }
        self.agents[agent_id] = record
        return record

    def get_agent(self, agent_id):
        self.calls.append(("get_agent", agent_id))
        if self.lookup_error is not None:
            raise self.lookup_error
        found = self.agents.get(agent_id)
        if found is None:
            return {"error": {"code": "not_found"}}
        return found

    def get_run(self, agent_id, run_id):
        self.calls.append(("get_run", (agent_id, run_id)))
        found = self.runs.get((agent_id, run_id))
        if found is None:
            return {"id": run_id, "agentId": agent_id, "status": "CREATING"}
        return found


class FakeSlackClient:
    """Deterministic stand-in for SlackAdapter._get_client() WebClient methods."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.messages: list[dict] = []
        self.post_error: BaseException | None = None
        self.lookup_error: BaseException | None = None

    def chat_postMessage(self, **kwargs):
        self.calls.append(("chat_postMessage", dict(kwargs)))
        if self.post_error is not None:
            raise self.post_error
        ts = f"ts-{len(self.messages) + 1}"
        record = {
            "ok": True,
            "ts": ts,
            "channel": kwargs.get("channel"),
            "message": {
                "ts": ts,
                "thread_ts": kwargs.get("thread_ts") or ts,
                "client_msg_id": kwargs.get("client_msg_id"),
                "text": kwargs.get("text"),
            },
        }
        self.messages.append(record["message"])
        return record

    def conversations_replies(self, **kwargs):
        self.calls.append(("conversations_replies", dict(kwargs)))
        if self.lookup_error is not None:
            raise self.lookup_error
        return {"ok": True, "messages": list(self.messages)}


class _ResponseLike:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def _complete(tmp_path: Path, **overrides) -> dict:
    section = {
        "enabled": True,
        "dispatch_enabled": False,
        "backend": "sqlite",
        "sqlite_path": str(tmp_path / "jobs.sqlite"),
        "checkpoint_sqlite_path": str(tmp_path / "checkpoints.sqlite"),
        "cursor_adapter_mode": "injected",
        "slack_adapter_mode": "injected",
        "cursor_secret_ref": _SECRET_CURSOR,
        "slack_secret_ref": _SECRET_SLACK,
        "policy_version": "eng29-matrix-v1",
        "identity_binding": {
            "workspace_id": WORKSPACE,
            "repository_identity": REPO,
        },
    }
    section.update(overrides)
    return {"durable_jobs": section}


def _load_ports():
    from agent.durable_jobs.request_ports import (
        CursorCloudInjectedRequestPort,
        RequestPortClosed,
        RequestPortError,
        RequestPortMismatch,
        RequestPortTimeout,
        SlackInjectedRequestPort,
    )

    return SimpleNamespace(
        CursorCloudInjectedRequestPort=CursorCloudInjectedRequestPort,
        SlackInjectedRequestPort=SlackInjectedRequestPort,
        RequestPortClosed=RequestPortClosed,
        RequestPortError=RequestPortError,
        RequestPortMismatch=RequestPortMismatch,
        RequestPortTimeout=RequestPortTimeout,
    )


def _cursor_port(ports, client, **kwargs):
    return ports.CursorCloudInjectedRequestPort(
        client=client,
        secret_ref=_SECRET_CURSOR,
        workspace_id=WORKSPACE,
        repository_identity=REPO,
        **kwargs,
    )


def _slack_port(ports, client, **kwargs):
    return ports.SlackInjectedRequestPort(
        client=client,
        secret_ref=_SECRET_SLACK,
        workspace_id=WORKSPACE,
        channel_id=CHANNEL,
        repository_identity=REPO,
        root_thread_ts=THREAD,
        **kwargs,
    )


def _cursor_create_payload(key: str = CURSOR_KEY, **extra):
    name, agent_id = _cursor_ids(key)
    payload = {
        "name": name,
        "agentId": agent_id,
        "idempotency_key": key,
        "workspace_id": WORKSPACE,
        "repository_identity": REPO,
    }
    payload.update(extra)
    return payload


def _slack_payload(key: str = SLACK_KEY, **extra):
    payload = {
        "workspace_id": WORKSPACE,
        "channel_id": CHANNEL,
        "root_thread_ts": THREAD,
        "idempotency_key": key,
        "client_msg_id": key,
        "text": "hello",
    }
    payload.update(extra)
    return payload


def test_request_ports_module_exists_and_exports_injected_adapters():
    ports = _load_ports()
    assert inspect.isclass(ports.CursorCloudInjectedRequestPort)
    assert inspect.isclass(ports.SlackInjectedRequestPort)


def test_request_ports_module_does_not_import_live_sdks():
    for name in ("requests", "httpx", "slack_sdk", "slack_bolt", "aiohttp"):
        sys.modules.pop(name, None)
    import agent.durable_jobs.request_ports as rp  # noqa: F401

    assert "requests" not in sys.modules
    assert "httpx" not in sys.modules
    assert "slack_sdk" not in sys.modules
    assert "slack_bolt" not in sys.modules
    assert "aiohttp" not in sys.modules
    for banned in (
        "CursorCloudHttpClient",
        "LiveCursorCloudTransport",
        "SlackSdkClient",
        "LiveSlackTransport",
        "SlackHttpClient",
    ):
        assert not hasattr(rp, banned)


def test_constructing_ports_does_not_open_sockets_or_read_secret_values():
    ports = _load_ports()
    cursor_client = FakeCursorCloudClient()
    slack_client = FakeSlackClient()
    resolver_calls: list[str] = []

    def resolver(name: str) -> str:
        resolver_calls.append(name)
        return CURSOR_SECRET_VALUE if name == _SECRET_CURSOR else SLACK_SECRET_VALUE

    cursor = _cursor_port(ports, cursor_client, credential_resolver=resolver)
    slack = _slack_port(ports, slack_client, credential_resolver=resolver)

    assert cursor_client.calls == []
    assert slack_client.calls == []
    assert resolver_calls == []
    blob = repr(cursor) + repr(slack)
    assert CURSOR_SECRET_VALUE not in blob
    assert SLACK_SECRET_VALUE not in blob


def test_ports_require_injected_client():
    ports = _load_ports()
    with pytest.raises(TypeError):
        ports.CursorCloudInjectedRequestPort(
            secret_ref=_SECRET_CURSOR,
            workspace_id=WORKSPACE,
            repository_identity=REPO,
        )
    with pytest.raises(TypeError):
        ports.SlackInjectedRequestPort(
            secret_ref=_SECRET_SLACK,
            workspace_id=WORKSPACE,
            channel_id=CHANNEL,
        )


def test_cursor_create_lookup_status_success():
    ports = _load_ports()
    client = FakeCursorCloudClient()
    port = _cursor_port(ports, client)
    created = port(operation="create", secret_ref=_SECRET_CURSOR, payload=_cursor_create_payload())
    name, agent_id = _cursor_ids()
    assert created["agentId"] == agent_id
    assert created["name"] == name
    assert client.calls[0][0] == "create_agent"
    assert "token" not in client.calls[0][1]
    assert CURSOR_SECRET_VALUE not in repr(client.calls[0][1])

    looked = port(
        operation="lookup",
        secret_ref=_SECRET_CURSOR,
        payload={"idempotency_key": CURSOR_KEY, "name": name},
    )
    assert looked["agentId"] == agent_id
    assert client.calls[1] == ("get_agent", agent_id)

    client.runs[(agent_id, "run-1")] = {
        "id": "run-1",
        "agentId": agent_id,
        "status": "FINISHED",
    }
    status = port(
        operation="status",
        secret_ref=_SECRET_CURSOR,
        payload={"agent_id": agent_id, "run_id": "run-1"},
    )
    assert status["status"] == "FINISHED"
    assert client.calls[2] == ("get_run", (agent_id, "run-1"))


def test_slack_post_root_and_lookup_success():
    ports = _load_ports()
    client = FakeSlackClient()
    port = _slack_port(ports, client)
    posted = port(operation="post_root", secret_ref=_SECRET_SLACK, payload=_slack_payload())
    assert posted["ok"] is True
    assert client.calls[0][0] == "chat_postMessage"
    kwargs = client.calls[0][1]
    assert kwargs["channel"] == CHANNEL
    assert kwargs["thread_ts"] == THREAD
    assert kwargs["client_msg_id"] == SLACK_KEY
    assert kwargs["text"] == "hello"
    assert "token" not in kwargs
    assert SLACK_SECRET_VALUE not in repr(kwargs)

    looked = port(operation="lookup", secret_ref=_SECRET_SLACK, payload=_slack_payload())
    assert looked["ok"] is True
    assert looked["messages"][0]["client_msg_id"] == SLACK_KEY
    assert client.calls[1][0] == "conversations_replies"
    assert client.calls[1][1]["channel"] == CHANNEL
    assert client.calls[1][1]["ts"] == THREAD


def test_cursor_unwraps_response_json():
    ports = _load_ports()

    class ResponseClient(FakeCursorCloudClient):
        def create_agent(self, payload):
            raw = super().create_agent(payload)
            return _ResponseLike(raw)

    client = ResponseClient()
    port = _cursor_port(ports, client)
    created = port(operation="create", secret_ref=_SECRET_CURSOR, payload=_cursor_create_payload())
    assert created["agentId"] == _cursor_ids()[1]


def test_missing_request_port_stays_unbound(tmp_path, monkeypatch):
    bind_runtime_secret_env(monkeypatch)
    bound = bind_production_transports(_complete(tmp_path), owner=SimpleNamespace())
    assert bound == {}
    assert bound.get("cursor_transport") is None
    assert bound.get("slack_transport") is None


def test_wrong_port_type_rejected():
    ports = _load_ports()
    slack_client = FakeSlackClient()
    slack_port = _slack_port(ports, slack_client)
    with pytest.raises(TypeError):
        _cursor_port(ports, slack_port)


def test_secret_ref_mismatch_is_fail_closed_zero_client_calls():
    ports = _load_ports()
    client = FakeCursorCloudClient()
    port = _cursor_port(ports, client)
    with pytest.raises(ports.RequestPortMismatch):
        port(operation="create", secret_ref=_SECRET_SLACK, payload=_cursor_create_payload())
    assert client.calls == []


def test_cursor_identity_mismatch_is_fail_closed():
    ports = _load_ports()
    client = FakeCursorCloudClient()
    port = _cursor_port(ports, client)
    with pytest.raises(ports.RequestPortMismatch):
        port(
            operation="create",
            secret_ref=_SECRET_CURSOR,
            payload=_cursor_create_payload(workspace_id="other-ws"),
        )
    with pytest.raises(ports.RequestPortMismatch):
        port(
            operation="create",
            secret_ref=_SECRET_CURSOR,
            payload=_cursor_create_payload(repository_identity="github.com/other/repo"),
        )
    assert client.calls == []


def test_slack_identity_mismatch_is_fail_closed():
    ports = _load_ports()
    client = FakeSlackClient()
    port = _slack_port(ports, client)
    with pytest.raises(ports.RequestPortMismatch):
        port(
            operation="post_root",
            secret_ref=_SECRET_SLACK,
            payload=_slack_payload(workspace_id="T999"),
        )
    with pytest.raises(ports.RequestPortMismatch):
        port(
            operation="post_root",
            secret_ref=_SECRET_SLACK,
            payload=_slack_payload(channel_id="C999"),
        )
    with pytest.raises(ports.RequestPortMismatch):
        port(
            operation="post_root",
            secret_ref=_SECRET_SLACK,
            payload=_slack_payload(root_thread_ts="9.9"),
        )
    assert client.calls == []


def test_foreign_cursor_correlation_key_is_fail_closed():
    ports = _load_ports()
    client = FakeCursorCloudClient()
    port = _cursor_port(ports, client)
    name, agent_id = _cursor_ids(CURSOR_KEY)
    foreign_name, foreign_id = _cursor_ids("foreign-cursor-key")
    with pytest.raises(ports.RequestPortMismatch):
        port(
            operation="create",
            secret_ref=_SECRET_CURSOR,
            payload={
                "name": foreign_name,
                "agentId": foreign_id,
                "idempotency_key": CURSOR_KEY,
            },
        )
    with pytest.raises(ports.RequestPortMismatch):
        port(
            operation="create",
            secret_ref=_SECRET_CURSOR,
            payload={
                "name": name,
                "agentId": agent_id,
                "idempotency_key": "foreign-cursor-key",
            },
        )
    assert client.calls == []


def test_foreign_slack_idempotency_key_lookup_does_not_invent_a_hit():
    ports = _load_ports()
    client = FakeSlackClient()
    port = _slack_port(ports, client)
    port(operation="post_root", secret_ref=_SECRET_SLACK, payload=_slack_payload())
    looked = port(
        operation="lookup",
        secret_ref=_SECRET_SLACK,
        payload=_slack_payload(key="foreign-slack-key"),
    )
    assert looked["messages"] == []
    assert [call[0] for call in client.calls] == [
        "chat_postMessage",
        "conversations_replies",
    ]
    assert client.calls[0][1]["client_msg_id"] == SLACK_KEY


def test_timeout_and_cancellation_do_not_call_client():
    ports = _load_ports()
    client = FakeCursorCloudClient()
    port = _cursor_port(ports, client)
    with pytest.raises(ports.RequestPortTimeout):
        port(
            operation="create",
            secret_ref=_SECRET_CURSOR,
            payload=_cursor_create_payload(),
            timeout_seconds=0,
        )
    cancel = threading.Event()
    cancel.set()
    with pytest.raises(ports.RequestPortTimeout):
        port(
            operation="create",
            secret_ref=_SECRET_CURSOR,
            payload=_cursor_create_payload(),
            cancel_event=cancel,
        )
    assert client.calls == []


def test_provider_error_is_redacted_and_does_not_log_secret():
    ports = _load_ports()
    client = FakeCursorCloudClient()
    client.create_error = RuntimeError(f"upstream 401 token={CURSOR_SECRET_VALUE}")
    port = _cursor_port(ports, client)
    with pytest.raises(ports.RequestPortError) as caught:
        port(operation="create", secret_ref=_SECRET_CURSOR, payload=_cursor_create_payload())
    assert CURSOR_SECRET_VALUE not in str(caught.value)
    assert CURSOR_SECRET_VALUE not in repr(caught.value)
    assert [call[0] for call in client.calls] == ["create_agent"]


def test_retry_same_idempotency_key_looks_up_existing_cursor_agent():
    ports = _load_ports()
    client = FakeCursorCloudClient()
    port = _cursor_port(ports, client)
    first = port(operation="create", secret_ref=_SECRET_CURSOR, payload=_cursor_create_payload())
    second = port(
        operation="lookup",
        secret_ref=_SECRET_CURSOR,
        payload={"idempotency_key": CURSOR_KEY, "name": first["name"]},
    )
    assert second["agentId"] == first["agentId"]
    assert [call[0] for call in client.calls] == ["create_agent", "get_agent"]


def test_slack_retry_same_client_msg_id_is_lookup_not_a_second_post():
    ports = _load_ports()
    client = FakeSlackClient()
    port = _slack_port(ports, client)
    port(operation="post_root", secret_ref=_SECRET_SLACK, payload=_slack_payload())
    looked = port(operation="lookup", secret_ref=_SECRET_SLACK, payload=_slack_payload())
    assert looked["messages"][0]["client_msg_id"] == SLACK_KEY
    assert [call[0] for call in client.calls] == [
        "chat_postMessage",
        "conversations_replies",
    ]


def test_close_is_bounded_and_subsequent_calls_are_fail_closed():
    ports = _load_ports()
    client = FakeCursorCloudClient()
    port = _cursor_port(ports, client)
    port.close()
    with pytest.raises(ports.RequestPortClosed):
        port(operation="create", secret_ref=_SECRET_CURSOR, payload=_cursor_create_payload())
    assert client.calls == []


def test_async_client_methods_are_rejected_without_running_them():
    ports = _load_ports()

    class AsyncCursor:
        async def create_agent(self, payload):
            raise AssertionError("async create_agent must not run")

        async def get_agent(self, agent_id):
            raise AssertionError("async get_agent must not run")

        async def get_run(self, agent_id, run_id):
            raise AssertionError("async get_run must not run")

    port = _cursor_port(ports, AsyncCursor())
    with pytest.raises(ports.RequestPortError, match="async"):
        port(operation="create", secret_ref=_SECRET_CURSOR, payload=_cursor_create_payload())


def test_credential_resolver_runs_only_at_request_time_and_never_logs_value():
    ports = _load_ports()
    client = FakeCursorCloudClient()
    seen: list[str] = []

    def resolver(name: str) -> str:
        seen.append(name)
        return CURSOR_SECRET_VALUE

    port = _cursor_port(ports, client, credential_resolver=resolver)
    assert seen == []
    port(operation="create", secret_ref=_SECRET_CURSOR, payload=_cursor_create_payload())
    assert seen == [_SECRET_CURSOR]
    assert CURSOR_SECRET_VALUE not in repr(port)

    empty_client = FakeCursorCloudClient()
    empty_port = _cursor_port(ports, empty_client, credential_resolver=lambda _name: "")
    with pytest.raises(ports.RequestPortError):
        empty_port(
            operation="create",
            secret_ref=_SECRET_CURSOR,
            payload=_cursor_create_payload(),
        )
    assert empty_client.calls == []


def test_lane_closed_error_is_preserved_through_injected_transport():
    ports = _load_ports()
    client = FakeCursorCloudClient()
    client.create_error = LaneClosedError("lane closed during request")
    port = _cursor_port(ports, client)
    transport = CursorCloudInjectedTransport(request=port, secret_ref=_SECRET_CURSOR)
    name, agent_id = _cursor_ids()
    with pytest.raises(LaneClosedError, match="lane closed during request"):
        transport.create(
            idempotency_key=CURSOR_KEY,
            job_id="job-1",
            name=name,
            agent_id=agent_id,
        )


def test_explicit_injected_client_can_be_wrapped_by_production_binding(
    tmp_path, monkeypatch
):
    bind_runtime_secret_env(monkeypatch)
    ports = _load_ports()
    cursor_client = FakeCursorCloudClient()
    slack_client = FakeSlackClient()
    owner = SimpleNamespace(
        _durable_job_cursor_client=cursor_client,
        _durable_job_slack_client=slack_client,
        _durable_job_slack_channel_id=CHANNEL,
        _durable_job_slack_root_thread_ts=THREAD,
        _durable_job_runtime_identity={
            "workspace_id": WORKSPACE,
            "repository_identity": REPO,
        },
    )
    kwargs = production_attach_kwargs(owner=owner, raw_config=_complete(tmp_path))
    bound = bind_production_transports(owner=owner, raw_config=_complete(tmp_path))
    assert type(bound.get("cursor_transport")) is CursorCloudInjectedTransport
    assert type(bound.get("slack_transport")) is SlackInjectedTransport
    name, agent_id = _cursor_ids()
    created = bound["cursor_transport"].create(
        idempotency_key=CURSOR_KEY,
        job_id="job-1",
        name=name,
        agent_id=agent_id,
    )
    assert created["agentId"] == agent_id
    assert cursor_client.calls[0][0] == "create_agent"
    assert isinstance(kwargs["cursor_transport"]._request, ports.CursorCloudInjectedRequestPort)


def test_production_binding_does_not_mint_clients_from_config_flags(
    tmp_path, monkeypatch
):
    bind_runtime_secret_env(monkeypatch)
    owner = SimpleNamespace(
        config={
            "durable_jobs": {
                "enabled": True,
                "dispatch_allowed": True,
                "cursor_cloud": {"enabled": True, "mint_client": True},
                "slack": {"enabled": True, "use_gateway_adapter": True},
            }
        }
    )
    bound = bind_production_transports(
        _complete(tmp_path, dispatch_enabled=True),
        owner=owner,
    )
    assert bound == {}
