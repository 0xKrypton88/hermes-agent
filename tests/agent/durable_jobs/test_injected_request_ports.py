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

Reworked onto ENG-50: production binding still requires a concrete matching
``_durable_job_runtime_identity`` in instance ``__dict__``. Missing identity
or getattr/descriptor client seams must not bind. Shadow attach therefore
passes an owner with matching identity rather than weakening that gate.
"""

from __future__ import annotations

import inspect
import sys
import threading
import time
from collections.abc import Mapping
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


def _matching_identity(**overrides) -> dict:
    identity = {
        "workspace_id": WORKSPACE,
        "repository_identity": REPO,
    }
    identity.update(overrides)
    return identity


def _owner_with_matching_identity(**attrs):
    owner = type("Owner", (), {})()
    owner.__dict__["_durable_job_runtime_identity"] = _matching_identity()
    for name, value in attrs.items():
        owner.__dict__[name] = value
    return owner


def _assert_no_secrets(payload: object) -> None:
    dumped = str(payload)
    assert CURSOR_SECRET_VALUE not in dumped
    assert SLACK_SECRET_VALUE not in dumped
    assert "xoxb-" not in dumped
    assert "supersecret" not in dumped


def _load_ports():
    from agent.durable_jobs.request_ports import (
        CursorCloudInjectedRequestPort,
        RequestPortClosed,
        RequestPortError,
        RequestPortMismatch,
        RequestPortTimeout,
        SlackInjectedRequestPort,
        _FrozenDict,
    )

    return SimpleNamespace(
        CursorCloudInjectedRequestPort=CursorCloudInjectedRequestPort,
        SlackInjectedRequestPort=SlackInjectedRequestPort,
        RequestPortClosed=RequestPortClosed,
        RequestPortError=RequestPortError,
        RequestPortMismatch=RequestPortMismatch,
        RequestPortTimeout=RequestPortTimeout,
        FrozenDict=_FrozenDict,
    )


def _cursor_port(ports, client, **kwargs):
    kwargs.setdefault(
        "credential_resolver", lambda _secret_ref: "offline-cursor-credential"
    )
    return ports.CursorCloudInjectedRequestPort(
        client=client,
        secret_ref=_SECRET_CURSOR,
        workspace_id=WORKSPACE,
        repository_identity=REPO,
        **kwargs,
    )


def _slack_port(ports, client, **kwargs):
    kwargs.setdefault(
        "credential_resolver", lambda _secret_ref: "offline-slack-credential"
    )
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
        "repository_identity": REPO,
        "channel_id": CHANNEL,
        "root_thread_ts": THREAD,
        "idempotency_key": key,
        "client_msg_id": key,
        "text": "hello",
    }
    payload.update(extra)
    return payload


def _assert_receipts_sanitized(port) -> None:
    receipts = getattr(port, "receipts", None)
    assert receipts is not None
    assert isinstance(receipts, tuple)
    dumped = repr(receipts)
    _assert_no_secrets(dumped)
    for receipt in receipts:
        assert isinstance(receipt, Mapping)
        assert receipt["secret_ref"] in {_SECRET_CURSOR, _SECRET_SLACK}
        assert CURSOR_SECRET_VALUE not in str(receipt)
        assert SLACK_SECRET_VALUE not in str(receipt)


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
    assert cursor.receipts == ()
    assert slack.receipts == ()
    blob = repr(cursor) + repr(slack)
    _assert_no_secrets(blob)


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


def test_client_seam_validation_is_static_and_rejects_descriptors_without_effects():
    ports = _load_ports()
    effects = []

    class Descriptor:
        def __get__(self, obj, owner=None):
            effects.append("descriptor")
            return lambda *args, **kwargs: None

    class HostileCursor:
        create_agent = Descriptor()
        get_agent = Descriptor()
        get_run = Descriptor()

        def __getattr__(self, name):
            effects.append(("getattr", name))
            return lambda *args, **kwargs: None

    with pytest.raises(TypeError, match="missing required seam methods"):
        _cursor_port(ports, HostileCursor())

    assert effects == []


def test_hostile_getattribute_cannot_mint_concrete_client_capability():
    ports = _load_ports()
    effects = []

    class HostileCursor:
        def __getattribute__(self, name):
            if name in {"create_agent", "get_agent", "get_run"}:
                effects.append(("dynamic_lookup", name))
                return lambda *_args, **_kwargs: effects.append(("provider", name))
            return object.__getattribute__(self, name)

        def create_agent(self, _payload):
            effects.append("concrete_provider")

        def get_agent(self, _agent_id):
            effects.append("concrete_provider")

        def get_run(self, _agent_id, _run_id):
            effects.append("concrete_provider")

    with pytest.raises(TypeError, match="static client lookup"):
        _cursor_port(
            ports,
            HostileCursor(),
            credential_resolver=lambda _name: effects.append("resolver") or "secret",
        )

    assert effects == []


def test_missing_resolver_denies_preflight_and_direct_request_but_positive_resolves_once(
    tmp_path,
):
    from agent.durable_jobs.preflight import preflight_durable_jobs

    ports = _load_ports()
    cursor_client = FakeCursorCloudClient()
    slack_client = FakeSlackClient()
    cursor_request = _cursor_port(ports, cursor_client, credential_resolver=None)
    slack_request = _slack_port(ports, slack_client, credential_resolver=None)
    cursor_transport = CursorCloudInjectedTransport(
        request=cursor_request, secret_ref=_SECRET_CURSOR
    )
    slack_transport = SlackInjectedTransport(
        request=slack_request, secret_ref=_SECRET_SLACK
    )

    report = preflight_durable_jobs(
        _complete(tmp_path, dispatch_enabled=True),
        cursor_transport=cursor_transport,
        slack_transport=slack_transport,
    )
    assert report.runtime_ready is False
    assert report.dispatch_allowed is False
    assert "secret_refs_missing" in report.reasons

    with pytest.raises(ports.RequestPortError, match="credential resolver is required"):
        cursor_request(
            operation="create",
            secret_ref=_SECRET_CURSOR,
            payload=_cursor_create_payload(),
        )
    with pytest.raises(ports.RequestPortError, match="credential resolver is required"):
        slack_request(
            operation="post_root",
            secret_ref=_SECRET_SLACK,
            payload=_slack_payload(),
        )
    assert cursor_client.calls == []
    assert slack_client.calls == []

    resolver_calls = []

    def resolver(secret_ref):
        resolver_calls.append(secret_ref)
        return "request-bound-test-credential"

    positive_cursor_request = _cursor_port(
        ports, FakeCursorCloudClient(), credential_resolver=resolver
    )
    positive_slack_request = _slack_port(
        ports, FakeSlackClient(), credential_resolver=resolver
    )
    positive_cursor_transport = CursorCloudInjectedTransport(
        request=positive_cursor_request, secret_ref=_SECRET_CURSOR
    )
    positive_slack_transport = SlackInjectedTransport(
        request=positive_slack_request, secret_ref=_SECRET_SLACK
    )
    positive_report = preflight_durable_jobs(
        _complete(tmp_path, dispatch_enabled=True),
        cursor_transport=positive_cursor_transport,
        slack_transport=positive_slack_transport,
    )
    assert positive_report.runtime_ready is True
    assert positive_report.dispatch_allowed is True
    assert resolver_calls == []
    positive_cursor_request(
        operation="create",
        secret_ref=_SECRET_CURSOR,
        payload=_cursor_create_payload("resolver-once"),
    )
    assert resolver_calls == [_SECRET_CURSOR]


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
    _assert_no_secrets(client.calls[0][1])

    looked = port(
        operation="lookup",
        secret_ref=_SECRET_CURSOR,
        payload={
            "idempotency_key": CURSOR_KEY,
            "name": name,
            "workspace_id": WORKSPACE,
            "repository_identity": REPO,
        },
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
        payload={
            "idempotency_key": CURSOR_KEY,
            "agent_id": agent_id,
            "run_id": "run-1",
            "workspace_id": WORKSPACE,
            "repository_identity": REPO,
        },
    )
    assert status["status"] == "FINISHED"
    assert client.calls[2] == ("get_run", (agent_id, "run-1"))
    _assert_receipts_sanitized(port)
    assert [item["operation"] for item in port.receipts] == ["create", "lookup", "status"]
    assert all(item["outcome"] == "ok" for item in port.receipts)
    assert all(item["client_invoked"] is True for item in port.receipts)
    assert port.receipts == port.receipts


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
    _assert_no_secrets(kwargs)

    looked = port(operation="lookup", secret_ref=_SECRET_SLACK, payload=_slack_payload())
    assert looked["ok"] is True
    assert looked["messages"][0]["client_msg_id"] == SLACK_KEY
    assert client.calls[1][0] == "conversations_replies"
    assert client.calls[1][1]["channel"] == CHANNEL
    assert client.calls[1][1]["ts"] == THREAD
    _assert_receipts_sanitized(port)
    assert [item["operation"] for item in port.receipts] == ["post_root", "lookup"]


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
    _assert_receipts_sanitized(port)
    assert port.receipts[-1]["client_invoked"] is False
    assert port.receipts[-1]["outcome"] == "error"


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
    assert all(item["client_invoked"] is False for item in port.receipts)


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
            payload=_slack_payload(repository_identity="github.com/other/repo"),
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


@pytest.mark.parametrize("field", ["workspace_id", "repository_identity"])
def test_cursor_missing_mandatory_identity_is_fail_closed(field):
    ports = _load_ports()
    client = FakeCursorCloudClient()
    port = _cursor_port(ports, client)
    payload = _cursor_create_payload()
    del payload[field]
    with pytest.raises(ports.RequestPortMismatch):
        port(operation="create", secret_ref=_SECRET_CURSOR, payload=payload)
    assert client.calls == []


@pytest.mark.parametrize(
    "field",
    ["workspace_id", "repository_identity", "channel_id", "root_thread_ts"],
)
def test_slack_missing_mandatory_identity_is_fail_closed(field):
    ports = _load_ports()
    client = FakeSlackClient()
    port = _slack_port(ports, client)
    payload = _slack_payload()
    del payload[field]
    with pytest.raises(ports.RequestPortMismatch):
        port(operation="post_root", secret_ref=_SECRET_SLACK, payload=payload)
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
                "workspace_id": WORKSPACE,
                "repository_identity": REPO,
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
                "workspace_id": WORKSPACE,
                "repository_identity": REPO,
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
    assert looked["messages"] == ()
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
    assert all(item["client_invoked"] is False for item in port.receipts)


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
    _assert_receipts_sanitized(port)
    assert port.receipts[-1]["client_invoked"] is True
    assert port.receipts[-1]["outcome"] == "error"


def test_retry_same_idempotency_key_looks_up_existing_cursor_agent():
    ports = _load_ports()
    client = FakeCursorCloudClient()
    port = _cursor_port(ports, client)
    first = port(operation="create", secret_ref=_SECRET_CURSOR, payload=_cursor_create_payload())
    second = port(
        operation="lookup",
        secret_ref=_SECRET_CURSOR,
        payload={
            "idempotency_key": CURSOR_KEY,
            "name": first["name"],
            "workspace_id": WORKSPACE,
            "repository_identity": REPO,
        },
    )
    assert second["agentId"] == first["agentId"]
    assert [call[0] for call in client.calls] == ["create_agent", "get_agent"]
    assert [item["operation"] for item in port.receipts] == ["create", "lookup"]


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
    assert port.receipts[-1]["client_invoked"] is False


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


@pytest.mark.parametrize(
    ("operation", "payload"),
    [
        *[
            (operation, _cursor_create_payload(**{field: "foreign"}))
            for operation in ("create", "lookup")
            for field in ("workspace_id", "repository_identity")
        ],
        *[
            (
                "status",
                {
                    "agent_id": "agent-1",
                    "run_id": "run-1",
                    "workspace_id": WORKSPACE,
                    "repository_identity": REPO,
                    field: "foreign",
                },
            )
            for field in ("workspace_id", "repository_identity")
        ],
        ("create", _cursor_create_payload(name="foreign-name")),
        ("create", _cursor_create_payload(agentId="foreign-agent")),
        ("lookup", _cursor_create_payload(name="foreign-name")),
        ("lookup", _cursor_create_payload(agentId="foreign-agent")),
        (
            "status",
            {
                "run_id": "run-1",
                "workspace_id": WORKSPACE,
                "repository_identity": REPO,
            },
        ),
        (
            "status",
            {
                "agent_id": "agent-1",
                "workspace_id": WORKSPACE,
                "repository_identity": REPO,
            },
        ),
        ("unsupported", _cursor_create_payload()),
    ],
)
def test_cursor_rejects_invalid_dispatch_before_credential_resolution(operation, payload):
    ports = _load_ports()
    client = FakeCursorCloudClient()
    resolver_calls = []
    port = _cursor_port(
        ports,
        client,
        credential_resolver=lambda name: resolver_calls.append(name) or CURSOR_SECRET_VALUE,
    )

    with pytest.raises(ports.RequestPortError):
        port(operation=operation, secret_ref=_SECRET_CURSOR, payload=payload)

    assert resolver_calls == []
    assert client.calls == []


@pytest.mark.parametrize(
    ("operation", "payload"),
    [
        *[
            (operation, _slack_payload(**{field: "foreign"}))
            for operation in ("post_root", "lookup")
            for field in (
                "workspace_id",
                "repository_identity",
                "channel_id",
                "root_thread_ts",
            )
        ],
        *[
            (operation, {key: value for key, value in _slack_payload().items() if key not in {"client_msg_id", "idempotency_key"}})
            for operation in ("post_root", "lookup")
        ],
        ("post_root", _slack_payload(text=None)),
        ("unsupported", _slack_payload()),
    ],
)
def test_slack_rejects_invalid_dispatch_before_credential_resolution(operation, payload):
    ports = _load_ports()
    client = FakeSlackClient()
    resolver_calls = []
    port = _slack_port(
        ports,
        client,
        credential_resolver=lambda name: resolver_calls.append(name) or SLACK_SECRET_VALUE,
    )

    with pytest.raises(ports.RequestPortError):
        port(operation=operation, secret_ref=_SECRET_SLACK, payload=payload)

    assert resolver_calls == []
    assert client.calls == []


def test_close_waits_for_request_blocked_in_credential_resolver_and_prevents_dispatch():
    ports = _load_ports()
    client = FakeCursorCloudClient()
    resolver_entered = threading.Event()
    resolver_release = threading.Event()
    errors = []

    def resolver(_name):
        resolver_entered.set()
        assert resolver_release.wait(1)
        return CURSOR_SECRET_VALUE

    port = _cursor_port(ports, client, credential_resolver=resolver)

    def call():
        try:
            port(
                operation="create",
                secret_ref=_SECRET_CURSOR,
                payload=_cursor_create_payload(),
                timeout_seconds=1,
            )
        except BaseException as exc:
            errors.append(exc)

    caller = threading.Thread(target=call)
    caller.start()
    assert resolver_entered.wait(1)
    assert port.close(timeout_seconds=0.01) is False
    assert caller.is_alive()
    assert client.calls == []

    resolver_release.set()
    caller.join(1)
    assert not caller.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], ports.RequestPortClosed)
    assert client.calls == []
    assert port.close(timeout_seconds=1) is True
    assert port._active_calls == 0


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
    _assert_no_secrets(repr(port))
    _assert_receipts_sanitized(port)

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


def test_retirement_preserves_lane_closed_as_primary_over_idle_closed_hook():
    """Request-port LaneClosedError remains the primary through lease unwind."""
    ports = _load_ports()
    client = FakeCursorCloudClient()
    client.create_error = LaneClosedError("lane closed during request")
    port = _cursor_port(ports, client)
    transport = CursorCloudInjectedTransport(request=port, secret_ref=_SECRET_CURSOR)
    name, agent_id = _cursor_ids()

    class _Lane:
        def _after_idle_closed(self):
            raise RuntimeError("hook exploded")

    lane = _Lane()
    with pytest.raises(LaneClosedError, match="lane closed during request"):
        try:
            transport.create(
                idempotency_key=CURSOR_KEY,
                job_id="job-1",
                name=name,
                agent_id=agent_id,
            )
        except LaneClosedError:
            try:
                lane._after_idle_closed()
            except Exception:
                pass
            raise


def test_explicit_injected_client_can_be_wrapped_by_production_binding(
    tmp_path, monkeypatch
):
    bind_runtime_secret_env(monkeypatch)
    ports = _load_ports()
    cursor_client = FakeCursorCloudClient()
    slack_client = FakeSlackClient()
    owner = _owner_with_matching_identity(
        _durable_job_cursor_client=cursor_client,
        _durable_job_slack_client=slack_client,
        _durable_job_slack_channel_id=CHANNEL,
        _durable_job_slack_root_thread_ts=THREAD,
        _durable_job_credential_resolver=lambda name: (
            CURSOR_SECRET_VALUE if name == _SECRET_CURSOR else SLACK_SECRET_VALUE
        ),
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
    _assert_no_secrets(repr(kwargs))


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


def test_injected_clients_on_class_or_descriptor_do_not_bind(tmp_path, monkeypatch):
    """ENG-50: wrapping must not read class/property/descriptor client seams."""
    bind_runtime_secret_env(monkeypatch)
    ports = _load_ports()
    cursor_client = FakeCursorCloudClient()
    slack_client = FakeSlackClient()

    class _Desc:
        def __init__(self, value):
            self.value = value

        def __get__(self, obj, objtype=None):
            raise AssertionError("client descriptor must not run")

    class Owner:
        _durable_job_cursor_client = cursor_client
        _durable_job_slack_client = slack_client
        _durable_job_slack_channel_id = CHANNEL
        _durable_job_slack_root_thread_ts = THREAD
        _durable_job_credential_resolver = _Desc("nope")

        @property
        def _durable_job_cursor_request(self):
            raise AssertionError("request property must not run")

    owner = Owner()
    owner.__dict__["_durable_job_runtime_identity"] = _matching_identity()
    bound = bind_production_transports(_complete(tmp_path), owner=owner)
    assert bound == {}
    assert cursor_client.calls == []
    assert slack_client.calls == []
    assert ports.CursorCloudInjectedRequestPort is not None


def test_wrapped_clients_without_matching_identity_do_not_bind(tmp_path, monkeypatch):
    bind_runtime_secret_env(monkeypatch)
    ports = _load_ports()
    owner = type("Owner", (), {})()
    owner.__dict__["_durable_job_cursor_client"] = FakeCursorCloudClient()
    owner.__dict__["_durable_job_slack_client"] = FakeSlackClient()
    owner.__dict__["_durable_job_slack_channel_id"] = CHANNEL
    owner.__dict__["_durable_job_slack_root_thread_ts"] = THREAD
    owner.__dict__["_durable_job_runtime_identity"] = _matching_identity(
        workspace_id="T-FOREIGN"
    )
    bound = bind_production_transports(_complete(tmp_path), owner=owner)
    assert bound == {}
    assert ports.SlackInjectedRequestPort is not None


def test_concurrent_cursor_claim_invokes_provider_once():
    ports = _load_ports()
    entered = threading.Event()
    release = threading.Event()

    class BlockingCursor(FakeCursorCloudClient):
        def create_agent(self, payload):
            entered.set()
            assert release.wait(1)
            return super().create_agent(payload)

    client = BlockingCursor()
    port = _cursor_port(ports, client)
    results = []
    errors = []

    def call():
        try:
            results.append(
                port(
                    operation="create",
                    secret_ref=_SECRET_CURSOR,
                    payload=_cursor_create_payload(),
                    timeout_seconds=1,
                )
            )
        except BaseException as exc:
            errors.append(exc)

    callers = [threading.Thread(target=call) for _ in range(2)]
    for caller in callers:
        caller.start()
    assert entered.wait(1)
    release.set()
    for caller in callers:
        caller.join(1)
    assert errors == []
    assert len(results) == 2
    assert results[0] == results[1]
    assert [item[0] for item in client.calls] == ["create_agent"]


def test_cursor_idempotent_retry_after_credential_rotation_never_leaks_either_value():
    ports = _load_ports()
    old_credential = "rotation-old-provider-credential"
    new_credential = "rotation-new-provider-credential"
    resolved = iter((old_credential, new_credential))

    class CredentialEchoingCursor(FakeCursorCloudClient):
        def create_agent(self, payload):
            self.calls.append(("create_agent", payload))
            return {
                "agentId": payload["agentId"],
                "nested": {"provider_echo": f"accepted:{old_credential}"},
            }

    client = CredentialEchoingCursor()
    port = _cursor_port(
        ports,
        client,
        credential_resolver=lambda _name: next(resolved),
    )

    first = port(operation="create", secret_ref=_SECRET_CURSOR, payload=_cursor_create_payload())
    second = port(operation="create", secret_ref=_SECRET_CURSOR, payload=_cursor_create_payload())

    exposed = repr((first, second, port.receipts))
    assert old_credential not in exposed
    assert new_credential not in exposed
    assert first == second
    assert [item[0] for item in client.calls] == ["create_agent"]
    assert len(port.receipts) == 2


def test_hostile_mapping_sanitizer_failure_cannot_rethrow_resolved_credential():
    ports = _load_ports()
    secret = "resolved-credential-must-never-escape"
    sanitizer_effects = []

    class HostileMapping(Mapping):
        def __getitem__(self, _key):
            raise AssertionError(secret)

        def __iter__(self):
            raise AssertionError(secret)

        def __len__(self):
            return 1

        def items(self):
            sanitizer_effects.append("items")
            raise RuntimeError(f"sanitizer exploded with {secret}")

    class HostileResponseCursor(FakeCursorCloudClient):
        def create_agent(self, payload):
            self.calls.append(("create_agent", payload))
            return {"safe": "ok", "nested": HostileMapping()}

    client = HostileResponseCursor()
    port = _cursor_port(
        ports,
        client,
        credential_resolver=lambda _name: secret,
    )

    result = port(
        operation="create",
        secret_ref=_SECRET_CURSOR,
        payload=_cursor_create_payload("hostile-sanitizer"),
    )

    exposed = repr((result, port.receipts))
    assert secret not in exposed
    assert result["safe"] == "ok"
    assert result["nested"]["type"] == "HostileMapping"
    assert sanitizer_effects == []
    assert [call[0] for call in client.calls] == ["create_agent"]


def test_provider_supplied_frozen_snapshot_is_resanitized():
    ports = _load_ports()
    secret = "resolved-credential-inside-frozen-snapshot"

    class FrozenResponseCursor(FakeCursorCloudClient):
        def create_agent(self, payload):
            self.calls.append(("create_agent", payload))
            return ports.FrozenDict({"nested": secret})

    port = _cursor_port(
        ports,
        FrozenResponseCursor(),
        credential_resolver=lambda _name: secret,
    )
    result = port(
        operation="create",
        secret_ref=_SECRET_CURSOR,
        payload=_cursor_create_payload("frozen-snapshot-redaction"),
    )

    assert secret not in repr((result, port.receipts))
    assert result["nested"] == "[REDACTED]"


def test_hostile_response_json_descriptor_is_not_executed():
    ports = _load_ports()
    secret = "resolved-credential-response-json-descriptor"
    descriptor_effects = []

    class HostileJsonResponse:
        @property
        def json(self):
            descriptor_effects.append("json")
            raise RuntimeError(secret)

    class HostileResponseCursor(FakeCursorCloudClient):
        def create_agent(self, payload):
            self.calls.append(("create_agent", payload))
            return HostileJsonResponse()

    client = HostileResponseCursor()
    port = _cursor_port(
        ports,
        client,
        credential_resolver=lambda _name: secret,
    )
    result = port(
        operation="create",
        secret_ref=_SECRET_CURSOR,
        payload=_cursor_create_payload("hostile-response-json"),
    )

    assert result["type"] == "HostileJsonResponse"
    assert descriptor_effects == []
    assert secret not in repr((result, port.receipts))
    assert [call[0] for call in client.calls] == ["create_agent"]


def test_provider_base_exception_is_converted_without_secret_chain():
    ports = _load_ports()
    secret = "resolved-cursor-baseexception-secret"

    class BaseExceptionCursor(FakeCursorCloudClient):
        def create_agent(self, payload):
            self.calls.append(("create_agent", payload))
            raise KeyboardInterrupt(secret)

    client = BaseExceptionCursor()
    port = _cursor_port(
        ports,
        client,
        credential_resolver=lambda _name: secret,
    )

    with pytest.raises(ports.RequestPortError) as caught:
        port(
            operation="create",
            secret_ref=_SECRET_CURSOR,
            payload=_cursor_create_payload(key="provider-baseexception"),
        )

    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert secret not in repr(caught.value)
    assert secret not in repr(port.receipts)
    assert len(client.calls) == 1


def test_hostile_cancellation_exception_has_no_secret_chain():
    ports = _load_ports()
    secret = "hostile-cancellation-secret"

    class HostileCancellation:
        def is_set(self):
            raise RuntimeError(secret)

    client = FakeCursorCloudClient()
    resolver_calls = []
    port = _cursor_port(
        ports,
        client,
        credential_resolver=lambda name: resolver_calls.append(name) or "unused",
    )

    with pytest.raises(ports.RequestPortTimeout) as caught:
        port(
            operation="create",
            secret_ref=_SECRET_CURSOR,
            payload=_cursor_create_payload(key="hostile-cancellation"),
            cancel_event=HostileCancellation(),
        )

    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert secret not in repr(caught.value)
    assert secret not in repr(port.receipts)
    assert resolver_calls == []
    assert client.calls == []


def test_slack_repeated_post_claim_invokes_provider_once():
    ports = _load_ports()
    client = FakeSlackClient()
    port = _slack_port(ports, client)
    first = port(operation="post_root", secret_ref=_SECRET_SLACK, payload=_slack_payload())
    second = port(operation="post_root", secret_ref=_SECRET_SLACK, payload=_slack_payload())
    assert first == second
    assert [item[0] for item in client.calls] == ["chat_postMessage"]


def test_inflight_cancellation_and_close_are_bounded_and_accounted():
    ports = _load_ports()
    entered = threading.Event()
    release = threading.Event()
    cancel = threading.Event()

    class BlockingCursor(FakeCursorCloudClient):
        def create_agent(self, payload):
            entered.set()
            assert release.wait(1)
            return super().create_agent(payload)

    client = BlockingCursor()
    port = _cursor_port(ports, client)
    errors = []

    def call():
        try:
            port(
                operation="create",
                secret_ref=_SECRET_CURSOR,
                payload=_cursor_create_payload(),
                timeout_seconds=1,
                cancel_event=cancel,
            )
        except BaseException as exc:
            errors.append(exc)

    caller = threading.Thread(target=call)
    caller.start()
    assert entered.wait(1)
    cancel.set()
    caller.join(0.2)
    assert not caller.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], ports.RequestPortTimeout)
    started = time.monotonic()
    assert port.close(timeout_seconds=0.01) is False
    assert time.monotonic() - started < 0.2
    release.set()
    assert port.close(timeout_seconds=1) is True


def test_completion_after_deadline_still_times_out_and_releases_active_call(monkeypatch):
    import agent.durable_jobs.request_ports as request_ports_module

    ports = _load_ports()
    real_event = threading.Event
    provider_release = real_event()

    class DeadlineCrossingEvent:
        def __init__(self):
            self._event = real_event()

        def set(self):
            self._event.set()

        def wait(self, _timeout=None):
            return self._event.wait(1)

    class DelayedCursor(FakeCursorCloudClient):
        def create_agent(self, payload):
            assert provider_release.wait(1)
            return super().create_agent(payload)

    client = DelayedCursor()
    port = _cursor_port(ports, client)
    controlled_event = DeadlineCrossingEvent()
    monkeypatch.setattr(
        request_ports_module,
        "threading",
        SimpleNamespace(
            Event=lambda: controlled_event,
            Thread=threading.Thread,
        ),
    )
    releaser = threading.Timer(0.04, provider_release.set)
    releaser.start()
    started = time.monotonic()
    try:
        with pytest.raises(ports.RequestPortTimeout, match="timeout"):
            port(
                operation="create",
                secret_ref=_SECRET_CURSOR,
                payload=_cursor_create_payload(key="deadline-crossing-claim"),
                timeout_seconds=0.01,
            )
    finally:
        releaser.join(1)
    assert time.monotonic() - started < 0.2
    assert [item[0] for item in client.calls] == ["create_agent"]
    assert port.close(timeout_seconds=1) is True
    assert port._active_calls == 0


@pytest.mark.parametrize(
    ("provider", "operation"),
    (
        ("cursor", "create"),
        ("cursor", "lookup"),
        ("cursor", "status"),
        ("slack", "post_root"),
        ("slack", "lookup"),
    ),
)
@pytest.mark.parametrize("hostile_hook", ("timeout", "cancel"))
def test_admission_budget_includes_hostile_timeout_and_cancel_hooks(
    monkeypatch, provider, operation, hostile_hook
):
    import agent.durable_jobs.request_ports as request_ports_module

    ports = _load_ports()
    clock = {"now": 10.0}
    monkeypatch.setattr(request_ports_module.time, "monotonic", lambda: clock["now"])

    class Timeout:
        def __float__(self):
            if hostile_hook == "timeout":
                clock["now"] += 2.0
            return 1.0

    class Cancel:
        calls = 0

        def is_set(self):
            self.calls += 1
            if hostile_hook == "cancel":
                clock["now"] += 2.0
            return False

    cancel = Cancel()
    resolver_calls = []

    def resolver(name):
        resolver_calls.append(name)
        return "secret"

    if provider == "cursor":
        client = FakeCursorCloudClient()
        port = _cursor_port(ports, client, credential_resolver=resolver)
        payload = _cursor_create_payload()
        if operation == "status":
            payload = _cursor_create_payload(run_id="run-1", agent_id=_cursor_ids()[1])
    else:
        client = FakeSlackClient()
        port = _slack_port(ports, client, credential_resolver=resolver)
        payload = _slack_payload()

    with pytest.raises(ports.RequestPortTimeout):
        port(
            operation=operation,
            secret_ref=_SECRET_CURSOR if provider == "cursor" else _SECRET_SLACK,
            payload=payload,
            timeout_seconds=Timeout(),
            cancel_event=cancel,
        )

    assert resolver_calls == []
    assert client.calls == []
    if hostile_hook == "timeout":
        assert cancel.calls == 0
    assert port._active_calls == 0
    assert port.close(timeout_seconds=1) is True


def test_resolved_secret_is_recursively_redacted_from_frozen_snapshots():
    ports = _load_ports()

    class SecretReturningCursor(FakeCursorCloudClient):
        def create_agent(self, payload):
            self.calls.append(("create_agent", payload))
            return {
                "agentId": payload["agentId"],
                "nested": [{"arbitrary": f"prefix:{CURSOR_SECRET_VALUE}:suffix"}],
            }

    client = SecretReturningCursor()
    resolver_calls: list[str] = []

    def resolver(name: str) -> str:
        resolver_calls.append(name)
        return CURSOR_SECRET_VALUE

    port = _cursor_port(ports, client, credential_resolver=resolver)
    assert resolver_calls == []
    result = port(operation="create", secret_ref=_SECRET_CURSOR, payload=_cursor_create_payload())
    assert resolver_calls == [_SECRET_CURSOR]
    assert CURSOR_SECRET_VALUE not in repr(result)
    assert CURSOR_SECRET_VALUE not in repr(port.receipts)
    assert result["nested"][0]["arbitrary"] == "prefix:[REDACTED]:suffix"
    with pytest.raises(TypeError):
        result["nested"][0]["arbitrary"] = "mutated"
    with pytest.raises(TypeError):
        port.receipts[-1]["response"] = "mutated"


def test_receipts_resist_dict_base_class_and_nested_mutation():
    ports = _load_ports()
    client = FakeCursorCloudClient()
    port = _cursor_port(ports, client)
    port(operation="create", secret_ref=_SECRET_CURSOR, payload=_cursor_create_payload())

    receipt = port.receipts[-1]
    original = repr(port.receipts)
    with pytest.raises(TypeError):
        dict.__setitem__(receipt, "outcome", "forged")
    with pytest.raises(TypeError):
        receipt["response"]["agentId"] = "forged-agent"

    authoritative = port.receipts[-1]
    assert authoritative["outcome"] == "ok"
    assert authoritative["response"]["agentId"] == _cursor_ids()[1]
    assert repr(port.receipts) == original


def test_resolved_secret_is_redacted_from_mapping_keys_in_response_and_receipt():
    ports = _load_ports()

    class SafeNonStringKey:
        def __str__(self):
            return f"numeric:{CURSOR_SECRET_VALUE}:key"

    class SecretKeyCursor(FakeCursorCloudClient):
        def create_agent(self, payload):
            self.calls.append(("create_agent", payload))
            self.raw_response = {
                f"top:{CURSOR_SECRET_VALUE}": {
                    f"nested:{CURSOR_SECRET_VALUE}": "visible",
                    SafeNonStringKey(): "non-string",
                }
            }
            return self.raw_response

    client = SecretKeyCursor()
    port = _cursor_port(
        ports,
        client,
        credential_resolver=lambda _name: CURSOR_SECRET_VALUE,
    )
    result = port(
        operation="create",
        secret_ref=_SECRET_CURSOR,
        payload=_cursor_create_payload(),
    )

    assert CURSOR_SECRET_VALUE not in repr(result)
    assert CURSOR_SECRET_VALUE not in repr(port.receipts)
    top = result["top:[REDACTED]"]
    assert top["nested:[REDACTED]"] == "visible"
    assert top["numeric:[REDACTED]:key"] == "non-string"
    assert port.receipts[-1]["response"] == result
    assert f"top:{CURSOR_SECRET_VALUE}" in client.raw_response
    assert f"nested:{CURSOR_SECRET_VALUE}" in next(iter(client.raw_response.values()))
    with pytest.raises(TypeError):
        top["new"] = "mutated"


def test_provider_baseexception_is_converted_to_chainless_domain_error():
    ports = _load_ports()
    client = FakeCursorCloudClient()
    primary = KeyboardInterrupt("provider interrupt")
    client.create_error = primary
    port = _cursor_port(ports, client)
    with pytest.raises(ports.RequestPortError, match="provider request failed") as caught:
        port(operation="create", secret_ref=_SECRET_CURSOR, payload=_cursor_create_payload())
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def test_non_idempotent_reads_ignore_clock_collisions_and_return_fresh_results(
    monkeypatch,
):
    import agent.durable_jobs.request_ports as request_ports_module

    ports = _load_ports()
    monkeypatch.setattr(request_ports_module.time, "monotonic_ns", lambda: 7)

    class ChangingCursor(FakeCursorCloudClient):
        def get_run(self, agent_id, run_id):
            self.calls.append(("get_run", (agent_id, run_id)))
            return {
                "id": run_id,
                "agentId": agent_id,
                "status": f"STATUS-{len(self.calls)}",
            }

    cursor_client = ChangingCursor()
    cursor = _cursor_port(ports, cursor_client)
    status_key = "fresh-status-read"
    status_payload = {
        "idempotency_key": status_key,
        "agent_id": _cursor_ids(status_key)[1],
        "run_id": "run-1",
        "workspace_id": WORKSPACE,
        "repository_identity": REPO,
    }
    first_status = cursor(
        operation="status", secret_ref=_SECRET_CURSOR, payload=status_payload
    )
    second_status = cursor(
        operation="status", secret_ref=_SECRET_CURSOR, payload=status_payload
    )
    assert first_status["status"] == "STATUS-1"
    assert second_status["status"] == "STATUS-2"
    assert [call[0] for call in cursor_client.calls] == ["get_run", "get_run"]

    class ChangingSlack(FakeSlackClient):
        def conversations_replies(self, **kwargs):
            self.calls.append(("conversations_replies", dict(kwargs)))
            return {
                "ok": True,
                "messages": [
                    {
                        "client_msg_id": SLACK_KEY,
                        "text": f"fresh-{len(self.calls)}",
                    }
                ],
            }

    slack_client = ChangingSlack()
    slack = _slack_port(ports, slack_client)
    first_lookup = slack(
        operation="lookup", secret_ref=_SECRET_SLACK, payload=_slack_payload()
    )
    second_lookup = slack(
        operation="lookup", secret_ref=_SECRET_SLACK, payload=_slack_payload()
    )
    assert first_lookup["messages"][0]["text"] == "fresh-1"
    assert second_lookup["messages"][0]["text"] == "fresh-2"
    assert [call[0] for call in slack_client.calls] == [
        "conversations_replies",
        "conversations_replies",
    ]


def test_completed_read_state_is_not_retained_after_success_error_or_close(
    monkeypatch,
):
    import agent.durable_jobs.request_ports as request_ports_module

    ports = _load_ports()
    monkeypatch.setattr(request_ports_module.time, "monotonic_ns", lambda: 11)
    client = FakeCursorCloudClient()
    port = _cursor_port(ports, client)
    status_key = "completed-status-read"
    payload = {
        "idempotency_key": status_key,
        "agent_id": _cursor_ids(status_key)[1],
        "run_id": "run-1",
        "workspace_id": WORKSPACE,
        "repository_identity": REPO,
    }

    for index in range(500):
        result = port(
            operation="status", secret_ref=_SECRET_CURSOR, payload=payload
        )
        assert result["status"] == "CREATING"
        assert port._claims == {}, f"read state retained after call {index}"

    client.lookup_error = RuntimeError("read failed")
    lookup_payload = {
        "idempotency_key": CURSOR_KEY,
        "workspace_id": WORKSPACE,
        "repository_identity": REPO,
    }
    with pytest.raises(ports.RequestPortError, match="read failed"):
        port(
            operation="lookup",
            secret_ref=_SECRET_CURSOR,
            payload=lookup_payload,
        )
    assert port._claims == {}
    assert port._active_calls == 0
    assert port.close(timeout_seconds=1) is True
    assert port._claims == {}
    assert len(client.calls) == 501


def test_concurrent_non_idempotent_reads_never_coalesce(monkeypatch):
    import agent.durable_jobs.request_ports as request_ports_module

    ports = _load_ports()
    monkeypatch.setattr(request_ports_module.time, "monotonic_ns", lambda: 13)
    entered_two = threading.Event()
    release = threading.Event()

    class BlockingReads(FakeCursorCloudClient):
        def __init__(self):
            super().__init__()
            self._calls_lock = threading.Lock()

        def get_run(self, agent_id, run_id):
            with self._calls_lock:
                self.calls.append(("get_run", (agent_id, run_id)))
                ordinal = len(self.calls)
                if ordinal == 2:
                    entered_two.set()
            assert release.wait(1)
            return {"id": run_id, "agentId": agent_id, "status": f"READ-{ordinal}"}

    client = BlockingReads()
    port = _cursor_port(ports, client)
    status_key = "concurrent-status-read"
    payload = {
        "idempotency_key": status_key,
        "agent_id": _cursor_ids(status_key)[1],
        "run_id": "run-1",
        "workspace_id": WORKSPACE,
        "repository_identity": REPO,
    }
    results = []
    errors = []

    def read_status():
        try:
            results.append(
                port(
                    operation="status",
                    secret_ref=_SECRET_CURSOR,
                    payload=payload,
                    timeout_seconds=1,
                )
            )
        except BaseException as exc:
            errors.append(exc)

    callers = [threading.Thread(target=read_status) for _ in range(2)]
    for caller in callers:
        caller.start()
    try:
        assert entered_two.wait(0.5), "distinct reads were coalesced"
    finally:
        release.set()
        for caller in callers:
            caller.join(1)

    assert errors == []
    assert sorted(result["status"] for result in results) == ["READ-1", "READ-2"]
    assert [call[0] for call in client.calls] == ["get_run", "get_run"]
    assert port._claims == {}
    assert port._active_calls == 0


@pytest.mark.parametrize(
    "payload",
    [
        {
            "agent_id": _cursor_ids()[1],
            "run_id": "run-authority",
            "workspace_id": WORKSPACE,
            "repository_identity": REPO,
        },
        {
            "idempotency_key": {"not": "text"},
            "agent_id": _cursor_ids()[1],
            "run_id": "run-authority",
            "workspace_id": WORKSPACE,
            "repository_identity": REPO,
        },
        {
            "idempotency_key": CURSOR_KEY,
            "agent_id": _cursor_ids("foreign-cursor-key")[1],
            "run_id": "run-authority",
            "workspace_id": WORKSPACE,
            "repository_identity": REPO,
        },
        {
            "idempotency_key": "foreign-cursor-key",
            "agent_id": _cursor_ids()[1],
            "run_id": "run-authority",
            "workspace_id": WORKSPACE,
            "repository_identity": REPO,
        },
    ],
)
def test_cursor_status_requires_exact_originating_correlation_before_resolver(payload):
    ports = _load_ports()
    client = FakeCursorCloudClient()
    resolver_calls = []
    port = _cursor_port(
        ports,
        client,
        credential_resolver=lambda name: resolver_calls.append(name) or CURSOR_SECRET_VALUE,
    )

    with pytest.raises(ports.RequestPortMismatch):
        port(operation="status", secret_ref=_SECRET_CURSOR, payload=payload)

    assert resolver_calls == []
    assert client.calls == []


def test_cursor_status_dispatches_only_the_agent_derived_from_originating_key():
    ports = _load_ports()
    client = FakeCursorCloudClient()
    port = _cursor_port(ports, client)
    expected_agent = _cursor_ids()[1]
    client.runs[(expected_agent, "run-authority")] = {
        "id": "run-authority",
        "agentId": expected_agent,
        "status": "FINISHED",
    }

    result = port(
        operation="status",
        secret_ref=_SECRET_CURSOR,
        payload={
            "idempotency_key": CURSOR_KEY,
            "agent_id": expected_agent,
            "run_id": "run-authority",
            "workspace_id": WORKSPACE,
            "repository_identity": REPO,
        },
    )

    assert result["status"] == "FINISHED"
    assert client.calls == [("get_run", (expected_agent, "run-authority"))]


@pytest.mark.parametrize("kind", ["cursor", "slack-write", "slack-read"])
def test_resolver_time_consumes_the_single_absolute_request_deadline(monkeypatch, kind):
    import agent.durable_jobs.request_ports as request_ports_module

    ports = _load_ports()
    now = [100.0]
    monkeypatch.setattr(request_ports_module.time, "monotonic", lambda: now[0])

    def resolver(_name):
        now[0] = 100.2
        return CURSOR_SECRET_VALUE

    if kind == "cursor":
        client = FakeCursorCloudClient()
        port = _cursor_port(ports, client, credential_resolver=resolver)
        operation, secret_ref, payload = "create", _SECRET_CURSOR, _cursor_create_payload()
    else:
        client = FakeSlackClient()
        port = _slack_port(ports, client, credential_resolver=resolver)
        operation = "post_root" if kind == "slack-write" else "lookup"
        secret_ref, payload = _SECRET_SLACK, _slack_payload()

    with pytest.raises(ports.RequestPortTimeout, match="timeout"):
        port(
            operation=operation,
            secret_ref=secret_ref,
            payload=payload,
            timeout_seconds=0.1,
        )

    assert client.calls == []
    assert port._active_calls == 0
    assert port._claims == {}
    assert port.close(timeout_seconds=1) is True


def test_worker_completion_crossing_remaining_absolute_budget_still_fails_closed(monkeypatch):
    import agent.durable_jobs.request_ports as request_ports_module

    ports = _load_ports()
    now = [200.0]
    monkeypatch.setattr(request_ports_module.time, "monotonic", lambda: now[0])

    def resolver(_name):
        now[0] = 200.09
        return CURSOR_SECRET_VALUE

    class DeadlineCrossingCursor(FakeCursorCloudClient):
        def create_agent(self, payload):
            now[0] = 200.11
            return super().create_agent(payload)

    client = DeadlineCrossingCursor()
    port = _cursor_port(ports, client, credential_resolver=resolver)
    with pytest.raises(ports.RequestPortTimeout, match="timeout"):
        port(
            operation="create",
            secret_ref=_SECRET_CURSOR,
            payload=_cursor_create_payload(key="absolute-budget-crossing"),
            timeout_seconds=0.1,
        )
    assert port.close(timeout_seconds=1) is True
    assert port._active_calls == 0


@pytest.mark.parametrize("kind", ["idempotent-write", "non-idempotent-read"])
def test_worker_start_failure_rolls_back_accounting_claim_and_allows_retry(
    monkeypatch, kind
):
    import agent.durable_jobs.request_ports as request_ports_module

    ports = _load_ports()
    real_thread = threading.Thread
    attempts = []

    class FailFirstThread:
        def __init__(self, *args, **kwargs):
            self._thread = real_thread(*args, **kwargs)

        def start(self):
            attempts.append("start")
            if len(attempts) == 1:
                raise RuntimeError(f"worker refused {CURSOR_SECRET_VALUE}")
            return self._thread.start()

    monkeypatch.setattr(request_ports_module.threading, "Thread", FailFirstThread)
    client = FakeCursorCloudClient()
    port = _cursor_port(ports, client)
    if kind == "idempotent-write":
        operation = "create"
        payload = _cursor_create_payload(key="start-failure-retry")
    else:
        operation = "status"
        key = "start-failure-read"
        payload = {
            "idempotency_key": key,
            "agent_id": _cursor_ids(key)[1],
            "run_id": "run-start-failure",
            "workspace_id": WORKSPACE,
            "repository_identity": REPO,
        }

    with pytest.raises(ports.RequestPortError) as caught:
        port(operation=operation, secret_ref=_SECRET_CURSOR, payload=payload)
    assert CURSOR_SECRET_VALUE not in str(caught.value)
    assert client.calls == []
    assert port._active_calls == 0
    assert port._claims == {}

    result = port(operation=operation, secret_ref=_SECRET_CURSOR, payload=payload)
    assert result is not None
    assert len(client.calls) == 1
    assert port._active_calls == 0
    if kind == "non-idempotent-read":
        assert port._claims == {}
    assert port.close(timeout_seconds=1) is True


@pytest.mark.parametrize("provider", ["cursor", "slack"])
def test_completed_write_claim_capacity_is_permanent_and_reads_do_not_consume_it(
    monkeypatch, provider
):
    import agent.durable_jobs.request_ports as request_ports_module

    ports = _load_ports()
    monkeypatch.setattr(
        request_ports_module, "_MAX_PERMANENT_WRITE_CLAIMS", 2, raising=False
    )
    resolver_calls = []

    def resolver(name):
        resolver_calls.append(name)
        return "resolved-secret"

    if provider == "cursor":
        client = FakeCursorCloudClient()
        port = _cursor_port(ports, client, credential_resolver=resolver)
        operation, secret_ref = "create", _SECRET_CURSOR
        payload_for = _cursor_create_payload
        read_operation = "lookup"
    else:
        client = FakeSlackClient()
        port = _slack_port(ports, client, credential_resolver=resolver)
        operation, secret_ref = "post_root", _SECRET_SLACK
        payload_for = _slack_payload
        read_operation = "lookup"

    first_key = f"p24-{provider}-permanent-1"
    second_key = f"p24-{provider}-permanent-2"
    first = port(
        operation=operation, secret_ref=secret_ref, payload=payload_for(first_key)
    )
    # The constructor must snapshot the validated module-private production limit.
    monkeypatch.setattr(request_ports_module, "_MAX_PERMANENT_WRITE_CLAIMS", 1)
    port(operation=operation, secret_ref=secret_ref, payload=payload_for(second_key))
    assert len(port._claims) == 2

    resolver_count = len(resolver_calls)
    provider_count = len(client.calls)
    with pytest.raises(ports.RequestPortError, match="capacity"):
        port(
            operation=operation,
            secret_ref=secret_ref,
            payload=payload_for(f"p24-{provider}-capacity-rejected"),
        )
    assert len(resolver_calls) == resolver_count
    assert len(client.calls) == provider_count
    assert port._active_calls == 0

    replay = port(
        operation=operation, secret_ref=secret_ref, payload=payload_for(first_key)
    )
    assert replay == first
    assert len(resolver_calls) == resolver_count
    assert len(client.calls) == provider_count
    with pytest.raises(TypeError):
        replay["p24_mutation"] = "forbidden"

    # Reads remain non-retained and continue to dispatch even at write capacity.
    port(
        operation=read_operation,
        secret_ref=secret_ref,
        payload=payload_for(first_key),
    )
    assert len(port._claims) == 2
    assert len(client.calls) == provider_count + 1
    assert len(resolver_calls) == resolver_count + 1
    assert port._active_calls == 0
    assert port.close(timeout_seconds=1) is True


@pytest.mark.parametrize("provider", ["cursor", "slack"])
def test_concurrent_unique_writes_racing_for_final_slot_dispatch_exactly_once(
    monkeypatch, provider
):
    import agent.durable_jobs.request_ports as request_ports_module

    ports = _load_ports()
    monkeypatch.setattr(
        request_ports_module, "_MAX_PERMANENT_WRITE_CLAIMS", 1, raising=False
    )
    provider_entered = threading.Event()
    provider_release = threading.Event()
    start = threading.Barrier(3)
    resolver_calls = []

    if provider == "cursor":
        class BlockingClient(FakeCursorCloudClient):
            def create_agent(self, payload):
                provider_entered.set()
                assert provider_release.wait(1)
                return super().create_agent(payload)

        client = BlockingClient()
        port = _cursor_port(
            ports,
            client,
            credential_resolver=lambda name: resolver_calls.append(name) or "secret",
        )
        operation, secret_ref, payload_for = (
            "create",
            _SECRET_CURSOR,
            _cursor_create_payload,
        )
    else:
        class BlockingClient(FakeSlackClient):
            def chat_postMessage(self, **kwargs):
                provider_entered.set()
                assert provider_release.wait(1)
                return super().chat_postMessage(**kwargs)

        client = BlockingClient()
        port = _slack_port(
            ports,
            client,
            credential_resolver=lambda name: resolver_calls.append(name) or "secret",
        )
        operation, secret_ref, payload_for = "post_root", _SECRET_SLACK, _slack_payload

    results = []
    errors = []

    def write(key):
        start.wait()
        try:
            results.append(
                port(operation=operation, secret_ref=secret_ref, payload=payload_for(key))
            )
        except BaseException as exc:
            errors.append(exc)

    callers = [
        threading.Thread(target=write, args=(f"p24-{provider}-race-{index}",))
        for index in range(2)
    ]
    for caller in callers:
        caller.start()
    start.wait()
    try:
        assert provider_entered.wait(1)
    finally:
        provider_release.set()
        for caller in callers:
            caller.join(1)

    assert all(not caller.is_alive() for caller in callers)
    assert len(results) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], ports.RequestPortError)
    assert "capacity" in str(errors[0])
    assert len(resolver_calls) == 1
    assert len(client.calls) == 1
    assert len(port._claims) == 1
    assert port._active_calls == 0
    assert port.close(timeout_seconds=1) is True


@pytest.mark.parametrize("provider", ["cursor", "slack"])
@pytest.mark.parametrize("failure_phase", ["construction", "start"])
def test_failed_worker_creation_releases_only_new_claim_and_reuses_capacity(
    monkeypatch, provider, failure_phase
):
    import agent.durable_jobs.request_ports as request_ports_module

    ports = _load_ports()
    monkeypatch.setattr(
        request_ports_module, "_MAX_PERMANENT_WRITE_CLAIMS", 1, raising=False
    )
    real_thread = threading.Thread
    attempts = []

    class FailFirstWorker:
        def __init__(self, *args, **kwargs):
            attempts.append("construction")
            if len(attempts) == 1 and failure_phase == "construction":
                raise RuntimeError("p24 construction refused")
            self._thread = real_thread(*args, **kwargs)

        def start(self):
            if len(attempts) == 1 and failure_phase == "start":
                raise RuntimeError("p24 start refused")
            return self._thread.start()

    monkeypatch.setattr(request_ports_module.threading, "Thread", FailFirstWorker)
    if provider == "cursor":
        client = FakeCursorCloudClient()
        port = _cursor_port(ports, client)
        operation, secret_ref, payload_for = (
            "create",
            _SECRET_CURSOR,
            _cursor_create_payload,
        )
    else:
        client = FakeSlackClient()
        port = _slack_port(ports, client)
        operation, secret_ref, payload_for = "post_root", _SECRET_SLACK, _slack_payload

    with pytest.raises(ports.RequestPortError, match="worker failed to start"):
        port(
            operation=operation,
            secret_ref=secret_ref,
            payload=payload_for(f"p24-{provider}-{failure_phase}-failed"),
        )
    assert client.calls == []
    assert port._claims == {}
    assert port._active_calls == 0

    result = port(
        operation=operation,
        secret_ref=secret_ref,
        payload=payload_for(f"p24-{provider}-{failure_phase}-reused"),
    )
    assert result is not None
    assert len(client.calls) == 1
    assert len(port._claims) == 1
    assert port._active_calls == 0
    assert port.close(timeout_seconds=1) is True


def test_permanent_write_capacity_constant_is_strictly_validated(monkeypatch):
    import agent.durable_jobs.request_ports as request_ports_module

    ports = _load_ports()
    client = FakeCursorCloudClient()
    for invalid in (True, 0, -1, 1.5, "2"):
        monkeypatch.setattr(
            request_ports_module,
            "_MAX_PERMANENT_WRITE_CLAIMS",
            invalid,
            raising=False,
        )
        with pytest.raises((TypeError, ValueError), match="capacity"):
            _cursor_port(ports, client)
    assert client.calls == []
