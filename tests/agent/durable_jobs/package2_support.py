"""Shared Package 2 test helpers. No Slack SDK / plugin adapter imports."""

from __future__ import annotations

from typing import Any, Mapping


def bind_runtime_secret_env(monkeypatch) -> None:
    monkeypatch.setenv("CURSOR_API_KEY", "cursor-test-ref-value")
    monkeypatch.setenv("SLACK_BOT_TOKEN", "slack-test-ref-value")


def idle_injected_transports():
    from agent.durable_jobs.injected_transports import (
        CursorCloudInjectedTransport,
        SlackInjectedTransport,
    )
    from agent.durable_jobs.request_ports import (
        CursorCloudInjectedRequestPort,
        SlackInjectedRequestPort,
    )

    class _IdleCursorClient:
        def create_agent(self, _payload):
            raise AssertionError("transport must stay idle during attach")

        def get_agent(self, _agent_id):
            raise AssertionError("transport must stay idle during attach")

        def get_run(self, _agent_id, _run_id):
            raise AssertionError("transport must stay idle during attach")

    class _IdleSlackClient:
        def chat_postMessage(self, **_kwargs):
            raise AssertionError("transport must stay idle during attach")

        def conversations_replies(self, **_kwargs):
            raise AssertionError("transport must stay idle during attach")

    def _no_network_resolver(secret_ref: str) -> str:
        return f"test-only:{secret_ref}"

    cursor_request = CursorCloudInjectedRequestPort(
        client=_IdleCursorClient(),
        secret_ref="CURSOR_API_KEY",
        workspace_id="T1",
        repository_identity="github.com/example/repo",
        credential_resolver=_no_network_resolver,
    )
    slack_request = SlackInjectedRequestPort(
        client=_IdleSlackClient(),
        secret_ref="SLACK_BOT_TOKEN",
        workspace_id="T1",
        channel_id="C123",
        repository_identity="github.com/example/repo",
        root_thread_ts="111.222",
        credential_resolver=_no_network_resolver,
    )
    return (
        CursorCloudInjectedTransport(
            request=cursor_request, secret_ref="CURSOR_API_KEY"
        ),
        SlackInjectedTransport(request=slack_request, secret_ref="SLACK_BOT_TOKEN"),
    )


def runtime_ready_transport_kwargs(monkeypatch) -> dict[str, Any]:
    bind_runtime_secret_env(monkeypatch)
    cursor, slack = idle_injected_transports()
    return {"cursor_transport": cursor, "slack_transport": slack}


def attach_runtime_ready_lane(
    *,
    raw_config: Mapping[str, Any] | None,
    monkeypatch,
    **kwargs: Any,
):
    from gateway.durable_job_lane import attach_durable_job_lane

    kwargs.setdefault("raw_config", raw_config)
    kwargs.update(runtime_ready_transport_kwargs(monkeypatch))
    return attach_durable_job_lane(**kwargs)
