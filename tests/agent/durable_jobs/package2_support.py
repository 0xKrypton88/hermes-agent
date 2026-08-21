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

    def _idle(**_k):
        raise AssertionError("transport must stay idle during attach")

    return (
        CursorCloudInjectedTransport(request=_idle, secret_ref="CURSOR_API_KEY"),
        SlackInjectedTransport(request=_idle, secret_ref="SLACK_BOT_TOKEN"),
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
