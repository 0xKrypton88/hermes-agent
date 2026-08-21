"""External adapter *ports* for ENG-3 durable-jobs slices.

This package ships only protocols and inert null adapters. Live Slack / Cursor /
network adapters are intentionally absent from this module. The ENG-30 Cursor
Cloud adapter (``cursor_cloud.CursorCloudAdapter``) and ENG-31 Slack client
bridge (``slack_bridge.SlackClientBridge``) require an injected transport —
they are not exported here. Package 2 production-shaped transports live in
``injected_transports`` and still never construct an HTTP/SDK client or read
credential values. Even when a fake is injected into ``DurableJobService``,
``attempt_dispatch`` stays hard-disabled and never calls ``dispatch()``.
ENG-26/27 ledgers talk only to injected fakes after a durable claim/binding.
It must be impossible for this slice to call Slack/Cursor/network.
"""

from __future__ import annotations

from typing import Protocol


class DispatchPort(Protocol):
    """Injected-only dispatch seam (no live implementation in Package 1)."""

    def dispatch(self, job_id: str) -> None: ...


class NullDispatchAdapter:
    """Safe stand-in that never performs I/O."""

    def dispatch(self, job_id: str) -> None:
        raise RuntimeError(
            "NullDispatchAdapter refuses dispatch; inject an explicit fake "
            "in tests. Live Slack/Cursor adapters are out of Package 1 scope."
        )


class CursorProviderPort(Protocol):
    """Injected-only Cursor create/lookup seam (no live client)."""

    def create_run(self, *, idempotency_key: str, job_id: str) -> object: ...

    def lookup_runs(self, *, idempotency_key: str) -> list[object]: ...

    def status_run(self, *, run_id: str) -> object: ...


class NullCursorProvider:
    """Safe stand-in that never performs I/O or Cursor API calls."""

    def create_run(self, *, idempotency_key: str, job_id: str) -> object:
        raise RuntimeError(
            "NullCursorProvider refuses create_run; inject an explicit fake "
            "in tests. Live Cursor adapters are out of durable-lane scope."
        )

    def lookup_runs(self, *, idempotency_key: str) -> list[object]:
        raise RuntimeError(
            "NullCursorProvider refuses lookup_runs; inject an explicit fake "
            "in tests. Live Cursor adapters are out of durable-lane scope."
        )

    def status_run(self, *, run_id: str) -> object:
        raise RuntimeError(
            "NullCursorProvider refuses status_run; inject an explicit fake "
            "in tests. Live Cursor adapters are out of durable-lane scope."
        )


class SlackMessagePort(Protocol):
    """Injected-only Slack post/lookup seam (no live client)."""

    def post_root(
        self,
        *,
        client_msg_id: str,
        workspace_id: str,
        channel_id: str,
        root_thread_ts: str,
        job_id: str,
    ) -> object: ...

    def lookup_by_client_msg_id(self, client_msg_id: str) -> list[object]: ...


class NullSlackPort:
    """Safe stand-in that never performs I/O or Slack API calls."""

    def post_root(
        self,
        *,
        client_msg_id: str,
        workspace_id: str,
        channel_id: str,
        root_thread_ts: str,
        job_id: str,
    ) -> object:
        raise RuntimeError(
            "NullSlackPort refuses post_root; inject an explicit fake in tests. "
            "Live Slack adapters are out of durable-lane scope."
        )

    def lookup_by_client_msg_id(self, client_msg_id: str) -> list[object]:
        raise RuntimeError(
            "NullSlackPort refuses lookup; inject an explicit fake in tests. "
            "Live Slack adapters are out of durable-lane scope."
        )
