"""External adapter *ports* for ENG-3 Package 1.

Package 1 ships only protocols and an inert null adapter. Live Slack / Cursor /
network adapters are intentionally absent. Even when a fake is injected into
``DurableJobService``, Package 1 hard-disables dispatch and never calls
``dispatch()``. It must be impossible for this slice to call Slack/Cursor/network.
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
