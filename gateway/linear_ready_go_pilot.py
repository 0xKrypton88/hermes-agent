"""Activation-gated pilot / dispatch bridge (intentionally unimplemented).

Creating a non-dispatched ``LaunchIntent`` is the terminal local Go outcome.
Pilot activation (Cursor / LangGraph / agent dispatch, listener binding, or
live delivery) requires a separate approved control-plane change.
"""

from __future__ import annotations


class ActivationGatedError(RuntimeError):
    """Raised when pilot dispatch is requested before activation approval."""


def start_pilot_dispatch(*_args, **_kwargs):  # pragma: no cover - activation-gated
    """Refuse pilot / agent dispatch from the local Ready/Go plane."""
    raise ActivationGatedError(
        "linear_ready_go_pilot is activation-gated and unimplemented; "
        "LaunchIntent.dispatched remains False and no work is started"
    )


def arm_pilot_listener(*_args, **_kwargs):  # pragma: no cover - activation-gated
    """Refuse listener / webhook arming."""
    raise ActivationGatedError(
        "pilot listener arming is activation-gated; receipt-only webhook "
        "behavior remains untouched and unwired to this control plane"
    )
