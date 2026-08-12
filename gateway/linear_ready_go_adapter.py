"""Activation-gated Linear mutation / webhook adapter (intentionally unimplemented).

Ready/Go local control plane must not open Linear API clients, register
webhooks, or apply comment/state mutations. This module documents that boundary
and fails closed if invoked.
"""

from __future__ import annotations


class ActivationGatedError(RuntimeError):
    """Raised when a live Linear adapter is requested before activation."""


def connect_linear_adapter(*_args, **_kwargs):  # pragma: no cover - activation-gated
    """Refuse live Linear adapter construction."""
    raise ActivationGatedError(
        "linear_ready_go_adapter is activation-gated and unimplemented; "
        "no Linear API, webhook registration, or mutation client is available"
    )


def apply_ready_mutation(*_args, **_kwargs):  # pragma: no cover - activation-gated
    """Refuse Ready comment/transition mutations."""
    raise ActivationGatedError(
        "Ready mutation adapter is activation-gated; local control plane "
        "persists Ready receipts only"
    )
