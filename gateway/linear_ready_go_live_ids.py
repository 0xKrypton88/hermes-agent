"""Activation-gated live Linear identity binding (intentionally unimplemented).

This package exists only as a fail-closed placeholder. Binding real Linear
workspace / team / state IDs is an operator activation step and is not part of
the local Ready/Go control plane.

Do not import this module from Ready freeze, store, or Go control paths.
"""

from __future__ import annotations


class ActivationGatedError(RuntimeError):
    """Raised when live-ID binding is requested before activation approval."""


def bind_live_linear_ids(*_args, **_kwargs):  # pragma: no cover - activation-gated
    """Refuse live Linear ID binding until a separate activation brief lands."""
    raise ActivationGatedError(
        "linear_ready_go_live_ids is activation-gated and unimplemented; "
        "local Ready/Go uses synthetic/canonical identities only"
    )
