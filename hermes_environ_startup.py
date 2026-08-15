"""Trusted process-environment witness captured at Hermes entry.

Importing this module does not pin anything. Real process entry points
must call ``capture_trusted_startup()`` before loading plugins or durable-job
preflight. Preflight never calls this function and never imports this
module to create a pin; it only reads pins if a prior capture already
succeeded.

Identity-only behavior:

- The capture function records environment singletons and the current
  platform once.
- No secrets are read. No environment values are iterated or compared.
- No module-level startup attributes are reused as mutable witnesses.
"""

from __future__ import annotations

import os
import sys


def _startup_pin_state():
    """Return callables that hold the startup boundary in closure state."""
    ready = False
    pinned_os = None
    pinned_posix = None

    def capture_trusted_startup() -> bool:
        nonlocal ready
        nonlocal pinned_os
        nonlocal pinned_posix
        if ready:
            return pinned_os is not None

        try:
            environ = object.__getattribute__(os, "environ")
        except AttributeError:
            ready = True
            return False

        pinned_os = environ
        try:
            platform = object.__getattribute__(sys, "platform")
        except AttributeError:
            platform = None

        if type(platform) is str and not str.__eq__(platform, "win32"):
            try:
                modules = object.__getattribute__(sys, "modules")
            except AttributeError:
                modules = None
            if type(modules) is dict:
                posix = dict.get(modules, "posix")
                if posix is not None:
                    try:
                        mapping = object.__getattribute__(posix, "environ")
                    except AttributeError:
                        mapping = None
                    if type(mapping) is dict:
                        pinned_posix = mapping

        ready = True
        return True

    def trusted_startup_ready() -> bool:
        return ready is True and pinned_os is not None

    def startup_pin_snapshot():
        return ready, pinned_os, pinned_posix

    return capture_trusted_startup, trusted_startup_ready, startup_pin_snapshot


(_capture_trusted_startup, _trusted_startup_ready, _startup_pin_snapshot) = _startup_pin_state()


def capture_trusted_startup() -> bool:
    """Pin original ``os.environ`` / ``posix.environ`` identities."""
    return _capture_trusted_startup()


def trusted_startup_ready() -> bool:
    """True when a prior ``capture_trusted_startup()`` pinned ``os.environ``."""
    try:
        return bool(_trusted_startup_ready())
    except Exception:
        return False


def startup_pin_snapshot():
    """Return the raw pinned witness tuple for internal preflight checks."""
    try:
        ready, pinned_os, pinned_posix = _startup_pin_snapshot()
    except Exception:
        return False, None, None
    return ready, pinned_os, pinned_posix
