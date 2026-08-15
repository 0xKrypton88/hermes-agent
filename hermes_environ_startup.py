"""Trusted process-environment witness captured at Hermes entry.

Importing this module does not pin anything. Real process entry points
must call ``capture_trusted_startup()`` before loading plugins or
durable-job preflight. Preflight never calls this function and never
imports this module to create a pin; it only reads pins if a prior
capture already succeeded.

Identity only: never iterates environment keys and never reads or
compares secret values.
"""

from __future__ import annotations

import os
import sys

_TRUSTED_CAPTURE_READY = False
_PINNED_OS_ENVIRON = None
_PINNED_POSIX_ENVIRON = None


def capture_trusted_startup() -> bool:
    """Pin original ``os.environ`` / ``posix.environ`` identities.

    First successful call wins. A later call after a mapping replacement
    does not overwrite the pin. Importing this module does not capture.
    """
    global _TRUSTED_CAPTURE_READY, _PINNED_OS_ENVIRON, _PINNED_POSIX_ENVIRON
    if _TRUSTED_CAPTURE_READY:
        return _PINNED_OS_ENVIRON is not None
    try:
        environ = object.__getattribute__(os, "environ")
    except AttributeError:
        _TRUSTED_CAPTURE_READY = True
        return False
    _PINNED_OS_ENVIRON = environ
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
                    _PINNED_POSIX_ENVIRON = mapping
    _TRUSTED_CAPTURE_READY = True
    return True


def trusted_startup_ready() -> bool:
    """True when a prior ``capture_trusted_startup()`` pinned ``os.environ``."""
    return _TRUSTED_CAPTURE_READY is True and _PINNED_OS_ENVIRON is not None
