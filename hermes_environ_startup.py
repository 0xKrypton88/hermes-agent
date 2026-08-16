"""Trusted process-environment witness captured at Hermes entry.

Importing this module does not remember or pin anything. A site ``.pth``
hook calls ``remember_process_origin()`` at interpreter start, before
user code or plugins can replace ``os.environ``. Real process entry
points then call ``capture_trusted_startup()`` before loading plugins or
durable-job preflight.

Capture succeeds only when the current mappings are still the objects
recorded as process origin. A pre-import exact ``os._Environ`` whose
``_data`` is a copy, or a POSIX triple-rebind of that copy onto
``os.environ`` / ``os.environb`` / ``posix.environ``, cannot prove
origin and fails closed. Late import of this module, ``hermes_constants``,
or ``gateway.run`` does not self-attest mutable ``os`` / ``posix`` state.

Preflight never calls these functions and never imports this module to
create a pin; it only reads pins if a prior capture already succeeded.

Identity only: never iterates environment keys and never reads or
compares secret values.
"""

from __future__ import annotations

import os
import sys

_ORIGIN_RECORDED = False
_ORIGIN_OS_ENVIRON = None
_ORIGIN_POSIX_ENVIRON = None
_TRUSTED_CAPTURE_READY = False
_PINNED_OS_ENVIRON = None
_PINNED_POSIX_ENVIRON = None


def _current_os_environ():
    try:
        return object.__getattribute__(os, "environ")
    except AttributeError:
        return None


def _current_posix_environ():
    try:
        platform = object.__getattribute__(sys, "platform")
    except AttributeError:
        return None
    if type(platform) is not str or str.__eq__(platform, "win32"):
        return None
    try:
        modules = object.__getattribute__(sys, "modules")
    except AttributeError:
        return None
    if type(modules) is not dict:
        return None
    posix = dict.get(modules, "posix")
    if posix is None:
        return None
    try:
        mapping = object.__getattribute__(posix, "environ")
    except AttributeError:
        return None
    if type(mapping) is not dict:
        return None
    return mapping


def remember_process_origin() -> bool:
    """Record interpreter-original mapping identities. Does not pin.

    First call wins. Intended for a site ``.pth`` hook at interpreter
    start, while ``os.environ`` is still the process original. Importing
    this module does not remember.
    """
    global _ORIGIN_RECORDED, _ORIGIN_OS_ENVIRON, _ORIGIN_POSIX_ENVIRON
    if _ORIGIN_RECORDED:
        return _ORIGIN_OS_ENVIRON is not None
    environ = _current_os_environ()
    _ORIGIN_OS_ENVIRON = environ
    _ORIGIN_POSIX_ENVIRON = _current_posix_environ()
    _ORIGIN_RECORDED = True
    return environ is not None


def capture_trusted_startup() -> bool:
    """Pin original ``os.environ`` / ``posix.environ`` identities.

    Succeeds only when a prior ``remember_process_origin()`` recorded
    the interpreter-original objects and the current mappings are still
    those objects. First successful call wins. A later call after a
    mapping replacement does not overwrite the pin. Importing this
    module does not capture. Missing origin is not provenance.
    """
    global _TRUSTED_CAPTURE_READY, _PINNED_OS_ENVIRON, _PINNED_POSIX_ENVIRON
    if _TRUSTED_CAPTURE_READY:
        return _PINNED_OS_ENVIRON is not None
    if _ORIGIN_RECORDED is not True or _ORIGIN_OS_ENVIRON is None:
        _TRUSTED_CAPTURE_READY = True
        return False
    environ = _current_os_environ()
    if environ is None or environ is not _ORIGIN_OS_ENVIRON:
        _TRUSTED_CAPTURE_READY = True
        return False
    try:
        environ_type = object.__getattribute__(os, "_Environ")
    except AttributeError:
        _TRUSTED_CAPTURE_READY = True
        return False
    if type(environ) is not environ_type:
        _TRUSTED_CAPTURE_READY = True
        return False
    try:
        data = object.__getattribute__(environ, "_data")
    except AttributeError:
        _TRUSTED_CAPTURE_READY = True
        return False
    if type(data) is not dict:
        _TRUSTED_CAPTURE_READY = True
        return False
    try:
        platform = object.__getattribute__(sys, "platform")
    except AttributeError:
        platform = None
    if type(platform) is str and str.__eq__(platform, "win32"):
        try:
            object.__getattribute__(os, "environb")
        except AttributeError:
            pass
        else:
            _TRUSTED_CAPTURE_READY = True
            return False
        _PINNED_OS_ENVIRON = environ
        _TRUSTED_CAPTURE_READY = True
        return True
    origin_posix = _ORIGIN_POSIX_ENVIRON
    live_posix = _current_posix_environ()
    if (
        origin_posix is None
        or type(origin_posix) is not dict
        or live_posix is None
        or live_posix is not origin_posix
        or data is not origin_posix
    ):
        _TRUSTED_CAPTURE_READY = True
        return False
    try:
        environb = object.__getattribute__(os, "environb")
    except AttributeError:
        _TRUSTED_CAPTURE_READY = True
        return False
    if type(environb) is not environ_type:
        _TRUSTED_CAPTURE_READY = True
        return False
    try:
        sibling = object.__getattribute__(environb, "_data")
    except AttributeError:
        _TRUSTED_CAPTURE_READY = True
        return False
    if sibling is not origin_posix:
        _TRUSTED_CAPTURE_READY = True
        return False
    _PINNED_OS_ENVIRON = environ
    _PINNED_POSIX_ENVIRON = origin_posix
    _TRUSTED_CAPTURE_READY = True
    return True


def trusted_startup_ready() -> bool:
    """True when a prior ``capture_trusted_startup()`` pinned ``os.environ``."""
    return _TRUSTED_CAPTURE_READY is True and _PINNED_OS_ENVIRON is not None
