"""Trusted process-environment witness captured at Hermes entry.

Importing this module does not remember or pin anything. Interpreter
start records origin via a source-checkout ``sitecustomize`` (when the
checkout is on ``PYTHONPATH`` during ``site.main()``) and, when
installed, a site ``.pth`` copied only into that install's destination.
Both call ``remember_process_origin()`` before user code or plugins can
replace ``os.environ``. Real process entry points then call
``capture_trusted_startup()`` before loading plugins or durable-job
preflight.

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

The process-bound trust root is a write-once seal on the genuine
``os.environ`` instance dict. Module globals, ``sys.modules`` entries,
and a later ``types.ModuleType`` injection are not provenance.
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
_MISSING = object()
_ORIGIN_SEAL_ATTR = "__hermes_trusted_environ_origin__"
_PIN_SEAL_ATTR = "__hermes_trusted_environ_pin__"


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


def _environ_instance_storage(environ):
    """Return the genuine ``os._Environ`` instance dict, or None."""
    if environ is None:
        return None
    try:
        storage = object.__getattribute__(environ, "__dict__")
    except AttributeError:
        return None
    if type(storage) is not dict:
        return None
    return storage


def _seal_get(storage, name):
    """Return the ``(environ, posix)`` tuple stored under ``name``, or None.

    Walks ``dict.items`` and matches an exact ``str`` key with ``str.__eq__``.
    """
    if type(storage) is not dict or type(name) is not str:
        return None
    found = _MISSING
    try:
        items = dict.items(storage)
    except Exception:
        return None
    for pair in items:
        if type(pair) is not tuple or tuple.__len__(pair) != 2:
            return None
        key = tuple.__getitem__(pair, 0)
        value = tuple.__getitem__(pair, 1)
        if type(key) is not str:
            continue
        if str.__eq__(key, name):
            if found is not _MISSING:
                return None
            found = value
    if found is _MISSING:
        return None
    if type(found) is not tuple or tuple.__len__(found) != 2:
        return None
    return found


def _seal_put(storage, name, payload) -> bool:
    """Write-once seal. Fails if ``name`` is already a key."""
    if type(storage) is not dict or type(name) is not str:
        return False
    if type(payload) is not tuple or tuple.__len__(payload) != 2:
        return False
    if _seal_get(storage, name) is not None:
        return False
    try:
        dict.__setitem__(storage, name, payload)
    except Exception:
        return False
    return _seal_get(storage, name) is payload


def remember_process_origin() -> bool:
    """Record interpreter-original mapping identities. Does not pin.

    First call wins. Intended for a site ``.pth`` hook at interpreter
    start, while ``os.environ`` is still the process original. Importing
    this module does not remember.
    """
    global _ORIGIN_RECORDED, _ORIGIN_OS_ENVIRON, _ORIGIN_POSIX_ENVIRON
    environ = _current_os_environ()
    storage = _environ_instance_storage(environ)
    existing = _seal_get(storage, _ORIGIN_SEAL_ATTR)
    if existing is not None:
        _ORIGIN_OS_ENVIRON = existing[0]
        _ORIGIN_POSIX_ENVIRON = existing[1]
        _ORIGIN_RECORDED = True
        return existing[0] is environ
    if _ORIGIN_RECORDED:
        return False
    posix = _current_posix_environ()
    _ORIGIN_RECORDED = True
    if environ is None or storage is None:
        return False
    payload = (environ, posix)
    if not _seal_put(storage, _ORIGIN_SEAL_ATTR, payload):
        return False
    _ORIGIN_OS_ENVIRON = environ
    _ORIGIN_POSIX_ENVIRON = posix
    return True


def _fail_capture(storage) -> bool:
    """Lock capture closed. Write-once tombstone so a later plant cannot mint."""
    global _TRUSTED_CAPTURE_READY
    _TRUSTED_CAPTURE_READY = True
    if storage is not None:
        _seal_put(storage, _PIN_SEAL_ATTR, (None, None))
    return False


def capture_trusted_startup() -> bool:
    """Pin original ``os.environ`` / ``posix.environ`` identities.

    Succeeds only when a prior ``remember_process_origin()`` recorded
    the interpreter-original objects and the current mappings are still
    those objects. First successful call wins. A later call after a
    mapping replacement does not overwrite the pin. Importing this
    module does not capture. Missing origin is not provenance.
    """
    global _TRUSTED_CAPTURE_READY, _PINNED_OS_ENVIRON, _PINNED_POSIX_ENVIRON
    environ = _current_os_environ()
    storage = _environ_instance_storage(environ)
    existing_pin = _seal_get(storage, _PIN_SEAL_ATTR)
    if existing_pin is not None:
        pinned_environ = existing_pin[0]
        if pinned_environ is None or pinned_environ is not environ:
            _TRUSTED_CAPTURE_READY = True
            return False
        _PINNED_OS_ENVIRON = pinned_environ
        _PINNED_POSIX_ENVIRON = existing_pin[1]
        _TRUSTED_CAPTURE_READY = True
        return True
    if _TRUSTED_CAPTURE_READY:
        return False
    origin = _seal_get(storage, _ORIGIN_SEAL_ATTR)
    if origin is None or origin[0] is not environ:
        return _fail_capture(storage)
    if environ is None or environ is not origin[0]:
        return _fail_capture(storage)
    try:
        environ_type = object.__getattribute__(os, "_Environ")
    except AttributeError:
        return _fail_capture(storage)
    if type(environ) is not environ_type:
        return _fail_capture(storage)
    try:
        data = object.__getattribute__(environ, "_data")
    except AttributeError:
        return _fail_capture(storage)
    if type(data) is not dict:
        return _fail_capture(storage)
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
            return _fail_capture(storage)
        payload = (environ, None)
        if not _seal_put(storage, _PIN_SEAL_ATTR, payload):
            return _fail_capture(storage)
        _PINNED_OS_ENVIRON = environ
        _PINNED_POSIX_ENVIRON = None
        _TRUSTED_CAPTURE_READY = True
        return True
    origin_posix = origin[1]
    live_posix = _current_posix_environ()
    if (
        origin_posix is None
        or type(origin_posix) is not dict
        or live_posix is None
        or live_posix is not origin_posix
        or data is not origin_posix
    ):
        return _fail_capture(storage)
    try:
        environb = object.__getattribute__(os, "environb")
    except AttributeError:
        return _fail_capture(storage)
    if type(environb) is not environ_type:
        return _fail_capture(storage)
    try:
        sibling = object.__getattribute__(environb, "_data")
    except AttributeError:
        return _fail_capture(storage)
    if sibling is not origin_posix:
        return _fail_capture(storage)
    payload = (environ, origin_posix)
    if not _seal_put(storage, _PIN_SEAL_ATTR, payload):
        return _fail_capture(storage)
    _PINNED_OS_ENVIRON = environ
    _PINNED_POSIX_ENVIRON = origin_posix
    _TRUSTED_CAPTURE_READY = True
    return True


def trusted_startup_ready() -> bool:
    """True when a prior ``capture_trusted_startup()`` sealed ``os.environ``."""
    environ = _current_os_environ()
    storage = _environ_instance_storage(environ)
    pin = _seal_get(storage, _PIN_SEAL_ATTR)
    if pin is None:
        return False
    return pin[0] is environ
