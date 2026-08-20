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

Threat interval
---------------
Trust is a bootstrap-unique capability: ``remember_process_origin()``
draws ``os.urandom(32)`` and HMAC-binds origin/pin payloads. Deterministic
string keys on ``os.environ.__dict__`` are not provenance. A process
without the install ``.pth`` / worktree ``sitecustomize`` / explicit
remember call has no token, so planting known fields before a late
import cannot mint ``trusted_startup_ready()``.

This is not an immutable boundary against arbitrary Python mutation after
a real remember() (rewriting this module's token and recomputing HMAC is
equivalent to invoking bootstrap). Module globals, ``sys.modules``
entries, and ``types.ModuleType`` injection are not a witness by
themselves.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import sys

_ORIGIN_RECORDED = False
_ORIGIN_OS_ENVIRON = None
_ORIGIN_POSIX_ENVIRON = None
_TRUSTED_CAPTURE_READY = False
_PINNED_OS_ENVIRON = None
_PINNED_POSIX_ENVIRON = None
_BOOTSTRAP_TOKEN = None
_MISSING = object()
_ORIGIN_SEAL_ATTR = "__hermes_trusted_environ_origin__"
_PIN_SEAL_ATTR = "__hermes_trusted_environ_pin__"
_BOOTSTRAP_TOKEN_LEN = 32
_ORIGIN_MAC_KIND = b"hermes-environ-origin-v1"
_PIN_MAC_KIND = b"hermes-environ-pin-v1"


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


def _id_bytes(obj):
    if obj is None:
        return b"\x00" * 8
    try:
        return int.to_bytes(id(obj), 8, "little", signed=False)
    except (OverflowError, TypeError):
        return None


def _identity_mac(token, kind, environ, posix):
    """HMAC over mapping identities. Never hashes env keys or values."""
    if type(token) is not bytes or bytes.__len__(token) != _BOOTSTRAP_TOKEN_LEN:
        return None
    if type(kind) is not bytes:
        return None
    env_b = _id_bytes(environ)
    posix_b = _id_bytes(posix)
    if env_b is None or posix_b is None:
        return None
    try:
        return hmac.new(token, kind + b"\0" + env_b + posix_b, hashlib.sha256).digest()
    except Exception:
        return None


def _valid_identity_seal(payload, token, kind, environ) -> bool:
    if type(payload) is not tuple or tuple.__len__(payload) != 3:
        return False
    pinned_environ = tuple.__getitem__(payload, 0)
    pinned_posix = tuple.__getitem__(payload, 1)
    mac = tuple.__getitem__(payload, 2)
    if pinned_environ is not environ:
        return False
    expected = _identity_mac(token, kind, pinned_environ, pinned_posix)
    if expected is None or type(mac) is not bytes:
        return False
    try:
        return hmac.compare_digest(mac, expected)
    except Exception:
        return False


def _seal_get(storage, name):
    """Return the seal tuple stored under ``name``, or None.

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
    if type(found) is not tuple:
        return None
    n = tuple.__len__(found)
    if n != 2 and n != 3:
        return None
    return found


def _seal_put(storage, name, payload) -> bool:
    """Write-once seal. Fails if ``name`` is already a key."""
    if type(storage) is not dict or type(name) is not str:
        return False
    if type(payload) is not tuple:
        return False
    n = tuple.__len__(payload)
    if n != 2 and n != 3:
        return False
    if _seal_get(storage, name) is not None:
        return False
    try:
        dict.__setitem__(storage, name, payload)
    except Exception:
        return False
    return _seal_get(storage, name) is payload


def _called_during_initial_site_bootstrap() -> bool:
    """Return whether the caller runs inside CPython's initial ``site.main``.

    ``remember_process_origin`` mints the process capability only from the
    interpreter's startup path.  A later direct call, ``site.addsitedir`` call,
    worker-thread call, or ``python -S`` process has no such frame chain and
    therefore cannot self-attest.  Code executed from startup ``.pth`` files or
    ``sitecustomize`` is part of the trusted installation boundary.
    """
    try:
        flags = sys.flags
        no_site = flags.no_site
    except (AttributeError, TypeError):
        return False
    # ``python -S`` remains a no-site process even if user code later calls
    # ``site.main()`` (including from ``atexit``).  Such a replay must never
    # become indistinguishable from interpreter startup.
    if type(no_site) is not int or no_site != 0:
        return False
    try:
        frame = sys._getframe()
    except (AttributeError, ValueError):
        return False
    found_site_main = False
    while frame is not None:
        try:
            globals_dict = frame.f_globals
            code = frame.f_code
            module_name = dict.get(globals_dict, "__name__")
            function_name = code.co_name
        except Exception:
            return False
        if type(module_name) is not str or type(function_name) is not str:
            return False
        if str.__eq__(module_name, "__main__"):
            return False
        if str.__eq__(module_name, "site") and str.__eq__(function_name, "main"):
            found_site_main = True
        elif found_site_main and not (
            str.__eq__(module_name, "site")
            or str.__eq__(module_name, "importlib._bootstrap")
            or str.__eq__(module_name, "_frozen_importlib")
        ):
            return False
        frame = frame.f_back
    return found_site_main


def remember_process_origin() -> bool:
    """Record interpreter-original mapping identities. Does not pin.

    First successful call wins and is the only way to mint the
    process-unique bootstrap token. Intended for a site ``.pth`` hook at
    interpreter start, while ``os.environ`` is still the process original.
    Importing this module does not remember. A pre-import plant of the
    known origin/pin string keys is not origin.
    """
    global _BOOTSTRAP_TOKEN, _ORIGIN_RECORDED, _ORIGIN_OS_ENVIRON, _ORIGIN_POSIX_ENVIRON
    environ = _current_os_environ()
    storage = _environ_instance_storage(environ)
    token = _BOOTSTRAP_TOKEN
    if type(token) is bytes and bytes.__len__(token) == _BOOTSTRAP_TOKEN_LEN:
        existing = _seal_get(storage, _ORIGIN_SEAL_ATTR)
        if not _valid_identity_seal(existing, token, _ORIGIN_MAC_KIND, environ):
            return False
        _ORIGIN_OS_ENVIRON = existing[0]
        _ORIGIN_POSIX_ENVIRON = existing[1]
        _ORIGIN_RECORDED = True
        return True
    if _ORIGIN_RECORDED:
        return False
    if not _called_during_initial_site_bootstrap():
        return False
    _ORIGIN_RECORDED = True
    if environ is None or storage is None:
        return False
    existing = _seal_get(storage, _ORIGIN_SEAL_ATTR)
    if existing is not None:
        return False
    try:
        token = os.urandom(_BOOTSTRAP_TOKEN_LEN)
    except Exception:
        return False
    if type(token) is not bytes or bytes.__len__(token) != _BOOTSTRAP_TOKEN_LEN:
        return False
    posix = _current_posix_environ()
    mac = _identity_mac(token, _ORIGIN_MAC_KIND, environ, posix)
    if mac is None:
        return False
    payload = (environ, posix, mac)
    if not _seal_put(storage, _ORIGIN_SEAL_ATTR, payload):
        return False
    _BOOTSTRAP_TOKEN = token
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
    module does not capture. Missing origin is not provenance. A
    planted 2-tuple under the known pin key is not a pin.
    """
    global _TRUSTED_CAPTURE_READY, _PINNED_OS_ENVIRON, _PINNED_POSIX_ENVIRON
    token = _BOOTSTRAP_TOKEN
    environ = _current_os_environ()
    storage = _environ_instance_storage(environ)
    existing_pin = _seal_get(storage, _PIN_SEAL_ATTR)
    if existing_pin is not None:
        if not _valid_identity_seal(existing_pin, token, _PIN_MAC_KIND, environ):
            _TRUSTED_CAPTURE_READY = True
            return False
        _PINNED_OS_ENVIRON = existing_pin[0]
        _PINNED_POSIX_ENVIRON = existing_pin[1]
        _TRUSTED_CAPTURE_READY = True
        return True
    if _TRUSTED_CAPTURE_READY:
        return False
    origin = _seal_get(storage, _ORIGIN_SEAL_ATTR)
    if not _valid_identity_seal(origin, token, _ORIGIN_MAC_KIND, environ):
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
        mac = _identity_mac(token, _PIN_MAC_KIND, environ, None)
        if mac is None:
            return _fail_capture(storage)
        payload = (environ, None, mac)
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
    mac = _identity_mac(token, _PIN_MAC_KIND, environ, origin_posix)
    if mac is None:
        return _fail_capture(storage)
    payload = (environ, origin_posix, mac)
    if not _seal_put(storage, _PIN_SEAL_ATTR, payload):
        return _fail_capture(storage)
    _PINNED_OS_ENVIRON = environ
    _PINNED_POSIX_ENVIRON = origin_posix
    _TRUSTED_CAPTURE_READY = True
    return True


def trusted_startup_ready() -> bool:
    """True when a prior ``capture_trusted_startup()`` sealed ``os.environ``.

    Requires this process's remember() token. Planting known string keys
    on ``os.environ.__dict__`` is not sufficient.
    """
    token = _BOOTSTRAP_TOKEN
    if type(token) is not bytes or bytes.__len__(token) != _BOOTSTRAP_TOKEN_LEN:
        return False
    environ = _current_os_environ()
    storage = _environ_instance_storage(environ)
    pin = _seal_get(storage, _PIN_SEAL_ATTR)
    return _valid_identity_seal(pin, token, _PIN_MAC_KIND, environ)
