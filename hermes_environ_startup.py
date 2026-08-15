"""Trusted process-environment witness captured at Hermes entry.

Importing this module does not pin anything. Real process entry points
must call ``capture_trusted_startup()`` before loading plugins or durable-job
preflight. Preflight never calls this function and never imports this
module to create a pin; it only reads pins if a prior capture already
succeeded.

Identity-only behavior:

- The capture function records environment singletons and the current
  platform once.
- Pin state lives only in the capture/ready/snapshot closures. It is
  not stored in module globals, ``os.environ``, or replaceable facades.
- A replaced ``os.environ`` is never pinned. Capture succeeds only when
  the wrapper still backs the genuine process mapping.
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
        """Pin original ``os.environ`` / ``posix.environ`` identities."""
        nonlocal ready
        nonlocal pinned_os
        nonlocal pinned_posix
        if ready:
            return pinned_os is not None

        def dict_has_str_key(storage, name):
            if type(storage) is not dict or type(name) is not str:
                return False
            try:
                items = dict.items(storage)
            except Exception:
                return False
            found = False
            for pair in items:
                if type(pair) is not tuple or tuple.__len__(pair) != 2:
                    return False
                key = tuple.__getitem__(pair, 0)
                if type(key) is not str:
                    return False
                if str.__eq__(key, name):
                    if found:
                        return False
                    found = True
            return found

        try:
            environ = object.__getattribute__(os, "environ")
            environ_type = object.__getattribute__(os, "_Environ")
        except AttributeError:
            ready = True
            return False
        if type(environ) is not environ_type:
            ready = True
            return False
        try:
            data = object.__getattribute__(environ, "_data")
        except AttributeError:
            ready = True
            return False
        if type(data) is not dict:
            ready = True
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
                ready = True
                return False
            try:
                encodekey = object.__getattribute__(environ, "encodekey")
            except AttributeError:
                ready = True
                return False
            if type(encodekey) is not type(dict_has_str_key):
                ready = True
                return False
            try:
                import ctypes
            except Exception:
                ready = True
                return False
            try:
                kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
                get_var = kernel32.GetEnvironmentVariableW
                get_var.argtypes = [
                    ctypes.c_wchar_p,
                    ctypes.c_wchar_p,
                    ctypes.c_uint32,
                ]
                get_var.restype = ctypes.c_uint32
            except Exception:
                ready = True
                return False
            native_any = False
            probe_names = ("PATH", "SystemRoot", "USERPROFILE", "windir")
            i = 0
            n = tuple.__len__(probe_names)
            while i < n:
                name = tuple.__getitem__(probe_names, i)
                try:
                    size = get_var(name, None, 0)
                except Exception:
                    ready = True
                    return False
                if size != 0:
                    native_any = True
                    try:
                        encoded = encodekey(name)
                    except Exception:
                        ready = True
                        return False
                    if type(encoded) is not str:
                        ready = True
                        return False
                    if not dict_has_str_key(data, encoded):
                        ready = True
                        return False
                i += 1
            if native_any is not True:
                ready = True
                return False
            pinned_os = environ
            ready = True
            return True

        try:
            modules = object.__getattribute__(sys, "modules")
        except AttributeError:
            modules = None
        posix_mapping = None
        if type(modules) is dict:
            posix = dict.get(modules, "posix")
            if posix is not None:
                try:
                    mapping = object.__getattribute__(posix, "environ")
                except AttributeError:
                    mapping = None
                if type(mapping) is dict:
                    posix_mapping = mapping
        if posix_mapping is None or data is not posix_mapping:
            ready = True
            return False

        pinned_os = environ
        pinned_posix = posix_mapping
        ready = True
        return True

    def trusted_startup_ready() -> bool:
        """True when a prior ``capture_trusted_startup()`` pinned ``os.environ``."""
        return ready is True and pinned_os is not None

    def startup_pin_snapshot():
        """Return the raw pinned witness tuple for internal preflight checks."""
        return ready, pinned_os, pinned_posix

    return capture_trusted_startup, trusted_startup_ready, startup_pin_snapshot


capture_trusted_startup, trusted_startup_ready, startup_pin_snapshot = _startup_pin_state()


def _startup_pin_state():
    """Return the import-time pin callables. Does not create a new pin universe."""
    return capture_trusted_startup, trusted_startup_ready, startup_pin_snapshot
