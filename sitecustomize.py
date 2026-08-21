"""Record Hermes process-environ origin before user code.

``python -c`` drops cwd from ``sys.path`` while ``site.main()`` runs, so a
worktree checkout is not visible at interpreter start unless it is on
``PYTHONPATH``. Installed / editable layouts use ``hermes_environ_startup.pth``
in the install destination instead.

Importing this module does not pin or mark startup ready. This file is not a
setuptools ``py-module`` and must not be installed as a global sitecustomize.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
_root_s = str(_ROOT)
if _root_s not in sys.path:
    sys.path.insert(0, _root_s)

try:
    import hermes_environ_startup as _hermes_environ_startup  # noqa: E402
except ImportError:
    _hermes_environ_startup = None
if _hermes_environ_startup is not None:
    _hermes_environ_startup.remember_process_origin()
