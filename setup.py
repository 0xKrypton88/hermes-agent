"""
setup.py — wheel/sdist build guard.

pip/PyPI and Homebrew are no longer supported distribution methods for
Hermes Agent (see website/docs/getting-started/platform-support.md). The
wheel would ship without bundled assets (locales, skills, optional-mcps,
web_dist, tui_dist, plugin manifests) since those are resolved at runtime
via env-var overrides set by the nix wrapper or the source-checkout layout.

This file overrides the ``bdist_wheel`` and ``sdist`` setuptools commands
to raise an error when run outside a Nix build. The PEP 517
``build_wheel`` / ``build_sdist`` hooks in
``setuptools.build_meta`` call these commands internally, so the guard
fires for ``uv build``, ``pip wheel``, ``python -m build``, and direct
``setup.py`` invocations alike.

The one legitimate consumer of ``build_wheel`` is uv2nix, which calls
``setuptools.build_meta.build_wheel`` (→ ``bdist_wheel``) inside a Nix
build sandbox. ``nix/python.nix`` sets ``HERMES_NIX_BUILD=1`` on the
Hermes package derivation, so only that build may create an artifact.

Editable installs (``uv sync``, ``pip install -e .``, ``nix develop``)
use ``build_editable``, which does NOT call ``bdist_wheel`` — it calls
``build_ext`` in editable mode. So the guard does not affect development.

Importing this module does not write into ``site.getsitepackages()``.
The process-origin ``.pth`` is copied only by install/build commands
into that command's destination (build lib, install_lib, or the
editable unpacked wheel), never into every ambient site directory.
"""

import os
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.install_lib import install_lib as _install_lib
from setuptools.command.sdist import sdist

_PTH_NAME = "hermes_environ_startup.pth"


def _environ_startup_pth_src() -> Path:
    return Path(__file__).resolve().parent / _PTH_NAME


def _copy_environ_startup_pth(dest_dir: str | None, copy_file) -> None:
    """Copy the origin ``.pth`` into one install/build destination."""
    if not dest_dir:
        return
    src = _environ_startup_pth_src()
    if not src.is_file():
        return
    dest = str(Path(dest_dir) / _PTH_NAME)
    copy_file(str(src), dest)


class _BuildPyWithEnvironPth(_build_py):
    def run(self, *args, **kwargs):
        super().run(*args, **kwargs)
        _copy_environ_startup_pth(getattr(self, "build_lib", None), self.copy_file)


class _InstallLibWithEnvironPth(_install_lib):
    def run(self, *args, **kwargs):
        super().run(*args, **kwargs)
        _copy_environ_startup_pth(getattr(self, "install_dir", None), self.copy_file)


_IN_NIX_BUILD = os.environ.get("HERMES_NIX_BUILD") == "1"

_BLOCK_MESSAGE = (
    "Building wheels or sdists for hermes-agent is not supported.\n"
    "Hermes is distributed via the shell installer, Docker image, or Nix.\n"
    "See: https://hermes-agent.nousresearch.com/docs/getting-started/installation\n"
    "\n"
    "If you are developing, use an editable install instead:\n"
    "  uv sync          # or: uv pip install -e .\n"
    "\n"
    "If you are building with Nix (uv2nix), this error should not fire —\n"
    "the Hermes Nix derivation sets HERMES_NIX_BUILD=1. If it does, file a bug."
)


class _GuardedSdist(sdist):
    def run(self, *args, **kwargs):
        if not _IN_NIX_BUILD:
            raise RuntimeError(_BLOCK_MESSAGE)
        return super().run(*args, **kwargs)


cmdclass = {
    "sdist": _GuardedSdist,
    "build_py": _BuildPyWithEnvironPth,
    "install_lib": _InstallLibWithEnvironPth,
}

# bdist_wheel is only available when the `wheel` package is installed.
# setuptools.build_meta.build_wheel() calls it internally, so the guard
# fires for all PEP 517 wheel build paths. Define the subclass only when
# the import succeeds — otherwise a None base class raises TypeError at
# class-definition time, before the cmdclass guard can run.
try:
    from setuptools.command.bdist_wheel import bdist_wheel

    class _GuardedBdistWheel(bdist_wheel):
        def run(self, *args, **kwargs):
            if not _IN_NIX_BUILD:
                raise RuntimeError(_BLOCK_MESSAGE)
            return super().run(*args, **kwargs)

    cmdclass["bdist_wheel"] = _GuardedBdistWheel
except ImportError:
    pass

try:
    from setuptools.command.editable_wheel import editable_wheel as _editable_wheel

    class _EditableWheelWithEnvironPth(_editable_wheel):
        def _configure_build(self, name, unpacked_wheel, build_lib, tmp_dir):
            super()._configure_build(name, unpacked_wheel, build_lib, tmp_dir)
            src = _environ_startup_pth_src()
            if not src.is_file():
                return
            text = src.read_text(encoding="utf-8")
            for dest_dir in (unpacked_wheel, build_lib):
                if not dest_dir:
                    continue
                dest = Path(dest_dir) / _PTH_NAME
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_text(text, encoding="utf-8")

    cmdclass["editable_wheel"] = _EditableWheelWithEnvironPth
except ImportError:
    pass

setup(cmdclass=cmdclass)
