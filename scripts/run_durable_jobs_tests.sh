#!/usr/bin/env bash
# Reproducible clean-environment runner for ENG-3 Package 1 durable-jobs tests.
#
# LangGraph is an *opt-in* extra ([langgraph-durable], also pulled by [dev]).
# It is NOT a core hermes-agent dependency. Release venvs therefore omit it;
# this script builds/uses an isolated venv, installs locked opt-in deps, and
# runs the focused suite.
#
# Usage:
#   scripts/run_durable_jobs_tests.sh
#   scripts/run_durable_jobs_tests.sh -q --tb=short
#   DURABLE_JOBS_VENV=/tmp/dj-venv scripts/run_durable_jobs_tests.sh
#
# Equivalent manual setup (uv, preferred — uses uv.lock):
#   uv venv .venv-durable-jobs
#   uv pip install -e ".[dev]" --python .venv-durable-jobs
#   .venv-durable-jobs/bin/python -m pytest tests/agent/durable_jobs/
#
# Equivalent manual setup (pip):
#   python3 -m venv .venv-durable-jobs
#   .venv-durable-jobs/bin/pip install -U pip
#   .venv-durable-jobs/bin/pip install -e ".[dev]"
#   .venv-durable-jobs/bin/python -m pytest tests/agent/durable_jobs/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

VENV="${DURABLE_JOBS_VENV:-$REPO_ROOT/.venv-durable-jobs}"
PYTHON_BIN="${DURABLE_JOBS_PYTHON:-python3}"

echo "▶ durable-jobs clean env: venv=$VENV"

if [ ! -x "$VENV/bin/python" ] && [ ! -x "$VENV/Scripts/python.exe" ]; then
  echo "▶ creating venv via $PYTHON_BIN -m venv"
  "$PYTHON_BIN" -m venv "$VENV"
fi

if [ -x "$VENV/bin/python" ]; then
  PY="$VENV/bin/python"
  PIP="$VENV/bin/pip"
elif [ -x "$VENV/Scripts/python.exe" ]; then
  PY="$VENV/Scripts/python.exe"
  PIP="$VENV/Scripts/pip.exe"
else
  echo "error: could not locate python inside $VENV" >&2
  exit 1
fi

echo "▶ installing opt-in deps: pip install -e '.[dev]' (includes [langgraph-durable])"
"$PY" -m pip install -U pip >/dev/null
"$PIP" install -e ".[dev]"

if ! "$PY" -c 'import langgraph, langgraph.checkpoint.sqlite, pytest' 2>/dev/null; then
  echo "error: langgraph/pytest missing after install -e '.[dev]'" >&2
  exit 1
fi

echo "▶ running focused suite: tests/agent/durable_jobs/"
exec "$PY" -m pytest tests/agent/durable_jobs/ "$@"
