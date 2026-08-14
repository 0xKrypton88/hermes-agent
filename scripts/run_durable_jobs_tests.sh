#!/usr/bin/env bash
# Lock-respecting clean-environment runner for ENG-3 Package 1 durable-jobs tests.
#
# LangGraph is an *opt-in* extra ([langgraph-durable], also pulled by [dev]).
# It is NOT a core hermes-agent dependency. Release venvs therefore omit it.
#
# This script REQUIRES `uv` and installs via:
#   UV_PROJECT_ENVIRONMENT=<venv> uv sync --extra dev --extra langgraph-durable-postgres --locked
# so versions match uv.lock (e.g. langgraph-checkpoint==4.1.1). Plain
# `pip install -e '.[dev]'` is intentionally NOT used — it is non-locked and
# can resolve newer transitive pins (observed: langgraph-checkpoint 4.2.0).
#
# Windows: uv creates/uses Scripts/python.exe under the target venv; the
# UV_PROJECT_ENVIRONMENT path works the same on POSIX and Windows.
#
# Usage:
#   scripts/run_durable_jobs_tests.sh
#   scripts/run_durable_jobs_tests.sh -q --tb=short
#   DURABLE_JOBS_VENV=/tmp/dj-venv scripts/run_durable_jobs_tests.sh
#
# Manual equivalent:
#   uv venv .venv-durable-jobs
#   UV_PROJECT_ENVIRONMENT=.venv-durable-jobs uv sync --extra dev --extra langgraph-durable-postgres --locked
#   .venv-durable-jobs/bin/python -m pytest tests/agent/durable_jobs/
#   # Windows: .venv-durable-jobs\Scripts\python.exe -m pytest ...

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

VENV="${DURABLE_JOBS_VENV:-$REPO_ROOT/.venv-durable-jobs}"

if ! command -v uv >/dev/null 2>&1; then
  echo "error: uv is required for lock-respecting durable-jobs test installs." >&2
  echo "       Install uv (https://docs.astral.sh/uv/), then re-run this script." >&2
  echo "       Do not use bare pip install -e '.[dev]' as locked evidence." >&2
  exit 1
fi

echo "▶ durable-jobs clean env (uv --locked): venv=$VENV"
echo "▶ UV_PROJECT_ENVIRONMENT=$VENV uv sync --extra dev --extra langgraph-durable-postgres --locked"

UV_PROJECT_ENVIRONMENT="$VENV" uv sync --extra dev --extra langgraph-durable-postgres --locked

if [ -x "$VENV/bin/python" ]; then
  PY="$VENV/bin/python"
elif [ -x "$VENV/Scripts/python.exe" ]; then
  PY="$VENV/Scripts/python.exe"
else
  echo "error: could not locate python inside $VENV after uv sync" >&2
  exit 1
fi

if ! "$PY" -c 'import langgraph, langgraph.checkpoint.sqlite, pytest' 2>/dev/null; then
  echo "error: langgraph/pytest missing after uv sync --extra dev --extra langgraph-durable-postgres --locked" >&2
  exit 1
fi

# Prove the installed transitive matches uv.lock (pip previously drifted here).
LOCKED_CP="$("$PY" -c 'import importlib.metadata as m; print(m.version("langgraph-checkpoint"))')"
EXPECTED_CP="$("$PY" - <<'PY'
from pathlib import Path
text = Path("uv.lock").read_text(encoding="utf-8")
needle = 'name = "langgraph-checkpoint"\n'
idx = text.find(needle)
if idx < 0:
    raise SystemExit("langgraph-checkpoint missing from uv.lock")
chunk = text[idx : idx + 200]
for line in chunk.splitlines():
    if line.startswith("version = "):
        print(line.split("=", 1)[1].strip().strip('"'))
        break
else:
    raise SystemExit("version not found for langgraph-checkpoint in uv.lock")
PY
)"
echo "▶ langgraph-checkpoint installed=$LOCKED_CP uv.lock=$EXPECTED_CP"
if [ "$LOCKED_CP" != "$EXPECTED_CP" ]; then
  echo "error: langgraph-checkpoint drift vs uv.lock ($LOCKED_CP != $EXPECTED_CP)" >&2
  exit 1
fi

echo "▶ running focused suite: tests/agent/durable_jobs/"
exec "$PY" -m pytest tests/agent/durable_jobs/ "$@"
