"""Opt-in LangGraph gate for Package 1 graph-backed tests.

LangGraph is declared in the ``langgraph-durable`` / ``dev`` extras only —
never a core dependency. Graph-flow tests skip with an actionable message
when the extra is absent (e.g. release venv). Prefer:

    scripts/run_durable_jobs_tests.sh
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="module")
def require_langgraph():
    pytest.importorskip(
        "langgraph",
        reason=(
            "langgraph extra missing; install opt-in deps via "
            "`scripts/run_durable_jobs_tests.sh` or "
            "`pip install -e '.[dev]'` / `uv sync --extra dev` "
            "(does not add LangGraph to core dependencies)"
        ),
    )
