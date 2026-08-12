# ENG-3 LangGraph Durable-Job Pilot — Package 1

Status: implemented as an isolated, disabled-by-default module
Digest: `5506a50c7dc4493c00f72147670b28e9cd5cbdaf936b6c09c0387454a37cb056`
Code: `agent/durable_jobs/`

## Decision summary

Package 1 proves a durable job + LangGraph checkpoint seam **without**
production integration. The application job store and LangGraph checkpointer
remain distinct. Dispatch is **hard-disabled** in Package 1 — not merely
configuration-gated. `attempt_dispatch` always rejects and never invokes any
adapter, regardless of `enabled` / `dispatch_enabled` or injected fakes.

## Boundaries

- SQLite paths are explicit and disposable (tests/config only).
- Single-process / dev-only SQLite; production durable store is PostgreSQL-first
  and **not** implemented or provisioned here.
- External systems exist only as injected Protocol ports + fakes
  (`agent/durable_jobs/adapters.py`). Package 1 never calls them.
- Config booleans must be real `bool` values; string/int forms are rejected.
- Existing Hermes completion/outbox modules and `state.db` are untouched.
- LangGraph is an opt-in extra (`[langgraph-durable]` / `[dev]`), never core.

## Minimal state flow

`INTAKE → FREEZE_BASELINE → AWAIT_DISPATCH` (no actual dispatch).

Phase transitions use compare-and-swap inside a single SQLite transaction so
stale concurrent updates cannot diverge job state from audit history.

## Clean-environment tests

```bash
scripts/run_durable_jobs_tests.sh
```

## Follow-on attachment points (not in Package 1)

- Gateway / Slack action wiring
- Cursor/cloud provider dispatch adapter invocation (later package)
- PostgreSQL job store + PostgreSQL LangGraph checkpointer
