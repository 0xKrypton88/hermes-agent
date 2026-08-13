# ENG-3 LangGraph Durable-Job Pilot — Package 1 + ENG-25 persistence slice

Status: Package 1 implemented as an isolated, disabled-by-default module.
ENG-25 first slice adds opt-in PostgreSQL application + checkpointer
persistence without production cutover.
Code: `agent/durable_jobs/`

## Decision summary

Package 1 proves a durable job + LangGraph checkpoint seam **without**
production integration. The application job store and LangGraph checkpointer
remain distinct. Dispatch is **hard-disabled** in Package 1 — not merely
configuration-gated. `attempt_dispatch` always rejects and never invokes any
adapter, regardless of `enabled` / `dispatch_enabled` or injected fakes.

ENG-25 keeps that hard-disable. PostgreSQL is selected only by
`durable_jobs.backend: postgresql` plus explicit DSNs/schemas. SQLite/dev
behavior remains available via `[langgraph-durable]` / `[dev]`. The
PostgreSQL extra is `[langgraph-durable-postgres]` — not core, not `[all]`.

## Boundaries

- SQLite paths are explicit and disposable (tests/config only).
- PostgreSQL uses two identities: application DSN+schema and checkpointer
  DSN+schema. Identical identity is rejected. `public` / unsafe identifiers
  and in-memory persistence are rejected. DSNs are redacted from repr/errors.
- External systems exist only as injected Protocol ports + fakes
  (`agent/durable_jobs/adapters.py`). Package 1 never calls them.
- Config booleans must be real `bool` values; string/int forms are rejected.
- Existing Hermes completion/outbox modules and `state.db` are untouched.
- LangGraph is an opt-in extra (`[langgraph-durable]` / `[dev]`), never core.
- PostgreSQL drivers/checkpointer are a second opt-in extra
  (`[langgraph-durable-postgres]`).

## Topology

```
application DSN + postgres_schema
  durable_jobs / durable_job_events / schema marker
  (ledger tables created for later slices; lane does not use them yet)

checkpointer DSN + checkpoint_postgres_schema
  LangGraph PostgresSaver tables (search_path only; never application schema)
```

Never MemorySaver. Never share the application schema with checkpoints.
Never silently fall back to SQLite when `backend: postgresql`.

## Minimal state flow

`INTAKE → FREEZE_BASELINE → AWAIT_DISPATCH` (no actual dispatch).

Phase transitions use compare-and-swap inside a single transaction so
stale concurrent updates cannot diverge job state from audit history.
On PostgreSQL the same CAS is fenced with `SELECT … FOR UPDATE` and
`pg_advisory_xact_lock`.

## Clean-environment tests

Requires `uv`. Lock-respecting install
(`uv sync --extra dev --extra langgraph-durable-postgres --locked`):

```bash
scripts/run_durable_jobs_tests.sh
```

Live PostgreSQL integration tests skip only with
`missing-test-DSN: HERMES_DURABLE_JOBS_PG_TEST_DSN is unset`.

## Remaining operational gaps (not this slice)

- Gateway / Slack action wiring
- Cursor/cloud provider dispatch adapter invocation
- Port ENG-26/27 ledgers onto the PostgreSQL application schema
- Production migration runner / pooling / SERIALIZABLE policy
- Replica failover and operational cutover
- Package 1 dispatch remains hard-disabled
