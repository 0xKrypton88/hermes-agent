# ENG-3 LangGraph Durable-Job Pilot (Package 1)

Isolated, **disabled-by-default** durable-job pilot. Not wired into the gateway,
Slack actions, Cursor/cloud providers, or production Hermes `state.db`.

## What this package does

- Durable application job records with opaque `job_id` (never a Slack timestamp)
- Correlation fields: origin platform/chat/root thread, objective, repository
  identity, frozen baseline SHA, phase/`next_action`, idempotency key, timestamps
- Deterministic phase flow via LangGraph: `INTAKE → FREEZE_BASELINE → AWAIT_DISPATCH`
- Append-only `durable_job_events` outbox for idempotent intent recording
- Crash-safe reopen/recovery by `job_id` on the same disposable SQLite path
- Compare-and-swap phase transitions (no TOCTOU lost updates vs audit history)
- **Hard-disabled dispatch**: `attempt_dispatch` always raises
  `DispatchDisabledError` and never invokes any adapter, even when
  `enabled` and `dispatch_enabled` are both true and a fake adapter is injected

## Explicit non-goals (Package 1)

- No production integration / gateway wiring / Slack action wiring
- No Cursor or cloud provider calls
- No external dispatch capability whatsoever (not configuration-gated)
- No service restarts, deployment, credentials, live trading, order mutation,
  arming/disarming, or reconciliation
- Does **not** touch existing completion/outbox modules or Hermes `state.db`
- Does **not** add LangGraph to core dependencies (opt-in extra only)

## Storage boundaries

| Store | Path | Purpose |
|-------|------|---------|
| Application job store | `durable_jobs.sqlite_path` (required, explicit) | Jobs + append-only events |
| LangGraph checkpointer | `durable_jobs.checkpoint_sqlite_path` (required, distinct) | Graph thread checkpoints |

Both are **dev/test SQLite only**, single-process. Schema version is local
(`SCHEMA_VERSION` in `store.py`).

### Later: production PostgreSQL

1. Keep the application job/outbox schema on PostgreSQL (migrations owned by
   this domain, not Hermes SessionDB).
2. Replace `langgraph.checkpoint.sqlite.SqliteSaver` with a PostgreSQL
   checkpointer (e.g. `langgraph-checkpoint-postgres`) pointed at a separate
   checkpoint schema/database.
3. Do not merge checkpointer tables into the application job store.

## Config

```yaml
durable_jobs:
  enabled: false          # default; must be a real boolean (not "false")
  dispatch_enabled: false # retained for shape only — Package 1 hard-disables dispatch
  sqlite_path: null       # must be set explicitly when enabling
  checkpoint_sqlite_path: null
```

`enabled` / `dispatch_enabled` reject non-bool values (strings/ints) to avoid
`bool("false") == True` ambiguity. Even with both flags true, Package 1 never
calls an injected dispatch adapter.

## Tests (clean / release-venv safe)

LangGraph is **not** installed in the release venv. Use the harness — it requires
`uv` and installs via `uv sync --extra dev --locked` into an isolated venv so
transitive pins match `uv.lock` (e.g. `langgraph-checkpoint==4.1.1`). Bare
`pip install -e '.[dev]'` is **non-locked** and must not be treated as
reproducible locked evidence.

```bash
scripts/run_durable_jobs_tests.sh
```

Manual equivalent (Windows: use `Scripts\python.exe`):

```bash
uv venv .venv-durable-jobs
UV_PROJECT_ENVIRONMENT=.venv-durable-jobs uv sync --extra dev --locked
.venv-durable-jobs/bin/python -m pytest tests/agent/durable_jobs/
```

If a local `.venv` already has `[dev]` synced from the lockfile:

```bash
scripts/run_tests.sh tests/agent/durable_jobs/
```
