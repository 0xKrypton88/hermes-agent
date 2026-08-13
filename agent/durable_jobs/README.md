# ENG-3 LangGraph Durable-Job Pilot (Package 1 + ENG-26/ENG-27 slices)

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

### ENG-26 — Cursor provider reconciliation (default-off)

- Atomic `provider_effect_claims` before any injected fake `create_run`
- Stable provider idempotency key `cursor:{job_id}:{action_id}`
- Explicit mapping `job_id == langgraph_thread_id` plus frozen origin Slack
  binding / candidate / version (not LangGraph context)
- Origin platform/chat/root is derived from the durable Slack binding;
  a supplied mismatch is rejected before the effect claim
- Lost-create / existing-CLAIMED recovery: unique lookup is adopted;
  empty/ambiguous lookup or ambiguous create persists typed `unknown` and
  never redispatches / never blindly `create_run`

### ENG-27 — Slack job-thread + Go/Hold/Cancel (default-off)

- Immutable job ↔ workspace/channel/root-thread ↔ candidate/version binding
  **before** any Slack or provider effect; rebind is rejected
- Stable outbound `client_msg_id`; atomic CLAIMED CAS before `post_root`
  (concurrent losers do not post); existing CLAIMED after restart looks up
  by `client_msg_id` — unique adopt, zero/multiple typed `unknown`, never a
  blind repost
- Cross-job and cross-binding resume fail closed
- Go/Hold/Cancel records bound to job, candidate/version, actor, policy version,
  and decision idempotency key; unauthorized / mismatch / expired / replayed
  fail closed. Cancel is terminal: later or replayed pre-Cancel Go/Hold stay
  rejected as canceled; Cancel replay remains idempotent
- No Slack routing fork: gateway adapters are untouched

## Explicit non-goals (Package 1 + these slices)

- No production integration / gateway wiring / Slack action wiring
- No Cursor or cloud provider calls (injected fakes in tests only)
- No external dispatch capability whatsoever (not configuration-gated)
- No service restarts, deployment, credentials, live trading, order mutation,
  arming/disarming, or live reconciliation
- Does **not** touch existing completion/outbox modules or Hermes `state.db`
- Does **not** add LangGraph to core dependencies (opt-in extra only)
- SQLite here is **not** ENG-25 production PostgreSQL acceptance

## Storage boundaries

| Store | Path | Purpose |
|-------|------|---------|
| Application job store | `durable_jobs.sqlite_path` (required, explicit) | Jobs + events + ENG-26/27 ledgers |
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
calls an injected dispatch adapter. ENG-26/27 lane methods also no-op unless
`enabled` is true, and still never construct live Cursor/Slack clients.

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
