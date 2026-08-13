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
- Persisted claim owner token + lease timestamps; the live owner renews
  the lease (owner-fenced heartbeat) while `create_run` is in flight
- Complete is CAS-fenced to the current owner (old owner cannot mark
  ACCEPTED after takeover)
- A non-owner that sees an unexpired CLAIMED must poll — no lookup, create,
  adopt, or typed UNKNOWN. Process-local in-flight sets are not correctness
- Only an expired (or legacy-null) lease may be taken over atomically;
  recovery looks up by the stable idempotency key and never blindly creates
- Empty lookup after takeover/lost-create enters bounded `recovering`;
  typed UNKNOWN is persisted only when the recovery bound is exceeded and
  the terminal CAS succeeds. Delayed provider visibility can still be adopted
- Stable provider idempotency key `cursor:{job_id}:{action_id}`
- Explicit mapping `job_id == langgraph_thread_id` plus frozen origin Slack
  binding / candidate / version (not LangGraph context)
- Origin platform/chat/root is derived from the durable Slack binding;
  a supplied mismatch is rejected before the effect claim
- Lost-create / stale-CLAIMED recovery: unique lookup is adopted;
  empty/ambiguous lookup or ambiguous create persists typed `unknown` and
  never redispatches / never blindly `create_run`

### ENG-27 — Slack job-thread + Go/Hold/Cancel (default-off)

- Immutable job ↔ workspace/channel/root-thread ↔ candidate/version binding
  **before** any Slack or provider effect; rebind is rejected
- Stable outbound `client_msg_id`; atomic CLAIMED CAS before `post_root`
  with owner token + lease (concurrent losers do not post, lookup, or
  terminalize a live claim). The live owner renews the lease while
  `post_root` is in flight. Only a stale/expired CLAIMED may be taken
  over; recovery looks up by `client_msg_id` — unique adopt, empty
  lookup enters bounded `recovering` (not an immediate UNKNOWN),
  multiple typed `unknown`, never a blind repost. Previous owner is
  fenced from `mark_delivered` after takeover
- Cross-job and cross-binding resume fail closed
- Go/Hold/Cancel records bound to job, candidate/version, actor, policy version,
  and decision idempotency key; unauthorized / mismatch / expired / replayed
  fail closed. Cancel is terminal: later or replayed pre-Cancel Go/Hold stay
  rejected as canceled; Cancel replay remains idempotent
- No Slack routing fork: gateway adapters are untouched

### ENG-29 — mandatory Go guard (default-off, local policy-contract)

- Versioned immutable action matrix (`eng29-matrix-v1`) in `eng29.py`
- Mandatory Go: scope_change, missing_prerequisites,
  unresolved_provider_ambiguity, deploy, restart, cutover,
  production_migration, external_promotion_release, financial_action;
  unknown/unclassified default deny
- Immutable authorization tuple (job, source package/version, candidate SHA,
  environment, target action, actor, expiry, policy/matrix version, replay key)
- Guard runs before provider/Slack effect claim, stale takeover, recovery
  lookup, `create_run`, and `post_root`. Claim/takeover validation shares the
  write connection with the mutation; `create_run` / `post_root` / lookup
  re-check a latest-safe snapshot immediately before the adapter RPC (not
  atomic with the network call). No deploy/restart adapters are invented
- `allowed_actors_json` is a JSON list of non-empty strings only (stripped;
  malformed elements are never stringified). `set_policy` rejects coerced
  members at writer ingress before any policy row or event
- Claim/takeover sample authorization `now` only after `BEGIN IMMEDIATE`
  succeeds so a lock wait cannot keep a pre-wait clock
- Test fixtures that write default Go live under
  `tests/agent/durable_jobs/authz_fixtures.py`, not production agent modules
- Production modules expose no-op fault-injection seams (`after_*_before_commit`,
  `after_inbound_select_before_insert`, `after_in_transaction_adapter_go`,
  `before_begin_immediate`). They are instrumentation: defaults return `None`
  and cannot grant Go, bypass ENG-29, or ACK. See `ENG28_MATRIX.md`.
- **Local policy-contract evidence only** — not Slack/live authorization,
  not gateway ingress, not PostgreSQL claims

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
