# ENG-3 LangGraph Durable-Job Pilot (Package 1 + ENG-26/27 + Package 2 coupling)

Isolated, **disabled-by-default** durable-job pilot. Package 2 adds one
Gateway lifecycle seam (`gateway/durable_job_lane.py`) that constructs the
lane only when explicit validated gates pass. Default remains
`enabled: false` / dispatch off. Flags cannot mint a live Slack/Cursor
client. `attempt_dispatch` stays hard-disabled. Does not use production
Hermes `state.db`.

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
- No parallel Slack router: Package 2 reuses existing Slack Block Kit action
  ingress (`hermes_durable_go/hold/pause/cancel`) and forwards to
  `DurableLaneService.consume_inbound_action` when the lane is attached

### Package 2 — Gateway coupling without activation (default-off)

- One lifecycle-owned Gateway seam reads `durable_jobs` from active config
- Constructs `DurableLaneService` only when enabled, SQLite lane storage,
  explicit adapter modes (`null` or `injected`), and policy/identity
  bindings are complete **and** injected transports are the approved
  concrete types bound to the configured secret refs (`runtime_ready`).
  Missing/mismatched refs, metadata-only ducks, and self-attested
  subclasses refuse attach (no handle, no adapters). Missing/unknown/partial
  config is fail-closed
- Public `DurableLaneService` writers (`consume_inbound_action`,
  `bind_slack`, `deliver_slack_root`, `reconcile_cursor_create`,
  `set_job_policy`, `record_decision`) verify repository identity and a
  present, readable persisted Slack workspace binding equal to
  `identity_binding.workspace_id` before the first write, effect, or ACK.
  `bind_slack` is the sole bootstrap: it may create the initial binding
  only when the caller workspace is non-empty and matches configured
  authority. Missing/unreadable binding rows fail closed. Platform
  wrappers are defense-in-depth.
- `dispatch_allowed` is True only for complete SQLite + both modes
  `injected` + secret *references* (env var names) + policy/identity.
  PostgreSQL lane storage cannot set the flag. `attempt_dispatch` still
  raises `DispatchDisabledError`
- Production-shaped `CursorCloudInjectedTransport` /
  `SlackInjectedTransport` require an injected request callable and a
  secret-ref name. No built-in credentials, no implicit HTTP/SDK client
- ENG-50 production binding (`production_binding.py`) is the
  lifecycle-owned Gateway startup seam: `_maybe_attach_durable_job_lane`
  injects only those approved concrete types when a truthful request
  port and a concrete matching `_durable_job_runtime_identity` are
  already stored on the runner instance. Owner seam names are read only
  from instance `__dict__` via builtin type access and identity-only
  checks against trusted instance-dict descriptor types (never
  properties, descriptors, class attributes, ``isinstance``, or custom
  metaclass ``__getattribute__`` / ``__eq__`` / ``__hash__`` hooks).
  Preflight detects secret-ref *names* from the interpreter process
  environ dict (``posix.environ`` / ``nt.environ``) with builtin
  ``dict.__contains__`` only — never ``os.environ`` mapping APIs and
  never credential values. Config flags and secret-ref *names* cannot
  mint a client.
  ENG-58 adds explicit `CursorCloudInjectedRequestPort` /
  `SlackInjectedRequestPort` adapters that wrap an already-injected
  client seam (`create_agent`/`get_agent`/`get_run`,
  `chat_postMessage`/`conversations_replies`). They are not
  auto-activated from config flags: attach still requires the ports
  (or already-injected clients plus bound Slack channel/thread identity
  in instance `__dict__` storage) together with matching runtime
  identity. Missing or mismatched ports fail closed rather than
  inventing HTTP/SDK behavior. Isolated shadow/E2E uses deterministic
  fakes only.
- Preflight validates config/backend/schema/path/adapter modes/bindings/
  secret-ref names/runtime readiness with no sockets and no `psycopg`
  import on the SQLite path
- Process-global attach is exclusive; shutdown is idempotent; reconstruct
  can reopen the same SQLite path (restart/takeover)
- Status/errors redact DSN, token, and `xoxb-` values

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

## Explicit non-goals (Package 1 + Package 2 coupling)

- No activation: default `enabled: false`, dispatch off, no live Slack/Cursor
  calls, no sandbox E2E
- No OAuth/token/secret/permission changes and no production database/migration
- No Cursor or cloud provider calls (injected request callable in tests only)
- No external dispatch capability (`attempt_dispatch` is hard-disabled)
- No service restarts, deployment, credentials, live trading, order mutation,
  arming/disarming, or live reconciliation
- Does **not** touch existing completion/outbox modules or Hermes `state.db`
- Does **not** add LangGraph to core dependencies (opt-in extra only)
- SQLite here is **not** a substitute for the ENG-25 PostgreSQL extra when
  `backend: postgresql` is selected (no silent fallback)
- PostgreSQL/psycopg remains opt-in; default install and default CI must not
  import or require psycopg

## Storage boundaries

| Store | Selector | Purpose |
|-------|----------|---------|
| Application job store (SQLite, dev/test) | `backend: sqlite` (or inferred from sqlite paths) + `sqlite_path` | Jobs + events + ENG-26/27 ledgers |
| LangGraph checkpointer (SQLite, dev/test) | `checkpoint_sqlite_path` (distinct file) | Graph thread checkpoints |
| Application job store (PostgreSQL, ENG-25) | `backend: postgresql` + `postgres_dsn` + `postgres_schema` | Jobs + events (+ unused ledger DDL for later slices) |
| LangGraph checkpointer (PostgreSQL, ENG-25) | `checkpoint_postgres_dsn` + `checkpoint_postgres_schema` | Graph thread checkpoints |

SQLite remains single-process / disposable. PostgreSQL uses a dedicated
application schema (CAS + `SELECT … FOR UPDATE` + `pg_advisory_xact_lock`)
and a **separate** checkpointer schema via `search_path`. Schema version is
local (`SCHEMA_VERSION` in `store.py`). Unknown/future/missing markers fail
closed before application mutation.

Mixed SQLite+PostgreSQL config, missing DSNs, identical checkpointer/
application schema identity (same host/port/database/schema), unsafe or
`public` schema names, and in-memory persistence are rejected at load.
DSNs never appear in `repr` / errors.

```yaml
durable_jobs:
  enabled: false
  dispatch_enabled: false
  backend: null   # sqlite | postgresql; postgresql must be explicit
  sqlite_path: null
  checkpoint_sqlite_path: null
  postgres_dsn: null
  postgres_schema: null
  checkpoint_postgres_dsn: null
  checkpoint_postgres_schema: null
  postgres_storage_id: null
  checkpoint_postgres_storage_id: null
  cursor_adapter_mode: null   # null | injected; unset is not explicit
  slack_adapter_mode: null
  cursor_secret_ref: null     # env var NAME only, never a token value
  slack_secret_ref: null
  policy_version: null
  identity_binding: null      # {workspace_id, repository_identity}
```

Install PostgreSQL support with the opt-in extra (not core, not `[all]`,
not `[dev]`):

```bash
uv sync --extra langgraph-durable-postgres --locked
```

### Remaining operational gaps (not this slice)

- ENG-26/27 ledgers (`provider_effect_claims`, Slack bindings, decisions,
  inbound ACK) still execute on SQLite connections; the durable-lane
  facade **refuses** PostgreSQL rather than falling back to SQLite
- No production migration runner, no replica/failover, no connection-pool
  policy beyond per-call `psycopg.connect`
- No SERIALIZABLE isolation (this slice uses row locks + advisory xact
  locks + CAS)
- Package 1 dispatch remains hard-disabled: `attempt_dispatch` never calls
  adapters. Package 2 coupling does not activate live dispatch
- ENG-29 Go/cancel/authorization semantics are unchanged and still
  local-policy-contract on SQLite ledgers
- Not a production datastore cutover; not credentials, deploy, or restart
- Backup/restore is sandbox-gated only; see
  `docs/design/durable-jobs-postgres-backup-restore.md`. Cursor Cloud does
  not execute restore.

PostgreSQL application vs checkpointer isolation is fail-closed:
loopback aliases (`localhost` / `127.0.0.1` / `::1`) that target the
same database+schema are rejected at config load; distinct
`postgres_storage_id` values are required; live setup additionally
compares `pg_control_system().system_identifier` + `current_database()`
+ schema. DNS is not used. Empty/foreign/unmarked/wrong-owner schemas
are refused. `attempt_dispatch` on PostgreSQL still raises before any
store I/O.

## Config

```yaml
durable_jobs:
  enabled: false          # default; must be a real boolean (not "false")
  dispatch_enabled: false # required for dispatch_allowed; attempt_dispatch still hard-disabled
  sqlite_path: null       # must be set explicitly when enabling sqlite
  checkpoint_sqlite_path: null
  cursor_adapter_mode: null
  slack_adapter_mode: null
```

`enabled` / `dispatch_enabled` reject non-bool values (strings/ints) to avoid
`bool("false") == True` ambiguity. `dispatch_allowed` is True only when every
Package 2 gate is complete (SQLite lane, both adapter modes `injected`,
secret-ref names, policy, identity). Even then `attempt_dispatch` never
calls an injected dispatch adapter. ENG-26/27 lane methods also no-op unless
`enabled` is true, and still never construct live Cursor/Slack clients from
flags.

## Tests (clean / release-venv safe)

LangGraph is **not** installed in the release venv. Use the harness — it requires
`uv` and installs via
`uv sync --extra dev --extra langgraph-durable-postgres --locked`
into an isolated venv so transitive pins match `uv.lock`
(e.g. `langgraph-checkpoint==4.1.1`). Bare `pip install -e '.[dev]'` is
**non-locked** and must not be treated as reproducible locked evidence.

```bash
scripts/run_durable_jobs_tests.sh
```

Manual equivalent (Windows: use `Scripts\python.exe`):

```bash
uv venv .venv-durable-jobs
UV_PROJECT_ENVIRONMENT=.venv-durable-jobs uv sync --extra dev --extra langgraph-durable-postgres --locked
.venv-durable-jobs/bin/python -m pytest tests/agent/durable_jobs/
```

PostgreSQL integration tests require `HERMES_DURABLE_JOBS_PG_TEST_DSN`.
They skip only with `missing-test-DSN` when that variable is unset.

If a local `.venv` already has `[dev]` synced from the lockfile (SQLite
path only):

```bash
scripts/run_tests.sh tests/agent/durable_jobs/
```

## ENG-118 / ENG-122 disposable offline acceptance

`offline_acceptance.py` is the only API in this package that materializes the
ENG-118 immutable adoption ledger. It requires a newly initialized SQLite file
beneath a caller-declared disposable root, verifies the root-bound
`live_effects = 0` marker and the immutable ledger before every write, and maps
only legacy `sessions` and `messages` into dedicated disposable application tables. A
repeat application is an exact no-op, divergent identity fails closed, readback
compares canonical bytes and hashes, and the batch journal supports scoped
rollback. It cannot open an unmarked existing database or a path outside the
attested root. It is not imported or called by a runtime path.

`offline_continuation_harness.py` drives the existing continuation store only
when constructed with `enabled=True`; the default raises before claiming work
or touching an adapter. Its disposable adapter owns deterministic idempotency
keys and authoritative receipt bytes. The scheduler hashes bytes read directly
from that adapter, never a caller-supplied digest. `FailIfCalledPorts` is the
acceptance sentinel for gateway, provider, network, Slack, or other external
effects.

ENG-110 mapping evidence for this slice:

| Criterion | Offline evidence |
| --- | --- |
| Durable checkpoint | `ContinuationStore` persists stage and next action |
| Restart/reclaim | Expired lease is reclaimed with a fenced generation |
| Effect dedupe | Stable adapter idempotency key and immutable digest |
| Receipt authority | Scheduler hashes adapter-returned/readback bytes |
| Manual resume | Digest mismatch remains blocked until verified resume |
| External isolation | Every external sentinel port raises if called |
| Default off | Missing explicit enablement fails before claim/adapter access |

The receipt is `eng118_offline_acceptance_receipt.json`. Production/live
materialization, runtime scheduler wiring, gateway/provider/network/Slack
effects, and client receipt-authority acceptance remain separate gates.
