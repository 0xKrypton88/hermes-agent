# ENG-25 PostgreSQL backup / restore / restart acceptance (sandbox only)

This is a **contract**, not a production runbook and not a live restore.
Cursor Cloud must not execute restore, touch credentials, or mutate a
real database. Local Hermes may run the destructive sandbox only under
its own gate.

## Paired snapshot scope

One restore point includes **both**:

- Application schema objects: `durable_jobs_meta`, `durable_jobs`,
  `durable_job_events`, `durable_job_advance_claims` (plus later-slice
  ledger tables if present).
- Checkpointer schema objects: `durable_checkpoint_meta`, LangGraph
  `checkpoints` / `checkpoint_blobs` / `checkpoint_writes` /
  `checkpoint_migrations`.

Restoring only one schema is an inconsistent job: graph thread state
would not match application phase/events.

## Consistency / quiesce assumptions

- No in-flight `create_and_advance`.
- No open application or checkpointer transactions.
- No unexpired `durable_job_advance_claims` lease (`status=claimed`).
- Capture application + checkpointer in one logical snapshot.

## Owner / ACL restoration

- Schema owner must be the role that will reopen the stores
  (`CURRENT_USER` at restore time).
- Domain markers (`hermes.durable_jobs.application` /
  `hermes.durable_jobs.checkpointer`) and `owner_role` must be restored
  with the tables. Reopen fail-closes on empty, unmarked, foreign, or
  wrong-owner schemas.

## Clean-database restore

- Target a disposable sandbox database named `hermes_dj_sandbox_*` on
  loopback only.
- Drop/create that database (or `DROP SCHEMA ... CASCADE` inside it)
  before restore. Never restore onto leftover production schemas.

## Post-restore validation

1. Reopen `PostgresDurableJobStore` (domain + version + owner).
2. Reopen the Postgres checkpointer (checkpointer domain marker).
3. `recover_job(job_id)` matches pre-snapshot phase.
4. `list_events(job_id)` matches pre-snapshot event types/order.
5. LangGraph thread `job_id` still loads from the checkpointer.
6. Restart acceptance: a new process repeats steps 1–5.

## Tooling gate

`agent/durable_jobs/sandbox_backup.py` defaults **fail-closed**. It
requires explicit disposable DSNs and
`--i-understand-this-destroys-disposable-data`. Cursor Cloud tests
exercise the gate only; they never call `pg_restore`.
