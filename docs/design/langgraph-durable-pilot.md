# ENG-3 LangGraph Durable-Job Pilot — Package 1

Status: implemented as an isolated, disabled-by-default module  
Digest: `14e3e29f5f37fa7f7fcd2b42da2471ed4df28400cbe417a818c1c170455596bf`  
Code: `agent/durable_jobs/`

## Decision summary

Package 1 proves a durable job + LangGraph checkpoint seam **without**
production integration. The application job store and LangGraph checkpointer
remain distinct. Dispatch is configuration-gated and rejected by default.

## Boundaries

- SQLite paths are explicit and disposable (tests/config only).
- Single-process / dev-only SQLite; production durable store is PostgreSQL-first
  and **not** implemented or provisioned here.
- External systems exist only as injected Protocol ports + fakes
  (`agent/durable_jobs/adapters.py`). No Slack/Cursor/network clients.
- Existing Hermes completion/outbox modules and `state.db` are untouched.

## Minimal state flow

`INTAKE → FREEZE_BASELINE → AWAIT_DISPATCH` (no actual dispatch).

## Follow-on attachment points (not in Package 1)

- Gateway / Slack action wiring
- Cursor/cloud provider dispatch adapter (injected)
- PostgreSQL job store + PostgreSQL LangGraph checkpointer
