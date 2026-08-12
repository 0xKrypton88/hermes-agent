# Linear Ready/Go control plane (local-only, fail-closed)

This document covers the **durable Ready freeze + non-dispatched Go intent**
slice. It sits after pure Ready evaluation and explicit-Go launch planning, and
*before* any dispatch boundary.

It does **not** replace:

- `gateway/linear_go_launch_gate.py` (local receipt-listener preflight)
- `gateway/webhook_receipts.py` / webhook receipt-only intake
- `gateway/linear_go_launch_plan.py` (pure Go launch planning)

## Safety contract

`gateway.linear_ready_go_control.LinearReadyGoControlPlane` is local-only:

- Ready path freezes explicitly supplied source/review inputs through a **strict
  identity allowlist** (`[A-Za-z0-9._:-]+`), hashes the frozen source
  deterministically (SHA-256), and persists exactly one immutable Ready review
  receipt for `READY_FOR_GO` (unless `persist_blocked=True`). Mapping intake
  preserves raw identity types (no `str()` coercion) so numeric / non-string
  `issue_id` / `issue_identifier` values fail closed as noncanonical. Ready
  **never** starts work (`starts_agent_work=False`).
- Go path accepts only an exact normalized Go transition whose `issue_id` and
  `issue_identifier` are exact canonical strings (no trim / coercion) and equal
  the matching successful stored Ready provenance (**both** ``review_key`` and
  ``source_digest`` mandatory, canonical, nonempty, and matching the same
  ``READY_FOR_GO`` row exactly — no latest-READY fallback). Optional Go
  ``team_key`` must be exact-canonical when present (padded/non-string reject,
  no trim) and equal the frozen Ready team key. Creates exactly one persistent
  `LaunchIntent(dispatched=False)`. Duplicate delivery/intent keys are
  explainable no-ops.
- Unknown / mismatched / noncanonical identities, cross-team mismatches, stale
  provenance, malformed transitions, and storage failures fail closed.
- never invokes Cursor, LangGraph, subprocess, network I/O, Linear APIs,
  `handle_message`, webhook registration, or listener lifecycle

Existing receipt-only Issue→Go webhook behavior is preserved and untouched.
This control plane is **not** wired into webhook listeners.

## Components

| Module | Role |
| --- | --- |
| `gateway/linear_ready_freeze.py` | Pure Ready evaluation / source freeze + identity allowlist |
| `gateway/linear_ready_go_store.py` | Profile-local SQLite ledger (`linear_ready_go.db`) |
| `gateway/linear_ready_go_control.py` | Durable Ready + Go orchestration |

Storage uses the project SQLite pattern: WAL with DELETE fallback, `synchronous=FULL`,
transactional uniqueness, foreign keys, and a process `RLock`.

Audit keys persisted on Ready: `review_id`, `issue_id`, `issue_identifier`,
`review_key`, `source_digest`, `decision`, `starts_agent_work=0`,
`frozen_source_json`, `created_at`.

Audit keys persisted on Go intents: `intent_id`, `issue_id`, `issue_identifier`,
`review_key`, `source_digest`, `go_event_key`, `idempotency_key`,
`dispatched=0`, `created_at`.

## Activation-gated (explicitly blocked)

These packages exist only as fail-closed placeholders and must stay unimplemented
until a separate activation brief:

| Package | Blocked capability |
| --- | --- |
| `gateway/linear_ready_go_live_ids.py` | Live Linear workspace/team/state ID binding |
| `gateway/linear_ready_go_adapter.py` | Linear API client, webhook registration, Ready mutations |
| `gateway/linear_ready_go_pilot.py` | Pilot/listener arming and Cursor/LangGraph/agent dispatch |

Calling any of those entry points raises `ActivationGatedError`.

## Verification

```bash
scripts/run_tests.sh tests/gateway/test_linear_ready_go_control_plane.py \
  tests/gateway/test_linear_go_launch_plan.py \
  tests/gateway/test_linear_go_launch_gate.py \
  tests/gateway/test_linear_webhook_receipts.py -q
```

Success proves only that Ready provenance and a non-dispatched Go intent can be
derived and persisted locally. It does **not** authorize dispatch, webhook
registration, Gateway restart, Linear mutation, live-ID binding, or pilot
activation.
