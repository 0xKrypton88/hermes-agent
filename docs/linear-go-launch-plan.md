# Explicit-Go launch planning (plan-only)

This document covers the **bounded fail-closed launch planning** slice that
sits *after* receipt-only Go intake and Ready-review provenance, and *before*
any dispatch boundary.

It does **not** replace:

- `gateway/linear_go_launch_gate.py` (local receipt-listener preflight)
- `gateway/webhook_receipts.py` / webhook receipt-only intake
- Ready-review decision production

## Safety contract

`gateway.linear_go_launch_plan.plan_explicit_go_launch(...)` is pure:

- accepts only an explicit normalized Go transition plus Ready-review provenance
- returns a non-dispatched immutable `LaunchIntent` (or fail-closed reason codes)
- never invokes Cursor, LangGraph, subprocess, network I/O, Linear APIs, or
  `handle_message`
- holds no shared mutable state; idempotency uses caller-provided seen-key sets

`dispatched` on a successful intent is always `False`. Persistence and any later
dispatch decision are out of scope for this module.

## Inputs

### Normalized Go transition

| Field | Rule |
| --- | --- |
| `issue_id` | non-blank |
| `issue_identifier` | non-blank string identity (missing/empty/whitespace/non-string fail closed) |
| `target_state` | must normalize to exactly `Go` |
| `previous_state` | non-blank and must not already be `Go` (no duplicate/no-op) |
| `go_event_key` | canonical delivery/idempotency identity: non-empty string matching `[A-Za-z0-9._-]+` with no surrounding/internal whitespace; missing/empty/whitespace/non-string/noncanonical fail closed |

### Ready-review provenance

| Field | Rule |
| --- | --- |
| `issue_id` | same canonical id as the transition |
| `review_key` | non-blank |
| `source_digest` | lowercase 64-char SHA-256 hex |
| `decision` | exactly `READY_FOR_GO` |
| `starts_agent_work` | must be `false` |

## Fail-closed reason codes

Stable codes returned in `LaunchPlanResult.reason_codes`:

- `missing_go_target_state` / `non_go_target_state`
- `blank_issue_id`
- `blank_issue_identifier`
- `blank_go_event_key` / `noncanonical_go_event_key`
- `missing_state_transition` / `noop_duplicate_go_transition`
- `missing_ready_provenance`
- `ready_provenance_issue_mismatch`
- `blank_review_key`
- `invalid_source_digest`
- `ready_decision_not_ready_for_go`
- `ready_starts_agent_work`
- `duplicate_delivery_key`
- `duplicate_intent_key`

## Idempotency

Callers supply:

- `seen_delivery_keys` — previously persisted Go event/delivery keys
- `seen_intent_keys` — previously persisted intent idempotency keys

Deterministic intent key:

```text
go_launch:{issue_id}:{review_key}:{source_digest}:{go_event_key}
```

Duplicate membership in either set returns no intent.

## Verification

```bash
scripts/run_tests.sh tests/gateway/test_linear_go_launch_plan.py \
  tests/gateway/test_linear_go_launch_gate.py \
  tests/gateway/test_linear_webhook_receipts.py -q
```

Success proves only that a plan record can be derived. It does **not** authorize
dispatch, webhook registration, Gateway restart, or Linear mutation.
