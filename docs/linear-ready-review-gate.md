# Linear Ready review gate (ENG-14): fail-closed source-package check

This runbook covers the **Ready review** vertical slice only. It evaluates a
normalized issue snapshot before any Go path. It is not a launch procedure and
does not start coding or agent work.

## Safety contract

- Side-effect free: input is a normalized issue snapshot + parsed policy.
- No Linear API client, webhook registration, route exposure, service restart,
  deployment, credential changes, or live external activation.
- Ready review **never** dispatches an agent, Graph/Cursor job, or Go handler.
- The existing Issue→Go receipt module (`gateway/linear_go_launch_gate.py` and
  receipt-only webhook intake) remains non-dispatching and separate.

## Decision outcomes

| Decision | Meaning |
|----------|---------|
| `READY_FOR_GO` | Source package is adequate. Emit digest + comment intent for Ready-for-Go. |
| `BLOCKED` | One or more required inputs are missing. Emit actionable reasons. |

`starts_agent_work` is always `false` for both outcomes.

## Required source-package fields

Absence reasons are stable and individually named:

1. `missing_issue_identifier`
2. `missing_issue_title`
3. `missing_issue_description`
4. `missing_acceptance_criteria`
5. `missing_repository_binding`
6. `missing_target_ref`
7. `unresolved_required_inputs`

A passing decision builds an immutable normalized source-package snapshot and a
SHA-256 digest suitable for later persistence/checkpointing.

## Idempotency

`review_key = {issue_id}:{source_package_digest}`

Duplicate Ready deliveries that produce the same key must not emit a second
comment/transition request. Model this at the pure boundary with
`should_emit_review` / `plan_linear_mutation(..., seen_review_keys=...)`.
No persistence layer is required in this slice.

## Mutation adapter boundary (later layer only)

Protocol surface is intentionally narrow: **one comment + one state
transition**. No provider API client ships in this slice.

```text
LinearMutationIntent
  issue_id
  comment_body
  target_state_id
  review_key

LinearMutationPort.apply_comment_and_transition(intent)  # not implemented here
```

### Intended sequence

1. **Ready event** → normalize issue snapshot (outside this module).
2. **Snapshot / review** → `assess_linear_ready_review(snapshot, policy)`.
3. **One comment** → body from the decision (includes digest + what was checked,
   or exact missing reasons).
4. **Transition** → Ready-for-Go **or** Blocked.
5. **Go** is strictly separate and out of scope (receipt-only today; no dispatch).

## Verification

```bash
python -m pytest -q \
  tests/gateway/test_linear_ready_review_gate.py \
  tests/gateway/test_linear_go_launch_gate.py \
  tests/gateway/test_linear_webhook_receipts.py
```

Assess programmatically with
`gateway.linear_ready_review_gate.assess_linear_ready_review(...)`.
Do not interpret `READY_FOR_GO` as approval to start coding, register webhooks,
or activate Go.

## Remaining explicit activation prerequisites (out of scope)

- Wiring a Ready webhook/event into this gate
- Implementing `LinearMutationPort` against Linear
- Persisting review keys / source-package checkpoints
- Any Gateway start/restart, Tailnet/public route, or webhook registration
- Go dispatch / agent execution (must stay fail-closed / receipt-only until a
  separate approved control-plane change)
