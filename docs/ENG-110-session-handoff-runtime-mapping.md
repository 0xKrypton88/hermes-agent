# ENG-110 offline session-handoff runtime evidence

This slice connects the previously verified `SessionHandoff` and continuation
contracts to the real Hermes top-level turn ingress without activating a live
product path. The only gate is an injected `SessionHandoffRuntime` constructed
with the exact values `enabled=True` and `mode="offline_shadow_test"`, then
attached to one agent with another exact `enabled=True`. No config, environment,
service startup, Gateway, provider, Linear, Slack, or child-session client is
looked up or constructed.

The shipped/default path has no `_session_handoff_runtime` attribute. At turn
ingress it performs one attribute read, does not import the pilot module, and
does not open or create a handoff database. The runtime observation does not
replace the user message, mutate the cached prompt/toolset/model, or change the
legacy/orchestrator control decision.

## Criterion mapping

| ENG-110 criterion | Product boundary and offline proof |
|---|---|
| Runtime ingress | `agent.conversation_loop.run_conversation` invokes only an explicitly attached controller before `maybe_orchestrate_turn`; the E2E enters through that real boundary. |
| Strict default-off | Runtime construction and client attachment both require literal booleans and the exact offline mode. Disabled/ambiguous values fail before a lane or port call. |
| Safe semantic waypoint | The request-bound injected policy must return `SemanticWaypoint`; the canonical lane rejects unsafe waypoints and unarmed pressure. |
| Durable checkpoint/resume | `DurableLaneService.resume_session_handoff` retains the canonical `SessionHandoffLedger` stage/checkpoint transitions in the disposable durable-job database. |
| Canonical projections | Canonical JSON remains ledger-owned; Linear readback equivalence is checked before advancement. E2E projections are injected SQLite-only ports. |
| Owner/fence semantics | Existing lane writer authority, effect-owner OS guard, owner token, and generation CAS remain the only advancement route. |
| Request-bound receipt authority | The ingress object binds job, parent, canonical handoff, pressure inputs, and manual-resume intent; projection receipts are accepted only by the claimed effect stage. The continuation harness separately hashes bytes read back from its authoritative disposable adapter. |
| Restart/reclaim and dedupe | A fresh agent/controller and reopened projection store replay a completed handoff without a second child, injection, receipt, or first turn. Continuation lease reclaim remains covered by the durable continuation suite. |
| Manual resume | A crash after durable child creation leaves `FAILED_CLOSED` plus an in-flight claim. Explicit dead-owner reconciliation and a request with literal `manual_resume=True` continue from the checkpoint without duplicating the child/effects. |
| First-turn path | E2E crosses real agent client attachment, conversation ingress, lane policy, canonical ledger, injected child creation/handoff injection, and `start_first_turn`. |
| No live effects | All projection/session boundaries are disposable SQLite ports; fail-if-called external ports remain injected and untouched. Tests stop before model/provider execution. |

Machine-readable evidence is recorded in
`docs/eng-110-session-handoff-runtime-receipt.json`. This receipt is an offline
test mapping, not production activation approval.

## Remaining production gates

- Define and approve live request construction and a session-scoped enablement
  source; this slice deliberately has neither.
- Bind a production adapter's authoritative receipt bytes and idempotency
  contract before any external delivery is allowed.
- Approve real Linear/Slack/session projection implementations and their
  reconciliation/operator workflow.
- Validate multi-process scheduler ownership, lease timing, observability,
  rollout/rollback, and Gateway topology without weakening prompt caching.
- Run an explicitly authorized staging acceptance before any config change,
  service restart, deployment, or live datastore migration.
