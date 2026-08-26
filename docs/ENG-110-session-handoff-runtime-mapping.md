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
| Runtime ingress | `agent.conversation_loop.run_conversation` considers only an explicitly attached controller after `maybe_orchestrate_turn` and after `build_turn_context` has established the parent turn ID. The initialized-agent E2E enters through `AIAgent.run_conversation`. |
| Strict default-off | Runtime construction and client attachment both require literal booleans and the exact offline mode. Disabled/ambiguous values fail before a lane or port call. |
| Safe semantic waypoint | The request-bound injected policy must return `SemanticWaypoint`; the canonical lane rejects unsafe waypoints and unarmed pressure. |
| Durable checkpoint/resume | `DurableLaneService.resume_session_handoff` retains the canonical `SessionHandoffLedger` stage/checkpoint transitions in the disposable durable-job database. |
| Canonical projections | Canonical JSON remains ledger-owned; Linear readback equivalence is checked before advancement. E2E projections are injected SQLite-only ports. |
| Owner/fence semantics | Existing lane writer authority, effect-owner OS guard, owner token, and generation CAS remain the only advancement route. |
| Request/turn-bound authority | Attachment verifies `request.parent_session_id` against the actual agent session and creates authority for one successful current turn only. Ingress rechecks both identities and the prologue's current turn ID before the lane or any port. Terminal authorization outcomes consume that authority with zero handoff effects. |
| Adapter receipt evidence | The initialized-agent E2E reads the exact Slack-shadow receipt bytes back from its injected disposable adapter store (`b"slack:handoff-runtime-1"`). This proves the offline adapter boundary only; it does not claim production transport receipt authority. |
| Restart/reclaim and dedupe | A fresh agent/controller and reopened projection store replay a completed handoff without a second child, injection, receipt, or first turn. Continuation lease reclaim remains covered by the durable continuation suite. |
| Manual resume | A crash after durable child creation leaves `FAILED_CLOSED` plus an in-flight claim. Explicit dead-owner reconciliation and a request with literal `manual_resume=True` continue from the checkpoint without duplicating the child/effects. |
| First-turn path | E2E crosses an initialized `AIAgent`, `run_conversation`, the real `build_turn_context` prologue, orchestration decision boundary, canonical ledger, injected child creation/handoff injection, `start_first_turn`, a fake model client, and real turn finalization. |
| No live effects | All projection/session boundaries are disposable SQLite ports. The initialized SDK client is replaced with an in-memory fake before the turn, socket connects fail if called, orchestration config is injected, and no Gateway/provider/Linear/Slack/service operation is called. Configuration reads are confined to the temporary `HERMES_HOME`. |

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
