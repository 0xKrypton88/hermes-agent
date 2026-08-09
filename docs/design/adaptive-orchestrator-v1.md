# Adaptive Orchestrator V1 — Feature-Flagged Control Plane ADR

Status: accepted design record (WP0 — documentation only; not implemented)
Scope: documentation mutation only; no product implementation in this change
Digest: `0e8f54708cb460b0574f40b987f8a7f0518d7c5c2916c25cdf70748536dc22cd`
Base ref: `da3a0a852fd82041ce69e8170bf60bb747782080`

## Decision summary

Adaptive Orchestrator V1 is a **feature-flagged control plane** that may create
**isolated worker runs** for a top-level user turn. It is not a second agent
loop, not a gateway-only shim, and not a live-action or trading subsystem.

The control plane **never mutates** the parent conversation’s model selection,
cached system prompt, message history, or tool schemas mid-session. Parent
prompt-cache stability and strict role alternation remain sacred (see root
`AGENTS.md`). Workers run in isolated child-agent contexts constructed through
existing delegation machinery; their outcomes re-enter the parent only as
ordinary turn results, never by rewriting prior parent context.

## Goals

1. Decide, at a single universal boundary, whether a top-level turn should run
   under legacy execution or spawn an isolated worker run.
2. Reuse Hermes child-agent construction, provider resolution, tool registry
   dispatch, approval binding, and SessionDB auxiliary traces.
3. Preserve legacy behavior in `off` and `shadow` modes.
4. Keep enforcement local, declarative, and auditable — with no outbound
   telemetry and no private chain-of-thought export.

## Non-goals

The following are explicitly out of scope for V1 and must not land under this
ADR’s umbrella:

- Gateway-only implementation (CLI / TUI / desktop / messaging must share one
  agent-loop hook)
- Recursive orchestration (workers must not orchestrate further workers)
- Duplicate transports, tool registries, approval UI, SessionDB, scheduler, or
  credential store
- ML / RL router or learned policy
- UI / dashboard surface for the orchestrator
- Separate long-lived orchestrator service
- Deployment, service restart, or config activation as part of this workstream
- Live action or trading mutation (credentials exchange, live trading access,
  order mutation, arming/disarming, reconciliation are hard-denied)

## Architecture decisions

### 1. Feature-flagged control plane; parent context is immutable mid-session

V1 is gated by a feature flag / mode switch. When active, it may create
isolated worker runs. It must not:

- swap the parent’s model or provider mid-conversation
- rebuild or mutate the parent system prompt
- rewrite parent history
- alter parent tool schemas / toolsets for the life of the parent session

Those parent surfaces stay byte-stable for cache reuse. Worker isolation is the
only allowed divergence.

### 2. Single hook: top-level turns at the universal agent loop boundary

Hook only **top-level** turns at the universal agent / conversation-loop
boundary, **immediately before** `build_turn_context`.

Requirements:

- One hook site shared by all surfaces that enter the agent loop (not a
  gateway-only path).
- Explicit **worker recursion guard**: if the current execution is already a
  worker / child run, the orchestrator must no-op and fall through to legacy
  execution.
- No mid-loop re-entry that would rebuild parent turn context or invalidate
  cached prefixes.

### 3. Reuse `tools/delegate_tool.py` via a typed `WorkerRunRequest` adapter

Worker construction reuses the existing child-agent path in
`tools/delegate_tool.py` (the same machinery behind `delegate_task`).

V1 introduces a typed **`WorkerRunRequest`** adapter that maps orchestrator
intent onto that child-construction API. It does **not** fork a parallel
spawn stack, transport, or subagent runtime.

### 4. Provider / model / reasoning intent via `resolve_runtime_provider`

All worker provider, model, and reasoning intent is routed through
`hermes_cli/runtime_provider.py` → `resolve_runtime_provider`.

No ad-hoc base URL / API key / provider branching inside the orchestrator.
Credential resolution stays in the existing runtime-provider and credential
pool paths; V1 adds no credential store.

### 5. Static worker toolsets; registry-owned risk metadata; late enforcement

- Worker toolsets are **static** for a worker run (chosen up front; not mutated
  mid-worker-session).
- Risk metadata is **declarative** and **registry-owned** (declared with tool
  registration / tool metadata), not inferred by an ML policy.
- Enforcement runs **after** middleware has finalized tool arguments and
  **before** registry dispatch. That is the only authoritative deny/approve
  gate for risky worker tool calls.

### 6. Approval binding is exact and digest-scoped

Approvals bind all of:

- session identity
- turn identity
- tool-call identity
- tool name
- canonical digest of the **final** arguments / action

If the action changes after approval (arguments digest differs, tool name
differs, or call identity is new), a **new approval** is required. Reuse of a
prior approval for a mutated action is forbidden.

V1 reuses the existing approval hooks / UX paths; it does not introduce a
second approval UI.

### 7. Local versioned traces only; no outbound telemetry or private CoT

Tracing reuses:

- SessionDB auxiliary usage records
- existing tool hooks
- existing approval hooks

Traces are **local** and **versioned**. V1 must not emit outbound telemetry,
usage attribution to third parties, or private chain-of-thought content.

### 8. `off` and `shadow` preserve legacy execution

| Mode | Behavior |
|---|---|
| `off` | Orchestrator is inert. Legacy top-level execution only. |
| `shadow` | May observe / record a routing decision or would-be worker plan, but **execution remains legacy**. No worker side effects replace the parent turn. |
| (active / on — later WP) | May create isolated worker runs per this ADR; still must not mutate parent cached context. |

`off` and `shadow` are required so enabling the flag for observation cannot
change user-visible execution.

### 9. Non-goals restated as hard boundaries

Implementations claiming Adaptive Orchestrator V1 compliance must reject
gateway-only hooks, recursive worker orchestration, duplicated infra
(transports / registries / approval UI / SessionDB / scheduler / credential
store), ML/RL routers, dashboard/UI work, separate services, deployment /
restart / config-activation tasks, and any live-action or trading mutation
paths.

## Implementation surface (ADR-level, not code)

Documentation-level map of where future WPs are expected to touch — recorded
here so later diffs can be reviewed against intent. This WP creates **no**
product code.

| Surface | Role under V1 |
|---|---|
| Agent / conversation loop (universal entry) | Single pre-`build_turn_context` hook + worker recursion guard |
| Feature flag / mode (`off` \| `shadow` \| active) | Config-shaped behavioral gate (not a new secret env var; no activation in this WP) |
| `WorkerRunRequest` adapter | Typed bridge into `tools/delegate_tool.py` child construction |
| `hermes_cli/runtime_provider.resolve_runtime_provider` | Sole resolver for worker provider/model/reasoning intent |
| Tool registry + declarative risk metadata | Static worker toolsets; risk labels owned by registry metadata |
| Pre-dispatch enforcement point | After finalized middleware arguments, before registry dispatch |
| Existing approval hooks | Bind session, turn, tool-call, tool name, canonical args digest |
| SessionDB auxiliary usage + tool/approval hooks | Local versioned traces only |
| Parent session context | Immutable mid-session (model, system prompt, history, tool schemas) |

Out of surface: gateway-only adapters as the sole hook, new scheduler, new
credential store, new approval UI, dashboard pages, separate orchestrator
process, deployment/restart scripts, trading/live-order code paths.

## Verification strategy (ADR-level)

Future implementation WPs should prove the following contracts. This WP only
records the strategy; it does not add tests.

1. **HEAD / scope hygiene** — documentation-only change is limited to this ADR
   path; product code remains untouched in WP0.
2. **Hook placement** — a single top-level hook runs immediately before
   `build_turn_context` on the universal loop; worker/child executions hit the
   recursion guard and do not re-enter orchestration.
3. **Parent immutability** — under orchestrator activity, parent model, system
   prompt bytes, history, and tool schemas remain unchanged for the session
   (cache-safe).
4. **Delegation reuse** — worker runs are constructed only through the
   `WorkerRunRequest` → `delegate_tool` child path; no parallel spawn stack.
5. **Runtime provider reuse** — worker provider/model/reasoning resolution
   calls `resolve_runtime_provider` (no bypass).
6. **Enforcement ordering** — risky tools are checked after argument
   finalization and before registry dispatch; static worker toolsets do not
   change mid-run.
7. **Approval binding** — approvals fail closed when session/turn/call/name /
   args-digest tuples differ; mutated actions require fresh approval.
8. **Mode matrix** — `off` and `shadow` produce legacy execution results;
   shadow may record intent locally but must not replace legacy side effects.
9. **Telemetry boundary** — traces land only in local SessionDB auxiliary /
   existing hooks; no outbound telemetry; no private chain-of-thought export.
10. **Safety denies** — no credentials exchange, live trading access, order
    mutation, arming/disarming, reconciliation, deployment, or service restart
    surfaces are introduced by V1 work.

Preferred proof style for later WPs: hermetic tests via `scripts/run_tests.sh`
against a temp `HERMES_HOME`, asserting invariants and ordering rather than
change-detector snapshots. E2E paths for resolution, approval binding, and
mode matrix should exercise real imports, not only mocks.

## Compatibility and rollout posture

- Default posture remains legacy execution (`off`) until a later WP deliberately
  enables observation (`shadow`) or active worker creation.
- This ADR does not activate config, deploy services, or restart processes.
- Cloud / contributor work under this digest is limited to code, tests,
  simulation/replay, and synthetic/test data in later WPs — never live trading
  or credential exchange.

## Relationship to Hermes invariants

- **Prompt caching is sacred** — parent cached prefix and system prompt stay
  stable; workers are isolated runs, not mid-session parent rewrites.
- **Narrow waist** — V1 is a control-plane decision + reuse of delegation, not
  a new core tool schema paid on every API call.
- **Extend, don’t duplicate** — child agents, runtime provider, registry,
  approvals, and SessionDB are reused rather than reimplemented.

## WP0 deliverable

Exactly one documentation file:

`docs/design/adaptive-orchestrator-v1.md`

No production code, tests, config, services, runtime, dependency, or generated
files are modified by WP0.

## V1.1 follow-on

Exact-ID Slack DM canary activation, trusted `TurnOrigin`, bilingual
LUNA-class classifier path, and openai-codex family model maps are specified
in `docs/design/adaptive-orchestrator-v1.1-canary.md`.
