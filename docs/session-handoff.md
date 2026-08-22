# Durable session handoff (default-off pilot)

The `agent.durable_jobs.session_handoff` path reuses the durable-job SQLite
database and mutation lease. It is inactive by default and has no live Linear,
Slack, child-session, or provider client. Those boundaries are injected ports.
External effects require the exact boolean pair `enabled is True` and
`shadow is False`; non-boolean truthy/falsy values are rejected before the
coordinator is constructed. The effectful coordinator exists only in the
authorized lane method's local scope, so it cannot be imported and called
around activation, identity, or mutation-lease checks. A normally constructed
ledger is read-only: every durable mutation helper also requires an opaque,
identity-checked mutation authority bound to the exact SQLite database, the
authorized durable job, and an exact live ticket in closure-owned lane state.
Only the two legitimate lane entrypoints can reach ticket issuance; the builder
and decorator bindings are deleted after class construction, while the
importable validator is read-only. Issuance re-runs enabled/identity/owner
authorization and nests the exact ticket inside a live mutation lease. Every
mutation revalidates ticket, thread, job, and database. Replacing the stored
value, constructing or subclassing the slotted authority, importing a direct
issuer, reusing a ticket after lease release, or crossing job/database/thread
boundaries fails closed before SQL. Plain-ledger construction never initializes
or migrates schema. Renaming helpers private is not treated as authorization.
This prevents
a caller that merely constructs a ledger from forging completed effects without
invoking adapters. Shadow mode refuses the coordinator call rather than invoking
injected ports.

## Policy

`SessionHandoffConfig.default_shadow()` defines a soft arm at 45% and a hard
pre-compression threshold at 80% only for
`openai-codex/gpt-5.6-sol`. Policies are configurable per provider/model. A
handoff requires both an armed pressure reading and a verified semantic
waypoint. Hard pressure never permits a handoff during a tool call, external
mutation, commit, push, deployment, or authority boundary.

## Durable sequence

The canonical sequence is:

`STAGED -> LINEAR_VERIFIED -> SLACK_RECEIPTED -> CHILD_CREATED -> HANDOFF_INJECTED -> FIRST_TURN_STARTED -> COMPLETE`

The durable record is canonical. Linear and Slack are projections. Every
external port receives a stable, stage-specific idempotency key.

## Failure and manual resume

Any unverifiable boundary records `FAILED_CLOSED`, preserves the last durable
checkpoint stage, and stores only the exception type. The persisted failure is
a fence: ordinary or stale callers cannot advance it; only an explicit
`manual_resume=True` transition clears it back to the checkpoint. The flag must
be an actual boolean; truthy strings and integers are rejected. Canonical
handoff text is passed through the durable-job password/DSN redactor before it
is hashed, persisted, or projected. Raw provider exception text, prompt fields,
and reasoning fields are not stored.

The resume pointer and first-turn action are projected from the redacted
canonical payload, not from the raw caller object. Secret-bearing idempotency
keys are rejected before staging, and the Linear issue selector must be a
strict secret-free identifier. Before any effect, the lane also requires a
persisted Slack workspace binding and verifies the handoff repository and a
non-empty frozen SHA against the durable job.

After fixing the boundary, an operator may call the same
`resume_session_handoff(...)` request with `manual_resume=True`. The same
`handoff_id`, canonical payload, idempotency key, parent session, and ports must
be supplied. The coordinator resumes from the last checkpoint. Stable adapter
idempotency keys remain defense in depth, not the crash fence.

Before every external effect, the lane-internal coordinator takes an OS-backed
effect-owner lock and the ledger atomically creates a persistent `IN_FLIGHT`
claim with an owner token and monotonically increasing generation. A successful
adapter return is committed only when both owner and generation still match,
together with `APPLIED` and the next checkpoint in one SQLite transaction. A
concurrent caller or restarted process that observes an unresolved claim raises
`EffectReconciliationRequired` and never invokes that effect again.

An operator must verify the external system and use
`reconcile_session_handoff_effect(...)` under the lane's authorized mutation
lease. `outcome="APPLIED"` requires a secret-free verification receipt; it marks
the claim applied and advances only the immediately following checkpoint
without replay. `outcome="NOT_APPLIED"` marks the claim retryable. If the
handoff is already `FAILED_CLOSED`, reconciliation preserves that fence and the
operator must still use `manual_resume=True` afterward. Manual resume refuses
to clear a failure while any `IN_FLIGHT` claim remains unresolved.

Reconciliation is effectful and uses the same `enabled=True, shadow=False`
gate as resume. It also revalidates the staged canonical repository and exact
SHA against the current durable job. The operator must submit the exact owner
token and generation observed for the ambiguous claim plus an explicit
dead-owner witness. Reconciliation must also acquire the same non-blocking
OS-backed owner lock; a lock still held by a live effect caller fails closed.
The lane acquires its mutation lease before job lookup or ledger construction, so
constructor schema DDL and identity checks cannot race `close()`. This prevents
reconciliation from invalidating a live owner and prevents stale owners from
completing or fail-closing a reassigned generation, including when an owner token
is reused. Each effect guard first binds a deterministic, kernel-owned loopback
TCP endpoint derived only from `(job_id, handoff_id, effect_name)`, then takes
two ordered file locks. The socket namespace has no replaceable filesystem
pathname, so a live owner cannot be bypassed by replacing the global lockfile,
a database pathname, or one hardlink alias while another remains open. An
endpoint collision or an endpoint occupied by another process fails closed.
The two additional namespaces are derived from the SQLite file's filesystem
identity (device plus inode/file index) and the normalized absolute pathname.
The host-global socket intentionally serializes matching effect identities
across otherwise independent ledger files on the same host; safety takes
precedence over that concurrency. The ledger captures the file identity at
construction, revalidates it after all owner locks are acquired and
immediately around every SQLite open, and fails closed if pathname replacement
changes the database identity.
Secret-shaped receipts are rejected before persistence. Legacy effect tables
are upgraded transactionally with the generation and reconciliation-receipt
columns before use.

The crash-window regression test terminates a real subprocess after a simulated
external child effect but before durable stage advancement. The restarted
coordinator stays fenced until explicit verified reconciliation and does not
create the child again.

## Disable and rollback

- Keep `SessionHandoffConfig.enabled=False, shadow=True` (the shipped default)
  to disable all effects. Setting only `enabled=True` remains side-effect-free;
  activation requires the explicit pair `enabled=True, shadow=False`.
- Do not wire live ports or call `resume_session_handoff` during rollback.
- Existing `session_handoffs` rows may remain for audit/recovery; deleting them
  is not required and would remove the canonical resume record.
- Production activation, Gateway restart, configuration mutation, and writer
  cutover require separate approval and client E2E verification.
