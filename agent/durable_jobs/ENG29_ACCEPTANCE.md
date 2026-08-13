# ENG-29 acceptance specification (local policy-contract evidence)

**Matrix version:** `eng29-matrix-v1`
**Scope:** isolated, default-off, single-file SQLite durable-jobs pilot
(dev/test only). This file is **local policy-contract evidence**. It is
**not** Slack authorization, **not** live provider authorization, **not**
gateway ingress, and **not** a PostgreSQL or production control plane.

No deploy, restart, cutover, migration, promotion, or financial adapter is
implemented here. Categories without an adapter are enforced by the
classifier and authorization guard only.

## Immutable action matrix

The executable matrix lives in `agent/durable_jobs/eng29.py`
(`ACTION_MATRIX`, `ADAPTER_TARGET_ACTIONS`, `MATRIX_VERSION`). That module
is the versioned contract. Changing category membership, default-deny
behavior, or adapter bindings requires a new matrix version string — in-place
mutation of an already-registered tuple is rejected.

| `target_action` / category | Disposition | Adapter in this slice |
|---|---|---|
| `scope_change` | mandatory Go | none (classifier/guard only) |
| `missing_prerequisites` | mandatory Go; also fail-closed unless prerequisites are marked satisfied | none (classifier/guard only) |
| `unresolved_provider_ambiguity` | mandatory Go; also fail-closed unless ambiguity is marked resolved | none (classifier/guard only) |
| `deploy` | mandatory Go | none (classifier/guard only) |
| `restart` | mandatory Go | none (classifier/guard only) |
| `cutover` | mandatory Go | none (classifier/guard only) |
| `production_migration` | mandatory Go | none (classifier/guard only) |
| `external_promotion_release` | mandatory Go | none (classifier/guard only) |
| `financial_action` | mandatory Go | none (classifier/guard only) |
| `cursor.create_run` | mandatory Go (explicit adapter binding) | injected fake `create_run` only |
| `slack.post_root` | mandatory Go (explicit adapter binding) | injected fake `post_root` only |
| unknown / unclassified / empty | **default deny / require Go** | none |

## Authorization tuple (immutable)

A durable authorization tuple is bound to:

- `job_id`
- `source_package_id`
- `source_package_version`
- `candidate_sha`
- `candidate_id`
- `candidate_version`
- `target_environment`
- `target_action`
- authorized actor
- expiry
- policy version
- matrix version
- replay / idempotency key

Conflicting re-registration (different authority under the same job+action or
the same idempotency key) is **rejected**. Exact same tuple may duplicate.
There is no upsert that changes authority.

## Guard

A single reusable guard runs **before** provider/Slack effect claim, stale
takeover, recovery lookup, `create_run`, and `post_root`. Adapter identity is
derived from the job row, live policy, and the effect/binding
`candidate_id`/`candidate_version` — never from hardcoded defaults or the
stored tuple actor. Missing or mismatching identity fields default-deny.
The current live `job_authz_policies` row is revalidated on every guard:
absent, expired, revoked/inactive, malformed, actor-mismatched, or
policy-version-mismatched fail closed even when an accepted Go and the
immutable tuple are still unexpired under the same policy version.
Candidate/package/SHA/environment/action/matrix bindings are preserved.
Mandatory and unknown actions require an **exact matching, unexpired ACCEPTED
Go**. Hold, Cancel, tuple mismatch, expiry, missing prerequisites, or
unresolved provider ambiguity fail closed: zero new claim persistence and
zero injected adapter calls.

**Claim / stale takeover atomicity.** Live mandatory-Go validation and the
durable initial-claim or stale-takeover mutation/event share the caller's
active SQLite connection (IMMEDIATE write transaction). A concurrent policy
delete, revoke, expiry, version, or actor change cannot commit between
validation and the claim/takeover write. Cancellation checks, CAS, exact
tuple binding, default-deny, and event atomicity are preserved.

**External-call boundary (not atomic).** Recovery lookup, injected
`create_run`, and injected `post_root` re-run the latest-safe guard on a
private connection immediately before the adapter call. A SQLite transaction
cannot be atomic with a network RPC. A policy revoke or Cancel that commits
after that snapshot may still race an in-flight adapter call; adapters cannot
abort an outstanding RPC. Bind stays fail-closed.

**Actors.** `allowed_actors_json` must be a JSON list of non-empty strings.
Each element is stripped; empty or whitespace-only strings, numbers, objects,
nested arrays, booleans, null, non-list JSON, and malformed JSON default-deny.
Malformed elements are never stringified.

Terminal Cancel remains authoritative. Existing owner-token / lease / inflight
fencing is unchanged. This SQLite path does not claim distributed isolation.
