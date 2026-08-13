# ENG-28 deterministic failure-injection matrix

**Matrix version:** `eng28-failure-matrix-v1`
**Machine-readable twin:** `agent/durable_jobs/eng28_matrix.json`

Isolated, default-off, disposable SQLite evidence for the durable
Cursor → job → Slack-resume lane. **SQLite is not PostgreSQL. Injected
fakes are not sandbox E2E.** This file is not live Slack authorization,
not live Cursor access, not gateway ingress, and not a production
control plane.

ENG-29 mandatory immutable-Go gating is preserved. Tests must not use
`PYTEST_CURRENT_TEST` or any production auto-grant.

## Proof layers

| Layer | Meaning |
|---|---|
| `PROVEN_LOCAL` | Exercised against real temp SQLite + deterministic fakes / subprocesses; no network |
| `PARTIAL` | Local seam proven, but a required sibling path is missing or only characterized |
| `BLOCKED_EXTERNAL` | Requires PostgreSQL, sandbox/runtime, or live provider/Slack — not claimed from SQLite/fakes |
| `BLOCKED_MISSING_SEAM` | Production architecture has no coherent coordinator; not invented |

Unsupported PostgreSQL / sandbox / runtime rows are never marked passing
from SQLite or fakes.

## Rows

See `eng28_matrix.json` for selectors, proof_layer, and status. Row titles:

1. before/after immutable job/package commit
2. before decision and Go persist/consume
3. concurrent Go/Hold/Cancel and crash during consume+claim
4. effect claim before provider create
5. accepted-lost provider create plus timeout/5xx/busy; lookup/adopt or `PROVIDER_AMBIGUOUS`; never blind retry
6. response before correlation commit; mismatch/orphan fail closed
7. duplicate ingress/concurrent dispatch/restart one job/effect
8. poll transient + missing/multiple/wrong lookup bounded retry then typed Hold
9. cancellation ambiguity reconciliation
10. Slack root intent before send
11. accepted-lost root/status adopt or `REMOTE_DELIVERY_AMBIGUOUS`
12. root accepted before durable bind; no cross bind
13. duplicate/stale/cross-job/workspace actions and ACK-before-decision-commit
14. terminal evidence commit/checkpoint
15. terminal evidence before resume enqueue; accepted enqueue before local mark; stable idempotency key
16. delivery lease/sink accepted-lost/ACK-before-delivered; fail-closed/at-least-once
17. exact lease expiry and clock changes
18. restart each supported nonterminal state executes only persisted permitted `next_action`
19. real OS-process contention for effect/resume/delivery single winner where supported by SQLite
20. datastore unavailable/locked/full and transaction failure no partial mutation
21. unknown policy/version/schema/pruned data default-deny and key nonreuse
22. logs/events redact tokens/prompts and preserve correlation IDs

## External blockers (never PROVEN_LOCAL from this slice)

- PostgreSQL durable store / advisory locks / SERIALIZABLE isolation (ENG-25)
- Sandbox E2E against live Cursor or Slack
- Gateway ingress, production `state.db`, deploy, restart, credentials
