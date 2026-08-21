# Linear Issue→Go receipt gate: local launch preflight

This runbook covers the **ENG-13 receipt-only** boundary. It is a configuration
preflight, not a launch procedure.

## Safety contract

- The route may only persist an idempotent receipt keyed by `(provider,
  delivery_id)`.
- It must not dispatch an agent, create a job, mutate a provider, or change a
  Linear issue.
- The preflight module is side-effect free: it reads a parsed configuration
  object only.
- `LOCAL_READY` means that a configuration would be constrained to loopback.
  It never authorizes an external listener, Tailnet route, Linear webhook,
  Gateway start/restart, or pilot.

## Required local-only shape

Keep this out of an active configuration until a separate launch decision.
The secret is a placeholder and must be supplied through the normal secret
mechanism, never committed.

```yaml
platforms:
  webhook:
    enabled: true
    extra:
      host: 127.0.0.1
      port: 8644
      routes:
        linear-go:
          secret: ${LINEAR_GO_WEBHOOK_SECRET}
          receipt_only: linear_issue_go
          allowed_state_ids:
            - <approved-Go-state-id>
          events:
            - Issue
```

The preflight intentionally returns `BLOCKED` when any of the following holds:

1. `platforms.webhook.enabled` is not exactly `true`.
2. The host is unset, broad, or not loopback.
3. There is not exactly one `receipt_only: linear_issue_go` route.
4. The route has no secret, Go-state allowlist, or `Issue` event filter.

## Verification

Run the focused test suite before considering a configuration change:

```bash
python -m pytest -q tests/gateway/test_linear_go_launch_gate.py \
  tests/gateway/test_linear_webhook_receipts.py
```

A local config can be assessed programmatically with
`gateway.linear_go_launch_gate.assess_linear_go_launch_gate(parsed_config)`.
Do not interpret a `LOCAL_READY` result as approval to launch.

## Separate external-activation gate

The following are **out of scope** for this preflight and require an explicit
human Go decision plus a bounded change plan:

- starting or restarting the Gateway;
- binding any listener, including `127.0.0.1:8644`;
- exposing a Tailnet or public route;
- registering, changing, or enabling a Linear webhook;
- sending a test delivery from Linear;
- dispatching an agent or promoting a receipt to work.

## Rollback

If an approved future launch is underway, reverse external changes first:

1. Disable/remove the Linear webhook at its provider boundary.
2. Remove any external route/forwarder.
3. Stop the dedicated listener or restore the previous Gateway configuration.
4. Re-run the receipt suite and confirm no listener remains on port 8644.
5. Preserve receipt storage for audit/reconciliation; do not delete receipts
   merely to roll back a listener.
