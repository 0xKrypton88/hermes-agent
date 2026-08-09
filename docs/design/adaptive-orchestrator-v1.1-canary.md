# Adaptive Orchestrator V1.1 — Slack DM Canary Contract

Status: implemented (code + tests; no live config activation in this change)
Extends: `docs/design/adaptive-orchestrator-v1.md`

## Goal

One trusted Slack DM canary routes each user turn to the cheapest adequate
GPT-5.6 worker family (LUNA / TERRA / SOL) **before** the legacy parent model
is called. All other Slack chats, Desktop, CLI, API, cron, and
untrusted/missing-origin turns remain shadow/legacy.

## Canary identity (exact IDs)

| Dimension | Value |
|---|---|
| platform | `slack` |
| workspace / scope_id | `T0BP4UYH012` |
| chat / channel_id | `D0BNXU62YLD` |
| user_id | `U0BNXPWV8N9` |

Global mode remains `shadow`. Only the exact-ID activation rule elevates this
canary to `active`.

## Trusted TurnOrigin

Activation reads a server-side `TurnOrigin` stamped from authenticated
gateway `SessionSource` / agent session attrs (`platform`, `scope_id`,
`chat_id`, `user_id`). Prompt text and client-supplied trust flags are
**never** authoritative. Absent or untrusted origin can never activate.

## Config example

```yaml
orchestration:
  enabled: true
  mode: shadow                    # global stays shadow
  activation:
    default_mode: shadow          # non-matching trusted turns
    rules:
      - id: slack-dm-canary-v11
        mode: active
        platform: slack
        workspace_ids: ["T0BP4UYH012"]
        channel_ids: ["D0BNXU62YLD"]
        user_ids: ["U0BNXPWV8N9"]
  families:
    LUNA:
      provider_alias: openai-codex
      model_alias: luna
      reasoning_default: low
      toolsets: [file, web]
    TERRA:
      provider_alias: openai-codex
      model_alias: terra
      reasoning_default: medium
      toolsets: [file, web, terminal, browser]
    SOL:
      provider_alias: openai-codex
      model_alias: sol
      reasoning_default: high
      toolsets: [file, web, terminal, browser]
  model_aliases:
    luna: gpt-5.6-luna
    terra: gpt-5.6-terra
    sol: gpt-5.6-sol
  approval:
    require_for_destructive: true
    require_for_financial: true
    workers_cannot_self_approve: true
  telemetry:
    enabled: true                 # local traces only
    store_raw_prompt: false
```

Contradictions (duplicate exact-ID match sets, wildcard rules without IDs,
invalid modes) fail config parsing / startup validation.

## Routing contract (canary)

| Work class | Family | Reasoning |
|---|---|---|
| Ordinary talk / read-only / simple | LUNA | low |
| Normal troubleshooting / multi-step / tool work | TERRA | medium |
| High-consequence / security / production / destructive / financial / heavy | SOL | high / max |

Classifier path: bounded structured LUNA-class intake (deterministic
Swedish+English shortcuts first; optional LUNA structured call; never SOL
merely to classify). Explicit/hard risk cannot be lowered; workers cannot
self-approve. Destructive/financial actions still require approval. Worker
recursion guard remains in force.

Active canary turns must not invoke the legacy GPT-5.6 Sol parent before
worker routing. Parent prompt-cache bytes stay immutable; workers are
isolated child runs.

## Telemetry (local, ID-safe)

Traces record: trusted origin dimensions (platform / workspace / channel /
user IDs), effective mode, matched activation rule id, family, reasoning,
concrete provider/model, and whether legacy parent execution occurred.
Raw prompts and token bodies are never stored.

## Non-goals

No live config deploy/restart, credential/auth changes, trading/exchange
mutation, or broadened activation beyond this exact canary.
