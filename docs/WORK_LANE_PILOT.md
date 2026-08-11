# Work-Lane Handoff Pilot (ENG-7)

Status: documentation-only artifact (no runtime behavior change)
Scope: records a durable work-lane handoff contract for the isolated end-to-end workflow pilot
Digest: `a98de8d0b2397382d99dca80317cbd628290e0b8fa98b106eb8a55df623168d9`

## Purpose

Prove that a durable work-lane handoff can be recorded in-repo without modifying
source code, configuration, dependencies, CI, credentials, or service launchers.

## Handoff contract

1. **Linear is the canonical work record.** Issue state, acceptance criteria, and
   durable decisions live in Linear.
2. **Slack is the correlated coordination thread.** Discussion and status updates
   stay in Slack and must reference the Linear work item.
3. **Cursor is the visible writer.** Implementation and documentation edits in this
   pilot are authored through Cursor on an isolated branch.
4. **Normal focused verification failure → `VERIFY_FIX_REQUIRED`.** A focused
   verification miss is a recoverable handoff state, not a terminal block.
5. **Merge / deploy / restart / live mutation require a separate explicit gate.**
   Those actions are out of band for this pilot and must not proceed without an
   independent, explicit approval gate.

## Explicit non-actions

This artifact does not authorize merge, deploy, service restart, credential
exchange, live trading access, order mutation, arming/disarming, reconciliation,
or any external release.
