# Phase 1.4 Minimal Local Workflow Example

Status: implemented

This note documents the reliability boundary demonstrated by
`crates/rakka-agent-workflow/tests/minimal_local_workflow.rs`.

The example is intentionally single-process and in-memory. It registers a
workflow definition, accepts a `StartRun` command through `AgentRunInbox`,
recovers the command from the lower-level `rakka-workflow::DurableInbox`,
executes one deterministic planner step, marks the durable inbox entry
completed, and returns an `AgentRunState` with `AgentRunStatus::Completed`.

## What It Proves

- Public command construction uses the Phase 1 command facade.
- `StartRun` is acknowledged only after durable inbox persistence succeeds.
- A fresh local facade can recover the persisted inbox entry from the same
  durable store.
- The persisted inbox payload contains the serialized `AgentCommand` envelope.
- The deterministic step does not require sharding, PostgreSQL, networking,
  OpenTelemetry Collector setup, Kubernetes, model calls, tool calls, timers, or
  human checkpoints.
- Completed inbox entries are no longer returned by `recoverable_inbox`.
- Command acceptance metrics use bounded labels and avoid run id, command id,
  workflow id, and deduplication key labels.

## What It Does Not Yet Prove

- It is not the durable run engine from Phase 2.
- It does not persist `AgentRunState` through a dedicated agent-run store.
- It does not dispatch effects through the durable outbox.
- It does not model timers, retries, human checkpoints, compensation, retention,
  sharding, Kubernetes drain, or multi-pod ownership.

## Reliability Boundary

The correctness boundary for this slice is the durable inbox, not the local
test runner. If the process exits after `AgentRunInbox::accept_command`
returns `Accepted`, the command can be recovered from the durable inbox store.
If the process exits before acceptance returns, no successful acknowledgement
has been issued by the agent facade.

The deterministic runner is deliberately small so later phases can replace it
with the real step runner without weakening the command acceptance contract.
