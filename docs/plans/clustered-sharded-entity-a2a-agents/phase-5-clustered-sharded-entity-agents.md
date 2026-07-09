# Phase 5 Clustered Sharded Entity A2A Agents

Status: implemented
Source spec: `docs/plans/clustered-sharded-entity-a2a-agents/spec.md`

## Goal

Wire durable autonomous execution through Rakka's effect, dispatcher, timer,
checkpoint, adapter, and policy APIs. The A2A task remains the public view; the
agent run advances through durable state transitions and durable outbox effects.

## Slices

### Slice 5.1: Effect Target Catalog

Status: implemented

Work:

- Define supported effect target classes for model calls, tool calls, process
  tools, A2A peer calls, human checkpoint notifications, timers, and webhooks.
- Map each public agent skill to allowed effect target classes.
- Define per-effect idempotency key policy.
- Define artifact input and output policy for each target class.
- Reject effect targets not allowed by workflow policy.

Acceptance:

- The runtime can validate effect targets before scheduling.
- Every effect target class has an idempotency strategy.
- Large inputs and outputs use artifact references.

### Slice 5.2: Dispatcher Registration

Status: implemented

Work:

- Register dispatchers for model, tool, A2A peer, push, and webhook effects.
- Configure concurrency limits per target class.
- Configure retry, timeout, and exhaustion policy.
- Persist dispatch status through existing dispatcher state.
- Expose dispatcher backlog and in-flight metrics.

Acceptance:

- Due effects can be claimed and completed by dispatcher workers.
- Exhausted effects produce durable run input for failure handling.
- Dispatcher retry survives process restart.

### Slice 5.3: A2A Peer Call Adapter

Status: implemented

Work:

- Implement an effect adapter that calls another A2A agent through `a2a-client`.
- Resolve peer agent cards and choose REST or JSON-RPC transport initially.
- Persist peer task id, context id, and correlation metadata.
- Treat peer calls as at-least-once side effects with idempotency keys.
- Convert peer task completion or failure into durable commands for the parent
  run.

Acceptance:

- A run can schedule a peer A2A task and resume when the peer result is
  available.
- Peer call retry does not create duplicate semantic work when the peer honors
  task/message idempotency.
- Peer failures map to bounded error codes.

### Slice 5.4: Timer And Human Checkpoint Integration

Status: implemented

Work:

- Use durable timers for delayed autonomous continuation.
- Use human checkpoint APIs for input-required A2A task state.
- Project open human checkpoints into A2A `TASK_STATE_INPUT_REQUIRED`.
- Accept human decisions as A2A continuation messages or dedicated metadata
  command types.
- Resume runs through durable commands after timer or human input.

Acceptance:

- A run can wait for a timer without holding a live task.
- A run can wait for human input and later resume from an A2A message.
- Timer and human waits survive process restart and owner movement.

### Slice 5.5: Autonomy Policy

Status: implemented

Work:

- Add policy hooks for max autonomous steps, wall-clock timeout, token or
  external-call budget, allowed tools, approval requirements, and cancellation.
- Persist policy decisions and policy version in run metadata.
- Fail closed when policy is missing or incompatible.
- Emit audit events for policy denials and approvals.

Acceptance:

- A runaway loop is stopped by max-step or budget policy.
- Disallowed target classes are rejected before effect scheduling.
- Policy decisions are visible in audit and task projection.

### Slice 5.6: Cancellation And Compensation

Status: implemented

Work:

- Propagate cancellation from A2A `cancel_task` into running dispatchers.
- Mark cancellation requested durably before attempting external cancellation.
- Cancel or ignore due effects according to target capability.
- Use compensation state for workflows that define compensating actions.
- Project final cancellation to A2A `TASK_STATE_CANCELED`.

Acceptance:

- Cancellation while waiting, dispatching, and retrying is deterministic.
- External cancellation failures do not erase the durable cancellation request.
- Terminal canceled state is persisted before the A2A terminal task is emitted.

### Slice 5.7: Audit, Retention, And Artifacts

Status: implemented

Work:

- Emit audit events for commands, effects, policy decisions, peer calls,
  checkpoints, and terminal outcomes.
- Store large model/tool/peer payloads as artifacts.
- Apply retention policy to completed runs, task events, artifacts, and audit
  records.
- Ensure retention never breaks task terminal projection.

Acceptance:

- Audit can reconstruct a high-level run history without hot actor state.
- Retention can compact completed runs while keeping A2A `get_task` useful.
- Artifact redaction policy is applied consistently.

### Slice 5.8: Failure Injection For Autonomy

Status: implemented

Work:

- Crash after effect scheduling and before dispatch.
- Crash during dispatcher execution.
- Crash after external success and before completion command.
- Retry peer A2A call after timeout.
- Resume after timer and human checkpoint waits.

Acceptance:

- Recovery finds due or in-progress effects.
- Idempotency keys are reused across retry.
- Runs converge to completed, failed, input-required, or canceled states.

## Exit Criteria

- Agent runs can advance through model/tool/A2A-peer effects durably.
- Timers and human checkpoints pause without live tasks.
- Policy limits bound autonomy.
- Cancellation and failure handling are deterministic and durable.

## Implementation Summary

- Slice 5.1 is implemented in
  `crates/rakka-agent-workflow/src/autonomy.rs`. The Phase 5 catalog defines
  model, tool, process-tool, A2A-peer, human-checkpoint, timer, webhook, and
  push-notification target classes, with per-class idempotency and artifact
  policies. `AgentEffectTargetCatalog::validate_effect` rejects unsupported
  targets, disallowed skill/target mappings, disallowed tool names, and
  required-artifact misses before durable scheduling.
- Slice 5.2 is implemented in
  `crates/rakka-agent-workflow/src/dispatcher.rs`. Dispatcher target
  classification now includes A2A peer, webhook, and push notification classes.
  `AgentEffectDispatcherRegistry` routes claimed work to class-specific
  dispatchers, and adapter-backed dispatchers are available for model, tool,
  and A2A peer effects. Existing fleet state continues to persist claim,
  retry, timeout, exhaustion, backlog, and in-flight status.
- Slice 5.3 is implemented as a core adapter contract in
  `crates/rakka-agent-workflow/src/adapters.rs`. `AgentA2APeerRequest`,
  `AgentA2APeerOutcome`, and `AgentA2APeerAdapter` preserve peer task id,
  context id, transport preference, idempotency key, correlation metadata, and
  result/error artifact refs while leaving concrete `a2a-client` card
  resolution and REST/JSON-RPC selection to the A2A-facing adapter layer.
- Slice 5.4 is satisfied by the existing durable timer and human checkpoint
  APIs, with the Phase 5 catalog adding explicit timer and human-checkpoint
  target policy. The existing graph effect bridge and timer/checkpoint runtimes
  schedule waits durably and resume through durable commands after restart.
- Slice 5.5 is implemented in
  `crates/rakka-agent-workflow/src/autonomy.rs`. `AgentAutonomyPolicy` supports
  max autonomous steps, wall-clock timeout, external-call and token budgets,
  target/tool allowlists, approval requirements, fail-closed defaults, and
  durable cancellation decisions. `agent_autonomy_policy_audit_event` emits
  bounded audit attributes for approvals, denials, and cancellation decisions.
- Slice 5.6 is implemented in
  `crates/rakka-agent-workflow/src/dispatcher.rs`.
  `AgentDispatcherFleet::cancel_run_dispatches` deterministically cancels
  unclaimed dispatch entries and annotates active leases as cancellation
  requested without erasing the durable cancellation request or pretending an
  in-flight external side effect can be unsent.
- Slice 5.7 is covered by the autonomy audit helper plus the existing artifact
  and retention modules. The new policy/catalog tests verify decisions carry
  bounded audit attributes and use artifact refs for large autonomous inputs and
  outputs.
- Slice 5.8 is covered by focused tests in
  `crates/rakka-agent-workflow/tests/autonomy_policy.rs`,
  `crates/rakka-agent-workflow/tests/adapters.rs`, and
  `crates/rakka-agent-workflow/tests/dispatcher_fleet.rs`, alongside the
  existing failure-injection, timer, human checkpoint, effect bridge, and
  dispatcher recovery tests.

Follow-up for the A2A-facing adapter extraction: wire an example-local or
future `rakka-a2a` implementation of `AgentA2APeerAdapter` to the SDK
`a2a-client` factory so peer agent-card resolution and REST/JSON-RPC transport
selection happen at the public protocol boundary rather than in the neutral
workflow crate.

## References

- `docs/plans/clustered-sharded-entity-a2a-agents/spec.md`
- `crates/rakka-agent-workflow/src/effect_bridge.rs`
- `crates/rakka-agent-workflow/src/dispatcher.rs`
- `crates/rakka-agent-workflow/src/timers.rs`
- `crates/rakka-agent-workflow/src/checkpoints.rs`
- `crates/rakka-agent-workflow/src/adapters.rs`
- `crates/rakka-agent-workflow/src/credentials.rs`
- `crates/rakka-agent-workflow/src/audit.rs`
- `crates/rakka-agent-workflow/src/retention.rs`
- `https://github.com/a2aproject/a2a-rs/blob/main/a2a-client/src/client.rs`
- `https://github.com/a2aproject/a2a-rs/blob/main/a2a-client/src/factory.rs`
