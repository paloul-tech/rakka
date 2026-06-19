# Rakka Agentic Workflow Implementation Plan

Status: planning draft
Date: 2026-06-17
Source spec: `docs/plans/agentic-workflow/agentic-workflow-spec.md`

## Purpose

This plan turns the agentic workflow spec into an implementation roadmap. It
keeps the spec's reliability boundary intact: core actor, remote, and sharded
delivery remain at-most-once, while long-running agent reliability is built
through durable workflow state, durable inbox acceptance, durable outbox
effects, idempotency keys, recovery, observability, and explicit policy.

The plan is organized as high-level phases. Each phase is broken into slices
that can be implemented, reviewed, tested, and documented independently.

## Evaluation Summary

The spec is directionally strong and correctly builds on Rakka's existing
foundations:

- `rakka-workflow` already provides durable inbox/outbox semantics,
  deduplication, retries, and recovery.
- `rakka-persistence` and `rakka-persistence-postgres` already provide durable
  state, event journal, snapshots, and revision fencing.
- `rakka-sharding` and `rakka-sharding-postgres` already provide stable entity
  identity, shard ownership, graceful handoff, coordinator storage, leases, and
  remembered entities.
- `rakka-process`, `rakka-stream`, `rakka-http`, and `rakka-grpc` provide tool,
  bounded-output, and public-ingress integration points.
- `rakka-k8s` already has readiness, liveness, drain, compatibility, and
  shutdown hooks that can be extended for workflow-aware operation.
- `rakka-core` already has metrics, tracing integration points, and operational
  snapshots, which gives the OpenTelemetry plan a place to attach.

The main risk is scope coupling. The spec includes a workflow engine, timer
service, dispatcher fleet, human checkpoints, model/tool adapters, query
indexes, retention, audit, OpenTelemetry, and Kubernetes deployment. Building
all of that as one large crate change would hide the reliability semantics and
make testing too diffuse.

Recommended implementation shape:

- Add an additive `rakka-agent-workflow` crate for first-class agent concepts.
- Keep `rakka-workflow` as the lower-level durable inbox/outbox substrate.
- Add shared helpers to existing crates only when the agent layer exposes a real
  gap in the substrate.
- Make OpenTelemetry schemas part of the early domain model, but defer native
  SDK/OTLP exporter wiring until after the first durable runner works.
- Treat Kubernetes deployment as a scale validation phase, not the first
  implementation milestone.

## Release Targets

`MVP`

- A single agent run can start through a durable inbox, execute a simple step,
  schedule a fake effect through durable outbox, pause for human input, resume,
  complete, recover after process restart, and expose bounded metrics,
  snapshots, logs, and traces.

`Scale Preview`

- Agent runs are sharded across multiple Rakka nodes, dispatcher workers claim
  due work, timers are durable, query indexes support operational views, and a
  local Collector receives OTLP telemetry.

`Production Candidate`

- PostgreSQL-backed state, indexes, audit, and coordinator leases are covered by
  failure-injection tests; Kubernetes manifests or Helm-style templates cover
  startup, drain, rolling update, autoscaling metrics, Collector topology,
  network policy, and PodDisruptionBudget guidance.

## Design Rules

- Every externally accepted command must be durably recorded before
  acknowledgement.
- Every external side effect must be represented as a durable outbox entry
  before execution.
- Human pauses, timers, and model/tool waits must not hold live tasks or
  unbounded mailbox state.
- Large prompts, completions, files, and tool outputs should be stored as
  artifact references, not inline hot state.
- Workflow ids, run ids, entity ids, prompt text, and full error text must not
  become hot metric labels.
- Trace context should be persisted across durable boundaries; long pauses and
  retries should use span links instead of long-lived spans.
- Kubernetes drain improves availability, but correctness must come from durable
  recovery after abrupt pod death.

## Phase 0: Plan Lock and Substrate Audit

Goal: establish the minimum API shape, ownership boundaries, and test harness
before adding runtime behavior.

### Slice 0.1: Crate and API Boundary Decision

Status: implemented.

Scope:

- Decide whether the first implementation lands in a new
  `rakka-agent-workflow` crate or under `rakka-workflow::agent`.
- Preferred path: create `rakka-agent-workflow` as an additive facade over
  `rakka-workflow`, then re-export selected stable pieces through `rakka`.
- Define feature flags for optional integrations such as HTTP/gRPC examples,
  process-backed tools, PostgreSQL integration, and OpenTelemetry SDK wiring.

Deliverables:

- Short API boundary note in the implementation PR or docs.
- Initial crate/module map.
- List of types that must remain substrate-level in `rakka-workflow`.

Acceptance:

- Existing workspace tests remain unaffected.
- The new API boundary does not require changing the reliability semantics of
  `rakka-workflow`.

Implementation notes:

- Added `crates/rakka-agent-workflow` as the additive agent orchestration
  facade.
- Kept `rakka-workflow` as the durable inbox/outbox substrate.
- Added the optional `agent-workflow` feature and `rakka::agent_workflow`
  re-export in the top-level `rakka` facade.
- Recorded the boundary decision, initial module map, feature map, and
  substrate-owned type list in
  `docs/plans/agentic-workflow/phase-0-1-api-boundary.md`.

### Slice 0.2: Domain Data Contract Draft

Status: implemented.

Scope:

- Draft serializable data contracts for `AgentRunState`, `AgentWorkflow`,
  `AgentStep`, `AgentEffect`, `HumanCheckpoint`, `ArtifactRef`,
  `AgentTelemetryContext`, and `AgentAuditEvent`.
- Define identifier types for workflow id, run id, step id, effect id,
  checkpoint id, command id, causation id, and correlation id.
- Define versioning fields for workflow definition version and serialized state
  schema version.

Deliverables:

- Domain model module with serde-compatible structs/enums.
- Round-trip tests for all persisted shapes.
- Explicit policy for high-cardinality fields.

Acceptance:

- Persisted structs can be serialized and deserialized without application code.
- Large payload fields are represented by artifact references or application
  extension points.

Implementation notes:

- Added `crates/rakka-agent-workflow/src/domain.rs` with serde-compatible
  domain contracts for agent workflow definitions, run state, steps, effects,
  human checkpoints, artifact references, telemetry context, and audit events.
- Added explicit id newtypes for workflow, run, step, effect, checkpoint,
  command, causation, correlation, audit event, tenant, and workflow definition
  version values.
- Added `StateSchemaVersion` and `AgentTimestampMillis` for persisted version
  and timestamp fields.
- Added high-cardinality policy constants that separate forbidden hot metric
  fields from bounded metric fields and trace/log/audit id fields.
- Added round-trip JSON tests for the persisted shapes in
  `crates/rakka-agent-workflow/tests/domain_contract.rs`.

### Slice 0.3: Agent Test Harness Foundation

Status: implemented.

Scope:

- Add test helpers for fake clocks, fake model/tool adapters, fake artifact
  stores, fake audit sinks, and deterministic workflow ids.
- Reuse existing in-memory persistence, metrics, and actor testkit utilities.

Deliverables:

- Agent workflow testkit helpers, either inside `rakka-agent-workflow` tests or
  later promoted to `rakka-testkit`.
- A minimal fixture that can run a workflow with no network, PostgreSQL, or
  Kubernetes dependencies.

Acceptance:

- Unit tests can exercise start, duplicate start, effect scheduling, and state
  serialization deterministically.

Implementation notes:

- Added `crates/rakka-agent-workflow/src/testkit.rs` behind the `testkit`
  feature and for crate-local unit tests.
- Added deterministic id generation, fake clock, in-memory artifact store,
  fake model/tool adapters, fake audit sink, and a `MinimalAgentFixture`.
- Built the fixture on the real `rakka-workflow::DurableInbox` plus
  `rakka-persistence::InMemoryDurableStateStore` so early tests exercise the
  substrate boundary.
- Wired `rakka-agent-workflow/testkit` through the top-level `rakka` `testkit`
  feature when the agent workflow facade is enabled.
- Added fixture tests for deterministic ids, artifact storage, fake adapter
  outcomes, durable start deduplication, effect scheduling, and run-state JSON
  serialization.

## Phase 1: Agent Domain Facade

Goal: expose first-class agent workflow concepts without yet solving sharding,
fleet dispatch, query indexes, or Kubernetes scale.

### Slice 1.1: Agent Workflow Definitions

Status: implemented.

Scope:

- Define `AgentWorkflow` registration API.
- Define workflow metadata: type, version, status labels, allowed command
  types, step definitions, retry policy, timeout policy, approval policy, and
  observability labels.
- Support application-owned payload typing through traits or opaque serialized
  payload references.

Deliverables:

- Workflow definition registry.
- Validation for duplicate workflow type/version registrations.
- Documentation showing how applications define a workflow.

Acceptance:

- An application can register at least one workflow definition and query the
  registry by workflow type and version.

Implementation notes:

- Added `crates/rakka-agent-workflow/src/definition.rs` with
  `AgentWorkflowRegistry`, `AgentWorkflowKey`, `AgentWorkflowRegistryError`,
  and `AgentPayload`.
- Extended `AgentWorkflow` metadata with `status_labels` and
  `payload_types`.
- Added `AgentPayloadDescriptor` for typed application payloads and opaque
  serialized payload/schema references.
- Added registry validation for required metadata, duplicate command types,
  duplicate step ids, duplicate payload types, and duplicate workflow
  type/version registrations.
- Added rustdoc usage documentation and registry tests in
  `crates/rakka-agent-workflow/tests/workflow_registry.rs`.

### Slice 1.2: Command and Effect Facade

Status: implemented.

Scope:

- Define first-class commands: `StartRun`, `SubmitSignal`, `ContinueRun`,
  `EffectCompleted`, `EffectFailed`, `HumanDecisionSubmitted`, `TimerFired`,
  `CancelRun`, `RetryRun`, and `ForgetRun`.
- Define effect kinds: model call, tool call, process call, HTTP call, gRPC
  call, stream publish, artifact write, human approval request, notification,
  child workflow command, and audit event.
- Require deduplication key, causation id, correlation id, tenant or namespace,
  and optional trace context on all external commands.

Deliverables:

- Command metadata and effect metadata types.
- Validation helpers for required ids and idempotency keys.
- Tests for rejected invalid commands and accepted valid commands.

Acceptance:

- Public command construction makes durability metadata explicit.
- Effects cannot be scheduled without stable ids and idempotency metadata.

Implementation notes:

- Added `crates/rakka-agent-workflow/src/facade.rs` with first-class
  `AgentCommand`, `AgentCommandKind`, `AgentCommandMetadata`,
  `AgentDurabilityMetadata`, `AgentEffectMetadata`, `AgentEffectSchedule`,
  `AgentFacadeError`, and validation helpers.
- Added stable command message types for `StartRun`, `SubmitSignal`,
  `ContinueRun`, `EffectCompleted`, `EffectFailed`,
  `HumanDecisionSubmitted`, `TimerFired`, `CancelRun`, `RetryRun`, and
  `ForgetRun`.
- Added stable effect message labels for model, tool, process, HTTP, gRPC,
  stream publish, artifact write, human approval, notification, child workflow,
  and audit event effects.
- Added `AgentDeduplicationKey` and `AgentIdempotencyKey` domain identifiers
  and made scheduled `AgentEffect` values carry both stable outbox
  deduplication and downstream idempotency metadata.
- Kept trace context optional through `AgentTelemetryContext` while requiring
  tenant or namespace, command id, run id, workflow id, deduplication key,
  causation id, and correlation id on command metadata.
- Added facade tests in
  `crates/rakka-agent-workflow/tests/command_effect_facade.rs` for accepted
  valid commands, rejected invalid commands, stable command/effect message
  types, and rejected effect schedules without stable idempotency metadata.

### Slice 1.3: Durable Inbox Facade

Status: implemented.

Scope:

- Wrap `rakka-workflow::DurableInbox` with agent-specific command acceptance.
- Map duplicate message ids and deduplication keys to agent-level outcomes.
- Record command acceptance telemetry events through the existing metrics
  boundary.

Deliverables:

- `AgentRunInbox` or equivalent facade.
- Tests for accepted, duplicate, rejected, and recovered commands.
- Error mapping from lower-level `WorkflowError` to agent-level errors.

Acceptance:

- `StartRun` is acknowledged only after durable inbox persistence.
- Duplicate `StartRun` returns the existing durable command outcome.

Implementation notes:

- Added `crates/rakka-agent-workflow/src/inbox.rs` with `AgentRunInbox`,
  `AgentInboxAcceptance`, `AgentInboxDuplicateReason`, `AgentInboxError`, and
  `agent_run_workflow_id`.
- Mapped `AgentCommand` values to `rakka-workflow::InboxCommand` by using the
  command id as the durable message id, the command deduplication key as the
  durable inbox deduplication key, and the serialized command envelope as the
  durable payload.
- Preserved the substrate persistence boundary: `AgentInboxAcceptance::Accepted`
  is returned only after `DurableInbox::accept` persists the entry.
- Mapped duplicate outcomes back to agent-level duplicate reasons for message id
  matches and deduplication-key matches.
- Added bounded command acceptance metrics through `rakka-core::MetricsRecorder`
  with command type, message type, outcome, and detail labels, while avoiding
  workflow id, run id, command id, and deduplication key labels.
- Added `crates/rakka-agent-workflow/tests/inbox_facade.rs` covering persisted
  `StartRun` acceptance, duplicate message id, duplicate deduplication key after
  recovery, rejected invalid commands, and lower-level `WorkflowError` mapping.

### Slice 1.4: Minimal Local Workflow Example

Status: implemented.

Scope:

- Add a small local example or test fixture that starts a workflow, executes one
  deterministic step, and completes.
- Keep it in-memory and single-process.

Deliverables:

- Example or integration test.
- Short docs explaining the reliability boundary.

Acceptance:

- The example demonstrates the agent facade without requiring sharding,
  PostgreSQL, OpenTelemetry Collector, or Kubernetes.

Implementation notes:

- Added `crates/rakka-agent-workflow/tests/minimal_local_workflow.rs` as an
  executable single-process example.
- The example registers a workflow definition, accepts `StartRun` through
  `AgentRunInbox`, recovers the serialized command from the durable inbox,
  executes one deterministic planner step, transitions the inbox entry to
  completed, and returns a completed `AgentRunState`.
- Kept the example in-memory and local by using
  `InMemoryDurableStateStore`, `ManualWorkflowClock`, and
  `InMemoryMetricsRecorder`.
- Added `docs/plans/agentic-workflow/phase-1-4-minimal-local-workflow.md` to
  document the reliability boundary and clarify that the local runner is not
  yet the Phase 2 durable run engine.

## Phase 2: Durable Run Engine

Goal: make agent runs recoverable, resumable, and actor-backed while preserving
the durable inbox/outbox boundary.

### Slice 2.1: Step Runner State Machine

Status: implemented.

Scope:

- Implement the core state transition loop for agent runs.
- Support statuses from the spec: accepted, running, waiting-for-timer,
  waiting-for-human, waiting-for-effect, cancelling, completed, failed,
  compensating, and cancelled.
- Persist transitions through durable state before exposing outcomes.

Deliverables:

- Step runner trait or state-machine module.
- State transition tests for start, step success, step failure, wait, resume,
  completion, and failure.

Acceptance:

- A run can recover from stored state and continue from the expected status.
- Invalid transitions fail with stable error codes.

Implementation notes:

- Added `crates/rakka-agent-workflow/src/runner.rs` with
  `AgentStepRunner`, `AgentRunTransition`, `AgentRunTransitionKind`,
  `AgentRunWaitReason`, `AgentStepSuccess`, and `AgentRunEngineError`.
- Persisted all run transitions through `DurableStateStore<AgentRunState>`
  using optimistic revision fencing before returning transition outcomes.
- Added recovery through the stable `agent-run:<run_id>` persistence id, so a
  fresh runner can load stored state and continue from the recovered status.
- Covered accepted, running, waiting-for-timer, waiting-for-human,
  waiting-for-effect, cancelling, completed, failed, compensating, and
  cancelled statuses.
- Added stable error codes for unrecovered runners, missing durable state,
  already-started runs, workflow mismatch, missing current step, unknown step,
  invalid transition, invalid state, and underlying persistence failures.
- Added `crates/rakka-agent-workflow/tests/step_runner.rs` for start,
  recovery, step success, completion, step failure, run failure, wait, resume,
  cancellation, compensation, and invalid-transition cases.

### Slice 2.2: Durable Outbox Scheduling

Status: implemented.

Scope:

- Schedule `AgentEffect` values through the existing durable outbox model.
- Persist effect scheduling before dispatch can occur.
- Store causation id, correlation id, and trace context on each effect.

Deliverables:

- Agent effect to durable outbox mapping.
- Tests for persisted scheduled effects, duplicate effects, and due-effect
  discovery.

Acceptance:

- No external dispatcher can observe an effect before it is durably scheduled.

Implementation notes:

- Added `crates/rakka-agent-workflow/src/outbox.rs` with agent-level durable
  outbox scheduling, duplicate mapping, due-effect decoding, stable error
  codes, and bounded scheduling metrics.
- Extended `AgentRunInbox` with `schedule_effect` and `due_effects`, keeping
  the durable persistence boundary in `rakka-workflow::DurableInbox`.
- Mapped `AgentEffect` to `OutboxCommand` by using the effect id as the durable
  outbox message id, the effect deduplication key as the durable outbox
  deduplication key, the effect kind message type as the substrate message
  type, and the full serialized `AgentEffect` as the durable payload.
- Preserved causation id, correlation id, idempotency key, and
  `AgentTelemetryContext` in the persisted outbox payload so dispatchers and
  recovery paths can reconstruct telemetry context after pauses or restarts.
- Added optional first-dispatch scheduling to `rakka-workflow::OutboxCommand`
  so `AgentEffect::due_at` controls outbox due discovery while existing callers
  continue to default to the workflow clock's current time.
- Added `crates/rakka-agent-workflow/tests/outbox_facade.rs` for persisted
  scheduled effects, recovered due effects, duplicate effect ids, duplicate
  deduplication keys, delayed due discovery, and rejected invalid effects.

### Slice 2.3: Actor-Backed Run Runtime

Status: implemented.

Scope:

- Host one active run inside a typed actor or sharded-entity-compatible runtime.
- Ensure actor restart rehydrates durable run state.
- Keep active mailbox messages small and move large payloads to artifact refs.

Deliverables:

- `AgentRunActor` or equivalent runtime wrapper.
- Tests for actor restart and durable recovery.

Acceptance:

- A process-local restart recovers a run and continues pending work without
  duplicate accepted commands.

Implementation notes:

- Added `crates/rakka-agent-workflow/src/runtime.rs` with `AgentRunActor`,
  `AgentRunActorCommand`, `AgentRunActorSnapshot`, `AgentRunRuntimeError`, and
  `AgentRunRuntimeResult`.
- Hosted one active run behind a typed Rakka actor by composing
  `AgentStepRunner` with `AgentRunInbox`; the actor recovers both durable
  components in `started` and `restarted`.
- Exposed small actor messages for recovery, snapshots, durable inbox command
  acceptance, step transitions, waits, cancellation, compensation, effect
  scheduling, and due-effect discovery.
- Kept large workflow data out of actor mailbox expectations by relying on the
  existing `AgentRunState`, `AgentEffect`, and `ArtifactRef` payload-reference
  policy for prompts, completions, files, and tool outputs.
- Added `crates/rakka-agent-workflow/tests/runtime_actor.rs` covering
  process-local actor restart over shared durable stores, recovered run state,
  recovered inbox work, duplicate command acceptance after restart, continued
  step execution to completion, and recovered due outbox effects.

### Slice 2.4: Sharded Run Integration

Status: implemented.

Scope:

- Register agent runs as sharded entities keyed by workflow id or run id.
- Use remembered entities only for runs that must restart on shard acquisition.
- Support passivation for idle waiting runs.

Deliverables:

- Sharded entity registration helper.
- Tests or example showing local sharded routing to an agent run.

Acceptance:

- A command routes to the correct run entity by stable id.
- An idle waiting run can be passivated and later resumed from durable state.

Implementation notes:

- Added an optional `sharding` feature to `rakka-agent-workflow`, wired through
  the top-level `rakka` crate feature surface.
- Added sharded agent run helpers under `rakka_agent_workflow::sharding`:
  entity type/id helpers, registration helpers with clock and metrics injection,
  sharded ref lookup, explicit passivation, and remembered-entity forget support.
- Agent run entity ids map directly to `AgentRunId`, so Kubernetes-scale
  routing can address a durable run by stable id while the actor runtime remains
  process-local and recoverable from the durable run, inbox, and outbox stores.
- Remembered entities are opt-in via `AgentRunShardingSettings`; most long-lived
  waiting runs can remain passivated and restart lazily on the next command,
  while runs that must come back on shard acquisition can use the sharding
  facade's remembered-entity store.
- Added feature-gated integration coverage showing stable run-id routing,
  passivation of an idle waiting run, durable recovery on the next sharded
  message, and remembered-entity registration diagnostics.

### Slice 2.5: Runtime Snapshots

Status: implemented.

Scope:

- Register operational snapshots for runtime, shards, outbox, recovery, and
  active run status.
- Keep snapshot payloads diagnostic and bounded.

Deliverables:

- Snapshot providers under names defined in the spec.
- Tests for snapshot registration and representative payloads.

Acceptance:

- Operators can inspect active status counts, pending commands, due effects,
  and recovery state without reading raw persistence records.

Implementation notes:

- Added `AgentWorkflowSnapshotRegistry` as the process-local, synchronous
  observation source used by operational snapshot providers. It records bounded
  run summaries from actor-hosted run snapshots rather than querying raw
  persistence records.
- Added serializable snapshot payloads and spec-aligned names for
  `agent_workflow_runtime`, `agent_workflow_outbox`, `agent_workflow_recovery`,
  and `agent_workflow_shards`.
- Updated `AgentRunActor` to publish snapshots after recovery, durable inbox
  command acceptance, state-machine transitions, durable outbox scheduling, and
  due-effect checks. Recovery/runtime errors are recorded with stable error
  codes for diagnostic inspection.
- Added HTTP registration helpers behind the `http` feature and shard snapshot
  conversion/registration behind the `sharding` feature, so Kubernetes
  deployments can expose the payloads through the existing `/snapshots` route.
- Extended sharded run settings with an optional snapshot registry so
  actor-backed sharded runs can publish the same bounded operational snapshots.
- Added tests covering active status counts, pending command counts, due effect
  counts, recovery state, HTTP snapshot names, and sharding facade summaries.

## Phase 3: Durable Async Boundaries

Goal: make long-running behavior real: timers, dispatcher workers, human
pauses, adapters, and artifacts.

### Slice 3.1: Durable Timer Model

Status: implemented.

Scope:

- Define durable timer entries with run id, timer id, due time, deduplication
  key, causation id, trace context, and policy metadata.
- Implement a scanner or sharded timer owner.
- Inject `TimerFired` through the durable inbox.

Deliverables:

- Timer storage shape.
- Timer scanner with bounded polling and back-pressure.
- Tests for due timers, duplicate firing, restart recovery, and late firing.

Acceptance:

- A run waiting for a timer resumes after restart.
- Duplicate timer delivery is deduplicated by inbox key.

Implementation notes:

- Added `AgentTimerId`, durable timer entries, timer policy metadata, timer
  statuses, and a durable timer-store state backed by the existing
  `DurableStateStore` abstraction.
- Added `AgentTimerStore` for scheduling, due selection, idempotent fired
  marking, cancellation, and recovery from the durable timer index.
- Added `AgentTimerScanner` with explicit clock injection, configurable
  `max_batch_size`, due-count reporting, and `backpressure_limited` reporting
  when more timers are due than can be fired in one scan.
- Timer firing builds a first-class `TimerFired` command from the timer's
  deduplication key, causation id, correlation id, trace context, tenant, run id,
  and workflow id, then accepts it through `AgentRunInbox`.
- After durable inbox acceptance, the scanner resumes the target run when its
  recovered durable state is still `waiting-for-timer`; duplicate or late
  delivery against an already-running run remains idempotent at the inbox layer.
- Added timer firing metrics and late-by-milliseconds reporting for overdue
  scans.
- Added tests for restart recovery, due timer firing, duplicate `TimerFired`
  delivery deduplication, bounded scans, and late firing.

### Slice 3.2: Dispatcher Fleet

Status: implemented.

Scope:

- Implement workers that scan due outbox effects across many workflows.
- Add claiming, lease duration, fencing token, and retry-after behavior.
- Add per-target concurrency limits for model, tool, process, HTTP, gRPC, and
  notification targets.

Deliverables:

- Dispatcher runtime.
- Claim/lease protocol.
- Dispatcher health snapshots and metrics.
- Failure tests for crash before dispatch result persistence.

Acceptance:

- Multiple workers do not intentionally execute the same claimed effect
  concurrently.
- Crash after marking dispatching leads to recoverable retry or reconciliation.

Implementation notes:

- Added `crates/rakka-agent-workflow/src/dispatcher.rs` with a durable
  dispatcher fleet index backed by `DurableStateStore`.
- Added stable dispatcher ids and worker ids, dispatcher entries, leases,
  monotonic fencing tokens, statuses, target classes, claim batches, worker
  cycles, health snapshots, and bounded dispatcher metrics.
- Dispatcher workers refresh due effects from per-run durable outboxes into the
  fleet index, claim bounded due work with lease duration and target concurrency
  limits, and dispatch claimed effects through an application-supplied
  `AgentEffectDispatcher`.
- The per-run durable outbox remains the source of execution truth. Workers mark
  the source outbox entry dispatching before external execution, then record
  success, retry, timeout, or exhaustion back into the durable outbox before
  updating the fleet entry.
- Fencing checks run before dispatch and again before result persistence, so an
  expired or superseded claim cannot write a stale dispatch result.
- Added class and target-name concurrency limits for model, tool, process, HTTP,
  gRPC, notification, and related effect classes.
- Added tests covering concurrent worker claiming, lease expiry after marking an
  effect dispatching, recovered redispatch with a newer fencing token, target
  concurrency throttling, retry-after scheduling, and outbox status updates.

### Slice 3.3: Human Checkpoints

Status: implemented.

Scope:

- Add first-class checkpoint state with allowed decisions, roles/policy hints,
  due timestamp, escalation target, artifact refs, and audit references.
- Schedule `HumanApprovalRequest` outbox effects.
- Accept `HumanDecisionSubmitted` through public ingress and durable inbox.

Deliverables:

- Checkpoint model.
- Approval command validation.
- Timeout and escalation hooks.
- HTTP/gRPC example path for approval submission.

Acceptance:

- A workflow can wait without holding a task, appear in snapshots, resume after
  approval, and handle duplicate approval submissions.

Implementation notes:

- Added `crates/rakka-agent-workflow/src/checkpoints.rs` with a durable human
  checkpoint facade that composes `AgentStepRunner`, `AgentRunInbox`, and
  durable outbox scheduling.
- Existing `HumanCheckpoint` state is now used as the durable source of truth
  for allowed decisions, required roles, due timestamps, escalation targets,
  artifact references, principals, and audit references.
- Added runner transitions to open a checkpoint, persist
  `waiting-for-human`, resolve a checkpoint and resume the run, and mark an
  overdue checkpoint escalated while the run remains idle.
- Opening a checkpoint schedules a first-class `HumanApprovalRequest` effect
  through the durable outbox, so the dispatcher fleet can deliver approval work
  to UI, ticketing, chat, or service integrations.
- Added `AgentHumanDecisionSubmission` and `human_decision_command` so public
  HTTP/gRPC ingress can build `HumanDecisionSubmitted` commands with stable
  command ids and deduplication keys.
- Added a feature-gated JSON HTTP route helper for human decision submission;
  gRPC integrations can use the same submission and command-building facade.
- Added human checkpoint metrics, wait latency reporting, overdue discovery, and
  escalation hooks.
- Added the `agent_workflow_human_checkpoints` operational snapshot with bounded
  per-run samples, waiting-run count, open checkpoint count, escalated count, and
  due checkpoint count.
- Added tests covering checkpoint open, approval-request outbox scheduling,
  approval resume, duplicate approval deduplication, overdue discovery,
  escalation, and snapshot visibility.

### Slice 3.4: Model and Tool Adapter Traits

Status: implemented.

Scope:

- Define adapter traits for model calls and tool calls.
- Include timeout, idempotency, receipt, retry classification, token/cost
  metadata, artifact refs, and redaction status.
- Provide fake local adapters for tests before integrating real providers.

Deliverables:

- `ModelAdapter` and `ToolAdapter` traits or equivalent.
- Fake adapters.
- Process-backed tool adapter example using `rakka-process`.

Acceptance:

- Model/tool effects can complete, fail retryably, fail permanently, time out,
  and write artifact references.

Implementation notes:

- Added `AgentModelAdapter` and `AgentToolAdapter` boundaries in
  `rakka-agent-workflow`, with request metadata derived from durable effects.
- Added adapter outcomes for completion, retryable/permanent failure, and
  timeout, preserving receipts, idempotency, token/cost usage, redaction, and
  artifact references.
- Extended the testkit fake model/tool adapters to exercise adapter traits
  before real provider integrations are added.
- Added a feature-gated process file-watch tool adapter example backed by
  `rakka-process` for Kubernetes-friendly external tool execution patterns.

### Slice 3.5: Artifact Reference Policy

Status: implemented.

Scope:

- Define `ArtifactRef` semantics for prompts, completions, files, embeddings,
  tool output, screenshots, logs, and large state.
- Keep storage backend application-owned for v1, but provide trait boundaries.
- Include checksum, content type, size, retention class, redaction status, and
  optional encryption metadata.

Deliverables:

- Artifact reference type.
- Artifact store trait for tests and examples.
- Validation that hot state does not inline large payloads by default.

Acceptance:

- Large model/tool payloads are represented by refs and can be correlated from
  audit events, logs, and workflow state.

Implementation notes:

- Added an artifact policy module with validation for artifact references,
  inline state size limits, effect references, run-state references, and audit
  artifact correlation.
- Extended `ArtifactRef` with typed optional encryption metadata while keeping
  storage application-owned for v1.
- Added `AgentArtifactStore` and deterministic in-memory testkit support so
  examples and future harnesses can exercise artifact reads/writes without
  choosing a production object store.
- Added default hot-state policy that rejects large inline run state unless an
  explicit policy raises the limit.
- Phase 3 is complete after this slice: timers, dispatcher fleet, human
  checkpoints, model/tool adapters, and artifact reference policy are now in
  place.

## Phase 4: OpenTelemetry Observability and Audit

Goal: make agent workflow execution observable across process, pod, durable
pause, retry, and human decision boundaries.

### Slice 4.1: Workflow Metric Instruments

Status: implemented.

Scope:

- Add stable workflow metric names and attribute keys.
- Record command, inbox, outbox, step, human, model, tool, timer, dispatcher,
  and recovery metrics through Rakka's current metrics boundary.
- Preserve Prometheus-compatible exposition.

Deliverables:

- Metric constants and helpers.
- Cardinality tests for hot labels.
- Example metric output.

Acceptance:

- Raw workflow ids, run ids, entity ids, prompts, and full error strings are not
  used as hot metric labels.

Implementation notes:

- Added a central workflow metric registry with stable instrument names, kinds,
  units, and descriptions for inbox, outbox, run/step transitions, human
  checkpoints, model/tool adapters, timers, dispatcher, and recovery.
- Added bounded metric attribute constants and helper validation that rejects
  raw ids, prompts, tool payloads, full error strings, unknown label keys, and
  overlong or multi-line label values.
- Added recording helpers for counters, gauges, and histograms on top of
  Rakka's existing `MetricsRecorder`, preserving Prometheus text export and the
  current OpenTelemetry bridge model.
- Example Prometheus output:
  `rakka_agent_workflow_run_transitions{workflow_type="research",transition="begin-step",status="running",outcome="success"} 1`

### Slice 4.2: Trace Context and Span Links

Status: implemented.

Scope:

- Define `AgentTelemetryContext`, `AgentTraceContext`, and span link metadata.
- Persist W3C `traceparent` and `tracestate` across inbox, outbox, timers,
  remoting metadata, process/tool protocols, callbacks, and human decisions.
- Use span links for asynchronous resumes, retries, timers, and human
  decisions.

Deliverables:

- Trace context propagation helpers.
- Tests showing parent spans for synchronous boundaries and links for durable
  resume boundaries.

Acceptance:

- Public ingress, workflow step, effect dispatch, callback, human approval, and
  recovery can be correlated in traces.

Implementation notes:

- Added `AgentTraceContext` plus strict W3C `traceparent` and `tracestate`
  validation helpers for durable agent workflow metadata.
- Added injection and extraction helpers for lowercase W3C text-map carriers,
  with case-insensitive reads and overwrite-safe writes for HTTP, gRPC,
  process, tool, callback, and remoting metadata boundaries.
- Added synchronous child telemetry helpers that preserve trace ids, flags,
  tracestate, and baggage without span links.
- Added durable resume telemetry helpers that create a new propagated context
  and attach span-link metadata back to the parked span for timers, callbacks,
  human decisions, retries, and recovery.
- Added tests proving trace context survives public ingress commands, workflow
  step propagation, effect dispatch, callbacks, timers, human approvals,
  recovery commands, and serialization with span links.

### Slice 4.3: Structured Logs and Durable Audit

Status: implemented.

Scope:

- Define OpenTelemetry-compatible log event schema for workflow lifecycle
  events.
- Define durable audit event schema for prompts, model calls, tool calls,
  artifacts, checkpoints, human decisions, policy overrides, completion,
  failure, cancellation, and retention deletion.
- Keep audit retention separate from telemetry backend retention.

Deliverables:

- `AgentLogEvent` and `AgentAuditEvent` models.
- Audit sink abstraction using event journal or a dedicated store.
- Redaction policy hooks.

Acceptance:

- Logs include trace/span correlation when a span exists.
- Durable audit events can be queried independently from the telemetry backend.

Implementation notes:

- Added OpenTelemetry-compatible `AgentLogEvent` fields for event name,
  timestamp, observed timestamp, trace id, span id, trace flags, severity text,
  severity number, body, resource, instrumentation scope, and attributes.
- Added audit-derived log helpers that map `AgentAuditEvent` into structured
  log events while preserving workflow/run ids, causation id, correlation id,
  redaction status, artifact references, and trace correlation.
- Added `AgentAuditSink`, query, write acceptance, and deterministic
  `InMemoryAgentAuditSink` so audit records can be stored and queried
  independently from telemetry backend retention.
- Added `AgentRedactionPolicy` hooks for unredacted log bodies, log body size,
  and redacted/reference-only audit evidence requirements.
- Added tests for structured log trace correlation, audit sink record/query and
  duplicate handling, redaction policy failures, trace field consistency, and
  stable audit event names.

### Slice 4.4: OTLP and Collector Integration

Status: implemented.

Scope:

- Decide whether Rakka ships feature-gated OpenTelemetry SDK integration,
  application-owned SDK adapter hooks, or both.
- Add resource configuration helpers for `service.name`, namespace, version,
  deployment environment, Kubernetes pod, node, deployment, container, and
  Rakka node attributes.
- Provide Collector config for local development.

Deliverables:

- Feature-gated OTLP exporter path or documented adapter API.
- Collector config example.
- Integration test that routes metrics, traces, and logs to a Collector or a
  deterministic test receiver.

Acceptance:

- Example telemetry includes resource attributes and can be exported through an
  OpenTelemetry-compatible path.

Implementation notes:

- Kept Rakka aligned with the existing v1 observability boundary: application
  code owns concrete OpenTelemetry SDK/exporter setup, while Rakka provides a
  stable adapter-facing bridge model.
- Added OpenTelemetry resource helpers for service name, namespace, version,
  instance id, deployment environment name, Kubernetes namespace, pod, pod UID,
  node, deployment, container, and Rakka node id attributes.
- Added OTLP exporter configuration helpers using standard `OTEL_EXPORTER_OTLP_*`
  names, protocol selection, signal-specific endpoint overrides, timeouts, and
  headers.
- Added `AgentOtlpBridgeExport`, `AgentOtelSpanExport`, `AgentOtlpBridgeReceiver`,
  and `InMemoryAgentOtlpReceiver` so metrics, traces, and logs can be exported
  through a deterministic OpenTelemetry-compatible handoff.
- Added local development Collector config at
  `docs/plans/agentic-workflow/otel-collector-local.yaml` with OTLP gRPC/HTTP
  receivers and traces, metrics, and logs pipelines.
- Added tests proving resource attributes, exporter config parsing, metrics,
  spans, and logs route to a deterministic receiver, and the Collector config
  exposes all three signal pipelines.

### Slice 4.5: Observability Testkit

Status: implemented.

Scope:

- Add assertions for metric names, bounded labels, span attributes, span links,
  log fields, audit event correlation ids, and resource attributes.

Deliverables:

- Testkit assertions available to agent workflow tests.
- Golden or structured assertions for representative workflow execution.

Acceptance:

- Observability regressions fail tests before dashboards or backends are
  involved.

Implementation notes:

- Added feature-gated agent workflow testkit assertions for registered metric
  instruments, bounded metric labels, span attributes, span links, structured
  log fields, audit causation/correlation ids, OpenTelemetry resource
  attributes, and OTLP bridge exports.
- Assertions reuse production validation helpers for metric label policy, trace
  context, structured logs, durable audit events, resource attributes, spans,
  and exporter configuration so test expectations stay aligned with runtime
  behavior.
- Added a representative observability test that records a workflow transition
  metric, creates a durable resume span with a link back to the parked span,
  converts an audit event into an OpenTelemetry-compatible log, validates audit
  correlation ids, checks Kubernetes/service resource attributes, and verifies
  the OTLP bridge export contains the expected metric.
- Phase 4 now has implemented coverage for workflow metrics, trace propagation
  and span links, structured logs and durable audit, OTLP/Collector bridge
  integration, and pre-export observability regression assertions.

## Phase 5: Query, Indexing, Retention, and Compaction

Goal: avoid full scans and unbounded workflow state growth as run volume grows.

### Slice 5.1: Workflow Query Model

Status: implemented.

Scope:

- Define query dimensions: tenant, namespace, workflow type, version, status,
  updated_at, waiting reason, checkpoint age, failed step, due timer, stuck
  dispatcher, and shard ownership.
- Separate operational query indexes from durable run state.

Deliverables:

- Query trait.
- In-memory query index for tests.
- Query API docs.

Acceptance:

- Tests can list waiting, failed, running, and stuck workflows without scanning
  every persistence id.

Implementation notes:

- Added an operational query model that keeps durable run state as the source of
  truth while projecting runs, timers, dispatch entries, and optional shard
  ownership into bounded query records.
- Added `AgentWorkflowQueryIndex` with in-memory support for upserting and
  removing run, timer, and dispatcher projections plus querying runs, due
  timers, and dispatcher work.
- Added run query dimensions for tenant, namespace, workflow type, definition
  version, status, updated-at range, waiting reason, checkpoint age, failed
  step, due timer, stuck dispatcher, shard owner, shard id, and limit.
- Added timer and dispatcher query dimensions so Phase 5.2 can map the same
  trait to PostgreSQL indexes without changing the public query vocabulary.
- Added tests proving waiting, running, failed, stale-checkpoint, due-timer,
  stuck-dispatcher, and shard-owner lookups work through the operational index
  without reading durable persistence records.

### Slice 5.2: PostgreSQL Index Store

Status: implemented.

Scope:

- Add PostgreSQL schema for workflow run index, timer index, checkpoint index,
  dispatcher claims, and audit query support where appropriate.
- Add migrations and revision/fencing policy.

Deliverables:

- PostgreSQL implementation.
- Integration tests gated the same way existing PostgreSQL tests are gated.

Acceptance:

- Query paths remain bounded under high run counts.
- Lease/fencing behavior prevents stale writers from corrupting dispatcher or
  timer ownership.

Implementation notes:

- Added a `postgres` feature-gated `PostgresAgentWorkflowQueryIndex` with
  namespaced run, timer, checkpoint, dispatcher, and audit index tables.
- Forwarded the top-level `rakka/postgres` feature to the agent workflow
  PostgreSQL index so application imports can use the main facade crate.
- Added migration SQL guarded by a PostgreSQL advisory lock so Kubernetes
  replicas can safely start concurrently against the same database.
- Added bounded indexed query paths for running, waiting, failed, due-timer,
  stuck-dispatcher, shard-owner, timer, and dispatcher lookups.
- Added projection freshness rules for runs and timers plus dispatcher fencing
  token checks so stale writers cannot overwrite newer operational state.
- Added gated PostgreSQL integration tests using `RAKKA_POSTGRES_TEST_DSN`,
  including migration verification, query round trips, and stale-write
  rejection against the local Postgres container.

### Slice 5.3: Retention Windows and Snapshot Compaction

Status: implemented.

Scope:

- Define retention windows for completed inbox entries, completed outbox
  entries, deduplication keys, audit events, artifact refs, prompts, and
  completions.
- Add compaction for long-lived `WorkflowState` snapshots.
- Define archival handoff to event journal or application storage.

Deliverables:

- Retention policy API.
- Compaction implementation.
- Tests for deduplication window preservation and completed history trimming.

Acceptance:

- Completed high-volume workflows do not grow snapshots without bound.
- Deduplication remains correct within configured windows.

Implementation notes:

- Added `WorkflowStateCompactionPolicy` and `WorkflowState::compact` to the
  durable workflow substrate so completed inbox entries, terminal outbox
  entries, and deduplication keys can be trimmed without touching retryable or
  in-flight work.
- Preserved deduplication correctness by retaining terminal entries with
  deduplication keys until both the terminal-entry window and deduplication-key
  window have elapsed.
- Added `AgentRetentionPolicy` with windows for terminal checkpoints, terminal
  effects, audit events, general artifact refs, prompt refs, completion refs,
  and inline state, plus per-run caps for terminal checkpoints/effects.
- Added pure compaction helpers for `AgentRunState` and durable audit event
  batches that return archive handoff records for event journals or
  application-owned storage before hot-state references are removed.
- Added tests for completed history trimming, deduplication-window
  preservation, active/retryable work preservation, archive records, artifact
  reference cleanup, inline state cleanup, and audit event retention.

### Slice 5.4: Migration and Backfill Policy

Status: implemented.

Scope:

- Define how workflow definition versions, persisted state schema versions, and
  index schema versions evolve.
- Support backfill or lazy index repair for existing runs.

Deliverables:

- Migration notes.
- Versioned serialization tests.
- Index repair tool or admin API sketch.

Acceptance:

- N/N+1 compatibility policy covers workflow state and index schemas.

Implementation notes:

- Added `AgentWorkflowMigrationPolicy` with an explicit N/N+1 compatibility
  constructor covering durable run-state schema versions and query index schema
  versions.
- Added `AgentWorkflowIndexSchemaVersion` and
  `CURRENT_AGENT_WORKFLOW_INDEX_SCHEMA_VERSION` so operational projections can
  evolve independently from durable run-state serialization.
- Added workflow definition version allow-listing for deployments that need to
  reject or quarantine runs whose definitions are not enabled in the current
  rollout.
- Added migration assessments and machine-readable reasons for current,
  compatible previous, too-old, and ahead-of-binary state/index versions.
- Added a dry-run backfill planner plus `repair_agent_workflow_index`, a small
  admin API or Kubernetes Job building block that rebuilds query projections
  from durable `AgentRunState` through the existing `AgentWorkflowQueryIndex`
  trait.
- Added versioned serialization tests for durable run state and index repair
  tests that rebuild supported runs while skipping unsupported state or disabled
  definition versions.

## Phase 6: Kubernetes Scale and Deployment

Goal: validate that the agent workflow runtime behaves predictably in a
cloud-based Kubernetes environment.

### Slice 6.1: Reference Topology

Status: implemented.

Scope:

- Define reference manifests or Helm-style templates for Rakka app Deployment,
  public HTTP/gRPC Service, internal headless remoting Service, PostgreSQL
  connection config, object storage config, and health/drain endpoints.
- Keep public ingress separate from internal remoting.

Deliverables:

- Reference deployment docs and manifest templates.
- Manifest contract tests.

Acceptance:

- Required ports, probes, labels, env vars, compatibility metadata, and service
  names are validated by tests.

Implementation notes:

- Added `kubernetes-reference-topology.yaml` as a raw, Helm-friendly reference
  topology for `rakka-system`, a three-replica agent workflow `Deployment`,
  separate public HTTP/gRPC and internal remoting Services, pre-stop drain,
  readiness, liveness, startup probes, compatibility annotations, PostgreSQL,
  artifact-store, and OpenTelemetry configuration.
- Modeled local Docker Desktop PostgreSQL access with
  `Service/rakka-postgres` as an `ExternalName` to `host.docker.internal` and
  a local `rakka-postgres-credentials` Secret.
- Added `kubernetes-reference-topology.md` with the local runtime contract,
  service boundary notes, object-storage placeholder, compatibility policy,
  validation commands, and future Helm values map.
- Added `agent_workflow_topology` manifest contract tests in `rakka-k8s` to
  validate namespace defaults, service separation, local PostgreSQL wiring,
  required ports/probes/env vars, compatibility metadata, documentation, and
  optional `kubectl apply --dry-run=client` validation.

### Slice 6.2: Startup and Readiness

Status: implemented.

Scope:

- Implement or document startup order: configure telemetry resources, configure
  tracing/exporters, connect durable stores, initialize actor system, configure
  remoting, configure sharding, register workflows, register snapshots, mark
  services ready.
- Readiness should fail until required services and telemetry/exporter
  initialization requirements are satisfied.

Deliverables:

- Startup checklist.
- Readiness integration tests.

Acceptance:

- A pod does not accept public workflow commands before durable stores,
  workflow registrations, compatibility checks, and required telemetry setup
  are ready.

Implementation notes:

- Added `rakka-agent-workflow` Kubernetes startup helpers behind the `k8s`
  feature, including `AgentWorkflowStartupStep`,
  `AgentWorkflowKubernetesStartup`, startup snapshots, and stable readiness
  service-name constants.
- The startup helper registers required services with
  `rakka_k8s::KubernetesNodeHealth`, then marks them available as each startup
  step completes so the existing Kubernetes readiness probe fails closed until
  cluster membership, compatibility, and every required agent workflow service
  are ready.
- Added parsing and default-service helpers for the `RAKKA_REQUIRED_SERVICES`
  vocabulary used by the reference topology.
- Forwarded the top-level `rakka/k8s` feature into
  `rakka-agent-workflow?/k8s` so facade users get the startup helpers when both
  the top-level Kubernetes and agent workflow surfaces are enabled.
- Expanded the reference topology's `RAKKA_REQUIRED_SERVICES` to include
  telemetry resource configuration, OTLP exporter setup, PostgreSQL, durable
  state, query index, artifact store, actor system, remoting, sharding,
  workflow registry, and operational snapshots.
- Added `kubernetes-startup-readiness.md` with the intended startup order,
  readiness/liveness behavior, app wiring, and failure handling.
- Added tests proving readiness remains false until required startup services
  complete, compatibility failures fail closed, drain flips readiness false,
  snapshots report pending steps, and manifest contract tests use the expanded
  startup service vocabulary.

### Slice 6.3: Drain and Shutdown

Status: implemented.

Scope:

- Extend coordinated shutdown with workflow-aware drain hooks.
- Stop new public workflow commands on draining pods.
- Drain streams, hand off shards, stop process actors, flush persistence and
  telemetry buffers, and leave durable recovery as the correctness mechanism.

Deliverables:

- Drain hook registration.
- Drain report fields for agent workflow blockers.
- Tests for interrupted and completed drain.

Acceptance:

- Readiness flips false before drain work starts.
- Abrupt termination during drain still recovers accepted commands and
  scheduled effects.

Implementation notes:

- Added `AgentWorkflowIngressGate` behind the `k8s` feature to close public
  workflow ingress when `KubernetesNodeHealth` enters drain.
- Added workflow-specific coordinated shutdown registration helpers for
  stop-ingress and best-effort telemetry flush tasks, with stable task and
  operation names for drain reports and observability.
- Added `kubernetes-drain-shutdown.md` documenting shutdown order, readiness
  behavior, durable recovery expectations, local validation, and open runtime
  hooks.
- Added tests proving pre-drain commands cross the durable inbox boundary,
  coordinated drain flips readiness false, telemetry flush runs, post-drain
  public commands are rejected, and accepted work remains recoverable.

### Slice 6.4: Autoscaling Signals

Status: implemented.

Scope:

- Expose bounded metrics for active workflows, pending inbox commands, due
  outbox effects, dispatch latency, human waits, mailbox depth, stream
  pressure, process state, PostgreSQL latency, and shard ownership distribution.
- Document HPA/KEDA-style usage without requiring one autoscaler.

Deliverables:

- Autoscaling metrics guide.
- Example metric names and labels.

Acceptance:

- Operators can scale on pending/due workflow work and dispatch latency, not
  only CPU and memory.

Implementation notes:

- Added `AGENT_WORKFLOW_AUTOSCALING_SIGNALS` with stable metric names, kinds,
  units, roles, bounded label sets, and recommended aggregation hints.
- Added agent workflow autoscaling metrics for active runs, pending inbox
  commands, due outbox effects, dispatch latency, human waiting runs, mailbox
  depth, stream pressure, process capacity, PostgreSQL latency, and shard
  ownership distribution.
- Added `kubernetes-autoscaling-signals.md` with HPA/KEDA-style guidance,
  Prometheus examples, recording guidance, and bounded-label policy.
- Updated the reference topology with autoscaling metric emission defaults and
  linked the new guide from the Kubernetes topology documentation.
- Added tests covering the autoscaling catalog, bounded labels, representative
  Prometheus/OTLP export names, and topology documentation wiring.

### Slice 6.5: OpenTelemetry Collector Topology

Scope:

- Provide Collector DaemonSet guidance for local pod/node/container telemetry
  and OTLP intake.
- Provide Collector gateway Deployment guidance for batching, sampling,
  filtering/redaction, routing, and backend export.
- Document resource attribute enrichment with Kubernetes metadata.

Deliverables:

- Collector config examples.
- Manifest tests where feasible.

Acceptance:

- Rakka app pods can export OTLP telemetry to a local or gateway Collector
  endpoint with service, pod, namespace, deployment, node, container, and Rakka
  node attributes present.

### Slice 6.6: Security and Policy Envelope

Scope:

- Document required authN/authZ boundaries for public APIs and human approval
  submissions.
- Provide NetworkPolicy guidance for internal remoting, database access,
  Collector access, and public ingress.
- Document service account, secret, pod security, and least-privilege tool
  execution expectations.

Deliverables:

- Security deployment guide.
- Policy checklist.

Acceptance:

- The reference deployment does not imply public access to internal remoting or
  unsafe tool execution defaults.

## Phase 7: Production Hardening and Release Readiness

Goal: prove the workflow runtime can survive realistic failures and be operated
by teams that did not build it.

### Slice 7.1: Failure-Injection Suite

Scope:

- Test crashes after inbox acceptance, after effect scheduling, after marking
  dispatching, after external result but before success persistence, during
  human approval, during timer firing, and during shard handoff.
- Test PostgreSQL revision conflicts, lease loss, stale coordinator writers,
  remote delivery failures, model provider timeouts, and process restart-budget
  exhaustion.

Deliverables:

- Failure-injection tests in `rakka-testkit` or agent workflow integration
  tests.
- Repeatable commands for gated PostgreSQL and multi-process scenarios.

Acceptance:

- Every documented reliability guarantee and non-guarantee has at least one
  test or example.

### Slice 7.2: Load, Back-Pressure, and Cardinality

Scope:

- Run high-volume workflows with bounded mailbox sizes, dispatcher concurrency,
  stream pressure, timer backlog, and persistence contention.
- Validate metric cardinality, memory behavior, and snapshot size under load.

Deliverables:

- Load test scenario.
- Cardinality report.
- Back-pressure tuning notes.

Acceptance:

- Work queues apply back-pressure instead of unbounded memory growth.
- Metrics remain bounded under large run counts.

### Slice 7.3: API Review and Compatibility

Scope:

- Review public agent workflow APIs, error codes, feature flags, docs, and
  re-exports.
- Add N/N+1 compatibility tests for workflow commands, effects, persisted
  state, trace context metadata, query indexes, and Kubernetes manifests.

Deliverables:

- Agent workflow API review document.
- Compatibility tests and versioning notes.

Acceptance:

- Additive changes are clearly separated from breaking changes.
- Rolling-update compatibility expectations are test-backed.

### Slice 7.4: Operational Runbooks and Dashboards

Scope:

- Document how to inspect waiting runs, stuck dispatchers, overdue timers,
  failed effects, duplicate callbacks, human checkpoint age, and drain blockers.
- Provide dashboard and alert recommendations without requiring one backend.

Deliverables:

- Runbook docs.
- Dashboard metric/span/log field catalog.
- Alert recommendations.

Acceptance:

- An operator can diagnose a failed or stuck workflow using metrics, traces,
  logs, audit events, snapshots, and query indexes.

### Slice 7.5: Production Candidate Gate

Scope:

- Finalize docs, examples, tests, feature flags, and release notes for the
  agent workflow preview or production candidate.

Deliverables:

- Release checklist.
- Example acceptance test matrix.
- Known limitations and non-goals.

Acceptance:

- The release candidate demonstrates durable long-running workflows at local,
  multi-process, PostgreSQL-backed, and Kubernetes-reference levels.

## Suggested Sequencing

1. Finish Phase 0 before adding runtime code.
2. Build Phases 1 and 2 as the MVP path.
3. Add Phase 3 timers, dispatcher fleet, and human checkpoints before exposing
   broad public examples.
4. Wire Phase 4 observability before scale testing, so failures are visible
   while the runtime is still small.
5. Add Phase 5 query and retention before large-volume tests.
6. Validate Phase 6 Kubernetes behavior after the runtime can recover correctly
   from abrupt local process death.
7. Use Phase 7 to decide whether the result is a preview feature or a
   production candidate.

## Cross-Phase Acceptance Matrix

Reliability:

- Durable command acceptance is covered in Phases 1, 2, and 7.
- Durable effect scheduling and dispatcher recovery are covered in Phases 2, 3,
  and 7.
- Timer and human pause recovery are covered in Phases 3 and 7.

Scale:

- Sharded run ownership starts in Phase 2.
- Dispatcher fleet, timer scanning, and indexes are added in Phases 3 and 5.
- Kubernetes scale signals and Collector topology are validated in Phase 6.

Observability:

- Basic metrics and snapshots begin in Phases 1 and 2.
- OpenTelemetry traces, span links, logs, audit correlation, and OTLP export are
  formalized in Phase 4.
- Dashboards, alerts, and runbooks land in Phase 7.

Operations:

- Startup, readiness, drain, autoscaling, NetworkPolicy, and service-account
  guidance land in Phase 6.
- Failure-injection and compatibility hardening land in Phase 7.

## Initial MVP Checklist

The first useful implementation milestone is complete when:

- one workflow definition can be registered;
- `StartRun` is durably accepted and deduplicated;
- one run can execute a deterministic step and persist status;
- one effect can be durably scheduled and completed by a fake dispatcher;
- one human checkpoint can pause without a live wait task and resume through a
  later command;
- a process restart recovers accepted commands and scheduled effects;
- bounded metrics and snapshots show run status, pending commands, pending
  effects, and checkpoint state;
- trace context metadata can be stored on commands and effects, even before the
  full OTLP exporter slice lands;
- tests cover duplicate command, duplicate effect, recovery, failure, and
  terminal states.
