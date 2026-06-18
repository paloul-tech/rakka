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

### Slice 1.4: Minimal Local Workflow Example

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

## Phase 2: Durable Run Engine

Goal: make agent runs recoverable, resumable, and actor-backed while preserving
the durable inbox/outbox boundary.

### Slice 2.1: Step Runner State Machine

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

### Slice 2.2: Durable Outbox Scheduling

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

### Slice 2.3: Actor-Backed Run Runtime

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

### Slice 2.4: Sharded Run Integration

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

### Slice 2.5: Runtime Snapshots

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

## Phase 3: Durable Async Boundaries

Goal: make long-running behavior real: timers, dispatcher workers, human
pauses, adapters, and artifacts.

### Slice 3.1: Durable Timer Model

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

### Slice 3.2: Dispatcher Fleet

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

### Slice 3.3: Human Checkpoints

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

### Slice 3.4: Model and Tool Adapter Traits

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

### Slice 3.5: Artifact Reference Policy

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

## Phase 4: OpenTelemetry Observability and Audit

Goal: make agent workflow execution observable across process, pod, durable
pause, retry, and human decision boundaries.

### Slice 4.1: Workflow Metric Instruments

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

### Slice 4.2: Trace Context and Span Links

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

### Slice 4.3: Structured Logs and Durable Audit

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

### Slice 4.4: OTLP and Collector Integration

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

### Slice 4.5: Observability Testkit

Scope:

- Add assertions for metric names, bounded labels, span attributes, span links,
  log fields, audit event correlation ids, and resource attributes.

Deliverables:

- Testkit assertions available to agent workflow tests.
- Golden or structured assertions for representative workflow execution.

Acceptance:

- Observability regressions fail tests before dashboards or backends are
  involved.

## Phase 5: Query, Indexing, Retention, and Compaction

Goal: avoid full scans and unbounded workflow state growth as run volume grows.

### Slice 5.1: Workflow Query Model

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

### Slice 5.2: PostgreSQL Index Store

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

### Slice 5.3: Retention Windows and Snapshot Compaction

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

### Slice 5.4: Migration and Backfill Policy

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

## Phase 6: Kubernetes Scale and Deployment

Goal: validate that the agent workflow runtime behaves predictably in a
cloud-based Kubernetes environment.

### Slice 6.1: Reference Topology

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

### Slice 6.2: Startup and Readiness

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

### Slice 6.3: Drain and Shutdown

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

### Slice 6.4: Autoscaling Signals

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
