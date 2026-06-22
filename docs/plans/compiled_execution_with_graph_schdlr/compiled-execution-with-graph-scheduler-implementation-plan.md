# Rakka Compiled Execution With Graph Scheduler Implementation Plan

Status: approved
Date: 2026-06-21
Source spec:
`docs/plans/compiled_execution_with_graph_schdlr/compiled-execution-with-graph-scheduler-spec.md`

## Purpose

This plan turns the compiled execution with graph scheduler spec into an
implementation roadmap.

The core decision is fixed: Rakka interprets and executes a compiled,
product-neutral IR. The future Langflow/Sim-like product backend owns the
visual editor DSL, compiler, deployment lifecycle, credentials, trigger
registration, auth, policy, and product APIs.

All follow-up tracking and documentation for this effort should live under:

```text
docs/plans/compiled_execution_with_graph_schdlr/
```

## Design Rules

- Keep all runtime additions in `rakka-agent-workflow`.
- Keep `rakka-workflow` as the durable inbox/outbox substrate.
- Do not store raw editor DSL, UI layout, credentials, or trigger registration
  records in Rakka runtime state.
- Store only logical credential binding refs in compiled plans, graph state,
  effects, runtime events, snapshots, and query indexes.
- Resolve third-party credentials only at dispatch time through an
  application-provided resolver, and do not persist resolved credential values.
- Every external command must enter through durable inbox acceptance.
- Every external side effect must be scheduled through durable outbox or an
  equivalent durable runtime boundary before execution.
- Graph state transitions must be persisted before runtime events are emitted.
- Large prompts, completions, request bodies, files, and tool outputs should be
  artifact references.
- Scheduler decisions must be deterministic from compiled plan, durable state,
  command, and explicit time input.
- Arbitrary cycles are rejected in v1; iteration uses explicit bounded
  loop/iterator nodes.
- Metrics must use bounded labels and avoid workflow ids, run ids, node ids,
  edge ids, effect ids, prompts, completions, and full error text.

## Intended Public API Additions

The implementation should add these public API types in `rakka-agent-workflow`:

- `AgentCompiledExecutionPlan`
- `AgentCompiledPlanNode`
- `AgentCompiledPlanEdge`
- `AgentCompiledPlanPort`
- `AgentCompiledNodeKind`
- `AgentCompiledNodeKindDescriptor`
- `AgentCompiledNodeKindCatalog`
- `AgentCompiledPlanRuntimeCapabilities`
- `AgentCompiledIteratorPolicy`
- `AgentCompiledPortPolicy`
- `AgentCompiledPlanValidationError`
- `AgentCompiledWorkflowRegistration`
- `AgentGraphRunState`
- `AgentGraphNodeState`
- `AgentGraphNodeStatus`
- `AgentGraphStateSchemaVersion`
- `AgentGraphTerminalStatus`
- `AgentGraphWaitReason`
- `AgentGraphLoopInstanceState`
- `AgentGraphBlockedReason`
- `AgentGraphRunProjection`
- `AgentGraphNodeProjection`
- `AgentGraphScheduler`
- `AgentGraphSchedulerError`
- `AgentGraphSchedulerResult`
- `AgentGraphSchedulerTransition`
- `AgentGraphEffectBridge`
- `AgentGraphEffectBridgeError`
- `AgentGraphEffectBridgeResult`
- `AgentGraphEffectScheduleRequest`
- `AgentGraphEffectScheduleOutcome`
- `AgentGraphEffectCommandOutcome`
- `AgentGraphEffectFailureDisposition`
- `AgentGraphTimerScheduleRequest`
- `AgentGraphTimerScheduleOutcome`
- `AgentGraphHumanCheckpointScheduleRequest`
- `AgentGraphHumanCheckpointScheduleOutcome`
- `AgentGraphRuntime`
- `AgentRuntimeEvent`
- `AgentRuntimeEventSink`
- `AgentTriggerSource`
- `AgentCredentialBindingRef`
- `AgentCredentialResolver`
- `AgentCredentialResolverFuture`
- `AgentCredentialResolutionRequest`
- `AgentCredentialResult`
- `AgentCredentialError`
- `AgentCredentialUse`
- `AgentEphemeralCredential`
- `AgentEphemeralCredentialMaterial`
- `validate_compiled_execution_plan`
- `validate_compiled_execution_plan_with_catalog`
- `CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION`

The top-level `rakka` facade should re-export these through
`rakka::agent_workflow` when the `agent-workflow` feature is enabled.

## Release Targets

`MVP`

- A compiled plan with input, transform, effect, branch, join, wait, and
  terminal nodes can start, run, pause, resume, complete, recover after process
  restart, and emit runtime events in memory-backed tests.

`Scale Preview`

- Graph runs work through actor-backed and sharded runtimes, dispatcher workers
  claim effects, timers recover after scanner restart, query projections expose
  waiting and failed nodes, and bounded metrics survive load tests.

`Production Candidate`

- PostgreSQL-backed graph state and query indexes are covered by optional
  integration gates, Kubernetes drain/passivation behavior is documented and
  tested, compatibility tests cover N/N+1 serialized contracts, and runbooks
  describe graph-specific incidents.

## Phase 0: Documentation And Boundary Lock

Status: implemented.

Goal: establish the tracking home and prevent future scope drift into product
backend concerns.

Scope:

- Create `docs/plans/compiled_execution_with_graph_schdlr/`.
- Add the spec and this implementation plan.
- Record that follow-up docs for this effort should stay in this directory.
- Keep the editor DSL/compiler outside Rakka.

Acceptance:

- The directory contains exactly the initial spec and implementation plan for
  the first documentation change.
- The docs clearly state that Rakka owns compiled IR execution, not product
  DSL interpretation.

## Phase 1: Compiled Execution Plan IR

Status: implemented.

Goal: add the product-neutral runtime IR and validation surface.

### Slice 1.1: IR Types

Status: implemented.

Scope:

- Add a new module such as `compiled_plan` in `rakka-agent-workflow`.
- Define `AgentCompiledExecutionPlan`, `AgentCompiledPlanNode`,
  `AgentCompiledPlanEdge`, `AgentCompiledPlanPort`, and
  `AgentCompiledNodeKind`.
- Add stable id newtypes only when existing id types are not sufficient.
- Include plan id, workflow id, workflow type, definition version, plan schema
  version, plan fingerprint, entry nodes, nodes, edges, labels, and artifact
  references.
- Include optional logical credential binding refs on effect-producing nodes
  that need third-party credentials.
- Keep product-specific block configuration behind artifact references or
  bounded attributes.

Acceptance:

- All IR structs derive serde round-trip support.
- The IR can represent linear, branch, join, effect, wait, human, child
  workflow, and terminal nodes.
- No IR field stores raw editor DSL, UI layout, credentials, or secret values.
- Credential-using nodes store only logical binding refs.

Tests:

- JSON round-trip for a representative plan.
- Missing required identity fields are rejected by construction or validation.
- Node kinds serialize with stable names.
- Logical credential binding refs round trip without resolved secret material.

Implementation notes:

- Added `crates/rakka-agent-workflow/src/compiled_plan.rs` with serializable
  compiled execution plan, node, edge, port, target, compatibility, schema
  version, fingerprint, and credential binding reference contracts.
- Re-exported the new IR contracts through `rakka-agent-workflow` and the
  top-level `rakka::agent_workflow` facade path.
- Added `crates/rakka-agent-workflow/tests/compiled_plan_contract.rs` covering
  representative plan JSON round-trip, stable node-kind wire names, and
  credential binding references without resolved secret material.

### Slice 1.2: Runtime Validation

Status: implemented.

Scope:

- Add runtime node capability discovery with
  `AgentCompiledNodeKindDescriptor`, `AgentCompiledNodeKindCatalog`, and
  `AgentCompiledPlanRuntimeCapabilities`.
- Expose supported product-neutral compiled node kinds, required targets,
  credential-binding support, policy artifact support, port policy, semantic
  hints, required runtime features, and current availability.
- Add `validate_compiled_execution_plan`.
- Drive node-kind validation from the runtime capability catalog where
  practical.
- Validate unique node, edge, and port ids.
- Validate all edge references.
- Validate output-to-input port direction.
- Validate entry and terminal reachability.
- Reject arbitrary cycles.
- Require explicit bounds for loop/iterator nodes.
- Validate branch and join declarations.
- Reject high-cardinality or sensitive values in hot labels.
- Reject raw credential-like fields or credential binding refs in hot labels.

Acceptance:

- Application backends can discover Rakka runtime node-kind capabilities without
  treating Rakka as the product editor's node palette.
- Invalid plans fail before run start.
- Validation errors expose stable error codes.
- Deterministic sorting produces a stable validation order.
- Nodes that need credentials use logical binding refs only.

Tests:

- Duplicate ids.
- Missing nodes and ports.
- Edge direction mismatch.
- Invalid branch targets.
- Forbidden cycles.
- Invalid loop bounds.
- Missing terminal reachability.
- Raw secret-like field rejected.
- Credential binding ref accepted on the typed node field but rejected as a
  metric label.
- Runtime capability catalog lists all supported product-neutral node kinds.
- Product-specific editor blocks are not represented in the runtime catalog.

Implementation notes:

- Added runtime node capability discovery APIs in
  `crates/rakka-agent-workflow/src/compiled_plan.rs`:
  `AgentCompiledNodeKindDescriptor`, `AgentCompiledNodeKindCatalog`,
  `AgentCompiledPlanRuntimeCapabilities`, and `AgentCompiledPortPolicy`.
- Added explicit iterator bounds with `AgentCompiledIteratorPolicy`.
- Added `validate_compiled_execution_plan` and
  `validate_compiled_execution_plan_with_catalog` with stable
  `AgentCompiledPlanValidationError::code()` values.
- Validation now covers required identity fields, duplicate node/edge/port ids,
  catalog availability, target and credential-binding support, node policy
  support, port policies, edge endpoint existence, output-to-input direction,
  terminal reachability, cycle rejection, bounded iterator declarations,
  branch connected-path declarations, join merge behavior declarations,
  required input reachability, and bounded/sensitive attribute checks.
- Re-exported the new validation and catalog APIs through
  `rakka-agent-workflow`; the top-level `rakka::agent_workflow` facade receives
  them through its existing wildcard re-export.
- Expanded
  `crates/rakka-agent-workflow/tests/compiled_plan_contract.rs` with runtime
  catalog, validation success, duplicate id, missing node/port, direction
  mismatch, cycle, iterator bound, unreachable terminal, branch/join
  declaration, secret-like attribute, and credential-binding label tests.

### Slice 1.3: Plan Registration And Compatibility

Status: implemented.

Scope:

- Extend or parallel `AgentWorkflowRegistry` with compiled plan registration.
- Support lookup by workflow type and definition version.
- Preserve multiple versions for rolling updates.
- Store plan fingerprint and schema version compatibility metadata.

Acceptance:

- Existing `AgentWorkflowRegistry` behavior remains compatible.
- Applications can register an `AgentWorkflow` plus compiled execution plan.
- Disabled or incompatible plan versions can be rejected by runtime policy.

Tests:

- Duplicate workflow type and definition version rejection.
- Same workflow with multiple versions accepted.
- Incompatible plan schema version rejected.
- Plan fingerprint preserved across serde round trip.

Implementation notes:

- Extended `AgentWorkflowRegistry` in
  `crates/rakka-agent-workflow/src/definition.rs` with compiled registration
  storage keyed by `workflow_type + definition_version`, without changing the
  existing workflow-only registration behavior.
- Added `AgentCompiledWorkflowRegistration` as a serializable pair of
  `AgentWorkflow` metadata and `AgentCompiledExecutionPlan` runtime IR.
- Added `register_compiled` for atomic workflow-plus-plan registration and
  `register_compiled_plan` for attaching a compiled plan to an already
  registered workflow definition.
- Added lookup helpers: `get_compiled`, `contains_compiled`,
  `compiled_registrations_for_type`, `compiled_registrations`, and
  `compiled_len`.
- Added registration compatibility checks for compiled plan validation,
  supported compiled plan schema version, workflow id, workflow type, and
  definition version agreement.
- Added `CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION` for the current supported
  registration schema boundary.
- Expanded `crates/rakka-agent-workflow/tests/workflow_registry.rs` to cover
  compiled pair registration, attaching a plan to an existing definition,
  duplicate compiled registrations, multiple versions for rolling updates,
  unsupported schema rejection, mismatched metadata rejection, and fingerprint
  preservation across serde round trip.

## Phase 2: Durable Graph Run State

Status: implemented.

Goal: persist enough graph state for recovery, passivation, and deterministic
scheduling.

### Slice 2.1: Graph State Types

Status: implemented.

Scope:

- Add a new module such as `graph_state`.
- Define `AgentGraphRunState`, `AgentGraphNodeState`, and
  `AgentGraphNodeStatus`.
- Track plan id, plan fingerprint, graph schema version, node states, selected
  branches, skipped nodes, loop instances, blocked reason, output refs,
  scheduler revision, and last event sequence.
- Track per-node status, attempts, dependency readiness, input/output artifact
  refs, scheduled effect ids, timer ids, checkpoint ids, timestamps, and error
  codes.

Acceptance:

- Graph state is serializable.
- Graph state can represent pending, runnable, running, waiting, completed,
  skipped, failed, cancelled, and terminal runs.
- Large values use artifact refs.

Tests:

- Graph state JSON round trip.
- Bounded inline state policy remains enforced.
- High-cardinality fields remain out of metric label helpers.

Implementation notes:

- Added `crates/rakka-agent-workflow/src/graph_state.rs` with serializable
  durable graph state contracts.
- Defined `AgentGraphRunState`, `AgentGraphNodeState`, and
  `AgentGraphNodeStatus`, plus supporting schema, terminal status, wait reason,
  loop instance, and blocked reason contracts.
- Graph run state tracks compiled plan id/fingerprint, graph schema version,
  per-node state, selected branch paths, skipped nodes, loop instances, blocked
  reason, output artifact refs, scheduler revision, last event sequence, and
  terminal status.
- Node state tracks node kind, status, attempts, dependency readiness,
  input/output artifact refs, scheduled effect ids, timer ids, checkpoint ids,
  wait reason, error code, and timestamps.
- Re-exported graph state contracts from `rakka-agent-workflow`; the top-level
  `rakka::agent_workflow` facade receives them through its existing wildcard
  re-export.
- Added `crates/rakka-agent-workflow/tests/graph_state_contract.rs` covering
  JSON round trip, stable status wire names, artifact-ref-only large value
  surfaces, inline state policy enforcement, and metric-label cardinality
  separation.

### Slice 2.2: Additive `AgentRunState` Integration

Status: implemented.

Scope:

- Add `graph_state: Option<AgentGraphRunState>` to `AgentRunState` with serde
  defaults.
- Ensure old serialized run state without `graph_state` still deserializes.
- Keep existing `current_step_id` behavior for non-graph runs.
- Define migration helper behavior for runs that do not use graph execution.

Acceptance:

- Existing `rakka-agent-workflow` tests still pass.
- Existing run-state JSON compatibility tests accept missing graph fields.
- New graph runs can persist graph state without breaking old readers in an
  additive compatibility window.

Tests:

- Existing fixture JSON with missing graph state deserializes.
- New graph state serializes and deserializes.
- N/N+1 compatibility test covers additive graph fields.

Implementation notes:

- Added `graph_state: Option<AgentGraphRunState>` to
  `crates/rakka-agent-workflow/src/domain.rs` with serde defaulting and
  skip-when-none serialization.
- Updated existing non-graph `AgentRunState` fixture constructors to set
  `graph_state: None`, preserving current step-cursor behavior for non-graph
  runs.
- Added API compatibility coverage in
  `crates/rakka-agent-workflow/tests/api_compatibility.rs` proving a new run
  state with graph state round-trips and old JSON without `graph_state`
  deserializes with `None`.
- Existing domain, migration, runtime, query, timer, actor, and failure
  injection tests continue to pass with the additive field.

### Slice 2.3: Graph Snapshots And Query Shape

Status: implemented.

Scope:

- Extend operational snapshots with graph-specific summaries.
- Add query projection records for node status, waiting reason, node kind,
  error code, and plan fingerprint.
- Keep query indexes separate from correctness.

Acceptance:

- Operators can identify runs waiting on effects, timers, or humans at node
  granularity.
- Query fields use bounded labels where appropriate.

Tests:

- Snapshot reports graph node counts by status.
- Query index lists failed nodes and waiting nodes.
- Projection can rebuild from durable graph state.

Implementation notes:

- Added `AgentGraphRunProjection` and `AgentGraphNodeProjection` in
  `crates/rakka-agent-workflow/src/graph_state.rs` as bounded derived
  read-model records from durable graph state.
- Extended `AgentRunIndexEntry` in
  `crates/rakka-agent-workflow/src/query.rs` with optional graph projection
  data using serde defaults for additive compatibility.
- Added graph run query filters for compiled plan fingerprint, node status,
  node kind, node wait reason, and stable node error code.
- Extended process-local runtime snapshots in
  `crates/rakka-agent-workflow/src/snapshots.rs` with graph run counts,
  waiting/failed node counts, blocked graph run count, and sampled run graph
  projections.
- Kept `rakka-persistence-postgres` query decoding additive by projecting
  `graph: None`; durable graph state remains the correctness source and
  PostgreSQL graph projection columns remain a later optional storage follow-up.
- Added tests in `crates/rakka-agent-workflow/tests/workflow_query_model.rs`
  for graph projection/filter behavior and in
  `crates/rakka-agent-workflow/tests/runtime_snapshots.rs` for graph snapshot
  summaries.

## Phase 3: Graph Scheduler Core

Status: implemented.

Goal: implement deterministic ready-node evaluation and graph state
transitions.

### Slice 3.1: Scheduler Engine

Status: implemented.

Scope:

- Add `AgentGraphScheduler`.
- Initialize graph state from a compiled plan and start command.
- Compute ready nodes from dependency state.
- Persist transitions from pending to runnable, runnable to running, and
  running to terminal/waiting statuses.
- Sort ready nodes by stable node id.

Acceptance:

- A linear compiled graph starts and completes in deterministic order.
- A crash after a node becomes runnable recovers the same ready set.
- Scheduler transitions expose stable error codes.

Tests:

- Linear flow.
- Crash after command acceptance.
- Crash after node becomes runnable.
- Recovery recomputes ready nodes deterministically.

Implementation notes:

- Added `crates/rakka-agent-workflow/src/graph_scheduler.rs` with a pure,
  deterministic `AgentGraphScheduler`.
- Added public scheduler result, transition, and stable error types:
  `AgentGraphSchedulerResult`, `AgentGraphSchedulerTransition`, and
  `AgentGraphSchedulerError`.
- Scheduler initialization validates the compiled plan and creates durable
  graph node state sorted by stable node id.
- Ready-node evaluation derives runnable work from durable graph state and
  required incoming dependencies instead of trusting an in-memory queue.
- State transitions now cover pending to runnable, runnable to running, running
  to completed or terminal, running to waiting, and running to failed.
- Added focused scheduler tests in
  `crates/rakka-agent-workflow/tests/graph_scheduler.rs`.

### Slice 3.2: Branch, Fan-Out, Fan-In, And Skip Propagation

Status: implemented.

Scope:

- Implement fan-out readiness for multiple downstream nodes.
- Implement join behavior for wait-for-all and wait-for-any joins.
- Implement branch selection and skip propagation.
- Persist selected branches before downstream nodes run.

Acceptance:

- Branch choices are durable.
- Unselected branches are skipped when unreachable.
- Joins behave deterministically with completed and skipped upstream nodes.

Tests:

- Fan-out runs independent nodes.
- Fan-in waits for required upstream nodes.
- Branch selects one path and skips the other.
- Join after branch handles skipped upstream according to policy.

Implementation notes:

- Extended `AgentGraphScheduler` dependency evaluation so fan-out marks every
  eligible downstream node runnable in stable node-id order.
- Added durable branch completion through `complete_branch_node`, which records
  selected outgoing edge ids before downstream nodes can run.
- Added branch-selection validation with stable `invalid-branch-selection`
  errors.
- Added deterministic skip propagation for pending nodes that become
  unreachable through unselected branch edges or skipped upstream nodes.
- Added join readiness for `wait-for-all` and `wait-for-any` merge behavior;
  skipped upstreams count as resolved for wait-for-all, while wait-for-any can
  proceed after the first completed active upstream.
- Added scheduler tests for fan-out, fan-in, wait-for-any joins, durable branch
  selection, skip propagation, and invalid branch selections.

### Slice 3.3: Bounded Iteration

Status: implemented.

Scope:

- Implement explicit iterator or loop node semantics.
- Reject unbounded loops.
- Persist loop instance ids and iteration counters.
- Allow loop body execution through expanded logical node instances or scoped
  loop state.

Acceptance:

- Bounded iteration executes at most the declared max iterations.
- Recovery resumes the correct iteration.
- Loop node ids and iteration ids are deterministic.

Tests:

- Zero-iteration loop.
- Multi-iteration loop.
- Max iteration exceeded failure.
- Crash during loop body recovers current iteration.

Implementation notes:

- Added iterator-specific scheduler transitions on `AgentGraphScheduler` for
  starting an iteration, completing an iteration, completing the iterator node,
  and querying the active iteration index.
- Kept iteration state scoped in durable `AgentGraphLoopInstanceState` records
  keyed by iterator node id plus deterministic zero-based iteration index.
- Required iterator nodes to complete through iterator-specific transitions so
  generic node completion cannot bypass bounded iteration policy.
- Enforced `max_iterations` at runtime; an attempt to start an iteration beyond
  the declared bound durably fails the iterator node and graph with stable node
  error code `iterator-bound-exceeded`.
- Added scheduler tests for zero actual iterations, multiple bounded
  iterations, bound-exceeded failure, and recovery of an active running
  iteration after a crash.

### Slice 3.4: Cancellation And Terminal Policy

Status: implemented.

Scope:

- Add graph-aware cancellation transitions.
- Define terminal success and terminal failure rules.
- Integrate compensation placeholder behavior without requiring full
  compensation workflows in MVP.

Acceptance:

- Cancelling a graph run stops scheduling new nodes.
- Running or waiting nodes move to cancelled or cancelling status according to
  policy.
- Terminal state is durable and idempotent.

Tests:

- Cancel before start.
- Cancel while running.
- Cancel while waiting for effect.
- Terminal failure stops downstream scheduling.

Implementation notes:

- Added graph-level cancellation through `AgentGraphScheduler::cancel_graph_run`.
- Cancellation is durable and idempotent for already-cancelled graph state.
- Unresolved pending, runnable, running, and waiting nodes move to
  `cancelled`; completed, skipped, failed, and terminal nodes keep their
  terminal status.
- Active unresolved loop instances also move to `cancelled`.
- Terminal success and terminal failure now cancel remaining unresolved work so
  no stale runnable nodes remain after graph terminal status is set.
- Terminal failure marks the failing node `failed`, marks the graph `failed`,
  and cancels the remaining unresolved graph work.
- Compensation is left as an explicit later runtime concern; this slice only
  preserves durable terminal/cancelled state where compensation hooks can attach.
- Added scheduler tests for cancel before start, cancel while running, cancel
  while waiting for an effect, terminal failure stopping downstream scheduling,
  terminal success cancelling leftover parallel work, and idempotent repeated
  cancellation.

## Phase 4: Effect Bridge

Status: implemented.

Goal: connect graph nodes to durable outbox, timers, human checkpoints, child
workflow commands, and adapter dispatch.

### Slice 4.1: Effect Node Mapping

Status: implemented.

Scope:

- Add conversion from effect-producing plan nodes to `AgentEffect`.
- Use deterministic effect ids, deduplication keys, and idempotency keys.
- Include node id, loop instance id, plan fingerprint, and target class in
  stable identity construction.
- Schedule effects through `AgentRunInbox::schedule_effect`.

Acceptance:

- No external effect is dispatched before durable outbox scheduling succeeds.
- Duplicate scheduling returns the existing durable outbox entry.
- Effect payloads and results use artifact refs.

Tests:

- Model/tool node schedules effect.
- Duplicate effect node scheduling deduplicates.
- Crash after effect scheduling recovers due effect.
- Idempotency key is stable across recovery.

Implementation notes:

- Added `crates/rakka-agent-workflow/src/effect_bridge.rs` with
  `AgentGraphEffectBridge`, `AgentGraphEffectScheduleRequest`,
  `AgentGraphEffectScheduleOutcome`, `AgentGraphEffectBridgeError`, and
  `AgentGraphEffectBridgeResult`.
- Mapped effect-producing compiled node kinds to `AgentEffectKind` values for
  durable outbox scheduling while leaving timer and human checkpoint nodes for
  their dedicated Phase 4 slices.
- Generated deterministic effect ids, deduplication keys, and idempotency keys
  from run id, plan fingerprint, compiled node id, loop instance id, effect
  kind, and target class.
- Copied logical node target metadata into `AgentEffectTarget` and persisted
  only logical credential binding refs, not resolved credentials.
- Scheduled effects exclusively through `AgentRunInbox::schedule_effect`; graph
  node state is marked waiting for an effect only after durable outbox
  scheduling returns scheduled or duplicate acceptance.
- Added `crates/rakka-agent-workflow/tests/effect_bridge.rs` covering
  model/tool scheduling, duplicate scheduling deduplication, due-effect recovery
  after a crash, and idempotency-key stability across recovery.

### Slice 4.2: Credential Binding Resolver Contract

Status: implemented.

Scope:

- Add `AgentCredentialBindingRef`, `AgentCredentialResolver`,
  `AgentCredentialUse`, and `AgentEphemeralCredential`.
- Keep the resolver trait application-implemented.
- Pass tenant, workflow id, run id, plan fingerprint, node id, target
  descriptor, credential binding ref, causation id, correlation id, and trace
  context to the resolver.
- Ensure resolved credentials are held in memory only for one dispatch attempt
  or short-lived adapter call.
- Ensure resolved credentials are never serialized into `AgentEffect`, graph
  state, runtime events, logs, metrics, snapshots, or query indexes.
- Surface resolver errors as stable effect dispatch failures.

Acceptance:

- A credential-using effect can be dispatched through a fake resolver.
- Resolver failures produce retryable or terminal bounded error codes according
  to effect policy.
- Credential rotation can happen behind the same binding ref without changing
  compiled plan fingerprints.
- Rakka never persists resolved credential values.

Tests:

- Fake resolver returns an ephemeral credential for a tool effect.
- Resolver missing binding maps to stable dispatch failure.
- Resolver revoked binding maps to stable dispatch failure.
- Resolved credential value is absent from serialized effect, graph state,
  runtime event, logs, metrics, snapshots, and query projection fixtures.
- Same compiled plan works after fake resolver changes the secret version for
  an existing binding ref.

Implementation notes:

- Added `crates/rakka-agent-workflow/src/credentials.rs` with
  `AgentCredentialResolver`, `AgentCredentialResolverFuture`,
  `AgentCredentialResolutionRequest`, `AgentCredentialUse`,
  `AgentEphemeralCredential`, `AgentEphemeralCredentialMaterial`,
  `AgentCredentialError`, and `AgentCredentialResult`.
- Added shared `AGENT_CREDENTIAL_BINDING_REF_ATTRIBUTE` plus
  `credential_binding_ref_from_effect` so effect mapping, dispatch helpers, and
  adapters agree on the logical binding-ref metadata key.
- Kept resolved credential material non-serializable and gave ephemeral
  credential types redacted debug output so they are safe for accidental
  diagnostic formatting.
- Added stable resolver error codes and mapped resolver failures to existing
  `AgentAdapterFailureClass` plus durable `OutboxDispatchResult` failure
  strings.
- Updated `AgentGraphEffectBridge` to write credential binding refs using the
  shared credential metadata key.
- Added `crates/rakka-agent-workflow/tests/credentials.rs` covering fake
  resolver success, missing/revoked resolver failures, retryable resolver
  unavailability, credential rotation behind a stable binding ref, and absence
  of resolved secrets from serialized durable effect, graph state, and graph
  projection records.

### Slice 4.3: Completion And Failure Commands

Status: implemented.

Scope:

- Route effect completions and failures through durable inbox commands.
- Map `EffectCompleted` to node completion.
- Map `EffectFailed` to retry, exhaustion, or terminal failure.
- Persist result artifact refs before downstream nodes become runnable.

Acceptance:

- Duplicate callbacks do not advance graph state twice.
- Failed effects follow retry policy.
- Exhausted effects produce stable node failure state.

Tests:

- Duplicate completion callback.
- Retryable failure.
- Exhausted retry budget.
- Crash after completion command acceptance but before graph transition.

Implementation notes:

- Extended `AgentGraphScheduler` with waiting-node transitions for
  `Waiting -> Completed` and `Waiting -> Failed` so effect callbacks can resume
  graph nodes without pretending the node is still running.
- Added `AgentGraphEffectBridge` command helpers for `EffectCompleted` and
  `EffectFailed`, plus `accept_and_apply_effect_completed` and
  `accept_and_apply_effect_failed`.
- Added `AgentGraphEffectFailureDisposition` to carry whether a failed effect
  was retry-scheduled, exhausted, or terminal after durable outbox retry policy
  handling.
- Completion commands use the command `payload_ref` as the result artifact ref
  and persist it onto the effect node output before downstream scheduling can
  observe completion.
- Duplicate inbox acceptance remains apply-safe: if a command was accepted
  before a crash but graph state did not advance, recovery applies it; if graph
  state already advanced, duplicate callbacks return no-op transitions.
- Retry-scheduled failures leave the graph node waiting with a bounded
  `error_code`; exhausted or terminal failures mark the node failed, fail the
  graph, and cancel unresolved downstream work.
- Expanded `crates/rakka-agent-workflow/tests/effect_bridge.rs` with duplicate
  completion, retryable failure, exhausted failure, and crash-after-command-
  acceptance recovery coverage.

### Slice 4.4: Timers, Human Checkpoints, And Child Workflows

Status: implemented.

Scope:

- Map timer wait nodes to durable timer entries.
- Map human checkpoint nodes to checkpoint openings and decision commands.
- Map child workflow nodes to durable child workflow commands.
- Preserve trace, causation, and correlation context across each boundary.

Acceptance:

- Timer waits do not hold live tasks.
- Human waits do not mutate live actor state directly.
- Child workflow starts are idempotent.

Tests:

- Timer node resumes once after scanner restart.
- Human decision resumes a waiting graph node.
- Child workflow command uses stable deduplication metadata.

Implementation notes:

- Added graph timer scheduling through `AgentGraphTimerScheduleRequest` and
  `AgentGraphTimerScheduleOutcome`. Timer nodes map to deterministic
  `AgentTimerEntry` values and move to `Waiting` with
  `AgentGraphWaitReason::Timer` only after the durable timer store accepts the
  timer or reports an equivalent duplicate.
- Added `timer_fired_command` and `accept_and_apply_timer_fired` helpers on
  `AgentGraphEffectBridge`. Accepted `TimerFired` commands complete the waiting
  timer node, persist any command `payload_ref` onto the first output port, and
  return no-op transitions for duplicate callbacks after the graph already
  advanced.
- Added graph human checkpoint opening through
  `AgentGraphHumanCheckpointScheduleRequest` and
  `AgentGraphHumanCheckpointScheduleOutcome`. Human checkpoint nodes schedule a
  deterministic `HumanApprovalRequest` outbox effect, record the checkpoint id
  and approval effect id in graph state, and move to `Waiting` with
  `AgentGraphWaitReason::Human` only after durable outbox acceptance.
- Added `human_decision_submitted_command` and
  `accept_and_apply_human_decision` helpers. Accepted
  `HumanDecisionSubmitted` commands complete the waiting human node and persist
  the decision `payload_ref` to the first output port.
- Child workflow nodes continue to use durable outbox scheduling as
  `AgentEffectKind::ChildWorkflowCommand`, but now record
  `AgentGraphWaitReason::ChildWorkflow` so query projections can distinguish
  child workflow waits from ordinary effect waits.
- Expanded `crates/rakka-agent-workflow/tests/effect_bridge.rs` with timer
  redelivery/idempotency, human decision resume, and child workflow command
  deduplication coverage.

### Slice 4.5: Dispatcher Fleet Integration

Status: implemented.

Scope:

- Ensure graph-scheduled effects are discoverable by dispatcher fleet indexes.
- Include bounded target class and node kind dimensions.
- Ensure dispatcher claim/fence behavior remains independent from graph state
  correctness.

Acceptance:

- Dispatcher workers can claim graph effects.
- Stuck graph effects are visible in query projections.
- Claim expiration does not corrupt node state.

Tests:

- Claimed graph effect completes.
- Expired claim is recovered by another worker.
- Stuck dispatch query returns graph run/node context.

Implementation notes:

- Added optional graph dispatch context to `AgentDispatchEntry`,
  `AgentDispatcherEntrySnapshot`, and `AgentDispatchIndexEntry`: compiled plan
  fingerprint, compiled node id, compiled node kind, and loop instance id.
- Dispatcher fleet registration now extracts only whitelisted bounded graph
  metadata from `AgentEffectTarget.attributes`; it does not copy credential
  binding refs or arbitrary product metadata into fleet/query projections.
- Added `AgentDispatchQuery` filters for graph plan fingerprint, graph node id,
  and graph node kind so stuck dispatches can be queried back to compiled graph
  context.
- Kept dispatcher claim/fence correctness independent from graph state. Expired
  claims are recovered through durable outbox/fleet state, and graph node state
  is not mutated by dispatcher claim failures.
- Updated the optional PostgreSQL query index with nullable graph dispatch
  columns, additive `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` migration
  statements, and graph-node/graph-plan indexes.
- Added `AgentCompiledNodeKind::from_label` for stable dispatch projection
  decoding.
- Expanded `crates/rakka-agent-workflow/tests/dispatcher_fleet.rs` with graph
  effect claim/completion coverage plus expired graph claim recovery and stuck
  dispatch query coverage.

## Phase 5: Trigger Normalization

Goal: define runtime command helpers for trigger executions without making
Rakka own trigger registration.

### Slice 5.1: Trigger Source Metadata

Status: implemented.

Scope:

- Add `AgentTriggerSource`.
- Support API, webhook, schedule, on-demand, system, child workflow, external
  callback, and human decision categories.
- Add bounded labels such as trigger kind, deployment channel, and tenant tier.
- Reject unbounded or sensitive metadata from hot labels.

Acceptance:

- Trigger source metadata can attach to `AgentCommand`.
- No raw webhook URL, token, signature, request body, or user id becomes a hot
  metric label.

Tests:

- Trigger source JSON round trip.
- Forbidden metadata rejection.
- Bounded labels accepted.

Implementation notes:

- Added `crates/rakka-agent-workflow/src/triggers.rs` with
  `AgentTriggerSource`, `AgentTriggerSourceKind`,
  `AgentTriggerSourceError`, and stable command attribute constants for
  `trigger_kind`, `deployment_channel`, and `tenant_tier`.
- Trigger source categories now cover API, webhook, schedule, on-demand,
  system, child workflow, external callback, and human decision trigger
  executions.
- `AgentTriggerSource::attach_to_command` adds normalized trigger metadata to
  `AgentCommand.attributes` without altering durable command ids, inbox
  deduplication keys, causation ids, or correlation ids.
- Trigger label validation reuses the hot-metric label boundary: labels must
  be bounded, single-line, low-cardinality metric attributes and must not
  contain raw webhook URLs, tokens, signatures, request bodies, user ids, or
  other secret-like material.
- Added `trigger_kind` and `deployment_channel` to the bounded metric label
  catalog alongside the existing `tenant_tier` label.
- Re-exported the trigger source API through `rakka-agent-workflow` and the
  top-level `rakka::agent_workflow` facade path.
- Added `crates/rakka-agent-workflow/tests/triggers.rs` covering JSON
  round-trip, stable kind labels, command attachment, forbidden metadata
  rejection, bounded metric-label acceptance, and command attribute conflict
  rejection.

### Slice 5.2: Command Builders

Scope:

- Add helpers to construct `StartRun`, `SubmitSignal`,
  `HumanDecisionSubmitted`, `CancelRun`, and `RetryRun` with trigger metadata.
- Require command id, deduplication key, causation id, correlation id, tenant,
  timestamp, and optional payload artifact ref.

Acceptance:

- API, webhook, schedule, and on-demand starts normalize to durable commands.
- Commands are accepted only through `AgentRunInbox`.
- Product trigger registration remains outside Rakka.

Tests:

- API start command.
- Webhook start command.
- Schedule start command.
- On-demand start command.
- Duplicate trigger command deduplicates by key.

## Phase 6: Runtime Event Stream

Goal: emit ordered events after durable transitions for UI projections,
auditing, logs, and live execution streams.

### Slice 6.1: Event Contracts

Scope:

- Add `AgentRuntimeEvent`.
- Include run id, workflow id, definition version, plan fingerprint, scheduler
  revision, event sequence, timestamp, kind, optional node/effect/timer/
  checkpoint ids, causation id, correlation id, trace context, and bounded
  attributes.
- Define event kinds for run, node, effect, timer, human, branch, loop,
  cancellation, completion, and failure transitions.

Acceptance:

- Events serialize with stable names.
- Event sequence is per-run and monotonic.
- Events are emitted only after persistence succeeds.

Tests:

- Event JSON round trip.
- Stable event sequence.
- Failed persistence does not emit success event.

### Slice 6.2: Event Sink Trait

Scope:

- Add `AgentRuntimeEventSink`.
- Provide in-memory test implementation.
- Define sink failure behavior.
- Decide whether durable event publication uses best-effort sink writes,
  durable outbox/audit events, or both.

Acceptance:

- Tests can assert emitted events deterministically.
- Sink errors are observable.
- Sink errors cannot create false graph state transitions.

Tests:

- In-memory sink records ordered events.
- Sink failure is reported.
- State transition remains correct under sink failure policy.

### Slice 6.3: Projection And Live Stream Guidance

Scope:

- Add docs in this directory for projecting runtime events into product UI
  run-history views.
- Optionally add HTTP/SSE helper only as an adapter surface, not as the product
  API.
- Align event fields with logs, audit, and trace context.

Acceptance:

- Applications can build live run views without polling raw durable state for
  every transition.
- Runtime events avoid high-cardinality hot metric labels.

Tests:

- Projection rebuild from event stream.
- Event-to-log/audit field consistency.
- Metric cardinality guard.

## Phase 7: Runtime Integration

Goal: make graph execution work in local, actor-backed, and sharded runtime
paths.

### Slice 7.1: Actor-Backed Runtime

Scope:

- Extend `AgentRunActorCommand` or add graph-specific commands.
- Add `AgentGraphRuntime` that composes scheduler, run store, workflow store,
  inbox, outbox, timers, checkpoint runtime, and event sink.
- Keep messages small and artifact-ref based.

Acceptance:

- Actor restart recovers graph state and pending effects.
- Actor snapshots include graph summaries.

Tests:

- Local actor graph run.
- Actor restart after node runnable.
- Actor restart after effect scheduled.
- Snapshot reports node status counts.

### Slice 7.2: Sharded Runtime

Scope:

- Integrate graph runtime with `init_agent_run_sharding`.
- Route graph run commands by stable run id.
- Preserve passivation and remembered entity behavior.
- Ensure graph state recovers after shard movement.

Acceptance:

- Sharded graph runs route by run id.
- Passivation does not lose runnable or waiting nodes.
- Shard movement recovers graph state and due effects.

Tests:

- Sharded graph run routes by stable run id.
- Recovery after passivation.
- Recovery after shard movement in deterministic local test.

### Slice 7.3: Drain And Shutdown

Scope:

- Register graph runtime drain blockers and shutdown tasks where needed.
- Stop public ingress through existing gates.
- Preserve accepted commands and scheduled effects through abrupt shutdown.
- Report graph-specific drain blockers in snapshots.

Acceptance:

- Kubernetes drain stops new public graph commands.
- Accepted work remains recoverable.
- Drain reports waiting graph nodes and in-flight effects.

Tests:

- Drain before graph start.
- Drain while waiting for effect.
- Drain while waiting for human checkpoint.
- Abrupt shutdown recovery.

## Phase 8: Hardening, Compatibility, And Operations

Goal: make the feature production-candidate quality.

### Slice 8.1: Compatibility Suite

Scope:

- Extend API compatibility tests for compiled plan and graph state JSON.
- Verify additive fields are accepted.
- Verify missing new fields deserialize with defaults.
- Document allowed breaking changes and migration rules.

Acceptance:

- N/N+1 serialized compatibility is covered.
- Old non-graph runs still recover.

Tests:

- Current and previous graph schema versions.
- Missing graph state default.
- Additive event fields accepted.

### Slice 8.2: Failure Injection Suite

Scope:

- Add failure tests for every durable boundary.
- Include crash after inbox acceptance, graph initialization, runnable marking,
  effect scheduling, callback acceptance, timer firing, human decision, event
  sink write, and passivation.

Acceptance:

- Each failure either recovers safely or reports a stable error.
- No duplicate state advancement occurs.

Tests:

- Crash after command acceptance.
- Crash after node runnable.
- Crash after effect scheduling.
- Crash after callback acceptance.
- Crash during event sink write.

### Slice 8.3: Load And Cardinality

Scope:

- Add deterministic load tests for large node counts, fan-out/fan-in graphs,
  many waiting timers, many pending effects, and many runtime events.
- Assert bounded metric series.
- Assert ready-node evaluation remains deterministic.

Acceptance:

- Scheduler handles realistic graph sizes without unbounded memory growth.
- Metrics reject high-cardinality labels.

Tests:

- Large linear graph.
- Wide fan-out graph.
- Many joins.
- Many waiting nodes.
- Cardinality validation.

### Slice 8.4: Optional PostgreSQL Gate

Scope:

- Add PostgreSQL query-index integration tests for graph run projections.
- Cover run, node, effect, timer, checkpoint, event, and stale-write behavior.
- Keep tests gated by `RAKKA_POSTGRES_TEST_DSN`.

Acceptance:

- PostgreSQL indexes support operational graph queries.
- Stale writes are rejected or fenced.
- Indexes can be rebuilt from durable state.

Tests:

- Graph run projection round trip.
- Waiting node query.
- Failed node query.
- Runtime event projection query.
- Stale write rejection.

### Slice 8.5: Operational Docs

Scope:

- Add follow-up operational docs under
  `docs/plans/compiled_execution_with_graph_schdlr/`.
- Cover graph-specific runbooks, dashboards, alerts, autoscaling signals,
  Kubernetes drain, and compatibility gates.
- Link back to existing agent workflow docs instead of duplicating all
  Kubernetes and OpenTelemetry guidance.

Acceptance:

- Operators can diagnose stuck graph nodes, failed effects, late timers, open
  human checkpoints, event sink failures, and drain blockers.
- Documentation distinguishes product backend incidents from Rakka runtime
  incidents.

## Test Matrix

The implementation should add focused tests for:

- IR validation: duplicate ids, missing node/port refs, invalid branch targets,
  forbidden cycles, invalid loop bounds.
- Scheduler behavior: linear flow, fan-out, fan-in, branch skip propagation,
  bounded loop execution, cancellation, terminal failure.
- Durability: crash after command acceptance, crash after node runnable, crash
  after effect scheduling, recovery after passivation.
- Effect bridge: duplicate effect ids, duplicate callbacks, retry exhaustion,
  idempotency key stability, credential resolver success and failure, and no
  resolved secret persistence.
- Trigger normalization: API, webhook, schedule, and on-demand triggers all
  become durable commands with deduplication.
- Runtime events: stable ordering, no high-cardinality hot metric labels,
  replay/projection safety.

## Documentation Follow-Up Index

Future docs in this directory should use focused files rather than growing this
plan indefinitely:

- `phase-1-compiled-plan-ir.md`
- `phase-2-durable-graph-run-state.md`
- `phase-3-graph-scheduler.md`
- `phase-4-effect-bridge.md`
- `phase-5-trigger-normalization.md`
- `phase-6-runtime-event-stream.md`
- `phase-7-runtime-integration.md`
- `phase-8-production-candidate-gate.md`

These files should be added only as their implementation slices become active.
