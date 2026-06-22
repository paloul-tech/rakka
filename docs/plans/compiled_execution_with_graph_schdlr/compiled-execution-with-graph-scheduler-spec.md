# Rakka Compiled Execution With Graph Scheduler Spec

Status: planning draft
Date: 2026-06-21
Tracking directory: `docs/plans/compiled_execution_with_graph_schdlr/`

## Purpose

This spec defines the next runtime layer for Rakka: a durable interpreter for
compiled workflow execution plans with graph scheduling.

The product direction is intentionally split:

- A separate web application backend owns the visual editor, product DSL,
  compiler, deployments, triggers, credentials, policy, and user-facing APIs.
- Rakka owns the durable execution kernel that receives a compiled,
  product-neutral IR and executes it through durable state, durable inbox,
  durable outbox, timers, checkpoints, sharding, recovery, metrics, and runtime
  events.

Rakka must not become the Langflow/Sim-like application backend. It should
become the reusable runtime that such a backend imports.

## Boundary Decision

The editor DSL and interpreter do not live in Rakka.

Rakka receives a compiled execution plan that has already been validated by
application code and then performs runtime validation before execution. The
compiled plan is a durable runtime contract, not an editor document.

### Owned by the application backend

- Visual graph JSON and UI layout.
- Product DSL, node catalog, compiler, and deployment workflow.
- Workspace, tenant, user, team, billing, and authorization models.
- Trigger registration for API, webhook, schedule, and manual runs.
- Credential binding, secret storage, provider account policy, and quotas.
- Product-specific adapter implementations and prompt/tool policy.
- Run history UI, workflow editor UI, release management UI, and audit UI.

### Owned by Rakka

- Product-neutral compiled execution IR.
- Durable graph run state.
- Deterministic per-run graph scheduler.
- Bridge from runtime nodes to durable `AgentEffect` outbox work.
- Normalized trigger command metadata.
- Runtime event stream records and sink traits.
- Actor-backed and sharded execution of graph runs.
- Recovery, passivation, drain, bounded metrics, tracing context, and
  operational snapshots.

## Non-Goals

- Do not store raw editor DSL or UI layout in Rakka runtime state.
- Do not make Rakka own authentication, authorization, billing, or tenant
  policy.
- Do not make Rakka own trigger registration, public webhook routing, cron
  management, or API gateway behavior.
- Do not store raw credentials, provider tokens, or secret material in Rakka.
- Do not persist resolved credential values in compiled plans, durable graph
  state, durable outbox entries, runtime events, logs, metrics, snapshots, or
  query indexes.
- Do not promise exactly-once external side effects. Rakka provides durable
  intent, recovery, idempotency keys, and retry/compensation hooks.
- Do not support arbitrary cyclic graphs in v1. Iteration must be represented
  by explicit bounded loop or iterator nodes.
- Do not make runtime events the source of correctness. Durable run state and
  durable inbox/outbox state remain the correctness boundary.

## Existing Foundations

The new layer builds on existing `rakka-agent-workflow` and `rakka-workflow`
foundations:

- `AgentWorkflow`, `AgentRunState`, `AgentStep`, `AgentEffect`,
  `HumanCheckpoint`, `ArtifactRef`, and telemetry context.
- `AgentRunInbox` for durable command acceptance.
- Durable outbox scheduling for model, tool, process, HTTP, gRPC, stream,
  artifact, human, child workflow, notification, and audit effects.
- `AgentStepRunner` and `AgentRunActor` as the current durable run host.
- Durable timers, human checkpoint runtime, dispatcher fleet, query indexes,
  audit/log records, metrics, snapshots, OpenTelemetry helpers, and sharding.

The graph scheduler layer should be additive. `rakka-workflow` remains the
lower-level durable inbox/outbox substrate.

## Compiled Execution Plan

The compiled execution plan is the runtime IR that Rakka interprets. It is
created by application code from a product DSL or visual editor graph.

### Required shape

The public API should introduce:

- `AgentCompiledExecutionPlan`
- `AgentCompiledPlanNode`
- `AgentCompiledPlanEdge`
- `AgentCompiledPlanPort`
- `AgentCompiledNodeKind`

An `AgentCompiledExecutionPlan` should include:

- stable plan id;
- workflow id and workflow type;
- definition version;
- plan schema version;
- deterministic plan fingerprint;
- entry node ids;
- node definitions;
- directed edges between output and input ports;
- bounded workflow labels suitable for telemetry;
- optional artifact references for source graph digest, compiled metadata,
  schemas, retry policy, timeout policy, approval policy, and adapter config;
- default concurrency and timeout policy references;
- compatibility metadata for N/N+1 rolling updates.

An `AgentCompiledPlanNode` should include:

- stable node id from the compiled plan;
- product-neutral node kind;
- declared input and output ports;
- optional display name for diagnostics only;
- optional config artifact reference;
- timeout, retry, and concurrency policy references;
- logical target information for effect-producing nodes;
- optional logical credential binding reference for effect-producing nodes that
  need third-party credentials;
- bounded observability labels.

An `AgentCompiledPlanEdge` should include:

- stable edge id;
- source node id and source output port id;
- target node id and target input port id;
- optional edge condition reference for branches;
- optional merge behavior for joins;
- bounded metadata only.

`AgentCompiledPlanPort` should include:

- stable port id;
- port direction;
- payload type name;
- required/optional input flag;
- optional schema artifact reference;
- bounded metadata.

### Product-neutral node kinds

`AgentCompiledNodeKind` should cover runtime behavior categories rather than
product-branded editor blocks:

- input;
- transform;
- branch;
- join;
- iterator;
- model call;
- tool call;
- process call;
- HTTP call;
- gRPC call;
- stream publish;
- artifact write;
- human checkpoint;
- timer wait;
- child workflow command;
- notification;
- audit event;
- terminal.

The application compiler maps product-specific blocks into these runtime
categories. Product-specific configuration belongs in artifact references or
bounded attributes, not in custom Rakka enum variants.

### Runtime node capability discovery

Rakka should expose a runtime capability catalog that lets application backends
discover which compiled node kinds this Rakka runtime understands. This is not
the product editor's node palette.

The public API should introduce:

- `AgentCompiledNodeKindDescriptor`
- `AgentCompiledNodeKindCatalog`
- `AgentCompiledPlanRuntimeCapabilities`

The catalog should describe product-neutral runtime capabilities:

- supported `AgentCompiledNodeKind` values;
- stable kind labels;
- whether a node kind requires a logical target;
- whether a node kind may use a credential binding ref;
- whether a node kind supports config, retry, timeout, concurrency, approval,
  or schema artifact refs;
- expected input and output port policy;
- whether branch, join, or iterator semantics apply;
- required Rakka feature flag or runtime capability, when applicable;
- whether the node kind is available in the current build/configuration.

Application backends can use this catalog to validate compiler output and to
decide which product nodes to expose. Rakka should not expose product-specific
blocks such as "Send Slack Message" or "OpenAI Response"; those remain editor
and compiler concerns that map onto product-neutral runtime node kinds such as
`ToolCall` or `ModelCall`.

### Validation rules

Rakka should validate compiled plans before registration or run start:

- plan id, workflow id, workflow type, definition version, and fingerprint are
  present;
- node ids, edge ids, and port ids are unique within the plan;
- edges reference existing nodes and ports;
- edges connect output ports to input ports;
- all required inputs are reachable from an entry node or a constant/input
  source;
- terminal nodes are reachable;
- no arbitrary graph cycles exist;
- iterator nodes declare explicit max iteration bounds;
- branch nodes have declared branch outputs and skip propagation behavior;
- join nodes declare wait-for-all or wait-for-any behavior;
- effect-producing nodes declare logical targets without raw credentials;
- credential-using nodes declare only logical credential binding refs, never
  raw secret material;
- config, schema, prompt, and payload data use `ArtifactRef` when large or
  sensitive;
- metric labels are bounded and do not contain ids, prompts, completions,
  tool arguments, artifact URIs, or full error text;
- the plan is deterministic when nodes and edges are sorted by stable ids.

### Versioning

Compiled plan compatibility should be explicit:

- plan schema changes must be additive for N/N+1 compatibility;
- old runs must continue using the plan fingerprint they started with;
- a deployment may register multiple definition versions at once;
- application code owns deployment enablement, rollback, and compiler version
  policy;
- Rakka stores enough plan metadata to reject a run if the compiled plan is
  missing, incompatible, or disabled by runtime policy.

## Durable Graph Run State

The graph scheduler needs durable state that is more expressive than the
current single-step cursor.

The public API should introduce:

- `AgentGraphRunState`
- `AgentGraphNodeState`
- `AgentGraphNodeStatus`

`AgentRunState` should gain an additive optional graph state field, using serde
defaults so existing serialized run state remains readable.

### Run-level graph state

`AgentGraphRunState` should include:

- plan id and fingerprint used by the run;
- graph schema version;
- map of node id to `AgentGraphNodeState`;
- selected branch paths;
- skipped node ids and skip reasons;
- active loop or iterator instance records;
- ready queue or ready set derived from persisted state;
- blocked/waiting reason, when no node is runnable;
- run-level output artifact refs;
- scheduler revision or transition counter;
- last emitted runtime event sequence;
- terminal status and terminal reason.

The persisted state should be sufficient to recover after:

- crash after command acceptance;
- crash after node becomes runnable;
- crash after node starts;
- crash after effect is durably scheduled;
- crash after effect callback is accepted but before graph state advances;
- passivation and shard movement.

### Node-level graph state

`AgentGraphNodeState` should include:

- node id;
- status;
- current attempt;
- loop or iterator instance id, when applicable;
- dependency readiness summary;
- started, updated, completed, or skipped timestamps;
- input artifact refs;
- output artifact refs;
- scheduled effect ids;
- open timer id, when waiting for a timer;
- open human checkpoint id, when waiting for a human;
- last bounded error code;
- retry or compensation metadata;
- causation and correlation ids for runtime events.

`AgentGraphNodeStatus` should cover:

- pending;
- runnable;
- running;
- waiting for effect;
- waiting for timer;
- waiting for human;
- completed;
- skipped;
- failed;
- cancelling;
- cancelled.

The current scheduler implementation models graph-node cancellation by moving
unresolved graph nodes directly to `cancelled` when graph cancellation becomes
terminal. Run-level cancellation request or cancelling intent may be represented
outside graph-node status by the host run runtime.

### State principles

- Durable graph state is the source of truth for scheduler recovery.
- Runtime events are projections emitted after state persistence succeeds.
- Large payloads, prompts, completions, files, and tool outputs should be
  artifact references.
- Node ids and edge ids may be in traces, logs, audit records, snapshots, and
  query indexes, but not hot metric labels.
- The scheduler may recompute ready nodes from durable graph state instead of
  trusting an in-memory queue.

## Graph Scheduler

The public API should introduce:

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
- `AgentGraphRuntime`

The graph scheduler is a deterministic per-run component that evaluates a
compiled execution plan against durable graph run state.

### Responsibilities

The scheduler should:

- initialize graph state from a compiled plan and start command;
- compute runnable nodes when dependencies are satisfied;
- persist node state transitions before executing work;
- dispatch pure nodes locally and effect nodes through durable outbox or
  specialized durable runtime services;
- handle fan-out by marking all eligible downstream nodes runnable;
- handle fan-in by waiting for required upstream nodes;
- handle branch decisions and skip propagation;
- handle explicit bounded iterator nodes;
- handle terminal success when terminal conditions are satisfied;
- handle terminal failure, cancellation, and compensation policy;
- recover from persisted state after restart, passivation, or shard movement;
- emit runtime events after successful persistence.

### Determinism

Given the same compiled plan, durable graph state, command, and timestamp, the
scheduler should produce the same transition result.

Determinism requirements:

- stable sorting by node id and edge id;
- no dependency on hash map iteration order;
- explicit time input from `WorkflowClock` or command metadata;
- deterministic ids for effects, timers, checkpoints, and runtime events;
- persisted transition counters for event ordering;
- no hidden live task state required for recovery.

### Parallelism

The scheduler should expose bounded parallelism by making multiple nodes
runnable while keeping per-run state transitions serialized through the run
actor or sharded entity.

Execution may use dispatcher workers for external effects, but graph state
advancement remains serialized per run. This prevents concurrent transitions
from corrupting dependency state.

### Branches and skips

Branch nodes choose one or more outgoing branch edges. Downstream nodes on
unselected branches should be marked skipped when they cannot otherwise become
reachable. Joins must declare whether skipped upstreams count as satisfied.
The branch condition interpreter remains outside the scheduler core; Rakka
persists the selected outgoing edge ids supplied by the runtime before any
selected downstream node is made runnable.

### Loops

Arbitrary cycles are rejected in v1.

Iteration is represented with explicit bounded loop or iterator nodes. Each
iteration should have a stable loop instance id and max iteration bound. The
scheduler must persist the current iteration before scheduling work for that
iteration.
In the first scheduler implementation, loop instance identity is represented by
the iterator node id plus deterministic zero-based iteration index, stored in
scoped durable loop state. The runtime that owns item discovery and loop body
interpretation asks the scheduler to start or complete each iteration; the
scheduler enforces the declared bound and recovers the active iteration from
durable state.

### Cancellation And Terminal Policy

Graph cancellation should stop scheduling new nodes and durably mark unresolved
pending, runnable, running, and waiting nodes as cancelled. Completed, skipped,
failed, and terminal nodes keep their existing terminal status. Unresolved loop
instances should also be cancelled.

Once graph terminal status is set, the scheduler should report no runnable nodes.
Terminal success cancels leftover unresolved parallel work. Terminal failure
marks the failing node failed, marks the graph failed, and cancels the remaining
unresolved work. Repeated cancellation of an already-cancelled graph is
idempotent.

Compensation hooks are a later runtime concern. The scheduler preserves durable
terminal, failed, and cancelled state where compensation orchestration can attach,
but it does not execute compensation workflows in the Phase 3 scheduler core.

## Effect Bridge

The effect bridge maps graph nodes to existing durable workflow mechanisms.

For durable outbox effects, `AgentGraphEffectBridge` maps effect-producing
compiled plan nodes to `AgentEffect` values and schedules them through
`AgentRunInbox::schedule_effect`. The graph node should move to waiting for an
effect only after the durable outbox boundary returns scheduled or duplicate
acceptance.

The bridge should convert effect-producing nodes into:

- `AgentEffect` entries scheduled through durable outbox;
- timer entries for timer wait nodes;
- human checkpoint openings for human checkpoint nodes;
- child workflow start or signal commands for child workflow nodes;
- audit events for audit nodes.

## Credential References And Secret Resolution

Third-party API credentials are application-owned secret material. They should
be stored in an application credential service or external secret manager, not
in the compiled execution plan, durable graph run state, durable outbox entries,
runtime events, logs, metrics, snapshots, or query indexes.

The public API should introduce:

- `AgentCredentialBindingRef`
- `AgentCredentialResolver`
- `AgentCredentialResolverFuture`
- `AgentCredentialResolutionRequest`
- `AgentCredentialResult`
- `AgentCredentialError`
- `AgentCredentialUse`
- `AgentEphemeralCredential`
- `AgentEphemeralCredentialMaterial`

`AgentCredentialBindingRef` should be a stable logical reference supplied by
the application backend. It may identify a tenant-scoped credential binding,
provider account, OAuth connection, or secret alias, but it must not contain the
credential value.

Example runtime intent:

```json
{
  "node_id": "send-slack-message",
  "kind": "tool-call",
  "target": {
    "target_type": "tool",
    "name": "slack.chat.postMessage",
    "credential_binding_ref": "cred_binding_123"
  }
}
```

The application backend remains responsible for:

- credential UX and OAuth flows;
- encrypted secret storage;
- tenant authorization checks;
- credential scopes;
- provider account policy;
- rotation and revocation;
- audit of secret access;
- mapping credential bindings to secret-manager records.

Rakka should only persist logical credential binding refs and bounded provider
metadata. During dispatch, an application-provided `AgentCredentialResolver`
can resolve a binding ref into an `AgentEphemeralCredential` for one dispatch
attempt or a short-lived time window.

`AgentCredentialResolutionRequest` should be bounded metadata only. It may
carry tenant, workflow id, run id, compiled plan fingerprint, node id, logical
target, credential binding ref, requested credential use, causation id,
correlation id, and trace context. It must not carry credential values.

`AgentEphemeralCredential` and `AgentEphemeralCredentialMaterial` should be
in-memory dispatch values, not durable data contracts. They should not derive
serde serialization, and diagnostic formatting should redact secret material.

`AgentCredentialError` should expose stable bounded error codes and classify
failures as retryable or permanent using the existing adapter failure
classification language. Resolver unavailability is retryable; missing,
revoked, unauthorized, invalid-use, and malformed requests are permanent unless
a later explicit effect policy overrides handling.

Recommended resolver inputs:

- tenant;
- workflow id;
- run id;
- plan fingerprint;
- node id;
- target type and target name;
- credential binding ref;
- requested credential use;
- causation id and correlation id;
- trace context.

Resolver outputs should be short-lived and in-memory only. Rakka must not:

- serialize resolved secrets into durable state;
- include credential values in `AgentEffect` payloads;
- log credential values;
- expose credential values in metrics, runtime events, snapshots, or query
  indexes;
- retain resolved credentials after the dispatch attempt finishes.

Credential rotation and revocation should not require recompiling a workflow.
A compiled plan can continue to reference the same binding ref while the
application credential service decides which secret version is current and
whether the run is still authorized to use it.

### Effect identity

Effect ids, deduplication keys, and idempotency keys must be deterministic.

Recommended inputs:

- workflow id;
- definition version;
- plan fingerprint;
- run id;
- node id;
- loop instance id, if present;
- logical effect ordinal;
- effect kind and target class.

Retries should reuse the same logical outbox entry when possible. External
targets should receive stable idempotency keys for the logical side effect.

### Completion path

External completion, failure, timeout, and cancellation results should return
to the run through durable inbox commands:

- `EffectCompleted`;
- `EffectFailed`;
- `TimerFired`;
- `HumanDecisionSubmitted`;
- `SubmitSignal` for product-specific external events.

The scheduler should only advance graph state after the completion command has
been durably accepted and the resulting transition has been persisted.

For graph effect nodes, the bridge should apply accepted `EffectCompleted`
commands by moving the waiting node to completed. The completion command
`payload_ref` is the result artifact reference for the effect node and should
be persisted to the node output before downstream nodes become runnable.

Accepted `EffectFailed` commands should carry a bounded failure disposition:
retry-scheduled, exhausted, or terminal. Retry-scheduled failures keep the node
waiting and record a bounded error code. Exhausted or terminal failures mark
the node failed, fail the graph, and cancel unresolved downstream work.

Duplicate completion or failure commands must be idempotent. If durable inbox
acceptance succeeded before a crash but graph state did not advance, recovery
may see duplicate command acceptance and should still apply the transition.
If graph state already advanced, duplicate callbacks should produce no graph
state changes.

### Adapter policy

Rakka defines runtime contracts and dispatch boundaries. Application code owns:

- concrete provider adapters;
- credentials and account binding;
- credential storage, resolution, rotation, revocation, and access audit;
- model selection;
- prompt policy;
- tool authorization;
- quota and cost controls.

## Trigger Normalization

The public API should introduce:

- `AgentTriggerSource`

Rakka should not own trigger registration. Instead, application ingress code
normalizes trigger executions into Rakka commands.

Trigger source categories:

- API;
- webhook;
- schedule;
- on demand;
- system;
- child workflow;
- external callback;
- human decision.

`AgentTriggerSource` should be bounded metadata attached to `AgentCommand`
attributes or command metadata. It should include stable low-cardinality labels
such as trigger kind, deployment channel, and tenant tier, but not raw webhook
URLs, signatures, tokens, request bodies, or user ids as metric labels.

### Command mapping

Recommended mappings:

- API run request -> `StartRun`;
- manual editor run -> `StartRun`;
- scheduled fire -> `StartRun`;
- webhook event -> `StartRun` or `SubmitSignal`;
- external callback -> `SubmitSignal` or effect completion command;
- human approval -> `HumanDecisionSubmitted`;
- cancellation request -> `CancelRun`;
- retry request -> `RetryRun`.

Every trigger-derived command needs:

- command id;
- deduplication key;
- causation id;
- correlation id;
- tenant;
- principal, when known;
- received timestamp;
- optional payload artifact ref.

## Runtime Event Stream

The public API should introduce:

- `AgentRuntimeEvent`
- `AgentRuntimeEventSink`

Runtime events are emitted after durable state transitions succeed. They help
applications build run-history views, live execution streams, logs, audit
correlation, and operational projections.

### Event categories

Runtime events should cover:

- run accepted;
- run started;
- node became runnable;
- node started;
- node completed;
- node skipped;
- node failed;
- effect scheduled;
- effect completed;
- effect failed;
- timer scheduled;
- timer fired;
- human checkpoint opened;
- human decision accepted;
- branch selected;
- loop iteration started;
- loop iteration completed;
- run waiting;
- run resumed;
- run cancelled;
- run completed;
- run failed.

### Event ordering

Each event should include:

- run id;
- workflow id;
- definition version;
- plan fingerprint;
- scheduler revision;
- event sequence;
- event timestamp;
- event kind;
- optional node id;
- optional effect id;
- optional timer id;
- optional checkpoint id;
- causation id;
- correlation id;
- trace context;
- bounded attributes.

Events should be ordered by per-run event sequence. Cross-run global ordering is
not required in v1.

### Event sink behavior

`AgentRuntimeEventSink` should support in-memory tests and application-provided
durable projections. A sink failure should be visible, but it must not be
allowed to create a false state transition. The implementation plan should
decide whether event sink writes are best-effort after persistence or part of a
separate durable outbox/audit path for stronger projection guarantees.

## Observability And Query Model

Metrics should use bounded labels only:

- workflow type;
- definition version;
- node kind;
- node status;
- event kind;
- effect kind;
- target class;
- trigger kind;
- outcome;
- error code;
- tenant tier.

High-cardinality fields should remain in traces, logs, audit records,
snapshots, and query indexes:

- workflow id;
- run id;
- plan id;
- node id;
- edge id;
- effect id;
- timer id;
- checkpoint id;
- command id;
- correlation id;
- artifact id.

Query projections should support:

- runs by status;
- runs waiting for effect, timer, or human input;
- failed nodes by bounded node kind and error code;
- stuck effects and stale callbacks;
- due timers;
- open human checkpoints;
- active plan fingerprints;
- shard ownership for graph runs.

## Security And Policy

Rakka must treat compiled plans and payload artifacts as untrusted application
input until validated.

Runtime validation should reject:

- raw credentials;
- resolved credential values in plans, graph state, outbox payloads, events,
  logs, snapshots, or query projections;
- credential binding refs that are blank, malformed, or placed in hot metric
  labels;
- raw webhook URLs intended for metric labels;
- unbounded inline prompts or completions;
- arbitrary command execution;
- nodes without logical adapter targets;
- plans with forbidden cycles;
- loop nodes without bounds;
- missing idempotency metadata for effect nodes.

The application and platform remain responsible for:

- identity provider integration;
- authorization decisions;
- secret management;
- credential service or vault integration;
- credential rotation and revocation;
- provider account policy;
- network egress controls;
- object-store encryption and retention;
- legal/compliance retention;
- billing and cost controls.

## Compatibility

The implementation should preserve existing `rakka-agent-workflow` serialized
contracts:

- new fields in existing serialized structs must use serde defaults;
- existing runs without graph state must still recover;
- compiled plan schema changes should be additive across N/N+1 rolling updates;
- runtime events should be versioned;
- query index migrations should be reversible or rebuildable from durable run
  state;
- optional features such as `postgres`, `sharding`, `http`, `k8s`, and
  `testkit` should remain additive.

## Acceptance Summary

This effort is successful when Rakka can:

- register or receive a compiled execution plan;
- start a durable graph run from a normalized trigger command;
- execute linear, fan-out, fan-in, branch, and bounded iterator workflows;
- schedule external side effects through durable outbox;
- pause for timers and human checkpoints without live waits;
- recover after crash, passivation, and shard movement;
- emit ordered runtime events after persisted transitions;
- expose bounded metrics, snapshots, and query projections;
- keep the editor DSL and product backend outside Rakka.
