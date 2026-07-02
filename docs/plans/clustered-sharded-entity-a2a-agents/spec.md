# Clustered Sharded Entity A2A Agents Spec

Status: planning draft
Date: 2026-07-02
Primary SDK source: https://github.com/a2aproject/a2a-rs

## Purpose

This spec evaluates whether Rakka's clustered sharded entities are a good fit
for long-running, durable, autonomous agents exposed over the Agent-to-Agent
(A2A) protocol, and defines an implementation shape that uses Rakka's current
public APIs and the A2A Rust SDK.

The short answer is yes, with a clear boundary: Rakka sharded entities are a
good fit for stable agent-run identity, single-writer ownership, cluster routing,
passivation, shard movement, and recovery activation. They are not, by
themselves, the durability boundary for autonomous agents. Durable correctness
must continue to come from Rakka's agent workflow state, durable inbox/outbox,
idempotency keys, PostgreSQL-backed state, recovery, query projections, and
dispatcher retry policy.

## Fit Assessment

Rakka clustered sharded entities fit the A2A agent problem when an A2A task is
modeled as a durable Rakka agent run:

- A2A `Task.id` maps to `AgentRunId` and the sharded entity id.
- The sharded entity is the live owner for one run and serializes local runtime
  commands.
- The durable run state, durable inbox, durable outbox, timers, artifacts,
  checkpoints, and query projections remain the source of truth.
- A2A ingress can run on every node; non-owning nodes route work to the owner
  through Rakka sharding/remoting.
- Passivation and shard movement are availability mechanisms, not correctness
  mechanisms.

This is the right shape for long-running autonomous agents because agent runs
spend most of their lifetime waiting for model calls, tools, timers, other
agents, or humans. Those waits should not occupy a live async task, stream, or
mailbox. The live sharded entity should wake up to accept commands, persist
state transitions, schedule durable effects, publish bounded snapshots, and then
be free to passivate.

The fit is poor if the desired model is an always-hot in-memory agent loop with
unbounded context, unbounded SSE streams, non-idempotent side effects, or
correctness tied to a specific pod. Those patterns bypass the Rakka primitives
that make crash recovery and scale-out viable.

## Goals

- Expose durable autonomous agents through A2A REST/HTTP+JSON and JSON-RPC
  bindings using `a2a-server`.
- Keep A2A task identity stable across node restarts, shard movement, pod
  rescheduling, and client retries.
- Use Rakka sharded entities as the live ownership and routing layer for active
  agent runs.
- Use Rakka durable inbox acceptance before acknowledging externally submitted
  work.
- Use Rakka durable outbox effects before executing model calls, tool calls,
  callbacks, A2A calls to peer agents, push notifications, or other side
  effects.
- Provide A2A task reads, listing, cancellation, streaming, and push
  notification behavior from durable projections.
- Preserve Rakka's reliability boundary: actor, remote, and sharding delivery
  remain at-most-once; durable behavior is opt-in through the workflow layer.

## Non-Goals

- Do not turn Rakka remoting into a public A2A transport.
- Do not promise exactly-once execution of arbitrary external side effects.
- Do not require one live actor per known task forever.
- Do not keep long-running model/tool/human waits inside an in-memory
  `AgentExecutor` task.
- Do not require per-agent pods or sidecars.
- Do not replace application-owned model selection, prompts, authorization,
  tool policy, or agent planning logic.
- Do not implement the effort in this planning slice.

## Current API Inventory

### A2A Rust SDK

The A2A Rust SDK is a Rust workspace for A2A v1. The repository currently
contains these relevant crates:

- `a2a` (`a2a-lf` package): core A2A types, including `AgentCard`,
  `AgentInterface`, `AgentCapabilities`, `AgentSkill`, `Message`, `Part`,
  `Task`, `TaskStatus`, `TaskState`, `SendMessageRequest`,
  `SendMessageResponse`, `GetTaskRequest`, `ListTasksRequest`,
  `CancelTaskRequest`, `SubscribeToTaskRequest`, push notification config
  types, stream events, and protocol errors.
- `a2a-server` (`a2a-server-lf` package): async server framework with
  `RequestHandler`, `DefaultRequestHandler`, `AgentExecutor`,
  `ExecutorContext`, `TaskStore`, `InMemoryTaskStore`, push config stores,
  `agent_card_router`, REST routes, JSON-RPC routes, SSE helpers, and axum
  integration.
- `a2a-client`: client transport abstraction and factory for resolving agent
  cards and selecting bindings.
- `a2a-grpc`: tonic-based gRPC client/server binding.
- `a2a-pb`: protobuf schema and conversion layer.
- `a2a-slimrpc`: SLIMRPC binding.
- `a2acli`: CLI useful for compatibility and smoke tests.

The SDK's `RequestHandler` is the best integration point for Rakka. Its methods
cover:

- `send_message`
- `send_streaming_message`
- `get_task`
- `list_tasks`
- `cancel_task`
- `subscribe_to_task`
- push notification config create/get/list/delete
- `get_extended_agent_card`

The SDK's `DefaultRequestHandler` is useful as a reference and for simple
examples, but it starts active executions in an in-memory execution manager.
Durable Rakka agents should implement a custom `RequestHandler` so acceptance,
task status, streaming, and push behavior are driven by Rakka durable state.

### Rakka

Rakka already has the major substrate APIs needed for this effort:

- `rakka::agent_workflow`: `AgentRunState`, `AgentWorkflow`,
  `AgentCommand`, `AgentCommandKind`, `AgentDurabilityMetadata`,
  `AgentRunActor`, `AgentRunActorCommand`, `AgentRunInbox`,
  `AgentStepRunner`, `AgentGraphRuntime`, compiled graph execution types,
  timers, effect bridge, dispatcher, query models, snapshots, retention, audit,
  and Kubernetes startup helpers.
- `rakka::agent_workflow::sharding`: `AgentRunShardingSettings`,
  `agent_run_entity_type_key`, `init_agent_run_sharding`,
  `init_agent_run_sharding_with_metrics`,
  `init_agent_run_sharding_with_clock_and_metrics`,
  `agent_run_entity_ref`, `registered_agent_run_entity_ref`,
  `passivate_agent_run`, and `forget_agent_run`.
- `rakka::sharding`: `ClusterSharding`, `Entity`, `EntityContext`,
  `EntityTypeKey`, `EntityTypeRegistration`, `ShardedEntityRef`,
  `ShardBufferConfig`, `RememberedEntities`, shard allocation strategies,
  passivation, and cluster node runtime integration.
- `rakka::sharding::ClusterNodeRuntimeBuilder`: networked cluster node runtime
  setup with membership, TCP remoting, serialization registry, shard coordinator
  stores, and coordinator leases.
- `rakka::remote`: trusted cluster remoting and serialization registry used for
  inter-node entity routing.
- `rakka::persistence` and `rakka-persistence-postgres`: durable state,
  event journal, snapshots, revision fencing, and PostgreSQL persistence.
- `rakka-sharding-postgres`: PostgreSQL shard coordinator state, coordinator
  lease, and remembered entity stores.
- `rakka::k8s`, `rakka::http`, and `rakka::grpc`: operational probes,
  shutdown/drain integration, public HTTP/gRPC adapters, and snapshots.

The existing clustered agent workflow example is especially relevant. It
registers a serializable `RunRequest` with `ClusterSharding::init_remote_with_ask`
and maps that remote-safe request to local `AgentRunActorCommand` messages on
the owner. This matters because `AgentRunActorCommand` carries process-local
values such as reply channels and `Arc<AgentCompiledExecutionPlan>`; it should
not be used directly as the public inter-node wire protocol.

## Proposed Architecture

### Component Overview

The effort should add an A2A adapter layer around the existing agent workflow
runtime. The preferred production shape is:

```text
A2A client
  -> A2A REST or JSON-RPC route from a2a-server
  -> RakkaA2ARequestHandler
  -> durable command normalization
  -> sharded run request routed by ClusterSharding
  -> owning RunEntity
  -> local AgentRunActor
  -> DurableStateStore / DurableInbox / DurableOutbox
  -> dispatcher, timers, model/tool/A2A effects, query projections
```

The new adapter can live in a new additive crate such as `rakka-a2a`, or in an
example first if the public API should stay experimental. The crate should be
feature-gated behind A2A SDK dependencies and re-exported by the top-level
`rakka` facade only after the API has stabilized.

### Public A2A Surface

Each Rakka A2A agent service exposes:

- `/.well-known/agent-card.json` through `a2a_server::agent_card_router`.
- A2A REST/HTTP+JSON through `a2a_server::rest::rest_router`.
- A2A JSON-RPC through the SDK JSON-RPC router.
- Optional gRPC through `a2a-grpc` after the REST/JSON-RPC path is stable.

The agent card should be produced by a dynamic `AgentCardProducer` that includes:

- service name, description, version, provider, documentation URL, and icon URL;
- supported interfaces for REST and JSON-RPC, and optionally gRPC;
- `AgentCapabilities` with `streaming: true`, `push_notifications: true` once
  durable push configs are implemented, and `extended_agent_card: true` if the
  service exposes tenant- or auth-specific cards;
- `AgentSkill` entries derived from registered `AgentWorkflow` definitions;
- security schemes and requirements reflecting the application's public auth.

### A2A Task to Rakka Run Mapping

Use one stable id across all layers:

| A2A concept | Rakka concept |
| --- | --- |
| `Task.id` | `AgentRunId` and sharded entity id |
| `Message.message_id` | `AgentCommandId` and durable inbox message id |
| `Message.task_id` | target `AgentRunId` when continuing an existing run |
| `Message.context_id` | conversation or correlation grouping |
| `SendMessageRequest.tenant` | `AgentTenantId` |
| `SendMessageRequest.metadata` | workflow selection, auth-derived principal, trace context, and adapter extension data |
| `Part::Text` | small inline user input or artifact reference producer |
| `Part::Raw`, `Part::Url`, `Part::Data` | artifact reference, tool input, or application payload |
| `Task.artifacts` | bounded projection of Rakka `ArtifactRef` records |
| `Task.history` | bounded message projection controlled by A2A `history_length` |
| `Task.metadata` | low-cardinality run metadata and links to operational/audit data |

If the incoming message has no `task_id`, the adapter creates a new task/run id.
It may use A2A's UUIDv7 helpers for wire compatibility, but the resulting value
must be wrapped as `AgentRunId` and used as the sharded entity id. If the message
does include `task_id`, the adapter treats it as a command against an existing
run and rejects mismatched tenant/workflow metadata.

### Status Mapping

The A2A task state is a projection of durable Rakka state:

| Rakka state | A2A task state |
| --- | --- |
| accepted but not yet running | `TASK_STATE_SUBMITTED` |
| running, dispatching, retrying, compensating | `TASK_STATE_WORKING` |
| waiting for human input or agent/user signal | `TASK_STATE_INPUT_REQUIRED` |
| waiting for auth reauthorization | `TASK_STATE_AUTH_REQUIRED` |
| completed | `TASK_STATE_COMPLETED` |
| failed or exhausted | `TASK_STATE_FAILED` |
| cancellation requested but not final | `TASK_STATE_WORKING` with cancellation metadata |
| cancelled | `TASK_STATE_CANCELED` |
| rejected before durable acceptance | `TASK_STATE_REJECTED` |

Terminal A2A states must only be emitted after the corresponding durable Rakka
state is persisted.

### Sharded Entity Model

The sharded entity should be a lightweight run host, not the full durable
business object. It owns:

- the stable `AgentRunId`;
- a local child `AgentRunActor`;
- routing from remote-safe A2A run requests to local actor commands;
- bounded ask/reply behavior for command acceptance, task snapshots, and short
  drive steps;
- passivation and local cleanup.

Use the existing APIs in one of two ways:

1. For local-only or single-node tests, use `init_agent_run_sharding` and
   `registered_agent_run_entity_ref`.
2. For a real networked cluster, use `ClusterNodeRuntimeBuilder`,
   `ClusterSharding::for_node_runtime`, and
   `ClusterSharding::init_remote_with_ask` with an adapter-defined,
   serializable request enum such as `A2ARunRequest`.

The remote-safe request enum should include commands like:

- `AcceptMessage`
- `CancelTask`
- `QueryTask`
- `OpenSubscription`
- `RecordPushConfig`
- `DeletePushConfig`

The owning entity maps those requests to local `AgentRunActorCommand` values:

- `AcceptCommand`
- `Start` or `StartGraph`
- `RequestCancellation`
- `Cancel`
- `Snapshot`
- `ScheduleEffect`
- `DueEffects`

This mirrors the existing clustered agent workflow example and avoids trying to
serialize process-local actor messages over the remote boundary.

### Clustered Streaming and Load-Balanced Ingress

A2A streaming connections are public transport subscriptions. They are not the
ownership boundary for an agent run. In a multi-node deployment, the node that
holds the client HTTP/SSE connection may be different from the node that owns
the sharded run entity.

When a client calls `message:stream` on node A for a task owned by node B, node
A should:

1. authenticate the request and normalize the A2A task/message metadata;
2. durably accept or route durable acceptance to the owner;
3. route write/drive commands to the owner through sharding;
4. return an SSE stream to the client from node A;
5. emit a current task snapshot first, then task/status/artifact updates.

The preferred implementation streams from durable task events or query
projections. The owner persists run state and runtime events; any public node
can read those durable projections and serve `send_streaming_message` or
`subscribe_to_task`. This lets a client reconnect through a load balancer to a
different node and resume from the same `Task.id`.

A simpler first implementation may proxy a live owner stream:

```text
client -> node A public SSE
node A -> node B owner subscription over Rakka remoting
node B -> node A -> client
```

That proxy shape is acceptable for a first slice only if disconnects are
treated as normal transport loss. A node A restart, node B restart, owner
movement, shard handoff, network interruption, or load-balancer timeout may end
the stream, but it must not cancel the durable run. The client can reconnect to
any public node and recover through `get_task` or `subscribe_to_task`.

For deployments behind a load balancer:

- publish the load balancer URL, not pod-local URLs, in the public
  `AgentCard.supported_interfaces`;
- assume each HTTP/SSE request can land on any healthy Rakka node;
- do not require sticky sessions for correctness;
- allow sticky sessions as an optimization for long-lived streams if the
  operator wants fewer stream reconnects;
- set load-balancer and ingress idle/read timeouts high enough for expected A2A
  stream duration, or send periodic heartbeat/status events;
- make reconnect and replay part of the client contract by using stable
  `Task.id`, `context_id`, and bounded event/history replay;
- ensure every public node can authenticate, authorize, route, and project tasks
  for the same tenants;
- keep Rakka remoting private to the cluster and expose only the load-balanced
  A2A endpoint publicly.

### Durable Request Handling

Implement `RakkaA2ARequestHandler` as a custom `a2a_server::RequestHandler`.

`send_message` flow:

1. Authenticate and derive tenant/principal from SDK `ServiceParams`.
2. Normalize `Task.id`, `context_id`, workflow id/type/version, command id,
   causation id, correlation id, and trace context.
3. Convert the A2A message to an `AgentCommand`.
4. Route `A2ARunRequest::AcceptMessage` to the owning sharded entity.
5. The owner calls `AgentRunActorCommand::AcceptCommand` to durably record the
   command in the inbox before the adapter acknowledges the A2A request.
6. If this is a new run, persist the initial `AgentRunState` with `Start` or
   `StartGraph`.
7. Return a durable `Task` projection. If `return_immediately` is true, return
   after durable acceptance. Otherwise wait only for a bounded first transition
   or terminal result; never block for an unbounded autonomous run.

`send_streaming_message` flow:

1. Perform the same durable acceptance path as `send_message`.
2. Return an SSE stream that first emits the current durable task snapshot.
3. Emit subsequent task/status/artifact events from durable runtime events or a
   projection watcher.
4. On disconnect, keep the run alive through durable state; clients can resume
   with `subscribe_to_task` or `get_task`.

`get_task` flow:

- Read from the durable task projection/query index.
- Optionally ask the owning entity for a fresh bounded snapshot if the owner is
  reachable.
- Respect A2A `history_length` and `include_artifacts` controls to avoid
  returning unbounded histories.

`list_tasks` flow:

- Query the durable Rakka agent workflow index.
- Filter by tenant, context id, status, status timestamp, and page token.
- Do not fan out to all live actors.

`cancel_task` flow:

- Normalize to an `AgentCommandKind::CancelRun` or direct cancellation request.
- Durably accept the cancellation command.
- Route to the owning entity and persist `RequestCancellation`; persist
  `Cancel` when cancellation is complete.
- Return the latest durable task projection.

`subscribe_to_task` flow:

- Stream durable task projection updates for the task id.
- Send a snapshot first, then incremental updates.
- If no live owner exists, recover from the durable projection instead of
  failing solely because no actor is active.

Push notification config flows:

- Store A2A `TaskPushNotificationConfig` in a durable store keyed by tenant and
  task id.
- Dispatch push notifications from Rakka durable outbox effects.
- Include idempotency keys and retry policy; do not send push notifications as
  an in-memory side effect of an HTTP handler.

`get_extended_agent_card` flow:

- Return an `AgentCard` enriched with tenant-specific skills, auth
  requirements, or deployment metadata after checking authorization.

### Autonomous Execution Model

Autonomy should be represented as a durable loop of state transitions and
effects:

1. The run records a planning or continuation state.
2. The run schedules one or more effects through the durable outbox:
   model calls, tool calls, process calls, A2A calls to peer agents, timers, or
   human checkpoint notifications.
3. Dispatcher workers claim due effects and execute them with idempotency keys.
4. Results return as durable commands such as `EffectCompleted`,
   `EffectFailed`, `TimerFired`, `HumanDecisionSubmitted`, or `SubmitSignal`.
5. The owning sharded entity resumes the run, persists the next transition, and
   schedules more effects or terminates.

This makes "long-running" mean long-lived durable intent, not one long-lived
thread.

### Persistence and Cluster Topology

For production:

- Use `rakka-persistence-postgres` for `AgentRunState`, workflow inbox/outbox
  state, event journal, and snapshots.
- Use durable query projections for A2A task list/read operations.
- Use artifact references for large prompts, model responses, files, and tool
  outputs.
- Use etcd discovery plus Rakka self-fencing for Kubernetes-style dynamic
  membership unless the deployment intentionally adopts a single-coordinator
  shard topology.
- Treat `rakka-sharding-postgres` coordinator state, leases, and remembered
  entities as available APIs, but do not require the PostgreSQL shard
  coordinator for the first A2A cluster shape. The existing clustered agent
  workflow example uses external-arbiter discovery and deterministic ownership
  for symmetric per-node hosting.
- Use remembered entities only for runs that must be eagerly reactivated after
  ownership changes. Most completed or idle tasks should recover lazily on
  reference and be eligible for passivation.

### Reliability Model

Accepted external work is durable only after the inbox write succeeds. Client
retries must use the same A2A `message_id` or adapter-provided idempotency key
so duplicates can return the existing task projection.

External side effects are at-least-once unless the target system participates in
idempotency. Every model call, tool call, A2A peer call, webhook, and push
notification needs an idempotency key derived from Rakka effect metadata.

Failure behavior:

- Crash before durable acceptance: the client may retry; no task is promised.
- Crash after durable acceptance before HTTP response: retry returns the same
  task/run from the durable inbox and projection.
- Crash after scheduling an effect before dispatch: recovery finds the due
  outbox effect.
- Crash during effect dispatch: retry policy and idempotency keys determine the
  external outcome.
- Owner pod exits: membership eventually removes it, shard ownership moves, and
  the next owner recovers from durable state.
- Shard handoff or passivation: bounded buffers may improve availability, but
  correctness still comes from the durable inbox/outbox and run state.
- SSE disconnect: the run continues durably; clients call `subscribe_to_task` or
  `get_task`.
- Load balancer routes reconnect to a different node: the new node serves the
  current durable task projection and resumes streaming from durable events when
  replay is available.
- Network partition: use Rakka discovery/self-fencing policy. Do not claim
  exactly-once behavior across split-brain conditions.

### Security and Tenancy

The A2A adapter must treat tenant and principal as part of the durable command
boundary:

- derive tenant from authenticated context or `SendMessageRequest.tenant`;
- reject task access when tenant does not match the run owner tenant;
- persist principal references in `AgentCommandMetadata`;
- include auth scopes in `AgentCard.security_schemes` and
  `security_requirements`;
- avoid placing secrets, prompts, task ids, full error text, or user content in
  hot metric labels;
- propagate trace context from A2A headers/metadata into
  `AgentTelemetryContext`.

### Observability

The adapter should emit:

- A2A ingress metrics: requests, latency, error codes, status mapping, and
  streaming disconnects.
- Durable acceptance metrics: accepted, duplicate, rejected, and conflict
  counts.
- Sharding metrics: owner resolution, local/remote route, route failures,
  handoff buffering, and passivation.
- Runtime metrics from existing `rakka-agent-workflow` metrics and snapshots.
- OpenTelemetry trace context across A2A ingress, durable commands, outbox
  effects, dispatcher attempts, A2A peer calls, and callbacks.

Operational snapshots should include:

- task/run counts by state;
- sampled active runs;
- due effect count;
- subscription counts;
- push notification retry counts;
- shard ownership and buffered message counts.

## Implementation Plan

### Phase 0: API Lock and Spike

- Pin the A2A Rust SDK dependency source and version policy.
- Start as a runnable example with module boundaries that can later be
  extracted into `rakka-a2a`.
- Add a small design note for crate features and top-level `rakka` re-exports.
- Verify the SDK REST and JSON-RPC routers can be mounted beside existing Rakka
  HTTP routes without path conflicts.

Acceptance:

- No production behavior yet.
- A sample `AgentCard` can be served from `/.well-known/agent-card.json`.

### Phase 1: Type Mapping and Task Projection

- Implement conversion between A2A `Task`/`Message`/`Artifact` and Rakka
  `AgentRunState`/`AgentCommand`/`ArtifactRef`.
- Define the `io.rakka.*` metadata namespace for workflow selection, principal,
  idempotency, adapter version, and trace-context fallback.
- Add a durable A2A task projection backed by the existing agent workflow query
  model and an A2A task event projection.

Acceptance:

- Completed, failed, cancelled, input-required, and working Rakka runs project
  to valid A2A tasks.
- `history_length` and artifact inclusion are bounded.

### Phase 2: Durable A2A Request Handler

- Implement `RakkaA2ARequestHandler`.
- Support `send_message`, `get_task`, `list_tasks`, and `cancel_task`.
- Persist command acceptance before A2A acknowledgement.
- Return duplicate results for repeated message ids or deduplication keys.

Acceptance:

- Process crash after durable acceptance but before response can be retried
  without creating a duplicate run.
- `a2acli send`, `a2acli list-tasks`, and `a2acli cancel` work against one
  node.

### Phase 3: Clustered Sharded Run Host

- Define a remote-safe `A2ARunRequest`/`A2ARunResponse` protocol.
- Register a run-host entity with `ClusterSharding::init_remote_with_ask`.
- Map owner-local requests to `AgentRunActorCommand`.
- Use `ClusterNodeRuntimeBuilder` and the existing Rakka serialization registry
  for inter-node routing.

Acceptance:

- Any node can accept an A2A request for any task id.
- Only the owner drives the local `AgentRunActor`.
- Owner restart or shard movement recovers from durable state on next access.

### Phase 4: Streaming and Push

- Implement `send_streaming_message` and `subscribe_to_task` from the durable
  A2A task event projection.
- Add a durable push notification config store.
- Schedule push sends through Rakka durable outbox effects.

Acceptance:

- A streaming client receives a current snapshot followed by updates.
- Disconnecting the stream does not affect run execution.
- Push notifications are retried durably and deduplicated by effect key.

### Phase 5: Autonomous Effect Loop

- Wire model/tool/A2A-peer calls as durable outbox effects.
- Use existing dispatcher, timers, human checkpoints, and effect bridge APIs.
- Add policy hooks for max autonomy steps, budget, timeout, approval, and
  cancellation.

Acceptance:

- A run can plan, call a tool/model, wait for a timer or human checkpoint,
  resume, and complete after process restart.
- External side effects are idempotency-keyed.

### Phase 6: Production Topology

- Provide Kubernetes deployment guidance using load-balanced A2A ingress, etcd
  or equivalent external discovery, PostgreSQL persistence, Rakka remoting,
  readiness/liveness/drain, PodDisruptionBudget, and autoscaling signals.
- Add operational snapshots and OpenTelemetry propagation.
- Document when to enable remembered entities and when to recover lazily.

Acceptance:

- Multi-pod failure-injection tests cover pod kill, owner movement, duplicate
  client retry, dispatcher retry, stream reconnect, and cancellation.

## Test Strategy

Required test coverage:

- A2A wire compatibility using `a2acli` against REST and JSON-RPC.
- Task/run id mapping for new and existing tasks.
- Durable duplicate handling by A2A `message_id` and deduplication key.
- `return_immediately` behavior.
- Task projection for every terminal and waiting state.
- Cancel while running, waiting, and already terminal.
- SSE stream snapshot, update, disconnect, and resubscribe.
- Push config create/list/get/delete and durable push send retry.
- Non-owner ingress routing to owner.
- Owner crash before/after acceptance, before/after effect scheduling, and
  during dispatcher work.
- Shard movement with passivation and bounded buffering.
- Tenant isolation and authorization failures.

## Resolved Questions

| Question | Decision | Rationale |
| --- | --- | --- |
| First deliverable | Build a runnable example first, then extract `rakka-a2a` after API review. | The A2A adapter has several policy decisions around task identity, projection, streaming, and push delivery. A concrete example keeps the first slice reviewable while preserving the option to promote stable pieces into a reusable crate. |
| `Task.id` identity | Make A2A `Task.id` equal `AgentRunId` and the sharded entity id. Do not support aliases in the first milestone. | One canonical id keeps routing, recovery, idempotency, query projection, and client reconnect behavior simple. Externally meaningful ids can live in `context_id` or metadata until there is a proven need for an alias table. |
| Metadata keys | Reserve an `io.rakka.*` metadata namespace. Use W3C trace headers as canonical trace context, with metadata fallback for transports that cannot carry headers. | Namespaced metadata avoids collisions with application and A2A fields while making durable command normalization explicit. |
| First cluster topology | Keep single-node local mode as the developer default. Require external discovery and shared durable storage for production. | A2A should be easy to run locally, but production correctness across pods requires shared state, shared projection, private remoting, and membership driven by etcd or an equivalent discovery provider. |
| `subscribe_to_task` projection | Use existing Rakka runtime events as the source and build a dedicated durable A2A task event projection for public streaming/replay. | Query indexes are good for current state, but streams need ordered public events and replay cursors. A task event projection gives A2A a stable public read model without exposing internal runtime event details. |
| First production transports | Support A2A REST/HTTP+JSON and JSON-RPC first. Add A2A gRPC after the handler and projection model are stable. | REST/JSON-RPC cover the SDK's core public surface and SSE streaming path with fewer moving parts. gRPC should share the same durable handler semantics later, not introduce a second behavior path. |

### Standard Metadata

The adapter should use these Rakka-owned metadata keys when the value is not
already represented by a first-class A2A field:

| Key | Meaning |
| --- | --- |
| `io.rakka.adapter.version` | Adapter schema/version marker. |
| `io.rakka.workflow.id` | Target `AgentWorkflowId` when the service hosts more than one workflow. |
| `io.rakka.workflow.type` | Stable workflow type selected by the A2A skill or request metadata. |
| `io.rakka.workflow.definition_version` | `WorkflowDefinitionVersion` selected for the run. |
| `io.rakka.command.id` | Explicit command id when not using A2A `Message.message_id`. |
| `io.rakka.command.deduplication_key` | Stable durable inbox deduplication key. |
| `io.rakka.causation_id` | Command or effect that caused this request. |
| `io.rakka.correlation_id` | Correlation id shared by commands, effects, traces, logs, and audit. |
| `io.rakka.principal.ref` | Authenticated principal reference, when supplied by the public auth layer. |
| `io.rakka.trace.traceparent` | W3C traceparent fallback when headers are unavailable. |
| `io.rakka.trace.tracestate` | W3C tracestate fallback when headers are unavailable. |

The canonical source for tenant remains `SendMessageRequest.tenant` or the
authenticated request context. The canonical source for task/run identity
remains `Task.id` / `Message.task_id`.

### Remaining Follow-Ups

- Define the exact A2A task event projection schema, replay cursor shape, and
  retention policy.
- Decide when the example API is stable enough to extract into a reusable
  `rakka-a2a` crate and top-level `rakka` facade feature.
- Decide whether a later compatibility mode should support external task-id
  aliases for systems that cannot use Rakka-generated task ids.

## Decision

Proceed with Rakka clustered sharded entities as the live ownership and routing
model for A2A durable agents. The design is a good fit if the implementation
keeps durability in Rakka's agent workflow substrate and treats A2A as a public
protocol adapter over durable run state.

The effort should not use the A2A SDK's in-memory default execution manager as
the correctness boundary. It should use the SDK's wire types, agent cards,
REST/JSON-RPC routers, SSE helpers, client tooling, and request-handler trait,
while implementing durable Rakka-backed request handling, task projection,
streaming, and push notification dispatch.

## Sources

- A2A Rust SDK repository:
  https://github.com/a2aproject/a2a-rs
- A2A server handler and request boundary:
  https://github.com/a2aproject/a2a-rs/blob/main/a2a-server/src/handler.rs
- A2A server executor boundary:
  https://github.com/a2aproject/a2a-rs/blob/main/a2a-server/src/executor.rs
- A2A REST binding:
  https://github.com/a2aproject/a2a-rs/blob/main/a2a-server/src/rest.rs
- A2A agent card model:
  https://github.com/a2aproject/a2a-rs/blob/main/a2a/src/agent_card.rs
- A2A task and message model:
  https://github.com/a2aproject/a2a-rs/blob/main/a2a/src/types.rs
- Local Rakka references:
  `crates/rakka-agent-workflow/src/sharding.rs`,
  `crates/rakka-agent-workflow/src/runtime.rs`,
  `crates/rakka-sharding/src/facade.rs`,
  `crates/rakka-sharding/src/node_runtime.rs`,
  `crates/rakka-persistence-postgres/src/lib.rs`,
  `crates/rakka-sharding-postgres/src/lib.rs`,
  `examples/clustered-agent-workflow-http-grpc/README.md`.
