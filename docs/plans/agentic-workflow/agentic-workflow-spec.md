# Rakka Agentic Workflow Spec

Status: planning draft
Date: 2026-06-17

## Purpose

This spec defines how Rakka can grow from its current v1 release-candidate
foundations into a runtime for durable, long-running agentic workflows deployed
on cloud Kubernetes.

Agentic workflows are application workflows that may call language models,
tools, APIs, child processes, humans, and other agents over minutes, hours, or
days. They must survive process restarts, pod eviction, node replacement,
rolling updates, network failures, and human approval pauses without losing
accepted work or creating unbounded runtime state.

The current repository already contains many of the required primitives:

- typed actors and bounded mailboxes;
- durable state, event journals, snapshots, and PostgreSQL adapters;
- durable workflow inbox/outbox reliability;
- cluster membership, remoting, sharding, shard movement, and coordinator
  persistence;
- process actors for supervised local tools;
- bounded streams;
- HTTP and gRPC integration adapters;
- Kubernetes readiness, liveness, drain, and compatibility hooks;
- backend-neutral metrics, OpenTelemetry-oriented export helpers, tracing
  conventions, and operational snapshot routes.

The missing layer is an agent workflow orchestration model that composes these
primitives into first-class runs, steps, tool/model calls, human checkpoints,
durable timers, observability, and operational policies.

## Goals

- Define a durable workflow architecture for long-running agent runs.
- Preserve Rakka's explicit reliability boundary: core actor delivery remains
  at-most-once; durable reliability is opt-in through workflow state and
  outbox semantics.
- Support human-in-the-loop pauses without occupying a live task, mailbox, or
  stream while waiting.
- Support Kubernetes scale-out, rolling updates, pod drains, and shard movement.
- Use OpenTelemetry as the standard observability contract for traces, metrics,
  logs, resources, semantic conventions, context propagation, and collector
  integration.
- Provide complete agent-domain telemetry requirements on top of Rakka's
  existing metrics, tracing, logs, and snapshots while keeping storage and
  visualization backends pluggable.
- Identify implementation gaps and organize them into future planning slices.

## Non-Goals

- Do not turn Rakka internal remoting into a public workflow API.
- Do not promise exactly-once external side effects against arbitrary systems.
- Do not require one hosted observability vendor, tracing backend, queue, or
  dashboard product.
- Do not require Rakka applications to bypass OpenTelemetry by coupling
  directly to a single vendor SDK, agent, or protocol.
- Do not require per-agent Kubernetes pods or sidecars for v1. Process actors
  remain local child processes inside a Rakka node container until a later
  workload ownership model is designed.
- Do not make Rakka a full web framework, authentication platform, or policy
  engine.
- Do not replace application-owned prompt engineering, model selection,
  authorization, or business approval policy.

## OpenTelemetry Alignment

OpenTelemetry should be the primary observability standard for agentic
workflows. Rakka should treat OpenTelemetry as the contract for generating,
correlating, processing, and exporting telemetry, while leaving backend storage
and visualization to tools chosen by the application or operator.

The agent workflow layer should align with these OpenTelemetry concepts:

- signals: traces, metrics, logs, baggage, and later profiles;
- context propagation: W3C `traceparent` and `tracestate` through public
  ingress, durable inbox commands, durable outbox entries, remoting metadata,
  process/tool protocols, callbacks, and human approval submissions;
- resources: stable `service.name`, `service.namespace`, `service.version`,
  `deployment.environment.name`, Kubernetes pod/namespace, container, node, and
  cluster attributes;
- semantic conventions: standard names for HTTP, gRPC, messaging, database,
  RPC, errors, Kubernetes, service, process, and model/tool telemetry where
  current conventions apply;
- instrumentation scope: each Rakka crate and each application workflow layer
  should emit telemetry under an explicit scope name and version;
- Collector pipeline: application telemetry should be exported through OTLP to
  an OpenTelemetry Collector whenever practical, then routed to Prometheus,
  traces, logs, and commercial or open-source backends.

Rakka's existing backend-neutral `MetricsRecorder`, `tracing` spans, and
OpenTelemetry-oriented metrics bridge can support a transition path. The target
state for production agent workflows should be native OTLP export for traces,
metrics, and logs, with Prometheus text exposition retained as a compatibility
option.

## Target Runtime

The target deployment is a cloud Kubernetes cluster running multiple Rakka
pods. A production deployment should assume:

- pods can restart or move at any time;
- nodes can be drained or replaced;
- multiple Rakka versions may coexist during an N/N+1 rolling update;
- PostgreSQL or an equivalent durable store is available;
- internal remoting runs on trusted cluster networking, not public ingress;
- public workflow ingress uses application HTTP/gRPC APIs;
- Rakka applications emit OpenTelemetry traces, metrics, and logs through OTLP
  to a local or gateway OpenTelemetry Collector;
- Prometheus scraping remains available for compatibility, but OTLP is the
  preferred telemetry path for complete correlation;
- secrets, network policies, service accounts, TLS/mTLS, pod security, resource
  limits, and autoscaling are operator responsibilities.

## Current Foundations

### Typed Actors

What exists:

- `rakka-core` provides `ActorSystem`, `ActorRef`, `ActorContext`, supervision,
  timers, bounded mailboxes, dead letters, actor snapshots, and actor metrics.

How agentic workflows use it:

- Each active workflow run can be represented by a typed actor or sharded
  entity that receives commands such as `StartRun`, `ContinueStep`,
  `ToolResultReceived`, `HumanDecisionReceived`, and `CancelRun`.
- Worker actors can isolate model calls, tool dispatch, artifact indexing,
  progress notification, and cleanup.
- Bounded mailboxes make pressure visible instead of silently growing memory.

Benefits:

- Clear concurrency boundaries.
- Typed command protocols.
- Supervision and restart hooks.
- Bounded queues suitable for Kubernetes memory limits.

Limits:

- Core `tell` and `ask` are at-most-once.
- Mailboxes are not durable.
- `ask` timeout does not prove the actor did not finish later.

Required agent workflow policy:

- External client commands must enter through a durable inbox.
- Side effects must be scheduled through a durable outbox.
- Commands and side effects need stable idempotency keys.

### Durable Persistence

What exists:

- `rakka-persistence` defines `DurableStateStore`, `EventJournal`, and
  `SnapshotStore`.
- Writes are revision-fenced.
- PostgreSQL adapters provide durable state, journal, and snapshot tables.
- Query helpers can list persistence ids and query events by persistence id or
  tag when the backend supports it.

How agentic workflows use it:

- Store workflow run state under stable persistence ids such as
  `agent-workflow:<tenant>:<workflow-id>` or a structured equivalent.
- Store event-sourced history for audit-heavy domains where replay is needed.
- Store snapshots for fast recovery after long histories.
- Store coordinator state and remembered entity identity separately from
  workflow business state.

Benefits:

- Crash recovery.
- Optimistic concurrency through revisions.
- PostgreSQL durability for cloud Kubernetes.
- Typed storage boundary that can support more backends later.

Limits:

- Durable state does not make actor delivery exactly once.
- Durable state does not make external side effects transactional.
- Schema evolution and migration policy are application responsibilities.

### Durable Workflow Inbox and Outbox

What exists:

- `rakka-workflow` provides `DurableInbox`, `WorkflowState`, inbox entries,
  outbox entries, deduplication keys, retry attempts, retry policies, clocks,
  and `WorkflowTelemetryEvent`.
- Inbound commands are persisted before being accepted.
- Duplicate message ids or deduplication keys return the existing entry.
- Outbox entries are persisted before dispatch.
- Outbox dispatching is persisted before an external side effect starts.
- Success, retry, timeout, and exhaustion states are durable.
- Recovery can find pending or in-progress inbox entries and due outbox entries.

How agentic workflows use it:

- Every user/API/system command enters the workflow through the durable inbox.
- Every model call, tool call, notification, callback, approval request, or
  downstream write is represented as an outbox effect before execution.
- Workflow code treats outbox dispatch results as input to the next state
  transition.
- Human approval waits are represented as persisted workflow state plus a
  later inbox command, not as a blocked async task.

Benefits:

- Durable intent.
- Built-in deduplication.
- Bounded retry scheduling.
- Recovery after restart.
- A natural place to attach telemetry events.

Limits:

- It is a reliability substrate, not a full workflow engine.
- It does not define agent steps, model calls, tool calls, approvals, artifacts,
  timers, leases, or query indexes.
- It currently emits workflow telemetry events but does not record them through
  stable workflow metric constants.
- The current snapshot model keeps inbox/outbox maps in one state object; long
  histories need retention, archival, or event-sourced storage.

### Cluster, Remoting, and Sharding

What exists:

- `rakka-cluster` provides node identity, membership, discovery snapshots, and
  compatibility admission.
- `rakka-remote` provides trusted-cluster remoting, known-peer admission,
  Protobuf envelope transport, schema compatibility policy, request/reply
  correlation, and bounded remote queues.
- `rakka-sharding` provides entity identity, shard ownership, regions, remote
  routes, remembered entities, graceful handoff, passivation, bounded handoff
  buffers, and coordinator snapshots.
- `rakka-sharding-postgres` provides PostgreSQL coordinator storage, leadership
  leases, fencing tokens, and remembered entity storage.

How agentic workflows use it:

- Each workflow run is a sharded entity keyed by workflow id.
- Shard ownership distributes active workflows across pods.
- Durable coordinator snapshots allow ownership state to recover.
- Remembered entities can restart active workflow ids after shard acquisition.
- Graceful handoff and drain reduce disruption during rolling updates.

Benefits:

- Horizontal scale across Kubernetes pods.
- Logical workflow identity independent of pod placement.
- Recovery after shard movement.
- Compatibility gates for N/N+1 rolling updates.

Limits:

- Remote actor/entity delivery remains at-most-once.
- Durable coordinator state is control-plane placement, not workflow state.
- Multi-coordinator deployments need lease policy and careful renewal.
- The project does not yet ship a full Kubernetes operator, Helm lifecycle, or
  durable consensus backend.

### Process Actors

What exists:

- `rakka-process` owns supervised child processes with explicit specs,
  executable allowlists, conservative environment defaults, readiness/health
  checks, restart policy, line-json protocols, one-shot execution, file-watch
  mode, socket foundations, and process operational snapshots.

How agentic workflows use it:

- Tool adapters can run as supervised child processes inside a Rakka pod.
- Legacy tools, local indexers, sandbox wrappers, and protocol bridges can be
  modeled as process actors.
- Process-backed sharded entities can tie tool ownership to shard ownership
  where that is appropriate.

Benefits:

- Tool lifecycle is visible to Rakka.
- Restart budgets and health checks are explicit.
- Child process exits can be measured.
- Default process specs reduce accidental secret and environment leakage.

Limits:

- Rakka is not an OS sandbox.
- Child processes share the node container.
- Per-tool pod sidecars or external workloads are future work.
- Tool protocols must tolerate retries, cancellation, and partial output.

### Streams

What exists:

- `rakka-stream` provides bounded stream source/sink handles, explicit
  back-pressure, lifecycle snapshots, drain, cancellation, and pressure metrics.
- HTTP/gRPC streaming adapters map public streams into the bounded stream model.

How agentic workflows use it:

- Stream large model outputs, logs, progress events, artifacts, or tool output
  through bounded channels.
- Drain stream sinks during Kubernetes pre-stop.
- Avoid unbounded memory when agents produce more output than consumers can
  process.

Benefits:

- Explicit pressure and cancellation.
- Operationally useful stream status.
- Integration with drain.

Limits:

- Streams are not durable.
- Durable tool/model effects still need workflow outbox entries.

### HTTP and gRPC Adapters

What exists:

- `rakka-http` and `rakka-grpc` expose typed service, actor, entity, stream,
  workflow, and process-backed services.
- Request metrics record latency, status, outcome, and stable error labels.
- Generated contract examples show how application proto/HTTP APIs call Rakka
  primitives.

How agentic workflows use it:

- Public APIs submit workflow commands, query workflow state, stream progress,
  and deliver human decisions.
- Internal remoting remains separate from public ingress.
- API handlers translate public request ids into workflow inbox deduplication
  keys.

Benefits:

- Clean edge boundary.
- Typed error mapping.
- Observable request latencies.

Limits:

- Auth, authorization, tenant isolation, rate limiting, validation, TLS,
  public API lifecycle, and ingress policy remain application/operator work.

### Kubernetes Operation

What exists:

- `rakka-k8s` provides readiness/liveness health, compatibility failure state,
  drain state, required service checks, runtime stuck markers, rebalancing
  markers, drain orchestration, and drain report mapping from coordinated
  shutdown.
- Example manifests separate public HTTP/gRPC ports from internal remoting.

How agentic workflows use it:

- Readiness fails before pod termination or unsafe compatibility.
- Drain stops new work before stream drains, shard leave, process stops, and
  persistence flush tasks.
- Liveness can fail on runtime stuck conditions.
- Rebalancing markers make shard movement visible during scale operations.

Benefits:

- Safer Kubernetes rolling updates.
- Fail-closed compatibility behavior.
- Observable drain reports.
- Hook points for agent-specific shutdown tasks.

Limits:

- No full operator or Helm lifecycle.
- Kubernetes NetworkPolicy, PodDisruptionBudget, service accounts, secrets,
  resource limits, and autoscaling must be supplied by deployment code.

### OpenTelemetry, Metrics, Tracing, Logs, and Snapshots

What exists:

- `rakka-core::MetricsRecorder` is the backend-neutral metrics boundary.
- Stable metric names exist for actors, sharding, persistence, remoting,
  process exits, streams, HTTP, gRPC, Kubernetes readiness/compatibility, and
  coordinated shutdown.
- Prometheus text and OpenTelemetry-oriented JSON exporters exist.
- `OperationalSnapshotRegistry` exposes named JSON snapshots.
- Tracing uses the `tracing` crate. HTTP/gRPC create request spans, streams
  create pipeline spans, and remoting emits connection/failure events.

How agentic workflows use it:

- Register workflow runtime snapshots under `/snapshots`.
- Record workflow metrics through the configured actor-system recorder while
  adding an OTLP-capable metrics exporter path.
- Install a `tracing` subscriber with OpenTelemetry propagation/export at the
  application boundary.
- Attach workflow, run, step, effect, tenant-tier, and error-code attributes to
  spans and structured logs according to OpenTelemetry semantic conventions and
  Rakka-specific extensions.
- Export traces, metrics, and logs through OTLP to an OpenTelemetry Collector.
- Retain `/metrics`, `/otel/metrics`, and `/snapshots` as useful compatibility
  and diagnostics routes.

Benefits:

- Backend-neutral instrumentation.
- Production backend choice stays outside Rakka.
- OpenTelemetry context propagation can correlate public ingress, sharded
  workflow execution, durable resumes, outbox dispatch, process tools, and
  human approval callbacks.
- OpenTelemetry resource attributes can identify cloud, Kubernetes, service,
  deployment, pod, container, and process dimensions consistently.
- Operational snapshots can include application-owned state.

Limits:

- No workflow-specific metric constants yet.
- No native OTLP exporter integration yet; the current bridge is a serialized
  OpenTelemetry-oriented model rather than an SDK-backed OTLP pipeline.
- No agent trace schema or span-link policy yet.
- No OpenTelemetry log/audit schema for prompts, model calls, tool calls,
  approvals, and artifacts.
- No hosted dashboards, alert rules, or vendor-specific agents.

## Proposed Agentic Workflow Model

### Core Concepts

`AgentWorkflow`

- A workflow definition registered by application code.
- Owns command schema, step graph, retry policy, timeout policy, approval
  policy, tool bindings, model bindings, and observability labels.

`AgentRun`

- One durable execution of a workflow definition.
- Has a stable `run_id`, `workflow_id`, tenant or namespace, status, version,
  current step cursor, state payload, and timestamps.

`AgentStep`

- One resumable unit of work.
- Types include model call, tool call, planner step, branch, wait, human
  checkpoint, child workflow, compensation, and terminal step.

`AgentEffect`

- A side effect scheduled through the durable outbox.
- Types include model request, tool request, HTTP/gRPC request, process
  request, progress notification, artifact write, callback, approval request,
  and child workflow command.

`HumanCheckpoint`

- A persisted workflow state that waits for a human decision.
- The workflow is idle while waiting.
- Approval, rejection, edit, escalation, or timeout is delivered later as a
  durable inbox command.

`ArtifactRef`

- A reference to durable external storage for prompts, completions, files,
  embeddings, tool output, screenshots, logs, or other large payloads.
- Workflow state stores references and checksums rather than unbounded blobs.

### Agent Run State

The first implementation should define a serializable agent state shape on top
of `rakka-workflow::WorkflowState` or a companion domain state:

```text
AgentRunState
  run_id
  workflow_id
  tenant
  definition_version
  status
  current_step_id
  current_attempt
  inputs_ref
  state_ref_or_inline_state
  checkpoints
  pending_effects
  pending_human_checkpoint
  cancellation
  created_at
  updated_at
  completed_at
```

Status values:

- `accepted`
- `running`
- `waiting-for-timer`
- `waiting-for-human`
- `waiting-for-effect`
- `cancelling`
- `completed`
- `failed`
- `compensating`
- `cancelled`

The exact Rust API should avoid storing high-cardinality or large data directly
in metrics labels. Use durable state and snapshots for detailed inspection.

### Command Model

Workflow commands should be accepted only after durable inbox persistence:

- `StartRun`
- `SubmitSignal`
- `ContinueRun`
- `EffectCompleted`
- `EffectFailed`
- `HumanDecisionSubmitted`
- `TimerFired`
- `CancelRun`
- `RetryRun`
- `ForgetRun`

Every command should carry:

- workflow id;
- run id;
- command id;
- deduplication key;
- causation id;
- correlation id;
- optional trace context;
- tenant or namespace;
- authenticated principal metadata supplied by application code.

### Effect Model

Every external side effect should be represented as a durable outbox entry:

- `ModelCall`
- `ToolCall`
- `ProcessCall`
- `HttpCall`
- `GrpcCall`
- `StreamPublish`
- `ArtifactWrite`
- `HumanApprovalRequest`
- `Notification`
- `ChildWorkflowCommand`
- `AuditEvent`

Every effect should carry:

- stable effect id;
- deduplication key;
- target;
- payload or artifact reference;
- timeout;
- retry policy;
- idempotency policy;
- expected result type;
- causation id;
- trace context.

Dispatchers must persist dispatching before executing the effect, then persist
success, retry, timeout, or exhaustion. This matches the existing outbox
boundary.

### Human-in-the-Loop Pauses

A human pause should be modeled as durable state, not an in-memory wait:

1. Workflow reaches a checkpoint step.
2. Workflow persists `waiting-for-human`.
3. Workflow schedules a `HumanApprovalRequest` outbox effect.
4. Dispatcher sends the approval request to the application UI, ticket system,
   Slack/Teams bridge, or another system.
5. Workflow actor becomes idle and can be passivated.
6. Human decision enters through public HTTP/gRPC.
7. API handler accepts `HumanDecisionSubmitted` into the workflow inbox with a
   deduplication key.
8. Workflow resumes when the sharded entity processes the command.

Required checkpoint metadata:

- checkpoint id;
- prompt or decision summary;
- available decisions;
- required roles or policy hints;
- due timestamp;
- escalation target;
- artifact references for context;
- principal that created the checkpoint;
- principal that resolved it;
- immutable audit event references.

Timeout handling:

- If a checkpoint has an SLA, schedule a durable timer.
- Timer expiry delivers `TimerFired`.
- The workflow definition decides whether to escalate, auto-reject, auto-approve
  under policy, retry, or fail.

## Kubernetes Scale Architecture

### Recommended Topology

For the first cloud Kubernetes deployment:

- Rakka application deployment with N replicas.
- Public HTTP/gRPC service behind ingress or service mesh.
- Internal headless service for Rakka remoting.
- PostgreSQL for workflow state, event journal, snapshots, shard coordinator
  state, shard coordinator lease, and remembered entities.
- Object storage for large artifacts.
- OpenTelemetry Collector deployed as a DaemonSet for node/pod/container
  telemetry and as a gateway Deployment for cluster-wide processing, sampling,
  routing, and backend export.
- Rakka application pods exporting OTLP traces, metrics, and logs to the local
  Collector endpoint.
- Optional Prometheus scrape endpoint for compatibility and local debugging.
- OpenTelemetry tracing subscriber and metrics/log exporters installed by the
  application binary.
- Kubernetes readiness, liveness, and pre-stop drain endpoints exposed through
  HTTP.

### Sharded Workflow Ownership

- Register `AgentRun` as a sharded entity type.
- Use workflow id or run id as entity id.
- Keep entity commands small; put large payloads in artifact storage.
- Use remembered entities only for active runs that should restart on shard
  acquisition.
- Use passivation for idle waiting runs to reduce memory.
- On resume command, the entity should recover durable state and continue.

### Pod Startup

On startup:

1. Configure OpenTelemetry resource attributes such as `service.name`,
   `service.namespace`, `service.version`, `deployment.environment.name`,
   Kubernetes namespace, pod name, node name, and cluster name.
2. Configure `tracing` plus OpenTelemetry propagation and OTLP export.
3. Configure the Rakka metrics recorder and OpenTelemetry meter/export path.
4. Connect to PostgreSQL and run explicit migrations when enabled by deployment
   policy.
5. Create actor system with metrics recorder.
6. Configure cluster node identity and compatibility metadata.
7. Configure remote registry, schema compatibility, and trace context
   propagation metadata.
8. Configure sharding with async PostgreSQL coordinator store and lease.
9. Register agent workflow entity types.
10. Register workflow snapshot providers.
11. Mark required services registered.
12. Accept readiness only after cluster compatibility, telemetry exporter
   initialization, and service registration.

### Pod Drain

On pre-stop:

1. Mark readiness false.
2. Stop accepting new public workflow commands on the draining pod.
3. Drain HTTP/gRPC streams and bounded stream sinks.
4. Mark local node leaving.
5. Handoff shards when possible.
6. Stop process actors and external tool children.
7. Flush persistence and telemetry buffers controlled by the application,
   including OpenTelemetry span, metric, and log processors.
8. Stop user actors, system actors, and remoting through coordinated shutdown.

Workflow correctness should not depend on drain completing. Drain improves
availability and reduces duplicate work, but durable recovery must handle pod
death at any boundary.

### Autoscaling

Useful scaling signals:

- active workflow entities;
- running workflow count;
- pending inbox count;
- due outbox count;
- outbox dispatch latency;
- workflow step latency;
- human wait count;
- mailbox depth;
- stream pressure;
- process actor state;
- remote queue pressure;
- PostgreSQL latency;
- shard ownership distribution.

Horizontal Pod Autoscaler can start with CPU and memory, but production agent
workloads should add custom metrics for pending/due workflow work and dispatch
latency.

### Multi-Tenancy

The workflow layer should treat tenant or namespace as a first-class field:

- persistence id namespace;
- shard entity id prefix or entity type partition;
- bounded metric attribute where appropriate;
- trace/log attribute;
- authorization scope;
- artifact bucket/prefix;
- retention policy;
- rate limit policy.

Avoid putting raw tenant-specific ids in hot metric labels unless bounded by
deployment policy.

## Observability Requirements

Rakka's current observability primitives are necessary but not sufficient for
complete agent operations. The agent workflow layer should add a domain
observability contract.

The agent workflow layer should emit OpenTelemetry-native telemetry first.
Rakka-specific names should remain stable, but they should be shaped so they
can be exported through OTLP and interpreted with OpenTelemetry resources,
instrumentation scopes, semantic conventions, and context propagation.
Prometheus-compatible exposition remains useful for local debugging,
compatibility, and autoscaling integrations, but it should not be the only
production observability path.

### Resource Model

Every Rakka process that hosts agent workflows should configure an
OpenTelemetry resource before creating tracers, meters, or loggers. Required
attributes:

- `service.name`;
- `service.namespace`;
- `service.version`;
- `deployment.environment.name`;
- `k8s.cluster.name`;
- `k8s.namespace.name`;
- `k8s.deployment.name`;
- `k8s.pod.name`;
- `k8s.node.name`;
- `container.name`;
- `container.image.name`;
- `container.image.tag`.

SDK-provided attributes such as `telemetry.sdk.name`,
`telemetry.sdk.language`, and `telemetry.sdk.version` should be retained.

Rakka-specific resource attributes should be bounded and stable:

- `rakka.cluster.name`;
- `rakka.node.id`;
- `rakka.protocol.version`;
- `rakka.compatibility.version`;
- `rakka.workflow.runtime.version`.

Configuration should work through OpenTelemetry environment variables such as
`OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`, and `OTEL_EXPORTER_OTLP_*`, as
well as explicit Rakka application config. In Kubernetes, resource detectors
and the Collector Kubernetes attributes processor should enrich telemetry with
pod, namespace, node, deployment, and cluster metadata where possible.

### Metrics

Add stable OpenTelemetry metric instruments, likely in `rakka-core` or
`rakka-workflow`, with Prometheus-compatible mappings where applicable:

- `rakka.workflow.runs`
- `rakka.workflow.commands.accepted`
- `rakka.workflow.commands.duplicates`
- `rakka.workflow.commands.rejected`
- `rakka.workflow.inbox.depth`
- `rakka.workflow.outbox.pending`
- `rakka.workflow.outbox.due`
- `rakka.workflow.outbox.dispatch.latency_ms`
- `rakka.workflow.outbox.dispatch.failures`
- `rakka.workflow.outbox.dispatch.exhausted`
- `rakka.workflow.steps.started`
- `rakka.workflow.steps.completed`
- `rakka.workflow.steps.failed`
- `rakka.workflow.steps.latency_ms`
- `rakka.workflow.human.waiting`
- `rakka.workflow.human.wait.latency_ms`
- `rakka.workflow.model.calls`
- `rakka.workflow.model.latency_ms`
- `rakka.workflow.model.tokens`
- `rakka.workflow.tool.calls`
- `rakka.workflow.tool.latency_ms`
- `rakka.workflow.timers.due`
- `rakka.workflow.recovery.count`
- `rakka.workflow.recovery.latency_ms`

Recommended instrument types:

- counters for accepted, duplicate, rejected, started, completed, failed,
  retry, exhausted, recovery, model-call, tool-call, and dispatch-failure
  totals;
- up-down counters or observable gauges for running workflows, waiting
  workflows, inbox depth, outbox pending depth, due timers, human checkpoints,
  and active dispatcher leases;
- histograms for command acceptance latency, step latency, effect dispatch
  latency, model/tool latency, human wait duration, recovery latency, database
  latency, remote routing latency, and process execution latency;
- optional histograms for model tokens and cost units when the application can
  bound cardinality and unit semantics.

Recommended bounded labels:

- workflow type;
- workflow version;
- status;
- step type;
- effect type;
- outcome;
- error code;
- retry attempt bucket;
- tenant tier, not raw tenant id unless bounded;
- Kubernetes namespace/pod/node as resource attributes.

Use current OpenTelemetry semantic conventions for HTTP, gRPC/RPC, messaging,
database, error, service, process, and Kubernetes telemetry at boundaries that
already fit those domains. Use `rakka.*` attributes only for workflow-specific
concepts such as workflow type, run status, effect type, retry bucket, and
approval status.

Avoid labels for:

- raw workflow id;
- raw run id;
- raw entity id;
- prompt text;
- tool arguments;
- user-provided filenames;
- full error messages;
- model output.

### Tracing

Every workflow command and effect should participate in a trace:

- `workflow.command.accept`
- `workflow.command.process`
- `workflow.step.run`
- `workflow.effect.schedule`
- `workflow.effect.dispatch`
- `workflow.human.wait`
- `workflow.recover`
- `workflow.timer.fire`

Span roles:

- public HTTP/gRPC APIs create server spans using the existing adapter
  conventions;
- workflow command acceptance, state transition, step execution, recovery, and
  timer handling create internal spans;
- model calls, tool calls, HTTP/gRPC calls, database calls, process calls, and
  callback dispatch create client spans;
- durable inbox and outbox writes should record messaging-like or database
  attributes where the current semantic conventions fit;
- long pauses should end the active span rather than holding it open.

Durable trace context is required. Workflow commands, inbox entries, outbox
entries, timer entries, process protocols, remote envelopes, callbacks, and
human approval submissions should persist or carry W3C Trace Context values
such as `traceparent` and `tracestate`.

Long-running workflows should use span links for asynchronous resume points.
For example, a `HumanDecisionSubmitted` span may start from the human
submission request and link to the original checkpoint span. A retry dispatch
span may link to the span that originally scheduled the effect. This preserves
causal history without forcing a single parent-child trace tree across hours or
days of idle time.

Baggage can carry low-cardinality routing context such as tenant tier,
deployment environment, workflow type, and policy class. It must not carry
secrets, raw prompts, raw completions, unbounded user ids, auth tokens,
personal data, or tool payloads unless an application-specific policy explicitly
allows it.

Required span attributes:

- `rakka.workflow.type`
- `rakka.workflow.version`
- `rakka.workflow.run_id`, sampled carefully;
- `rakka.workflow.step_id`, bounded by definition;
- `rakka.workflow.step_type`
- `rakka.workflow.effect_type`
- `rakka.workflow.status`
- `rakka.error.code`
- `rakka.retry.attempt`
- `rakka.tenant.tier` or bounded tenant label;
- `messaging.message_id`
- `messaging.conversation_id` or correlation id when available.

Boundary spans should also use standard OpenTelemetry attributes where
applicable, including current equivalents of:

- `http.route`;
- `rpc.service`;
- `rpc.method`;
- `db.system.name`;
- `db.operation.name`;
- `messaging.message.id`;
- `messaging.operation.name`;
- `process.command`;
- `error.type`.

High-cardinality ids can appear in traces because trace storage is designed for
individual requests, but metrics must stay bounded.

### Structured Logs

Structured runtime logs should be emitted as OpenTelemetry log records when the
application installs an SDK-backed log pipeline. A JSON log format that can be
ingested by the OpenTelemetry Collector filelog receiver is acceptable as a
transition path, but records must include trace and span correlation fields when
a current span exists.

Required log events:

- command accepted;
- duplicate command detected;
- workflow recovered;
- step started;
- step completed;
- step failed;
- outbox scheduled;
- dispatch started;
- dispatch succeeded;
- dispatch retry scheduled;
- dispatch exhausted;
- human checkpoint opened;
- human decision received;
- timer scheduled;
- timer fired;
- workflow completed;
- workflow failed;
- workflow cancelled;
- compensation started/completed/failed.

Required log record fields:

- timestamp;
- severity;
- event name;
- trace id and span id when available;
- resource attributes supplied by OpenTelemetry;
- instrumentation scope;
- workflow type and version;
- bounded workflow/run/step/effect identifiers according to policy;
- outcome;
- stable error code;
- retry attempt bucket;
- artifact references;
- redaction status.

Logs should include stable ids and artifact references, but should avoid raw
prompt/model/tool payloads by default. Payload logging must be opt-in,
redactable, and controlled by application policy.

### Audit Log

Agent workflows need an audit stream distinct from ordinary runtime logs.

Audit events should be durable and queryable:

- run created;
- workflow definition version selected;
- input accepted;
- model requested;
- model response received;
- tool requested;
- tool response received;
- artifact written;
- checkpoint created;
- human decision made;
- policy override;
- run completed;
- run failed;
- run cancelled;
- retention deletion.

Audit events should include:

- actor principal;
- tenant;
- workflow type and version;
- run id;
- step id;
- causation id;
- correlation id;
- artifact references;
- content hashes;
- redaction status.

The event journal can support this, but the agent layer should define the
schema and retention policy. Audit events can also be emitted as OpenTelemetry
logs or span events for correlation, but the telemetry backend must not be the
only compliance or forensic retention mechanism.

### Operational Snapshots

Register named snapshots through `OperationalSnapshotRegistry`:

- `agent_workflow_runtime`
- `agent_workflow_shards`
- `agent_workflow_outbox`
- `agent_workflow_timers`
- `agent_workflow_human_checkpoints`
- `agent_workflow_dispatchers`
- `agent_workflow_process_tools`
- `agent_workflow_recovery`

Snapshots should answer:

- how many active runs exist by status;
- how many commands are pending;
- how many outbox effects are due;
- which workflow definitions are registered;
- which dispatchers are healthy;
- whether timer scanning is active;
- whether leases are held;
- whether any workflow scopes are stuck;
- whether pod drain is blocking on workflow tasks.

Operational snapshots are not a replacement for OpenTelemetry. They are
point-in-time diagnostic views for humans and automation. Selected snapshot
counters can be surfaced as metrics, and snapshot changes can produce
OpenTelemetry logs, but the canonical snapshot payload should remain directly
queryable through the existing operational route.

## Main Gaps

### Gap 1: No First-Class Agent Workflow Engine

Current state:

- `rakka-workflow` provides durable inbox/outbox reliability primitives.
- Docs explicitly keep orchestration beyond those primitives out of scope.

Needed:

- `AgentWorkflow` definition API.
- Step graph/state-machine runner.
- Run lifecycle model.
- Command/effect schema.
- Policy hooks for retry, timeout, cancellation, compensation, and approval.

Planning slice:

- Add a new crate or module such as `rakka-agent-workflow` or extend
  `rakka-workflow` behind an additive facade.

### Gap 2: No Durable Timer Service

Current state:

- `WorkflowClock` exists.
- Outbox entries can become due based on workflow time.

Needed:

- Durable timer entries.
- Timer scanner or sharded timer ownership.
- Timer lease/fencing in Kubernetes.
- `TimerFired` inbox command delivery.
- Back-pressure and metrics for overdue timers.

Planning slice:

- Add timer storage and scanner integration on top of durable state or event
  journal.

### Gap 3: Workflow State May Grow Without Retention

Current state:

- `WorkflowState` stores inbox/outbox maps and deduplication indexes in a
  snapshot.

Needed:

- Retention policy for completed inbox/outbox entries.
- Deduplication retention windows.
- Archival to event journal or audit store.
- Snapshot compaction.
- Query indexes that do not require loading every workflow state.

Planning slice:

- Define retention and compaction APIs before high-volume agent use.

### Gap 4: No Workflow Query and Index Model

Current state:

- Persistence query helpers exist at lower levels.

Needed:

- Query active runs by tenant, status, workflow type, updated_at, waiting
  reason, checkpoint age, failed step, due timer, and stuck dispatcher.
- Avoid scanning all persistence ids for hot operational paths.

Planning slice:

- Add a workflow index store with PostgreSQL implementation.

### Gap 5: No OpenTelemetry-Native Workflow Instruments

Current state:

- Metrics constants cover many runtime boundaries.
- `WorkflowTelemetryEvent` exists but is returned to caller code.
- Prometheus text and OpenTelemetry-oriented JSON export paths exist, but the
  workflow layer does not yet define SDK-backed OTLP metrics, traces, and logs
  as one correlated signal set.

Needed:

- Stable workflow metric instruments and attribute keys.
- Recorder integration for inbox/outbox/status/steps/human/model/tool events
  that can feed OpenTelemetry meters.
- OTLP-capable metrics export path and Collector example.
- Resource attribute configuration helpers for Kubernetes.
- Semantic convention mapping for HTTP, gRPC/RPC, messaging, database,
  process, Kubernetes, errors, and model/tool calls.
- Rakka-specific `rakka.*` attribute registry for workflow concepts that do not
  fit current OpenTelemetry semantic conventions.
- Cardinality guidance specific to agent workloads.

Planning slice:

- Add workflow OpenTelemetry helpers and tests similar to HTTP/gRPC metrics
  helpers, with Prometheus compatibility retained as an output format.

### Gap 6: No OpenTelemetry Context, Trace, Log, and Audit Schema

Current state:

- Tracing conventions are minimal and framework-level.
- HTTP/gRPC, streams, and remoting add spans/events.
- Workflow inbox/outbox state can carry trace context by convention, but there
  is no first-class schema for persisted trace context, baggage, span links, or
  correlated log/audit events.

Needed:

- Trace context propagation through inbox commands, outbox entries, remote
  envelopes, process actors, HTTP/gRPC adapters, and human decisions.
- W3C Trace Context propagation policy for `traceparent` and `tracestate`.
- Baggage allowlist and redaction policy.
- Span-link policy for async resumes, retries, timers, human decisions, and
  outbox dispatch.
- OpenTelemetry log schema for workflow lifecycle events.
- Durable audit event schema for prompts, model calls, tools, artifacts, and
  approvals.
- Clear split between durable audit storage and telemetry correlation.

Planning slice:

- Define `AgentTelemetryContext`, `AgentTraceContext`, `AgentSpanLink`,
  `AgentLogEvent`, and `AgentAuditEvent`.

### Gap 7: No Human Approval Primitive

Current state:

- Workflows can pause through durable state by convention.

Needed:

- First-class checkpoint state.
- Approval request outbox target.
- Approval command schema.
- SLA timeout and escalation policy.
- Snapshot/query surfaces for waiting approvals.
- Audit events for decisions.

Planning slice:

- Add human checkpoint model and example HTTP/gRPC approval path.

### Gap 8: No Outbox Dispatcher Fleet Model

Current state:

- `OutboxDispatcher` is an application-supplied trait used by one
  `DurableInbox` instance.

Needed:

- Dispatcher workers that can scan due outbox effects across many workflows.
- Work claiming/lease/fencing.
- Dispatcher health snapshots.
- Per-target concurrency limits.
- Back-pressure and retry policies per effect type.

Planning slice:

- Add a sharded or leased outbox dispatcher runtime.

### Gap 9: No Model and Tool Adapters

Current state:

- Process actors and HTTP/gRPC adapters can call external systems.

Needed:

- Model provider adapter trait.
- Tool adapter trait.
- Idempotency and receipt handling.
- Token, cost, latency, and error telemetry.
- Rate-limit handling.
- Secrets policy.
- Artifact storage for large prompts/responses.

Planning slice:

- Add adapter traits and one example model/tool adapter using fake local
  implementations for tests.

### Gap 10: Kubernetes Packaging Is Not a Productized Operator

Current state:

- Kubernetes health, drain, manifests, and local-cluster scripts exist.

Needed:

- Reference deployment for agent workflow services.
- Helm or operator plan.
- Autoscaling metrics.
- PodDisruptionBudget guidance.
- NetworkPolicy guidance.
- PostgreSQL migration job guidance.
- Secret and service-account guidance.

Planning slice:

- Add a cloud Kubernetes deployment guide and manifest contract tests for
  agent workflow workloads.

## Implementation Slices

### Slice A: Domain Model and Facade

Deliverables:

- Define agent workflow concepts and status enums.
- Define `AgentWorkflow`, `AgentRun`, `AgentStep`, `AgentEffect`, and
  `HumanCheckpoint` data shapes.
- Define public command and effect traits.
- Provide a minimal in-memory example using existing `DurableInbox`.

Acceptance:

- A workflow can start, persist state, complete one step, and recover.
- Duplicate start commands are detected.
- Example demonstrates no long-lived task during a human pause.

### Slice B: Durable Step Runner

Deliverables:

- Add a sharded workflow actor/entity runner.
- Persist step transitions.
- Schedule effects through outbox.
- Resume recoverable inbox and due outbox.
- Add cancellation and terminal states.

Acceptance:

- A run survives actor restart and pod-like process restart in tests.
- A waiting human checkpoint resumes after a later command.
- Side effects are never executed before dispatching state is persisted.

### Slice C: Timers

Deliverables:

- Durable timer model.
- Timer scanner or sharded timer worker.
- Timer lease/fencing for multi-pod deployment.
- `TimerFired` command injection.

Acceptance:

- A workflow waiting on a timer resumes after restart.
- Duplicate timer firing is deduplicated by inbox key.
- Overdue timer metrics are emitted.

### Slice D: Dispatcher Fleet

Deliverables:

- Dispatcher runtime for due effects across workflows.
- Claiming or leasing protocol.
- Per-target concurrency limits.
- Dispatcher snapshots and metrics.

Acceptance:

- Multiple dispatcher workers do not intentionally dispatch the same claimed
  effect concurrently.
- Crash after dispatching state leads to recoverable retry.
- Exhausted effects are visible in metrics and snapshots.

### Slice E: Human Checkpoints

Deliverables:

- Human checkpoint state and approval command.
- HTTP/gRPC example for approval submission.
- Timeout/escalation policy.
- Audit events.

Acceptance:

- Checkpoint appears in operational snapshots.
- Approval resumes workflow.
- Timeout path escalates or fails according to policy.

### Slice F: Agent OpenTelemetry Observability

Deliverables:

- Stable workflow metric instruments and attribute definitions.
- Metric recording helpers that can feed the existing `MetricsRecorder` and an
  OpenTelemetry meter.
- OTLP exporter integration or an adapter path for metrics, traces, and logs.
- OpenTelemetry resource configuration helpers for Kubernetes deployments.
- Semantic convention mapping plus a Rakka-specific `rakka.*` attribute
  registry.
- Agent trace context, baggage allowlist, and span-link model.
- OpenTelemetry log event schema.
- Durable audit event schema with trace/log correlation ids.
- Collector example config for local development and Kubernetes.
- Snapshot providers.
- Testkit assertions for metrics, spans, span links, logs, resource
  attributes, and cardinality.

Acceptance:

- Example exposes workflow metrics on `/metrics`.
- Example exports OTLP metrics, traces, and logs to an OpenTelemetry Collector.
- OpenTelemetry bridge includes workflow metrics for compatibility.
- Spans connect public command, workflow step, effect dispatch, and completion
  through parent relationships or span links.
- Logs are correlated with trace id and span id when a current span exists.
- Resource attributes identify service, namespace, version, environment,
  Kubernetes pod, node, deployment, container, and Rakka node.
- Cardinality tests ensure raw ids are not used as hot metric labels.

### Slice G: Kubernetes Reference Deployment

Deliverables:

- Reference manifest or Helm-style plan.
- Pod startup/drain sequence.
- Autoscaling metric guidance.
- OpenTelemetry Collector DaemonSet and gateway Deployment guidance.
- OTLP endpoint, resource attribute, and exporter environment variable
  guidance.
- Collector processors for batching, memory limiting, Kubernetes attributes,
  redaction/filtering, and sampling where appropriate.
- PostgreSQL migration guidance.
- Network/security guidance.

Acceptance:

- Local dry-run validates required ports, probes, labels, env vars, and
  compatibility metadata.
- Local or integration test can route OTLP telemetry from a Rakka app to a
  Collector and inspect metrics, traces, and logs.
- Drain marks readiness false and runs workflow-aware shutdown steps.
- Rolling update compatibility checks include workflow entity routing.

### Slice H: Production Hardening

Deliverables:

- Retention and compaction.
- Workflow query index.
- Load tests.
- Failure injection tests.
- Operational dashboards and alert recommendations.

Acceptance:

- Large completed histories do not grow workflow snapshots without bound.
- Queries for waiting/failed/stuck workflows do not require full scans.
- Tests cover pod kill, process crash, PostgreSQL conflict, duplicate callback,
  duplicate human approval, timer replay, and rolling update.

## Reliability Semantics

Rakka agent workflows should document these guarantees:

- Accepted workflow commands are durably recorded before acknowledgement.
- Duplicate commands with the same message id or deduplication key do not create
  duplicate inbox work inside a workflow.
- Scheduled effects are durably recorded before dispatch.
- Dispatching state is durably recorded before external side effects start.
- Dispatch success, retry, timeout, and exhaustion are durably recorded.
- Workflow recovery can identify pending commands, in-progress commands,
  dispatching effects, failed retryable effects, and waiting checkpoints.
- Sharded workflow entities can move across pods and recover by stable
  persistence id.

Non-guarantees:

- External systems can still observe duplicate effects.
- Exactly-once side effects require idempotent downstream APIs, receipts,
  application reconciliation, or compensation.
- Human decisions can be submitted twice; the workflow must deduplicate them.
- Kubernetes drain can be interrupted; correctness must come from recovery.
- Timers can fire late or more than once; timer commands must be idempotent.

## Security and Policy

The agent workflow layer must not hide security decisions:

- Public APIs must authenticate and authorize workflow commands.
- Approval commands must verify the principal and policy before entering the
  durable inbox.
- Tool execution must use explicit allowlists and least-privilege secrets.
- Model adapters must redact or reference sensitive payloads according to
  application policy.
- Prompt, completion, and tool output retention must be configurable.
- Tenant isolation must be explicit in persistence ids, artifact refs, logs,
  metrics, and traces.
- Internal remoting must remain trusted-cluster traffic protected by Kubernetes
  network policy or equivalent controls.

## Testing Strategy

Unit tests:

- state transitions;
- deduplication;
- retry policy;
- timer due calculations;
- human checkpoint decisions;
- retention/compaction;
- metric label bounding.

Integration tests:

- durable workflow restart;
- sharded workflow movement;
- PostgreSQL durable state;
- PostgreSQL coordinator store and lease;
- duplicate callback handling;
- duplicate human approval handling;
- process tool crash and restart;
- HTTP/gRPC command ingress;
- OTLP metrics, traces, and logs reaching an OpenTelemetry Collector;
- Prometheus-compatible metrics exposition;
- trace context propagation across inbox, outbox, timers, process actors,
  callbacks, and human decisions;
- log correlation with trace id and span id;
- OpenTelemetry resource attributes on exported telemetry.

Failure-injection tests:

- crash after inbox acceptance;
- crash after effect scheduled;
- crash after marking dispatching but before side effect returns;
- crash after side effect returns but before success persistence;
- pod drain timeout;
- lease loss;
- stale coordinator writer;
- PostgreSQL revision conflict;
- remote delivery failure;
- model provider timeout;
- tool process restart-budget exhaustion.

Kubernetes tests:

- readiness fails during drain;
- compatibility rejection fails readiness;
- rolling update with N/N+1 schema policy;
- shard handoff during pre-stop;
- workflow resumes on replacement pod;
- autoscaling metrics are exposed;
- Collector DaemonSet and gateway Deployment configs route OTLP data and enrich
  it with Kubernetes attributes.

## Example Workflow

An approval-gated research workflow:

1. Client submits `StartRun` through HTTP.
2. API handler creates `InboxCommand` with request id as deduplication key.
3. Sharded workflow entity recovers or starts state.
4. Step runner schedules `ModelCall` effect for planning.
5. Dispatcher calls model provider and writes prompt/response artifacts.
6. Workflow receives `EffectCompleted`.
7. Workflow schedules `ToolCall` effect for retrieval.
8. Process or HTTP tool adapter executes retrieval.
9. Workflow opens `HumanCheckpoint` for plan approval.
10. Approval request outbox notifies a UI.
11. Workflow passivates while waiting.
12. Human submits approval through HTTP/gRPC.
13. Approval command enters durable inbox.
14. Workflow resumes, performs final model/tool steps, writes artifacts, and
    completes.
15. OpenTelemetry metrics, traces, logs, audit events, and snapshots show every
    boundary.

## OpenTelemetry Background References

The OpenTelemetry portions of this spec are based on the following official
documentation:

- `https://opentelemetry.io/docs/what-is-opentelemetry/`: OpenTelemetry as a
  vendor-agnostic framework for generating, collecting, and exporting telemetry
  while leaving storage and visualization to backend tools.
- `https://opentelemetry.io/docs/concepts/signals/`: supported signal model for
  traces, metrics, logs, baggage, and developing profile support.
- `https://opentelemetry.io/docs/concepts/context-propagation/`: propagation of
  trace and span context across process and network boundaries using W3C Trace
  Context by default.
- `https://opentelemetry.io/docs/concepts/resources/`: resource attributes such
  as service, process, container, pod, namespace, and deployment metadata.
- `https://opentelemetry.io/docs/concepts/semantic-conventions/`: common
  operation and attribute names across traces, metrics, logs, profiles, and
  resources.
- `https://opentelemetry.io/docs/collector/`: Collector architecture for
  receiving, processing, and exporting traces, metrics, and logs.
- `https://opentelemetry.io/docs/platforms/kubernetes/getting-started/`:
  Kubernetes Collector topology using DaemonSet and Deployment collectors.
- `https://opentelemetry.io/docs/languages/rust/`: Rust API, SDK, OTLP,
  Prometheus, and semantic convention crates relevant to a Rakka implementation.

## Open Questions

- Should the agent workflow layer live in `rakka-workflow`, a new
  `rakka-agent-workflow` crate, or application templates first?
- Should workflow history be event-sourced from the start, or should v1 extend
  durable snapshots with retention and indexes?
- Which PostgreSQL schema should own workflow indexes and audit events?
- What is the minimum timer model needed before adding a full scheduler?
- Should dispatch claiming be per-workflow, per-shard, or global with database
  leases?
- How much of model/tool adapter policy belongs in Rakka versus application
  code?
- What is the right default retention policy for prompts and completions?
- Should raw workflow ids be allowed in traces by default while forbidden in
  metrics?
- Should Rakka ship a feature-gated OpenTelemetry SDK integration, an adapter
  API that applications wire to their own SDK, or both?
- Which model/tool semantic convention version should be adopted first, and how
  should Rakka handle convention changes over time?
- What deployment artifact comes first: raw manifests, Helm chart, or operator
  design?

## Summary

Rakka already has the core runtime substrate for durable agentic workflows:
typed actors, sharded identity, durable state, inbox/outbox reliability,
PostgreSQL persistence, process ownership, bounded streams, Kubernetes
operation hooks, metrics, tracing, and snapshots.

The next step is to make agent workflow orchestration first-class: define runs,
steps, effects, human checkpoints, timers, dispatcher fleets, workflow queries,
retention, audit events, and OpenTelemetry-native workflow observability. The
design should continue Rakka's current philosophy: keep failure modes explicit,
make durable semantics opt-in and visible, preserve bounded runtime behavior,
emit complete telemetry through standard protocols, and let applications and
operators choose their production security and observability backends.
