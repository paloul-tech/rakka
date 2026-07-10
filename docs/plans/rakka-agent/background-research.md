# Rakka Agents Background Research

Status: research record
Research date: 2026-07-10
Related guidance: [technical-guidance.md](technical-guidance.md)
Emerging specification: [spec.md](spec.md)

## Purpose

This document records the research behind a proposed Rakka crate for durable,
sharded, autonomous agents. It separates source observations and reliability
constraints from the recommendations in the guidance document and the
normative requirements beginning to form in the specification.

The target is an agent that:

- pursues an explicit goal and persists logically until that goal reaches an
  evidenced terminal outcome;
- uses Rig as its language-model abstraction;
- plans, decides, reasons, and invokes tools and/or other agents through
  its own agentic loop;
- collaborates concurrently with specialized agents, tools, and durable
  workflows in a shared environment;
- has a stable sharded identity, typed durable tasks, and independently durable
  execution runs;
- survives dispatcher restart, pod loss, passivation, and shard movement;
- does not automatically duplicate an opaque non-idempotent effect after an
  unknowable crash window;
- supports durable human approval, security authorization, and operator
  reconciliation gates;
- maintains session, agent-private, and communal memory; and
- exposes agent-to-agent and external interaction through `rakka-a2a` and the
  A2A protocol.

## Research Scope

The research covered:

- Rakka's actor, sharding, workflow, dispatcher, checkpoint, persistence, and
  A2A documentation and implementation;
- the OpenFang comparison under `docs/comparisons/openfang/` and the linked
  OpenFang implementation areas;
- Akka SDK's Autonomous Agent documentation, public Java API/Javadocs, and the
  official `autonomous-agent-playground` implementation samples;
- the supplied "always-on" autonomous-agent article and its publicly readable
  companion post;
- Rig's agent-run, conversation-memory, memory-policy, and vector-store
  integration surfaces;
- the A2A task lifecycle and in-task authorization model;
- OpenTelemetry traces, metrics, logs, context propagation, GenAI semantic
  conventions, sampling, sensitive-data guidance, and Collector deployment;
- PostgreSQL plus `pgvector` for scoped semantic memory; and
- database-neutral storage requirements for a shared knowledge graph.

This is design research, not a statement that the proposed `rakka-agent` API
or storage adapters already exist.

## Executive Findings

1. Rakka already contains most of the distributed correctness substrate. The
   missing layer is an agent-domain runtime that turns a Rig model/tool loop
   into small, durable transitions and effects.
2. The existing human-checkpoint runtime already models a wait as durable
   state, schedules notification through the outbox, accepts decisions through
   the deduplicating inbox, and allows the actor to become idle and passivate.
3. An opaque non-idempotent external effect cannot be guaranteed exactly once
   across a crash after the external system commits but before Rakka records
   the result. The safe Rakka behavior is to stop autonomous progress and mark
   the effect indeterminate, not retry it automatically.
4. Rig is a good LLM/provider/tool abstraction and supplies useful loop and
   memory-policy components. Rakka must remain the owner of durable loop state,
   memory scopes, idempotent writes, recovery, and effect dispatch policy.
5. OpenFang's loop behavior and memory domain are useful requirements input,
   but its single-daemon SQLite storage architecture is not a multi-pod source
   of truth for Rakka.
6. Memory must be split by ownership and consistency requirements. Session
   transcript state, agent-private semantic memory, and a communal knowledge
   graph are different domains and should not share one implicit key space.
7. The communal knowledge-graph contract must remain independent of any
   database vendor, query language, licensing model, or managed service.
8. A2A has distinct interrupted states for additional input and authorization.
   Rakka should preserve this distinction instead of presenting every gate as
   generic human input.
9. Rakka already has substantial OpenTelemetry-oriented workflow support, but
   the new agent layer still needs one coherent session correlation model,
   GenAI semantic-convention mapping, decision telemetry, and production
   sampling/content policy.
10. Goal-driven multi-agent execution needs a stable goal identity above any
    one agent run. Specialized child agents and workflows may finish their own
    tasks while the root goal remains active, waiting, or under evaluation.
11. Akka's implementation model demonstrates that a durable typed task should
    be distinct from the agent execution currently assigned to it. Rakka needs
    `AgentTaskId` above `AgentRunId` to preserve one public task across handoff,
    reassignment, and multiple specialized execution sessions.
12. Akka's declared delegation, handoff, team, and moderation capabilities are
    a useful capability vocabulary. Rakka should compile equivalent contracts
    into its durable task, A2A, graph, inbox/outbox, and passivation substrate.
13. The full always-on article independently supports treating autonomy as the
    durable operating model around a loop, but its pod-started daemon
    interpretation is incompatible with Rakka's logical-availability and
    passivation contract.
14. Continuous goals should be stable controllers over finite child epoch
    tasks/runs, with versioned durable wakes, explicit overlap/missed-occurrence
    policy, and no schedule/budget reset on runtime restart.
15. Budgets, autonomy admission, tool authority, executor isolation, and
    authoritative operational queries are correctness/safety contracts rather
    than prompt or telemetry conventions.
16. Task materialized state must remain bounded and separate from append-only
    history, content/artifacts, scoped memory, and derived projections.

## Goal-Driven and Multi-Agent Findings

### Useful "Always-On" Vocabulary

The full referenced LinkedIn article was reviewed from the project owner's
saved 12-page PDF after the original page was inaccessible to the initial
research environment. The article argues that autonomy is the operating model
around an agentic loop, not the loop itself. Once work continues without a
synchronous caller, the system must own task identity, lifecycle, output,
error handling, cancellation, retention, audit, permissions, memory,
resumability, budgets, and observability.

The article therefore provides strong practitioner support for the Rakka
boundary already selected: Rig supplies model-facing reasoning and tool
abstractions, while Rakka supplies the durable distributed-systems operating
model. The article is directional evidence rather than a normative standard;
its conclusions were checked against the current A2A, Kubernetes, and
OpenTelemetry primary documentation before being carried into the guidance and
specification.

That vocabulary fits Rakka if "always-on" describes logical availability and
durable intent rather than a permanently running process. A Rakka Agent is
always addressable and recoverable while active, but may have no resident actor
or task between observations, delegated work, timers, and HITL gates.

This distinction is foundational:

> **“Always-on” means logically addressable and recoverable, not a resident
> thread or process.**

An active goal or run is durable domain state, not evidence that code is
currently executing. During a quiescent period, a Rakka Agent should consume no
per-agent thread, Tokio task, actor instance, child process, network
connection, dispatcher lease, or open trace span. It may be represented only
by durable records, durable timer/outbox entries, shared indexes, and bounded
fleet-level infrastructure.

The always-on promise is instead:

| Logical service property | Required meaning |
| --- | --- |
| Addressable | The stable tenant/agent/goal/task/run identity can receive an authorized command regardless of the last pod or shard owner |
| Durable | Accepted state, waits, effects, deadlines, and deduplication survive loss of the current runtime process |
| Reactivatable | A2A ingress, a durable inbox item, timer, callback, child result, or administrative command can activate the current shard owner |
| Recoverable | The new activation reconstructs the next legal transition from shared durable state and rejects stale ownership/results |
| Long-lived | The logical lifecycle can span months or years without requiring a correspondingly long-lived OS or async resource |

Passivation is therefore the expected steady state for an agent waiting on the
world, not an exceptional suspension of service. Actor residency is a cache of
currently useful computation. It is neither agent identity nor proof of
liveness or correctness.

This also changes how continuous agents observe their environment. They wake
from durable timers or application events, execute one bounded observation or
decision epoch, persist the next effect/wait, and become passivatable again.
They do not maintain an immortal polling loop.

### Full-Article Technical Review

The complete article adds six findings that materially reinforce the proposed
architecture:

| Article finding | Rakka consequence |
| --- | --- |
| Long-running autonomy needs a durable unit of work | Keep `AgentTaskId` distinct from `AgentRunId` and map the task to A2A `Task.id` |
| Continuous monitoring and bounded asynchronous work need different controls | Model a continuous goal as a durable controller that admits finite child epoch tasks/runs |
| Budgets constrain excessive agency, not only cost | Use durable hierarchical limits for iterations, model/tool calls, effects, tokens, cost, time, concurrency, and descendants |
| Task state and memory are separate domains | Keep lifecycle state small and bounded; store conversations, observations, tool history, and learned context in scoped memory/artifact stores |
| Model-visible tools are not an authority boundary | Bind dispatch to current capabilities, credentials, approvals, execution identity, network/sandbox policy, and effect safety |
| Debugging autonomous fleets requires task, budget, tool, cancellation, memory, and trace views | Provide authoritative operational queries independently from sampled telemetry, plus linked OpenTelemetry signals |

The article's distinction between continuous execution and asynchronous task
execution is useful, but its daemon-like description of a loop that begins
when a pod starts is rejected. Pod creation is a deployment event, not an
agent-domain wake. A pod start, restart, replacement, rollout, or shard move
must not create an epoch, reset a schedule, reset a budget, or alter logical
agent lifetime.

The recommended continuous model is instead:

```text
stable continuous goal/root control task
    -> durable wake occurrence
    -> finite child AgentTaskId
    -> finite AgentRunId with isolated short-term memory
    -> result/evidence returned to the continuous goal
    -> next durable wake or retirement
```

This makes every epoch an ordinary recoverable unit of work while the
continuous goal may remain logically active and fully passivated for months or
years. Durable timer/event records, not resident computation, provide future
wakes.

### Wake, Overlap, and Missed-Occurrence Findings

Kubernetes `CronJob` controls demonstrate that overlap and missed occurrences
are explicit scheduling policy rather than safe implementation defaults.
Rakka should reuse that vocabulary without using a `CronJob`, pod-local timer,
or pod start as the agent scheduler.

The initial Rakka policy should:

- attach a monotonic schedule revision to every wake occurrence;
- derive a stable wake/deduplication identity from the goal, revision, and
  logical occurrence;
- forbid overlapping epochs by default and durably coalesce triggers received
  while one epoch is active;
- admit one coalesced epoch after downtime by default rather than replaying an
  unbounded backlog;
- fence pending wakes from obsolete schedule revisions; and
- make bounded replay or parallel epochs explicit opt-in policy.

The current `rakka-agent-workflow` timer substrate already persists one-shot
timer identity, target run, due time, deduplication key, telemetry context,
status, and optional maximum lateness. The agent layer needs a durable wake
controller above that primitive, not a second resident scheduling system.

### Budget and Admission Findings

The current autonomy policy records autonomous steps, external calls, tokens,
and wall-clock start. That is a useful foundation but not yet a multi-scope
safety ledger. Agent budgets need definition ceilings, goal allocations,
task/epoch allocations, run allocations, and turn/effect reservations.

An effect that reaches durable `Started` consumes an attempt even if the
worker later disappears and the outcome becomes indeterminate. An idempotent
retry also consumes an attempt. Continuous budget refills must be defined by a
durable logical window or schedule revision and must never occur because a pod
or actor restarted.

The article's negative suitability criteria also support a fail-closed
autonomy-admission step. An unattended definition should not be admitted
without inspectable progress, cancellation, bounded cost/effects, scoped tool
authority, success or health criteria, and approval/reconciliation policy.
Rakka should persist and enforce the admission decision; the application owns
the industry-specific risk rules.

### Tool Authority and Workload Isolation Findings

Kubernetes assigns ServiceAccounts and network policy to workloads, not to
individual logical actors inside a shared process. Therefore a model-visible
tool schema, an allowed Rakka capability, and the workload identity that can
actually reach the target are three distinct layers.

Shared dispatcher pods with broad ambient credentials would weaken isolation
even if Rakka performs correct software authorization. Production deployments
should be able to route effects to trust-tier dispatcher pools or ephemeral
effect sandboxes with constrained workload identity, credentials, and network
reachability. This is effect-executor isolation, not a pod per logical agent.

Kubernetes NetworkPolicy is useful only when the selected network plugin
enforces it and generally controls network-layer reachability rather than
application authorization. The Rakka contract should consequently carry a
logical execution-policy reference without claiming to implement the
application's sandbox, identity provider, service mesh, or policy engine.

### Bounded State and Operational Query Findings

Task ownership does not require embedding unbounded history in the task
entity's materialized state. The research supports four separate domains:

1. bounded authoritative task/run state needed for the next transition;
2. append-only durable domain events and audit history;
3. content, artifacts, observations, and scoped memory; and
4. derived list/search/observability projections.

Current A2A 1.0 documentation supports bounded task history, list pagination,
optional artifact inclusion, caller-scoped visibility, and explicit task
cancellation. Cancellation remains an attempt and is not proof that an
external operation stopped. For Rakka, a cancellation request must fence new
work immediately, but a task with an ambiguous non-idempotent effect should
remain nonterminal in reconciliation rather than falsely reporting safe
cancellation.

An authorized operational snapshot must be reconstructable from durable state
even when the entity is passivated, trace sampling discarded routine spans,
or the telemetry exporter is unavailable. OpenTelemetry supplies timing,
correlation, aggregation, and investigation; it does not replace the task,
run, budget, effect, checkpoint, timer, or event sources of truth.

### Goal Versus Agent Versus Run

The design needs four distinct identities:

| Identity | Meaning |
| --- | --- |
| `AgentId` | Stable specialized service identity, settings, policy, tools, and private memory |
| `AgentGoalId` | Stable top-level objective shared across one collaboration |
| `AgentTaskId` | Stable typed unit of work and public A2A task, preserved across handoff |
| `AgentRunId` | One agent's independently durable execution session contributing to a task/goal |

For the first implementation, the stable root `AgentTaskEntity` can own the
goal-coordinator state and `AgentGoalId` may default to the root `AgentTaskId`.
The current finite root or continuous-epoch run proposes decisions against
that state. Delegated children receive their own `AgentTaskId` and `AgentRunId`.
A handoff preserves
`AgentTaskId` but creates a new target-agent `AgentRunId`, leaving the source
run as immutable lineage rather than changing its agent/memory scope.

Keeping `AgentGoalId` as a distinct type prevents a future architecture from
assuming that one typed task/execution run and one multi-agent goal are always
the same thing. It also lets operators reconstruct all collaborating tasks and
runs without using one process, actor path, or trace ID as the goal identity.

### Goal Completion Needs Evidence

A goal-driven agent must not mark itself complete merely because the model says
it is done or one turn produced a plausible answer. The goal needs explicit,
versioned success criteria and a completion evaluation with evidence.

Evaluation may be:

- deterministic validation;
- an authoritative external query;
- a compiled verification workflow;
- model-assisted evaluation under a distinct policy; or
- HITL approval for high-impact or subjective goals.

The root goal remains durable until it is satisfied, cancelled, expired, or
ended as failed/unsatisfied under policy. Budget exhaustion should pause for
authorization, escalation, or fail-closed termination; it should not produce an
unbounded loop.

Continuous monitoring goals need an explicit mode because they have no single
final satisfaction instant. They evaluate bounded epochs and remain active
until cancellation, expiry, or policy termination, using durable timers rather
than a resident polling loop.

### Concurrent Collaboration

Specialization belongs in each agent definition and A2A Agent Card/skills.
Collaboration is then a durable delegation graph:

```text
root goal / root AgentRunId
    |
    +-- delegation -> specialist AgentRunId A
    |                     +-- tool effects
    |
    +-- delegation -> specialist AgentRunId B
    |                     +-- compiled workflow tool
    |
    +-- durable fan-in -> evaluate goal evidence
```

Each delegation is an A2A effect with a stable `AgentDelegationId` and
idempotent message/task identity. The parent persists the delegation before
sending, waits durably, may passivate, and accepts child progress/result
messages through the deduplicating inbox. It does not call another agent
through public actor remoting.

The collaboration graph needs maximum depth, fan-out, active-child, token,
cost, deadline, and external-effect budgets. Ancestor lineage and stable
delegation IDs are required to detect accidental delegation cycles and replay.
Cancellation and deadline propagation need an explicit policy; a child must
not silently continue privileged work after its parent goal has terminated.

### Shared Environment and Collective Memory

"Shared environment" should mean an application-defined environment/resource
scope, authorized knowledge spaces, and external systems observed through
tools. It must not mean unsynchronized mutable memory inside multiple actors.

Agents collaborate through:

- immutable task/result artifacts;
- A2A messages and task state;
- append-only communal knowledge claims with provenance;
- application-owned environment tools and events; and
- explicit coordination resources when mutually exclusive action is required.

Agent-private memory remains private to `AgentId`. Collective memory is the
communal `KnowledgeSpaceId` graph plus shared artifacts, not automatic access
to every agent's private memory. Concurrent graph appends use stable operation
IDs, and conflicting claims remain visible rather than being overwritten.

### Workflows as Tools

A compiled Rakka workflow can be presented in an agent's tool catalog through
a `WorkflowToolDescriptor`. Invoking it should create or address a durable
child workflow run with stable input, output, idempotency, deadline,
cancellation, capability, and trace correlation.

The workflow must not be wrapped as one opaque dispatcher call. Its internal
model/tool/non-idempotent effects retain their normal durable boundaries. A
parent agent waits for the child workflow outcome through a durable callback or
command and may passivate in the meantime.

Rakka already has useful substrate for this direction:

- autonomy policy and bounded step/call/token/wall-clock budgets;
- A2A peer and child-workflow target/trigger concepts;
- compiled graph fan-out/fan-in and bounded iteration;
- deterministic child-workflow effect identity; and
- durable inbox/outbox, timers, checkpoints, cancellation, and recovery.

One concrete integration gap remains: the dispatcher and trigger domains have
first-class child-workflow concepts, but the current autonomy classifier maps
`AgentDispatchTargetClass::ChildWorkflow` to the generic `Other` policy class.
A workflow exposed as an agent tool needs a first-class autonomy target with
explicit budgets, capabilities, approval policy, and observability before it
is admitted as production agent behavior.

The missing layer is a goal/delegation domain that composes those primitives
without moving product-specific routing, team formation, or evaluator policy
into `rakka-agent-workflow`.

## Rakka Baseline

### Reliability Boundary

Rakka core actors, remoting, and sharding are at-most-once by default. Durable
state plus the workflow inbox/outbox add accepted-command deduplication,
persisted effect intent, retry policy, recovery, and revision fencing. This is
the correct substrate for an agent loop, provided that every effectful tool or
model operation crosses the durable effect boundary.

The existing agent workflow design explicitly does not claim exactly-once
effects against arbitrary external systems. See:

- [Rakka v1 reliability boundaries](../../rakka-v1-reliability-boundaries.md);
- [agentic workflow specification](../agentic-workflow/agentic-workflow-spec.md);
  and
- [compiled graph scheduler specification](../compiled_execution_with_graph_schdlr/compiled-execution-with-graph-scheduler-spec.md).

### Durable Human Checkpoints

The agentic workflow specification describes the desired human pause as:

1. persist `waiting-for-human`;
2. schedule a `HumanApprovalRequest` outbox effect;
3. deliver the request to an application-owned human surface;
4. allow the workflow actor to become idle and passivate;
5. accept the later decision through a durable inbox with a deduplication key;
   and
6. recover or reactivate the sharded entity to resume the run.

The implementation in
[`checkpoints.rs`](../../../crates/rakka-agent-workflow/src/checkpoints.rs)
already provides the durable checkpoint facade, duplicate decision handling,
escalation, timeout support, and metrics. The run engine persists
`WaitingForHuman`, while the A2A adapter currently maps that state to
`TaskState::InputRequired`.

### Passivation and Shard Movement

The sharded A2A host states that idle entities passivate and recover durable
run, inbox, and projection state lazily on the next access. Its default idle
passivation period is 120 seconds. This is compatible with approval pauses
lasting hours or days because the wait is data, not a resident future, thread,
process, stream, or mailbox.

Passivation is a resource-management mechanism, not a correctness mechanism.
Recovery correctness still depends on durable state, revision checks,
idempotent command/effect acceptance, and fenced dispatcher ownership.

### Current Gaps Relevant to Rakka Agents

| Area | Existing foundation | Agent-layer gap |
| --- | --- | --- |
| Stable identity | Sharded entity and durable run IDs | Explicit `AgentId`, `AgentGoalId`, `AgentTaskId`, `AgentRunId`, and delegation ownership model |
| Agent loop | Durable steps, effects, graph runs | Rakka-owned autonomous turn state, progress evaluation, and evidence-backed completion using Rig |
| Collaboration | A2A effects plus graph fan-out/fan-in | Durable delegation lineage, specialist resolution, goal budgets, and workflow tools |
| Human pause | Durable human checkpoints | Typed approval, authorization, and reconciliation gates |
| A2A task state | `WaitingForHuman` to `InputRequired` | First-class `AuthRequired` and indeterminate reconciliation projection |
| Dispatcher | Durable outbox, lease, retry, result acceptance | First-class indeterminate effect outcome and safety-class policy |
| Settings | Durable run state and application configuration | Versioned agent settings with defined mid-run update semantics |
| Memory | Artifact references and correctness state | Scoped session, private semantic, and communal graph stores |

## Akka Autonomous Agent Findings

The detailed cross-project evaluation is in
[Rakka and Akka Technical Comparison](../../comparisons/akka/rakka-akka-technical-comparison.md).
The findings here focus on contracts that should influence `rakka-agent`.

### Public Implementation Surface Reviewed

Akka's proprietary runtime internals were not treated as inspectable source.
The research used its public SDK contracts, Javadocs, documentation, and
official sample implementation. Those surfaces show a coherent component
model:

| Akka surface | Public behavior | Rakka implication |
| --- | --- | --- |
| `AgentDefinition` | Declares outcome description, accepted tasks, tools, model, guardrails, iteration limits, and coordination capabilities | Add a versioned Rakka definition contract compiled into durable runtime policy |
| `AgentSetup` | Supplies per-instance instructions and capabilities before work starts | Add a per-run setup revision that may narrow/parameterize the definition but never bypass policy |
| `TaskDefinition<R>` / `TaskEntity` | Gives work a stable identity, typed result, dependencies, attachments, assignment, rejection, and terminal lifecycle | Add a first-class `AgentTaskId` and typed task definition above run state |
| `TaskAcceptance` | Limits which task types an agent may process and their iteration budget | Make accepted task contracts explicit agent capabilities |
| component/function/MCP tools | Exposes functions, entities, workflows, views, and remote tools to the loop | Keep tool kinds behind Rakka effect and credential boundaries; workflows stay durable child runs |
| coordination capabilities | Generates runtime-mediated tools for delegation, handoff, teams, and moderation | Define equivalent Rakka capability descriptors rather than prompt-only conventions |
| `ComponentClient` | Starts agents, assigns/queries tasks, applies setup, suspends/resumes, and retrieves typed results | Provide a typed facade implemented over Rakka's durable commands and A2A boundary |
| notifications | Emits lifecycle, task, coordination, and struggle signals | Preserve Rakka's stronger replayable event/query direction; never use notifications as correctness state |
| `TestModelProvider` | Scripts deterministic responses and tool calls | Add a Rig-backed deterministic model/tool test harness plus crash injection |

Akka's public Javadocs expose `TaskEntity`, shared `BacklogEntity`, and
`SessionMemoryEntity` as event-sourced entities. The backlog API documents
atomic task claiming. This validates using independently durable task state,
claim fencing, and event/history projections rather than embedding a mutable
task list inside one live agent loop.

More specifically, the public `TaskEntity` extends an event-sourced entity over
`TaskState`/`TaskEvent` and exposes create, assign, start, complete,
reject-result, fail, cancel, reassign, notification, and query commands. Its
event application is recovery-safe and side-effect-free. Rakka should mirror
that command/state separation while using its own Rust types, inbox
deduplication, persistence, and A2A projection.

### Task Identity Is Not Run Identity

Akka's samples make the distinction operational:

- delegation creates a new child task while the coordinator retains its parent
  task;
- handoff transfers the same task to a new agent owner;
- a team exposes a shared backlog whose members atomically claim tasks; and
- human input can complete an unassigned typed task that unblocks dependents.

The previous Rakka draft mapped `AgentRunId` directly to A2A `Task.id`. That is
too restrictive for handoff because a run is scoped to one `AgentId` and owns
that agent's short-term memory. The revised identity model should be:

```text
AgentGoalId
    +-- AgentTaskId (stable public work/result identity)
          +-- AgentRunId A (source-agent execution session)
          +-- AgentRunId B (handoff/reassignment execution session)
```

`AgentTaskId` maps to A2A `Task.id`. `AgentRunId` remains an internal durable
execution/session identity and keeps short-term memory scoped by
`(TenantId, AgentId, AgentRunId)`. A handoff therefore does not mutate an
existing run's `AgentId` or expose its private/session memory implicitly.

Akka processes one task at a time in one autonomous-agent instance and obtains
parallelism through other instances. Rakka should preserve serialization at
the `AgentRunEntity` boundary without serializing the entire stable `AgentId`:
one run works one task, while an agent service may own multiple concurrent runs
under configured concurrency policy.

### Four Coordination Patterns

Akka's capability vocabulary covers four materially different state machines:

1. **Handoff:** preserve one task, transfer execution ownership, and stop the
   source agent from completing it.
2. **Delegation:** create child tasks/runs, retain the parent, then fan results
   back in.
3. **Team:** create a bounded membership plus a durable shared task board;
   members claim tasks atomically and exchange mediated messages.
4. **Moderation:** persist a bounded participant set, turn schedule, transcript,
   round/iteration limits, and moderator completion decision.

These should not be four prompt templates. Each requires durable identities,
authorization, budget enforcement, cancellation, recovery, passivation, and
observable transitions. Rakka can compile them into its existing graph,
inbox/outbox, A2A, timer, and sharding primitives.

### Typed Results, Dependencies, and Human Tasks

Akka task definitions declare the expected result type and may apply result
rules. Rejected model results remain visible as task/struggle events and can
consume bounded additional iterations. Task dependencies provide deterministic
ordering and propagate failure/cancellation.

Rakka should adopt typed input/result schema references and deterministic
result rules. A model may propose completion, but the task runtime validates
the result before accepting it, while root goal satisfaction remains a
separate evidence-based decision.

Akka also demonstrates a useful HITL distinction: a human can own and complete
an unassigned typed task in a dependency graph. Rakka should support this for
human work products, while retaining `AgentCheckpoint` for approval,
authorization, and indeterminate-effect decisions that must be bound to an
exact effect intent.

### Static Definition, Dynamic Setup, and Guardrails

Akka keeps tools, model provider, and guardrails in the static definition while
allowing per-instance instructions and capabilities. Rakka needs more lifecycle
configuration flexibility, but should retain the security principle:

- a durable setup/settings revision may select or narrow instructions,
  task acceptance, budgets, and capabilities within an authorized envelope;
- it must not introduce an undeclared tool, weaken a mandatory guardrail,
  select an unapproved model, or expand credential access; and
- any later settings change follows the existing next-turn, immediate-safety,
  or explicit-migration timing rules.

Request/response guardrails are worth adopting as first-class ordered policy
stages around model, tool, retrieval, and protocol boundaries. They remain
distinct from effect idempotency, authorization, and credential resolution.

### Deliberate Rakka Divergences

Akka's public client documentation says explicitly managed autonomous agents do
not automatically passivate when their queue drains; they retain in-memory
state until terminated, while suspend can release the actor. Rakka should not
copy this residency behavior. Its stronger invariant remains:

> **"Always-on" means logically addressable and recoverable, not a resident
> thread or process.**

Every quiescent Rakka Agent should auto-passivate regardless of whether it may
receive more tasks later. `Suspend` controls whether new work may start;
passivation only evicts runtime state. `Terminate` is a durable lifecycle
decision, not a resource-management requirement.

Akka notifications are live and non-replayable, whereas Rakka should preserve
durable task/run event cursors and explicit resynchronization. Akka's public
autonomous-agent material also does not establish exactly-once behavior for
arbitrary function, MCP, or model effects. Rakka must retain its explicit
`Indeterminate` stop for ambiguous non-idempotent effects.

## OpenFang Findings

The detailed source comparison is in
[Rakka and OpenFang Technical Comparison](../../comparisons/openfang/rakka-openfang-technical-comparison.md).

### Capabilities Worth Learning From

OpenFang provides a complete agent application/runtime surface including an
agent loop, provider drivers, tools, sessions, context management, semantic
memory, knowledge graphs, channels, capabilities, and autonomous behavior.
Its loop includes valuable behaviors such as:

- context budgeting and repair;
- model and tool iteration;
- loop detection;
- retry handling;
- streaming;
- session persistence; and
- integration with skills, MCP, browser, and other runtime contexts.

These behaviors inform the Rakka Agent requirements. They should not be copied
as one opaque dispatch operation.

### Critical Execution-Granularity Finding

If a complete multi-turn OpenFang loop is executed as one Rakka effect, a late
worker crash can replay tool calls already performed. A safe integration must
pause the loop at each model or tool boundary:

```text
durable loop transition
    -> durable model/tool effect
    -> dispatcher invocation
    -> durable result command
    -> recovered loop transition
```

Read-only and externally idempotent tools may be retried under policy.
Effectful tools require a stable effect ID, explicit safety class, and either
an external idempotency key, an authoritative reconciliation protocol, or an
indeterminate stop after an ambiguous crash.

### Memory Finding

OpenFang's `MemorySubstrate` combines structured memory, semantic search,
knowledge graph behavior, sessions, consolidation, and usage tracking around a
shared SQLite connection. That is appropriate for a single daemon and useful
for domain-model study, but it is not suitable as the authoritative store for:

- multiple concurrent Rakka pods;
- shard movement and pod replacement;
- cross-node session recovery;
- revision-fenced multi-writer access; or
- high-availability failover.

The reusable part is the memory model and algorithms. Rakka needs shared store
traits, tenant-aware keys, revision checks, idempotent writes, and distributed
adapters.

## Rig Findings

Rig was reviewed as the preferred LLM abstraction for the new agent layer.

### Agent Run

Rig's `AgentRun` exposes a serializable, sans-I/O step machine that separates
model calls, tool calls, and completion. This is a strong adapter boundary for
turn execution because Rakka can persist its own state before dispatching the
requested I/O.

Rig's serialized run representation is not documented as a stable
cross-version persistence format. Rakka should therefore persist a
Rakka-owned, versioned loop intermediate representation and reconstruct the
required Rig request at dispatch time.

### Conversation Memory

Rig's `ConversationMemory` loads an ordered message history by a string
conversation ID and appends messages after a successful turn. It does not
define Rakka's tenant, agent, or run scoping. Rakka must construct and enforce
the composite scope rather than trusting a caller-generated conversation ID.

Rig's memory demotion and compaction hooks warn implementers to make downstream
operations idempotent because in-process watermarks are not durable and a
restart may deliver the same evicted messages again. This reinforces the need
for Rakka-owned `MemoryOperationId` values and durable outbox writes.

### Useful Rig Components

- `rig-core` supplies the core agent, model, tool, vector-store, and
  conversation-memory abstractions.
- `rig-memory` supplies sliding-window, token-window, demotion, and compaction
  policies that can help shape prompt history.
- Rig offers multiple store-specific vector integrations. These are useful
  implementation adapters, but none defines Rakka's communal knowledge-graph
  provenance, conflict, authorization, or verification rules.

Rakka should use these as algorithms and adapters, not delegate durable agent
correctness to them.

## A2A Findings

The A2A protocol models a long-running interaction as a `Task`. Its current
task lifecycle includes both:

- `TASK_STATE_INPUT_REQUIRED`, for additional user/client input; and
- `TASK_STATE_AUTH_REQUIRED`, for authorization needed during a task.

The in-task authorization section gives human approval before a destructive
action and acquiring an OAuth credential as example use cases. It recommends
that credentials be delivered out of band unless an explicit in-band extension
has been negotiated, and that credentials be bound to the originating agent.

The current public A2A documentation identifies 1.0 as the latest release. It
adds/clarifies task listing with pagination/filtering, bounded history and
artifact inclusion, caller-scoped visibility, timestamps, subscription, and
cancellation behavior. `CancelTask` attempts cancellation but does not promise
that already-started external work stopped. Rakka therefore needs a pinned
protocol version and a richer internal cancellation/reconciliation state than
the public terminal task enum alone can express.

For Rakka Agents this implies:

- `AgentTaskId` is the natural durable identity for A2A `Task.id`, while
  `AgentRunId` identifies one assignee's execution session;
- ordinary approval and indeterminate reconciliation project as
  `InputRequired`;
- credential or capability acquisition projects as `AuthRequired`;
- task status may carry a structured, non-secret gate description;
- resolved credentials must not be stored in task history, durable effects,
  prompts, logs, or snapshots; and
- agent-to-agent calls are external effects sent through the Rakka outbox, not
  direct actor remoting exposed as a client protocol;
- cancellation requests fence new work but remain nonterminal while an
  indeterminate consequential effect requires reconciliation; and
- A2A list/history/artifact projections remain bounded views rather than task
  correctness or memory stores.

## Why an Indeterminate State Is Necessary

Consider an external operation with no idempotency key and no outcome query:

1. Rakka durably records that dispatch is starting.
2. The worker invokes the external system.
3. The external system commits the operation.
4. The worker or pod fails before Rakka durably accepts the receipt.

After recovery, Rakka cannot distinguish that sequence from a failure between
steps 1 and 2. Retrying may duplicate the effect; treating it as successful may
invent a result that never occurred.

No actor, queue, transaction log, or dispatcher lease can remove this
uncertainty unless the external system participates through one of:

- an idempotency key;
- a transaction coordinated with Rakka's durable state;
- an authoritative status/reconciliation API; or
- an application-specific receipt or deduplication protocol.

The honest behavior for an opaque non-idempotent effect is therefore:

- never invoke it before a durable `Started` transition;
- never automatically retry it after `Started` becomes ambiguous;
- persist the effect as `Indeterminate`;
- stop autonomous run progress;
- open an operator reconciliation checkpoint; and
- require evidence before recording completion or creating a new effect
  generation.

This is stronger and more useful than claiming exactly-once behavior the
runtime cannot provide.

## Memory Storage Findings

### PostgreSQL and pgvector

PostgreSQL is a good initial authoritative store for short-term and
agent-private memory because it provides transactions, uniqueness constraints,
revision compare-and-set, tenant filtering, backup/recovery, and joins with
existing Rakka persistence data.

`pgvector` adds exact and approximate vector search, including HNSW and
IVFFlat. Approximate search with filters needs deliberate indexing and recall
testing because filtering may occur after the approximate index scan. Tenant
and `AgentId` filters must be included in both schema and query design; index
performance must not become a reason to weaken access boundaries.

### Graph Backend Portability

Neo4j was surveyed as one property-graph implementation, but it has commercial
licensing and paid product offerings and must not become a required dependency
or the name of Rakka's communal graph crate. Selecting it would be a deployment
choice with licensing, cost, migration, and operational consequences.

The communal graph boundary must support interchangeable implementations such
as relational graph tables, property-graph databases, RDF/triplestores,
embedded or in-memory test stores, and managed graph services. Public Rakka
types must not expose vendor clients, Cypher, SQL, SPARQL, or vendor-specific
identifiers.

Regardless of backend, the adapter does not supply the application semantics
of truth. Rakka still needs immutable claims, stable append operation IDs,
provenance, trust state, retraction, access control, and promotion policy.

### Derived Indexes Versus Correctness

Embeddings and graph/vector search indexes may be eventually consistent. They
must not become workflow correctness state. Before a model call, the agent run
should persist an immutable context snapshot containing the selected memory
content or content-addressed references. A retry then reuses that snapshot
instead of performing a new retrieval against a changing index.

## OpenTelemetry and Agent Observability Findings

### OpenTelemetry Signal Model

OpenTelemetry separates observability into traces, metrics, logs, and baggage.
For Rakka Agents these signals answer different questions:

- traces explain the causal path and latency of a particular activation,
  decision, model request, tool call, memory operation, or recovery;
- metrics describe fleet-wide rates, latency distributions, token usage,
  waits, errors, saturation, and SLOs without per-run cardinality;
- structured logs and events record detailed state changes and correlate them
  with trace and durable identities; and
- baggage propagates a deliberately small set of execution-scoped values, but
  does not automatically become span, metric, or log attributes.

Telemetry remains a projection. It can explain durable state but cannot
replace run, inbox, outbox, checkpoint, timer, effect, or memory correctness
state. Sampling or exporter failure must not change an agent decision.

### GenAI Semantic Conventions

OpenTelemetry moved the GenAI semantic conventions from the main semantic
conventions repository into the dedicated
[`semantic-conventions-genai`](https://github.com/open-telemetry/semantic-conventions-genai)
repository. The conventions were still marked **Development** when reviewed.
Rakka should therefore pin a reviewed convention revision and isolate mapping
behind its OpenTelemetry adapter instead of making developing attribute names
part of the durable schema.

The reviewed conventions cover the major Rakka Agent operations:

- agent creation and invocation;
- multi-agent workflow invocation;
- explicit planning/task decomposition;
- model inference and embeddings;
- tool execution;
- retrieval and memory create/search/update/upsert/delete operations; and
- agent, workflow, tool, model operation, streaming, and token metrics.

Important standard operations include `invoke_agent`, `invoke_workflow`,
`plan`, `chat`, `execute_tool`, `retrieval`, `search_memory`, and
`upsert_memory`. The planning convention says to emit a `plan` span only when
instrumentation can reliably distinguish planning from generic reasoning or
normal inference. Rakka should follow that rule and use its own stable decision
event for ordinary loop choices.

Relevant standard correlation attributes include:

- `gen_ai.agent.id` for a stable agent resource;
- `gen_ai.agent.name` and `gen_ai.agent.version`;
- `gen_ai.conversation.id` for a session/thread;
- `gen_ai.operation.name`;
- requested and response model;
- provider and server identity;
- token usage and finish reason; and
- tool name/type, retrieval data source, and memory store/record metadata.

`AgentId` naturally maps to `gen_ai.agent.id` and `AgentRunId` naturally maps
to `gen_ai.conversation.id`, subject to telemetry access and pseudonymization
policy. Neither belongs in metric labels.

### Trace Topology for a Long-Lived Agent Session

A single agent session may include multiple A2A requests, process activations,
dispatcher attempts, waits, recoveries, and other-agent calls over hours or
days. One in-memory root span should not remain open for that entire lifetime.

The better model is a linked trace graph:

```text
A2A ingress trace
  -> active turn / invoke_agent
       -> decision or explicit plan
       -> model inference
       -> effect scheduled

dispatcher trace
  -> effect attempt
       -> execute_tool / downstream HTTP, RPC, DB, or process span
  link: effect-scheduled span

later resume trace
  -> run recovery / resumed turn
  links: parked wait span + timer, human, callback, or A2A trigger span
```

Each active operation gets a bounded span that ends when that operation ends.
On a durable asynchronous boundary, Rakka persists W3C `traceparent` and
`tracestate` plus causal span-link metadata. A later activation may continue a
bounded trace or start a new trace linked to the parked/scheduling span. The
stable `AgentRunId` correlates all trace segments into one session view.

OpenTelemetry links are specifically designed for causal relationships across
traces and long-running asynchronous work. Adding known links and
sampling-relevant attributes at span creation is preferable because head
samplers cannot consider information added later.

### Decisions, Reasoning, and Explainability

An observable decision is not the same thing as recording private model
reasoning. Rakka needs a structured decision record containing operational
facts such as:

- turn index and loop phase;
- decision kind, such as continue, call tools, delegate, wait, complete, fail,
  or request authorization;
- decision source, such as model, deterministic policy, human, or external
  authorization service;
- selected tool/target classes and count;
- settings, policy, plan, and memory-context revisions;
- safety class and gate outcome;
- stable causation/correlation fields; and
- a bounded reason code or protected decision-summary artifact reference.

Raw hidden chain-of-thought, unrestricted prompts, completions, tool arguments,
tool results, and memory content are not required to make the control flow
observable. Counts, hashes, redaction state, bounded summaries, and authorized
artifact references are safer default evidence.

### GenAI Content and Sensitive Data

The GenAI conventions mark system instructions, input/output messages, prompt
variables, retrieval queries/documents, memory queries/records, tool
arguments, and tool results as potentially sensitive. Their recommended
default is not to record instructions, inputs, or outputs. For production, the
conventions recommend storing content externally and placing references on
telemetry when detailed capture is necessary.

This aligns with Rakka's existing artifact-reference and redaction discipline:

- content capture must be disabled by default;
- enablement must be explicit, scoped, authorized, and auditable;
- telemetry should carry content hashes, sizes, classification, redaction
  status, and immutable protected artifact references;
- credentials and secret material must never be captured, even in opt-in
  content mode; and
- Collector redaction is defense in depth, not permission to emit secrets from
  the application.

OpenTelemetry's baggage guidance is especially important because baggage is
often propagated in network headers and has no built-in integrity guarantee.
Rakka baggage should contain only bounded policy/routing classes, never raw
tenant, user, agent, run, prompt, credential, or personal data.

### Metrics and Sampling

The reviewed GenAI metrics include:

- `gen_ai.client.token.usage`;
- `gen_ai.client.operation.duration`;
- time to first and subsequent streamed chunks;
- `gen_ai.workflow.duration`;
- `gen_ai.invoke_agent.duration`; and
- `gen_ai.execute_tool.duration`.

These complement Rakka's durable runtime metrics for inbox/outbox activity,
run transitions, recovery, timers, checkpoints, dispatcher backlog and
latency, model/tool adapter calls, active runs, mailbox/stream pressure,
PostgreSQL latency, and shard ownership.

Trace sampling must not be used to compute correctness counts. Metrics and
durable audit/query projections remain the source for totals. If trace volume
requires sampling, tail sampling is valuable for retaining error, slow,
indeterminate, security-denied, escalated, and recovery traces. Tail sampling
is stateful: all spans for one trace must reach the same sampler. A horizontally
scaled gateway therefore requires trace-ID-aware load balancing and explicit
memory/queue sizing.

### Existing Rakka Support

| Layer | Existing support |
| --- | --- |
| `rakka-core` | Backend-neutral `MetricsRecorder`, snapshots, Prometheus export, OpenTelemetry-oriented metrics bridge, and `tracing` integration |
| HTTP/gRPC/streams/remoting | Request and pipeline spans plus stable metrics/events |
| `rakka-agent-workflow` context | Serializable W3C `traceparent`, `tracestate`, bounded baggage, span links, validation, carrier injection/extraction, child and durable-resume helpers |
| Durable workflow records | Telemetry context on commands, effects, timers, checkpoints, adapters, callbacks, and credentials contracts |
| Runtime events | Per-run event sequence, causation/correlation IDs, trace context, and post-persistence projection events |
| Structured logs/audit | OpenTelemetry-compatible trace/span correlation, instrumentation scope, redaction state, protected artifact references, and validation |
| OTLP bridge | Resource helpers, exporter configuration metadata, serializable metrics/spans/log envelope, and in-memory receiver tests |
| `rakka-a2a` | W3C context extraction from transport headers or metadata fallback, durable propagation, bounded metrics, and operational snapshot |
| Kubernetes | Agent/gateway Collector topology with OTLP intake, Kubernetes enrichment, memory limiting, redaction/transform, batching, sampling, and export |

The existing `rakka-agent-workflow` OTLP surface is a bridge, not a native SDK
or network exporter. The application still owns the `tracing` subscriber,
OpenTelemetry SDK, and actual OTLP exporter at the binary boundary. This
preserves backend/version neutrality but means the new agent integration must
provide a clear, tested mapping rather than assume export happens
automatically.

The current span bridge record carries name, trace/span/parent IDs, flags,
timestamps, links, and attributes, but does not yet model the complete GenAI
span contract such as span kind, status, events, and per-span instrumentation
scope/schema metadata. The metrics bridge is likewise useful for basic
counter/gauge/histogram observations but does not yet express the full units,
explicit bucket guidance, and exemplars expected by richer GenAI metrics. The
agent integration needs either additive bridge extensions or a direct
application-SDK mapping that preserves these fields.

### Gaps for the New Rakka Agent Layer

The following remain to be specified or implemented:

- stable `AgentId`, `AgentRunId`, turn, decision, settings revision, and memory
  snapshot correlation across all signals;
- complete span kind/status/event/instrumentation-scope and metric
  unit/bucket/exemplar mapping beyond the current bridge shape;
- a bounded trace-segment policy for active turns and a linked-new-trace policy
  after long waits/recovery;
- GenAI semantic-convention mapping for Rig model calls, tools, retrieval,
  memory, agent invocation, and A2A delegation;
- explicit decision events without hidden reasoning capture;
- effect safety, gate, approval, authorization, and indeterminate attributes;
- token, streaming, active-time versus wait-time, and agent-session metrics;
- a session/query view that assembles linked trace segments, logs, audit
  records, runtime events, and artifact references by `AgentRunId`;
- a goal/query view that joins authorized specialist sessions, delegations,
  workflow runs, progress evaluations, and evidence by `AgentGoalId`;
- tail-sampling policies that always retain important failures and ambiguous
  effects;
- telemetry exporter health and loss visibility; and
- version pinning/migration because the GenAI conventions are still in
  development.

The current Kubernetes Collector reference pins a specific older Collector
image and uses probabilistic sampling. It should be treated as a reviewable
topology, not an evergreen production configuration. Before an agent release,
the component versions, redaction rules, sampling processors, queue sizing,
TLS/authentication, and trace-ID-aware scaling must be revalidated against the
selected Collector distribution.

## Source Index

### Local Rakka Sources

- [Rakka Agentic Workflow Spec](../agentic-workflow/agentic-workflow-spec.md)
- [Compiled Graph Scheduler Spec](../compiled_execution_with_graph_schdlr/compiled-execution-with-graph-scheduler-spec.md)
- [Rakka/OpenFang Technical Comparison](../../comparisons/openfang/rakka-openfang-technical-comparison.md)
- [Rakka/Akka Technical Comparison](../../comparisons/akka/rakka-akka-technical-comparison.md)
- [`rakka-agent-workflow` checkpoints](../../../crates/rakka-agent-workflow/src/checkpoints.rs)
- [`rakka-agent-workflow` run domain](../../../crates/rakka-agent-workflow/src/domain.rs)
- [`rakka-agent-workflow` runner](../../../crates/rakka-agent-workflow/src/runner.rs)
- [`rakka-agent-workflow` autonomy policy](../../../crates/rakka-agent-workflow/src/autonomy.rs)
- [`rakka-agent-workflow` trigger domain](../../../crates/rakka-agent-workflow/src/triggers.rs)
- [`rakka-a2a` task projection](../../../crates/rakka-a2a/src/task.rs)
- [`rakka-a2a` sharded host](../../../crates/rakka-a2a/src/host.rs)
- [Rakka observability exporters](../../rakka-v1-observability-exporters.md)
- [`rakka-agent-workflow` trace context](../../../crates/rakka-agent-workflow/src/trace_context.rs)
- [`rakka-agent-workflow` OTLP bridge](../../../crates/rakka-agent-workflow/src/otlp.rs)
- [`rakka-agent-workflow` runtime events](../../../crates/rakka-agent-workflow/src/runtime_events.rs)
- [Rakka Agent Workflow Collector topology](../agentic-workflow/kubernetes-otel-collector-topology.md)

### External Primary Sources

- [Rig repository](https://github.com/0xPlaygrounds/rig)
- [Rig v0.39.0 core memory source](https://github.com/0xPlaygrounds/rig/blob/v0.39.0/crates/rig-core/src/memory.rs)
- [Rig v0.39.0 memory policies](https://github.com/0xPlaygrounds/rig/tree/v0.39.0/crates/rig-memory)
- [OpenFang repository](https://github.com/RightNow-AI/openfang)
- [Akka autonomous-agent use case](https://doc.akka.io/sdk/use-cases/autonomous-agents.html)
- [Akka autonomous-agent definition](https://doc.akka.io/sdk/autonomous-agents/defining.html)
- [Akka autonomous-agent client API](https://doc.akka.io/sdk/autonomous-agents/client.html)
- [Akka multi-agent systems](https://doc.akka.io/sdk/use-cases/multi-agent-systems.html)
- [Akka public SDK API/Javadocs](https://doc.akka.io/sdk/_attachments/api/)
- [Akka autonomous-agent sample implementation](https://github.com/akka-samples/autonomous-agent-playground)
- [A2A 1.0 specification](https://a2a-protocol.org/dev/specification/)
- [A2A task lifecycle](https://a2a-protocol.org/latest/topics/life-of-a-task/)
- [A2A specification source](https://github.com/a2aproject/A2A/blob/main/docs/specification.md)
- [A2A protocol schema](https://github.com/a2aproject/A2A/blob/main/specification/a2a.proto)
- [Kubernetes CronJob](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/)
- [Kubernetes ServiceAccounts](https://kubernetes.io/docs/concepts/security/service-accounts/)
- [Kubernetes RBAC good practices](https://kubernetes.io/docs/concepts/security/rbac-good-practices/)
- [Kubernetes NetworkPolicy](https://kubernetes.io/docs/concepts/services-networking/network-policies/)
- [pgvector](https://github.com/pgvector/pgvector)
- [OpenTelemetry documentation](https://opentelemetry.io/docs/)
- [OpenTelemetry tracing API](https://opentelemetry.io/docs/specs/otel/trace/api/)
- [OpenTelemetry GenAI semantic conventions](https://github.com/open-telemetry/semantic-conventions-genai)
- [OpenTelemetry GenAI agent spans](https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/gen-ai-agent-spans.md)
- [OpenTelemetry GenAI model, retrieval, memory, and tool spans](https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/gen-ai-spans.md)
- [OpenTelemetry GenAI metrics](https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/gen-ai-metrics.md)
- [OpenTelemetry sampling](https://opentelemetry.io/docs/concepts/sampling/)
- [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/)
- [OpenTelemetry sensitive-data guidance](https://opentelemetry.io/docs/security/handling-sensitive-data/)
- [OpenTelemetry baggage guidance](https://opentelemetry.io/docs/concepts/signals/baggage/)
- [Autonomous Agentic Systems at Scale article](https://www.linkedin.com/pulse/autonomous-agentic-systems-scale-practical-guide-agents-saucedo-ialvf/)
- Project-owner supplied 12-page PDF export of the preceding article, reviewed
  on 2026-07-10; the local research artifact is not committed to this
  repository.
- [Author's public companion post](https://www.linkedin.com/posts/axsaucedo_autonomous-agentic-systems-at-scale-a-practical-activity-7464918399110967296-wJNJ)

## Research Conclusions Carried Forward

The guidance and specification should carry these constraints forward:

- Rakka owns durable truth; Rig owns LLM abstraction.
- The actor performs short state transitions; dispatchers perform bounded I/O.
- Every model, tool, A2A call, and memory write has an explicit effect policy.
- Opaque non-idempotent effects stop as indeterminate after ambiguity.
- HITL waits occupy durable storage, not a live execution resource.
- Short-term, private long-term, and communal memory have separate scopes.
- Communal memory stores claims with provenance, not unqualified truth.
- The communal knowledge-graph crate and public API remain database-agnostic.
- All public and agent-to-agent interaction uses A2A; internal remoting remains
  trusted cluster infrastructure.
- Credentials remain application-owned and are resolved only at dispatch time.
- `AgentGoalId` correlates a goal across its typed tasks and specialized
  agent/workflow runs; `AgentTaskId` is the stable A2A work identity and
  `AgentRunId` remains one independently durable execution session.
- Typed task definitions/results and first-class handoff, delegation, team, and
  moderation capabilities compile into Rakka durable state rather than living
  only in prompts.
- Per-run setup may narrow a definition, while mandatory guardrails, tool/model
  allowlists, and credential policy remain non-bypassable.
- **"Always-on" means logically addressable and recoverable, not a resident
  thread or process.** Active is a durable lifecycle state, not a claim of
  runtime residency.
- Goal satisfaction requires versioned criteria and evidence, while continuous
  goals are durable controllers that admit finite child epoch tasks/runs from
  deduplicated durable wakes and may passivate between every epoch.
- Pod start, restart, replacement, rollout, and shard movement never create an
  epoch, reset a schedule/budget, or define logical agent lifetime.
- Continuous wake policy makes schedule revision, overlap, missed occurrence,
  lateness, coalescing, failure backoff, suspension, and retirement explicit;
  default behavior forbids overlap and admits at most one coalesced epoch after
  downtime.
- Budgets form a durable hierarchy from definition ceiling through goal,
  task/epoch, run, and turn/effect reservations; started and indeterminate
  attempts still consume their applicable safety budgets.
- Unattended execution requires a fail-closed autonomy-admission decision that
  verifies criteria, bounded authority/effects, cancellation, inspectability,
  escalation, and recovery policy.
- Model-visible tool descriptors, Rakka tool bindings, effect intents,
  dispatch grants, and executor workload isolation are distinct contracts.
- Authoritative task state remains bounded and separate from durable history,
  content/memory, and observability projections.
- Cancellation is an observable propagation process, not proof that a started
  external effect stopped; indeterminate consequential effects reconcile
  before the task projects terminal cancellation.
- Agent observability uses bounded trace segments linked across durable waits,
  with `AgentRunId` as session correlation rather than one resident session
  span.
- Prompts, outputs, tool payloads, memory content, and hidden reasoning are not
  captured by default; detailed content remains in protected artifacts.
