# Rakka Agent Specification

Status: planning draft
Date: 2026-07-10
Background research: [background-research.md](background-research.md)
Technical guidance: [technical-guidance.md](technical-guidance.md)
Akka comparison: [rakka-akka-technical-comparison.md](../../comparisons/akka/rakka-akka-technical-comparison.md)

## 1. Purpose

This specification defines the emerging contract for durable, sharded Rakka
agents. A Rakka Agent is an autonomous, long-running logical entity powered by
Rig and a configured language model. It can plan, decide, reason, use tools,
communicate with other agents, wait for humans or authorization, and maintain
memory on behalf of its owner.

Rakka Agents are goal-driven services, not merely chatbots. Multiple
specialized agents MAY operate concurrently in a shared environment, delegate
work through A2A, invoke durable workflows as tools, contribute to collective
memory, and coordinate until the goal reaches an explicit terminal outcome.

The logical agent may live for months or years. No actor handler, thread,
future, stream, child process, or pod is expected to remain alive for that
lifetime. Durable state, inbox/outbox effects, sharding, passivation, and
recovery provide continuity.

The foundational runtime invariant is:

> **“Always-on” means logically addressable and recoverable, not a resident
> thread or process.**

An active agent, goal, or run MAY be fully passivated. Runtime residency is a
temporary optimization for bounded work and MUST NOT be part of identity,
correctness, future wake-up, or lifecycle semantics.

This is a planning specification. It describes target behavior that is not yet
fully implemented.

## 2. Normative Language

The terms **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are to
be interpreted as normative requirements for the proposed implementation.

### 2.1 Milestone Binding

This specification describes the complete target system. To keep it
implementable, every requirement is bound to the delivery milestone at which
it first becomes an acceptance obligation. A MUST in a section bound to a
later milestone is not a compliance gate for an earlier milestone, with one
standing exception: identity semantics, tenant-aware scope keys, and persisted
schema/versioning rules bind from M1 whenever an earlier milestone persists
the affected record, so that later milestones never force a durable-state
migration.

| Milestone | Content | Acceptance |
| --- | --- | --- |
| M1 | Core durable agent: identities, typed task and run, durable loop, inter-entity choreography, effect model, checkpoints/HITL, session memory, escrow budgets, autonomy admission, A2A task surface, recovery, baseline observability | Section 22 initial statement |
| M2 | Agent-private long-term memory and communal knowledge graph | Section 22 memory milestone |
| M3 | Continuous goals: wake controller, epochs, schedule fencing | Section 22 continuous milestone |
| M4 | Multi-agent goals: delegation, fan-in, workflow tools, goal evaluation | Section 22 multi-agent milestone |
| M5 | Coordination capabilities: handoff, team, moderation, human-owned tasks, replayable coordination events | Section 22 coordination milestone |

M1 is the base for all other milestones; M2 through M5 MAY be re-ordered
during implementation planning.

Primary section bindings are listed below. A clause inside an earlier-bound
section that describes a later feature binds with that feature.

| Spec area | First binding milestone |
| --- | --- |
| 5-7 core boundaries, identity, definition, settings, admission (6.6 binds at M4; 6.9 and the continuous clauses of 7.4 bind at M3) | M1 |
| 8.1-8.3 goal contract, controller, evidence (8.2 and continuous clauses bind at M3) | M4 |
| 8.4-8.7 delegation, shared environment, workflow tools, goal cancellation | M4 |
| 8.8-8.12 coordination capabilities and human-owned tasks | M5 |
| 9 task/run/loop state, inter-entity choreography, escrow budgets | M1 |
| 10 model adapter trait and Rig integration | M1 |
| 11 effect model | M1 |
| 12 checkpoints and HITL | M1 |
| 13.1, 13.2, 13.5 memory core (13.3, 13.4, and the graph clauses of 13.6 bind at M2) | M1 |
| 14 A2A surface (14.4 collaboration metadata binds at M4) | M1 |
| 15 passivation and recovery (continuous clauses bind at M3) | M1 |
| 16 security and authorization | M1 |
| 17 observability, for the signals the active milestone emits | M1 |
| 18 recovery scenarios | per scenario (tags in Section 18) |
| 19-22 crate shape, compatibility, decisions, acceptance | M1 |

Single-task cancellation semantics (dispatch fencing and nonterminal
reconciliation) bind at M1 through Sections 11.5 and 14.3; the
child-propagation clauses of Section 8.7 bind at M4.

## 3. Goals

- Provide a stable, sharded agent identity independent of process or pod.
- Keep active agents logically addressable and recoverable without requiring a
  resident actor, async task, thread, process, connection, stream, or pod.
- Represent a durable goal independently from the agent runs that contribute
  to it, with versioned success criteria, evidence, progress, and terminal
  authority.
- Represent continuous goals as fully passivatable durable controllers that
  admit finite epoch tasks/runs from versioned, deduplicated wake occurrences.
- Represent typed durable tasks independently from the agent execution sessions
  assigned to them, preserving one public task across handoff/reassignment.
- Provide independently recoverable agent runs with their own durable agentic
  loops.
- Support concurrent specialist agents that delegate and share tasks through
  A2A with bounded fan-out, lineage, budgets, cancellation, and cycle
  prevention.
- Provide typed durable handoff, delegation, team, and moderation capabilities
  for model-driven coordination, with compiled workflows for deterministic
  orchestration.
- Allow independently durable compiled workflows to be invoked from an
  agent's toolset without hiding their internal effect-safety boundaries.
- Use Rig as the default model/provider/tool abstraction behind a
  feature-gated, Rakka-owned adapter trait, without making Rig the durable
  correctness owner.
- Survive actor restart, dispatcher restart, passivation, pod loss, rolling
  update, and shard movement.
- Persist accepted work before acknowledgement and deduplicate replayed
  commands and results.
- Never automatically retry an opaque non-idempotent effect after an ambiguous
  execution window.
- Support durable human approval, security authorization, and indeterminate
  effect reconciliation without a live wait.
- Support versioned settings at instantiation and throughout the agent
  lifecycle.
- Provide a typed task/client developer surface, mandatory guardrail envelope,
  replayable progress events, and deterministic Rig-based testing.
- Provide session-scoped short-term memory, agent-private long-term memory, and
  a database-agnostic communal knowledge graph/blackboard with attributable
  collective memory.
- Use `rakka-a2a` and A2A for public and agent-to-agent interaction.
- Preserve bounded execution, bounded telemetry labels, credential-reference
  discipline, and explicit tenant isolation.

## 4. Non-Goals

- Claiming exactly-once side effects against arbitrary external systems.
- Executing a complete autonomous loop inside one actor handler.
- Treating Rakka internal remoting as a public agent protocol.
- Making Rakka a credential vault, identity provider, policy authoring system,
  model provider, or full agent product UI.
- Persisting resolved credentials, bearer tokens, provider secrets, or private
  keys in agent state, effects, memory, task history, logs, or telemetry.
- Requiring one vector database or graph database implementation.
- Treating retrieved memory or a communal graph claim as trusted instruction or
  canonical truth merely because an agent wrote it.
- Assigning one Kubernetes pod to each logical agent.
- Porting or depending on Akka SDK runtime internals; Rakka adopts public
  behavioral concepts through an independent Rust implementation.
- Defining "always-on" as permanently resident computation, a sleeping async
  task, an immortal polling loop, a held connection, or a pinned shard owner.
- Starting or resuming agent-domain work because a pod, actor, dispatcher, or
  node process started, restarted, moved, or rolled out.
- Using a Kubernetes `CronJob`, pod-local scheduler, or recurring in-memory
  timer as the correctness source for continuous-agent wakes.

## 5. Ownership Boundaries

Rakka owns:

- durable goal, agent, typed task, run, delegation, and workflow-link identity;
- sharding, placement, passivation, and recovery;
- continuous wake identity/admission, schedule fencing, epoch creation, and
  hierarchical budget accounting;
- versioned loop state and deterministic transitions;
- accepted-command and result deduplication;
- durable effect intent, safety policy, dispatch eligibility, and result
  acceptance;
- checkpoints, timers, cancellation, drain, and reconciliation state;
- memory scopes, storage traits, idempotent write contracts, and retrieval
  snapshots;
- runtime events, audit references, bounded metrics, and operational
  snapshots; and
- A2A task projection through `rakka-a2a`.

Rig owns:

- model-provider abstraction;
- completion and embedding request construction;
- tool schemas and model-facing tool representation;
- model response parsing and applicable Rig agent-run algorithms; and
- optional history-shaping and vector-store adapter components used behind
  Rakka-owned boundaries.

The application owns:

- tenant/user authentication and authorization policy;
- agent templates, typed task definitions/result rules, prompts, available
  tools, coordination policy, and business approval rules;
- model/provider accounts and credential storage;
- logical credential bindings and dispatch-time credential resolution;
- tool implementations, sandboxes, and external reconciliation adapters;
- workload identity, dispatcher trust-tier placement, network-egress policy,
  and execution-policy realization;
- specialist catalog/routing, team-formation, goal-evaluator, and terminal
  authority policy;
- retention, classification, privacy, cost, and safety policy;
- human-facing UI, ticketing, messaging, and escalation integrations; and
- product-specific agent cards, skills, billing, and audit consumption.

## 6. Core Terms and Identities

### 6.1 TenantId

`TenantId` identifies the security and data-isolation boundary. Every durable
goal, agent, task, run, assignment/coordination record, memory, checkpoint,
effect, query, projection, and audit lookup MUST be tenant-scoped.

### 6.2 AgentId

`AgentId` is the stable identity of one configured logical agent. It MUST
remain stable across runs, activation, passivation, owner changes, pod loss,
and shard movement.

The logical `AgentEntity` is sharded by `(TenantId, AgentId)` and owns:

- definition and lifecycle status;
- current settings revision;
- policy and logical credential-binding references;
- the namespace for agent-private memory; and
- administrative commands over its runs (suspend, resume, terminate).

Routine run creation flows through task assignment (Section 9.8) against the
agent's durable definition/admission state; it MUST NOT require a synchronous
command round trip through `AgentEntity`, so a popular agent does not become a
serialization bottleneck for its own runs.

### 6.3 AgentGoalId

`AgentGoalId` identifies one top-level collaborative goal. It is distinct from
the agent identities and run sessions that contribute to the goal. A goal MAY
span multiple specialized agents, concurrent `AgentRunId` values, A2A tasks,
workflow runs, waits, recoveries, and trace segments.

The initial implementation SHOULD let the stable root `AgentTaskEntity`
coordinate the goal, while the current finite root or continuous-epoch
`AgentRunEntity` proposes decisions against it. Its generated `AgentGoalId` MAY
default to the root `AgentTaskId` value, but the types and semantics MUST remain
distinct so coordination can later move to a dedicated entity without changing
the public contract.

### 6.4 AgentTaskId

`AgentTaskId` identifies one durable, typed unit of work and its eventual
public result. It is stable across assignment, handoff, reassignment, agent
restart, and changes in the `AgentRunId` currently executing it.

The logical `AgentTaskEntity` is sharded by `(TenantId, AgentTaskId)` and owns:

- task-definition ID/version, bounded instructions, and input/artifact refs;
- dependency graph and failure-propagation policy;
- current assignment plus immutable assignment/handoff history;
- result proposals, validation decisions, and accepted typed result;
- human ownership when the task is intentionally unassigned to an agent;
- task lifecycle, retention, audit, and A2A projection source; and
- links to `AgentGoalId`, parent/child tasks, and execution runs.

`AgentTaskId` SHOULD map one-to-one to A2A `Task.id`. An initial implementation
MAY generate the same underlying value for a task and its first run, but their
types and semantics MUST remain distinct.

### 6.5 AgentRunId

`AgentRunId` identifies one autonomous session or one agent's contribution to a
typed task/collaborative goal. A run belongs to exactly one
`(TenantId, AgentId, AgentTaskId)` and MUST be independently recoverable. Root,
specialist, and handoff runs MAY share an `AgentGoalId` or `AgentTaskId`, but
MUST NOT share mutable loop state or short-term memory namespaces.

The logical `AgentRunEntity` is sharded by
`(TenantId, AgentId, AgentRunId)` and owns:

- loop state and status;
- short-term session memory namespace;
- effects and accepted results;
- checkpoints and timers;
- run budgets and deadline; and
- its contribution to the owning task's projection/evidence.

A run MUST remain bound to its single `AgentTaskId` for its entire lifetime;
handoff and reassignment create a new run rather than re-targeting an existing
one. Parallel work uses multiple independently sharded runs.

For a continuous goal, each admitted evaluation/observation epoch MUST use a
distinct finite child `AgentTaskId` and `AgentRunId`. Cross-epoch continuity
belongs in the stable goal/controller state, agent-private memory, and explicit
artifacts; one unbounded run and short-term-memory session MUST NOT be the
default continuous-execution model.

### 6.6 AgentDelegationId

`AgentDelegationId` identifies one durable assignment of work from a parent
run to a specialist agent. It binds the root goal, parent run, target skill and
resolved agent, child A2A task/run, lineage, budget allocation, and expected
result contract.

Replaying one delegation operation MUST resolve to the same logical child task
or to an explicit conflict. It MUST NOT silently create a second child run.

### 6.7 AgentEnvironmentRef

`AgentEnvironmentRef` is a logical, authorized reference to an application-
owned shared resource, event stream, workspace, simulator, physical system, or
other changing world state. It MUST NOT contain resolved credentials. Access
and mutation occur through declared tools/effects with the concurrency control
required by the external system.

### 6.8 KnowledgeSpaceId

`KnowledgeSpaceId` identifies a communal knowledge-graph boundary. The default
space MUST be tenant- or organization-scoped. Cross-tenant knowledge sharing
MUST require an explicit federation design and authorization policy.

### 6.9 AgentWakeId and ScheduleRevision

`AgentWakeId` identifies one durable logical occurrence that may admit a
continuous-goal epoch. It MUST be stable across scanner/dispatcher restart,
pod loss, passivation, duplicate trigger delivery, and shard movement.

Every wake MUST bind `AgentGoalId`, the current `ScheduleRevision`, trigger
kind/source, logical occurrence or event identity, due/accepted time,
deduplication identity, and policy revision. A schedule update MUST create a
monotonic revision and fence pending wakes from obsolete revisions unless an
explicit migration adopts them.

### 6.10 Stable Operation IDs

Commands, wake/epoch admission, budget reservation/settlement,
task/dependency/assignment/handoff/claim/turn transitions, effects/grants,
memory writes, checkpoint resolutions, A2A sends, and graph claim appends MUST
carry stable operation or deduplication IDs. Replaying the same accepted
operation MUST NOT produce a second state transition or logical write.

### 6.11 Logical Availability and Runtime Residency

Logical availability is the ability to address a stable agent/goal/task/run,
durably accept authorized work, and recover the next legal transition on the
current shard owner. Runtime residency means that an actor or bounded worker
is temporarily instantiated to perform work. These are independent properties.

An `Active`, `Running`, or waiting lifecycle status MUST NOT imply runtime
residency. A logically active entity MAY have no actor instance on any pod.
When it is quiescent, continuity MUST be represented by durable state and
durable or replayable wake sources rather than per-agent execution resources.

Quiescence MUST NOT require a per-agent:

- actor instance, mailbox, Tokio task, future, thread, or child process;
- in-memory polling loop or timer;
- network connection, response stream, or dispatcher lease; or
- open telemetry span.

Shared runtime pools, shard routing metadata, durable inbox/outbox rows,
durable timers, checkpoints, and indexes MAY remain. They describe or route
future work; they are not resident execution for one agent.

## 7. Agent Definition and Settings

### 7.1 Agent Definition

An agent definition SHOULD include:

- display metadata and owner reference;
- a mandatory bounded, outcome-oriented description used consistently for A2A
  discovery, model purpose, documentation, and observability class;
- system instructions or prompt artifact reference;
- accepted typed task definition/version references and result rules;
- allowed model/provider profiles;
- registered tools and their safety/capability declarations;
- declared specialties, accepted goal classes, and typed handoff, delegation,
  team, and moderation capability limits;
- allowed workflow tools and versioned input/output contracts;
- admitted operation classes and continuous wake/overlap/missed-occurrence/
  suspension/retirement policy where applicable;
- loop, model/tool/effect, token, cost, time, and concurrency budgets;
- goal-evaluator and progress/stagnation policy references;
- memory and retrieval policy;
- approval, authorization, and escalation policy references;
- logical credential binding references;
- execution-policy/trust-class references for tool dispatch;
- A2A agent-card/skill metadata;
- retention and classification policy; and
- definition schema version.

Large prompts or policies SHOULD use immutable artifact references rather than
unbounded inline durable state.

### 7.2 Settings Revisions

Each accepted settings update MUST create a monotonic `SettingsRevision` with
an authenticated principal, timestamp, causation ID, and immutable audit
reference.

Settings MUST be divided by application timing:

| Class | Examples | Application point |
| --- | --- | --- |
| Turn-bound | prompt, model profile, sampling, retrieval limit | Next model turn |
| Immediate safety | suspend, cancel, capability/credential revocation, safety policy | Before any further dispatch |
| Run-pinned | loop-state schema, incompatible tool contract, memory schema | New run or explicit migration |

An in-flight model/tool effect uses the revision recorded in its intent.
Ordinary settings changes MUST NOT silently mutate an already dispatched
effect. Immediate safety changes MUST be rechecked before dispatch and MAY
invalidate an existing approval or authorization.

External settings changes MUST enter through an authenticated application
command exposed by `rakka-a2a` or an explicitly defined A2A extension/skill.
They MUST NOT use internal actor remoting as a public administrative API.

### 7.3 Definition Versus Run Setup

The implementation SHOULD distinguish a static/versioned
`AgentDefinitionRevision` from a per-run `AgentSetupRevision`.

The definition owns the outcome description, accepted task envelope, tool and
workflow/MCP classes, approved model profiles, mandatory guardrails,
coordination-capability envelope, and hard policy/budget ceilings. Setup MAY
select or narrow instructions, accepted task capabilities, collaborators,
knowledge/environment scopes, and budgets for one run.

Setup and later settings revisions MUST NOT introduce an undeclared tool,
weaken a mandatory guardrail, choose an unapproved model, widen credential or
knowledge access, add an unauthorized peer, or downgrade effect safety. The
effective definition, setup, settings, and policy revisions MUST be recorded on
the task/run and every resulting effect.

### 7.4 Autonomy Admission

Rakka Agent MUST distinguish at least `Interactive`, `BoundedAsync`, and
`Continuous` operation classes. These classes describe operating behavior;
industry-specific risk classification remains application policy.

Unattended `BoundedAsync` or `Continuous` execution MUST fail closed unless an
authorized admission policy verifies:

- measurable completion, health, or progress criteria;
- bounded time, cost, iterations, model/tool calls, effects, concurrency, and
  collaboration as applicable;
- cancellation, suspension, escalation, and recovery behavior;
- authorized operational inspection;
- classified tool/effect safety and scoped capabilities/credential bindings;
- approval or security-authorization policy for consequential operations; and
- indeterminate-effect reconciliation policy.

Every accepted decision MUST create an immutable
`AutonomyAdmissionDecision` containing operation class, admitted
definition/setup/settings/policy revisions, evaluator principal or service,
stable reasons/constraints, creation time, and optional expiry.

Admission MUST run when a definition is published or instantiated and whenever
an update may widen tools, peers, credentials, environment/knowledge access,
schedule, budgets, or other autonomy. Narrowing updates MAY reuse an admission
only when policy proves them monotonic. Immediate cancellation, revocation,
grant validity, and safety policy MUST still be checked before every dispatch.

Rakka owns the admission contract, durable decision, and enforcement points.
The application owns policy authoring, risk taxonomy, and business/regulatory
approval rules.

## 8. Goal-Driven Collaboration

### 8.1 Goal Contract and Lifecycle

Every goal MUST have a durable, versioned `AgentGoalSpec` containing at least:

- `TenantId`, `AgentGoalId`, owner/principal, root `AgentTaskId`, and initial/
  current coordinator `AgentRunId` when applicable;
- objective and immutable or versioned success criteria;
- finite or continuous goal mode and, for continuous mode, versioned wake,
  overlap, missed-occurrence, suspension, and retirement policy;
- constraints, priority, deadline, and cancellation policy;
- iteration, model/tool/effect, token, cost, active/elapsed-time, descendant,
  fan-out, depth, and concurrency budgets plus refill/window policy where
  applicable;
- allowed agent skills, tools, workflows, knowledge spaces, and environment
  references;
- evaluator/policy reference and required evidence classes;
- escalation, stagnation, and terminal-decision policy; and
- schema, settings, and policy revisions.

The initial goal lifecycle SHOULD be:

```rust
enum AgentGoalStatus {
    Proposed,
    Active,
    Waiting,
    Satisfied,
    Unsatisfied,
    Failed,
    Cancelled,
    Expired,
}
```

`Satisfied`, `Unsatisfied`, `Failed`, `Cancelled`, and `Expired` are terminal.
`Unsatisfied` records an evaluator or policy decision that the success
criteria were not met under the current goal revision; `Failed` records an
execution or policy failure that ended the goal.
The logical goal MUST remain durably addressable through dispatcher restart,
pod loss, passivation, and shard movement until an authorized terminal
transition occurs. This durability does not authorize unbounded compute: a
budget or progress limit MUST park, escalate, or terminate according to policy.

A finite goal terminates after its current versioned criteria are evaluated. A
continuous goal, if enabled, MUST execute as bounded durable epochs with an
explicit health condition, wake, renewal/budget, suspension, and retirement
policy; it MUST NOT be implemented as an immortal polling future.

### 8.2 Continuous Goal Controller and Epochs

A continuous goal MUST be represented as a stable, durable, fully passivatable
controller. Its root control task MAY remain nonterminal for the goal's logical
lifetime, but it MUST NOT own a resident loop, sleeping task, in-memory timer,
held connection, open span, dispatcher reservation, or pod lease.

Each admitted epoch MUST create one finite child `AgentTaskId` and one finite
`AgentRunId`. The epoch MUST carry the goal/root task, `AgentWakeId`, schedule,
definition/setup/settings/policy revisions, input observation scope, budget,
deadline, and result/evidence contract. Epoch completion returns evidence to
the controller and MUST NOT by itself terminate the continuous goal.

Every continuous goal MUST have a versioned `AgentWakePolicy` defining:

- allowed durable timer, external-event, authenticated A2A command, callback,
  and/or hybrid triggers;
- schedule revision, occurrence/deduplication construction, admission window,
  and maximum lateness;
- overlap, coalescing, and missed-occurrence behavior;
- per-epoch budget/deadline and goal-level rolling/window ceiling;
- failure backoff and escalation; and
- suspension, renewal, expiry, and retirement.

The default overlap policy MUST forbid a second active epoch and durably
coalesce triggers received while one is active. The default missed-occurrence
policy after downtime MUST admit at most one coalesced epoch. Parallel epochs,
bounded catch-up, or replay of multiple occurrences MUST require an explicit
definition and concurrency/result policy.

A schedule update MUST fence obsolete occurrences. Duplicate timer scans,
events, callbacks, A2A commands, or scanner restarts MUST produce one logical
`AgentWakeId` and at most one admitted child epoch. A pod/actor/dispatcher start
or restart MUST NOT itself create a wake, epoch, schedule reset, or budget
refill. Kubernetes scheduling MAY operate shared Rakka services but MUST NOT be
the continuous agent's correctness scheduler.

Between every wake, admission, epoch transition, and result, the controller,
task, and run MAY passivate. Future progress MUST depend only on durable or
safely replayable triggers and authoritative shared state.

### 8.3 Progress, Evidence, and Completion

An agent or model MAY propose that a goal is complete, but that declaration is
not sufficient to transition the goal to `Satisfied`. The configured evaluator
MUST assess the current success-criteria revision against durable evidence and
produce a stable outcome and reason/evidence references.

Evidence MAY include verified artifacts, tool results, workflow outputs,
authorized human decisions, external-state observations, and attributed
knowledge claims. Hidden chain-of-thought MUST NOT be required or persisted as
evidence.

Child-run or workflow completion is evidence for the parent. It is not by
itself proof that the root goal is satisfied. Progress evaluation MUST detect
bounded forms of repetition, lack of material state change, budget exhaustion,
and stale environmental assumptions, then continue, replan, wait, escalate, or
terminate according to deterministic policy.

### 8.4 Specialization and Durable Delegation

An agent's specialties and accepted work contracts SHOULD be advertised as
versioned A2A Agent Card/skill metadata. A model or deterministic planner MAY
request a skill, but an application-owned authorized catalog MUST resolve the
concrete target `AgentId`, endpoint, scopes, and current compatibility.

Every delegation MUST be represented by a durable record containing:

- `AgentDelegationId`, `AgentGoalId`, parent `AgentTaskId`/`AgentRunId`, and
  lineage/depth;
- requested skill and resolved target `AgentId`/endpoint;
- stable A2A task/message and effect/deduplication identities;
- bounded input or artifact reference and versioned output schema;
- allocated budgets, deadline, capability scopes, and credential-binding
  references;
- settings/policy revisions, causation/correlation, and trace propagation; and
- child `AgentTaskId`/`AgentRunId`, status, result/evidence references, and
  terminal reason.

Delegation MUST traverse the durable effect/outbox and A2A boundary even when
both agents currently reside on one node. Direct public invocation through an
actor reference is forbidden. The parent MUST be able to persist a fan-out or
wait and passivate; child results return through a durable, deduplicated A2A
command and participate in deterministic fan-in.

The coordinator MUST enforce maximum depth, fan-out, descendants, concurrent
children, time, tokens, and cost. It MUST detect repeated delegation lineage or
other policy-defined cycles. Reassignment after ambiguity MUST use
reconciliation or a new explicit delegation generation; it MUST NOT assume the
prior child and its external effects never ran.

### 8.5 Shared Environment and Collective Memory

Agents collaborate in a shared environment through authorized
`AgentEnvironmentRef` values and tools, not through unsynchronized actor
memory. Sharding one agent or run does not serialize different agents mutating
the same external resource. The application/tool adapter MUST use the external
system's idempotency key, lease, compare-and-swap, transaction, reservation,
or reconciliation protocol when coordination is required.

Collective memory consists of authorized communal knowledge-space claims and
shared artifact references. It MUST NOT grant implicit access to another
agent's private long-term memory. A claim contributed during collaboration
SHOULD include `AgentGoalId`, `AgentTaskId`, source `AgentId`/`AgentRunId`,
delegation identity, evidence references, trust state, and stable append
operation ID. Concurrent or conflicting claims MUST retain provenance and
coexist for policy-aware resolution rather than silently overwriting communal
truth.

### 8.6 Workflows as Tools

An agent MAY expose or consume a compiled workflow as a tool only through a
versioned `WorkflowToolDescriptor` containing the workflow definition/version,
input/output schemas, capabilities, logical credential bindings, deadline and
cancellation policy, compensation support, and result/evidence contract.

Invocation MUST create or adopt an independently durable child workflow run
with a stable identity linked to `AgentGoalId`, parent `AgentTaskId`/
`AgentRunId`, and the invocation effect. The parent persists the wait and MAY
passivate. Workflow completion returns a deduplicated result/evidence
reference.

The entire workflow MUST NOT be wrapped as one opaque retryable tool effect.
Its scheduler, checkpoints, and individual external effects retain their own
durable safety, retry, reconciliation, and indeterminate semantics.

### 8.7 Cancellation, Failure, and Waiting

Goal cancellation, deadline, and immediate capability revocation MUST be
propagated durably to active child runs and workflows. Propagation is a request
with an observable outcome; it is not proof that an already-started external
effect was cancelled.

Acceptance of a cancellation request MUST immediately fence new model, tool,
workflow, and delegation dispatch for the affected scope. Cancellation MUST
track durable progress through request, propagation, quiescence, optional
reconciliation, and terminal completion. It MUST NOT be represented only by a
single in-memory flag or best-effort broadcast.

If any started consequential effect has an unknowable outcome, the task MUST
remain nonterminal in `WaitingForReconciliation` with cancellation requested.
It MUST NOT project terminal `Cancelled` until internal work is quiescent and
every such effect has a known outcome or explicit reconciliation decision.
An A2A cancellation response represents the current result of an attempted
cancellation, not proof that every external side effect stopped.

A root coordinator MAY wait for all children, a quorum, a policy-selected
subset, or an early satisfying result. The fan-in rule MUST be fixed in durable
state before results are accepted. Failed, timed-out, cancelled, or
indeterminate children MUST be handled explicitly by policy. While waiting for
agents, workflows, humans, timers, or reconciliation, the coordinator SHOULD
passivate and MUST NOT hold a live thread, future, or trace span.

### 8.8 Coordination Capability Model

Rakka SHOULD expose a versioned `AgentCoordinationCapability` model with four
initial variants:

```rust
enum AgentCoordinationCapability {
    Handoff(HandoffPolicy),
    Delegation(DelegationPolicy),
    TeamLeadership(TeamPolicy),
    Moderation(ModerationPolicy),
}
```

Capabilities are trusted definition/setup data. The runtime MAY present their
allowed operations to Rig as tools, but model output MUST NOT create a new
capability, target class, budget, or authorization scope. Every capability
transition MUST be typed, durable, deduplicated, bounded, and auditable.

When task order and compensation are fixed, the application SHOULD use a
compiled workflow. When the model is authorized to decide which specialist or
coordination transition occurs next, it SHOULD use these capabilities. A
workflow MAY invoke a durable agent task for a model-driven stage.

### 8.9 Handoff

Handoff transfers responsibility for the same `AgentTaskId` from a source run
to a target agent. It MUST:

- validate that the current typed task contract is accepted by the target;
- persist a stable handoff ID, source/target agents, reason, capability and
  policy revisions, and explicit task context/artifact projection;
- fence the source run from further task completion/effect scheduling;
- create a distinct target `AgentRunId` under the target `AgentId`;
- preserve the same `AgentTaskId` and public A2A task history; and
- record the source run as terminal `HandedOff` only after target assignment is
  durably accepted or resolve through explicit recovery/reconciliation.

Handoff MUST NOT mutate the source run's `AgentId`, reuse its short-term-memory
namespace, or expose source private memory. An ambiguous handoff or reassignment
MUST NOT authorize replay of an opaque non-idempotent source effect.
The handoff command/result MUST traverse the durable outbox/inbox and
`rakka-a2a` boundary, including when source and target are currently colocated.

### 8.10 Team Coordination

A team MUST have a stable `AgentTeamId`, root goal, leader, bounded member
types/instances, capability/scopes, creation/expiry policy, and a durable shared
task board. Team members MAY atomically claim, release, or transfer authorized
`AgentTaskId` values and exchange mediated messages.

Task claims MUST use revision/lease fencing and stable operation IDs. One task
MUST NOT have two active claim owners unless its definition explicitly permits
a replicated/quorum execution mode with separate runs and result policy. Team
membership, backlog, and peer messages are correctness/coordination state and
MUST NOT be implemented only as communal knowledge-graph claims.

The team board does not grant access to member private memory. Shared artifacts
and knowledge spaces remain explicitly authorized. An idle team or member MUST
passivate; a shared board is durable data, not a resident coordinator.
Peer messages and task-transfer notifications MUST use durable commands and
`rakka-a2a`, not direct actor references.

### 8.11 Moderation

A moderated interaction MUST have a stable `AgentConversationId`, moderator,
authorized participant set, mode, durable turn/round state, transcript artifact
or bounded messages, completion rule, and round/iteration/time/token budgets.

Only the current authorized participant MAY submit the next turn. Duplicate or
out-of-order turns MUST be deduplicated or rejected. The moderator MAY end
early under policy, but its proposed result still passes typed task-result and
goal-evidence validation. Participants and moderator MUST be passivatable
between turns.
Turn requests/results MUST use durable A2A effects/commands with stable
conversation, round, turn, participant, and deduplication identity.

### 8.12 Human-Owned Tasks

An `AgentTaskId` MAY be deliberately unassigned to an agent and completed by an
authenticated human or external service with a typed result. This supports
human work products and deterministic dependency graphs.

A human-owned task is not a substitute for an effect-bound checkpoint. When a
decision approves, authorizes, or reconciles a specific effect, the runtime
MUST use `AgentCheckpoint` and bind the resolution to the exact effect intent.

## 9. Agent Task, Run, and Loop State

### 9.1 Typed Task Definition

A versioned `AgentTaskDefinition<R>` MUST declare:

- stable definition ID, version, and outcome-oriented description;
- bounded input/instructions schema and typed result schema;
- deterministic result-validation rules and rejection limits;
- permitted assignee agent classes/skills and human/service ownership policy;
- dependency, failure/cancellation propagation, and handoff policy;
- attachment/artifact media, size, classification, and loader policy;
- per-task iteration, model-call, tool/effect-attempt, token, cost,
  active/elapsed-time, artifact-size, and coordination budgets;
- required evidence, guardrail, retention, and audit policy; and
- schema-compatibility/migration metadata.

Rust generics MAY provide compile-time ergonomics in application code, but
durable state and A2A metadata MUST use stable versioned schema references and
bounded serialized values/artifacts. Deserializing a result under a mismatched
task definition/version MUST fail closed.

### 9.2 Task Lifecycle and Result Rules

The initial task status set SHOULD be:

```rust
enum AgentTaskStatus {
    Created,
    Blocked,
    Assigned,
    InProgress,
    WaitingForInput,
    Completed,
    Failed,
    Cancelled,
}
```

`Completed`, `Failed`, and `Cancelled` are terminal. Task status is independent
from whether any actor/run is resident. A task MAY be `Blocked`, `Assigned`, or
`WaitingForInput` with no live execution resource.

Task dependencies MUST be durable, bounded, acyclic, and created with stable
operation IDs. A task MUST NOT become eligible until its dependency rule is
satisfied. The default failed/cancelled-dependency policy SHOULD cancel
dependents; alternatives such as continue-with-evidence MUST be explicit in the
definition.

A model, human, service, or workflow MAY submit a typed result proposal. Before
completion, the task entity MUST validate the schema and run every applicable
deterministic `AgentTaskResultRule`. A rejection MUST persist a stable reason,
rule/version, proposal digest/artifact reference, rejection count, and
causation. Policy MAY return sanitized feedback to an active run for another
bounded iteration. Exceeding the rejection/iteration budget fails or escalates
the task; it MUST NOT silently accept the proposal.

A model-assisted or external result evaluator MUST execute as an explicit
durable effect or verification workflow and return evidence to the task entity;
it MUST NOT run nondeterministic I/O inside the task entity's deterministic
transition.

Task attachments MUST be immutable bounded content or artifact references with
digest, media type, size, classification, provenance, and authorization
metadata. Content loading occurs through bounded adapters/effects. Resolved
credentials MUST NOT be persisted in the attachment or loader configuration.

### 9.3 Run Status

The target run status set is:

```rust
enum AgentRunStatus {
    Accepted,
    Running,
    WaitingForTimer,
    WaitingForEffect,
    WaitingForApproval,
    WaitingForAuthorization,
    WaitingForReconciliation,
    Suspended,
    Cancelling,
    Compensating,
    HandedOff,
    Superseded,
    Completed,
    Failed,
    Cancelled,
}
```

`HandedOff`, `Superseded`, `Completed`, `Failed`, and `Cancelled` are terminal
for one run. `HandedOff` records a completed handoff (Section 8.9);
`Superseded` records replacement through reassignment or a new run
generation. Waiting and `Suspended` states are interrupted/non-executing
states, not terminal states and not resident waits. `Accepted`, `Running`, and
`Cancelling` also MUST NOT be interpreted as physical-residency guarantees; the
entity MAY passivate whenever no bounded transition is immediately executable.

One task MAY reference several sequential runs due to handoff or reassignment,
but at most one run is the normal current owner. Claim/assignment revisions
MUST fence prior runs from scheduling effects or accepting task completion.

The implementation MAY initially preserve the existing `WaitingForHuman`
variant as a compatibility representation of `WaitingForApproval`, but public
behavior and persisted migrations MUST be explicit before the status is split.

### 9.4 Loop Phase

A run MUST persist a Rakka-owned, versioned loop phase. The initial phase model
SHOULD support:

```rust
enum AgentLoopPhase {
    PreparingContext,
    AwaitingModel,
    EvaluatingModelOutput,
    AwaitingTools,
    RecordingTurn,
    DecidingContinuation,
    Suspended,
    Complete,
}
```

The durable loop state MUST include at least:

- `AgentGoalId`, `AgentTaskId`, root/parent task/run, handoff/delegation/team/
  moderation identity when applicable;
- turn index and phase;
- settings and policy revision;
- context snapshot reference when a model call is prepared;
- pending effect IDs and accepted result references;
- remaining loop, token, cost, and time budgets;
- pending checkpoint/timer reference; and
- loop-state schema and adapter version.

### 9.5 Execution Rule

Actor handlers MUST perform bounded state transitions. They MUST NOT await an
LLM, effectful tool, human, remote agent, long timer, or other unbounded
external operation inside the actor handler.

The run transition MUST persist the next effect or wait before returning. The
dispatcher performs bounded I/O and returns a durable result command through
the inbox.

A run that proposes completion MUST submit a typed result proposal to its
`AgentTaskEntity` through the deduplicated inter-entity exchange defined in
Section 9.8. It MUST NOT make the public task terminal by mutating only
run state. The task becomes `Completed` only after schema/result-rule
validation; root goal satisfaction remains a separate evaluation.

### 9.6 Bounded Task State and History

The materialized state required for task transitions MUST be bounded
independently from retention of domain history, content, memory, and
observability projections.

`AgentTaskEntity` current state MAY contain current identity/status/revisions,
bounded dependency summary, assignment/claim, current run, pending
effect/checkpoint/result references, accepted result reference, and terminal
reason. It MUST NOT embed unbounded messages, observations, tool payloads,
artifacts, assignment/handoff history, result proposals, audit events, or
memory records.

The runtime MUST separate:

1. bounded authoritative materialized state;
2. append-only durable domain/audit history;
3. content/artifact and scoped-memory stores; and
4. derived A2A, list/search, and observability projections.

Task definitions or deployment policy MUST bound dependencies, children,
handoffs/reassignments, result rejections, pending effects/checkpoints, inline
metadata, history replay/query windows, and page sizes. Historical content MUST
be queried through authorized cursors or immutable artifact references.
Snapshotting/compaction MUST preserve correctness, deduplication, audit, and
retention semantics without making memory the task lifecycle source.

### 9.7 Hierarchical Budget Ledger

Budgets MUST form a durable hierarchy from definition ceiling through goal
allocation, task/epoch allocation, run allocation, and turn/effect reservation
and settlement. A child allocation MUST NOT widen its parent or definition
ceiling.

The ledger MUST support applicable dimensions for autonomous iterations,
model calls, tokens, provider cost, tool calls, external effect starts and
attempts, active execution time, elapsed deadline, concurrent effects,
delegation depth/fan-out/descendants, and bounded output/artifact size.

The hierarchy MUST be realized as down-front escrow allocations rather than
dispatch-time distributed transactions. When a goal, task, epoch, or run is
created, its allocation MUST be durably debited from the parent scope inside
the parent entity's own creating transition and carried on the deduplicated
creation command. The sum of a parent's outstanding child allocations plus its
own consumption MUST NOT exceed its allocation, and replaying an allocation
command MUST NOT debit the parent twice.

Before dispatch, the runtime MUST atomically reserve the applicable budget
from the run's own durable ledger or deny/park the operation. A dispatch-time
reservation MUST be a single-entity transition on the run and MUST NOT require
a synchronous cross-entity or cross-shard read, lock, or transaction. Parent
and definition ceilings are enforced at allocation and admission time;
goal-window ceilings for continuous goals are enforced at epoch admission.

The runtime MUST settle usage from the durable accepted result. An effect that
reaches durable `Started` consumes an attempt even if it later becomes
`Indeterminate`; an idempotent retry consumes another attempt. Settlement and
the return of unused child allocation MUST flow upward through the
deduplicated inter-entity command path of Section 9.8, and only after a known
terminal child outcome; replaying a settlement or return command MUST NOT
credit a parent twice.

A run that exhausts its escrowed allocation MUST park with a structured
budget-exhaustion reason. It MAY request additional allocation from its parent
scope through a deduplicated command; the grant is an ordinary parent-local
allocation decision under the same ceilings. Exhaustion MUST NOT be resolved
by reading parent budget state directly at dispatch time.

Budget decisions MUST carry scope, dimension, limit, consumed/reserved value,
policy revision, and stable reason. Soft thresholds MAY warn or request
authorization. Hard ceilings MUST deterministically reject, park, suspend,
escalate, fail, expire, or retire according to persisted policy. Budget
exhaustion SHOULD be a structured wait/stop/terminal reason rather than a new
top-level task status for every dimension.

Continuous goals MUST combine per-epoch allocation with a durable rolling or
calendar-window goal ceiling. Refill MUST be a persisted logical-time policy
transition and MUST NOT occur because an actor/pod restarted, a shard moved, or
an entity was activated.

### 9.8 Inter-Entity Choreography

`AgentEntity`, `AgentTaskEntity`, `AgentRunEntity`, and later coordination
entities are independently sharded single writers. Every state-changing
exchange between two entities MUST traverse the durable outbox/inbox
substrate: the sender persists the command intent through its outbox as part
of its own transition, the receiver durably accepts and deduplicates the
command by stable operation ID before acknowledgement, and any reply returns
through the same mechanism. In-memory actor calls, shared mutable state, and
synchronous cross-entity transactions MUST NOT be used for correctness, even
when both entities are currently colocated on one node.

Each cross-entity exchange is therefore a small saga with an explicit owner:

- the initiating entity records the pending exchange (operation ID, target,
  and expected reply) in its own durable state before or with the send;
- recovery of the initiator MUST re-drive an unacknowledged exchange by
  re-emitting the same operation ID, never by minting a new one;
- the receiving entity MUST return the original logical result for a replayed
  operation ID without performing a second transition; and
- neither side MAY treat the absence of a reply as evidence that the exchange
  did not execute.

The canonical creation and assignment flow is:

```text
A2A ingress (durable, deduplicated)
    -> AgentTaskEntity: create task (operation ID from ingress)
    -> assignment decision recorded on the task
       (definition/admission revisions read from durable state;
        no synchronous round trip through AgentEntity)
    -> run-creation command -> AgentRunEntity
       (deduplicated by task ID + assignment generation)
    -> run Accepted -> acceptance reply -> AgentTaskEntity
    -> task InProgress
```

Replaying any step MUST converge on one task, one assignment for the current
generation, and one run. `AgentEntity` participates through its durable
definition, settings, and admission state; immediate safety policy is still
rechecked before dispatch under Section 7.2.

The canonical result flow is:

```text
AgentRunEntity: persist result proposal (proposal ID + digest)
    -> proposal command -> AgentTaskEntity
    -> schema + deterministic result-rule validation (Section 9.2)
    -> durable accept/reject decision recorded on the task
    -> decision reply -> AgentRunEntity
    -> run records the outcome and transitions
```

The task entity's persisted decision is the source of truth for the
validation outcome; the run's persisted state is the source of truth for the
run's consequence of that outcome. If either side is lost mid-exchange,
recovery re-drives the pending proposal or reply and converges without a
second validation, a duplicate completion, or a lost rejection. A duplicate
proposal with the same proposal ID MUST return the original decision.

Every inter-entity exchange defined by this specification — creation,
assignment, run acceptance, result proposal/decision, budget allocation,
settlement and return, delegation, handoff, claim, turn, cancellation
propagation, and evaluation requests — MUST document its failure windows
(initiator loss before send, receiver loss after acceptance, reply loss, and
duplicate delivery), and each window MUST converge under replay.

## 10. Model Adapter and Rig Integration

### 10.1 Model Adapter Trait and Rig Feature Gate

`rakka-agent` MUST define a Rakka-owned, provider-neutral model adapter trait
(working name `AgentModelAdapter`) as the core model contract. The trait
converts an immutable context snapshot and settings revision into a bounded
model request and converts the provider response into a bounded Rakka
result/artifact. The durable loop, effect model, and testkit MUST depend only
on this trait.

The Rig-backed implementation of the trait MUST live behind a `rig` cargo
feature of `rakka-agent`. The feature SHOULD be enabled by default, but the
crate MUST compile and pass its tests with `--no-default-features`, and the
workspace minimal-feature checks MUST cover that configuration. Types from
`rig-core` or other Rig crates MUST NOT appear in the crate's non-`rig` public
API, persisted state, or A2A metadata. The `rakka` facade MUST propagate the
feature as an optional passthrough (`rakka-agent?/rig`).

Rakka MUST NOT treat provider clients, streams, open HTTP requests, or
credential values as durable state.

### 10.2 Persistence Compatibility

Raw Rig `AgentRun` serialization MUST NOT be the sole durable compatibility
format. Rakka MUST persist its own versioned loop representation and SHOULD pin
the Rig dependency to a reviewed version for each compatibility release.

Rig upgrades that change request, tool-call, message, or serialized run
semantics MUST receive an adapter compatibility review and, when required, an
explicit migration.

The Rig version pin and its compatibility review are properties of the `rig`
feature. A Rig upgrade MUST NOT change the Rakka-owned model adapter trait,
core domain types, or persisted loop representation.

### 10.3 Conversation Memory

Rig memory policies MAY be used to select, compact, summarize, or demote
history. Rakka's scoped memory stores and stable `MemoryOperationId` values
remain authoritative. Automatic memory callbacks MUST NOT bypass the durable
effect and deduplication boundary.

### 10.4 Deterministic Rig Test Adapter

`rakka-agent` SHOULD provide a deterministic test adapter that can script
model text/results, structured task-result proposals, tool/delegation
requests, and responses conditional on prior messages or tool results. It
MUST implement the Rakka-owned model adapter trait of Section 10.1 and MUST be
available without the `rig` feature. It SHOULD compose with fake tools, peers,
humans/authorization services, clocks, and memory adapters.

The test adapter MUST exercise the same durable model/effect/result path as a
production provider. It MUST NOT make tests pass by invoking the loop directly
around persistence. Testkit assertions SHOULD cover typed task state, ordered
runtime events, effect attempts, trace links, budgets, result rejections,
handoff/claim/turn ownership, passivation, and recovery.

## 11. Effect Model

### 11.1 Effect Intent

Every external operation MUST have a durable effect intent containing:

- stable `EffectId` and generation;
- tenant, goal, task, agent, run, and delegation identity as applicable;
- effect kind and target/tool identity;
- bounded payload or immutable artifact reference;
- canonical argument digest;
- settings and policy revision;
- timeout and deadline;
- safety class and retry/reconciliation policy;
- external idempotency key when applicable;
- logical credential binding reference;
- causation, correlation, and trace context; and
- expected bounded result or artifact type.

Resolved credentials MUST NOT be included.

### 11.2 Safety Class

```rust
enum EffectSafety {
    ReadOnly,
    Idempotent { external_key: ExternalIdempotencyKey },
    Reconcileable { protocol: ReconciliationProtocolRef },
    NonIdempotent,
}
```

The registered tool or adapter supplies the permitted safety declaration.
Model output MUST NOT be able to downgrade a tool from non-idempotent to
idempotent or bypass a required gate.

### 11.3 Effect State

The effect state model MUST distinguish at least:

```rust
enum AgentEffectStatus {
    Pending,
    Ready,
    Started,
    RetryScheduled,
    Succeeded,
    Failed,
    Exhausted,
    Indeterminate,
    Cancelled,
}
```

`Succeeded`, `Failed`, `Exhausted`, `Indeterminate`, and `Cancelled` are
terminal outcomes for one effect generation. Reconciliation of an
indeterminate effect records evidence against that outcome; if a new invocation
is authorized, it uses a new effect generation.

### 11.4 Dispatch Invariants

- The dispatcher MUST NOT invoke an external operation before `Started` is
  durably accepted with a valid lease/fence.
- The dispatcher MUST resolve credentials only after acquiring the attempt and
  MUST keep them only for the bounded dispatch attempt.
- Result commands MUST carry effect ID, generation, attempt, and lease/fence.
- Duplicate or stale results MUST NOT advance the run twice.
- A retry of an idempotent effect MUST reuse its external idempotency key.
- A retry policy MUST NOT override the non-idempotent ambiguity rule.

### 11.5 Crash and Timeout Rules

If an attempt is known not to have invoked the target, policy MAY return it to
`Ready`. Once invocation may have occurred:

| Safety class | Recovery after ambiguous worker loss |
| --- | --- |
| `ReadOnly` | Retry under bounded policy |
| `Idempotent` | Retry with the same external idempotency key |
| `Reconcileable` | Query authoritative outcome; retry only when proven absent |
| `NonIdempotent` | Persist `Indeterminate`; never auto-retry |

For a non-idempotent ambiguous attempt, the run MUST transition to
`WaitingForReconciliation`, all automatic dispatch eligibility for that effect
MUST be revoked, and a reconciliation checkpoint MUST be opened.

Cancellation does not prove that a started external effect was cancelled. An
ambiguous non-idempotent effect remains subject to reconciliation even if the
run has received a cancellation request.

### 11.6 Exactly-Once Claim

Rakka MUST NOT claim exactly-once external side effects. It MAY claim:

- durable intent before dispatch;
- deduplicated internal command and result acceptance;
- stable idempotency-key reuse where supported;
- authoritative reconciliation where supported; and
- no automatic retry of an opaque non-idempotent effect after ambiguity.

### 11.7 Tool Registry and Component Tools

The task/run-effective tool registry SHOULD distinguish at least function,
workflow, process, remote MCP, retrieval/memory, environment, and
agent-coordination tools. Every descriptor MUST declare stable name/version,
input/output schema, safety class, capabilities, credential binding class,
guardrails, timeout/deadline, and bounded result/artifact behavior.

Entities, views, or compiled workflows MAY be presented through tool adapters,
but their invocation MUST preserve the underlying component's durable identity
and effect semantics. A workflow tool creates/adopts a durable workflow run as
specified in Section 8.6.

Remote MCP MAY be supported as an optional adapter for ordinary tools. It MUST
NOT be used as an indirect peer-agent channel that bypasses the typed
coordination runtime and `rakka-a2a`. Resolved endpoint credentials and tool
responses remain subject to secret exclusion, content policy, and effect
safety.

### 11.8 Tool Authority and Execution Isolation

The runtime MUST distinguish:

- `ToolDescriptor`: bounded schema/description visible to Rig/model;
- `ToolBinding`: definition/setup-authorized target, safety, capability, and
  credential class;
- `EffectIntent`: exact target, canonical argument digest, and revisions; and
- `DispatchGrant`: current authorization to execute that exact intent.

Model selection or generation of a tool call is a request only. It MUST NOT
grant capability, credential access, network reachability, approval, or
executor placement.

Before durable `Started`, a dispatch grant MUST bind tenant, goal/task/agent/
run/effect, descriptor name/version/schema digest, target and argument digest,
safety class, definition/setup/settings/policy revisions, capability,
credential binding, effect-bound checkpoint/grant where required, expiry, and
allowed use count. The dispatcher MUST recheck immediate revocation and grant
validity before each attempt.

An effect intent SHOULD carry an application-owned `ExecutionPolicyRef`
describing the required trust domain, workload identity class, network-egress
class, sandbox/process class, secret-resolution class, and tenant-isolation
class. Rakka owns routing, persistence, and enforcement of the reference; the
application/platform owns the actual worker pool, Kubernetes RBAC,
NetworkPolicy/service-mesh policy, credential issuer, and sandbox.

Deployments MUST NOT claim strong per-agent isolation when all effects run in
a shared worker with ambient authority for every tool/tenant class. Rakka MUST
support routing by bounded trust/execution class and SHOULD support isolated or
ephemeral effect executors for consequential tools. Such workers are bounded
effect resources, not resident pods for logical agents.

When MCP or another catalog can change descriptors/endpoints dynamically, the
accepted effect MUST record or digest the selected descriptor/endpoint
revision. Recovery MUST NOT silently execute against a materially different
schema or target.

## 12. Durable Checkpoints and HITL

### 12.1 Checkpoint Kinds

```rust
enum AgentCheckpointKind {
    Approval,
    SecurityAuthorization,
    IndeterminateEffectReconciliation,
}
```

All checkpoint kinds use the same durable wait, notification, timer,
passivation, inbox-deduplication, and audit substrate. Their resolver policies
and A2A projections differ.

### 12.2 Checkpoint Record

A checkpoint MUST contain:

- stable checkpoint ID and kind;
- tenant, goal, task, agent, run, and delegation identity as applicable;
- prompt/decision summary without secret material;
- allowed decisions;
- required roles, capabilities, or policy reference;
- bound effect ID, target, and canonical arguments digest when applicable;
- settings and policy revision;
- creation, due, expiration, and escalation timestamps;
- escalation target;
- immutable context/evidence artifact references;
- creator and resolver principal references;
- status and immutable audit event references; and
- a decision deduplication key.

### 12.3 Grant Binding

An approval or authorization resolution MUST be bound to the exact effect
intent. At minimum the binding covers tenant, goal, task, agent, run, effect
ID, target, argument digest, policy/settings revision, expiration, resolver,
and allowed use count.

A changed binding MUST invalidate the previous grant. The dispatcher MUST
recheck the grant and current immediate-safety policy before invocation.

### 12.4 Authorization and Credentials

An authorization checkpoint MAY be resolved by an authenticated human,
authorization service, or negotiated A2A extension. The durable resolution
stores only non-secret grant metadata and a logical credential binding
reference.

Credential material SHOULD arrive out of band over a secure channel and MUST
be obtained again at dispatch time from the application-owned credential
resolver.

### 12.5 Reconciliation Decisions

An indeterminate checkpoint MAY resolve as:

- `ConfirmedCompleted`, with authoritative receipt/result evidence;
- `ConfirmedNotExecuted`, with evidence sufficient to create a new effect
  generation;
- `Compensate`, which schedules an explicitly defined compensation effect;
- `Escalate`; or
- `AbandonAndFail`.

The normal checkpoint API MUST NOT expose a plain `Retry` decision for an
ambiguous non-idempotent effect.

### 12.6 Passivation and Timers

Once a wait and its notification effect are durable, no live task is required.
The entity SHOULD become idle and passivate normally. A later A2A message,
secure callback, durable timer, cancellation, or administrative command
reactivates the run and recovers state.

Checkpoint SLA and escalation MUST use durable timers. Sensitive or
non-idempotent work MUST NOT auto-approve merely because a timer expired.

## 13. Memory Model

### 13.1 General Requirements

All memory APIs MUST:

- require explicit tenant and ownership scope;
- authorize before revealing existence or content;
- support stable idempotent operation IDs;
- preserve provenance and classification;
- provide retention, tombstone, and deletion semantics;
- keep resolved credentials and secret material out of records;
- use bounded inline content or immutable artifact references; and
- distinguish authoritative records from derived embeddings and indexes.

Memory is application-domain context. It MUST NOT replace durable run, inbox,
outbox, timer, checkpoint, or effect state as the correctness source.

### 13.2 Short-Term Session Memory

Short-term memory MUST be scoped by
`(TenantId, AgentId, AgentRunId)`. Reads or writes under one run MUST NOT be
visible through another run's session API, including another run of the same
agent.

Entries SHOULD be ordered by a monotonic sequence and include:

- `MemoryEntryId` and `MemoryOperationId`;
- role/type and bounded content or artifact reference;
- source command/effect ID;
- content hash;
- timestamp and revision;
- classification/redaction metadata; and
- compaction or summary provenance.

An append replay with the same operation ID MUST return the original logical
result without creating another entry.

Short-term memory MAY retain a bounded recent window plus rolling summaries.
Terminal-run retention is controlled by tenant policy.

### 13.3 Agent-Private Long-Term Memory

Private memory MUST be scoped by `(TenantId, AgentId)`. The originating
`AgentRunId` SHOULD be recorded as provenance but MUST NOT broaden access to
another agent.

A private memory SHOULD contain:

- stable memory and operation IDs;
- semantic, episodic, preference, or application-defined type;
- content or artifact reference and hash;
- source run/effect/entry references;
- confidence and creation/update timestamps;
- embedding model, dimensions, and version when embedded;
- retention, expiry, classification, and tombstone state; and
- policy/audit references.

Promotion, consolidation, or demotion from short-term memory MUST be an
idempotent durable effect. Embeddings are rebuildable derived projections, not
the only copy of memory content.

### 13.4 Communal Knowledge Graph

The graph MUST be scoped by `(TenantId, KnowledgeSpaceId)`. All authorized
agents in that space MAY read and append subject to capabilities and
classification policy.

Agents MUST append provenance-bearing claims rather than overwrite an
unqualified canonical fact. A claim SHOULD include:

- stable `ClaimId` and append operation ID;
- subject, predicate, and object or equivalent node/edge representation;
- source `AgentGoalId`, `AgentTaskId`, `AgentId`, `AgentRunId`, delegation, and
  effect;
- evidence artifact references;
- creation time, confidence, and policy revision;
- classification, ACL, and trust status; and
- optional embedding metadata.

Initial trust states are `Proposed`, `Verified`, `Disputed`, and `Retracted`.
Conflicting claims MAY coexist. Verification, dispute, and retraction MUST
preserve the original provenance and append an auditable transition or related
claim.

Policy MAY require HITL or a verifier service before a claim becomes
`Verified`, especially when it can authorize or materially influence a
high-impact effect.

### 13.5 Memory Context Snapshot

Before every model effect, the run MUST persist an immutable
`MemoryContextSnapshot` containing:

- the exact bounded short-term entries or immutable references used;
- selected private memory IDs and exact content/references;
- selected communal claim IDs, trust states, and exact content/references;
- retrieval queries and retriever versions;
- embedding/index version or watermark when available;
- trust, classification, and ranking policy revision;
- prompt/context budget accounting; and
- a content digest.

Retries of the same model effect MUST reuse the same snapshot. A new turn MAY
create a new snapshot from newer memory.

Retrieved memory MUST be represented as untrusted contextual data. It MUST NOT
be allowed to replace system instructions or expand tool capabilities.

### 13.6 Storage Adapters

The communal knowledge-graph API and adapter crate MUST be independent of the
underlying database, query language, licensing model, and deployment form.
Public traits and records MUST NOT expose a vendor client, vendor-specific
identifier, SQL, Cypher, SPARQL, or vendor result type.

The portable graph SPI MUST define Rakka-domain operations and capabilities,
including claim append by stable operation ID, claim lookup, bounded
relationship traversal, provenance/trust filtering, and optional semantic
search. An implementation MUST report optional capabilities rather than force
the core API to assume that every backend supports the same query features.

Recommended storage boundaries are:

- PostgreSQL for short-term and private authoritative records;
- `pgvector` or another configured vector backend for private semantic
  retrieval;
- object storage for large immutable memory/artifact content; and
- a deployment-selected relational, property-graph, RDF/triplestore,
  embedded, or managed-service implementation behind the communal graph SPI.

Backend bindings MAY live in separately versioned crates or in application
code. Changing a backend MUST NOT change Rakka's claim identity, provenance,
trust, idempotency, authorization, or retrieval-snapshot semantics.

Approximate vector results MAY be eventually consistent. Storage/query design
MUST preserve tenant and agent filters even when this reduces index
performance.

## 14. A2A Protocol Surface

### 14.1 Public Boundary

External clients and other agents MUST interact with Rakka Agents through
`rakka-a2a` and the A2A protocol. Rakka remoting remains trusted internal
cluster traffic and MUST NOT become a public client transport.

Incoming task messages, cancellations, settings commands, gate resolutions,
and other state-changing operations MUST be durably accepted and deduplicated
before successful acknowledgement.

### 14.2 Task Identity and Projection

- A2A `Task.id` SHOULD equal or map immutably to `AgentTaskId`.
- A delegated child task MUST have its own `AgentTaskId` and `AgentRunId`; it
  MUST NOT reuse the parent task/run identity.
- A handoff MUST preserve `AgentTaskId` while creating a new target-agent
  `AgentRunId`; source and target run lineage SHOULD be present in authorized
  task metadata/history.
- The target `AgentId` MUST be resolved through the authenticated endpoint,
  agent card/skill, or stable routing metadata.
- Agent Cards/skills SHOULD advertise versioned specialty, input/output,
  authorization, and compatibility metadata sufficient for catalog-based
  resolution without exposing credentials.
- A2A task projection is a public view, not the correctness source.
- Push notification scheduling MUST use the durable outbox.
- Stream connection loss MUST NOT cancel or lose a durable task/run.

### 14.3 Task/Run-State Mapping

| Rakka task/current-run condition | A2A `TaskState` |
| --- | --- |
| Task `Created`, `Blocked`, `Assigned` | `SUBMITTED` with bounded dependency/assignment metadata |
| Task `InProgress`; run `Accepted`, `Running`, `WaitingForTimer`, `WaitingForEffect`, `Suspended`, `Cancelling`, `Compensating` | `WORKING` |
| Task `WaitingForInput` (including a human-owned task awaiting its typed result) | `INPUT_REQUIRED` |
| `WaitingForApproval` | `INPUT_REQUIRED` |
| `WaitingForAuthorization` | `AUTH_REQUIRED` |
| `WaitingForReconciliation` | `INPUT_REQUIRED` with stable indeterminate reason |
| Task `Completed` | `COMPLETED` |
| Task `Failed` | `FAILED` |
| Task `Cancelled` | `CANCELED` |

Run `HandedOff` or `Superseded` MUST NOT make the public task terminal while a
successor run owns it. The projection follows authoritative task state and may
include the current run condition as metadata.

A `Suspended` run projects `WORKING` because A2A defines no paused state; the
projection SHOULD carry bounded suspension metadata so a client can
distinguish administrative suspension from active work.

A cancellation request alone MUST NOT project `CANCELED`. While cancellation
is propagating, project the authoritative nonterminal condition. If an
indeterminate consequential effect still requires reconciliation, project
`INPUT_REQUIRED` with bounded cancellation/reconciliation metadata until the
task is safe to make terminal under Section 8.7.

A2A `UNSPECIFIED` MUST NOT be used to represent an indeterminate external
effect.

### 14.4 Agent-to-Agent Effects

An outbound agent-to-agent message is an external effect. It MUST carry a
stable effect/deduplication identity, timeout, authentication binding,
causation, trace context, and response correlation. It MUST traverse the
durable outbox rather than directly invoking another local actor through a
public API shortcut.

For collaboration, `rakka-a2a` SHOULD define a versioned metadata extension or
management skill that can carry `AgentGoalId`, parent `AgentTaskId`/
`AgentRunId`, `AgentDelegationId`, handoff/team/moderation identity,
lineage/depth, criteria or result-contract references, allocated budgets,
capability scopes, and deadline. Unknown optional metadata MUST remain
compatible with ordinary A2A clients; required collaboration metadata MUST fail
closed when a peer cannot honor its version.

The extension MUST carry logical credential-binding references at most, never
resolved credentials. Duplicate delivery of one stable delegation/task
operation MUST resolve to the same child task or an explicit conflict. A child
task's terminal A2A state is evidence returned to the parent and MUST NOT
automatically make the root goal `Satisfied`.

Rakka Agents MUST NOT use generic HTTP, gRPC, MCP, or direct actor calls to
reach another Rakka Agent from inside the model/tool loop. Such calls would
bypass task identity, durable lineage, effect policy, and audit. Peer-agent
coordination uses the typed capability runtime and `rakka-a2a` effect adapter.

### 14.5 Typed Agent Client

`rakka-agent` SHOULD provide a typed `RakkaAgentClient` facade for:

- definition/setup discovery;
- typed task create, dependency declaration, assignment, and handoff;
- run-single-task convenience with automatic task finalization and no retained
  runtime residency;
- task/run/goal state and typed result query;
- suspend, resume, cancel, and terminate lifecycle commands; and
- replayable event subscription with cursor/resync behavior.

The facade MUST encode state-changing external/peer operations through
`rakka-a2a` and the same durable command/deduplication path. It MUST NOT expose
a local actor-ref shortcut with weaker behavior. A blocking result convenience
MAY exist for synchronous application callers, but the Rakka runtime MUST not
hold a thread or agent actor while waiting.

## 15. Passivation, Recovery, and Shard Movement

- Durable state MUST be sufficient to recover an agent, task, or run on a
  different pod without node-local memory.
- Waiting and otherwise quiescent agent/task/run/team/moderation entities
  SHOULD passivate under normal idle policy even when their goal remains
  `Active` or future tasks remain assigned/blocked.
- No correctness or liveness guarantee MAY depend on keeping an actor, async
  task, process, connection, stream, in-memory timer, open span, or dispatcher
  lease resident for the logical lifetime.
- A2A ingress, durable inbox/result delivery, durable timers, child-agent or
  workflow results, settings/cancellation commands, and recovery scanning MUST
  be capable of routing to and reactivating the current owner.
- After activation, the entity MUST load authoritative state, re-establish
  revision/ownership fencing, perform bounded transitions, persist the next
  effect or wait, and become passivatable again.
- Actor residency MUST be treated as an evictable computation cache, not as
  identity, durable ownership, or evidence that future work will run.
- Recovery MUST reacquire the latest revision and reject stale owner writes.
- A recovered run MUST inspect pending effects, timers, checkpoints, settings
  revisions, delegations, child workflows, goal evaluation, and cancellation
  before advancing.
- Shard movement MUST NOT make an effect dispatchable twice.
- Dispatcher leases and shard ownership are hints/fences, not proof of an
  external effect outcome.
- Pod-local SQLite, process memory, or per-pod PVC state MUST NOT be the
  production source of truth for run or memory recovery.
- Continuous goals MUST use bounded epochs triggered by durable timers/events
  and MUST become passivatable between epochs; an immortal poller is forbidden.
- Pod/actor/dispatcher start, restart, replacement, rollout, drain, or shard
  movement MUST NOT create an epoch, reset a wake schedule, replay missed
  occurrences without policy, or refill a budget.
- Shared timer/event scanners MAY restart on a pod, but they MUST recover
  durable `AgentWakeId` occurrences and inject them through deduplicated inbox
  commands; they MUST NOT infer logical occurrences from their own uptime.

## 16. Security and Authorization

- Every request MUST be authenticated and tenant-authorized before data access.
- Policy checks MUST occur before queries that could reveal resource
  existence across a tenant boundary.
- Tool capabilities MUST be declared outside model output and enforced before
  effect scheduling and dispatch.
- Model-visible tool descriptors MUST NOT imply dispatch authority. The
  admitted binding, exact effect intent, current dispatch grant, credential
  resolution, and execution-policy reference MUST each be validated at their
  boundary.
- Deployments claiming workload isolation MUST ensure the selected dispatcher
  or sandbox lacks ambient authority beyond its declared trust/tool/tenant
  class. Logical agent isolation MUST NOT be claimed solely from in-process
  checks inside one universally privileged worker.
- The runtime MUST apply versioned ordered guardrail stages, as configured, to
  A2A ingress/egress, retrieval/memory ingress, model request/response, and tool
  request/response boundaries.
- A guardrail outcome MUST be one of an explicit bounded set such as `allow`,
  `block`, `transform`, `report-only`, or `require-checkpoint`, with a stable
  reason code and protected evidence reference when required.
- Deployment/tenant policy MAY add mandatory guardrails that an agent
  definition, setup, model, or later settings update MUST NOT remove or weaken.
- A guardrail transformation MUST be deterministic under a recorded revision
  or executed/persisted as an explicit durable effect/artifact; a retry MUST
  reuse the accepted transformed input rather than silently re-evaluate against
  changing policy/content.
- `report-only` MUST NOT grant a capability or make a denied effect eligible.
- Guardrails MUST NOT be treated as a replacement for authentication,
  capability authorization, typed result rules, credential resolution, effect
  safety, reconciliation, or goal evaluation.
- Immediate revocation MUST be checked again before external invocation.
- Approval and authorization principals MUST be preserved as stable references
  with immutable audit evidence.
- Credentials MUST be resolved only for the bounded dispatcher attempt and
  MUST NOT be logged or persisted.
- Memory retrieval MUST enforce tenant, agent/knowledge-space, classification,
  and purpose restrictions before ranking results.
- Communal memory MUST be treated as a possible prompt-injection and poisoning
  source; provenance and trust policy MUST be available to the model context
  builder.
- Tool arguments, prompts, raw memory, resolved credentials, and high-cardinality
  IDs MUST NOT become metrics labels.

## 17. Audit and Observability

### 17.1 Signal Roles and Correctness Boundary

Rakka Agents MUST support correlated traces, metrics, structured logs/events,
audit records, and operational snapshots. Each signal has a distinct role:

- traces describe causality and latency for one bounded operation or linked
  group of operations;
- metrics describe aggregate rates, distributions, gauges, saturation, and
  SLO indicators;
- logs/events describe detailed operational occurrences;
- audit records provide immutable compliance/security evidence; and
- snapshots provide bounded point-in-time operational state.

No observability signal is the correctness source. Durable goal, agent, task,
run, assignment/coordination, workflow, inbox/outbox, effect, checkpoint,
timer, and memory state remain authoritative. Trace sampling, log loss,
event-sink failure, Collector failure, or backend unavailability MUST NOT
create, roll back, duplicate, or suppress a durable state transition.

Telemetry export SHOULD be asynchronous and bounded. Export failures and drops
MUST be observable through bounded metrics and operational snapshots and MUST
NOT create unbounded in-process queues.

### 17.2 Instrumentation Scope and Resource

Agent telemetry MUST use an explicit instrumentation scope name and version.
When OpenTelemetry GenAI conventions are emitted, the adapter MUST record the
reviewed convention/schema revision in instrumentation scope or schema
metadata.

Resource attributes SHOULD include, when applicable:

- `service.name`, `service.namespace`, and `service.version`;
- `service.instance.id`;
- `deployment.environment.name`;
- Kubernetes namespace, deployment, pod, pod UID, node, and container;
- stable Rakka node ID and runtime role; and
- deployment channel or region as bounded configured values.

Kubernetes/process identity belongs on the resource rather than being repeated
as labels on every agent metric.

### 17.3 Session, Task, and Goal Correlation

`AgentRunId` is the durable correlation identity for one agent session. The
OpenTelemetry adapter SHOULD map:

- `AgentId` to `gen_ai.agent.id`;
- a bounded configured telemetry/template name to `gen_ai.agent.name`;
- agent definition revision to `gen_ai.agent.version`;
- `AgentGoalId` to Rakka's restricted `rakka.agent.goal.id`;
- `AgentTaskId` to Rakka's restricted `rakka.agent.task.id`;
- `AgentDelegationId` to Rakka's restricted
  `rakka.agent.delegation.id`; and
- `AgentRunId` to `gen_ai.conversation.id`.

Tenant policy MAY require a stable scoped pseudonym instead of a raw
`AgentId`/`AgentGoalId`/`AgentTaskId`/`AgentRunId`/`AgentDelegationId` in
exported traces and logs. The reversible mapping, if any, MUST remain outside
the telemetry backend.

Agent/goal/task/run/coordination identifiers MAY appear in access-controlled
traces, logs, audit records, and query projections. They MUST NOT appear in hot
metric labels or propagated baggage.

A trace ID is not the session identity. One session MAY and normally will span
multiple trace IDs due to asynchronous dispatch, long waits, passivation,
recovery, shard movement, and independent A2A requests.

One `AgentTaskId` MAY span several execution sessions after handoff or
reassignment. One `AgentGoalId` MAY span many independently traced tasks,
sessions, and workflow runs. Task/goal assembly MUST use durable identities,
assignment/delegation lineage, causation, and span links rather than assuming
one trace tree or one `gen_ai.conversation.id`.

### 17.4 Bounded Trace Segments

The runtime MUST NOT hold one in-memory span open for the lifetime of a durable
agent session. It MUST end spans when the bounded operation ends, including
before a run becomes passivatable.

A session SHOULD be represented as a graph of bounded trace segments:

- A2A request/stream ingress;
- active agent invocation or turn;
- dispatcher effect attempt;
- model/provider call;
- tool and downstream operation;
- outbound A2A delegation;
- task assignment, handoff/reassignment, result validation, and dependency
  resolution;
- team claim/message and moderated-turn transitions;
- specialist child runs and durable workflow invocations;
- goal progress/evidence evaluation;
- checkpoint/timer park and later resume;
- recovery after restart/passivation/shard movement; and
- terminal completion/failure notification.

Synchronous nested work SHOULD use parent/child relationships. Deferred,
retried, resumed, fan-out/fan-in, or cross-trace work MUST use span links when a
simple enclosing parent relationship would be misleading.

Known links and sampling-relevant bounded attributes SHOULD be attached at
span creation so head/tail sampling policy can consider them.

### 17.5 Durable Trace Context

W3C `traceparent` and `tracestate`, bounded baggage, and causal span-link
metadata MUST survive every applicable durable boundary:

- A2A ingress and egress;
- agent delegation and workflow-run creation/result;
- inbox command acceptance;
- outbox effect scheduling;
- dispatcher attempt and callback/result command;
- timers;
- approval, authorization, and reconciliation checkpoints;
- passivation/recovery and shard movement;
- memory promotion/consolidation effects;
- goal progress/evidence evaluation; and
- process/MCP/tool protocols when they support propagation.

Rakka MUST persist serializable propagation metadata, not an SDK span/context
object. Invalid remote trace context MUST be handled according to the public
protocol policy without allowing malformed values into durable state.

After a long wait or recovery, the runtime SHOULD start a new bounded trace
segment and link it to:

- the span that persisted/parked the wait;
- the span for the timer, human, authorization, callback, or A2A trigger that
  caused the resume; and
- the prior dispatcher attempt when the resume concerns a retry or
  reconciliation.

### 17.6 Required Span Model

The OpenTelemetry adapter MUST use the reviewed GenAI semantic conventions
where they correctly describe an operation. Rakka-specific durable/runtime
operations use stable `rakka.agent.*` names.

| Agent operation | Required span behavior |
| --- | --- |
| A2A ingress | Protocol `SERVER` span that extracts context before durable acceptance |
| Continuous wake/epoch admission | `rakka.agent.wake.admit` bounded span linked to timer/event/A2A trigger with bounded outcome class |
| Autonomy admission | `rakka.agent.autonomy.admit` bounded span/event with operation class and allow/deny reason |
| Budget operation | `rakka.agent.budget.reserve` or `.settle` bounded span/event with scope/dimension class and outcome |
| Active turn/invocation | Bounded `invoke_agent {agent.name}` `INTERNAL` span |
| General decision | `rakka.agent.decide` `INTERNAL` span or event |
| Explicit planning | `plan {agent.name}` `INTERNAL` span only when planning is reliably distinguishable |
| Model inference | `{gen_ai.operation.name} {gen_ai.request.model}` `CLIENT` span, or `INTERNAL` for a same-process model |
| Embeddings | `embeddings {model}` span |
| Retrieval | `retrieval {data_source}` span |
| Memory operation | Standard create/search/update/upsert/delete memory span |
| Effect schedule | `rakka.agent.effect.schedule` `PRODUCER` span ending after durable acceptance |
| Effect dispatch | `rakka.agent.effect.dispatch` `CONSUMER` span linked to schedule/prior attempts |
| Tool dispatch grant | `rakka.agent.tool.authorize` bounded span/event with tool/trust class and allow/deny reason |
| Tool execution | `execute_tool {tool.name}` `INTERNAL` span with downstream client spans |
| Outbound A2A call | `invoke_agent {peer.name}` `CLIENT` span |
| Task result validation | `rakka.agent.task.validate_result` `INTERNAL` span with bounded rule class/outcome |
| Handoff | `rakka.agent.handoff` `PRODUCER`/`CONSUMER` segments linking source and target runs |
| Team operation | `rakka.agent.team.claim` or `.message` bounded span |
| Moderated turn | `rakka.agent.moderation.turn` bounded span with round/participant class |
| Workflow tool invocation | `rakka.agent.workflow.invoke` `INTERNAL` span with bounded workflow class linked to the child workflow run |
| Goal evaluation | `rakka.agent.goal.evaluate` `INTERNAL` span with criteria revision, evaluator class, evidence counts/refs, and outcome |
| Checkpoint open | `rakka.agent.checkpoint.open` span ending after durable park/notification scheduling |
| Run resume/recovery | `rakka.agent.run.resume` or `rakka.agent.run.recover` span with causal links |

Span names MUST use stable operation/tool/agent classes from configured
registries. They MUST NOT embed raw goal/task/agent/run/coordination/effect IDs,
user input, URLs, arguments, or result text.

Span status MUST follow OpenTelemetry error guidance. Error spans SHOULD carry
a stable low-cardinality `error.type` and Rakka error code rather than an
unbounded error message as a grouping attribute.

### 17.7 Agent Decision Observability

Every durable agent-loop decision MUST produce a structured runtime event and,
when tracing is enabled, a correlated decision span or span event.

Decision telemetry MUST include, when applicable:

- agent/goal/task/run and coordination identity under the applicable telemetry
  access policy;
- durable event sequence, causation ID, and correlation ID;
- turn index and loop phase;
- decision kind;
- decision source;
- selected tool/target classes and count;
- settings, policy, plan, run-state, and memory-context revisions;
- budget/limit outcome and stop reason;
- effect safety class and gate result;
- state before/after as bounded labels; and
- a stable reason code or protected artifact reference to an authorized
  redacted decision summary.

Initial decision kinds SHOULD include `continue`, `call-tools`, `delegate`,
`handoff`, `team-operation`, `moderated-turn`, `submit-result`, `wait`,
`complete`, `fail`, `request-approval`, `request-authorization`, and
`reconcile`. Initial sources SHOULD include `model`, `deterministic-policy`,
`human`, and `authorization-service`.

Decision telemetry MUST NOT require or imply capture of hidden chain-of-thought
or private model reasoning. Operational explainability is provided by durable
inputs/revisions, selected action, policy evaluation, effect/result evidence,
and protected summaries.

### 17.8 Model and Provider Observability

Each model/provider operation executed through the model adapter MUST be
observable as a GenAI model span.
When supplied by the provider or safely known, the span/log/metrics mapping
SHOULD include:

- operation name;
- provider and requested/response model;
- response/finish reason;
- input, output, cached, and reasoning token counts;
- streaming flag, time to first chunk, and chunk timing;
- retry count and bounded retry reason;
- timeout/cancellation/error type;
- settings/model-profile revision;
- context-snapshot digest/reference and bounded size/count; and
- provider response ID only in restricted traces/logs, never metric labels.

If the provider reports both billable and model-consumed token counts, the
OpenTelemetry adapter SHOULD follow the reviewed convention's billing-token
rule. It MUST NOT invent token usage when unavailable; an estimated value must
be explicitly marked as estimated.

An automatic provider retry within one logical request MAY remain inside one
logical GenAI span with retry events. Durable Rakka effect attempts MUST remain
individually correlatable because they govern fencing, idempotency, and
indeterminate outcomes.

### 17.9 Tool and Effect Observability

Every scheduled tool effect and dispatcher attempt MUST be traceable from
decision through durable intent to result or indeterminate state.

Tool/effect telemetry MUST include bounded or restricted fields for:

- effect kind, generation, attempt, and safety class;
- tool name/type from the configured registry;
- durable schedule/start/result timestamps;
- queue delay, dispatch duration, and downstream duration;
- retry policy bucket and attempt number;
- idempotency/reconciliation support as a boolean/class, never the external
  idempotency key itself;
- approval/authorization requirement and outcome;
- terminal outcome: success, failed, exhausted, cancelled, or indeterminate;
- compensation/reconciliation relationship; and
- stable error/reason code.

The `execute_tool` span SHOULD wrap the application-level tool execution. A
tool's HTTP, RPC, database, process, object-store, or A2A calls SHOULD appear as
normal child client spans using the relevant semantic conventions.

An indeterminate transition MUST be an error/important event suitable for
tail-sampling retention and alerting. It MUST link to the ambiguous dispatch
attempt and later reconciliation decision.

### 17.10 Memory and Retrieval Observability

Short-term, private long-term, and communal memory operations MUST be
distinguishable by bounded memory tier and operation.

Memory/retrieval spans SHOULD record:

- standard GenAI operation name for retrieval or memory mutation;
- memory tier and backend class;
- authorized knowledge-space/data-source class;
- result/record count;
- duration and outcome;
- embedding model/version and dimensions when applicable;
- context-snapshot digest/reference and size/count;
- trust/classification filter outcome; and
- consistency/index watermark when safely available.

Raw retrieval query text, returned memory records, graph claims, embeddings,
and context-snapshot content MUST be content-capture opt-in and MUST NOT be
metric labels.

### 17.11 HITL, Authorization, Wait, and Recovery Observability

Checkpoint telemetry MUST distinguish approval, authorization, and
indeterminate reconciliation. It SHOULD include open/resolved/expired/escalated
status, bounded resolver type, policy class, and wait duration.

The span that opens a checkpoint MUST end after the durable wait and
notification effect are accepted. No span object is held during passive wait.
The later resolution/resume span MUST link to the parked span and the incoming
human/service request span.

Recovery spans MUST include bounded recovery cause and outcome, prior state,
new owner/runtime component, recovered pending counts, stale-write conflicts,
and recovery duration. Raw node/entity paths and high-cardinality owner IDs
belong in restricted logs/snapshots rather than metric labels.

### 17.12 Metrics

The OpenTelemetry adapter SHOULD emit, when supported by the reviewed GenAI
convention and source data:

- GenAI client operation duration;
- input/output token usage;
- time to first chunk and time per output chunk;
- agent invocation duration;
- multi-agent workflow duration; and
- tool execution duration.

Rakka Agent metrics SHOULD additionally cover:

- decision count and duration;
- goal evaluation count/duration/outcome and bounded progress/stagnation class;
- delegation count/duration/outcome and active descendant count;
- workflow-tool invocation count/duration/outcome;
- task assignment/handoff/result-validation/dependency count and duration;
- continuous wake accepted/duplicate/stale/coalesced/missed/late count, epoch
  admission/result, and schedule-revision conflict;
- autonomy admission allow/deny/expiry/recheck and budget
  reserve/settle/deny/exhaustion by bounded scope/dimension class;
- team claim/transfer/message and moderation turn/round count and duration;
- active turn duration and outcome;
- logically active/waiting goals and runs by bounded status class;
- currently resident entities, activation/passivation rate, cold-activation
  latency, and state-recovery latency;
- durable trigger/timer/outbox backlog and oldest age;
- wait duration and current age by wait/checkpoint kind;
- effect queue/dispatch latency, retry/exhaustion, and indeterminate count;
- model/tool call rates, latency, errors, tokens, and streaming delay;
- memory operation/retrieval latency and returned-record count;
- context snapshot size/count;
- recovery rate/duration/failure;
- inbox/outbox backlog, dispatcher in-flight/backlog, mailbox/stream pressure,
  and shard ownership distribution; and
- telemetry export queue, drops, failures, and Collector/exporter health.

Metric labels MUST be bounded and documented. Suitable labels include bounded
agent class/template and release version, model profile, provider class, tool
class, decision kind, decision source, outcome, error code, effect safety
class, checkpoint kind, memory tier, backend class, tenant tier, and deployment
channel.

Raw `TenantId`, `AgentId`, `AgentGoalId`, `AgentTaskId`, `AgentRunId`,
coordination/effect/checkpoint/memory/claim/workflow-run IDs, provider response
IDs, prompts, tool arguments/results, URLs, user values, and full errors MUST
NOT appear in metric labels.

Sampled traces MUST NOT be used to derive correctness totals or denominators.
Use metrics and durable query/audit projections for counts and SLOs.
When supported by the application SDK and backend, histograms SHOULD include
exemplars linking representative measurements to sampled trace/span IDs.

### 17.13 Structured Logs, Runtime Events, and Audit

Structured logs emitted during an active span MUST carry OpenTelemetry
`trace_id`, `span_id`, and trace flags when available. They SHOULD also carry
durable event sequence, causation ID, correlation ID, state revision, stable
event name, stable error code, and redaction/classification state.

Runtime events MUST be emitted only after the corresponding durable transition
succeeds. Duplicate processing MUST NOT create two logical runtime events for
one durable transition. Runtime-event sink failure is observable but does not
make a persisted transition false.

Task/run/coordination event projections SHOULD support a monotonic scoped
sequence, bounded retention, reconnect cursor, and explicit expired-window/
resync response. Derived struggle signals—approaching budgets, repeated
iteration failure, repeated result rejection, stuck dependencies, stalled team
claims, or moderation exhaustion—MUST remain observability projections and
MUST NOT independently mutate correctness state.

The runtime SHOULD produce immutable, queryable audit events for:

- agent creation, settings change, suspension, and retirement;
- autonomy admission/rejection/expiry and policy recheck;
- goal proposal, activation, criteria revision, progress evaluation, wait,
  satisfaction, failure, cancellation, and expiry;
- wake creation/admission/duplicate/stale/coalesced/missed/late outcome,
  schedule revision, epoch creation, suspension, renewal, and retirement;
- budget allocation/reservation/settlement/return/threshold/exhaustion by
  scope and dimension class;
- task creation, dependency, assignment, handoff/reassignment, result proposal,
  result acceptance/rejection, completion, failure, and cancellation;
- delegation creation/resolution, child task acceptance/result, fan-in,
  cancellation propagation, and reassignment;
- team creation/membership/claim/transfer/message/disband and moderated
  conversation/turn/round/termination;
- workflow-tool invocation, child workflow identity, result, and cancellation;
- run acceptance, start, decision, wait, resume, completion, failure, and
  cancellation;
- model request/response metadata and token/cost evidence without content;
- effect creation, start, retry, result, exhaustion, and indeterminate
  transition;
- checkpoint open, resolution, timeout, escalation, and invalidation;
- memory append, promotion, verification, dispute, retraction, tombstone, and
  deletion;
- authorization grant reference and dispatch-time policy outcome;
- tool binding/descriptor revision, dispatch-grant outcome, and selected
  execution-policy/trust class; and
- shard ownership/recovery events relevant to an incident.

Audit events MUST reference durable identities and revisions without embedding
credentials, hidden reasoning, or unrestricted prompt/tool/memory payloads.

### 17.14 Content Capture and Redaction

Production content capture MUST be disabled by default for:

- system instructions and prompt variables;
- model input/output messages and hidden reasoning;
- tool definitions, arguments, and results;
- retrieval query text and returned documents;
- short-term/private/communal memory content and embeddings;
- authorization material and credential values; and
- A2A message/artifact bodies.

Default telemetry SHOULD record only bounded metadata such as content byte/token
counts, hashes/digests, redaction/classification status, result counts, and
immutable protected artifact references.

An opt-in content policy MUST specify tenant/scope, principal/capability,
purpose, allowed fields, redaction, encryption, access control, retention,
deletion, sampling interaction, and audit behavior. Credentials, authentication
headers, secret values, decrypted private keys, and equivalent security
material MUST never be captured, including under opt-in.

Detailed content SHOULD be stored in application-owned protected artifact
storage with a reference in telemetry rather than as large span/log
attributes. Collector allowlist/redaction/transform processors MUST be used as
defense in depth, but the application MUST minimize sensitive emission before
export.

### 17.15 Baggage

Baggage MAY carry only policy-approved, bounded routing/context classes such as
tenant tier, deployment channel, workload class, and policy class.

Baggage MUST NOT carry raw tenant/user/goal/task/agent/run/coordination IDs,
prompts, completions, memory content, tool payloads, credentials, authorization
scopes/tokens, or personal data. Baggage received from an external caller MUST
be treated as untrusted and MUST NOT grant authorization or expand capabilities.

### 17.16 Sampling

Sampling MUST affect only trace export/recording. It MUST NOT affect durable
events, audit records, metric recording, effect safety, or execution policy.

When sampling is required, policy SHOULD retain all trace segments containing:

- `ERROR` status or stable failure codes;
- indeterminate effects or reconciliation;
- security denials, policy overrides, or credential/capability revocation;
- checkpoint escalation/timeout;
- recovery failure or stale-owner conflict;
- configured high latency or excessive retry; and
- newly deployed agent/model/tool versions under investigation.

Routine successful turns MAY be sampled at a lower rate.

Tail sampling is RECOMMENDED when these end-of-trace decisions justify its
operational cost. All spans for one trace MUST reach the same tail-sampling
instance. A horizontally scaled Collector deployment using tail sampling MUST
use trace-ID-aware routing and MUST size decision wait, trace buffers, memory
limiter, queues, and exporter retry together.

### 17.17 OTLP and Collector Boundary

The application binary owns the OpenTelemetry SDK, `tracing` subscriber/layer,
OTLP exporter, exporter credentials, and shutdown/flush behavior. Rakka core
crates SHOULD remain SDK/version neutral while providing structured spans,
events, metrics, logs, resource helpers, and serializable bridge records.

The OpenTelemetry adapter MUST preserve span kind, status, events, links,
instrumentation scope/schema, and applicable GenAI metric unit, bucket,
temporality, and exemplar semantics. If an existing Rakka bridge record cannot
represent a required field, the implementation MUST extend the bridge
additively or map directly into the application SDK; it MUST NOT silently drop
the field while claiming semantic-convention compliance.

Production deployments SHOULD export traces, metrics, and logs over OTLP to an
OpenTelemetry Collector. The Collector SHOULD provide:

- memory limiting and bounded queues;
- batching and exporter retry;
- Kubernetes/resource enrichment;
- allowlist/redaction/filter/transform processing;
- sampling and trace-ID-aware routing when tail sampling is enabled;
- TLS/mTLS/authentication and network isolation;
- one or more backend exporters selected by the operator; and
- its own internal telemetry for refusal, queue, drop, processing, and export
  failures.

The selected Collector distribution and component versions MUST be pinned and
revalidated during upgrades. Example manifests are not an evergreen security
or compatibility guarantee.

### 17.18 Authoritative Operational Queries and Observability Views

Rakka Agent integrations MUST provide an authorized authoritative point query
derived from durable task/run/effect/checkpoint/timer/budget state. It MUST
remain useful when the entity is passivated, telemetry is sampled or delayed,
and the Collector/exporter is unavailable.

An `AgentOperationalSnapshot` SHOULD expose:

- durable state revision and observation time;
- logical lifecycle separately from current/last runtime residency;
- current goal/task/run/phase, assignment/owner, and last material progress;
- current wait reason and next durable wake occurrence;
- budget allocations, reservations, consumption, thresholds, and exhaustion;
- pending effects, attempts, safety classes, grants, and indeterminate work;
- checkpoint/authorization state and bounded resolver requirements;
- cancellation progress;
- last activation, recovery, passivation, shard owner, and dispatcher state;
  and
- durable event cursor plus derived-projection revision/lag.

Cancellation progress MUST distinguish at least `NotRequested`, `Requested`,
`Propagating`, `Quiesced`, `WaitingForReconciliation`, and `Completed`. This
state MUST follow Section 8.7 and MUST NOT infer terminal cancellation merely
from acceptance of an A2A cancellation request.

Authoritative point reads MUST return a durable state revision. List/search
queries MAY use eventually consistent indexes but SHOULD expose projection
revision/lag and MUST NOT be used to authorize or advance execution.

Rakka Agent integrations SHOULD provide an authorized query/projection keyed by
tenant plus `AgentId`/`AgentRunId` that assembles references to:

- current durable state and ordered runtime events;
- linked trace segments;
- correlated logs and audit records;
- decisions and state/policy/context revisions;
- model calls, tokens, streaming latency, finish/error reason;
- tool/effect attempts, retries, safety class, and outcome;
- memory retrieval snapshot and result metadata;
- waits, checkpoint decisions, and resolver type;
- activation, recovery, passivation, shard owner, and dispatcher transitions;
- logical lifecycle state separately from current/last runtime residency; and
- protected content artifacts when the caller is authorized.

This view MUST remain an observability projection and MUST NOT become an
alternate execution state machine.

Rakka Agent integrations SHOULD provide an authorized task projection keyed by
tenant plus `AgentTaskId`. It SHOULD assemble the typed definition/result,
dependencies, artifacts, current assignment, handoff/reassignment history,
all contributing runs, result-rule decisions, checkpoints, A2A events, and
terminal task outcome.

Rakka Agent integrations SHOULD also provide an authorized goal projection
keyed by tenant plus `AgentGoalId`. It SHOULD assemble root and specialist
tasks/runs, handoff and delegation/fan-in graphs, teams and moderated
conversations, workflow invocations, budget allocation and consumption,
progress/evidence evaluations, shared knowledge/artifact references,
cancellation propagation, and the terminal goal decision. These views likewise
MUST NOT become alternate execution state machines.

### 17.19 Operational Views and Alerts

Initial dashboards and alerts SHOULD cover:

- active/waiting/stagnant/terminal goals and goal-evaluation outcomes;
- blocked/assigned/in-progress/waiting/terminal tasks, dependency age, result
  rejection, handoff, and reassignment;
- delegation fan-out/depth, active descendants, failures, timeouts, and cycles;
- team backlog/claim age/transfer and moderation turn/round exhaustion;
- workflow-tool invocation latency, failure, and cancellation;
- active, waiting, failed, cancelled, and indeterminate runs/effects;
- continuous wake backlog/age, coalescing, missed/late occurrence, active epoch,
  and retirement state;
- autonomy admission denials/expiry and budget allocation/reservation/
  exhaustion by bounded scope/dimension;
- A2A acceptance latency and durable rejection/duplicate/conflict rate;
- active-turn and decision latency;
- model/provider latency, errors, tokens, and streaming delay;
- tool latency, failure, retry, exhaustion, and indeterminate rate;
- checkpoint count, age, timeout, and escalation;
- memory retrieval/write latency and error rate;
- recovery duration/failure and shard ownership imbalance;
- dispatcher backlog/in-flight and inbox/outbox pressure; and
- Collector/exporter queue, refusal, drop, and failure signals.

Alert thresholds remain deployment/application policy. Alerts MUST use bounded
aggregates and SHOULD link to an authorized high-cardinality session query.

### 17.20 Semantic Convention Compatibility

OpenTelemetry's GenAI semantic conventions were Development at the time of this
specification. The Rakka Agent domain MUST use its own stable internal event,
metric, and attribute vocabulary. Mapping to GenAI conventions MUST be behind
the `otel` feature or another adapter boundary and MUST pin/document a reviewed
convention revision.

An upgrade MUST review span names/kinds, metric names/units/buckets, required
attributes, operation values, content-capture guidance, and Collector rules.
Telemetry convention changes MUST NOT require durable agent-state migration
unless an independent domain change also requires it.

## 18. Required Recovery Scenarios

Each scenario is bound to a milestone (Section 2.1) and joins the acceptance
gate when that milestone is implemented. Scenarios 15, 16, 18, and 20 bind at
M2; scenarios 36 and 47-51 bind at M3; scenarios 27-34 and 39 bind at M4;
scenarios 38, 41-43, and 45 bind at M5. All other scenarios, including 58-61,
bind at M1 and form the initial acceptance gate.

The implementation is not production-ready until tests demonstrate:

1. duplicate A2A task message acceptance does not create two tasks, initial
   runs, or turns;
2. agent/task/run actor restart after each loop transition resumes correctly;
3. passivation during approval, authorization, timer, and reconciliation waits
   consumes no live execution task and resumes on the next command;
4. shard movement rejects stale owner state writes;
5. dispatcher loss before durable `Started` safely redispatches;
6. dispatcher loss after `Started` retries a read-only effect under policy;
7. dispatcher loss after `Started` reuses the same idempotency key for an
   idempotent effect;
8. dispatcher loss after `Started` reconciles a reconcileable effect before
   any retry;
9. dispatcher loss in the ambiguous non-idempotent window produces exactly
   one durable `Indeterminate` outcome and no automatic re-invocation;
10. duplicate or stale tool/model completions do not advance twice;
11. duplicate human/authorization decisions do not resume twice;
12. a changed effect digest invalidates an old approval;
13. immediate capability or credential revocation prevents later dispatch;
14. short-term memory is isolated by both `AgentId` and `AgentRunId`;
15. concurrent runs append private memory without stale overwrite;
16. replayed memory and graph writes are idempotent;
17. a model-effect retry uses the original memory context snapshot;
18. unauthorized graph/private-memory reads do not reveal existence;
19. terminal run recovery does not reschedule completed effects;
20. every communal graph backend passes the same claim identity, idempotent
    append, provenance, trust-filtering, authorization, and bounded-query
    conformance suite without changing agent-domain code;
21. A2A ingress, decisions, Rig model calls, effect scheduling, dispatcher
    attempts, tool calls, waits, recovery, and terminal outcomes are
    reconstructable as one authorized session view by `AgentRunId`;
22. passivation and long waits leave no open in-memory span and later resume
    spans link to both parked and triggering operations;
23. trace context and causal links survive dispatcher restart, owner pod loss,
    and shard movement without changing effect behavior;
24. trace sampling does not change metrics, audit records, runtime-event
    acceptance, or durable execution;
25. default telemetry contains no prompt, completion, hidden reasoning, tool
    payload, memory content, or credential material; and
26. unavailable Collector/exporter paths do not block correctness and produce
    bounded queue/drop/failure visibility;
27. a root run durably fans out to multiple specialist agents, passivates, and
    deterministically resumes/fans in after restart or shard movement;
28. replaying a delegation command or A2A send creates exactly one logical
    child task/run or an explicit conflict;
29. root, parent, or dispatcher crashes do not replay a child's opaque
    non-idempotent effect, and ambiguity remains indeterminate in the child;
30. a root goal becomes `Satisfied` only after the current success-criteria
    revision is evaluated against durable evidence;
31. cancellation, deadline, and immediate revocation propagate durably to
    children without falsely claiming that their started effects stopped;
32. replaying a workflow-tool invocation creates or adopts one durable child
    workflow run and does not duplicate its internal effects;
33. concurrent specialist appends to communal memory retain goal/task/run/
    delegation provenance and stable append idempotency; and
34. depth, fan-out, descendant, concurrency, budget, and cycle limits fail
    closed and are recoverable after coordinator loss;
35. an `Active` goal and its waiting runs can all passivate with no per-agent
    actor, async task, thread, process, connection, lease, timer task, or open
    span, then one durable trigger reactivates the correct owner and advances
    exactly once; and
36. a continuous goal completes one bounded epoch, persists its next durable
    wake condition, passivates, and later resumes without an immortal poller;
37. replaying typed task creation/dependency/assignment commands yields one
    `AgentTaskId`, one dependency edge, and one current assignment;
38. handoff preserves `AgentTaskId`, terminates/fences the source run, creates
    one target-agent `AgentRunId`, and does not expose source session/private
    memory;
39. delegation creates exactly one child `AgentTaskId`/`AgentRunId` while the
    parent task identity and ownership remain unchanged;
40. a malformed or rule-rejected task result never completes the task, persists
    one rejection decision, and consumes only bounded additional iterations;
41. a human-owned typed task can unblock dependents after authenticated,
    deduplicated completion, while a failed human task propagates its declared
    dependency policy;
42. concurrent team members atomically claim a task so only one normal current
    owner may schedule effects, and stale claim/release/transfer commands fail
    closed;
43. moderation recovers participant, round, turn owner, transcript reference,
    and budgets after passivation/shard movement without duplicating a turn;
44. per-run setup/settings cannot add an undeclared tool/peer/model, widen
    authorization, or weaken a mandatory guardrail;
45. task/run/coordination event replay resumes from a cursor or returns an
    explicit retention-gap/resync response; and
46. an idle agent with assigned/blocked future tasks auto-passivates without
    requiring `terminate` or `suspend` and reactivates when work becomes
    eligible;
47. pod/actor/dispatcher start, restart, rollout, and shard movement create no
    continuous epoch unless a durable wake is independently due/accepted;
48. duplicate timer scans, events, callbacks, or A2A trigger delivery resolve
    to one `AgentWakeId` and at most one child epoch task/run;
49. an obsolete schedule revision cannot admit an epoch after an update, and a
    restart does not reset the revision or missed-occurrence policy;
50. the default overlap policy coalesces concurrent triggers while exactly one
    epoch owns execution, and the default downtime policy admits at most one
    coalesced epoch rather than unbounded catch-up;
51. continuous epochs use distinct finite task/run short-term-memory scopes and
    recover cross-epoch continuity only from authorized goal/private-memory/
    artifact state;
52. budget allocation/reservation/settlement survives restart and concurrency
    without oversubscription, and a `Started` attempt that becomes
    `Indeterminate` still consumes its applicable attempt budget;
53. unattended execution fails closed when admission is missing/expired or a
    settings update widens an unadmitted tool, peer, credential, environment,
    schedule, or budget scope;
54. a model-visible tool call remains undispatchable when its binding, grant,
    credential, checkpoint, execution-policy, or immediate safety check fails;
55. bounded task materialized state remains within configured limits while
    older history/content is available only through authorized cursors or
    artifact references;
56. authoritative lifecycle/wait/wake/budget/effect/cancellation queries remain
    correct when telemetry is sampled, delayed, dropped, or unavailable; and
57. cancellation with an ambiguous consequential effect fences all new work,
    remains nonterminal in reconciliation, and projects terminal cancellation
    only after the effect outcome/risk is explicitly resolved;
58. replaying any Section 9.8 inter-entity exchange (creation, assignment,
    run acceptance, result proposal/decision, allocation, settlement/return)
    produces one logical transition per operation ID on both entities;
59. loss of the run or task entity at any point in the result exchange,
    including after the task records its validation decision, converges on
    recovery without a second validation, a duplicate completion, or a lost
    rejection;
60. cross-entity commands between colocated entities traverse durable
    outbox/inbox acceptance and remain correct after the entities move to
    different nodes; and
61. dispatch-time budget reservation touches only the run's own durable
    ledger, and replaying an allocation, settlement, or return command never
    double-debits or double-credits a parent scope.

Fault injection SHOULD kill the dispatcher or owner pod at every durable
effect boundary, including after a test external system commits but before it
returns the receipt.

## 19. Crate and Feature Shape

The intended workspace shape is:

- `rakka-agent`: goal/typed-task/run/evaluation/handoff/delegation/team/
  moderation/workflow-tool domain, typed client, loop runtime,
  provider-neutral model adapter trait, continuous wake controller,
  escrow-based hierarchical budget ledger, autonomy admission, guardrails,
  gates, tool-binding/dispatch-grant/effect policy, execution-policy
  references, bounded operational query contracts, session/private memory
  traits, structured decision/runtime telemetry, and deterministic test
  support;
- `rakka-agent-postgres`: PostgreSQL session/private memory and `pgvector`
  retrieval adapter;
- `rakka-agent-knowledge-graph`: optional database-agnostic communal graph
  domain, adapter SPI, portable query/capability model, and test support;
- `rakka-a2a` `agents` feature: agent/goal/task/run routing, versioned
  coordination metadata, task projection/event replay, and authenticated
  command/gate mapping; and
- top-level `rakka` feature gates and curated prelude exports after API review.

The `rakka-agent` `rig` feature (default) supplies the Rig-backed
implementation of the model adapter trait and owns the pinned Rig version per
Section 10.1. The core crate MUST build and test without it, the deterministic
test adapter MUST NOT require it, and the top-level `rakka` facade MUST
propagate it as an optional passthrough feature.

The `rakka-agent` `otel` feature SHOULD map the stable Rakka telemetry domain to
a pinned, reviewed OpenTelemetry GenAI semantic-convention revision and compose
with the existing agent-workflow OTLP bridge. It MUST NOT own application
exporter credentials or require the core runtime to install a global SDK.

Concrete communal graph backend bindings MAY be application-owned or published
as separate adapter crates. `rakka-agent-knowledge-graph` MUST NOT depend on a
specific database driver. Adapter crates MAY be deferred while the core traits
and in-memory contract are proven. Production durability claims MUST NOT be
based solely on the in-memory implementations.

## 20. Compatibility and Migration

- All persisted goal, agent, typed-task/result, run, assignment/handoff,
  wake/schedule, budget/admission, delegation/team/moderation, workflow-link,
  loop, effect/grant/execution-policy, checkpoint, memory, and claim records
  MUST carry schema versions where evolution is expected.
- New statuses and fields SHOULD be additive and use stable kebab-case labels
  and stable error codes.
- Existing `WaitingForHuman` data requires an explicit compatibility mapping if
  replaced by typed waiting states.
- A Rig dependency upgrade requires adapter and serialized-artifact review.
- An OpenTelemetry GenAI convention upgrade requires an adapter compatibility
  review but MUST NOT by itself require durable agent-state migration.
- The `rakka-a2a` agent surface MUST pin a reviewed A2A protocol version. The
  target baseline is A2A 1.0; retaining or bridging an older surface requires a
  documented compatibility matrix, explicit state/operation mapping, and
  protocol negotiation rather than accidental mixed-version behavior.
- N/N+1 nodes MUST agree on protocol/status/schema compatibility before sharing
  a cluster during rolling updates.
- Unsupported state versions MUST fail closed rather than executing with
  guessed semantics.

## 21. Decision Register

### 21.1 Resolved Article-Review Decisions

The following decisions are normative in this draft:

1. Continuous goals are stable durable controllers that admit finite child
   epoch tasks/runs; pod lifetime never defines agent lifetime or wake-up.
2. Default wake policy forbids overlap, durably coalesces concurrent triggers,
   admits at most one coalesced occurrence after downtime, and fences obsolete
   schedule revisions.
3. Budgets are hierarchical durable allocations/reservations with per-epoch
   and rolling/window ceilings; started/retried/indeterminate attempts consume
   applicable budget.
4. Unattended execution requires fail-closed autonomy admission.
5. Tool descriptor, binding, intent, dispatch grant, and executor isolation
   are separate contracts; deployments route by bounded execution/trust class.
6. Task materialized state is bounded separately from history, content,
   memory, and derived projections.
7. Authoritative operational queries do not depend on telemetry, and
   cancellation remains nonterminal during consequential-effect
   reconciliation.
8. The target agent protocol baseline is a reviewed, pinned A2A 1.0 contract.

### 21.2 Resolved Design-Review Decisions

A follow-up design review (2026-07-10) resolved four additional defaults:

9. Requirements bind by milestone (Section 2.1); identity, scope-key, and
   persisted-schema semantics bind from M1.
10. Inter-entity exchanges are deduplicated outbox/inbox sagas re-driven by
    the initiator (Section 9.8); synchronous cross-entity transactions are
    forbidden, including between colocated entities.
11. Budgets are escrow allocations debited down-front in the parent's own
    creating transition; dispatch-time reservation is run-local only, and
    settlement/return flows upward through deduplicated commands.
12. The core model contract is the Rakka-owned adapter trait of Section 10.1;
    Rig is the default implementation behind the `rig` feature and never
    appears in the core public API or persisted state.

### 21.3 Open Decisions

These decisions remain open until accepted by maintainers and product owners.
The current recommended default is shown after each question. Decisions
confirmed or exercised during Phase 1 (M1) carry an inline *Disposition (M1)*
note naming the implementation-plan slice that recorded the resolution; items
without one remain open.

1. **Can an `AgentId` have concurrent active runs?** Recommended: yes, with
   independent run entities and idempotent/CAS private-memory writes.
   *Disposition (M1): accepted structurally — every run is an independent
   sharded entity with its own ledger and nothing serializes an agent's runs;
   the idempotent/CAS private-memory write rule binds at M2 (slice 2.1).*
   *Disposition (M2): accepted — a private-memory create is idempotent on its
   operation id (a replay answers the original result from the store's
   operation ledger), an update is a compare-and-set on an explicit expected
   revision, a stale writer is refused rather than overwriting, and two
   runs' promotions derive disjoint memory identities (slice 2.1).*
2. **What is the default communal boundary?** Recommended: tenant or
   organization `KnowledgeSpaceId`; no implicit cross-tenant global graph.
   *Disposition (M2): accepted structurally — every graph operation is
   addressed through `KnowledgeSpaceScope`, whose injective key includes the
   tenant, so a cross-tenant graph is unrepresentable rather than merely
   disallowed; federation would be an explicit later design (slice 2.3).*
3. **Does every agent-written claim begin as `Proposed`?** Recommended: yes;
   policy or HITL promotes consequential claims to `Verified`.
   *Disposition (M2): accepted — `Claim::new` takes no trust parameter, the
   `Proposed ⇔ zero-transitions` coherence invariant is re-validated on every
   load, the store's append door refuses anything else, and `Verified` is
   reachable only through the append-only transition path, gated for
   consequential claims by a slice 1.10 checkpoint grant bound to the exact
   claim content and history ordinal (slice 2.3).*
4. **Which model calls are safe to retry?** Recommended: an explicit
   deployment policy based on provider idempotency, cost, and replay tolerance.
   *Disposition (M1): accepted — the adapter declares an explicit
   `AgentModelRetryPolicy` (safety class plus bounded attempts, default
   read-only), deployment configures it through the model effect spec, and
   dispatch re-enforces it (slices 1.6 and 1.7).*
5. **May a human force retry an ambiguous non-idempotent effect?** Recommended:
   no generic retry; require `ConfirmedNotExecuted` evidence and a new effect
   generation. A future unsafe override would need a distinct capability and
   conspicuous audit semantics.
   *Disposition (M1): accepted — the reconciliation decision set ships with no
   generic retry and `ConfirmedNotExecuted` creates a new effect generation
   (slice 1.10); no unsafe override exists.*
6. **How are agent cards assigned?** Recommended: stable cards/skills identify
   an `AgentId` or agent template; every created task receives a distinct
   `AgentTaskId` and its initial assignee receives a distinct `AgentRunId`.
7. **How long is short-term memory retained after a terminal run?**
   Recommended: tenant policy with bounded default, legal hold, export, and
   deletion support.
   *Disposition (M1): deliberately still open — deferred to Phase 2
   slice 2.1; until then a deployment retains session rows until it deletes
   them itself (slice 1.11 amendment).*
   *Disposition (M2): accepted — a per-tenant `SessionRetentionPolicy`
   (30-day bounded default, legal hold) is deployment configuration passed
   to each purge call and enforced inside it; the idempotent terminal-run
   `purge_run` lands on both the session and snapshot stores, because
   snapshots embed session content; export is the ordinary bounded cursor
   read taken before the purge; sweeps are bounded, deployment-invoked
   calls, never a resident poller (slice 2.1).*
8. **Which communal graph backend ships first?** Recommended: do not select one
   in the domain specification. Capture representative queries and validate
   the database-agnostic SPI against at least two structurally different
   implementations or contract test doubles before choosing reference
   adapters.
   *Disposition (M2): accepted — no reference backend is named in the domain
   specification. The representative claim, traversal, tenancy, and
   bounded-query families are captured as the conformance clauses themselves,
   tabled in the `rakka-agent-knowledge-graph` conformance-module docs, and
   the SPI is validated by running that suite unchanged against two
   structurally different implementations: the in-memory reference store and
   the relational `rakka-agent-knowledge-graph-postgres` adapter (scenario
   20, with zero agent-domain change). Migration queries stay backend-owned —
   the portable SPI deliberately has no migration surface, so each backend
   crate owes its own idempotence and concurrent-migrator proofs
   (slice 2.4).*
9. **Can authorization be resolved by a service without a human?**
   Recommended: yes, if the resolver is authenticated, authorized, audited,
   and bound to the same exact effect intent.
   *Disposition (M1): accepted — a resolution enters through the same
   authenticated, deduplicated decision path as a human's, bound to the exact
   intent and argument digest (slice 1.10).*
10. **Should settings updates be ordinary A2A messages or a versioned A2A
    extension/management skill?** Recommended: define a versioned management
    skill or extension so schemas, authorization, and audit semantics are
    explicit.
    *Disposition (M1): accepted as a versioned extension, not a skill —
    `urn:rakka:a2a-extension:agent-management:v1` (slice 1.12).*
11. **Where should linked trace segments split?** Recommended: split at durable
    asynchronous boundaries and long waits; keep one bounded active turn or
    logical provider operation together when doing so remains operationally
    useful.
    *Disposition (M1): accepted — a segment is one entity activation, every
    durable asynchronous boundary splits segments, and a model call lives in
    the dispatcher's consumer segment (slice 1.13).*
12. **Can production telemetry capture model/tool/memory content?**
    Recommended: disabled by default; allow only explicit scoped opt-in to
    protected artifact storage, never credentials or hidden reasoning.
    *Disposition (M1): accepted as structurally off — no M1 code path emits
    content; the scoped opt-in policy object is deferred to Phase 6
    (slice 1.13).*
13. **Which trace sampling policy ships first?** Recommended: retain errors,
    indeterminate effects, security/policy events, escalations, recovery
    failures, and slow traces; sample routine success, using trace-ID-routed
    tail sampling only when the deployment can operate it safely.
    *Disposition (M1): accepted — sampling ships as pinned Collector
    configuration carrying the Section 17.16 retain list; the crate owes
    bounded attributes, recording-independent propagation, and the
    scenario-24 proof (slice 1.13).*
14. **Are goal, task, and run identities distinct?** Recommended: yes. Let the
    stable root task coordinate initially and optionally generate the same
    underlying value for the goal/root task/initial run, but keep
    `AgentGoalId`, `AgentTaskId`, and `AgentRunId` as separate types and
    contracts.
    *Disposition (M4): accepted as the resolved default —
    `AgentGoalId::for_root_task` derives the goal id from the root
    `AgentTaskId` value when a creation institutes a goal without an explicit
    binding; the types, validation, and semantics stay distinct; the root
    `AgentTaskEntity` coordinates the goal record inside its own
    compare-and-set, so a dedicated goal entity can later take over without
    changing the public contract (slice 4.1).*
15. **Who selects a specialist agent?** Recommended: the model/planner requests
    a skill, while an application-owned authorized catalog resolves the
    concrete `AgentId`, endpoint, compatible contract, and scopes.
    *Disposition (M4): accepted — model output can only name an
    `AgentCapabilityId` skill through the one declared coordination tool the
    loop intercepts (unknown fields fail the parse, so an agent id or
    endpoint in model output is refused rather than ignored); the
    application-wired `AgentDelegationCatalog` resolves the concrete agent,
    logical endpoint, task definition, scopes, and compatibility inside the
    same compare-and-set that persists the delegation record, replays reuse
    the recorded resolution verbatim, and the resolved selection travels as
    `io.rakka.agent.id`/`io.rakka.agent.task-definition` on the send
    (slice 4.3).*
16. **How does a compiled workflow appear as a tool?** Recommended: a versioned
    descriptor creates or adopts an independently durable child workflow run;
    never treat the whole workflow as one opaque retryable external effect.
17. **What maps to A2A `Task.id`?** Recommended: `AgentTaskId`; preserve it
    across handoff while each assignee execution receives a new `AgentRunId`.
    *Disposition (M1): accepted as the equal mapping — A2A `Task.id` is the
    `AgentTaskId` value verbatim and the tenant always derives from the
    authenticated context (slice 1.12).*
18. **Which coordination patterns become first-class?** Recommended: handoff,
    delegation, team, and moderation, each compiled into typed durable state
    with bounded policy rather than prompt-only conventions.
19. **How dynamic may run setup be?** Recommended: instructions and selected
    capabilities may vary within an authorized definition envelope; setup may
    never add undeclared tools/models/peers or weaken mandatory guardrails.
    *Disposition (M1): accepted — narrow-only envelope validation at creation
    (slice 1.2), enforced at dispatch against both the definition and the
    resolved setup (slice 1.8).*
20. **Should Rakka copy Akka's idle residency behavior?** Recommended: no.
    Every quiescent Rakka task/run auto-passivates; suspend controls admission
    and terminate controls lifecycle, not memory-resource release.
    *Disposition (M1): accepted — no idle residency; scenario 46 proves
    auto-passivation with no lifecycle command and one deduplicated trigger
    reactivating the owner (slice 1.14).*
21. **Are coordination notifications replayable?** Recommended: yes, with
    bounded retention, monotonic scoped cursor, and explicit resync on an
    expired window; authoritative task/run state remains the correctness source.

## 22. Initial Acceptance Statement

The first production-quality milestone (M1) is one sharded Rakka Agent that:

- is instantiated with versioned settings;
- accepts an A2A task mapped to one durable `AgentTaskId` and initial
  `AgentRunId`;
- validates one versioned typed result before completing the public task;
- persists a fail-closed autonomy-admission decision and rejects any widening
  setup/settings update not covered by it;
- reserves and settles bounded run/model/tool/effect budgets durably;
- remains logically addressable while fully passivated and uses no per-agent
  resident async task/thread/process while waiting;
- executes a bounded Rig model turn through a dispatcher;
- emits correlated A2A, decision, model, effect, tool, wait/resume, and recovery
  trace segments that assemble into one session view by `AgentRunId`;
- reports bounded GenAI/Rakka latency, token, decision, wait, recovery, and
  indeterminate metrics without high-cardinality IDs;
- persists short-term session context;
- dispatches each effectful tool call as a separate durable effect;
- pauses and passivates at an approval or authorization gate;
- recovers after owner and dispatcher pod loss;
- marks an ambiguous non-idempotent tool effect indeterminate without
  automatically invoking it again;
- resumes or terminates only after an authenticated, deduplicated reconciliation
  decision;
- exposes an authoritative lifecycle/wait/budget/effect/cancellation snapshot
  that remains correct without telemetry;
- captures no prompt, output, hidden reasoning, tool payload, memory content,
  or credential material in default telemetry; and
- remains correct when telemetry export is unavailable while exposing bounded
  exporter/Collector loss signals.

Private vector memory and the communal graph follow as the memory milestone
(M2), but their scopes and interfaces are defined now so the first
implementation does not bake in an incompatible conversation or storage
identity.

### Continuous Goal Milestone (M3)

The first continuous milestone is one stable `AgentGoalId`/root control task
that:

- remains logically active and fully passivatable without a resident agent
  actor, loop, timer task, connection, span, or pod;
- admits finite child `AgentTaskId`/`AgentRunId` epochs only from versioned,
  deduplicated durable `AgentWakeId` occurrences;
- defaults to forbidden overlap, durable trigger coalescing, and at most one
  coalesced occurrence after downtime;
- preserves per-epoch short-term-memory isolation while using authorized
  agent-private memory/artifacts for cross-epoch continuity;
- enforces per-epoch and rolling/window budget ledgers without reset on pod
  restart, activation, or shard movement;
- fences obsolete schedule revisions and survives duplicate timer scans/events
  without duplicate epochs or effects;
- supports suspension, renewal, failure backoff, expiry, and retirement; and
- exposes current schedule revision, next wake, last progress, active epoch,
  budget, missed/coalesced trigger, and retirement state through an
  authoritative operational query.

### Multi-Agent Goal Milestone (M4)

The first collaboration milestone is one durable `AgentGoalId` coordinated by
a stable root task and its current root run that:

- resolves and delegates bounded work to at least two specialized Rakka Agents
  through `rakka-a2a`, giving each child its own `AgentTaskId` and
  `AgentRunId`;
- persists fan-out, waits without a resident task, survives root and child pod
  loss, and deterministically fans results in;
- invokes at least one compiled workflow through a versioned workflow-tool
  descriptor and recovers the same child workflow run after replay;
- records attributable shared knowledge/artifact contributions without
  exposing private agent memory;
- reconstructs goal, delegation, session, workflow, evaluation, and evidence
  telemetry through an authorized goal view; and
- transitions to `Satisfied` only after a configured evaluator verifies the
  current criteria against durable evidence, while preserving indeterminate
  non-idempotent outcomes in any child.

### Coordination Capability Milestone (M5)

The Akka-informed coordination milestone additionally proves:

- handoff preserves one typed `AgentTaskId`, starts a target-agent run, fences
  the source run, and transfers only explicit task context/artifacts;
- team members atomically claim tasks from a durable bounded board and may all
  passivate while the team remains logically active;
- moderation recovers a bounded ordered turn protocol without duplicating a
  participant turn;
- a human-owned typed task unblocks dependencies, while exact effect approval
  still uses a bound checkpoint;
- per-run setup cannot widen the static definition/policy envelope;
- task/run/coordination progress can be replayed from a cursor or explicitly
  resynchronized; and
- deterministic Rig model/tool scripts plus fault injection cover every task,
  handoff, claim, turn, and effect boundary.
