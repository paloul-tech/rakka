# Rakka Agent Specification

Status: planning draft
Date: 2026-07-09
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

## 3. Goals

- Provide a stable, sharded agent identity independent of process or pod.
- Keep active agents logically addressable and recoverable without requiring a
  resident actor, async task, thread, process, connection, stream, or pod.
- Represent a durable goal independently from the agent runs that contribute
  to it, with versioned success criteria, evidence, progress, and terminal
  authority.
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
- Use Rig as the model/provider/tool abstraction without making Rig the
  durable correctness owner.
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

## 5. Ownership Boundaries

Rakka owns:

- durable goal, agent, typed task, run, delegation, and workflow-link identity;
- sharding, placement, passivation, and recovery;
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
- commands that create or administratively affect runs.

### 6.3 AgentGoalId

`AgentGoalId` identifies one top-level collaborative goal. It is distinct from
the agent identities and run sessions that contribute to the goal. A goal MAY
span multiple specialized agents, concurrent `AgentRunId` values, A2A tasks,
workflow runs, waits, recoveries, and trace segments.

The initial implementation SHOULD let the stable root `AgentTaskEntity`
coordinate the goal, while the current root `AgentRunEntity` proposes decisions
against it. Its generated `AgentGoalId` MAY default to the root `AgentTaskId`
value, but the types and semantics MUST remain distinct so coordination can
later move to a dedicated entity without changing the public contract.

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

One run MUST work on at most one `AgentTaskId` at a time. Parallel work uses
multiple independently sharded runs.

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

### 6.9 Stable Operation IDs

Commands, task/dependency/assignment/handoff/claim/turn transitions, effects,
memory writes, checkpoint resolutions, A2A sends, and graph claim appends MUST
carry stable operation or deduplication IDs. Replaying the same accepted
operation MUST NOT produce a second state transition or logical write.

### 6.10 Logical Availability and Runtime Residency

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
- loop, token, cost, time, and concurrency budgets;
- goal-evaluator and progress/stagnation policy references;
- memory and retrieval policy;
- approval, authorization, and escalation policy references;
- logical credential binding references;
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

## 8. Goal-Driven Collaboration

### 8.1 Goal Contract and Lifecycle

Every goal MUST have a durable, versioned `AgentGoalSpec` containing at least:

- `TenantId`, `AgentGoalId`, owner/principal, root `AgentTaskId`, and root
  `AgentRunId`;
- objective and immutable or versioned success criteria;
- finite or continuous goal mode;
- constraints, priority, deadline, and cancellation policy;
- token, cost, elapsed-time, descendant, fan-out, depth, and concurrency
  budgets;
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
The logical goal MUST remain durably addressable through dispatcher restart,
pod loss, passivation, and shard movement until an authorized terminal
transition occurs. This durability does not authorize unbounded compute: a
budget or progress limit MUST park, escalate, or terminate according to policy.

A finite goal terminates after its current versioned criteria are evaluated. A
continuous goal, if enabled, MUST execute as bounded durable epochs with an
explicit health condition, renewal/budget policy, and retirement path; it MUST
NOT be implemented as an immortal polling future.

### 8.2 Progress, Evidence, and Completion

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

### 8.3 Specialization and Durable Delegation

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

### 8.4 Shared Environment and Collective Memory

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

### 8.5 Workflows as Tools

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

### 8.6 Cancellation, Failure, and Waiting

Goal cancellation, deadline, and immediate capability revocation MUST be
propagated durably to active child runs and workflows. Propagation is a request
with an observable outcome; it is not proof that an already-started external
effect was cancelled.

A root coordinator MAY wait for all children, a quorum, a policy-selected
subset, or an early satisfying result. The fan-in rule MUST be fixed in durable
state before results are accepted. Failed, timed-out, cancelled, or
indeterminate children MUST be handled explicitly by policy. While waiting for
agents, workflows, humans, timers, or reconciliation, the coordinator SHOULD
passivate and MUST NOT hold a live thread, future, or trace span.

### 8.7 Coordination Capability Model

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

### 8.8 Handoff

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

### 8.9 Team Coordination

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

### 8.10 Moderation

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

### 8.11 Human-Owned Tasks

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
- per-task iteration, token, cost, elapsed-time, external-effect, and
  coordination budgets;
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
for one run. Waiting and `Suspended` states are interrupted/non-executing
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
`AgentTaskEntity`. It MUST NOT make the public task terminal by mutating only
run state. The task becomes `Completed` only after schema/result-rule
validation; root goal satisfaction remains a separate evaluation.

## 10. Rig Integration

### 10.1 Adapter Boundary

`rakka-agent` MUST expose a Rig adapter behind Rakka-owned domain types. The
adapter converts an immutable context snapshot and settings revision into a Rig
request and converts the Rig response into a bounded Rakka result/artifact.

Rakka MUST NOT treat provider clients, streams, open HTTP requests, or
credential values as durable state.

### 10.2 Persistence Compatibility

Raw Rig `AgentRun` serialization MUST NOT be the sole durable compatibility
format. Rakka MUST persist its own versioned loop representation and SHOULD pin
the Rig dependency to a reviewed version for each compatibility release.

Rig upgrades that change request, tool-call, message, or serialized run
semantics MUST receive an adapter compatibility review and, when required, an
explicit migration.

### 10.3 Conversation Memory

Rig memory policies MAY be used to select, compact, summarize, or demote
history. Rakka's scoped memory stores and stable `MemoryOperationId` values
remain authoritative. Automatic memory callbacks MUST NOT bypass the durable
effect and deduplication boundary.

### 10.4 Deterministic Rig Test Adapter

`rakka-agent` SHOULD provide a deterministic test adapter that can script Rig
model text/results, structured task-result proposals, tool/delegation requests,
and responses conditional on prior messages or tool results. It SHOULD compose
with fake tools, peers, humans/authorization services, clocks, and memory
adapters.

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
specified in Section 8.5.

Remote MCP MAY be supported as an optional adapter for ordinary tools. It MUST
NOT be used as an indirect peer-agent channel that bypasses the typed
coordination runtime and `rakka-a2a`. Resolved endpoint credentials and tool
responses remain subject to secret exclusion, content policy, and effect
safety.

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
| `WaitingForApproval` | `INPUT_REQUIRED` |
| `WaitingForAuthorization` | `AUTH_REQUIRED` |
| `WaitingForReconciliation` | `INPUT_REQUIRED` with stable indeterminate reason |
| Task `Completed` | `COMPLETED` |
| Task `Failed` | `FAILED` |
| Task `Cancelled` | `CANCELED` |

Run `HandedOff` or `Superseded` MUST NOT make the public task terminal while a
successor run owns it. The projection follows authoritative task state and may
include the current run condition as metadata.

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

## 16. Security and Authorization

- Every request MUST be authenticated and tenant-authorized before data access.
- Policy checks MUST occur before queries that could reveal resource
  existence across a tenant boundary.
- Tool capabilities MUST be declared outside model output and enforced before
  effect scheduling and dispatch.
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
| Active turn/invocation | Bounded `invoke_agent {agent.name}` `INTERNAL` span |
| General decision | `rakka.agent.decide` `INTERNAL` span or event |
| Explicit planning | `plan {agent.name}` `INTERNAL` span only when planning is reliably distinguishable |
| Rig inference | `{gen_ai.operation.name} {gen_ai.request.model}` `CLIENT` span, or `INTERNAL` for a same-process model |
| Embeddings | `embeddings {model}` span |
| Retrieval | `retrieval {data_source}` span |
| Memory operation | Standard create/search/update/upsert/delete memory span |
| Effect schedule | `rakka.agent.effect.schedule` `PRODUCER` span ending after durable acceptance |
| Effect dispatch | `rakka.agent.effect.dispatch` `CONSUMER` span linked to schedule/prior attempts |
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

### 17.8 Model and Rig Observability

Each Rig model/provider operation MUST be observable as a GenAI model span.
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
- goal proposal, activation, criteria revision, progress evaluation, wait,
  satisfaction, failure, cancellation, and expiry;
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
- authorization grant reference and dispatch-time policy outcome; and
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

### 17.18 Session, Task, and Goal Observability Queries

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
    eligible.

Fault injection SHOULD kill the dispatcher or owner pod at every durable
effect boundary, including after a test external system commits but before it
returns the receipt.

## 19. Crate and Feature Shape

The intended workspace shape is:

- `rakka-agent`: goal/typed-task/run/evaluation/handoff/delegation/team/
  moderation/workflow-tool domain, typed client, loop runtime, Rig adapter,
  guardrails, gates, effect policy, session/private memory traits, structured
  decision/runtime telemetry, and deterministic test support;
- `rakka-agent-postgres`: PostgreSQL session/private memory and `pgvector`
  retrieval adapter;
- `rakka-agent-knowledge-graph`: optional database-agnostic communal graph
  domain, adapter SPI, portable query/capability model, and test support;
- `rakka-a2a` `agents` feature: agent/goal/task/run routing, versioned
  coordination metadata, task projection/event replay, and authenticated
  command/gate mapping; and
- top-level `rakka` feature gates and curated prelude exports after API review.

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
  delegation/team/moderation, workflow-link, loop, effect, checkpoint, memory,
  and claim records MUST carry schema versions where evolution is expected.
- New statuses and fields SHOULD be additive and use stable kebab-case labels
  and stable error codes.
- Existing `WaitingForHuman` data requires an explicit compatibility mapping if
  replaced by typed waiting states.
- A Rig dependency upgrade requires adapter and serialized-artifact review.
- An OpenTelemetry GenAI convention upgrade requires an adapter compatibility
  review but MUST NOT by itself require durable agent-state migration.
- N/N+1 nodes MUST agree on protocol/status/schema compatibility before sharing
  a cluster during rolling updates.
- Unsupported state versions MUST fail closed rather than executing with
  guessed semantics.

## 21. Open Decisions

These decisions remain open until accepted by maintainers and product owners.
The current recommended default is shown after each question.

1. **Can an `AgentId` have concurrent active runs?** Recommended: yes, with
   independent run entities and idempotent/CAS private-memory writes.
2. **What is the default communal boundary?** Recommended: tenant or
   organization `KnowledgeSpaceId`; no implicit cross-tenant global graph.
3. **Does every agent-written claim begin as `Proposed`?** Recommended: yes;
   policy or HITL promotes consequential claims to `Verified`.
4. **Which model calls are safe to retry?** Recommended: an explicit
   deployment policy based on provider idempotency, cost, and replay tolerance.
5. **May a human force retry an ambiguous non-idempotent effect?** Recommended:
   no generic retry; require `ConfirmedNotExecuted` evidence and a new effect
   generation. A future unsafe override would need a distinct capability and
   conspicuous audit semantics.
6. **How are agent cards assigned?** Recommended: stable cards/skills identify
   an `AgentId` or agent template; every created task receives a distinct
   `AgentTaskId` and its initial assignee receives a distinct `AgentRunId`.
7. **How long is short-term memory retained after a terminal run?**
   Recommended: tenant policy with bounded default, legal hold, export, and
   deletion support.
8. **Which communal graph backend ships first?** Recommended: do not select one
   in the domain specification. Capture representative queries and validate
   the database-agnostic SPI against at least two structurally different
   implementations or contract test doubles before choosing reference
   adapters.
9. **Can authorization be resolved by a service without a human?**
   Recommended: yes, if the resolver is authenticated, authorized, audited,
   and bound to the same exact effect intent.
10. **Should settings updates be ordinary A2A messages or a versioned A2A
    extension/management skill?** Recommended: define a versioned management
    skill or extension so schemas, authorization, and audit semantics are
    explicit.
11. **Where should linked trace segments split?** Recommended: split at durable
    asynchronous boundaries and long waits; keep one bounded active turn or
    logical provider operation together when doing so remains operationally
    useful.
12. **Can production telemetry capture model/tool/memory content?**
    Recommended: disabled by default; allow only explicit scoped opt-in to
    protected artifact storage, never credentials or hidden reasoning.
13. **Which trace sampling policy ships first?** Recommended: retain errors,
    indeterminate effects, security/policy events, escalations, recovery
    failures, and slow traces; sample routine success, using trace-ID-routed
    tail sampling only when the deployment can operate it safely.
14. **Are goal, task, and run identities distinct?** Recommended: yes. Let the
    stable root task coordinate initially and optionally generate the same
    underlying value for the goal/root task/initial run, but keep
    `AgentGoalId`, `AgentTaskId`, and `AgentRunId` as separate types and
    contracts.
15. **Which goal modes ship first?** Recommended: finite, evidence-verifiable
    goals first; add continuous goals only as bounded durable epochs with
    explicit suspension and retirement semantics.
16. **Who selects a specialist agent?** Recommended: the model/planner requests
    a skill, while an application-owned authorized catalog resolves the
    concrete `AgentId`, endpoint, compatible contract, and scopes.
17. **How does a compiled workflow appear as a tool?** Recommended: a versioned
    descriptor creates or adopts an independently durable child workflow run;
    never treat the whole workflow as one opaque retryable external effect.
18. **What maps to A2A `Task.id`?** Recommended: `AgentTaskId`; preserve it
    across handoff while each assignee execution receives a new `AgentRunId`.
19. **Which coordination patterns become first-class?** Recommended: handoff,
    delegation, team, and moderation, each compiled into typed durable state
    with bounded policy rather than prompt-only conventions.
20. **How dynamic may run setup be?** Recommended: instructions and selected
    capabilities may vary within an authorized definition envelope; setup may
    never add undeclared tools/models/peers or weaken mandatory guardrails.
21. **Should Rakka copy Akka's idle residency behavior?** Recommended: no.
    Every quiescent Rakka task/run auto-passivates; suspend controls admission
    and terminate controls lifecycle, not memory-resource release.
22. **Are coordination notifications replayable?** Recommended: yes, with
    bounded retention, monotonic scoped cursor, and explicit resync on an
    expired window; authoritative task/run state remains the correctness source.

## 22. Initial Acceptance Statement

The first production-quality milestone is one sharded Rakka Agent that:

- is instantiated with versioned settings;
- accepts an A2A task mapped to one durable `AgentTaskId` and initial
  `AgentRunId`;
- validates one versioned typed result before completing the public task;
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
- captures no prompt, output, hidden reasoning, tool payload, memory content,
  or credential material in default telemetry; and
- remains correct when telemetry export is unavailable while exposing bounded
  exporter/Collector loss signals.

Private vector memory and the communal graph may follow as additive milestones,
but their scopes and interfaces are defined now so the first implementation
does not bake in an incompatible conversation or storage identity.

### Multi-Agent Goal Milestone

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

### Coordination Capability Milestone

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
