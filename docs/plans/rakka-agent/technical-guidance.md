# Rakka Agents Technical Guidance

Status: design guidance
Date: 2026-07-10
Research basis: [background-research.md](background-research.md)
Emerging specification: [spec.md](spec.md)

## Purpose

This document turns the background research into opinionated architecture and
technical-discovery guidance. It is intentionally non-normative. Requirements
that become agreed behavior should move into `spec.md`; implementation slices
should later move into a dedicated implementation plan.

## Recommended Direction

Build a new `rakka-agent` domain crate on top of
`rakka-agent-workflow`. Use Rig for model/provider/tool abstraction, while
Rakka owns identity, loop state, durable transitions, effect safety, memory
scope, recovery, passivation, and A2A projection.

Do not run a complete autonomous loop inside one actor handler or one opaque
dispatcher effect. Persist after every decision boundary and external
operation.

```text
A2A ingress
    |
    v
AgentEntity (TenantId + AgentId)
    | settings, lifecycle, private-memory namespace
    v
AgentTaskEntity (TenantId + AgentTaskId)
    | typed public work, dependencies, assignment, result
    v
AgentRunEntity (TenantId + AgentId + AgentRunId)
    | durable loop transition
    v
workflow outbox effect
    |
    v
dispatcher -> Rig / tool / A2A peer / memory adapter
    |
    v
workflow inbox result -> recovered AgentRunEntity
```

The sharded entity should serialize decisions, not occupy a thread while a
model, tool, human, timer, or authorization service is pending.

## Always-On Contract

Treat this as a non-negotiable design invariant:

> **“Always-on” means logically addressable and recoverable, not a resident
> thread or process.**

`Active`, `Running`, and `Waiting` are durable domain states. They are not
deployment or residency states. An agent can be logically active and fully
addressable while no actor instance for it exists on any pod.

When a run has persisted its next wait/effect and has no bounded transition to
perform, it should become quiescent and passivatable. Quiescence should require
no per-agent:

- actor instance or mailbox;
- Tokio task, future, thread, or child process;
- polling loop or in-memory timer;
- network connection or stream;
- dispatcher lease or worker reservation; or
- open OpenTelemetry span.

Durable rows and shared fleet infrastructure do not violate this invariant. A
timer row, inbox item, outbox effect, checkpoint, or shard routing record is
data describing future work; it is not a sleeping execution resource.

The reactivation contract should be explicit:

1. A2A ingress, a durable timer, an inbox/result item, a child-agent/workflow
   result, a settings/cancellation command, or recovery scanning identifies the
   stable logical entity.
2. Sharding routes to the current owner and activates the entity if necessary.
3. The entity loads authoritative state and re-establishes revision/ownership
   fencing.
4. It performs only bounded transitions, persists the next work or wait, and
   becomes passivatable again.

Never use actor residency, a pod-local scheduler, an open stream, or a held
dispatcher lease as the mechanism that guarantees a future wake-up. The wake
source must be durable or safely replayable.

Measure logical service health separately from physical activity:

- logical active/waiting goal and run counts;
- durable trigger/timer/outbox backlog and oldest age;
- cold activation and state-recovery latency;
- activation/passivation churn and recovery failures; and
- currently resident entities and runtime saturation.

This makes the architecture scale with active computation rather than the
number or lifetime of logical agents. Continuous goals follow the same rule:
one bounded epoch, one persisted next wake condition, then quiescence.

## Continuous Goals and Operational Safety

The full "Autonomous Agentic Systems at Scale" article provides strong
supporting evidence for the existing architecture, but Rakka deliberately
rejects its daemon-like continuous mode. Pod lifetime must never define agent
lifetime, schedule, progress, or recovery.

### Continuous Goal Controller

Model continuous operation as a durable controller over finite work:

```text
stable AgentGoalId and root control task
    -> durable AgentWakeId
    -> finite child AgentTaskId
    -> finite AgentRunId
    -> result/evidence returned to the controller
    -> next wake, suspension, or retirement
```

Prefer a distinct child task and run for every admitted epoch. This bounds
short-term memory, makes task/effect history inspectable, gives each epoch a
clean budget and setup revision, and creates a natural compatibility boundary
for upgrades. Cross-epoch continuity belongs in agent-private long-term memory
and explicit controller state, not one unbounded run transcript.

Use a versioned `AgentWakePolicy` containing:

- timer, external-event, A2A-command, callback, or hybrid triggers;
- schedule revision and stable wake-ID construction;
- maximum lateness and admission window;
- overlap and trigger-coalescing policy;
- missed-occurrence policy after downtime;
- per-epoch budget and deadline;
- failure backoff; and
- suspension, renewal, and retirement rules.

Recommended defaults are:

- forbid overlapping epochs;
- durably coalesce triggers received while an epoch is active;
- admit one coalesced occurrence after downtime;
- never perform unbounded catch-up;
- fence wakes created by an obsolete schedule revision; and
- require explicit policy for bounded parallel or replayed epochs.

Build this controller on `rakka-agent-workflow` one-shot durable timers and
deduplicated trigger injection. Do not introduce a recurring actor timer,
sleeping future, Kubernetes `CronJob` correctness dependency, or pod-start
hook. A pod start can make shared scanners available, but it cannot create an
agent-domain occurrence unless a durable wake is due or a replayable event was
accepted.

### Hierarchical Budget Ledger

Evolve the current autonomy counters into a durable allocation and reservation
hierarchy:

```text
definition ceiling
    -> goal allocation
        -> task/epoch allocation
            -> run allocation
                -> turn/effect reservation and settlement
```

Track, as applicable:

- autonomous iterations and model calls;
- input/output/total tokens and provider-reported cost;
- tool calls, external effect starts, and attempts;
- active-execution time, elapsed deadline, and per-attempt timeout;
- concurrent effects, children, depth, fan-out, and descendants; and
- bounded artifact/output size.

Reserve before dispatch and settle from the durable accepted result. Count an
effect once it reaches `Started`, including an attempt that later becomes
indeterminate. Idempotency changes safe retry behavior; it does not make an
attempt free. Allocate child budgets atomically and return unused allocation
only after a known terminal child outcome.

Distinguish hard ceilings from soft thresholds. A threshold may warn or
request authorization; a ceiling must deterministically reject, park,
suspend, escalate, fail, or retire according to persisted policy. Represent
budget exhaustion as a structured stop/wait reason rather than multiplying
top-level task states.

For continuous goals, combine a per-epoch allocation with a rolling or
calendar-window goal ceiling. Window refill is a durable policy transition. It
must not be inferred from process uptime or reset on activation, pod restart,
or shard movement.

### Autonomy Admission

Add a fail-closed admission check for `Interactive`, `BoundedAsync`, and
`Continuous` operation classes. The class describes operating behavior, not an
industry risk taxonomy.

Run admission when a definition is published, an agent/run is instantiated,
or an update widens tools, peers, credentials, environment access, schedule,
or budgets. Recheck immediate revocation at dispatch even when the previously
recorded admission remains valid.

Do not admit unattended execution without:

- measurable completion, health, or progress criteria;
- bounded cost, time, calls, effects, and collaboration;
- cancellation, suspension, escalation, and recovery behavior;
- inspectable state and progress;
- classified tools/effects and scoped authority;
- approval/authorization policy for consequential operations; and
- indeterminate-effect reconciliation policy.

Persist an `AutonomyAdmissionDecision` with definition/setup/policy revisions,
reason codes, evaluating principal/service, constraints, and expiry. Rakka
owns the contract and enforcement points; the application owns policy
authoring and industry-specific risk classification.

### Tool Visibility, Authority, and Executor Isolation

Keep four layers explicit:

| Layer | Purpose |
| --- | --- |
| `ToolDescriptor` | Bounded schema/description visible to Rig and the model |
| `ToolBinding` | Definition-authorized target, safety class, capability, and credential class |
| `EffectIntent` | Exact requested invocation, target, arguments digest, and revisions |
| `DispatchGrant` | Current authorization to execute that exact intent |

Model selection of a descriptor is only a request. Before `Started`, bind the
dispatch grant to tenant/goal/task/agent/run/effect, descriptor version/schema
digest, target and argument digest, safety class, capabilities, policy/setup
revisions, credential binding, checkpoint grant, expiration, and allowed use
count.

Add an application-owned `ExecutionPolicyRef` to describe the required trust
domain, workload identity, network-egress class, sandbox/process class, secret
resolution class, and tenant-isolation class. Rakka persists and routes by the
reference; it does not claim to implement Kubernetes RBAC, a service mesh, an
identity provider, or an OS sandbox.

Avoid a single broadly privileged dispatcher pool. Route effects by trust tier
and support ephemeral effect sandboxes or dedicated workers for consequential
tools. This is bounded executor placement, not a permanently resident pod per
agent. Freeze or digest dynamic MCP descriptor/endpoint metadata in the effect
intent so catalog drift cannot silently change a recovered invocation.

### Bounded Task State and History

Separate:

1. bounded materialized state needed for the next legal transition;
2. append-only domain/audit history;
3. messages, observations, artifacts, tool content, and memory; and
4. list/search/observability projections.

An `AgentTaskEntity` can own assignment, result-validation, and lifecycle
semantics without embedding every historical assignment, message, proposal,
or tool result in its active state. Keep current IDs/status/revisions,
dependency summary, assignment, pending references, accepted result, and
terminal reason inline; keep unbounded content and old history behind bounded
artifact/event references and cursors.

Define limits for dependencies, children, handoffs, result rejections,
pending effects/checkpoints, inline metadata, replay windows, and query page
sizes. Snapshot/compact materialized state without turning memory into the
lifecycle source. Public A2A task history remains bounded and authorized.

### Authoritative Operational Queries

Provide authoritative point queries from durable state and separate derived
observability/search views. An operator must be able to inspect an agent when
it is passivated and when telemetry is sampled, delayed, or unavailable.

An `AgentOperationalSnapshot` should include:

- logical lifecycle separately from current/last residency;
- state revision, current task/run/phase, and last meaningful progress;
- current wait reason and next durable wake;
- budget allocation, reservation, consumption, and exhaustion reason;
- pending effects, attempts, safety classes, and indeterminate work;
- checkpoint/authorization state;
- cancellation propagation state;
- last recovery, passivation, shard owner, and dispatcher transition; and
- event cursor plus projection freshness/lag.

Use a cancellation progress model such as `NotRequested`, `Requested`,
`Propagating`, `Quiesced`, `WaitingForReconciliation`, and `Completed`.
Cancellation fences new work immediately but does not prove a started external
effect stopped. If a non-idempotent effect is indeterminate, keep the task
nonterminal in reconciliation with cancellation requested; project terminal
`Cancelled` only after internal work is quiesced and consequential effect
outcomes are known.

Authoritative point reads should expose a state revision. List/search queries
may be eventually consistent but should report projection revision or lag.
OpenTelemetry and runtime-event projections remain essential for investigation
and aggregation, but neither becomes an alternate execution state machine.

## Identity and Ownership

Use explicit identity and ownership scopes:

| Concern | Scope | Logical owner |
| --- | --- | --- |
| Agent definition and settings | `(TenantId, AgentId)` | `AgentEntity` |
| Collaborative root goal | `(TenantId, AgentGoalId)` | Root `AgentTaskEntity` initially |
| Typed public task | `(TenantId, AgentTaskId)` | `AgentTaskEntity` |
| One autonomous session/run | `(TenantId, AgentId, AgentRunId)` | `AgentRunEntity` |
| Short-term memory | `(TenantId, AgentId, AgentRunId)` | Run session store |
| Agent-private long-term memory | `(TenantId, AgentId)` | Private memory store |
| Communal graph | `(TenantId, KnowledgeSpaceId)` | Knowledge graph store |

`AgentId` should remain stable across activations, pods, shard owners, and
runs. `AgentGoalId` should identify one root objective across all collaborating
agents. `AgentTaskId` should identify one typed unit of work/result and map to
A2A `Task.id`. `AgentRunId` should identify one agent's independently
recoverable execution session for that task.

Avoid making one agent entity execute all of its runs serially. Let the stable
agent entity own lifecycle and settings while independently sharded run
entities own active loops. Private-memory writes still need compare-and-set or
idempotent append because concurrent runs may learn simultaneously.

## Akka-Informed Contract Surface

Akka's Autonomous Agent API is the clearest high-level reference for the
developer experience Rakka should provide. Adopt its useful contract shapes,
but compile them into Rakka's existing durable primitives rather than creating
a parallel runtime.

### Definition and Setup Revisions

Use two related layers:

- `AgentDefinitionRevision`: outcome-oriented description, accepted task
  definitions, tool/workflow/MCP classes, approved model profiles, mandatory
  guardrails, coordination capability envelope, memory policy, and hard
  budgets; and
- `AgentSetupRevision`: per-run instructions, selected task capabilities,
  narrower budgets, permitted collaborators/knowledge spaces, and other
  authorized instance configuration.

The definition description should be the single bounded source for A2A Agent
Card discovery, specialist selection, model-facing purpose, documentation, and
observability class. User-provided display text must not become a metric label.

Setup may parameterize or narrow the definition. It must not add an undeclared
tool, widen authorization, weaken mandatory guardrails, choose an unapproved
model, or make a non-idempotent effect safer. Persist the effective definition,
setup, settings, and policy revisions on every task/run/effect boundary.

### Typed Task Contract

Introduce a versioned `AgentTaskDefinition` with:

- stable definition ID/version and outcome-oriented description;
- input and result schema references;
- deterministic result-validation rules;
- allowed assignee agent classes/skills and human ownership policy;
- dependency/failure-propagation policy;
- attachment/artifact policy;
- per-task iteration/token/cost/time/effect budgets; and
- retention, audit, classification, and evidence requirements.

`AgentTaskEntity` should own the public work lifecycle independently from any
one execution:

```text
AgentTaskId
    -> definition + input/artifacts + dependencies
    -> current assignment / AgentRunId
    -> result proposal -> validation -> accepted or rejected
    -> terminal typed result / failure / cancellation
```

One `AgentRunEntity` works on one task at a time. Parallelism comes from
multiple run entities, not concurrent mutation inside one run. A result-rule
rejection is a persisted bounded event: it may return sanitized feedback to the
loop, increment a rejection/struggle counter, and consume another iteration. It
does not silently accept the model's proposed result.

Attachments should be immutable artifact references with digest, media type,
size, classification, and authorization metadata. Content loading is a bounded
effect/context-building step; loaders receive logical credential bindings, not
persisted secrets.

### Task, Run, Delegation, and Handoff Identity

Use the following identity rule:

```text
Goal
  +-- parent AgentTaskId
  |     +-- source AgentRunId
  |     +-- handoff AgentRunId (same task)
  |
  +-- delegated child AgentTaskId
        +-- child AgentRunId
```

- Handoff preserves `AgentTaskId`, creates a new target-agent `AgentRunId`, and
  marks the source run handed off/superseded.
- Delegation creates a new child `AgentTaskId` and `AgentRunId`, while the
  parent task/run waits for fan-in.
- Reassignment uses a new run generation and must reconcile ambiguous prior
  effects before permitting duplicate non-idempotent work.
- Short-term memory remains isolated to `(TenantId, AgentId, AgentRunId)`;
  handoff transfers only explicit task artifacts/context, never private memory.

### Coordination Capabilities

Make four capability descriptors first-class:

| Capability | Durable runtime shape |
| --- | --- |
| Handoff | Same task identity, fenced owner/run transfer, explicit context/artifact projection |
| Delegation | Child task/run creation, bounded fan-out, result fan-in, parent retention |
| Team | `AgentTeamId`, bounded membership, durable shared task board, atomic claim/release/transfer, mediated peer messages |
| Moderation | `AgentConversationId`, participant set, durable turn schedule/transcript, bounded rounds and iterations per turn |

The runtime may expose these capabilities to Rig as tools, but their execution
is typed durable state—not an arbitrary model-authored HTTP/MCP call. Direct
agent-to-agent work still uses `rakka-a2a`; a model cannot bypass lineage,
budgets, authorization, or audit by calling a peer endpoint as a generic tool.

Choose the supervisor deliberately:

| Decision ownership | Mechanism |
| --- | --- |
| Fixed order, explicit retry/compensation | Compiled Rakka workflow |
| Model chooses next specialist/pattern | Rakka Agent coordination capability |
| Fixed outer process with model-driven stage | Workflow invoking a durable agent task/run |

### Human Tasks Versus Effect Gates

Support unassigned or human-assigned typed tasks for durable human work products
and dependency graphs. Keep them distinct from effect-bound checkpoints:

- use an `AgentTask` when the human produces a typed result that is itself part
  of the plan; and
- use an `AgentCheckpoint` when approval, authorization, or reconciliation must
  bind to an exact effect intent and its argument digest.

### Guardrail Chain

Add ordered, versioned guardrail stages for model request/response, tool
request/response, retrieval/memory ingress, and A2A ingress/egress. Each stage
declares `block`, `transform`, `report-only`, or `require-checkpoint` behavior,
stable reason codes, and protected evidence references.

Guardrails do not replace capability authorization, credential resolution,
effect safety, result validation, or goal evaluation. Deployment policy may
add mandatory stages that an agent definition/setup cannot remove.

### Client, Events, and Testkit

Provide a typed `RakkaAgentClient` facade for setup, task create/assign/query,
typed result retrieval, suspend/resume/cancel/terminate, and event subscription.
External and peer calls must still be implemented through `rakka-a2a` and
durable Rakka commands; the client must not become a direct actor shortcut.

Unlike Akka's live non-replayable notifications, Rakka should expose ordered,
replayable task/run/coordination events with bounded retention, cursors, and an
explicit resync response. Add derived struggle signals for approaching budgets,
repeated iteration failure, repeated result rejection, stuck dependencies, and
stalled team work, but keep them as observability projections.

The testkit should script Rig model responses and tool-call requests, provide
fake tools/peers/humans, assert typed results and durable events, and inject
crashes at every model/tool/delegation/handoff/task-claim boundary.

### Deliberate Passivation Difference

Do not copy Akka's explicit-management behavior where an idle autonomous agent
can retain in-memory state after its queue drains until termination/suspension.
Rakka should auto-passivate every quiescent task/run while remaining logically
addressable. `Suspend` is a durable admission state; `Terminate` is a durable
lifecycle transition; neither is required merely to free an actor.

## Goal-Driven, Multi-Agent Collaboration Guidance

### Define the Goal Before Starting the Loop

A Rakka Agent should be driven by a durable `AgentGoalSpec`, not merely by the
latest chat message. A useful contract includes:

- stable `AgentGoalId`, owner/tenant, and goal revision;
- finite or continuous goal mode;
- objective artifact reference and bounded summary;
- machine-testable and/or policy-evaluated success criteria;
- constraints, prohibited outcomes, and environment references;
- priority, deadline, and step/call/token/cost/delegation budgets;
- permitted agent skills, tools, workflows, knowledge spaces, and credential
  bindings;
- completion evaluator and evidence requirements;
- cancellation, escalation, and budget-exhaustion policy; and
- retention, audit, and observability policy.

The stable root `AgentTaskEntity` can own goal coordination initially, while
the current finite root or continuous-epoch `AgentRunEntity` proposes
decisions. Give the goal a separate type even if its initial ID defaults to the
root `AgentTaskId`; this keeps child tasks/runs and future handoff from
collapsing goal identity into one execution.

### Goal Lifecycle

Use an explicit goal lifecycle distinct from one agent run's technical status:

```text
Proposed -> Active -> Waiting -> Active
                 |         |
                 |         +-> Satisfied
                 +------------> Failed / Unsatisfied / Cancelled / Expired
```

`Satisfied` requires an accepted evaluation against the current goal revision
and evidence. A child run may complete successfully without satisfying the root
goal; its output is evidence or a completed subgoal for the coordinator.

For a finite goal, the coordinator continues through bounded turns,
delegations, workflow tools, timers, and gates until it is satisfied or reaches
another explicit terminal outcome. For a continuous goal, use bounded
evaluation epochs admitted by durable wakes. Prefer one finite child
`AgentTaskId` and `AgentRunId` per epoch while the stable goal/root control task
remains logically active until cancelled, expired, retired, or terminated by
policy. Neither the controller nor a future epoch depends on a resident loop,
pod start, or process lifetime.

### Verify Progress and Completion

Do not let the same unconstrained model both perform work and unilaterally
declare success for consequential goals. Completion evaluation may combine:

- deterministic assertions over artifacts/state;
- authoritative environment/tool queries;
- a compiled verification workflow;
- a separately configured evaluator model/agent;
- policy validation; and
- human approval.

Persist evaluation revision, outcome, evidence references, evaluator identity,
and stable reason code. If the goal changes, invalidate evaluations against the
old revision.

Track progress signals such as new evidence, completed subgoals, changed
environment observations, reduced unresolved requirements, and repeated
action fingerprints. Detect stagnation through bounded repeated-decision/tool
patterns and no-progress epochs. Exceeding a loop/delegation/budget limit should
wait for authorization, escalate, or fail closed rather than silently reset the
counter.

### Specialization and Discovery

Treat an agent as a durable specialized service. Its definition and A2A Agent
Card/skills should describe:

- bounded specialty/role and supported task schemas;
- allowed tools and workflow tools;
- required capabilities/credential classes;
- accepted environment and knowledge-space classes;
- concurrency and cost limits;
- input/output artifact schemas; and
- supported goal/evaluation modes.

Agent selection and team formation remain application policy. The model may
recommend a skill, but an authorized catalog/router resolves it to an allowed
`AgentId` and current Agent Card. Avoid exposing internal receptionist or actor
paths as the public discovery protocol.

### Durable Delegation Graph

Every child-agent assignment should be a durable A2A delegation:

```text
root AgentGoalId
    |
    +-- root AgentTaskId -> root AgentRunId
           |
           +-- AgentDelegationId A -> child AgentTaskId A -> specialist AgentRunId A
           |
           +-- AgentDelegationId B -> child AgentTaskId B -> specialist AgentRunId B
           |
           +-- durable fan-in -> goal evaluation
```

Persist before sending:

- root goal, parent task/run, delegation, and child task/run identities;
- target `AgentId` or resolved skill;
- bounded subgoal and success criteria/artifact references;
- lineage/depth and fan-out group;
- delegated capabilities, knowledge spaces, and environment references;
- budget/deadline/cancellation policy;
- input/output schema and result correlation; and
- idempotent A2A message/task key and trace context.

The parent must be able to wait and passivate. Child updates/results return
through A2A and the durable inbox. Duplicate send/result messages must not
create a second child task/run or complete a subgoal twice.

Support parallel delegation through the existing durable graph fan-out/fan-in
model. Bound maximum delegation depth, total descendants, concurrent children,
tokens/cost, and wall time. Carry ancestor lineage to reject direct or indirect
cycles unless a deliberately bounded iterative protocol says otherwise.

### Shared Environment

Represent a shared environment with logical `AgentEnvironmentRef` values and
application-owned tool/event adapters. The goal says which environment scopes
an agent may observe or modify; it does not persist raw credentials or assume
the world is an in-memory actor map.

Concurrent agents may observe stale or conflicting state. External mutation
still requires the normal effect safety class, approval policy, idempotency or
reconciliation support, and—when needed—an application-provided lease,
reservation, compare-and-set, or transaction. Rakka's per-run single writer
does not serialize multiple agents acting on the same external resource.

### Collective Memory

Collective memory consists of authorized communal knowledge spaces and shared
artifacts. It does not merge or expose each agent's private memory.

- Child agents inherit only explicitly delegated `KnowledgeSpaceId` access.
- Agents append claims with stable operation IDs and provenance containing
  goal, task, agent, run, delegation, and evidence references.
- Concurrent or conflicting claims coexist under trust/dispute rules.
- A parent may promote verified child results into communal memory through an
  idempotent memory effect.
- Retrieval snapshots record which shared claims influenced each decision.

This gives collaboration a blackboard without turning it into an
uncontrolled shared mutable prompt.

### Workflows as Tools

Expose a compiled workflow in an agent toolset using a
`WorkflowToolDescriptor` containing:

- stable tool/workflow definition and version;
- input/output artifact schemas;
- required capabilities and credential bindings;
- allowed goal/environment/knowledge-space classes;
- deadline, cancellation, and compensation policy;
- child-run identity/idempotency construction; and
- declared contained effect/safety capabilities.

Invoking the tool creates or addresses a durable child workflow run. The
parent receives a handle and waits for a durable result command; it does not
hold a dispatcher task open.

Never wrap the entire child workflow as one opaque retryable effect. The
start/adopt operation must deduplicate by stable child-run key, and each
internal model/tool/non-idempotent effect keeps its own durable boundary. The
workflow tool's apparent safety cannot be stronger than its admitted internal
effects and policy.

### Cancellation and Failure

Root cancellation, expiry, or immediate capability revocation should propagate
to active child agents and workflows as durable A2A/workflow commands. Parent
termination does not prove a started child side effect was cancelled; child
effects retain their own reconciliation duties.

Treat cancellation as progress rather than one instantaneous boolean. Fence
new model/tool/delegation dispatch immediately, then record propagation and
quiescence. If any consequential effect has an unknowable outcome, retain a
nonterminal reconciliation wait with cancellation requested. Do not project
terminal cancellation until those effects have known outcomes or an explicit
reconciliation decision records the remaining risk.

Define fan-in policy explicitly: wait-for-all, wait-for-any, quorum, first
acceptable evidence, or policy evaluator. Define whether failed children may be
reassigned and ensure reassignment creates a new delegation generation without
replaying ambiguous non-idempotent work.

## Durable Agent Loop

Model the loop as a small Rakka-owned state machine. A useful initial phase set
is:

```text
PreparingContext
    -> AwaitingModel
    -> EvaluatingModelOutput
    -> AwaitingToolEffects (zero or more)
    -> RecordingTurn
    -> DecidingContinuation
    -> PreparingContext | Completed | Suspended
```

Persist enough information to recover the next transition without rerunning a
previous decision:

- turn number and phase;
- settings and policy revision;
- immutable context snapshot reference;
- current model/tool effect IDs;
- accepted result references;
- iteration, token, cost, and deadline budgets;
- pending checkpoint or timer ID; and
- a versioned loop-state schema.

Use Rig to build and execute a model request. Do not make Rig's internal
serialized runner state the durable compatibility contract. This keeps Rakka
able to upgrade Rig through an explicit adapter migration.

## Effect Safety Guidance

Every operation outside the deterministic transition should declare a safety
class:

```rust
enum EffectSafety {
    ReadOnly,
    Idempotent { external_key: String },
    Reconcileable { protocol: ReconciliationProtocolRef },
    NonIdempotent,
}
```

The declaration should be policy-controlled, not chosen freely by model
output. Tool registration should supply the maximum permitted safety class,
required capabilities, timeout, and default gate policy.

### Dispatch Rules

- Persist effect intent before making it dispatchable.
- Persist `Started` with a lease/fence before external invocation.
- Reuse the same external idempotency key for an idempotent retry.
- Reconcile a reconcileable effect after an ambiguous timeout or lease loss.
- Never auto-retry a `NonIdempotent` effect once invocation may have started.
- Reject stale completions using effect ID, generation, lease/fence, and
  terminal-state checks.

If a non-idempotent worker disappears after `Started`, record the effect as
`Indeterminate`, transition the run to `WaitingForReconciliation`, and revoke
automatic dispatch eligibility.

An operator who proves that no invocation occurred should cause creation of a
new effect generation. The system should not mutate the ambiguous original
effect back into a routine retry.

### Model Calls

Model calls are externally billed and may be stochastic. Treat them as normal
effects with explicit retry policy rather than assuming they are read-only.
Applications may classify a provider call as retryable when duplicate billing
and a different answer are acceptable. A strict deployment may instead stop
an ambiguous model call for reconciliation or budget authorization.

## HITL and Authorization Guidance

Use one durable checkpoint substrate with distinct checkpoint kinds:

- `Approval`: a principal decides whether an already-defined effect may run;
- `SecurityAuthorization`: a principal or authorization service supplies a
  capability or credential binding needed for the effect; and
- `IndeterminateEffectReconciliation`: an operator supplies evidence about an
  effect whose outcome cannot be known automatically.

### Bind the Decision to Exact Intent

An approval or authorization grant should bind to:

- tenant, `AgentGoalId`, `AgentTaskId`, `AgentId`, and `AgentRunId`;
- checkpoint and effect ID;
- tool/target identity;
- canonical argument digest;
- settings and policy revision;
- required role/capability;
- approving principal;
- creation and expiration timestamps;
- maximum use count, normally one; and
- a logical credential binding reference, when applicable.

If the target, arguments, policy, relevant settings, or credential binding
changes, invalidate the decision and open a new checkpoint. Revalidate the
grant immediately before dispatch.

### Waiting Behavior

After the durable checkpoint and notification effect are accepted, the run
does no work. Let the entity become idle and passivate. Durable timers handle
SLA, expiration, and escalation.

For sensitive or non-idempotent effects, a timeout should reject, fail, or
escalate. It should not auto-approve by default.

Human decisions and authorization resolutions should enter through authenticated
A2A or application-owned secure callbacks, then through Rakka's durable inbox.
Duplicate submissions must not resume the run twice.

### A2A State Projection

Recommended public mapping:

| Rakka task/current-run condition | A2A task state |
| --- | --- |
| Waiting for ordinary approval/input | `INPUT_REQUIRED` |
| Waiting for credential/capability authorization | `AUTH_REQUIRED` |
| Waiting for indeterminate-effect reconciliation | `INPUT_REQUIRED` plus structured reason |
| Operator abandoned an indeterminate effect | `FAILED` |

A2A does not have a dedicated indeterminate-effect task state. Do not use
`UNSPECIFIED`; expose a stable structured status code and non-secret context.

## Memory Architecture Guidance

### 1. Short-Term Session Memory

Store ordered session entries under
`(TenantId, AgentId, AgentRunId, Sequence)`. Use PostgreSQL as the initial
authoritative store.

Each append should carry a stable `MemoryOperationId`, source command/effect
ID, content hash or artifact reference, redaction/classification metadata, and
revision. A uniqueness constraint makes replay harmless.

Keep recent history plus a rolling summary. Large prompts, outputs, and tool
results belong in encrypted object storage with bounded artifact references,
not in actor snapshots.

### 2. Agent-Private Long-Term Memory

Store semantic or episodic memories under `(TenantId, AgentId, MemoryId)`.
Preserve the source `AgentRunId` as provenance without making it part of the
ownership boundary.

Recommended fields include:

- content or artifact reference;
- source and creation time;
- confidence and memory type;
- embedding model, dimensions, and version;
- classification and access policy;
- retention/expiry and tombstone state; and
- the idempotent operation that created or updated the memory.

Use PostgreSQL plus `pgvector` first. Treat embeddings as rebuildable derived
data and preserve the source content/reference independently.

### 3. Communal Knowledge Graph

Make sharing explicit through `KnowledgeSpaceId`. The safe default is a
tenant/organization knowledge space, not a process-global graph across
customers.

Agents should append claims with provenance rather than overwrite canonical
facts. A claim should include subject, predicate, object, source agent/run,
evidence references, confidence, classification, trust status, and policy
revision.

Recommended trust states are:

```text
Proposed -> Verified
    |          |
    +-> Disputed
    +-> Retracted
```

Retraction should append a durable statement referencing the original claim.
It should not erase audit provenance. High-impact promotion to `Verified` can
be protected by HITL or a specialized verifier agent.

Keep the communal graph interface independent of database and query language.
An implementation may use relational graph tables, a property-graph database,
an RDF/triplestore, or a managed graph service. Backend selection belongs to
deployment and adapter wiring, not the agent-domain API.

### Retrieval Snapshots

For each model turn:

1. load the bounded short-term window;
2. retrieve agent-private semantic memories;
3. retrieve authorized communal claims;
4. apply trust, classification, recency, and budget filters;
5. persist an immutable `MemoryContextSnapshot`; and
6. dispatch the model effect using only that snapshot.

The snapshot should contain exact bounded content or immutable
content-addressed references, result IDs, query/retriever versions, index
watermarks where available, and policy/settings revisions. Retries reuse the
snapshot. The next turn may retrieve a newer view.

This prevents a dispatcher retry from silently changing the model's input due
to concurrent memory writes or eventually consistent indexes.

## Agent Observability Architecture Guidance

### Treat a Session as a Linked Trace Graph

Use `AgentRunId` as the stable session correlation key and `AgentGoalId` as the
cross-agent collaboration key. Use `AgentTaskId` as the stable public work
correlation across handoff. Do not hold one OpenTelemetry span object open for
any of these lifetimes. A run can wait, passivate, move shards, and resume in
another process days later; a task may span multiple runs; a goal can span many
tasks and runs concurrently.

Recommended segmentation:

- one A2A server trace for each incoming request/stream interaction;
- one bounded agent invocation/turn trace segment for active reasoning and
  deterministic transitions;
- one dispatcher trace segment for each asynchronous effect attempt;
- one resume/recovery trace segment after a timer, human decision,
  authorization, callback, passivation, owner restart, or shard movement; and
- one outbound client trace segment for an A2A delegation or remote provider
  call.

Persist W3C context and causal links in commands, effects, timers,
checkpoints, and callbacks. On synchronous work, use normal parent/child
relationships. On deferred work or long waits, end the producer/parking span
and create a later consumer/resume span with links known at span creation.

This gives an operator a session trace graph:

```text
AgentGoalId / rakka.agent.goal.id
    |
    +-- AgentTaskId / rakka.agent.task.id
    |   +-- AgentRunId / gen_ai.conversation.id
    |   +-- trace: A2A ingress and active turn
    |      +-- decision / plan
    |      +-- model inference
    |      +-- effect scheduled
    |
    +-- specialist AgentRunId
    |   +-- trace: outbound delegation and child work
    |
    +-- trace: dispatcher attempt
    |      +-- execute tool
    |      +-- downstream HTTP/RPC/DB/process
    |
    +-- trace: approval or authorization resume
    |
    +-- trace: recovery on another shard owner
```

The session view is assembled by `AgentRunId`, causation/correlation IDs, and
span links. A task view joins current and prior runs by `AgentTaskId`, including
handoff/reassignment. A goal view joins all authorized tasks/runs by
`AgentGoalId`, delegation lineage, and workflow-run links. A trace ID is not a
durable task, session, or goal identity.

### Recommended Span Topology

Use OpenTelemetry GenAI conventions where they accurately describe the
operation and Rakka-specific names for durable runtime operations they do not
cover.

| Operation | Span name/kind | Notes |
| --- | --- | --- |
| A2A request ingress | protocol HTTP/RPC `SERVER` | Extract W3C context before durable acceptance |
| Active agent turn | `invoke_agent {agent.name}`, `INTERNAL` | Bounded active work only, not passive wait time |
| General loop decision | `rakka.agent.decide`, `INTERNAL` | Emits structured outcome; does not capture hidden reasoning |
| Explicit task decomposition | `plan {agent.name}`, `INTERNAL` | Emit only when planning is reliably distinguishable |
| Rig model call | `{gen_ai.operation.name} {model}`, `CLIENT` | Usually `chat`, `generate_content`, or `text_completion` |
| Embedding call | `embeddings {model}`, `CLIENT` | Child of memory promotion/retrieval preparation |
| Memory retrieval | `retrieval {data_source}`, `CLIENT` | Query/content remains opt-in and protected |
| Memory mutation | `create_memory`, `upsert_memory`, etc. | `CLIENT` for remote store, `INTERNAL` for in-process test store |
| Outbox scheduling | `rakka.agent.effect.schedule`, `PRODUCER` | Ends after durable scheduling and carries effect correlation |
| Dispatcher processing | `rakka.agent.effect.dispatch`, `CONSUMER` | Links to scheduling and prior-attempt spans |
| Tool wrapper | `execute_tool {tool.name}`, `INTERNAL` | Downstream HTTP/RPC/DB/process spans are children |
| Outbound A2A delegation | `invoke_agent {peer.name}`, `CLIENT` | Preserve causation and task/message correlation |
| Workflow tool invocation | `rakka.agent.workflow.invoke`, `INTERNAL` | Link a bounded workflow class plus parent goal/run to the independently durable child workflow run |
| Goal evaluation | `rakka.agent.goal.evaluate`, `INTERNAL` | Record evaluator, criteria revision, evidence references, and outcome without hidden reasoning |
| Checkpoint park | `rakka.agent.checkpoint.open`, `INTERNAL` | Ends immediately after durable wait/notification scheduling |
| Resume/recovery | `rakka.agent.run.resume` or `.recover`, `INTERNAL` | Link to parked span and triggering timer/human/callback span |

An automatic provider retry that is part of one logical model operation may be
represented by one logical GenAI span plus bounded retry events. Rakka effect
attempts still need their own durable attempt correlation because they govern
idempotency and indeterminate outcomes.

### Decision Telemetry

Represent every durable agent decision as a structured runtime event and a
bounded span/event when tracing is enabled. Recommended fields are:

- `AgentId`, `AgentGoalId`, `AgentTaskId`, `AgentRunId`, delegation lineage,
  turn index, and loop phase;
- decision kind: `continue`, `call-tools`, `delegate`, `wait`, `complete`,
  `handoff`, `team-operation`, `moderated-turn`, `submit-result`, `fail`,
  `request-approval`, `request-authorization`, or `reconcile`;
- decision source: `model`, `deterministic-policy`, `human`, or
  `authorization-service`;
- selected tool/target classes and count;
- settings, policy, plan, context-snapshot, and state revisions;
- current budget bucket and stop reason;
- effect safety class and gate outcome;
- causation ID, correlation ID, and durable event sequence; and
- stable reason code or an authorized artifact reference to a redacted
  decision summary.

Do not equate observability with chain-of-thought capture. Raw hidden reasoning
should not be emitted. The durable inputs, selected action, policy checks,
state revisions, model/tool results, and protected summary artifacts are the
explainability surface.

### Attribute Mapping

The OpenTelemetry adapter should map Rakka identities consistently:

| Rakka field | OpenTelemetry mapping | Signal policy |
| --- | --- | --- |
| `AgentId` | `gen_ai.agent.id` | Traces/logs only; stable pseudonym allowed |
| Bounded agent telemetry/template name | `gen_ai.agent.name` | Must not be an arbitrary per-instance display name |
| Definition revision | `gen_ai.agent.version` | Traces/logs; bounded version |
| `AgentGoalId` | `rakka.agent.goal.id` | Restricted traces/logs only; never a metric label |
| `AgentTaskId` | `rakka.agent.task.id` | Restricted traces/logs only; never a metric label |
| `AgentRunId` | `gen_ai.conversation.id` | Traces/logs only; never a metric label |
| `AgentDelegationId` | `rakka.agent.delegation.id` | Restricted traces/logs only; never a metric label |
| Rig/provider operation | `gen_ai.operation.name` | Standard well-known value where applicable |
| Provider/model | `gen_ai.provider.name`, request/response model | Traces and bounded metrics |
| Tool registration | `gen_ai.tool.name`, `gen_ai.tool.type` | Tool name must come from a bounded registry |
| Error | `error.type` plus `rakka.error.code` | Stable low-cardinality code |
| Settings revision | `rakka.agent.settings_revision` | Trace/log attribute, not hot metric label |
| Turn index | `rakka.agent.turn.index` | Trace/log attribute |
| Decision | `rakka.agent.decision.kind/source` | Span/event; bounded values |
| Effect safety/status | `rakka.agent.effect.safety/status` | Span/event and bounded metrics |

IDs may be useful in restricted traces and logs but must not be copied into
metric labels or baggage. If tenant policy forbids raw IDs in telemetry, emit a
stable scoped pseudonym and keep the reversible mapping outside the telemetry
backend.

### Metrics

Emit the standard GenAI metrics supported by the selected convention revision
when values are known from the provider:

- client operation duration;
- input/output token usage, including provider-reported cached or reasoning
  token categories where supported;
- time to first chunk and time per output chunk for streaming;
- agent invocation duration;
- workflow duration for multi-agent orchestration; and
- tool execution duration.

Add Rakka metrics for durable/runtime concerns not covered upstream:

| Metric | Instrument | Bounded labels |
| --- | --- | --- |
| `rakka.agent.decisions` | Counter | decision kind/source, outcome |
| `rakka.agent.task.transitions` | Counter | task status class, transition, outcome |
| `rakka.agent.task.result_rejections` | Counter | bounded rule class, outcome |
| `rakka.agent.handoffs` | Counter | target class, outcome |
| `rakka.agent.team.operations` | Counter | operation, outcome |
| `rakka.agent.moderation.turns` | Counter | mode, outcome |
| `rakka.agent.goal.evaluations` | Counter | goal mode, evaluator class, outcome |
| `rakka.agent.delegations` | Counter | peer/skill class, outcome |
| `rakka.agent.decision.duration` | Histogram, seconds | decision kind/source |
| `rakka.agent.turn.duration` | Histogram, seconds | outcome, model profile, bounded agent class/version |
| `rakka.agent.run.active` | Gauge | status class, tenant tier |
| `rakka.agent.wait.duration` | Histogram, seconds | wait/checkpoint kind, outcome |
| `rakka.agent.effect.indeterminate` | Counter | effect/tool class, safety class |
| `rakka.agent.memory.operation.duration` | Histogram, seconds | memory tier, operation, backend class |
| `rakka.agent.memory.records` | Histogram | memory tier, operation |
| `rakka.agent.recovery.duration` | Histogram, seconds | recovery reason, outcome |
| `rakka.agent.telemetry.export.failures` | Counter | signal, exporter class, error code |

Use metric histograms and gauges for fleet health rather than querying sampled
traces for totals. Do not label metrics with goal/task/agent/run/delegation/
effect/checkpoint/memory IDs, prompt names supplied by users, raw model
responses, URLs, or full errors.
When supported by the application SDK and backend, histograms should attach
exemplars that link representative measurements to sampled trace/span IDs.

### Structured Logs, Runtime Events, and Audit

Use structured logs for detailed operational occurrences and correlate them
with active `trace_id`, `span_id`, durable IDs, event sequence, causation ID,
and correlation ID. Logs should use stable event names and stable error codes.

Keep the roles distinct:

- durable run/effect/memory state is correctness truth;
- audit records are immutable compliance evidence;
- runtime events are ordered post-persistence projections;
- spans show causality and latency;
- logs provide detailed diagnostic occurrences; and
- metrics provide aggregates and SLO indicators.

Telemetry export failure must not roll back or invent a durable transition.
Exporter failure must itself be visible through bounded metrics and operational
snapshots, with bounded buffering and explicit drop counts.

### Content and Privacy Policy

Default production policy:

- do not record system instructions, input/output messages, prompt variables,
  hidden reasoning, tool definitions, tool arguments/results, retrieval query
  text/documents, or memory query/content in telemetry;
- record counts, byte/token sizes, hashes, model/tool identifiers from bounded
  registries, redaction/classification status, and protected artifact refs;
- prohibit credentials, authorization headers, secret bindings, and decrypted
  data in all telemetry modes;
- require explicit tenant/admin opt-in for content capture;
- use a separate encryption, access-control, retention, and audit policy for
  any captured content; and
- apply Collector allowlist/redaction/transform processors as defense in depth.

Content hooks should write to application-owned protected artifact storage and
attach an immutable reference. They should not inject large JSON prompts into
span attributes merely because a backend accepts them.

Baggage should be restricted to low-cardinality routing/policy classes such as
deployment channel, tenant tier, workload class, and policy class. Do not put
raw tenant/user/agent/run IDs or content in baggage because it can propagate to
third-party endpoints.

### Sampling and Collector Guidance

Prefer native OTLP export from the application-owned OpenTelemetry SDK to a
Collector gateway. Keep the Rakka crates SDK-neutral: they emit `tracing`
spans/events, backend-neutral metrics, and bridge records; the binary installs
the subscriber, SDK, processors, and exporter.

The current Rakka bridge is not a substitute for every OTLP field. The agent
adapter must preserve span kind, status, events, instrumentation scope/schema,
and GenAI metric units, buckets, temporality, and exemplars. Extend the bridge
additively or map directly into the selected SDK when the existing record
cannot represent a required field; do not silently discard it.

For trace sampling:

- propagate valid trace context even when a span is not recorded;
- never sample metrics, audit correctness, or durable event acceptance through
  the trace sampler;
- retain all traces containing errors, `Indeterminate` effects, security
  denials, policy overrides, checkpoint escalations, stale-owner conflicts,
  recovery failures, or configured high latency;
- sample routine successful turns at a lower rate; and
- place sampling-relevant bounded attributes on spans at creation.

Tail sampling is the recommended production direction when volume requires
sampling, but it is operationally stateful. A scaled gateway needs a first
tier/load-balancing exporter that routes all spans for a trace ID to the same
tail-sampling instance. Size `decision_wait`, trace buffers, memory limiter,
queues, and exporter retry together; monitor Collector refusal, queue, drop,
and export-failure metrics.

The existing Rakka agent/gateway Collector manifests are a strong topology
starting point. Before production, update the pinned Collector version and
revalidate component names/stability, TLS/mTLS and authentication, NetworkPolicy,
redaction allowlists, tail sampling, trace-ID routing, persistent queues if
required, and backend endpoints.

### Session Query and Operator Views

Start with an authoritative operational point query backed by durable
task/run/effect/checkpoint/timer/budget state. It must remain available when an
entity is passivated and must not depend on trace retention or exporter health.
Return the state revision, lifecycle versus residency, last material progress,
current wait, next wake, budget usage/reservations, pending/indeterminate
effects, cancellation propagation, and projection freshness.

Provide an authorized session observability query keyed by tenant plus
`AgentId`/`AgentRunId`. It should assemble references to:

- ordered durable runtime events and state revisions;
- linked trace segments;
- correlated structured logs and audit records;
- model calls, token usage, latency, and finish reasons;
- decisions and selected actions;
- tool attempts, retries, safety class, and effect outcome;
- memory retrieval snapshot IDs and result counts;
- checkpoint waits and resolver outcomes;
- recovery, passivation, ownership, and dispatcher changes; and
- protected content/artifact references when authorized.

This query is an observability projection, not a second state machine. List and
search projections may lag but should expose their revision or lag; an
authoritative point read must make its durable revision explicit.

Provide an authorized task view keyed by tenant plus `AgentTaskId`. It should
assemble the typed definition/result, dependencies, assignment history,
handoff/reassignment runs, result-rule rejections, artifacts, checkpoints, and
terminal outcome. It remains a projection of durable task/run state.

Provide a separate authorized goal view keyed by tenant plus `AgentGoalId`.
It should assemble the root and specialist runs, delegation/fan-in graph,
workflow invocations, progress evaluations, evidence, budget allocation,
cancellation propagation, and terminal goal decision. This view is also a
projection; durable goal/run/effect state remains authoritative.

Initial dashboards and alerts should cover active/waiting/failed runs, active
turn latency, provider latency/errors/tokens, tool latency/errors,
indeterminate effects, checkpoint age, recovery latency/failure, dispatcher
backlog, shard ownership imbalance, memory retrieval latency, and Collector
drops/queue pressure.

### Semantic Convention Versioning

The core Rakka event and metric vocabulary should remain stable. Put mapping to
the developing OpenTelemetry GenAI conventions behind the `otel` feature or an
adapter layer and record the reviewed convention revision in the
instrumentation scope/schema metadata.

An OpenTelemetry GenAI convention upgrade requires a compatibility review of
span names/kinds, metric names/units, required attributes, content-capture
rules, and Collector transformations. It must not require a migration of
durable agent state merely because a telemetry convention changed.

## Security Guidance

- Include `TenantId` in every durable key and authorization decision.
- Authorize access before returning whether an agent, run, memory, or claim
  exists.
- Persist credential binding references only. Resolve credentials at dispatch
  time and keep them in memory only for the bounded attempt.
- Treat a model-visible `ToolDescriptor`, an admitted `ToolBinding`, a durable
  `EffectIntent`, and a current `DispatchGrant` as separate security layers.
- Bind consequential dispatch to an application-owned execution-policy
  reference describing the workload identity, trust tier, network-egress,
  sandbox, secret-resolution, and tenant-isolation classes.
- Route high-authority effects to appropriately isolated dispatcher pools or
  bounded sandboxes; do not give every shared dispatcher ambient access to
  every agent tool or credential class.
- Treat retrieved memory as untrusted content, not system instructions.
- Preserve provenance and trust state in the prompt representation.
- Require capabilities for private-memory writes and communal claim appends.
- Redact prompts, memory content, credentials, tool arguments, and raw IDs from
  metrics and bounded-label telemetry.
- Provide retention, tombstone, export, and deletion semantics from the first
  persistent-memory schema, even if policy UI comes later.

## Suggested Crate Boundaries

Follow Rakka's existing foundation/adapter split:

```text
crates/rakka-agent
    goal/task/run domain types and typed task/result contracts
    goal, evaluation, delegation, and workflow-tool contracts
    durable loop runtime
    Rig adapter
    tool/effect policy
    checkpoint, session-memory, and private-memory traits
    structured decision/runtime telemetry contracts
    in-memory/test implementations

    feature "otel"
        reviewed GenAI semantic-convention mapping
        OTLP bridge integration without owning the application SDK

crates/rakka-agent-postgres
    short-term memory store
    private-memory store
    pgvector retrieval adapter

crates/rakka-agent-knowledge-graph
    database-agnostic communal graph domain and adapter SPI
    portable query and capability model
    in-memory test implementation

crates/rakka-a2a, feature = "agents"
    AgentId/AgentGoalId/AgentTaskId/AgentRunId routing
    task-state projection
    goal/delegation metadata extension
    authenticated gate resolution
```

`rakka-agent` should depend on `rakka-agent-workflow`. `rakka-a2a` may adapt
the agent crate through an additive feature. Avoid making the core agent domain
depend on the public protocol adapter and then creating a dependency cycle.

`rakka-agent-knowledge-graph` should not depend on a database driver or expose
SQL, Cypher, SPARQL, vendor result types, or vendor-specific identifiers. A
concrete store binding may be supplied by the application or by separately
versioned backend crates without changing the agent-facing contract.

## Suggested Delivery Slices

### Slice 0: Contract and Failure Model

- Finalize agent, goal, typed task, run, delegation, run status, loop state,
  effect safety, result/evaluation, and checkpoint types.
- Specify the indeterminate transition and recovery invariants.
- Define the Rig version/upgrade boundary.
- Define session correlation, span topology, decision events, redaction, and
  OpenTelemetry GenAI convention version policy.
- Add no production model or memory backend yet.

### Slice 1: One Durable Rig Agent With Read-Only Tools

- Create `rakka-agent` and an in-memory example.
- Accept one versioned typed task and validate a typed terminal result.
- Drive one Rig model turn as a durable effect.
- Trace A2A ingress, the bounded turn, one Rig model call, and recovery with
  correlated structured events and metrics.
- Use read-only tools only.
- Recover across actor restart, passivation, and shard movement.
- Prove that a logically active waiting run has no resident per-agent task,
  process, connection, lease, or open span and resumes from a durable trigger.

### Slice 2: Tool Effects and Indeterminate Recovery

- Split every tool call into its own effect.
- Add safety classes and dispatcher enforcement.
- Add `EffectIndeterminate` and `WaitingForReconciliation`.
- Link effect scheduling, dispatcher attempts, tool execution, and
  indeterminate reconciliation across trace segments.
- Kill the dispatcher before invocation, during invocation, and after external
  commit to prove the boundary.

### Slice 3: Typed HITL and A2A

- Generalize the existing human checkpoint with compatible typed metadata.
- Add authorization and reconciliation statuses.
- Project `INPUT_REQUIRED` and `AUTH_REQUIRED` correctly.
- End spans at durable waits and link later human/authorization resume spans.
- Resume exclusively through durable inbox commands.

### Slice 4: Durable Session Memory

- Add PostgreSQL short-term memory with idempotent append and revision checks.
- Persist one context snapshot per model effect.
- Trace retrieval and snapshot creation without recording query or memory
  content by default.
- Prove isolation between two runs of the same agent.

### Slice 5: Private and Communal Memory

- Add private semantic memory and versioned embeddings.
- Add the database-agnostic claim graph crate and in-memory contract tests.
- Validate at least two structurally different backend implementations or test
  doubles before declaring the SPI portable.
- Add HITL/policy promotion of high-impact claims.

### Slice 6: Multi-Agent Goals and Workflow Tools

- Add a durable root goal with versioned success criteria and evidence-based
  evaluation.
- Resolve specialist capabilities through an application-owned authorized
  catalog and advertise them through A2A Agent Cards/skills.
- Add durable A2A delegation with child `AgentTaskId`/`AgentRunId` values,
  lineage, fan-out/fan-in, cycle detection, and bounded descendant budgets.
- Add handoff preserving `AgentTaskId` while fencing source/target runs.
- Add a durable team task board with atomic claims and a bounded moderation
  protocol with durable turns.
- Add workflow descriptors whose invocation creates or adopts an independently
  durable child workflow run rather than an opaque retryable tool effect.
- Add communal graph/artifact provenance for `AgentGoalId`, `AgentTaskId`,
  `AgentRunId`, and delegation identity.
- Add goal-level query, tracing, metrics, cancellation, deadline, and
  passivation/recovery tests.

### Slice 7: Production Fault and Security Validation

- Run multi-pod dispatcher and shard-movement fault injection.
- Exercise settings changes and credential revocation during waits.
- Test memory ACL, poisoning defenses, retention, and deletion.
- Validate native OTLP wiring at the application boundary, GenAI semantic
  mapping, trace/log correlation, redaction, tail-sampling retention,
  Collector loss visibility, bounded metrics, audit events, and artifacts.

## Decision Register

### Approved Article-Review Decisions

The full-article technical review resolved these defaults:

1. **Continuous execution:** use a stable durable goal/root controller that
   admits finite child epoch tasks/runs. Pod lifetime never defines agent
   lifetime and pod start never creates an epoch.
2. **Wake behavior:** forbid overlapping epochs and coalesce triggers by
   default; after downtime admit one coalesced occurrence rather than
   unbounded catch-up. Fence obsolete schedule revisions.
3. **Budgets:** use hierarchical durable allocations/reservations with
   per-epoch and rolling/window ceilings. Count started, retried, and
   indeterminate attempts against applicable budgets.
4. **Admission:** fail closed for unattended operation unless criteria,
   bounds, cancellation, inspectability, scoped authority, gates, escalation,
   and recovery are defined.
5. **Tool authority:** separate model-visible descriptor, admitted binding,
   effect intent, dispatch grant, and executor isolation. Use trust-tier
   dispatcher pools with stronger per-effect sandboxing where policy requires.
6. **State and operations:** keep materialized task state bounded and separate
   from history/content/memory/projections. Provide authoritative operational
   queries independent of telemetry.
7. **Cancellation:** fence new work immediately, but keep a task nonterminal
   in reconciliation while a consequential effect has an indeterminate
   outcome.
8. **A2A baseline:** target and pin a reviewed A2A 1.0 contract for the agent
   surface, with any legacy compatibility documented explicitly.

### Remaining Discovery Decisions

Recommended defaults are included so research can continue without blocking.

1. **Communal scope:** use tenant/organization `KnowledgeSpaceId`; require an
   explicit federation feature for cross-tenant sharing.
2. **Concurrent runs:** allow them; use idempotent append/CAS for shared private
   memory.
3. **Indeterminate resolution:** permit completion, proven-not-executed,
   compensation, escalation, or abandonment; do not provide an ordinary
   `Retry` decision.
4. **Approval timeout:** reject or escalate sensitive effects; never
   auto-approve opaque non-idempotent work.
5. **Authorization resolver:** allow either a human principal or an external
   authorization service, both represented by an authenticated resolver
   identity and the same durable command path.
6. **Memory promotion:** let agents append `Proposed` claims; require policy or
   HITL for promotion to `Verified` when the claim can authorize or influence
   high-impact action.
7. **Short-term retention:** scope one execution session by `AgentRunId` and
   retain according to tenant policy after terminal completion; map the public
   A2A task to `AgentTaskId`.
8. **Settings updates:** apply ordinary prompt/model changes at the next turn;
   recheck cancellation, capability revocation, credential revocation, and
   safety policy immediately before dispatch.
9. **Model-call ambiguity:** make the application's cost/replay tolerance an
   explicit policy rather than a hidden dispatcher default.
10. **Initial graph backend:** keep the product contract database-agnostic;
    choose a reference backend only after representative claim, traversal,
    tenancy, and migration queries have been captured.
11. **Session tracing:** use bounded trace segments linked across waits and
    recovery; use `AgentRunId`/`gen_ai.conversation.id` for session assembly.
12. **Content capture:** disabled by default; authorized protected artifact
    references are preferred over content-bearing span attributes.
13. **Sampling:** keep errors, indeterminate effects, security/policy events,
    escalations, and slow/recovery traces with tail sampling where operationally
    justified; sample routine success.
14. **GenAI semantic conventions:** pin a reviewed Development revision behind
    an adapter and do not persist its attribute names as domain state.
15. **Goal identity:** use a separate `AgentGoalId`; initially let the stable
    root `AgentTaskEntity` coordinate it and allow its generated value to
    default to the root `AgentTaskId` without making the two types
    interchangeable.
16. **Specialist selection:** allow a model or deterministic planner to request
    a skill, but let an application-owned authorized catalog resolve the
    concrete target `AgentId` and capabilities.
17. **Workflow tools:** create or adopt an independently durable child workflow
    run and preserve every internal effect boundary; do not dispatch an entire
    compiled workflow as one opaque retryable effect.
18. **Task identity:** introduce `AgentTaskId` and map it to A2A `Task.id`;
    preserve it across handoff while each assignee uses a distinct
    `AgentRunId`.
19. **Coordination capabilities:** make handoff, delegation, team, and
    moderation first-class typed durable state machines, not prompt templates.
20. **Dynamic setup:** allow per-run instructions/capabilities only within the
    versioned definition and mandatory policy envelope.
21. **Events:** expose replayable task/run/coordination events with cursor and
    resync semantics; derived struggle signals remain observability only.

## Anti-Patterns to Avoid

- One actor future executing an entire autonomous loop.
- Starting or resuming an agent epoch because a pod, actor, or dispatcher
  process started.
- Using a Kubernetes `CronJob`, pod-local scheduler, recurring actor timer, or
  sleeping future as the correctness source for continuous-agent wakes.
- Reusing one unbounded `AgentRunId` and short-term-memory session for every
  epoch of a continuous goal.
- Resetting a schedule, missed-occurrence backlog, or safety budget on process
  restart, activation, or shard movement.
- Keeping a sleeping task, polling loop, connection, stream, dispatcher lease,
  or actor alive so an "always-on" agent can wake later.
- Treating `Active`, `Running`, or `Waiting` as a physical residency state.
- Treating a model's self-declaration of completion as sufficient goal
  evidence.
- Collapsing stable typed task identity into one assignee's execution run,
  making handoff mutate `AgentId` or memory scope.
- Implementing handoff, team, or moderation as prompt-only conventions without
  durable ownership, backlog, or turn state.
- Treating successful child-run completion as proof that the root goal is
  satisfied.
- Using direct actor references or unbounded recursive delegation for agent
  collaboration.
- Implicitly exposing one agent's private memory to collaborators.
- Assuming a sharded actor or per-run owner serializes multiple agents acting
  on the same external resource.
- Wrapping a compiled workflow and all of its effects in one opaque retryable
  tool call.
- One durable effect wrapping multiple effectful tool calls.
- Treating dispatcher lease ownership as proof that an effect did not happen.
- Automatically retrying an opaque non-idempotent `Started` effect.
- Persisting a resolved credential or bearer token in agent state.
- Treating a model-visible tool schema as proof of capability, authorization,
  credential access, network reachability, or executor isolation.
- Giving one shared dispatcher pool ambient authority for every tenant/tool
  class when the software policy expects stronger isolation.
- Embedding unbounded messages, tool results, assignment history, or memory in
  materialized task state.
- Using a caller-provided conversation string as the only memory boundary.
- Letting Rig memory hooks write directly without stable operation IDs.
- Treating vector retrieval results as deterministic correctness state.
- Letting agents overwrite communal truth without provenance or conflict.
- Leaking a graph vendor's driver types or query language into public Rakka
  APIs.
- Sharing one SQLite database or pod-local PVC as clustered memory.
- Exposing Rakka internal remoting as the agent-to-agent protocol.
- Holding one span open across an hours- or days-long passive session.
- Treating one trace ID as the durable agent-session identity.
- Recording chain-of-thought, prompts, tool payloads, or memory content by
  default.
- Using sampled traces to calculate correctness totals or audit evidence.
- Using telemetry or an eventually consistent search projection as the only
  way to answer current lifecycle, wait, wake, budget, cancellation, or
  indeterminate-effect state.
- Reporting terminal cancellation while a consequential external effect still
  has an unknowable outcome.
- Putting goal/task/agent/run/delegation/effect/memory IDs or raw user values
  in metric labels or baggage.
- Coupling durable domain records to a Development GenAI convention revision.

## Design-Phase Definition of Done

The design is ready for implementation planning when:

- every identity and memory scope has an explicit tenant-aware key;
- typed tasks have versioned schemas, dependencies, assignments, result rules,
  artifacts, and a public identity distinct from execution sessions;
- logical availability, runtime residency, quiescence, durable wake sources,
  and cold reactivation have separate contracts and tests;
- continuous goals have versioned wake, overlap, missed-occurrence, lateness,
  coalescing, epoch, suspension, and retirement semantics that do not depend on
  pod lifetime;
- every continuous epoch is finite, independently budgeted, and isolated by a
  distinct task/run session unless an explicit bounded alternative is
  justified;
- goal success criteria, progress evaluation, evidence, and terminal authority
  are explicit and versioned;
- delegation lineage, fan-out/fan-in, cycle prevention, cancellation, and
  descendant budgets have durable rules;
- workflow-tool invocation preserves an independently recoverable child run
  and its internal effect boundaries;
- each run status and checkpoint has defined recovery and A2A projection;
- the dispatcher table covers every effect safety class and crash window;
- settings update and revocation timing is specified;
- autonomy admission and hierarchical budget reservation/settlement are
  specified across definition, goal, task/epoch, run, and effect scopes;
- tool visibility, admitted binding, effect intent, dispatch grant, credential
  resolution, and executor isolation are separate contracts;
- Rig state versus Rakka durable state ownership is unambiguous;
- the first storage schemas can enforce idempotency and stale-writer rejection;
- the communal graph SPI passes the same conformance suite without exposing a
  backend's client or query types;
- one session can be reconstructed across ingress, decisions, model/tool
  effects, waits, recovery, and shard movement without a long-lived span;
- one collaborative goal can be reconstructed across specialist sessions,
  delegations, workflow runs, evaluations, and evidence without using raw IDs
  as metric labels;
- an active or waiting agent can be fully passivated with zero per-agent live
  execution resources and resume exactly once from every supported durable
  wake source;
- content capture, redaction, sampling, and telemetry-loss behavior are
  explicitly testable;
- authoritative operational queries remain correct without telemetry and task
  materialized state remains bounded independently from history/content;
- memory trust, deletion, and retention policies are represented in the model;
- fault-injection acceptance scenarios are enumerated; and
- unresolved product decisions are explicitly recorded in `spec.md` rather
  than hidden in implementation choices.
