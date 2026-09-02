# Rakka Agents

Status: current through Phase 6 (slice 6.4) of
[the agent implementation plan](plans/rakka-agent/implementation-plan.md).

Rakka Agents are durable, goal-driven agents built as sharded entities on the
actor framework. An agent is a durable record with a versioned definition; the
work it does is a typed task; the execution of that work is a run whose every
model call, tool call, and outbound message is a durable effect; and every
cross-entity step is an exchange that survives the loss of either side. Nothing
about an agent lives in a pod: a fully passivated agent, task, run, team, or
conversation is still addressable, still correct, and resumes on the next
durable trigger, on whichever node then owns its shard.

This document describes the surface as it behaves. The normative text is
[the specification](plans/rakka-agent/spec.md); where the two differ, the
specification governs and the difference is a defect. The companion documents
are validation artifacts. Where one states something the code can contradict —
a scenario's proving file, a schema version, a pin, a crate row, a link — a
test holds it; the fault, security, and telemetry matrices' clause dispositions
(enforced, delegated, inferred, owed) are hand-recorded and dated, and each
enforced row names the test behind it:

| Document | What it records |
| --- | --- |
| [`rakka-agent-recovery-scenarios.md`](rakka-agent-recovery-scenarios.md) | Specification 18's sixty-one recovery scenarios, each bound to the test that proves it and the fidelity it runs at. |
| [`rakka-agent-fault-injection-matrix.md`](rakka-agent-fault-injection-matrix.md) | Every durable boundary an owner is killed at, in-process and across real pods. |
| [`rakka-agent-security-validation-matrix.md`](rakka-agent-security-validation-matrix.md) | Specification 16 and the memory clauses of 13.1, clause by clause: enforced, delegated to the deployment, or still inferred. |
| [`rakka-agent-telemetry-validation-matrix.md`](rakka-agent-telemetry-validation-matrix.md) | Specification 17 clause by clause, and the pinned SDK, convention, and Collector versions. |
| [`rakka-agent-observability-catalogue.md`](rakka-agent-observability-catalogue.md) | Every `rakka.agent.*` metric with unit, buckets, and bounded labels, and every span class. |
| [`rakka-compatibility.md`](rakka-compatibility.md) | The Agent Domain section: schema versions, N/N+1 rules, stable codes, and the pinned-dependency matrix. |

## What ships, and where

| Crate | Owns | Facade feature |
| --- | --- | --- |
| `rakka-agent-workflow` | The durable execution kernel the agent domain runs on: durable inbox and outbox acceptance, the dispatcher fleet and its claim filters, timers and triggers, runtime events, the compiled execution IR and graph scheduler, and the OTLP bridge. | `agent-workflow` (default) |
| `rakka-agent` | The agent domain: the five entity classes and their choreography, the loop runtime, the model adapter trait, effects and tool authority, budgets, admission, guardrails, checkpoints, goals and wakes, delegation and coordination, memory traits and retrieval, telemetry, operational queries, schema policy, and a deterministic testkit. | `agent`; `agent-rig` adds the Rig-backed model adapter, `agent-otel` the GenAI convention mapping |
| `rakka-agent-postgres` | PostgreSQL session memory, context snapshots, agent-private long-term records, the pgvector retriever, and their migrations. | — (depend on it directly) |
| `rakka-agent-knowledge-graph` | The communal knowledge graph: provenance-bearing claims, the trust lattice, the promotion gate, the portable store SPI, an in-memory reference store, and the backend conformance harness. | — |
| `rakka-agent-knowledge-graph-postgres` | The graph's relational backend. | — |
| `rakka-a2a` (`agents` feature) | The typed A2A surface over the entities: ingress, the state projection, the agent-management and collaboration extensions, replay, and the goal view. | `a2a-agents`; `a2a-otel` adds the ingress span |

Application code reaches the surface through `rakka::agent`,
`rakka::agent_workflow`, and `rakka::a2a`; no agent-domain type enters
`rakka::prelude`. The A2A adapter's service builder, request handler,
settings, and two error types are the one feature-gated exception, re-exported
from the prelude under `a2a` and `a2a-server` and listed in the
[boundary inventory](rakka-api-boundary-inventory.md). `rakka-agent` builds
and passes its tests with `--no-default-features` (Rig is its only default feature and the facade makes
it opt-in), and the `otel` feature adds no dependency: no crate under
`crates/` imports an OpenTelemetry SDK, which specification 17.17 places at the
application binary. The agent crates are outside the publishable crate set
until they are reviewed into it
([`rakka-v1-release-packaging.md`](rakka-v1-release-packaging.md)).

## The shape of an agent

Five sharded entity classes, each a durable record with a schema version,
materialized on its shard owner from the store alone:

- **Agent** — the durable identity. Carries the versioned definition (skills,
  tools, peers, models, credential and knowledge-space references, guardrails,
  budgets, coordination capabilities), setup and settings revisions that may
  only *narrow* the definition's authority, a lifecycle status (`Active`,
  `Suspended`, terminated), and the autonomy admission decision without which
  it does no unattended work.
- **Task** — a typed unit of work with a versioned task definition and result
  rules, dependency edges, an assignment with a generation, and the goal
  contract when it is a root. A task is either agent-owned or human-owned; it
  carries a team-board claim, handoff provenance, and delegation lineage when
  those apply, and its materialized record stays inside a configured bound
  while history flows to a separate log.
- **Run** — one execution of a task by one agent under one assignment
  generation: the loop state, the effect outbox, the escrow ledger, open
  checkpoints, delegation and fan-in cells, and the session-memory scope. A
  task may have many runs over its life; a run has exactly one task.
- **Team** — a board of tasks and the members that may claim them, with a
  claim epoch per entry so a stale claim, release, or transfer fails closed.
- **Conversation** — a moderated multi-agent exchange: participants, rounds,
  whose turn it is, a transcript reference, and conversation-local budgets.

Identities are tenant-scoped and distinct types even where values coincide;
composite scopes flatten injectively into entity and persistence ids, so two
tenants can never alias one record. Every command derives its operation id from
its content rather than generating one, and the derived id is the durable
inbox's deduplication key, so a retried command is answered from the record
and a different command under a reused key derives a different operation.

Entities talk to each other only through **exchanges**, of which there are
eighteen kinds: `creation`, `assignment`, `result-proposal`,
`budget-allocation`, `budget-settlement`, `budget-return`, `epoch-result`,
`goal-evaluation`, `delegation-result`, `run-cancel`, `delegation-cancel`,
`handoff-result`, `team-claim`, `team-claim-result`, `dependency-registration`,
`dependency-outcome`, `team-terminal-notice`, and
`conversation-terminal-notice` (`AgentExchangeKind::ALL`, held to this list by
a test). Run acceptance and the result decision are the replies to assignment
and proposal, not exchanges of their own. A delegation or handoff *send* is not
an exchange either: it is an A2A outbox effect claimed by the dispatcher, whose
exhaustion becomes a fan-in disposition or a handoff refusal rather than a
courier re-drive, and only its result comes back as an exchange. An exchange is
a journal entry on the sender, re-driven by a courier
until the receiver answers from its own record; acceptance makes local
progress only and never drives an owed exchange of its own, which is what
keeps the choreography acyclic. The
route resolves the target's shard owner and either asks the local entity or
crosses `rakka-remote` under a versioned codec; the durable record an exchange
leaves behind is the same either way.

## A run, from ingress to terminal

1. **Ingress.** A task arrives through the A2A surface or the typed client. It
   is accepted into the owning entity's inbox under its derived operation id
   before it is acknowledged, so a duplicate send maps to the one task and its
   one initial run.
2. **Assignment.** The task's settle pass decides an assignment against the
   agent's *current* definition and admission: an agent that is suspended, not
   admitted, or whose definition no longer covers the task is refused, and the
   task stays assignable rather than failing.
3. **The loop.** The run persists an immutable memory context snapshot, then
   schedules a model call as a durable effect. The model adapter (Rig behind
   the `rig` feature; a deterministic adapter in the testkit) returns a bounded
   turn: text, tool-call *requests*, or a typed result proposal. Turns are
   recorded to session memory and dropped, so a run that iterates a hundred
   times persists no more state of its own than one that iterates once.
4. **Effects.** Each model call, tool call, A2A send, workflow start, and
   compensation is its own outbox ticket with a stable idempotency key. A
   dispatcher fleet claims tickets under a lease; workers may declare the
   execution classes they serve and skip the rest before taking a lease. A tool
   call is dispatched only when its binding, dispatch grant, credential
   binding, checkpoint, execution policy, and immediate safety check all pass;
   credentials are resolved at dispatch and never outlive the attempt.
5. **Waiting.** A consequential effect parks the run `WaitingForApproval` on a
   durable checkpoint whose grant is bound to the exact intent digest; a
   changed argument invalidates it. A worker lost after a non-idempotent effect
   was `Started` parks one `Indeterminate` outcome and waits for an explicit,
   deduplicated reconciliation decision — never an automatic re-invocation.
   Parked runs consume no live task, thread, or timer.
6. **Budgets.** Every run holds an escrow ledger: allocations flow down from a
   parent scope in the parent's own transition, settlements and returns flow
   back up through the exchange path, and a `Started` attempt that becomes
   `Indeterminate` still consumes the attempt budget it reserved.
7. **Terminal.** A typed result passes the task's result rules before the task
   completes; a malformed or rejected one records a rejection decision and
   costs bounded iterations. The terminal transition stamps the run's
   `terminal_at`, the clock retention is measured from.

Every step above is a durable write, and each is a boundary the fault-injection
suites kill the owner at; the [fault-injection
matrix](rakka-agent-fault-injection-matrix.md) records, per boundary, which
sweeps are demonstrated and which are still inferred.

## Goals: finite and continuous

A goal has no entity of its own. The root task carries the goal contract —
owner, objective, success criteria, budgets, allowed references, evaluator,
escalation — and every goal transition commits in the same compare-and-set as
the task transition that decided it. A finite goal becomes `Satisfied` only
through the configured evaluator's assessment of the current criteria revision
against durable evidence; an agent may propose completion, and with an
evaluator configured that proposal has one door.

A continuous goal executes as bounded **epochs**. The wake controller derives
one `AgentWakeId` per logical occurrence — from a schedule, an event, a
callback, or an A2A trigger — and admits at most one child epoch task and run
for it; concurrent triggers coalesce while one epoch owns execution, a downtime
backlog admits one coalesced representative, an obsolete schedule revision is
fenced, and a pod, actor, or dispatcher restart never creates a wake by
itself. Failure backs the goal off through durable re-wakes and escalates into
suspension; an exhausted goal window defers to the window boundary. Between
every wake, admission, epoch, and result the controller, task, and run may
passivate. Three things reactivate them, and nothing else: the wake scanner
over the durable timer index for a scheduled occurrence, an `AdmitWake` command
at the task for an event, callback, command, or A2A-trigger occurrence, and the
courier re-driving an owed exchange at its receiver — a restart by itself never
does.

An operator asking what a goal or run is doing gets an **authoritative
operational snapshot** from the durable record, with the revision it read,
correct while every telemetry path is down. The session view by `AgentRunId`
joins that snapshot with retained decision events and trace segments, exposes
its own lag, and is a projection, never a second state machine.

## Working with other agents

- **Delegation** is a run-state cell plus an A2A send effect: the child task
  and run identities are derived, so a replayed send converges on the one
  child or answers an explicit conflict, and the parent task's identity and
  ownership never change. Depth, fan-out, descendant, concurrency, budget, and
  cycle ceilings fail closed and are charged once across coordinator loss.
- **Fan-in** groups a fan-out's members into one durable cell resolved when
  every member has a disposition; child results arrive as an exchange, and the
  root resumes its model deterministically from the resolved group.
- **Workflow tools** invoke a compiled workflow as a child by a derived
  invocation id that is also the child run id and the deduplication key, so a
  replay adopts the same child and its internal effects run once.
- **Cancellation, deadlines, and revocation** propagate durably down the tree
  through cancel exchanges and effects, never claiming a started effect
  stopped: a child with an ambiguous consequential effect stays nonterminal in
  reconciliation, and the root finalizes only after its ledger closed.
- **Handoff** transfers the same `AgentTaskId` to another agent: the source run
  is fenced, one target run is created, `HandedOff` is recorded strictly after
  the target's durable acceptance, and no session or private memory travels.
- **Teams** post tasks to a board; members claim them atomically through the
  A2A surface, a claim drives the assignment under an epoch fence, and stale
  commands fail closed. Terminal tasks close their board entries.
- **Moderated conversations** run bounded rounds with a designated turn owner;
  a replayed turn is absorbed by a dense ledger, and participant, round, turn
  owner, transcript, and budgets recover across passivation.
- **Human-owned tasks** complete through authenticated, deduplicated
  submissions and unblock their dependents; a failed one propagates its
  declared dependency policy.
- **Capability envelopes** gate all of it: board membership and a participant
  roster are trusted wiring, never authority, and an agent whose definition
  does not grant `Team` or `Moderation` is refused at the operation it spends
  that capability on.

## Memory

Memory is context, never the correctness source: an empty, lagging, or
unavailable store degrades a turn's context and cannot make a run resume
incorrectly.

- **Session memory** is scoped `(TenantId, AgentId, AgentRunId)`, append-only
  with idempotent operation ids, and isolated by both agent and run.
- **Context snapshots** are immutable and persisted before every model effect;
  a retry reuses the snapshot, so drift in a store or an index cannot change a
  retried model input.
- **Agent-private long-term memory** is scoped `(TenantId, AgentId)`,
  promoted from session memory through a checkpoint-gated executor, and ranked
  for retrieval by a retriever (pgvector in `rakka-agent-postgres`). The
  retriever supplies a ranking and nothing else: every ranked identity is
  resolved through the authoritative store before it is admitted, so a foreign
  or forged record is dropped and counted, and an unauthorized read reveals
  nothing about existence.
- **The communal knowledge graph** holds provenance-bearing claims in a
  `(TenantId, KnowledgeSpaceId)` space. Claims are born `Proposed`; trust moves
  through `Verified`, `Disputed`, and `Retracted` by append-only transitions,
  promotion to `Verified` passes the checkpoint grant, and every backend passes
  one conformance suite unchanged. Concurrent specialist appends keep goal,
  task, run, and delegation provenance under stable idempotency.
- **Retention** discharges a terminal run's session rows and snapshots after a
  per-tenant policy, snapshots first; agent-private memory outlives every run.
  Guardrails evaluate memory ingress, and a deployment attests that the chain
  its retrieval bundle evaluates is the chain its dispatch authority declares.

## The A2A surface

With the `agents` feature, `rakka-a2a` serves the entities as A2A agents: the
public task id is the `AgentTaskId` verbatim, ingress is deduplicated by the
owning entity before it is acknowledged, and task state is projected
row-for-row from the authoritative task and run condition. Two versioned
extensions carry what plain A2A cannot: agent management (settings updates,
suspension, resumption, termination, and description) and collaboration
(delegation, handoff, team claims, and conversation turns), each identified by
a versioned URI with the envelope schema inside it. Typed result submission is
plain ingress — a message carrying the `io.rakka.agent.result` metadata key —
and admission has no wire operation at all: it is an entity command an
authorized evaluator issues, deliberately outside the A2A surface. Reads —
task state, task-event replay, coordination-event replay across task, team,
and conversation logs, and the bounded goal view — answer from durable state
with explicit cursors and an explicit `WindowExpired` when a retention window
was outgrown, and each carries its own authorization operation class. The
`io.rakka.*` metadata keys, the extension echo keys, and the refusal-code
families are public commitments, registered in
[`rakka-compatibility.md`](rakka-compatibility.md).

## Observability

The loop, the entities, and the dispatcher close bounded **segments** for the
operations a run performs, always compiled and feature-free; under `otel` they
map to the pinned GenAI semantic-convention revision with a status, a stable
`error.type`, and the attributes the tail-sampling retention classes select
on. Metrics are catalogued as data with units, buckets, and bounded label keys,
and a source scan holds the catalogue to the call sites. Decision events are
durable records with a bounded outbox and a sink that may lag but never blocks.
The SDK, exporter, and Collector live at the application binary;
`examples/agent-otlp-export-acceptance` is that binary, and
`docs/plans/rakka-agent/kubernetes-agent-otel-collector-topology.yaml` is the
Collector topology it exports to. Telemetry is never a correctness input.

## Security

The security matrix records the posture clause by clause. The rules that hold
by construction: no resolved credential or secret is persisted anywhere
(durable state, effects, memory, runtime events, telemetry, logs, metrics,
snapshots); setup and settings revisions can only narrow a definition; a tool
call is undispatchable until every gate passes; retrieved memory is untrusted
input to the guardrail chain and can never widen a capability; failure text an
executor or resolver returns is bounded before it is persisted, and never a
credential; and workload isolation is routed at the claim by execution class.
Authentication, tenant resolution, and what an execution class isolates are
the deployment's, and are named as such rather than assumed.

## Recovery

An owner loss at any durable write is the ordinary case, not the exception.
In-process suites kill the owner at every write of every exchange and rebuild
the entity from the store; the multi-pod harness kills a real process whose
store is a directory outside it, downs the departed pod, moves the shards, and
finishes from the record — including after an external system committed and
before it returned a receipt. Which scenario is proven where is the roster's
table; which boundary is swept at which fidelity is the fault-injection
matrix's.

## Compatibility

Every persisted record carries a schema version and is read under an N/N+1
window that fails closed in both directions; the exchange codec, the A2A
extensions, and the A2A protocol version are pinned; the Rig, A2A SDK,
OpenTelemetry SDK, GenAI convention, and Collector pins are recorded in one
table a test holds to the manifests. See the Agent Domain section of
[`rakka-compatibility.md`](rakka-compatibility.md) and the agent items of
[`rakka-v1-rolling-update-upgrade.md`](rakka-v1-rolling-update-upgrade.md).

## Examples

The five acceptance walks (durable agent, continuous goal, multi-agent goal,
coordination capability, OTLP export) are in-process and deterministic, print a
numbered transcript, and have a test that asserts that transcript verbatim. The
other three differ: the minimal kernel example prints six fixed lines whose
typed facts `rakka-agent-workflow`'s `minimal_local_workflow` test holds (the
lines themselves are documented in its README, not asserted); the multi-pod
soak's counts vary run to run, and its gate is the compatibility matrix's
gated exit-status check; the clustered A2A example is a long-running server
with fixtures only.

| Example | Demonstrates |
| --- | --- |
| `examples/minimal-local-agent-workflow` | The smallest durable command boundary of the kernel: a `StartRun` accepted into a durable inbox, recovered, executed once. |
| `examples/durable-agent-acceptance` | The M1 statement: one sharded agent from ingress through checkpoints, dispatcher and owner loss, reconciliation, and a terminal result. |
| `examples/continuous-goal-acceptance` | The M3 statement: one continuous goal through wakes, coalescing, downtime, backoff, fencing, renewal, and retirement. |
| `examples/multi-agent-goal-acceptance` | The M4 statement: a root goal delegating to specialists, invoking a workflow tool, surviving root and child loss, and reaching `Satisfied` through its evaluator. |
| `examples/coordination-capability-acceptance` | The M5 statement: a board claim, a handoff, a human-owned upstream, a moderated conversation, and cursor replay across pod loss. |
| `examples/multi-pod-agent-fault-soak` | Two real processes, a shared durable directory, and a sweep that aborts a pod at every durable write. Gated. |
| `examples/agent-otlp-export-acceptance` | A real OpenTelemetry SDK exporting one real run over OTLP to an in-process receiver, asserted on the decoded protobuf. |
| `examples/clustered-sharded-entity-a2a-agents` | The A2A adapter over sharded runs with file or etcd discovery and Kubernetes manifests. |

## What is owed

Every matrix ends with an "Owed" section, and the roster says what it does not
claim. The open items as of this document are recorded there rather than here:
the guardrail boundaries with no evaluation point and the knowledge graph's
absent retention in the security matrix; the segment classes with no
production call site and the untested two-replica tail-sampling gateway in the
telemetry matrix; the coordination workload across pods, the PostgreSQL arm
for the shared substrate, and detected rather than announced departure in the
fault-injection matrix.
