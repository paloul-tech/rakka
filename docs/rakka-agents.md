# Rakka Agents

Status: current through gap slice 3 (post-Phase-6) of
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
| `rakka-agent-knowledge-graph` | The communal knowledge graph: provenance-bearing claims, the trust lattice, the promotion gate, the portable store SPI with optional keyword search, the adapters that serve `rakka-agent`'s communal read ports, an in-memory reference store, and the backend conformance harness. | — |
| `rakka-agent-knowledge-graph-postgres` | The graph's relational backend, with an indexed keyword search and the pass that indexes earlier claims. | — |
| `rakka-agent-mcp` | The MCP client adapter: server bindings with operator-declared tool policies, publish-time descriptor sync, a tool listing for choosing tools before any binding names them, the dispatch executor over an injected transport client with a required egress check, and the child-process launcher seam. | `agent-mcp` |
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
an exchange either: it is an A2A outbox effect claimed by the dispatcher, never
a courier re-drive, and only its result comes back as an exchange. A delegation
send's exhaustion becomes a fan-in disposition, and a handoff send the task
definitively refused fails its call while the run survives, as does one whose
only attempt provably sent nothing (a definitive refusal, or a credential that
could not be resolved). Any other handoff send that exhausts its budget without
an answer parks the run behind a reconciliation checkpoint until the task's
handoff result settles it or a decision does. An exchange is
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
   the `rig` feature — `RigModelAdapter` over any Rig completion model, and
   `RigProviderAdapter` over a model profile and an injected HTTP backend
   (`rakka_agent::rig::ReqwestClient`, rig's own `reqwest::Client`, is the
   plain backend a deployment wires without taking a direct `reqwest`
   dependency), building the provider client per attempt from the credential
   the dispatcher resolved; a deterministic adapter in the testkit) returns a
   bounded turn: text, tool-call *requests*, or a typed result proposal. A
   provider is shown each tool under a name inside the providers' tool-name
   rule (`provider_tool_name`: `mcp.crm.lookup` is declared as
   `mcp_crm_lookup_c41f30c9`), and each call is read back to its tool id; the
   provider's name for a tool is never durable. Turns are
   recorded to session memory and dropped, so a run that iterates a hundred
   times persists no more state of its own than one that iterates once.
4. **Effects.** Each model call, tool call, A2A send, workflow start, and
   compensation is its own outbox ticket with a stable idempotency key. A
   dispatcher fleet claims tickets under a lease; workers may declare the
   execution classes they serve and skip the rest before taking a lease. A
   dispatcher write to a run's inbox that loses its compare-and-set to another
   writer is recovered and re-applied, a bounded number of times, inside the
   same pass. A tool call is dispatched only when its binding, dispatch
   grant, credential binding, checkpoint, execution policy, and immediate
   safety check all pass;
   credentials are resolved at dispatch and never outlive the attempt. Remote
   MCP tools are registered from stored descriptor sets synced ahead of time
   at publish, never discovered live inside a call, and are called per attempt
   through the MCP executor over an injected transport client — built with no
   proxy and no redirects, so the request reaches the URL the check judged —
   behind a required egress check; MCP is never an agent-to-agent channel, and
   a server that identifies itself as one is refused. A result too large to
   stay inline is written through an artifact sink the deployment implements
   (`McpArtifactSink`), handed the run it belongs to. A sync refuses a listing
   whose server text echoes the credential it sent, and runs under a time
   bound (`MCP_SYNC_TIMEOUT_DEFAULT_MS`, or the caller's own).
   A call whose request was sent and whose answer was lost has an unknown
   outcome and is settled by its safety class — a non-idempotent tool parks
   for reconciliation, recording how far the call got, when it was handed
   over, and whether a cancellation was issued — and a refusal the
   deployment declares definitive (a non-retryable egress refusal, an
   artifact store's refusal) is not retried. A call the client never handed
   over never left, and fails like any other failure before the send.
   It settles `Exhausted`, as a spent budget does, and its failure reason is
   marked definitive; a run that fails on an effect records how many attempts
   it made and was allowed.
   An outcome settles only the effect it belongs to: the success variant of
   the effect's own request, naming the effect's own call, cell or
   generation, or a failure outcome (`Failed`, `Exhausted`, `Indeterminate`
   or `Cancelled` from a dispatcher; `Failed` or `Exhausted` from a person's
   confirmation). A tool result carries the effect's own call id, and a
   compensation's result the id `compensation_call_id` derives for it. Any
   other outcome is refused `effect-outcome-mismatch` before anything
   changes, on every run; a stale or duplicate delivery still answers as
   stale first. A confirmation carrying `Indeterminate` or `Cancelled`, and
   a confirmed exhaustion of a handoff send, still answer
   `effect-policy-invalid`. The dispatcher records a tool's result under the
   call id the model asked for, even when the dispatch authority transformed
   the call.
   When a deployment's reconciler answers `Executed` with an outcome that
   does not fit the effect, the dispatcher handles the finding as `Unknown`:
   on a live run it burns one attempt and asks again on a later claim, and a
   spent budget parks the generation; a winding-down run parks it at once.
   The failed attempt's line and the park carry `effect-outcome-mismatch`.
5. **Waiting.** A consequential effect parks the run `WaitingForApproval` on a
   durable checkpoint whose grant is bound to the exact intent digest; a
   changed argument invalidates it. A worker lost after a non-idempotent effect
   was `Started` parks one `Indeterminate` outcome and waits for an explicit,
   deduplicated reconciliation decision — never an automatic re-invocation.
   Parked runs consume no live task, thread, or timer. When the last
   ambiguous outcome is answered, a run that is still live and not winding
   down leaves `WaitingForReconciliation` for the status of what it rests on
   at that moment: the dispatch wait while a call whose outcome moves the
   run is outstanding (the model call, a tool call, an A2A send, a workflow
   start), and `Running` otherwise. A memory promotion, a claim append, a
   goal evaluation and a workflow cancel are no part of that status. An
   answer that fails the model call or a tool call winds the run down, as
   that failure always does. One that fails a workflow cancel does not: the
   failure stays on the cancel's effect record and the run goes on (issue
   #197). `Compensate` winds the run down behind the compensation, and
   `Escalate` keeps the park. A record an
   earlier binary left parked with nothing to reconcile is repaired by the
   first settle pass that reaches it. While an outcome is unanswered the
   loop takes no step. A rejection of the run's proposal, or the deadline of
   a fan-in wait, that reaches a parked run that is not winding down is
   taken, and the run goes on from it when the park is answered: it prepares
   the turn the rejection began, or records the turn the wait closed once
   every call of that turn is back. A child
   the deadline timed out is sent its cancellation without waiting for that
   answer. On a parked run that is winding down, the rejection begins no
   turn and the deadline resumes nothing. A turn does not rest while one of
   its own calls has an unknown outcome: the result of another of its calls,
   or a wait resolved meanwhile, leaves it open at `AwaitingTools`. It rests
   when the answer says what the call returned, or, where the answer sends
   the call again, when that call returns, so a turn is recorded with the
   result of every call it made (issue #189). A goal evaluation or a
   workflow cancel whose outcome is unknown holds a turn the same way, and
   while the turn waits for one sent again the run reads `Running`. A gate that closes without a grant
   (denied or expired) fails its effect. For a tool call that winds the run
   down. Where the run survives the failure, as it does for a goal
   evaluation and for the send or start of a fan-out member, a run that
   was on that gate's `WaitingForApproval` or `WaitingForAuthorization`,
   with no other gate of that kind standing, has its dispatch wait
   recomputed: the wait of a gate that still stands, or
   `WaitingForEffect`.
   A call the run committed and has not sent is cancelled in place when the
   run stops dispatching: when a call fails for good, on a `Cancel`, when a
   compensation is scheduled, when a handoff is accepted, and when the run
   ends. Four kinds are exempt and are still sent: a compensation, a
   workflow cancel, a memory promotion and a claim append. An approval or
   authorization a cancelled call waited behind is closed in the same
   transition, so a decision that arrives after is refused
   `checkpoint-already-resolved`, or `run-terminal` once the run has ended.
   A goal evaluation still unsent when a run ends is cancelled the same way
   and its gate closed. An ended run therefore holds no waiting approval or
   authorization but that of a memory promotion or a claim append. A record
   an earlier binary left holding such a gate has it closed by the first
   settle pass that reaches it; until then a decision for it is refused the
   same way and the timer leaves it alone.
6. **Budgets.** Every run holds an escrow ledger: allocations flow down from a
   parent scope in the parent's own transition, settlements and returns flow
   back up through the exchange path, and a `Started` attempt that becomes
   `Indeterminate` still consumes the attempt budget it reserved. A call
   cancelled before it was sent consumes none, and what it reserved is
   released in the transition that cancels it; a compensation is reserved
   after the calls its decision cancels are released, so it can spend what
   they held.
7. **Terminal.** A typed result passes the task's result rules before the task
   completes; a malformed or rejected one records a rejection decision and
   costs bounded iterations. The terminal transition stamps the run's
   `terminal_at`, the clock retention is measured from.

Every step above is a durable write, and each is a boundary the fault-injection
suites kill the owner at; the [fault-injection
matrix](rakka-agent-fault-injection-matrix.md) records, per boundary, which
sweeps are demonstrated and which are still inferred.

### The dispatcher fleet record

The fleet keeps its index in one durable record per database. Every
registration, claim and settle is a compare-and-set of the whole record, and
every worker reads it. Three rules hold at every site that writes it or a
run's outbox (issue #145):

1. **A fleet entry is finished only after its outbox row is finished or
   absent.** The dispatcher settles a ticket at the run's outbox first and at
   the fleet second. The workflow-tier worker's cancellation fences a run's
   unfinished entries, settles their rows, and only then finishes them.
2. **A registration's list of due tickets is never older than the fleet
   record it writes against.** The fleet record is read first and the run's
   outbox after it.
3. **No settle rewrites a finished outbox row.** A settle that finds its row
   already `Dispatched`, `Exhausted` or `Cancelled` writes nothing, and the
   worker's fleet entry follows the row. A worker that still holds the claim
   and finds its row `Exhausted` tells the run so; a worker whose lease
   lapsed tells it nothing. The workflow-tier worker never dispatches a row
   that is finished or gone.

A settle that loses its compare-and-set to another writer reads the record
again and re-applies, up to three more times, under the claim's own fence:
this worker, this fencing token, a lease still active.

**A write that loses its compare-and-set** (issues #159 and #160). Every
worker writes the one record, so a write made from one read of it can find
that another worker's write landed first. Five writes are then read again
and made again, each at most three more times: a registration, a claim, a
settle, a release and a prune. The loss after that is returned as
`revision-conflict`, and a pass that meets it fails.

- **Each attempt is made whole from the record it read.** A claim made again
  takes what is due, the class and target limits, its batch and its fencing
  tokens from that record, at that instant. What a lost attempt claimed is
  dropped: its write did not land, so it leased nothing.
- **A registration is made again by its caller, not by the fleet.** The list
  of due tickets it registers has to be no older than the record it is
  written against (rule 2 above), so the record is read again and then the
  list. `AgentRunEffectDispatcher` and `AgentDispatcherWorker::refresh_run`
  do this. A deployment that calls
  `AgentDispatcherFleet::register_due_effects` itself gets one try, and
  `AgentDispatcherFleet::registration_lost` tells it whether to go again.
- **`AgentDispatchPass::registered` is what the write that landed changed.**
  A ticket another worker registered in between counts for nothing.
- **A re-application waits a short random time first,** so that writers
  that lost to the same write do not read again together: between 5 and
  10 ms before the first, 10 and 20 ms before the second, 20 and 40 ms
  before the third. One write asks for under 70 ms in all.
- **Only a settle's first re-application follows at once.** A settle is
  written from the copy its claim left, which is as old as the call it
  settles, so its first loss says the copy was old. Every other write
  follows a read, so its first loss is already a write that got in between.
  A settle asks for under 60 ms in all, while its claim's lease runs.
- **The wait is never on the fleet's clock.** A fleet handed a logical clock
  that moves only when it is told to still waits and goes on. By default a
  thread sleeps and wakes the task, which needs no async runtime and no
  timer. `AgentRunEffectDispatcher::with_fleet_write_pause` and
  `AgentDispatcherFleet::set_write_pause` replace it
  (`AgentFleetWritePause::with`): with a runtime's `sleep`, with a wait in a
  simulated time, or with one that ends at once, which turns the waiting
  off.
- **A wait does not shorten the time a write can be lost in.** A write is
  lost when another lands between its read and its compare-and-set, and
  that time grows with the record. Pruning is what shortens it.

**A batch and its leases** (issue #157). A claim pass leases every claim of
its batch at one instant, and a worker runs the claims one after another.
Three rules keep a claim from being taken by another worker while its pass
is still working on it, and from being run by two:

- **Every settle renews what the pass still holds.** In the same write, each
  claim the pass has taken and not started is leased for a full lease from
  that settle. A renewal costs no write of its own, and the lease has to
  cover the longest single call, not the sum of a batch. The claim being run
  is renewed by nothing: a call that outlives the lease loses its claim, and
  the claims held behind it lapse with it. An entry whose run's cancellation
  was requested is not renewed either.
- **A claim whose lease lapsed is not started.** The pass passes it over,
  counts it in `AgentDispatchPass::lapsed` and writes nothing. Any worker
  can claim it.
- **A claim makes a later attempt only under a lease that covers it**
  (issue #171). One claim makes more than one attempt only for a
  reconcileable effect whose attempt ends unknown and is found not
  executed. Each attempt after the first is made only if the lease is still
  active for the length of an attempt: the effect's `timeout_ms`, or the
  instant itself when it declares none. That attempt's `deadline_at` is
  counted from the instant the claim asked, so it falls inside the lease,
  and the `timeout_ms` its executor is handed is what is left of the bound
  when the attempt starts.
  Otherwise the claim ends there, writes nothing and is counted in
  `AgentDispatchPass::lapsed`. Its batch ends with it: nothing renewed the
  claims behind it, so the pass gives back each whose lease still stands,
  claimable at once. Once
  the lease has lapsed, the next claim asks the reconciler about the
  attempt that one left and goes on from its answer, as for an attempt
  whose worker was lost. The lease has to cover one attempt, not a budget.
  An attempt that spends the budget has none after it, so its claim is not
  asked and settles it as before; a claim's first attempt is not asked
  either.

A renewed batch stays with its worker. A ticket claimed behind long calls
waits for every one of them, and no other worker can take it meanwhile,
where one lease used to bound that wait. The batch size bounds it now.

**A pass that fails gives back what it did not start.** The first error a
claim meets ends the pass. Before the pass returns that error, one write
puts the claims it had not started back to `Pending` with no lease. Another
pass can claim them at once, and they stop counting against their class's
concurrency limit. The claim that failed keeps its lease, which is its
backoff: a ticket that fails every pass would otherwise be first in every
batch. That lease can be a full one, counted from the settle before the
failure. A killed worker gives nothing back, and neither does a pass whose
release fails, which is logged. Those claims wait out their lease.

A renewal and a release keep the settle's fence, and each reads the record
again when it loses its compare-and-set. A deployment that drives a fleet
handle itself has the two calls, `AgentDispatcherFleet::hold_claims` and
`AgentDispatcherFleet::release_claims`; `AgentDispatcherWorker::run_once`
uses both. A hold is spent by the settle that lands, so a worker names the
claims it still holds before each one it runs, and a claim nobody names
again is leased for at most one more lease. The `AgentDispatchClaim` a
worker was handed keeps the expiry of its claim pass; the entry holds the
lease as it stands. `AgentDispatcherFleet::is_current_claim_for` asks
whether a claim's lease is still active a given time from now, from the
handle's own copy of the record and its clock.

**The concurrency limit counts leases.** An entry counts against its class
and its target from its claim until it is settled or its lease lapses,
because the settle is what tells the record a call has ended. A batch's
claimed and unstarted tickets count too, and the batch size is the lever.
`AgentDispatchPass::concurrency_limited` is how many due tickets a claim
left at a limit before its batch filled. The claim stops counting once the
batch is full, so zero is not evidence that no limit binds.

**Pruning.** Nothing else removes an entry, so without a prune the record
holds one entry for every effect a deployment ever dispatched. An application
calls it on its own schedule:

| Call | From |
| --- | --- |
| `AgentRunEffectDispatcher::prune_fleet(before)` | A dispatch worker |
| `AgentDispatcherFleet::prune_terminal(before)` | An operator command that holds only the fleet store |

- It removes the entries that are `Completed`, `Exhausted` or `Cancelled`
  and finished before `before`. A pending, claimed or retry-scheduled entry
  is never touched, nor is a claimed entry whose lease has lapsed.
- Any horizon is safe while workers run, including one in the future: no
  effect is run again because its entry was pruned. In the workflow tier
  the three rules above mean a finished effect is never registered again.
- A prune that finds nothing writes nothing. One that loses its write reads
  again and re-applies.
- **What goes with an entry is its `last_error_code`**, the only durable copy
  of a cancelled ticket's settle reason. The horizon is how long that stays
  readable.

**Before the first prune** on an existing deployment: every node runs a build
with this change; a deployment that cancels through `AgentDispatcherWorker`
refreshes each run that holds a `Cancelled` fleet entry once on that build;
and a deployment that must prune during a roll keeps the horizon at an hour
or more. [`rakka-compatibility.md`](rakka-compatibility.md) says why.

**Watching it.** A dispatcher built `with_metrics` records its fleet's
series too. `rakka.agent_workflow.dispatcher.entries` is the record's entry
count by status; the finished statuses are what grows when nothing prunes.

Each lost compare-and-set of those five writes is counted once on
`rakka.agent_workflow.dispatcher.fleet`, under the `operation` that lost it
(`register`, `claim`, `settle`, `release` or `prune`):

| `outcome` | `detail` | The lost write |
|---|---|---|
| `reapplied` | `1`, `2` or `3` | was read again; `detail` is which re-application of that write it led to |
| `revision-conflict` | `none` | was returned under that code, the three re-applications spent |

`reapplied` with `detail=1` is how many writes were read again at all, and
`detail=3` how many came within one loss of running out. A registration
made through `register_due_effects` alone is counted only if its caller
asks `registration_lost`.

`revision-conflict` under `settle` is the one that costs something. The
ticket's outbox row is finished and its fleet entry is not: the entry stays
`Claimed`, holding its slot, until its lease lapses and it is claimed again.
What that re-claim finds depends on how the call ended:

- **A result that was delivered.** The run already has it. The re-claim
  invokes nothing and ends the entry `Cancelled`.
- **A call that spent its attempts.** The row is `Exhausted` and the run has
  not been told: on that path the fleet is settled before the run. The
  re-claim ends the entry `Exhausted` and tells the run then, a lease late.
- **A failure with attempts left.** The row is scheduled to be tried again,
  and the re-claim is that next attempt.

**A ticket whose outbox row is gone.** A fleet entry outlives its row when a
run's workflow record is purged, or when one store is restored without the
other. `AgentRunEffectDispatcher` writes nothing to the missing row: the
entry is finished as `Cancelled` with `last_error_code` `ticket-row-missing`,
and the pass goes on with its batch. A run that still holds the effect
unresolved is parked on it as `Indeterminate` under the same code, behind the
usual reconciliation checkpoint, whatever the effect's safety class. The row
was the only record of whether an attempt started, so that effect is not
invoked again until someone decides.

A lost row gets that answer however it is reached. A run puts a missing row
back whenever it settles: every command it handles and every settle pass
sends its ready effects to the outbox again, as new rows with no attempt
history, and the park is itself such a command. The fleet entry records
which row it was registered from (`AgentDispatchEntry::row_scheduled_at`),
so a claim that finds another row in its place parks the effect in the same
way and finishes the row the run put back; when one run lost several rows
together, every one of them is parked. A winding-down run that is pumped is
fenced before anything is claimed, and the fence reads the fleet record for
a ready effect that has no row: a ticket with a fleet entry once had a row,
so it is left to the claim, or parked by the fence when no claim will come,
and only a ticket the fleet never indexed is cancelled as a flush that never
landed.

Two things are not told apart. A ticket with no fleet entry, never
registered or finished and pruned, has no witness: the row the run puts back
runs as a first attempt. And an entry registered by a build that recorded no
row stamp behaves as it did before, until it finishes.
[`rakka-compatibility.md`](rakka-compatibility.md) has the cases.

**A ticket that is finished while its run still waits.** A worker that spends
a ticket's budget writes the outbox row `Exhausted`, then finishes the fleet
entry, then tells the run, and it can be lost between any two. The row then
holds what it knew: the attempt count and the last failure line. A claim that
finds a finished row under an effect the run still waits on starts no
attempt. It settles the fleet entry to follow the row and tells the run what
the row says: for an `Exhausted` row, the outcome and code its worker was
about to deliver, without the collaborator's failure reason, which no
durable record keeps. When the entry is finished too, no claim will come, and
the per-run half of a pass (`register_run`, `pump_run`) tells the run instead,
once the entry has been finished for a lease, so that it does not speak ahead
of a worker that is about to. It also parks a live run whose finished ticket
lost its row, which nothing else reaches. An ordinary pass reads nothing more
for this. The repair runs only in a pass: a deployment that pumps a run only
when something happens to it should also pump the runs that are waiting on an
effect. [`rakka-compatibility.md`](rakka-compatibility.md) has the tables.

**What is not bounded.** The record is still one record, written whole. A
prune bounds it to the live entries plus the finished ones inside the
horizon, which on a busy fleet can be megabytes. A write that is made again
is another read and another attempt to write all of it. A registration or a
claim that lost used to leave the record alone until its next pass; it now
reads and tries again within tens of milliseconds, so the same writes land
sooner and closer together. The two writes of a cancellation through
`AgentDispatcherWorker` are still one try each and are not counted.

`AgentRunEffectDispatcher` checks a claim's lease when its pass reaches the
claim, and not again before it marks the ticket's row `Dispatching`. A
finished row is protected at both ends: the mark and a settle each leave a
row that is `Dispatched`, `Exhausted` or `Cancelled` as it is. What is left
concerns a row that is not finished:

- A worker that stalls past its lease between that check and its durable
  `Started` can still start an attempt on a ticket another worker has since
  claimed and is running.
- Nothing renews a lease during a call. A call that outlives its lease loses
  its claim, and the next claim recovers the ticket as an attempt that
  disappeared, by its safety class, while that call may still be out. The
  lease has to be longer than the longest call. A claim's later attempts
  are held to that (see "A batch and its leases"); its first is not, and
  neither is an attempt of an effect that declares no `timeout_ms`, which
  is asked only whether the lease has already lapsed.
- A worker whose lease lapsed during its call still writes that call's
  result to a row that is not finished. Its failure burns an attempt, and
  can exhaust the row, under a ticket another worker is running; its success
  finishes the row under that worker. Only then is it answered
  `claim-fenced`.

## Goals: finite and continuous

A goal has no entity of its own. The root task carries the goal contract —
owner, objective, success criteria, budgets, allowed references, evaluator,
escalation — and every goal transition commits in the same compare-and-set as
the task transition that decided it. A finite goal becomes `Satisfied` only
through the configured evaluator's assessment of the current criteria revision
against durable evidence; an agent may propose completion, and with an
evaluator configured that proposal has one door. An evaluation the
deployment's spec gates opens its gate in the `EvaluateGoal` command's own
transition: a `SecurityAuthorization` when the spec requires authorization,
with or without a checkpoint, else an `Approval`. A human review always opens
one, and takes its verdict from that gate's grant. The command moves neither
the run's status nor its phase.

A continuous goal executes as bounded **epochs**. The wake controller derives
one `AgentWakeId` per logical occurrence — from a schedule, an event, a
callback, or an A2A trigger — and admits at most one child epoch task and run
for it; concurrent triggers coalesce while one epoch owns execution, a downtime
backlog admits one coalesced representative, an obsolete schedule revision is
fenced, and a pod, actor, or dispatcher restart never creates a wake by
itself. Failure backs the goal off through durable re-wakes and escalates into
suspension; an exhausted goal window defers to the window boundary. An
occurrence whose epoch the goal's own ledger cannot pay for is deferred too,
and no epoch is created for it: it is admitted when an epoch still out returns
the budget, and where no epoch holds any, the goal's exhaustion policy decides. Between
every wake, admission, epoch, and result the controller, task, and run may
passivate. Three things reactivate them, and nothing else: the wake scanner
over the durable timer index for a scheduled occurrence, an `AdmitWake` command
at the task for an event, callback, command, or A2A-trigger occurrence, and the
courier re-driving an owed exchange at its receiver — a restart by itself never
does.

An operator asking what a goal or run is doing gets an **authoritative
operational snapshot** from the durable record, with the revision it read,
correct while every telemetry path is down. Each pending effect names its
target (`tool:{tool id}` for a tool call), when it was handed to the sink, and
its last error code and reason; each open checkpoint names the effect it gates,
the decisions it allows, and its summary. A run snapshot's `next_wake` is the
earliest deadline the run's checkpoint timer will act on
(`AgentCheckpoint::next_timer_deadline`). An expiry counts, except on a
reconciliation checkpoint, which never expires. An SLA due time counts only
while the checkpoint is `Open` and names an escalation target, so a due time an
escalation has spent is not reported. A person can escalate only a
reconciliation checkpoint, which carries no expiry, so after a person's
`Escalate` that checkpoint adds nothing to `next_wake`. On an ended run a
checkpoint not bound to a memory promotion or claim append adds nothing either,
because the timer leaves it alone. A run's end closes every other approval and
authorization, so what is left is a reconciliation checkpoint parked on a call
of another kind: a run whose result is accepted while a goal evaluation's
outcome is unknown ends holding one, and no decision answers it. A deadline that has passed but that no timer
has fired yet is still reported. The checkpoint views keep `due_at` and
`expires_at` as the record holds them. The session view by `AgentRunId`
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
  reconciliation, and the root finalizes only after its ledger closed. A
  fan-in that resolves early chases the members it left behind with a
  cancel. A straggler's workflow cancel that fails, because no executor is
  wired, the executor refuses, its retries are spent or a person abandons
  it, leaves the live run going: the child is left to report on its own,
  and the goal view shows the cancel's status and error code beside the
  invocation (`cancel_status`, `cancel_error_code`).
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
  with idempotent operation ids, and isolated by both agent and run. A
  tool-result entry names the tool that produced it and the effect whose
  outcome it records — provenance, never authority — so claims and
  promotions can be derived from durable session memory by tool.
- **Context snapshots** are immutable and persisted before every model effect;
  a retry reuses the snapshot, so drift in a store or an index cannot change a
  retried model input.
- **Agent-private long-term memory** is scoped `(TenantId, AgentId)`,
  promoted from session memory through a checkpoint-gated executor — by
  sequence window and, optionally, by role — and ranked for retrieval by a
  retriever (pgvector in `rakka-agent-postgres`). A promotion or a communal
  claim append is not new work but a copy of work already recorded: neither
  is fenced by a wind-down, neither is among the effects a turn waits on —
  one committed beside a turn's in-flight tool never holds the turn open,
  and its outcome never rests one — a failure of either stays on the effect
  record rather than ending the run, and a run that has ended still accepts
  both for a deployment-configured window after its terminal stamp, its
  effects riding the ordinary outbox the application keeps pumping. A
  promotion or claim append the deployment's spec gates opens its gate in
  the command's own transition, on a live run and on an ended one: a
  `SecurityAuthorization` when the spec requires authorization, else an
  `Approval`. The gate is not the turn's wait. It moves neither the run's
  status nor its phase, it is listed among the snapshot's open checkpoints,
  and a tool-calling turn never opens it. A grant dispatches the effect, and
  a denial or an expiry fails that generation only. A `Cancel` closes the
  gate and, in the same transition, settles the effect `Cancelled` if it is
  still `Pending` and ungranted, releasing its attempt reservation; a
  granted one still dispatches. A wind-down that is not a cancel, such as
  one after a failed effect, leaves the gate open, and the open gate keeps
  the run from ending until it is answered, it expires, or a `Cancel`
  arrives. A run already at its bound on open
  checkpoints refuses the command whole with `effect-pending-overflow`. If a
  promotion's or claim append's attempt may have run and its recovery retry
  is refused (the agent was terminated just after the run ended), the
  generation parks `Indeterminate` behind a reconciliation checkpoint, as a
  live run's does.
  An ended run's checkpoint lists `confirmed-completed`,
  `confirmed-not-executed`, `escalate` and `abandon-and-fail`, never
  `compensate`: one opened after the run ended is opened without it, and the
  point query drops it from one opened while the run was live, whose durable
  record keeps it. A `Compensate` decision on an ended run is refused
  `run-terminal`. Either `ResolveCheckpoint` or
  `ResolveIndeterminateEffect` answers it, at any time after the run ended;
  the window bounds new commands, not the answer. The terminal status,
  reason and stamp never move. `ConfirmedCompleted` records the outcome it
  carries, which must be the effect's own receipt, a `Failed` or an
  `Exhausted`; any other outcome, such as a model turn, is refused
  `effect-outcome-mismatch`, as on a live run, because it would move the
  ended run's loop. A confirmation carrying `Indeterminate` or `Cancelled`
  still answers `effect-policy-invalid`. `AbandonAndFail` fails that
  generation only. `ConfirmedNotExecuted` is a person's assertion that the attempt never ran, not a probe: it re-sends
  under a new idempotency key, so if the first attempt did land, a re-sent
  claim append appends a second claim and a re-sent consolidation is refused
  on a stale revision. A person who does not know chooses `AbandonAndFail`:
  `ConfirmedCompleted` must carry the effect's receipt (the promoted
  memories' ids and revisions, or the claim's id), which only someone who
  knows what happened has. For a plain promotion, one without a `target`,
  `ConfirmedNotExecuted` is safe too: the memory's identity comes from its
  session entry, not the generation, so a re-send after an attempt that
  landed converges on the memory it wrote. A re-send the authority still
  refuses fails definitively; after memory retention has purged the run's
  session rows, a re-sent promotion settles without parking (`Exhausted`,
  code `memory-promotion-source-missing`, once its attempts run out). A timer
  escalates the checkpoint but never expires it, and the settle pass opens
  the checkpoint for a park that has none. A gate on an ended run's
  promotion or claim append, whether it was open when the run ended or a
  command opened it after, is answered and timed as on a live run. The run's
  status does not move. The decision events all of this records follow the
  run's end event (see Observability below). A run does not end while it
  holds any other call it sent. When its end is decided for it (its result
  accepted, its proposal refused or its rejections spent, a budget spent
  that cannot be topped up) it records that reason and waits: `Cancelling`,
  or `WaitingForReconciliation` while one of those calls has an unknown
  outcome. It takes their results and answers their parks as any live run
  does, and ends under the recorded reason when the last is known, so an
  accepted result still ends `Completed` (issues #123, #172 and #173). Only
  a record an earlier build ended can hold such a call. It stays as it was,
  and every other resolution on an ended run is still refused
  `run-terminal`. A
  model, compensation or workflow-cancel spec may not require a checkpoint
  or an authorization: the run opens no gate when it records any of the
  three, so the spec is refused where it is set (`effect-policy-invalid`,
  issues #128 and #176). The retriever supplies a ranking and nothing else:
  every ranked identity is resolved through the authoritative store before it
  is admitted, so a foreign or forged record is dropped and counted, and an
  unauthorized read reveals nothing about existence.
- **The communal knowledge graph** holds provenance-bearing claims in a
  `(TenantId, KnowledgeSpaceId)` space. Claims are born `Proposed`; trust moves
  through `Verified`, `Disputed`, and `Retracted` by append-only transitions,
  promotion to `Verified` passes the checkpoint grant, and every backend passes
  one conformance suite unchanged. Concurrent specialist appends keep goal,
  task, run, and delegation provenance under stable idempotency.
- **Reading the graph into a model context** is the communal read path
  (issue #69). A run reads nothing from the graph until its memory is wired
  with a bundle: `AgentRunMemory::with_communal_retrieval(AgentCommunalRetrieval::new(retriever, authority, grants, guardrails, policy))`.
  An unwired run behaves, and writes its snapshots, exactly as before.
  - **What happens on a turn.** After the session window and the private
    selection are assembled, the same assembly derives a query from the recent
    session text, reads the run's read grant, and searches each granted
    knowledge space, in ascending id order, up to the policy's `max_spaces`
    (4 by default, 16 at most). The rankings are merged best first. Each
    ranked identity is then resolved through the authority, in the space whose
    ranking named it, and the record the authority answers is re-checked,
    evaluated at the memory-ingress guardrail boundary, and embedded in the
    snapshot's `communal_claims`: up to 8 claims and 64 KiB of statements and
    evidence references by default, 16 claims at most. A claim whose record is
    outside a claim's bounds, an evidence reference over 2 KiB among them, is
    not read. The selection is a prefix of the merged
    ranking: the walk stops at the first claim that does not fit, and does not
    skip it for a smaller one.
  - **A ranking is not authority.** A retriever answers identities and scores
    and nothing else. Trust state, classification, confidence and required
    capabilities are decided on the authoritative record at the moment of the
    turn, so a claim retracted, tombstoned, deleted or reclassified since it
    was indexed is not read. A claim of another space or tenant is simply
    absent, and is counted `unverified`.
  - **Three things bound what a run reads.**
    1. *The space.* A run searches only the knowledge spaces its definition
       grants (`knowledge_spaces` on the authority envelope), narrowed by its
       setup and by its delegation. A space outside that grant is never named
       to the retriever.
    2. *The claim's own requirements.* A claim may require read capabilities
       (`ClaimAccess::required_read_capabilities`). A run reads it only when
       its **read capabilities** hold every one. Read capabilities are a set
       of their own on the authority envelope
       (`AgentAuthorityEnvelope::read_capabilities`): they are not tool
       capabilities, and holding one authorizes no tool dispatch. A setup may
       only narrow the set, and an autonomous admission is refused when the
       envelope would hold one it was not admitted with
       (`widened-read-capability`). A delegation passes on what the catalog's
       target states (`AgentDelegationTarget::with_read_capability`). When the
       delegating run was itself delegated, that statement is intersected with
       what the run was delegated. At a root (a goal-bearing or plain task) it
       is passed on as it stands: it is not intersected with the root agent's
       own definition, so the catalog is what bounds a root's delegation, and
       a root can pass on a read capability it does not itself hold. A
       delegated run holds what its delegation states and no more: stating
       nothing delegates nothing. A handoff follows the same rule for every task: its
       target holds what the catalog states for it, intersected with the
       task's own statement where the task has one (a delegated task, or a
       goal specification that names knowledge spaces), and stating nothing
       passes on nothing. That holds for communal appends as for reads. When
       the target delegates, it passes on no more than it was passed; like a
       root, it can pass on what its own agent is not granted. When it hands
       the task off again, the next target holds the catalog's statement for
       it, not intersected with the earlier handoff's. A task with no
       envelope of its own used to be the exception: one that institutes no
       goal, carries no delegation, and whose definition declares no
       delegation ceilings, which includes a wake-epoch task and a task that
       only names a goal. Its handoff narrowed nothing, and the target kept
       its own agent's whole grant (issue #142). A catalog target such a task
       can be handed to must therefore state the knowledge spaces and read
       capabilities it needs (`with_knowledge_space`, `with_read_capability`).
    3. *The policy.* `CommunalRetrievalPolicy` names the trust states and
       classifications a deployment admits into a model context at all.
       **The default admits only `Verified`, `Unclassified` claims.** A
       `Proposed` claim is one agent's unreviewed assertion; admitting it is a
       decision a deployment makes with
       `CommunalRetrievalPolicy::verified_only().with_trust(...)`. No policy
       can admit a `Retracted` claim.
  - **Claims are untrusted input.** Each claim's statement is evaluated by the
    bundle's memory-ingress chain, as JSON `{"subject", "predicate",
    "object"}`, with the claim id and its space in the guardrail context
    (`AgentGuardrailContext::claim`, `claim_space`). A blocked claim, and one
    a stage would park on a checkpoint, is dropped. A stage may rewrite the
    subject, the predicate, or an inline value; a transform that changes a
    node object or an artifact reference, or that is not a valid statement,
    drops the claim. **A stage written for private memory does not protect
    claims until it reads this shape.** A private memory is evaluated as its
    bare content; a claim is evaluated as the statement object, with its text
    under `object.value.inline`. A stage that inspects a top-level string
    therefore allows every claim, and one that replaces the value with a bare
    string drops every claim. Branch on `context.claim` in each memory-ingress
    stage before wiring the bundle, and run the chain against a claim first:
    `rakka_agent::testkit::evaluate_claim_at_memory_ingress` evaluates one
    through the step assembly runs and answers what assembly would do with
    it, and `CommunalClaimFixture` builds a claim carrying text the chain
    should catch. That clause attests nothing; Rakka does not know whether it
    was run. The chain must be the one the dispatch
    authority declares: `AgentToolAuthority::with_memory_ingress` attests the private
    and the communal bundle together, and refuses a memory in which either
    carries a different chain (`guardrail-chain-mismatch`). Rakka selects claims and records
    them; rendering them into a prompt is the application's model adapter's
    job, and it must present them as untrusted data, with each claim's trust
    state and provenance, which the snapshot carries for that purpose.
  - **The graph's adapters.** `rakka-agent` declares three ports and cannot see
    the graph crate. `KnowledgeGraphCommunalRetriever` and
    `KnowledgeGraphCommunalAuthority` (in `rakka-agent-knowledge-graph`)
    implement two of them over any `KnowledgeGraphStore`; the retriever
    refuses a store that does not report the `keyword-search` capability.
    The retriever ranks by shared words, or, given an embedder and a store
    that reports `semantic-search`, by meaning (below).
    `AgentEntityCommunalReadGrants` implements the third from the run's own
    agent entity, and from its setup when given a setup resolver.
  - **Search is by shared words.** `KnowledgeGraphStore::search` is an
    optional store operation. A claim's words are those of its subject, its
    predicate, and its object (a node id, or the text of an inline value; a
    value held by artifact reference gives none). Words are runs of
    alphanumeric characters, lowercased. Nothing is normalized or segmented:
    a decomposed accent splits a word where the precomposed letter does not,
    text without spaces (Chinese, Japanese) is one word per run, and case
    folding is Unicode lowercasing only, so `STRASSE` does not match `Straße`.
    Such text is not found, and the search does not say so. A claim is a hit
    when it shares at least one word with the query, and its score is the
    share of the query's words it holds. Restrictions apply before ranking, so a claim the reader
    may not read takes no place in the ranking.
  - **Search by meaning is opt-in (issue #140).** A deployment that wants a
    claim found when the turn asks about it in other words gives the
    retriever an embedder: `KnowledgeGraphCommunalRetriever::with_embedder`,
    over a store that reports the `semantic-search` capability. The
    retriever then embeds each turn's query and the store ranks the space's
    claims by how near their stored embedding is to it. The same
    restrictions apply before ranking, every ranked identity is still
    resolved through the authority and re-checked, and context assembly and
    the snapshot's shape do not change. The retriever reports version 2
    instead of 1, which the snapshot records; `ranking()` says which of the
    two a retriever is, and the choice is made when it is built. Given a
    store that cannot rank by meaning, the embedder is dropped and the
    retriever ranks by words.
    - **What a store keeps.** One embedding per claim, beside the model,
      dimension count and pipeline version that produced it. Only vectors
      under the query's own reference are compared with it. A store never
      calls a model and never returns a vector. Storing an embedding changes
      no byte of the claim's record; `Claim.embedding` is the appender's own
      note and is not written. A tombstone and a delete remove the embedding
      in the operation that withdraws the content, and nothing can store one
      for a tombstoned claim afterwards.
    - **How a claim gets its embedding.** An append executor given an
      embedder (`KnowledgeGraphClaimAppendExecutor::with_embedder`) embeds
      each claim it appends, best effort: a failed embedding never fails the
      append. It embeds the claim as the store holds it after the append, so
      a retry of an append whose claim was retracted meanwhile sends nothing. `embed_claims` is the pass that embeds whatever is left: claims
      from before the deployment opted in, claims whose embedding failed at
      append, and, after a change of model or pipeline version, every claim.
      **No loop in Rakka calls the pass.** A deployment calls it for each
      space until it answers no cursor. A claim the model refuses is counted
      `failed` and does not stop the claims after it.
    - **What is sent to the embedder.** The embedder is the deployment's and
      may be a service outside it. Two kinds of text reach it. *Claims:* the
      subject, predicate and object text of each claim the executor or the
      pass embeds. Both take the set of classifications they may send, and
      **the set has no default**: a deployment states it. It should cover
      every classification the read policy admits, because a claim that is
      never embedded is never found by meaning, and a search that admits it
      reports `index_incomplete` for as long as it is not. A `Redacted` claim
      is never sent whatever the set says, and a `Retracted` or tombstoned
      claim is not embedded. *Queries:* the retriever sends each searching
      turn's query, up to 1,024 bytes derived from the run's recent session
      entries, with no classification of its own. The private tier's
      meaning-based retrieval already sends its query to its own embedder.
    - **Limits.** The score is cosine similarity on the private tier's
      scale: 10000 for the same direction, 5000 for unrelated, and a claim
      that scores 0 is not ranked. There is no relevance floor: the nearest
      admitted claims are answered however far they are. A query with no
      text ranks nothing and calls no model. A retriever remembers the
      vectors of its last 16 query texts, so a turn that searches several
      spaces embeds its query once. A query that cannot be embedded
      fails that space's search, which is counted `degraded`; the retriever
      never falls back to words. The embedding model is not recorded in the
      snapshot, only the retriever's version. No metric counts embeddings
      that failed at append, and nothing bounds the embedder call in time.
      An artifact-backed object is embedded from its subject and predicate
      alone.
    - **PostgreSQL.** `PostgresKnowledgeGraphStore::with_semantic_search`
      opts a store in: it then reports the capability, and its `migrate`
      adds four nullable columns to the claim table, a check and a tombstone
      trigger, and needs the `vector` extension (pgvector). A store that does
      not opt in never names the extension. The search is an exact scan of
      the claims of one tenant and space that pass the filters, so its cost
      is linear in those. **No approximate index is shipped, and the search
      statement cannot use one a deployment adds.** The vector column has no
      fixed dimension, so that a model change needs no schema change; pgvector
      indexes only a fixed-dimension expression, and the statement orders by
      the bare column. An approximate index also picks its candidates before
      a filter runs, so a selective filter loses results behind one. Ranking
      through one is not available in this release. The roll order is in
      [`rakka-compatibility.md`](rakka-compatibility.md).
  - **A fault never fails the turn.** A grant that cannot be read selects
    nothing; a space whose search fails costs that space's ranking and the
    others continue; an authority fault part-way keeps the claims already
    verified. Each is counted `degraded`. The counters are
    `rakka.agent.communal.retrievals` (`backend`, `outcome`) and
    `rakka.agent.communal.ingress.outcomes` (`outcome`); no label carries
    claim text, a claim id, a space id or the query.
  - **`index-incomplete`.** A backend that indexes its claims can hold claims
    its index does not cover. PostgreSQL does after an upgrade, and whenever a
    node on an earlier build appends: search does not find such a claim, and
    says so. The snapshot's communal record carries `index_incomplete`, and
    the outcome counter counts `index-incomplete`. The remedy is the
    deployment's: call `PostgresKnowledgeGraphStore::index_claims` for the
    space until it answers no cursor. No loop in Rakka calls it. A search by
    meaning reports the same thing for a claim it admits that has no
    embedding under the retriever's embedder, on every backend, and the
    remedy is `embed_claims`.
  - **What a snapshot keeps.** A snapshot is immutable and is reused on every
    retry of its turn. A claim that is tombstoned, deleted, retracted, or put
    out of the run's reach after it was selected stays in the snapshots that
    already hold it, until the run's snapshot purge. It cannot enter a later
    snapshot. Rakka keeps no index from a claim to the snapshots that hold it.
  - **The snapshot record.** A snapshot for whose turn a communal search ran
    is written at schema version 2 and carries `communal`, the record of the
    search: the query, the spaces searched, the policy in force, and the
    definition and setup revisions the grant was read under. Every other
    snapshot is still written at version 1 with the bytes it always had. The
    roll order is in [`rakka-compatibility.md`](rakka-compatibility.md).
- **Erasing from the graph** is three operations an application calls on the
  store (issue #68). No run effect, command, or tool reaches them.
  - `tombstone` withdraws one claim's content and returns a stub. The stub
    loses the statement (subject, predicate, object), the evidence, and the
    embedding. It keeps the claim's identity, provenance, classification,
    access set, trust status, statement fingerprint, and whole transition
    history. `get` still returns the stub; a query lists it only when the
    filter says `including_tombstoned()`; no subject, predicate, or object
    filter matches it; traversal never follows it. Its trust status is frozen.
  - `delete` removes one claim and its transition history. A delete may carry
    the transition count and tombstone state the caller read; if the claim has
    moved since, it answers `claim-delete-stale`, changes nothing, and can be
    tried again under the same operation id.
  - `KnowledgeSpaceRetentionSweep::discharge` ages claims out of one space
    under a `KnowledgeSpaceRetentionPolicy`: a window per trust status, counted
    from when the claim entered that status, a separate window for stubs, and
    a legal hold. **Nothing ages out by default.** One call examines a bounded
    batch and returns a cursor; call again until there is none. The
    application names the spaces it sweeps and runs the sweep on its own
    schedule. A legal hold stops the sweep; it does not block an explicit
    `tombstone` or `delete`.

  After a tombstone or a delete, a replay of any earlier operation of that
  claim answers `claim-operation-erased` and writes nothing, so a retried
  append cannot bring the claim back. The store keeps one small row, with no
  content, for each erased or deleted operation, and never removes it.

  **One operation id per claim.** Derive each tombstone's and each delete's
  operation id from the claim it addresses (`ClaimOperationId::derive_tombstone`,
  `derive_delete`). An id already spent on one claim answers
  `claim-operation-conflict` when it is sent for another, so an application
  that reuses one request id across a batch of deletes is told, rather than
  being answered "done" for claims that are still there.

  **A claim whose stored history is damaged** (issue #138). A store writes a
  claim and its transition history together. Rows edited by hand, or tables
  restored to different moments, can leave a claim whose stored transitions
  are not the ones its record counts: a row missing, a row too many, a row
  that is another claim's, or a row that cannot be read. On PostgreSQL a
  `tombstone` or a `delete` of such a claim answers `claim-history-damaged`,
  writes nothing and spends no operation id. It is not a lost race, and a
  retry answers the same. The refusal is deliberate: the stored result of a
  transition whose row is gone holds the whole claim as it stood, and no row
  names it, so erasing only what the rows name would leave a copy behind and
  report success.

  A store built `with_damaged_history_erasure()` erases such a claim, and
  clears those stored results too. To find them it reads every transition
  row of the tenant and space in the operation ledger, on each attempt to
  erase a damaged claim, which takes time in proportion to that ledger. Build
  one for the operator's action that answers a `claim-history-damaged`.

  The retention sweep counts such a claim in `damaged` where it meets the
  damage, leaves it, and goes on to the claims behind it. It deletes no claim
  whose history does not end where its record says, through either store. It
  is no scan for damage: a `damaged` of zero does not say a space holds no
  damaged claim. The in-memory store cannot be damaged this way.

  **Erasure reaches the search index.** On PostgreSQL a claim's search words
  are held beside its row. A tombstone clears them in its one statement, a
  delete removes the row, and a trigger on the table clears the words of any
  tombstoned row, so the rule holds for a tombstone written by a binary from
  before the index too.

  **Erasure does not reach copies outside the graph.** The `ClaimAppend`
  request on the run record, the session memory the claim was mapped from,
  evidence artifacts, the context snapshots of runs that read the claim, and,
  after a tombstone, the free-text reason of a trust transition, all still
  hold what they held. To erase a subject's content, delete the claims and
  also discharge the retention of the runs that produced them and of the runs
  that read them.

  **Nor the database's own copies.** After a tombstone or a delete the content
  is gone from every row a query can read. PostgreSQL still holds the old row
  versions until vacuum reclaims them, and write-ahead-log archives, backups,
  and replicas hold them for as long as you keep those. Set backup retention
  and vacuum policy to match the erasure promise you make.

  **A stub can confirm a guess.** A tombstoned stub keeps the statement's
  fingerprint, and a gated promotion in its history keeps a second one.
  Neither is salted, so someone who knows or can enumerate the statement can
  confirm it against the stub. Use `delete` when that is not acceptable.

  **Roll order on PostgreSQL.** First run `migrate` from the new build: its
  reads need the new column, and the migration is safe under binaries that
  have not been upgraded. The first run takes an exclusive lock on the claim
  and ledger tables for catalog changes only, with no table scan or rewrite
  under it; the scan that validates the wider ledger check runs afterwards
  and does not block reads or writes. `migrate` sets no `lock_timeout`: if a
  long-running session holds a lock on either table, the upgrade and the
  statements queued behind it wait, so set one on the migrating session if you
  would rather fail fast. Then upgrade every binary that reads the graph. Only
  then call `tombstone`, `delete`, or the sweep. A binary that was missed
  fails closed: it refuses a stub with `schema-version-ahead`, and answers a
  replay of an erased operation `claim-operation-conflict`.
- **Retention** discharges a terminal run's session rows and snapshots after a
  per-tenant policy, snapshots first; agent-private memory outlives every run.
  Guardrails evaluate all seven boundaries — model and tool request, model
  and tool response, memory ingress, A2A ingress and egress — and a
  deployment attests that the chain its retrieval bundles, private and
  communal, and its A2A surface evaluate is the chain its dispatch authority
  declares
  (`with_memory_ingress`, `with_a2a_guardrails`). A blocked model response
  fails the effect once under `guardrail-blocked`; the run's record names the
  stage and the reason code that decided beside that code; a transformed one
  is what the run records. Four dependency-free stages ship in
  `guardrails::builtin`.

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
on. The `PromoteMemory`, `AppendClaim` and `EvaluateGoal` commands close the
`checkpoint-open` segment of a gate they open once the command commits, under
the parked identity the checkpoint stores, as the loop does for a gate a turn
opens; a replayed command closes none. Metrics are catalogued as data with
units, buckets, and bounded label keys, and a source scan holds the catalogue
to the call sites. Decision events are
durable records with a bounded outbox and a sink that may lag but never blocks.
The outbox is bounded by count (32) and by bytes
(`AGENT_RUN_DECISION_OUTBOX_MAX_BYTES`, 40 KiB); an event over the byte bound on
its own is dropped and counted (`decision_drops`, `rakka.agent.decision.drops`).
Besides the proposal decisions (`continue`, `call-tools`, `submit-result`, `evaluate`), the stream records a run's
lifecycle: checkpoint opens (`request-approval`, `request-authorization`),
reconciliations (`reconcile`, including the one a handoff notice settles, and
`request-reconciliation`, `Version2` only), checkpoint resolutions and escalations
(`resolve-checkpoint`, `escalate-checkpoint`), the waits a run enters (`wait`,
slots `wait-top-up-{sequence}`, `wait-children-q{n}`, `wait-handoff-q{n}`,
`wait-winding-down`), and the run's end (`complete`, `fail`, `cancel`,
`handoff`, `supersede`). Every new event but the approval and authorization
opens (`request-approval`, `request-authorization`, whose kind names the
checkpoint) carries a `reason_code`. Under `Version2`, and where they apply,
events also carry the optional `checkpoint`, `effect`, `outcome`
(`AgentDecisionOutcome`) and `run_status` fields; readers should key on
`reason_code`, since an
`awaiting-children` wait entered during a wind-down reads `run_status`
`cancelling`, a `budget-top-up` wait reads `running` (the run parks for a
top-up under `Running`), and a wind-down begun while an effect's outcome is unknown reads
`waiting-for-reconciliation`. The A2A coordination replay shows only a run
event's kind and `reason_code`. When a call is cancelled before it is sent (by
a failed call, a scheduled compensation, an accepted handoff or the run's
end), the approval or authorization it waited behind records
`resolve-checkpoint` with outcome and reason `cancelled`, as a cancel's close
does, before the run's end event (under `Version2`; see below). On an ended
run, events can follow the run's end event: for a parked memory promotion or
claim append, `request-reconciliation` (the checkpoint's open),
`escalate-checkpoint` and `reconcile`; for a gate left open on one of them,
`resolve-checkpoint` (its grant, denial or expiry); and for a gated one
commanded after the run ended, or a re-sent gated generation, the
`request-approval` or `request-authorization` of its new gate. A record an
earlier binary left ended with another call's approval or authorization
waiting gets no close event when a settle pass closes it: that run's end
event closed it for readers.
`request-reconciliation`, `escalate-checkpoint` and `resolve-checkpoint` are
written only under `Version2`; the other three are written under
`Version1` too. `AgentDecisionEventVocabulary` selects what is written:
`Version1`, the default, writes only the kinds an earlier reader decodes
(`request-approval`, `request-authorization`, `reconcile`, `complete`,
`fail`, `wait`) and none of the four fields, so a binary at the earlier pin
writes back exactly the event it read; `Version2` adds the six newer kinds and
the four fields and stamps events with schema version 2. Select it with
`with_decision_event_vocabulary` once every process that loads the run store or
reads the sink reads version 2 (see `docs/rakka-compatibility.md`).
The SDK, exporter, and Collector live at the application binary;
`examples/agent-otlp-export-acceptance` is that binary, and
`docs/plans/rakka-agent/kubernetes-agent-otel-collector-topology.yaml` is the
Collector topology it exports to. Telemetry is never a correctness input.

## Security

The security matrix records the posture clause by clause. The rules that hold
by construction: no resolved credential or secret is persisted anywhere
(durable state, effects, memory, runtime events, telemetry, logs, metrics,
snapshots); setup and settings revisions can only narrow a definition; a tool
call is undispatchable until every gate passes; retrieved memory, private and
communal, is untrusted input to the guardrail chain and can never widen a
capability; a communal claim is read only by a run granted its space and
holding every read capability it requires; failure text an
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
| `examples/multi-agent-goal-acceptance` | The M4 statement: a root goal delegating to specialists, invoking a workflow tool, surviving root and child loss, and reaching `Satisfied` through its evaluator. Its `tests/communal_read_path.rs` wires the communal read path into the same world and shows a run reading only the claims it is granted. |
| `examples/coordination-capability-acceptance` | The M5 statement: a board claim, a handoff, a human-owned upstream, a moderated conversation, and cursor replay across pod loss. |
| `examples/multi-pod-agent-fault-soak` | Two real processes, a shared durable directory, and a sweep that aborts a pod at every durable write. Gated. |
| `examples/agent-otlp-export-acceptance` | A real OpenTelemetry SDK exporting one real run over OTLP to an in-process receiver, asserted on the decoded protobuf. |
| `examples/clustered-sharded-entity-a2a-agents` | The A2A adapter over sharded runs with file or etcd discovery and Kubernetes manifests. |

## What is owed

Every matrix ends with an "Owed" section, and the roster says what it does not
claim. The open items as of this document are recorded there rather than here:
the segment classes with no production call site and the untested two-replica
tail-sampling gateway in the telemetry matrix; the coordination workload
across pods, the PostgreSQL arm for the shared substrate, and detected rather
than announced departure in the fault-injection matrix. The items a consumer
reported and the gap slices carry forward are tracked as GitHub issues, cited
from the implementation plan's "Owed onward" lines.
