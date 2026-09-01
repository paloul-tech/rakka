# Rakka Agent Implementation Plan

Status: implementation plan  
Date: 2026-07-11  
Normative source: [spec.md](spec.md); milestone bindings in
[spec 2.1](spec.md#21-milestone-binding)  
Design guidance: [technical-guidance.md](technical-guidance.md)  
Research record: [background-research.md](background-research.md)  

## How to Use This Plan

- Phases map to the spec milestones M1-M5 defined in
  [spec 2.1](spec.md#21-milestone-binding), plus a final production-hardening
  phase. A phase is complete when its milestone acceptance statement in
  [spec 22](spec.md#22-initial-acceptance-statement) is demonstrated.
- Slices are tight, focused units of work inside a phase. They are ordered by
  dependency; a slice may span more than one PR, but each PR should belong to
  exactly one slice.
- Every slice links the spec sections that govern it. Read those sections (and
  the linked guidance) before implementing the slice; the spec text is the
  contract, this plan is the sequencing.
- Exit criteria reference the recovery scenarios of
  [spec 18](spec.md#18-required-recovery-scenarios) by number. A scenario
  listed in a slice means a test proving it lands in that slice.
- Every slice ends with `scripts/validate.sh` green (fmt, clippy
  `-D warnings`, workspace tests, no-default-feature checks, doc build). All
  public items need doc comments from the first commit.
- Nothing is published to any registry at any point without explicit approval
  (workspace publishing policy).
- When a slice lands durable user-facing behavior, update the relevant
  `docs/` product doc and record the change in `CHANGELOG.md`.

## Standing Constraints (every phase)

These are restated here because every slice touches them:

1. **Always-on invariant** — active/waiting are durable states; quiescent
   entities passivate with zero per-agent live resources
   ([spec 6.11](spec.md#611-logical-availability-and-runtime-residency),
   [spec 15](spec.md#15-passivation-recovery-and-shard-movement)).
2. **Inter-entity choreography** — every cross-entity exchange is a
   deduplicated outbox/inbox saga re-driven by the initiator; no in-memory
   shortcuts, even colocated
   ([spec 9.8](spec.md#98-inter-entity-choreography)).
3. **Escrow budgets** — allocations debit the parent at creation; dispatch-time
   reservation touches only the run's own ledger
   ([spec 9.7](spec.md#97-hierarchical-budget-ledger)).
4. **Model adapter trait** — the loop depends only on the Rakka-owned trait;
   Rig lives behind the default-on `rig` feature and never appears in core
   public API or persisted state
   ([spec 10.1](spec.md#101-model-adapter-trait-and-rig-feature-gate)).
5. **Serializable entity protocols** — every new sharded entity command/reply
   surface must be serializable from the first commit (no `Arc` payloads or
   in-process reply channels in the remote surface; box large payloads). The
   existing process-local `AgentRunActorCommand` pattern in
   `rakka-agent-workflow` cannot cross `rakka-remote`; see
   `examples/clustered-agent-workflow-http-grpc` for the working
   remote-ask pattern and verify against current code before reuse.
6. **No resolved credentials** in state, effects, memory, events, telemetry,
   or snapshots ([spec 11.1](spec.md#111-effect-intent),
   [spec 16](spec.md#16-security-and-authorization)).
7. **Schema versions** on every persisted record where evolution is expected;
   unsupported versions fail closed
   ([spec 20](spec.md#20-compatibility-and-migration)).

## Phase Map

| Phase | Milestone | Content | Acceptance |
| --- | --- | --- | --- |
| 1 | M1 | Core durable agent: crate, identities, agent/task/run entities, choreography, loop, model adapter, effects, tool authority, budgets, admission, checkpoints, session memory, A2A, observability, recovery | [spec 22 initial statement](spec.md#22-initial-acceptance-statement) |
| 2 | M2 | Private long-term memory, vector retrieval, communal knowledge graph | [spec 22 memory note](spec.md#22-initial-acceptance-statement) |
| 3 | M3 | Continuous goals: wake identity/policy, controller, epochs, fencing | [Continuous Goal Milestone](spec.md#continuous-goal-milestone-m3) |
| 4 | M4 | Multi-agent goals: goal contract, evaluation, delegation, fan-in, workflow tools, cancellation propagation | [Multi-Agent Goal Milestone](spec.md#multi-agent-goal-milestone-m4) |
| 5 | M5 | Coordination: handoff, teams, moderation, human-owned tasks, replayable events | [Coordination Capability Milestone](spec.md#coordination-capability-milestone-m5) |
| 6 | — | Production fault, security, and telemetry validation; docs | Phase 6 exit criteria below |

Sequencing: Phase 1 is the base for everything. Phases 2-5 may be re-ordered
per [spec 2.1](spec.md#21-milestone-binding), with these practical
dependencies:

- Phase 3 (continuous) needs Phase 1 budgets, timers, and the goal-contract
  continuous clauses of [spec 8.1](spec.md#81-goal-contract-and-lifecycle);
  it does not need Phase 2 or 4.
- Phase 4 scenario 33 (communal provenance) needs the Phase 2 graph; if
  Phase 4 runs first, defer that scenario to Phase 2 completion.
- Phase 5 handoff/human tasks need only Phase 1; teams and moderation benefit
  from Phase 4's collaboration metadata but do not strictly require it.

---

## Phase 1 — M1 Core Durable Agent

Milestone: M1 ([spec 2.1](spec.md#21-milestone-binding)).
Acceptance: [spec 22 initial statement](spec.md#22-initial-acceptance-statement).
Scenarios owed: 1-14, 17, 19, 21-26, 35, 37, 40, 44, 46, 52-61
(all M1-tagged scenarios in [spec 18](spec.md#18-required-recovery-scenarios)).

Open decisions from [spec 21.3](spec.md#213-open-decisions) to confirm or
revise during this phase: 1 (concurrent runs), 4 (model-call retry policy),
5 (no generic retry for ambiguous non-idempotent effects), 7 (short-term
retention), 9 (service-resolved authorization, exercised by Slice 1.10),
10 (settings as A2A management skill), 11 (trace-segment split),
12 (content capture), 13 (sampling), 17 (`Task.id` mapping), 19 (setup
envelope, designed in Slice 1.2 and enforced in Slice 1.8), 20 (no idle
residency).

### Slice 1.1 — Crate scaffolding and feature gates

Spec: [19](spec.md#19-crate-and-feature-shape),
[10.1](spec.md#101-model-adapter-trait-and-rig-feature-gate).
Guidance: [Suggested Crate Boundaries](technical-guidance.md#suggested-crate-boundaries).

- Create `crates/rakka-agent` with workspace lints, `publish` metadata, and a
  module map matching the spec's crate-shape bullet.
- Wire features: `rig` (default), `otel`; crate compiles and tests with
  `--no-default-features`; add that configuration to the workspace
  minimal-feature checks in `scripts/validate.sh`.
- Add `rakka` facade passthroughs (`rakka-agent?/rig`, `rakka-agent?/otel`)
  following the existing `rakka-agent-workflow?/...` pattern in
  `crates/rakka/Cargo.toml`.
- Depend on `rakka-agent-workflow`; do not depend on `rakka-a2a` (the A2A
  adaptation happens in `rakka-a2a` behind an `agents` feature later).

Done when: empty-but-documented crate builds in both feature configurations
and `scripts/validate.sh` is green.

### Slice 1.2 — Identity/definition/settings contracts and AgentEntity

Spec: [6.1-6.8](spec.md#6-core-terms-and-identities),
[6.10](spec.md#610-stable-operation-ids),
[6.11](spec.md#611-logical-availability-and-runtime-residency),
[7.1-7.3](spec.md#7-agent-definition-and-settings),
[20](spec.md#20-compatibility-and-migration).
Guidance: [Identity and Ownership](technical-guidance.md#identity-and-ownership),
[Definition and Setup Revisions](technical-guidance.md#definition-and-setup-revisions).

- Tenant-scoped newtype IDs: `AgentId`, `AgentGoalId`, `AgentTaskId`,
  `AgentRunId` (plus `AgentDelegationId`, `AgentWakeId`,
  `AgentEnvironmentRef`, and `KnowledgeSpaceId` types now, used in later
  phases — the latter two fix the environment/memory scope keys that M2 and
  M4 build on, [spec 6.7](spec.md#67-agentenvironmentref),
  [spec 6.8](spec.md#68-knowledgespaceid)). Types stay distinct even where
  initial values coincide ([spec 6.3](spec.md#63-agentgoalid),
  [spec 6.4](spec.md#64-agenttaskid)).
- A run is bound to one task for its lifetime
  ([spec 6.5](spec.md#65-agentrunid)); encode that in the constructor, not a
  setter.
- `AgentDefinitionRevision`, `SettingsRevision` with the three timing classes
  (turn-bound / immediate safety / run-pinned,
  [spec 7.2](spec.md#72-settings-revisions)), and `AgentSetupRevision` with
  narrow-only envelope validation ([spec 7.3](spec.md#73-definition-versus-run-setup)).
- Sharded `AgentEntity` keyed `(TenantId, AgentId)` with a serializable
  command protocol (standing constraint 5), owning definition and lifecycle
  status, the current settings revision, policy and logical
  credential-binding references, the agent-private memory namespace, and
  administrative suspend/resume/terminate commands
  ([spec 6.2](spec.md#62-agentid)). Routine run creation never round-trips
  through it synchronously: the Slice 1.4 assignment flow reads its durable
  definition/admission state
  ([spec 9.8](spec.md#98-inter-entity-choreography)).
- Stable operation/deduplication ID construction helpers
  ([spec 6.10](spec.md#610-stable-operation-ids)).
- Schema-version fields and fail-closed deserialization on every persisted
  record introduced here.

Done when: contract-level tests prove envelope validation rejects widening
setups (enforcement at dispatch lands in Slice 1.8), unsupported schema
versions fail closed, and an `AgentEntity` instantiated with versioned
settings persists, passivates, and recovers its definition/settings state.

### Slice 1.3 — Inter-entity choreography substrate

Spec: [9.8](spec.md#98-inter-entity-choreography),
[6.10](spec.md#610-stable-operation-ids).
Guidance: [Inter-Entity Choreography](technical-guidance.md#inter-entity-choreography).

- Build exchange primitives over the `rakka-agent-workflow` inbox/outbox
  (`inbox.rs`, `outbox.rs`): pending-exchange record on the initiator,
  operation-ID re-drive on recovery, receiver-side dedup that returns the
  original logical result.
  - **Amended as implemented (2026-07-13):** the saga journal
    (`AgentExchangeJournal`) is a component of each participant's *own*
    durable state record, not a layer over the `rakka-agent-workflow`
    inbox/outbox. [Spec 9.8](spec.md#98-inter-entity-choreography) requires
    the sender to persist the command intent "as part of its own transition";
    the agent-workflow inbox/outbox is a separate `WorkflowState` record — a
    second compare-and-set — and cannot give that atomicity. Slices 1.4, 1.5,
    and 1.9 embed the journal in their entity state via `AgentExchangeState`
    and host transitions through `AgentExchangeHost` (the
    `ChoreographyProbe` in `rakka_agent::testkit` is the worked reference).
    The agent-workflow inbox/outbox remains the substrate for external
    effects, where Slice 1.7 lands.
- Write the failure-window table (initiator loss before send, receiver loss
  after acceptance, reply loss, duplicate delivery) for the exchanges this
  phase implements: creation, assignment, run acceptance, result
  proposal/decision, budget allocation, settlement/return. Commit it as a
  doc section in the crate (`rustdoc` or `docs/`), one row per window per
  exchange, with the test name that proves each row.
- No colocated shortcut: the exchange path is identical whether entities
  share a node or not.
- Cross-node variants run on the default per-node deterministic-modulo shard
  coordinator (symmetric hosting); the fenced-lease coordinator collapses an
  entity type onto the single lease holder and cannot host them.

Done when: scenarios 58 and 60 pass against an in-memory store, including a
split-across-nodes variant of 60.

### Slice 1.4 — AgentTaskEntity

Spec: [6.4](spec.md#64-agenttaskid), [9.1](spec.md#91-typed-task-definition),
[9.2](spec.md#92-task-lifecycle-and-result-rules),
[9.6](spec.md#96-bounded-task-state-and-history).
Guidance: [Typed Task Contract](technical-guidance.md#typed-task-contract),
[Bounded Task State and History](technical-guidance.md#bounded-task-state-and-history).

- Sharded `AgentTaskEntity` keyed `(TenantId, AgentTaskId)` with a
  serializable command protocol (standing constraint 5).
- Assignment decisions read the agent's durable definition/admission state
  owned by the Slice 1.2 `AgentEntity`; no synchronous `AgentEntity` round
  trip ([spec 9.8](spec.md#98-inter-entity-choreography)).
- Versioned `AgentTaskDefinition<R>`: typed input/result schema references,
  deterministic result rules, rejection limits, dependency policy,
  per-task budgets, generics only as compile-time ergonomics
  ([spec 9.1](spec.md#91-typed-task-definition)).
- Task lifecycle enum and transitions ([spec 9.2](spec.md#92-task-lifecycle-and-result-rules));
  dependencies are durable, bounded, acyclic; default failed-dependency
  policy cancels dependents.
- Result proposals validated in-entity by deterministic rules only;
  model-assisted evaluation is a durable effect, never in-entity I/O.
- Bounded materialized state with history/content behind cursors and
  artifact references ([spec 9.6](spec.md#96-bounded-task-state-and-history));
  configured limits enforced.

**Amended as implemented (2026-07-13):**

- **Creation has two doors, one transition.** An ingress creates a task with the
  deduplicated `AgentTaskEntityCommand::Create` (the operation id comes from the
  ingress, per the canonical flow); a delegating run creates a child task with
  the `AgentExchangeKind::Creation` exchange. Both reach the same bounded
  transition, so the paths cannot diverge. The exchange envelope is also the
  entity's *remote* ask surface, because `rakka-sharding` registers one ask pair
  per entity type and cross-entity commands are what must cross nodes.
- **The assignment decision is a separate read, not a separate transition.** It
  needs the agent's durable admission state, and reading it is I/O, which
  [spec 9.5](spec.md#95-execution-rule) forbids inside a transition. The entity
  therefore reads `load_agent_entity_state` *before* the transition and then
  decides on what it read, so creation, assignment, and the run-creation command
  the assignment owes all commit in one compare-and-set. `settle_side_effects`
  re-runs the read on recovery, which is what lets a task refused because its
  agent was suspended be assigned later with no new command.
- **Task history is an outbox, not a second store write.** Bounded state and
  durable history are two records, and two records mean two compare-and-sets. The
  entity therefore commits each history entry to a bounded outbox *inside* the
  transition that produced it, and `flush_task_history` appends it to the
  `AgentTaskHistoryStore` afterwards; the append is idempotent on the entry's
  sequence. This is the same argument the slice 1.3 amendment makes for the
  exchange journal.
- **Dependency propagation lands here on the receiving side only.** The task
  applies a dependency's outcome (a deduplicated command) and its failure policy.
  The *sending* side — an upstream task notifying its dependents when it goes
  terminal, which needs a durable dependents registry — is cancellation
  propagation, and it belongs to slice 4.6 ([spec 8.7](spec.md#87-cancellation-failure-and-waiting))
  and the human-owned tasks of slice 5.4.
- **`AgentContentDigest` is a fingerprint, not a security boundary.** It
  identifies which proposal a rejection refused
  ([spec 9.2](spec.md#92-task-lifecycle-and-result-rules)). The digest-bound
  authorization grants of [spec 12.3](spec.md#123-grant-binding) need a
  cryptographic algorithm; slice 1.10 adds it to the record's existing
  `AgentDigestAlgorithm`, whose non-exhaustive shape is there for exactly that.

**Amended in review (2026-07-14):**

- **An agent-owned task id reserves run-id headroom at creation.** Run ids are
  derived as `{task}-gen-{generation}`, and the derived id must satisfy the
  identity bound for every reachable generation. Creation refuses an
  agent-owned id longer than `AGENT_TASK_ASSIGNABLE_ID_MAX_LENGTH`
  (`task-id-too-long`), where the caller can still choose another — the
  alternative was a task that is valid at creation and permanently
  unassignable at decision time.
- **An unchanged refusal is recorded once.** The assignment decision runs
  inside the command transition and again on every settle pass, so the refusal
  path deduplicates against `last_refusal`: the same agent refused for the
  same reason is not a new fact, and neither the append-only history nor the
  store revision moves. The settle pass also stands behind the same
  history-headroom fence as the command and exchange doors, so a sink outage
  cannot push the pending outbox past its bound through settlement.
- **Duplicate dependency declarations inside one creation follow the
  post-creation rule.** A repeated edge is idempotent; a repeat under a
  conflicting failure policy is refused (`task-dependency-conflict`) rather
  than collapsing last-wins, and the dependency limit counts edges, not
  declarations.
- **A delegated creation's accepted reply has its own payload type.**
  `AGENT_TASK_CREATION_OUTCOME_PAYLOAD_TYPE` carries the `AgentTaskOutcome`;
  `AGENT_TASK_DECISION_PAYLOAD_TYPE` keeps naming exactly the
  `AgentTaskDecision` shape, so one type tag never names two schemas.
- **Exchange transitions are stamped where they commit.**
  `AgentExchangeParticipant::apply`/`settle` now receive the host's
  commit-time `now`; the task participant previously stamped `updated_at` and
  history rows with `envelope.created_at()` — the initiator's clock at the
  earlier moment the envelope was recorded — which could move `updated_at`
  backwards and put history rows out of time order.

Done when: scenarios 37, 40, and 55 pass; the creation/assignment exchange
from [spec 9.8](spec.md#98-inter-entity-choreography) replays to one task and
one assignment per generation.

### Slice 1.5 — AgentRunEntity and the durable loop

Spec: [6.5](spec.md#65-agentrunid), [9.3](spec.md#93-run-status),
[9.4](spec.md#94-loop-phase), [9.5](spec.md#95-execution-rule).
Guidance: [Durable Agent Loop](technical-guidance.md#durable-agent-loop).

- Sharded `AgentRunEntity` keyed `(TenantId, AgentId, AgentRunId)` with a
  serializable command protocol.
- Run status enum ([spec 9.3](spec.md#93-run-status)) including the
  `WaitingForHuman` compatibility note; loop phase enum and the durable
  loop-state record ([spec 9.4](spec.md#94-loop-phase)) with schema/adapter
  version.
- Execution rule: handlers perform bounded transitions only; every transition
  persists the next effect or wait before returning
  ([spec 9.5](spec.md#95-execution-rule)).
- Result proposal/decision exchange with the task entity over Slice 1.3
  primitives; the run never makes the public task terminal by itself.
- Passivation-by-default: after any persisted wait, the entity is idle.
- Interim effect contract: until Slice 1.7 lands, transitions persist
  effects through the existing `rakka-agent-workflow` `AgentEffect`/outbox
  substrate, and scenario 2 is driven by a scripted transition stub (the
  deterministic adapter arrives in Slice 1.6). Slice 1.7 retrofits the full
  `EffectIntent` machine; the Slice 1.14 regression re-proves scenario 2 on
  it.

**Amended as implemented (2026-07-14):**

- **The effect record is a field of the run's own state; the agent-workflow
  outbox is its sink.** This is the third instance of the argument the slice 1.3
  and 1.4 amendments make, and it is forced by the same fact: the run's state and
  the agent-workflow outbox are two independent compare-and-sets.
  [Spec 9.5](spec.md#95-execution-rule) requires the run transition to "persist
  the next effect or wait before returning", and a run that committed
  `AwaitingModel` and then lost its node before writing the outbox row would wait
  forever for an effect nobody will dispatch. `AgentRunEffect` therefore lives in
  `AgentLoopState`, committed by the transition that decided it, and
  `AgentRunEffectSink` hands it to the agent-workflow `AgentEffect` outbox
  afterwards — idempotently on the derived `effect_id`, which is also why shard
  movement cannot make an effect dispatchable twice
  ([spec 15](spec.md#15-passivation-recovery-and-shard-movement)).
- **The loop cranks; it does not run.** `AgentRunEntityStore::settle_side_effects`
  performs one bounded transition per compare-and-set and stops at the first
  durable wait, then dispatches owed effects and drives owed exchanges. It reads
  only durable state, so calling it after a transition, after recovery, or from a
  sweep are the same operation. Both `AGENT_RUN_MAX_LOOP_STEPS_PER_PASS` and
  `AGENT_RUN_MAX_SETTLE_ROUNDS` fence the handler's work, because a bound that
  holds only by construction is not a bound.
- **A run awaiting its task's decision is `Running`, not a new wait status.** The
  waiting statuses of [spec 9.3](spec.md#93-run-status) enumerate what a run waits
  *for* — a timer, an effect, an approval, an authorization, a reconciliation — and
  a pending inter-entity exchange is none of them: it is the run's own durable
  outbox, re-driven by the courier. `Running` is not a residency claim (spec 9.3
  says so explicitly), and the entity passivates there like anywhere else. The
  loop phase `DecidingContinuation` plus a persisted proposal is what records the
  wait.
- **The `WaitingForHuman` compatibility note is discharged by splitting, not by
  aliasing.** [Spec 9.3](spec.md#93-run-status) permits keeping the substrate's
  single `WaitingForHuman` as a compatibility representation, provided the split
  is explicit before it happens. `AgentRunStatus` makes the split at its first
  commit and preserves no alias, so no agent-domain record was ever written under
  an unsplit status and there is no migration to owe.
  `AgentRunStatus::is_waiting_for_human` is the explicit public behavior: the set
  the substrate's variant corresponds to, and what slice 1.12 projects onto A2A.
- **The run's own budget ledger lands here, because the loop state requires it.**
  [Spec 9.4](spec.md#94-loop-phase) puts "remaining loop, token, cost, and time
  budgets" in the durable loop state, so `AgentRunBudget` is part of it: charged by
  the run's own single-entity transitions, never by reading a parent scope. Slice
  1.9 adds the escrow hierarchy above it (allocation, settlement, return, top-up)
  as deduplicated exchanges. Exhaustion is already structured
  (`AgentBudgetExhaustion` names the dimension, limit, and consumed value), which
  is what makes the later top-up a pure addition.
- **`AgentModelTurn` is the durable half of the model contract, and it lands
  before the adapter.** [Spec 10.2](spec.md#102-persistence-compatibility) requires
  the Rakka-owned versioned loop representation to be the only durable format, and
  the loop must act on *something*. The turn record therefore lands in `model.rs`
  now, and slice 1.6 adds the adapter trait that produces one. This is also what
  lets the loop be driven end to end before any adapter exists: a turn arrives the
  way every effect result arrives — as a durable command from the dispatcher
  ([spec 9.5](spec.md#95-execution-rule)) — so the scripted stub is a faithful
  dispatcher rather than a shortcut around one.
- **Content does not accumulate in the loop.** `RecordingTurn` is where slice 1.11
  appends the turn to session memory; the loop's own record keeps no turn content
  and drops resolved effects with the turn, so a run that iterates a hundred times
  persists no more than one that iterates once
  ([spec 9.6](spec.md#96-bounded-task-state-and-history), and content capture is
  off by default per [spec 17.14](spec.md#1714-content-capture-and-redaction)).
  A late result for an effect of a cleared turn is refused as unknown; one for an
  unresolved effect of the current turn is refused as stale. Both fences are
  proven.
- **A refusal is not a rejection, and the run does not iterate on one.** A task
  *rejection* is a validation decision and returns feedback plus a remaining
  iteration count, so the run takes another bounded turn. A task *refusal* means
  the task would not evaluate the proposal at all — the run is fenced by a newer
  generation, or the task has moved on — so there is nothing for the run to
  correct: it stops, mapping the task's status onto `Superseded`, `Cancelled`, or
  `Failed`. Guessing either way would complete a task the rules may have refused,
  or burn an iteration the task never charged.

Done when: scenarios 2 and 59 pass (restart after every loop transition;
result-exchange loss on either side converges).

### Slice 1.6 — Model adapter trait, deterministic test adapter, Rig feature

Spec: [10](spec.md#10-model-adapter-and-rig-integration) (all subsections).
Guidance: [Durable Agent Loop](technical-guidance.md#durable-agent-loop),
[Client, Events, and Testkit](technical-guidance.md#client-events-and-testkit).

- Define `AgentModelAdapter` (working name): immutable context snapshot +
  settings revision in, bounded result/artifact out
  ([spec 10.1](spec.md#101-model-adapter-trait-and-rig-feature-gate)).
- Deterministic test adapter implementing the trait without the `rig`
  feature: scripted text/results, structured task-result proposals, tool
  requests, conditional responses ([spec 10.4](spec.md#104-deterministic-rig-test-adapter)).
  It exercises the same durable effect path as production.
- `rig` feature: Rig-backed implementation, pinned Rig version, no Rig types
  outside the feature; Rakka-owned versioned loop representation is the only
  durable format ([spec 10.2](spec.md#102-persistence-compatibility)).
- Model calls are effects with explicit retry policy
  ([spec 11.5](spec.md#115-crash-and-timeout-rules); open decision 4).
- The scripted turn rides the existing dispatcher/effect-bridge substrate
  (`dispatcher.rs`, `effect_bridge.rs`); Slice 1.7 upgrades this path to the
  full effect-state machine without changing the adapter contract.
- The adapter's context-snapshot input starts as an opaque versioned
  reference; Slice 1.11 formalizes it as the `MemoryContextSnapshot` of
  [spec 13.5](spec.md#135-memory-context-snapshot).

**Amended as implemented (2026-07-15):**

- **The "bounded model request" is a type, and the adapter is the whole
  contract.** `AgentModelAdapter::call` takes an `AgentModelRequest` — the
  context-snapshot reference, the model profile and sampling a settings revision
  selected, and the turn — and returns an `AgentModelTurn`. There is no separate
  "map the response" surface: a provider request and response are the adapter's
  private concern, never durable state, so the turn stays the only durable
  format ([spec 10.2](spec.md#102-persistence-compatibility)). The interim
  request carries an `AgentRevisionNumber` settings revision that defaults to the
  initial one; Slice 1.8 resolves settings at dispatch and fills the profile and
  sampling from what it resolves.
- **The retry policy is a value the adapter declares, not behavior it runs.**
  `AgentModelRetryPolicy` carries the effect safety class (default `ReadOnly`)
  and a bounded attempt count, and `validate` runs where a policy enters — the
  adapters' fallible `with_retry_policy` builders, `read_only`, and
  deserialization — so a `NonIdempotent` call permits exactly one attempt and a
  retry count can never override the non-idempotent ambiguity rule
  ([spec 11.4](spec.md#114-dispatch-invariants)).
  Slice 1.7's effect machine reads and re-enforces it at dispatch; landing it
  here is what keeps that enforcement an addition rather than a change to the
  adapter contract.
- **The deterministic adapter is split from the dispatcher stub.**
  `DeterministicModelAdapter` is the trait implementation; `ScriptedDispatcher`
  is the effect-bridge stub, now generic over the adapter it answers model calls
  through and defaulting to the deterministic one. A scripted turn therefore
  travels the exact effect path a provider's turn travels — the dispatcher reads
  a dispatched effect, invokes the adapter, and returns the turn as a durable
  result command — and an adapter error (a provider failure, or an unboundable
  turn) surfaces as a failed effect, exactly as a real dispatcher surfaces one.
  The `rig`-backed adapter rides the same generic stub, which is what lets one
  end-to-end test body prove both adapters.
- **Rig is pinned exactly (`rig-core = "=0.37.0"`) with default features off.**
  Rakka needs
  only Rig's provider-neutral completion contract, so the HTTP/TLS stack Rig's
  default features pull in is disabled; the deploying application supplies the
  concrete provider client and its credentials. A model's typed result proposal
  is expressed as a call to a configurable *result tool* (`submit_result` by
  default): the adapter declares that tool on every completion request it builds
  — a provider can only call a tool it was offered — maps that one tool call
  onto the run's result proposal,
  and maps every other tool call onto a tool-call request. This is the interim bridge
  from a provider's function-calling surface to Rakka's typed task result; the
  tool registry of Slice 1.8 and the context snapshot of Slice 1.11 formalize the
  surrounding machinery without changing the adapter contract. `rig.rs` also
  ships `ScriptedCompletionModel`, a deterministic stub `CompletionModel`, so the
  Rig adapter is driven with no network, credentials, or provider account.

Done when: one scripted model turn runs end-to-end through the durable
effect path under `--no-default-features`, and the same test passes with the
`rig` feature against a stub provider.

### Slice 1.7 — Effect model and dispatcher integration

Spec: [11.1-11.6](spec.md#11-effect-model).
Guidance: [Effect Safety Guidance](technical-guidance.md#effect-safety-guidance).

- `EffectIntent` record ([spec 11.1](spec.md#111-effect-intent)), safety
  classes ([spec 11.2](spec.md#112-safety-class)), effect status machine with
  generations ([spec 11.3](spec.md#113-effect-state)).
- Integrate with the `rakka-agent-workflow` dispatcher and effect bridge
  (`dispatcher.rs`, `effect_bridge.rs`): durable `Started` with lease/fence
  before invocation, dispatch-time credential resolution only, stale-result
  rejection ([spec 11.4](spec.md#114-dispatch-invariants)).
- Crash/timeout recovery per safety class, `Indeterminate` transition to
  `WaitingForReconciliation`, dispatch-eligibility revocation
  ([spec 11.5](spec.md#115-crash-and-timeout-rules)).
- Cancellation fencing at the effect layer: fence new dispatch immediately;
  ambiguous non-idempotent effects stay in reconciliation
  ([spec 8.7](spec.md#87-cancellation-failure-and-waiting) single-task
  clauses, [spec 11.5](spec.md#115-crash-and-timeout-rules)).
- Retrofit the Slice 1.5 loop and Slice 1.6 model-call paths onto this
  machine, and record a first rough per-turn durable-write count/latency
  measurement (formalized in Slice 1.14).

**Amended as implemented (2026-07-16):**

- **The status machine spans two durable layers, and the split is the
  correctness argument.** The run's own effect record (the slice 1.5
  amendment's decision, kept) owns `Pending`, `Ready`, and the five terminal
  outcomes; the dispatch layer's durable records — the agent-workflow outbox
  row and the dispatcher-fleet entry with its lease and fencing token — own
  `Started` and `RetryScheduled`. Requiring the run's record to track
  attempt-level states would add one run compare-and-set per attempt for no
  fence the outbox row does not already provide; the outbox row's
  `Dispatching` status *is* the durable ambiguity marker recovery reads.
- **The flush order was inverted so `Pending` proves non-dispatch.** Slice 1.5
  wrote the sink first and marked the record after, which meant a `Pending`
  effect *might* be in the outbox and cancellation had to flush-and-settle it.
  Now the transition that marks `Ready` commits before any sink write starts,
  so a `Pending` effect has provably never reached the outbox and the
  cancellation acceptance fences it in place atomically
  ([spec 8.7](spec.md#87-cancellation-failure-and-waiting)'s "immediately").
  The price — `Ready` no longer proves the sink write landed — is paid by
  re-driving the idempotent sink write for every `Ready`-unresolved effect on
  each settle pass. The 1.5 test that asserted the old flush-under-cancel
  behavior was rewritten to assert the fence.
- **Attempt-level retries never reach the run.** A result command is final for
  its generation: `Succeeded`, `Failed`, `Exhausted`, `Indeterminate`, or
  `Cancelled`. The outbox row's retry budget is aligned to the intent's
  attempt bound when the ticket is scheduled, so the dispatch layer cannot
  retry what the policy does not permit, and the run is not woken once per
  attempt.
- **A new generation is a new dispatch ticket.** The outbox deduplicates on a
  generation-qualified ticket id (`{effect}#g{n}`), so `ConfirmedNotExecuted`
  yields a fresh dispatchable row while the superseded generation's terminal
  row can never be redispatched; the external idempotency key derives from
  identity *and* generation for the same reason. The reconciliation decision
  itself landed here as the run command `ResolveIndeterminateEffect` with the
  two non-retry decisions of [spec 12.5](spec.md#125-reconciliation-decisions),
  because scenario 57's effect half cannot be proven without a resolution
  entry point; slice 1.10 wraps it in the checkpoint record and grant binding
  without changing the run-side semantics.
- **Effect specs are deployment registration until slice 1.8.**
  `AgentEffectPolicies` on the run entity supplies the class-level declaration
  (model calls default to one read-only attempt; unclassified tools fail safe
  as non-idempotent, exactly because an unknown tool's ambiguous loss must
  park rather than guess). The tool registry replaces the map without touching
  the effect record. The model adapter's declared retry policy remains the
  1.6 surface; open decision 4 is exercised by configuring the model spec.
- **Recovery is the claim path, not a separate sweep.** An expired-lease fleet
  entry is claimable, and re-claiming it under a fresh fencing token *is* the
  recovery: the claim path reads the outbox row's status to distinguish a
  fresh ticket from an ambiguous one, re-reads the run's durable intent (which
  also rejects stale tickets without invocation), and applies the
  safety-class table. A `Reconcileable` intent treats a retry-scheduled row as
  still ambiguous — a burned attempt proves nothing about what the prior
  attempt did — so every one of its retries re-queries the protocol and
  invokes only when proven absent. The wind-down fence pass repairs only what no claim can
  see — a ticket that never reached the outbox gets a tombstone row planted
  before its cancelled word is delivered, so a laggard flush racing the fence
  lands on a terminal row instead of creating dispatchable work post-fence.
- **The effect record's schema version deliberately stays at 1.** The slice
  reshaped the persisted `AgentRunEffect` — renamed status labels, new
  required fields — without bumping
  `CURRENT_AGENT_RUN_EFFECT_SCHEMA_VERSION`, because the record has only ever
  existed on this unreleased phase branch: there is no released writer whose
  records a version gate would protect, and a gate would only dignify
  test-environment leftovers. The first reshape after a release must bump the
  version so the spec 20 policy fails closed on the old records instead of
  surfacing a raw decode error.
- **Measured (rough, debug build, in-memory stores):** one clean model turn —
  accept, commit effect, ticket, claim, invoke, deliver, propose, accept —
  costs 8 run-store compare-and-sets, 3 workflow-store writes, ~6 ms wall.
  Recorded by `per_turn_durable_write_count_and_latency_measurement`;
  slice 1.14 sets the budget.

Done when: scenarios 5-10 pass with fault injection at each crash window
(before `Started`, after `Started` per safety class, after external commit),
and the effect-layer half of scenario 57 passes.

### Slice 1.8 — Tool registry, tool authority, and guardrail baseline

Spec: [11.7](spec.md#117-tool-registry-and-component-tools),
[11.8](spec.md#118-tool-authority-and-execution-isolation),
[16](spec.md#16-security-and-authorization).
Guidance: [Tool Visibility, Authority, and Executor Isolation](technical-guidance.md#tool-visibility-authority-and-executor-isolation),
[Guardrail Chain](technical-guidance.md#guardrail-chain).

- Tool registry with kinds, descriptor schema/version, safety class,
  capabilities, credential class ([spec 11.7](spec.md#117-tool-registry-and-component-tools)).
- Four authority layers: `ToolDescriptor` / `ToolBinding` / `EffectIntent` /
  `DispatchGrant`; grant binding and pre-attempt revalidation; model output
  cannot widen anything ([spec 11.8](spec.md#118-tool-authority-and-execution-isolation)).
- `ExecutionPolicyRef` persistence and trust-class routing hooks (the
  application owns realization).
- Versioned ordered guardrail stages at model/tool/A2A/memory boundaries with
  the bounded outcome set and deterministic-transform rule
  ([spec 16](spec.md#16-security-and-authorization)); deployment-mandatory
  stages cannot be removed by definition/setup.
- Enforce the Slice 1.2 setup/settings envelope at dispatch.

**Amended as implemented (2026-07-16):**

- **The registry replaces `AgentEffectPolicies` by projection, not deletion.**
  `AgentToolRegistry::effect_policies` derives the run entity's commit-time
  policies from the registered bindings, so the loop transition code and the
  effect record are untouched (exactly what the 1.7 amendment reserved), and
  the same registry backs the dispatch-time authority — one source, two
  enforcement points. "Unclassified" now means *registered without a
  declaration* (fails safe as one non-idempotent attempt); a tool with no
  registration at all is refused outright at dispatch
  (`tool-binding-missing`), because a binding is the only thing that can
  vouch for what a call would execute.
- **The authority gate is a required dispatcher collaborator, and the
  refusal's shape follows its cause.** `AgentRunEffectDispatcher` cannot be
  constructed without an `AgentDispatchAuthority`; a permissive default would
  be the universally privileged worker spec 16 forbids. The gate runs before
  every attempt's durable `Started` against the agent's *current* durable
  state (`AgentEntityAuthority` over `load_agent_entity_state`), which is
  what makes immediate-safety revocations and suspension per-attempt facts.
  A *transient* refusal — suspension — spends nothing durable: the claim is
  deferred at the fleet with the outbox row untouched, so no attempt burns
  (the budget keeps meaning "external invocation attempts", and a suspension
  cannot exhaust a single-attempt effect), no `Failed` row is written that
  recovery could misread as a possibly-executed reconcileable attempt, and a
  resumed agent's next attempt rechecks and proceeds. A *definitive* refusal
  settles the generation as `Failed` with the refusal's stable code and
  cancels the ticket, with nothing invoked — unless the refused attempt was
  the truth-finding retry of an ambiguous idempotent loss, where a prior
  attempt may already have committed externally: that generation parks
  `Indeterminate` under the refusal's code instead, preserving the spec 11.5
  ambiguity for the explicit reconciliation decision. The gate also
  revalidates the intent's reconciliation protocol and per-attempt timeout
  against the binding, and enforces the settings' guardrail-policy selection
  (`guardrail-policy-mismatch`) — the third immediate-safety field —
  against the policy reference the deployed chain carries.
- **The grant is derived per attempt, not persisted.** Issuing fresh and
  revalidating (`AgentDispatchGrant::validate_for`: exact intent and
  generation, argument digest, safety class, expiry, use count) *is* the
  pre-attempt revalidation spec 11.8 requires; there is no released grant a
  store could outlive. A grant is valid *through* its expiry instant, so the
  mint-and-spend path can never be refused by its own issuance timestamp
  whatever the TTL; the TTL bounds a grant a holder retains, which no
  shipped path does yet. Slice 1.10's checkpoint-bound grants add the
  durable half without changing this seam — until then a binding or
  guardrail that requires a checkpoint fails closed (`checkpoint-required`),
  because no grant can exist yet.
- **Setup enforcement rides the gate, resolved per claimed run.** Runs do
  not yet carry a setup reference, and the dispatcher's claim batch is
  fleet-wide — one worker serves every run whose tickets are due — so a
  setup fixed per worker would be enforced against runs it does not govern.
  `AgentEntityAuthority::with_setup_resolver` (and the single-run
  `with_setup_for_run`) maps each claimed run onto the setup it was created
  under; both the definition envelope *and* the resolved setup are checked,
  which fails closed when a definition narrowed after the setup was
  validated. When a later slice gives runs a durable setup reference, the
  gate reads it from the run state instead — the resolver seam disappears,
  not the semantics.
- **Guardrail transforms run at dispatch, never touch the durable intent,
  and are pinned to the chain revision the intent committed under.** Each
  committed intent records the chain revision of the policies it was stamped
  from (`AgentToolAuthority::effect_policies` projects the registry and the
  configured chain together), and the pipeline refuses an attempt
  (`guardrail-revision-mismatch`) whenever a transform would decide the
  payload — or any retry follows a possibly-delivered attempt — under a
  chain that no longer matches the pin. That is how "a retry reuses the
  accepted transformed input" is honored without a second durable write:
  re-derivation is provably under the pinned revision, one external
  idempotency key can never carry two different payloads, and the intent's
  argument digest stays bound to what the run committed. The chain itself is
  deployment configuration; a definition or setup can require stages
  (envelope `mandatory_guardrails`, enforced as `guardrail-stage-missing`
  when the chain cannot run one) and disable optional ones (`narrowed`,
  which mints a revision of its own — a different stage set is a different
  evaluation), and no operation exists that removes a deployment-mandatory
  stage. Presence in the chain is never coverage, in two steps: a stage must
  declare at least one boundary (`guardrail-stage-unbound`, refused at
  registration), and a *required* stage must run at a boundary the caller
  actually evaluates (`guardrail-stage-unevaluated`, refused at dispatch —
  the chain cannot decide this for itself, because which boundaries have
  evaluation points is a property of the caller, so
  `AGENT_EVALUATED_GUARDRAIL_BOUNDARIES` passes them into
  `validate_covers`). This slice evaluates the tool-request and
  model-request boundaries; the declared response/A2A/memory boundaries gain
  their evaluation points — and with them, the ability of stages bound there
  to satisfy coverage — from the slices that own those flows, which is a
  one-line extension of that set. Until slice
  1.11 gives context snapshots content, the model-request boundary evaluates
  a bounded request descriptor — enough for a kill-switch or checkpoint
  stage, which is why a transform there fails closed
  (`guardrail-transform-unsupported`) instead of being silently ignored.
  Block evidence rides the refusal detail, and applied transforms and
  report-only findings surface on the dispatcher's trace.
- **A stage is evaluated against a context, not a bare boundary.**
  `AgentGuardrailContext` carries the boundary, the run scope, and — at the
  tool boundaries — the tool being called, so a stage can gate *which* tool
  is invoked or scope a policy to a tenant; a stage handed only the arguments
  could not tell one tool's call from another's. Identity rides the context
  rather than the content because content is exactly what a transform
  rewrites: an envelope would make a transform responsible for reproducing
  the identity fields and would spend the boundary's content budget on fields
  no stage may change. The split keeps identity readable and structurally
  unrewritable, and keeps determinism intact — everything the context carries
  is durable identity the same intent re-derives on every attempt.
- **The 1.6 amendment's settings note is discharged here.** The authority
  resolves the turn-bound settings of spec 7.2 at dispatch: the granted model
  call carries the profile and sampling the current settings revision
  selects, validated against the definition and setup envelopes, and the
  `AgentModelRequest` now records the settings revision it actually resolved
  rather than the interim initial value.
- **`EffectIntent` gained its `ExecutionPolicyRef`.** The intent persists the
  binding's execution policy, the dispatch ticket echoes it for routing, and
  `AgentExecutionPolicyRouter` is the application-owned hook: an intent that
  names a trust class no configured executor accepts stays undispatchable
  (`execution-policy-unroutable`) rather than running with ambient
  authority. The reshape keeps the effect schema at version 1 under the same
  unreleased-branch argument the 1.7 amendment recorded.

Done when: scenarios 44 and 54 pass (widening setups rejected; a
model-visible call stays undispatchable when binding, grant, credential,
checkpoint, execution-policy, or immediate-safety checks fail).

### Slice 1.9 — Escrow budget ledger and autonomy admission

Spec: [9.7](spec.md#97-hierarchical-budget-ledger),
[7.4](spec.md#74-autonomy-admission).
Guidance: [Hierarchical Budget Ledger](technical-guidance.md#hierarchical-budget-ledger),
[Autonomy Admission](technical-guidance.md#autonomy-admission).

- Escrow ledger: parent-local allocation debit inside the creating
  transition, carried on the creation command; run-local dispatch-time
  reservation as a single-entity transition; deduplicated settlement/return
  upward through Slice 1.3 exchanges; top-up request command with structured
  exhaustion parking ([spec 9.7](spec.md#97-hierarchical-budget-ledger)).
- Extend the existing `autonomy.rs` counters into the ledger dimensions;
  `Started` and `Indeterminate` attempts consume budget.
- `AutonomyAdmissionDecision`: fail-closed admission for unattended classes,
  recheck on widening updates, immediate-safety recheck at dispatch
  ([spec 7.4](spec.md#74-autonomy-admission)).
  - Enforcement re-derives the decision against the definition **now in force**
    (`admits_definition`), never a flag or the revision the decision recorded.
    "Narrowing updates MAY reuse an admission only when policy proves them
    monotonic" ([spec 7.4](spec.md#74-autonomy-admission)) is proven by *two*
    checks, not one: the authority-envelope narrowing (`admits`) **and** the
    structural requirements (`verify`, shared via `first_unmet_requirement`).
    The second is load-bearing because an approval/authorization/escalation
    policy is an `AgentDefinition` policy reference, not an envelope entry — a
    republish that drops one is not an envelope widening, so the envelope check
    alone would wave it through. Because enforcement derives from the current
    definition, `publish_definition` deliberately does **not** retract the
    admission: a definition that no longer satisfies a verified requirement is
    refused at assignment (`admission-requirement-regressed`) whatever path
    changed it, and no future call site has to remember to retract. Follow this
    pattern for any later admission surface (setup revisions, epoch admission):
    add the dimension to `verify`, not a new retract-on-mutation hook.

**Amended as implemented:**

- **The run emits its escrow exchanges from its own transitions; delivery never
  drives them.** A terminal run commits its settlement/return, and a parked run
  its top-up request, into its own exchange journal in the transition that owed
  it; the courier drains the journal. `accept` of an incoming exchange makes
  *local* progress only and never drives an owed cross-entity exchange, because
  the initiator of the exchange being accepted is mid-delivery and driving an
  exchange back to it would re-enter a transition whose reply has not settled —
  a run owing its task a settlement, and the task re-driving that run's
  still-outstanding assignment, otherwise recurse without bound. This made the
  accept/settle split a uniform property of both entities (the durable-outbox
  discipline), not a ledger special case.
- **Exhaustion parks and asks before it fails.** A run that exhausts a *conserved*
  ceiling records `AgentPendingTopUp` (status stays `Running` — a pending
  exchange is the run's own outbox, the slice 1.5 argument for the result
  proposal) and sends a deduplicated `BudgetAllocation` request. The charge that
  exhausted is made all-or-nothing (`reserve_model_turn`, `reserve_tool_turn`) so
  a re-attempt after the grant double-counts nothing. The run resumes iff the
  parent granted *something* in the exhausted dimension; a grant of nothing stops
  it with the *original* exhaustion. The relieve test is "did the grant add
  room," not "does one more unit fit" — the latter is wrong for a multi-unit tool
  fan-out, where a zero grant on a limit-1 budget would otherwise read as
  relieved and re-park forever. Because each grant strictly reduces the parent's
  headroom, the asking always terminates. A *non-conserved* exhaustion (a
  wall-clock deadline, a concurrency ceiling) is not a quantity a parent can
  grant, so it terminates the run rather than parking — `park_or_terminate`
  branches on `AgentBudgetDimension::is_conserved`.
- **Effect and attempt budgets are reserved at commit and settled at
  resolution.** Every effect a run commits reserves one durable `effect` and its
  whole attempt bound from the run's own ledger before dispatch, folded into the
  all-or-nothing per-turn reservation with a concurrency check; `settle_effect`
  at each generation's resolution consumes the attempts that reached `Started`
  (an `Indeterminate` attempt included) and releases the rest. Reserving the max
  up front is what denies a run work it could not afford to finish retrying, and
  what makes the `effects`/`effect_attempts`/`concurrent-effects` dimensions
  real rather than declared. The settle runs exactly once, on the generation's
  first resolution — a reconciliation that confirms an `Indeterminate`
  generation executed bills nothing further — and a reconciliation-authorized
  re-invocation reserves the *new* generation's attempt bound the same way
  (`reserve_attempts`): a run that cannot afford it refuses the resolution
  (`run-redispatch-unaffordable`) rather than dispatching unreserved work, and
  the operator's remaining decision is a cancellation, whose wind-down settles
  the generation without invocation.
- **The `rakka-agent-workflow` autonomy counters are superseded, not extended.**
  The slice text said to "extend the existing `autonomy.rs` counters into the
  ledger dimensions." `rakka-agent-workflow`'s `AgentAutonomyUsage`/
  `AgentAutonomyPolicy` are a pre-existing workflow-*dispatcher* policy subsystem
  (target-class routing, per-target step/call/token budgets) that lives entirely
  inside that crate and that the agent domain never consumed. Rather than graft
  the agent's escrow hierarchy onto that flat per-target counter, the slice
  built the richer, product-neutral `AgentRunBudget`/`AgentEscrowLedger` in
  `rakka-agent`: a conserved/non-conserved dimension split, up-front escrow with
  settlement and return, and per-run reservation. The workflow autonomy policy
  stays where it is (the workflow dispatcher still uses it); the agent domain's
  budget accounting is the new ledger. No agent-domain code depends on the
  workflow counters, so there is nothing to migrate.

Done when: scenarios 52, 53, and 61 pass, including a concurrency test that
cannot oversubscribe a parent allocation and a replay test that never
double-debits or double-credits.

### Slice 1.10 — Checkpoints and HITL

Spec: [12](spec.md#12-durable-checkpoints-and-hitl) (all subsections).
Guidance: [HITL and Authorization Guidance](technical-guidance.md#hitl-and-authorization-guidance),
[Human Tasks Versus Effect Gates](technical-guidance.md#human-tasks-versus-effect-gates).

- Extend the approval-centric `checkpoints.rs` runtime to the three kinds
  (`Approval`, `SecurityAuthorization`,
  `IndeterminateEffectReconciliation`) with the full checkpoint record
  ([spec 12.2](spec.md#122-checkpoint-record)); digest-bound grants and the
  reconciliation decision set are new surface, not a refactor — size the
  slice accordingly.
- Grant binding to exact effect intent + argument digest; changed binding
  invalidates; pre-dispatch revalidation ([spec 12.3](spec.md#123-grant-binding)).
- Reconciliation decision set without a generic `Retry`
  ([spec 12.5](spec.md#125-reconciliation-decisions)); `ConfirmedNotExecuted`
  creates a new effect generation.
- Durable timers for SLA/escalation; no auto-approve on timeout for
  sensitive/non-idempotent work ([spec 12.6](spec.md#126-passivation-and-timers));
  full passivation during waits.

**Amended as implemented:**

- **One durable substrate carries all three kinds.** `Approval`,
  `SecurityAuthorization`, and `IndeterminateEffectReconciliation` share the
  checkpoint record, the decision-key deduplication, the timer machinery, and
  passivation; only the decision set and resolution semantics differ per kind.
  The checkpoint is plain durable state on the run's loop state — it commits in
  the *same* transition as the effect it gates (a gated effect never exists
  without its gate) and survives serialization, so the run passivates behind the
  wait with no live task and any later command resumes it. Entity-level
  operation-id deduplication is the primary scenario-11 guard; the checkpoint's
  own bounded `applied_keys` ring is defence in depth for the
  escalate-then-resolve flow, where the checkpoint stays open across decisions.
- **The cryptographic digest is a new algorithm, not a new type.**
  `AgentDigestAlgorithm::Sha256` was added to the existing `AgentContentDigest`
  (inline safe-Rust FIPS 180-4, pinned to the standard vectors, no new
  dependency) because the FNV fingerprint the effect carries is explicitly not
  a security boundary. Both `AgentCheckpoint::open` and
  `AgentCheckpointGrant::validate_for` refuse a non-cryptographic digest
  outright — defence in depth, so a grant that somehow bound the fingerprint
  can never gate dispatch. The grant digest covers the whole canonical request
  encoding, and the dispatcher-side authority *recomputes* it from the intent
  on every attempt, so a changed argument invalidates a stale approval
  (scenario 12) without trusting anything the effect recorded about itself.
- **Declared gates park at commit; guardrail gates refuse at dispatch.** A tool
  binding's `checkpoint_required`/`authorization_required` is projected onto
  `AgentEffectSpec` and the durable `AgentRunEffect`, so the run knows at
  commit time to open the checkpoint and park (`WaitingForApproval`/
  `WaitingForAuthorization`, both fencing `permits_progress`) instead of
  letting the effect reach the authority and fail. A guardrail-driven
  `CheckpointRequired` disposition is discovered dynamically at dispatch, so it
  stays a dispatch-time refusal — it is satisfied by the same grant evaluation,
  but it cannot be a commit-time park. A tool that requires *both* gates opens
  a single `SecurityAuthorization` checkpoint: its grant satisfies either gate,
  and the authority enforces the grant's **kind**, so an approval-kind grant
  never stands in for a security authorization ([spec 12.4](spec.md#124-security-authorization)).
- **The run holds the grant; the real authority revalidates it per attempt.**
  A resolution stores the digest-bound grant on the run's loop state, keyed by
  effect id *and generation*; `dispatch_effects` never hands a gated effect to
  the sink without one, and `AgentEntityAuthority` sources it into the
  authority context, where it is revalidated against the exact intent — and
  the actual 1-based attempt number, threaded through
  `AgentDispatchAuthority::authorize`, so a grant's allowed use count bounds
  retries and a spent grant refuses the next attempt
  (`checkpoint-grant-uses-exhausted`). Revocation is an immediate-safety check
  ordered *before* the checkpoint gate, so a valid approval cannot outrun a
  revocation (scenario 13). Because the grant binds the generation exactly, a
  `ConfirmedNotExecuted` re-invocation of a gated effect re-parks the *new*
  generation behind a fresh checkpoint of the matching kind in the resolving
  transition — without that, the new generation would sit undispatchable with
  no wait left to resolve.
- **Reconciliation waits are checkpoints too, and they never hard-expire.** An
  `Indeterminate` outcome opens the reconciliation checkpoint in the transition
  that recorded the ambiguity, so the wait carries the full spec 12.2 surface.
  Expiry is refused for this kind whatever deadline was stamped — a timer
  cannot make an unknown outcome known — and run cancellation cancels only
  approval-family checkpoints, so the parked ambiguity stays resolvable and
  terminal cancellation projects only after its decision (scenario 57).
  Approval-family expiry denies the gated effect (`checkpoint-expired`); the
  durable `FireCheckpointTimers` command can only escalate or expire, never
  grant. The effect-layer `ResolveIndeterminateEffect` command remains valid
  and retires the checkpoint it bypasses — both paths converge in one shared
  resolution core.
- **`Compensate` schedules a real durable effect across the wind-down fence.**
  A new `AgentRunEffectRequest::Compensation` (target type `compensation`,
  routed to the application-owned `AgentCompensationExecutor`; absent executor
  fails closed) is committed *after* the fence and is the one effect kind a
  winding-down run still flushes; the ambiguous generation settles as
  `Compensated`, the compensation is budget-reserved like a re-authorized
  generation (an unaffordable one refuses the decision and keeps the effect
  parked), and the run reaches its terminal `EffectCompensated` status only
  after the compensation's own outcome settles.

Done when: scenarios 3, 11, 12, and 13 pass, and the reconciliation half of
scenario 57 passes (terminal cancellation only after outcomes are resolved).

### Slice 1.11 — Session memory and context snapshots

Spec: [13.1](spec.md#131-general-requirements),
[13.2](spec.md#132-short-term-session-memory),
[13.3](spec.md#133-agent-private-long-term-memory) (interface only),
[13.5](spec.md#135-memory-context-snapshot),
[13.6](spec.md#136-storage-adapters) (short-term clauses).
Guidance: [Memory Architecture Guidance](technical-guidance.md#memory-architecture-guidance).

- Session-memory trait scoped `(TenantId, AgentId, AgentRunId)` with
  `MemoryOperationId` idempotent append, ordered sequence, classification
  metadata; in-memory implementation in `rakka-agent`.
- Declare (without implementing) the agent-private long-term memory trait
  scoped `(TenantId, AgentId)` so session and snapshot identities cannot
  bake in an incompatible scope; Phase 2 delivers the stores
  ([spec 22](spec.md#22-initial-acceptance-statement) memory note).
- `rakka-agent-postgres` crate: PostgreSQL session store with uniqueness
  constraints making replay harmless; gated tests via
  `RAKKA_POSTGRES_TEST_DSN` like `rakka-persistence-postgres`.
- Immutable `MemoryContextSnapshot` persisted before every model effect;
  retries reuse the snapshot ([spec 13.5](spec.md#135-memory-context-snapshot));
  retrieved memory is untrusted context.
- Rig memory policies (windowing/compaction) may shape history behind the
  Rakka-owned write path only ([spec 10.3](spec.md#103-conversation-memory)).

**Amended as implemented (2026-07-19):**

- **The snapshot lives in a store, not the run's loop state, and the run keeps
  only the reference.** [Spec 13.5](spec.md#135-memory-context-snapshot) requires
  the snapshot to be *immutable* and *reused by a retry*, and the loop already
  carries an `AgentContextSnapshotRef` from slice 1.5. Putting the snapshot
  *content* — a bounded window of session entries — into the run's durable state
  would grow that state per turn, which the slice 1.5 amendment forbids ("content
  does not accumulate in the loop"). The snapshot therefore lives in a separate
  immutable, content-addressed `ContextSnapshotStore` whose `persist` is
  first-writer-wins: the run's transition commits only the reference (unchanged),
  and the store facade assembles and persists the snapshot *outside* the
  transition (I/O is forbidden in a transition, [spec 9.5](spec.md#95-execution-rule))
  before the model effect is handed to the sink. A re-driven settle loads the
  existing snapshot and reuses it, so a concurrent memory write cannot change a
  retried input — scenario 17 — without ever making the durable run record grow.
- **Session appends are an outbox on the run's own state, flushed like task
  history.** [Spec 13.2](spec.md#132-short-term-session-memory)'s append and the
  run's transition are two records — the session store is separate — so committing
  a turn to session memory *inside* the transition that recorded it would be a
  second compare-and-set. The run instead records the turn's entries into a
  bounded `session_outbox` on its loop state at `RecordingTurn`, in the same
  compare-and-set that advanced the phase, and the store facade flushes them
  idempotently on the derived `MemoryOperationId` afterward — the exact argument,
  and the exact pattern, the slice 1.4 amendment makes for task history. A
  re-driven flush after a restart re-appends the same entries at the same
  sequences rather than duplicating them (scenario 16). The task's bounded input
  enters the same outbox as the session's opening `User` entry when the first
  model call is prepared — recorded at turn zero in the same compare-and-set
  that commits the first model effect, and always within bounds, since task
  content and session entries share one inline limit — so the first turn's
  snapshot carries the input the run was created to serve rather than an empty
  session.
- **The memory backend is an optional collaborator, not a new generic.** The run
  entity's `<Store, Effects>` parameters are threaded through the actor, the
  sharding registration, and every test; adding a required `Memory` generic would
  churn all of them and force every unwired run to name a null backend. Instead
  `AgentRunEntityStore::with_memory(AgentRunMemory)` holds the session and
  snapshot stores behind `Arc<dyn …>`, absent by default. An unwired run behaves
  exactly as it did before this slice — it records nothing to the outbox (the flag
  is `self.memory.is_some()`, threaded into `record_turn`), so its durable state
  is byte-identical — which is why the ~90 existing run/effect/checkpoint tests
  needed no change. Session-memory retention is a deployment choice: with no store
  wired, there is no session memory, consistent with content capture being off by
  default ([spec 17.14](spec.md#1714-content-capture-and-redaction), which governs
  *telemetry*, not the authoritative session record).
- **The private long-term trait is declared, and its scope is fixed, but nothing
  implements it.** `AgentPrivateMemoryStore` scoped `(TenantId, AgentId)` and its
  `AgentPrivateMemory` record exist so the session and snapshot identities cannot
  bake in an incompatible scope; the `MemoryContextSnapshot`'s private and
  communal selections are present but empty. Phase 2 delivers the stores without
  reshaping the snapshot record.
- **`rakka-agent-postgres` is a standalone adapter crate, like
  `rakka-persistence-postgres`.** It depends on `rakka-agent` (default features
  off — the stores never touch the model adapter) and is not wired through the
  `rakka` facade, because the agent crates are not yet in the publishable set. Its
  `migrate` takes a session advisory lock before its idempotent DDL (the
  `rakka-a2a` precedent) so concurrent test migrators cannot race the system
  catalogs, and loads fail closed on an unsupported schema version. The gated
  tests were validated against a live PostgreSQL 16.
- **Retention, tombstone, and deletion semantics are deferred, not delivered.**
  The session and snapshot stores ship append/persist and bounded reads only;
  the retention, tombstone, and deletion semantics of
  [spec 13.1](spec.md#131-general-requirements) and the terminal-run retention
  policy of [spec 13.2](spec.md#132-short-term-session-memory) wait on open
  decision 7 and land with the Phase 2 memory slices (slice 2.1 fixes them from
  the first private-memory schema; slice 6.2 validates the flows). Until then a
  deployment retains a run's session and snapshots until it deletes the rows
  itself.

Done when: scenarios 14 and 17 pass against both the in-memory and Postgres
stores. **Done (2026-07-19):** scenarios 14 and 17 pass on both stores, and
scenario 16 (idempotent replayed memory writes) is proven alongside them.
Session-store scenario 14/16 and snapshot-reuse scenario 17 have in-memory unit
tests in `memory.rs` and PostgreSQL gated tests in `rakka-agent-postgres`;
`crates/rakka-agent/tests/session_memory.rs` proves all three end to end through
the run entity.

### Slice 1.12 — A2A surface and typed client

Spec: [14.1](spec.md#141-public-boundary),
[14.2](spec.md#142-task-identity-and-projection),
[14.3](spec.md#143-taskrun-state-mapping),
[14.5](spec.md#145-typed-agent-client),
[7.2](spec.md#72-settings-revisions) (external settings entry).
Guidance: [Client, Events, and Testkit](technical-guidance.md#client-events-and-testkit).

- `rakka-a2a` `agents` feature: `AgentTaskId` <-> A2A `Task.id` mapping,
  durable deduplicated ingress, and the full state projection table of
  [spec 14.3](spec.md#143-taskrun-state-mapping) including the
  `WaitingForInput` row, `Suspended` metadata, and the
  no-terminal-cancellation-during-reconciliation rule.
- Settings commands as a versioned A2A management skill/extension (open
  decision 10), authenticated, entering through the durable inbox of the
  owning Slice 1.2 `AgentEntity`.
- `RakkaAgentClient` facade over the same durable command path — no local
  actor shortcut ([spec 14.5](spec.md#145-typed-agent-client)); replayable
  task/run event subscription reuses the existing A2A event replay
  (coordination events extend this in Phase 5).

**Design decisions resolved (2026-07-20, ahead of implementation):**

- **Open decision 10 is accepted as a versioned management *extension*, not a
  skill.** Settings and administrative commands enter through an
  `AgentExtension` declared in the card's `capabilities.extensions` under a
  stable URI that carries the version; a command is a `message/send` whose
  typed data part is tagged with that URI, and an unsupported or unknown
  required version fails closed. Authorization gates per operation (new
  `A2AOperation` variants), the authenticated principal becomes the
  `SettingsRevision` provenance of [spec 7.2](spec.md#72-settings-revisions),
  and deduplication rides the existing normalized-command derivation into the
  `AgentEntity` inbox's `UpdateSettings` with its `expected_revision` fence.
  An `AgentSkill` was rejected as the carrier because skills have no
  version-negotiation or required semantics — fail-closed versioning would
  move into the payload — and the extension sets the precedent
  [spec 14.4](spec.md#144-agent-to-agent-effects) reuses for the Phase 5
  collaboration metadata. A discovery-only skill entry can be added later
  without changing semantics.
- **A settings command answers with an immediate message, never an A2A task.**
  Durable inbox acceptance and dedup precede the acknowledgement
  ([spec 14.1](spec.md#141-public-boundary)); the response carries the
  accepted revision or the stale-revision conflict. A settings command is not
  a unit of work: [spec 14.2](spec.md#142-task-identity-and-projection) gives
  every created task a distinct `AgentTaskId`, and administrative operations
  must not enter task identity, projection, or the recovery-scenario surface.
  Audit rides `SettingsRevision` provenance, not task history.
- **Open decision 17 is accepted as the equal mapping.** A2A `Task.id` is the
  `AgentTaskId` value verbatim, immutable across assignment, handoff, and
  restart ([spec 6.4](spec.md#64-agenttaskid),
  [spec 14.2](spec.md#142-task-identity-and-projection)); the tenant always
  derives from the authenticated context and is never parsed from the id.
  This does not collide with the substrate surface's existing
  `Task.id` -> run-id mapping because the `agents` feature is a separate
  handler surface.
- **`contextId` is opaque on the agents surface, defaulting to the task's own
  id.** A client-supplied `contextId` is honored as an opaque grouping key;
  absent one, the server assigns the task id. It is documented as
  non-authoritative, so later phases can surface `AgentGoalId` through
  metadata without changing the field's meaning — goal semantics are not
  baked into a public field before goals exist (Phase 3).
- **Client placement follows the crate rule already in force.**
  `RakkaAgentClient` is defined in `rakka-agent` (`client.rs`) over a durable
  command port, and the `rakka-a2a` `agents` feature — which adds
  `rakka-agent` as an optional dependency, one-directional per the
  `crate_shape` guard — provides the A2A-backed transport. The
  no-local-shortcut rule of [spec 14.5](spec.md#145-typed-agent-client) is a
  property of the port's contract and its tests, not of which crate names the
  type.

**Amended as implemented (2026-07-20):**

- **The agents surface is a sibling service, not a parametrization of the
  substrate handler.** The existing request handler is hardwired to the
  workflow substrate's run engine, so `rakka_a2a::agents` adds
  `RakkaAgentA2AService` beside it — generic over the four durable stores
  exactly like the entity facades it drives, holding the same
  `AgentExchangeRouter` seam the entities use to reach each other, and
  reusing the crate's existing trait seams unchanged (`A2ATaskProjectionStore`,
  `A2AAuthorizer`, `A2ATenantResolver`). The `agents` feature implies
  `server` and adds `rakka-agent` as an optional dependency; the reverse
  direction stays forbidden by the `crate_shape` guard, and the new
  `cargo check -p rakka-a2a --no-default-features --features agents` line in
  `scripts/validate.sh` keeps the composition honest.
- **Scenario 1 is the id derivation plus the entity inbox, nothing more.** A
  send without a task id derives its `AgentTaskId` deterministically from the
  tenant and the request's deduplication discriminator — the explicit
  `io.rakka.command.deduplication_key` metadata, or the A2A `message_id` —
  using the substrate surface's generated-task-id derivation, so a retried
  send reaches the same entity with the same
  `AgentOperationId`, and the slice 1.4 operation-id inbox answers with the
  original outcome: one task, one assignment, one run, one turn. An explicit
  deduplication key also converges sends whose message ids differ. The
  end-to-end proof (`tests/agents_surface.rs`) drives the real task, agent,
  and run entities with the deterministic adapter and asserts the run took
  exactly one turn and the history holds one proposal and one acceptance.
- **The projection computes from the authoritative snapshot plus the current
  run condition, and it feeds the existing event replay.** The 14.3 table is
  a pure function over `(AgentTaskStatus, Option<AgentRunStatus>)`: the
  domain's three-way human-wait split (slice 1.5 amendment) drives the
  `INPUT_REQUIRED`/`AUTH_REQUIRED` rows, `Suspended` projects `WORKING` with
  the run condition as bounded metadata, `HandedOff`/`Superseded` never close
  the public task, `WaitingForReconciliation` holds `INPUT_REQUIRED` with the
  stable `indeterminate-effect` reason (which is also how terminal
  cancellation stays unprojectable until the decision), and `UNSPECIFIED` is
  never produced — an unknown future status projects the neutral nonterminal
  `WORKING`. The table has a row-for-row test plus a full-cartesian
  never-`UNSPECIFIED` sweep. Accepted commands and lagging reads project into
  the shared `A2ATaskProjectionStore` through the same
  bootstrap-snapshot/message-heal/status-event idiom `runsync.rs` uses, under
  the shared no-regression rule — which is why the client's replayable
  subscription (cursors, bounded retention, explicit
  `ReplayWindowExpired` resync) is the existing machinery, reused unchanged.
  A client-supplied `contextId` persists in the projection read model from
  the bootstrap event, honoring the opaque-grouping resolution.
- **The management extension landed exactly as resolved, plus the lifecycle
  commands the client owes.** `urn:rakka:a2a-extension:agent-management:v1`
  carries the version in the URI; the envelope re-checks a schema number;
  unknown versions, malformed envelopes, and unauthenticated writes fail
  closed. The v1 command set is `update-settings`, `suspend`, `resume`,
  `terminate`, and `describe` — the lifecycle verbs ride the same extension
  because [spec 14.5](spec.md#145-typed-agent-client) owes them to the typed
  client and they enter the same durable `AgentEntity` inbox with the same
  revision fences. Instantiation, definition publishing, and admission stay
  off the public surface: they are provisioning, owned by the application.
  A domain refusal — stale settings or lifecycle revision, not-instantiated,
  terminated — answers as a structured `Refused` payload in the immediate
  response message so the caller can rebase; only persistence/schema
  failures surface as transport errors. Authorization gates on the new
  `A2AOperation::AgentManagementWrite`/`AgentManagementRead`, and the card
  builder gained a general `extensions(...)` declaration setter (it still
  advertises nothing by default).
- **The client port follows the crate's boxed-future idiom, and the shipped
  transport is the service core.** `AgentClientTransport` in
  `rakka-agent/client.rs` uses the same hand-rolled
  `Pin<Box<dyn Future>>` shape as the crate's store traits (no `async-trait`
  dependency), with a bounded client vocabulary that names no A2A type.
  `A2AAgentClientTransport` implements it over `RakkaAgentA2AService`, so a
  client call takes the identical normalize → authorize → durable-inbox →
  settle → project path as a network caller; `run_task` polls to terminal
  with no retained server-side residency. `rakka-agent` gained `tokio` as a
  lib dependency for the poll timer.
- **Two continuations are deferred with stable refusals, not silently.**
  Input delivery to an existing task (`message/send` naming a `task_id`)
  refuses with a stable reason until human-owned task results land
  (Phase 5, [spec 8.12](spec.md#812-human-owned-tasks)); binary and URL
  message parts refuse until the substrate's artifact strategy is adapted
  for agent task input. The axum/SDK route binding for the agents surface is
  also deferred: the service core takes and returns the A2A SDK
  request/response types, so the `RequestHandler` wrapper is mechanical and
  lands with the edge integration that first mounts it.

Done when: scenario 1 passes (duplicate A2A task messages create one task,
one run, one turn) and the projection table has a test row-for-row.
**Done (2026-07-20):** scenario 1 passes end to end over the real entities
(`crates/rakka-a2a/tests/agents_surface.rs`), the 14.3 table is proven
row-for-row with a full-cartesian never-`UNSPECIFIED` sweep
(`agents/projection.rs`), and the management extension, typed client, and
cancellation projection are proven over the same wiring.

### Slice 1.13 — Observability baseline and operational queries

Spec: [17](spec.md#17-audit-and-observability) (17.1-17.9, 17.12-17.18 as
they apply to M1 signals), [17.18](spec.md#1718-authoritative-operational-queries-and-observability-views).
Guidance: [Agent Observability Architecture Guidance](technical-guidance.md#agent-observability-architecture-guidance).

- Bounded trace segments with persisted W3C context and links across every
  durable boundary this phase created (reuse `trace_context.rs`); no span
  open across a wait ([spec 17.4](spec.md#174-bounded-trace-segments),
  [17.5](spec.md#175-durable-trace-context)).
- Structured decision events ([spec 17.7](spec.md#177-agent-decision-observability))
  and the M1 rows of the span model ([spec 17.6](spec.md#176-required-span-model)).
- Bounded metrics per [spec 17.12](spec.md#1712-metrics); no IDs in labels.
- Content capture disabled by default ([spec 17.14](spec.md#1714-content-capture-and-redaction)).
- `otel` feature: pinned GenAI convention mapping over the existing OTLP
  bridge (`otlp.rs`), extending the bridge additively where fields are
  missing ([spec 17.17](spec.md#1717-otlp-and-collector-boundary)).
- `AgentOperationalSnapshot` authoritative point query from durable state,
  correct with telemetry unavailable ([spec 17.18](spec.md#1718-authoritative-operational-queries-and-observability-views)),
  plus the session view assembled by `AgentRunId`.

**Design decisions resolved (2026-07-20, ahead of implementation) — the
trace-context schema retrofit:**

- **Telemetry context is additive, defaulted, and never fail-closed.** No
  observability signal is a correctness source
  ([spec 17.1](spec.md#171-signal-roles-and-correctness-boundary)), and that
  has a schema corollary: an absent trace context must always be a legal
  read — "nothing recorded", never "uninterpretable record". Every
  retrofitted record therefore gains one `#[serde(default)]`
  `AgentTelemetryContext` field, a pre-1.13 encoding decodes to the empty
  context, and no record kind bumps its schema version — the fail-closed
  N/N+1 windows of `schema.rs` guard against reinterpretation of correctness
  fields, which this is not
  ([spec 20](spec.md#20-compatibility-and-migration)). The precedent is
  already in force: `AgentRunEffect` gained `execution_policy` (Slice 1.8)
  and `guardrail_revision` (Slice 1.10) as defaulted optionals inside v1.
  Writes stay strict where reads are permissive — inbound context passes the
  existing `trace_context.rs` validation, and a malformed remote value is
  dropped at the boundary, never persisted
  ([spec 17.5](spec.md#175-durable-trace-context)).
- **Four records carry the field, and one extraction point feeds them.** Only
  a record that crosses or parks at an asynchronous durable boundary carries
  context. `AgentExchangeEnvelope`: the initiator stamps its current
  segment's context when the exchange commits to its journal; a re-drive
  re-sends the persisted envelope, so the original context rides every
  re-drive and the receiver's acceptance span links to the initiating
  segment — one field covering inbox acceptance for all
  [spec 9.8](spec.md#98-inter-entity-choreography) traffic (the reply does
  not carry it: it returns to the initiator, who still holds the journal
  entry). `AgentRunEffect`: the schedule -> dispatch boundary — the
  dispatch-ticket conversion stops stamping an empty context and forwards
  the run's context plus a link to the scheduling segment into the
  substrate's existing `AgentEffect.telemetry_context` (no workflow-crate
  schema change), and a new generation appends a link to the prior attempt.
  `AgentCheckpoint`: captures the `checkpoint.open` segment's context so
  resolution produces the
  [spec 17.11](spec.md#1711-hitl-authorization-wait-and-recovery-observability)
  double link — parked span from this field, trigger span from the
  resolution command's inbound context; checkpoint timers need nothing new
  because the substrate timer entry already persists context.
  `AgentLoopState`: the plain-passivation boundary — a run quiescent with no
  checkpoint or effect open still owes its resume span a link to the segment
  that persisted the wait, and the loop state is the record that versions
  independently precisely because it is the part that evolves. A2A ingress
  adds no record: the agents surface extracts W3C context before durable
  acceptance and hands it to the normalized-command derivation, and the
  typed client injects on egress. Context always flows with commands — an
  entity never invents one, and a command without context starts a root
  segment.
- **The exclusions are deliberate.** `SessionMemoryEntry` and
  `MemoryContextSnapshot` are content records, not boundaries — a snapshot
  is content-addressed, and ambient trace context would make identical
  content hash differently; the span carries the snapshot digest instead
  ([spec 17.10](spec.md#1710-memory-and-retrieval-observability)).
  `AgentModelTurn` is produced inside one bounded dispatch attempt whose
  effect record holds the correlation. Admission decisions and the escrow
  ledger are synchronous sub-steps of an already-open segment —
  [spec 17.6](spec.md#176-required-span-model) gives them bounded
  spans/events, not propagation. Definition, settings, and setup revisions
  outlive any legitimate trace; they correlate through the accepting
  command's ingress span, `SettingsRevision` provenance, and audit events
  ([spec 17.13](spec.md#1713-structured-logs-runtime-events-and-audit)).
  Task history entries and the exchange journal correlate by operation and
  causation id, which is what the scenario-21 session view joins on.
- **Baggage is persisted empty at M1.** The shared context type has the
  field, but the agent domain writes no baggage:
  [spec 17.15](spec.md#1715-baggage) restricts it to policy-approved bounded
  classes, no M1 component consumes any, and externally received baggage is
  untrusted and never persisted.
- **Proof obligations.** Scenario 23's context survival comes from the four
  persisted fields, and its "without changing effect behavior" clause falls
  out of the never-fail-closed rule — with a direct test that a context-less
  v1 record replays identically through the effect path. Scenario 22's
  double link is the checkpoint field plus the resolution command's context.
  `schema_compatibility.rs` gains cases proving pre-1.13 encodings of all
  four retrofitted records decode under the unchanged v1 window with the
  empty context.

**Design decisions resolved (2026-07-20, ahead of implementation) —
segments, signals, and queries:**

- **Open decision 11 is accepted, with the loop's own crank as the segment
  boundary.** A bounded trace segment is one entity activation: durable
  command acceptance through the settle pass that dispatches owed effects
  and drives owed exchanges — the Slice 1.5 "the loop cranks; it does not
  run" contract already names the boundary. The A2A `SERVER` span is its own
  protocol segment ending with durable acceptance, and the activation it
  caused links to it. The model call is not part of the turn segment: it is
  an asynchronously dispatched effect, so it lives in the dispatcher's
  `CONSUMER` segment, where one logical provider operation — including the
  provider's automatic in-call retries — stays one GenAI span with retry
  events ([spec 17.8](spec.md#178-model-and-provider-observability)). Every
  durable asynchronous boundary (journal commit -> receiver acceptance,
  effect schedule -> dispatch attempt, checkpoint open -> resolution,
  passivation -> reactivation) splits segments, joined by the context and
  links the retrofit above persists.
- **Open decision 12 is accepted as structurally off at M1.** Content
  capture is not a flag defaulting to false; no M1 code path emits content.
  Telemetry records bounded metadata only — counts, sizes,
  `AgentContentDigest` fingerprints, `RedactionStatus`, and artifact
  references, all vocabulary the substrate already defines. The scoped
  opt-in policy object of
  [spec 17.14](spec.md#1714-content-capture-and-redaction) (tenant, purpose,
  redaction, retention, audit) is deferred to Phase 6 with the rest of
  production telemetry validation. Deferring the hook entirely is what makes
  scenario 25 provable by construction rather than by configuration.
- **Open decision 13 is accepted; the sampling policy ships as pinned
  configuration, not code.** Rakka stays SDK-neutral, so sampling belongs to
  the application SDK and the Collector: the shipped artifact is the
  existing pinned agent-workflow Collector topology
  (`kubernetes-otel-collector-topology.yaml`, `otel-collector-local.yaml`,
  already validated by the `rakka-k8s` topology test), extended where needed
  with the [spec 17.16](spec.md#1716-sampling) retain list — errors,
  indeterminate effects, security denials, escalations, recovery failures,
  slow traces. What the crate owes in code: sampling-relevant bounded
  attributes at span creation, context propagation independent of any
  recording decision, and the scenario-24 proof that sampling changes no
  metric, audit record, runtime event, or durable transition.
- **The metric vocabulary splits by layer, not by crate reach.** The
  substrate keeps measuring the substrate: the `rakka.agent_workflow.*`
  instruments (inbox, outbox, dispatcher backlog and in-flight, timers,
  adapters) are untouched, and an agent effect riding that dispatcher is
  counted there as transport. `rakka-agent` adds `rakka.agent.*` instruments
  only where an agent-domain durable transition commits — decisions, turns,
  waits, admission, budget, effect outcomes and indeterminates, recovery,
  residency gauges — so one physical dispatch is never the same concern in
  both vocabularies: the substrate measures the pipe, the domain measures
  the outcome. The substrate's bounded/forbidden label guards are reused
  unchanged, and no instrument labels an identifier
  ([spec 17.12](spec.md#1712-metrics)).
- **The snapshot's cancellation vocabulary is complete from M1; its emitters
  are not.** `AgentOperationalSnapshot` defines all six
  [spec 17.18](spec.md#1718-authoritative-operational-queries-and-observability-views)
  progress states as a non-exhaustive enum from the first commit — the
  contract is fixed even where a state is not yet reachable. M1 derives
  `NotRequested`, `Requested`, `Quiesced`, `WaitingForReconciliation` (the
  run's `Cancelling` holding an ambiguous consequential effect,
  scenario 57), and `Completed` from durable run/effect/checkpoint state;
  `Propagating` becomes derivable when delegation lands (Phase 4). Per
  [spec 8.7](spec.md#87-cancellation-failure-and-waiting), no state is ever
  inferred from mere acceptance of a cancellation request.
- **Decision events get their own agent-domain record and sink; the A2A
  replay stays the public stream.** The substrate's runtime events are
  graph-shaped — node kinds, workflow correlation — so the
  [spec 17.7](spec.md#177-agent-decision-observability) decision events do
  not squeeze into them. `observability.rs` defines the bounded decision
  record (kind, source, turn index, loop phase, revisions, budget outcome,
  safety class, stable reason code) and a sink trait mirroring the
  substrate's contract — emitted only after the durable transition, per-run
  monotonic sequence, deduplicated per transition — with the crate's usual
  in-memory implementation for tests. The scenario-21 session view joins
  durable state, decision events, and trace links by `AgentRunId`; the
  Slice 1.12 replayable subscription (cursors, bounded retention, explicit
  resync) remains the public streaming surface and gains no second
  machinery.

**Amended as implemented (2026-07-21):**

- **Two records joined the retrofit's carrier list as the flow was wired.**
  `AgentTaskCreation` carries the ingress context (context flows with
  commands), and the task's materialized state holds it — an exclusion-list
  amendment forced by a fact the resolution had not weighed: the assignment
  is decided in a *later* transition than the creation, so the envelope it
  owes can only carry the ingress cause if the task state kept it. Same
  rules as every carrier: `#[serde(default)]`, no version bump, never read
  to decide anything. The choreography host propagates the causing
  exchange's context onto owed envelopes that have none (accept and settle),
  so the chain creation -> assignment -> run acceptance flows with no
  per-participant work, and the run participant records the accepted
  exchange's context into its loop state in the same compare-and-set.
- **The operational snapshot is content-redacted.** The scenario 25 sentinel
  sweep caught the run projection's proposal/accepted-result/feedback riding
  into the point answer; the query strips them and reports the bounded
  `has_pending_proposal` fact instead — content stays in durable state and
  artifacts, the observability surface gets labels, counts, and references.
- **Ingress and egress are the W3C text-map keys on A2A request metadata.**
  `normalize_agent_send`/`normalize_agent_cancel` extract
  `traceparent`/`tracestate` case-insensitively before anything durable
  happens, dropping malformed context whole without refusing the send; the
  typed client's A2A transport injects the caller's context under the same
  keys. The HTTP-header edge binding lands with the deferred route mounting
  of slice 1.12.

Done when: scenarios 21-26 and 56 pass.
**Done (2026-07-21):** all seven scenario proofs pass — 21 and 56 over the
real entities (`tests/decision_events.rs`, `tests/operational_query.rs`), 22,
23, 24, 25, and 26 over the traced end-to-end flow
(`tests/trace_scenarios.rs`, with the schema half of 23 in
`tests/telemetry_context.rs` and the metric half of 25 in
`tests/agent_metrics.rs`); the slice 1.14 regression re-proves the set under
fault injection.

### Slice 1.14 — Recovery suite, fault injection, and M1 acceptance

Spec: [15](spec.md#15-passivation-recovery-and-shard-movement),
[18](spec.md#18-required-recovery-scenarios),
[22](spec.md#22-initial-acceptance-statement).

- Complete the M1 scenario suite: 4, 19, 35, 46 plus a full-regression run of
  every M1 scenario landed by earlier slices, with dispatcher/owner kill
  injection at every durable boundary (extend `testkit.rs` crash points).
- End-to-end example crate (`publish = false`) demonstrating the
  [spec 22](spec.md#22-initial-acceptance-statement) initial statement with
  the deterministic model adapter; document expected stdout.
- Measure per-turn durable-transition overhead (persist -> dispatch ->
  result -> reactivate) and record the numbers in the example README; this
  validates the durable-boundary design before Phases 3-5 multiply write
  load (design-review follow-up, not a spec requirement).
- Walk the [spec 22](spec.md#22-initial-acceptance-statement) checklist item
  by item; fix or explicitly defer anything unmet.

Done when: every M1 scenario passes under fault injection and the acceptance
checklist is demonstrated by the example.
**Done (2026-07-22):** the four owed scenarios are proven — 4
(`tests/stale_owner_fencing.rs`: a stale owner's write is rejected by the
revision fence on both the run and the task, then answered from the
authoritative record; the transport half of movement remains scenario 60's
2-node proof), 19 (`tests/terminal_run_recovery.rs`: terminal recovery is
*writeless*, proven by arming a permanent crash point so any attempted write
fails loudly), 35 (`tests/goal_passivation.rs`), and 46
(`tests/idle_agent_reactivation.rs`) — and the full M1 regression re-proves
every previously landed scenario under owner-kill injection at every durable
write via the `sweep_crash_points` testkit harness, with every store class
(run, task, agent, workflow outbox, dispatcher fleet) crash-armable. Notes:

- **Scenario 35's M1 interpretation is pinned in the test doc.** `goal.rs`
  stays a doc-only stub, so "an `Active` goal" is a non-terminal root task
  carrying an `AgentGoalId` and "its waiting runs" is its run parked on the
  durable approval wait; all three real sharded entity types passivate to a
  local entity count of zero and one durable decision command reactivates
  the correct owner exactly once. Timer-driven wakes stay in phase 3.
  Scenario 46 uses the real idle timer and is open decision 20's proof: no
  lifecycle command is issued, and the dependency-outcome command is the
  durable trigger the later coordination couriers will inject.
- **Scenarios 35/46 and the example run over `LocalShardedExchangeRoute`**,
  a testkit transport mirroring exactly the local arm of the production
  `ShardedExchangeRoute`, so the M1 gate never silently skips in sandboxes
  the TCP route cannot run in.
- **The sharded run factory gained `with_memory`.** The acceptance walk
  found the gap: metrics and decision events were wired into the sharded
  factory in slice 1.13, but session memory was not, so a sharded run —
  the production driver — could not persist the context spec 22 requires.
  Fixed in-slice, mirroring `with_metrics`.
- **The per-turn durable-write budget is pinned exactly** in
  `tests/effect_dispatch.rs`: 10 run-store, 8 task-store, and 3
  workflow-outbox compare-and-sets per clean accepted turn (creation
  through settled escrow; ~2 ms release-build wall time over in-memory
  stores, recorded in the example README). The assertions are deliberate
  change-detectors; the fleet store is unbudgeted because its lease
  bookkeeping scales with worker churn, not turns.
- **The acceptance statement is demonstrated end to end** by
  `examples/durable-agent-acceptance`: one sharded Rakka Agent over real
  `ClusterSharding` and all three entity types, the in-process A2A service
  core, and the production dispatcher fleet, printing one stable line per
  spec 22 bullet; the in-crate test asserts the transcript verbatim against
  the const the README quotes. The item-by-item walk:

| Spec 22 item | Status | Proof |
| --- | --- | --- |
| versioned settings | Met | example line 1; `agent_entity.rs` |
| A2A task -> one `AgentTaskId` + initial run | Met | example line 2; scenario 1 swept (`agents_surface.rs`) |
| typed result validated before completion | Met | example line 3; scenario 40 swept (`task_results.rs`) |
| fail-closed admission; widening rejected | Met | example line 4; scenario 53 swept (`autonomy_admission.rs`) |
| budgets reserved/settled durably | Met | example line 5; scenarios 52/61 swept (`escrow_ledger.rs`) |
| addressable while fully passivated | Met | example line 6; scenario 35 (`goal_passivation.rs`) |
| bounded Rig model turn through a dispatcher | Met | example line 7; `effect_dispatch.rs` |
| correlated trace segments -> one session view | Met | example line 8; scenarios 23-25 swept (`trace_scenarios.rs`), scenario 21 swept (`operational_query.rs`, `decision_events.rs`) |
| bounded metrics, no high-cardinality IDs | Met | example line 9; scenario 25 (`agent_metrics.rs`) |
| short-term session context persisted | Met | example line 10; scenarios 14/16/17 swept (`session_memory.rs`) |
| each effectful tool call a separate durable effect | Met | example line 11; `run_entity.rs` |
| pauses/passivates at an approval gate | Met | example line 12; scenario 3 swept (`checkpoint_run.rs`) |
| recovers after owner and dispatcher pod loss | Met | example line 13; the owner-kill sweeps + scenarios 5-9 |
| ambiguous non-idempotent -> indeterminate, no auto re-invoke | Met | example line 14; scenario 9 (`effect_dispatch.rs`) |
| resume only after deduplicated reconciliation | Met | example line 15; scenarios 3/11 swept (`checkpoint_reconciliation.rs`) |
| authoritative snapshot without telemetry | Met | example line 16; scenario 56 swept (`operational_query.rs`) |
| no content/credentials in default telemetry | Met | example line 17; scenario 25 (`trace_scenarios.rs`) + example sentinels |
| correct under unavailable export, loss visible | Met | example line 18; scenario 26 (`trace_scenarios.rs`) |

  No item is deferred. The standing deferrals recorded by earlier slices are
  unchanged: session retention/tombstones wait on open decision 7 (Phase 2,
  slice 1.11 note), the content-capture opt-in policy object is Phase 6
  (slice 1.13 note), and the A2A route mounting, binary/URL parts, and
  input-to-existing-task continuations keep their stable refusals (slice
  1.12 note).

---

## Phase 2 — M2 Durable Memory

Milestone: M2. Acceptance: the memory note in
[spec 22](spec.md#22-initial-acceptance-statement) — scopes and interfaces
were fixed in Phase 1; this phase delivers the stores.
Scenarios owed: 15, 16, 18, 20.

Open decisions to resolve: 2 (communal boundary), 3 (claims start
`Proposed`), 7 (retention), 8 (graph backend selection deferred until the
SPI is proven).

### Slice 2.1 — Agent-private long-term memory

Spec: [13.3](spec.md#133-agent-private-long-term-memory),
[13.1](spec.md#131-general-requirements).

- Private-memory trait scoped `(TenantId, AgentId)`; run provenance recorded
  without widening access; full record shape per
  [spec 13.3](spec.md#133-agent-private-long-term-memory).
- Promotion/consolidation from session memory as idempotent durable effects;
  CAS or idempotent append for concurrent runs (open decision 1).
- Retention, tombstone, and deletion semantics from the first schema.

**Amended as implemented (2026-07-23):**

- **A blind overwrite is unrepresentable, and a replay answers its original
  result.** `upsert` takes an explicit `PrivateMemoryExpectation` — `Absent`
  (a create, idempotent on its operation id) or `Revision(n)` (a
  compare-and-set update) — so open decision 1's M2 half is the type system's
  answer, not a runtime flag. The store keeps an operation ledger: a replayed
  create returns the *original* stored result even after later updates moved
  the record, a stale expectation is refused (`memory-revision-conflict`)
  rather than overwriting a concurrent run's write (scenario 15), and the
  store — not the caller — stamps every revision. The record was reshaped in
  place at schema version 1 under the unreleased-branch rule the 1.7
  amendment recorded; nothing outside the workspace ever persisted the
  slice 1.11 declaration shape.
- **Deletion is final in both directions of time.** Tombstone and delete are
  separate idempotent operations with their own operation ids, and both erase
  the ledger's earlier content payloads for the memory: a replayed old write
  fails closed (`memory-operation-erased`) instead of resurrecting withdrawn
  content. The tombstone keeps a content-free stub — identity, digest, and
  provenance intact, visible in scope through `get` and an opt-in audit
  listing — because the withdrawal itself must stay auditable
  ([spec 13.1](spec.md#131-general-requirements)); a cross-scope read of
  anything is byte-identical to reading a memory that never existed
  (scenario 18). Expiry is a read-visibility rule from the instant itself;
  the bounded `purge_expired` sweep is deployment-invoked, never a resident
  poller.
- **Promotion is a durable effect whose identities are all derived.** A
  deduplicated `AgentRunEntityCommand::PromoteMemory` selects a bounded
  contiguous range of the run's own durably assigned session sequences and
  commits one `MemoryPromotion` effect in a bounded transition —
  budget-reserved, wind-down-fenced, no I/O
  ([spec 9.5](spec.md#95-execution-rule)); the settle pass flushes owed
  session entries before dispatching, so the selection is durably readable
  before the dispatcher-side `SessionMemoryPromotionExecutor` reads it. The
  promoted memory id derives from (agent scope, source entry, kind); the
  upsert operation id from (run scope, effect, generation, entry). Any
  replay of a generation therefore converges on the same records, a distinct
  later promotion of the same entry *converges on the existing memory*
  (session entries are immutable; a withdrawn memory stays withdrawn) rather
  than duplicating or updating it, and two runs of one agent derive disjoint
  memories. Consolidation is the same effect naming a
  `AgentMemoryConsolidationTarget`: a compare-and-set update of exactly one
  memory from exactly one entry.
- **A failed promotion does not wind the run down.** The generic
  failed-generation arm fences the run and records a terminal reason; a
  `MemoryPromotionCall` failure deliberately records only the effect's
  failure, because [spec 13.1](spec.md#131-general-requirements) makes memory
  never the correctness source — a memory-store outage must not kill a live
  run. The initiator observes the failed effect and may re-issue under a new
  operation id. The run keeps a bounded receipt ring
  (`AgentLoopState::memory_promotions`, identities and revisions only, the
  store is the source of truth); a promotion result racing the run's
  completion is refused as terminal and treated as convergence — memory
  persists, the receipt does not — which the delivery layer already treats
  as the fence doing its job.
- **The guardrail `MemoryIngress` evaluation point is explicitly deferred to
  slice 2.2.** The boundary's meaning is "before retrieved memory enters a
  model context" — the retrieval flow, which 2.2 owns. Adding it to
  `AGENT_EVALUATED_GUARDRAIL_BOUNDARIES` on the promotion path alone would
  let a required MemoryIngress-only stage satisfy coverage while never
  running where retrieval happens — coverage-as-fail-open, the exact thing
  `validate_covers` exists to refuse. Deferral is safe: promoted content is
  verbatim session content, inert in the store, and cannot reach a model
  context without passing the retrieval-side evaluation when it lands;
  meanwhile an envelope requiring a MemoryIngress stage keeps failing closed
  (`guardrail-stage-unevaluated`).
- **Open decision 7 is resolved as config enforced inside the purge call.**
  `SessionRetentionPolicy` (30-day bounded default, legal hold) is per-tenant
  deployment configuration passed to each call, never stored per row —
  retention is evaluated at sweep time, and a policy frozen at write time
  could never tighten. The idempotent `purge_run` landed on **both**
  `SessionMemoryStore` and `ContextSnapshotStore`, because snapshots embed
  copies of session content and purging one without the other would not
  discharge retention; held and not-yet-due are reported as values so a
  fleet sweep never aborts, and export is the ordinary bounded cursor read
  taken before the purge.
- **The PostgreSQL store writes each operation as one data-modifying-CTE
  statement.** A single statement is a single implicit transaction on the
  shared pipelined client, so the operation-ledger row and the memory-row
  mutation commit or fail together — no crash window between them, and no
  raw `BEGIN`/`COMMIT` held on a shared connection. The `WHERE revision = $n`
  update is the genuinely concurrent compare-and-set, proven over two live
  connections. The crate's advisory migration lock moved to its own id
  (`982_451_881`); the previous value collided with
  `rakka-sharding-postgres`, needlessly serializing two subsystems'
  migrations in a shared database.

Done when: scenario 15 passes and the private-memory half of scenarios 16
and 18 passes. **Done (2026-07-23):** all three pass at the store level
(`memory` unit tests, in-memory and DSN-gated PostgreSQL 16), end to end
through the real run entity including the owner-kill crash sweep over the
command → effect → upsert → settle chain
(`crates/rakka-agent/tests/private_memory_promotion.rs`), and through the
production dispatch pipeline with the authority's promotion arm under
dispatcher loss mid-pass (`tests/effect_dispatch.rs`).

### Slice 2.2 — Vector retrieval adapter

Spec: [13.3](spec.md#133-agent-private-long-term-memory),
[13.6](spec.md#136-storage-adapters).

- `pgvector` retrieval in `rakka-agent-postgres`: embeddings as rebuildable
  derived data with model/dimension/version metadata; source content
  preserved independently.
- Tenant and `AgentId` filters enforced in schema and query even where it
  costs index performance; recall characteristics documented.
- Retrieval feeds `MemoryContextSnapshot` only through the Slice 1.11 path.
- Seams slice 2.1 left for this slice: the snapshot's
  `private_memory: Vec<MemoryEntryId>` selection field predates
  `AgentPrivateMemoryId` and must be reconciled when retrieval first fills
  it; the guardrail `MemoryIngress` boundary joins
  `AGENT_EVALUATED_GUARDRAIL_BOUNDARIES` here, where the retrieval flow it
  describes is evaluated (the 2.1 amendment records why not earlier); the
  record's `MemoryEmbeddingRef` metadata is already persisted, content-free,
  for the vectors this slice derives.

**Amended as implemented (2026-07-24):**

- **The snapshot embeds selected content, not just ids.** The reconciliation
  the slice text ordered went further than a type swap: `private_memory`
  became `Vec<SnapshotPrivateMemory>` — id, revision, kind, the *exact
  content used*, digest, classification, confidence, relevance, embedding
  metadata, and the recorded ingress transforms/reports — because
  [spec 13.5](spec.md#135-memory-context-snapshot) requires "selected private
  memory IDs and exact content/references", and an id alone cannot make a
  retried model input immune to index drift: a retry that re-read the store
  by id would observe whatever the id points at *now*. Content in the
  snapshot plus first-writer-wins persistence is what discharges the
  done-when test. The reshape stayed at snapshot schema version 1 under the
  unreleased-branch rule (every record written so far carries an empty
  selection, which still loads); 2.1's purge-both-stores retention argument
  now covers private content embedded in snapshots unchanged — and a memory
  tombstoned *after* a snapshot embedded it keeps that embedded copy until
  the run's snapshot retention purges it, immutability over withdrawal, the
  same rule redacted session entries follow.
- **Retrieval is a settle-pass concern with a required guardrail chain.** The
  Rakka-owned seam (`AgentPrivateMemoryRetriever`, `AgentMemoryEmbedder`,
  `rakka_agent::retrieval`) rides `AgentRunMemory` as an
  `AgentMemoryRetrieval` bundle whose memory-ingress chain is a *required*
  constructor argument — a wired retriever can never silently skip the
  boundary; a no-stage deployment passes an empty chain explicitly. The
  query is derived deterministically from the just-assembled session window
  (never a second store read), every returned record is re-checked
  fail-closed against the query's own pre-ranking filter table — duplicates,
  inadmissible classifications, and invalid records are rejected — and the
  ingress evaluation runs per record with the memory's identity on the
  guardrail context. The re-check's limit is documented rather than
  overstated: *scope* is the one clause it cannot cover, because an
  `AgentPrivateMemory` carries no tenant or agent, so a wrong-scope record is
  indistinguishable from a correct one by the time the assembly sees it.
  Answering only for the addressed scope therefore stays the retriever's own
  obligation, and the one clause a backend must prove with its own tests
  (scenario 18). Outcomes: block drops that
  record only; a transform's output is what the snapshot embeds, recorded
  with its stage revision, and a retry reuses it *structurally*;
  report-only records the finding on the selection; require-checkpoint is a
  fail-closed drop, because no checkpoint plumbing exists at snapshot
  assembly and memory must never gate liveness (confirmed decision). Blocked
  and checkpoint-refused records are deliberately absent from the snapshot —
  absence is the decision, and a reason code for absent content would leak
  what was blocked into model-adjacent data — so the new bounded
  `rakka.agent.memory.retrievals` / `rakka.agent.memory.ingress.outcomes`
  counters are their visibility. A retriever outage degrades the turn to an
  empty selection with the attempted retrieval still recorded
  ([spec 13.1](spec.md#131-general-requirements) over 13.5, the exact
  argument of 2.1's failed-promotion amendment); the degraded turn keeps its
  empty selection forever, because first-writer-wins determinism is the
  stronger promise. Coverage note: the dispatch authority's
  `validate_covers` cannot see the retrieval bundle's chain, so a deployment
  must wire the same chain into both — documented on both seams; a
  deployment with no retrieval wired is not fail-open, because nothing
  crosses the boundary at all.
- **Nothing stamps `AgentPrivateMemory.embedding` in this slice.** A
  compare-and-set stamp from the indexing path would bump the revision the
  just-derived vector was keyed to (invalidating its own `source_revision`
  fence) and race live runs for nothing the derived row does not already
  record. The derived row *is* the model/dimensions/version record;
  `RetrievedPrivateMemory.embedding` carries it into the snapshot selection.
  The record field remains for deployment writers that know their embedder
  configuration.
- **The pgvector adapter separates its migration and lets the join carry
  reveal-nothing.** `VECTOR_MIGRATION_SQL` (extension + a typmod-less
  `vector` table keyed `(tenant, agent, memory_id)` with a per-row dimension
  check, so one shared migration serves any embedder) is applied only by the
  retriever's `migrate()` under the crate's existing advisory lock; the
  three 2.1 stores and `MIGRATION_SQL` are untouched and stay green on
  databases without pgvector — which is also why the 2.1 tombstone/delete/
  purge CTEs were *not* extended to touch the derived table. Instead the
  retrieval statement inner-joins the authoritative row on scope,
  `source_revision = revision`, tombstone, and expiry — every filter the
  query carries, classification *and* the confidence floor included, a
  `WHERE` predicate ahead of the `ORDER BY` distance — so a leftover vector
  row is unretrievable in any scope even mid-crash between a delete and its
  deindex, and eventual consistency manifests as absence, never as ranking
  current content by stale geometry. Both policy columns are denormalized
  onto the derived row for exactly that reason (a predicate can only sit
  ahead of the `ORDER BY` if its column is in the ranked table), and neither
  can go stale, because the revision fence makes a row whose authoritative
  policy metadata has moved a non-candidate outright. Enforcing either one
  only in the adapter's post-decode re-check would have been a *post-`LIMIT`*
  drop: the record it removes has already consumed a result slot, so a
  retrieval would answer short of what the corpus holds — the review finding
  this amendment records as fixed.
  Deployment-invoked maintenance (`index_memory`, paged `reindex` as the
  spec 13.3 rebuild path, `deindex_memory`/`purge_orphaned` as residual-row
  hygiene) mirrors `purge_expired`'s no-resident-sweeper pattern, and
  `reindex` pages *past* a record it cannot decode — advancing its cursor on
  each row's own id before any fallible step and counting the record into
  `ReindexPage::failed` — because propagating instead would wedge the sweep
  on the same page forever and take the rebuild path offline for that agent
  during exactly the rolling upgrade that produces unreadable records.
  Vectors
  bind as text literals (`$n::text::vector`, no new dependency; f32
  shortest-round-trip formatting is exact through pgvector's parser, proven
  server-side by zero self-distance), failing closed on non-finite or
  wrong-length embedder output.
- **Recall characteristics: exact scan ships; ANN is a documented opt-in.**
  The `(tenant, agent)` primary-key prefix bounds the candidate set to one
  agent's corpus and the distance is exact within it — recall 1.0, cost
  linear in the agent's live indexed corpus, the right default because
  pgvector's approximate indexes post-filter their candidates and silently
  lose recall under exactly the scope predicate spec 13.6 makes mandatory.
  The module rustdoc documents the expression-HNSW opt-in (deployment DDL,
  fixed-dimension cast, pgvector ≥ 0.8 iterative scans to restore recall
  under filters). Redacted and artifact-backed memories are not semantically
  indexed in v1 (the adapter never loads artifact bytes); they surface as
  visible skips in `ReindexPage`.
- **CI now exercises the crate, on every pull request.** The postgres job's
  service image moved to `pgvector/pgvector:pg16` (a drop-in postgres:16 with
  the extension package) and gained `cargo test -p rakka-agent-postgres`. On
  its own that was not enough to close the pre-existing gap where even 2.1's
  DSN-gated tests ran nowhere but developer machines: the job was still
  gated to `workflow_dispatch` with an opt-in input, so it ran only when
  someone remembered to ask. The gate is now `github.event_name !=
  'workflow_dispatch' || inputs.run_postgres` — always on pull requests and
  pushes, still skippable on a manual run. What these suites prove cannot be
  proven another way (the genuinely concurrent compare-and-set race,
  cross-scope invisibility, the filter-before-ranking predicates are
  properties of live SQL), and this slice is the evidence: both review
  findings it fixes were defects only a live database exposes. The pgvector
  tests additionally probe `pg_available_extensions` and skip with a message
  on a plain database, so a DSN without the extension keeps the crate green.

Done when: retrieval isolation tests pass and a snapshot-reuse test proves
index drift cannot change a retried model input (extends scenario 17).
**Done (2026-07-24):** isolation holds at the reference-retriever, run-entity
(`crates/rakka-agent/tests/private_memory_retrieval.rs`), and pgvector
(DSN-gated, per-tenant/per-agent with empty-scope indistinguishability)
levels; the drift test re-drives a persisted turn after a CAS update, a
tombstone, *and* a retriever upgrade and reloads the snapshot byte-identical;
the ingress boundary's outcome table and the 2.1 coverage flip are proven in
`tests/memory_ingress_guardrails.rs`; the pgvector suite passes against
`pgvector/pgvector:pg16` and probe-skips (with the base stores green) against
plain PostgreSQL 16.

### Slice 2.3 — Communal knowledge graph crate

Spec: [13.4](spec.md#134-communal-knowledge-graph),
[13.6](spec.md#136-storage-adapters).

- `rakka-agent-knowledge-graph`: claim records with provenance, trust states
  (`Proposed`/`Verified`/`Disputed`/`Retracted`), append-only transitions,
  `(TenantId, KnowledgeSpaceId)` scoping.
- Database-agnostic SPI: claim append by operation ID, lookup, bounded
  traversal, provenance/trust filtering, optional capability reporting; no
  vendor clients, SQL/Cypher/SPARQL, or vendor identifiers in public types.
- In-memory implementation plus contract-test harness.
- HITL/policy promotion gate for consequential claims reusing Slice 1.10
  checkpoints.

**Amended as implemented (2026-07-26):**

- **A claim's identity derives from its append operation, not its statement.**
  Conflicting claims MUST coexist ([spec 13.4](spec.md#134-communal-knowledge-graph)),
  so two agents asserting the same subject/predicate/object are two claims
  with distinct provenance — the statement cannot be the identity. The append
  operation id is the one value reconstructable by the writer after any crash
  and unique per logical write, so `ClaimId::derive_appended(scope, operation)`
  makes a replayed append converge on the same claim (scenario 16) while two
  distinct operations never collide. The salted derivation domains
  (`claim-append` / `claim-transition` / `claim`) follow the slice 2.1
  `MemoryOperationId` discipline exactly, and every one of them digests with
  sha2-256 rather than the default FNV fingerprint: salting stops a
  discriminator from being *spelled* as another domain's input, but only a
  collision-resistant digest stops one from being *searched* for a collision
  inside a domain — and a steered operation-id collision would make a distinct
  logical write replay to another writer's stored result, while a steered
  claim-id collision would deny one of two writers its append. Because the
  append door re-derives, the algorithm is part of the durable contract:
  changing it changes the identity of every stored claim, so it is a breaking
  change to a persisted graph rather than a transparent strengthening. The
  derivation is closed the same way
  born-`Proposed` is: `Claim::new` takes no claim id — it derives one from the
  scope and the operation id — and because `Claim::restore` must accept any
  persisted id to load a record at all, the store's append door re-derives and
  refuses a mismatch (`claim-append-id-not-derived`, conformance-tested).
  Without that door the derivation would be convention rather than invariant,
  and a writer could squat the id another writer's operation will derive and
  deny that append forever.
- **Born-`Proposed` is the type system's answer, closed at three doors.**
  `Claim::new` takes no trust parameter (open decision 3); the only path to a
  non-`Proposed` claim is `Claim::restore` from a persisted record, whose
  `Proposed ⇔ zero-transitions` coherence invariant is re-validated on every
  load and inside deserialization; and the store's append door refuses any
  claim that is not `Proposed`-with-no-history
  (`claim-append-not-proposed`) — conformance-tested so no slice 2.4 backend
  can drift. Trust moves only through `Claim::apply_transition`, the single
  legality-enforcement point every backend shares (the fields are private, so
  no path can skip the table). The lattice: `Proposed → Verified(gated) |
  Disputed | Retracted`; `Verified → Disputed | Retracted`; `Disputed →
  Verified(gated) | Retracted`; `Retracted` terminal; nothing transitions
  *to* `Proposed` (that would launder history); un-retracting is a new claim
  referencing the old one, preserving both provenances. The bounded history
  (32) refuses explicitly — an oscillating claim is a policy incident to
  surface, never a truncation.
- **Every derived field on a claim is re-derived on load, including the audit
  fingerprint.** `Claim::validate` recomputes `content_digest` from the
  record's own subject/predicate/object and refuses a mismatch
  (`claim-statement-digest-mismatch`), on construction and on every restore,
  which is also how deserialization is implemented — so a forged fingerprint,
  or the realistic case of a statement edited under a stale one (a
  hand-repaired row, an adapter that rewrote one column), cannot cross the
  wire or reach a store. It was the one field crossing the load boundary
  unverified while every other was bounded and re-checked, and an audit
  fingerprint that disagrees with its own statement is worse than none: it
  reports two claims as differing when they do not, and it hides an edit. The
  refusal ranks after the field bounds and beside the trust-coherence check —
  a bound describes the content and answers with the specific refusal a writer
  needs, while these two describe whether the record contradicts itself.
  Nothing authorizes on this field (the promotion gate recomputes sha2-256
  over the statement itself), so this is integrity, not a security boundary,
  and the fingerprint stays FNV on purpose.
- **The promotion gate reuses the slice 1.10 grant verbatim, through one new
  additive seam.** `AgentCheckpointGrant::validate_for_binding(binding,
  attempt, now)` landed in `rakka-agent`'s checkpoints module, and the
  existing effect-path `validate_for` now delegates to it (its identity
  checks still run first, so error precedence is preserved; the binding path
  additionally compares the dispatch `target`, a pure strengthening). The
  graph crate derives the canonical binding from the *authoritative* claim —
  sha256 over the scope key, claim id, full statement, `from` status, and the
  one-based ordinal the promotion would occupy, with the effect generation
  pinned to that same ordinal — so a grant authorizes exactly one promotion
  at exactly one history position: after a dispute, re-promotion is a new
  ordinal, a new generation, a new grant, and the stale grant fails the
  identity check before the digest is even compared (proven in the gate test
  matrix). The effect id (`claim-promotion:{claim}`) and target are
  deterministic so the M4 run-driven claim-append effect (scenario 33) adopts
  them unchanged and its checkpoint's grant is already what this gate
  accepts. `AgentCheckpoint::open` is deliberately *not* used — it requires a
  real `AgentRunEffect`, which exists only when M4 makes promotion a run
  effect; grants are constructed by the resolving surface (all fields
  public, as a resolved checkpoint populates them). A replayed promotion
  answers its original outcome without re-evaluating the gate — a decided
  promotion is not re-litigated, even by a grant that has since expired —
  the same argument the checkpoint's own decision dedup makes.
- **The default promotion policy gates everything.** `ClaimPromotionPolicy`
  defaults to gate-all (fail closed); `ungated` is an explicit deployment
  statement, exactly as a no-stage deployment passes an empty guardrail
  chain in slice 2.2; `gating(classifications, predicates)` scopes the gate.
  The policy is passed per `transition` call, the recorded decision-7 M2
  precedent (deployment configuration passed to each call and enforced
  inside it), which also lets the conformance suite exercise every mode
  against any backend through `&dyn KnowledgeGraphStore`.
- **The crate owns its schema window; `AgentRecordKind` is not extended.**
  `AgentSchemaPolicy::check` takes `rakka-agent`'s non-exhaustive record-kind
  enum with its fixed-length `ALL` array — widening it for records the base
  crate does not own would couple `rakka-agent` to every sibling's records
  and invert the dependency promise its crate docs make. The cross-crate
  contract is the stable codes: the graph crate's `check_schema_window`
  fails closed with the same `schema-version-ahead` / `schema-version-too-old`
  vocabulary under the same N/N+1 default.
- **Scenario 18 is a shape, not a filter.** Scope data lives under the
  scope's injective key, so a wrong-scope read is structurally empty: `get`
  answers `None`, `query`/`transitions` answer the empty page, `traverse`
  answers the empty report (a start node appears only when an in-scope edge
  touches it), and a wrong-scope *write* fails with the exact
  `claim-not-found` an absent claim produces. The conformance clause
  compares whole answer values against a genuinely empty space. As with the
  2.2 retriever, answering only for the addressed scope is the
  implementation's own obligation — a returned claim carries no tenant or
  space, so no layer above can re-check it — documented on the SPI trait.
- **The conformance harness is the workspace's first shared contract suite,
  and it injects scopes, not stores.** `conformance` is an ungated `pub mod`
  (the `rakka_agent::testkit` precedent): twelve clause functions plus the
  `check_knowledge_graph_contract` umbrella, each taking
  `&dyn KnowledgeGraphStore` and fresh `ConformanceScopes`. Fresh
  *scopes* are what clause isolation actually needs — a live-database 2.4
  backend cannot cheaply construct stores, but every backend can serve one
  more tenant (the 2.1/2.2 per-tenant DSN-suite pattern, made reusable).
  Uniqueness has to hold along three axes, not one: a sequence counter
  isolates clauses within a process, but it is process-local and starts at
  zero, so it cannot separate the test binaries `cargo test` runs
  concurrently, nor a second run from the rows the first left in a live
  database. `ConformanceScopes::unique` therefore prefixes a per-run
  namespace digested from the process id and the wall clock (the pid alone is
  recycled by the operating system; a coarse clock alone can repeat), which
  `RAKKA_KNOWLEDGE_GRAPH_CONFORMANCE_RUN` pins when a 2.4 suite wants a
  namespace it can find again, and `unique_in` names explicitly for a suite
  that manages its own. Until then idempotency was masking the collision —
  replayed writes converged on their originals — which is exactly the kind of
  accident a contract suite must not rely on.
- **The suite states its bounded-query and bounded-traversal expectations
  against the *effective* limit, not the crate cap.** The SPI lets a backend
  declare traversal and page bounds tighter than the crate caps, and the
  effective limit of a request is the smallest of the request, the
  declaration, and the cap. A suite that asserted the cap would therefore
  reject a backend for honouring its own declaration, which makes the feature
  unusable — so `bounded_traversal` derives its expected edge count and
  `truncated` flag from `capabilities().max_traversal_depth()`, and
  `bounded_queries` bounds its pages by `capabilities().max_page_entries()`.
  Both stay exact equalities or `<=` against the effective value, so a backend
  that serves *more* than it declares still fails. Consequently every other
  clause that means "all claims a filter admits" drains the cursor through the
  `drained_query` helper instead of reading one page: only the paging clause
  inspects pages. `tests/knowledge_graph_conformance.rs` carries a store that
  declares depth two and page-entries two and serves both, running the whole
  umbrella — the 2.4 shape, and a regression test rather than a promise.
- **Deliberately not in the `rakka` facade yet.** The agent-adapter
  precedent (`rakka-agent-postgres` is not in the facade either) and the
  spec 19 "feature gates and curated prelude exports after API review"
  clause; adding `agent-knowledge-graph = ["dep:...", "agent"]` later is a
  one-line additive change. Recorded in the CHANGELOG as a decision.

Done when: the graph halves of scenarios 16 and 18 pass on the in-memory
implementation. **Done (2026-07-26):** both pass by name
(`scenario_16_replayed_graph_writes_are_idempotent`,
`scenario_18_unauthorized_graph_reads_do_not_reveal_existence`) in
`crates/rakka-agent-knowledge-graph/tests/knowledge_graph_conformance.rs`,
which drives every conformance clause individually plus the one-call umbrella
a 2.4 backend runs unchanged; the nine-case promotion-gate matrix (grant
required, valid grant with receipt, expired, wrong-content digest, wrong
kind, spent, replay-without-re-evaluation, foreign tenant, generation
pinning across a dispute) passes in `tests/claim_promotion_gate.rs`; the
transition table, derivation stability, bounds, and fail-closed
schema/coherence loads are proven in the inline module tests; and the
binding/effect validation-path parity is proven in
`crates/rakka-agent/tests/checkpoints.rs`.

### Slice 2.4 — Backend conformance and M2 acceptance

Spec: [13.6](spec.md#136-storage-adapters),
[18](spec.md#18-required-recovery-scenarios) scenario 20.

- Capture representative claim/traversal/tenancy/migration queries (open
  decision 8), then validate the SPI against at least two structurally
  different implementations or contract doubles before naming any reference
  backend.
- Run the full conformance suite (claim identity, idempotent append,
  provenance, trust filtering, authorization, bounded queries) unchanged
  across backends.

**Amended as implemented (2026-07-26):**

- **The second backend is PostgreSQL relational tables in a new crate,
  `rakka-agent-knowledge-graph-postgres`, and no reference backend is
  named.** The disposition on open decision 8 records the resolution:
  the representative claim/traversal/tenancy/bounded-query families are
  captured as the conformance clauses themselves (a table in the
  conformance-module docs maps each family to the clause proving it), and
  migration stays backend-owned because the portable SPI deliberately has no
  migration surface. A separate crate follows the
  `rakka-persistence`/`rakka-persistence-postgres` precedent;
  `rakka-agent-postgres` is scoped "one crate, one schema, one lock" to
  agent memory and never referenced the graph domain, so grafting claims
  onto it would have coupled memory-only consumers to the graph crate.
- **The scenario-20 proof is the commit shape, not just the test.** The
  capture doc landed as its own docs-only commit to the domain crate; the
  backend commit then touches nothing under `crates/rakka-agent` or
  `crates/rakka-agent-knowledge-graph` — the suite ran unchanged by
  construction, and `scenario_20_the_whole_contract_passes_unchanged...`
  drives the one-call umbrella against the live store.
- **The record BYTEA is authoritative; columns are fences and predicates
  only.** Claims and transitions persist as their canonical `serde_json`
  encodings and are rebuilt through the domain `restore` doors on every
  load, so the schema window, statement-digest re-derivation, and trust
  coherence all fail closed against live rows (proven by doctoring rows in
  place). `subject`/`predicate`/`object_node`/`trust` are denormalized only
  for traversal predicates, `transition_count` only as the compare-and-set
  fence, and a column that disagrees with its own record is refused as
  drift — never skipped (a skip would answer short) and never preferred.
- **Queries are `admits()`-in-Rust keyset scans, by design of the filter.**
  `ClaimFilter` exposes builders and the shared `admits` predicate but no
  field accessors, so SQL pushdown is impossible without widening the domain
  API — and unnecessary: resumption is by claim-id position, not offset, so
  Rust-side admission loses no rows, and the `COLLATE "C"` column makes SQL
  order exactly the reference implementation's string order. Cost is linear
  in one scope's corpus, documented in the crate docs — the same
  exactness-first trade slice 2.2 recorded for pgvector. The `next` cursor
  is minted only when a further *admitted* claim was actually seen, the one
  convention a raw `LIMIT n+1` cannot reproduce under Rust-side admission.
- **Writes are single data-modifying-CTE statements; the transition is a
  bounded CAS loop that reads the ledger first on every attempt.** The
  slice 2.1 discipline carries over: ledger consultation, the claim
  mutation, the transition append, and the ledger insert commit or fail as
  one implicit transaction on the shared pipelined client. A replay answers
  the ledger's original bytes — a decided promotion is not re-litigated,
  even by a grant that has since expired, proven across a reconnect — and a
  lost race loops into a fresh read where the legality table re-runs
  against the state that won, so contention converges on the winner's
  replay or a typed refusal (`TRANSITION_CAS_MAX_ATTEMPTS` bounds the loop;
  exhaustion is `claim-backend-failed`, never a silent wrong answer).
  Traversal is the reference breadth-first expansion over bounded per-node
  queries — every predicate ahead of the per-node `LIMIT`, the global
  spent-edge set threaded through each statement — because a recursive CTE
  cannot express the global dedup, the deterministic edge order, or
  truncation at the exact budgeted edge.
- **The migration advisory lock takes the fresh id `982_451_927`.** The
  existing family (`…653/659/707/777/881`) is folklore-prime but really
  just distinct values; distinctness is the only real constraint, and the
  doc on the constant says so.
- **`tests/claim_promotion_gate.rs` deliberately stays typed to the
  in-memory store.** Its nine cases exercise `validate_promotion` and the
  binding derivation — domain logic the backend calls verbatim before its
  write statement; the backend-coupled cases (grant required, granted
  receipt, expired grant, replay-without-re-evaluation) already run against
  the live store through the conformance clauses, and the one genuinely
  backend-shaped risk — a replayed gated promotion racing the CAS loop
  after grant expiry — has its own dedicated proof. Generalizing the
  helper file would have put agent-domain edits into the slice whose
  acceptance is their absence; it remains an optional follow-up.

Done when: scenario 20 passes across both implementations without touching
agent-domain code. **Done (2026-07-26):** the umbrella and all twelve
clauses pass by name against the live store in
`crates/rakka-agent-knowledge-graph-postgres/tests/postgres_conformance.rs`
(scenarios 16, 18, and 20 named), the backend-only durability proofs pass in
`tests/postgres_backend_proofs.rs` (reconnect replay, two-connection
distinct- and same-operation races, gated-promotion replay after expiry,
migration idempotence and the four-migrator race, doctored rows failing
closed), the CI postgres job runs the crate on every pull request, and the
backend commit's diff contains zero agent-domain changes.

---

## Phase 3 — M3 Continuous Goals

Milestone: M3. Acceptance:
[Continuous Goal Milestone](spec.md#continuous-goal-milestone-m3).
Scenarios owed: 36, 47-51.

The continuous defaults are already resolved
([spec 21.1](spec.md#211-resolved-article-review-decisions) items 1-3).

### Slice 3.1 — Wake identity and policy

Spec: [6.9](spec.md#69-agentwakeid-and-schedulerevision),
[8.2](spec.md#82-continuous-goal-controller-and-epochs),
[8.1](spec.md#81-goal-contract-and-lifecycle) (continuous clauses only).
Guidance: [Continuous Goal Controller](technical-guidance.md#continuous-goal-controller).

- `AgentWakeId` construction (goal + `ScheduleRevision` + logical
  occurrence), versioned `AgentWakePolicy` with the full field set of
  [spec 8.2](spec.md#82-continuous-goal-controller-and-epochs).
- Continuous-mode fields on the goal/root-task contract (the full
  `AgentGoalSpec` lands in Phase 4; here only what the controller needs).

Done when: wake-ID dedup construction is property-tested (same occurrence
from any trigger path yields one identity).

**Amended as implemented (2026-07-27):**

- **The wake identity is derived, never generated, following the slice 2.3
  derived-claim-identity precedent.** `wake_id_for_occurrence` is a pure
  function of `(tenant, goal, ScheduleRevision, logical occurrence)` — a
  `wake-`-prefixed SHA-256 digest over a length-prefixed canonical encoding
  that is injective whatever the goal or event identity contains — and
  deliberately takes no trigger source, delivery time, or lateness, so every
  trigger path reconstructs one identity and deduplication is a construction
  property rather than runtime coordination. Delivery metadata lives on
  `AgentWakeBinding`, whose deserialization re-derives the identity and fails
  closed on a record its own components do not derive. The derivation is a
  persisted compatibility surface: golden vectors in `tests/wake_identity.rs`
  pin it (verified against an independent recomputation), and the fixed
  69-byte output leaves headroom inside `AGENT_IDENTITY_MAX_LENGTH` for the
  epoch task/run ids slice 3.3 derives from it.
  `wake_admission_operation_id` is the durable-inbox deduplication value the
  slice 3.2 controller admits on.
- **The policy is versioned under its own record kind with the standard
  N/N+1 window.** `AgentWakePolicyRevision` persists as
  `AgentRecordKind::WakePolicyRevision` with its own schema version, because
  a wake binds the policy revision in force at construction and outlives the
  policy that admitted it. The constructor produces the resolved continuous
  defaults (spec 21.1 items 1-3); parallel epochs and bounded catch-up are
  representable but constructible only through explicit builders that demand
  the concurrency bound and result policy; an epoch must be bounded from
  construction (a deadline or at least one bounded budget dimension); and a
  policy violating any bounded invariant fails closed on deserialization.
- **The goal contract carries only the controller's slice.**
  `AgentGoalMode`/`AgentContinuousGoalSpec` hold the schedule revision, wake
  policy revision, and explicit health condition; the full `AgentGoalSpec`
  still lands in slice 4.1. Records persisted before the field load as
  finite, a continuous creation without a goal binding is refused closed
  (`task-continuous-without-goal`), and A2A ingress always creates finite
  work — a continuous root control task is instituted by the goal surface,
  never by ingress.
- **Review hardening (post-review, same slice).** The task-state load gate
  version-checks the embedded wake-policy revision exactly as it checks the
  embedded task definition, proven by a doctored-record test
  (`schema-version-ahead`); a maximum lateness that undercuts the admission
  window is refused (`wake-lateness-below-admission-window`) because an
  occurrence between the two would be both admittable and missed — the band
  between window and lateness is what the overlap policy durably coalesces;
  and a zero `ScheduleRevision` is unrepresentable: construction clamps to
  the initial revision, and a persisted zero fails closed on load, so the
  slice 3.2 fencing comparison never sees a revision no schedule issued.

### Slice 3.2 — Wake controller and scanners

Spec: [8.2](spec.md#82-continuous-goal-controller-and-epochs),
[15](spec.md#15-passivation-recovery-and-shard-movement) (continuous
clauses).

- Durable wake controller over the `rakka-agent-workflow` one-shot timer and
  trigger substrate (`timers.rs`, `triggers.rs`): default forbid-overlap,
  durable coalescing, at-most-one occurrence after downtime, revision
  fencing.
- Shared scanners recover durable occurrences and inject deduplicated inbox
  commands; scanner/pod uptime never creates an occurrence.

Done when: scenarios 47, 48, 49, and 50 pass, including duplicate-scan and
obsolete-revision injection.

**Amended as implemented (2026-07-27):**

- **The wake-timer store and scanner are agent-domain, not the workflow
  kernel's.** The substrate's `AgentTimerScanner` fire path is run-bound — it
  fences on `workflow_id`, injects into an `AgentRunInbox`, and resumes an
  `AgentStepRunner`, erring `MissingRunState` for a target with no run —
  while a goal wake has no run until slice 3.3 admits an epoch, and
  `AgentTimerEntry` carries no `ScheduleRevision` to fence on. So
  `AgentWakeTimerStore`/`AgentWakeScanner` live in `rakka-agent` and mirror
  the substrate's discipline exactly (one compare-and-set durable record, a
  bounded due scan, idempotent terminal marks, the `WorkflowClock` seam, the
  metrics conventions) with agent-typed entries keyed by the derived wake id
  under the new fail-closed `AgentRecordKind::WakeTimerState` version; the
  substrate's trigger-metadata half (`AgentTriggerSource`) is reused as-is on
  the binding. Generalizing the workflow kernel's persisted timer schema for
  one consumer was rejected.
- **Every disposition is a recorded transition, and "admitted" is an active
  slot.** `AgentWakeControllerState` — embedded in the root control task's
  record — dispositions each delivery deterministically (fence, duplicate,
  admit, coalesce, skip) and records the result under the wake's derived
  admission operation id, so the counters are exact and a replay answers from
  the record. The active slot is what slice 3.3 turns into the epoch's child
  task/run; `CompleteWakeOccurrence` releases it and promotes the oldest
  parked occurrence in the same durable transition, and is the transition the
  epoch-result exchange of 3.3 drives rather than replaces. Deduplication
  beyond the operation-log ring is a state property: the active/parked slots,
  a bounded recent-wake ring, and a monotone scheduled-due-time watermark
  (scheduled occurrences arrive in due order, so at-or-below-watermark
  answers as a duplicate). The scanner enforces that due-order invariant per
  task: after a failed or refused delivery, a pass holds back the same
  task's later-due entries (`AgentWakeScanOutcome::HeldBack`) rather than
  delivering around the failure, which would advance the watermark over an
  occurrence that was never applied and silently lose it.
- **`BoundedCatchUp` runs minimally by design** (decision locked at planning):
  the parked queue caps at `min(bound, AGENT_WAKE_PENDING_CAPACITY)` inside
  the bounded task state, drains one occurrence per release, and skips the
  overflow with an exact `missed` count; the deeper replay sequencing lands
  with 3.3's real epochs. The defaults are complete: latest-wins single-slot
  coalescing while exactly one occurrence owns execution, at most one
  coalesced admission after downtime, `Skip` counts and drops.
- **Delivery follows shard ownership.** `ShardedWakeDelivery` asks the
  locally owned task entity and reports a stable `wake-remote-owner` failure
  for the rest, leaving those entries pending for the owning node's own
  scanner — every node may run a scanner, overlap is safe because every
  delivery is deduplicated by construction, and no wake needs a cross-node
  command surface. A schedule update fences parked occurrences in its own
  transition (`UpdateContinuousSchedule`, strictly monotonic on schedule and
  policy revisions); a binding *ahead* of the controller fails closed
  (`wake-revision-ahead`) because no accepted schedule issued it.

### Slice 3.3 — Epoch admission and budget windows

Spec: [8.2](spec.md#82-continuous-goal-controller-and-epochs),
[9.7](spec.md#97-hierarchical-budget-ledger) (window clauses),
[6.5](spec.md#65-agentrunid) (per-epoch task/run rule).

- Admitted wake -> one finite child `AgentTaskId`/`AgentRunId` epoch carrying
  goal, wake, revisions, observation scope, budget, deadline.
- Per-epoch allocation plus durable rolling/calendar-window goal ceiling;
  refill is a persisted logical-time transition, never restart-triggered.
- Cross-epoch continuity only via controller state, private memory, and
  artifacts; per-epoch session-memory isolation.

Done when: scenarios 36 and 51 pass.

**Amended as implemented (2026-07-27):**

- **The epoch's identities are derived from the wake.** The child task is
  `epoch-` plus the wake's own digest (`epoch_task_id_for_wake`, a constant
  70 bytes independent of the root control task's id length, pinned by a
  golden vector) and the run is the existing `run_id_for_assignment` at
  generation one, so a replayed admission resolves to the same child. The
  epoch contract — task definition, assignee, observation scope — lives on
  `AgentContinuousGoalSpec` as `AgentEpochSpec` (still only what the
  controller needs; the full `AgentGoalSpec` remains slice 4.1); a pre-3.3
  record or a goal without one fails admission closed
  (`task-epoch-undefined`), rolling the whole admitting transition back.
- **Admission owes the epoch atomically.** One compare-and-set carries the
  wake's disposition, the goal-window charge (and any logical-time refill it
  observed), the escrow debit from the root's own ledger
  (`AgentEscrowChildId` keyed on the derived run, so a replay never debits
  twice), the epoch reference on the active slot, and the owed
  `Creation` exchange — which now carries the debited grant and the wake on
  `AgentTaskCreation`. Release-time promotion runs the identical path for
  the promoted occurrence.
- **Epoch completion returns through a new `AgentExchangeKind::EpochResult`**
  owed by the epoch task's own transitions once its ledger closes — the run's
  settlement *and* return have applied — so the consumption it reports is
  never an early under-count; a cancelled epoch with no outstanding escrow
  owes it from the cancel transition. The journal's initiation record is the
  once-guard. The controller's apply verifies the sender is the very task the
  wake derives (`task-epoch-forged`), settles and returns the epoch's escrow
  idempotently, releases the wake (an explicit `CompleteWakeOccurrence`
  having raced is tolerated as already-released — the settlement still
  counts), and owes the promoted occurrence's epoch creation in the same
  transition.
- **The goal window charges the full epoch allocation at admission**
  (decision locked at planning; unused-budget credit-back is revisited with
  3.4's observability). The ledger lives on the controller state; refill
  happens inside whatever recorded transition first observes logical time
  across the boundary — rolling windows anchored at first charge, calendar
  windows on UTC civil-date boundaries computed in-crate — and never on
  restart, activation, or shard movement. An admission the window cannot pay
  for parks with the new recorded `Deferred` disposition and is retried —
  oldest parked first — by the next release or delivery whose transition
  observes a window able to pay: the admission transition promotes the
  oldest parked occurrence into a free slot before dispositioning the fresh
  delivery (`promote_admittable`, the same promotion release runs), so a
  deferred occurrence is never leapfrogged by a fresher one. Nothing fires
  at the window turn itself — on a quiet schedule a deferred occurrence
  waits for the next durable delivery. **Decision (post-review):** a
  controller-originated window-turn re-wake is deliberately deferred to
  slice 3.4, where failure backoff needs the identical mechanism (a
  controller-owned durable re-wake at a computed time, which also requires a
  transition that re-attempts a wake `contains()` would otherwise answer as
  a duplicate); the two are designed once, together. A policy whose window
  bounds a dimension its epoch budget leaves unbounded is refused at
  construction (`wake-window-epoch-unbounded`), as is an epoch budget
  exceeding a ceiling dimension (`wake-window-epoch-exceeds-ceiling`) — a
  window that could never pay for a single epoch.
- **The next durable wake condition is the parked timer entry.** Schedule
  computation is application-owned, so the goal's "next wake condition" is
  the occurrence the schedule layer parks in the durable wake-timer store —
  which is exactly what scenario 36's test does between epochs; nothing
  resident stands in for it.
- **Review carryovers from PR #41 closed.** The obsolete-revision fence now
  runs before the watermark in `admit()` (a stale binding answers `Fenced`,
  never a swallowed duplicate), a fence no longer advances the watermark (a
  future-dated obsolete straggler could otherwise swallow the new schedule's
  occurrences), `UpdateContinuousSchedule` resets the watermark so a new
  revision may issue earlier due times, and — resolving the 21.1 question —
  an active downtime representative *absorbs* later missed occurrences of
  its backlog (counted `missed`, never parked), so one downtime yields
  exactly one epoch rather than a representative plus an echo.

### Slice 3.4 — Continuous lifecycle and M3 acceptance

Spec: [8.2](spec.md#82-continuous-goal-controller-and-epochs),
[17.18](spec.md#1718-authoritative-operational-queries-and-observability-views),
[Continuous Goal Milestone](spec.md#continuous-goal-milestone-m3).

- Suspension, renewal, failure backoff, expiry, and retirement transitions.
- Controller-originated durable re-wakes, one mechanism for two consumers
  (decision recorded in slice 3.3's amendment): the failure-backoff retry
  and the window-turn re-attempt of a `Deferred` occurrence. Both need the
  controller to park a timer entry due at a computed time and a delivery
  path that re-attempts a wake the admission dedup would otherwise answer
  as a duplicate.
- Operational query exposure: schedule revision, next wake, last progress,
  active epoch, budget window, missed/coalesced counts, retirement state.
- Wake/epoch metrics and audit events
  ([spec 17.12](spec.md#1712-metrics),
  [17.13](spec.md#1713-structured-logs-runtime-events-and-audit)).

Done when: the continuous milestone checklist is demonstrated end to end by
an example with fault injection across pod restart and shard movement.

**Amended as implemented (2026-07-28):**

- **Lifecycle is controller state under its own monotonic revision.**
  `AgentGoalLifecycleState` (status, lifecycle revision, provenance of the
  last change, suspension reason, expiry override, failure streak, backoff,
  re-wake slots) lives on the controller state. The four operator commands —
  `Suspend`/`Resume`/`Renew`/`RetireContinuousGoal` — fence on the expected
  lifecycle revision inside the compare-and-set (the agent-entity
  suspend/resume precedent); every observation-driven change (expiry
  crossed, retirement count reached, failure streak escalated) also bumps
  it, so a racing operator command gets `wake-stale-lifecycle-revision`
  rather than acting on a state that no longer exists. Expiry and
  `AfterOccurrences`/`At` retirement are *observed* by whatever recorded
  transition first sees them true (the window-refill shape) — never by a
  timer. `Expired` and `Retired` are absorbing: fresh deliveries answer the
  recorded `Barred` disposition and the scanner marks their entries
  terminal, while an in-flight epoch's `EpochResult` settlement still lands,
  so budgets close cleanly on a goal that retired mid-epoch. Resume clears
  the failure streak and the backoff (approved: an operator resume is an
  explicit override), and promotes what suspension parked — owing the
  promoted epoch's creation in its own transition. Renewal fences to
  `[effective expiry − window, effective expiry)` under `RequiredBefore` and
  must strictly extend; the extension is an override the policy's own
  `expires_at` no longer caps.
- **Failure accounting runs before the release attempt.** A `Failed` epoch
  grows the streak and arms the backoff (geometric, saturating, capped)
  even when an explicit `CompleteWakeOccurrence` raced the settlement;
  `Cancelled` neither grows nor resets. The backoff gates fresh admissions
  *and* promotions. A streak reaching `escalate_after_failures`
  auto-suspends with a bounded reason.
- **One re-wake mechanism, two per-cause slots.** The controller owes itself
  at most one backoff re-wake and one window-turn re-wake
  (`AgentWakeRewakes`), recomputed idempotently by `ensure_rewakes` at the
  end of every mutating entry point — computed from current state, cleared
  while the lifecycle forbids admission, self-healing after any crash — 
  rather than written piecemeal at each cause site. One slot for both causes
  would lose liveness when they coexist. The re-wake is a new
  `AgentWakeOccurrence::Retry { due_at, cause }` under a new
  `AgentWakeTriggerKind::Controller`; the two require each other (neither
  can be smuggled through the other's path), `Controller` is exempt from the
  policy's trigger allow-list for exactly this pairing, and `Retry` exposes
  its `due_at` so the parked entry is due at the computed time, not
  immediately. Consequence, behavior-preserving today: the controller's
  due-time watermark is scoped to `Scheduled` occurrences. The retry arm of
  `admit()` consumes the delivery without admitting — the pre-admit
  `promote_admittable` already did the real work — so a stale parked re-wake
  is a recorded no-op nudge and never needs cancellation. The slot and the
  `Retry` occurrence carry an *attempt generation* that is part of the wake
  identity: a retry delivered before this host's clock reaches its due time
  (a scanner host running ahead) is consumed while its cause still holds and
  its timer entry goes terminal, so the consume re-arms the slot unparked
  under the next attempt and the same transition's settle pass parks a fresh
  entry the fired one cannot absorb — under skew the retry cycles once per
  scan until the due time passes here, instead of stranding the parked
  occurrences behind a terminal entry. Attempt zero keeps the original
  two-segment identity form, so previously persisted retries re-derive
  unchanged.
- **Parking is a settle-pass seam behind an object-safe trait.** The entity
  parks owed re-wakes through an optional `Arc<dyn AgentWakeRewakeParker>`
  (`SharedWakeTimerParker` adapts the shared wake-timer store), wired via
  `with_wake_timers` on the store, entity, and sharding settings — approved
  over a fourth store generic; without the handle, owed re-wakes stay
  durably owed and query-visible. Park-then-mark crash windows converge:
  the slot's `parked` flag commits only after the timer entry exists, and
  the next settle pass re-parks idempotently on the derived wake id.
  Terminal timer entries are never pruned implicitly; `prune_terminal` is
  an explicit operational act.
- **The wake-timer store converges on a lost compare-and-set.** The parker
  made the store multi-writer *within one scan pass* — the scanner's mark
  races the re-wake the delivered entity's own settle just parked (found by
  the acceptance example). `schedule_occurrence` and the mark/cancel
  operations now re-recover and retry a bounded number of times; every
  mutation is idempotent over the re-read record, so losing the race is
  normal operation, not a failed pass.
- **Metrics count committed transitions only.** Three bounded instruments —
  `rakka.agent.wake.dispositions{outcome,trigger}`,
  `rakka.agent.epochs{outcome}`, `rakka.agent.goal.lifecycle{transition}` —
  emitted post-commit on `Applied` replies only (never `Duplicate`;
  reply-driven and replay-suppressed on the exchange path). Admitted epochs
  are counted as the difference of the controller's monotone `admitted`
  counter across the committed transition — the one source that sees a
  promotion made in the same breath as a fresh delivery, a release, or a
  resume. Lifecycle transitions are counted the same way: the difference of
  the goal's lifecycle status across the committed transition, so observed
  flips — expiry, retirement by policy, escalation into suspension — count
  exactly like commanded ones, from whatever transition first recorded
  them; `renewed` alone is counted from its command, because renewal leaves
  the status unchanged. The scanner's existing raw counter now goes through
  the agent-domain label validator like every other instrument.
- **Audit is task history** (judgment against 17.13's `AgentAuditSink`
  wording, approved): new `AgentTaskHistoryKind` entries — wake
  dispositioned, epoch admitted/settled, goal
  suspended/resumed/renewed/expired/retired, schedule updated — recorded
  only inside the recorded transition they describe, with the wake id and
  disposition in the bounded detail. Observation-driven flips record with
  detail `observed`. Worst case adds 4 entries to a transition against the
  34-entry headroom fence.
- **The operational query is one durable read.**
  `agent_task_operational_snapshot` mirrors the run-scoped query (content
  redacted, schema-checked, no entity activation); `AgentWakeStatusView`
  grew additively (window ledger, lifecycle state, active epoch refs); and
  `next_pending_wake_for_task` is a pure join over the wake-timer state,
  because "next wake" lives in the timer store, not the task record.
- **The milestone's done-when is `examples/continuous-goal-acceptance`**: a
  16-line transcript pinned three ways (README, `EXPECTED_TRANSCRIPT`,
  `tests/acceptance.rs`) walking the M3 checklist with fault injection —
  crash mid-settlement with convergent replay, a downtime backlog, window
  exhaustion and the window-turn re-wake, failure backoff and escalation,
  fenced and real resume, a schedule-revision fence, a stale former owner
  losing the compare-and-set after shard movement, renewal, retirement by
  occurrence count, and the operational query answering from durable state
  alone.

---

## Phase 4 — M4 Multi-Agent Goals

Milestone: M4. Acceptance:
[Multi-Agent Goal Milestone](spec.md#multi-agent-goal-milestone-m4).
Scenarios owed: 27-34, 39.

Open decisions to resolve: 14 (distinct goal identity — resolved default,
disposition recorded by slice 4.1), 15 (catalog resolves specialists),
16 (workflow tools).

### Slice 4.1 — Goal contract and lifecycle

Spec: [8.1](spec.md#81-goal-contract-and-lifecycle),
[6.3](spec.md#63-agentgoalid).
Guidance: [Define the Goal Before Starting the Loop](technical-guidance.md#define-the-goal-before-starting-the-loop).

- Full `AgentGoalSpec` and `AgentGoalStatus` lifecycle with the
  `Unsatisfied`-vs-`Failed` semantics; root `AgentTaskEntity` coordinates,
  `AgentGoalId` defaults to the root `AgentTaskId` value while types stay
  distinct.
- Budget-exhaustion parking/escalation policy at goal scope.

Done when: goal lifecycle transitions are covered by unit tests and the goal
remains addressable while fully passivated.

**Amended as implemented (2026-07-28):**

- **The contract status is orthogonal to the M3 admission gate, and the gate
  projects one-way onto it.** `AgentGoalStatus` (the spec 8.1 eight-state
  contract lifecycle) lives on a new goal record; the wake-side
  `AgentGoalLifecycleStatus` stays exactly the continuous admission gate it
  was. Where the two overlap, the gate drives the contract in the same
  compare-and-set (`project_gate_onto_goal`): an observed or commanded
  expiry → `Expired`, a retirement → `Cancelled` with the structured
  `Retired` reason (spec 8.1 has no `Retired` status, and spec 9.7 says a
  structured reason, not a new status — the `AgentTaskTerminalReason`
  precedent), a suspension parks the goal `Waiting(AdmissionSuspended)`, and
  a gate resume reactivates only a goal waiting on exactly that suspension.
  Contract-side transitions drive the gate the other way — a goal-terminal
  decision retires it, a budget park suspends it — through new
  provenance-free `suspend_by_policy`/`retire_by_policy` methods, the same
  class of durable transition as M3's failure-escalation auto-suspend.
- **The terminal status is derived from the reason, never stored beside it.**
  `AgentGoalDecision` carries an `AgentGoalTerminalReason` whose `status()`
  determines the outcome, so an inconsistent outcome/reason pair is
  unrepresentable. `CriteriaSatisfied` and `CriteriaNotMet` are
  unconstructible without an `AgentGoalEvaluationRef` that assessed the
  criteria revision in force — the slice 4.2 hook is the shape of the entry
  point from day one, and `Satisfied`-by-declaration is refused at the
  entity surface (spec 8.3). From `Proposed` only `Cancelled` and `Expired`
  are reachable: no work happened, so no execution failure and no evaluation
  can exist.
- **The spec is a bounded component of the root task's record, and identity
  is composed, not duplicated.** `AgentGoalSpecRevision` (own record kind,
  fail-closed on load like the wake policy) rides `AgentTask.goal_state`,
  so every goal transition commits in the root task's one compare-and-set —
  spec 6.3's coordination without a new entity. The spec serializes under a
  4 KiB cap with per-collection bounds, which is what keeps a maximal
  goal-bearing continuous task inside the 32 KiB materialized bound with the
  growth reserve intact (proven in `task_bounded_state.rs`). Tenant, goal
  id, root task id, mode, and coordinator run are the task record's own
  fields, composed around the spec. `allowed_workflows` landed after all —
  `AgentWorkflowToolId` has been a stable envelope identity since Phase 1,
  so the planned deferral had nothing to wait for; only stagnation's numeric
  thresholds wait for 4.2 to define the detector they bound.
- **Creation institutes the goal, and a `Proposed` goal spends nothing.**
  `AgentTaskCreation.goal_spec` institutes the goal in the creating
  transition; the binding defaults to `AgentGoalId::for_root_task` (open
  decision 14's disposition). The goal's allocation seeds the root escrow
  narrowed to the definition ceilings — the definition-ceiling → goal →
  task rung of spec 9.7 with no new machinery. `activate_on_creation`
  defaults to true (creating the task is the authorization to work);
  opting out starts the goal `Proposed`, which gates `awaits_assignment`
  and parks the continuous gate under the `goal-proposed` reason until
  `ActivateGoal` lifts exactly that park.
- **Exhaustion policy: park by default, at two trigger points.**
  `AgentGoalExhaustionPolicy` (default `Park`, per-dimension overrides)
  is consulted when the root grants a zero top-up in the exhausted conserved
  dimension, and when an assignment refusal is *permanent* — zero headroom
  and no outstanding child escrow whose return could restore any. `Park`
  moves the goal `Waiting` and suspends continuous admission in the same
  compare-and-set; `Escalate` is the same park with the escalation recorded
  against the spec's escalation reference (goal-scope HITL is a later
  slice, and nothing pretends otherwise); `Terminate` fails the goal, the
  root task (`goal-budget-exhausted`), and the gate together. The M3
  window-ceiling deferral is deliberately exempt: a window wait relieves
  itself at the persisted window turn. The run-side contract is untouched —
  the run still receives the honest zero grant and stops with its original
  exhaustion.
- **One `ResumeGoal` door un-parks, and each door owns its wait.**
  `ResumeGoal` widens the root ledger under the definition ceilings
  (`AgentEscrowLedger::widen` — grow, cap at the ceiling, never shrink),
  reactivates the contract, and lifts the goal-driven admission park, in one
  fenced compare-and-set; a resume that leaves the exhausted dimension
  without headroom is refused (`task-goal-resume-unrelieved`) rather than
  re-parking on the next decision. Ownership is fenced both ways
  (`task-goal-wait-owned-elsewhere`): the gate's resume refuses a budget
  park, and the goal's resume refuses an admission suspension — without the
  fence, a gate resume would re-admit spending the contract says is parked.
- **The settle pass is the goal entry point that always commits.** A goal's
  own deadline expiry is observed by `settle_side_effects`/
  `make_local_progress` (skipping the write while nothing would flip),
  because a command-side observation is discarded with the command's
  refusal — activate-on-expired refuses `goal-terminal`, and the durable
  expiry must not depend on a command that succeeds.
- **A terminal root ends the goal it coordinates — except completion.**
  Every task-terminal path projects onto a held goal record (cancellation →
  `RootTaskCancelled`, failures → `ExecutionFailed`), because a terminal
  coordinator can drive nothing further. `ResultAccepted` deliberately does
  not: completion is evidence, and only the configured evaluator makes a
  goal `Satisfied` (spec 8.3) — slice 4.2 wires who may call the decision
  door. Symmetrically, a failed-and-released run does *not* release its
  accepted assignment; reassignment/handoff semantics belong to later
  slices, and `ResumeGoal` honestly reactivates only the contract.
- **Bookkeeping.** Four new task-history kinds (`GoalActivated`,
  `GoalParked`, `GoalReactivated`, `GoalDecided`) beside M3's wake-scoped
  `Goal*` kinds; `AGENT_TASK_MAX_HISTORY_PER_TRANSITION` grew to
  `max_dependencies + 3` for the creation that also activates a goal; the
  `rakka.agent.goal.status` counter counts contract transitions by status
  difference across the committed transition, distinct from the gate's
  `rakka.agent.goal.lifecycle`; `AgentTaskOutcome`/`AgentTaskSnapshot`
  carry the goal outcome and view, so replays and `Describe` answer the
  goal without waking anything.

### Slice 4.2 — Progress, evidence, and evaluation

Spec: [8.3](spec.md#83-progress-evidence-and-completion).
Guidance: [Verify Progress and Completion](technical-guidance.md#verify-progress-and-completion).

- Evaluator contract: deterministic assertions, authoritative queries,
  verification workflows, evaluator model/agent under a distinct policy, and
  HITL — all executed as durable effects with persisted outcome, evidence
  references, and criteria revision.
- Stagnation detection (repetition fingerprints, no-progress epochs) feeding
  deterministic continue/replan/wait/escalate/terminate policy.

Done when: scenario 30 passes (goal `Satisfied` only after evaluation of the
current criteria revision against durable evidence).

**Amended as implemented (2026-07-28):**

- **Evaluation is a run-side durable effect, and the exchange is the
  attestation.** `EvaluateGoal` commits a read-only `Evaluation` effect
  (`GoalEvaluationCall`, default two attempts) in a deduplicated bounded
  transition; the application-owned `AgentGoalEvaluationExecutor` judges it;
  the completed `AgentGoalEvaluationRecord` (own fail-closed
  `AgentRecordKind::GoalEvaluation`) parks in the run's one durable
  evaluation cell and crosses to the root task as the eighth
  `AgentExchangeKind::GoalEvaluation`, owed from durable state under a
  derived operation id so any crash re-owes the identical exchange. Under a
  configured `spec.evaluator` the open `RecordGoalDecision` command refuses
  criteria decisions (`task-goal-decision-unattested`); the exchange —
  sender-fenced to the currently assigned run
  (`task-goal-evaluation-forged`) — is the only ingress, and both ingresses
  share one decision core so their fences can never diverge. A door refusal
  becomes the exchange's refused reply and settles the cell with the door's
  code: the caller re-evaluates, never a crash loop.
- **All five evaluator methods are typed; four execute.** Deterministic
  assertion, authoritative query, and the evaluator model run through the
  executor; human review is an `Approval` checkpoint bound to the evaluation
  effect itself — the digest-bound grant is the verdict, the record carries
  the resolver and the durable decision as its evidence, a denial is a
  *failed evaluation* (the goal stays `Active`), and expiry never
  auto-approves. A verification workflow is refused closed at commit
  (`run-goal-evaluation-workflow-deferred`) until 4.5 lands
  workflows-as-tools — the ChildWorkflow autonomy-classifier gap makes
  anything else unsound now. The evaluator model's "distinct policy" is the
  request's pinned profile: `authorize_goal_evaluation` resolves it from the
  request alone against the definition and setup envelopes, so the agent's
  turn-bound settings profile never clobbers it. A failed evaluation is the
  second exception (beside memory promotion) to the run's effect-failure
  wind-down: the coordinator must outlive it so the goal stays decidable.
- **The decision door grew its remaining fences.** Beside 4.1's revision
  fence: evaluator identity (`goal-evaluator-mismatch`; `evaluator: None`
  keeps the 4.1 allow-any-commander contract), required-evidence coverage
  over the new classed `evidence_items` (`goal-evidence-missing`), and
  evidence bounds (`AGENT_GOAL_EVALUATION_MAX_EVIDENCE = 16`; a spec
  requiring more classes than one evaluation may present is refused as
  statically unsatisfiable). `AgentGoalEvaluationRef` grew additively:
  evaluation id, method, evidence items, and the SHA-256 attestation digest
  of the full record — the cryptographic one, never the FNV fingerprint.
  The new criteria-only `ReviseGoalCriteria` command (fenced on the criteria
  revision itself, exercising the previously dead
  `AgentGoalSpecRevision::updated`) makes the staleness fence real; an
  in-flight evaluation is invalidated purely by that existing fence.
- **Stagnation detects at the epoch settlement, and only there.** The
  detector needs no settle-pass observation: no stagnation fact becomes true
  by time passing, so `record_epoch_progress` accounts each settlement beside
  the failure streak — completed epochs only, `result_digest` as the
  repetition fingerprint (previously dropped at settle), streaks and trip
  counters additive on `AgentGoalLifecycleState`/`AgentWakeCounters`, trips
  exactly at a set threshold, `RepeatedResult` before `NoProgress`. The
  thresholds live in `AgentGoalStagnationPolicy` on the spec (disabled by
  default — the `escalate_after_failures` posture; user-approved), `Replan`
  is typed but refused at validation until a slice can execute it honestly
  (user-approved), and the actions execute in `apply_goal_stagnation`, a
  parallel of the exhaustion executor under the same infallibility
  obligation: `Continue` records only; `Wait`/`Escalate` park
  `Waiting(Stagnant)` and close the gate *before* the release so a coalesced
  occurrence is never promoted; `Terminate` fails goal
  (`Stagnant` → `Failed`, never `Unsatisfied`), task (`goal-stagnant`), and
  gate together. `ResumeGoal` owns the wait and performs the one deliberate
  non-progress reset; widening the gate-resume fence to refuse *any* wait it
  does not own fixed a real 4.1 gap — a stagnation park (exhaustion-free)
  would have slipped the old `exhaustion().is_some()` fence and split the
  two records permanently. Worst-case history stays inside the
  `max_dependencies + 3` headroom: a terminate settlement records
  detection + decision + termination + settlement, and stagnation rows are
  mutually exclusive with failure-escalation rows (one outcome class per
  settlement).
- **Deliberately out of scope, documented:** finite-goal stagnation (no
  epoch signal; the finite root's repeated units are already bounded by
  rejection/assignment ceilings — delegation slices extend the detector),
  within-run repetition (the loop's iteration budget bounds it), stale
  environmental assumptions (needs 4.6's environment surface), goal-scope
  HITL beyond the evaluation checkpoint, and a post-completion evaluation
  path: a finite goal's coordinator evaluates before proposing its result —
  after `ResultAccepted` clears the assignment, the sender fence refuses,
  and an unevaluated completed root's goal remains decidable by
  cancellation/expiry until later slices own reassignment.
- **Bookkeeping.** New audit kinds `GoalStagnationDetected` (repeated
  fingerprint in the digest slot) and `GoalCriteriaRevised`; `EpochSettled`
  rows carry the epoch's result fingerprint (history stays observability —
  the durable counters are the correctness record); the
  `rakka.agent.goal.stagnation{trigger}` counter counts trips by
  durable-counter difference (an observe-only `Continue` trip is visible;
  replays count nothing); `AgentGoalStatusView` gained the configured
  `evaluator`; the goal-evaluation target class routes as substrate
  `ToolCall` with target type `"goal-evaluation"` and classifies `Other` in
  the autonomy catalog — the memory-promotion posture, failing closed under
  strict autonomy policies.

### Slice 4.3 — Durable delegation and A2A collaboration metadata

Spec: [8.4](spec.md#84-specialization-and-durable-delegation),
[6.6](spec.md#66-agentdelegationid),
[14.4](spec.md#144-agent-to-agent-effects).
Guidance: [Durable Delegation Graph](technical-guidance.md#durable-delegation-graph).

- `AgentDelegationId` records persisted before send with the full field set
  of [spec 8.4](spec.md#84-specialization-and-durable-delegation); replay
  resolves to the same child or an explicit conflict.
- Versioned A2A collaboration metadata extension carrying goal/parent/
  delegation/lineage/budget/deadline; unknown-optional compatibility and
  fail-closed required metadata ([spec 14.4](spec.md#144-agent-to-agent-effects)).
- Application-owned catalog resolution boundary: model requests a skill; the
  catalog resolves the `AgentId`/endpoint (open decision 15).
- Peer calls only via the outbox + `rakka-a2a`; the model cannot reach a peer
  through a generic tool.

Done when: scenarios 28 and 39 pass.

**Amended as implemented (2026-07-31):**

- **Initiation is a model-visible coordination tool; child results defer to
  4.4.** The loop's `evaluate_model_output` intercepts calls to the one
  declared coordination tool (wired via
  `AgentRunEntityStore::with_delegation(AgentRunDelegationConfig)`; the
  config refuses construction without the `Delegation` capability) and
  commits the `AgentDelegationRecord` plus its
  `AgentRunEffectKind::A2aSendCall` effect in one compare-and-set — the
  record lives in the run's bounded cell map
  (`AGENT_RUN_MAX_DELEGATIONS = 16`), not on the task, which makes scenario
  39's "parent task identity and ownership unchanged" a construction
  property. This slice ends when the child task/run is durably created and
  the send outcome settles the cell (`ChildCreated`/`Conflicted`/`Failed`);
  no child→parent result return, no fan-in, no parent wait-for-children, and
  the continuation-send ingress stays refused. No new exchange kind: the
  send is an effect through the outbox and `rakka-a2a`, and
  `AgentOperationKind::{Delegation, A2aSend}` remain reserved — convergence
  rests on the derived deduplication key through the single task-creation
  ingress, so a delegated and a plain creation cannot diverge.
- **Every identity is a pure derivation.** `delegation_id_for(scope, turn,
  slot)` (the wake-id digest construction) doubles as the A2A message id and
  the deduplication key; the receiving surface derives the child
  `AgentTaskId` from the key, so `rakka-a2a`'s id derivation stays out of
  `rakka-agent` and the child ids fill in from the send receipt. The send's
  policy defaults idempotent, three attempts, no reconciliation protocol —
  an ambiguous loss retries safely under the same key. Explicit conflict is
  a settled cell status: the peer's `task-already-created` maps to
  `delegation-child-conflict`, and a child answering under another
  delegation (detected by the projection's `io.rakka.collaboration` echo) to
  `delegation-child-mismatch`; catalog drift cannot mint a second child
  because resolution happens once, inside the committing compare-and-set,
  and replays reuse the recorded `AgentDelegationTarget` verbatim.
- **Refusals let the run survive (user-approved divergence from the
  dispatch-authority wind-down).** Parse, cap, skill-narrowing, catalog, and
  bounds refusals become failed tool results under stable codes; the model
  corrects course inside the existing iteration/budget ceilings, and
  stagnation detection catches futile retries. `delegation-not-configured`
  turned out unreachable and was dropped: an unwired run cannot recognize
  the tool, so its calls take the generic path where the authority's
  defense-in-depth refusal `coordination-tool-not-intercepted` (real
  enforced code, not just structure) answers. `allowed_tools` enforcement
  joined the slice as approved: goal-scope narrowing rides the new
  `AgentRunAssignment.delegation` envelope (`AgentRunDelegationEnvelope`,
  copied from the goal spec at the root or from the child's own
  `AgentTaskDelegationProvenance`, which also gives every run its
  lineage/depth so 4.4 enforces ceilings without schema change); an empty
  set means no narrowing — declaredness is not recorded, so empty-set
  fail-closed was not implementable honestly.
- **The extension is one metadata object, not a data part.**
  `urn:rakka:a2a-extension:collaboration:v1` +
  `io.rakka.collaboration` carrying `AgentCollaborationMetadata` (the
  management-extension versioning pattern: version in the URI, schema number
  in the envelope, fail-closed on every half-formed engagement including the
  reserved key without the declaration); the message's parts stay the
  child's input. Ingress converts the envelope to the child's recorded
  provenance and parent/goal bindings; `escrow` stays `None` — the
  envelope's budget is advisory provenance because a conserved grant cannot
  ride A2A (4.4 enforces). `A2AAgentDelegationSendExecutor` implements the
  `AgentA2aSendExecutor` port in-process over the same service core an
  external caller uses; `AgentDispatchTargetClass::A2aPeer` accepts the
  executor-routed tool family so a declared `a2a-peer` target classifies
  truthfully. Open decision 15's disposition is recorded in spec 21.3.
- Proof roster: `tests/delegation_record.rs`, `tests/delegation_dispatch.rs`,
  the `tools.rs` authority pin, the `task_bounded_state.rs` provenance
  bounds, the substrate's `A2aPeer` classification pin, and `rakka-a2a`'s
  `tests/collaboration_surface.rs` (scenario 39 end to end and scenario 28's
  A2A half, the fail-closed matrix, plain-client compatibility, credential
  hygiene); the whole M3, 4.1, and 4.2 suites pass unchanged.

**Amended after review (2026-08-01):** three review findings closed before
4.4 builds on this state.

- **Every fence settles the cell.** The in-place fence
  (`fence_unsent_effects`, reached by cancellation and the failed-effect
  wind-down) and the winding-down `ConfirmedNotExecuted` reconciliation now
  settle a fenced send's cell `Failed { run-winding-down }` exactly as the
  dispatch-layer fence always did — no `Pending` cell can survive under a
  cancelled effect for 4.4's fan-in to misread as an in-flight child.
- **Ingress provenance is byte-bounded.**
  `AgentTaskDelegationProvenance::validate()` enforces
  `AGENT_DELEGATION_PROVENANCE_MAX_BYTES` (8 KiB, the receiving side of the
  parent's record bound) besides the lineage cap, so a peer's scope and
  binding collections cannot inflate the child's durable record; the
  creation refuses whole under `task-delegation-provenance-invalid`.
- **`task-already-created` is disambiguated, not presumed a conflict.** The
  child's deduplication window is bounded
  (`AGENT_TASK_OPERATION_LOG_CAPACITY`), so an aged-out replay of a
  delegation's own send earns the same refusal a genuine conflict does. The
  executor now fetches the held task and compares its collaboration echo
  first: an echoing child converges as a replay, and only a foreign child is
  `delegation-child-conflict` — this supersedes the unconditional mapping
  described in the 2026-07-31 note above.
- **Delegation admission is priced against the materialized bound.** A
  committed delegation is held twice in the run's durable record (cell and
  effect payload), so the interception refuses
  `delegation-headroom-exceeded` when twice the record's serialized bytes
  plus a fixed commit overhead would cross the run's remaining
  `AGENT_RUN_MATERIALIZED_MAX_BYTES` headroom — a failed tool result the
  model corrects course from, never a committed turn the bound check wedges
  afterwards on every re-drive.
- **Refusal text is bounded at the recording door.** A catalog's
  `Unavailable` refusal carries application text of unchecked length; the
  refusal tool result passes both code and message through the run's
  bounded-detail truncation, so an oversized message cannot fail the inline
  content bound and poison the transition.
- **Forged parent bindings refuse at the creation door.** The provenance's
  declared depth must agree with its presented lineage (depth is always the
  ancestor count plus one; `delegation-depth-incoherent` otherwise — the
  record enforces the same coherence), and the parent run must live in the
  child's own tenant. 4.4's ceiling and cycle enforcement reads these fields
  as validated inputs, never as peer assertions. The envelope's `allowed_*`
  rustdoc now states the enforced empty-means-no-narrowing semantics, and
  `create_task`'s doc comment is back on `create_task`.

### Slice 4.4 — Fan-out/fan-in, lineage, and coordinator limits

Spec: [8.4](spec.md#84-specialization-and-durable-delegation),
[8.7](spec.md#87-cancellation-failure-and-waiting) (fan-in policy),
[9.7](spec.md#97-hierarchical-budget-ledger) (descendant dimensions).

- Durable fan-out groups and deterministic fan-in (all/any/quorum/policy)
  fixed before results are accepted; parent passivates while waiting.
- Depth/fan-out/descendant/concurrency ceilings via the escrow ledger;
  lineage-based cycle rejection.

Done when: scenarios 27 and 34 pass, including coordinator loss and resume.

**Amended as implemented (2026-08-02):**

- **The cells are the membership; the group is one cell beside them.**
  `AgentFanInCell` on the loop state opens in the same CAS as the first
  committed delegation — the policy (`AgentFanInPolicy::{All, Any, Quorum}`,
  non-exhaustive for the deferred policy-evaluator variant) comes from the
  goal spec's new `fan_in` field or the wiring's `default_fan_in`, never
  model output, so "fixed in durable state before results are accepted" is a
  construction property with no early-result window. The model's second
  declared coordination verb (`with_fan_in_tool`; closed vocabulary carrying
  only an optional deadline) closes membership; the run rests in the new
  `AwaitingChildren` phase under status `Running` (the documented honest
  non-residency status — no new `AgentRunStatus`), holding no effect and no
  residency; a resolved group is absorbing and the next delegation replaces
  it, so sequential rounds are one cell, not a history. `evaluate_fan_in` is
  a pure function of durable cells + policy + parent-side `timed_out` marks;
  the bounded resolution table (no child content, no repeated child ids, the
  reason codes truncated — the growth-reserve test caught the unbounded
  shape) answers the awaiting call, and the model still proposes: fan-in
  never completes the parent task. `FireFanInDeadline` marks stragglers
  timed out and resolves; chasing them is 4.6.
- **Result return = ninth exchange, user-approved divergence from spec
  8.4's letter.** `AgentExchangeKind::DelegationResult`, child task → parent
  run over the courier (the epoch-result template verbatim): owed exactly
  once from the transition that closes the terminal child's ledger, pure
  operation id `delegation_result_operation_id(tenant, delegation)`,
  `AgentDelegationReport` references only. Parent-side fences: sender must
  be the very child the cell created; `Pending` cell → `delegation-result-
  early` (re-drivable); settled-non-created → `-not-owned`; unknown →
  `-unknown-delegation`/`-unknown-run`; the cell's `result` field is the
  first-writer-wins durable fence past the journal window; a terminal or
  winding-down parent records evidence and resumes nothing. The child-side
  settle rule advances only on the four definitive codes. The 9.8
  failure-window table lives on the kind's rustdoc. The remote A2A carrier
  (a collaboration result envelope + the `ContinueTask` lift) is reserved
  for federation, when a remote forward executor exists at all.
- **Limits split by conservation.** Descendants is the 8th conserved
  `AgentBudgetDimension`: seeded at `create_task` from min(goal spec,
  definition's new `AgentTaskDefinition.delegation` ceilings — the forged-
  root-child defense, approved in-slice — spec allocation), escrowed
  task→run via the existing `open_child`, spent 1 + `granted_descendants`
  per live cell at the door (settled-failed sends release; pending counts),
  folded into consumption once at `terminate` so the existing ledger
  exchanges conserve it across generations. The new `WORK` set keeps
  `first_empty_for` from refusing assignments over `descendants: Some(0)`
  (verified hazard). Depth/fan-out/concurrency stay door checks off the
  envelope + cells, priced per planned call; `max_concurrent` re-documented
  as per-run unsettled direct children. The even-split sub-quota crosses
  A2A as the child's narrowed `max_descendants` (a validated cap the child's
  seed min-narrows below its own ceilings — never a conserved grant);
  credit-back of unused sub-quota is deliberately off, `descendants_created`
  recorded on the cell for a later slice.
- **Cycle rejection reads a validated ancestor-agent chain.** Lineage
  entries are delegation digests, so the parallel `ancestors: Vec<AgentId>`
  (record/provenance/envelope/collaboration metadata; skip-if-empty keeps
  root sends v1-wire-compatible, deeper chains fail closed cross-version;
  coherence `len == lineage.len()` enforced at every door,
  `delegation-ancestry-incoherent`) is what makes direct and indirect
  rejection implementable: resolved target ∈ ancestors ∪ {own agent} →
  `delegation-cycle-detected`. An unaccounted chain (pre-4.4 parent) works
  but cannot sub-delegate (`delegation-ancestry-unknown`). The bounded-
  iterative escape hatch stays deferred, refusal-only.
- **A failed send became a fan-in disposition.** With every delegation now a
  group member, a definitive send failure settles the cell, reaches the
  model as the call's failed tool result, and the coordinator survives —
  superseding 4.3's unconditional wind-down (which still governs sends
  outside any fan-out group). Membership alone is the test, never a
  still-unresolved group: an `Any` satisfied by its first child while a
  sibling's send was still in flight must not be wound down by that
  straggler's later failure. `delegation_dispatch.rs`'s conflict test pins
  the survival, and `fan_out_fan_in.rs`'s post-resolution straggler pins the
  resolved-group half.
- Proof roster: `tests/fan_out_fan_in.rs` (scenario 27's loop and fabric
  halves, with real child task entities and re-driven settles),
  `tests/delegation_limits.rs` (scenario 34's six classes, each fail-closed
  and re-derived across the fixture's per-command restarts), the pre-4.4
  decode pin, the two-part growth-reserve/door-price empirical test,
  `fan_in.rs`'s policy/order-invariance units, and
  `collaboration_surface.rs`'s ancestors round-trip and forged-ancestry
  refusals. Owed onward: snapshot/projection views (4.7), cancellation
  propagation and straggler chase (4.6), workflow members reusing the
  member-disposition interface (4.5), descendant credit-back, and the
  federated A2A result carrier.
- **Review fixes (2026-08-03).** Four findings closed after review: (1) an
  `AssignmentsExhausted` terminal now owes the child's terminal reports from
  the terminating transition, and a run-refused generation releases its
  escrow at its settle — without both, an `All` parent with no deadline
  parked forever over a definitively failed child; (2) the planner refuses a
  delegation planned after the same turn's await (`delegation-after-await`)
  — the orphan member would have revived the superseded wind-down;
  (3) `delegation_envelope_for` grew a third, definition-only arm so epoch
  and plain tasks enforce `AgentTaskDefinition::delegation` at the door;
  (4) `create_task` refuses a creation carrying both a goal spec and
  delegation provenance, closing the lineage re-rooting door. Pinned by
  `an_assignments_exhausted_delegated_child_owes_its_delegation_result`,
  `a_delegation_planned_after_the_await_is_refused_and_the_run_survives`,
  `a_plain_tasks_definition_ceilings_enforce_at_the_door`, and
  `a_delegated_creation_carrying_a_goal_spec_is_refused`.
- **Minor-findings pass (2026-08-04).** A model-supplied await deadline must
  lie in the future (`fan-in-invalid-arguments` otherwise; the envelope
  bound stays trusted); a direct `settle_side_effects` sweep counts its
  fan-in resolution (public wrapper + unsampled inner pass); the
  `FireFanInDeadline` rustdoc states the hosting application owes the
  scheduler — record that obligation wherever ops docs grow; the goal
  door's quorum bound delegates to `AgentFanInPolicy::validate`;
  `satisfied_by` documented as timing-dependent evidence. Coverage added:
  quorum through the run entity, deadline-vs-late-result absorption, uneven
  sub-quota split per slot, conflicting-duplicate first-writer fence, exact
  forged-report codes, wound-down pinned by phase+effects, duplicate
  deadline counts once, pre-4.4 run-state decode, ancestors-key-omitted
  wire decode. Still owed from the review: a descendants-conservation test
  across an actual replacement generation (the terminal fold is pinned;
  driving a real second generation through a failed run is not), and
  crash-point sweeps over the new fan-in compare-and-sets.

### Slice 4.5 — Workflows as tools

Spec: [8.6](spec.md#86-workflows-as-tools),
[11.7](spec.md#117-tool-registry-and-component-tools).
Guidance: [Workflows as Tools](technical-guidance.md#workflows-as-tools).

- `WorkflowToolDescriptor`; invocation creates or adopts an independently
  durable child workflow run keyed by stable identity; parent waits durably.
- Internal workflow effects keep their own boundaries — never one opaque
  retryable effect; fix the autonomy classifier gap where
  `AgentDispatchTargetClass::ChildWorkflow` maps to `Other` (see
  [research](background-research.md#workflows-as-tools); verify current code
  first).

Done when: scenario 32 passes (replayed invocation adopts one child run, no
duplicated internal effects).

**Amended as implemented (2026-08-04):**

- **Create-or-adopt is an identity property, not a protocol.** The
  interception commits — in one CAS, the 4.3/4.4 discipline — the
  `AgentWorkflowInvocationRecord`, its cell, its fan-in membership, and the
  new `WorkflowStartCall` effect. The derived
  `workflow_invocation_id_for(scope, turn, slot)` (delegation-digest
  construction, disjoint `workflow-invocation-` prefix) *is* the child
  workflow run id and the `StartRun` deduplication key, and the command id
  (`{invocation}#start-run`) is **generation-free**: a reconciled new effect
  generation re-derives the identical `StartRun`, so recovery can never mint
  a second child run — the scenario-32 keystone. The descriptor's shape
  (version, digest, workflow type and definition version) is copied at
  commit; a replay never re-resolves, and a mid-flight descriptor upgrade is
  a `Conflict`, never an adopt (dedup-key match under a foreign command id
  likewise). The effect is the start, never the workflow; it completes at
  the durable start receipt, and the wait is fan-in membership.
- **The model surface is the config map, not the registry.** Each configured
  `AgentWorkflowToolDescriptor` (in `AgentRunWorkflowConfig`, wired via
  `with_workflow_tools`) appears as its own named tool; the planner
  intercepts by map lookup after the coordination arm and before the goal
  tool narrowing. A kind-`Workflow` registry entry that reaches generic
  dispatch refuses `workflow-tool-requires-interception` — defense in depth.
  The dead placeholders now enforce: `workflow_tools` on the envelope
  per attempt (`undeclared-workflow-tool`), `allowed_workflows` at the door
  (`goal-workflow-not-allowed`) via the delegation envelope.
- **Workflow members reuse the member-disposition interface** (the 4.4 owed
  item): `AgentFanInMemberId` widens members/timed-out/satisfied-by as raw
  prefixed id strings — a delegation-only 4.4 group round-trips
  byte-identically, and a pre-4.5 node reading a mixed group parks
  deny-when-unknown: the load-bearing cross-version fence, since a turn's
  effects clear when it records (while retained, the `workflow-start`
  request variant additionally fails a pre-4.5 binary loudly as
  unknown-variant). No schema bump. One group, one
  CAS join, `AwaitingChildren` reused, `workflow-after-await` mirrors 4.4's
  refusal, combined membership bounded at both doors, failed/conflicted/
  unwired starts are surviving fan-in dispositions, wind-down fences settle
  unsent starts' cells. Deliberate: workflow invocations never debit
  `Descendants` (no agent-task creation path; `descendants_created`
  recorded for a later credit fold) and never count against
  `max_concurrent` (delegation-envelope ceiling; the membership bound is
  the cap until a descriptor-level ceiling exists).
- **The result path is an entity command, not a tenth exchange.** A workflow
  run is not a choreography participant, so
  `AgentRunEntityCommand::RecordWorkflowResult` (pure
  `workflow_result_operation_id(tenant, invocation)`; the hosting
  application owes the relay, the `FireFanInDeadline` obligation idiom)
  carries the terminal status, bounded reason, and result reference/digest.
  Refusals are non-committing errors (`workflow-result-unknown-run`/
  `-unknown-invocation`/`-forged`/`-not-owned`; non-terminal statuses are
  unrepresentable), duplicates answer from the journal and the cell's
  first-writer-wins result behind it, a wound-down parent records evidence
  and resumes nothing — and there is deliberately **no early window**,
  diverging from `delegation-result-early`: the child's identity is derived
  at commit, so an early result authenticates against the record and
  records first-writer-wins while the receipt settles the effect
  independently.
- **The classifier gap is closed minimally.**
  `AgentAutonomyTargetClass::ChildWorkflow` is first-class
  (`from_dispatch_class`, `from_label`, phase-5 catalog under
  `DeduplicationKey` idempotency, policy allowance, concurrency seed);
  dispatcher registration and the compiled-plan node stay out of scope, and
  an unregistered class still fails closed. `VerificationWorkflow` stays
  refused (`run-goal-evaluation-workflow-deferred`), re-worded to name the
  remaining work: bridging the evaluation cell to this invocation path.
- Proof roster: `tests/workflow_tool.rs` — the derived-identity commit, the
  scenario-32 crash sweep (one invocation/child-run/`StartRun` identity
  across every owner-loss window's executor sightings), the end-to-end
  adopt over a **real** child `AgentRunInbox` (replayed invocations
  deduplicate in the child's own durable inbox before and after its one
  internal step executes exactly once; the relayed result resumes the
  parent to completion), duplicate/conflicting/forged/not-owned results,
  the no-early-window proof, the wound-down parent, the after-await and
  goal-narrowing refusals, the mixed group, and the pre-4.5 decode +
  wire-tag pins; `fan_in.rs` member round-trip/prefix/mixed-group units;
  `workflow_tool.rs` (src) identity and bounds units; the classifier pins
  in `rakka-agent-workflow`. Owed onward: cancellation propagation to child
  workflows (4.6), the evaluation-cell bridge for `VerificationWorkflow`,
  descriptor-level concurrency ceilings, descendant credit for
  `descendants_created`, a combined-membership 17-member integration sweep
  (the bound is door-enforced and unit-covered; a full 17-effect turn
  exceeds the per-turn effect bound), snapshot/projection views (4.7), and
  an envelope-side per-workflow-tool capability declaration — the review
  pass made `required_capabilities` flow (copied onto the record at commit,
  carried on the dispatch grant per attempt), but the definition envelope
  declares workflow tools by id only, so the regular tool declaration's
  capability *subset* check has no definition-side set to check against
  yet.

### Slice 4.6 — Cancellation propagation and shared environment

Spec: [8.7](spec.md#87-cancellation-failure-and-waiting),
[8.5](spec.md#85-shared-environment-and-collective-memory).

- Durable cancellation/deadline/revocation propagation to children with the
  progress model (`Requested` -> `Propagating` -> `Quiesced` ->
  `WaitingForReconciliation` -> `Completed`); child indeterminate effects
  stay in reconciliation.
- `AgentEnvironmentRef` contract and tool-adapter concurrency rules;
  communal claims carry goal/task/run/delegation provenance (needs the
  Phase 2 graph; defer scenario 33 if Phase 2 has not run).

Done when: scenarios 29, 31, and 33 pass.

**Amended as implemented (2026-08-05):**

- **Propagation is edges over the vocabulary M1 already fixed.**
  `AgentCancellationProgress` needed only its `Propagating` arm — the run
  derivation reads `awaits_children()`, the new subtree half of the
  quiescence condition — and the progress model stays a pure derivation of
  durable record, never a stored enum. Four legs: goal → root task is
  intra-entity (the settle pass's new `settle_requested_cancellation` step,
  directly after `observe_goal_deadline`, converts any terminal goal
  decision in the cancel/expiry families —
  `AgentGoalTerminalReason::requests_root_cancellation()` — into the root's
  request, so operator cancel, retirement, and deadline expiry ride one
  chokepoint); task → run is the tenth exchange `RunCancel` (owed only
  after durable acceptance — the Offered-window race fix); parent run →
  child task is the eleventh exchange `DelegationCancel` (in-fabric, the
  `DelegationResult` precedent; A2A carrier reserved for federation);
  parent run → child workflow is the `WorkflowCancelCall` effect
  (generation-free `"{invocation}#cancel-run"`, the start's discipline,
  gated on `supports_cancellation` with durable
  `Unsupported`/`Unaffordable` cell dispositions when no effect may exist).
  The child's `DelegationCancel` arm calls the same request core and owes
  its own `RunCancel` onward — recursion is the machinery re-entering.
- **The task defers; the ledger is the finalization gate.** The
  pre-existing gap — `Cancel` terminalized the task over a run holding an
  indeterminate effect — closed: the nonterminal `AgentTaskCancellation`
  marker decides the goal and closes admission at request time, fences
  assignment (`awaits_assignment`) and proposals (`task-cancel-requested`,
  definitive), and finalizes through the existing `terminate` only when
  `escrow.outstanding()` is empty — budget settlement travels only after a
  known terminal run outcome, so ledger closure is durable proof of
  quiescence and no new "run terminal report" exchange exists. A task with
  no live generation finalizes in the requesting transition. Run-side,
  `settle_run_disposition`'s winding-down branch gained
  `awaits_children()`: a cancelling parent rests until every delegation and
  workflow cell holds a terminal outcome, the last child result
  terminalizes it (settle tails at `accept_delegation_result` and
  `record_workflow_result`), and a child parked in reconciliation holds the
  ancestry nonterminal — scenario 31's "never falsely claim their started
  effects stopped", with the child's view `WaitingForReconciliation` and
  the parent's `Propagating`.
- **The chase is one pure condition.** A created, unsettled child is chased
  when the run winds down under a cancellation *or* when the resolved
  fan-in group left it unresolved — which makes `FireFanInDeadline`'s
  timed-out stragglers and an early `Any`/`Quorum` satisfaction's losers
  the same case with zero plumbing, since `owed_run_exchanges` runs after
  every transition. Owed cancels never set `terminal_reason`, so a chase
  cannot wind a satisfied coordinator down; the cell's settled
  `AgentDelegationCancelOutcome` / `AgentWorkflowCancelDisposition` is the
  durable once-guard past the journal's bounded ring and the request's
  observable outcome. Wind-down dispatch fences became kind-based
  (`exempt_from_wind_down_fence`: compensation + workflow cancel), fixing
  the claim-path fence's missing `CompensationCall` exemption alongside.
- **Revocation stays honest.** A revocation-driven goal decision rides the
  same propagation; per-agent `RevokeTool`/`RevokeCredentialBinding` stays
  pull-at-next-dispatch (already immediate for every agent's own
  dispatches, descendants included); agent lifecycle Suspend/Terminate
  fan-out needs an agent→run registry and stays deferred, as do the
  dependents-registry sending half (re-pointed at 5.4), per-delegation
  child-side deadline enforcement, and descendant credit-back.
- **The environment contract is declaration + protocol + per-attempt
  doors.** `AgentToolDeclaration.environments` (observe *is* `ReadOnly` —
  no second mode axis), `AgentEnvironmentConcurrencyProtocol` on the
  binding (no fail-open variant; required at registration exactly when a
  mutating tool names an environment; `Environment`-kind descriptors must
  name one), ordered authority checks (binding ⊆ declaration ⊆ definition
  envelope, `setup-excludes-environment`, `goal-environment-not-allowed`)
  with the goal scope reaching the authority through the run's delegation
  envelope on the context; the adapter-side rules are the trait contract —
  Rakka cannot enforce the external protocol. Scope projection: envelope
  `environments` is a narrowing (empty = none), envelope
  `knowledge_spaces` is a fail-closed grant (`Option`; `None` under
  lineage refuses — the ancestry-gap posture), the catalog's explicit
  `AgentDelegationTarget.knowledge_spaces` intersects the parent's grant
  at the interception door, and both ride the record, the provenance, and
  the A2A metadata skip-if-empty.
- **Scenario 33 is a command-initiated effect (user-approved).**
  `AppendClaim` → `ClaimAppendCall`, the `PromoteMemory` idiom: provenance
  stamped from durable run identity in the committing transition (agent,
  goal, task, run, delegation = envelope lineage tail), space validated at
  the door (`run-claim-space-not-delegated`) and per attempt at the
  authority; the executor trait lives in `rakka-agent`
  (`AgentCommunalClaimId` is a mirror newtype — the dependency runs
  graph → agent) and the graph crate ships
  `KnowledgeGraphClaimAppendExecutor`, deriving the store operation from
  the intent's external idempotency key so a generation's attempts converge
  on one claim and a re-decided generation is a new one. The pre-derived
  `claim_promotion_*` ids stay untouched for the deferred promotion-gate
  flow; the model-visible claim tool, communal retrieval and the
  `SnapshotCommunalClaim` shape, per-claim read-capability enforcement,
  and claim metrics stay deferred.
- **Review pass (2026-08-05).** Four liveness holes in the new quiescence
  machinery closed: the fired-deadline chase (the deadline marks its
  stragglers `timed_out` *before* resolving, so reading `unresolved_members`
  chased nobody — the chase set is now `unreported_members`, unresolved **or**
  timed out); a definitively-refused delegation-cancel now releases its cell
  from `awaits_children` and re-checks the disposition in that settle, where
  before a child that could never report held the parent `Cancelling` forever
  with its escrow open; `commit_workflow_cancels` refuses to commit on a
  terminal run and stops at the outstanding-effect bound rather than
  overflowing it into a transition that re-aborts forever; and
  `fence_unsent_effects` now honours `exempt_from_wind_down_fence`, with an
  already-winding-down run answering a re-driven run-cancel idempotently, so a
  re-entered wind-down cannot fence the compensation or workflow-cancel the
  first one authorized. Two ingresses that still terminalized over a live run
  — a failed dependency, and a proposal refused by the cancellation fence —
  now take the request path (`AGENT_TASK_REFUSAL_CANCEL_REQUESTED` is the
  run-side constant, the stale-generation precedent). `derive_task` reads the
  snapshot's new `outstanding_escrow` rather than the assignment alone (a
  cancelled continuous root between epochs was reporting `Quiesced` over
  running epochs); `AgentClaimAppendRequest::validate` gained its
  `confidence_bps` range check; and the `RecordWorkflowResult` relay is
  documented as load-bearing for cancellation — unwired, a workflow-invoking
  deployment cannot complete one, which is 8.7's own posture, with the command
  itself being the "explicit reconciliation decision" 8.7 names as the way
  out. Coverage added for the environment/knowledge authority doors, the
  graph-backed append executor (new
  `rakka-agent-knowledge-graph/tests/claim_append_executor.rs`), the widest
  provenance with both scope sets full, the door price with the cell's cancel
  outcome, the fired-deadline chase, and the dependency deferral.
- Proof roster: `tests/cancellation_propagation.rs` (the scenario-31 spine
  over real child entities with the send-log pinning scenario 29's
  at-most-once half; receiver fences; settle-pass expiry propagation; the
  `Any`-resolution chase), `tests/communal_claim_append.rs` (the stamp,
  the doors, replay convergence, the delegated grant), the
  `concurrent_specialist_append_provenance` conformance clause across both
  backends plus the racing two-connection PostgreSQL append proof, the
  environment-contract registration units, and the honest-semantics
  updates in `goal_contract.rs`, `task_entity.rs`, and
  `workflow_tool.rs`. Owed onward: snapshot/projection views (4.7), the
  crash-point sweeps over the new task-cancellation compare-and-sets, and
  the deferrals above.

### Slice 4.7 — M4 acceptance and goal views

Spec: [Multi-Agent Goal Milestone](spec.md#multi-agent-goal-milestone-m4),
[17.18](spec.md#1718-authoritative-operational-queries-and-observability-views).

- Authorized goal projection (tasks, runs, delegation graph, workflow links,
  evaluations, evidence, budgets, cancellation state).
- End-to-end example: root goal delegating to two specialists plus one
  workflow tool, surviving root and child pod loss, satisfied only through
  the evaluator.

Done when: the multi-agent milestone checklist is demonstrated end to end.

**Amended as implemented (2026-08-06):**

- **The goal view is one bounded assembly over durable state, in `query.rs`.**
  `assemble_agent_goal_view` resolves the root by the recorded
  open-decision-14 default (goal identity = root task value; any other goal
  id answers absent, documented), walks breadth-first over the delegation
  cells' `ChildCreated` edges plus a continuous root's admitted epochs
  (epoch refs live only while their occurrence is active — a released epoch
  is history, owned by the task projection), and joins children fail-closed:
  provenance not naming the traversing delegation, a foreign goal binding, a
  missing record, or an unreadable schema each become a stable
  `AgentGoalViewOmission` code rather than a joined forgery or a failed
  view; only the root record failing fails the call. The view is documented
  as a causal cut, never a snapshot: per-node durable revisions,
  `root_revision` as the one fence-able anchor, `records_read` +
  `observed_at` as the multi-read freshness statement, and a MUST-NOT on
  authorizing or advancing execution. Node budget
  `AGENT_GOAL_VIEW_MAX_TASKS = 64`, truncate-with-marker (never refuse),
  plus `assemble_agent_goal_view_bounded` clamped to `1..=64` — the seam
  that makes the truncation path testable and cheaper views possible.
- **Each task resolves its highest-generation run even after completion.**
  `ResultAccepted` clears the assignment, so the walk re-derives the last
  run from `assignee` + `assignment_generation` when the assignment is gone
  — found by the acceptance walk itself: a *completed* goal reconstructed
  nothing, exactly when reconstruction matters. Earlier generations stay an
  explicit gap (the node's generation counts surface it); full run history
  is the 17.18 task projection's job, later work.
- **Authorization = the owner check the record can answer (user-approved).**
  `authorized_agent_goal_view` fences on `AgentGoalSpec.owner`; a non-owner
  gets `Ok(None)` byte-identical to a missing goal (proven against the
  absent-goal and child-task-id-probe answers), short-circuiting after the
  root read. The principal-free core remains the composition point for a
  boundary authorizer; an `A2AOperation`-gated wire surface + typed-client
  query stay recorded follow-ups.
- **The 4.3-4.6 snapshot debt is one shared derivation.**
  `AgentRunCollaborationView` (delegation edges, fan-in with the
  `unreported_members` chase set, workflow invocations, the evaluation view,
  retained claim-append effects) is carried by `AgentOperationalSnapshot`
  (serde-defaulted; pre-collaboration snapshots serialize unchanged — the
  `skip_serializing_if` keeps old bytes byte-identical) and by the goal
  view's run nodes, so the run-scoped query and the goal view can never
  disagree. Redaction follows 17.14: delegated input, credential/capability
  refs, proposals, results, and the objective summary never ride a view;
  digests and stable codes do.
- **Shared-knowledge references are a port with honest degradation
  (user-approved in scope).** Settled claim receipts are pruned at
  `clear_turn`, so the view carries three layers: the goal's grant
  statement, retained `ClaimAppendCall` effect views, and the joined
  `AgentGoalClaimSource` port — implemented by
  `KnowledgeGraphGoalClaimSource` beside the append executor (graph → agent
  dependency direction), serving explicitly named spaces over
  `ClaimFilter::with_goal`. Absent/failing source ⇒ `claims_available:
  false` with the durable half intact (scenario 56's shape).
- **The sharded run factory gained the coordination wiring** —
  `AgentRunEntityShardingSettings::with_delegation`/`::with_workflow_tools`,
  plumbed into every hosted run; before this no sharded deployment could
  serve the delegation or workflow-tool interception at all (the milestone
  example was the first deployment-shaped consumer). The testkit's
  `InProcessRunResultDelivery` gained the matching `with_delegation` — the
  delivered model result is where a fan-out turn is intercepted.
- **The milestone's done-when is `examples/multi-agent-goal-acceptance`**:
  an 18-line transcript pinned three ways (README, `EXPECTED_TRANSCRIPT`,
  `tests/acceptance.rs`) walking every checklist bullet — three sharded
  agents over real `ClusterSharding`; one fan-out turn committing two
  delegations + one workflow invocation + a closed three-member group; real
  children through the in-process `rakka-a2a` service core with a replayed
  send converging on the deduplication key; a registry-validated compiled
  refund workflow over a real durable child inbox, replay-adopting the same
  child with the compiled step executed exactly once; the wait fully
  passivated; root pod loss (killed result write, redelivered by the
  child's re-driven settle) and child pod loss (non-idempotent payment
  invoked once, parked Indeterminate, resolved by a deduplicated
  reconciliation decision); a provenance-stamped communal claim;
  `Satisfied` only through the configured evaluator after
  `task-goal-decision-unattested` refused the direct decision; and the
  authorized goal view reconstructing the whole tree, with content
  sentinels absent from every queried surface. Wiring facts the walk
  enforced: the definition envelope must declare a workflow tool for the
  invocation to commit (`undeclared-workflow-tool`), a specialist's
  envelope must grant a knowledge space for its append to dispatch
  (`widened-knowledge-access`), and sharded asks racing a
  passivation-in-progress retry exactly as a caller does across a shard
  handoff.
- Proof roster: `tests/goal_view.rs` (13 tests: assembly, restart
  determinism, redaction sweep, existence-safe denial, truncation, partial
  children, failed-send edges, schema fail-closed/omit split, epoch join
  and release, evaluation + terminal decision, claim join + degradation),
  the operational snapshot's collaboration assertion, the graph crate's
  `tests/goal_claim_source.rs`, and the acceptance example's triple-pinned
  transcript. Owed onward: the goal view's A2A/typed-client wire surface,
  crash-point sweeps over the 4.6 task-cancellation compare-and-sets
  (Phase 6.1), earlier-generation run assembly, and the M5
  teams/conversations dimensions the `#[non_exhaustive]` views keep
  additive.

---

## Phase 5 — M5 Coordination Capabilities

Milestone: M5. Acceptance:
[Coordination Capability Milestone](spec.md#coordination-capability-milestone-m5).
Scenarios owed: 38, 41-43, 45.

Open decisions to resolve: 6 (agent cards/assignment — slice 4.3's
`AgentDelegationTarget` catalog already implements the recommended shape
for delegation; slice 5.1 records the disposition and reuses the catalog
for handoff target resolution), 18 (first-class patterns — resolved
default), 19 (setup envelope — enforced since Phase 1), 21 (replayable
coordination events — resolved by slice 5.5).

**Slice text revised 2026-08-07** against the delivered M1-M4 architecture;
the scenario mapping and done-whens are unchanged. What prior phases
already supply: `AgentCoordinationCapabilityKind` as an admission-enforced
envelope dimension (slices 1.2/4.3), `AgentRunStatus::HandedOff`, the
coordination-tool interception door, the cell + `A2aSendCall` +
executor-port + `io.rakka.collaboration` extension idioms (4.3-4.6), the
`AgentRunCollaborationView`/goal-view lockstep rule (4.7), the per-task
replay cursor with expired-window resync (1.12), and the `WaitingForInput`
projection row (1.12). Debts explicitly parked here by earlier slices:
input delivery to an existing task refuses pending 5.4 (slice 1.12), the
dependents registry (4.6 → 5.4), the goal-view wire surface (4.7 → 5.5).
Sequencing: 5.4 depends only on Phase 1 machinery and may run first or in
parallel; 5.5 must trail 5.1-5.3, whose events it replays. Slice 5.5b was
added 2026-08-13 to own the two terminal-notification exchanges 5.2 and 5.3
had recorded against 5.5, which shipped as a read contract.

### Slice 5.1 — Capability model and handoff

Spec: [8.8](spec.md#88-coordination-capability-model),
[8.9](spec.md#89-handoff), [14.2](spec.md#142-task-identity-and-projection)
(handoff lineage).
Guidance: [Coordination Capabilities](technical-guidance.md#coordination-capabilities).

- Descriptors only — the capability *kind* set, the
  `CoordinationCapability` envelope dimension, and its admission
  enforcement shipped in Phases 1/4. This slice adds
  `AgentCoordinationCapability` descriptors (the four policy payloads) in
  `coordination.rs` as trusted definition/setup data and wires the existing
  `AgentRunDelegationConfig` in as the `Delegation` policy's realization.
  Unchanged rule: the runtime may expose capabilities to the model as
  tools, but model output cannot create capability, target, budget, or
  scope.
- Handoff reuses the delegation idioms wholesale: initiation is a
  model-visible coordination tool through the 4.3 interception door
  (refusals are failed tool results the run survives); the handoff id
  derives from (run scope, turn, slot) via the `delegation_id_for` digest
  construction and doubles as the A2A message/dedup key; a handoff cell on
  `AgentLoopState` commits in the same compare-and-set as the `A2aSendCall`
  effect whose payload is the handoff record; the `io.rakka.collaboration`
  extension widens with handoff identity
  ([spec 14.4](spec.md#144-agent-to-agent-effects) reserves it); the target
  resolves once inside the committing CAS through the
  `AgentDelegationTarget` catalog (open decision 6's disposition).
- The new machinery is same-task transfer: ingress drives a new assignment
  generation on the *same* `AgentTaskId` (`decide_assignment`, not
  `generated_task_id`); the source run is fenced from completion and effect
  scheduling; `HandedOff` records only after durable target acceptance;
  context/artifact projection is explicit-only, with no session-memory
  namespace reuse and no private-memory exposure; handoff lineage lands in
  authorized task metadata/history; traversal is outbox/inbox +
  `rakka-a2a` even colocated.
- Interactions to settle in-slice: the wind-down fence treatment of a
  pending handoff send under cancellation (`exempt_from_wind_down_fence` is
  kind-based); the 4.6 chase condition must cover a
  handed-off-but-unaccepted target; handoff does not debit `Descendants`
  (same task) but `delegation_envelope_for` needs an explicit
  handoff-target arm; the 4.7 goal-view run re-derivation
  (`run_id_for_assignment` over assignee + generation) must handle a
  handed-off generation chain — resolve the earlier-generations gap here or
  keep it explicitly surfaced.
- View lockstep: the handoff cell joins `AgentRunCollaborationView` and the
  goal-view task node in this slice (the view structs are
  `#[non_exhaustive]` for exactly this).

Done when: scenario 38 passes.

**Amended as implemented (2026-08-07).** Landed as specified, with the
in-slice interactions resolved as follows (decision points user-approved):

- **Descriptors** are construction-validated wiring, never serialized
  definition data: `AgentCoordinationCapability` and the four policies live
  in `coordination.rs`; `AgentRunDelegationConfig::descriptor()` derives the
  Delegation payload, and the handoff policy rides the same config
  (`with_handoff`, gated on the `Handoff` kind exactly as `new()` gates on
  `Delegation`) — so the existing `with_delegation` plumbing (entity, store,
  sharding settings, testkit) needed no second config path.
- **The task-side resolution machine is the load-bearing piece** the risk
  review demanded: the bounded `AgentTask::handoff` provenance (latest hop
  only, chain in history per 9.6) stashes the source assignment whole and
  carries a status + `result_settled` once-guard. It is the deduplication
  echo past the journal window, the source address for every owed
  derivation, and the goal view's source-run join. The twelfth exchange,
  `HandoffResult` (task → source run), is owed by a pure derivation the
  settle pass re-derives; its settle terminates the source
  `HandedOff` (the status finally became reachable via the new
  `AgentRunTerminalReason::HandedOff`) or restores the stashed source on a
  refusal — the **single-attempt** posture: readiness, exhaustion
  (`handoff-assignments-exhausted`), and the fail-closed escrow refusal
  (`handoff-budget-unaffordable`; the source's open escrow child makes an
  exact-fit budget unable to afford the target's generation — the recorded
  policy hook for reserved handoff headroom stays unimplemented) all
  resolve through one helper.
- **Wind-down fence**: the handoff send stays non-exempt
  (`exempt_from_wind_down_fence` unchanged); `fence_unsent_effects` gained
  the handoff-cell arm (`Failed{run-winding-down}`). The 4.6 chase needed
  no run-side arm: cancellation routes to exactly one owner through the
  task — an accepted target generation takes the `RunCancel` while the
  source terminalizes `HandedOff`; a refusal restores the source into its
  own wind-down; an unminted pending transfer resolves refused inside the
  cancellation-marker transition. An unresolved transfer holds
  `settle_run_disposition` open via `awaits_children`.
- **Interception exclusivity** goes beyond the planned refusals: the
  transfer must be the turn's only work (`handoff-with-planned-calls`) and
  refuses beside outstanding children, an unresolved group, or a live or
  ambiguous effect — closing the 8.9 replay-ambiguity window structurally.
  The delegation cycle check is deliberately not copied (A→B→A is bounded
  by the definition's new `max_handoffs`, default 4), and the planner never
  touches the descendants ceilings. There is deliberately no door-side
  escrow pre-check: the task's ledger is not readable at the run's door,
  so affordability resolves through the task's own refusal.
- **Goal-view gap (user-approved: latest hop only)**: the re-derivation
  resolves a mid-transfer task to the provenance's recorded source pair;
  generations before the latest handoff stay the explicitly surfaced gap.
- **Ambiguous send (user-approved: probe, then definitive)**: the
  `A2AAgentHandoffSendExecutor` probes the task's durable handoff echo on
  an ambiguous failure; an unanswerable probe leaves the attempt retryable,
  and exhaustion parks the source indeterminate (the run-side `Exhausted`
  arm maps a handoff send to `Indeterminate`) rather than resuming it
  beside a possibly-live transfer.
- **Wire**: the collaboration extension's handoff cluster is a second
  shape under the one metadata key, discriminated by its `handoff` field —
  the delegation envelope is untouched, old receivers fail closed on a
  populated cluster (14.4), plain clients stay untouched — and ingress
  derives the operation id under the reserved `AgentOperationKind::Handoff`.
- Proof roster: `tests/handoff_record.rs` (6), `tests/handoff_cancellation.rs`
  (3, including the crash-point sweep over the committed-but-unsent fence
  window), the goal view's handoff join + lockstep, and rakka-a2a's
  `tests/handoff_surface.rs` (scenario 38 end to end over the real service
  core + the wire fail-closed matrix). Owed onward: `AgentDecisionKind`'s
  reserved `handoff` label (interception still rides `CallTools`, the
  delegation precedent), the `rakka.agent.handoff` span rows (otel), the
  reserved-headroom policy hook, and 5.5's replayable handoff events.

### Slice 5.2 — Team coordination

Spec: [8.10](spec.md#810-team-coordination).

- `AgentTeamId` plus a sharded team entity: leader, root goal, bounded
  member types/instances, capability scopes, creation/expiry policy, and
  the durable shared task board as entity state; claim/release/transfer are
  atomic compare-and-sets under revision/lease fencing with stable
  operation IDs; stale commands fail closed.
- A board claim composes with the existing assignment machinery — it drives
  `decide_assignment` on the task entity, whose assignment-generation
  fencing is already the one-normal-owner guarantee; the board never holds
  a second copy of ownership.
- Team↔task exchanges follow the acyclic choreography rule (accept() makes
  local progress only; the courier drains the journal) as new exchange
  kinds beyond the current eleven; mediated peer messages are durable
  commands over `rakka-a2a` carrying team identity in the collaboration
  extension — never direct actor references.
- Idle teams and members passivate; the board is data, not a resident
  coordinator — no single-coordinator topology.
- [Spec 17.13](spec.md#1713-structured-logs-runtime-events-and-audit) audit
  and bounded metrics for
  creation/membership/claim/transfer/message/disband.

Done when: scenario 42 passes (one normal claim owner; stale commands fail
closed).

**Amended as implemented (2026-08-08).** Landed as specified, with the
in-slice decisions resolved as follows (scope decisions user-approved):

- **Claim ingress is A2A/entity commands only** (user-approved): no
  model-visible team tool this slice — no interception-door arm, no team
  effect kind, no executor port. `AgentTeamPolicy` filled out with the
  bounded ceilings, `claim_lease_ms`, `expires_after_ms`, and a
  shaped-but-dormant `tool: Option<AgentToolId>` hook so the door lands
  later without re-plumbing; every field serde-defaulted so the 5.1
  revision-only shell still decodes. **Mediated peer messages are a
  durable board ring** (user-approved): bounded, recipient-read through
  the team query surface, drop-oldest with a durable `messages_dropped`
  counter, no push delivery. **Membership is mutable** (user-approved):
  join/leave fence on the lifecycle revision with provenance; the leader
  is immovable, a member holding an unresolved claim cannot leave.
- **The claim choreography is two new exchange kinds** (`ALL` is 14):
  `TeamClaim` (team → task) owed in the same compare-and-set as the board
  mutation, whose pure task-side arbitration records the bounded
  `AgentTask::team_claim` provenance (echo-before-every-guard, the
  `record_handoff` precedent) against a new durable claim-epoch fence
  (`team_claim_fence`) that closes courier reordering — the reply means
  "claim recorded", never "assignment made"; and `TeamClaimResult`
  (task → team), the `HandoffResult` mirror (pure derivation, settle-pass
  re-drive, `result_settled` once-guard) settling the board entry Active
  with an observational generation/run echo or reopening it under the
  refusal code. Board tasks are ordinary creations with the new
  `AgentTaskCreation::team` provenance and a deferred assignee (the
  `MissingAssignee` guard relaxes only under a team); the claim sets the
  assignee and the existing `decide_assignment` mints the one generation —
  the assignment fence stays the one-owner guarantee, proven by a direct
  fence test (`team-claim-already-owned` over an accepted assignment).
- **Single-attempt posture, the handoff precedent**: readiness refusal,
  `team-claim-assignments-exhausted` (the claim resolves rather than the
  task terminating — the board's members decide what an unassignable
  entry means, bounded by the definition's new `max_team_claims`,
  default 4), `team-claim-budget-unaffordable`, a run refusal, and a
  pre-mint cancellation all resolve through one helper
  (`resolve_team_claim_refusal`) that clears the assignee back to the
  board-pending posture. A superseding claim (transfer, expired-lease
  steal) refuses over an in-flight offer
  (`team-claim-assignment-inflight`) so exactly one generation is ever in
  flight; the lease bounds the claim-pending window only, and an
  activated claim is never stealable. Release is holder-or-leader,
  pre-acceptance only, epoch-qualified in its operation id so a retried
  release is a new durable operation; post-acceptance transfer defers to
  the handoff machinery. Terminal/foreign tasks close entries lazily
  through claim refusals (`Done` + code) — the task→team terminal
  notification exchange stays owed to 5.5b (re-parked from 5.5 on
  2026-08-13).
- **Wire**: the team cluster is the third shape under
  `io.rakka.collaboration`, discriminated by its `team` field checked
  before `handoff` (`deny_unknown_fields` fails a two-discriminator
  payload whole); verbs are claim/release/transfer/post-task/message/
  join/leave — create and disband are entity-command-only trusted
  wiring. A team send must not name `message.task_id`
  (`team-send-names-task`); ids derive under `TeamClaim` plus the new
  `TeamMessage`/`TeamOperation` operation kinds; membership changes
  require an authenticated principal. The command authorizes under its
  own `A2AOperation::TeamCommand` with the typed `A2ATeamClaim` bound in,
  answers with an immediate message (never a task), returns stale fences
  as structured refusals, and the projection echoes the governing claim
  beside the delegation/handoff echoes via the metadata-synced path. The
  service gained team store generics; no persistence/postgres work
  (stores are state-generic), though the `AgentTeamHistoryStore`
  PostgreSQL backend is owed.
- **Audit + metrics**: a parallel `AgentTeamHistoryStore` (idempotent
  slot-keyed appends, identities and codes only), three new task-history
  kinds (`team-claim-recorded/accepted/refused`), and
  `rakka.agent.team.operations` {operation, outcome} counted once per
  durable decision at the entity boundary ("operation" joined the closed
  metric-key set) — asserted in tests, closing the unasserted-counter gap
  5.1 left. Otel team span rows owed, the 5.1 precedent.
- Proof roster: `tests/team_board.rs` (7), `tests/team_claim_assignment.rs`
  (11, scenario 42's core incl. the two-shard stale-owner race and the
  three-layer replay convergence), `tests/team_claim_recovery.rs` (3,
  self-covering crash sweeps over every team- and task-store write of the
  claim round trip), `tests/team_passivation.rs` (2, real sharded
  entities: zero-resident board with the claim activating across
  passivation), the team-operations metric assertion, and rakka-a2a's
  `tests/team_surface.rs` (4, scenario 42's wire half + fail-closed
  matrix + per-operation authorization). Owed onward: 5.5's replayable
  team events (landed 2026-08-13), the task→team terminal notification
  (landed 2026-08-14 in 5.5b; the handoff-refresh of the board owner echo
  was user-scoped out of 5.5b and stays owed), board rewake
  parking, goal/collaboration-view team dimensions, the team otel rows,
  the Postgres team-history backend, the model-visible team tool door
  (+ its reserved `AgentDecisionKind` label), and A2A-carried
  create/disband if ever needed.

### Slice 5.3 — Moderation

Spec: [8.11](spec.md#811-moderation).

- `AgentConversationId` plus a conversation entity: moderator, authorized
  participant set, mode, durable turn/round state, transcript artifact or
  bounded messages, completion rule.
- Turn ownership reuses the sender-fence and settled-cell idioms (the M3
  epoch precedent): only the current participant may submit; duplicate or
  out-of-order turns are rejected via journal deduplication keyed on
  (conversation, round, turn, participant).
- Round/iteration/time/token budgets ride the conserved-dimension escrow
  model — no parallel budget machinery.
- The moderator may end early under policy; its proposed result still
  passes typed task-result validation and the 4.2 evaluation door.
- Participants and moderator passivate between turns.

Done when: scenario 43 passes (turn recovery without duplication across
passivation/shard movement).

**Amended as implemented (2026-08-10).** Landed as specified, with the
in-slice decisions resolved as follows (scope decisions user-approved):

- **Turn ingress is A2A/entity commands only** (user-approved, the 5.2
  posture): no model-visible moderation tool this slice — no
  interception-door arm, no conversation effect kind, no executor port.
  `AgentModerationPolicy` filled out with the clamped serde-defaulted
  ceilings (`max_rounds` 4/16, `max_turns_per_round` 8/16, `max_messages`
  8/16, `max_message_bytes` 1024, `moderator_may_end_early`) and the
  shaped-but-dormant `tool: Option<AgentToolId>` hook, so the 5.1
  revision-only shell still decodes. **Budgets reuse the existing
  vocabulary with no new conserved dimension** (user-approved):
  the creation carries a token grant and a wall-clock horizon whose
  deadline fixes at creation (the run-budget idiom), consumption is
  `AgentBudgetConsumption` recorded even when a turn overshoots (the
  spend already happened in the speaker's run), and exhaustion refuses
  the next turn under the dimension's own code — never parking, never a
  task escrow child. **The task binding is required and observational**
  (user-approved): creation names the governing `AgentTaskId`, the
  moderator is that task's assignee, and the early-end *result* rides
  the existing run-side evaluation and result-proposal doors unchanged —
  zero new door machinery, the assignee sender fences already enforce
  it. **The transcript is the bounded in-state ring plus an
  identity-only artifact reference** (user-approved): drop-oldest with
  visible `messages_dropped`, never a deduplication surface, the
  reference recovered verbatim per scenario 43.
- **Zero new exchange kinds and no task-side machinery**: unlike the
  team claim, a conversation drives nothing on the task, so
  `AgentExchangeKind::ALL` stays at fourteen and `task.rs` is untouched;
  the conversation (`conversation.rs`, `RakkaAgentConversation`, the
  fifth sharded entity under `AgentEntityClass::Conversation`) embeds
  the exchange host with a refuse-all participant and an empty journal
  so the terminal-notification exchange is a code change, not a schema
  migration. That exchange, the task-side conversation provenance, and the
  projection echo were re-parked from 5.5 to 5.5b on 2026-08-13.
- **Turn identity carries the body digest**: the operation id derives
  over (tenant, conversation, round, turn, participant, body digest)
  under the reserved `ConversationTurn` kind — a durable redelivery
  re-derives the same operation and converges; a *regenerated*
  same-coordinate submission derives a new one that the dense turn
  ledger refuses loudly (`conversation-turn-content-mismatch`) instead
  of silently absorbing. The ledger is the durable echo past the bounded
  operation-log window, checked before every guard including the
  terminal one (the `record_handoff` idiom); a creation-time worst-case
  arithmetic (`conversation-policy-too-large`) keeps it affordable so a
  mid-round state-bounds refusal cannot wedge the protocol. The new
  `AgentOperationKind::ConversationOperation` covers create/end/expiry,
  the end round-qualified (the release-epoch hazard); ingress derives
  turn operations from these coordinates, never the wire discriminator.
- **Two modes, owner derived not stored**: round-robin (owner =
  `participants[turn]`) and moderator-directed (moderator owns even
  turns; each carries `Designate` or `CloseRound`; `designated` is the
  one stored owner fact) — the derived-owner and stored-owner recovery
  shapes scenario 43 must prove. `AllRounds` completion ends the
  conversation in the compare-and-set that finishes the final round
  (completion beats exhaustion); `ModeratorDecides` parks the cursor at
  the ceiling with the status active. One deliberate deviation from the
  planned ladder: a passed deadline refuses `conversation-expired`
  before *and* after the durable flip (the team `require_active` rule —
  the refusal code must not depend on whether the sweep ran) rather
  than a separate pre-flip `wall-clock` code.
- **Wire**: the conversation cluster is the fourth shape under
  `io.rakka.collaboration`, discriminated by its `conversation` field
  checked before `team` and `handoff`; verbs are submit-turn/end —
  create is entity-command-only trusted wiring. A conversation send must
  not name `message.task_id` (`conversation-send-names-task`); the end
  requires an authenticated principal. The command authorizes under its
  own `A2AOperation::ConversationCommand` with the typed
  `A2AConversationClaim` bound in, answers with an immediate message,
  returns domain refusals as structured `Rejected` payloads, and the
  service gained conversation store generics (eight, spelled once via
  the new `SharedRakkaAgentA2AService` alias); the
  `AgentConversationHistoryStore` PostgreSQL backend is owed, the team
  precedent.
- **Audit + metrics**: a parallel `AgentConversationHistoryStore`
  (idempotent slot-keyed appends; identities, coordinates, and codes
  only), five conversation history kinds
  (`conversation-created/turn-recorded/round-advanced/ended/expired`),
  and `rakka.agent.moderation.turns` {operation, outcome} counted once
  per durable decision at the entity boundary — duplicates and ledger
  echoes count nothing; both keys were already in the closed metric
  vocabulary, so the guidance table's `mode` label is owed with the
  model-visible tool, as are the otel moderation span rows.
- Proof roster: `tests/conversation_protocol.rs` (9, lifecycle +
  budgets + expiry + the golden-vector operation-id pin),
  `tests/conversation_turns.rs` (7, ownership/ordering/dedup incl. the
  seventy-turn past-window ledger echo converging after the end),
  `tests/conversation_recovery.rs` (2, self-covering crash sweeps over
  every conversation-store write of a full round), and
  `tests/conversation_passivation.rs` (3, real sharded entities:
  scenario 43's five nouns recovered at zero residency without
  duplicating a turn, the stored designation surviving, idle
  auto-passivation), the moderation-turns metric assertion, and
  rakka-a2a's `tests/conversation_surface.rs` (4, scenario 43's wire
  half + fail-closed matrix + per-operation authorization). Owed
  onward: 5.5's replayable turn events (landed 2026-08-13), the
  conversation-terminal → task notification with the task-side
  conversation provenance and projection echo (landed 2026-08-14 in
  5.5b), goal/collaboration-view conversation dimensions, the moderation
  otel rows, the Postgres conversation-history backend, and the
  model-visible moderation tool door (+ its reserved
  `AgentDecisionKind` label and `mode` metric label).

### Slice 5.4 — Human-owned tasks

Spec: [8.12](spec.md#812-human-owned-tasks),
[14.3](spec.md#143-taskrun-state-mapping) (`WaitingForInput` row).

- Ownership policy: a task deliberately unassigned per its definition's
  human/service ownership policy; the `WaitingForInput` status and its
  `INPUT_REQUIRED` projection row are already proven (slice 1.12).
- Unlock the slice 1.12 deferral: `message/send` naming an existing
  `task_id` currently refuses with a stable reason — replace the refusal
  with authenticated, deduplicated typed-result delivery through the same
  validation path, on the `RecordWorkflowResult` idiom (operation id pure
  over identity, non-committing refusals, first-writer-wins).
- Build the dependents registry (deferred from 4.6): completion unblocks
  dependents; failure propagates the declared dependency policy; any
  ingress firing over a live run takes `request_task_cancellation`, never
  `terminate`.
- Keep the boundary with effect-bound checkpoints explicit
  ([spec 8.12](spec.md#812-human-owned-tasks)): exact-effect approval stays
  `AgentCheckpoint`-bound; a human task never substitutes.

**Amended as implemented (2026-08-11):**

- **The human result is an entity command on the `RecordWorkflowResult`
  idiom, sharing the run path's validation cores.**
  `AgentTaskEntityCommand::SubmitHumanResult` carries
  `AgentHumanResultSubmission` (principal, claimed definition/version/schema,
  bounded content, causation); `human_result_operation_id` is pure over
  `(tenant, task, discriminator)` under the new
  `AgentOperationKind::ResultSubmission`, so a retry converges on the
  original decision — a recorded rejection included — and a corrected
  resubmission is a new discriminator. `validate_proposal` became the
  origin-neutral `validate_result(&AgentResultClaim)`;
  `accept_result`/`reject_result` split into cores parameterized by origin,
  the exchange wrappers byte-identical. The ladder answers durable echoes
  before every guard, terminal included: the accepted result's
  `proposal_id`, the materialized `last_rejection`, and a bounded
  fingerprint ring (`AGENT_TASK_REJECTED_SUBMISSION_ECHO_CAPACITY` = 32,
  FNV fingerprints — full operation ids would not fit the 32 KiB record)
  refusing older rejections `submission-already-rejected` so a replay never
  re-spends the budget. Refusals are non-committing
  (`AgentTaskError::SubmissionRefused`); exhaustion terminalizes by
  `terminate` — no live run exists. `AgentAcceptedResult.run` became
  `Option<AgentRunId>` + additive `principal` (user-approved);
  `AgentTaskHistoryEntry` gained additive `principal`; the submission
  decision rides `AgentTaskOutcome.submission` (bounded summary) via the
  new `AgentTaskOutcomeExtras` closure plumbing, so duplicates echo it.
- **The dependents registry is two exchange kinds over the existing
  choreography.** `DependencyRegistration` (dependent→upstream, owed in the
  same CAS as the forward edge from Create/DeclareDependency/the creation
  exchange, plus the `settle_dependency_registrations` courier half that
  also self-heals pre-registry edges); the upstream records
  `AgentTask.dependents` (`AgentTaskDependentRecord`, cap
  `AGENT_TASK_MAX_DEPENDENTS` = 32, `task-dependents-exhausted` beyond it —
  the refused dependent stays Blocked on the relay path). An
  already-terminal upstream answers the receipt with its outcome and
  records nothing; the dependent's settle arm applies it through the
  existing `record_dependency_outcome` core. `DependencyOutcome`
  (upstream→each unsettled dependent) is folded into `owed_child_reports`
  and fires **immediately at terminal commit with no escrow gate**
  (user-approved: the payload is absorbing at `terminate`, unlike a
  delegation report's consumption fields); the goal-budget and stagnation
  terminals are backstopped by `settle_dependent_notifications`. The
  `ResultProposal` exchange arm converted to the owing form so a run-path
  acceptance/exhaustion owes dependent outcomes in its own CAS. Fencing:
  initiator matched against the claimed dependent / the forward edge
  (`dependency-registration-forged`, `dependency-outcome-forged`);
  same-outcome idempotent, conflict fails closed; settled markers
  (`registration_settled`, `outcome_settled`) quiesce the derivations past
  the journal window. **`task-not-created` at registration is a
  non-settling refusal** (user-approved): a racing create converges on
  re-drive; a never-created upstream leaves the dependent durably Blocked —
  the stuck-dependency struggle signal. Enabling that posture,
  `drive_pending_exchanges` records an `UnsettleableRefusal` as a failed
  attempt on the outstanding exchange instead of erroring the pass (the
  task_entity assignments-exhausted test re-pinned to the new shape).
- **The wire half replaced the slice 1.12 refusal in place.** A plain
  `ContinueTask` send is the submission: `io.rakka.agent.result` carries
  the declared contract as one `deny_unknown_fields` object, the principal
  is required, and authorization runs under the new
  `A2AOperation::SubmitTaskResult` with `A2ATaskResultClaim` bound in
  (`authorize_claimed` gained the second claim parameter). A committed
  rejection answers **`Ok(Task)`** (user-approved) with the rule code on
  the projection's new rejection echo (`io.rakka.agent.rejections`,
  `io.rakka.agent.last-rejection`, assembled in
  `agent_metadata_from_snapshot` per the 5.1 metadata-half rule);
  non-committing entity refusals map to `Refused` decisions at the
  submission branch (the handoff-path `Err(Task(...))` wart not repeated).
  New normalize guards: `delegation-send-names-task` (closing the
  accidental fall-through), `result-submission-requires-task`,
  `result-binding-conflicts-with-collaboration`; the operation-kind
  fallback split by intent so a submission id can never alias a creation
  id. The typed client gained `submit_task_result`
  (`AgentClientTaskResultRequest`, required trait method). Metrics:
  `rakka.agent.human.results` {outcome} and
  `rakka.agent.dependency.outcomes` {outcome}, durable-diff counted.
- Proof roster: `tests/human_owned_tasks.rs` (9: scenario 41 both halves —
  completion unblocking a real dependent, exhaustion cancelling a live
  dependent through the request path — op-id golden vectors, the refusal
  ladder, past-window accepted/rejected replays, the owner-loss sweep,
  metric assertions), `tests/dependency_registry.rs` (6: terminal-upstream
  receipt, continue-with-evidence, the fencing matrix, the ceiling,
  relay/exchange convergence, pre-registry self-heal),
  tests/choreography.rs's ALL-driven failure windows over the two new
  kinds, and rakka-a2a's `tests/human_task_surface.rs` (8: the four-part
  surface shape + rejection echo, exhaustion, crash sweep, typed client).
  Owed onward: 5.5 replays the dependency events; evidence artifacts on
  the wire submission stay behind the deferred artifact strategy; the
  agents-surface metric consumer for `A2AOperation::as_label` remains
  absent.

Done when: scenario 41 passes.
**Done (2026-08-11):** scenario 41 passes end to end over the real
entities (`tests/human_owned_tasks.rs`) and over the wire
(`crates/rakka-a2a/tests/human_task_surface.rs`), with the owner-loss
sweeps covering every task-store write of both flows.

### Slice 5.5 — Replayable coordination events

Spec: [17.13](spec.md#1713-structured-logs-runtime-events-and-audit),
[14.5](spec.md#145-typed-agent-client).
Guidance: [Client, Events, and Testkit](technical-guidance.md#client-events-and-testkit).

- Extend the slice 1.12 replay (per-task event log, task-scoped cursor,
  expired-window resync in `rakka-a2a`) to coordination events —
  assignment, handoff, claim, turn — emitted only after the durable
  transition and deduplication-safe.
- Generalize to scoped cursors: team and conversation scopes do not fit the
  `<task-id>:<sequence>` shape; bounded retention and explicit resync per
  scope. Resolves open decision 21.
- Derived struggle signals (stalled claims, moderation exhaustion) stay
  observability projections.
- Typed-client subscription surface, absorbing the 4.7 recorded follow-up:
  the goal-view wire surface (an `A2AOperation::GoalViewRead`-shaped
  operation plus typed-client query over the untouched principal-free
  assembly core and `authorized_agent_goal_view` wrapper).

Done when: scenario 45 passes.
**Done (2026-08-13):** scenario 45 passes over the real logs
(`tests/coordination_replay.rs`) and over the real service core
(`crates/rakka-a2a/tests/coordination_surface.rs`), both halves: a cursor
resuming across every scope class that keeps a log, and an exhausted window
answering a floor the reader resumes from. Open decision 21 is resolved.

**Amended as implemented (2026-08-13):**

- **The coordination event log already existed; what was missing was the way
  out.** Every coordination transition already writes an ordered, deduplicated
  record — the task, team, and conversation history logs, and the run's
  decision-event sink — each written *after* the compare-and-set that decided
  it, on a sequence that transition consumed. So this slice adds **no second
  write path, no new durable record, and no new outbox**: 17.13's "emitted only
  after the durable transition" and "duplicate processing creates one logical
  event" were already satisfied on the write side, and durable task/run state
  stays the correctness source. `events.rs` is a *read* contract over the four
  logs. Open decision 21 resolves accordingly: yes, replayable, with bounded
  retention, a monotonic scoped cursor, and explicit resync.
- **The scoped cursor is the entity address.**
  `AgentCoordinationCursor` encodes `AgentEntityAddress::key()` + `:` +
  sequence, so `task/acme/order-1:7` and `team/acme/support:3` are both legal
  and the team and conversation scopes that could never fit
  `<task-id>:<sequence>` now have one. Identity segments are validated free of
  `/` but may contain `:`, so the sequence is taken from the *last* separator —
  the suffix the encoder always appends. The substrate's public task cursor is
  a documented compatibility commitment and is untouched; the two shapes
  cross-reject (a bare `<task-id>:<seq>` has no class segment and fails closed).
  **The fence is scope equality, not just tenant**: a bare address whose last
  segment ends in digits is a syntactically valid cursor for a *different*
  entity in the same tenant, so a cursor naming any scope other than the one
  addressed is refused (`coordination-cursor-scope-mismatch`), the substrate
  projection's own rule.
- **Two answers, never a short page.** `AgentCoordinationReplay` is
  `Page { events, next_cursor, complete_through, has_more, unrecoverable_losses }`
  or `WindowExpired { oldest_retained, resume_from }`, and the expired arm names
  a cursor the reader can actually resume from. `complete_through` closes the
  matching ambiguity at the head: entries reach their log on the settle pass
  *after* the transition, so an empty tail can mean "you are current" or "the
  entity still owes its sink" — the team and conversation snapshots gained
  `owed_history` beside their existing `history_entries` so a reader can tell.
  The merged `AgentCoordinationEventKind` label is `<scope-class>/<source-label>`
  because the source vocabularies are **not** disjoint: a task records
  `team-claim-recorded` when it takes a board claim and a team records
  `team-claim-recorded` when it makes one, and a label that merged them would
  merge two different events at two sequences in two logs.
- **Retention is an opt-in read window, and the contract lives on the trait.**
  The three in-memory history stores gained `with_retention(n)` — *off* by
  default, because these logs are also the audit record 17.13 requires and the
  entity refuses a transition rather than lose an entry (`require_history_headroom`),
  so enabling eviction forfeits that audit obligation for whatever the window
  drops, and the builder says so. A read below the floor answers the new
  `…HistoryWindowExpired { oldest_retained }` on each entity's error enum, and
  the page is walked for contiguity so a hole the reader would *cross* is
  refused too, not only one at the head. `testkit`'s
  `assert_{task,team,conversation}_history_store_contract` is the conformance
  harness, so the owed PostgreSQL backends inherit the contract instead of
  reimplementing it — which is exactly how the substrate's two projection
  backends came to duplicate the same check twice.
- **Six defects had to be fixed for the contract to be real rather than
  nominal**, all found by design review and verified in source before any
  code changed. (1) `record_decision` dropped the lowest-sequence *unflushed*
  event after it had already consumed a sequence, and the sink checked only the
  head — so a reader paged silently across the hole; the sink now walks the page
  it is about to hand over and refuses at the discontinuity, the rule the
  substrate's public event log already kept. (2) The assemble-failure branch
  counted a drop *without* consuming the sequence, making that loss undetectable
  by any reader; it now consumes it, so the loss is a hole rather than a
  silence. (3) `is_domain_refusal` on the team and conversation errors is
  exclusionary, so a new variant becomes a "refusal" by default — the
  window-expired read answer would have been returned to a caller as a rejected
  *command* and counted against the entity's refusal metric; both now exclude it
  and say why. (4) `RakkaAgentA2AError::code()` flattened every `Projection(_)`
  to `"projection"`, so the existing `replay-window-expired` code never reached
  the wire despite `TaskProjectionError::code` documenting those codes as a
  compatibility commitment; it forwards now. (5) `A2AAuthorizationRequest` had
  no constructor, so each new typed claim broke every literal site — it gained
  `new(operation)` plus `with_*` setters and became `#[non_exhaustive]`, and the
  five existing sites moved over. (6) The run scope is served by an
  `Option<Arc<dyn AgentDecisionEventSink>>` builder rather than a ninth store
  generic, and the three history stores are read from the fields the service
  already holds — adding a store accessor would have broken
  `tests/service_shape.rs`, which pins the wired-store construction sites.
- **Deliberate deviations from the approved plan, both narrowing:**
  `AgentTaskError` did *not* gain an `is_domain_refusal` — it has no
  default-open classifier for a new variant to fall into, so the method would
  have been unused public API rather than a fix. And
  `RakkaAgentA2AError::code()` kept its `&'static str` signature: rather than
  relax it, `AgentCoordinationReplayError` carries the *typed* source errors
  (`Task`/`Team`/`Conversation`/`RunEvents`) instead of a stringly code, which
  forwards their static codes and lets a caller match the underlying failure.
  The sink's own backend code stays in the message under one stable
  `coordination-run-events-failed`, which loses nothing semantically distinct
  because the expired window is an answer rather than an error.
- **Wire = two read operations as direct service methods** (the
  `replay_task_events` precedent; the agents surface has no route binding at
  all, deferred since 1.12, so the service core *is* the surface).
  `A2AOperation::CoordinationEventRead` and `::GoalViewRead`, each with its own
  typed claim (`A2ACoordinationClaim`, `A2AGoalViewClaim`) bound in before
  anything is read. Neither borrows `normalize_agent_cancel`'s task-shaped
  normalization — these reads name no task — so `resolve_agent_tenant` is the
  new tenant-only helper. `authorized_agent_goal_view_bounded` is the clamped
  entry point the wire needs: the unbounded wrapper fans out to
  `AGENT_GOAL_VIEW_MAX_TASKS` whatever the caller asked. **A `GoalViewRead`
  denial answers `Ok(None)`, not `Unauthorized`** — byte-identical to an absent
  goal, to an unauthenticated caller, and to a goal that never existed, because
  a distinguishable wire error would reopen exactly the existence oracle the
  owner fence closed.
- **Typed client**: `coordination_events(scope, cursor, limit)` and
  `goal_view(goal, max_tasks)` as required `AgentClientTransport` methods
  (breaking for external implementors, the 5.4 precedent), returning
  `rakka-agent` domain types directly. The expired window travels as the
  reply's own arm rather than an error, because a caller that must resynchronize
  still needs to know *where* to resume. The reconnect cursor is the
  subscription contract: there is no watcher for domain history and live push
  stays out of scope.
- **Six derived struggle signals, read-time only** (`AgentStruggleSignal`,
  `AgentStruggleSignalKind`, `AgentStrugglePolicy` in `query.rs`): approaching
  budgets, repeated iteration failure, repeated result rejection, stuck
  dependencies, stalled team claims, moderation exhaustion. Pure over the
  authoritative snapshots, deriving twice gives the same answer, thresholds are
  deployment policy and never durable, and nothing they observe can mutate
  correctness state. The stuck-dependency signal is gated on
  `dependency_stall_millis` (default 15 minutes) against the task's
  `updated_at`: an unsettled registration is *normally* one settle pass wide,
  so an ungated derivation would report every freshly blocked task as stuck in
  the window between its own commit and its settle.
- **The two notification exchanges 5.2 and 5.3 parked here are re-parked
  explicitly** (user-directed): they are write-path choreography, not event
  replay, and leaving them implicitly owed to a slice that no longer covers them
  is how a debt disappears. Slice 5.5b below owns them.
- Proof roster: `tests/coordination_replay.rs` (7: the cursor resuming across
  task, team, and conversation with no gap or repeat; the idempotent page,
  including across a re-driven flush; the foreign-scope, cross-tenant, and
  substrate-shaped cursor refusals; the agent scope refused rather than answered
  empty; the store conformance harness bounded and unbounded across all three
  logs; the reported floor resuming for real; the kind-label injectivity pin),
  `tests/decision_events.rs`'s `a_dropped_decision_is_a_declared_gap_not_a_silent_one`
  (which fails against the pre-slice sink — verified by reverting the walk), and
  `rakka-a2a`'s `tests/coordination_surface.rs` (5: the scoped cursor paging over
  the real service core, the tenant and scope fences, per-operation authorization
  with the claim asserted inside the authorizer, the owner's positive read plus
  the clamped budget and the four-way deny-is-absent equality against it, and
  the two unserved scopes refused by name), plus two struggle-signal proofs in
  `tests/operational_query.rs` (the stall threshold telling a young registration
  from a stuck one, and a parked conversation reported without the projection
  changing anything it observed). Owed onward:
  the coordination-read metric consumer, the PostgreSQL history backends (now
  covered by the harness), the goal view's team and conversation dimensions, and
  live push.

### Slice 5.5b — Terminal coordination notifications

Spec: [8.10](spec.md#810-team-coordination), [8.11](spec.md#811-moderation),
[14.2](spec.md#142-task-identity-and-projection).

The two write-path debts slices 5.2 and 5.3 recorded against 5.5, re-parked
here on 2026-08-13 because 5.5 became a read contract and 5.6 is acceptance
only. Neither is implicitly owed any longer.

- **Task → team terminal notification.** A terminal or foreign task currently
  closes its board entry lazily, through a claim refusal (`Done` + code), so a
  board can hold an entry for a task that ended minutes ago. The notification
  exchange closes it eagerly.
- **Conversation → task terminal notification**, plus the task-side
  conversation provenance cell and the projection echo. Slice 5.3 pre-wired the
  conversation entity with an exchange host, a refuse-all participant, and an
  empty journal precisely so this is a code change rather than a schema
  migration.
- Both ride the established idioms: a new `AgentExchangeKind` with sender
  fencing at both ends, a settled marker as the once-guard past the journal
  window, an owed-derivation consult point, and the courier's
  `UnsettleableRefusal` posture for a receiver that cannot yet answer. Each new
  kind joins `AgentExchangeKind::ALL`, so `tests/choreography.rs`'s failure
  windows cover it by construction.

Done when: a terminal task closes its board entry without a claim attempt, and
a terminated conversation is observable from its governing task.

**Amended as implemented (2026-08-14).** Landed as specified — two new
exchange kinds (`TeamTerminalNotice`, `ConversationTerminalNotice`;
`AgentExchangeKind::ALL` is 18) riding the established idioms whole — with
the in-slice decisions resolved as follows (scope decisions user-approved):

- **Terminal-only** (user-approved): the handoff-refresh of the board's
  owner echo that slice 5.2's owed list bundled into this entry is *not*
  included — the echo is observational and a refresh is not absorbing the
  way terminality is, so it needs its own idempotence design. It is
  re-parked explicitly below, no longer implicitly owed.
- **The provenance cell is not a task transition.** Recording it leaves
  `AgentTaskState::updated_at` alone, because that field means the time of
  the last accepted transition *of this task* and is the sole clock the
  board-governed unclaimed horizon runs on. Code review found the cell
  advancing it, which let any conversation naming a never-claimed task
  postpone its expiry — and a task keeps no registry of the conversations
  it governs, so it cannot tell a legitimate one from a series minted to
  keep it alive. The record still changes and still persists; it simply
  does not claim to be a transition, and any later echo about another
  entity must follow the same rule.
- **The round coordinate is a count, and named one.** `rounds_completed`
  on both the notice and the task cell. `AgentConversation::round` is a
  next-expected cursor that advances once per closed round, so on a
  terminated conversation it counts the rounds that finished — but both
  projections documented it as "the round it ended in", which is wrong
  under `RoundsComplete`, the one flip that closes its final round before
  ending. The value never changed; the name and the docs did, and the
  public `conversation-rounds` echo already read as a count.
- **The `team-not-found` / `task-not-created` divergence is documented**
  on the classifier rather than left to look like an oversight: a
  conversation is created against an existing task, so an early notice is
  a race that waiting resolves; a team is trusted wiring created ahead of
  the tasks naming it, so a task naming a missing one is a mistake the
  unclaimed horizon already surfaces, and waiting would trade it for an
  exchange owed forever. The cost of settling — a later-created team
  holding an `Open` entry that closes the old lazy way — is named beside
  it.
- **Every growth point checks its bound, including the close.** The
  terminal close writes the reason code onto the entry, and
  `terminal_reason` is a free-form wire string capped only at
  `AGENT_TEAM_DETAIL_MAX_LENGTH` — so an entry no claim ever named grows,
  and thirty-two of them carry up to sixteen kilobytes against an
  effective cap of twenty-eight. Code review found this the one growth
  point that skipped `check_bounds`, which meant its overflow committed
  and surfaced at the next `post_task` or `add_member`. Validate-then-
  mutate is now literal here, the `record_team_claim` discipline, and the
  refusal stays outstanding because board eviction is what lets a re-drive
  converge.
- **The two `check_bounds` exits classify opposite ways** at the task's
  conversation arm. The size bound waits (the growth reserve relaxes when
  the task terminalizes); `task-dependency-limit-exceeded` is definitive,
  because the dependency map only grows and this arm never touches it, so
  an unclassified forever-refusal would re-run the receiving arm's durable
  write on every settle pass for the life of both entities.
- **The settle precheck asks the derivation, not a copy of it.** The
  conversation's owed-notice precheck restated `owed_terminal_notice`'s
  bail-outs by hand and missed the reason-less terminal record; since
  `initiate` persists even for an empty owed vector, that wrote
  byte-identical state on every pass forever. It now calls the derivation,
  so the two cannot drift, and the derivation's error surfaces before the
  revision it would otherwise have cost.
- **The eager close stands behind two payload fences.** The notice must be
  initiated by the task it names *and* report that task as ended. Code
  review found the second missing: `AgentTeamTerminalNotice.status` was
  decoded and never read, so the irreversible close rested on a fence that
  proves *who* sent the notice — and a task's own entity is the legitimate
  sender whatever the payload says. A non-terminally populated notice
  therefore cleared it trivially and evicted a working member from live
  work permanently. `team-terminal-notice-not-terminal` joins the shared
  classifier as definitive: the courier re-delivers the stored envelope
  rather than re-deriving it, so a payload failing on its own face answers
  the same way for as long as it exists.
- **`Done` is absorbing under `settle_claim_action`, by its own guard.**
  The eager close bumps the entry's claim epoch, which design review found
  load-bearing because the `(Release, "team-claim-already-owned")` settle
  arm restored an entry `Active` with none of the `claim_is_current` guard
  its five siblings carry. Code review then found the other half: the
  *lazy* close — the `team-claim-task-terminal` / `-task-unknown` /
  `-wrong-team` arm — closes at the entry's **current** epoch, so the epoch
  guard could not absorb a second reply for that same decision, and a
  `Done` entry could be rewritten `Active` around a claim no task ever
  accepted. `Done` has no way back (eviction only removes closed entries;
  claim, release, and transfer all refuse one), and the terminal notice is
  owed exactly once, so the entry would be wedged for the board's
  lifetime. Both halves are now structural: an explicit terminal guard
  after the epoch guard, and the missing currency check on the arm that
  lacked it — after which the epoch bump is defense in depth rather than
  the only defense. Pinned by two unit tests, each verified to fail with
  its own guard removed. A missing or already-`Done` entry accepts
  idempotently with no board write; no `require_active` gate (the board is
  data — an expired team's entry still closes); a re-posted entry after
  eviction closes lazily exactly as before.
- **The task→team notice rides `owed_child_reports`** (the one terminal
  consult point, counted into the exchange budget) plus a settle-pass twin
  covering the terminals that never consult it — goal exhaustion,
  stagnation, human-path rejection exhaustion — and deliberately
  back-filling pre-slice terminal tasks, whose unset marker closes their
  stale board entries once. Operation id pure over `(tenant, team, task)`;
  the payload carries status + terminal-reason code, echoed on
  `last_code`.
- **An already-terminal task still records the conversation provenance
  cell** (user-approved): the cell is observational provenance, not new
  work, and the common race is the conversation completing beside the
  task's own result acceptance — a deliberate, documented deviation from
  `apply_dependency_outcome`'s no-mutation-at-terminal posture. The
  recording keeps validate-then-mutate literal (both fields restored on a
  bounds refusal, the `record_team_claim` pin duplicated for it) and a
  pre-terminal record passes the growth reserve so the cell can never eat
  the room the task's own lifecycle still needs.
- **Latest-only cell + history chain** (user-approved):
  `AgentTask::conversation` (`AgentTaskConversation` — identity, terminal
  status/reason, round/turn coordinates, `ended_at`; never transcript
  content) + a `conversations` lifetime counter, the handoff/team-claim
  precedent; the chain is `conversation-terminal-recorded` history. The
  cell's `ended_at` is **strictly monotonic**, and the counter rides that
  guard because it counts notices recorded: code review found the
  latest-only past-window hazard was not merely a re-record but a
  *regression* — a duplicate replayed past the applied window after a
  second conversation overwrote the cell took the cell back and
  re-incremented, and `get_task` then healed the public echo to the older
  conversation on every read. What remains is the opposite direction, and
  it is documented on the type with the bounded-map alternative named: an
  older conversation whose first delivery arrives after a newer one is
  accepted without being materialized, absent from the cell it could never
  have won and from the count, still readable through its own entity and
  the replay surface.
- **The conversation owes the notice in all three terminal CAS's** — the
  `transition()` wrapper covers rounds-complete and the early end in one
  owing point, `observe_expiry` owes in its own flip — plus the
  settle-pass consult as crash backstop and pre-slice back-fill. The 5.3
  pre-wiring held: the exchange host, refuse-all participant, and journal
  needed no schema change. Delivery is pumped by whatever drives the
  conversation's settle pass — including the conversation store's own
  `apply`, which couriers best-effort after the command that flipped it,
  because the sharded entity's `Command` arm sends itself no `Settle`, a
  terminal conversation accepts no further command, and it runs no timer:
  the flipping command is the last drive the notice would otherwise get.
  Beyond it, the A2A surface after every conversation operation, the
  application sweep, and recovery — stated in the kind's doc and in the
  rewritten `rakka-a2a` service comment that previously said "no courier
  hop this slice".
- **One shared refusal classifier per kind** (`coordination.rs`), used by
  the initiator's settle rule *and* the receiver's memoization gate — the
  5.4 two-sides-agree-by-construction rule made literal. On the team side
  `team-not-found`/expired/disbanded/forged are definitive and flip the
  markers, so a notice to a team that never existed settles rather than
  re-driving forever. On the conversation side only `forged` is: both
  `task-not-created` (the dependency-registration posture) and
  `task-state-too-large` stay outstanding and unmemoized, because neither
  is the task saying *never*. The bound is the sharper case — the receiver
  charges a pre-terminal task the `AGENT_TASK_STATE_GROWTH_RESERVE_BYTES`
  headroom its own lifecycle still needs and a terminal task nothing, so
  the very cell refused today fits once the task ends; settling on it
  would quiesce both ends over a refusal the receiver is about to stop
  making.
- **Wire**: no new A2A operations — the exchanges are in-fabric. The task
  projection's `io.rakka.collaboration` echo gained the conversation
  cluster (`conversation`, `conversation-status`, `conversation-reason`,
  `conversation-rounds`, `conversation-turns`) beside the
  delegation/handoff/team echoes, healed by `sync_agent_status` on read
  and write paths. Audit: new history kinds `team-task-closed` and
  `conversation-terminal-recorded`, flowing into the 5.5 replay surface
  under scope-qualified labels; metric: `rakka.agent.team.operations`
  {`close`, `applied`} once per fresh application at the accept boundary.
  Both snapshots expose their settled markers.
- Proof roster: `tests/team_terminal_notice.rs` (8: the Active entry
  closing without a claim attempt — the done-when — the unclaimed close,
  the never-posted idempotent no-op, the closed-entry regression pin
  (every precondition of its interleaving asserted, and failing only with
  all three guards removed), marker
  quiescence, the committed-but-unsent window under an injected lost
  delivery, and self-covering crash sweeps over every team- and task-store
  write of the terminal flow), `tests/conversation_terminal_notice.rs` (9:
  all three terminal flips recording the cell, the already-terminal task
  still recording, the racing-creation notice outstanding then converging,
  the second conversation overwriting the cell with the chain in history,
  quiescence, and crash sweeps over both stores), the missing-team
  definitive settle in `tests/task_unclaimed_expiry.rs`, the
  bounds-restore pin beside `record_team_claim`'s, the `close` counter in
  `tests/agent_metrics.rs`, the label pins in `tests/coordination_replay.rs`,
  both kinds in `tests/choreography.rs`'s failure windows by construction,
  and `rakka-a2a`'s `tests/conversation_surface.rs` end-to-end echo test
  over the public surface. Owed onward (explicitly, per this slice's own
  rule): the handoff-refresh of the board owner echo; board rewake parking
  (the team-side wake-timer affordance — the eager close runs on the
  task's clock and does not need it); the goal/collaboration-view team and
  conversation dimensions; the Postgres team/conversation history
  backends; the team and moderation otel span rows; the model-visible
  team/moderation tool doors.

### Slice 5.6 — M5 acceptance

Spec: [Coordination Capability Milestone](spec.md#coordination-capability-milestone-m5).

- Deterministic model/tool scripts plus fault injection covering every
  task, handoff, claim, turn, and effect boundary; walk the coordination
  milestone checklist end to end.
- Acceptance example on the `multi-agent-goal-acceptance` pattern: pinned
  transcript, sharded entities, real in-process A2A, pod loss over the new
  coordination CAS points.
- Re-prove "per-run setup cannot widen the envelope" with coordination
  capabilities as the widened dimension.
- Scope fence: the 4.4/4.6 crash-sweep debt stays in Phase 6.1
  (user-confirmed); 5.6 sweeps only the CAS points Phase 5 introduces.

Done when: the checklist is demonstrated and all M5 scenarios pass under
fault injection.

**Amended as implemented (2026-08-17):**

- **Two of the seven checklist bullets did not hold, and the walk could not
  honestly print them until they did.** The `CoordinationCapability` envelope
  dimension had been subset-checked since 1.2, but only `Delegation` and
  `Handoff` were ever consulted at a runtime door (`delegation.rs`,
  `tools.rs`, `run.rs`); `Team` and `Moderation` were declared and never
  read, so a board claim and a moderated turn rested on their roster alone.
  Both doors now exist and both read the agent's durable definition rather
  than asking its entity — the assignment decision's own read path, for its
  own reason. The team door is one field on `AgentAssignmentReadiness`
  (`permits_team_coordination`) applied in `decide_assignment` *only* where
  `team_claim_pending` already is, so a direct assignment spends no
  coordination capability and is untouched; the refusal
  (`team-coordination-unauthorized`) routes through
  `resolve_team_claim_refusal`, keeping the single-attempt posture — the
  entry reopens for a member that may, rather than parking the task. The
  moderation door cost `AgentConversationEntityStore` a third generic (the
  agents store, symmetric with `AgentTaskEntityStore`) and no new generic on
  `RakkaAgentA2AService`, which already carried one and now forwards it. The
  dense-ledger echo still answers first: a committed turn converges on its
  recorded outcome even under a since-narrowed definition, because re-judging
  it would make recovery depend on a record the turn never consulted. An
  unreadable speaker record is separately retryable
  (`conversation-participant-record-unreadable`), never a definitive refusal.
- **The walk found one defect in shipped code.** `AgentTeamEntityStore::ensure_recovered`
  trusted its own flag rather than asking the host whether it still held the
  authoritative record — the rule the task, run, and conversation stores all
  carry, missed only here. A board has two writers by construction (the
  resident sharded entity and the A2A service's own store, which is how every
  wire claim reaches it), so the loser of one compare-and-set answered
  `exchange-not-recovered` for the rest of its residency. This example is the
  first deployment-shaped consumer to drive a board through both paths.
- **The scope fence held, and the two families it names are swept.** Slice 5.1
  had swept only the run store's committed-but-unsent fence window; the
  task-side resolution machine and the whole dependents registry had no crash
  injection. `tests/handoff_recovery.rs` sweeps both stores across the full
  transfer and asserts the *convergence property* rather than one outcome —
  a transfer has two correct endings, which one a window produces depends on
  whether the offer had committed, and the sweep asserts it reached one of
  them whole with the send attempted once either way, plus that it covered
  both arms. `tests/dependency_registry.rs` gained the matching sweep over
  the declaration, the upstream edge, and the two settled markers, and the
  `ExchangeFault` triple now covers `HandoffResult`, `DependencyRegistration`,
  and `DependencyOutcome` at the real entity rather than the synthetic probe.
- **The milestone's done-when is `examples/coordination-capability-acceptance`**:
  a 16-line transcript pinned three ways, one continuous story over all five
  sharded entity types and the real in-process A2A core — board post, atomic
  wire claim with the loser failing closed, zero-resident wait, same-task
  transfer committed in one CAS and terminalized only after durable
  acceptance with the two runs' memory namespaces proven disjoint, an owner
  death injected inside the transfer, a human-owned approval unblocking its
  dependent over the wire, a moderated conversation absorbing a replayed turn
  and surviving an owner death mid-round, and the terminal task closing its
  board entry with the claim epoch bumped. Two beats are the envelope bullet
  and both are refusals. The consequential effect stays checkpoint-parked and
  uninvoked throughout. The task history is deliberately bounded so the
  replay bullet demonstrates both arms. Deployment facts the walk enforced:
  every settle and read re-materializes from the durable record (an entity
  sharing its store with a second writer that believes it owes nothing never
  writes, never conflicts, and so never re-reads), and the sentinel sweep
  covers what content must not *cross onto* — board, replay pages, metrics —
  while a turn body in the conversation's own ring and a typed result on its
  task are the contract working.
- Proof roster: `examples/coordination-capability-acceptance` (2 tests: the
  README-to-const pin and the walk plus its typed facts),
  `tests/handoff_recovery.rs` (3), the registry sweeps and fault triple in
  `tests/dependency_registry.rs` (2 added), the live envelope refusals in
  `tests/team_claim_assignment.rs` and `tests/conversation_turns.rs`, the
  coordination-widening admission proof in `tests/autonomy_admission.rs`, and
  the missing positive control plus per-kind coverage in
  `tests/definition_setup_envelope.rs`. Owed onward: the handoff-refresh of
  the board owner echo; board rewake parking; the goal/collaboration-view
  team and conversation dimensions; the PostgreSQL team and conversation
  history backends; the team and moderation otel span rows; the model-visible
  team and moderation tool doors; and `DeterministicModelAdapter`
  conditioning on prior messages and tool results rather than turn number
  alone. The 4.4/4.6 crash-sweep debt stays in Phase 6.1.

---

## Phase 6 — Production Fault, Security, and Telemetry Validation

No new milestone; this hardens whatever phases have shipped
(guidance [Slice 7](technical-guidance.md#slice-7-production-fault-and-security-validation)).

### Slice 6.1 — Multi-pod fault and soak validation

Spec: [15](spec.md#15-passivation-recovery-and-shard-movement),
[18](spec.md#18-required-recovery-scenarios) (fault-injection note).

- Multi-process dispatcher and shard-movement fault injection (extend the
  `RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY` harness); kill owners and
  dispatchers at every durable boundary including after external commit.
- Settings changes and credential revocation injected during waits.

**Amended as implemented (2026-08-24):**

- **The gap was wider than "add more crashes".** Five public functions —
  `init_agent_{entity,task,run,team,conversation}_entity_remote_sharding` — were
  defined, re-exported, and called nowhere; the production `ShardedExchangeRoute`
  was exercised only by the synthetic `ChoreographyProbe`; and every agent proof
  ran in one process over one in-memory store, which can demonstrate
  re-materialization but not spec 15's actual requirement that durable state
  suffice *on a different pod, without node-local memory*. The existing
  `RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY` gate did no fault injection at all: it
  shelled `examples/multi-node-sharding`, sent one hardcoded `CartCommand`, and
  asserted a stdout marker.
- **`examples/multi-pod-agent-fault-soak` is the answer, and it is the first
  consumer of all five registrations and of the production route.** Two real OS
  processes, one shared directory whose commits are `hard_link` claims (so two
  pods racing one compare-and-set cannot both win), all five entity classes
  registered remotely, the task and the run landing on different pods so their
  exchanges cross TCP, and a model adapter that commits to a shared ledger
  *before* it answers — spec 18's "after a test external system commits but
  before it returns the receipt", made observable across the death of the pod
  that committed it. The crash-free reference reports each pod's durable write
  count and the sweep replays the world once per `(pod, store, ordinal, window)`,
  arming that pod to `abort()` there. 32 pod-loss windows converge on
  `Completed` from the shared record with exactly one logical external turn.
  Departure is *announced* and the survivor calls `mark_down` — not
  `mark_leaving`, because a killed pod never got to leave — after which the
  shards move and the entities re-materialize.
- **The parked 4.4 and 4.6 crash-sweep debt is closed.** `fan_in_recovery.rs`
  sweeps every run- and task-store write of the fan-out → child-result → fan-in
  resolution (46 windows) plus the `DelegationResult` fault triple at the real
  entity; `cancellation_recovery.rs` sweeps every write of the propagation spine
  (38 windows), asserting every child terminal under the requested reason, every
  chase accepted, and no propagation leg replaying a send. `delegation_limits.rs`
  gained scenario 34's recovery clause as a fact rather than an inference: its
  module doc argued that re-materialization is already a coordinator loss, which
  is true of the *read* path but says nothing about a loss inside the
  compare-and-set that spends the ceiling. It is swept now, and the quota is
  charged once.
- **Revocation during a wait had no coverage at all.** Every scenario-13 proof
  applied the change *before* the first dispatch pass, so the run was never
  waiting when the operator acted. `wait_invalidation.rs` parks the run first:
  all three `ImmediateSafety` change kinds, both orderings against the human
  decision, an agent suspended mid-wait (which must *defer* without spending
  budget, not fail closed), a grant that binds another credential than the
  intent carries, a guardrail chain upgraded on the fleet while the intent stays
  pinned, a definition narrowed while a task is blocked on a dependency, and the
  whole revocation flow swept under owner loss.
- **Two dispositions worth recording, because the slice plan predicted
  otherwise.** `effective_settings_for_turn` is documented as the
  pinned-versus-current resolution point and has no production call site — but
  neither do the two `RunPinned` fields it resolves (`loop_state_schema_version`,
  `memory_schema_version`), which are stored and read nowhere. Wiring it today
  would change nothing observable, so the honest outcome is a behavioural test
  that a run-pinned change is inert for a run in flight, and this note.
  Separately, `AgentRunStatus::WaitingForTimer` is declared, labelled, and
  counted by `is_waiting()` but never assigned: durable timers are owned by the
  goal/wake layer, and it stays reserved rather than acquiring an invented park.
- **Soak is a property test, not a timing one.** `agent_soak.rs` drives many
  tasks through one agent and asserts what must *not* grow: the agent's durable
  record (identical at 24 and 500 tasks), the metric series set (6 series while
  observations grow from 192 to 4000), each task's materialized bound, and the
  exchange journals settling empty. `RAKKA_AGENT_SOAK_ITERATIONS` scales it.
- **Shared fixtures rather than a fourth copy.** `SkillNamedExecutor` and the
  fan-out helpers were already duplicated across `fan_out_fan_in.rs` and
  `cancellation_propagation.rs`; they, `create_real_child`, and the whole
  `AuthorityFixture` (the only fixture that drives the *real* dispatch pipeline,
  which is the only place an authority gate is consulted) moved to
  `tests/common/mod.rs`. The fixture also gained a credential resolver, without
  which an intent carrying a binding is refused `credential-resolver-missing`
  before any later gate can be observed.
- Proof roster: `examples/multi-pod-agent-fault-soak` (1 gated test plus the
  driver), `tests/fan_in_recovery.rs` (3), `tests/cancellation_recovery.rs` (2),
  `tests/wait_invalidation.rs` (12), `tests/agent_soak.rs` (1), the scenario-34
  sweep in `tests/delegation_limits.rs`, and the second gated entry in
  `crates/rakka-testkit/tests/compatibility_matrix.rs`. Documentation:
  `docs/rakka-agent-fault-injection-matrix.md`. Owed onward: a file-backed task
  history passing `assert_task_history_store_contract`, a coordination workload
  across pods (team and conversation are registered but unexercised), a
  PostgreSQL arm for the shared substrate, and detected rather than announced
  departure.

### Slice 6.2 — Security validation

Spec: [16](spec.md#16-security-and-authorization),
[13.1](spec.md#131-general-requirements).

- Memory ACL and poisoning defenses, retention/deletion/tombstone flows,
  cross-tenant existence-leak tests, executor trust-class routing, secret
  exclusion sweeps over state/events/telemetry.

**Amended as implemented (2026-08-25):**

- **Four of the five items were fail-opens, not missing tests.** Like 6.1, the
  gap was wider than the line item suggested: in each case the code was wrong
  and no test would have noticed. Each fix is verified by *falsification* —
  reverted, the suite confirmed failing, restored — which is the standing
  lesson from the 6.1 review that a fix proven only by a green suite is not
  proven.
- **The retriever was trusted for content and for scope, and the trait doc
  said that was unavoidable.** `assemble_context` re-checked every property a
  retrieved record carried except the one deciding whose memory it is, and
  `AgentPrivateMemoryRetriever`'s doc called scope "the one contract clause no
  downstream layer can catch a violation of" — because an `AgentPrivateMemory`
  carries no tenant or agent. That reasoning was wrong: the assembly holds the
  authoritative `AgentScope`, the store is scope-addressed, and the crate's own
  boundary already says the index is a rebuildable projection while the store
  holds the authoritative record. **The retriever now supplies a ranking and
  the store supplies the record.** Resolving each ranked identity through
  `AgentPrivateMemoryStore::get` closes four things at once, three of which
  nobody had named: cross-scope leakage, content forgery, metadata forgery (a
  fabricated classification or confidence no longer passes `admits`), and index
  drift — including a memory tombstoned *after* it was indexed, which the old
  path embedded because it checked the retriever's pre-tombstone copy. A
  ranked identity the store does not hold is dropped and counted on
  `RetrievalReport::unverified`; the resulting snapshot is byte-identical to
  one assembled from an empty ranking, so the drop is not an existence oracle.
  `AgentMemoryRetrieval::new` takes the store as a required argument, on the
  same argument that already made the chain required.
- **The two-chain wiring was documented and unverifiable.**
  `AGENT_EVALUATED_GUARDRAIL_BOUNDARIES` unconditionally claimed memory-ingress
  was evaluated, so `validate_covers` admitted a mandatory ingress-only stage
  at an authority that had never seen a bundle. Now the deployment attests
  (`AgentToolAuthority::with_memory_ingress`) and the attestation is *checked*
  against `AgentGuardrailChain::declaration_digest`, which compares stage
  declarations rather than revisions — the shape a deployment actually lands
  in is an empty bundle chain carrying the *same* revision number, which a
  revision comparison waves through. Unattested, the authority counts only
  `AGENT_AUTHORITY_EVALUATED_GUARDRAIL_BOUNDARIES` and fails closed on the
  existing `guardrail-stage-unevaluated`. Two alternatives were rejected:
  `Arc::ptr_eq` (unforgeable, but refuses a deployment that legitimately
  rebuilds an identical chain from one config) and a factory minting both
  consumers (convention, not enforcement — the plain constructors would
  remain, and omission would not fail closed).
- **A credential resolver's failure text was reaching durable state, and the
  substrate already knew better.** The leak was not in run state — `run.rs`
  persists `bounded_detail(code)` — but in the workflow outbox row's
  `last_error` and the fleet index's `last_error_code`, both unbounded and
  fleet-readable, neither enumerated by `AgentRecordKind`. Two facts that
  reinforce each other, and the reason the sweep scans the substrate beside the
  catalogue. `AgentCredentialError::to_outbox_dispatch_result` already emitted
  its code alone, so this was bringing `rakka-agent` into line with its own
  substrate rather than inventing policy. Every persisted attempt detail is now
  bounded at 512 bytes, and the bounding-is-not-sanitizing distinction is
  stated on all nine executor traits.
- **There was no trust-class routing, and the fix is not the obvious one.**
  Making `execution-policy-unroutable` *retryable* was the first instinct and
  is wrong three ways: it turns a shipped definitive failure into an unbounded
  spin for a single-worker deployment; it is a claim-then-release race, so the
  non-accepting worker holds the lease while the serving one cannot claim; and
  `defer_dispatch` writes to the durable fleet index on every refusal, giving
  write amplification proportional to workers × classes. The refusal is
  downstream of the mistake. Filtering happens **at the claim**, and needs no
  durable schema change because `ATTR_AGENT_EFFECT_EXECUTION_POLICY` already
  rides the ticket into the fleet index. Partitioning the fleet persistence id
  by class was also rejected: `pump_run` registers a run's whole due-effect
  batch in one write and a run's effects span classes, so every worker would
  still need write access to every class's index — no isolation gained, and a
  retag strands tickets with no migration path. The cost accepted: a class no
  worker serves now waits rather than failing fast, which is why
  `class_filtered` exists beside `due_dispatch_count`.
- **Retention had no production caller, and `updated_at` could not have been
  the clock.** A terminal run keeps accepting settlement and return commands,
  each advancing `updated_at`, so a deadline measured from it recedes
  indefinitely. `AgentRun::terminal_at` stamps the single terminal transition
  under its existing once-only guard. `discharge_run_memory_retention` purges
  snapshots *before* session rows, so a kill between them leaves the copy
  something else can still sweep rather than stranding content in the immutable
  tier. Private memory is untouched by design.
- **Two things documented rather than fixed, with the reasons recorded.**
  `SessionPurgeOutcome::Purged { entries }` is a cardinality oracle — a
  nonexistent scope answers zero — and stays one: the count is what makes a
  fleet sweep observable and a purge auditable, no memory surface is reachable
  from A2A or `query.rs`, and a conformance clause asserting otherwise would
  freeze the leak into the contract. And a private `delete` does not discharge
  an erasure request on its own, because a snapshot that embedded the memory
  keeps its copy until the run's snapshot purge — required by scenario 17's
  retry determinism, bounded by the retention window, and now proven as a fact
  with its bound rather than left a footnote.
- **One memory contract, run by both backends.** Four traits with two
  implementations each, and the semantics were asserted *twice by hand* — once
  in `memory.rs`'s unit tests and once, copied, in the PostgreSQL adapter's.
  `rakka_agent::memory_conformance` is now the single suite, following the
  knowledge graph's `conformance.rs` idiom rather than the testkit's
  `assert_*_store_contract` shape for two structural reasons: the subject is
  inherently *three*-scoped (a primary, a foreign, and a third
  genuinely-empty scope to compare against), and a live-DSN runner needs
  per-run namespacing once rather than at every call site. The isolation
  clauses compare by **whole value** against the empty scope: `is_empty()` and
  `is_none()`, which both hand-written copies used, are satisfied by a backend
  that answers "empty" *differently* from how it answers an unknown scope —
  a distinguishable `Ok` versus error, a different page shape — which is
  exactly the disclosure the clause forbids. Exhaustiveness is a compiler
  matter: one `…Operation` enum per trait, matched without a wildcard, so a
  new method fails to compile until its isolation arm is written. Writing the
  suite found two things about *the clauses*, both instructive: an isolation
  clause must do all its reads before any of its writes, and a "duplicate
  create" that reuses the original's derived operation id is answered from the
  ledger as a replay — the idempotence contract working, not a violation.
- Proof roster: `tests/memory_store_contract.rs` (14),
  `crates/rakka-agent-postgres/tests/memory_conformance.rs` (12, DSN-gated),
  `tests/memory_scope_fence.rs` (7), `tests/memory_guardrail_chain_consistency.rs`
  (10), `tests/memory_retention.rs` (8), `tests/secret_exclusion.rs` (9),
  `tests/executor_isolation.rs` (6), `tests/tenant_isolation.rs` (5).
  Documentation: `docs/rakka-agent-security-validation-matrix.md`, plus the new
  stable codes in `docs/rakka-compatibility.md`.
- Owed onward, and recorded in the matrix: guardrail evaluation points for
  `ModelResponse`, `ToolResponse`, `A2aIngress`, and `A2aEgress` — 4 of 7
  declared boundaries, with `ToolResponse` the poisoning-relevant one, since a
  tool result enters session memory and every later model context without
  crossing a boundary; communal retrieval and `SnapshotCommunalClaim` (slice
  4.6's deferral stands, and until it exists there is no communal poisoning
  surface); the knowledge graph's absent retention/tombstone/deletion, an
  absolute spec-13.1 requirement; the unwired model-visible descriptor rung;
  descriptor revision pinning across recovery; tenant-scoped mandatory
  guardrails; the non-atomic revocation re-check; the exhaustive
  `A2AOperation::ALL` deny-is-absent sweep at the A2A surface, whose two doors
  that fail open on tenancy by design (`A2AHeaderTenantResolver`'s
  request-supplied tenant and the `default_tenant` fallback) are named in the
  matrix but not yet driven under a denying authorizer; and the PostgreSQL
  adapter's hand-written store assertions, which the shared suite now
  duplicates and which should be reduced to backend-only proofs — the
  two-connection compare-and-set, migration idempotence, the doctored-row
  fail-closed, and the vector-encoding round trip.

### Slice 6.3a — GenAI adapter, metric catalogue, and export redaction

Spec: [17.2](spec.md#172-instrumentation-scope-and-resource),
[17.6](spec.md#176-required-span-model), [17.12](spec.md#1712-metrics),
[17.14](spec.md#1714-content-capture-and-redaction),
[17.15](spec.md#1715-baggage),
[17.20](spec.md#1720-semantic-convention-compatibility).

Slice 6.3 was split on 2026-08-27 after a survey of what it would have to
touch. The single entry read as validation work — wire the exporter, review
the pinning, add Collector rules — but four of its six items are blocked on
emission that does not exist: a tail-sampling policy selects on attributes no
mapping function writes, a redaction allowlist has nothing to allow until the
adapter names its keys, and both GenAI client metrics are histograms in a
crate that records none. 6.3a is that emission work; 6.3b is the deployment
validation that can only be honest once 6.3a has landed. The ordering is a
dependency, not a preference.

Two scope decisions are user-approved (2026-08-27): the **full metric
catalogue** of [17.12](spec.md#1712-metrics) is in scope rather than a named
subset, and 6.3b wires a **real OpenTelemetry SDK** rather than a fourth
serializable bridge record.

- **The `otel` module has no production consumer.** `AgentGenAiOperation`,
  `agent_instrumentation_scope`, `decision_span_event`, `usage_attributes`,
  and `AgentGenAiIdentity` are reached only from their own `#[cfg(test)]`
  block and the `lib.rs` re-export, and nothing in the workspace constructs an
  `AgentOtlpBridgeExport` outside tests. This is the shape slice 6.1 found in
  the five `init_agent_*_entity_remote_sharding` registrations, and the same
  corrective applies: the module needs a call site on the path a run actually
  takes, not more unit tests.
- **The convention pin is a string, not a review.**
  [17.20](spec.md#1720-semantic-convention-compatibility) requires an upgrade
  to review span names and kinds, metric names, units, and buckets, required
  attributes, operation values, content-capture guidance, and Collector rules.
  Span names and kinds are mapped; the rest are not.
  `ATTR_GEN_AI_OPERATION_NAME`, `ATTR_GEN_AI_PROVIDER_NAME`,
  `ATTR_GEN_AI_TOOL_NAME`, and `ATTR_GEN_AI_TOOL_TYPE` are declared and
  re-exported but written by no mapping function; `ATTR_ERROR_TYPE`,
  `ATTR_RAKKA_ERROR_CODE`, and `ATTR_RAKKA_AGENT_EFFECT_STATUS` are declared,
  unexported, and unused; `AgentGenAiOperation::span` leaves every status
  `Unset`. Status and error mapping is load-bearing for 6.3b, whose retention
  policies select on exactly those attributes.
- **The full 17.12 catalogue, into a crate that records no histogram.** Every
  duration the section lists — decision, goal evaluation, delegation,
  workflow-tool, assignment/handoff/result-validation/dependency, wake and
  epoch, autonomy admission and budget, team and moderation, active turn,
  wait, effect queue and dispatch, model and tool, memory and retrieval,
  recovery — is unrecorded, and both GenAI client metrics (operation
  duration, token usage) are histograms. The section's gauges join them:
  logically active and waiting by bounded status class, resident entities and
  activation/passivation rate, trigger/timer/outbox backlog and oldest age,
  shard ownership distribution. Every new label key joins
  `AGENT_METRIC_FIELDS` and passes `validate_agent_domain_metric_attributes`,
  and the catalogue is written down — 17.12 requires labels to be bounded
  *and documented*, and no document lists them today.
- **Units, buckets, and exemplars are structurally unrepresentable.**
  `rakka_core::OpenTelemetryMetric` carries name, kind, temporality, and data
  points and no unit; `OpenTelemetryDataPoint` carries attributes, value,
  count, and sum and no bucket boundaries or exemplars.
  [17.17](spec.md#1717-otlp-and-collector-boundary) permits extending the
  bridge additively or mapping directly into the application SDK, and forbids
  dropping the field silently while claiming semantic-convention compliance.
  `rakka-core` is in the publishable set and
  `docs/rakka-v1-observability-exporters.md` currently documents count/sum
  only as a deliberate choice, so whichever way this resolves it is a recorded
  decision with a doc change, not an implementation detail.
- **Redaction owes an allowlist before export, not only at the Collector.**
  `AgentOtelSpanExport::from_telemetry_context` copies
  `telemetry_context.baggage` straight into span attributes, and `validate()`
  checks only the trace context: there is a bounded-label validator for
  metrics and no counterpart for spans or logs. The agent domain clears
  baggage on persist through `sanitize_agent_telemetry_context`, which does
  nothing for a caller that builds a span from a context it was handed.
  [17.14](spec.md#1714-content-capture-and-redaction) puts minimization at the
  application and the Collector second, as defense in depth, and
  [17.15](spec.md#1715-baggage) makes received baggage untrusted.
- **Two `otel` features gate nothing.** `rakka-agent-workflow` and
  `rakka-a2a` each declare `otel = []`; the only `#[cfg(feature = "otel")]` in
  the workspace is `rakka-agent`'s. The A2A one is documented as gating
  trace-context propagation and attribute helpers, which is also why the A2A
  ingress `SERVER` span that `otel.rs` defers to the protocol adapter is built
  by nobody, leaving scenario 21's ingress row without its span half. Each
  feature is implemented or removed; a declared-inert feature is the finding,
  not the fix.

Done when: every span row, metric, and attribute the reviewed revision
requires for the shipped milestones is emitted by a mapping function with a
production call site; the 17.12 catalogue is recorded, its labels documented
and validated; unit, bucket, and exemplar semantics are either represented in
the bridge or documented as mapped in the application; and an attribute
outside the allowlist cannot reach a span or log export record. Scenario 25 is
re-proven at the export boundary rather than only at durable state.

**Landed.** Four decisions were taken, each because the obvious alternative was
wrong in a specific way rather than merely less tidy.

- **Emission is a neutral vocabulary, and only the mapping is gated.** The
  loop, the entities, and the dispatcher close `AgentTelemetrySegment` values
  named by `AgentSegmentOperation` with no feature gate, and `otel` owns the
  translation to `invoke_agent`, `execute_tool`, and the rest. Emitting the
  convention records directly under `#[cfg(feature = "otel")]` was rejected:
  it puts the GenAI vocabulary on the durable execution path, and it makes the
  emission absent from every default build — including the
  `cargo test -p rakka-agent --no-default-features` run that `validate.sh`
  performs, which is exactly where an unreachable adapter would hide again.
  `AgentGenAiSpanExporter` is the segment sink that closes the loop, and
  `genai_operation` is total over the Rakka vocabulary and matched without a
  wildcard, so adding a class fails to compile until its convention row exists.
- **Durations have two rules, because they have two kinds of endpoint.** A
  duration spanning a durable boundary — an outstanding effect, a turn — is the
  difference of two persisted timestamps, so a run that passivated or moved
  shard mid-effect reports what a resident one would. A duration inside one
  process has exactly one injected `now` available and measures its own
  monotonic width, anchored at that timestamp. Using the injected clock for
  both would have reported every live operation as instantaneous; using
  `Instant` for both would have made a figure that survives passivation
  impossible to compute. The effect pair was deliberately *not* split into
  queue and dispatch: `dispatched_at` is stamped when the run hands the effect
  to the outbox, not when a worker begins an attempt, so the split would report
  the run's hand-off latency under a name promising queue delay.
- **The catalogue is data, and a source scan holds it to the code.** 17.12
  requires labels to be bounded *and documented*, and the two prose tables that
  existed were both wrong — the technical guidance named fifteen metrics that
  did not exist, and the in-crate label list had gone stale on four keys in
  use. A third prose copy would have gone the same way, so
  `AGENT_DOMAIN_METRIC_INSTRUMENTS` carries name, kind, unit, labels, and
  buckets, and `tests/metric_catalogue.rs` scans the crate's own sources in
  both directions. The 17.12 gauges the substrate already publishes are
  deliberately absent rather than mirrored, with the providing instrument named
  in the catalogue page: two names for one number is two catalogues that drift.
- **Redaction is two layers, because bounding is not sanitizing.** The bridge
  stopped copying the durable context's baggage into span attributes — baggage
  is a propagation context, externally received baggage is untrusted, and the
  agent domain's own sanitizer runs on the persist path and so never saw a span
  built from a handed-in context — and gained the generic attribute, count, and
  ordering bounds the metric vocabulary always had. On top of that the adapter
  applies a closed allowlist before a record is built, because which keys may
  be exported is a domain decision and a denylist is a guess about what content
  will be called next time. Both were falsified: restoring the baggage copy
  fails the bridge test, removing the allowlist filter leaks `tool_arguments`.

A follow-up pass closed what the first left open, and the gap is worth
recording because it was the slice's own defect class recurring: three
attributes had been *declared and allowlisted* while nothing wrote them, which
is exactly the shape — a vocabulary that reads complete and emits nothing —
that made the adapter unreachable in the first place. Four of
[17.16](spec.md#1716-sampling)'s eight retention classes had nothing to select
on, so a Collector rule expressing them would have matched nothing in
production while passing its own tests. `rakka.agent.effect.status`,
`rakka.agent.effect.attempt`, `rakka.agent.checkpoint.kind`, and
`rakka.agent.settings_revision` now ride the segments that know them; the
`checkpoint-open` and `run-resume` rows are wired; the two remaining orphaned
mapping functions (`decision_span_event`, `usage_attributes`) have callers
through segment fields; and the log allowlist joins the span one — wider by
design, since a structured log carries the durable identities a metric may not.
The convention constants alias the ungated segment keys, so one key can never
again be two literals across a feature boundary.

Of the two inert features, `rakka-agent-workflow`'s was removed and
`rakka-a2a`'s was implemented — the ingress `SERVER` span, which closes
scenario 21's ingress half and is the only `AgentOtelSpanKind::Server` the
workspace constructs. Propagation stayed unconditional; gating it would have
removed trace continuity from a default build, which is the opposite of what
the feature claimed.

Owed onward, and named in
`docs/rakka-agent-observability-catalogue.md` rather than left silent: the
17.12 clauses with no instrument yet — logically active and waiting goals and
runs by status class, activation/passivation rate and cold-activation latency,
trigger and timer backlog with oldest age, wait duration by kind, the
delegation, workflow-tool, task-operation, wake, epoch, autonomy, budget, team,
and moderation *durations* whose counters exist, memory and retrieval latency,
and context snapshot size — several of which need the bounded,
deployment-invoked sweep shape of `AgentMemoryRetentionSweep`, because Rakka
keeps no index to enumerate them from. Exemplars are 6.3b's, by decision.

### Slice 6.3b — Application binary, Collector, sampling, and exporter failure

Spec: [17.14](spec.md#1714-content-capture-and-redaction),
[17.16](spec.md#1716-sampling),
[17.17](spec.md#1717-otlp-and-collector-boundary).

The deployment half of the split described in 6.3a, and it depends on 6.3a
whole: a Collector rule can only allowlist keys the adapter emits, and a
tail-sampling policy can only retain traces whose spans carry a status and an
error code. Wiring it first would produce configuration that passes its own
string-matching tests and drops everything in production.

- **A real SDK at a real binary** (user-approved 2026-08-27). The workspace
  depends on neither `opentelemetry` nor `tracing-subscriber` today —
  `rakka-agent`'s testkit hand-rolls `CapturingSubscriber` to avoid the
  latter. [17.17](spec.md#1717-otlp-and-collector-boundary) puts the SDK, the
  `tracing` subscriber and layer, the OTLP exporter, exporter credentials, and
  shutdown/flush at the application boundary, so this lands in a new
  `publish = false` example that owns all five and drives a real agent run
  through them. The workspace's pinned tonic 0.12 / prost 0.13 generation
  constrains the SDK version; that pin is reviewed and recorded like the Rig
  and A2A pins. Core crates stay SDK-neutral, and `scripts/package-check.sh`
  stays offline and green.
- **Spans come from the live loop, not from durable state** (resolved in
  planning). `AgentGenAiOperation::span` needs a start and an end per bounded
  segment, and durable records carry only `occurred_at`. Stamping segment
  boundaries into persisted schema would make a telemetry change a durable
  migration, which [17.20](spec.md#1720-semantic-convention-compatibility)
  forbids absent an independent domain change. The loop emits its own bounded
  segments and the persisted context supplies links and resume — the model
  [17.4](spec.md#174-bounded-trace-segments) already describes.
- **An agent-domain Collector configuration.** The shipped topology under
  `docs/plans/agentic-workflow/` is the workflow domain's: its
  `transform/redact` is a denylist of six keys (`prompt_text`,
  `completion_text`, `tool_arguments`, `tool_output`, `artifact_uri`,
  `authorization`), none of which the GenAI vocabulary uses, and its metric
  rules drop workflow identifiers. The agent domain needs its own, keyed on
  `gen_ai.*` and `rakka.agent.*` and expressed as the allowlist
  [17.14](spec.md#1714-content-capture-and-redaction) asks for. Contract tests
  mirror `crates/rakka-k8s/tests/agent_workflow_otel_collector_topology.rs`,
  including its gated `kubectl` validation. The Collector distribution and
  component versions are pinned with a stated revalidation procedure; the
  present manifests pin `otel/opentelemetry-collector-contrib:0.107.0` against
  a convention revision of 1.36.0, which the review either reconciles or
  records.
- **Tail sampling, with the routing it requires.** The gateway runs
  `probabilistic_sampler` and no `tail_sampling`, and the agent-to-gateway hop
  is a plain `otlp/gateway` exporter, so nothing guarantees
  [17.16](spec.md#1716-sampling)'s requirement that every span of one trace
  reach the same decision instance. A `loadbalancing` exporter with
  trace-ID-aware routing and a `tail_sampling` policy expressing all eight
  retention classes — error status or stable failure code, indeterminate
  effect or reconciliation, security denial, policy override, or revocation,
  checkpoint escalation or timeout, recovery failure or stale-owner conflict,
  configured high latency, excessive retry, and a newly deployed version under
  investigation — replaces it, sized together with decision wait, trace
  buffers, memory limiter, queues, and exporter retry as the section requires.
- **Loss visibility on the export path, not on one sink.**
  `METRIC_AGENT_TELEMETRY_FLUSH_FAILURES` has a single recording site — the
  decision sink's refusal in `run.rs` — with `METRIC_AGENT_DECISION_DROPS` as
  a gauge beside it, and scenario 26 is proven against that sink alone.
  [17.12](spec.md#1712-metrics) asks for export queue, drops, failures, and
  Collector/exporter health, and neither Collector configuration enables
  `service.telemetry`, which is where the refusal, queue, drop, processing,
  and export-failure counters
  [17.17](spec.md#1717-otlp-and-collector-boundary) lists come from.
- **Exporter failure proven by behavior, not by grep.** The gateway config
  carries `sending_queue` and `retry_on_failure`, and the existing test
  asserts those strings are present. 6.3b drives an unreachable endpoint and a
  saturated queue against the real exporter and asserts the run still
  converges from durable state, that the loss is counted, and that drain still
  flushes. A live-Collector arm is gated in the established idiom (an endpoint
  environment variable, as `RAKKA_POSTGRES_TEST_DSN` gates the PostgreSQL
  suites); a deterministic in-process arm always runs, so the claim is never
  gate-only.
- **Falsification, per the standing lesson of 6.1 and 6.2.** Every fix across
  6.3a and 6.3b is reverted, the suite confirmed failing, and restored, with
  the falsification recorded. A green suite over a silent path proves nothing,
  which is exactly how the adapter arrived at this slice fully unit-tested and
  entirely unreachable.

Done when: an example binary exports agent traces, metrics, and logs to a
Collector over OTLP through a pinned SDK it owns; the agent Collector
configuration allowlists, tail-samples with trace-ID-aware routing, and
reports its own health; an unavailable exporter changes no durable outcome and
is visible in bounded counters; and
`docs/rakka-agent-telemetry-validation-matrix.md` records the reviewed
convention revision, the pinned distribution and component versions, and which
telemetry claims are enforced, which are delegated to the deployment, and
which remain inferred — the third companion to the fault-injection and
security matrices.

**Landed.** Five decisions, and three defects that only a real SDK, a real
socket, and a real Collector could have found.

- **The SDK pin is decided by an API boundary, not by recency.** `opentelemetry`
  0.29 is both the highest release on the workspace's `tonic 0.12` / `prost
  0.13` generation *and* the last one whose `metrics::data` types an
  application can construct — 0.30 sealed `ResourceMetrics`, `Histogram`, and
  `HistogramDataPoint`. That second constraint is the load-bearing one: Rakka
  arrives at the boundary with metrics **already aggregated**, carrying the
  catalogue's declared units and bucket boundaries, and 0.29 is the last
  generation that can accept them as they are. On 0.30 or later the only path
  is re-recording every measurement through the `Meter` API and re-declaring
  the buckets in the binary, which makes the catalogue advisory and
  [17.17](spec.md#1717-otlp-and-collector-boundary)'s "MUST NOT silently drop
  the field" hard to keep. Recorded as a design change rather than a version
  bump.
- **The receiver is in-process, so the wire claim is never gate-only.** The
  export walk stands up a real gRPC server speaking the generated OTLP service
  definitions on an ephemeral port and asserts on the **decoded protobuf** it
  was handed. Asserting on the batch the mapping built would have proven the
  mapping and nothing about the export — a serialization OTLP rejects, a signal
  wired to the wrong service, a unit dropped in translation would all have
  passed. A live-Collector arm exists and is gated; it is not what the claim
  rests on.
- **Exemplars are declared, not inferred.** 6.3a recorded them as owed to this
  boundary because `MetricsRecorder` has no trace identity to read — trace
  context here is an explicit value on a durable record, never an ambient one.
  A closed `AgentTelemetrySegment` is the one value carrying a measurement's
  operation *and* its trace and span ids, so a bounded reservoir fed by the
  segment sink fills `HistogramDataPoint.exemplars`, and `EXEMPLAR_SOURCES`
  names which segment class supplies which histogram. A representative link is
  what an exemplar is; that it is not a per-measurement one is recorded rather
  than implied.
- **The allowlist is checked against the code, in the crate that has it.** The
  Kubernetes shape is contract-tested in `rakka-k8s` and the allowlist in
  `rakka-agent`, because `rakka-k8s` sits below it in the DAG and can only
  compare a list of strings to a copy of itself. That is the failure this entry
  warned about — configuration that passes its own string-matching tests and
  drops everything in production — and a same-crate test would have been
  precisely it. The retention selectors are checked the same way, in both
  directions: a policy key no mapping function writes fails, and so does a key
  the allowlist strips before the sampler runs.
- **A gated arm runs the pinned distribution's own `validate`.** `kubectl`
  validates Kubernetes objects and knows nothing about a ConfigMap's contents;
  a string assertion knows only that a word appears. This arm found two of the
  three defects below within a minute of first running.

The three defects, each with the falsification that keeps its test honest:

- **`InProcessRunResultDelivery` recorded no metrics.** It threaded a segment
  sink and no `MetricsRecorder`, so `turn.duration`, `model.tokens`,
  `effect.outcomes`, and `effect.outstanding.duration` were recorded by
  **nobody** on the delivery path while the sharded entity beside it reported a
  healthy surface. This is the "every driver of a run must share one wiring"
  rule the segment sink states, one field over and unnoticed. Found because the
  export walk's exported metric set was missing exactly the instruments that
  path owns — four metrics where seven were expected.
- **`container.name` is not a `k8sattributes` field** at a current
  distribution. The metadata list was inherited from the workflow topology,
  which pins a 2024 build; the Collector refuses to start on it. This is what
  the 0.107.0-against-1.36.0 spread looks like in practice, and it is why the
  agent topology took a current pin while the workflow one's stays where its
  own plan put it.
- **`loadbalancing` refuses `routing_key: traceID` for metrics.** Wiring the
  metrics pipeline to the exporter traces need produces a Collector that fails
  at startup — and a string assertion for `loadbalancing` calls it correct. The
  metrics pipeline has its own `otlp/gateway` exporter; traces and logs keep
  the trace-id router, over a **headless** service, because a `ClusterIP` there
  would spread one trace's spans across gateway replicas and each would sample
  a partial trace with nothing failing.

Two things are recorded as inferred rather than claimed, in
`docs/rakka-agent-telemetry-validation-matrix.md`: tail sampling is contract-
tested and distribution-validated as configuration but is not exercised against
a running two-replica gateway, and **an agent trace can outlive its own
sampling decision** — a run parked on a human approval resumes long after
`decision_wait`, and the segments it closes then are governed by a decision
taken without them. That is inherent to tail sampling, and it is why the
retention classes select on attributes that appear early in a trace wherever
they can.

Owed onward and named rather than left silent: 13 of the 24 segment classes
still have no production call site, and `AgentSegmentIdentity::of_task` has no
caller at all — so `rakka.agent.task.id`, `.goal.id`, and `.delegation.id` are
allowlisted at both layers and written by nothing, which is 6.3a's own defect
class still open for three attributes.

### Slice 6.4 — Documentation and compatibility

Spec: [20](spec.md#20-compatibility-and-migration).

- Product docs under `docs/` for the agent surface, API boundary tier entry
  in `docs/rakka-api-boundary-inventory.md`, `CHANGELOG.md` entries, N/N+1
  compatibility notes, and the pinned A2A/Rig/GenAI revision matrix.

Done when: every shipped-phase scenario passes under in-process fault
injection; every scenario the fault-injection matrix names as requiring
multi-pod fidelity passes there too; and the documentation set is current.

Stated that way because it is checkable. "At the fidelity its claim requires"
named no artifact that says, per scenario, what fidelity that is — and the
matrix enumerates the multi-pod subset (currently scenarios 1's
creation-deduplication half, 2, and 60) rather than all sixty-one. The matrix
is the authority for that subset; a scenario added to it is added to this
criterion.

The multi-process harness carries the claims an in-process kill structurally
cannot reach — a durable store outside the dying process, real shard movement
after a downing decision, and an external commit that outlives its pod — and
`docs/rakka-agent-fault-injection-matrix.md` records which scenarios are
re-proven at that fidelity and which remain in-process. Porting all 61 scenarios
into process-spawning form was considered and rejected: it would re-prove logic
the in-process suite already covers, at a large cost in harness code and gated
runtime, without adding a claim.

---

## Appendix — Scenario-to-Slice Coverage

All scenario numbers refer to
[spec 18](spec.md#18-required-recovery-scenarios).

| Scenarios | Milestone | Proving slice |
| --- | --- | --- |
| 1 | M1 | 1.12 |
| 2 | M1 | 1.5 |
| 3, 11, 12, 13 | M1 | 1.10 |
| 4, 19, 35, 46 | M1 | 1.14 |
| 5-10 | M1 | 1.7 |
| 14, 17 | M1 | 1.11 |
| 21-26, 56 | M1 | 1.13 |
| 37, 40, 55 | M1 | 1.4 |
| 44, 54 | M1 | 1.8 |
| 52, 53, 61 | M1 | 1.9 |
| 57 | M1 | 1.7 (effect fencing) + 1.10 (reconciliation) |
| 58, 60 | M1 | 1.3 |
| 59 | M1 | 1.5 |
| 15 | M2 | 2.1 |
| 16, 18 | M2 | 2.1 (private) + 2.3 (graph) |
| 20 | M2 | 2.4 |
| 36, 51 | M3 | 3.3 |
| 47-50 | M3 | 3.2 |
| 27, 34 | M4 | 4.4 |
| 28, 39 | M4 | 4.3 |
| 29, 31, 33 | M4 | 4.6 |
| 30 | M4 | 4.2 |
| 32 | M4 | 4.5 |
| 38 | M5 | 5.1 |
| 41 | M5 | 5.4 |
| 42 | M5 | 5.2 |
| 43 | M5 | 5.3 |
| 45 | M5 | 5.5 |
