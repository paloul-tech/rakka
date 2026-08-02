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

### Slice 4.7 — M4 acceptance and goal views

Spec: [Multi-Agent Goal Milestone](spec.md#multi-agent-goal-milestone-m4),
[17.18](spec.md#1718-authoritative-operational-queries-and-observability-views).

- Authorized goal projection (tasks, runs, delegation graph, workflow links,
  evaluations, evidence, budgets, cancellation state).
- End-to-end example: root goal delegating to two specialists plus one
  workflow tool, surviving root and child pod loss, satisfied only through
  the evaluator.

Done when: the multi-agent milestone checklist is demonstrated end to end.

---

## Phase 5 — M5 Coordination Capabilities

Milestone: M5. Acceptance:
[Coordination Capability Milestone](spec.md#coordination-capability-milestone-m5).
Scenarios owed: 38, 41-43, 45.

Open decisions to resolve: 6 (agent cards/assignment), 18 (first-class
patterns — resolved default), 19 (setup envelope — enforced since Phase 1),
21 (replayable coordination events).

### Slice 5.1 — Capability model and handoff

Spec: [8.8](spec.md#88-coordination-capability-model),
[8.9](spec.md#89-handoff), [14.2](spec.md#142-task-identity-and-projection)
(handoff lineage).
Guidance: [Coordination Capabilities](technical-guidance.md#coordination-capabilities).

- `AgentCoordinationCapability` descriptors as trusted definition/setup data;
  runtime may expose them to the model as tools, but model output cannot
  create capability, target, budget, or scope.
- Handoff: same `AgentTaskId`, source-run fencing, target-run creation,
  explicit context/artifact projection only, `HandedOff` terminal recorded
  after durable target acceptance; traverses outbox/inbox + `rakka-a2a` even
  colocated.

Done when: scenario 38 passes.

### Slice 5.2 — Team coordination

Spec: [8.10](spec.md#810-team-coordination).

- `AgentTeamId`, bounded membership, durable shared task board; atomic
  claim/release/transfer with revision/lease fencing and stable operation
  IDs; mediated peer messages over durable commands.
- Idle teams and members passivate; the board is data.

Done when: scenario 42 passes (one normal claim owner; stale commands fail
closed).

### Slice 5.3 — Moderation

Spec: [8.11](spec.md#811-moderation).

- `AgentConversationId`, participant set, durable turn/round state,
  transcript artifacts, budgets; only the current participant may submit;
  duplicates rejected; participants passivate between turns.

Done when: scenario 43 passes (turn recovery without duplication across
passivation/shard movement).

### Slice 5.4 — Human-owned tasks

Spec: [8.12](spec.md#812-human-owned-tasks),
[14.3](spec.md#143-taskrun-state-mapping) (`WaitingForInput` row).

- Tasks deliberately unassigned to agents, completed by authenticated
  humans/services with typed results through the same validation path;
  dependency unblocking and failure propagation.
- Keep the boundary with effect-bound checkpoints explicit
  ([spec 8.12](spec.md#812-human-owned-tasks)).

Done when: scenario 41 passes.

### Slice 5.5 — Replayable coordination events

Spec: [17.13](spec.md#1713-structured-logs-runtime-events-and-audit),
[14.5](spec.md#145-typed-agent-client).
Guidance: [Client, Events, and Testkit](technical-guidance.md#client-events-and-testkit).

- Extend the Phase 1 event replay to coordination events (assignment,
  handoff, claim, turn) with monotonic scoped cursor, bounded retention, and
  explicit resync; derived struggle signals stay projections.

Done when: scenario 45 passes.

### Slice 5.6 — M5 acceptance

Spec: [Coordination Capability Milestone](spec.md#coordination-capability-milestone-m5).

- Deterministic model/tool scripts plus fault injection covering every task,
  handoff, claim, turn, and effect boundary.
- Walk the coordination milestone checklist end to end.

Done when: the checklist is demonstrated and all M5 scenarios pass under
fault injection.

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

### Slice 6.2 — Security validation

Spec: [16](spec.md#16-security-and-authorization),
[13.1](spec.md#131-general-requirements).

- Memory ACL and poisoning defenses, retention/deletion/tombstone flows,
  cross-tenant existence-leak tests, executor trust-class routing, secret
  exclusion sweeps over state/events/telemetry.

### Slice 6.3 — Telemetry and Collector validation

Spec: [17.14-17.17](spec.md#1714-content-capture-and-redaction),
[17.20](spec.md#1720-semantic-convention-compatibility).

- Native OTLP wiring at an application binary, pinned GenAI convention
  review, redaction/allowlist processors, tail-sampling retention rules,
  Collector loss visibility, exporter failure behavior.

### Slice 6.4 — Documentation and compatibility

Spec: [20](spec.md#20-compatibility-and-migration).

- Product docs under `docs/` for the agent surface, API boundary tier entry
  in `docs/rakka-api-boundary-inventory.md`, `CHANGELOG.md` entries, N/N+1
  compatibility notes, and the pinned A2A/Rig/GenAI revision matrix.

Done when: all shipped-phase scenarios pass in the multi-process harness and
the documentation set is current.

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
