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
  sequences rather than duplicating them (scenario 16).
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

Done when: scenario 1 passes (duplicate A2A task messages create one task,
one run, one turn) and the projection table has a test row-for-row.

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

Done when: scenarios 21-26 and 56 pass.

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

Done when: scenario 15 passes and the private-memory half of scenarios 16
and 18 passes.

### Slice 2.2 — Vector retrieval adapter

Spec: [13.3](spec.md#133-agent-private-long-term-memory),
[13.6](spec.md#136-storage-adapters).

- `pgvector` retrieval in `rakka-agent-postgres`: embeddings as rebuildable
  derived data with model/dimension/version metadata; source content
  preserved independently.
- Tenant and `AgentId` filters enforced in schema and query even where it
  costs index performance; recall characteristics documented.
- Retrieval feeds `MemoryContextSnapshot` only through the Slice 1.11 path.

Done when: retrieval isolation tests pass and a snapshot-reuse test proves
index drift cannot change a retried model input (extends scenario 17).

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

Done when: the graph halves of scenarios 16 and 18 pass on the in-memory
implementation.

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

Done when: scenario 20 passes across both implementations without touching
agent-domain code.

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

### Slice 3.4 — Continuous lifecycle and M3 acceptance

Spec: [8.2](spec.md#82-continuous-goal-controller-and-epochs),
[17.18](spec.md#1718-authoritative-operational-queries-and-observability-views),
[Continuous Goal Milestone](spec.md#continuous-goal-milestone-m3).

- Suspension, renewal, failure backoff, expiry, and retirement transitions.
- Operational query exposure: schedule revision, next wake, last progress,
  active epoch, budget window, missed/coalesced counts, retirement state.
- Wake/epoch metrics and audit events
  ([spec 17.12](spec.md#1712-metrics),
  [17.13](spec.md#1713-structured-logs-runtime-events-and-audit)).

Done when: the continuous milestone checklist is demonstrated end to end by
an example with fault injection across pod restart and shard movement.

---

## Phase 4 — M4 Multi-Agent Goals

Milestone: M4. Acceptance:
[Multi-Agent Goal Milestone](spec.md#multi-agent-goal-milestone-m4).
Scenarios owed: 27-34, 39.

Open decisions to resolve: 14 (distinct goal identity — resolved default),
15 (catalog resolves specialists), 16 (workflow tools).

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
