# Run Lifecycle Decision Events Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The agent run loop records one decision event, in the run's own sequence and after the transition persists, for each checkpoint transition, live-run unknown outcome, run end and wait (issue #119). The new kinds are written only when the host turns on `AgentDecisionEventVocabulary::Version2`.

**Architecture:** Transitions queue drafts on a non-persisted list on `AgentLoopState`, capturing positional fields at queue time. A new provided `seal` hook on `AgentExchangeParticipant` runs on all three `AgentExchangeHost` write paths before persist. The run participant's `seal` discards the drafts when no sink is wired. Otherwise it derives the waits from the before/after states, filters by vocabulary, and assigns sequences into the bounded outbox ring. Every slot is built from durable state a `bc0c224` binary already keeps.

**Tech Stack:** Rust 1.88 workspace, crate `rakka-agent`, serde, tokio tests, the crate's in-memory test kit.

**Spec:** `docs/superpowers/specs/2026-11-02-run-lifecycle-decision-events-design.md` (approved 2026-11-03, commit `9763aa6`). Read it before any task. Section numbers below (§) refer to it.

## Global Constraints

- **No semantic changes** to any transition, to compiled workflows, to trace segments, or to the snapshot and queries (§3 non-goals). Recording is the only change at every site.
- **Never modify rakka.host.** Never publish any crate.
- **Bounded labels only.** No summary, roles, resolver identity, arguments or results on an event (§3 anti-goals).
- **No new durable field may carry an event's identity** (§4). Slots come from effect `turn`/`slot`/`generation`, the top-up `sequence`, or the run's existing `decision_sequence`.
- **One logical event per transition.** A committed transition never records an identity an earlier committed transition recorded (§5.6).
- **Default vocabulary is `Version1`.** With no sink wired, nothing is recorded and the record never grows (§5.3).
- **Workspace lints:** every new public item gets a doc comment; `cargo clippy --workspace --all-targets --all-features -- -D warnings` must pass.
- **Existing expectations:** when a task's events appear in an existing test's expected list, append or insert them at their true sequence positions. Never remove, reorder or re-attribute an existing event. List every test you changed in the task's commit message.
- **Machine rules:**
  - Run `cargo clean` before starting each task, never mid-task.
  - Run tests per crate (`-p rakka-agent`), not the one-shot workspace run.
  - Redirect long test output to a file in the scratchpad and capture `$?`; never pipe `validate.sh` into `tail`.
- **Commits:** one per task, imperative subject, ending with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Branch `run-lifecycle-decision-events`.

## Review Focus

1. **Re-opening a checkpoint the run already holds.** `record_checkpoint` silently returns `Ok(())` for a held id, so `repair_park` or a re-driven open must record nothing. Pinned in Task 6.
2. **A transition that queues a draft and is then rejected.** It must leave no event and consume no sequence: drafts live on the discarded clone. Pinned in Task 7 with a refused `ResolveIndeterminateEffect`.
3. **A run driven by entity instances with different vocabularies.** That happens mid-roll, when the host flips the switch. The run's sequence stays contiguous, and each instance writes only its kinds. Pinned in Task 8.
4. **Both reconciliation paths aimed at one generation.** A `ResolveIndeterminateEffect` after a checkpoint `EffectResolution` resolved the same generation records exactly one `reconcile`. Pinned in Task 7.
5. **An SLA escalation followed by expiry on the same checkpoint.** The result is one `escalate-checkpoint`, then one `resolve-checkpoint` (`expired`). Timer passes in between record nothing. Pinned in Task 7.

## File Structure

| File | Responsibility in this change |
|---|---|
| `crates/rakka-agent/src/observability.rs` | The six new kinds, `AgentDecisionOutcome`, `AgentDecisionCheckpoint`, `AgentDecisionEventVocabulary`, the four event fields, draft builders, and `AgentDecisionEvent::stamp`. |
| `crates/rakka-agent/src/query.rs` | `AgentCheckpointBoundEffectView::of_binding` / `of_effect`; the `next_wake` doc comment. |
| `crates/rakka-agent/src/loop_runtime.rs` | The pending-draft list, `queue_decision`, `commit_decisions`, `discard_pending_decisions`, the byte bound, `record_checkpoint -> bool`, `cancel_open_checkpoints -> Vec<HumanCheckpointId>`. |
| `crates/rakka-agent/src/choreography.rs` | `AgentExchangeParticipant::seal`, the three host call sites, `AgentExchangeHost::participant_mut`. |
| `crates/rakka-agent/src/run.rs` | `AgentRunParticipant` struct and `seal`, the vocabulary builders, every recording site, the wait derivation, `AgentRun::winding_down`. |
| `crates/rakka-agent/src/schema.rs` | `CURRENT_AGENT_DECISION_EVENT_SCHEMA_VERSION` = 2. |
| `crates/rakka-agent/src/lib.rs` | Re-exports. |
| `crates/rakka-agent/src/testkit.rs` | The vocabulary knob on `InProcessRunEntityTransport` and `InProcessRunResultDelivery`. |
| `crates/rakka-agent/tests/common/mod.rs` | The fixture's vocabulary knob, `CountingDecisionSink`, `assert_decision_stream_whole`, and the `bc0c224` mirror. |
| `crates/rakka-agent/tests/decision_event_vocabulary.rs` | New: labels, vocabulary filtering, version stamping, the `bc0c224` mirror. |
| `crates/rakka-agent/tests/decision_events_checkpoints.rs` | New: asks 1 and 2. |
| `crates/rakka-agent/tests/decision_events_run_end.rs` | New: ask 3. |
| `crates/rakka-agent/tests/decision_events_waits.rs` | New: ask 4. |
| `docs/rakka-compatibility.md`, `docs/rakka-agents.md`, `CHANGELOG.md` | §6 and §8. |

---

### Task 1: The event vocabulary and its fields

Types only; no recording behavior changes.

**Files:**
- Modify: `crates/rakka-agent/src/observability.rs:224-470`
- Modify: `crates/rakka-agent/src/query.rs:281-291,841-846`
- Modify: `crates/rakka-agent/src/lib.rs:261-262`
- Create: `crates/rakka-agent/tests/decision_event_vocabulary.rs`

**Interfaces:**
- Produces:
  - `AgentDecisionKind::{RequestReconciliation, ResolveCheckpoint, EscalateCheckpoint, Cancel, Handoff, Supersede}`, with labels `request-reconciliation`, `resolve-checkpoint`, `escalate-checkpoint`, `cancel`, `handoff`, `supersede`.
  - `pub enum AgentDecisionOutcome { Granted, Denied, Expired, Cancelled, ConfirmedExecuted, ConfirmedNotExecuted, Compensate, Abandon }`: `#[non_exhaustive]`, kebab-case, with `pub const fn as_label(self) -> &'static str` and `Display`.
  - `pub struct AgentDecisionCheckpoint { pub checkpoint_id: HumanCheckpointId, pub kind: AgentCheckpointKind, pub due_at: Option<AgentTimestampMillis>, pub expires_at: Option<AgentTimestampMillis> }`: `#[non_exhaustive]`, deriving `Debug, Clone, PartialEq, Eq, Serialize, Deserialize`, with `pub(crate) fn of(checkpoint: &AgentCheckpoint) -> Self`.
  - `pub enum AgentDecisionEventVocabulary { #[default] Version1, Version2 }`: `#[non_exhaustive]`, deriving `Debug, Clone, Copy, PartialEq, Eq, Hash, Default`. It has:
    - `pub const fn writes(self, kind: AgentDecisionKind) -> bool`;
    - `pub const fn schema_version(self) -> StateSchemaVersion` (1 and 2).
  - Four public fields on `AgentDecisionEvent`, each `#[serde(default, skip_serializing_if = "Option::is_none")]`:
    - `checkpoint: Option<AgentDecisionCheckpoint>`;
    - `effect: Option<AgentCheckpointBoundEffectView>`;
    - `outcome: Option<AgentDecisionOutcome>`;
    - `run_status: Option<AgentRunStatus>`.
  - `AgentDecisionDraft` methods:
    - `with_checkpoint(self, AgentDecisionCheckpoint) -> Self`;
    - `with_effect(self, AgentCheckpointBoundEffectView) -> Self`;
    - `with_outcome(self, AgentDecisionOutcome) -> Self`;
    - `with_run_status(self, AgentRunStatus) -> Self`;
    - `pub const fn kind(&self) -> AgentDecisionKind`.
  - `AgentCheckpointBoundEffectView` constructors:
    - `pub(crate) fn of_binding(binding: &AgentCheckpointEffectBinding) -> Self`;
    - `pub(crate) fn of_effect(effect: &AgentRunEffect) -> Self`, using `effect.request.target_label()` and `effect.safety.class()`.

- [ ] **Step 1: Write the failing tests** in `tests/decision_event_vocabulary.rs` (module doc: §5.1-5.3; `mod common;`):

```rust
const NEW_KINDS: [(AgentDecisionKind, &str); 6] = [
    (AgentDecisionKind::RequestReconciliation, "request-reconciliation"),
    (AgentDecisionKind::ResolveCheckpoint, "resolve-checkpoint"),
    (AgentDecisionKind::EscalateCheckpoint, "escalate-checkpoint"),
    (AgentDecisionKind::Cancel, "cancel"),
    (AgentDecisionKind::Handoff, "handoff"),
    (AgentDecisionKind::Supersede, "supersede"),
];
// OLD_KINDS: the ten existing kinds with their labels.

#[test] fn every_kind_label_is_its_serde_label()
// for (kind, label) in OLD_KINDS ++ NEW_KINDS:
//   assert_eq!(kind.as_label(), label); assert_eq!(serde_json::to_value(kind)?, json!(label));

#[test] fn every_outcome_label_is_its_serde_label()
// granted, denied, expired, cancelled, confirmed-executed, confirmed-not-executed, compensate, abandon

#[test] fn version_one_writes_exactly_the_ten_kinds_a_bc0c224_reader_decodes()
// Version1.writes(k) for every OLD kind, !Version1.writes(k) for every NEW kind,
// Version2.writes(k) for all 16; AgentDecisionEventVocabulary::default() == Version1;
// Version1.schema_version().get() == 1; Version2.schema_version().get() == 2.

#[tokio::test] async fn an_event_without_the_new_fields_serializes_without_their_keys()
// Drive the two-turn flow of tests/decision_events.rs (copy tool_calling_turn / proposing_turn)
// with a sink; for every event: let value = serde_json::to_value(&event)?;
// for key in ["checkpoint", "effect", "outcome", "run_status"] { assert!(value.get(key).is_none()) }
```

- [ ] **Step 2: Run them and confirm they fail to compile**

Run: `cargo test -p rakka-agent --all-features --test decision_event_vocabulary`
Expected: compile errors naming `RequestReconciliation`, `AgentDecisionOutcome` and `AgentDecisionEventVocabulary`.

- [ ] **Step 3: Implement the types**
  - Add them in `observability.rs` beside `AgentDecisionKind`, and re-export them all from `lib.rs`.
  - Replace the doc comment at `observability.rs:224-228` with the shipped vocabulary. Remove the "coordination kinds arrive with their phases" sentence. Say that `handoff` is the run's end by handoff, not the model's choice (which is `call-tools`).
  - Doc-comment each new kind with its transition from §5.1.
  - `AgentDecisionEvent::assemble` sets the four new fields from the draft.
  - Switch `query.rs:841-846` to `AgentCheckpointBoundEffectView::of_binding`.

- [ ] **Step 4: Run the new tests and the existing decision tests**

Run: `cargo test -p rakka-agent --all-features --test decision_event_vocabulary --test decision_events --test operational_query`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent/src/observability.rs crates/rakka-agent/src/query.rs crates/rakka-agent/src/lib.rs crates/rakka-agent/tests/decision_event_vocabulary.rs
git commit   # "Declare the run lifecycle decision kinds and their event fields (issue #119)"
```

---

### Task 2: Queued drafts and the seal hook

The mechanism of §5.4: the existing four sites move onto it, and the `decision_events: bool` parameter goes. Version stamping stays at `CURRENT_AGENT_DECISION_EVENT_SCHEMA_VERSION` until Task 3.

**Files:**
- Modify: `crates/rakka-agent/src/loop_runtime.rs:314-400,520-640`
- Modify: `crates/rakka-agent/src/observability.rs` (`assemble`, the new `stamp`)
- Modify: `crates/rakka-agent/src/choreography.rs:1905-1977,2118-2270`
- Modify: `crates/rakka-agent/src/run.rs`:
  - the participant, `:6466-6474`;
  - the store, `:6775-6890`;
  - the actor, `:9149-9157`;
  - the sharding settings, `:9286`, `:9318`, `:9414-9422` and `:9580-9582`;
  - the four sites, `:2768`, `:3973`, `:4206`, `:4227` and `:6112`;
  - the five functions with the bool, `:2621`, `:2708`, `:3291`, `:4191` and `:6047`, and their callers at `:7451-7453` and `:7959-8013`;
  - the unit test at `:10529-10580`.
- Modify: `crates/rakka-agent/src/testkit.rs` (`InProcessRunEntityTransport`, `:1579-1775`; `InProcessRunResultDelivery`, `:3069-3225`)
- Modify: `crates/rakka-agent/tests/common/mod.rs` (`Fixture`: `:860-863`, `:982`, `:1022-1031`, `:1344`, `:1622`, `:3038`)
- Modify: `crates/rakka-agent/tests/escrow_ledger.rs:663` and `tests/exchange_receiver_memoization.rs:431`, to `AgentRunParticipant::default()`
- Test: `crates/rakka-agent/tests/decision_event_vocabulary.rs`; unit tests in `run.rs`'s test module

**Interfaces:**
- Consumes: Task 1's types.
- Produces:
  - Loop-state methods:
    - `AgentLoopState::queue_decision(&mut self, scope: &AgentRunScope, draft: AgentDecisionDraft, now: AgentTimestampMillis)` (pub(crate));
    - `AgentLoopState::commit_decisions(&mut self, vocabulary: AgentDecisionEventVocabulary)` (pub(crate));
    - `AgentLoopState::discard_pending_decisions(&mut self)` (pub(crate)).
    - `record_decision` is removed.
  - `AgentDecisionEvent::stamp(&mut self, sequence: u64, schema_version: StateSchemaVersion)` (pub(crate)). `assemble` loses its `sequence` parameter.
  - The participant hook, `AgentExchangeParticipant::seal(&self, before: &Self::State, after: &mut Self::State, now: AgentTimestampMillis) {}`: a provided method that does nothing by default.
  - `AgentExchangeHost::participant_mut(&mut self) -> &mut P` (pub(crate)).
  - The run participant, `pub struct AgentRunParticipant { decisions: Option<AgentDecisionEventVocabulary> }`: deriving `Debug, Clone, Copy, Default, PartialEq, Eq`, with `pub(crate) fn set_decisions(&mut self, vocabulary: Option<AgentDecisionEventVocabulary>)`.
  - The public builder `with_decision_event_vocabulary(self, AgentDecisionEventVocabulary) -> Self`, on `AgentRunEntityStore`, `AgentRunEntity` and `AgentRunEntityShardingSettings`.
  - Test-kit knobs:
    - `InProcessRunEntityTransport::install_decision_vocabulary(&self, AgentDecisionEventVocabulary)`;
    - `InProcessRunResultDelivery::with_decision_event_vocabulary(self, AgentDecisionEventVocabulary) -> Self`;
    - `Fixture::with_decision_event_vocabulary(self, AgentDecisionEventVocabulary) -> Self`.

- [ ] **Step 1: Write the failing unit tests** in `run.rs`'s test module, beside `a_saturated_decision_outbox_still_reports_what_a_transition_added`, using `segment_mark_fixture()`:

```rust
#[test] fn version_one_discards_a_new_kind_without_consuming_a_sequence()
// queue Continue("a"), ResolveCheckpoint("b"), Continue("c"); commit_decisions(Version1)
// outbox kinds == [Continue, Continue]; sequences == [1, 2]; decision_sequence() == 2; decision_drops() == 0

#[test] fn an_unformable_draft_of_a_written_kind_consumes_a_sequence_and_counts_a_drop()
// queue Continue with slot "" (invalid identity segment), then Continue("ok"); commit(Version1)
// outbox sequences == [2]; decision_drops() == 1

#[test] fn an_unformable_draft_of_an_unwritten_kind_consumes_nothing()
// queue Cancel with slot ""; commit(Version1) → decision_sequence() == 0, decision_drops() == 0

#[test] fn a_queued_draft_keeps_the_turn_and_phase_it_was_queued_under()
// queue at turn t / phase p; then begin_turn + set_phase; commit(Version2)
// event.turn == t, event.phase == p

#[test] fn pending_drafts_never_count_in_equality()
// let a = loop_state.clone(); loop_state.queue_decision(..); assert_eq!(a, loop_state)
```

Rewrite `a_saturated_decision_outbox_still_reports_what_a_transition_added` to `queue_decision` + `commit_decisions(Version1)`, and keep its assertions.

- [ ] **Step 2: Write the failing integration tests** in `tests/decision_event_vocabulary.rs`:

```rust
#[tokio::test] async fn an_unwired_run_keeps_no_decisions_in_its_record()
// the two-turn flow with no sink; the loaded state's decision_outbox() is empty and decision_sequence() == 0

#[tokio::test] async fn iterate_keeps_the_turn_and_context_it_was_decided_in()
// the two-turn flow with a sink; events[2] is the iterate Continue:
// events[2].turn == 1 && events[2].context_snapshot == events[1].context_snapshot && events[2].context_snapshot.is_some()
```

- [ ] **Step 3: Run them and confirm they fail**

Run: `cargo test -p rakka-agent --all-features --lib -- decision` and `cargo test -p rakka-agent --all-features --test decision_event_vocabulary`
Expected: compile errors for `queue_decision`, `commit_decisions` and `with_decision_event_vocabulary`.

- [ ] **Step 4: Implement the pending list**
  - A private `AgentPendingDecisions(Vec<AgentPendingDecision>)` newtype on `AgentLoopState`:
    - the field is `#[serde(skip)]`;
    - `impl PartialEq` always returns `true`, so equality is neutral, and `Default` gives the empty list;
    - `AgentPendingDecision` is a private enum: `Assembled(Box<AgentDecisionEvent>)`, or `Unformable(AgentDecisionKind)` when `assemble` failed.
  - `queue_decision` assembles at queue time, from `self.task`, goal, turn, phase, revisions, context snapshot and telemetry, exactly as `record_decision` did.
  - `commit_decisions` drains the list in order. For each entry:
    1. **skip** it if `!vocabulary.writes(kind)`;
    2. if `Unformable`, **consume** a sequence and count a drop;
    3. if an owed event has the same `operation_id`, **skip** it without consuming a sequence;
    4. otherwise **evict** the oldest owed event (counting a drop) while `outbox.len() >= AGENT_RUN_DECISION_OUTBOX_CAPACITY`; then `stamp(sequence + 1, CURRENT_AGENT_DECISION_EVENT_SCHEMA_VERSION)` and push.

- [ ] **Step 5: Implement the hook and the participant**
  - `AgentExchangeHost` calls `self.participant.seal(&record.state, &mut state, now)` on three paths:
    - in `initiate`, after `record_owed`;
    - in `accept`, immediately after `apply`, before the journal record;
    - in `settle`, after `record_owed`.
  - The other four participants keep the default.
  - `AgentRunParticipant::seal`: if `after.run` is `None`, return. If `self.decisions` is `None`, `discard_pending_decisions()`. Otherwise call `commit_decisions(vocabulary)` on `after`'s loop state. Commit needs no scope, because drafts were assembled when queued.
  - `AgentRunEntityStore` gains a field `decision_vocabulary: AgentDecisionEventVocabulary` and a private `sync_decision_participant(&mut self)`, which sets `self.host.participant_mut().set_decisions(self.decisions.as_ref().map(|_| self.decision_vocabulary))`. Both `with_decision_events` and `with_decision_event_vocabulary` call it, so builder order doesn't matter.
  - The actor and the sharding settings delegate to the store, as `with_decision_events` does. The sharding entity builder at `:9580` applies the settings' vocabulary.

- [ ] **Step 6: Migrate the four sites**
  - Each `if decision_events { run.loop_state.record_decision(..) }` becomes an unconditional `run.loop_state.queue_decision(..)`.
  - Remove the `decision_events: bool` parameter from the five functions, and `self.decisions.is_some()` at `:7451` and `:7959`.
  - The `goal-evaluation` slot is unchanged here (Task 5).

- [ ] **Step 7: Wire the test kit and the fixture**
  - The transport stores the vocabulary in a `Mutex` slot beside `decisions` and applies it where it applies the sink (`testkit.rs:1765-1771`). The delivery does the same at `:3219`.
  - `Fixture` gets a field `decision_vocabulary: AgentDecisionEventVocabulary`, default `Version1`. `with_decision_event_vocabulary` stores it and calls `run_transport.install_decision_vocabulary`. `run()`, `run_at()`, the builder at `:1344` and the pipeline delivery at `:3038` all apply it.
  - The two tests that build the unit struct now use `AgentRunParticipant::default()`.

- [ ] **Step 8: Run the decision tests and the whole crate**

Run: `cargo test -p rakka-agent --all-features > $SCRATCH/t2.log 2>&1; echo $?`
Expected: `0`. `tests/decision_events.rs` passes unchanged.

- [ ] **Step 9: Commit**: "Record run decisions through a seal hook on every exchange write path (issue #119)"

---

### Task 3: The schema version follows the vocabulary

§5.3 and §6.

**Files:**
- Modify: `crates/rakka-agent/src/schema.rs:157-164` (value 2; the doc comment states the vocabulary rule)
- Modify: `crates/rakka-agent/src/loop_runtime.rs` (`commit_decisions` stamps `vocabulary.schema_version()`)
- Modify: `crates/rakka-agent/tests/operational_query.rs:906-916`
- Modify: `docs/rakka-compatibility.md:188` and the bullet list after `:205`
- Modify: `crates/rakka-agent/tests/common/mod.rs` (the `bc0c224` mirror)
- Test: `crates/rakka-agent/tests/decision_event_vocabulary.rs`

**Interfaces:**
- Produces, in `tests/common/mod.rs`: `pub fn assert_bc0c224_decodes(events: &[AgentDecisionEvent])`, over a test-local `Bc0c224DecisionEvent` and `Bc0c224DecisionKind`. Later tasks call it.

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test] async fn version_one_stamps_one_and_version_two_stamps_two()
// the two-turn flow under each vocabulary; every event's VersionedAgentRecord::schema_version().get() == 1 (resp. 2)

#[tokio::test] async fn a_bc0c224_reader_decodes_every_version_one_event_and_no_new_kind()
// assert_bc0c224_decodes(&events) for the two-turn flow under Version1;
// for (kind, _) in NEW_KINDS: serde_json::from_value::<Bc0c224DecisionKind>(serde_json::to_value(kind)?) is Err
```

`Bc0c224DecisionEvent` mirrors the event at `47f31ec`:
- every public field with its type, plus `schema_version: u32`;
- no `deny_unknown_fields`, as the real one;
- `kind: Bc0c224DecisionKind`, with exactly the ten kebab-case variants.

`assert_bc0c224_decodes` round-trips each event through `serde_json::to_value`, then `from_value::<Bc0c224DecisionEvent>`, and asserts `schema_version == 1`.

- [ ] **Step 2: Run them and confirm they fail**

Run: `cargo test -p rakka-agent --all-features --test decision_event_vocabulary`
Expected: `version_one_stamps_one_and_version_two_stamps_two` fails (Version2 stamps 1).

- [ ] **Step 3: Implement the version rule**
  - Set the constant to 2 and stamp by vocabulary. Rakka's readers need no change: `AgentSchemaCompatibility::n_plus_one(2)` already reads 1 and 2.
  - In `operational_query.rs:906-916`, assert the stored version equals `AgentDecisionEventVocabulary::default().schema_version().get()` rather than the current one. The doctored version stays `current + 1`, which is still ahead.
  - In `docs/rakka-compatibility.md`, set the `agent-decision-event` row to `2`. Add one bullet under "The records above version 1" with:
    - the two-step staging and the rollback limit, from §6;
    - that a new kind is a wire change for readers, the A2A coordination replay included;
    - the next-bump rule from §5.3.

- [ ] **Step 4: Run the version tests, the compatibility tests and the crate**

Run: `cargo test -p rakka-agent --all-features > $SCRATCH/t3.log 2>&1; echo $?`
Expected: `0`, including `compatibility_currency` and `operational_query`.

- [ ] **Step 5: Commit**: "Stamp each decision event with its vocabulary's schema version (issue #119)"

---

### Task 4: The owed outbox's byte bound

§5.7. The value comes from measurement.

**Files:**
- Modify: `crates/rakka-agent/src/loop_runtime.rs` (the constant beside `AGENT_RUN_DECISION_OUTBOX_CAPACITY`, `:137`; eviction in `commit_decisions`)
- Modify: `crates/rakka-agent/src/lib.rs` (export)
- Modify: `crates/rakka-agent/src/run.rs:10683-10900` (the growth-reserve test)
- Test: `crates/rakka-agent/tests/decision_events.rs`

**Interfaces:**
- Produces: `pub const AGENT_RUN_DECISION_OUTBOX_MAX_BYTES: usize`, the bound on the sum of the owed events' serialized `serde_json` lengths.

- [ ] **Step 1: Extract the maximal working set** from `the_growth_reserve_covers_the_maximal_working_set` into a test helper `maximal_working_set() -> (AgentRun, usize /* baseline bytes */, AgentRunScope)`. The existing test keeps its assertion.

- [ ] **Step 2: Write the failing measurement test**

```rust
#[test] fn the_growth_reserve_covers_the_working_set_and_a_full_decision_outbox()
// let (mut run, baseline, scope) = maximal_working_set();
// fill: queue + commit(Version2), 64 times, a maximal event (below); then:
// let owed: usize = run.loop_state.decision_outbox().iter().map(|e| serde_json::to_vec(e).unwrap().len()).sum();
// assert!(owed <= AGENT_RUN_DECISION_OUTBOX_MAX_BYTES);
// assert!(run.loop_state.decision_drops() > 0, "the bound must have evicted, or this proves nothing");
// assert!(run.materialized_size_bytes() - baseline <= AGENT_RUN_STATE_GROWTH_RESERVE_BYTES);
```

A maximal event sets every field it can:
- **Identifiers:** the helper's maximal scope; slot `ck-escalate-reconcile-t{u64::MAX}-s{usize::MAX}-g{u64::MAX}`.
- **Checkpoint:** `AgentDecisionCheckpoint` from `id_for_effect(effect_id_for(&scope, u64::MAX, usize::MAX), u64::MAX gen, IndeterminateEffectReconciliation)`, with both deadlines set.
- **Effect:** the matching effect view, its target at `"tool:" + "t".repeat(AGENT_IDENTITY_MAX_LENGTH)`.
- **Outcome and status:** outcome `ConfirmedNotExecuted`, `run_status` `WaitingForReconciliation`.
- **Codes and tools:** a 64-byte reason code, and `AGENT_MODEL_MAX_TOOL_CALLS` selected tools at the maximal tool-id length.
- **Loop context:** the loop's context snapshot and telemetry at the helper's maximal values.

- [ ] **Step 3: Measure, and set the value**
  1. Temporarily print `growth` (working set only, from Step 1) and one maximal event's length:
     `cargo test -p rakka-agent --all-features --lib -- the_growth_reserve --nocapture`
  2. Let `slack = AGENT_RUN_STATE_GROWTH_RESERVE_BYTES - growth`. Set `AGENT_RUN_DECISION_OUTBOX_MAX_BYTES` to `slack` rounded down to a whole KiB, written as `N * 1024`.
  3. **Stop rule:** if that is smaller than one maximal event, stop. Report the two numbers to the owner and do not raise the reserve (§5.7).
  4. Remove the print. Write the measured numbers into the constant's doc comment, with the date and commit.

- [ ] **Step 4: Implement eviction by bytes** in `commit_decisions`, before the push:
  - Keep `owed_bytes`, computed once per commit and maintained as events are evicted and pushed.
  - An event larger than the bound on its own consumes its sequence and counts a drop.
  - Otherwise evict the oldest owed event, counting a drop, while `len >= CAPACITY || owed_bytes + size > MAX_BYTES`.

- [ ] **Step 5: Write the sink-down test** in `tests/decision_events.rs`:

```rust
#[tokio::test] async fn a_run_with_its_sink_down_completes_and_accounts_for_every_sequence()
// test-local AlwaysDownSink (append/read → Err(AgentObservabilityError::Unavailable{..}) — match the existing variant);
// the two-turn flow completes; on the loaded loop state:
// outbox.len() as u64 + decision_drops() == decision_sequence(); owed bytes <= AGENT_RUN_DECISION_OUTBOX_MAX_BYTES
```

- [ ] **Step 6: Run the crate**

Run: `cargo test -p rakka-agent --all-features > $SCRATCH/t4.log 2>&1; echo $?`
Expected: `0`.

- [ ] **Step 7: Commit**: "Bound the owed decision outbox by bytes as well as count (issue #119)"

---

### Task 5: The `goal-evaluation` slot

§5.6: an existing hole.

**Files:**
- Modify: `crates/rakka-agent/src/run.rs:6112-6121` (slot `format!("goal-evaluation-s{slot}")`; `slot` is the evaluation effect's slot, already in scope)
- Modify: `crates/rakka-agent/tests/common/mod.rs` (the shared no-hole helpers)
- Test: `crates/rakka-agent/tests/goal_evaluation.rs`

**Interfaces:**
- Produces, in `tests/common/mod.rs`, for every later task:
  - `pub struct CountingDecisionSink`: wraps `InMemoryAgentDecisionEventSink`; implements `AgentDecisionEventSink` by delegation; counts `Duplicate` answers. It has `pub fn new() -> Arc<Self>`, `pub fn events(&self, scope: &AgentRunScope) -> Vec<AgentDecisionEvent>` and `pub fn duplicates(&self) -> usize`.
  - `pub fn assert_decision_stream_whole(sink: &CountingDecisionSink, scope: &AgentRunScope)`, which asserts:
    - the sequences are exactly `1..=n`;
    - `sink.duplicates() == 0`.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test] async fn two_goal_evaluations_in_one_turn_record_two_contiguous_events()
// with CountingDecisionSink wired: commit an evaluation, report its result, flush (a settle pass),
// commit a second evaluation in the same turn, flush; then:
// events of kind Evaluate == 2 with distinct operation ids; assert_decision_stream_whole(&sink, &run_scope())
```

To commit the two evaluations, reuse the driving helpers already in `tests/goal_evaluation.rs`.

- [ ] **Step 2: Run it and confirm it fails at the old slot**

Run: `cargo test -p rakka-agent --all-features --test goal_evaluation -- two_goal_evaluations`
Expected: FAIL. The second evaluation's append answers `Duplicate`, and the sequences have a hole.

- [ ] **Step 3: Change the slot.**

- [ ] **Step 4: Run it, and the crate**

Expected: PASS; crate exit `0`.

- [ ] **Step 5: Commit**: "Give each goal evaluation its own decision identity (issue #119)"

---

### Task 6: Checkpoint opens (asks 1 and 2)

§5.1 rows 1-3 and §5.5 (ask 1 open, ask 2).

**Files:**
- Modify: `crates/rakka-agent/src/loop_runtime.rs:1073-1092` (`record_checkpoint` returns `Result<bool, AgentEffectError>`, `true` when newly recorded). Update its other callers.
- Modify: `crates/rakka-agent/src/run.rs:5449-5530` (`open_effect_checkpoint`; the new helpers below)
- Test: create `crates/rakka-agent/tests/decision_events_checkpoints.rs`; add to `tests/private_memory_promotion.rs`

**Interfaces:**
- Produces, in `run.rs`, for Task 7:
  - `fn checkpoint_draft(loop_state: &AgentLoopState, checkpoint: &AgentCheckpoint, kind: AgentDecisionKind, source: AgentDecisionSource, role: &str) -> AgentDecisionDraft`.
    - **Slot:** `"{role}-{tag}-t{turn}-s{slot}-g{generation}"`.
      - `tag` is `approval`, `authz` or `reconcile`, the tags `id_for_effect` uses.
      - `turn` and `slot` come from the bound effect held in `loop_state`.
      - If that effect is not held, the slot is `String::new()`, which becomes a counted drop, never a guessed identity.
    - **Fields:** `with_checkpoint(AgentDecisionCheckpoint::of(checkpoint))`, `with_effect(of_binding(&checkpoint.bound_effect))` and `with_safety_class(binding.safety_class)`.
  - Roles: `ck-open` here; `ck-close` and `ck-escalate` in Task 7.

- [ ] **Step 1: Write the failing tests** in `decision_events_checkpoints.rs`.
  - Copy the gated-flow helpers from `tests/checkpoint_run.rs:27-137`. Every fixture uses `CountingDecisionSink` and `Version2` unless a test says otherwise. Give `gated_policies()` an SLA with an escalation target, so deadlines are set.

```rust
#[tokio::test] async fn an_approval_checkpoint_open_records_request_approval_with_its_references()
// park(&fx); the RequestApproval event: source DeterministicPolicy; checkpoint.checkpoint_id == parked id;
// checkpoint.kind == Approval; due_at and expires_at == the snapshot's AgentCheckpointView values;
// effect == the view's bound_effect; safety_class == Some(that class); phase == the CallTools event's phase;
// assert_bc0c224_decodes on a Version1 run of the same flow; assert_decision_stream_whole after a re-driven pump

#[tokio::test] async fn an_authorization_checkpoint_open_records_request_authorization()

#[tokio::test] async fn an_unknown_outcome_on_a_live_run_records_request_reconciliation_with_its_code()
// tool outcome AgentRunEffectOutcome::indeterminate("mcp-outcome-unknown", ..) (drive it as tests/checkpoint_reconciliation.rs does);
// RequestReconciliation event: reason_code == Some("mcp-outcome-unknown"), checkpoint.kind == IndeterminateEffectReconciliation,
// checkpoint.expires_at == None; under Version1 the same flow records no RequestReconciliation and stays contiguous

#[test] fn recording_a_held_checkpoint_reports_it_was_not_new()   // run.rs unit test (Review Focus 1), using the
// existing `checkpoint(..)` test helper that run.rs:10636 already passes to record_checkpoint
// record_checkpoint(c.clone()) == Ok(true); record_checkpoint(c) == Ok(false)
```

  - Add to `tests/private_memory_promotion.rs`, beside `an_indeterminate_post_terminal_outcome_opens_no_checkpoint`. This holds existing behavior and pins the owner's ruling, so #120 has a test to change.

```rust
#[tokio::test] async fn an_indeterminate_post_terminal_outcome_records_no_decision_event()
// same flow wired with a sink and Version2: no event of kind RequestReconciliation; the sink's count is unchanged by the park
```

- [ ] **Step 2: Run them and confirm they fail**

Run: `cargo test -p rakka-agent --all-features --test decision_events_checkpoints`
Expected: FAIL, because no `RequestApproval` events are recorded.

- [ ] **Step 3: Implement the open site**
  - In `open_effect_checkpoint`, after `record_checkpoint` returns `Ok(true)`, queue one draft:
    - `checkpoint_draft(.., "ck-open")`;
    - the kind follows the checkpoint kind: `RequestApproval`, `RequestAuthorization` or `RequestReconciliation`;
    - a reconciliation open adds `with_reason_code(effect.last_error_code.as_deref().unwrap_or("indeterminate"))`.
  - On `Ok(false)`, queue nothing.
  - No other site records ask 2 (§5.5).

- [ ] **Step 4: Update the existing expectations, then run the crate**
  - Update existing tests that wire a sink and now see `request-approval` or `request-authorization` under the default vocabulary, per the Global Constraints.
  - Run: `cargo test -p rakka-agent --all-features > $SCRATCH/t6.log 2>&1; echo $?`
  - Expected: `0`.

- [ ] **Step 5: Commit**: "Record checkpoint opens and live unknown outcomes on the decision stream (issue #119, asks 1-2)"

---

### Task 7: Checkpoint closes, escalations and reconciliations (ask 1)

§5.1 rows 4-8 and §5.5 (ask 1).

**Files:**
- Modify: `crates/rakka-agent/src/loop_runtime.rs:1126-1135` (`cancel_open_checkpoints` returns `Vec<HumanCheckpointId>`, the ones it moved from waiting to `Cancelled`)
- Modify: `crates/rakka-agent/src/run.rs`:
  - `resolve_checkpoint`, `:5542-5660`;
  - `resolve_indeterminate_effect`, `:5169-5183`;
  - `drop_reconciliation_checkpoint`, `:5196`: return the retired checkpoints;
  - `cancel`, `:5416-5434`;
  - `fire_checkpoint_timers`, `:6145-6190`.
- Test: `crates/rakka-agent/tests/decision_events_checkpoints.rs`

**Interfaces:**
- Consumes: `checkpoint_draft` (Task 6).
- Produces: `fn reconcile_draft(loop_state: &AgentLoopState, effect_id: &AgentEffectId, generation: AgentEffectGeneration, checkpoint: Option<&AgentCheckpoint>, outcome: AgentDecisionOutcome) -> AgentDecisionDraft`.
  - Slot `reconcile-t{turn}-s{slot}-g{generation}`.
  - The effect view comes from the checkpoint's binding when there is one, else from `of_effect`.
  - Kind `Reconcile`, source `Human`, `with_outcome(outcome)` and `with_reason_code(outcome.as_label())`.

**Recording rules.** Queue each draft after the dedup return and before the consequence, so it carries the decision's phase, and capture the checkpoint before it is resolved or dropped.

| Where | Condition | Draft |
|---|---|---|
| `resolve_checkpoint` | `Granted` / `Denied` | `checkpoint_draft(.., ResolveCheckpoint, Human, "ck-close")`, outcome `granted`/`denied`, `reason_code` = the same label |
| `resolve_checkpoint` | `EffectResolution` | `reconcile_draft(.., Some(&checkpoint), ConfirmedExecuted / ConfirmedNotExecuted)`, by the resolution variant |
| `resolve_checkpoint` | `Compensate` / `Abandoned` | `reconcile_draft(.., Some(&checkpoint), Compensate / Abandon)` |
| `resolve_checkpoint` | `Escalated`, and the status read before `resolve` was `Open` | `checkpoint_draft(.., EscalateCheckpoint, Human, "ck-escalate")`, `reason_code` `escalated` |
| `resolve_indeterminate_effect` | always (a refusal rejects the transition) | `reconcile_draft(.., the reconciliation checkpoint bound to (effect, generation) if held, by resolution)`, queued before the core call |
| `fire_checkpoint_timers` | `Expired` | `checkpoint_draft(.., ResolveCheckpoint, DeterministicPolicy, "ck-close")`, outcome and `reason_code` `expired` |
| `fire_checkpoint_timers` | `Escalated` | `checkpoint_draft(.., EscalateCheckpoint, DeterministicPolicy, "ck-escalate")`, `reason_code` `sla-due` |
| `cancel` | each id `cancel_open_checkpoints` returned | `checkpoint_draft(.., ResolveCheckpoint, DeterministicPolicy, "ck-close")`, outcome and `reason_code` `cancelled` |

- [ ] **Step 1: Write the failing tests.** Every test ends with `assert_decision_stream_whole` after replaying its command (same operation id), running two more settle passes, and re-pumping.

```rust
#[tokio::test] async fn a_granted_and_a_denied_checkpoint_each_record_resolve_checkpoint()
// outcome Some(Granted) / Some(Denied); reason_code == the label; source Human; checkpoint id == the parked id

#[tokio::test] async fn an_expired_checkpoint_records_resolve_checkpoint_expired()
#[tokio::test] async fn an_sla_escalation_then_expiry_records_one_escalation_then_one_expiry()   // Review Focus 5
// timers fired at due_at, again before expires_at, then at expires_at:
// kinds after the open == [EscalateCheckpoint, ResolveCheckpoint]; the escalation's reason_code == "sla-due"

#[tokio::test] async fn a_persons_escalate_records_once()
// Escalate twice with distinct decision keys: one EscalateCheckpoint, reason_code "escalated", source Human

#[tokio::test] async fn a_run_cancel_closes_approval_checkpoints_and_keeps_reconciliation_ones()
// one approval + one reconciliation checkpoint open, then cancel:
// one ResolveCheckpoint(Cancelled) naming the approval checkpoint, none naming the reconciliation one

#[tokio::test] async fn each_reconciliation_outcome_records_one_reconcile()
// ConfirmedExecuted, ConfirmedNotExecuted, Compensate, Abandon via ResolveCheckpoint, and ConfirmedExecuted via
// ResolveIndeterminateEffect: each exactly one Reconcile with its outcome; the command form's checkpoint == the bound one

#[tokio::test] async fn a_deduplicated_checkpoint_decision_records_nothing()

#[tokio::test] async fn a_refused_resolution_records_nothing_and_consumes_no_sequence()   // Review Focus 2
// ResolveIndeterminateEffect for a generation that is not Indeterminate → the reply is a refusal;
// the loaded decision_sequence() and the sink's events are unchanged

#[tokio::test] async fn both_reconciliation_paths_on_one_generation_record_one_reconcile()   // Review Focus 4
// EffectResolution via the checkpoint, then ResolveIndeterminateEffect for the same generation (refused): one Reconcile
```

- [ ] **Step 2: Run them and confirm they fail**

Run: `cargo test -p rakka-agent --all-features --test decision_events_checkpoints`
Expected: the new tests fail; Task 6's still pass.

- [ ] **Step 3: Implement the rules in the table.**

- [ ] **Step 4: Update the expectations and run the crate.** Exit `0`.

- [ ] **Step 5: Commit**: "Record checkpoint resolutions, escalations and reconciliations (issue #119, ask 1)"

---

### Task 8: The run's end (ask 3)

§5.1 rows 9-10 and §5.5 (ask 3).

**Files:**
- Modify: `crates/rakka-agent/src/run.rs:1292-1328` (`terminate`)
- Modify: the existing tests that now see an end event. At least:
  - `tests/decision_events.rs:83-136` and `:185-205`: append `Complete` at sequence 5, turn 2;
  - `tests/operational_query.rs:825-838`: append `Complete`; `decision_cursor` becomes 5;
  - `tests/trace_scenarios.rs`.
- Test: create `crates/rakka-agent/tests/decision_events_run_end.rs`

**Interfaces:**
- Produces: `fn end_kind(status: AgentRunStatus) -> AgentDecisionKind`: `Completed` maps to `Complete`, `Failed` to `Fail`, `Cancelled` to `Cancel`, `HandedOff` to `Handoff`, `Superseded` to `Supersede`, and any other status to `Fail`. `reason.status()` yields only those five, which the doc comment says.

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test] async fn each_terminal_status_records_its_end()
// Completed (the two-turn flow), Failed (a denied checkpoint), Cancelled (a cancel command), HandedOff (the flow in
// tests/handoff_record.rs), Superseded (a refused proposal from a task already Completed): the last event has
// kind end_kind(status), run_status == Some(status), reason_code == Some(terminal_reason.code()), slot-derived
// operation id ending in "end"; assert_decision_stream_whole

#[tokio::test] async fn a_wind_down_re_driven_over_several_passes_records_one_end()
// cancel with a child outstanding (tests/cancellation_propagation.rs), settle three times, let the child end: one Cancel

#[tokio::test] async fn an_end_reached_through_the_accept_and_settle_paths_reaches_the_sink()
// accept: a RunCancel exchange (tests/cancellation_propagation.rs); settle: a refused proposal

#[tokio::test] async fn a_cancel_with_an_open_approval_records_its_closure_before_the_end()
// kinds after the RequestApproval == [ResolveCheckpoint(Cancelled), Cancel], in that order

#[tokio::test] async fn a_vocabulary_switch_mid_run_keeps_one_contiguous_sequence()   // Review Focus 3
// fixture under Version1 parks on an approval; then a Version2 entity (fx.run().with_decision_event_vocabulary(Version2))
// applies the cancel: the stream holds RequestApproval (from Version1) then ResolveCheckpoint + Cancel; whole
```

- [ ] **Step 2: Run them and confirm they fail.** Expected: no end events.

- [ ] **Step 3: Implement the end event.** In `terminate`, after `terminal_at` is stamped, queue:

`AgentDecisionDraft::new(end_kind(status), DeterministicPolicy, "end").with_run_status(status).with_reason_code(reason.code())`

The terminal guard makes it once per run.

- [ ] **Step 4: Update the expectations listed above and any others the run finds, then run the crate.** Exit `0`.

- [ ] **Step 5: Commit**: "Record the run's end on the decision stream (issue #119, ask 3)"

---

### Task 9: Waits (ask 4)

§5.5 (ask 4) and its table.

**Files:**
- Modify: `crates/rakka-agent/src/run.rs`:
  - `AgentRun::winding_down`, beside `:623`;
  - `settle_run_disposition`, `:5134`, which uses it;
  - `AgentRunParticipant::seal`, which derives the waits before `commit_decisions`.
- Test: create `crates/rakka-agent/tests/decision_events_waits.rs`

**Interfaces:**
- Produces:
  - `pub(crate) fn winding_down(&self) -> bool` on `AgentRun`: `terminal_reason.is_some() || status == AgentRunStatus::Cancelling`;
  - `fn entered_waits(before: &AgentRunState, after: &AgentRunState) -> Vec<AgentDecisionDraft>`.

**The derivation** (every draft: kind `Wait`, source `DeterministicPolicy`, `run_status` = `after`'s status, `reason_code` = the label):

```text
if after.run is None or after's status is terminal → []
n = after.loop_state.decision_sequence()            // read before commit_decisions: unique per entry
top-up:   after.pending_top_up = Some(p) and before's pending sequence != Some(p.sequence)
          → "budget-top-up", slot "wait-top-up-{p.sequence}"
children: after.phase == AwaitingChildren and not (before.phase == AwaitingChildren and before.turn == after.turn)
          → if after.handoff().is_some_and(|c| c.status.awaits_target())
               "awaiting-handoff", slot "wait-handoff-q{n}"
            else "awaiting-children", slot "wait-children-q{n}"
wind-down: after.winding_down() and not before.winding_down() (false when before.run is None)
          → "winding-down", slot "wait-winding-down"
```

The `q{n}` ordinal settles §9's open question without needing a proof. `n` is the run's existing `decision_sequence`, which each entry's own event advances, so a second entry in one turn can't reuse the first one's identity (§4: existing durable state only). Queue the drafts with `after`'s loop state, after the site drafts.

- [ ] **Step 1: Write the failing tests.** Each ends with `assert_decision_stream_whole`; Version1 flows also call `assert_bc0c224_decodes`.

```rust
#[tokio::test] async fn a_top_up_park_records_one_budget_wait()            // tests/escrow_ledger.rs flow
#[tokio::test] async fn a_fan_in_records_one_children_wait()               // tests/fan_out_fan_in.rs flow
#[tokio::test] async fn a_handoff_awaiting_its_target_records_one_handoff_wait()   // tests/handoff_record.rs flow
#[tokio::test] async fn a_wind_down_records_one_winding_down_wait()        // cancel with a child outstanding
#[tokio::test] async fn settle_passes_that_re_rest_the_same_wait_record_nothing()
// after each wait above, three more settle passes: the sink's count is unchanged
#[tokio::test] async fn a_wind_down_through_reconciliation_records_one_winding_down()
// scenario 57's shape (tests/effect_dispatch.rs:1451): Cancelling → WaitingForReconciliation → Cancelling → end:
// exactly one Wait("winding-down"), its run_status as entered
#[tokio::test] async fn a_cancel_that_ends_in_its_own_transition_records_the_end_and_no_wait()
```

- [ ] **Step 2: Run them and confirm they fail.** Expected: no `Wait` events.

- [ ] **Step 3: Implement `winding_down`, `entered_waits` and the seal call.**

- [ ] **Step 4: Update the expectations and run the crate.** Exit `0`.

- [ ] **Step 5: Commit**: "Record the waits a run enters on the decision stream (issue #119, ask 4)"

---

### Task 10: Ask 5, the docs, and validation

§5.5 (ask 5) and §8.

**Files:**
- Modify: `crates/rakka-agent/src/query.rs:757-761`. `next_wake` is the earliest durable checkpoint deadline. No agent run enters `WaitingForTimer`; timer wakes belong to the wake controller's epochs. Doc change only.
- Modify: `docs/rakka-agents.md`, the paragraph at `:334`. Add the lifecycle kinds, the vocabulary switch and its `Version1` default, and the outbox byte bound.
- Modify: `CHANGELOG.md`, under `## Unreleased` → `### Added`, one entry for issue #119:
  - the six kinds and their labels; `AgentDecisionOutcome`, `AgentDecisionCheckpoint` and the four optional fields;
  - `AgentDecisionEventVocabulary` and `with_decision_event_vocabulary` on the three types; schema version 2, stamped by vocabulary;
  - `AGENT_RUN_DECISION_OUTBOX_MAX_BYTES`; the `goal-evaluation` identity fix;
  - that an ended run's unknown outcome stays off the stream until #120.
  - **Source-breaking:**
    - `AgentRunParticipant` is no longer a unit struct (use `AgentRunParticipant::default()`);
    - `AgentDecisionKind` gains variants (it is `#[non_exhaustive]`);
    - `AgentExchangeParticipant` gains a provided method.

- [ ] **Step 1: Make the doc changes.**

- [ ] **Step 2: Run the example whose stdout the README pins**

Run: `cargo run -p rakka-example-durable-agent-acceptance > $SCRATCH/accept.log 2>&1; echo $?`
Expected: `0`, and the output equals the block in `examples/durable-agent-acceptance/README.md`.

- [ ] **Step 3: Full validation**

Run: `scripts/validate.sh > $SCRATCH/validate.log 2>&1; echo $?`
Expected: `0`. If the one-shot workspace test step is killed under load, run it per crate and say so.

- [ ] **Step 4: Commit**: "Document the run lifecycle decision events (issue #119, ask 5)"

---

## After the plan

- Open the PR against `main`. The description names the trace segments that now carry the new decision span events (§7). `segment_additions` picks up every new sequence, so the segment tests that changed in Tasks 6-9 are the list.
- After it merges, post the §10 report on #119:
  - each ask's commit;
  - the kinds, labels and fields;
  - the ring decision, with the measured byte bound;
  - what `bc0c224` reads, the two-step staging and the rollback limit;
  - the §2 differences;
  - the knob `with_decision_event_vocabulary(AgentDecisionEventVocabulary::Version2)`.

## Corrections to the spec found while planning

1. **§7 says `tests/decision_events.rs` "passes unchanged except for the goal-evaluation slot". It doesn't.** Its flow ends `Completed`, so it gains a `complete` event at sequence 5, turn 2. The same goes for `operational_query.rs` and `trace_scenarios.rs`. Their existing events keep their positions, which is the property §5.4 asks for. Task 8 appends the new event.
2. **The `AwaitingChildren` wait question in §9 is settled here, not left to the builder.** Its slots carry the run's existing `decision_sequence` as an ordinal (Task 9), so no proof that a turn can't re-enter the wait is needed.
3. **Two more drivers wire the decision sink:** `InProcessRunEntityTransport` and `InProcessRunResultDelivery` in the test kit. They and the test `Fixture` get the vocabulary knob too (Task 2). §5.3 named only the three public types.
