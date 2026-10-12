# Decision Ledger Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** a replay of a committed decision (a checkpoint or indeterminate-effect resolution on a run; creation, cancellation, or any revision-fenced goal, wake-lifecycle, or schedule command on a task) answers `Duplicate` with its original outcome after the 64-entry operation window has moved past its id.

**Architecture:** one generic bounded log, `AgentDecisionLedger<O>` (capacity 32), lives beside each entity's operation log in `AgentRunState` and `AgentTaskState`.
- **Write side:** each command enum gains `records_decision()`. `apply_command` (run) and `apply_once` (task) compute it once and pass it to their `transition` helper, which records the id into the ledger in the same compare-and-set that records it in the operation log.
- **Read side:** the dedup lookup consults the operation log, then the ledger, before every other guard.
- **Durable shape:** the field is serde-defaulted and omitted while empty, so no schema version moves.

**Tech Stack:** Rust (MSRV 1.88), serde, tokio tests, the `rakka-agent` integration-test fixtures in `crates/rakka-agent/tests/common/mod.rs`.

**Spec:** `docs/superpowers/specs/2026-10-12-decision-ledger-design.md`. Read it first, including its "Amendments from planning" section.

## Global Constraints

- **Capacity:** `AGENT_DECISION_LEDGER_CAPACITY: usize = 32`, exported from the crate root.
- **Run decisions:** exactly `ResolveCheckpoint` and `ResolveIndeterminateEffect`.
- **Task decisions:** exactly `Create`, `Cancel`, `ActivateGoal`, `RecordGoalDecision`, `ResumeGoal`, `ReviseGoalCriteria`, `SuspendContinuousGoal`, `ResumeContinuousGoal`, `RenewContinuousGoal`, `RetireContinuousGoal` and `UpdateContinuousSchedule`.
- **Lookup order:** operation window, then ledger, then every other guard, the terminal one included. A hit answers `Duplicate { outcome }` with the outcome the decision's transition recorded.
- **Durable compatibility:** no schema version moves. The new field is `#[serde(default, skip_serializing_if = "AgentDecisionLedger::is_empty")]`.
- **Lints:** workspace lints are strict (`missing_docs`, `clippy -D warnings`, `unsafe_code = "forbid"`). Every public item needs a doc comment.
- **Machine rule:** run `cargo clean` at every task boundary, never in the middle of one. A task's first build is therefore a full rebuild of `rakka-agent`, which takes 10–15 minutes.
- **Build output:** run focused tests per test file, with `--all-features`. Redirect long builds to a file and read its tail, never pipe into `tail`. The workspace-wide `scripts/validate.sh` runs only in Task 6.
- **Commits:** end every commit message with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **A replay under a decision's id with a different body.** For example, a Deny re-posted under the approval's id. It answers `Duplicate` with the *original* outcome, exactly as an in-window replay does (the window keys on the id alone). Pinned in Task 2, test `a_different_body_under_a_decided_id_answers_the_original_outcome`.
2. **A run or task that has never decided.** Its stored encoding has no `decision_ledger` key, so golden encodings and older peers see byte-identical records. Pinned in Task 2, as the first assertion in `a_replayed_approval_answers_duplicate_after_the_window_has_moved_past_it`.
3. **A record written before the ledger existed (no key at all).** It decodes and answers today's refusal. Pinned in Task 2 (`run-unknown-checkpoint`) and Task 3 (`task-already-created`) via `forget_operations(.., true)`.
4. **Non-decision traffic filling the run's window.** It never touches the ledger. Pinned in Task 2: after the approved run completes, `decision_ledger().len() == 1`.
5. **A resident facade with a stale cache.** It answers from the ledger after its one re-read. Pinned in Task 2, test `a_resident_facade_answers_a_replay_from_the_ledger_after_its_one_re_read`.

---

### Task 1: The `AgentDecisionLedger` type

**Files:**
- Create: `crates/rakka-agent/src/decision_ledger.rs`
- Modify: `crates/rakka-agent/src/lib.rs`. Add the module declaration at line ~61, between `pub mod coordination;` and `pub mod definition;`, and add the re-export before `pub use dispatch::{` (line ~151).

**Interfaces:**
- Produces:
  - `pub const AGENT_DECISION_LEDGER_CAPACITY: usize`;
  - `pub struct AgentDecisionLedger<O>`, which implements `Default`, `Clone`, `Debug`, `PartialEq`/`Eq` (when `O` does), `Serialize` and `Deserialize` (serde-transparent JSON array of `{"operation_id", "outcome"}`);
  - `pub fn outcome(&self, &AgentOperationId) -> Option<&O>`;
  - `pub fn len(&self) -> usize`;
  - `pub fn is_empty(&self) -> bool`;
  - `pub(crate) fn record(&mut self, AgentOperationId, O)`.

- [ ] **Step 1: Write the module with its failing tests**

Create `crates/rakka-agent/src/decision_ledger.rs`. The tests reference `record` and `outcome`, so first write the type with `todo!()` bodies for `outcome` and `record` so that it compiles:

```rust
//! The decision ledger: the answer a decision's replay gets once its
//! operation id has left the entity's operation window.
//!
//! A run and a task each remember their last 64 operation ids, and a re-post
//! inside that window is answered `Duplicate`. Ordinary traffic fills the
//! window — every effect result is one entry — so a client that lost the
//! answer to a committed decision and re-posts it later used to meet the
//! refusal a first attempt would meet, and could not tell "it applied" from
//! "it was refused". The ledger is a second, smaller log that only decisions
//! write: a checkpoint resolution, a task's creation or cancellation, a goal
//! or wake-lifecycle transition. The entity consults it right after its
//! window misses and before every other guard, so a decision's replay
//! converges on its original outcome after any number of non-decision
//! operations, and up to 31 later decisions.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use crate::identity::AgentOperationId;

/// How many decisions a run or a task remembers past its operation window.
pub const AGENT_DECISION_LEDGER_CAPACITY: usize = 32;

/// Bounded log of the decisions an entity committed, and the outcome each
/// produced, oldest first.
///
/// It encodes exactly as an entity's operation log does — a JSON array of
/// `{"operation_id", "outcome"}` — and an entity omits it while it is empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentDecisionLedger<O> {
    entries: VecDeque<AgentDecisionLedgerEntry<O>>,
}

impl<O> Default for AgentDecisionLedger<O> {
    fn default() -> Self {
        Self {
            entries: VecDeque::new(),
        }
    }
}

impl<O> AgentDecisionLedger<O> {
    /// The outcome the decision committed under `operation_id` produced, while
    /// the ledger still holds it.
    #[must_use]
    pub fn outcome(&self, operation_id: &AgentOperationId) -> Option<&O> {
        todo!()
    }

    /// How many decisions are remembered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no decision is remembered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remembers one committed decision, evicting the oldest past
    /// [`AGENT_DECISION_LEDGER_CAPACITY`].
    pub(crate) fn record(&mut self, operation_id: AgentOperationId, outcome: O) {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AgentDecisionLedgerEntry<O> {
    operation_id: AgentOperationId,
    outcome: O,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::AgentOperationKind;

    fn id(discriminator: &str) -> AgentOperationId {
        AgentOperationId::new(AgentOperationKind::Command, ["acme", "ledger", discriminator])
            .expect("the operation id derives")
    }

    #[test]
    fn a_recorded_decision_answers_its_outcome_and_an_unknown_one_answers_nothing() {
        let mut ledger = AgentDecisionLedger::default();
        ledger.record(id("approve"), 7_u32);
        assert_eq!(ledger.outcome(&id("approve")), Some(&7));
        assert_eq!(ledger.outcome(&id("deny")), None);
        assert_eq!(ledger.len(), 1);
        assert!(!ledger.is_empty());
    }

    #[test]
    fn the_ledger_keeps_the_newest_decisions_up_to_its_capacity() {
        let mut ledger = AgentDecisionLedger::default();
        for index in 0..=AGENT_DECISION_LEDGER_CAPACITY {
            ledger.record(id(&index.to_string()), index);
        }
        assert_eq!(ledger.len(), AGENT_DECISION_LEDGER_CAPACITY);
        assert_eq!(ledger.outcome(&id("0")), None, "the oldest decision is evicted");
        assert_eq!(ledger.outcome(&id("1")), Some(&1));
        let newest = AGENT_DECISION_LEDGER_CAPACITY;
        assert_eq!(ledger.outcome(&id(&newest.to_string())), Some(&newest));
    }

    #[test]
    fn the_ledger_encodes_as_an_operation_log_does() {
        let mut ledger = AgentDecisionLedger::default();
        ledger.record(id("approve"), 7_u32);
        let encoded = serde_json::to_value(&ledger).expect("the ledger encodes");
        assert_eq!(
            encoded,
            serde_json::json!([{
                "operation_id": serde_json::to_value(id("approve")).expect("the id encodes"),
                "outcome": 7,
            }])
        );
        let decoded: AgentDecisionLedger<u32> =
            serde_json::from_value(encoded).expect("the ledger decodes");
        assert_eq!(decoded, ledger);
        assert_eq!(
            serde_json::to_value(AgentDecisionLedger::<u32>::default()).expect("encodes"),
            serde_json::json!([])
        );
    }
}
```

In `crates/rakka-agent/src/lib.rs`, add `pub mod decision_ledger;` between `pub mod coordination;` and `pub mod definition;`. (`crates/rakka-agent/tests/crate_shape.rs` requires every module file to be declared `pub mod`.) Before `pub use dispatch::{`, add:

```rust
pub use decision_ledger::{AgentDecisionLedger, AGENT_DECISION_LEDGER_CAPACITY};
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent --all-features --lib decision_ledger > /tmp/t1.log 2>&1; tail -20 /tmp/t1.log`

Expected: the first two tests FAIL with a `not yet implemented` panic. `the_ledger_encodes_as_an_operation_log_does` also panics, because it calls `record`.

- [ ] **Step 3: Implement `outcome` and `record`**

Replace the two `todo!()` bodies:

```rust
    pub fn outcome(&self, operation_id: &AgentOperationId) -> Option<&O> {
        self.entries
            .iter()
            .find(|entry| &entry.operation_id == operation_id)
            .map(|entry| &entry.outcome)
    }
```

```rust
    pub(crate) fn record(&mut self, operation_id: AgentOperationId, outcome: O) {
        self.entries.push_back(AgentDecisionLedgerEntry {
            operation_id,
            outcome,
        });
        while self.entries.len() > AGENT_DECISION_LEDGER_CAPACITY {
            self.entries.pop_front();
        }
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p rakka-agent --all-features --lib decision_ledger > /tmp/t1.log 2>&1; tail -8 /tmp/t1.log`

Expected: `test result: ok. 3 passed`.

`record` is `pub(crate)` and has no caller outside the tests until Task 2. If clippy flags it as dead code, add `#[allow(dead_code)]` on `record` with the comment `// The run and task entities record into it from Task 2 on.`, and remove that allowance in Task 2. Check with `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings > /tmp/c1.log 2>&1; tail -5 /tmp/c1.log`.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent/src/decision_ledger.rs crates/rakka-agent/src/lib.rs
git commit -m "Add AgentDecisionLedger, a bounded log of decisions kept past an entity's operation window

Issue #96, ask 2.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Then `cargo clean`.

---

### Task 2: The run records and answers its decisions

**Files:**
- Modify: `crates/rakka-agent/src/run.rs`:
  - the `AGENT_RUN_OPERATION_LOG_CAPACITY` doc at ~143;
  - `AgentRunState` at ~820-900;
  - `apply_command` at ~6828-6860 and its 10 `.transition(now, move |state|` call sites;
  - `transition` at ~8222-8265;
  - `impl AgentRunEntityCommand` (the `operation_id()` method at ~8520).
- Modify: `crates/rakka-agent/tests/common/mod.rs`, to add `forget_operations`.
- Create: `crates/rakka-agent/tests/run_decision_ledger.rs`.

**Interfaces:**
- Consumes: `AgentDecisionLedger<O>` and its `record` / `outcome` / `len` / `is_empty` (Task 1).
- Produces:
  - `AgentRunEntityCommand::records_decision(&self) -> bool`;
  - `AgentRunState::decision_ledger(&self) -> &AgentDecisionLedger<AgentRunOutcome>`;
  - the test helper `forget_operations<S, Store>(store: &Store, id: &PersistenceId, forget_ledger: bool)` in `tests/common/mod.rs`, which Tasks 3–5 reuse.

- [ ] **Step 1: Add the test helper**

Append to `crates/rakka-agent/tests/common/mod.rs`. The file already has `#![allow(dead_code)]`.

```rust
/// Rewrites one durable entity record as if its operation window had moved
/// past every id it holds — exactly the state 64 later operations leave — and,
/// when `forget_ledger` is set, as if it had been written before the decision
/// ledger existed (which is also what a decision evicted from the ledger
/// leaves for its id). The same record surgery `rakka-a2a`'s collaboration
/// surface uses to age a task's window, rather than 64 scripted operations.
pub async fn forget_operations<S, Store>(
    store: &Store,
    id: &rakka_persistence::PersistenceId,
    forget_ledger: bool,
) where
    S: serde::Serialize + serde::de::DeserializeOwned + rakka_persistence::DurableState,
    Store: rakka_persistence::DurableStateStore<S>,
{
    let record = store
        .load(id)
        .await
        .expect("the store loads")
        .expect("the record exists");
    let mut encoded = serde_json::to_value(&record.state).expect("the state encodes");
    let fields = encoded
        .as_object_mut()
        .expect("an entity state encodes as an object");
    assert!(
        fields.contains_key("applied_operations"),
        "the state has an operation log to forget"
    );
    fields.insert("applied_operations".to_string(), serde_json::json!([]));
    if forget_ledger {
        fields.remove("decision_ledger");
    }
    let state: S = serde_json::from_value(encoded).expect("the rewritten state decodes");
    store
        .compare_and_set(id, record.revision, state)
        .await
        .expect("the rewrite lands");
}
```

- [ ] **Step 2: Write the failing tests**

Create `crates/rakka-agent/tests/run_decision_ledger.rs`:

```rust
//! A run's decisions answer a replay `Duplicate` after the operation window
//! has moved past them (issue #96, ask 2).
//!
//! The run remembers its last 64 operation ids, and ordinary traffic — every
//! effect result — fills that window. A checkpoint resolution or an
//! indeterminate effect's resolution is also recorded in the run's decision
//! ledger, which only decisions write, so a client that lost the answer to a
//! committed decision and re-posts it later is answered with the original
//! outcome rather than the refusal a first attempt would meet.
//!
//! Each test commits a decision, then rewrites the stored record as if 64
//! later operations had evicted its id ([`forget_operations`]).

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    AgentApprovalDecision, AgentCheckpointDecision, AgentDispatchWindow, AgentEffectPolicies,
    AgentEffectResolution, AgentEffectSpec, AgentModelTurn, AgentOperationId, AgentOperationKind,
    AgentRunEffectStatus, AgentRunEntityCommand, AgentRunEntityReply, AgentRunOutcome,
    AgentRunStatus, AgentTaskContent, AgentToolAuthority, AgentToolCallId, AgentToolCallRequest,
    AgentToolId, InMemoryAgentRunEffectSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentTimestampMillis, HumanCheckpointId, PrincipalRef};

mod common;

use common::*;

const TOOL: &str = "charge-card";

fn tool_call() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me charge that.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id is valid"),
                AgentToolId::new(TOOL).expect("the tool id is valid"),
                serde_json::json!({ "amount": 42 }),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposal() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// A run whose one tool needs an approval before it dispatches.
fn gated() -> Fixture {
    let policies = AgentEffectPolicies::new()
        .with_tool_spec(
            AgentToolId::new(TOOL).expect("the tool id is valid"),
            AgentEffectSpec::non_idempotent().with_checkpoint_required(),
        )
        .expect("the checkpoint-required tool spec is valid");
    Fixture::with_sink(
        ScriptedDispatcher::new()
            .with_turn(tool_call())
            .with_turn(proposal())
            .with_tool_result(
                TOOL,
                AgentTaskContent::inline(serde_json::json!({ "charged": true }))
                    .expect("the tool result is inline-bounded"),
            ),
        InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(AtomicU64::new(1)),
    )
}

fn decision_key(discriminator: &str) -> AgentOperationId {
    AgentOperationId::for_agent(
        AgentOperationKind::CheckpointResolution,
        &agent_scope(),
        discriminator,
    )
    .expect("the decision key derives")
}

fn resolve(
    checkpoint_id: &HumanCheckpointId,
    key: &str,
    decision: AgentCheckpointDecision,
) -> AgentRunEntityCommand {
    AgentRunEntityCommand::ResolveCheckpoint {
        operation_id: decision_key(key),
        checkpoint_id: checkpoint_id.clone(),
        resolver: PrincipalRef {
            principal_type: "user".to_string(),
            principal_id: "approver".to_string(),
            display_name: None,
        },
        decision: Box::new(decision),
        telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
    }
}

fn approve() -> AgentCheckpointDecision {
    AgentCheckpointDecision::Approval(AgentApprovalDecision::Approve {
        credential_binding: None,
        expires_at: AgentTimestampMillis::new(1_000_000),
        allowed_use_count: 1,
    })
}

fn deny() -> AgentCheckpointDecision {
    AgentCheckpointDecision::Approval(AgentApprovalDecision::Deny {
        reason: "not this time".to_string(),
    })
}

/// The id of the one checkpoint the run is parked on, read from durable state.
async fn open_checkpoint(fx: &Fixture) -> HumanCheckpointId {
    let mut store = fx.run();
    store.recover(fx.now()).await.expect("the run recovers");
    let open = store
        .state()
        .expect("state reads")
        .loop_state()
        .expect("the loop exists")
        .open_checkpoints()
        .to_vec();
    assert_eq!(open.len(), 1, "exactly one checkpoint gates the tool");
    open[0].checkpoint_id.clone()
}

async fn park(fx: &Fixture) -> HumanCheckpointId {
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop parks on the checkpoint");
    open_checkpoint(fx).await
}

/// Applies `command` through a fresh facade and returns the outcome it
/// committed.
async fn apply_fresh(fx: &Fixture, command: AgentRunEntityCommand) -> AgentRunOutcome {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    match run
        .apply(command, &fx.router, fx.now())
        .await
        .expect("the command applies")
    {
        AgentRunEntityReply::Applied { outcome } => outcome,
        other => panic!("expected the command to apply, got {other:?}"),
    }
}

async fn replay_fresh(
    fx: &Fixture,
    command: AgentRunEntityCommand,
) -> rakka_agent::AgentRunResult<AgentRunEntityReply> {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(command, &fx.router, fx.now()).await
}

#[tokio::test]
async fn a_replayed_approval_answers_duplicate_after_the_window_has_moved_past_it() {
    let fx = gated();
    let checkpoint_id = park(&fx).await;
    let undecided = {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("the run recovers");
        serde_json::to_value(run.state().expect("state reads")).expect("the state encodes")
    };
    assert!(
        undecided.get("decision_ledger").is_none(),
        "a run that has decided nothing encodes exactly as before the ledger"
    );

    let approval = resolve(&checkpoint_id, "approve", approve());
    assert!(approval.records_decision());
    let outcome = apply_fresh(&fx, approval.clone()).await;
    fx.pump().await.expect("the approved run completes");
    forget_operations(&fx.runs, &run_scope().persistence_id(), false).await;

    let replay = replay_fresh(&fx, approval).await.expect("the replay answers");
    assert_eq!(replay, AgentRunEntityReply::Duplicate { outcome });

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let state = run.state().expect("state reads");
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));
    assert_eq!(
        state.decision_ledger().len(),
        1,
        "the effect results the run went on to record are not decisions"
    );
}

#[tokio::test]
async fn a_replayed_denial_that_ended_the_run_answers_duplicate_rather_than_run_terminal() {
    let fx = gated();
    let checkpoint_id = park(&fx).await;
    let denial = resolve(&checkpoint_id, "deny", deny());
    let outcome = apply_fresh(&fx, denial.clone()).await;
    fx.pump().await.expect("the denied run winds down");
    assert_eq!(
        fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Failed
    );
    forget_operations(&fx.runs, &run_scope().persistence_id(), false).await;

    let replay = replay_fresh(&fx, denial).await.expect("the replay answers");
    assert_eq!(replay, AgentRunEntityReply::Duplicate { outcome });
}

#[tokio::test]
async fn a_different_body_under_a_decided_id_answers_the_original_outcome() {
    // The window keys on the id alone, and so does the ledger: a Deny
    // re-posted under the approval's id is that approval's replay.
    let fx = gated();
    let checkpoint_id = park(&fx).await;
    let outcome = apply_fresh(&fx, resolve(&checkpoint_id, "approve", approve())).await;
    forget_operations(&fx.runs, &run_scope().persistence_id(), false).await;

    let replay = replay_fresh(&fx, resolve(&checkpoint_id, "approve", deny()))
        .await
        .expect("the replay answers");
    assert_eq!(replay, AgentRunEntityReply::Duplicate { outcome });
}

#[tokio::test]
async fn a_decision_the_ledger_no_longer_holds_meets_the_refusal_a_first_attempt_would() {
    // A record written before the ledger existed, or one whose decision 32
    // later decisions evicted: the replay is refused exactly as before.
    let fx = gated();
    let checkpoint_id = park(&fx).await;
    let approval = resolve(&checkpoint_id, "approve", approve());
    apply_fresh(&fx, approval.clone()).await;
    forget_operations(&fx.runs, &run_scope().persistence_id(), true).await;

    let refusal = replay_fresh(&fx, approval)
        .await
        .expect_err("the replay is refused");
    assert_eq!(refusal.code(), "run-unknown-checkpoint");
}

#[tokio::test]
async fn a_resident_facade_answers_a_replay_from_the_ledger_after_its_one_re_read() {
    // The resident facade cached the run before it parked, so its cache has
    // never seen the checkpoint: it refuses the replay, re-reads once, and the
    // durable record's ledger answers.
    let fx = gated();
    fx.instantiate_agent().await;
    fx.create_task().await;
    let mut resident = fx.run();
    resident.recover(fx.now()).await.expect("the run recovers");

    fx.pump().await.expect("the loop parks on the checkpoint");
    let checkpoint_id = open_checkpoint(&fx).await;
    let approval = resolve(&checkpoint_id, "approve", approve());
    let outcome = apply_fresh(&fx, approval.clone()).await;
    forget_operations(&fx.runs, &run_scope().persistence_id(), false).await;

    let replay = resident
        .apply(approval, &fx.router, fx.now())
        .await
        .expect("the replay answers");
    assert_eq!(replay, AgentRunEntityReply::Duplicate { outcome });
}

#[tokio::test]
async fn a_replayed_indeterminate_resolution_answers_duplicate_after_the_window_has_moved_past_it() {
    let spec = AgentEffectSpec::non_idempotent();
    let fx = AuthorityFixture::new(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_call())
            .with_turn_for(2, proposal()),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, &spec)),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    // The worker dies after invoking, so the attempt is ambiguous and the
    // non-idempotent generation parks for reconciliation.
    fx.probe.arm(AgentDispatchWindow::AfterInvocation);
    let _died = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    fx.expire_lease();
    fx.pump().await;
    let effect = fx.effect_at(1).await.expect("the tool effect is committed");
    assert_eq!(effect.status, AgentRunEffectStatus::Indeterminate);

    let resolution = AgentRunEntityCommand::ResolveIndeterminateEffect {
        operation_id: decision_key("reconcile"),
        effect_id: effect.effect_id.clone(),
        generation: effect.generation,
        resolution: Box::new(AgentEffectResolution::ConfirmedNotExecuted),
        telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
    };
    assert!(resolution.records_decision());
    let outcome = {
        let mut run = fx.fx.run();
        run.recover(fx.fx.now()).await.expect("the run recovers");
        match run
            .apply(resolution.clone(), &fx.fx.router, fx.fx.now())
            .await
            .expect("the resolution applies")
        {
            AgentRunEntityReply::Applied { outcome } => outcome,
            other => panic!("expected the resolution to apply, got {other:?}"),
        }
    };
    fx.pump().await;
    forget_operations(&fx.fx.runs, &run_scope().persistence_id(), false).await;

    let mut run = fx.fx.run();
    run.recover(fx.fx.now()).await.expect("the run recovers");
    let replay = run
        .apply(resolution, &fx.fx.router, fx.fx.now())
        .await
        .expect("the replay answers");
    assert_eq!(replay, AgentRunEntityReply::Duplicate { outcome });
}

#[test]
fn only_resolutions_are_run_decisions() {
    assert!(!AgentRunEntityCommand::Describe.records_decision());
    assert!(!AgentRunEntityCommand::FireCheckpointTimers {
        operation_id: decision_key("timers"),
    }
    .records_decision());
    assert!(!AgentRunEntityCommand::FireFanInDeadline {
        operation_id: decision_key("deadline"),
    }
    .records_decision());
}
```

This file references `records_decision()` and `decision_ledger()`, which do not exist yet. Add both as stubs so that the tests compile and fail on behaviour.
- **`impl AgentRunEntityCommand`** (beside `operation_id()`, ~8520):

```rust
    /// Whether the command is a decision the run remembers past its operation
    /// window ([`AgentRunState::decision_ledger`]).
    #[must_use]
    pub const fn records_decision(&self) -> bool {
        false
    }
```

- **`impl AgentRunState`** (beside `applied_operations()`, ~882): first add the field, its initializer in `unassigned()`, and the `use crate::decision_ledger::AgentDecisionLedger;` import, all as in Step 4. Then add the accessor:

```rust
    /// The decisions the run committed, which answer a replay `Duplicate`
    /// after the operation window has moved past it.
    #[must_use]
    pub const fn decision_ledger(&self) -> &AgentDecisionLedger<AgentRunOutcome> {
        &self.decision_ledger
    }
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent --all-features --test run_decision_ledger > /tmp/t2.log 2>&1; grep -E "^test |test result|panicked" -A3 /tmp/t2.log | head -60`

Expected:

| Test | Expected result |
| --- | --- |
| `a_replayed_approval_…` | FAIL: the replay is `Err(… Terminal …)` (`run-terminal`) |
| `a_replayed_denial_…` | FAIL: `run-terminal` |
| `a_different_body_…` | FAIL: `run-unknown-checkpoint` |
| `a_resident_facade_…` | FAIL: `run-unknown-checkpoint` |
| `a_replayed_indeterminate_…` | FAIL: `run-terminal` |
| `only_resolutions_are_run_decisions` | PASS |
| `a_decision_the_ledger_no_longer_holds_…` | PASS. It pins that the fallback is today's refusal. |

The `records_decision()` assertions in the approval and indeterminate tests FAIL first, with `assertion failed: approval.records_decision()`. Either failure is the expected RED.

- [ ] **Step 4: Implement**

In `crates/rakka-agent/src/run.rs`:

1. **Import:** add `use crate::decision_ledger::AgentDecisionLedger;` beside the other `crate::` imports.

2. **Field on `AgentRunState`**, right after `applied_operations: AgentRunOperationLog,`:

```rust
    /// The decisions the run committed, kept past the operation window. A
    /// record written before the ledger existed decodes with an empty one, and
    /// an empty one is omitted, so a run that never decided encodes as before.
    #[serde(default, skip_serializing_if = "AgentDecisionLedger::is_empty")]
    decision_ledger: AgentDecisionLedger<AgentRunOutcome>,
```

   In `AgentRunState::unassigned`, add `decision_ledger: AgentDecisionLedger::default(),` after `applied_operations`. Then compile. Any other `AgentRunState { .. }` struct literal in the crate will fail to compile; add the same initializer to each one.

3. **Lookup helper on `AgentRunState`**, private, beside `outcome()`:

```rust
    /// The outcome an already-applied operation produced: from the window,
    /// or, once the window has moved past a decision, from the decision
    /// ledger.
    fn replayed_outcome(&self, operation_id: &AgentOperationId) -> Option<&AgentRunOutcome> {
        self.applied_operations
            .outcome(operation_id)
            .or_else(|| self.decision_ledger.outcome(operation_id))
    }
```

4. **`apply_command`'s dedup** (~6834): replace

```rust
            if let Some(outcome) = self
                .state()?
                .applied_operations
                .outcome(operation_id)
                .cloned()
            {
```

   with

```rust
            // The window answers a replay, and past it the decision ledger
            // answers a decision's — before every guard below, the terminal
            // one included, so a replayed Deny that ended the run converges
            // rather than being refused `run-terminal`.
            if let Some(outcome) = self.state()?.replayed_outcome(operation_id).cloned() {
```

5. **`records_decision`**: replace the stub body with:

```rust
    /// Whether the command is a decision the run remembers past its operation
    /// window ([`AgentRunState::decision_ledger`]): a checkpoint resolution or
    /// an indeterminate effect's resolution. A replay of one answers
    /// [`AgentRunEntityReply::Duplicate`] with its original outcome after any
    /// number of later non-decision operations, and up to
    /// [`crate::AGENT_DECISION_LEDGER_CAPACITY`] `- 1` later decisions.
    #[must_use]
    pub const fn records_decision(&self) -> bool {
        matches!(
            self,
            Self::ResolveCheckpoint { .. } | Self::ResolveIndeterminateEffect { .. }
        )
    }
```

6. **`transition`** takes the decision flag. Change the signature to:

```rust
    async fn transition<F>(
        &mut self,
        now: AgentTimestampMillis,
        decision: bool,
        transition: F,
    ) -> AgentRunResult<AgentRunEntityReply>
```

   In its `apply` closure, replace

```rust
                        let operation_id = transition(state)?;
                        let result = state.outcome();
                        state
                            .applied_operations
                            .record(operation_id, result.clone());
```

   with

```rust
                        let operation_id = transition(state)?;
                        let result = state.outcome();
                        if decision {
                            state
                                .decision_ledger
                                .record(operation_id.clone(), result.clone());
                        }
                        state
                            .applied_operations
                            .record(operation_id, result.clone());
```

   Add to its doc comment: `A decision ([`AgentRunEntityCommand::records_decision`]) is also recorded in the decision ledger, in the same compare-and-set.`

7. **Thread the flag.** In `apply_command`, right before `let reply = match command {`, add:

```rust
        let decision = command.records_decision();
```

   Then rewrite every call site:

```bash
perl -pi -e 's/\.transition\(now, move \|state\|/.transition(now, decision, move |state|/' crates/rakka-agent/src/run.rs
grep -c '\.transition(now, decision, move |state|' crates/rakka-agent/src/run.rs   # expect 10
grep -c '\.transition(now, move |state|' crates/rakka-agent/src/run.rs             # expect 0
```

8. **Window doc** on `AGENT_RUN_OPERATION_LOG_CAPACITY` (~143): append the sentence `A decision ([`AgentRunEntityCommand::records_decision`]) is answered past the window from the run's decision ledger instead, with its original outcome.`

- [ ] **Step 5: Run the tests to verify they pass**

```
cargo fmt -p rakka-agent
cargo test -p rakka-agent --all-features --test run_decision_ledger > /tmp/t2.log 2>&1; grep -E "test result|panicked" -A3 /tmp/t2.log
```

Expected: `test result: ok. 7 passed`.

Then run the run-entity suites that exercise the changed paths:

```
cargo test -p rakka-agent --all-features --test checkpoint_run --test checkpoint_reconciliation --test effect_dispatch --test command_rematerialization > /tmp/t2b.log 2>&1; grep -E "test result|FAILED|panicked" /tmp/t2b.log
```

Expected: every `test result: ok`.

Finally, if Task 1 added a `#[allow(dead_code)]` to `record`, remove it now and check clippy: `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings > /tmp/c2.log 2>&1; tail -5 /tmp/c2.log`.

- [ ] **Step 6: Commit**

```bash
git add crates/rakka-agent/src/run.rs crates/rakka-agent/tests/common/mod.rs crates/rakka-agent/tests/run_decision_ledger.rs
git commit -m "Answer a run decision's replay from the decision ledger past the operation window

ResolveCheckpoint and ResolveIndeterminateEffect are recorded in the run's
decision ledger in the same compare-and-set as the operation log, and a
replay whose id the window no longer holds is answered Duplicate from the
ledger before every guard, the terminal one included.

Issue #96, ask 2.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Then `cargo clean`.

---

### Task 3: The task records and answers creation and cancellation

**Files:**
- Modify: `crates/rakka-agent/src/task.rs`:
  - the `AGENT_TASK_OPERATION_LOG_CAPACITY` doc at ~250;
  - `AgentTaskState` at ~4465-4530;
  - `apply_once` at ~11910-11930 and its 17 `.transition(now, …, move |state|` call sites;
  - `transition` at ~13382-13430;
  - `impl AgentTaskEntityCommand` (its `operation_id()` method).
- Create: `crates/rakka-agent/tests/task_decision_ledger.rs`.

**Interfaces:**
- Consumes: `AgentDecisionLedger` (Task 1) and `forget_operations` (Task 2, `tests/common/mod.rs`).
- Produces:
  - `AgentTaskEntityCommand::records_decision(&self) -> bool`, true for `Create` and `Cancel` in this task (Tasks 4–5 widen it);
  - `AgentTaskState::decision_ledger(&self) -> &AgentDecisionLedger<AgentTaskOutcome>`;
  - the test-file helpers `applied`, `forget_window`, `assert_replay_converges` and `ledger_len`, which Tasks 4–5 extend.

- [ ] **Step 1: Write the failing tests**

Create `crates/rakka-agent/tests/task_decision_ledger.rs`:

```rust
//! A task's decisions answer a replay `Duplicate` after the operation window
//! has moved past them (issue #96, ask 2).
//!
//! The task remembers its last 64 operation ids. Its creation, its
//! cancellation, and every command fenced on a goal status, criteria, wake
//! lifecycle, or schedule revision are also recorded in the task's decision
//! ledger, which only those write, so a client that lost the answer to one
//! and re-posts it later is answered with the original outcome rather than
//! the refusal a first attempt would meet (`task-already-created`,
//! `task-terminal`, `goal-*`, `wake-*`, `task-schedule-not-monotonic`).
//!
//! Each test commits a decision, then rewrites the stored record as if 64
//! later operations had evicted its id ([`forget_operations`]).

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    load_agent_task_state, AgentOperationId, AgentOperationKind, AgentSchemaPolicy,
    AgentTaskDependencyDeclaration, AgentTaskDependencyOutcome, AgentTaskEntityCommand,
    AgentTaskEntityReply, AgentTaskError, AgentTaskId, AgentTaskOutcome, AgentTaskStatus,
};

mod common;

use common::{
    forget_operations, goal_spec, goal_spec_draft, goal_task_creation_command, task_definition,
    task_scope, Fixture, TASK, TENANT,
};

fn fixture() -> Fixture {
    Fixture::new(ScriptedDispatcher::new())
}

fn operation(step: &str) -> AgentOperationId {
    AgentOperationId::new(AgentOperationKind::Command, [TENANT, TASK, step])
        .expect("the operation id derives")
}

fn applied(result: Result<AgentTaskEntityReply, AgentTaskError>) -> AgentTaskOutcome {
    match result {
        Ok(AgentTaskEntityReply::Applied { outcome }) => outcome,
        other => panic!("expected the command to apply, got {other:?}"),
    }
}

/// Rewrites the task's record as if 64 later operations had moved its window
/// past every id it holds; `forget_ledger` also drops the ledger, as a record
/// written before the ledger existed.
async fn forget_window(fx: &Fixture, forget_ledger: bool) {
    forget_operations(&fx.tasks, &task_scope().persistence_id(), forget_ledger).await;
}

/// Re-posts `command` and asserts it converges on the outcome it committed.
async fn assert_replay_converges(
    fx: &Fixture,
    command: AgentTaskEntityCommand,
    outcome: AgentTaskOutcome,
) {
    match fx.apply_task_command(command).await {
        Ok(AgentTaskEntityReply::Duplicate { outcome: replayed }) => {
            assert_eq!(replayed, outcome, "the replay answers the original outcome");
        }
        other => panic!("expected the replay to answer Duplicate, got {other:?}"),
    }
}

async fn ledger_len(fx: &Fixture) -> usize {
    load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the state loads")
        .expect("the state exists")
        .decision_ledger()
        .len()
}

#[tokio::test]
async fn a_replayed_creation_answers_duplicate_after_the_window_has_moved_past_it() {
    let fx = fixture();
    fx.instantiate_agent().await;
    let create = goal_task_creation_command(task_definition(), goal_spec_draft(goal_spec(), true));
    assert!(create.records_decision());
    let outcome = applied(fx.apply_task_command(create.clone()).await);
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, create, outcome).await;
}

#[tokio::test]
async fn a_replayed_cancellation_of_a_task_it_ended_answers_duplicate_rather_than_task_terminal(
) {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.apply_task_command(goal_task_creation_command(
        task_definition(),
        goal_spec_draft(goal_spec(), true),
    ))
    .await
    .expect("the creation applies");
    let cancel = AgentTaskEntityCommand::Cancel {
        operation_id: operation("cancel"),
        reason: "operator".to_string(),
    };
    assert!(cancel.records_decision());
    let outcome = applied(fx.apply_task_command(cancel.clone()).await);
    fx.pump().await.expect("the wind-down drives");
    fx.pump().await.expect("the settlement drives");
    let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the state loads")
        .expect("the state exists");
    assert_eq!(
        state.task().expect("the task is created").status,
        AgentTaskStatus::Cancelled
    );
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, cancel, outcome).await;
    assert_eq!(ledger_len(&fx).await, 2, "the creation and the cancellation");
}

#[tokio::test]
async fn a_decision_the_ledger_no_longer_holds_meets_the_refusal_a_first_attempt_would() {
    let fx = fixture();
    fx.instantiate_agent().await;
    let create = goal_task_creation_command(task_definition(), goal_spec_draft(goal_spec(), true));
    fx.apply_task_command(create.clone())
        .await
        .expect("the creation applies");
    forget_window(&fx, true).await;

    let refusal = fx
        .apply_task_command(create)
        .await
        .expect_err("the replay is refused");
    assert_eq!(refusal.code(), "task-already-created");
}

#[test]
fn only_decisions_are_remembered_past_the_window() {
    let upstream = || AgentTaskId::new("upstream").expect("the task id is valid");
    assert!(!AgentTaskEntityCommand::Describe.records_decision());
    assert!(!AgentTaskEntityCommand::DeclareDependency {
        operation_id: operation("declare"),
        declaration: Box::new(AgentTaskDependencyDeclaration::new(upstream())),
    }
    .records_decision());
    assert!(!AgentTaskEntityCommand::RecordDependencyOutcome {
        operation_id: operation("resolve"),
        dependency: upstream(),
        outcome: AgentTaskDependencyOutcome::Completed,
    }
    .records_decision());
}
```

Add stubs so the file compiles. On `impl AgentTaskEntityCommand` (beside `operation_id()`):

```rust
    /// Whether the command is a decision the task remembers past its
    /// operation window ([`AgentTaskState::decision_ledger`]).
    #[must_use]
    pub const fn records_decision(&self) -> bool {
        false
    }
```

On `AgentTaskState`, add the field, its initializer in `uncreated()`, the `use crate::decision_ledger::AgentDecisionLedger;` import (all as in Step 3), and this accessor beside `applied_operations()`:

```rust
    /// The decisions the task committed, which answer a replay `Duplicate`
    /// after the operation window has moved past it.
    #[must_use]
    pub const fn decision_ledger(&self) -> &AgentDecisionLedger<AgentTaskOutcome> {
        &self.decision_ledger
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent --all-features --test task_decision_ledger > /tmp/t3.log 2>&1; grep -E "^test |test result|panicked" -A3 /tmp/t3.log | head -40`

Expected:
- the creation and cancellation tests FAIL at `assert!(….records_decision())`;
- `only_decisions_…` PASS;
- `a_decision_the_ledger_no_longer_holds_…` PASS. It pins that the fallback is today's refusal.

- [ ] **Step 3: Implement**

In `crates/rakka-agent/src/task.rs`:

1. **Import:** add `use crate::decision_ledger::AgentDecisionLedger;`.
2. **Field on `AgentTaskState`**, after `applied_operations: AgentTaskOperationLog,`:

```rust
    /// The decisions the task committed, kept past the operation window. A
    /// record written before the ledger existed decodes with an empty one, and
    /// an empty one is omitted, so a task that never decided encodes as before.
    #[serde(default, skip_serializing_if = "AgentDecisionLedger::is_empty")]
    decision_ledger: AgentDecisionLedger<AgentTaskOutcome>,
```

   In `uncreated()`, add `decision_ledger: AgentDecisionLedger::default(),` after `applied_operations`. Fix any other `AgentTaskState { .. }` struct literal the compiler names in the same way.

3. **Private lookup helper** on `AgentTaskState`:

```rust
    /// The outcome an already-applied operation produced: from the window,
    /// or, once the window has moved past a decision, from the decision
    /// ledger.
    fn replayed_outcome(&self, operation_id: &AgentOperationId) -> Option<&AgentTaskOutcome> {
        self.applied_operations
            .outcome(operation_id)
            .or_else(|| self.decision_ledger.outcome(operation_id))
    }
```

4. **`apply_once`'s dedup** (~11918): replace

```rust
            if let Some(outcome) = self
                .state()?
                .applied_operations
                .outcome(operation_id)
                .cloned()
            {
```

   with

```rust
            // The window answers a replay, and past it the decision ledger
            // answers a decision's — before every guard below, so a replayed
            // creation or cancellation converges rather than being refused
            // `task-already-created` or `task-terminal`.
            if let Some(outcome) = self.state()?.replayed_outcome(operation_id).cloned() {
```

   Keep the existing comment inside the block.

5. **`records_decision`** body:

```rust
    /// Whether the command is a decision the task remembers past its
    /// operation window ([`AgentTaskState::decision_ledger`]). A replay of one
    /// answers [`AgentTaskEntityReply::Duplicate`] with its original outcome
    /// after any number of later non-decision operations, and up to
    /// [`crate::AGENT_DECISION_LEDGER_CAPACITY`] `- 1` later decisions.
    #[must_use]
    pub const fn records_decision(&self) -> bool {
        matches!(self, Self::Create { .. } | Self::Cancel { .. })
    }
```

6. **`transition`** gains `decision: bool` after `readiness`:

```rust
    async fn transition<F>(
        &mut self,
        now: AgentTimestampMillis,
        readiness: Option<AgentAssignmentReadiness>,
        decision: bool,
        transition: F,
    ) -> AgentTaskResult<AgentTaskEntityReply>
```

   In its `assign` closure, replace

```rust
                        state
                            .applied_operations
                            .record(operation_id, result.clone());
```

   with

```rust
                        if decision {
                            state
                                .decision_ledger
                                .record(operation_id.clone(), result.clone());
                        }
                        state
                            .applied_operations
                            .record(operation_id, result.clone());
```

   Add to its doc comment: `A decision ([`AgentTaskEntityCommand::records_decision`]) is also recorded in the decision ledger, in the same compare-and-set.`

7. **Thread the flag.** In `apply_once`, right before `let reply = match command {`, add `let decision = command.records_decision();`. Then:

```bash
perl -pi -e 's/\.transition\(now, (readiness|None), move \|state\|/.transition(now, $1, decision, move |state|/' crates/rakka-agent/src/task.rs
grep -cE '\.transition\(now, (readiness|None), decision, move \|state\|' crates/rakka-agent/src/task.rs   # expect 17
grep -cE '\.transition\(now, (readiness|None), move \|state\|' crates/rakka-agent/src/task.rs             # expect 0
```

8. **Window doc** on `AGENT_TASK_OPERATION_LOG_CAPACITY` (~250): append `A decision ([`AgentTaskEntityCommand::records_decision`]) is answered past the window from the task's decision ledger instead, with its original outcome.`

- [ ] **Step 4: Run the tests to verify they pass**

```
cargo fmt -p rakka-agent
cargo test -p rakka-agent --all-features --test task_decision_ledger --test task_entity --test goal_contract --test human_owned_tasks > /tmp/t3.log 2>&1; grep -E "test result|FAILED|panicked" -A3 /tmp/t3.log
```

Expected: every `test result: ok`, with `task_decision_ledger` at 4 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent/src/task.rs crates/rakka-agent/tests/task_decision_ledger.rs
git commit -m "Answer a task's creation or cancellation replay from the decision ledger past the operation window

Issue #96, ask 2.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Then `cargo clean`.

---

### Task 4: Goal commands are decisions

**Files:**
- Modify: `crates/rakka-agent/src/task.rs`, the `records_decision` body.
- Modify: `crates/rakka-agent/tests/task_decision_ledger.rs`.

**Interfaces:**
- Consumes: Task 3's helpers in `task_decision_ledger.rs`.
- Produces: `records_decision()` true for `ActivateGoal`, `RecordGoalDecision`, `ResumeGoal` and `ReviseGoalCriteria`.

- [ ] **Step 1: Write the failing tests**

Extend the `use rakka_agent::{…}` import with `AgentBudgetAllocation, AgentBudgetCeilings, AgentGoalCriteriaSource, AgentGoalDecision, AgentGoalStatus, AgentGoalTerminalReason, AgentPolicyRef, AgentRevisionNumber, AgentTaskContent, AgentTaskCreation, AgentTaskDefinition, AgentTaskResultCheck, AgentTaskResultRule, AgentTaskRuleId`.

Extend `use common::{…}` with `agent_id, continuous_goal_mode, goal_id, provenance, schema, task_definition_id, wake_policy`.

Append:

```rust
// ---------------------------------------------------------------------------
// Goal commands, fenced on the goal's status or criteria revision.
// ---------------------------------------------------------------------------

async fn create_goal_task(fx: &Fixture, activate: bool) {
    fx.instantiate_agent().await;
    fx.apply_task_command(goal_task_creation_command(
        task_definition(),
        goal_spec_draft(goal_spec(), activate),
    ))
    .await
    .expect("the creation applies");
}

async fn goal_status_revision(fx: &Fixture) -> AgentRevisionNumber {
    load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the state loads")
        .expect("the state exists")
        .task()
        .expect("the task is created")
        .goal_state
        .as_deref()
        .expect("the goal record exists")
        .status_revision()
}

#[tokio::test]
async fn a_replayed_goal_activation_answers_duplicate_rather_than_a_stale_revision() {
    let fx = fixture();
    create_goal_task(&fx, false).await;
    let activate = AgentTaskEntityCommand::ActivateGoal {
        operation_id: operation("activate"),
        expected_status_revision: AgentRevisionNumber::INITIAL,
        provenance: Box::new(provenance(10)),
    };
    assert!(activate.records_decision());
    let outcome = applied(fx.apply_task_command(activate.clone()).await);
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, activate, outcome).await;
}

#[tokio::test]
async fn a_replayed_goal_decision_answers_duplicate_rather_than_goal_terminal() {
    let fx = fixture();
    create_goal_task(&fx, true).await;
    let decide = AgentTaskEntityCommand::RecordGoalDecision {
        operation_id: operation("cancel-goal"),
        decision: Box::new(AgentGoalDecision {
            reason: AgentGoalTerminalReason::CancellationRequested {
                reason: "operator".to_string(),
            },
            evaluation: None,
            provenance: Some(Box::new(provenance(21))),
            expected_status_revision: AgentRevisionNumber::INITIAL,
        }),
    };
    assert!(decide.records_decision());
    let outcome = applied(fx.apply_task_command(decide.clone()).await);
    assert_eq!(
        outcome.goal.as_ref().expect("the goal outcome rides").status,
        AgentGoalStatus::Cancelled
    );
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, decide, outcome).await;
}

#[tokio::test]
async fn a_replayed_criteria_revision_answers_duplicate_rather_than_a_stale_revision() {
    let fx = fixture();
    create_goal_task(&fx, true).await;
    let revise = AgentTaskEntityCommand::ReviseGoalCriteria {
        operation_id: operation("revise"),
        expected_criteria_revision: AgentRevisionNumber::INITIAL,
        source: AgentGoalCriteriaSource::Policy(
            AgentPolicyRef::new("ticket-resolved-v2").expect("the policy ref is valid"),
        ),
        digest: None,
        provenance: Box::new(provenance(80)),
    };
    assert!(revise.records_decision());
    let outcome = applied(fx.apply_task_command(revise.clone()).await);
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, revise, outcome).await;
}

/// A definition whose ceilings leave headroom above the goal's allocation, so
/// a resume's widening has room to grow into.
fn roomy_definition() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        task_definition_id(),
        "Resolve one customer support ticket under the goal's allocation.",
        schema("ticket-input"),
        schema("ticket-result"),
    )
    .expect("task definition should be valid")
    .with_result_rule(AgentTaskResultRule::new(
        AgentTaskRuleId::new("answer-present").expect("rule id should be valid"),
        AgentTaskResultCheck::NonEmptyString {
            pointer: "/answer".to_string(),
        },
    ))
    .with_budgets(AgentBudgetCeilings {
        max_loop_iterations: Some(10),
        ..AgentBudgetCeilings::unbounded()
    })
}

#[tokio::test]
async fn a_replayed_goal_resume_answers_duplicate_rather_than_a_stale_revision() {
    // An agent-owned continuous goal with no loop allocation parks `Waiting`
    // at creation: the first assignment refusal is permanent.
    let fx = fixture();
    fx.instantiate_agent().await;
    let mut spec = goal_spec();
    spec.allocation = AgentBudgetAllocation {
        loop_iterations: Some(0),
        ..AgentBudgetAllocation::unbounded()
    };
    let parked = applied(
        fx.apply_task_command(AgentTaskEntityCommand::Create {
            operation_id: AgentOperationId::new(
                AgentOperationKind::TaskCreation,
                [TENANT, TASK, "1"],
            )
            .expect("operation id should be derivable"),
            creation: Box::new(AgentTaskCreation {
                definition: roomy_definition(),
                input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                    .expect("the input is inline-bounded"),
                assignee: Some(agent_id()),
                team: None,
                goal: Some(goal_id()),
                goal_mode: continuous_goal_mode(wake_policy()),
                goal_spec: Some(Box::new(goal_spec_draft(spec, true))),
                parent: None,
                dependencies: Vec::new(),
                escrow: None,
                wake: None,
                delegation: None,
                telemetry: Default::default(),
            }),
        })
        .await,
    );
    assert_eq!(
        parked.goal.expect("the goal outcome rides").status,
        AgentGoalStatus::Waiting
    );

    let resume = AgentTaskEntityCommand::ResumeGoal {
        operation_id: operation("resume"),
        expected_status_revision: goal_status_revision(&fx).await,
        top_up: Some(Box::new(AgentBudgetAllocation {
            loop_iterations: Some(2),
            ..AgentBudgetAllocation::unbounded()
        })),
        provenance: Box::new(provenance(91)),
    };
    assert!(resume.records_decision());
    let outcome = applied(fx.apply_task_command(resume.clone()).await);
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, resume, outcome).await;
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent --all-features --test task_decision_ledger > /tmp/t4.log 2>&1; grep -E "^test |test result|panicked" -A3 /tmp/t4.log | head -50`

Expected: the four new tests FAIL at their `records_decision()` assertion. The Task 3 tests still PASS.

- [ ] **Step 3: Implement**

Widen `AgentTaskEntityCommand::records_decision`:

```rust
        matches!(
            self,
            Self::Create { .. }
                | Self::Cancel { .. }
                | Self::ActivateGoal { .. }
                | Self::RecordGoalDecision { .. }
                | Self::ResumeGoal { .. }
                | Self::ReviseGoalCriteria { .. }
        )
```

- [ ] **Step 4: Run the tests to verify they pass**

```
cargo fmt -p rakka-agent
cargo test -p rakka-agent --all-features --test task_decision_ledger --test goal_contract --test goal_budget --test goal_stagnation > /tmp/t4.log 2>&1; grep -E "test result|FAILED|panicked" -A3 /tmp/t4.log
```

Expected: every `test result: ok`, with `task_decision_ledger` at 8 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent/src/task.rs crates/rakka-agent/tests/task_decision_ledger.rs
git commit -m "Remember a task's goal activation, decision, resume, and criteria revision past the operation window

Issue #96, ask 2.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Then `cargo clean`.

---

### Task 5: Wake-lifecycle and schedule commands are decisions

**Files:**
- Modify: `crates/rakka-agent/src/task.rs`, the `records_decision` body.
- Modify: `crates/rakka-agent/tests/task_decision_ledger.rs`.

**Interfaces:**
- Consumes: Tasks 3–4's helpers in `task_decision_ledger.rs`.
- Produces: `records_decision()` true for `SuspendContinuousGoal`, `ResumeContinuousGoal`, `RenewContinuousGoal`, `RetireContinuousGoal` and `UpdateContinuousSchedule`.

- [ ] **Step 1: Write the failing tests**

Extend the `use rakka_agent::{…}` import with `AgentWakeLifecyclePolicy, ScheduleRevision`, and add `use rakka_agent_workflow::AgentTimestampMillis;`.

Append:

```rust
// ---------------------------------------------------------------------------
// Wake-lifecycle commands, fenced on the lifecycle revision, and the schedule
// update, fenced on the schedule revision.
// ---------------------------------------------------------------------------

async fn lifecycle_revision(fx: &Fixture) -> AgentRevisionNumber {
    load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the state loads")
        .expect("the state exists")
        .task()
        .expect("the task is created")
        .wake_controller
        .as_ref()
        .expect("the controller exists")
        .lifecycle()
        .lifecycle_revision()
}

fn lifecycle_operation(kind: AgentOperationKind, step: &str) -> AgentOperationId {
    AgentOperationId::new(kind, [TENANT, TASK, step]).expect("the operation id derives")
}

fn suspend(revision: AgentRevisionNumber) -> AgentTaskEntityCommand {
    AgentTaskEntityCommand::SuspendContinuousGoal {
        operation_id: lifecycle_operation(AgentOperationKind::LifecycleSuspend, "suspend-1"),
        expected_lifecycle_revision: revision,
        reason: Some("maintenance window".to_string()),
        provenance: Box::new(provenance(10)),
    }
}

#[tokio::test]
async fn a_replayed_suspension_answers_duplicate_rather_than_a_stale_lifecycle_revision() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;
    let command = suspend(lifecycle_revision(&fx).await);
    assert!(command.records_decision());
    let outcome = applied(fx.apply_task_command(command.clone()).await);
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, command, outcome).await;
}

#[tokio::test]
async fn a_replayed_lifecycle_resume_answers_duplicate_rather_than_a_stale_lifecycle_revision() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;
    applied(
        fx.apply_task_command(suspend(lifecycle_revision(&fx).await))
            .await,
    );
    let resume = AgentTaskEntityCommand::ResumeContinuousGoal {
        operation_id: lifecycle_operation(AgentOperationKind::LifecycleResume, "resume-1"),
        expected_lifecycle_revision: lifecycle_revision(&fx).await,
        provenance: Box::new(provenance(11)),
    };
    assert!(resume.records_decision());
    let outcome = applied(fx.apply_task_command(resume.clone()).await);
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, resume, outcome).await;
    assert_eq!(ledger_len(&fx).await, 3, "creation, suspension, resume");
}

#[tokio::test]
async fn a_replayed_renewal_answers_duplicate_rather_than_a_stale_lifecycle_revision() {
    // Renewal needs an effective expiry to extend; the default lifecycle
    // policy has none, and requires no renewal window.
    let fx = fixture();
    fx.instantiate_agent().await;
    let policy = wake_policy()
        .with_lifecycle(AgentWakeLifecyclePolicy {
            expires_at: Some(AgentTimestampMillis::new(50_000)),
            ..AgentWakeLifecyclePolicy::DEFAULT
        })
        .expect("the lifecycle policy is valid");
    fx.create_continuous_control_task(continuous_goal_mode(policy))
        .await;
    let renew = AgentTaskEntityCommand::RenewContinuousGoal {
        operation_id: lifecycle_operation(AgentOperationKind::Command, "renew-1"),
        expected_lifecycle_revision: lifecycle_revision(&fx).await,
        new_expires_at: AgentTimestampMillis::new(60_000),
        provenance: Box::new(provenance(12)),
    };
    assert!(renew.records_decision());
    let outcome = applied(fx.apply_task_command(renew.clone()).await);
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, renew, outcome).await;
}

#[tokio::test]
async fn a_replayed_retirement_answers_duplicate_rather_than_wake_lifecycle_terminal() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;
    let retire = AgentTaskEntityCommand::RetireContinuousGoal {
        operation_id: lifecycle_operation(AgentOperationKind::LifecycleTerminate, "retire-1"),
        expected_lifecycle_revision: lifecycle_revision(&fx).await,
        provenance: Box::new(provenance(13)),
    };
    assert!(retire.records_decision());
    let outcome = applied(fx.apply_task_command(retire.clone()).await);
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, retire, outcome).await;
}

#[tokio::test]
async fn a_replayed_schedule_update_answers_duplicate_rather_than_not_monotonic() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_task().await;
    let update = AgentTaskEntityCommand::UpdateContinuousSchedule {
        operation_id: lifecycle_operation(AgentOperationKind::SettingsUpdate, "rev-2"),
        schedule_revision: ScheduleRevision::new(2),
        wake_policy: None,
    };
    assert!(update.records_decision());
    let outcome = applied(fx.apply_task_command(update.clone()).await);
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, update, outcome).await;
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent --all-features --test task_decision_ledger > /tmp/t5.log 2>&1; grep -E "^test |test result|panicked" -A3 /tmp/t5.log | head -50`

Expected: the five new tests FAIL at their `records_decision()` assertion. The earlier 8 PASS.

If `RenewContinuousGoal` instead fails to *apply* (no existing test drives it end to end), read `renew_continuous_goal` (task.rs ~8344) and `AgentWakeControllerState::renew` (wake.rs ~2249). Adjust only the test's policy or `new_expires_at` until the first application succeeds, and record the adjustment in the commit message.

- [ ] **Step 3: Implement**

Widen `records_decision`:

```rust
        matches!(
            self,
            Self::Create { .. }
                | Self::Cancel { .. }
                | Self::ActivateGoal { .. }
                | Self::RecordGoalDecision { .. }
                | Self::ResumeGoal { .. }
                | Self::ReviseGoalCriteria { .. }
                | Self::SuspendContinuousGoal { .. }
                | Self::ResumeContinuousGoal { .. }
                | Self::RenewContinuousGoal { .. }
                | Self::RetireContinuousGoal { .. }
                | Self::UpdateContinuousSchedule { .. }
        )
```

Extend its doc comment's first sentence to name what the decisions are: `its creation and cancellation, and every command fenced on a goal status, criteria, wake-lifecycle, or schedule revision`.

- [ ] **Step 4: Run the tests to verify they pass**

```
cargo fmt -p rakka-agent
cargo test -p rakka-agent --all-features --test task_decision_ledger --test goal_lifecycle --test wake_fencing --test goal_contract > /tmp/t5.log 2>&1; grep -E "test result|FAILED|panicked" -A3 /tmp/t5.log
```

Expected: every `test result: ok`, with `task_decision_ledger` at 13 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent/src/task.rs crates/rakka-agent/tests/task_decision_ledger.rs
git commit -m "Remember a task's wake-lifecycle and schedule commands past the operation window

Issue #96, ask 2.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Then `cargo clean`.

---

### Task 6: Documentation and full validation

**Files:**
- Modify: `docs/rakka-compatibility.md`, adding one bullet directly above the bullet that begins `- Issue #85 ask 6, first part`, or above the newest `- Issue #96` bullet if PRs #97/#98 have merged.
- Modify: `CHANGELOG.md`, adding one entry as the last bullet of `### Fixed`, directly before `### Security`.
- Modify: `docs/superpowers/specs/2026-10-12-decision-ledger-design.md`. Change `Status: draft, 2026-10-12, for the owner's review.` to `Status: implemented, 2026-10-12. Branch `a18-decision-ledger` off `06d2cc7`.`

- [ ] **Step 1: The compatibility bullet**

```markdown
- Issue #96 ask 2 ("a resolution's key outlives the operation window") adds one public type, one constant, two command methods, two state accessors, and one serde-defaulted durable field on two records; no code, refusal meaning, or schema version changes. `AgentRunState` and `AgentTaskState` each gain a decision ledger (`AgentDecisionLedger`, at most `AGENT_DECISION_LEDGER_CAPACITY` = 32 entries of operation id and outcome, oldest evicted first), which only decision commands write, in the same compare-and-set that records the id in the 64-entry operation log. The run's decisions are `ResolveCheckpoint` and `ResolveIndeterminateEffect`; the task's are `Create`, `Cancel`, and every command fenced on a goal status, criteria, wake-lifecycle, or schedule revision (`ActivateGoal`, `RecordGoalDecision`, `ResumeGoal`, `ReviseGoalCriteria`, `SuspendContinuousGoal`, `ResumeContinuousGoal`, `RenewContinuousGoal`, `RetireContinuousGoal`, `UpdateContinuousSchedule`); `AgentRunEntityCommand::records_decision()` and `AgentTaskEntityCommand::records_decision()` answer which. A replay whose id the operation window no longer holds is looked up in the ledger before every other guard, the terminal one included, and a hit answers `Duplicate` with the outcome the decision committed — so a replayed Deny that ended its run, or a replayed cancellation of a task it ended, converges instead of being refused `run-terminal` or `task-terminal`. Past the window, a replay of a recorded decision no longer meets `run-unknown-checkpoint`, `run-terminal`, `run-stale-effect-generation`, `run-stale-effect-result`, `task-already-created`, `task-terminal`, `goal-terminal`, `goal-stale-status-revision`, `goal-stale-criteria-revision`, `wake-lifecycle-terminal`, `wake-stale-lifecycle-revision`, or `task-schedule-not-monotonic`; a first attempt meets each exactly as before. Like the window, the ledger keys on the id alone. The field (`decision_ledger`) is `#[serde(default)]` and omitted while empty, so a record that never decided encodes byte-identically and a record written before this change decodes with an empty ledger: decisions committed before the upgrade are not covered. No schema version moves: an older peer that rewrites a record during a rolling update drops the ledger, which degrades a later replay of those decisions to today's refusal and nothing worse, where a version bump would make that peer fail closed on every record a newer peer touched.
```

- [ ] **Step 2: The changelog entry** (the last bullet of `### Fixed`)

```markdown
- A client that lost the answer to a committed decision and re-posted it after the run's or task's 64-entry operation window had moved on met the refusal a first attempt would meet (`run-unknown-checkpoint`, `run-terminal`, `task-already-created`, `task-terminal`, a stale goal or lifecycle revision), so it could not tell a decision that applied from one that was refused — and ordinary traffic fills the window, since every effect result is one entry. Runs and tasks now keep a 32-entry decision ledger that only decisions write — a checkpoint or indeterminate-effect resolution, a task's creation or cancellation, and every revision-fenced goal, wake-lifecycle, or schedule command — and answer such a replay `Duplicate` with its original outcome (issue #96, ask 2).
```

- [ ] **Step 3: Update the spec's status line** as described under **Files** above.

- [ ] **Step 4: Run full validation**

```bash
scripts/validate.sh > /tmp/validate-ledger.log 2>&1; echo "EXIT=$?" >> /tmp/validate-ledger.log
tail -3 /tmp/validate-ledger.log
grep -E "^test result" /tmp/validate-ledger.log | awk '{p+=$4; f+=$6} END {print "passed="p, "failed="f}'
```

Expected: `EXIT=0` and `failed=0`.
- This takes about an hour. Run it in the background and wait for its completion notification, not a polling loop.
- If the one-shot workspace test run is killed under load, run the failing crate's tests on their own (`cargo test -p <crate> --all-features`) and report both runs.

- [ ] **Step 5: Commit**

```bash
git add docs/rakka-compatibility.md CHANGELOG.md docs/superpowers/specs/2026-10-12-decision-ledger-design.md
git commit -m "Record issue #96 ask 2 in the compatibility document and the changelog

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
