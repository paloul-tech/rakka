# Handoff Park Checkpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A handoff send that exhausts its retry budget parks its run behind a real reconciliation checkpoint, and the task's handoff-result notice releases that park by settling the send as executed (issue #106).

**Architecture:**
- A private classifier, `handoff_send`, says how the run's handoff cell stands toward an effect. Every site that settles a handoff send asks it.
- `record_effect_result` opens the checkpoint whenever a transition leaves the effect `Indeterminate`, not only on an `Indeterminate` outcome.
- `accept_handoff_result` and a send result arriving after the notice both settle a parked or in-flight send as executed.
- Two decisions get defined handoff behaviour: abandonment fails the run, and compensation settles the cell. A confirmation carrying an exhausted outcome is refused.
- A settle-pass repair (the gap slice 3 pattern) heals records parked before the change.

**Tech Stack:** Rust 1.88 (MSRV), serde, tokio. One crate: `rakka-agent`, mostly `src/run.rs`.

**Spec:** `docs/superpowers/specs/2026-10-14-handoff-park-checkpoint-design.md` (D1–D7 below refer to its sections). The brief is issue #106.

**Branch:** `handoff-park-checkpoint`, off `main` at `533fa20`. The spec is already committed on it.

## Global Constraints

- **MSRV is 1.88.** Use no newer language or library features.
- **Lints:** `cargo clippy --workspace --all-targets --all-features -- -D warnings` must pass, so test code may not leave an unused import, variant, field or helper. Every task adds only the helpers and imports it uses. Every public item needs a doc comment; `unsafe_code` is forbidden.
- **No durable field, serde attribute, schema version, public type, public method or stable code is added.** `needs_park_repair` is `pub(crate)`. The only code a refusal uses is the existing `effect-policy-invalid`.
- **Codes, verbatim:**
  - the abandoned cell and the run's `EffectFailed`: `reconciliation-abandoned`;
  - the compensated cell: `compensated`;
  - the refusal: `effect-policy-invalid`;
  - the receipt's `peer_status`: `"accepted"` / `"refused"`.
- **Never persist credential material.** Nothing here touches credentials.
- **Disk:** run `cargo clean` between tasks (an owner rule on this machine), and never while a cargo command another agent started may still be running. A cold `rakka-agent` test build takes 30–50 minutes here.
- **Running tests:**
  - Run per crate and per test target. Unit tests in `run.rs` run with `--lib`, separately from integration binaries.
  - Redirect `scripts/validate.sh` to a file and capture `$?`; never pipe it into `tail`.
- **Commits** end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **A notice reaching a parked run that is also winding down** (a cancellation requested while parked). An acceptance ends it `HandedOff`, and a refusal ends it `Cancelled`, exactly as for an unparked run. Pinned in Task 2, test `a_notice_to_a_parked_cancelled_run_ends_it_as_an_unparked_one_would`.
2. **The courier delivering the same notice again after it released the park.** It is absorbed: nothing re-parks and nothing moves. Pinned in Task 2, test `a_replayed_notice_after_the_release_changes_nothing`.
3. **An operator decision submitted after the notice released the park.** It is refused (`run-terminal` after a hand-off; `run-unknown-checkpoint` or `run-stale-effect-result` after a resumption), never applied a second time. Pinned in Task 2, test `a_decision_after_the_release_is_refused`.
4. **The checkpoint's SLA due date passing with no decision.** The checkpoint escalates, the run keeps waiting, and the send stays `Indeterminate`. This is the escalation the brief found missing. Pinned in Task 1, test `a_parked_handoff_sends_checkpoint_escalates_on_its_sla`.
5. **`ConfirmedNotExecuted` through the new checkpoint on a live run.** A new generation re-sends under the same deduplication key (the handoff id) while the cell keeps the fence. The behaviour is unchanged, but it is newly reachable. Pinned in Task 3, test `a_not_executed_decision_resends_the_handoff_under_the_fence`.

---

### Task 1: The park opens its checkpoint, and a late send result settles executed (D1, D2, D4)

**Files:**
- Modify: `crates/rakka-agent/src/run.rs`:
  - `record_effect_result`, the checkpoint condition and its comment at ~4201-4228;
  - `apply_effect_outcome`, its head ~4234-4254, a new first match arm, and the handoff branch ~4494-4509;
  - a new private `HandoffSend` / `handoff_send` just above `apply_effect_outcome`;
  - the test module (~10047): the fixture and five tests.
- Create: `crates/rakka-agent/tests/handoff_send_park.rs`.

**Interfaces:**
- Produces (crate-private, in `run.rs`):
  ```rust
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  enum HandoffSend { NotHeld, Unresolved, ResolvedByTask, SettledLocally }
  fn handoff_send(loop_state: &AgentLoopState, effect_id: &AgentEffectId) -> HandoffSend
  ```
- Produces (test module): `struct HandoffSendFixture`, `fn handoff_send_fixture(status: AgentRunStatus, cell_status: AgentHandoffStatus, effect_status: AgentRunEffectStatus) -> HandoffSendFixture`, and `fn sla_policies() -> AgentEffectPolicies`. The fixture has the methods `run()`, `send()`, `cell()`, `checkpoint_id()`, `hand_off()` and `exhaust(&AgentEffectPolicies)`, and the fields `state`, `scope`, `task`, `effect_id`, `generation`, `handoff` and `now`.
- Produces (integration file): `LostAnswerExecutor`, `parking_world(record_transfer: bool, target_ready: bool) -> (Fixture, Arc<InMemoryAgentSegmentSink>)`, `settle(&Fixture)`, `run_record(&Fixture) -> AgentRun`, `send_effect(&AgentRun) -> &AgentRunEffect`, and `checkpoint_opens(&InMemoryAgentSegmentSink) -> usize`.

- [ ] **Step 1: Write the unit-test fixture and the failing unit tests**

In `crates/rakka-agent/src/run.rs`, inside `mod tests`, add below the existing `use crate::AgentToolId;` line:

```rust
    use crate::coordination::AgentHandoffStatus;
```

Then add at the end of `mod tests` (before its closing `}`):

```rust
    /// A run whose handoff cell holds its send: the shape every issue #106
    /// proof starts from. The run serves `ticket-1` as generation 1, its turn
    /// rests `AwaitingTools` on the send, and the send's three attempts are
    /// reserved, exactly as the turn that committed it left them.
    struct HandoffSendFixture {
        state: AgentRunState,
        scope: AgentRunScope,
        task: AgentTaskId,
        effect_id: AgentEffectId,
        generation: AgentEffectGeneration,
        handoff: crate::identity::AgentHandoffId,
        now: AgentTimestampMillis,
    }

    impl HandoffSendFixture {
        fn run(&self) -> &AgentRun {
            self.state.run().expect("the run exists")
        }

        fn send(&self) -> &AgentRunEffect {
            self.run()
                .loop_state
                .effects()
                .iter()
                .find(|effect| effect.effect_id == self.effect_id)
                .expect("the send is held")
        }

        fn cell(&self) -> &crate::coordination::AgentHandoffCell {
            self.run().loop_state.handoff().expect("the cell is held")
        }

        fn checkpoint_id(&self) -> HumanCheckpointId {
            AgentCheckpoint::id_for_effect(
                &self.effect_id,
                self.generation,
                AgentCheckpointKind::IndeterminateEffectReconciliation,
            )
        }

        /// Records the terminal reason an accepted handoff result leaves.
        fn hand_off(&mut self) {
            let handoff = self.handoff.clone();
            self.state.run_mut().expect("the run exists").terminal_reason =
                Some(AgentRunTerminalReason::HandedOff {
                    handoff,
                    target: AgentId::new("billing-agent").expect("the agent id is valid"),
                });
        }

        /// Records the result the dispatcher returns once the executor's
        /// probe has failed on every attempt: a spent budget.
        fn exhaust(&mut self, policies: &AgentEffectPolicies) -> AgentRunResult<()> {
            let (effect_id, generation, now) = (self.effect_id.clone(), self.generation, self.now);
            record_effect_result(
                &mut self.state,
                &effect_id,
                generation,
                3,
                0,
                AgentRunEffectOutcome::exhausted(
                    "a2a-handoff-attempt-failed",
                    "the probe could not read the task's handoff record",
                ),
                &AgentSchemaPolicy::default(),
                policies,
                now,
            )
        }
    }

    fn handoff_send_fixture(
        status: AgentRunStatus,
        cell_status: AgentHandoffStatus,
        effect_status: AgentRunEffectStatus,
    ) -> HandoffSendFixture {
        use crate::coordination::{AgentHandoffCell, AgentHandoffRecord};

        let now = AgentTimestampMillis::new(10);
        let scope = AgentRunScope::new(
            TenantId::new("acme"),
            AgentId::new("support-agent").expect("the agent id is valid"),
            AgentRunId::new("ticket-1-gen-1").expect("the run id is valid"),
        )
        .expect("the scope is valid");
        let task = AgentTaskId::new("ticket-1").expect("the task id is valid");
        let schema = AgentSchemaRef::new(
            AgentSchemaId::new("result").expect("the schema id is valid"),
            AgentRevisionNumber::INITIAL,
        );
        let definition = AgentTaskDefinition::new(
            AgentTaskDefinitionId::new("resolve-ticket").expect("the definition id is valid"),
            "The handoff-park fixture.",
            schema.clone(),
            schema,
        )
        .expect("the definition is valid");
        let budget = AgentRunBudget::allocate(
            crate::budget::AgentBudgetGrant::from_ceilings(&definition.budgets),
            now,
        );
        let mut loop_state = AgentLoopState::started(
            task.clone(),
            None,
            AgentRevisionNumber::INITIAL,
            AgentRevisionNumber::INITIAL,
            definition.version,
            budget,
        );
        let turn = loop_state.turn();
        let effect_id = effect_id_for(&scope, turn, 0).expect("the effect id derives");
        let handoff =
            crate::coordination::handoff_id_for(&scope, turn, 0).expect("the handoff id derives");
        let record = AgentHandoffRecord {
            handoff: handoff.clone(),
            goal: None,
            task: task.clone(),
            source_run: scope.clone(),
            source_generation: AgentAssignmentGeneration::new(1),
            requested_skill: crate::AgentCapabilityId::new("billing")
                .expect("the skill id is valid"),
            resolved: crate::delegation::AgentDelegationTarget::new(
                AgentId::new("billing-agent").expect("the agent id is valid"),
                AgentTaskDefinitionId::new("resolve-ticket").expect("the definition id is valid"),
            ),
            reason: "needs billing authority".to_string(),
            policy_revision: AgentRevisionNumber::INITIAL,
            definition_revision: AgentRevisionNumber::INITIAL,
            settings_revision: AgentRevisionNumber::INITIAL,
            context: Vec::new(),
            a2a_message_id: handoff.as_str().to_string(),
            deduplication_key: handoff.as_str().to_string(),
            turn,
            slot: 0,
            effect: effect_id.clone(),
            call_id: AgentToolCallId::new("call-1").expect("the call id is valid"),
            telemetry: Default::default(),
            created_at: now,
        };
        let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
        let mut effect = AgentRunEffect::new(
            &scope,
            turn,
            0,
            AgentRunEffectRequest::A2aHandoff {
                handoff: Box::new(record.clone()),
            },
            &spec,
            AgentRevisionNumber::INITIAL,
            now,
        )
        .expect("the send derives");
        effect.status = effect_status;
        let generation = effect.generation;
        loop_state
            .budget_mut()
            .reserve_attempts(spec.max_attempts)
            .expect("the send's attempts are affordable");
        loop_state.record_effect(effect).expect("the send records");
        let mut cell = AgentHandoffCell::pending(Box::new(record));
        if cell_status.is_settled() {
            cell.settled_at = Some(now);
        }
        cell.status = cell_status;
        loop_state.record_handoff(cell);
        loop_state.set_phase(AgentLoopPhase::AwaitingTools);
        let mut state = AgentRunState::unassigned(scope.clone(), now);
        state.run = Some(AgentRun {
            binding: AgentRunBinding::new(scope.clone(), task.clone()),
            generation: AgentAssignmentGeneration::new(1),
            definition,
            input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                .expect("the input is inline-bounded"),
            status,
            loop_state,
            terminal_reason: None,
            settlement: AgentRunSettlementStatus::Owed,
            accepted_at: now,
            terminal_at: None,
        });
        HandoffSendFixture {
            state,
            scope,
            task,
            effect_id,
            generation,
            handoff,
            now,
        }
    }

    /// A deployment SLA: escalate a minute after opening, to `on-call`.
    fn sla_policies() -> AgentEffectPolicies {
        AgentEffectPolicies::default().with_checkpoint_sla(crate::checkpoints::AgentCheckpointSla {
            due_after_ms: Some(60_000),
            expire_after_ms: Some(120_000),
            escalation_target: Some("on-call".to_string()),
        })
    }

    /// Issue #106, Ask 1: an exhausted handoff send parks behind the same
    /// reconciliation checkpoint an ambiguous outcome opens: the same id,
    /// summary, decision set, record surface, and SLA.
    #[test]
    fn an_exhausted_handoff_send_parks_behind_its_reconciliation_checkpoint() {
        let mut fx = handoff_send_fixture(
            AgentRunStatus::WaitingForEffect,
            AgentHandoffStatus::Pending,
            AgentRunEffectStatus::Ready,
        );
        fx.exhaust(&sla_policies())
            .expect("the exhausted result records");

        assert_eq!(fx.run().status, AgentRunStatus::WaitingForReconciliation);
        assert_eq!(fx.send().status, AgentRunEffectStatus::Indeterminate);
        assert!(
            matches!(fx.cell().status, AgentHandoffStatus::Pending),
            "the cell still holds the fence"
        );
        let open = fx.run().loop_state.open_checkpoints();
        assert_eq!(open.len(), 1, "the park carries its checkpoint: {open:?}");
        let checkpoint = &open[0];
        assert_eq!(checkpoint.checkpoint_id, fx.checkpoint_id());
        assert_eq!(
            checkpoint.kind,
            AgentCheckpointKind::IndeterminateEffectReconciliation
        );
        assert_eq!(checkpoint.bound_effect.effect_id, fx.effect_id);
        assert_eq!(checkpoint.bound_effect.generation, fx.generation);
        assert_eq!(checkpoint.scope, fx.scope);
        assert_eq!(checkpoint.task, Some(fx.task.clone()));
        assert_eq!(checkpoint.summary, "Reconcile indeterminate effect on turn 1");
        assert_eq!(
            checkpoint.allowed_decisions.len(),
            5,
            "the five default reconciliation decisions"
        );
        assert_eq!(
            checkpoint.due_at,
            Some(AgentTimestampMillis::new(fx.now.as_millis() + 60_000))
        );
        assert_eq!(
            checkpoint.expires_at, None,
            "a reconciliation checkpoint never hard-expires"
        );
    }

    /// The SLA the brief found silent: the park's checkpoint escalates at its
    /// due date, and the run keeps waiting for a decision.
    #[test]
    fn a_parked_handoff_sends_checkpoint_escalates_on_its_sla() {
        let mut fx = handoff_send_fixture(
            AgentRunStatus::WaitingForEffect,
            AgentHandoffStatus::Pending,
            AgentRunEffectStatus::Ready,
        );
        fx.exhaust(&sla_policies())
            .expect("the exhausted result records");

        let due = AgentTimestampMillis::new(fx.now.as_millis() + 60_000);
        fire_checkpoint_timers(&mut fx.state, due).expect("the timers fire");

        assert_eq!(fx.run().status, AgentRunStatus::WaitingForReconciliation);
        let open = fx.run().loop_state.open_checkpoints();
        assert_eq!(open.len(), 1, "an escalation keeps the wait: {open:?}");
        assert_eq!(
            open[0].status,
            crate::checkpoints::AgentCheckpointStatus::Escalated
        );
        assert_eq!(fx.send().status, AgentRunEffectStatus::Indeterminate);
    }

    /// Issue #106: a send result that arrives after the task's handoff result
    /// settled the cell proves nothing new. The task's record says the send
    /// executed, so it settles `Succeeded` whatever the dispatcher reported,
    /// and nothing parks.
    #[test]
    fn a_send_result_after_an_accepted_notice_settles_the_send_executed() {
        let mut fx = handoff_send_fixture(
            AgentRunStatus::Cancelling,
            AgentHandoffStatus::Accepted {
                target_run: AgentRunId::new("ticket-1-gen-2").expect("the run id is valid"),
                generation: AgentAssignmentGeneration::new(2),
            },
            AgentRunEffectStatus::Ready,
        );
        fx.hand_off();
        fx.exhaust(&AgentEffectPolicies::default())
            .expect("the exhausted result records");

        assert_eq!(fx.send().status, AgentRunEffectStatus::Succeeded);
        assert!(fx.run().loop_state.open_checkpoints().is_empty());
        assert_eq!(fx.run().status, AgentRunStatus::HandedOff);
    }

    #[test]
    fn a_send_result_after_a_refused_notice_settles_the_send_executed() {
        let mut fx = handoff_send_fixture(
            AgentRunStatus::WaitingForEffect,
            AgentHandoffStatus::Refused {
                code: "agent-not-instantiated".to_string(),
            },
            AgentRunEffectStatus::Ready,
        );
        fx.exhaust(&AgentEffectPolicies::default())
            .expect("the exhausted result records");

        assert_eq!(fx.send().status, AgentRunEffectStatus::Succeeded);
        assert!(fx.run().loop_state.open_checkpoints().is_empty());
        assert_eq!(fx.run().status, AgentRunStatus::Running);
        assert_eq!(
            fx.run().loop_state.phase(),
            AgentLoopPhase::RecordingTurn,
            "the turn rests and the run resumes"
        );
        assert!(fx.run().terminal_reason.is_none());
    }

    /// The same holds for an unknown outcome: a refusal the task already
    /// recorded proves the send reached it.
    #[test]
    fn an_ambiguous_send_result_after_a_refused_notice_parks_nothing() {
        let mut fx = handoff_send_fixture(
            AgentRunStatus::WaitingForEffect,
            AgentHandoffStatus::Refused {
                code: "agent-not-instantiated".to_string(),
            },
            AgentRunEffectStatus::Ready,
        );
        let (effect_id, generation, now) = (fx.effect_id.clone(), fx.generation, fx.now);
        record_effect_result(
            &mut fx.state,
            &effect_id,
            generation,
            1,
            0,
            AgentRunEffectOutcome::indeterminate("connection-lost", "the worker died mid-send"),
            &AgentSchemaPolicy::default(),
            &AgentEffectPolicies::default(),
            now,
        )
        .expect("the ambiguous result records");

        assert_eq!(fx.send().status, AgentRunEffectStatus::Succeeded);
        assert!(fx.run().loop_state.open_checkpoints().is_empty());
        assert_eq!(fx.run().status, AgentRunStatus::Running);
    }
```

- [ ] **Step 2: Run the unit tests to see them fail**

Run: `cargo test -p rakka-agent --lib -- run::tests::an_exhausted_handoff_send run::tests::a_parked_handoff_sends_checkpoint run::tests::a_send_result_after run::tests::an_ambiguous_send_result`

Expected, all five failing:
- the first two at `open.len()` (left `0`, right `1`);
- the two `a_send_result_after_*` at `fx.send().status` (left `Indeterminate`, right `Succeeded`);
- the last at `fx.send().status` (left `Indeterminate`).

- [ ] **Step 3: Write the integration world and its failing test**

Create `crates/rakka-agent/tests/handoff_send_park.rs`:

```rust
//! Issue #106: an exhausted handoff send parks behind a reconciliation
//! checkpoint, and the task's handoff result releases it.
//!
//! A handoff send whose retry budget spends out without an answer parks its
//! run `Indeterminate`: the executor's probe could not establish whether the
//! task recorded the transfer
//! ([specification 8.9](../../docs/plans/rakka-agent/spec.md)). These worlds
//! lose the send's answer after the task recorded the transfer, or before it
//! reached the task, and drive the real task, the scripted dispatcher, and
//! the run entity to quiescence.

mod common;

use std::sync::Arc;

use common::{
    goal_spec_draft, goal_spec_with_handoff, goal_task_creation_command, handoff_config,
    handoff_target_scope, handoff_tool_id, task_definition, task_scope, ApplyingHandoffExecutor,
    Fixture, HANDOFF_SKILL,
};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    AgentA2aHandoffFinding, AgentA2aHandoffSendExecutor, AgentBudgetAllocation,
    AgentBudgetCeilings, AgentBudgetDimension, AgentCheckpoint, AgentCheckpointKind,
    AgentDispatchError, AgentDispatchFuture, AgentHandoffRecord, AgentModelTurn, AgentRun,
    AgentRunEffect, AgentRunEffectKind, AgentRunEffectStatus, AgentRunScope, AgentRunStatus,
    AgentSegmentOperation, AgentTaskContent, AgentTaskDefinition, AgentToolCallId,
    AgentToolCallRequest, InMemoryAgentSegmentSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::AgentEphemeralCredential;
use serde_json::json;

fn handoff_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Transferring the ticket to billing.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("the call id is valid"),
                handoff_tool_id(),
                json!({ "skill": HANDOFF_SKILL, "reason": "needs billing authority" }),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(json!({ "answer": "resolved" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// A bounded per-run allocation, so the source's open escrow leaves headroom
/// for the target's generation; `handoff_record.rs` documents why an
/// exact-fit budget makes every transfer unaffordable.
fn handoff_task_definition() -> AgentTaskDefinition {
    let mut per_run = AgentBudgetAllocation::unbounded();
    per_run.set(AgentBudgetDimension::LoopIterations, Some(3));
    task_definition()
        .with_budgets(AgentBudgetCeilings {
            max_loop_iterations: Some(12),
            ..AgentBudgetCeilings::unbounded()
        })
        .with_run_allocation(per_run)
}

/// A handoff executor whose answer is always lost.
///
/// With `applying` set, the transfer reaches the task first: the write
/// landed, and only the reply was lost. Without it, the send never reached
/// the task. Either way the attempt fails retryably. The scripted
/// dispatcher records that as a spent budget (`Exhausted`), which is what
/// the real dispatcher records once the executor's probe has failed on
/// every attempt.
struct LostAnswerExecutor {
    applying: Option<Arc<ApplyingHandoffExecutor>>,
}

impl AgentA2aHandoffSendExecutor for LostAnswerExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        handoff: &'a AgentHandoffRecord,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentA2aHandoffFinding> {
        Box::pin(async move {
            if let Some(applying) = &self.applying {
                let _ = applying.apply(handoff).await;
            }
            Err(AgentDispatchError::Invocation {
                code: "handoff-unrecorded",
                message: "the answer was lost and the probe could not read the task".to_string(),
            })
        })
    }
}

/// A world one turn from committing the transfer. `record_transfer` says
/// whether the lost send reached the task, and `target_ready` whether the
/// target agent exists to accept it (a missing target refuses).
async fn parking_world(
    record_transfer: bool,
    target_ready: bool,
) -> (Fixture, Arc<InMemoryAgentSegmentSink>) {
    let segments = Arc::new(InMemoryAgentSegmentSink::new());
    let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn(handoff_turn())
            .with_turn(proposing_turn()),
    ))
    .with_delegation(handoff_config())
    .with_segments(segments.clone());
    let executor = Arc::new(LostAnswerExecutor {
        applying: record_transfer.then(|| ApplyingHandoffExecutor::over(&fixture)),
    });
    let _ = fixture
        .dispatcher
        .clone()
        .with_a2a_handoff_executor(executor);
    fixture.instantiate_agent().await;
    fixture
        .apply_task_command(goal_task_creation_command(
            handoff_task_definition(),
            goal_spec_draft(goal_spec_with_handoff(), true),
        ))
        .await
        .expect("the goal task creates");
    if target_ready {
        fixture.instantiate_agent_at(handoff_target_scope()).await;
    }
    (fixture, segments)
}

/// Drives the world to quiescence. First the run's loop and the dispatcher;
/// then the task's remaining legs (the offer, the target's answer, and the
/// handoff result back to the source); then the run again, so a resumed
/// source finishes its task.
async fn settle(fixture: &Fixture) {
    fixture.pump().await.expect("the loop quiesces");
    for _ in 0..4 {
        fixture
            .settle_task_at(&task_scope())
            .await
            .expect("the task settles");
    }
    fixture.pump().await.expect("the loop quiesces again");
}

/// The run's durable record, re-materialized: recovery is a read path.
async fn run_record(fixture: &Fixture) -> AgentRun {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("the run recovers");
    run.state()
        .expect("the state reads")
        .run()
        .expect("the run exists")
        .clone()
}

fn send_effect(run: &AgentRun) -> &AgentRunEffect {
    run.loop_state
        .effects()
        .iter()
        .find(|effect| effect.kind() == AgentRunEffectKind::A2aSendCall)
        .expect("the handoff send is held")
}

/// How many `checkpoint-open` segments the run emitted: one per park.
fn checkpoint_opens(segments: &InMemoryAgentSegmentSink) -> usize {
    segments
        .segments()
        .into_iter()
        .filter(|segment| matches!(segment.operation, AgentSegmentOperation::CheckpointOpen))
        .count()
}

/// Issue #106, Ask 1: a send that never reached the task parks behind its
/// reconciliation checkpoint and waits there. No notice is owed, so only a
/// decision can end the wait, and the checkpoint is what offers one.
#[tokio::test]
async fn an_exhausted_send_that_never_reached_the_task_waits_behind_its_checkpoint() {
    let (fixture, segments) = parking_world(false, true).await;
    settle(&fixture).await;

    let run = run_record(&fixture).await;
    assert_eq!(run.status, AgentRunStatus::WaitingForReconciliation);
    let send = send_effect(&run);
    assert_eq!(send.status, AgentRunEffectStatus::Indeterminate);
    let open = run.loop_state.open_checkpoints();
    assert_eq!(open.len(), 1, "the park carries its checkpoint: {open:?}");
    assert_eq!(
        open[0].kind,
        AgentCheckpointKind::IndeterminateEffectReconciliation
    );
    assert_eq!(
        open[0].checkpoint_id,
        AgentCheckpoint::id_for_effect(
            &send.effect_id,
            send.generation,
            AgentCheckpointKind::IndeterminateEffectReconciliation,
        )
    );
    assert_eq!(open[0].bound_effect.effect_id, send.effect_id);
    assert_eq!(
        checkpoint_opens(&segments),
        1,
        "the park emitted its checkpoint-open segment"
    );
    let task = fixture.task_snapshot().await;
    assert!(task.handoff.is_none(), "the transfer never reached the task");
}
```

- [ ] **Step 4: Run the integration test to see it fail**

Run: `cargo test -p rakka-agent --test handoff_send_park`
Expected: FAIL at `open.len()` (left `0`, right `1`).

- [ ] **Step 5: Add the classifier**

In `crates/rakka-agent/src/run.rs`, directly above the doc comment of `fn apply_effect_outcome` ("Applies one established outcome to the effect that owes it and to the loop."), add:

```rust
/// How the run's handoff cell stands toward one send effect
/// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
///
/// Every site that settles a handoff send's generation asks this one
/// question:
/// - an effect the cell does not hold is not a handoff send;
/// - a transfer still awaiting the target is the slice 5.1 arm;
/// - a transfer the task already answered proves the send executed,
///   whatever the dispatcher reports;
/// - a cell the run settled itself takes the shared wind-down (issue #106).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandoffSend {
    /// The effect is not the run's handoff send.
    NotHeld,
    /// The cell holds the effect, and the transfer awaits the target.
    Unresolved,
    /// The task's handoff result settled the cell `Accepted` or `Refused`:
    /// the task owes one only for a transfer it recorded, so the send reached
    /// it.
    ResolvedByTask,
    /// The run settled the cell `Failed` on its own decision.
    SettledLocally,
}

/// Classifies one effect against the run's handoff cell.
fn handoff_send(loop_state: &AgentLoopState, effect_id: &AgentEffectId) -> HandoffSend {
    let Some(cell) = loop_state.handoff() else {
        return HandoffSend::NotHeld;
    };
    if cell.record.effect != *effect_id {
        return HandoffSend::NotHeld;
    }
    match &cell.status {
        crate::coordination::AgentHandoffStatus::Pending
        | crate::coordination::AgentHandoffStatus::Sent { .. } => HandoffSend::Unresolved,
        crate::coordination::AgentHandoffStatus::Accepted { .. }
        | crate::coordination::AgentHandoffStatus::Refused { .. } => HandoffSend::ResolvedByTask,
        crate::coordination::AgentHandoffStatus::Failed { .. } => HandoffSend::SettledLocally,
    }
}
```

- [ ] **Step 6: Classify in `apply_effect_outcome`, and settle a task-resolved send as executed**

In `apply_effect_outcome`, replace:

```rust
    let winding_down = {
        let run = state.run_mut()?;
        run.terminal_reason.is_some() || run.status == AgentRunStatus::Cancelling
    };

    let run = state.run_mut()?;
```

with:

```rust
    let winding_down = {
        let run = state.run_mut()?;
        run.terminal_reason.is_some() || run.status == AgentRunStatus::Cancelling
    };
    // How the run's handoff cell stands toward this effect, read before the
    // effect is borrowed (issue #106).
    let handoff_standing = state
        .run()
        .map_or(HandoffSend::NotHeld, |run| handoff_send(&run.loop_state, effect_id));

    let run = state.run_mut()?;
```

Then make this the **first** arm of `match outcome {` in the same function, directly before `AgentRunEffectOutcome::Model { turn } => {`:

```rust
        // The task's handoff result already settled the cell. The send reached
        // the task, and the task's durable record is the proof the attempt's
        // own answer could not give. Whatever the attempt reported (a failure,
        // a spent budget, an unknown outcome), the generation executed: it
        // settles `Succeeded`, and the cell and terminal reason the notice set
        // stand. A live run that is not winding down rests its turn, as the
        // A2aHandoff arm does (issue #106).
        AgentRunEffectOutcome::Failed { .. }
        | AgentRunEffectOutcome::Exhausted { .. }
        | AgentRunEffectOutcome::Indeterminate { .. }
            if handoff_standing == HandoffSend::ResolvedByTask =>
        {
            effect.status = AgentRunEffectStatus::Succeeded;
            let run = state.run_mut()?;
            if !winding_down && !run.loop_state.awaits_turn_effect() {
                let (phase, status) = turn_rest(&run.loop_state);
                run.loop_state.set_phase(phase);
                run.status = status;
            }
        }
```

- [ ] **Step 7: Narrow the handoff branch to an unresolved transfer**

In the `Failed | Exhausted` arm, replace:

```rust
            // `Indeterminate` for a reconciliation decision instead, with
            // the cell still holding the fence.
            let handoff_held = failed_kind == AgentRunEffectKind::A2aSendCall
                && state.run().is_some_and(|run| {
                    run.loop_state
                        .handoff()
                        .is_some_and(|cell| cell.record.effect == *effect_id)
                });
```

with:

```rust
            // `Indeterminate` for a reconciliation decision instead, with
            // the cell still holding the fence, and `record_effect_result`
            // opens the generation's reconciliation checkpoint.
            //
            // The branch is for a transfer still awaiting the target
            // (issue #106). A cell the task already answered settled the send
            // as executed in the arm above. A cell the run settled itself (an
            // abandoned transfer) takes the shared wind-down below, so the run
            // fails as the decision says.
            let handoff_held = handoff_standing == HandoffSend::Unresolved;
```

- [ ] **Step 8: Open the checkpoint wherever the effect ends `Indeterminate`**

In `record_effect_result`, replace:

```rust
    apply_effect_outcome(state, effect_id, &outcome, now)?;
    // An ambiguous outcome parks the run behind a durable reconciliation
    // checkpoint in the same transition that recorded it, so the wait carries
    // the full record surface — decision set, dedup key, SLA escalation,
    // roles — a bare indeterminate effect could not
    // ([specification 12.1](../../../docs/plans/rakka-agent/spec.md),
    // [12.2](../../../docs/plans/rakka-agent/spec.md)).
    //
```

with:

```rust
    apply_effect_outcome(state, effect_id, &outcome, now)?;
    // A transition that leaves the generation `Indeterminate` parks the run
    // behind a durable reconciliation checkpoint in the same transition, so
    // the wait carries the full record surface — decision set, dedup key,
    // SLA escalation, roles — a bare indeterminate effect could not
    // ([specification 12.1](../../../docs/plans/rakka-agent/spec.md),
    // [12.2](../../../docs/plans/rakka-agent/spec.md)). The generation's status
    // decides, not the outcome. An `Indeterminate` outcome parks it, and so
    // does a handoff send's `Exhausted` one, whose probe could not establish
    // the transfer; a park with no checkpoint was the defect of issue #106.
    //
```

and replace:

```rust
    let live = state.run().is_some_and(|run| !run.status.is_terminal());
    if live && matches!(outcome, AgentRunEffectOutcome::Indeterminate { .. }) {
```

with:

```rust
    let live = state.run().is_some_and(|run| !run.status.is_terminal());
    let parked = state.run().is_some_and(|run| {
        run.loop_state
            .indeterminate_effects()
            .any(|effect| effect.effect_id == *effect_id && effect.generation == generation)
    });
    if live && parked {
```

- [ ] **Step 9: Run the tests to see them pass**

Run: `cargo test -p rakka-agent --lib -- run::tests::an_exhausted_handoff_send run::tests::a_parked_handoff_sends_checkpoint run::tests::a_send_result_after run::tests::an_ambiguous_send_result`
Expected: 5 passed.

Run: `cargo test -p rakka-agent --test handoff_send_park`
Expected: 1 passed.

Run: `cargo test -p rakka-agent --test handoff_record --test handoff_recovery --test handoff_cancellation --test terminal_handoff_resolution --test checkpoint_reconciliation`
Expected: all pass. The existing handoff and reconciliation behaviour is unchanged.

- [ ] **Step 10: Falsify the three changes**

Break each in turn, run its tests, see them fail, then restore:
- Revert the `if live && parked` condition to the old `matches!(outcome, …Indeterminate…)`. Expect `an_exhausted_handoff_send_parks…`, `a_parked_handoff_sends_checkpoint…` and the integration test to fail.
- Delete the new first match arm. Expect all three `*_after_*` tests to fail.
- Change `handoff_held` to `handoff_standing != HandoffSend::NotHeld`. Nothing in Task 1 should fail (Task 3 pins it). Record that the narrowing is pinned by Task 3's abandonment test.

- [ ] **Step 11: Lint, format and commit**

Run: `cargo fmt --all` then `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings`
Expected: no warnings.

```bash
git add crates/rakka-agent/src/run.rs crates/rakka-agent/tests/handoff_send_park.rs
git commit -m "Park an exhausted handoff send behind its reconciliation checkpoint (issue #106)

The handoff arm turns an Exhausted send into an Indeterminate generation,
but record_effect_result opened the checkpoint only for an Indeterminate
outcome, so the run waited with nothing to decide on. The checkpoint now
opens whenever the transition leaves the effect Indeterminate. A send
result that arrives after the task's handoff result settles executed, as
the task's record proves.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Then run `cargo clean` before the next task.

---

### Task 2: The task's answer releases the park (D3)

**Files:**
- Modify: `crates/rakka-agent/src/run.rs`:
  - two new private fns just above `fn accept_handoff_result` (~1783);
  - the head of `accept_handoff_result`'s `apply` closure (~1905);
  - the test module: helpers and five tests.
- Modify: `crates/rakka-agent/tests/handoff_send_park.rs`: two tests and their imports.

**Interfaces:**
- Consumes: `HandoffSend`, `handoff_send`, `HandoffSendFixture`, `handoff_send_fixture` (Task 1); the existing `apply_effect_outcome` and `drop_reconciliation_checkpoint(state, effect_id, generation)`.
- Produces (crate-private, in `run.rs`):
  ```rust
  fn handoff_answer_receipt(
      handoff: &crate::identity::AgentHandoffId,
      target_generation: Option<AgentAssignmentGeneration>,
      accepted: bool,
  ) -> crate::coordination::AgentA2aHandoffReceipt
  fn settle_parked_handoff_send(
      state: &mut AgentRunState,
      receipt: crate::coordination::AgentA2aHandoffReceipt,
      now: AgentTimestampMillis,
  ) -> AgentRunResult<bool>
  ```
- Produces (test module): `fn parked() -> HandoffSendFixture`, `fn accepted()` / `fn refused() -> crate::task::AgentHandoffResolutionNotice`, `HandoffSendFixture::notice(resolution) -> AgentExchangeEnvelope`, `fn operator() -> PrincipalRef`, `fn decision_key(&str) -> crate::AgentOperationId`, `fn reconcile(crate::checkpoints::AgentReconciliationDecision) -> AgentCheckpointDecision`.

- [ ] **Step 1: Write the failing unit tests**

In `run.rs`'s test module, add to `impl HandoffSendFixture`:

```rust
        /// The handoff-result exchange the task owes this run.
        fn notice(
            &self,
            resolution: crate::task::AgentHandoffResolutionNotice,
        ) -> AgentExchangeEnvelope {
            let task_scope = AgentTaskScope::new(self.scope.tenant().clone(), self.task.clone())
                .expect("the task scope is valid");
            let operation_id = crate::coordination::handoff_result_operation_id(
                self.scope.tenant(),
                &self.handoff,
            )
            .expect("the operation id derives");
            let notice = crate::task::AgentHandoffResultNotice {
                task: task_scope.clone(),
                handoff: self.handoff.clone(),
                resolution,
            };
            AgentExchangeEnvelope::new(
                operation_id.clone(),
                AgentExchangeKind::HandoffResult,
                AgentEntityAddress::Task(task_scope),
                AgentEntityAddress::Run(self.scope.clone()),
                AgentExchangePayload::encode(crate::task::AGENT_HANDOFF_RESULT_PAYLOAD_TYPE, &notice)
                    .expect("the payload encodes"),
                rakka_agent_workflow::AgentCorrelationId::new(operation_id.as_str()),
                self.now,
            )
            .expect("the envelope builds")
        }
```

and add at the end of `mod tests`:

```rust
    /// A run whose handoff send parked on exhaustion, behind its checkpoint.
    fn parked() -> HandoffSendFixture {
        let mut fx = handoff_send_fixture(
            AgentRunStatus::WaitingForEffect,
            AgentHandoffStatus::Pending,
            AgentRunEffectStatus::Ready,
        );
        fx.exhaust(&AgentEffectPolicies::default())
            .expect("the exhausted result records");
        assert_eq!(
            fx.run().loop_state.open_checkpoints().len(),
            1,
            "the park carries its checkpoint"
        );
        fx
    }

    fn accepted() -> crate::task::AgentHandoffResolutionNotice {
        crate::task::AgentHandoffResolutionNotice::Accepted {
            target_run: AgentRunId::new("ticket-1-gen-2").expect("the run id is valid"),
            generation: AgentAssignmentGeneration::new(2),
        }
    }

    fn refused() -> crate::task::AgentHandoffResolutionNotice {
        crate::task::AgentHandoffResolutionNotice::Refused {
            code: "agent-not-instantiated".to_string(),
        }
    }

    fn operator() -> PrincipalRef {
        PrincipalRef {
            principal_type: "user".to_string(),
            principal_id: "operator".to_string(),
            display_name: None,
        }
    }

    fn decision_key(discriminator: &str) -> crate::AgentOperationId {
        crate::AgentOperationId::new(
            crate::AgentOperationKind::CheckpointResolution,
            ["acme", "support-agent", discriminator],
        )
        .expect("the decision key derives")
    }

    fn reconcile(
        decision: crate::checkpoints::AgentReconciliationDecision,
    ) -> AgentCheckpointDecision {
        AgentCheckpointDecision::Reconciliation(decision)
    }

    /// Issue #106, Ask 2: the task's acceptance is the proof the parked
    /// send's checkpoint waits for. The send settles executed, the checkpoint
    /// drops, and the run hands off, all in the notice's compare-and-set.
    #[test]
    fn an_accepted_notice_settles_a_parked_send_and_hands_off() {
        let mut fx = parked();
        let envelope = fx.notice(accepted());
        let result = accept_handoff_result(&mut fx.state, &envelope, fx.now);

        assert!(result.is_accepted(), "the notice settles the exchange");
        assert_eq!(fx.send().status, AgentRunEffectStatus::Succeeded);
        assert!(fx.run().loop_state.open_checkpoints().is_empty());
        assert!(matches!(
            fx.cell().status,
            AgentHandoffStatus::Accepted { .. }
        ));
        assert_eq!(fx.run().status, AgentRunStatus::HandedOff);
        assert!(matches!(
            fx.run().terminal_reason,
            Some(AgentRunTerminalReason::HandedOff { .. })
        ));
    }

    /// Ruling 1: a refusal is owed only for a transfer the task recorded, so
    /// it too proves the send executed. The send settles `Succeeded` (never
    /// "not executed", which would send the handoff again), and the run
    /// resumes as a refused handoff does.
    #[test]
    fn a_refused_notice_settles_a_parked_send_and_the_run_resumes() {
        let mut fx = parked();
        let envelope = fx.notice(refused());
        let result = accept_handoff_result(&mut fx.state, &envelope, fx.now);

        assert!(result.is_accepted(), "the notice settles the exchange");
        assert_eq!(fx.send().status, AgentRunEffectStatus::Succeeded);
        assert!(fx.run().loop_state.open_checkpoints().is_empty());
        assert!(matches!(
            fx.cell().status,
            AgentHandoffStatus::Refused { .. }
        ));
        assert_eq!(fx.run().status, AgentRunStatus::Running);
        assert_eq!(fx.run().loop_state.phase(), AgentLoopPhase::RecordingTurn);
        assert!(fx.run().terminal_reason.is_none());
        assert!(!fx.run().loop_state.handoff_fenced(), "the fence released");
    }

    /// Review focus 1: a parked run whose cancellation was requested while it
    /// waited ends as an unparked one would. An acceptance wins with
    /// `HandedOff`; a refusal releases it into its cancellation.
    #[test]
    fn a_notice_to_a_parked_cancelled_run_ends_it_as_an_unparked_one_would() {
        let mut fx = parked();
        cancel(&mut fx.state, "operator".to_string(), fx.now).expect("the cancellation records");
        assert_eq!(fx.run().status, AgentRunStatus::WaitingForReconciliation);
        let envelope = fx.notice(refused());
        assert!(accept_handoff_result(&mut fx.state, &envelope, fx.now).is_accepted());
        assert_eq!(fx.run().status, AgentRunStatus::Cancelled);

        let mut fx = parked();
        cancel(&mut fx.state, "operator".to_string(), fx.now).expect("the cancellation records");
        let envelope = fx.notice(accepted());
        assert!(accept_handoff_result(&mut fx.state, &envelope, fx.now).is_accepted());
        assert_eq!(fx.run().status, AgentRunStatus::HandedOff);
    }

    /// Review focus 2: the courier delivering the same notice again after it
    /// released the park changes nothing. The settled cell absorbs it.
    #[test]
    fn a_replayed_notice_after_the_release_changes_nothing() {
        let mut fx = parked();
        let envelope = fx.notice(refused());
        assert!(accept_handoff_result(&mut fx.state, &envelope, fx.now).is_accepted());
        let (status, phase) = (fx.run().status, fx.run().loop_state.phase());

        assert!(accept_handoff_result(&mut fx.state, &envelope, fx.now).is_accepted());
        assert_eq!(fx.run().status, status);
        assert_eq!(fx.run().loop_state.phase(), phase);
        assert_eq!(fx.send().status, AgentRunEffectStatus::Succeeded);
        assert!(fx.run().loop_state.open_checkpoints().is_empty());
    }

    /// Review focus 3: a decision submitted after the notice released the
    /// park is refused under the ordinary codes, never applied twice.
    #[test]
    fn a_decision_after_the_release_is_refused() {
        let policy = AgentSchemaPolicy::default();
        let policies = AgentEffectPolicies::default();

        // A run that resumed: the checkpoint is gone, and the send resolved.
        let mut fx = parked();
        let checkpoint_id = fx.checkpoint_id();
        let envelope = fx.notice(refused());
        assert!(accept_handoff_result(&mut fx.state, &envelope, fx.now).is_accepted());
        let error = resolve_checkpoint(
            &mut fx.state,
            &checkpoint_id,
            operator(),
            reconcile(crate::checkpoints::AgentReconciliationDecision::ConfirmedNotExecuted),
            decision_key("late"),
            &policy,
            &policies,
            fx.now,
        )
        .expect_err("the checkpoint is gone");
        assert_eq!(error.code(), "run-unknown-checkpoint");
        let (effect_id, generation, now) = (fx.effect_id.clone(), fx.generation, fx.now);
        let error = resolve_indeterminate_effect(
            &mut fx.state,
            &effect_id,
            generation,
            AgentEffectResolution::ConfirmedNotExecuted,
            &policy,
            &policies,
            now,
        )
        .expect_err("the send is resolved");
        assert_eq!(error.code(), "run-stale-effect-result");

        // A run that handed off: it is terminal.
        let mut fx = parked();
        let checkpoint_id = fx.checkpoint_id();
        let envelope = fx.notice(accepted());
        assert!(accept_handoff_result(&mut fx.state, &envelope, fx.now).is_accepted());
        let error = resolve_checkpoint(
            &mut fx.state,
            &checkpoint_id,
            operator(),
            reconcile(crate::checkpoints::AgentReconciliationDecision::ConfirmedNotExecuted),
            decision_key("late"),
            &policy,
            &policies,
            fx.now,
        )
        .expect_err("the run is terminal");
        assert_eq!(error.code(), "run-terminal");
    }
```

- [ ] **Step 2: Run the unit tests to see them fail**

Run: `cargo test -p rakka-agent --lib -- run::tests::an_accepted_notice_settles run::tests::a_refused_notice_settles run::tests::a_notice_to_a_parked_cancelled run::tests::a_replayed_notice_after run::tests::a_decision_after_the_release`

Expected, all five failing:
- `an_accepted_notice…` at `fx.send().status` (left `Indeterminate`);
- `a_refused_notice…` the same;
- `a_notice_to_a_parked_cancelled…` at the first status (left `WaitingForReconciliation`, right `Cancelled`);
- `a_replayed_notice…` at `fx.send().status`;
- `a_decision_after_the_release…` at the first `expect_err`, because the checkpoint was never dropped and the decision applied.

- [ ] **Step 3: Write the failing integration tests**

In `crates/rakka-agent/tests/handoff_send_park.rs`, add `AgentHandoffStatus`, `AgentRunTerminalReason`, `AgentTaskHandoffStatus` and `AgentTaskStatus` to the `use rakka_agent::{…}` list (in alphabetical order), and append:

```rust
/// Issue #106, Ask 2: the transfer landed and only the answer was lost. The
/// run parks behind its checkpoint, and the task's acceptance (its durable
/// record of the transfer) releases it into `HandedOff` with no decision.
#[tokio::test]
async fn a_lost_answer_whose_transfer_landed_hands_off_once_the_task_answers() {
    let (fixture, segments) = parking_world(true, true).await;
    settle(&fixture).await;

    let run = run_record(&fixture).await;
    assert_eq!(
        run.status,
        AgentRunStatus::HandedOff,
        "the task's acceptance released the park"
    );
    assert!(matches!(
        run.terminal_reason,
        Some(AgentRunTerminalReason::HandedOff { .. })
    ));
    assert_eq!(
        send_effect(&run).status,
        AgentRunEffectStatus::Succeeded,
        "the task's record proved the send executed"
    );
    assert!(
        run.loop_state.open_checkpoints().is_empty(),
        "the release dropped the checkpoint"
    );
    assert_eq!(
        checkpoint_opens(&segments),
        1,
        "the send parked behind its checkpoint first"
    );
    let task = fixture.task_snapshot().await;
    let provenance = task.handoff.as_deref().expect("the task recorded the transfer");
    assert!(matches!(provenance.status, AgentTaskHandoffStatus::Accepted));
}

/// Ruling 1, end to end: the target refused, so the task restored the source,
/// and its refusal releases the park. The source resumes and finishes the
/// task itself.
#[tokio::test]
async fn a_lost_answer_the_target_refused_resumes_the_source() {
    let (fixture, segments) = parking_world(true, false).await;
    settle(&fixture).await;

    let run = run_record(&fixture).await;
    assert_eq!(
        run.status,
        AgentRunStatus::Completed,
        "the source resumed past the refusal and finished the task"
    );
    let cell = run.loop_state.handoff().expect("the cell survives");
    assert!(
        matches!(&cell.status, AgentHandoffStatus::Refused { code } if code == "agent-not-instantiated"),
        "the cell settles under the target's refusal, got {:?}",
        cell.status
    );
    assert!(run.loop_state.open_checkpoints().is_empty());
    assert_eq!(checkpoint_opens(&segments), 1, "the send parked first");
    let task = fixture.task_snapshot().await;
    assert_eq!(task.status, AgentTaskStatus::Completed);
}
```

- [ ] **Step 4: Run them to see them fail**

Run: `cargo test -p rakka-agent --test handoff_send_park`
Expected: the two new tests fail at `run.status` (left `WaitingForReconciliation`); the Task 1 test passes.

- [ ] **Step 5: Implement the receipt and the settle**

In `run.rs`, directly above the doc comment of `fn accept_handoff_result`, add:

```rust
/// The receipt a task's handoff answer stands in for when it settles a
/// parked send ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
///
/// The send reached the task, and the task's own durable record is the proof.
/// The receipt is never persisted (the A2aHandoff arm reads only its handoff
/// id and generation), so its status label names the answer it was built
/// from.
fn handoff_answer_receipt(
    handoff: &crate::identity::AgentHandoffId,
    target_generation: Option<AgentAssignmentGeneration>,
    accepted: bool,
) -> crate::coordination::AgentA2aHandoffReceipt {
    crate::coordination::AgentA2aHandoffReceipt {
        handoff: handoff.clone(),
        target_generation,
        peer_status: if accepted { "accepted" } else { "refused" }.to_string(),
    }
}

/// Resolves the run's parked handoff send as executed, from the task's
/// answer, and drops the reconciliation checkpoint parked on it
/// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md),
/// [11.5](../../../docs/plans/rakka-agent/spec.md),
/// [12.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// The resolution is exactly the one a `ConfirmedExecuted` decision carrying
/// the receipt would apply. The generation's attempts were billed when it
/// parked, so nothing is billed again, and the evidence the park recorded
/// stays as the generation's history. Answers whether there was a parked send
/// to settle: a live run whose handoff cell holds an `Indeterminate` send
/// (issue #106).
fn settle_parked_handoff_send(
    state: &mut AgentRunState,
    receipt: crate::coordination::AgentA2aHandoffReceipt,
    now: AgentTimestampMillis,
) -> AgentRunResult<bool> {
    let parked = state.run().and_then(|run| {
        if run.status.is_terminal() {
            return None;
        }
        let cell = run.loop_state.handoff()?;
        run.loop_state
            .indeterminate_effects()
            .find(|effect| effect.effect_id == cell.record.effect)
            .map(|effect| (effect.effect_id.clone(), effect.generation))
    });
    let Some((effect_id, generation)) = parked else {
        return Ok(false);
    };
    apply_effect_outcome(
        state,
        &effect_id,
        &AgentRunEffectOutcome::A2aHandoff { receipt },
        now,
    )?;
    drop_reconciliation_checkpoint(state, &effect_id, generation)?;
    Ok(true)
}
```

Then in `accept_handoff_result`, replace:

```rust
    let apply = || -> AgentRunResult<()> {
        match &notice.resolution {
```

with:

```rust
    let apply = || -> AgentRunResult<()> {
        // A send parked `Indeterminate`, because its probe could not read the
        // transfer, is settled first. This notice is the task's durable record
        // of the transfer, which is exactly what the reconciliation checkpoint
        // waits for. Both answers prove the send executed (a refusal is owed
        // only for a transfer the task recorded), so the generation resolves
        // as executed and its checkpoint drops, in this same compare-and-set
        // (issue #106).
        let receipt = match &notice.resolution {
            crate::task::AgentHandoffResolutionNotice::Accepted { generation, .. } => {
                handoff_answer_receipt(&notice.handoff, Some(*generation), true)
            }
            crate::task::AgentHandoffResolutionNotice::Refused { .. } => {
                handoff_answer_receipt(&notice.handoff, None, false)
            }
        };
        settle_parked_handoff_send(state, receipt, now)?;
        match &notice.resolution {
```

- [ ] **Step 6: Run the tests to see them pass**

Run: `cargo test -p rakka-agent --lib -- run::tests::an_accepted_notice_settles run::tests::a_refused_notice_settles run::tests::a_notice_to_a_parked_cancelled run::tests::a_replayed_notice_after run::tests::a_decision_after_the_release run::tests::a_contradicted_accepted_resolution`
Expected: 6 passed (the last is the existing slice 5.1 correction test).

Run: `cargo test -p rakka-agent --test handoff_send_park --test handoff_record --test handoff_recovery --test terminal_handoff_resolution`
Expected: all pass.

- [ ] **Step 7: Falsify**

Comment out the `settle_parked_handoff_send(state, receipt, now)?;` line. Expect all five new unit tests and both new integration tests to fail. Restore it.

Then replace `drop_reconciliation_checkpoint(state, &effect_id, generation)?;` with nothing. Expect `an_accepted_notice…` and `a_refused_notice…` to fail on `open_checkpoints().is_empty()`. Restore it.

- [ ] **Step 8: Lint, format and commit**

Run: `cargo fmt --all` then `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings`

```bash
git add crates/rakka-agent/src/run.rs crates/rakka-agent/tests/handoff_send_park.rs
git commit -m "Release a parked handoff send on the task's handoff result (issue #106)

A handoff result reaching a run whose send is parked Indeterminate now
settles the send as executed and drops its reconciliation checkpoint in
the same compare-and-set. Before, the notice set HandedOff (or resumed
the run) but left the effect Indeterminate, so the run stayed parked.
A refusal settles the send executed too: the task owes one only for a
transfer it recorded.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Then run `cargo clean`.

---

### Task 3: The decisions on a handoff send's checkpoint (D5)

**Files:**
- Modify: `crates/rakka-agent/src/run.rs`:
  - `apply_indeterminate_resolution` (~4957): classify before the effect borrow, and refuse after the status fence;
  - `resolve_checkpoint`'s `Abandoned` arm (~5325);
  - `schedule_compensation` (~5421);
  - the test module: four tests.
- Modify: `crates/rakka-agent/src/checkpoints.rs:562-571` (the `Compensate` and `AbandonAndFail` docs).
- Modify: `crates/rakka-agent/tests/handoff_send_park.rs`: one test and its imports.

**Interfaces:**
- Consumes: `HandoffSend`, `handoff_send` (Task 1); `parked`, `operator`, `decision_key`, `reconcile` (Task 2 test helpers).
- Produces: no new names.

- [ ] **Step 1: Write the failing unit tests**

Append to `run.rs`'s `mod tests`:

```rust
    /// An exhausted outcome is what parked the send. Confirming it resolves
    /// nothing, and applying it re-parked the send with its checkpoint
    /// already dropped, so both paths that carry it refuse it.
    #[test]
    fn confirming_an_exhausted_handoff_send_is_refused() {
        let exhausted = || AgentEffectResolution::ConfirmedExecuted {
            outcome: Box::new(AgentRunEffectOutcome::exhausted(
                "a2a-handoff-attempt-failed",
                "the operator saw the budget spend out",
            )),
        };
        let policy = AgentSchemaPolicy::default();
        let policies = AgentEffectPolicies::default();

        let mut fx = parked();
        let (effect_id, generation, now) = (fx.effect_id.clone(), fx.generation, fx.now);
        let error = resolve_indeterminate_effect(
            &mut fx.state,
            &effect_id,
            generation,
            exhausted(),
            &policy,
            &policies,
            now,
        )
        .expect_err("an exhausted confirmation resolves nothing");
        assert_eq!(error.code(), "effect-policy-invalid");
        assert_eq!(fx.send().status, AgentRunEffectStatus::Indeterminate);
        assert_eq!(
            fx.run().loop_state.open_checkpoints().len(),
            1,
            "the wait is untouched"
        );

        let mut fx = parked();
        let checkpoint_id = fx.checkpoint_id();
        let error = resolve_checkpoint(
            &mut fx.state,
            &checkpoint_id,
            operator(),
            reconcile(
                crate::checkpoints::AgentReconciliationDecision::ConfirmedCompleted {
                    resolution: Box::new(exhausted()),
                },
            ),
            decision_key("confirm"),
            &policy,
            &policies,
            fx.now,
        )
        .expect_err("an exhausted confirmation resolves nothing");
        assert_eq!(error.code(), "effect-policy-invalid");
        assert_eq!(fx.send().status, AgentRunEffectStatus::Indeterminate);
    }

    /// Ruling 2: abandoning the transfer fails the run, as the decision's
    /// contract says. Before, the handoff arm's survivable path resumed the
    /// run under the false claim that no transfer was recorded.
    #[test]
    fn abandoning_a_parked_handoff_send_fails_the_run() {
        let mut fx = parked();
        let checkpoint_id = fx.checkpoint_id();
        resolve_checkpoint(
            &mut fx.state,
            &checkpoint_id,
            operator(),
            reconcile(crate::checkpoints::AgentReconciliationDecision::AbandonAndFail),
            decision_key("abandon"),
            &AgentSchemaPolicy::default(),
            &AgentEffectPolicies::default(),
            fx.now,
        )
        .expect("the abandonment applies");

        assert_eq!(fx.run().status, AgentRunStatus::Failed);
        match &fx.run().terminal_reason {
            Some(AgentRunTerminalReason::EffectFailed { code, .. }) => {
                assert_eq!(code, "reconciliation-abandoned");
            }
            other => panic!("expected the abandoned send to fail the run, found {other:?}"),
        }
        assert!(
            matches!(&fx.cell().status, AgentHandoffStatus::Failed { code, .. } if code == "reconciliation-abandoned"),
            "the transfer is abandoned with the send, got {:?}",
            fx.cell().status
        );
        assert!(fx.run().loop_state.open_checkpoints().is_empty());
    }

    /// Ruling 3: compensating the send settles the transfer in the same
    /// compare-and-set, so the wind-down never waits on a handoff result
    /// that may not come. The run ends once the compensation's outcome lands.
    #[test]
    fn compensating_a_parked_handoff_send_settles_its_cell() {
        let mut fx = parked();
        let checkpoint_id = fx.checkpoint_id();
        resolve_checkpoint(
            &mut fx.state,
            &checkpoint_id,
            operator(),
            reconcile(crate::checkpoints::AgentReconciliationDecision::Compensate {
                compensation: AgentCompensationRef::new("recall-transfer")
                    .expect("the ref is valid"),
            }),
            decision_key("compensate"),
            &AgentSchemaPolicy::default(),
            &AgentEffectPolicies::default(),
            fx.now,
        )
        .expect("the compensation schedules");

        assert!(
            matches!(&fx.cell().status, AgentHandoffStatus::Failed { code, .. } if code == "compensated"),
            "got {:?}",
            fx.cell().status
        );
        assert!(
            !fx.run().loop_state.awaits_children(),
            "the transfer no longer holds the wind-down"
        );
        assert_eq!(fx.run().status, AgentRunStatus::Cancelling);

        let compensation = fx
            .run()
            .loop_state
            .effects()
            .iter()
            .find(|effect| effect.kind() == AgentRunEffectKind::CompensationCall)
            .expect("the compensation is committed")
            .clone();
        record_effect_result(
            &mut fx.state,
            &compensation.effect_id,
            compensation.generation,
            1,
            0,
            AgentRunEffectOutcome::Tool {
                call_id: crate::effect::compensation_call_id(&compensation),
                content: AgentTaskContent::inline(serde_json::json!({ "recalled": true }))
                    .expect("the content is inline-bounded"),
            },
            &AgentSchemaPolicy::default(),
            &AgentEffectPolicies::default(),
            fx.now,
        )
        .expect("the compensation's result records");
        assert!(
            fx.run().status.is_terminal(),
            "the run ended, got {:?}",
            fx.run().status
        );
        assert_eq!(
            fx.run().terminal_reason.as_ref().map(AgentRunTerminalReason::code),
            Some("effect-compensated")
        );
    }

    /// Review focus 5: `ConfirmedNotExecuted` through the new checkpoint
    /// re-sends under a new generation and the same deduplication key, so a
    /// send that did land converges at the task, and the cell keeps the
    /// fence meanwhile. This behaviour is unchanged; the checkpoint makes it
    /// reachable.
    #[test]
    fn a_not_executed_decision_resends_the_handoff_under_the_fence() {
        let mut fx = parked();
        let checkpoint_id = fx.checkpoint_id();
        resolve_checkpoint(
            &mut fx.state,
            &checkpoint_id,
            operator(),
            reconcile(crate::checkpoints::AgentReconciliationDecision::ConfirmedNotExecuted),
            decision_key("resend"),
            &AgentSchemaPolicy::default(),
            &AgentEffectPolicies::default(),
            fx.now,
        )
        .expect("the decision applies");

        let send = fx.send();
        assert_eq!(send.generation.get(), fx.generation.get() + 1);
        assert_eq!(send.status, AgentRunEffectStatus::Pending);
        match &send.request {
            AgentRunEffectRequest::A2aHandoff { handoff } => {
                assert_eq!(handoff.deduplication_key, fx.handoff.as_str());
            }
            other => panic!("expected the handoff send, found {other:?}"),
        }
        assert!(matches!(fx.cell().status, AgentHandoffStatus::Pending));
        assert!(fx.run().loop_state.handoff_fenced());
        assert!(fx.run().loop_state.open_checkpoints().is_empty());
    }
```

- [ ] **Step 2: Run them to see the right ones fail**

Run: `cargo test -p rakka-agent --lib -- run::tests::confirming_an_exhausted run::tests::abandoning_a_parked run::tests::compensating_a_parked run::tests::a_not_executed_decision`

Expected:
- FAIL `confirming_an_exhausted…` at the first `expect_err` (the confirmation re-parked and returned `Ok`);
- FAIL `abandoning…` at the status (left `Running`, right `Failed`);
- FAIL `compensating…` at the cell status (left `Pending`);
- PASS `a_not_executed_decision…`, which pins unchanged behaviour.

- [ ] **Step 3: Write the failing integration test**

In `handoff_send_park.rs`:
- add `agent_scope` to the `use common::{…}` list;
- add `AgentCheckpointDecision`, `AgentOperationId`, `AgentOperationKind`, `AgentReconciliationDecision` and `AgentRunEntityCommand` to the `use rakka_agent::{…}` list;
- change the workflow import to `use rakka_agent_workflow::{AgentEphemeralCredential, PrincipalRef};`.

Then append:

```rust
/// Ruling 2, end to end: a send that never reached the task parks, and the
/// operator's `AbandonAndFail` through the checkpoint fails the run instead of
/// resuming it.
#[tokio::test]
async fn abandoning_a_send_that_never_reached_the_task_fails_the_run() {
    let (fixture, _segments) = parking_world(false, true).await;
    settle(&fixture).await;
    let checkpoint_id = run_record(&fixture)
        .await
        .loop_state
        .open_checkpoints()
        .first()
        .expect("the park carries its checkpoint")
        .checkpoint_id
        .clone();

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::ResolveCheckpoint {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::CheckpointResolution,
                &agent_scope(),
                "abandon",
            )
            .expect("the decision key derives"),
            checkpoint_id,
            resolver: PrincipalRef {
                principal_type: "user".to_string(),
                principal_id: "operator".to_string(),
                display_name: None,
            },
            decision: Box::new(AgentCheckpointDecision::Reconciliation(
                AgentReconciliationDecision::AbandonAndFail,
            )),
            telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
        },
        &fixture.router,
        fixture.now(),
    )
    .await
    .expect("the abandonment applies");
    settle(&fixture).await;

    let run = run_record(&fixture).await;
    assert_eq!(run.status, AgentRunStatus::Failed);
    match &run.terminal_reason {
        Some(AgentRunTerminalReason::EffectFailed { code, .. }) => {
            assert_eq!(code, "reconciliation-abandoned");
        }
        other => panic!("expected the abandoned send to fail the run, found {other:?}"),
    }
    let cell = run.loop_state.handoff().expect("the cell survives");
    assert!(
        matches!(&cell.status, AgentHandoffStatus::Failed { code, .. } if code == "reconciliation-abandoned"),
        "got {:?}",
        cell.status
    );
}
```

- [ ] **Step 4: Run it to see it fail**

Run: `cargo test -p rakka-agent --test handoff_send_park -- abandoning`
Expected: FAIL at `run.status`. The run resumed (left `Completed`: the second `settle` ran turn 2's proposal).

- [ ] **Step 5: Refuse an exhausted confirmation**

In `apply_indeterminate_resolution`, replace:

```rust
    let winding_down = run.terminal_reason.is_some();

    let Some(effect) = run.loop_state.effect_mut(effect_id) else {
```

with:

```rust
    let winding_down = run.terminal_reason.is_some();
    // Read before the effect is borrowed (issue #106).
    let handoff_standing = handoff_send(&run.loop_state, effect_id);

    let Some(effect) = run.loop_state.effect_mut(effect_id) else {
```

and directly after the block that ends:

```rust
        return Err(AgentRunError::StaleEffectResult {
            effect_id: effect_id.clone(),
            status: effect.status,
        });
    }
```

(the `effect.status != AgentRunEffectStatus::Indeterminate` fence, inside this function), add:

```rust
    // An exhausted outcome is what parked a handoff send: the probe could not
    // establish the transfer. Confirming it establishes nothing, and applying
    // it would re-park the generation with its checkpoint already dropped, so
    // it is refused, as a confirmation of an indeterminate outcome is
    // (issue #106).
    let confirms_exhaustion = matches!(
        &resolution,
        AgentEffectResolution::ConfirmedExecuted { outcome }
            if matches!(outcome.as_ref(), AgentRunEffectOutcome::Exhausted { .. })
    );
    if confirms_exhaustion && handoff_standing != HandoffSend::NotHeld {
        return Err(AgentEffectError::InvalidPolicy {
            message: "an exhausted outcome is what parked this handoff send; a reconciliation \
                      decision must establish what happened to the transfer"
                .to_string(),
        }
        .into());
    }
```

- [ ] **Step 6: Fail the run on abandonment**

In `resolve_checkpoint`, replace:

```rust
        AgentCheckpointOutcome::Abandoned => {
            state.run_mut()?.loop_state.drop_checkpoint(checkpoint_id);
```

with:

```rust
        AgentCheckpointOutcome::Abandoned => {
            state.run_mut()?.loop_state.drop_checkpoint(checkpoint_id);
            // A handoff send's transfer is abandoned with it. The cell settles
            // failed under the decision's code first, so the failure below takes
            // the shared wind-down rather than the survivable refusal path: the
            // decision's contract is that the run fails, and an unresolved cell
            // would hold the wind-down open on a notice that may never come
            // (issue #106).
            {
                let run = state.run_mut()?;
                if handoff_send(&run.loop_state, &bound_effect_id) == HandoffSend::Unresolved {
                    if let Some(cell) = run.loop_state.handoff_mut() {
                        cell.settle_failed("reconciliation-abandoned", now);
                    }
                }
            }
```

- [ ] **Step 7: Settle the cell on compensation**

In `schedule_compensation`, replace:

```rust
    if let Some(effect) = run.loop_state.effect_mut(effect_id) {
        effect.status = AgentRunEffectStatus::Compensated;
        effect.last_error_code = Some(bounded_detail("compensated".to_string()));
        effect.last_error_reason = None;
    }
```

with:

```rust
    if let Some(effect) = run.loop_state.effect_mut(effect_id) {
        effect.status = AgentRunEffectStatus::Compensated;
        effect.last_error_code = Some(bounded_detail("compensated".to_string()));
        effect.last_error_reason = None;
    }
    // A compensated handoff send's transfer settles failed in this same
    // compare-and-set. The decision closes the ambiguity, and an unresolved
    // cell would hold the wind-down open on a notice that may never come. A
    // notice that does come still corrects the cell, and `HandedOff` wins over
    // the compensating wind-down (issue #106).
    if handoff_send(&run.loop_state, effect_id) == HandoffSend::Unresolved {
        if let Some(cell) = run.loop_state.handoff_mut() {
            cell.settle_failed("compensated", now);
        }
    }
```

- [ ] **Step 8: Update the two decision docs**

In `crates/rakka-agent/src/checkpoints.rs`, replace:

```rust
    /// Schedule an explicitly defined compensation effect.
    Compensate {
```

with:

```rust
    /// Schedule an explicitly defined compensation effect. On a handoff send,
    /// the handoff cell settles failed in the same transition, so the
    /// wind-down never waits on a handoff result that may not come.
    Compensate {
```

and replace:

```rust
    /// Abandon the ambiguous effect; the run fails terminally.
    AbandonAndFail,
```

with:

```rust
    /// Abandon the ambiguous effect; the run fails terminally. A handoff
    /// send's cell settles failed with it, under `reconciliation-abandoned`.
    AbandonAndFail,
```

- [ ] **Step 9: Run the tests to see them pass**

Run: `cargo test -p rakka-agent --lib -- run::tests::confirming_an_exhausted run::tests::abandoning_a_parked run::tests::compensating_a_parked run::tests::a_not_executed_decision run::tests::compensating_an_effect_clears`
Expected: 5 passed.

Run: `cargo test -p rakka-agent --test handoff_send_park --test checkpoint_reconciliation --test handoff_record --test handoff_cancellation`
Expected: all pass.

- [ ] **Step 10: Falsify**

Break each change in turn, see the named test fail, then restore:
- Delete the `confirms_exhaustion` refusal: `confirming_an_exhausted…` fails.
- Delete the abandonment block: `abandoning_a_parked…` and the integration test fail.
- Delete the compensation block: `compensating_a_parked…` fails.
- Change Task 1's `handoff_held` to `handoff_standing != HandoffSend::NotHeld`: `abandoning_a_parked…` fails. That pins Task 1's narrowing.

- [ ] **Step 11: Lint, format and commit**

Run: `cargo fmt --all` then `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings`

```bash
git add crates/rakka-agent/src/run.rs crates/rakka-agent/src/checkpoints.rs crates/rakka-agent/tests/handoff_send_park.rs
git commit -m "Give a handoff send's checkpoint decisions their handoff meaning (issue #106)

AbandonAndFail now fails the run, as the decision documents, instead of
resuming it under a claim that no transfer was recorded. Compensate
settles the handoff cell failed, so the wind-down no longer waits on a
notice that may never come. A confirmation carrying an Exhausted
outcome is refused effect-policy-invalid: it re-parked the send with its
checkpoint already dropped.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Then run `cargo clean`.

---

### Task 4: Records parked before the fix heal on their next settle pass (D6)

**Files:**
- Modify: `crates/rakka-agent/src/run.rs`:
  - `AgentRun::needs_park_repair` beside `needs_turn_rest` (~728);
  - `has_reconciliation_checkpoint` beside `drop_reconciliation_checkpoint` (~4922);
  - `repair_park` beside `rest_unrested_turn` (~3919);
  - the `advance_once` head (~2517);
  - the `advance_loop` gate (~7630);
  - the test module: four tests.
- Modify: `crates/rakka-agent/tests/handoff_send_park.rs`: one test and its imports.

**Interfaces:**
- Consumes: `HandoffSend`, `handoff_send` (Task 1); `handoff_answer_receipt`, `settle_parked_handoff_send` (Task 2); the existing `open_effect_checkpoint(state, policies, effect_id, kind, now)`, `settle_run_disposition(state, now)` and `owed_run_exchanges(state, now)`.
- Produces:
  ```rust
  impl AgentRun { pub(crate) fn needs_park_repair(&self) -> bool }
  fn has_reconciliation_checkpoint(loop_state: &AgentLoopState, effect: &AgentRunEffect) -> bool
  fn repair_park(state: &mut AgentRunState, policies: &AgentEffectPolicies, now: AgentTimestampMillis) -> AgentRunResult<()>
  ```

- [ ] **Step 1: Write the failing unit tests**

Append to `run.rs`'s `mod tests`:

```rust
    /// Shape (b): a park a binary before issue #106 left with no checkpoint.
    /// The settle pass opens the ordinary one.
    #[test]
    fn a_park_without_its_checkpoint_is_repaired_by_the_settle_pass() {
        let mut fx = handoff_send_fixture(
            AgentRunStatus::WaitingForReconciliation,
            AgentHandoffStatus::Pending,
            AgentRunEffectStatus::Indeterminate,
        );
        assert!(fx.run().needs_park_repair(), "the pre-fix park needs its checkpoint");

        let now = fx.now;
        advance_once(
            &mut fx.state,
            &AgentEffectPolicies::default(),
            false,
            false,
            None,
            None,
            now,
        )
        .expect("the repair applies");

        let open = fx.run().loop_state.open_checkpoints();
        assert_eq!(open.len(), 1, "{open:?}");
        assert_eq!(open[0].checkpoint_id, fx.checkpoint_id());
        assert_eq!(fx.run().status, AgentRunStatus::WaitingForReconciliation);
        assert!(!fx.run().needs_park_repair(), "nothing is left to repair");
    }

    /// Shape (a), accepted: the old binary applied the task's acceptance but
    /// left the send parked. The repair settles the send executed, and the
    /// run hands off.
    #[test]
    fn an_accepted_park_is_released_by_the_settle_pass() {
        let mut fx = handoff_send_fixture(
            AgentRunStatus::WaitingForReconciliation,
            AgentHandoffStatus::Accepted {
                target_run: AgentRunId::new("ticket-1-gen-2").expect("the run id is valid"),
                generation: AgentAssignmentGeneration::new(2),
            },
            AgentRunEffectStatus::Indeterminate,
        );
        fx.hand_off();
        assert!(fx.run().needs_park_repair());

        let now = fx.now;
        advance_once(
            &mut fx.state,
            &AgentEffectPolicies::default(),
            false,
            false,
            None,
            None,
            now,
        )
        .expect("the repair applies");

        assert_eq!(fx.send().status, AgentRunEffectStatus::Succeeded);
        assert_eq!(fx.run().status, AgentRunStatus::HandedOff);
        assert!(fx.run().loop_state.open_checkpoints().is_empty());
    }

    /// Shape (a), refused: the old binary applied the refusal and rested the
    /// turn, but the parked send kept the run in reconciliation. The repair
    /// settles the send, and the run resumes.
    #[test]
    fn a_refused_park_is_released_by_the_settle_pass() {
        let mut fx = handoff_send_fixture(
            AgentRunStatus::WaitingForReconciliation,
            AgentHandoffStatus::Refused {
                code: "agent-not-instantiated".to_string(),
            },
            AgentRunEffectStatus::Indeterminate,
        );
        assert!(fx.run().needs_park_repair());

        let now = fx.now;
        advance_once(
            &mut fx.state,
            &AgentEffectPolicies::default(),
            false,
            false,
            None,
            None,
            now,
        )
        .expect("the repair applies");

        assert_eq!(fx.send().status, AgentRunEffectStatus::Succeeded);
        assert_eq!(fx.run().status, AgentRunStatus::Running);
        assert_eq!(fx.run().loop_state.phase(), AgentLoopPhase::RecordingTurn);
        assert!(!fx.run().needs_park_repair());
    }

    /// A terminal run is never repaired. Its checkpoint-less `Indeterminate`
    /// effect is the deliberate shape `record_effect_result` documents.
    #[test]
    fn a_terminal_run_needs_no_park_repair() {
        let mut fx = handoff_send_fixture(
            AgentRunStatus::Cancelled,
            AgentHandoffStatus::Pending,
            AgentRunEffectStatus::Indeterminate,
        );
        fx.state.run_mut().expect("the run exists").terminal_reason =
            Some(AgentRunTerminalReason::CancellationRequested {
                reason: "operator".to_string(),
            });
        assert!(!fx.run().needs_park_repair());
    }
```

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p rakka-agent --lib -- run::tests::a_park_without_its_checkpoint run::tests::an_accepted_park_is_released run::tests::a_refused_park_is_released run::tests::a_terminal_run_needs_no_park_repair`
Expected: compile error, because `needs_park_repair` does not exist yet.

- [ ] **Step 3: Write the failing integration test**

In `handoff_send_park.rs`:
- add `run_scope` to the `use common::{…}` list;
- add `use rakka_persistence::DurableStateStore;` after the `rakka_agent_workflow` import.

Then append:

```rust
/// Rewrites the stored record as a binary before issue #106 left a park: the
/// send `Indeterminate`, and no reconciliation checkpoint. The fixed code can
/// no longer write this shape, so it is written through the state store.
async fn unbind_checkpoints(fixture: &Fixture) {
    let id = run_scope().persistence_id();
    let record = fixture
        .runs
        .load(&id)
        .await
        .expect("the run record loads")
        .expect("the run record exists");
    let mut value = serde_json::to_value(&record.state).expect("the run state serializes");
    let loop_state = value["run"]["loop_state"]
        .as_object_mut()
        .expect("the loop state is an object");
    loop_state.insert("open_checkpoints".to_string(), json!([]));
    loop_state.insert("pending_checkpoint".to_string(), serde_json::Value::Null);
    let unbound = serde_json::from_value(value).expect("the pre-fix record deserializes");
    fixture
        .runs
        .compare_and_set(&id, record.revision, unbound)
        .await
        .expect("the pre-fix record persists");
}

/// Ruling 4: a park left without its checkpoint heals on the first settle
/// pass that touches it. This is the proof that `advance_loop`'s gate is
/// wired, which a unit test of `advance_once` cannot give.
#[tokio::test]
async fn a_park_left_without_its_checkpoint_heals_on_the_next_settle_pass() {
    let (fixture, _segments) = parking_world(false, true).await;
    settle(&fixture).await;
    unbind_checkpoints(&fixture).await;
    let wedged = run_record(&fixture).await;
    assert_eq!(wedged.status, AgentRunStatus::WaitingForReconciliation);
    assert!(
        wedged.loop_state.open_checkpoints().is_empty(),
        "the record is the pre-fix park"
    );

    fixture.pump().await.expect("the settle pass repairs the park");

    let healed = run_record(&fixture).await;
    let send = send_effect(&healed);
    let open = healed.loop_state.open_checkpoints();
    assert_eq!(open.len(), 1, "the repair opened the checkpoint: {open:?}");
    assert_eq!(
        open[0].checkpoint_id,
        AgentCheckpoint::id_for_effect(
            &send.effect_id,
            send.generation,
            AgentCheckpointKind::IndeterminateEffectReconciliation,
        )
    );
    assert_eq!(healed.status, AgentRunStatus::WaitingForReconciliation);
}
```

Run: `cargo test -p rakka-agent --test handoff_send_park -- a_park_left_without`
Expected: FAIL at `open.len()` (left `0`, right `1`). With no repair, the settle pass leaves the pre-fix park as it found it.

- [ ] **Step 4: Implement the predicate**

In `run.rs`, in `impl AgentRun`, directly after the closing `}` of `pub fn needs_turn_rest`, add:

```rust
    /// Whether the record holds a park a binary before issue #106 left
    /// unresolvable:
    /// - a live run whose handoff cell the task already answered while the
    ///   cell's send stayed `Indeterminate`, because the old notice path left
    ///   the effect parked;
    /// - a live run holding an `Indeterminate` effect with no reconciliation
    ///   checkpoint bound to its generation, because the old exhausted-send
    ///   path opened none.
    ///
    /// Under the fixed transitions neither shape is reachable: every
    /// transition that parks a live run's effect opens its checkpoint, and
    /// every notice settles a parked send. The settle pass repairs such a
    /// record on the first pass that touches it ([`repair_park`]), as the
    /// gap slice 3 repair rests an unrested turn. A terminal run is excluded:
    /// its checkpoint-less `Indeterminate` promotion or claim append is the
    /// deliberate shape `record_effect_result` documents.
    pub(crate) fn needs_park_repair(&self) -> bool {
        if self.status.is_terminal() {
            return false;
        }
        self.loop_state.indeterminate_effects().any(|effect| {
            handoff_send(&self.loop_state, &effect.effect_id) == HandoffSend::ResolvedByTask
                || !has_reconciliation_checkpoint(&self.loop_state, effect)
        })
    }
```

Directly above `fn drop_reconciliation_checkpoint`'s doc comment, add:

```rust
/// Whether the loop holds the reconciliation checkpoint bound to one parked
/// effect generation.
fn has_reconciliation_checkpoint(loop_state: &AgentLoopState, effect: &AgentRunEffect) -> bool {
    loop_state.open_checkpoints().iter().any(|checkpoint| {
        checkpoint.kind == AgentCheckpointKind::IndeterminateEffectReconciliation
            && checkpoint.bound_effect.effect_id == effect.effect_id
            && checkpoint.bound_effect.generation == effect.generation
    })
}
```

- [ ] **Step 5: Implement the repair**

Directly after `fn rest_unrested_turn`'s closing `}`, add:

```rust
/// Repairs a park a binary before issue #106 left unresolvable, in one
/// compare-and-set: the repair of [`AgentRun::needs_park_repair`].
///
/// A send whose cell the task already answered settles as executed, exactly
/// as the fixed notice path settles it. That terminalizes an accepted
/// transfer `HandedOff` and leaves a refused one resumed. Every remaining
/// `Indeterminate` effect without its reconciliation checkpoint gets the one
/// `record_effect_result` opens. The disposition is then re-settled.
fn repair_park(
    state: &mut AgentRunState,
    policies: &AgentEffectPolicies,
    now: AgentTimestampMillis,
) -> AgentRunResult<()> {
    let answered = state.run().and_then(|run| {
        let cell = run.loop_state.handoff()?;
        match &cell.status {
            crate::coordination::AgentHandoffStatus::Accepted { generation, .. } => Some(
                handoff_answer_receipt(&cell.record.handoff, Some(*generation), true),
            ),
            crate::coordination::AgentHandoffStatus::Refused { .. } => {
                Some(handoff_answer_receipt(&cell.record.handoff, None, false))
            }
            _ => None,
        }
    });
    if let Some(receipt) = answered {
        settle_parked_handoff_send(state, receipt, now)?;
    }
    let unbound: Vec<AgentEffectId> = state
        .run()
        .map(|run| {
            run.loop_state
                .indeterminate_effects()
                .filter(|effect| !has_reconciliation_checkpoint(&run.loop_state, effect))
                .map(|effect| effect.effect_id.clone())
                .collect()
        })
        .unwrap_or_default();
    for effect_id in &unbound {
        open_effect_checkpoint(
            state,
            policies,
            effect_id,
            AgentCheckpointKind::IndeterminateEffectReconciliation,
            now,
        )?;
    }
    settle_run_disposition(state, now)
}
```

- [ ] **Step 6: Wire it into the settle pass**

In `advance_once`, replace:

```rust
    let Some(run) = state.run.as_ref() else {
        return Ok(Vec::new());
    };
    if run.needs_turn_rest() {
```

with:

```rust
    let Some(run) = state.run.as_ref() else {
        return Ok(Vec::new());
    };
    if run.needs_park_repair() {
        // A park a binary before issue #106 left unresolvable: give it its
        // checkpoint, or settle the send its task already answered. A repair
        // that terminalizes the run owes what any terminalization owes, in
        // this same compare-and-set ([`AgentRun::needs_park_repair`]).
        repair_park(state, policies, now)?;
        return owed_run_exchanges(state, now);
    }
    if run.needs_turn_rest() {
```

In `advance_loop`, replace:

```rust
            // A record wedged by a binary before gap slice 3 cannot advance,
            // but one step rests its turn and it can thereafter
            // ([`AgentRun::needs_turn_rest`]).
            let can_advance = self
                .state()?
                .run()
                .is_some_and(|run| run.can_advance() || run.needs_turn_rest());
```

with:

```rust
            // A record wedged by a binary before gap slice 3 cannot advance,
            // but one step rests its turn and it can thereafter
            // ([`AgentRun::needs_turn_rest`]). A park a binary before issue
            // #106 left unresolvable is repaired the same way
            // ([`AgentRun::needs_park_repair`]).
            let can_advance = self.state()?.run().is_some_and(|run| {
                run.can_advance() || run.needs_turn_rest() || run.needs_park_repair()
            });
```

- [ ] **Step 7: Run the tests to see them pass**

Run: `cargo test -p rakka-agent --lib -- run::tests::a_park_without_its_checkpoint run::tests::an_accepted_park_is_released run::tests::a_refused_park_is_released run::tests::a_terminal_run_needs_no_park_repair`
Expected: 4 passed.

Run: `cargo test -p rakka-agent --test handoff_send_park --test private_memory_promotion --test checkpoint_reconciliation`
Expected: all pass. `private_memory_promotion` holds the gap slice 3 repair this sits beside.

- [ ] **Step 8: Falsify**

Break each in turn, see the named test fail, then restore:
- Drop `|| run.needs_park_repair()` from the `advance_loop` gate: `a_park_left_without_its_checkpoint…` (integration) fails, and the unit tests still pass. That is the reason the integration test exists.
- Delete the `answered` settle block in `repair_park`: `an_accepted_park…` and `a_refused_park…` fail.
- Delete the `unbound` loop: `a_park_without_its_checkpoint…` fails.

- [ ] **Step 9: Lint, format and commit**

Run: `cargo fmt --all` then `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings`

```bash
git add crates/rakka-agent/src/run.rs crates/rakka-agent/tests/handoff_send_park.rs
git commit -m "Repair handoff parks a binary before issue #106 left unresolvable

A run the old binary parked with no reconciliation checkpoint gets the
ordinary one on its next settle pass, and a send whose cell the task
already answered settles as executed, so an accepted transfer hands off
and a refused one resumes. The repair sits beside the gap slice 3
turn-rest repair, behind the same advance_loop gate.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Then run `cargo clean`.

---

### Task 5: Docs, the follow-up issue, validation and the PR (D7)

**Files:**
- Modify: `crates/rakka-agent/src/dispatch.rs:841-844` (the handoff executor's doc).
- Modify: `docs/rakka-v1-reliability-boundaries.md:224`.
- Modify: `docs/rakka-agents.md:99-102`.
- Modify: `docs/rakka-compatibility.md`: a new entry directly above the line beginning `- Issue #103 ("an MCP call that never left")`.
- Modify: `CHANGELOG.md`: a `### Fixed` entry directly below the issue #103 entry (the line beginning `- A non-idempotent MCP call that rmcp never accepted`).

**Interfaces:**
- Consumes: everything above. Produces no code.

- [ ] **Step 1: File the follow-up issue (ruling 5) and note its number**

```bash
gh issue create --repo paloul-tech/rakka \
  --title "rakka-agent: a handoff send whose credential cannot be resolved parks its run as ambiguous" \
  --body "$(cat <<'EOF'
Found while assessing #106, and left out of it by the owner's ruling.

## What happens

A handoff send uses the shared `a2a_send` effect spec (`crates/rakka-agent/src/effect.rs:945`), which can carry a credential binding. When the dispatcher cannot resolve that credential, it records a retryable attempt failure, `credential-resolution-failed` (`crates/rakka-agent/src/dispatch.rs`, the credential-resolution arm of `attempt_invocation`). Each retry fails the same way, and the spent budget arrives at the run as `Exhausted`.

The run's handoff arm (`apply_effect_outcome` in `crates/rakka-agent/src/run.rs`) reads every `Exhausted` handoff send as "the probe could not establish the transfer" and parks the generation `Indeterminate`. Since #106, it parks behind a reconciliation checkpoint.

But nothing left the run: the executor was never invoked. A person is asked to reconcile a transfer that provably never happened.

## Expected

A failure before the send should settle as a failure before the send, as #103 did for MCP calls. The handoff cell settles failed, the call fails, and the run survives, which is what a definitive handoff refusal already does.

## Notes

- Delegation sends are not affected. Their exhaustion is a fan-in disposition or the run's wind-down, never a park.
- One possible shape: the dispatcher reports a pre-invocation failure distinctly, and the handoff arm parks only an exhaustion whose attempts reached the executor.
EOF
)"
```

Record the issue number it prints; Steps 3 and 4 name it as `#<N>`.

- [ ] **Step 2: Fix the executor doc and the two product docs**

In `crates/rakka-agent/src/dispatch.rs`, replace:

```rust
/// the negative. A retry budget that spends out without an answer parks the
/// run for a reconciliation decision rather than resuming it beside a
/// possibly-live transfer. An absent executor fails closed at `invoke`.
```

with:

```rust
/// the negative. A retry budget that spends out without an answer parks the
/// run behind an indeterminate-effect reconciliation checkpoint rather than
/// resuming it beside a possibly-live transfer. The task's handoff result
/// settles the parked send as executed when it arrives, since the task owes
/// one only for a transfer it recorded. An absent executor fails closed at
/// `invoke`.
```

In `docs/rakka-v1-reliability-boundaries.md`, replace:

```
A delegation or handoff *send* is not in this list: it is an A2A outbox effect under the dispatcher's attempt accounting, whose exhaustion is a fan-in disposition or a handoff refusal rather than an indefinite re-drive; only its result exchange carries the guarantee above.
```

with:

```
A delegation or handoff *send* is not in this list: it is an A2A outbox effect under the dispatcher's attempt accounting, never an indefinite re-drive, and only its result exchange carries the guarantee above. A delegation send's exhaustion is a fan-in disposition, and a handoff send the task definitively refused fails its call while the run survives. A handoff send that exhausts its budget without an answer may have reached the task, so its run parks behind an indeterminate-effect reconciliation checkpoint until the task's `handoff-result` settles the send as executed or an explicit decision resolves it.
```

In `docs/rakka-agents.md`, replace:

```
and proposal, not exchanges of their own. A delegation or handoff *send* is not
an exchange either: it is an A2A outbox effect claimed by the dispatcher, whose
exhaustion becomes a fan-in disposition or a handoff refusal rather than a
courier re-drive, and only its result comes back as an exchange. An exchange is
```

with:

```
and proposal, not exchanges of their own. A delegation or handoff *send* is not
an exchange either: it is an A2A outbox effect claimed by the dispatcher, never
a courier re-drive, and only its result comes back as an exchange. A delegation
send's exhaustion becomes a fan-in disposition, and a handoff send the task
definitively refused fails its call while the run survives; a handoff send that
exhausts its budget without an answer parks the run behind a reconciliation
checkpoint until the task's handoff result settles it or a decision does. An
exchange is
```

- [ ] **Step 3: Add the compatibility entry**

In `docs/rakka-compatibility.md`, insert as a new line directly above the line beginning `- Issue #103 ("an MCP call that never left")`. Replace `#<N>` with the follow-up issue's number.

```
- Issue #106 ("a parked handoff send") changes how a run settles a handoff send whose retry budget spent out without an answer. It adds no type, field, code, or schema version. **The park carries its checkpoint.** A handoff send that arrives `Exhausted` still parks its generation `Indeterminate` (the executor's probe could not establish the transfer), and the run now parks behind an `IndeterminateEffectReconciliation` checkpoint in that same transition. The checkpoint has the id (`{effect id}#ck-reconcile-g{generation}`), the five default decisions, the summary, and the SLA escalation that an ambiguous outcome's checkpoint carries. Before, the run waited `WaitingForReconciliation` with no checkpoint, so no SLA fired and every checkpoint decision answered `run-unknown-checkpoint`. **The task's answer releases it.** A `handoff-result` notice that reaches a run whose handoff send is parked settles the send as executed and drops the checkpoint in the same compare-and-set. The task owes a notice only for a transfer it recorded, so an acceptance and a refusal both prove the send reached it. An acceptance terminalizes the run `HandedOff`, and a refusal resumes it with the failed tool result. A send result that arrives after the notice settles the send `Succeeded` too, whatever the dispatcher reported. **Decisions on the checkpoint.** `ConfirmedCompleted`, and `ResolveIndeterminateEffect`'s `ConfirmedExecuted`, carrying an `Exhausted` outcome is refused `effect-policy-invalid` on a handoff send, because it re-parked the send with its checkpoint already dropped. `AbandonAndFail` now settles the handoff cell `Failed` under `reconciliation-abandoned` and fails the run (`EffectFailed`, `reconciliation-abandoned`), as the decision documents. Before, the run survived it with a tool result that claimed no transfer was recorded. `Compensate` now settles the cell `Failed` under `compensated` in the same compare-and-set, so the wind-down no longer waits on a notice that may never come. A notice that arrives after either decision follows the slice 5.1 contradicted-cell rule: an acceptance on a live run hands off, and on a terminal run it corrects the cell only. **Records parked before this change** heal on the first settle pass that touches them. A send whose cell the task already answered settles as executed, and an `Indeterminate` effect on a live run with no reconciliation checkpoint gets one. A rolling downgrade reads the new checkpoint as an ordinary reconciliation checkpoint and applies that binary's decision rules, including its old `AbandonAndFail`. A handoff send whose credential cannot be resolved still parks; issue #<N> tracks it.
```

- [ ] **Step 4: Add the changelog entry**

In `CHANGELOG.md`, insert as a new line directly below the line beginning `- A non-idempotent MCP call that rmcp never accepted`:

```
- A handoff send whose retry budget spent out without an answer parked its run `WaitingForReconciliation` with no reconciliation checkpoint, so nothing escalated and no checkpoint decision could reach it. When the transfer had in fact landed, the task's handoff result recorded the outcome but left the run parked forever. The park now opens the same reconciliation checkpoint an ambiguous outcome does, and the task's handoff result settles the parked send as executed: the run hands off, or resumes on a refusal. `AbandonAndFail` on such a send now fails the run as documented, `Compensate` settles the transfer so the wind-down can finish, a confirmation carrying an exhausted outcome is refused, and runs parked before the fix heal on their next settle pass (issue #106).
```

- [ ] **Step 5: Run the doc checks**

Run: `cargo test -p rakka-agent --test product_doc_currency`
Expected: PASS. It holds the docs' exchange list, which these edits leave intact.

Run: `cargo test -p rakka-agent --features otel --test compatibility_currency`
Expected: PASS. Per memory, this test is gated on `otel`. If the target does not exist in `rakka-agent`, run `grep -rln compatibility_currency crates/*/tests` and run it in the crate that holds it.

Run: `cargo doc -p rakka-agent --no-deps`
Expected: no broken intra-doc links. `[`repair_park`]` in a `pub(crate)` doc must resolve; if rustdoc warns, write it as plain code (`` `repair_park` ``).

- [ ] **Step 6: Commit the docs**

```bash
git add crates/rakka-agent/src/dispatch.rs docs/rakka-v1-reliability-boundaries.md docs/rakka-agents.md docs/rakka-compatibility.md CHANGELOG.md
git commit -m "Record issue #106 in the docs, the compatibility document and the changelog

The reliability-boundaries and agents docs said a handoff send's
exhaustion was a handoff refusal, and the executor's doc said it parked
for a decision; the code now parks it behind a reconciliation checkpoint
that the task's handoff result releases, and all three say so.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 7: Validate**

Run `cargo clean`, then:

```bash
scripts/validate.sh > target/validate-106.log 2>&1; echo $? > target/validate-106.exit
cat target/validate-106.exit
```

Expected: `0`. If the one-shot workspace test run is killed under load (exit 137 or a signal in the log), run the crates individually instead: `cargo test -p rakka-agent --all-features`, then `cargo test -p rakka-a2a --all-features`. Then re-run the non-test stages of `validate.sh` by hand (fmt, clippy, the no-default-feature checks, doc).

Then check the MSRV:

```bash
CARGO_TARGET_DIR=target/msrv88-106 cargo +1.88.0 check -p rakka-agent --all-targets
```

Expected: success.

- [ ] **Step 8: Push and open the PR**

```bash
git push -u origin handoff-park-checkpoint
gh pr create --base main --head handoff-park-checkpoint \
  --title "A parked handoff send gets its checkpoint, and the task's answer releases it (issue #106)" \
  --body "$(cat <<'EOF'
Fixes #106.

## Summary

An exhausted handoff send parked its run `Indeterminate` with no reconciliation checkpoint. When the transfer had landed, the task's handoff result still left the run parked. Now:

| Ask | What changed | Commit |
|---|---|---|
| 1 | `record_effect_result` opens the reconciliation checkpoint whenever a transition leaves the effect `Indeterminate`. The handoff send's `Exhausted` arm was the only other path that parked, and a re-park through an exhausted confirmation is now refused. | Task 1, Task 3 |
| 2 | A handoff result settles a parked send as **executed** (both answers: a refusal is owed only for a recorded transfer) and drops the checkpoint in the same compare-and-set. A send result after the notice settles executed too. | Task 1, Task 2 |
| 3 | The reliability-boundaries doc, the agents doc and the executor's doc agree with the code. | Task 5 |

Owner rulings applied:
- `AbandonAndFail` on a handoff send fails the run.
- `Compensate` settles the cell failed `compensated`.
- Records parked before the fix heal on their next settle pass.
- The credential-resolution park is #<N>.

## Where the brief and the code disagreed

- Ask 2 asked for a `Refused` notice to settle the send "not executed". That would send the handoff again; the refusal proves the send reached the task. It settles executed.
- The docs were wrong in two places (`docs/rakka-agents.md:101` as well as the reliability doc).
- Opening the checkpoint exposed two decisions with wrong handoff behaviour (`AbandonAndFail` survived, `Compensate` could hang the wind-down), and a reconciliation path (`ConfirmedExecuted { Exhausted }`) that re-parked with no checkpoint.

## Test plan

- [ ] `cargo test -p rakka-agent --lib`: 18 new unit tests in `run.rs`
- [ ] `cargo test -p rakka-agent --test handoff_send_park`: 5 end-to-end tests
- [ ] Existing handoff and reconciliation suites unchanged
- [ ] Each behaviour falsified once (see the plan's falsification steps)
- [ ] `scripts/validate.sh` exit 0; MSRV 1.88 check

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Before running the command, replace `#<N>` with the follow-up issue's number, and the task names in the Commit column with the commits' short SHAs (`git log --oneline main..HEAD`).

- [ ] **Step 9: After the owner merges, report back on #106**

The brief asks for this (its §5). Post one comment on #106 with:
- a landing table: ask → what changed → commit → tests;
- the brief/code disagreements listed in the PR;
- the follow-up issue number;
- the pin to take, which is the merge commit or later;
- what the host sees, from the spec's "What rakka.host sees at the pin bump".

Closing #106 is left to the owner.
