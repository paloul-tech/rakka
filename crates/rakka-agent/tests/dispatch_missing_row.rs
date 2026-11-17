//! A dispatch ticket whose outbox row is gone (issue #150).
//!
//! A fleet entry can outlive its outbox row: a run's workflow record is
//! purged, or one store is restored without the other. The claim on such an
//! entry used to write its cancellation to the row first, fail on the row
//! that was not there, and never reach the fleet entry. The pass failed with
//! `outbox-entry-not-found`, the claims behind it in the batch were left
//! unexecuted, and the entry was claimed again on every lease, for good.
//!
//! Now nothing is written to the missing row. The fleet entry is finished as
//! `Cancelled` with `ticket-row-missing`, and a run that still holds the
//! effect unresolved is parked on it as indeterminate: the row was the only
//! record of whether an attempt started, so nobody can say the effect never
//! ran.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::{CrashPoint, DeterministicModelAdapter, SharedAtomicWorkflowClock};
use rakka_agent::{
    AgentCheckpointKind, AgentDispatchError, AgentDispatchFuture, AgentDispatchPass,
    AgentDispatchToolExecutor, AgentEffectSpec, AgentModelTurn, AgentRunEffect,
    AgentRunEffectStatus, AgentRunEntityCommand, AgentRunScope, AgentRunStatus, AgentTaskContent,
    AgentToolAuthority, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentDispatchEntry, AgentDispatchStatus, AgentEphemeralCredential};
use rakka_persistence::DurableStateStore;

mod common;

use common::*;

const TOOL: &str = "charge-card";

fn tool_call_request(call: &str) -> AgentToolCallRequest {
    AgentToolCallRequest::new(
        AgentToolCallId::new(call).expect("the call id is valid"),
        AgentToolId::new(TOOL).expect("the tool id is valid"),
        serde_json::json!({ "amount": 42 }),
    )
    .expect("the tool call is bounded")
}

fn proposal() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_proposal(
        AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
            .expect("the proposal is inline-bounded"),
    )
}

fn tool_then_proposal() -> DeterministicModelAdapter {
    DeterministicModelAdapter::new()
        .with_turn_for(
            1,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_tool_call(tool_call_request("call-1")),
        )
        .with_turn_for(2, proposal())
}

/// The fleet index entry for one effect's ticket, if it is registered.
async fn fleet_entry(fx: &AuthorityFixture, effect: &AgentRunEffect) -> Option<AgentDispatchEntry> {
    let record = fx
        .fleet_store
        .load(&rakka_agent_workflow::agent_dispatcher_fleet_persistence_id())
        .await
        .expect("the fleet store loads")?;
    record
        .state
        .entry(&rakka_agent_workflow::agent_dispatch_id(
            &rakka_agent::workflow_run_id(&run_scope()),
            &effect.dispatch_ticket_id(),
        ))
        .cloned()
}

/// A run whose tool ticket is in the outbox, under `spec`, with `tools`.
async fn ready_tool_ticket(
    spec: &AgentEffectSpec,
    tools: Option<Arc<dyn AgentDispatchToolExecutor>>,
) -> (AuthorityFixture, AgentRunEffect) {
    let mut fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, spec)),
        None,
    );
    if let Some(tools) = tools {
        fx = fx.with_tool_executor(tools);
    }
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let effect = fx.effect_at(1).await.expect("the tool effect is committed");
    (fx, effect)
}

/// Deletes the run's whole workflow record: its inbox and every outbox row.
async fn delete_workflow_record(fx: &AuthorityFixture) {
    let id = workflow_persistence_id(&run_scope());
    let record = fx
        .workflow_store
        .load(&id)
        .await
        .expect("the workflow store loads")
        .expect("the run has a workflow record");
    fx.workflow_store
        .delete(&id, record.revision)
        .await
        .expect("the workflow record is deleted");
}

/// The kinds of the checkpoints the run holds open.
async fn open_checkpoint_kinds(fx: &AuthorityFixture) -> Vec<AgentCheckpointKind> {
    let state = rakka_agent::load_agent_run_state(
        &fx.fx.runs,
        &run_scope(),
        &rakka_agent::AgentSchemaPolicy::default(),
    )
    .await
    .expect("the run state loads")
    .expect("the run exists");
    state
        .loop_state()
        .expect("the loop exists")
        .open_checkpoints()
        .iter()
        .map(|checkpoint| checkpoint.kind)
        .collect()
}

fn assert_settled_as_missing(entry: Option<AgentDispatchEntry>) {
    let entry = entry.expect("the entry is kept");
    assert_eq!(entry.status, AgentDispatchStatus::Cancelled);
    assert_eq!(entry.last_error_code.as_deref(), Some("ticket-row-missing"));
}

#[tokio::test]
async fn a_live_run_whose_ticket_row_is_gone_is_parked_and_the_entry_settled() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let (fx, effect) = ready_tool_ticket(&spec, None).await;
    let mut pipeline = fx.pipeline();
    pipeline
        .register_run(&run_scope())
        .await
        .expect("the registration runs");
    delete_workflow_record(&fx).await;

    let pass = pipeline
        .claim_and_execute()
        .await
        .expect("an orphan does not fail the pass");

    assert_eq!(
        (
            pass.claimed,
            pass.cancelled,
            pass.parked_indeterminate,
            pass.delivered
        ),
        (1, 1, 1, 1)
    );
    assert_eq!(pass.invoked, 0);
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    assert_settled_as_missing(fleet_entry(&fx, &effect).await);
    let parked = fx.effect_at(1).await.expect("the effect is held");
    assert_eq!(parked.status, AgentRunEffectStatus::Indeterminate);
    assert_eq!(
        parked.last_error_code.as_deref(),
        Some("ticket-row-missing")
    );
    assert_eq!(
        open_checkpoint_kinds(&fx).await,
        vec![AgentCheckpointKind::IndeterminateEffectReconciliation]
    );
    assert!(
        fx.outbox_row(&effect).await.is_none(),
        "nothing was written to the missing row"
    );

    fx.expire_lease();
    let again = fx
        .pipeline()
        .claim_and_execute()
        .await
        .expect("the pass runs");
    assert_eq!(again.claimed, 0, "a settled orphan is never claimed again");
}

fn counts(pass: &AgentDispatchPass) -> (usize, usize, usize, usize) {
    (
        pass.claimed,
        pass.cancelled,
        pass.parked_indeterminate,
        pass.delivered,
    )
}

#[tokio::test]
async fn with_the_run_gone_too_the_entry_is_settled_and_nobody_is_told() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let (fx, effect) = ready_tool_ticket(&spec, None).await;
    let mut pipeline = fx.pipeline();
    pipeline
        .register_run(&run_scope())
        .await
        .expect("the registration runs");
    delete_workflow_record(&fx).await;
    let run_id = run_scope().persistence_id();
    let run = fx
        .fx
        .runs
        .load(&run_id)
        .await
        .expect("the run store loads")
        .expect("the run has a record");
    fx.fx
        .runs
        .delete(&run_id, run.revision)
        .await
        .expect("the run record is deleted");

    let pass = pipeline
        .claim_and_execute()
        .await
        .expect("an orphan does not fail the pass");

    assert_eq!(counts(&pass), (1, 1, 0, 0));
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    assert_settled_as_missing(fleet_entry(&fx, &effect).await);
}

/// An operator cancels the run, which stays `Cancelling` on its ready effect.
async fn cancel_the_run(fx: &AuthorityFixture) {
    let mut run = fx.fx.run();
    run.recover(fx.fx.now()).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::Cancel {
            operation_id: rakka_agent::AgentOperationId::new(
                rakka_agent::AgentOperationKind::Cancellation,
                [TENANT, AGENT, "1"],
            )
            .expect("the operation id derives"),
            reason: "operator stopped the work".to_string(),
        },
        &fx.fx.router,
        fx.fx.now(),
    )
    .await
    .expect("cancellation applies");
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Cancelling
    );
}

/// A run that is winding down gets the answer a live one gets, when the
/// claim is what reaches it first. The claim path cannot tell a ticket that
/// never started from one whose row took the record of its attempt with it,
/// so it does not say `Cancelled` here either.
///
/// The claim reaches it first only on the per-worker half of a pass, as
/// here. The next test is the other half.
#[tokio::test]
async fn a_cancelling_run_is_parked_like_a_live_one() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let (fx, effect) = ready_tool_ticket(&spec, None).await;
    let mut pipeline = fx.pipeline();
    pipeline
        .register_run(&run_scope())
        .await
        .expect("the registration runs");
    cancel_the_run(&fx).await;
    delete_workflow_record(&fx).await;

    let pass = pipeline
        .claim_and_execute()
        .await
        .expect("an orphan does not fail the pass");

    assert_eq!(counts(&pass), (1, 1, 1, 1));
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    assert_settled_as_missing(fleet_entry(&fx, &effect).await);
    assert_eq!(
        fx.effect_status(1).await,
        Some(AgentRunEffectStatus::Indeterminate)
    );
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::WaitingForReconciliation,
        "the unknown outcome keeps the run from ending until it is decided"
    );
}

/// The same run, pumped: the per-run half of a pass fences a winding-down
/// run before anything is claimed, and the fence answers first.
///
/// Pinned as it stands. The wind-down fence reads a ready effect with no
/// outbox row as one whose flush never landed: it plants a cancelled
/// tombstone and tells the run `Cancelled`. It does that for a row that was
/// lost as well, whatever the row knew, and the run ends. The claim then
/// finds the tombstone and an intent already resolved. This change does not
/// touch the fence; when the fence learns to tell a lost row from a flush
/// that never landed, this test changes with it.
#[tokio::test]
async fn a_cancelling_run_that_is_pumped_is_fenced_before_the_claim() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let (fx, effect) = ready_tool_ticket(&spec, None).await;
    let mut pipeline = fx.pipeline();
    pipeline
        .register_run(&run_scope())
        .await
        .expect("the registration runs");
    cancel_the_run(&fx).await;
    delete_workflow_record(&fx).await;

    let pass = pipeline
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");

    assert_eq!(pass.parked_indeterminate, 0);
    assert_eq!(pass.invoked, 0);
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Cancelled,
        "the fence told the run the effect was cancelled, and the run ended"
    );
    let entry = fleet_entry(&fx, &effect).await.expect("the entry is kept");
    assert_eq!(entry.status, AgentDispatchStatus::Cancelled);
    assert_eq!(
        entry.last_error_code.as_deref(),
        Some("intent-already-resolved"),
        "the claim found the fence's tombstone, not a missing row"
    );
}

/// A run that has ended, with the orphan still registered: what a retention
/// that purges an ended run's workflow record leaves when a worker died
/// between telling the run and settling its ticket. The run waits on
/// nothing, so nobody is told and the entry is finished.
#[tokio::test]
async fn a_run_that_already_ended_is_told_nothing() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let (fx, effect) = ready_tool_ticket(&spec, None).await;
    let mut pipeline = fx.pipeline();
    pipeline
        .register_run(&run_scope())
        .await
        .expect("the registration runs");
    let mut run = fx.fx.run();
    run.recover(fx.fx.now()).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: effect
                .result_operation_id(&run_scope())
                .expect("the operation id derives"),
            effect_id: effect.effect_id.clone(),
            generation: effect.generation,
            attempt: 1,
            fence: 1,
            outcome: Box::new(rakka_agent::AgentRunEffectOutcome::failed(
                "tool-refused",
                "the tool refused the call",
            )),
        },
        &fx.fx.router,
        fx.fx.now(),
    )
    .await
    .expect("the result applies");
    let ended = fx.fx.run_snapshot().await.expect("the run exists").status;
    assert!(ended.is_terminal(), "the run ended: {ended:?}");
    delete_workflow_record(&fx).await;

    let pass = pipeline
        .claim_and_execute()
        .await
        .expect("an orphan does not fail the pass");

    assert_eq!(counts(&pass), (1, 1, 0, 0));
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    assert_settled_as_missing(fleet_entry(&fx, &effect).await);
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        ended,
        "the ended run was not moved"
    );
}

/// The fleet write that would finish the entry is refused after the run was
/// told. The claim's lease carries the ticket, and the next claim finds the
/// effect no longer waited on: it settles the entry and tells nobody twice.
#[tokio::test]
async fn a_refused_fleet_write_after_the_park_is_repaired_on_the_next_lease() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let (fx, effect) = ready_tool_ticket(&spec, None).await;
    let mut pipeline = fx.pipeline();
    pipeline
        .register_run(&run_scope())
        .await
        .expect("the registration runs");
    delete_workflow_record(&fx).await;

    // The claim is the pass's first fleet write and the settle its second.
    fx.fleet_store.crash_at(2, CrashPoint::BeforeWrite);
    let error = pipeline
        .claim_and_execute()
        .await
        .expect_err("the settle's fleet write is refused");
    fx.fleet_store
        .assert_crash_fired(2, CrashPoint::BeforeWrite);
    fx.fleet_store.survive();
    assert_eq!(error.code(), "store-error");
    assert_eq!(
        fx.effect_status(1).await,
        Some(AgentRunEffectStatus::Indeterminate),
        "the run was told before the entry was settled"
    );
    assert_eq!(
        fleet_entry(&fx, &effect).await.map(|entry| entry.status),
        Some(AgentDispatchStatus::Claimed)
    );

    fx.expire_lease();
    let repaired = fx
        .pipeline()
        .claim_and_execute()
        .await
        .expect("the pass runs");

    assert_eq!(counts(&repaired), (1, 1, 0, 0));
    assert_settled_as_missing(fleet_entry(&fx, &effect).await);
    assert_eq!(
        open_checkpoint_kinds(&fx).await,
        vec![AgentCheckpointKind::IndeterminateEffectReconciliation],
        "the run was told once"
    );
}

/// The run cannot be written when it is told. The pass fails with the entry
/// still claimed, so the next lease tells the run: an entry settled first
/// would be finished here, with the run waiting on an effect nothing claims.
#[tokio::test]
async fn a_run_that_cannot_be_written_keeps_the_claim_for_the_next_lease() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let (fx, effect) = ready_tool_ticket(&spec, None).await;
    let mut pipeline = fx.pipeline();
    pipeline
        .register_run(&run_scope())
        .await
        .expect("the registration runs");
    delete_workflow_record(&fx).await;

    fx.fx.runs.crash_at(1, CrashPoint::BeforeWrite);
    pipeline
        .claim_and_execute()
        .await
        .expect_err("the run's store refuses the result");
    fx.fx.runs.assert_crash_fired(1, CrashPoint::BeforeWrite);
    fx.fx.runs.survive();
    assert_eq!(fx.effect_status(1).await, Some(AgentRunEffectStatus::Ready));
    assert_eq!(
        fleet_entry(&fx, &effect).await.map(|entry| entry.status),
        Some(AgentDispatchStatus::Claimed),
        "the entry is not finished before the run is told"
    );

    fx.expire_lease();
    let pass = fx
        .pipeline()
        .claim_and_execute()
        .await
        .expect("the pass runs");

    assert_eq!(counts(&pass), (1, 1, 1, 1));
    assert_settled_as_missing(fleet_entry(&fx, &effect).await);
    assert_eq!(
        fx.effect_status(1).await,
        Some(AgentRunEffectStatus::Indeterminate)
    );
}

/// A tool whose every call times out.
struct TimingOutTool;

impl AgentDispatchToolExecutor for TimingOutTool {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        Box::pin(async move {
            Err(AgentDispatchError::Invocation {
                code: "tool-timeout",
                message: "the tool timed out".to_string(),
            })
        })
    }
}

/// The row counted the attempts and the run is told of none until an effect
/// resolves. With the row gone, the fleet entry holds the last count the
/// fleet learned from it, and the park reports that: an attempt the tool
/// received is not handed back to the run's budget as never made.
#[tokio::test]
async fn the_park_reports_the_attempts_the_fleet_entry_counted() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let (fx, effect) = ready_tool_ticket(&spec, Some(Arc::new(TimingOutTool))).await;
    let failed = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    assert_eq!((failed.invoked, failed.failed_attempts), (1, 1));
    let entry = fleet_entry(&fx, &effect).await.expect("the entry is kept");
    assert_eq!(
        (entry.status, entry.attempts),
        (AgentDispatchStatus::RetryScheduled, 1)
    );
    assert_eq!(
        fx.effect_at(1).await.expect("the effect is held").attempts,
        0,
        "the run has not been told of the attempt"
    );
    delete_workflow_record(&fx).await;
    // Past any retry backoff, so the entry is due again.
    fx.wf_clock.advance(24 * 60 * 60 * 1_000);

    let pass = fx
        .pipeline()
        .claim_and_execute()
        .await
        .expect("an orphan does not fail the pass");

    assert_eq!(counts(&pass), (1, 1, 1, 1));
    assert_eq!(pass.invoked, 0);
    let parked = fx.effect_at(1).await.expect("the effect is held");
    assert_eq!(parked.status, AgentRunEffectStatus::Indeterminate);
    assert_eq!(parked.attempts, 1);
    assert_settled_as_missing(fleet_entry(&fx, &effect).await);
}

// ---------------------------------------------------------------------------
// An orphan beside a healthy ticket of the same run.
// ---------------------------------------------------------------------------

fn two_tools_then_proposal() -> DeterministicModelAdapter {
    DeterministicModelAdapter::new()
        .with_turn_for(
            1,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_tool_call(tool_call_request("call-1"))
                .with_tool_call(tool_call_request("call-2")),
        )
        .with_turn_for(2, proposal())
}

/// A run with two tool tickets registered in the fleet, answered in the
/// order the fleet hands them out: by due time, then by dispatch id.
///
/// `tools` builds the executor from the fixture, whose clock an executor
/// that outlives a lease has to advance.
async fn two_registered_tool_tickets(
    tools: impl FnOnce(&AuthorityFixture) -> Option<Arc<dyn AgentDispatchToolExecutor>>,
) -> (AuthorityFixture, AgentRunEffect, AgentRunEffect) {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let mut fx = AuthorityFixture::new(
        two_tools_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, &spec)),
        None,
    );
    if let Some(tools) = tools(&fx) {
        fx = fx.with_tool_executor(tools);
    }
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    fx.pipeline()
        .register_run(&run_scope())
        .await
        .expect("the registration runs");
    let mut tickets = Vec::new();
    for slot in [1, 2] {
        let effect = fx.effect_at(slot).await.expect("the tool effect is held");
        assert_eq!(effect.status, AgentRunEffectStatus::Ready);
        let entry = fleet_entry(&fx, &effect)
            .await
            .expect("the ticket is registered");
        tickets.push(((entry.due_at, entry.dispatch_id), effect));
    }
    tickets.sort_by(|left, right| left.0.cmp(&right.0));
    let second = tickets.pop().expect("two tickets").1;
    let first = tickets.pop().expect("two tickets").1;
    (fx, first, second)
}

/// Removes one effect's outbox row and leaves the record's other rows.
async fn drop_outbox_row(fx: &AuthorityFixture, effect: &AgentRunEffect) {
    let id = workflow_persistence_id(&run_scope());
    let record = fx
        .workflow_store
        .load(&id)
        .await
        .expect("the workflow store loads")
        .expect("the run has a workflow record");
    let ticket = effect.dispatch_ticket_id();
    let mut state = serde_json::to_value(&record.state).expect("the state encodes");
    let outbox = state["outbox"]
        .as_object_mut()
        .expect("the outbox is a map of rows");
    assert!(
        outbox.remove(ticket.as_str()).is_some(),
        "the row was there to remove"
    );
    state["outbox_deduplication"]
        .as_object_mut()
        .expect("the deduplication index is a map")
        .retain(|_key, message| message.as_str() != Some(ticket.as_str()));
    fx.workflow_store
        .compare_and_set(
            &id,
            record.revision,
            serde_json::from_value(state).expect("the state decodes"),
        )
        .await
        .expect("the record is written without the row");
    assert!(fx.outbox_row(effect).await.is_none());
}

#[tokio::test]
async fn a_healthy_ticket_behind_the_orphan_runs_in_the_same_pass() {
    let (fx, orphan, healthy) = two_registered_tool_tickets(|_fx| None).await;
    drop_outbox_row(&fx, &orphan).await;

    let pass = fx
        .pipeline()
        .claim_and_execute()
        .await
        .expect("an orphan does not fail the pass");

    assert_eq!(pass.claimed, 2);
    assert_eq!(pass.invoked, 1, "the ticket behind the orphan was executed");
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
    assert_settled_as_missing(fleet_entry(&fx, &orphan).await);
    assert_eq!(
        fleet_entry(&fx, &healthy).await.map(|entry| entry.status),
        Some(AgentDispatchStatus::Completed)
    );
}

/// Two orphans of one run, pinned as it stands: one is parked, and its park
/// puts the other's row back.
///
/// The park is a command the run handles, and a run that handles a command
/// flushes its ready effects to the outbox again. The second ticket's row
/// comes back as a new row with no attempt history, the claim this pass
/// already holds on it finds a row, and the tool is invoked as a first
/// attempt. So of a run's several lost rows, the one claimed first waits for
/// a decision and the others run again. Nothing durable tells a row the run
/// recreated from the one that was lost; when something does, this test
/// changes with it.
#[tokio::test]
async fn of_two_orphans_of_one_run_the_park_of_the_first_flushes_the_second_back() {
    let (fx, first, second) = two_registered_tool_tickets(|_fx| None).await;
    delete_workflow_record(&fx).await;

    let pass = fx
        .pipeline()
        .claim_and_execute()
        .await
        .expect("an orphan does not fail the pass");

    assert_eq!(counts(&pass), (2, 1, 1, 2));
    assert_settled_as_missing(fleet_entry(&fx, &first).await);
    assert_eq!(
        fx.effect_status(first.slot).await,
        Some(AgentRunEffectStatus::Indeterminate)
    );
    assert_eq!(
        (pass.invoked, fx.tools.invocation_count(TOOL)),
        (1, 1),
        "the second ticket ran as a first attempt on the row the run put back"
    );
    assert_eq!(
        fleet_entry(&fx, &second).await.map(|entry| entry.status),
        Some(AgentDispatchStatus::Completed)
    );
}

/// A tool that refuses every call for good, and whose first call outlives
/// its lease: while it is still running, the lease lapses and another worker
/// runs a whole pass. Only then does the first call answer.
///
/// It refuses, and does not succeed, to keep the orphan an orphan. A result
/// the run accepts while it is not winding down makes it flush its ready
/// effects again, and that puts the missing row back before the other
/// worker reaches it.
struct OverrunningTool {
    calls: AtomicUsize,
    clock: SharedAtomicWorkflowClock,
    other: tokio::sync::Mutex<Option<Pipeline>>,
    other_pass: std::sync::Mutex<Option<AgentDispatchPass>>,
}

impl AgentDispatchToolExecutor for OverrunningTool {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        Box::pin(async move {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.clock.advance(LEASE_MS + 1);
                let mut other = self
                    .other
                    .lock()
                    .await
                    .take()
                    .expect("the other worker is wired before the first call");
                let pass = other
                    .claim_and_execute()
                    .await
                    .expect("the other worker's pass runs");
                *self
                    .other_pass
                    .lock()
                    .expect("the pass slot is not poisoned") = Some(pass);
            }
            Err(AgentDispatchError::Definitive {
                code: "tool-refused".to_string(),
                message: "the tool refused the call".to_string(),
            })
        })
    }
}

/// A worker reaches an orphan late: an earlier claim of its batch outlived
/// the lease, and another worker took both tickets over, finished the first
/// and settled the orphan. The fleet answers the late worker `claim-fenced`
/// for the orphan, which ends its claim and is not a failure of its pass.
#[tokio::test]
async fn a_late_worker_finds_the_orphan_already_settled_and_goes_on() {
    let mut wired = None;
    let (fx, _healthy, orphan) = two_registered_tool_tickets(|fx| {
        let tool = Arc::new(OverrunningTool {
            calls: AtomicUsize::new(0),
            clock: fx.wf_clock.clone(),
            other: tokio::sync::Mutex::new(None),
            other_pass: std::sync::Mutex::new(None),
        });
        wired = Some(tool.clone());
        Some(tool)
    })
    .await;
    let tool = wired.expect("the tool was built");
    drop_outbox_row(&fx, &orphan).await;
    *tool.other.lock().await = Some(fx.worker("worker-b", fx.tools.clone(), None));

    let late = fx
        .worker("worker-a", fx.tools.clone(), None)
        .claim_and_execute()
        .await
        .expect("a fenced settle of an orphan does not fail the pass");

    let other = tool
        .other_pass
        .lock()
        .expect("the pass slot is not poisoned")
        .expect("the other worker ran");
    assert_eq!(other.claimed, 2, "the other worker took both tickets over");
    assert_eq!(other.parked_indeterminate, 1);
    assert_eq!(tool.calls.load(Ordering::SeqCst), 2, "one call per worker");
    assert_eq!(late.claimed, 2);
    assert_eq!(late.parked_indeterminate, 0, "the run was told once");
    assert_eq!(late.cancelled, 0, "the late worker settled nothing");
    assert_settled_as_missing(fleet_entry(&fx, &orphan).await);
    assert_eq!(
        open_checkpoint_kinds(&fx).await,
        vec![AgentCheckpointKind::IndeterminateEffectReconciliation]
    );
}
