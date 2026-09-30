//! Handoff under owner loss: the task-side resolution machine, swept.
//!
//! Slice 5.6's fault-injection half of scenario 38
//! ([specification 8.9, 15, and 18](../../docs/plans/rakka-agent/spec.md)).
//! Slice 5.1 swept the *run* store's committed-but-unsent fence window
//! (`handoff_cancellation.rs`); the load-bearing piece it left unswept is the
//! task side — the offer, the target's assignment generation, the
//! `HandoffResult` once-guard, and the two ways a transfer resolves. Those
//! are the compare-and-sets that decide who owns one `AgentTaskId`, so an
//! owner that dies inside any of them must converge on exactly one answer.
//!
//! Every sweep iteration builds its own world, arms exactly one store at
//! exactly one write, drives to the loss, proves the window fired, and then
//! re-drives from durable state alone — which is all a new owner ever has.

mod common;

use std::sync::Arc;

use common::{
    goal_spec_draft, goal_spec_with_handoff, goal_task_creation_command, handoff_config,
    handoff_target_scope, handoff_tool_id, task_definition, ApplyingHandoffExecutor, Fixture,
    HANDOFF_SKILL, HANDOFF_TARGET,
};
use rakka_agent::testkit::{
    CrashPoint, DeterministicModelAdapter, ExchangeFault, ScriptedDispatcher,
};
use rakka_agent::{
    AgentAssignmentGeneration, AgentAssignmentStatus, AgentModelTurn, AgentRunStatus,
    AgentTaskHandoffStatus, AgentToolCallId, AgentToolCallRequest,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
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

/// The transfer's task definition: a bounded per-run allocation, so the
/// source's still-open escrow child leaves headroom for the target's
/// generation. `handoff_record.rs` documents why an exact-fit budget makes a
/// transfer deterministically unaffordable.
fn handoff_task_definition() -> rakka_agent::AgentTaskDefinition {
    let mut per_run = rakka_agent::AgentBudgetAllocation::unbounded();
    per_run.set(rakka_agent::AgentBudgetDimension::LoopIterations, Some(3));
    task_definition()
        .with_budgets(rakka_agent::AgentBudgetCeilings {
            max_loop_iterations: Some(12),
            ..rakka_agent::AgentBudgetCeilings::unbounded()
        })
        .with_run_allocation(per_run)
}

/// A world one turn away from committing the transfer: both agents
/// instantiated, the goal task created, and the applying executor wired so
/// the send reaches the task entity exactly as the A2A ingress would.
async fn transferring_world() -> (Fixture, Arc<ApplyingHandoffExecutor>) {
    let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn(handoff_turn()),
    ))
    .with_delegation(handoff_config());
    let executor = ApplyingHandoffExecutor::over(&fixture);
    let _ = fixture
        .dispatcher
        .clone()
        .with_a2a_handoff_executor(executor.clone());
    fixture.instantiate_agent().await;
    fixture
        .apply_task_command(goal_task_creation_command(
            handoff_task_definition(),
            goal_spec_draft(goal_spec_with_handoff(), true),
        ))
        .await
        .expect("the goal task creates");
    fixture.instantiate_agent_at(handoff_target_scope()).await;
    (fixture, executor)
}

/// Drives the whole transfer to quiescence, tolerating an injected loss: the
/// run commits the record and its send, the executor applies `RecordHandoff`,
/// and the settle passes carry the assignment out and the `HandoffResult`
/// back. Errors are swallowed because a crashed owner is *supposed* to fail
/// here; convergence is asserted afterwards, from durable state.
async fn drive(fixture: &Fixture) {
    let _ = fixture.pump().await;
    for _ in 0..8 {
        let _ = fixture.settle_task_at(&common::task_scope()).await;
    }
}

/// Which way a converged transfer resolved.
#[derive(Debug, PartialEq, Eq)]
enum Resolution {
    /// The target owns the task and the source terminalized `HandedOff`.
    Transferred,
    /// The transfer never took, and the stashed source assignment was
    /// restored — the single-attempt posture's other arm.
    Restored,
}

/// The convergence property, asserted from durable state alone.
///
/// A transfer has exactly two correct endings, and which one a crash window
/// produces is *not* the invariant — that depends on whether the offer had
/// committed when the owner died, which is the whole point of sweeping. The
/// invariant is that the ending is one of the two, reached whole: one owner,
/// one `AgentTaskId`, and no half state where the task believes it moved and
/// the source believes it did not, or the reverse.
async fn assert_converged(fixture: &Fixture, context: &str) -> Resolution {
    let task = fixture.task_snapshot().await;
    assert_eq!(
        task.scope.task().as_str(),
        common::TASK,
        "{context}: the task identity is preserved either way — a transfer is never a new task"
    );

    let mut run = fixture.run();
    run.recover(fixture.now())
        .await
        .expect("the source recovers");
    let source_status = run.state().expect("the source state loads").status();
    drop(run);

    let assignment = task.assignment.as_ref().unwrap_or_else(|| {
        panic!(
            "{context}: the task has an owner: handoff={:?}",
            task.handoff
        )
    });

    if assignment.agent.as_str() == HANDOFF_TARGET {
        assert_eq!(
            assignment.generation,
            AgentAssignmentGeneration::new(2),
            "{context}: exactly one new generation, however many times the flow re-drove"
        );
        assert_eq!(
            assignment.status,
            AgentAssignmentStatus::Accepted,
            "{context}"
        );
        let provenance = task
            .handoff
            .as_deref()
            .unwrap_or_else(|| panic!("{context}: a transferred task carries its provenance"));
        assert_eq!(
            provenance.status,
            AgentTaskHandoffStatus::Accepted,
            "{context}"
        );
        assert!(
            provenance.result_settled,
            "{context}: the result exchange settled, which is what let the source terminalize"
        );
        assert_eq!(
            provenance.source_assignment.agent.as_str(),
            common::AGENT,
            "{context}: the stashed source survives for the goal view to join"
        );
        assert_eq!(
            source_status,
            Some(AgentRunStatus::HandedOff),
            "{context}: the source terminalized, and only after the target's acceptance"
        );
        return Resolution::Transferred;
    }

    // The other arm: the offer never took, so the source keeps the task it
    // never stopped owning.
    assert_eq!(
        assignment.agent.as_str(),
        common::AGENT,
        "{context}: an untransferred task is owned by its source, not by nobody"
    );
    assert_eq!(
        assignment.generation,
        AgentAssignmentGeneration::new(1),
        "{context}: the restored assignment is the stashed one, not a fresh generation"
    );
    assert_ne!(
        source_status,
        Some(AgentRunStatus::HandedOff),
        "{context}: a source that kept the task never reports having handed it off"
    );
    if let Some(provenance) = task.handoff.as_deref() {
        assert_ne!(
            provenance.status,
            AgentTaskHandoffStatus::Accepted,
            "{context}: a restored source cannot sit beside an accepted transfer"
        );
    }
    Resolution::Restored
}

/// The durable writes one crash-free transfer attempts on each store, so the
/// sweeps below cover every real write rather than a guess.
async fn reference_writes() -> (usize, usize) {
    let (fixture, _executor) = transferring_world().await;
    fixture.tasks.reset_writes();
    fixture.runs.reset_writes();
    drive(&fixture).await;
    // The reference run is crash-free, so it must reach the *transferred*
    // arm; a reference that quietly restored would make every sweep below
    // measure a flow that never transferred anything.
    assert_eq!(
        assert_converged(&fixture, "the crash-free reference").await,
        Resolution::Transferred
    );
    (fixture.tasks.writes(), fixture.runs.writes())
}

#[tokio::test]
async fn the_transfer_converges_across_every_task_store_crash_point() {
    let (task_writes, _) = reference_writes().await;
    // The offer, the target's generation, the acceptance, and the result
    // settle are four distinct compare-and-sets at a minimum; asserting the
    // floor keeps a sweep from silently covering nothing if the flow changes.
    assert!(
        task_writes >= 4,
        "the transfer writes the task store at least four times \
         (offer, generation, acceptance, result settle), saw {task_writes}"
    );

    let mut transferred = 0usize;
    for point in 1..=task_writes {
        for window in [CrashPoint::BeforeWrite, CrashPoint::AfterWrite] {
            let (fixture, executor) = transferring_world().await;
            fixture.tasks.reset_writes();
            fixture.tasks.crash_at(point, window);
            drive(&fixture).await;
            fixture.tasks.assert_crash_fired(point, window);
            fixture.tasks.survive();

            // A new owner, with nothing but the durable record.
            drive(&fixture).await;
            let context = format!("task-store crash at write {point} ({window:?})");
            let resolution = assert_converged(&fixture, &context).await;
            transferred += usize::from(resolution == Resolution::Transferred);
            assert_eq!(
                executor.seen.lock().expect("the log is not poisoned").len(),
                1,
                "{context}: the send is attempted once, whichever way it resolved"
            );
        }
    }

    // The sweep is only worth anything if it exercised both endings: a run
    // where every window restored would prove nothing about the transferred
    // arm, and vice versa.
    let windows = task_writes * 2;
    assert!(
        transferred > 0 && transferred < windows,
        "the sweep must cover both resolutions: {transferred} of {windows} windows transferred"
    );
}

#[tokio::test]
async fn the_transfer_converges_across_every_run_store_crash_point() {
    // The source side of the same flow: the cell commits with the send
    // effect, and the `HandoffResult` settle is what finally terminalizes
    // `HandedOff`. A loss anywhere between them must not leave the source
    // alive over a responsibility that durably moved.
    let (_, run_writes) = reference_writes().await;
    assert!(
        run_writes >= 2,
        "the transfer writes the run store at least twice \
         (the committing cell, the terminalizing settle), saw {run_writes}"
    );

    for point in 1..=run_writes {
        for window in [CrashPoint::BeforeWrite, CrashPoint::AfterWrite] {
            let (fixture, _executor) = transferring_world().await;
            fixture.runs.reset_writes();
            fixture.runs.crash_at(point, window);
            drive(&fixture).await;
            fixture.runs.assert_crash_fired(point, window);
            fixture.runs.survive();

            drive(&fixture).await;
            assert_converged(
                &fixture,
                &format!("run-store crash at write {point} ({window:?})"),
            )
            .await;
        }
    }
}

#[tokio::test]
async fn the_handoff_result_survives_every_delivery_fault() {
    // The twelfth exchange's own failure windows, at the real entity rather
    // than through the synthetic choreography probe: the derivation that owes
    // `HandoffResult` is re-run by every settle pass, and the provenance's
    // `result_settled` marker is the once-guard past the journal window — so
    // a lost envelope, a lost reply, and a doubled delivery must all land on
    // one terminal source.
    for fault in [
        ExchangeFault::LoseEnvelope,
        ExchangeFault::LoseReply,
        ExchangeFault::DeliverTwice,
    ] {
        let (fixture, executor) = transferring_world().await;
        fixture.run_transport.inject(fault);
        drive(&fixture).await;

        let context = format!("{fault:?}");
        assert_converged(&fixture, &context).await;
        assert_eq!(
            executor.seen.lock().expect("the log is not poisoned").len(),
            1,
            "{context}: one transfer, whatever the delivery did"
        );
    }
}
