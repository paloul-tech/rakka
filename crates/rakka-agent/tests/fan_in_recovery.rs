//! Fan-out and fan-in under owner loss: the coordinator's compare-and-sets, swept.
//!
//! Slice 6.1 closes the crash-sweep debt slice 4.4 parked here by name.
//! `fan_out_fan_in.rs` proves the fan-in *logic* — the group opens with a policy
//! taken from trusted state, the await verb closes membership, and the policy
//! resolves as a pure function of the durable cells — but nothing ever killed an
//! owner inside those writes. They are the compare-and-sets that decide how many
//! logical children exist and whether a parked parent ever wakes, so an owner
//! that dies inside any of them must converge on exactly one answer
//! ([specification 15 and 18](../../docs/plans/rakka-agent/spec.md), scenarios
//! 27, 28, and 34).
//!
//! Every sweep iteration builds its own world, arms exactly one store at exactly
//! one write, drives to the loss, proves the window fired, and then re-drives
//! from durable state alone — which is all a new owner ever has.
//!
//! The scripted model is keyed by *turn number* rather than by call order
//! ([`DeterministicModelAdapter::with_turn_for`]). A crash that rolls a turn's
//! commit back re-asks for that same turn, and an adapter scripted by order
//! would answer the next one — which would make the sweep measure the harness
//! rather than the run.

mod common;

use std::sync::Arc;

use common::{
    child_result_envelope, create_fan_out_task, delegation_config_with_fan_in, fan_out_turn,
    proposing_turn, Fixture, SkillNamedExecutor,
};
use rakka_agent::testkit::{
    sweep_crash_points, DeterministicModelAdapter, ExchangeFault, ScriptedDispatcher,
};
use rakka_agent::{
    AgentDelegationId, AgentDelegationStatus, AgentExchangeEnvelope, AgentExchangeTransport,
    AgentRunStatus, AgentTaskId, AgentTaskStatus,
};

/// A fan-out world: the agent, the goal task, and a peer surface that names
/// each child after the skill it serves.
async fn fan_out_world() -> (Fixture, Arc<SkillNamedExecutor>) {
    let executor = SkillNamedExecutor::new();
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn_for(1, fan_out_turn())
                .with_turn_for(2, proposing_turn()),
        )
        .with_a2a_send_executor(executor.clone()),
    )
    .with_delegation(delegation_config_with_fan_in());
    create_fan_out_task(&fixture, None).await;
    (fixture, executor)
}

/// The committed members, tolerating a store that is currently crashing.
async fn children(fixture: &Fixture) -> Vec<(AgentDelegationId, AgentTaskId)> {
    let mut run = fixture.run();
    if run.recover(fixture.now()).await.is_err() {
        return Vec::new();
    }
    let Ok(state) = run.state() else {
        return Vec::new();
    };
    let Some(loop_state) = state.loop_state() else {
        return Vec::new();
    };
    loop_state
        .delegations()
        .iter()
        .filter_map(|(id, cell)| match &cell.status {
            AgentDelegationStatus::ChildCreated { child_task, .. } => {
                Some((id.clone(), child_task.clone()))
            }
            _ => None,
        })
        .collect()
}

/// Delivers one child's terminal report, tolerating an injected loss.
///
/// Through `run_transport`, which is where the faults are injected. Building a
/// run entity here and calling `accept` on it reaches the same durable path but
/// goes *around* the transport, so an injected `LoseEnvelope`, `LoseReply`, or
/// `DeliverTwice` was consumed by whatever run-bound exchange the router
/// delivered next — never by the `DelegationResult` this file exists to fault.
/// The transport builds its own run entity from the same store, so the durable
/// effect is unchanged; what changes is that the fault now lands here.
async fn deliver(fixture: &Fixture, envelope: &AgentExchangeEnvelope) {
    let _ = fixture.run_transport.deliver(envelope).await;
}

/// Drives the whole fan-out to quiescence, tolerating an injected loss: the
/// turn commits both delegations and closes the group, the sends dispatch, each
/// child's terminal report arrives as its own durable exchange, and the resumed
/// model proposes the parent's own result. Errors are swallowed because a
/// crashed owner is *supposed* to fail here; convergence is asserted afterwards,
/// from durable state.
///
/// Two rounds, because the first may find nothing to deliver: a window that
/// killed the owner before the delegations committed has no children to report
/// until the re-drive commits them.
async fn drive(fixture: &Fixture) {
    for _ in 0..2 {
        let _ = fixture.pump().await;
        for (delegation, child_task) in children(fixture).await {
            let envelope = child_result_envelope(
                fixture,
                &delegation,
                &child_task,
                AgentTaskStatus::Completed,
            );
            deliver(fixture, &envelope).await;
        }
        let _ = fixture.pump().await;
    }
}

/// The one ending a converged fan-out has: the group resolved on every member,
/// each cell carries its child's result, and the parent completed through its
/// own proposal rather than through a child's completion.
async fn assert_converged(fixture: &Fixture, executor: &SkillNamedExecutor, context: &str) {
    let mut run = fixture.run();
    run.recover(fixture.now())
        .await
        .unwrap_or_else(|error| panic!("{context}: the run recovers: {error}"));
    let state = run
        .state()
        .unwrap_or_else(|error| panic!("{context}: the run state reads: {error}"));
    assert_eq!(
        state.status(),
        Some(AgentRunStatus::Completed),
        "{context}: the parent completed"
    );

    let loop_state = state
        .loop_state()
        .unwrap_or_else(|| panic!("{context}: the loop state survives"));
    let group = loop_state
        .fan_in()
        .unwrap_or_else(|| panic!("{context}: the group is retained"));
    assert!(group.closed, "{context}: the await verb closed the group");
    assert_eq!(group.members.len(), 2, "{context}: both members survived");
    let resolution = group
        .resolution
        .as_ref()
        .unwrap_or_else(|| panic!("{context}: the group resolved"));
    assert!(resolution.satisfied, "{context}: the policy was satisfied");
    assert_eq!(
        resolution.code, "all-settled",
        "{context}: on every member, not on a deadline"
    );

    let mut settled = 0usize;
    for (delegation, cell) in loop_state.delegations() {
        let result = cell
            .result
            .as_ref()
            .unwrap_or_else(|| panic!("{context}: {delegation} recorded its child's result"));
        assert_eq!(result.status, AgentTaskStatus::Completed, "{context}");
        settled += 1;
    }
    assert_eq!(settled, 2, "{context}: two cells, no more and no fewer");

    // Scenario 28's half of the claim: a redispatched send is a retry of one
    // logical delegation, never a second child. The identity is derived, so a
    // re-drive that sends again still names the same two.
    assert_eq!(
        executor.delegations().len(),
        2,
        "{context}: one logical child per skill, however often the send retried"
    );

    let task = fixture.task_snapshot().await;
    assert!(
        task.accepted_result.is_some(),
        "{context}: the parent completed through its own proposal"
    );
}

/// The durable writes one crash-free fan-out attempts on each store, so the
/// sweeps below cover every real write rather than a guess.
async fn reference_writes() -> (usize, usize) {
    let (fixture, executor) = fan_out_world().await;
    fixture.tasks.reset_writes();
    fixture.runs.reset_writes();
    drive(&fixture).await;
    assert_converged(&fixture, &executor, "the crash-free reference").await;
    (fixture.tasks.writes(), fixture.runs.writes())
}

#[tokio::test]
async fn the_fan_out_converges_across_every_run_store_crash_point() {
    let (_, run_writes) = reference_writes().await;
    // The committing turn, each child's recorded result, and the resolution
    // that resumes the loop are distinct compare-and-sets at a minimum;
    // asserting the floor keeps a sweep from silently covering nothing if the
    // flow changes.
    assert!(
        run_writes >= 4,
        "the fan-out writes the run store at least four times \
         (the committing turn, two child results, the proposal), saw {run_writes}"
    );

    sweep_crash_points(run_writes, |point, window| async move {
        let (fixture, executor) = fan_out_world().await;
        fixture.runs.reset_writes();
        fixture.runs.crash_at(point, window);
        drive(&fixture).await;
        fixture.runs.assert_crash_fired(point, window);
        fixture.runs.survive();

        // A new owner, with nothing but the durable record.
        drive(&fixture).await;
        assert_converged(
            &fixture,
            &executor,
            &format!("run-store crash at write {point} ({window:?})"),
        )
        .await;
    })
    .await;
}

#[tokio::test]
async fn the_fan_out_converges_across_every_task_store_crash_point() {
    let (task_writes, _) = reference_writes().await;
    assert!(
        task_writes >= 2,
        "the fan-out writes the task store at least twice \
         (the assignment decision, the accepted result), saw {task_writes}"
    );

    sweep_crash_points(task_writes, |point, window| async move {
        let (fixture, executor) = fan_out_world().await;
        fixture.tasks.reset_writes();
        fixture.tasks.crash_at(point, window);
        drive(&fixture).await;
        fixture.tasks.assert_crash_fired(point, window);
        fixture.tasks.survive();

        drive(&fixture).await;
        assert_converged(
            &fixture,
            &executor,
            &format!("task-store crash at write {point} ({window:?})"),
        )
        .await;
    })
    .await;
}

#[tokio::test]
async fn the_delegation_result_survives_every_delivery_fault() {
    // The ninth exchange's own failure windows, at the real run entity rather
    // than through the synthetic choreography probe: a lost envelope, a lost
    // reply, and a doubled delivery must all land on one resolved group with
    // one result per cell.
    for fault in [
        ExchangeFault::LoseEnvelope,
        ExchangeFault::LoseReply,
        ExchangeFault::DeliverTwice,
    ] {
        let (fixture, executor) = fan_out_world().await;
        let before = fixture.run_transport.deliveries();
        fixture.run_transport.inject(fault);
        drive(&fixture).await;

        // Without this the test passes whether or not the envelope ever
        // travelled the transport the fault was queued on, which is exactly how
        // all three faults were no-ops here.
        assert!(
            fixture.run_transport.deliveries() > before,
            "{fault:?}: the DelegationResult never travelled the faulted transport"
        );
        assert_converged(&fixture, &executor, &format!("{fault:?}")).await;
    }
}
