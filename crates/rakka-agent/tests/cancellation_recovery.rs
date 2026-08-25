//! Cancellation propagation under owner loss: the task-cancellation
//! compare-and-sets, swept.
//!
//! Slice 6.1 closes the crash-sweep debt slice 4.6 parked here by name.
//! `cancellation_propagation.rs` proves the propagation *spine* — the root
//! defers on its escrow, the coordinator winds down through the run-cancel
//! exchange, both children accept their delegation-cancel chase and report
//! terminal upward — but nothing ever killed an owner inside those writes.
//!
//! They are the compare-and-sets that decide whether a cancelled tree ever
//! quiesces, and cancellation is the one transition that must never claim more
//! than it knows: a chase that a crash replays must not report a child stopped
//! when it did not, and a re-driven leg must not re-invoke a child's opaque
//! send ([specification 8.7, 15, and 18](../../docs/plans/rakka-agent/spec.md),
//! scenarios 29, 31, and 34).
//!
//! Every sweep iteration builds its own tree, arms exactly one store at exactly
//! one write, drives to the loss, proves the window fired, and then re-drives
//! from durable state alone.

mod common;

use std::sync::Arc;

use common::{
    committed_children, create_fan_out_task, create_real_child, fan_out_fixture, task_scope,
    Fixture, SkillNamedExecutor, SKILL, SKILL_2, TENANT,
};
use rakka_agent::testkit::CrashPoint;
use rakka_agent::{
    AgentCancellationProgress, AgentDelegationCancelOutcome, AgentRunStatus, AgentTaskScope,
    AgentTaskStatus, TenantId,
};

/// A cancelled world, built up to but not including the cancel: the fan-out has
/// parked awaiting two children, and both children exist as real task entities.
async fn cancelling_world() -> (Fixture, Arc<SkillNamedExecutor>, Vec<AgentTaskScope>) {
    let executor = SkillNamedExecutor::new();
    let fixture = fan_out_fixture(executor.clone());
    create_fan_out_task(&fixture, None).await;
    fixture.pump().await.expect("the fan-out parks");
    let children = committed_children(&fixture).await;
    assert_eq!(children.len(), 2, "the fan-out committed both children");

    for (index, (delegation, child_task)) in children.iter().enumerate() {
        create_real_child(
            &fixture,
            delegation,
            child_task,
            if index == 0 { SKILL } else { SKILL_2 },
        )
        .await;
    }
    let scopes = children
        .iter()
        .map(|(_, child)| {
            AgentTaskScope::new(TenantId::new(TENANT), child.clone()).expect("the scope is valid")
        })
        .collect();
    (fixture, executor, scopes)
}

/// Requests the root cancel. Deduplicated on its operation id, so a re-drive
/// after a loss either re-applies it or is answered from the durable record —
/// which is exactly what a recovering operator command does.
async fn request_cancel(fixture: &Fixture) {
    let _ = fixture
        .apply_task_command(rakka_agent::AgentTaskEntityCommand::Cancel {
            operation_id: rakka_agent::AgentOperationId::new(
                rakka_agent::AgentOperationKind::Cancellation,
                [TENANT, task_scope().task().as_str(), "root-cancel"],
            )
            .expect("the operation id derives"),
            reason: "operator".to_string(),
        })
        .await;
}

/// Drives the root, the coordinator run, and every child to rest, tolerating an
/// injected loss. Errors are swallowed because a crashed owner is *supposed* to
/// fail here; convergence is asserted afterwards, from durable state.
async fn pump_tree(fixture: &Fixture, children: &[AgentTaskScope]) {
    for _round in 0..64 {
        let settled_root = fixture.settle_task_at(&task_scope()).await.is_ok();
        for scope in children {
            let _ = fixture.settle_task_at(scope).await;
        }
        let mut run = fixture.run();
        if run.recover(fixture.now()).await.is_err() {
            continue;
        }
        let Ok(progress) = run
            .settle_side_effects(&fixture.router, fixture.now())
            .await
        else {
            continue;
        };
        let Ok(answered) = fixture
            .dispatcher
            .drive(&mut run, &fixture.router, fixture.now())
            .await
        else {
            continue;
        };
        if settled_root
            && answered == 0
            && progress.transitions == 0
            && progress.effects_dispatched == 0
            && progress.settled == 0
            && progress.failed == 0
            && progress.outstanding == 0
        {
            return;
        }
    }
}

/// The whole cancellation, from the request to the tree's quiescence.
///
/// Two rounds, because a window that killed the owner inside the request itself
/// leaves nothing for the first round's propagation to chase.
async fn drive(fixture: &Fixture, children: &[AgentTaskScope]) {
    for _ in 0..2 {
        request_cancel(fixture).await;
        pump_tree(fixture, children).await;
    }
}

/// The one ending a converged cancellation has: every child terminal under the
/// requested reason, every cell holding an accepted chase *and* the child's own
/// terminal report, the coordinator quiesced, and the root finalized only after
/// its ledger closed.
async fn assert_converged(
    fixture: &Fixture,
    executor: &SkillNamedExecutor,
    children: &[AgentTaskScope],
    context: &str,
) {
    for scope in children {
        let state = rakka_agent::load_agent_task_state(
            &fixture.tasks,
            scope,
            &rakka_agent::AgentSchemaPolicy::default(),
        )
        .await
        .unwrap_or_else(|error| panic!("{context}: the child state loads: {error}"))
        .unwrap_or_else(|| panic!("{context}: the child exists"));
        let snapshot = state
            .snapshot()
            .unwrap_or_else(|| panic!("{context}: the child snapshot derives"));
        assert_eq!(
            snapshot.status,
            AgentTaskStatus::Cancelled,
            "{context}: the child terminalized"
        );
        assert_eq!(
            snapshot
                .terminal_reason
                .as_ref()
                .map(|reason| reason.code()),
            Some("cancellation-requested"),
            "{context}: under the requested reason"
        );
    }

    let mut run = fixture.run();
    run.recover(fixture.now())
        .await
        .unwrap_or_else(|error| panic!("{context}: the run recovers: {error}"));
    let state = run
        .state()
        .unwrap_or_else(|error| panic!("{context}: the run state reads: {error}"));
    assert_eq!(
        state.status(),
        Some(AgentRunStatus::Cancelled),
        "{context}: the coordinator quiesced"
    );
    let progress = state
        .run()
        .map_or(AgentCancellationProgress::NotRequested, |run| {
            AgentCancellationProgress::derive(run)
        });
    assert_eq!(
        progress,
        AgentCancellationProgress::Completed,
        "{context}: nothing is still outstanding"
    );

    let loop_state = state
        .loop_state()
        .unwrap_or_else(|| panic!("{context}: the loop state survives"));
    let mut chased = 0usize;
    for (delegation, cell) in loop_state.delegations() {
        assert!(
            matches!(
                cell.cancel,
                Some(AgentDelegationCancelOutcome::Accepted { .. })
            ),
            "{context}: {delegation}'s chase settled accepted"
        );
        let result = cell
            .result
            .as_ref()
            .unwrap_or_else(|| panic!("{context}: {delegation} recorded its child's report"));
        assert_eq!(result.status, AgentTaskStatus::Cancelled, "{context}");
        chased += 1;
    }
    assert_eq!(chased, 2, "{context}: two cells, no more and no fewer");

    let view = fixture.task_snapshot().await;
    assert_eq!(
        view.status,
        AgentTaskStatus::Cancelled,
        "{context}: the root finalized"
    );
    assert_eq!(
        view.terminal_reason.as_ref().map(|reason| reason.code()),
        Some("cancellation-requested"),
        "{context}: under the marker's reason, not a later one"
    );
    assert_eq!(
        AgentCancellationProgress::derive_task(&view),
        AgentCancellationProgress::Completed,
        "{context}"
    );

    // Scenario 29's at-most-once half: no re-driven propagation leg may
    // re-invoke a child's opaque send. The identity is derived, so a redispatch
    // is a retry of one delegation rather than a second child — but a
    // *propagation* leg must not dispatch a send at all.
    assert_eq!(
        executor.delegations().len(),
        2,
        "{context}: one logical child per skill"
    );
    assert_eq!(
        executor.invocations(),
        2,
        "{context}: no propagation leg replays a send"
    );
}

/// The durable writes one crash-free cancellation attempts on each store, so
/// the sweeps below cover every real write rather than a guess.
async fn reference_writes() -> (usize, usize) {
    let (fixture, executor, children) = cancelling_world().await;
    fixture.tasks.reset_writes();
    fixture.runs.reset_writes();
    drive(&fixture, &children).await;
    assert_converged(&fixture, &executor, &children, "the crash-free reference").await;
    (fixture.tasks.writes(), fixture.runs.writes())
}

#[tokio::test]
async fn the_cancellation_converges_across_every_task_store_crash_point() {
    let (task_writes, _) = reference_writes().await;
    // The root's request marker, each child's durable acceptance, and the
    // root's finalization are distinct compare-and-sets at a minimum; asserting
    // the floor keeps a sweep from silently covering nothing if the flow
    // changes. The task store carries all three task entities.
    assert!(
        task_writes >= 4,
        "the cancellation writes the task store at least four times \
         (the root marker, two child acceptances, the root's finalization), \
         saw {task_writes}"
    );

    for point in 1..=task_writes {
        for window in [CrashPoint::BeforeWrite, CrashPoint::AfterWrite] {
            let (fixture, executor, children) = cancelling_world().await;
            fixture.tasks.reset_writes();
            fixture.tasks.crash_at(point, window);
            drive(&fixture, &children).await;
            fixture.tasks.assert_crash_fired(point, window);
            fixture.tasks.survive();

            // A new owner, with nothing but the durable record.
            drive(&fixture, &children).await;
            assert_converged(
                &fixture,
                &executor,
                &children,
                &format!("task-store crash at write {point} ({window:?})"),
            )
            .await;
        }
    }
}

#[tokio::test]
async fn the_cancellation_converges_across_every_run_store_crash_point() {
    let (_, run_writes) = reference_writes().await;
    // The wind-down, each chase's committed intent, each accepted receipt, and
    // the terminalizing quiescence are distinct writes at a minimum.
    assert!(
        run_writes >= 4,
        "the cancellation writes the run store at least four times \
         (the wind-down, two chases, the quiescence), saw {run_writes}"
    );

    for point in 1..=run_writes {
        for window in [CrashPoint::BeforeWrite, CrashPoint::AfterWrite] {
            let (fixture, executor, children) = cancelling_world().await;
            fixture.runs.reset_writes();
            fixture.runs.crash_at(point, window);
            drive(&fixture, &children).await;
            fixture.runs.assert_crash_fired(point, window);
            fixture.runs.survive();

            drive(&fixture, &children).await;
            assert_converged(
                &fixture,
                &executor,
                &children,
                &format!("run-store crash at write {point} ({window:?})"),
            )
            .await;
        }
    }
}
