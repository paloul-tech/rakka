//! The M3 acceptance walk: one continuous goal demonstrating every bullet of
//! the continuous-goal milestone checklist
//! (`docs/plans/rakka-agent/spec.md`, "Continuous Goal Milestone (M3)"),
//! with fault injection across pod restart and shard movement.
//!
//! Every entity in the walk is rebuilt from durable state per call, so every
//! step is already a restart; the crash windows and the stale-owner fence
//! inject the losses a fleet actually suffers.

use std::sync::atomic::Ordering;

use rakka_agent::{
    agent_task_operational_snapshot, epoch_result_operation_id, epoch_task_id_for_wake,
    load_agent_task_state, next_pending_wake_for_task, run_id_for_assignment,
    wake_admission_command, AgentAssignmentGeneration, AgentBudgetConsumption, AgentEntityAddress,
    AgentEpochResult, AgentExchangeEnvelope, AgentExchangeKind, AgentExchangePayload,
    AgentGoalLifecycleStatus, AgentOperationId, AgentOperationKind, AgentRunScope,
    AgentSchemaPolicy, AgentTaskEntityCommand, AgentTaskEntityReply, AgentTaskScope,
    AgentTaskStatus, AgentWakeBinding, AgentWakeDisposition, AgentWakePolicyRevision,
    AgentWakeScanOutcome, AgentWakeTimerStore, ScheduleRevision,
};
use rakka_agent_workflow::{AgentCorrelationId, AgentTimestampMillis};

use crate::report::AcceptanceReport;
use crate::wiring::{
    agent_id, goal_id, provenance, task_scope, tenant, windowed_policy, World, TASK, TENANT,
    WINDOW_MS,
};

/// The epoch task and run scopes one wake derives.
fn epoch_scopes(wake: &rakka_agent::AgentWakeId) -> (AgentTaskScope, AgentRunScope) {
    let task = epoch_task_id_for_wake(wake).expect("the epoch task derives");
    let run = run_id_for_assignment(&task, AgentAssignmentGeneration::new(1))
        .expect("the epoch run derives");
    (
        AgentTaskScope::new(tenant(), task).expect("the epoch task scope is valid"),
        AgentRunScope::new(tenant(), agent_id(), run).expect("the epoch run scope is valid"),
    )
}

/// A legitimate epoch-result envelope: what the epoch task itself owes the
/// controller once its run terminates.
fn epoch_result(
    world: &World,
    binding: &AgentWakeBinding,
    status: AgentTaskStatus,
) -> AgentExchangeEnvelope {
    let epoch_task = epoch_task_id_for_wake(binding.wake_id()).expect("the epoch derives");
    let epoch_scope =
        AgentTaskScope::new(tenant(), epoch_task.clone()).expect("the scope is valid");
    let operation_id = epoch_result_operation_id(&tenant(), &goal_id(), binding.wake_id())
        .expect("the operation id derives");
    let result = AgentEpochResult {
        wake: binding.wake_id().clone(),
        task: epoch_task,
        status,
        consumed: AgentBudgetConsumption::zero(),
        result_digest: None,
    };
    AgentExchangeEnvelope::new(
        operation_id.clone(),
        AgentExchangeKind::EpochResult,
        AgentEntityAddress::Task(epoch_scope),
        AgentEntityAddress::Task(task_scope()),
        AgentExchangePayload::encode(rakka_agent::AGENT_EPOCH_RESULT_PAYLOAD_TYPE, &result)
            .expect("the payload encodes"),
        AgentCorrelationId::new(operation_id.as_str()),
        world.now(),
    )
    .expect("the envelope builds")
}

/// Settles one epoch's result on the controller through a freshly built
/// owner, then runs the settle pass that parks whatever the acceptance owed.
async fn settle_epoch(world: &World, binding: &AgentWakeBinding, status: AgentTaskStatus) {
    let envelope = epoch_result(world, binding, status);
    let mut root = world.root_store();
    root.recover(world.now()).await.expect("the root recovers");
    let reply = root
        .accept(&envelope, &world.router, world.now())
        .await
        .expect("the result is answered");
    assert!(reply.result().is_accepted(), "the epoch result lands");
    world
        .settle_task(&task_scope())
        .await
        .expect("the root settles");
}

/// One scan pass over the durable wake index.
async fn scan(world: &World) -> Vec<AgentWakeScanOutcome> {
    world
        .scanner()
        .scan_due()
        .await
        .expect("the scan pass runs")
        .outcomes
}

/// The dispositions of one scan pass, in delivery order.
async fn scan_dispositions(world: &World) -> Vec<AgentWakeDisposition> {
    scan(world)
        .await
        .into_iter()
        .map(|outcome| match outcome {
            AgentWakeScanOutcome::Dispositioned { disposition, .. } => disposition,
            other => panic!("the delivery dispositions, got {other:?}"),
        })
        .collect()
}

/// An operation id for one lifecycle command.
fn lifecycle_operation(kind: AgentOperationKind, discriminator: &str) -> AgentOperationId {
    AgentOperationId::new(kind, [TENANT, TASK, discriminator]).expect("the operation id derives")
}

/// Runs the whole M3 acceptance walk and returns its report.
///
/// # Panics
///
/// Panics if any milestone fact fails to hold — this walk is the milestone's
/// executable acceptance statement.
#[allow(clippy::too_many_lines)]
pub async fn run_acceptance() -> AcceptanceReport {
    let world = World::new();
    let mut lines = Vec::new();

    // 1. The root: a human-owned continuous control task. After creation the
    // entity store is dropped — nothing but the durable record holds the
    // goal, which is exactly what passivation means here.
    world.instantiate_agent().await;
    world.create_root().await;
    let state = world.controller().await;
    assert_eq!(state.lifecycle().status(), AgentGoalLifecycleStatus::Active);
    assert!(state.active().is_empty() && state.pending().is_empty());
    lines.push(
        "ok  1/16 the continuous root is durable and passivatable: controller state persisted, \
         no resident actor, loop, or timer"
            .to_string(),
    );

    // 2. A scheduled occurrence, parked durably and delivered by the scanner,
    // admits one derived epoch — and the epoch actually runs: model call,
    // accepted result, completed task, and the owed result exchange that
    // releases the occurrence on the controller.
    let occ_a = world.schedule(5_000, ScheduleRevision::INITIAL).await;
    world.clock.store(5_050, Ordering::SeqCst);
    let dispositions = scan_dispositions(&world).await;
    assert!(
        matches!(dispositions[..], [AgentWakeDisposition::Admitted { .. }]),
        "the due occurrence admits, got {dispositions:?}"
    );
    let (epoch_a, run_a) = epoch_scopes(occ_a.wake_id());
    world
        .pump_epoch(&epoch_a, &run_a)
        .await
        .expect("the epoch converges");
    let state = world.controller().await;
    assert_eq!(state.counters().released, 1, "the result released the wake");
    assert!(state.active().is_empty());
    let epoch_a_state =
        load_agent_task_state(&world.tasks, &epoch_a, &AgentSchemaPolicy::default())
            .await
            .expect("the epoch state loads")
            .expect("the epoch task exists");
    assert_eq!(
        epoch_a_state.task().expect("the epoch is created").status,
        AgentTaskStatus::Completed
    );
    lines.push(
        "ok  2/16 a scheduled occurrence admitted one derived epoch task and run; the epoch ran \
         to completion and its result released the occurrence"
            .to_string(),
    );

    // 3. The same admission replayed — a crashed scanner's redelivery, a
    // concurrent scanner, a stutter — answers from the durable record.
    let replay = world
        .apply_root_command(wake_admission_command(occ_a.clone()).expect("the command derives"))
        .await
        .expect("the replay answers");
    assert!(
        matches!(replay, AgentTaskEntityReply::Duplicate { .. }),
        "the replay answers Duplicate, got {replay:?}"
    );
    assert_eq!(world.controller().await.counters().admitted, 1);
    lines.push(
        "ok  3/16 the replayed admission answered Duplicate from the durable record: one \
         admission, one epoch"
            .to_string(),
    );

    // 4. Overlap is forbidden by default: with an epoch active, the next
    // delivery coalesces durably instead of running beside it.
    let occ_b = world.schedule(10_000, ScheduleRevision::INITIAL).await;
    let occ_c = world.schedule(12_000, ScheduleRevision::INITIAL).await;
    world.clock.store(15_000, Ordering::SeqCst);
    let dispositions = scan_dispositions(&world).await;
    assert!(
        matches!(
            dispositions[..],
            [
                AgentWakeDisposition::Admitted { .. },
                AgentWakeDisposition::Coalesced { .. }
            ]
        ),
        "the second delivery coalesces, got {dispositions:?}"
    );
    lines.push(
        "ok  4/16 overlap is forbidden: the next occurrence admitted and the one behind it \
         coalesced durably"
            .to_string(),
    );

    // 5. Pod restart mid-settlement: the owner dies on the durable write that
    // would settle the active epoch's result. The rebuilt owner replays the
    // same exchange and converges — release, promotion, one epoch each.
    world
        .tasks
        .crash_at(1, rakka_agent::testkit::CrashPoint::BeforeWrite);
    let envelope = epoch_result(&world, &occ_b, AgentTaskStatus::Completed);
    let mut doomed = world.root_store();
    doomed
        .recover(world.now())
        .await
        .expect("the doomed owner recovers");
    let crashed = doomed.accept(&envelope, &world.router, world.now()).await;
    assert!(crashed.is_err(), "the owner died mid-write");
    world.tasks.survive();
    let mut rebuilt = world.root_store();
    rebuilt
        .recover(world.now())
        .await
        .expect("the rebuilt owner recovers");
    let reply = rebuilt
        .accept(&envelope, &world.router, world.now())
        .await
        .expect("the replayed exchange lands");
    assert!(reply.result().is_accepted());
    world
        .settle_task(&task_scope())
        .await
        .expect("the root settles");
    let state = world.controller().await;
    assert_eq!(state.counters().admitted, 3, "the promotion admitted");
    assert_eq!(state.active().len(), 1);
    lines.push(
        "ok  5/16 the owner died mid-settlement; the rebuilt owner replayed the same exchange \
         and converged: one release, one promotion"
            .to_string(),
    );
    settle_epoch(&world, &occ_c, AgentTaskStatus::Completed).await;

    // 6. Downtime: three occurrences come due while nothing scans. The
    // backlog admits exactly one coalesced representative; the rest are
    // absorbed as missed — one downtime, one epoch.
    let occ_d = world.schedule(20_000, ScheduleRevision::INITIAL).await;
    world.schedule(25_000, ScheduleRevision::INITIAL).await;
    world.schedule(30_000, ScheduleRevision::INITIAL).await;
    world.clock.store(100_000, Ordering::SeqCst);
    let dispositions = scan_dispositions(&world).await;
    assert!(
        matches!(
            dispositions[..],
            [
                AgentWakeDisposition::AdmittedCoalesced { .. },
                AgentWakeDisposition::Skipped { .. },
                AgentWakeDisposition::Skipped { .. }
            ]
        ),
        "one representative, two absorbed, got {dispositions:?}"
    );
    let state = world.controller().await;
    assert_eq!(state.counters().missed, 2);
    lines.push(
        "ok  6/16 a downtime backlog of 3 missed occurrences admitted one coalesced \
         representative and absorbed 2 as missed"
            .to_string(),
    );
    settle_epoch(&world, &occ_d, AgentTaskStatus::Completed).await;

    // 7. Failure backoff: a failed epoch starts the streak; the delivery that
    // arrives during the backoff parks; and the controller's own durable
    // backoff re-wake — parked in the same wake index the scanner scans —
    // retries and promotes it. No resident timer anywhere.
    let occ_g = world.schedule(101_000, ScheduleRevision::INITIAL).await;
    world.clock.store(101_050, Ordering::SeqCst);
    let dispositions = scan_dispositions(&world).await;
    assert!(matches!(
        dispositions[..],
        [AgentWakeDisposition::Admitted { .. }]
    ));
    settle_epoch(&world, &occ_g, AgentTaskStatus::Failed).await;
    let occ_h = world.schedule(101_200, ScheduleRevision::INITIAL).await;
    world.clock.store(101_250, Ordering::SeqCst);
    let dispositions = scan_dispositions(&world).await;
    assert!(
        matches!(dispositions[..], [AgentWakeDisposition::BackedOff { .. }]),
        "the backoff gates the fresh delivery, got {dispositions:?}"
    );
    let state = world.controller().await;
    assert_eq!(state.lifecycle().consecutive_failures(), 1);
    let slot = state
        .lifecycle()
        .rewakes()
        .backoff
        .expect("the backoff re-wake is owed");
    assert!(slot.parked, "the settle pass parked it durably");
    world
        .clock
        .store(slot.due_at.as_millis() + 1, Ordering::SeqCst);
    let dispositions = scan_dispositions(&world).await;
    assert!(matches!(
        dispositions[..],
        [AgentWakeDisposition::Retried { .. }]
    ));
    let state = world.controller().await;
    assert_eq!(state.active()[0].binding().wake_id(), occ_h.wake_id());
    lines.push(
        "ok  7/16 the failed epoch backed off; the durable backoff re-wake retried and admitted \
         the occurrence parked behind it"
            .to_string(),
    );

    // 8. Escalation: the second consecutive failure crosses the policy's
    // threshold and suspends the goal durably, bumping the lifecycle
    // revision so a racing operator command is fenced.
    let resumable_revision = world.controller().await.lifecycle().lifecycle_revision();
    settle_epoch(&world, &occ_h, AgentTaskStatus::Failed).await;
    let state = world.controller().await;
    assert_eq!(
        state.lifecycle().status(),
        AgentGoalLifecycleStatus::Suspended
    );
    assert!(state
        .lifecycle()
        .suspended_reason()
        .is_some_and(|reason| reason.contains("escalated")));
    lines.push(
        "ok  8/16 a second consecutive failure escalated: the goal auto-suspended durably"
            .to_string(),
    );

    // 9. Resume, fenced then real: the pre-suspension revision loses; the
    // current revision reactivates and clears the streak and the backoff.
    let stale = world
        .apply_root_command(AgentTaskEntityCommand::ResumeContinuousGoal {
            operation_id: lifecycle_operation(AgentOperationKind::LifecycleResume, "resume-stale"),
            expected_lifecycle_revision: resumable_revision,
            provenance: Box::new(provenance(9)),
        })
        .await
        .expect_err("the stale resume is fenced");
    assert_eq!(stale.code(), "wake-stale-lifecycle-revision");
    let suspended_revision = world.controller().await.lifecycle().lifecycle_revision();
    world
        .apply_root_command(AgentTaskEntityCommand::ResumeContinuousGoal {
            operation_id: lifecycle_operation(AgentOperationKind::LifecycleResume, "resume-1"),
            expected_lifecycle_revision: suspended_revision,
            provenance: Box::new(provenance(10)),
        })
        .await
        .expect("the resume applies");
    let state = world.controller().await;
    assert_eq!(state.lifecycle().status(), AgentGoalLifecycleStatus::Active);
    assert_eq!(state.lifecycle().consecutive_failures(), 0);
    assert!(state.lifecycle().backoff_until().is_none());
    lines.push(
        "ok  9/16 the stale-revision resume was fenced with wake-stale-lifecycle-revision; the \
         current-revision resume reactivated the goal and cleared the backoff"
            .to_string(),
    );

    // 10. The schedule update: revision 2 takes the windowed policy — goal
    // window, expiry, renewal, retirement — into force, and a still-parked
    // revision-1 occurrence is fenced terminally when delivered.
    world.schedule(102_000, ScheduleRevision::INITIAL).await;
    world
        .apply_root_command(AgentTaskEntityCommand::UpdateContinuousSchedule {
            operation_id: lifecycle_operation(AgentOperationKind::SettingsUpdate, "revision-2"),
            schedule_revision: ScheduleRevision::new(2),
            wake_policy: Some(Box::new(
                AgentWakePolicyRevision::initial(crate::wiring::initial_policy(), provenance(1))
                    .expect("the initial revision is valid")
                    .updated(windowed_policy(), provenance(11))
                    .expect("the updated revision is valid"),
            )),
        })
        .await
        .expect("the update applies");
    world.clock.store(102_500, Ordering::SeqCst);
    let dispositions = scan_dispositions(&world).await;
    assert!(
        matches!(dispositions[..], [AgentWakeDisposition::Fenced { .. }]),
        "the stale revision is fenced, got {dispositions:?}"
    );
    lines.push(
        "ok 10/16 the schedule update to revision 2 took the windowed policy into force and \
         fenced a stale revision-1 delivery terminally"
            .to_string(),
    );

    // 11. The goal window pays for exactly one epoch. The first revision-2
    // occurrence opens and exhausts it; the next defers — and, with nothing
    // else on the schedule, the controller parks its own window-turn
    // re-wake at the boundary so a quiet schedule cannot strand it.
    let occ_w1 = world.schedule(103_000, ScheduleRevision::new(2)).await;
    world.clock.store(103_050, Ordering::SeqCst);
    let dispositions = scan_dispositions(&world).await;
    assert!(matches!(
        dispositions[..],
        [AgentWakeDisposition::Admitted { .. }]
    ));
    settle_epoch(&world, &occ_w1, AgentTaskStatus::Completed).await;
    let occ_w2 = world.schedule(104_000, ScheduleRevision::new(2)).await;
    world.clock.store(104_050, Ordering::SeqCst);
    let dispositions = scan_dispositions(&world).await;
    assert!(
        matches!(dispositions[..], [AgentWakeDisposition::Deferred { .. }]),
        "the exhausted window defers, got {dispositions:?}"
    );
    let state = world.controller().await;
    let slot = state
        .lifecycle()
        .rewakes()
        .window_turn
        .expect("the window-turn re-wake is owed");
    assert!(slot.parked, "the settle pass parked it durably");
    lines.push(
        "ok 11/16 the exhausted goal window deferred the next occurrence and parked a durable \
         window-turn re-wake at the boundary"
            .to_string(),
    );

    // 12. The boundary crosses; the parked re-wake's scan delivers a
    // controller retry whose recorded transition refills the ledger and
    // promotes the deferred occurrence. The ledger itself never reset on
    // any of the rebuilds above — every step ran on a fresh entity.
    world
        .clock
        .store(slot.due_at.as_millis() + 1, Ordering::SeqCst);
    let dispositions = scan_dispositions(&world).await;
    assert!(matches!(
        dispositions[..],
        [AgentWakeDisposition::Retried { .. }]
    ));
    let state = world.controller().await;
    assert_eq!(state.active()[0].binding().wake_id(), occ_w2.wake_id());
    assert!(state.window().is_some(), "the refilled ledger is in force");
    lines.push(
        "ok 12/16 the window turned: the parked re-wake's scan promoted the deferred \
         occurrence; the ledger survived every rebuild"
            .to_string(),
    );
    settle_epoch(&world, &occ_w2, AgentTaskStatus::Completed).await;

    // 13. Renewal, and shard movement: a former owner recovered before the
    // renewal still holds the old durable revision; its write loses the
    // compare-and-set, and re-recovery converges on the renewed record.
    let mut former_owner = world.root_store();
    former_owner
        .recover(world.now())
        .await
        .expect("the former owner recovers");
    let lifecycle_revision = world.controller().await.lifecycle().lifecycle_revision();
    world
        .apply_root_command(AgentTaskEntityCommand::RenewContinuousGoal {
            operation_id: lifecycle_operation(AgentOperationKind::LifecycleCommand, "renew-1"),
            expected_lifecycle_revision: lifecycle_revision,
            new_expires_at: AgentTimestampMillis::new(50_000_000),
            provenance: Box::new(provenance(12)),
        })
        .await
        .expect("the renewal applies");
    let fenced = former_owner
        .apply(
            AgentTaskEntityCommand::SuspendContinuousGoal {
                operation_id: lifecycle_operation(
                    AgentOperationKind::LifecycleSuspend,
                    "stale-suspend",
                ),
                expected_lifecycle_revision: lifecycle_revision,
                reason: None,
                provenance: Box::new(provenance(13)),
            },
            &world.router,
            world.now(),
        )
        .await
        .expect_err("the stale owner's write is fenced");
    let stale_owner_code = fenced.code().to_string();
    assert_eq!(stale_owner_code, "revision-conflict");
    former_owner
        .recover(world.now())
        .await
        .expect("the former owner re-recovers");
    let state = world.controller().await;
    let renewed_expiry = state
        .lifecycle()
        .effective_expires_at(&windowed_policy().lifecycle)
        .expect("the renewal is in force")
        .as_millis();
    assert_eq!(renewed_expiry, 50_000_000);
    assert_eq!(state.lifecycle().status(), AgentGoalLifecycleStatus::Active);
    lines.push(
        "ok 13/16 the renewal extended expiry to 50000000; a stale former owner's write was \
         fenced with revision-conflict and re-recovery converged"
            .to_string(),
    );

    // 14. Per-epoch isolation: every epoch is its own derived finite task
    // and run, and its input carries only the occurrence it observes and
    // the authorized observation scope. Nothing epoch-local crosses epochs;
    // what persists between them is the root's durable controller alone.
    let (epoch_w2, _) = epoch_scopes(occ_w2.wake_id());
    let epoch_w2_state =
        load_agent_task_state(&world.tasks, &epoch_w2, &AgentSchemaPolicy::default())
            .await
            .expect("the epoch state loads")
            .expect("the epoch task exists");
    assert_ne!(epoch_a, epoch_w2, "each epoch is its own scope");
    let input_a = serde_json::to_value(&epoch_a_state.task().expect("created").input)
        .expect("the input serializes")
        .to_string();
    let input_w2 = serde_json::to_value(&epoch_w2_state.task().expect("created").input)
        .expect("the input serializes")
        .to_string();
    assert!(input_a.contains(occ_a.wake_id().as_str()));
    assert!(input_w2.contains(occ_w2.wake_id().as_str()));
    assert!(!input_w2.contains(occ_a.wake_id().as_str()));
    lines.push(
        "ok 14/16 each epoch is its own derived finite task and run with a bounded occurrence \
         input; continuity lives only in the root's durable controller"
            .to_string(),
    );

    // 15. Retirement: the ninth admitted occurrence reaches the policy's
    // bound; the transition that settles it observes the retirement, and a
    // later delivery is barred and marked terminal in the wake index.
    let occ_k = world
        .schedule(104_050 + 2 * WINDOW_MS, ScheduleRevision::new(2))
        .await;
    world.clock.store(104_100 + 2 * WINDOW_MS, Ordering::SeqCst);
    let dispositions = scan_dispositions(&world).await;
    assert!(
        matches!(dispositions[..], [AgentWakeDisposition::Admitted { .. }]),
        "the ninth occurrence admits, got {dispositions:?}"
    );
    settle_epoch(&world, &occ_k, AgentTaskStatus::Completed).await;
    let state = world.controller().await;
    assert_eq!(state.counters().admitted, 9);
    assert_eq!(
        state.lifecycle().status(),
        AgentGoalLifecycleStatus::Retired
    );
    world
        .schedule(104_200 + 2 * WINDOW_MS, ScheduleRevision::new(2))
        .await;
    world.clock.store(104_250 + 2 * WINDOW_MS, Ordering::SeqCst);
    let dispositions = scan_dispositions(&world).await;
    assert!(
        matches!(dispositions[..], [AgentWakeDisposition::Barred { .. }]),
        "the retired goal bars the delivery, got {dispositions:?}"
    );
    let rescan = scan(&world).await;
    assert!(rescan.is_empty(), "the barred entry never rescans");
    lines.push(
        "ok 15/16 the goal retired after its 9th admitted occurrence; a later delivery was \
         barred and its entry marked terminal"
            .to_string(),
    );

    // 16. The authoritative operational query: one durable read answers the
    // milestone's reference facts, with no entity resident and no telemetry
    // wired anywhere near it. "Next wake" joins purely from the wake index.
    let snapshot = agent_task_operational_snapshot(
        &world.tasks,
        &task_scope(),
        &AgentSchemaPolicy::default(),
        world.now(),
    )
    .await
    .expect("the point query answers")
    .expect("the root exists");
    let wake_view = snapshot
        .task
        .as_ref()
        .expect("the root is created")
        .wake
        .as_ref()
        .expect("the goal has a wake view")
        .clone();
    assert_eq!(wake_view.schedule_revision, ScheduleRevision::new(2));
    let lifecycle = wake_view.lifecycle.as_ref().expect("the lifecycle rides");
    assert_eq!(lifecycle.status(), AgentGoalLifecycleStatus::Retired);
    let mut timers = AgentWakeTimerStore::new(world.wakes.clone());
    let timer_state = timers
        .recover(world.now())
        .await
        .expect("the wake index recovers")
        .clone();
    let next_wake = next_pending_wake_for_task(&timer_state, &tenant(), task_scope().task());
    assert!(next_wake.is_none(), "nothing is pending after retirement");
    lines.push(format!(
        "ok 16/16 the operational query answered from durable state alone: schedule revision \
         {}, lifecycle {}, {} admitted, {} missed, {} coalesced, {} fenced, no pending wake",
        wake_view.schedule_revision.get(),
        lifecycle.status().as_label(),
        wake_view.counters.admitted,
        wake_view.counters.missed,
        wake_view.counters.coalesced,
        wake_view.counters.fenced,
    ));

    // The typed facts behind the transcript.
    let root_state =
        load_agent_task_state(&world.tasks, &task_scope(), &AgentSchemaPolicy::default())
            .await
            .expect("the root state loads")
            .expect("the root exists");
    let escrow_outstanding = root_state
        .task()
        .expect("the root is created")
        .escrow
        .outstanding()
        .count();
    let mut epoch_tasks = 0;
    for wake in [
        occ_a.wake_id(),
        occ_b.wake_id(),
        occ_c.wake_id(),
        occ_d.wake_id(),
        occ_g.wake_id(),
        occ_h.wake_id(),
        occ_w1.wake_id(),
        occ_w2.wake_id(),
        occ_k.wake_id(),
    ] {
        let (scope, _) = epoch_scopes(wake);
        if load_agent_task_state(&world.tasks, &scope, &AgentSchemaPolicy::default())
            .await
            .expect("the epoch state loads")
            .is_some()
        {
            epoch_tasks += 1;
        }
    }

    AcceptanceReport {
        lines,
        epoch_tasks,
        admitted: wake_view.counters.admitted,
        coalesced: wake_view.counters.coalesced,
        missed: wake_view.counters.missed,
        fenced: wake_view.counters.fenced,
        deferred: wake_view.counters.deferred,
        backed_off: wake_view.counters.backed_off,
        retried: wake_view.counters.retried,
        barred: wake_view.counters.barred,
        stale_owner_code,
        renewed_expiry,
        escrow_outstanding,
        pending_wake: next_wake.is_some(),
    }
}
