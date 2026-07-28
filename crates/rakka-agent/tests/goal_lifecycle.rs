//! Suspension, renewal, backoff, expiry, and retirement — executed, fenced,
//! and durably retried.
//!
//! Specification: section 8.2's lifecycle clauses ("suspension, renewal,
//! expiry, and retirement"; "failure backoff and escalation") and the
//! controller-originated durable re-wake of the slice 3.4 plan: one
//! mechanism, two consumers — the failure-backoff retry and the window-turn
//! re-attempt of a deferred occurrence — parked by the entity's settle pass
//! into the same durable wake-timer store the shared scanner scans, so a
//! quiet schedule cannot strand a parked occurrence. Every entity here is
//! rebuilt from durable state per call; every retry arrives through the real
//! scanner.

use std::sync::atomic::Ordering;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    epoch_result_operation_id, epoch_task_id_for_wake, load_agent_task_state,
    wake_admission_command, AgentBudgetConsumption, AgentEntityAddress, AgentEpochResult,
    AgentExchangeEnvelope, AgentExchangeKind, AgentExchangePayload, AgentGoalLifecycleStatus,
    AgentOperationId, AgentOperationKind, AgentSchemaPolicy, AgentTaskEntityCommand,
    AgentTaskEntityReply, AgentTaskScope, AgentTaskStatus, AgentWakeBackoffPolicy,
    AgentWakeControllerState, AgentWakeDisposition, AgentWakeOccurrence, AgentWakeRewakeCause,
    AgentWakeTimerStatus, ScheduleRevision, AGENT_EPOCH_RESULT_PAYLOAD_TYPE,
};
use rakka_agent_workflow::{AgentCorrelationId, AgentTimestampMillis};

mod common;

use common::{
    continuous_goal_mode, epoch_scopes_for, provenance, scheduled_wake_binding, task_scope,
    wake_policy, Fixture, TASK, TENANT,
};

fn fixture() -> Fixture {
    Fixture::new(ScriptedDispatcher::new())
}

async fn controller(fx: &Fixture) -> AgentWakeControllerState {
    let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the root state loads")
        .expect("the root exists");
    state
        .task()
        .expect("the root is created")
        .wake_controller
        .clone()
        .expect("the controller exists")
}

/// A legitimate epoch-result envelope for one admitted wake.
fn epoch_result(
    binding: &rakka_agent::AgentWakeBinding,
    status: AgentTaskStatus,
) -> AgentExchangeEnvelope {
    let epoch_task = epoch_task_id_for_wake(binding.wake_id()).expect("the epoch derives");
    let epoch_scope =
        AgentTaskScope::new(common::tenant(), epoch_task.clone()).expect("the scope is valid");
    let operation_id =
        epoch_result_operation_id(&common::tenant(), &common::goal_id(), binding.wake_id())
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
        AgentExchangePayload::encode(AGENT_EPOCH_RESULT_PAYLOAD_TYPE, &result)
            .expect("the payload encodes"),
        AgentCorrelationId::new(operation_id.as_str()),
        AgentTimestampMillis::new(9_000),
    )
    .expect("the envelope builds")
}

async fn accept_on_root(fx: &Fixture, envelope: &AgentExchangeEnvelope) {
    let mut root = rakka_agent::AgentTaskEntityStore::new(
        task_scope(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    )
    .with_wake_timers(fx.rewake_parker.clone());
    root.recover(fx.now()).await.expect("the root recovers");
    let reply = root
        .accept(envelope, &fx.router, fx.now())
        .await
        .expect("the result is answered");
    assert!(reply.result().is_accepted(), "the epoch result lands");
    // The settle pass that parks what the acceptance owed.
    fx.settle_task_at(&task_scope())
        .await
        .expect("the root settles");
}

#[tokio::test]
async fn lifecycle_commands_govern_admission_end_to_end() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;
    let revision = controller(&fx).await.lifecycle().lifecycle_revision();

    // Suspend, with provenance and a reason.
    let reply = fx
        .apply_task_command(AgentTaskEntityCommand::SuspendContinuousGoal {
            operation_id: AgentOperationId::new(
                AgentOperationKind::LifecycleSuspend,
                [TENANT, TASK, "suspend-1"],
            )
            .expect("the operation id derives"),
            expected_lifecycle_revision: revision,
            reason: Some("maintenance window".to_string()),
            provenance: Box::new(provenance(10)),
        })
        .await
        .expect("the suspend applies");
    assert!(matches!(reply, AgentTaskEntityReply::Applied { .. }));

    // A delivery while suspended parks; the scanner consumes the entry.
    fx.schedule_wake(5, ScheduleRevision::INITIAL).await;
    fx.clock.fetch_add(1_000, Ordering::SeqCst);
    let scan = fx.wake_scanner().scan_due().await.expect("the pass runs");
    assert!(matches!(
        &scan.outcomes[0],
        rakka_agent::AgentWakeScanOutcome::Dispositioned {
            disposition: AgentWakeDisposition::SuspendedParked { .. },
            ..
        }
    ));
    let state = controller(&fx).await;
    assert_eq!(
        state.lifecycle().status(),
        AgentGoalLifecycleStatus::Suspended
    );
    assert_eq!(state.pending().len(), 1);
    let suspended_revision = state.lifecycle().lifecycle_revision();

    // A stale resume — carrying the pre-suspension revision — is fenced.
    let stale = fx
        .apply_task_command(AgentTaskEntityCommand::ResumeContinuousGoal {
            operation_id: AgentOperationId::new(
                AgentOperationKind::LifecycleResume,
                [TENANT, TASK, "resume-stale"],
            )
            .expect("the operation id derives"),
            expected_lifecycle_revision: revision,
            provenance: Box::new(provenance(11)),
        })
        .await
        .expect_err("the stale resume is fenced");
    assert_eq!(stale.code(), "wake-stale-lifecycle-revision");

    // The correct resume promotes what the suspension parked and owes its
    // epoch in the same transition.
    let reply = fx
        .apply_task_command(AgentTaskEntityCommand::ResumeContinuousGoal {
            operation_id: AgentOperationId::new(
                AgentOperationKind::LifecycleResume,
                [TENANT, TASK, "resume-1"],
            )
            .expect("the operation id derives"),
            expected_lifecycle_revision: suspended_revision,
            provenance: Box::new(provenance(12)),
        })
        .await
        .expect("the resume applies");
    assert!(matches!(reply, AgentTaskEntityReply::Applied { .. }));
    let state = controller(&fx).await;
    assert_eq!(state.lifecycle().status(), AgentGoalLifecycleStatus::Active);
    assert_eq!(
        state.counters().admitted,
        1,
        "resume promoted the parked wake"
    );
    let promoted = state.active()[0].binding().wake_id().clone();
    let (epoch_scope, _run) = epoch_scopes_for(&promoted);
    assert!(
        load_agent_task_state(&fx.tasks, &epoch_scope, &AgentSchemaPolicy::default())
            .await
            .expect("the epoch state loads")
            .is_some(),
        "the resume owed the promoted wake's epoch"
    );

    // Retirement bars every further delivery, and the scanner marks the
    // barred entry terminal.
    let reply = fx
        .apply_task_command(AgentTaskEntityCommand::RetireContinuousGoal {
            operation_id: AgentOperationId::new(
                AgentOperationKind::LifecycleTerminate,
                [TENANT, TASK, "retire-1"],
            )
            .expect("the operation id derives"),
            expected_lifecycle_revision: state.lifecycle().lifecycle_revision(),
            provenance: Box::new(provenance(13)),
        })
        .await
        .expect("the retire applies");
    assert!(matches!(reply, AgentTaskEntityReply::Applied { .. }));
    fx.schedule_wake(2_000_000, ScheduleRevision::INITIAL).await;
    fx.clock.store(2_000_100, Ordering::SeqCst);
    let scan = fx.wake_scanner().scan_due().await.expect("the pass runs");
    assert!(matches!(
        &scan.outcomes[0],
        rakka_agent::AgentWakeScanOutcome::Dispositioned {
            disposition: AgentWakeDisposition::Barred { .. },
            ..
        }
    ));
    let scan = fx.wake_scanner().scan_due().await.expect("the rescan runs");
    assert!(scan.outcomes.is_empty(), "a barred entry never rescans");
}

#[tokio::test]
async fn a_failed_epoch_backs_off_and_the_parked_rewake_retries_it() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;

    // One occurrence executes; another arrives and coalesces behind it.
    let first = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(first.clone()).expect("the command derives"))
        .await
        .expect("the first admission applies");
    let second = scheduled_wake_binding(10, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(second.clone()).expect("the command derives"))
        .await
        .expect("the second delivery coalesces");

    // The epoch fails. The failure is accounted before the release, the
    // fresh backoff gates the promotion, and the settle pass parks the
    // backoff re-wake durably.
    accept_on_root(&fx, &epoch_result(&first, AgentTaskStatus::Failed)).await;
    let state = controller(&fx).await;
    assert_eq!(state.lifecycle().consecutive_failures(), 1);
    let until = state
        .lifecycle()
        .backoff_until()
        .expect("the backoff is in force");
    assert!(state.active().is_empty(), "the failed wake released");
    assert_eq!(state.pending().len(), 1, "the backoff held the promotion");
    let slot = state
        .lifecycle()
        .rewakes()
        .backoff
        .expect("the backoff re-wake is owed");
    assert_eq!(slot.due_at, until);
    assert!(slot.parked, "the settle pass parked it durably");

    // The parked retry is a real timer entry the shared scanner delivers.
    fx.clock.store(until.as_millis() + 1, Ordering::SeqCst);
    let scan = fx
        .wake_scanner()
        .scan_due()
        .await
        .expect("the retry pass runs");
    assert_eq!(scan.outcomes.len(), 1);
    let rakka_agent::AgentWakeScanOutcome::Dispositioned {
        disposition,
        marked,
        ..
    } = &scan.outcomes[0]
    else {
        panic!("the retry dispositions, got {:?}", scan.outcomes[0]);
    };
    assert!(matches!(disposition, AgentWakeDisposition::Retried { .. }));
    assert_eq!(*marked, AgentWakeTimerStatus::Fired);

    // The retry's own transition promoted the parked occurrence and owed its
    // epoch.
    let state = controller(&fx).await;
    assert_eq!(state.counters().retried, 1);
    assert_eq!(state.counters().admitted, 2);
    assert!(state.pending().is_empty());
    assert_eq!(state.active()[0].binding().wake_id(), second.wake_id());
    let (epoch_scope, _run) = epoch_scopes_for(second.wake_id());
    assert!(
        load_agent_task_state(&fx.tasks, &epoch_scope, &AgentSchemaPolicy::default())
            .await
            .expect("the epoch state loads")
            .is_some(),
        "the retried promotion owed the epoch"
    );

    // A completion resets the streak.
    accept_on_root(&fx, &epoch_result(&second, AgentTaskStatus::Completed)).await;
    let state = controller(&fx).await;
    assert_eq!(state.lifecycle().consecutive_failures(), 0);
    assert!(state.lifecycle().backoff_until().is_none());
}

#[tokio::test]
async fn an_early_delivered_retry_re_arms_and_the_next_scan_promotes() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;
    let first = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(first.clone()).expect("the command derives"))
        .await
        .expect("the first admission applies");
    let second = scheduled_wake_binding(10, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(second.clone()).expect("the command derives"))
        .await
        .expect("the second delivery coalesces");
    accept_on_root(&fx, &epoch_result(&first, AgentTaskStatus::Failed)).await;
    let state = controller(&fx).await;
    let until = state
        .lifecycle()
        .backoff_until()
        .expect("the backoff is in force");
    let parked = state
        .lifecycle()
        .rewakes()
        .backoff
        .expect("the backoff re-wake is owed");
    assert!(parked.parked, "the settle pass parked it durably");

    // A scanner host whose clock runs ahead delivers the parked retry while
    // the backoff is still in force on this host: the delivery is applied at
    // the entity's own (earlier) logical time and the fast scanner marks the
    // entry fired. Without re-arming, the slot would sit marked parked with
    // its only timer entry terminal — the parked occurrence stranded.
    let early_binding = {
        let store = fx
            .wake_scanner()
            .timers_mut()
            .recover(AgentTimestampMillis::new(0))
            .await
            .expect("the store recovers")
            .clone();
        store
            .entries()
            .values()
            .find(|entry| {
                entry.status() == AgentWakeTimerStatus::Pending
                    && matches!(
                        entry.binding().occurrence(),
                        AgentWakeOccurrence::Retry { .. }
                    )
            })
            .expect("the parked retry entry exists")
            .binding()
            .clone()
    };
    let early_wake = early_binding.wake_id().clone();
    assert!(
        fx.now().as_millis() < until.as_millis(),
        "the delivery is early by this host's clock"
    );
    fx.apply_task_command(wake_admission_command(early_binding).expect("the command derives"))
        .await
        .expect("the early retry is consumed");
    fx.wake_scanner()
        .timers_mut()
        .mark_fired(&early_wake, fx.now())
        .await
        .expect("the fast scanner marks the delivered entry");

    // Nothing promoted, and the consume re-armed the slot: attempt bumped,
    // the same transition's settle pass parked a fresh entry under a wake
    // the fired one cannot absorb.
    let state = controller(&fx).await;
    assert_eq!(state.counters().retried, 1);
    assert_eq!(state.counters().admitted, 1, "the backoff still gates");
    assert_eq!(state.pending().len(), 1, "the occurrence still waits");
    let slot = state
        .lifecycle()
        .rewakes()
        .backoff
        .expect("the slot survives the early consume");
    assert_eq!(slot.due_at, until);
    assert_eq!(slot.attempt, 1, "the consume bumped the generation");
    assert!(slot.parked, "the settle pass re-parked the next attempt");
    let pending: Vec<_> = fx
        .wake_scanner()
        .timers_mut()
        .recover(AgentTimestampMillis::new(0))
        .await
        .expect("the store recovers")
        .entries()
        .values()
        .filter(|entry| entry.status() == AgentWakeTimerStatus::Pending)
        .map(|entry| entry.wake_id().clone())
        .collect();
    assert_eq!(pending.len(), 1, "exactly one live retry entry");
    assert_ne!(pending[0], early_wake, "under a fresh wake identity");

    // Once this host's clock passes the due time, the ordinary scan delivers
    // the re-armed retry and its transition promotes the parked occurrence.
    fx.clock.store(until.as_millis() + 1, Ordering::SeqCst);
    let scan = fx
        .wake_scanner()
        .scan_due()
        .await
        .expect("the retry pass runs");
    assert_eq!(scan.outcomes.len(), 1);
    let state = controller(&fx).await;
    assert_eq!(state.counters().retried, 2);
    assert_eq!(state.counters().admitted, 2, "the promotion landed");
    assert!(state.pending().is_empty());
    assert_eq!(state.active()[0].binding().wake_id(), second.wake_id());
    assert!(
        state.lifecycle().rewakes().backoff.is_none(),
        "the consumed slot cleared"
    );
}

#[tokio::test]
async fn escalation_suspends_and_the_resume_clears_the_backoff() {
    let policy = wake_policy()
        .with_failure_backoff(AgentWakeBackoffPolicy {
            escalate_after_failures: Some(1),
            ..AgentWakeBackoffPolicy::DEFAULT
        })
        .expect("the backoff policy is valid");
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(policy))
        .await;

    let first = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(first.clone()).expect("the command derives"))
        .await
        .expect("the admission applies");
    accept_on_root(&fx, &epoch_result(&first, AgentTaskStatus::Failed)).await;

    let state = controller(&fx).await;
    assert_eq!(
        state.lifecycle().status(),
        AgentGoalLifecycleStatus::Suspended,
        "one failure over the threshold escalates into suspension"
    );
    assert!(state
        .lifecycle()
        .suspended_reason()
        .is_some_and(|reason| reason.contains("escalated")));

    // The operator's resume clears the streak and the backoff: try again.
    let reply = fx
        .apply_task_command(AgentTaskEntityCommand::ResumeContinuousGoal {
            operation_id: AgentOperationId::new(
                AgentOperationKind::LifecycleResume,
                [TENANT, TASK, "resume-esc"],
            )
            .expect("the operation id derives"),
            expected_lifecycle_revision: state.lifecycle().lifecycle_revision(),
            provenance: Box::new(provenance(20)),
        })
        .await
        .expect("the resume applies");
    assert!(matches!(reply, AgentTaskEntityReply::Applied { .. }));
    let state = controller(&fx).await;
    assert_eq!(state.lifecycle().status(), AgentGoalLifecycleStatus::Active);
    assert_eq!(state.lifecycle().consecutive_failures(), 0);
    assert!(state.lifecycle().backoff_until().is_none());
}

#[tokio::test]
async fn a_quiet_schedule_gets_a_window_turn_rewake() {
    const WINDOW: u64 = 3_600_000;
    let mut ceiling = rakka_agent::AgentBudgetAllocation::unbounded();
    ceiling.set(rakka_agent::AgentBudgetDimension::ModelCalls, Some(8));
    let policy = wake_policy()
        .with_goal_window(rakka_agent::AgentGoalWindowCeiling {
            window: rakka_agent::AgentBudgetWindow::Rolling {
                length_millis: WINDOW,
            },
            ceiling,
        })
        .expect("the windowed policy is valid");
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(policy))
        .await;

    // The window pays for one epoch; the second occurrence defers, and —
    // with nothing else on the schedule — the settle pass parks the
    // window-turn re-wake.
    let first = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(first.clone()).expect("the command derives"))
        .await
        .expect("the first admission applies");
    let second = scheduled_wake_binding(10, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(second.clone()).expect("the command derives"))
        .await
        .expect("the second delivery coalesces");
    accept_on_root(&fx, &epoch_result(&first, AgentTaskStatus::Completed)).await;

    let state = controller(&fx).await;
    assert_eq!(state.pending().len(), 1, "the deferred occurrence waits");
    let slot = state
        .lifecycle()
        .rewakes()
        .window_turn
        .expect("the window-turn re-wake is owed");
    assert!(slot.parked, "the settle pass parked it durably");

    // The window turns; the scanner delivers the retry; the deferred
    // occurrence takes the fresh window — no external delivery required.
    fx.clock
        .store(slot.due_at.as_millis() + 1, Ordering::SeqCst);
    let scan = fx
        .wake_scanner()
        .scan_due()
        .await
        .expect("the retry pass runs");
    assert!(matches!(
        &scan.outcomes[0],
        rakka_agent::AgentWakeScanOutcome::Dispositioned {
            disposition: AgentWakeDisposition::Retried { .. },
            ..
        }
    ));
    let state = controller(&fx).await;
    assert_eq!(state.counters().admitted, 2);
    assert_eq!(state.active()[0].binding().wake_id(), second.wake_id());
    assert!(
        state.lifecycle().rewakes().window_turn.is_none(),
        "the consumed slot cleared"
    );
}

#[tokio::test]
async fn a_crash_between_parking_and_marking_converges() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;
    let first = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(first.clone()).expect("the command derives"))
        .await
        .expect("the first admission applies");
    let second = scheduled_wake_binding(10, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(second.clone()).expect("the command derives"))
        .await
        .expect("the second delivery coalesces");

    // Kill the owner on the wake-store park write: the failure is accounted
    // and the re-wake owed, but nothing parked.
    fx.wakes
        .crash_at(1, rakka_agent::testkit::CrashPoint::BeforeWrite);
    let mut root = rakka_agent::AgentTaskEntityStore::new(
        task_scope(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    )
    .with_wake_timers(fx.rewake_parker.clone());
    root.recover(fx.now()).await.expect("the root recovers");
    let _ = root
        .accept(
            &epoch_result(&first, AgentTaskStatus::Failed),
            &fx.router,
            fx.now(),
        )
        .await;
    fx.wakes
        .assert_crash_fired(1, rakka_agent::testkit::CrashPoint::BeforeWrite);
    fx.wakes.survive();
    let state = controller(&fx).await;
    let slot = state
        .lifecycle()
        .rewakes()
        .backoff
        .expect("the re-wake stayed owed");
    assert!(!slot.parked, "the crash left it unparked");

    // The next settle pass — any node's — re-parks and marks.
    fx.settle_task_at(&task_scope())
        .await
        .expect("the recovery settle parks");
    let state = controller(&fx).await;
    assert!(
        state
            .lifecycle()
            .rewakes()
            .backoff
            .expect("the slot survives")
            .parked
    );
    // Exactly one durable entry exists for it.
    let entries = fx
        .wake_scanner()
        .timers_mut()
        .recover(AgentTimestampMillis::new(0))
        .await
        .expect("the store recovers")
        .entries()
        .values()
        .filter(|entry| {
            matches!(
                entry.binding().occurrence(),
                AgentWakeOccurrence::Retry {
                    cause: AgentWakeRewakeCause::Backoff,
                    ..
                }
            )
        })
        .count();
    assert_eq!(entries, 1);
}

#[tokio::test]
async fn a_crash_after_the_park_write_converges_on_the_durable_entry() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;
    let first = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(first.clone()).expect("the command derives"))
        .await
        .expect("the first admission applies");
    let second = scheduled_wake_binding(10, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(second.clone()).expect("the command derives"))
        .await
        .expect("the second delivery coalesces");

    // Kill the owner right after the park write commits: the timer entry
    // exists durably, but the mark transition never ran, so the slot is
    // owed-unparked while the store already holds the entry. The next settle
    // pass rebuilds the binding at a later accepted time — the store must
    // answer that re-park as a duplicate of the durable entry, never as a
    // disagreement.
    fx.wakes
        .crash_at(1, rakka_agent::testkit::CrashPoint::AfterWrite);
    let mut root = rakka_agent::AgentTaskEntityStore::new(
        task_scope(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    )
    .with_wake_timers(fx.rewake_parker.clone());
    root.recover(fx.now()).await.expect("the root recovers");
    let _ = root
        .accept(
            &epoch_result(&first, AgentTaskStatus::Failed),
            &fx.router,
            fx.now(),
        )
        .await;
    fx.wakes
        .assert_crash_fired(1, rakka_agent::testkit::CrashPoint::AfterWrite);
    fx.wakes.survive();
    let state = controller(&fx).await;
    let slot = state
        .lifecycle()
        .rewakes()
        .backoff
        .expect("the re-wake stayed owed");
    assert!(!slot.parked, "the crash left the mark uncommitted");

    // The next settle pass — any node's — re-parks onto the existing entry
    // and marks.
    fx.settle_task_at(&task_scope())
        .await
        .expect("the recovery settle converges on the parked entry");
    let state = controller(&fx).await;
    assert!(
        state
            .lifecycle()
            .rewakes()
            .backoff
            .expect("the slot survives")
            .parked
    );
    // Exactly one durable entry exists for it.
    let entries = fx
        .wake_scanner()
        .timers_mut()
        .recover(AgentTimestampMillis::new(0))
        .await
        .expect("the store recovers")
        .entries()
        .values()
        .filter(|entry| {
            matches!(
                entry.binding().occurrence(),
                AgentWakeOccurrence::Retry {
                    cause: AgentWakeRewakeCause::Backoff,
                    ..
                }
            )
        })
        .count();
    assert_eq!(entries, 1);
}

/// Every wake-audit entry of the root task, as `(kind, detail)` in sequence
/// order.
async fn history_entries(fx: &Fixture) -> Vec<(rakka_agent::AgentTaskHistoryKind, String)> {
    let mut entries = Vec::new();
    let mut cursor = Some(rakka_agent::AgentTaskHistoryCursor::start());
    while let Some(position) = cursor {
        let page = rakka_agent::AgentTaskHistoryStore::read(&fx.history, &task_scope(), position)
            .await
            .expect("the history reads");
        entries.extend(
            page.entries
                .iter()
                .map(|entry| (entry.kind, entry.detail.clone())),
        );
        cursor = page.next;
    }
    entries
}

#[tokio::test]
async fn the_audit_trail_is_history_recorded_once_per_transition() {
    use rakka_agent::AgentTaskHistoryKind as Kind;

    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;

    // Admission: one disposition row and one admitted-epoch row, the wake id
    // in the bounded detail of both.
    let binding = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    let admission = wake_admission_command(binding.clone()).expect("the command derives");
    fx.apply_task_command(admission.clone())
        .await
        .expect("the admission applies");
    let entries = history_entries(&fx).await;
    let dispositioned: Vec<&String> = entries
        .iter()
        .filter(|(kind, _)| *kind == Kind::WakeDispositioned)
        .map(|(_, detail)| detail)
        .collect();
    assert_eq!(dispositioned.len(), 1);
    assert_eq!(
        dispositioned[0],
        &format!("admitted {}", binding.wake_id()),
        "the disposition and the wake id are the audit detail"
    );
    assert_eq!(
        entries
            .iter()
            .filter(|(kind, _)| *kind == Kind::EpochAdmitted)
            .count(),
        1
    );

    // A replayed admission answers from the dedup record: no new rows.
    fx.apply_task_command(admission)
        .await
        .expect("the replay answers");
    assert_eq!(
        entries,
        history_entries(&fx).await,
        "a replay records nothing"
    );

    // The failed settlement records its class; suspend, resume, schedule
    // update, and retirement each record their own row.
    accept_on_root(&fx, &epoch_result(&binding, AgentTaskStatus::Failed)).await;
    let revision = controller(&fx).await.lifecycle().lifecycle_revision();
    fx.apply_task_command(AgentTaskEntityCommand::SuspendContinuousGoal {
        operation_id: AgentOperationId::new(
            AgentOperationKind::LifecycleSuspend,
            [TENANT, TASK, "suspend-audit"],
        )
        .expect("the operation id derives"),
        expected_lifecycle_revision: revision,
        reason: Some("audit drill".to_string()),
        provenance: Box::new(provenance(30)),
    })
    .await
    .expect("the suspend applies");
    let revision = controller(&fx).await.lifecycle().lifecycle_revision();
    fx.apply_task_command(AgentTaskEntityCommand::ResumeContinuousGoal {
        operation_id: AgentOperationId::new(
            AgentOperationKind::LifecycleResume,
            [TENANT, TASK, "resume-audit"],
        )
        .expect("the operation id derives"),
        expected_lifecycle_revision: revision,
        provenance: Box::new(provenance(31)),
    })
    .await
    .expect("the resume applies");
    fx.apply_task_command(AgentTaskEntityCommand::UpdateContinuousSchedule {
        operation_id: AgentOperationId::new(
            AgentOperationKind::SettingsUpdate,
            [TENANT, TASK, "schedule-audit"],
        )
        .expect("the operation id derives"),
        schedule_revision: ScheduleRevision::new(2),
        wake_policy: None,
    })
    .await
    .expect("the update applies");
    let revision = controller(&fx).await.lifecycle().lifecycle_revision();
    fx.apply_task_command(AgentTaskEntityCommand::RetireContinuousGoal {
        operation_id: AgentOperationId::new(
            AgentOperationKind::LifecycleTerminate,
            [TENANT, TASK, "retire-audit"],
        )
        .expect("the operation id derives"),
        expected_lifecycle_revision: revision,
        provenance: Box::new(provenance(32)),
    })
    .await
    .expect("the retire applies");

    let entries = history_entries(&fx).await;
    let count = |kind: Kind| entries.iter().filter(|(k, _)| *k == kind).count();
    assert_eq!(count(Kind::EpochSettled), 1);
    assert!(
        entries
            .iter()
            .any(|(kind, detail)| *kind == Kind::EpochSettled
                && detail == &format!("failed {}", binding.wake_id())),
        "the settlement's class and wake id are the audit detail"
    );
    assert_eq!(count(Kind::GoalSuspended), 1);
    assert!(
        entries
            .iter()
            .any(|(kind, detail)| *kind == Kind::GoalSuspended && detail == "audit drill"),
        "the operator's reason is the suspension detail"
    );
    assert_eq!(count(Kind::GoalResumed), 1);
    assert_eq!(count(Kind::ScheduleUpdated), 1);
    assert!(
        entries
            .iter()
            .any(|(kind, detail)| *kind == Kind::ScheduleUpdated
                && detail.starts_with("schedule-revision 2 ")),
        "the revisions and fence count are the update detail"
    );
    assert_eq!(count(Kind::GoalRetired), 1);
    assert_eq!(count(Kind::GoalExpired), 0, "nothing expired in this flow");
}
