//! Graph cancellation settles the outbox entries of the nodes it cancels
//! (issue #71 item 4).
//!
//! A cancelled node's queued effect stayed due in the durable outbox, and
//! `dispatch_due_outbox` sends whatever is due: a cancelled run's effects
//! still reached the dispatcher. These proofs hold the settle that closes it,
//! under the fleet's rule: an entry mid-dispatch is cancelled with its
//! attempt counted, a pending or retrying one is cancelled, and a finished
//! or absent one is left alone.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rakka_agent_workflow::{
    AgentCausationId, AgentCommand, AgentCommandId, AgentCommandMetadata,
    AgentCompiledExecutionPlan, AgentCompiledNodeId, AgentCompiledNodeKind,
    AgentCompiledNodeTarget, AgentCompiledPlanEdge, AgentCompiledPlanFingerprint,
    AgentCompiledPlanId, AgentCompiledPlanNode, AgentCompiledPlanPort, AgentCompiledPortDirection,
    AgentCorrelationId, AgentDeduplicationKey, AgentDurabilityMetadata, AgentEffectId,
    AgentEffectTarget, AgentGraphEffectBridge, AgentGraphEffectFailureDisposition,
    AgentGraphEffectScheduleRequest, AgentGraphHumanCheckpointScheduleRequest,
    AgentGraphNodeStatus, AgentGraphReconciliationDecision, AgentGraphRunState,
    AgentGraphScheduler, AgentGraphSchedulerTransition, AgentGraphTerminalStatus,
    AgentOutboxSettlement, AgentRunActor, AgentRunActorCommand, AgentRunCore, AgentRunEngineError,
    AgentRunId, AgentRunInbox, AgentRunRuntimeError, AgentRunState, AgentRunStatus,
    AgentRunTransitionKind, AgentSettledOutboxEntry, AgentStatePayload, AgentStep, AgentStepId,
    AgentStepKind, AgentStepRunner, AgentTenantId, AgentTimestampMillis, AgentWorkflow,
    AgentWorkflowId, HumanCheckpoint, HumanCheckpointId, HumanCheckpointStatus,
    HumanDecisionOption, StateSchemaVersion, WorkflowDefinitionVersion,
    CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION, DISPATCH_CANCELLED_IN_FLIGHT_LINE,
};
use rakka_core::{ActorRef, ActorSystem};
use rakka_persistence::{
    DurableError, DurableState, DurableStateStore, InMemoryDurableStateStore, PersistenceId,
    Revision, StateRecord, StoreFuture,
};
use rakka_workflow::{
    ManualWorkflowClock, OutboxDispatchFuture, OutboxDispatchResult, OutboxDispatcher, OutboxEntry,
    OutboxMessageId, OutboxStatus, WorkflowState, WorkflowTimestamp,
};

type FaultyWorkflowStore = FaultyStore<WorkflowState>;
type FaultyRunStore = FaultyStore<AgentRunState>;
type TestInbox = AgentRunInbox<FaultyWorkflowStore, ManualWorkflowClock>;
type TestCore = AgentRunCore<FaultyRunStore, FaultyWorkflowStore, ManualWorkflowClock>;

/// A store whose `n`th compare-and-set from arming fails once, as a store
/// error. Unarmed, it is the in-memory store.
#[derive(Clone)]
struct FaultyStore<S: DurableState> {
    inner: InMemoryDurableStateStore<S>,
    /// The compare-and-set count, from arming, that fails; 0 is unarmed.
    fail_at: Arc<AtomicUsize>,
    seen: Arc<AtomicUsize>,
}

impl<S: DurableState> FaultyStore<S> {
    fn new() -> Self {
        Self {
            inner: InMemoryDurableStateStore::new(),
            fail_at: Arc::new(AtomicUsize::new(0)),
            seen: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Fails the `nth` compare-and-set from now, once.
    fn fail_write(&self, nth: usize) {
        self.seen.store(0, Ordering::SeqCst);
        self.fail_at.store(nth, Ordering::SeqCst);
    }
}

impl<S: DurableState> DurableStateStore<S> for FaultyStore<S> {
    fn backend_name(&self) -> &'static str {
        "faulty-memory"
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<S>>> {
        self.inner.load(persistence_id)
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: S,
    ) -> StoreFuture<'a, StateRecord<S>> {
        let fail_at = self.fail_at.load(Ordering::SeqCst);
        if fail_at > 0 && self.seen.fetch_add(1, Ordering::SeqCst) + 1 == fail_at {
            self.fail_at.store(0, Ordering::SeqCst);
            return Box::pin(async {
                Err(DurableError::store(
                    "faulty-memory",
                    "write failure injected",
                ))
            });
        }
        self.inner
            .compare_and_set(persistence_id, expected_revision, state)
    }

    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
    ) -> StoreFuture<'a, Revision> {
        self.inner.delete(persistence_id, expected_revision)
    }
}

/// An outbox dispatcher that answers success and counts what it was handed.
#[derive(Default)]
struct CountingDispatcher {
    dispatched: usize,
}

impl OutboxDispatcher for CountingDispatcher {
    fn dispatch<'a>(&'a mut self, _entry: &'a OutboxEntry) -> OutboxDispatchFuture<'a> {
        self.dispatched += 1;
        Box::pin(async { OutboxDispatchResult::Success })
    }
}

/// A cancel transition from the scheduler: computed, never persisted.
fn cancel(
    plan: &AgentCompiledExecutionPlan,
    state: AgentGraphRunState,
) -> AgentGraphSchedulerTransition {
    AgentGraphScheduler::new()
        .cancel_graph_run(plan, state, ts(500))
        .expect("the graph cancels")
}

/// The scheduled entry of `effect` is cancelled by settling the cancel
/// transition, and a dispatch pass afterwards sends nothing.
#[tokio::test]
async fn settling_a_cancel_transition_cancels_its_nodes_pending_entry() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = effect_plan();
    let (mut inbox, _store) = recovered_inbox("run-cancel-pending").await;
    let (effect_id, state) = schedule(
        &bridge,
        &plan,
        &mut inbox,
        "run-cancel-pending",
        "effect",
        running_effect_state(&plan),
    )
    .await;

    let transition = cancel(&plan, state);
    let settled = bridge
        .settle_cancelled_nodes(&mut inbox, &transition)
        .await
        .expect("the settle writes");

    assert_eq!(
        settled
            .iter()
            .map(|entry| (entry.effect_id.clone(), entry.settlement))
            .collect::<Vec<_>>(),
        vec![(effect_id.clone(), AgentOutboxSettlement::Cancelled)]
    );
    assert_eq!(entry(&inbox, &effect_id).status(), OutboxStatus::Cancelled);
    let mut dispatcher = CountingDispatcher::default();
    inbox
        .inner_mut()
        .dispatch_due_outbox(&mut dispatcher)
        .await
        .expect("the dispatch pass runs");
    assert_eq!(
        dispatcher.dispatched, 0,
        "a cancelled node's effect is not sent"
    );
}

/// An entry whose attempt started is cancelled with that attempt counted, so
/// it still says the attempt started.
#[tokio::test]
async fn an_entry_mid_dispatch_is_cancelled_with_its_attempt_counted() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = effect_plan();
    let (mut inbox, _store) = recovered_inbox("run-cancel-in-flight").await;
    let (effect_id, state) = schedule(
        &bridge,
        &plan,
        &mut inbox,
        "run-cancel-in-flight",
        "effect",
        running_effect_state(&plan),
    )
    .await;
    inbox
        .inner_mut()
        .mark_outbox_dispatching(&message_id(&effect_id))
        .await
        .expect("the attempt starts");

    let settled = bridge
        .settle_cancelled_nodes(&mut inbox, &cancel(&plan, state))
        .await
        .expect("the settle writes");

    assert_eq!(
        settled
            .iter()
            .map(|entry| entry.settlement)
            .collect::<Vec<_>>(),
        vec![AgentOutboxSettlement::CancelledInFlight]
    );
    let entry = entry(&inbox, &effect_id);
    assert_eq!(entry.status(), OutboxStatus::Cancelled);
    assert_eq!(
        entry.attempts().attempts(),
        1,
        "the started attempt is counted"
    );
    assert_eq!(
        entry.attempts().last_error(),
        Some(DISPATCH_CANCELLED_IN_FLIGHT_LINE)
    );
}

/// An entry that already finished is left as it is, and an effect id with
/// no entry (compacted away) is skipped rather than refused.
#[tokio::test]
async fn a_terminal_or_absent_entry_is_left_alone() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = effect_plan();
    let (mut inbox, _store) = recovered_inbox("run-cancel-terminal").await;
    let (effect_id, state) = schedule(
        &bridge,
        &plan,
        &mut inbox,
        "run-cancel-terminal",
        "effect",
        running_effect_state(&plan),
    )
    .await;
    inbox
        .inner_mut()
        .mark_outbox_dispatching(&message_id(&effect_id))
        .await
        .expect("the attempt starts");
    inbox
        .inner_mut()
        .record_outbox_success(&message_id(&effect_id))
        .await
        .expect("the attempt succeeds");

    let mut transition = cancel(&plan, state);
    // An effect id the outbox has no entry for, as after compaction.
    transition
        .state
        .node_states
        .get_mut(&node("effect"))
        .expect("the node state")
        .scheduled_effect_ids
        .push(AgentEffectId::new("effect-compacted-away"));
    let settled = bridge
        .settle_cancelled_nodes(&mut inbox, &transition)
        .await
        .expect("an absent entry is skipped, not refused");

    assert!(settled.is_empty(), "nothing was settled: {settled:?}");
    assert_eq!(entry(&inbox, &effect_id).status(), OutboxStatus::Dispatched);
}

/// A node resolved before the cancel keeps its entries as they are, even one
/// that could still be cancelled: here a checkpoint decided while its
/// approval request was still pending. Only the nodes the transition
/// cancelled are settled.
#[tokio::test]
async fn only_the_nodes_the_transition_cancelled_are_settled() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = checkpoint_plan();
    let run_id = AgentRunId::new("run-cancel-resolved-node");
    let (mut inbox, _store) = recovered_inbox(run_id.as_str()).await;
    let opened = bridge
        .open_node_human_checkpoint(
            &plan,
            running_effect_state(&plan),
            human_checkpoint_request(
                run_id.clone(),
                "effect",
                checkpoint("checkpoint-review"),
                200,
            ),
            &mut inbox,
        )
        .await
        .expect("the checkpoint opens");
    let approval_id = opened.approval_effect.effect_id.clone();
    let decision = bridge
        .human_decision_submitted_command(
            command_metadata(&plan, run_id, "cmd-decided", 300),
            HumanCheckpointId::new("checkpoint-review"),
            "approve",
            HumanCheckpointStatus::Approved,
            None,
        )
        .expect("the decision command builds");
    let decided = bridge
        .accept_and_apply_human_decision(&plan, opened.transition.state, decision, &mut inbox)
        .await
        .expect("the decision completes the node");
    assert_eq!(
        decided
            .transition
            .state
            .node_states
            .get(&node("effect"))
            .expect("the node state")
            .status,
        AgentGraphNodeStatus::Completed
    );
    assert_eq!(
        entry(&inbox, &approval_id).status(),
        OutboxStatus::Pending,
        "the approval request was never sent"
    );

    let transition = cancel(&plan, decided.transition.state);
    let settled = bridge
        .settle_cancelled_nodes(&mut inbox, &transition)
        .await
        .expect("the settle writes");

    assert!(
        settled.is_empty(),
        "the completed node is not settled: {settled:?}"
    );
    assert_eq!(entry(&inbox, &approval_id).status(), OutboxStatus::Pending);
}

/// A transition that cancelled nothing settles nothing.
#[tokio::test]
async fn a_transition_that_cancelled_nothing_settles_nothing() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = effect_plan();
    let (mut inbox, _store) = recovered_inbox("run-cancel-nothing").await;
    let (effect_id, state) = schedule(
        &bridge,
        &plan,
        &mut inbox,
        "run-cancel-nothing",
        "effect",
        running_effect_state(&plan),
    )
    .await;
    let transition = AgentGraphScheduler::new()
        .mark_ready_nodes_runnable(&plan, state, ts(400))
        .expect("nothing new is runnable");

    let settled = bridge
        .settle_cancelled_nodes(&mut inbox, &transition)
        .await
        .expect("the settle runs");

    assert!(settled.is_empty());
    assert_eq!(entry(&inbox, &effect_id).status(), OutboxStatus::Pending);
}

/// A settle that fails part-way returns the error, keeps what it settled,
/// and a repeat settles the rest and reports only those.
#[tokio::test]
async fn a_settle_that_fails_part_way_converges() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = two_effect_plan();
    let (mut inbox, store) = recovered_inbox("run-cancel-part-way").await;
    let run = "run-cancel-part-way";
    let (first, state) = schedule(
        &bridge,
        &plan,
        &mut inbox,
        run,
        "effect-a",
        two_running_effects_state(&plan),
    )
    .await;
    let (second, state) = schedule(&bridge, &plan, &mut inbox, run, "effect-b", state).await;
    let transition = cancel(&plan, state);

    store.fail_write(2);
    let failed = bridge.settle_cancelled_nodes(&mut inbox, &transition).await;
    assert!(failed.is_err(), "the second write failed: {failed:?}");
    assert_eq!(entry(&inbox, &first).status(), OutboxStatus::Cancelled);
    assert_eq!(entry(&inbox, &second).status(), OutboxStatus::Pending);

    let settled = bridge
        .settle_cancelled_nodes(&mut inbox, &transition)
        .await
        .expect("the repeat settles the rest");
    assert_eq!(
        settled
            .iter()
            .map(|entry| entry.effect_id.clone())
            .collect::<Vec<_>>(),
        vec![second.clone()],
        "only what this call settled is reported"
    );
    assert_eq!(entry(&inbox, &second).status(), OutboxStatus::Cancelled);
}

// ---------------------------------------------------------------------------
// The run's cancel, through the core (issue #71 item 4, design 5.3).
// ---------------------------------------------------------------------------

/// The gap the issue names: a cancelled graph run's queued effect was still
/// sent by the next dispatch pass (on the code before this change, the
/// dispatcher counted 1). The cancel now cancels the graph, settles the
/// cancelled nodes' entries, and ends the run in one recorded `Cancel`.
#[tokio::test]
async fn a_cancelled_graph_run_sends_none_of_its_queued_effects() {
    let fixture = CoreFixture::new();
    let run_id = AgentRunId::new("run-core-cancel");
    let (mut core, effect_id) = core_with_effect_scheduled(&fixture, &run_id).await;
    request_cancellation(&mut core).await;

    let cancellation = core.cancel(ts(500)).await.expect("the run cancels");

    assert_eq!(cancellation.transition.kind, AgentRunTransitionKind::Cancel);
    assert_eq!(
        cancellation.transition.previous_status,
        Some(AgentRunStatus::Cancelling)
    );
    assert_eq!(
        cancellation.transition.next_status,
        AgentRunStatus::Cancelled
    );
    let state = &cancellation.transition.state;
    assert_eq!(state.current_step_id, None);
    assert_eq!(state.completed_at, Some(ts(500)));
    let graph = state.graph_state.as_ref().expect("the run has its graph");
    assert_eq!(
        graph.terminal_status,
        Some(AgentGraphTerminalStatus::Cancelled)
    );
    assert_eq!(
        cancellation.cancelled_node_ids,
        vec![node("effect"), node("terminal")]
    );
    assert_eq!(
        settled_pairs(&cancellation.settled_entries),
        vec![(effect_id.clone(), AgentOutboxSettlement::Cancelled)]
    );

    let mut dispatcher = CountingDispatcher::default();
    let parts = core.parts_mut();
    parts
        .inbox
        .inner_mut()
        .dispatch_due_outbox(&mut dispatcher)
        .await
        .expect("the dispatch pass runs");
    assert_eq!(
        dispatcher.dispatched, 0,
        "a cancelled run's effect is not sent"
    );
    assert_eq!(
        entry(parts.inbox, &effect_id).status(),
        OutboxStatus::Cancelled
    );
}

/// A node waiting on a human checkpoint has its approval request settled by
/// the cancel too, beside its sibling's effect.
#[tokio::test]
async fn cancel_settles_a_checkpoints_approval_request() {
    let fixture = CoreFixture::new();
    let run_id = AgentRunId::new("run-core-cancel-checkpoint");
    let plan = effect_and_review_plan();
    let mut core = core_with_started(&fixture, &run_id, &plan, &["effect", "review"]).await;
    let effect_id = core
        .schedule_graph_node_effect(&plan, effect_request(run_id.clone(), "effect", 160))
        .await
        .expect("the effect schedules")
        .effect_outcome
        .effect
        .effect_id;
    let parts = core.parts_mut();
    let opened = parts
        .graph_runtime
        .effect_bridge()
        .open_node_human_checkpoint(
            &plan,
            graph_of(parts.runner),
            human_checkpoint_request(
                run_id.clone(),
                "review",
                checkpoint("checkpoint-review"),
                170,
            ),
            &mut *parts.inbox,
        )
        .await
        .expect("the checkpoint opens");
    parts
        .runner
        .update_graph_state(opened.transition.state, ts(170))
        .await
        .expect("the checkpoint's wait persists");
    let approval_id = opened.approval_effect.effect_id;
    request_cancellation(&mut core).await;

    let cancellation = core.cancel(ts(500)).await.expect("the run cancels");

    assert_eq!(
        cancellation.cancelled_node_ids,
        vec![node("effect"), node("review"), node("terminal")]
    );
    assert_eq!(
        settled_pairs(&cancellation.settled_entries),
        vec![
            (effect_id.clone(), AgentOutboxSettlement::Cancelled),
            (approval_id.clone(), AgentOutboxSettlement::Cancelled),
        ]
    );
    let inbox = core.parts_mut().inbox;
    assert_eq!(entry(inbox, &effect_id).status(), OutboxStatus::Cancelled);
    assert_eq!(entry(inbox, &approval_id).status(), OutboxStatus::Cancelled);
}

/// The entries are settled before the run's write: a crash between them
/// leaves the entries settled and the run still cancelling, and a repeat
/// finishes the run and settles nothing more.
#[tokio::test]
async fn a_crash_between_the_settle_and_the_run_write_converges() {
    let fixture = CoreFixture::new();
    let run_id = AgentRunId::new("run-core-cancel-crash");
    let (mut core, effect_id) = core_with_effect_scheduled(&fixture, &run_id).await;
    request_cancellation(&mut core).await;

    fixture.run_store.fail_write(1);
    let failed = core.cancel(ts(500)).await;
    assert!(
        matches!(
            &failed,
            Err(AgentRunRuntimeError::RunEngine {
                error: AgentRunEngineError::Persistence { .. }
            })
        ),
        "the run's write failed: {failed:?}"
    );
    assert_eq!(
        entry(core.parts_mut().inbox, &effect_id).status(),
        OutboxStatus::Cancelled,
        "the entry was settled before the run's write"
    );
    assert_eq!(run_status(&mut core), AgentRunStatus::Cancelling);

    // The process that crashed is gone: a new core recovers the same stores.
    drop(core);
    let mut core = fixture.core(&run_id);
    core.recover().await.expect("the core recovers");
    assert_eq!(run_status(&mut core), AgentRunStatus::Cancelling);
    let cancellation = core.cancel(ts(510)).await.expect("the repeat finishes");
    assert_eq!(
        cancellation.transition.next_status,
        AgentRunStatus::Cancelled
    );
    assert_eq!(
        cancellation.cancelled_node_ids,
        vec![node("effect"), node("terminal")]
    );
    assert!(
        cancellation.settled_entries.is_empty(),
        "the repeat reports only what it wrote: {:?}",
        cancellation.settled_entries
    );
}

/// A run with no graph cancels as it did: the same transition, and nothing
/// cancelled or settled.
#[tokio::test]
async fn a_run_without_a_graph_cancels_as_before() {
    let fixture = CoreFixture::new();
    let run_id = AgentRunId::new("run-core-cancel-steps");
    let mut core = fixture.core(&run_id);
    core.recover().await.expect("the core recovers");
    core.start(accepted_run_state(&run_id, Some(AgentStepId::new("plan"))))
        .await
        .expect("the run starts");
    request_cancellation(&mut core).await;

    let cancellation = core.cancel(ts(500)).await.expect("the run cancels");

    assert_eq!(cancellation.transition.kind, AgentRunTransitionKind::Cancel);
    assert_eq!(
        cancellation.transition.next_status,
        AgentRunStatus::Cancelled
    );
    assert_eq!(cancellation.transition.state.graph_state, None);
    // The runner's own cancel, which leaves the step as it was.
    assert_eq!(
        cancellation.transition.state.current_step_id,
        Some(AgentStepId::new("plan"))
    );
    assert!(cancellation.cancelled_node_ids.is_empty());
    assert!(cancellation.settled_entries.is_empty());
}

/// A cancel on a run already cancelled is refused, as it was.
#[tokio::test]
async fn cancel_on_a_cancelled_run_is_refused() {
    let fixture = CoreFixture::new();
    let run_id = AgentRunId::new("run-core-cancel-twice");
    let (mut core, _effect_id) = core_with_effect_scheduled(&fixture, &run_id).await;
    request_cancellation(&mut core).await;
    core.cancel(ts(500)).await.expect("the run cancels");

    let refused = core
        .cancel(ts(510))
        .await
        .expect_err("a cancelled run cannot cancel again");
    assert!(
        matches!(
            &refused,
            AgentRunRuntimeError::RunEngine {
                error: AgentRunEngineError::InvalidTransition { .. }
            }
        ),
        "{refused:?}"
    );
}

/// The host's own sequence still works as it did: the plan's cancel
/// persisted through `update_graph_state` ends the run cancelled. That write
/// is composed by the owner, so it settles nothing on its own (design 5.3a),
/// and a `Cancel` afterwards is refused.
#[tokio::test]
async fn the_hosts_own_cancel_sequence_still_works() {
    let fixture = CoreFixture::new();
    let run_id = AgentRunId::new("run-core-cancel-host");
    let plan = effect_plan();
    let (mut core, effect_id) = core_with_effect_scheduled(&fixture, &run_id).await;
    request_cancellation(&mut core).await;

    let parts = core.parts_mut();
    let transition = parts
        .graph_runtime
        .scheduler()
        .cancel_graph_run(&plan, graph_of(parts.runner), ts(450))
        .expect("the graph cancels");
    parts
        .runner
        .update_graph_state(transition.state, ts(450))
        .await
        .expect("the cancelled graph persists");

    assert_eq!(run_status(&mut core), AgentRunStatus::Cancelled);
    assert_eq!(
        entry(core.parts_mut().inbox, &effect_id).status(),
        OutboxStatus::Pending,
        "an owner-composed write settles nothing on its own"
    );
    core.cancel(ts(500))
        .await
        .expect_err("the run is already cancelled");
}

/// The runner's cancel-with-graph write takes only a cancelled graph, so a
/// run never ends cancelled over a graph still running.
#[tokio::test]
async fn the_runner_cancels_with_a_graph_only_when_it_is_cancelled() {
    let fixture = CoreFixture::new();
    let run_id = AgentRunId::new("run-core-cancel-running-graph");
    let (mut core, _effect_id) = core_with_effect_scheduled(&fixture, &run_id).await;
    request_cancellation(&mut core).await;

    let parts = core.parts_mut();
    let running = graph_of(parts.runner);
    let refused = parts
        .runner
        .cancel_with_graph_state(running, ts(500))
        .await
        .expect_err("a graph still running is refused");
    assert!(
        matches!(refused, AgentRunEngineError::InvalidRunState { .. }),
        "{refused:?}"
    );
    assert_eq!(run_status(&mut core), AgentRunStatus::Cancelling);
}

/// The actor's `Cancel` answers what the core's cancel answers, for the same
/// stores and inputs.
#[tokio::test]
async fn the_actor_answers_cancel_as_the_core_does() {
    let run_id = AgentRunId::new("run-core-cancel-actor");
    let through_core = CoreFixture::new();
    let (mut core, _effect_id) = core_with_effect_scheduled(&through_core, &run_id).await;
    request_cancellation(&mut core).await;
    let from_core = core.cancel(ts(500)).await;

    let through_actor = CoreFixture::new();
    let (mut prepared, _effect_id) = core_with_effect_scheduled(&through_actor, &run_id).await;
    request_cancellation(&mut prepared).await;
    drop(prepared);
    let system = ActorSystem::new("graph-cancellation-actor");
    let actor: ActorRef<AgentRunActorCommand> = system
        .spawn("run", AgentRunActor::from_core(through_actor.core(&run_id)))
        .expect("the actor spawns");
    let from_actor = actor
        .ask(
            |reply_to| AgentRunActorCommand::Cancel {
                now: ts(500),
                reply_to,
            },
            Duration::from_secs(1),
        )
        .await
        .expect("the actor replies");

    assert!(from_core.is_ok(), "{from_core:?}");
    assert_eq!(from_actor, from_core);
    system.terminate().await.expect("the system terminates");
}

// ---------------------------------------------------------------------------
// A graph that ends by completing or failing cancels its unresolved
// siblings, and settles their entries before it is persisted (design 5.3a).
// ---------------------------------------------------------------------------

/// The core completes the terminal node while a sibling waits on its effect:
/// the graph ends `Completed`, the sibling is cancelled, and its entry is
/// settled, so a dispatch pass afterwards sends nothing.
#[tokio::test]
async fn a_graph_completed_while_a_sibling_waits_settles_the_siblings_entry() {
    let fixture = CoreFixture::new();
    let run_id = AgentRunId::new("run-core-complete-sibling");
    let plan = effect_beside_terminal_plan();
    let (mut core, effect_id) = core_with_sibling_scheduled(&fixture, &run_id, &plan).await;

    let completed = core
        .complete_graph_node(&plan, node("terminal"), ts(300))
        .await
        .expect("the terminal completes the graph");

    let graph = &completed.graph_transition.state;
    assert_eq!(
        graph.terminal_status,
        Some(AgentGraphTerminalStatus::Completed)
    );
    assert_eq!(
        graph.node_states[&node("effect")].status,
        AgentGraphNodeStatus::Cancelled
    );
    assert_eq!(
        completed.run_transition.next_status,
        AgentRunStatus::Completed
    );
    let mut dispatcher = CountingDispatcher::default();
    let parts = core.parts_mut();
    assert_eq!(
        entry(parts.inbox, &effect_id).status(),
        OutboxStatus::Cancelled
    );
    parts
        .inbox
        .inner_mut()
        .dispatch_due_outbox(&mut dispatcher)
        .await
        .expect("the dispatch pass runs");
    assert_eq!(
        dispatcher.dispatched, 0,
        "a cancelled sibling's effect is not sent"
    );
}

/// An effect failure that fails the graph settles its sibling's entry before
/// the bridge hands the transition back, and reports it. The failed node's
/// own entry is left to the dispatcher that failed it.
#[tokio::test]
async fn a_graph_failed_by_an_effect_failure_settles_the_siblings_entry() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = two_effect_plan();
    let run = "run-fail-sibling";
    let (mut inbox, _store) = recovered_inbox(run).await;
    let (failed_id, sibling_id, state) =
        two_effects_scheduled(&bridge, &plan, &mut inbox, run).await;
    let command = effect_failure(
        &bridge,
        &plan,
        run,
        "cmd-effect-a-exhausted",
        &failed_id,
        AgentGraphEffectFailureDisposition::Exhausted,
    );

    let outcome = bridge
        .accept_and_apply_effect_failed(&plan, state, command, &mut inbox)
        .await
        .expect("the exhausted failure fails the graph");

    assert_eq!(
        outcome.transition.state.terminal_status,
        Some(AgentGraphTerminalStatus::Failed)
    );
    assert_eq!(
        settled_pairs(&outcome.settled_entries),
        vec![(sibling_id.clone(), AgentOutboxSettlement::Cancelled)]
    );
    assert_eq!(
        entry(&inbox, &sibling_id).status(),
        OutboxStatus::Cancelled,
        "settled before the owner persists anything"
    );
    assert_eq!(entry(&inbox, &failed_id).status(), OutboxStatus::Pending);
}

/// An `Abandon` decision on a parked node fails the graph, and settles the
/// sibling's entry the same way. The park itself cancels nothing.
#[tokio::test]
async fn an_abandon_decision_settles_the_siblings_entry() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = two_effect_plan();
    let run = "run-abandon-sibling";
    let (mut inbox, _store) = recovered_inbox(run).await;
    let (parked_id, sibling_id, state) =
        two_effects_scheduled(&bridge, &plan, &mut inbox, run).await;
    inbox
        .inner_mut()
        .record_outbox_exhausted(&message_id(&parked_id), "effect-outcome-unknown")
        .await
        .expect("the entry exhausts");
    let reconcile = effect_failure(
        &bridge,
        &plan,
        run,
        "cmd-effect-a-reconcile",
        &parked_id,
        AgentGraphEffectFailureDisposition::Reconcile,
    );
    let parked = bridge
        .accept_and_apply_effect_failed(&plan, state, reconcile, &mut inbox)
        .await
        .expect("the node parks");
    assert!(parked.settled_entries.is_empty(), "a park cancels nothing");
    assert_eq!(entry(&inbox, &sibling_id).status(), OutboxStatus::Pending);
    let decision = bridge
        .reconciliation_decision_command(
            command_metadata(&plan, AgentRunId::new(run), "cmd-effect-a-abandon", 400),
            parked_id,
            AgentGraphReconciliationDecision::Abandon {
                error_code: "effect-abandoned".to_string(),
            },
        )
        .expect("the decision command builds");

    let abandoned = bridge
        .accept_and_apply_reconciliation_decision(
            &plan,
            parked.transition.state,
            decision,
            &mut inbox,
        )
        .await
        .expect("the abandon fails the graph");

    assert_eq!(
        abandoned.transition.state.terminal_status,
        Some(AgentGraphTerminalStatus::Failed)
    );
    assert_eq!(
        settled_pairs(&abandoned.settled_entries),
        vec![(sibling_id.clone(), AgentOutboxSettlement::Cancelled)]
    );
    assert_eq!(entry(&inbox, &sibling_id).status(), OutboxStatus::Cancelled);
}

/// A failure command re-delivered after the owner lost its persist: the
/// inbox answers it as a duplicate, the transition is recomputed the same,
/// and the settle finds the sibling's entry already settled.
#[tokio::test]
async fn a_replayed_failure_command_settles_nothing_more() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = two_effect_plan();
    let run = "run-fail-replayed";
    let (mut inbox, _store) = recovered_inbox(run).await;
    let (failed_id, sibling_id, state) =
        two_effects_scheduled(&bridge, &plan, &mut inbox, run).await;
    let command = effect_failure(
        &bridge,
        &plan,
        run,
        "cmd-effect-a-replayed",
        &failed_id,
        AgentGraphEffectFailureDisposition::Exhausted,
    );
    let first = bridge
        .accept_and_apply_effect_failed(&plan, state.clone(), command.clone(), &mut inbox)
        .await
        .expect("the failure applies");
    assert_eq!(first.settled_entries.len(), 1);

    let replayed = bridge
        .accept_and_apply_effect_failed(&plan, state, command, &mut inbox)
        .await
        .expect("the replay applies");

    assert!(replayed.acceptance.is_duplicate());
    assert_eq!(replayed.transition, first.transition);
    assert!(
        replayed.settled_entries.is_empty(),
        "nothing more to settle: {:?}",
        replayed.settled_entries
    );
    assert_eq!(entry(&inbox, &sibling_id).status(), OutboxStatus::Cancelled);
}

/// A cancel requested while the graph runs, then the graph completes in the
/// same pass. The completion ends the run `Completed` (as it did) and
/// settles the cancelled sibling's entry, so the cancel that follows is
/// refused as it was, with nothing left due.
#[tokio::test]
async fn cancel_after_the_graph_completed_in_the_same_pass() {
    let fixture = CoreFixture::new();
    let run_id = AgentRunId::new("run-core-cancel-after-complete");
    let plan = effect_beside_terminal_plan();
    let (mut core, effect_id) = core_with_sibling_scheduled(&fixture, &run_id, &plan).await;
    request_cancellation(&mut core).await;
    let completed = core
        .complete_graph_node(&plan, node("terminal"), ts(450))
        .await
        .expect("the terminal completes the graph");
    assert_eq!(
        completed.run_transition.next_status,
        AgentRunStatus::Completed
    );

    let refused = core
        .cancel(ts(500))
        .await
        .expect_err("a completed run is not cancelled");
    assert!(
        matches!(
            &refused,
            AgentRunRuntimeError::RunEngine {
                error: AgentRunEngineError::InvalidTransition { .. }
            }
        ),
        "{refused:?}"
    );
    let mut dispatcher = CountingDispatcher::default();
    let parts = core.parts_mut();
    assert_eq!(
        entry(parts.inbox, &effect_id).status(),
        OutboxStatus::Cancelled
    );
    parts
        .inbox
        .inner_mut()
        .dispatch_due_outbox(&mut dispatcher)
        .await
        .expect("the dispatch pass runs");
    assert_eq!(dispatcher.dispatched, 0);
}

// ---------------------------------------------------------------------------
// Fixtures.
// ---------------------------------------------------------------------------

fn node(id: &str) -> AgentCompiledNodeId {
    AgentCompiledNodeId::new(id)
}

fn ts(millis: u64) -> AgentTimestampMillis {
    AgentTimestampMillis::new(millis)
}

async fn recovered_inbox(run_id: &str) -> (TestInbox, FaultyWorkflowStore) {
    let store = FaultyStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let mut inbox = AgentRunInbox::with_clock(AgentRunId::new(run_id), store.clone(), clock);
    inbox.recover().await.expect("the inbox recovers");
    (inbox, store)
}

async fn schedule(
    bridge: &AgentGraphEffectBridge,
    plan: &AgentCompiledExecutionPlan,
    inbox: &mut TestInbox,
    run_id: &str,
    node: &str,
    state: AgentGraphRunState,
) -> (AgentEffectId, AgentGraphRunState) {
    let run_id = AgentRunId::new(run_id);
    let scheduled = bridge
        .schedule_node_effect(plan, state, effect_request(run_id, node, 200), inbox)
        .await
        .expect("the effect schedules");
    (scheduled.effect.effect_id, scheduled.transition.state)
}

fn message_id(effect_id: &AgentEffectId) -> OutboxMessageId {
    OutboxMessageId::new(effect_id.as_str())
}

fn entry(inbox: &TestInbox, effect_id: &AgentEffectId) -> OutboxEntry {
    inbox
        .inner()
        .state()
        .expect("the inbox is recovered")
        .outbox_entry(&message_id(effect_id))
        .expect("the outbox entry exists")
        .clone()
}

fn tool_node(id: &str, output: &str) -> AgentCompiledPlanNode {
    AgentCompiledPlanNode::new(id, AgentCompiledNodeKind::ToolCall)
        .input_port(AgentCompiledPlanPort::new(
            "payload",
            AgentCompiledPortDirection::Input,
            "input",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "result",
            AgentCompiledPortDirection::Output,
            output,
        ))
        .target(
            AgentCompiledNodeTarget::new("tool", "payments.charge")
                .address("tool://payments.charge")
                .attribute("target_class", "payments"),
        )
}

fn input_node() -> AgentCompiledPlanNode {
    AgentCompiledPlanNode::new("input", AgentCompiledNodeKind::Input).output_port(
        AgentCompiledPlanPort::new("payload", AgentCompiledPortDirection::Output, "input"),
    )
}

fn plan_shell(id: &str) -> AgentCompiledExecutionPlan {
    AgentCompiledExecutionPlan::new(
        AgentCompiledPlanId::new(id),
        AgentWorkflowId::new("workflow-graph-cancellation"),
        "graph-cancellation",
        WorkflowDefinitionVersion::new("v1"),
        CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
        AgentCompiledPlanFingerprint::new(format!("sha256:{id}")),
    )
    .entry_node("input")
}

/// `input` → `effect` (a tool call) → `terminal`.
fn effect_plan() -> AgentCompiledExecutionPlan {
    single_node_plan("plan-cancel-effect", tool_node("effect", "effect-result"))
}

/// `input` → `effect` (a human checkpoint) → `terminal`.
fn checkpoint_plan() -> AgentCompiledExecutionPlan {
    single_node_plan(
        "plan-cancel-checkpoint",
        checkpoint_node("effect", "effect-result"),
    )
}

fn checkpoint_node(id: &str, output: &str) -> AgentCompiledPlanNode {
    AgentCompiledPlanNode::new(id, AgentCompiledNodeKind::HumanCheckpoint)
        .input_port(AgentCompiledPlanPort::new(
            "payload",
            AgentCompiledPortDirection::Input,
            "input",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "result",
            AgentCompiledPortDirection::Output,
            output,
        ))
}

fn single_node_plan(id: &str, middle: AgentCompiledPlanNode) -> AgentCompiledExecutionPlan {
    let terminal = AgentCompiledPlanNode::new("terminal", AgentCompiledNodeKind::Terminal)
        .input_port(AgentCompiledPlanPort::new(
            "result",
            AgentCompiledPortDirection::Input,
            "effect-result",
        ));
    plan_shell(id)
        .node(input_node())
        .node(middle)
        .node(terminal)
        .edge(AgentCompiledPlanEdge::new(
            "edge-input-effect",
            "input",
            "payload",
            "effect",
            "payload",
        ))
        .edge(AgentCompiledPlanEdge::new(
            "edge-effect-terminal",
            "effect",
            "result",
            "terminal",
            "result",
        ))
}

/// `input` → `effect-a` and `effect-b` (tool calls) → `terminal`.
fn two_effect_plan() -> AgentCompiledExecutionPlan {
    two_branch_plan(
        "plan-cancel-two-effects",
        tool_node("effect-a", "effect-a-result"),
        tool_node("effect-b", "effect-b-result"),
    )
}

/// `input` → `effect` (a tool call) and `review` (a human checkpoint) →
/// `terminal`.
fn effect_and_review_plan() -> AgentCompiledExecutionPlan {
    two_branch_plan(
        "plan-cancel-effect-and-review",
        tool_node("effect", "effect-result"),
        checkpoint_node("review", "review-result"),
    )
}

/// `input` → `left` and `right` → `terminal`, which needs both. Each branch
/// node's output port is `result`, typed `<id>-result`.
fn two_branch_plan(
    id: &str,
    left: AgentCompiledPlanNode,
    right: AgentCompiledPlanNode,
) -> AgentCompiledExecutionPlan {
    let left_id = left.node_id.as_str().to_string();
    let right_id = right.node_id.as_str().to_string();
    let terminal = AgentCompiledPlanNode::new("terminal", AgentCompiledNodeKind::Terminal)
        .input_port(AgentCompiledPlanPort::new(
            "left",
            AgentCompiledPortDirection::Input,
            format!("{left_id}-result"),
        ))
        .input_port(AgentCompiledPlanPort::new(
            "right",
            AgentCompiledPortDirection::Input,
            format!("{right_id}-result"),
        ));
    plan_shell(id)
        .node(input_node())
        .node(left)
        .node(right)
        .node(terminal)
        .edge(AgentCompiledPlanEdge::new(
            format!("edge-input-{left_id}"),
            "input",
            "payload",
            left_id.clone(),
            "payload",
        ))
        .edge(AgentCompiledPlanEdge::new(
            format!("edge-input-{right_id}"),
            "input",
            "payload",
            right_id.clone(),
            "payload",
        ))
        .edge(AgentCompiledPlanEdge::new(
            format!("edge-{left_id}-terminal"),
            left_id,
            "result",
            "terminal",
            "left",
        ))
        .edge(AgentCompiledPlanEdge::new(
            format!("edge-{right_id}-terminal"),
            right_id,
            "result",
            "terminal",
            "right",
        ))
}

/// The plan's `input` completed and `effect` started.
fn running_effect_state(plan: &AgentCompiledExecutionPlan) -> AgentGraphRunState {
    let state = input_completed(plan);
    AgentGraphScheduler::new()
        .start_node(plan, state, "effect", ts(150))
        .expect("effect starts")
        .state
}

/// The two-effect plan's `input` completed and both effects started.
fn two_running_effects_state(plan: &AgentCompiledExecutionPlan) -> AgentGraphRunState {
    let scheduler = AgentGraphScheduler::new();
    let state = input_completed(plan);
    let state = scheduler
        .start_node(plan, state, "effect-a", ts(150))
        .expect("effect-a starts")
        .state;
    scheduler
        .start_node(plan, state, "effect-b", ts(151))
        .expect("effect-b starts")
        .state
}

fn input_completed(plan: &AgentCompiledExecutionPlan) -> AgentGraphRunState {
    let scheduler = AgentGraphScheduler::new();
    let state = scheduler
        .initialize_state(plan, ts(100))
        .expect("the graph initializes");
    let state = scheduler
        .mark_ready_nodes_runnable(plan, state, ts(110))
        .expect("input is runnable")
        .state;
    let state = scheduler
        .start_node(plan, state, "input", ts(120))
        .expect("input starts")
        .state;
    let state = scheduler
        .complete_node(plan, state, "input", ts(130))
        .expect("input completes")
        .state;
    scheduler
        .mark_ready_nodes_runnable(plan, state, ts(140))
        .expect("the next nodes are runnable")
        .state
}

fn effect_request(
    run_id: AgentRunId,
    node_id: &str,
    created_at_millis: u64,
) -> AgentGraphEffectScheduleRequest {
    AgentGraphEffectScheduleRequest::new(
        run_id,
        node_id,
        ts(created_at_millis),
        AgentCausationId::new("cause:graph-cancellation"),
        AgentCorrelationId::new("correlation:graph-cancellation"),
    )
    .timeout_ms(2_000)
    .expected_result_type("effect.result")
}

fn human_checkpoint_request(
    run_id: AgentRunId,
    node_id: &str,
    checkpoint: HumanCheckpoint,
    created_at_millis: u64,
) -> AgentGraphHumanCheckpointScheduleRequest {
    AgentGraphHumanCheckpointScheduleRequest::new(
        run_id,
        node_id,
        checkpoint,
        AgentEffectTarget {
            target_type: "human".to_string(),
            name: "approval-ui".to_string(),
            address: Some("https://approvals.local/queue".to_string()),
            attributes: BTreeMap::from([(
                "target_class".to_string(),
                "human-approval".to_string(),
            )]),
        },
        ts(created_at_millis),
        AgentCausationId::new("cause:graph-cancellation"),
        AgentCorrelationId::new("correlation:graph-cancellation"),
    )
    .timeout_ms(3_000)
}

fn checkpoint(checkpoint_id: &str) -> HumanCheckpoint {
    HumanCheckpoint {
        checkpoint_id: HumanCheckpointId::new(checkpoint_id),
        status: HumanCheckpointStatus::Open,
        summary: "Review the action".to_string(),
        available_decisions: vec![HumanDecisionOption {
            value: "approve".to_string(),
            label: "Approve".to_string(),
            requires_comment: false,
        }],
        required_roles: vec!["reviewer".to_string()],
        due_at: Some(ts(600)),
        escalation_target: None,
        context_artifacts: Vec::new(),
        created_by: None,
        resolved_by: None,
        created_at: ts(123),
        resolved_at: None,
        audit_event_ids: Vec::new(),
    }
}

fn command_metadata(
    plan: &AgentCompiledExecutionPlan,
    run_id: AgentRunId,
    command_id: &str,
    received_at_millis: u64,
) -> AgentCommandMetadata {
    let durability = AgentDurabilityMetadata::new(
        AgentDeduplicationKey::new(format!("dedupe-{command_id}")),
        AgentCausationId::new(format!("cause-{command_id}")),
        AgentCorrelationId::new("correlation:graph-cancellation"),
    );
    AgentCommandMetadata::new(
        plan.workflow_id.clone(),
        run_id,
        AgentCommandId::new(command_id),
        durability,
        AgentTenantId::new("tenant-a"),
        ts(received_at_millis),
    )
    .expect("the command metadata validates")
}

/// The stores a core runs over, shared by every core a test builds.
struct CoreFixture {
    run_store: FaultyRunStore,
    workflow_store: FaultyWorkflowStore,
    clock: ManualWorkflowClock,
}

impl CoreFixture {
    fn new() -> Self {
        Self {
            run_store: FaultyStore::new(),
            workflow_store: FaultyStore::new(),
            clock: ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100)),
        }
    }

    fn core(&self, run_id: &AgentRunId) -> TestCore {
        AgentRunCore::with_clock(
            workflow(),
            run_id.clone(),
            self.run_store.clone(),
            self.workflow_store.clone(),
            self.clock.clone(),
        )
    }
}

/// The workflow every plan shell belongs to, with one step for a run that
/// has no graph.
fn workflow() -> AgentWorkflow {
    AgentWorkflow {
        workflow_id: AgentWorkflowId::new("workflow-graph-cancellation"),
        workflow_type: "graph-cancellation".to_string(),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        state_schema_version: StateSchemaVersion::new(1),
        display_name: None,
        status_labels: Vec::new(),
        command_types: Vec::new(),
        steps: vec![AgentStep {
            step_id: AgentStepId::new("plan"),
            kind: AgentStepKind::Planner,
            display_name: None,
            next_step_ids: Vec::new(),
            timeout_ms: Some(1_000),
            config_ref: None,
            observability_labels: BTreeMap::new(),
        }],
        payload_types: Vec::new(),
        retry_policy_ref: None,
        timeout_policy_ref: None,
        approval_policy_ref: None,
        observability_labels: BTreeMap::new(),
    }
}

/// An accepted run, on `first_step_id` when it has no graph.
fn accepted_run_state(run_id: &AgentRunId, first_step_id: Option<AgentStepId>) -> AgentRunState {
    let workflow = workflow();
    AgentRunState {
        run_id: run_id.clone(),
        workflow_id: workflow.workflow_id,
        tenant: Some(AgentTenantId::new("tenant-a")),
        definition_version: workflow.definition_version,
        state_schema_version: workflow.state_schema_version,
        graph_state: None,
        status: AgentRunStatus::Accepted,
        current_step_id: first_step_id,
        current_attempt: 0,
        inputs_ref: None,
        state_payload: AgentStatePayload::Empty,
        checkpoints: Vec::new(),
        pending_effects: Vec::new(),
        pending_human_checkpoint: None,
        cancellation: None,
        created_at: ts(100),
        updated_at: ts(100),
        completed_at: None,
    }
}

/// A recovered core over `plan`, its `input` completed and `nodes` started.
async fn core_with_started(
    fixture: &CoreFixture,
    run_id: &AgentRunId,
    plan: &AgentCompiledExecutionPlan,
    nodes: &[&str],
) -> TestCore {
    let mut core = fixture.core(run_id);
    core.recover().await.expect("the core recovers");
    core.start_graph(accepted_run_state(run_id, None), plan, ts(100))
        .await
        .expect("the graph starts");
    core.mark_graph_ready(plan, ts(110))
        .await
        .expect("input is ready");
    core.start_graph_node(plan, node("input"), ts(120))
        .await
        .expect("input starts");
    core.complete_graph_node(plan, node("input"), ts(130))
        .await
        .expect("input completes");
    core.mark_graph_ready(plan, ts(140))
        .await
        .expect("the next nodes are ready");
    for (offset, name) in (0u64..).zip(nodes) {
        core.start_graph_node(plan, node(name), ts(150 + offset))
            .await
            .expect("the node starts");
    }
    core
}

/// A recovered core on [`effect_plan`] with `effect`'s effect scheduled.
async fn core_with_effect_scheduled(
    fixture: &CoreFixture,
    run_id: &AgentRunId,
) -> (TestCore, AgentEffectId) {
    let plan = effect_plan();
    let mut core = core_with_started(fixture, run_id, &plan, &["effect"]).await;
    let scheduled = core
        .schedule_graph_node_effect(&plan, effect_request(run_id.clone(), "effect", 160))
        .await
        .expect("the effect schedules");
    let effect_id = scheduled.effect_outcome.effect.effect_id.clone();
    (core, effect_id)
}

async fn request_cancellation(core: &mut TestCore) {
    core.request_cancellation("operator-cancel".to_string(), None, ts(400))
        .await
        .expect("cancellation is requested");
}

fn settled_pairs(
    settled: &[AgentSettledOutboxEntry],
) -> Vec<(AgentEffectId, AgentOutboxSettlement)> {
    settled
        .iter()
        .map(|entry| (entry.effect_id.clone(), entry.settlement))
        .collect()
}

/// The graph state the runner holds.
fn graph_of(runner: &AgentStepRunner<FaultyRunStore>) -> AgentGraphRunState {
    runner
        .state()
        .expect("the runner reads")
        .and_then(|state| state.graph_state.clone())
        .expect("the run has graph state")
}

fn run_status(core: &mut TestCore) -> AgentRunStatus {
    core.parts_mut()
        .runner
        .state()
        .expect("the runner reads")
        .expect("the run exists")
        .status
}

/// `input` → `effect` (a tool call), and `input` → `terminal` directly: the
/// graph can complete while `effect` still waits.
fn effect_beside_terminal_plan() -> AgentCompiledExecutionPlan {
    let terminal =
        AgentCompiledPlanNode::new("terminal", AgentCompiledNodeKind::Terminal).input_port(
            AgentCompiledPlanPort::new("payload", AgentCompiledPortDirection::Input, "input"),
        );
    plan_shell("plan-cancel-effect-beside-terminal")
        .node(input_node())
        .node(tool_node("effect", "effect-result"))
        .node(terminal)
        .edge(AgentCompiledPlanEdge::new(
            "edge-input-effect",
            "input",
            "payload",
            "effect",
            "payload",
        ))
        .edge(AgentCompiledPlanEdge::new(
            "edge-input-terminal",
            "input",
            "payload",
            "terminal",
            "payload",
        ))
}

/// A core on [`effect_beside_terminal_plan`] with `effect`'s effect
/// scheduled and `terminal` started.
async fn core_with_sibling_scheduled(
    fixture: &CoreFixture,
    run_id: &AgentRunId,
    plan: &AgentCompiledExecutionPlan,
) -> (TestCore, AgentEffectId) {
    let mut core = core_with_started(fixture, run_id, plan, &["effect", "terminal"]).await;
    let effect_id = core
        .schedule_graph_node_effect(plan, effect_request(run_id.clone(), "effect", 160))
        .await
        .expect("the effect schedules")
        .effect_outcome
        .effect
        .effect_id;
    (core, effect_id)
}

/// Both effects of [`two_effect_plan`] scheduled: `effect-a`'s id,
/// `effect-b`'s id, and the graph state.
async fn two_effects_scheduled(
    bridge: &AgentGraphEffectBridge,
    plan: &AgentCompiledExecutionPlan,
    inbox: &mut TestInbox,
    run: &str,
) -> (AgentEffectId, AgentEffectId, AgentGraphRunState) {
    let (first, state) = schedule(
        bridge,
        plan,
        inbox,
        run,
        "effect-a",
        two_running_effects_state(plan),
    )
    .await;
    let (second, state) = schedule(bridge, plan, inbox, run, "effect-b", state).await;
    (first, second, state)
}

fn effect_failure(
    bridge: &AgentGraphEffectBridge,
    plan: &AgentCompiledExecutionPlan,
    run: &str,
    command_id: &str,
    effect_id: &AgentEffectId,
    disposition: AgentGraphEffectFailureDisposition,
) -> AgentCommand {
    bridge
        .effect_failed_command(
            command_metadata(plan, AgentRunId::new(run), command_id, 300),
            effect_id.clone(),
            "effect-outcome-unknown",
            disposition,
        )
        .expect("the failure command builds")
}
