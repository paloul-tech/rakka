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

use rakka_agent_workflow::{
    AgentCausationId, AgentCommandId, AgentCommandMetadata, AgentCompiledExecutionPlan,
    AgentCompiledNodeKind, AgentCompiledNodeTarget, AgentCompiledPlanEdge,
    AgentCompiledPlanFingerprint, AgentCompiledPlanId, AgentCompiledPlanNode,
    AgentCompiledPlanPort, AgentCompiledPortDirection, AgentCorrelationId, AgentDeduplicationKey,
    AgentDurabilityMetadata, AgentEffectId, AgentEffectTarget, AgentGraphEffectBridge,
    AgentGraphEffectScheduleRequest, AgentGraphHumanCheckpointScheduleRequest,
    AgentGraphNodeStatus, AgentGraphRunState, AgentGraphScheduler, AgentGraphSchedulerTransition,
    AgentOutboxSettlement, AgentRunId, AgentRunInbox, AgentTenantId, AgentTimestampMillis,
    AgentWorkflowId, HumanCheckpoint, HumanCheckpointId, HumanCheckpointStatus,
    HumanDecisionOption, WorkflowDefinitionVersion, CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
    DISPATCH_CANCELLED_IN_FLIGHT_LINE,
};
use rakka_persistence::{
    DurableError, DurableStateStore, InMemoryDurableStateStore, PersistenceId, Revision,
    StateRecord, StoreFuture,
};
use rakka_workflow::{
    ManualWorkflowClock, OutboxDispatchFuture, OutboxDispatchResult, OutboxDispatcher, OutboxEntry,
    OutboxMessageId, OutboxStatus, WorkflowState, WorkflowTimestamp,
};

type TestStore = InMemoryDurableStateStore<WorkflowState>;
type TestInbox = AgentRunInbox<FaultyWorkflowStore, ManualWorkflowClock>;

/// A workflow store whose `n`th compare-and-set from arming fails once, as a
/// store error. Unarmed, it is the in-memory store.
#[derive(Clone)]
struct FaultyWorkflowStore {
    inner: TestStore,
    /// The compare-and-set count, from arming, that fails; 0 is unarmed.
    fail_at: Arc<AtomicUsize>,
    seen: Arc<AtomicUsize>,
}

impl FaultyWorkflowStore {
    fn new() -> Self {
        Self {
            inner: TestStore::new(),
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

impl DurableStateStore<WorkflowState> for FaultyWorkflowStore {
    fn backend_name(&self) -> &'static str {
        "faulty-memory"
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<WorkflowState>>> {
        self.inner.load(persistence_id)
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: WorkflowState,
    ) -> StoreFuture<'a, StateRecord<WorkflowState>> {
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
        .get_mut(&rakka_agent_workflow::AgentCompiledNodeId::new("effect"))
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
            .get(&rakka_agent_workflow::AgentCompiledNodeId::new("effect"))
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
// Fixtures.
// ---------------------------------------------------------------------------

fn ts(millis: u64) -> AgentTimestampMillis {
    AgentTimestampMillis::new(millis)
}

async fn recovered_inbox(run_id: &str) -> (TestInbox, FaultyWorkflowStore) {
    let store = FaultyWorkflowStore::new();
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
    let checkpoint = AgentCompiledPlanNode::new("effect", AgentCompiledNodeKind::HumanCheckpoint)
        .input_port(AgentCompiledPlanPort::new(
            "payload",
            AgentCompiledPortDirection::Input,
            "input",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "result",
            AgentCompiledPortDirection::Output,
            "effect-result",
        ));
    single_node_plan("plan-cancel-checkpoint", checkpoint)
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

/// `input` → `effect-a` and `effect-b` → `terminal`.
fn two_effect_plan() -> AgentCompiledExecutionPlan {
    let terminal = AgentCompiledPlanNode::new("terminal", AgentCompiledNodeKind::Terminal)
        .input_port(AgentCompiledPlanPort::new(
            "a",
            AgentCompiledPortDirection::Input,
            "effect-a-result",
        ))
        .input_port(AgentCompiledPlanPort::new(
            "b",
            AgentCompiledPortDirection::Input,
            "effect-b-result",
        ));
    plan_shell("plan-cancel-two-effects")
        .node(input_node())
        .node(tool_node("effect-a", "effect-a-result"))
        .node(tool_node("effect-b", "effect-b-result"))
        .node(terminal)
        .edge(AgentCompiledPlanEdge::new(
            "edge-input-a",
            "input",
            "payload",
            "effect-a",
            "payload",
        ))
        .edge(AgentCompiledPlanEdge::new(
            "edge-input-b",
            "input",
            "payload",
            "effect-b",
            "payload",
        ))
        .edge(AgentCompiledPlanEdge::new(
            "edge-a-terminal",
            "effect-a",
            "result",
            "terminal",
            "a",
        ))
        .edge(AgentCompiledPlanEdge::new(
            "edge-b-terminal",
            "effect-b",
            "result",
            "terminal",
            "b",
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
