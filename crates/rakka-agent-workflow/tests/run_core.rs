//! An owner-held run core (issue #71, item 1).
//!
//! `AgentRunCore` is what `AgentRunActor` was made of: the run's step runner,
//! its durable inbox, the graph runtime and the snapshot registry, with every
//! operation the actor offers. An owner that is already a single writer, such
//! as a sharded entity, holds one core and drives the run through it, with no
//! second inbox and runner over the same stores. The actor is one such owner:
//! it holds a core and delegates each command to it.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rakka_agent_workflow::{
    AgentCausationId, AgentCommand, AgentCommandId, AgentCommandKind, AgentCommandMetadata,
    AgentCompiledExecutionPlan, AgentCompiledNodeId, AgentCompiledNodeKind,
    AgentCompiledNodeTarget, AgentCompiledPlanEdge, AgentCompiledPlanFingerprint,
    AgentCompiledPlanId, AgentCompiledPlanNode, AgentCompiledPlanPort, AgentCompiledPortDirection,
    AgentCorrelationId, AgentDeduplicationKey, AgentDurabilityMetadata, AgentEffectId,
    AgentGraphEffectBridge, AgentGraphEffectScheduleRequest, AgentGraphNodeStatus,
    AgentGraphRunProjection, AgentGraphRunState, AgentGraphWaitReason, AgentPayloadDescriptor,
    AgentRunActor, AgentRunActorCommand, AgentRunActorSnapshot, AgentRunCore, AgentRunEngineError,
    AgentRunId, AgentRunInbox, AgentRunRuntimeError, AgentRunState, AgentRunStatus,
    AgentStatePayload, AgentStep, AgentStepId, AgentStepKind, AgentStepRunner, AgentStepSuccess,
    AgentTenantId, AgentTimestampMillis, AgentWorkflow, AgentWorkflowId,
    AgentWorkflowSnapshotRegistry, ArtifactEncryptionRef, ArtifactKind, ArtifactRef, InlineState,
    RedactionStatus, StateSchemaVersion, WorkflowDefinitionVersion,
    CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
};
use rakka_core::{ActorRef, ActorSystem, InMemoryMetricsRecorder, Message, ReplyTo};
use rakka_persistence::{
    DurableError, DurableStateStore, InMemoryDurableStateStore, PersistenceId, Revision,
    StateRecord, StoreFuture,
};
use rakka_workflow::{
    ManualWorkflowClock, OutboxDispatchFuture, OutboxDispatchResult, OutboxDispatcher, OutboxEntry,
    OutboxMessageId, WorkflowState, WorkflowTimestamp,
};

type RunStore = InMemoryDurableStateStore<AgentRunState>;
type WorkflowStore = InMemoryDurableStateStore<WorkflowState>;
type TestCore = AgentRunCore<RunStore, WorkflowStore, ManualWorkflowClock>;
type TestActor = AgentRunActor<RunStore, WorkflowStore, ManualWorkflowClock>;
type TestActorRef = ActorRef<AgentRunActorCommand>;

const ASK_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct RuntimeFixture {
    workflow: AgentWorkflow,
    run_store: RunStore,
    workflow_store: WorkflowStore,
    clock: ManualWorkflowClock,
    metrics: Arc<InMemoryMetricsRecorder>,
}

impl RuntimeFixture {
    fn new(workflow: AgentWorkflow) -> Self {
        Self {
            workflow,
            run_store: RunStore::new(),
            workflow_store: WorkflowStore::new(),
            clock: ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100)),
            metrics: Arc::new(InMemoryMetricsRecorder::new()),
        }
    }
}

/// A core over the fixture's stores, as an owner builds one.
fn core(fixture: &RuntimeFixture, run_id: AgentRunId) -> TestCore {
    TestCore::with_clock_and_metrics(
        fixture.workflow.clone(),
        run_id,
        fixture.run_store.clone(),
        fixture.workflow_store.clone(),
        fixture.clock.clone(),
        fixture.metrics.clone(),
    )
}

fn actor(fixture: &RuntimeFixture, run_id: AgentRunId) -> TestActor {
    TestActor::with_clock_and_metrics(
        fixture.workflow.clone(),
        run_id,
        fixture.run_store.clone(),
        fixture.workflow_store.clone(),
        fixture.clock.clone(),
        fixture.metrics.clone(),
    )
}

/// Asks the actor one command and answers its reply.
async fn ask<R>(actor: &TestActorRef, build: impl FnOnce(ReplyTo<R>) -> AgentRunActorCommand) -> R
where
    R: Send + 'static,
{
    actor
        .ask(build, ASK_TIMEOUT)
        .await
        .expect("the actor replies")
}

fn at(millis: u64) -> AgentTimestampMillis {
    AgentTimestampMillis::new(millis)
}

/// The scripted effect-graph sequence, through the actor. Its last step
/// completes a node already completed, and fails.
async fn drive_the_actor(actor: &TestActorRef, fixture: &RuntimeFixture, run_id: &AgentRunId) {
    let plan = Arc::new(effect_graph_plan(&fixture.workflow));
    ask(actor, |reply_to| AgentRunActorCommand::Recover { reply_to })
        .await
        .expect("recover");
    ask(actor, |reply_to| AgentRunActorCommand::StartGraph {
        initial_state: graph_accepted_run_state(&fixture.workflow, run_id),
        plan: plan.clone(),
        now: at(100),
        reply_to,
    })
    .await
    .expect("start graph");
    ask(actor, |reply_to| AgentRunActorCommand::MarkGraphReady {
        plan: plan.clone(),
        now: at(110),
        reply_to,
    })
    .await
    .expect("mark ready");
    ask(actor, |reply_to| AgentRunActorCommand::StartGraphNode {
        plan: plan.clone(),
        node_id: node_id("input"),
        now: at(120),
        reply_to,
    })
    .await
    .expect("start node");
    ask(actor, |reply_to| AgentRunActorCommand::CompleteGraphNode {
        plan: plan.clone(),
        node_id: node_id("input"),
        now: at(130),
        reply_to,
    })
    .await
    .expect("complete input");
    ask(actor, |reply_to| AgentRunActorCommand::MarkGraphReady {
        plan: plan.clone(),
        now: at(140),
        reply_to,
    })
    .await
    .expect("mark ready again");
    ask(actor, |reply_to| AgentRunActorCommand::StartGraphNode {
        plan: plan.clone(),
        node_id: node_id("effect"),
        now: at(150),
        reply_to,
    })
    .await
    .expect("start effect");
    ask(actor, |reply_to| {
        AgentRunActorCommand::ScheduleGraphNodeEffect {
            plan: plan.clone(),
            request: effect_request(run_id.clone(), "effect", 160),
            reply_to,
        }
    })
    .await
    .expect("schedule effect");
    ask(actor, |reply_to| AgentRunActorCommand::DueEffects {
        reply_to,
    })
    .await
    .expect("due effects");
    ask(actor, |reply_to| AgentRunActorCommand::CompleteGraphNode {
        plan,
        node_id: node_id("input"),
        now: at(170),
        reply_to,
    })
    .await
    .expect_err("completing a completed node fails");
}

/// The same sequence, through a core.
async fn drive_the_core(core: &mut TestCore, fixture: &RuntimeFixture, run_id: &AgentRunId) {
    let plan = effect_graph_plan(&fixture.workflow);
    core.recover().await.expect("recover");
    core.start_graph(
        graph_accepted_run_state(&fixture.workflow, run_id),
        &plan,
        at(100),
    )
    .await
    .expect("start graph");
    core.mark_graph_ready(&plan, at(110))
        .await
        .expect("mark ready");
    core.start_graph_node(&plan, node_id("input"), at(120))
        .await
        .expect("start node");
    core.complete_graph_node(&plan, node_id("input"), at(130))
        .await
        .expect("complete input");
    core.mark_graph_ready(&plan, at(140))
        .await
        .expect("mark ready again");
    core.start_graph_node(&plan, node_id("effect"), at(150))
        .await
        .expect("start effect");
    core.schedule_graph_node_effect(&plan, effect_request(run_id.clone(), "effect", 160))
        .await
        .expect("schedule effect");
    core.due_effects().await.expect("due effects");
    core.complete_graph_node(&plan, node_id("input"), at(170))
        .await
        .expect_err("completing a completed node fails");
}

/// The core and the actor record the same snapshots. One scripted sequence,
/// run once through an actor and once through a core, each over its own
/// stores and its own registry, leaves the two registries alike: the same
/// runtime, recovery and outbox snapshots, and the last call's error under
/// the same phase.
#[tokio::test]
async fn the_core_and_the_actor_record_the_same_snapshots() {
    let system = ActorSystem::new("run-core-parity");
    let run_id = AgentRunId::new("run-core-parity");

    let through_actor = RuntimeFixture::new(workflow());
    let actor_registry = AgentWorkflowSnapshotRegistry::new();
    let actor_ref: TestActorRef = system
        .spawn(
            "parity-actor",
            actor(&through_actor, run_id.clone()).with_snapshot_registry(actor_registry.clone()),
        )
        .expect("the actor spawns");
    drive_the_actor(&actor_ref, &through_actor, &run_id).await;

    let through_core = RuntimeFixture::new(workflow());
    let core_registry = AgentWorkflowSnapshotRegistry::new();
    let mut run_core =
        core(&through_core, run_id.clone()).with_snapshot_registry(core_registry.clone());
    drive_the_core(&mut run_core, &through_core, &run_id).await;

    assert_eq!(
        actor_registry.runtime_snapshot(),
        core_registry.runtime_snapshot()
    );
    assert_eq!(
        actor_registry.recovery_snapshot(),
        core_registry.recovery_snapshot()
    );
    assert_eq!(
        actor_registry.outbox_snapshot(),
        core_registry.outbox_snapshot()
    );
    for registry in [&actor_registry, &core_registry] {
        let recovery = registry.recovery_snapshot();
        let errors = recovery.sampled_errors();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].phase(), "complete-graph-node");
    }

    system.terminate().await.expect("the system terminates");
}

/// A core's recovery re-links an effect that exhausted while it was down.
/// The first core drives the effect node to `Running`; a standalone inbox
/// then commits the node's effect and exhausts it, as a crash between the
/// outbox write and the graph's persist leaves it. A second core over the
/// same stores recovers and finds the node waiting on that effect.
#[tokio::test]
async fn a_core_recovery_relinks_an_effect_that_exhausted_while_it_was_down() {
    let fixture = RuntimeFixture::new(workflow());
    let run_id = AgentRunId::new("run-core-relink");
    let plan = effect_graph_plan(&fixture.workflow);
    let mut first = core(&fixture, run_id.clone());
    first.recover().await.expect("recover");
    first
        .start_graph(
            graph_accepted_run_state(&fixture.workflow, &run_id),
            &plan,
            at(100),
        )
        .await
        .expect("start graph");
    first.mark_graph_ready(&plan, at(110)).await.expect("ready");
    first
        .start_graph_node(&plan, node_id("input"), at(120))
        .await
        .expect("start input");
    first
        .complete_graph_node(&plan, node_id("input"), at(130))
        .await
        .expect("complete input");
    first.mark_graph_ready(&plan, at(140)).await.expect("ready");
    let running = first
        .start_graph_node(&plan, node_id("effect"), at(150))
        .await
        .expect("start effect");
    drop(first);

    let bridge = AgentGraphEffectBridge::new();
    let mut inbox = AgentRunInbox::with_clock(
        run_id.clone(),
        fixture.workflow_store.clone(),
        fixture.clock.clone(),
    );
    inbox
        .recover()
        .await
        .expect("the standalone inbox recovers");
    let scheduled = bridge
        .schedule_node_effect(
            &plan,
            running.graph_transition.state,
            effect_request(run_id.clone(), "effect", 160),
            &mut inbox,
        )
        .await
        .expect("the effect commits to the outbox");
    let effect_id = scheduled.effect.effect_id.clone();
    inbox
        .inner_mut()
        .record_outbox_exhausted(
            &OutboxMessageId::new(effect_id.as_str()),
            "effect-outcome-unknown",
        )
        .await
        .expect("the entry exhausts");
    drop(inbox);

    let mut second = core(&fixture, run_id);
    let recovered = second.recover().await.expect("the second core recovers");
    assert_eq!(recovered.due_effect_count, 0);
    let graph = recovered.graph.as_ref().expect("the graph recovers");
    assert_eq!(node_status(graph, "effect"), AgentGraphNodeStatus::Waiting);
    let effect_node = recovered
        .run_state
        .as_ref()
        .and_then(|state| state.graph_state.as_ref())
        .and_then(|graph| graph.node_states.get(&node_id("effect")))
        .expect("the effect node recovers");
    assert_eq!(effect_node.wait_reason, Some(AgentGraphWaitReason::Effect));
    assert!(effect_node.scheduled_effect_ids.contains(&effect_id));
}

/// A core re-created over the same stores recovers the run, as a passivated
/// or restarted entity's core must: the same run state, the same graph and
/// the same due effect.
#[tokio::test]
async fn a_core_recreated_over_the_same_stores_recovers_the_run() {
    let fixture = RuntimeFixture::new(workflow());
    let run_id = AgentRunId::new("run-core-recreated");
    let plan = effect_graph_plan(&fixture.workflow);
    let mut first = core(&fixture, run_id.clone());
    first.recover().await.expect("recover");
    first
        .start_graph(
            graph_accepted_run_state(&fixture.workflow, &run_id),
            &plan,
            at(100),
        )
        .await
        .expect("start graph");
    first.mark_graph_ready(&plan, at(110)).await.expect("ready");
    first
        .start_graph_node(&plan, node_id("input"), at(120))
        .await
        .expect("start input");
    first
        .complete_graph_node(&plan, node_id("input"), at(130))
        .await
        .expect("complete input");
    first.mark_graph_ready(&plan, at(140)).await.expect("ready");
    first
        .start_graph_node(&plan, node_id("effect"), at(150))
        .await
        .expect("start effect");
    first
        .schedule_graph_node_effect(&plan, effect_request(run_id.clone(), "effect", 160))
        .await
        .expect("schedule effect");
    let before: AgentRunActorSnapshot = first.snapshot().await.expect("snapshot");
    assert_eq!(before.due_effect_count, 1);
    drop(first);

    let mut second = core(&fixture, run_id);
    second.recover().await.expect("the second core recovers");
    let after = second.snapshot().await.expect("snapshot");
    assert_eq!(after.run_state, before.run_state);
    assert_eq!(after.graph, before.graph);
    assert_eq!(after.due_effect_count, 1);
}

/// An operation on a core never recovered is refused with the runner's
/// `not-recovered`, and writes nothing.
#[tokio::test]
async fn an_operation_on_a_core_never_recovered_is_refused() {
    let fixture = RuntimeFixture::new(workflow());
    let run_id = AgentRunId::new("run-core-never-recovered");
    let plan = effect_graph_plan(&fixture.workflow);
    let mut fresh = core(&fixture, run_id.clone());

    let refused = fresh
        .start_graph(
            graph_accepted_run_state(&fixture.workflow, &run_id),
            &plan,
            at(100),
        )
        .await
        .expect_err("a core never recovered refuses");

    assert_eq!(refused.code(), "not-recovered");
    let mut reader = core(&fixture, run_id);
    let recovered = reader.recover().await.expect("a reader recovers");
    assert!(recovered.run_state.is_none(), "nothing was written");
}

/// A failed refresh runs nothing and records no operation's phase. A core
/// over a store that loses one compare-and-set is stale; when its store then
/// fails every load, the next operation answers the load failure, the
/// registry records it under `recover` (the refresh's own recovery) and
/// nothing under the operation's phase, and the run record is unchanged.
#[tokio::test]
async fn a_failed_refresh_runs_nothing_and_records_no_operation_phase() {
    let fixture = RuntimeFixture::new(two_step_workflow());
    let run_id = AgentRunId::new("run-core-failed-refresh");
    let fail_loads = Arc::new(AtomicBool::new(false));
    let failing_store = FailingRunStore {
        inner: fixture.run_store.clone(),
        fail_loads: fail_loads.clone(),
    };
    let mut other = core(&fixture, run_id.clone());
    other.recover().await.expect("recover");
    other
        .start(accepted_run_state(
            &fixture.workflow,
            &run_id,
            AgentStepId::new("plan"),
        ))
        .await
        .expect("start");

    let registry = AgentWorkflowSnapshotRegistry::new();
    let mut stale = AgentRunCore::with_clock_and_metrics(
        fixture.workflow.clone(),
        run_id.clone(),
        failing_store,
        fixture.workflow_store.clone(),
        fixture.clock.clone(),
        fixture.metrics.clone(),
    )
    .with_snapshot_registry(registry.clone());
    stale.recover().await.expect("recover");
    other
        .begin_step(at(200))
        .await
        .expect("the other core begins");
    let lost = stale
        .begin_step(at(210))
        .await
        .expect_err("the stale core loses the compare-and-set");
    assert!(is_revision_conflict(&lost), "{lost:?}");
    let before = other.snapshot().await.expect("snapshot").run_state;

    fail_loads.store(true, Ordering::SeqCst);
    let failed = stale
        .succeed_step(advance_to_review(), at(300))
        .await
        .expect_err("the refresh fails while the store is down");

    assert!(is_store_failure(&failed), "{failed:?}");
    let recovery = registry.recovery_snapshot();
    let errors = recovery.sampled_errors();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].phase(), "recover");
    fail_loads.store(false, Ordering::SeqCst);
    let mut reader = core(&fixture, run_id);
    let after = reader.recover().await.expect("recover").run_state;
    assert_eq!(after, before, "nothing ran");
}

/// An actor hosts a core built elsewhere. The actor answers for the core's
/// run, and what it writes is the core's stores' to read.
#[tokio::test]
async fn an_actor_hosts_a_core_built_elsewhere() {
    let system = ActorSystem::new("run-core-from-core");
    let fixture = RuntimeFixture::new(workflow());
    let run_id = AgentRunId::new("run-core-from-core");
    let plan = Arc::new(effect_graph_plan(&fixture.workflow));
    let hosted: TestActorRef = system
        .spawn(
            "from-core",
            AgentRunActor::from_core(core(&fixture, run_id.clone())),
        )
        .expect("the actor spawns");

    let snapshot = ask(&hosted, |reply_to| AgentRunActorCommand::Snapshot {
        reply_to,
    })
    .await
    .expect("snapshot");
    assert_eq!(snapshot.run_id, run_id);
    ask(&hosted, |reply_to| AgentRunActorCommand::StartGraph {
        initial_state: graph_accepted_run_state(&fixture.workflow, &run_id),
        plan,
        now: at(100),
        reply_to,
    })
    .await
    .expect("start graph");

    let mut reader = core(&fixture, run_id);
    let recovered = reader.recover().await.expect("recover");
    assert_eq!(
        recovered.run_state.map(|state| state.status),
        Some(AgentRunStatus::Accepted)
    );
    assert!(
        recovered.graph.is_some(),
        "the graph the actor started is in the core's stores"
    );
    system.terminate().await.expect("the system terminates");
}

// ---------------------------------------------------------------------------
// Fixtures, as runtime_actor.rs has them.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct FailingRunStore {
    inner: RunStore,
    fail_loads: Arc<AtomicBool>,
}

impl DurableStateStore<AgentRunState> for FailingRunStore {
    fn backend_name(&self) -> &'static str {
        "failing-memory"
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<AgentRunState>>> {
        if self.fail_loads.load(Ordering::SeqCst) {
            return Box::pin(async {
                Err(DurableError::store(
                    "failing-memory",
                    "load failure injected",
                ))
            });
        }
        self.inner.load(persistence_id)
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: AgentRunState,
    ) -> StoreFuture<'a, StateRecord<AgentRunState>> {
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

fn is_revision_conflict(error: &AgentRunRuntimeError) -> bool {
    matches!(
        error,
        AgentRunRuntimeError::RunEngine {
            error: AgentRunEngineError::Persistence {
                error: DurableError::RevisionConflict { .. },
                ..
            },
        }
    )
}

fn is_store_failure(error: &AgentRunRuntimeError) -> bool {
    matches!(
        error,
        AgentRunRuntimeError::RunEngine {
            error: AgentRunEngineError::Persistence {
                error: DurableError::Store { .. },
                ..
            },
        }
    )
}

#[allow(dead_code)]
async fn wait_until_terminated<M>(actor: &ActorRef<M>)
where
    M: Message,
{
    for _ in 0..100 {
        if actor.is_terminated() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn advance_to_review() -> AgentStepSuccess {
    AgentStepSuccess::advance(AgentStepId::new("review"), inline_payload("ready"))
}

fn inline_payload(value: &str) -> AgentStatePayload {
    let bytes = value.as_bytes().to_vec();
    AgentStatePayload::Inline(InlineState {
        content_type: "text/plain".to_string(),
        size_bytes: bytes.len() as u64,
        bytes,
    })
}

fn workflow() -> AgentWorkflow {
    AgentWorkflow {
        workflow_id: AgentWorkflowId::new("workflow-runtime"),
        workflow_type: "runtime".to_string(),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        state_schema_version: StateSchemaVersion::new(1),
        display_name: Some("Runtime workflow".to_string()),
        status_labels: vec![
            AgentRunStatus::Accepted.as_label().to_string(),
            AgentRunStatus::Running.as_label().to_string(),
            AgentRunStatus::Completed.as_label().to_string(),
        ],
        command_types: vec![AgentCommandKind::StartRun.type_name().to_string()],
        steps: vec![AgentStep {
            step_id: AgentStepId::new("plan"),
            kind: AgentStepKind::Planner,
            display_name: Some("Plan".to_string()),
            next_step_ids: Vec::new(),
            timeout_ms: Some(1_000),
            config_ref: None,
            observability_labels: BTreeMap::new(),
        }],
        payload_types: vec![
            AgentPayloadDescriptor::new("runtime.input").content_type("application/json")
        ],
        retry_policy_ref: None,
        timeout_policy_ref: None,
        approval_policy_ref: None,
        observability_labels: BTreeMap::from([(
            "workflow_type".to_string(),
            "runtime".to_string(),
        )]),
    }
}

fn two_step_workflow() -> AgentWorkflow {
    let mut workflow = workflow();
    workflow.steps = vec![
        AgentStep {
            step_id: AgentStepId::new("plan"),
            kind: AgentStepKind::Planner,
            display_name: Some("Plan".to_string()),
            next_step_ids: vec![AgentStepId::new("review")],
            timeout_ms: Some(1_000),
            config_ref: None,
            observability_labels: BTreeMap::new(),
        },
        AgentStep {
            step_id: AgentStepId::new("review"),
            kind: AgentStepKind::Planner,
            display_name: Some("Review".to_string()),
            next_step_ids: Vec::new(),
            timeout_ms: Some(1_000),
            config_ref: None,
            observability_labels: BTreeMap::new(),
        },
    ];
    workflow
}

fn accepted_run_state(
    workflow: &AgentWorkflow,
    run_id: &AgentRunId,
    first_step_id: AgentStepId,
) -> AgentRunState {
    AgentRunState {
        current_step_id: Some(first_step_id),
        ..graph_accepted_run_state(workflow, run_id)
    }
}

fn graph_accepted_run_state(workflow: &AgentWorkflow, run_id: &AgentRunId) -> AgentRunState {
    AgentRunState {
        run_id: run_id.clone(),
        workflow_id: workflow.workflow_id.clone(),
        tenant: Some(AgentTenantId::new("tenant-runtime")),
        definition_version: workflow.definition_version.clone(),
        state_schema_version: workflow.state_schema_version,
        graph_state: None,
        status: AgentRunStatus::Accepted,
        current_step_id: None,
        current_attempt: 0,
        inputs_ref: None,
        state_payload: AgentStatePayload::Empty,
        checkpoints: Vec::new(),
        pending_effects: Vec::new(),
        pending_human_checkpoint: None,
        cancellation: None,
        created_at: AgentTimestampMillis::new(100),
        updated_at: AgentTimestampMillis::new(100),
        completed_at: None,
    }
}

fn effect_graph_plan(workflow: &AgentWorkflow) -> AgentCompiledExecutionPlan {
    let input = AgentCompiledPlanNode::new("input", AgentCompiledNodeKind::Input).output_port(
        AgentCompiledPlanPort::new("payload", AgentCompiledPortDirection::Output, "input"),
    );
    let effect = AgentCompiledPlanNode::new("effect", AgentCompiledNodeKind::ToolCall)
        .input_port(AgentCompiledPlanPort::new(
            "payload",
            AgentCompiledPortDirection::Input,
            "input",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "result",
            AgentCompiledPortDirection::Output,
            "effect-result",
        ))
        .target(
            AgentCompiledNodeTarget::new("tool", "runtime-tool")
                .address("tool://runtime-tool")
                .attribute("target_class", "tool"),
        );
    let terminal = AgentCompiledPlanNode::new("terminal", AgentCompiledNodeKind::Terminal)
        .input_port(AgentCompiledPlanPort::new(
            "result",
            AgentCompiledPortDirection::Input,
            "effect-result",
        ));

    AgentCompiledExecutionPlan::new(
        AgentCompiledPlanId::new("plan-runtime-effect-graph"),
        workflow.workflow_id.clone(),
        workflow.workflow_type.clone(),
        workflow.definition_version.clone(),
        CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
        AgentCompiledPlanFingerprint::new("sha256:runtime-effect-graph"),
    )
    .entry_node("input")
    .node(input)
    .node(effect)
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

fn effect_request(
    run_id: AgentRunId,
    node_id_value: &str,
    created_at_millis: u64,
) -> AgentGraphEffectScheduleRequest {
    AgentGraphEffectScheduleRequest::new(
        run_id,
        node_id_value,
        AgentTimestampMillis::new(created_at_millis),
        AgentCausationId::new("cause:runtime-graph"),
        AgentCorrelationId::new("corr:runtime-graph"),
    )
    .due_at(AgentTimestampMillis::new(100))
    .timeout_ms(1_000)
    .expected_result_type("runtime.effect.result")
}

fn node_id(value: &str) -> AgentCompiledNodeId {
    AgentCompiledNodeId::new(value)
}

fn node_status(graph: &AgentGraphRunProjection, node_id_value: &str) -> AgentGraphNodeStatus {
    graph
        .nodes
        .iter()
        .find(|node| node.node_id == node_id(node_id_value))
        .expect("the node is in the projection")
        .status
}

// ---------------------------------------------------------------------------
// Composed writes on the core's own runner and inbox (Task 2).
// ---------------------------------------------------------------------------

/// An outbox dispatcher that answers success and counts what it dispatched.
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

/// A recovered core driven to the `effect` node with its effect scheduled.
async fn a_core_with_its_effect_scheduled(
    fixture: &RuntimeFixture,
    run_id: &AgentRunId,
    registry: Option<AgentWorkflowSnapshotRegistry>,
) -> (TestCore, AgentCompiledExecutionPlan, AgentEffectId) {
    let plan = effect_graph_plan(&fixture.workflow);
    let mut run_core = core(fixture, run_id.clone());
    if let Some(registry) = registry {
        run_core = run_core.with_snapshot_registry(registry);
    }
    run_core.recover().await.expect("recover");
    run_core
        .start_graph(
            graph_accepted_run_state(&fixture.workflow, run_id),
            &plan,
            at(100),
        )
        .await
        .expect("start graph");
    run_core
        .mark_graph_ready(&plan, at(110))
        .await
        .expect("ready");
    run_core
        .start_graph_node(&plan, node_id("input"), at(120))
        .await
        .expect("start input");
    run_core
        .complete_graph_node(&plan, node_id("input"), at(130))
        .await
        .expect("complete input");
    run_core
        .mark_graph_ready(&plan, at(140))
        .await
        .expect("ready");
    run_core
        .start_graph_node(&plan, node_id("effect"), at(150))
        .await
        .expect("start effect");
    let scheduled = run_core
        .schedule_graph_node_effect(&plan, effect_request(run_id.clone(), "effect", 160))
        .await
        .expect("schedule effect");
    let effect_id = scheduled.effect_outcome.effect.effect_id.clone();
    (run_core, plan, effect_id)
}

/// The graph state the runner holds.
fn graph_of(runner: &AgentStepRunner<RunStore>) -> AgentGraphRunState {
    runner
        .state()
        .expect("the runner reads")
        .and_then(|state| state.graph_state.clone())
        .expect("the run has graph state")
}

fn command_metadata(
    workflow: &AgentWorkflow,
    run_id: &AgentRunId,
    command_id: &str,
    received_at_millis: u64,
) -> AgentCommandMetadata {
    AgentCommandMetadata::new(
        workflow.workflow_id.clone(),
        run_id.clone(),
        AgentCommandId::new(command_id),
        AgentDurabilityMetadata::new(
            AgentDeduplicationKey::new(format!("dedupe-{command_id}")),
            AgentCausationId::new(format!("cause-{command_id}")),
            AgentCorrelationId::new("correlation:workflow"),
        ),
        AgentTenantId::new("tenant-runtime"),
        AgentTimestampMillis::new(received_at_millis),
    )
    .expect("the command metadata validates")
}

fn start_command(workflow: &AgentWorkflow, run_id: &AgentRunId, command_id: &str) -> AgentCommand {
    AgentCommand::new(
        AgentCommandKind::StartRun,
        command_metadata(workflow, run_id, command_id, 100),
    )
    .expect("the start command validates")
}

fn artifact(artifact_id: &str) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact_id.to_string(),
        kind: ArtifactKind::ToolOutput,
        uri: format!("object://bucket/{artifact_id}"),
        checksum: Some("sha256:abc".to_string()),
        content_type: Some("application/json".to_string()),
        byte_len: Some(128),
        retention_class: Some("standard".to_string()),
        encryption: Some(
            ArtifactEncryptionRef::new("AES256-GCM", "kms://agent-workflow/test-key")
                .context("tenant", "tenant-runtime"),
        ),
        redaction: RedactionStatus::ReferenceOnly,
        created_at: AgentTimestampMillis::new(123),
        metadata: BTreeMap::new(),
    }
}

/// The host's composed writes need no recover. One core drives the effect
/// graph to its scheduled effect; then, on the core's own runner and inbox,
/// the owner dispatches the due outbox, accepts and applies the effect's
/// completion through the bridge, and persists the graph; then the core
/// completes the rest of the graph. No `recover` is called between any of
/// these, and every write lands.
#[tokio::test]
async fn the_hosts_composed_writes_need_no_recover() {
    let fixture = RuntimeFixture::new(workflow());
    let run_id = AgentRunId::new("run-core-composed");
    let (mut run_core, plan, effect_id) =
        a_core_with_its_effect_scheduled(&fixture, &run_id, None).await;
    let mut dispatcher = CountingDispatcher::default();

    let parts = run_core.parts_mut();
    parts
        .inbox
        .inner_mut()
        .dispatch_due_outbox(&mut dispatcher)
        .await
        .expect("the due outbox dispatches");
    let graph = graph_of(parts.runner);
    let command = parts
        .graph_runtime
        .effect_bridge()
        .effect_completed_command(
            command_metadata(&fixture.workflow, &run_id, "cmd-effect-completed", 200),
            effect_id,
            Some(artifact("artifact:effect-result")),
        )
        .expect("the completion command builds");
    let outcome = parts
        .graph_runtime
        .effect_bridge()
        .accept_and_apply_effect_completed(&plan, graph, command, parts.inbox)
        .await
        .expect("the completion applies");
    parts
        .runner
        .update_graph_state(outcome.transition.state, at(200))
        .await
        .expect("the graph persists");

    run_core
        .mark_graph_ready(&plan, at(210))
        .await
        .expect("ready");
    run_core
        .start_graph_node(&plan, node_id("terminal"), at(220))
        .await
        .expect("start terminal");
    run_core
        .complete_graph_node(&plan, node_id("terminal"), at(230))
        .await
        .expect("complete terminal");

    assert_eq!(dispatcher.dispatched, 1);
    let snapshot = run_core.snapshot().await.expect("snapshot");
    let graph = snapshot.graph.as_ref().expect("graph");
    for node in ["input", "effect"] {
        assert_eq!(
            node_status(graph, node),
            AgentGraphNodeStatus::Completed,
            "{node}"
        );
    }
    assert_eq!(
        node_status(graph, "terminal"),
        AgentGraphNodeStatus::Terminal
    );
    assert_eq!(snapshot.due_effect_count, 0);
}

/// The hazard the core removes: the same composed writes made through a
/// second inbox and runner over the same stores, each recovered just before
/// it writes as the host's second writer is, leave the core behind, and the
/// core's next write loses its compare-and-set. This is what the host's
/// `Recover` after every write exists for.
#[tokio::test]
async fn a_second_inbox_and_runner_lose_the_compare_and_set_the_core_does_not() {
    let fixture = RuntimeFixture::new(workflow());
    let run_id = AgentRunId::new("run-core-second-writer");
    let (mut run_core, plan, effect_id) =
        a_core_with_its_effect_scheduled(&fixture, &run_id, None).await;
    let mut dispatcher = CountingDispatcher::default();

    let mut inbox = AgentRunInbox::with_clock(
        run_id.clone(),
        fixture.workflow_store.clone(),
        fixture.clock.clone(),
    );
    inbox.recover().await.expect("the second inbox recovers");
    let mut runner = AgentStepRunner::new(
        fixture.workflow.clone(),
        run_id.clone(),
        fixture.run_store.clone(),
    );
    runner.recover().await.expect("the second runner recovers");
    inbox
        .inner_mut()
        .dispatch_due_outbox(&mut dispatcher)
        .await
        .expect("the due outbox dispatches");
    let bridge = AgentGraphEffectBridge::new();
    let command = bridge
        .effect_completed_command(
            command_metadata(&fixture.workflow, &run_id, "cmd-effect-completed", 200),
            effect_id,
            Some(artifact("artifact:effect-result")),
        )
        .expect("the completion command builds");
    let outcome = bridge
        .accept_and_apply_effect_completed(&plan, graph_of(&runner), command, &mut inbox)
        .await
        .expect("the completion applies");
    runner
        .update_graph_state(outcome.transition.state, at(200))
        .await
        .expect("the graph persists");

    let lost = run_core
        .mark_graph_ready(&plan, at(210))
        .await
        .expect_err("the core is behind the second runner");

    assert!(is_revision_conflict(&lost), "{lost:?}");
}

/// `parts_mut` hands out the core's own components: a command accepted
/// through it is in the core's inbox at once, with no recover.
#[tokio::test]
async fn parts_mut_hands_out_the_cores_own_components() {
    let fixture = RuntimeFixture::new(workflow());
    let run_id = AgentRunId::new("run-core-own-parts");
    let mut run_core = core(&fixture, run_id.clone());
    run_core.recover().await.expect("recover");
    let before = run_core
        .inbox()
        .inner()
        .recoverable_inbox()
        .expect("the inbox reads")
        .len();

    run_core
        .parts_mut()
        .inbox
        .accept_command(start_command(&fixture.workflow, &run_id, "cmd-start"))
        .await
        .expect("the command is accepted");

    let after = run_core
        .inbox()
        .inner()
        .recoverable_inbox()
        .expect("the inbox reads")
        .len();
    assert_eq!(after, before + 1);
    assert_eq!(
        run_core
            .snapshot()
            .await
            .expect("snapshot")
            .recoverable_command_count,
        after
    );
}

/// A composed write lost to another writer is refreshed by the next core
/// operation, on either store. Another writer moves the workflow store, or
/// the run store; the core's composed write on that component loses its
/// compare-and-set and leaves the component stale; the core's next
/// operation recovers it and runs against durable state.
#[tokio::test]
async fn a_composed_write_lost_to_another_writer_is_refreshed_by_the_next_operation() {
    // (a) The workflow store.
    let fixture = RuntimeFixture::new(workflow());
    let run_id = AgentRunId::new("run-core-lost-inbox-write");
    let (mut run_core, _plan, _effect) =
        a_core_with_its_effect_scheduled(&fixture, &run_id, None).await;
    let mut other = AgentRunInbox::with_clock(
        run_id.clone(),
        fixture.workflow_store.clone(),
        fixture.clock.clone(),
    );
    other.recover().await.expect("the other inbox recovers");
    other
        .accept_command(start_command(&fixture.workflow, &run_id, "cmd-other"))
        .await
        .expect("the other writer accepts");

    run_core
        .parts_mut()
        .inbox
        .accept_command(start_command(&fixture.workflow, &run_id, "cmd-mine"))
        .await
        .expect_err("the core's composed write loses");
    assert!(run_core.inbox().is_stale());

    let due = run_core
        .due_effects()
        .await
        .expect("the next operation runs");
    assert_eq!(due.len(), 1);
    assert!(!run_core.inbox().is_stale());

    // (b) The run store.
    let fixture = RuntimeFixture::new(workflow());
    let run_id = AgentRunId::new("run-core-lost-runner-write");
    let (mut run_core, _plan, _effect) =
        a_core_with_its_effect_scheduled(&fixture, &run_id, None).await;
    let mut runner = AgentStepRunner::new(
        fixture.workflow.clone(),
        run_id.clone(),
        fixture.run_store.clone(),
    );
    runner.recover().await.expect("the other runner recovers");
    let theirs = runner
        .update_graph_state(graph_of(&runner), at(300))
        .await
        .expect("the other writer persists");

    let graph = graph_of(run_core.runner());
    run_core
        .parts_mut()
        .runner
        .update_graph_state(graph, at(301))
        .await
        .expect_err("the core's composed write loses");
    assert!(run_core.runner().is_stale());

    let snapshot = run_core.snapshot().await.expect("the next operation runs");
    assert_eq!(snapshot.run_state, Some(theirs.state));
    assert!(!run_core.runner().is_stale());
}

/// `refresh_if_stale` recovers only a stale core. A core that has lost
/// nothing does not re-read: another writer's change stays unseen. Once a
/// composed write has lost its compare-and-set, the refresh re-reads.
#[tokio::test]
async fn refresh_if_stale_recovers_only_a_stale_core() {
    let fixture = RuntimeFixture::new(workflow());
    let run_id = AgentRunId::new("run-core-refresh");
    let (mut run_core, _plan, _effect) =
        a_core_with_its_effect_scheduled(&fixture, &run_id, None).await;
    let mut runner = AgentStepRunner::new(
        fixture.workflow.clone(),
        run_id.clone(),
        fixture.run_store.clone(),
    );
    runner.recover().await.expect("the other runner recovers");
    let theirs = runner
        .update_graph_state(graph_of(&runner), at(300))
        .await
        .expect("the other writer persists");
    let mine = run_core
        .runner()
        .state()
        .expect("the runner reads")
        .cloned();

    run_core
        .refresh_if_stale()
        .await
        .expect("a fresh core refreshes");
    assert_eq!(
        run_core.runner().state().expect("reads").cloned(),
        mine,
        "a core that lost nothing does not re-read"
    );

    let graph = graph_of(run_core.runner());
    run_core
        .parts_mut()
        .runner
        .update_graph_state(graph, at(301))
        .await
        .expect_err("the composed write loses");
    run_core
        .refresh_if_stale()
        .await
        .expect("a stale core refreshes");
    assert!(!run_core.runner().is_stale());
    assert_eq!(
        run_core.runner().state().expect("reads").cloned(),
        Some(theirs.state)
    );
}

/// A composed write is recorded by the next core operation. Dispatching the
/// due outbox through `parts_mut` changes nothing in the snapshot registry;
/// the owner's next core operation records the run as it then stands.
#[tokio::test]
async fn a_composed_write_is_recorded_by_the_next_core_operation() {
    let fixture = RuntimeFixture::new(workflow());
    let run_id = AgentRunId::new("run-core-recorded-later");
    let registry = AgentWorkflowSnapshotRegistry::new();
    let (mut run_core, _plan, _effect) =
        a_core_with_its_effect_scheduled(&fixture, &run_id, Some(registry.clone())).await;
    assert_eq!(registry.outbox_snapshot().due_effect_count(), 1);
    let mut dispatcher = CountingDispatcher::default();

    run_core
        .parts_mut()
        .inbox
        .inner_mut()
        .dispatch_due_outbox(&mut dispatcher)
        .await
        .expect("the due outbox dispatches");

    assert_eq!(dispatcher.dispatched, 1);
    assert_eq!(
        registry.outbox_snapshot().due_effect_count(),
        1,
        "a composed write is not recorded"
    );
    run_core.snapshot().await.expect("snapshot");
    assert_eq!(registry.outbox_snapshot().due_effect_count(), 0);
}
