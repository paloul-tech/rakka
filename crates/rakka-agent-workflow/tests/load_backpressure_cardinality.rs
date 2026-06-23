//! Load, back-pressure, and metric-cardinality coverage for agent workflows.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rakka_agent_workflow::{
    validate_agent_metric_attributes, AgentCausationId, AgentCommandKind, AgentCorrelationId,
    AgentDeduplicationKey, AgentDispatchConcurrencyLimits, AgentDispatchTargetClass,
    AgentDispatcherFleetSettings, AgentDispatcherFleetState, AgentDispatcherWorker,
    AgentDispatcherWorkerId, AgentDurabilityMetadata, AgentEffect, AgentEffectId, AgentEffectKind,
    AgentEffectMetadata, AgentEffectSchedule, AgentEffectTarget, AgentIdempotencyKey, AgentRunId,
    AgentRunInbox, AgentRunIndexEntry, AgentRunState, AgentRunStatus, AgentRunTransitionKind,
    AgentRunWaitReason, AgentRuntimeEventDraft, AgentRuntimeEventKind, AgentRuntimeEventProjection,
    AgentRuntimeEventSink, AgentStatePayload, AgentStep, AgentStepId, AgentStepKind,
    AgentStepRunner, AgentTelemetryContext, AgentTenantId, AgentTimerEntry, AgentTimerId,
    AgentTimerIndexEntry, AgentTimerPolicy, AgentTimerQuery, AgentTimerScanner,
    AgentTimerScannerSettings, AgentTimerStatus, AgentTimerStore, AgentTimerStoreState,
    AgentTimestampMillis, AgentWorkflow, AgentWorkflowId, AgentWorkflowQueryIndex,
    AgentWorkflowRunQuery, HumanCheckpoint, HumanCheckpointId, HumanCheckpointStatus,
    HumanDecisionOption, InMemoryAgentRuntimeEventSink, InMemoryAgentWorkflowQueryIndex,
    PrincipalRef, StateSchemaVersion, WorkflowDefinitionVersion, FORBIDDEN_HOT_METRIC_FIELDS,
    METRIC_AGENT_DISPATCHER_BACKLOG, METRIC_AGENT_DISPATCHER_FLEET,
    METRIC_AGENT_DISPATCHER_IN_FLIGHT, METRIC_AGENT_INBOX_COMMANDS, METRIC_AGENT_TIMERS,
    METRIC_AGENT_TIMERS_LATE_BY_MS,
};
use rakka_agent_workflow::{
    AgentCompiledEdgeMergeBehavior, AgentCompiledExecutionPlan, AgentCompiledNodeId,
    AgentCompiledNodeKind, AgentCompiledNodeTarget, AgentCompiledPlanEdge,
    AgentCompiledPlanFingerprint, AgentCompiledPlanId, AgentCompiledPlanNode,
    AgentCompiledPlanPort, AgentCompiledPortDirection, AgentGraphNodeStatus,
    AgentGraphRunProjection, AgentGraphRunState, AgentGraphScheduler, AgentGraphWaitReason,
    CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
};
use rakka_core::{InMemoryMetricsRecorder, MetricKind, MetricsSnapshot};
use rakka_persistence::InMemoryDurableStateStore;
use rakka_workflow::{ManualWorkflowClock, WorkflowState, WorkflowTimestamp};

type RunStore = InMemoryDurableStateStore<AgentRunState>;
type WorkflowStore = InMemoryDurableStateStore<WorkflowState>;
type FleetStore = InMemoryDurableStateStore<AgentDispatcherFleetState>;
type TimerStoreBackend = InMemoryDurableStateStore<AgentTimerStoreState>;
type TestWorker = AgentDispatcherWorker<FleetStore, WorkflowStore, ManualWorkflowClock>;

const DISPATCH_RUN_COUNT: usize = 64;
const DISPATCH_TARGET_LIMIT: usize = 5;
const TIMER_RUN_COUNT: usize = 36;
const TIMER_BATCH_SIZE: usize = 7;
const QUERY_RUN_COUNT: usize = 128;
const LARGE_LINEAR_GRAPH_STEPS: usize = 80;
const WIDE_GRAPH_LEAF_COUNT: usize = 48;
const WIDE_GRAPH_JOIN_WIDTH: usize = 6;
const WAITING_GRAPH_NODE_COUNT: usize = 72;
const RUNTIME_EVENT_COUNT: usize = 128;

#[tokio::test]
async fn dispatcher_load_claims_bounded_work_and_keeps_metric_series_bounded() {
    let workflow_store = WorkflowStore::new();
    let fleet_store = FleetStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(1_000));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());

    for index in 0..DISPATCH_RUN_COUNT {
        let run_id = AgentRunId::new(format!("run-load-dispatch-{index:03}"));
        schedule_effect(
            &workflow_store,
            &clock,
            run_id,
            effect(
                format!("effect-load-dispatch-{index:03}"),
                AgentEffectKind::ModelCall,
                "model",
                "shared-chat-model",
                1_000,
            ),
        )
        .await;
    }

    let limits = AgentDispatchConcurrencyLimits::new(DISPATCH_RUN_COUNT).target_limit(
        AgentDispatchTargetClass::Model,
        "shared-chat-model",
        DISPATCH_TARGET_LIMIT,
    );
    let settings = AgentDispatcherFleetSettings::new(16, 30_000).concurrency_limits(limits);
    let mut worker = worker(
        "dispatcher-load",
        fleet_store,
        workflow_store,
        clock,
        metrics.clone(),
        settings,
    );
    worker.recover().await.expect("fleet should recover");
    for index in 0..DISPATCH_RUN_COUNT {
        worker
            .refresh_run(
                AgentRunId::new(format!("run-load-dispatch-{index:03}")),
                None,
            )
            .await
            .expect("due effect should be indexed");
    }

    let batch = worker
        .claim_due()
        .await
        .expect("claim pass should apply target limit");
    assert_eq!(batch.due_dispatch_count, DISPATCH_RUN_COUNT);
    assert_eq!(batch.claims.len(), DISPATCH_TARGET_LIMIT);
    assert_eq!(
        batch.concurrency_limited,
        DISPATCH_RUN_COUNT - DISPATCH_TARGET_LIMIT
    );
    assert!(batch.backpressure_limited);

    let snapshot = worker.fleet().snapshot(3);
    assert_eq!(snapshot.observed_dispatch_count, DISPATCH_RUN_COUNT);
    assert_eq!(snapshot.in_flight_count, DISPATCH_TARGET_LIMIT);
    assert_eq!(
        snapshot.due_dispatch_count,
        DISPATCH_RUN_COUNT - DISPATCH_TARGET_LIMIT
    );
    assert_eq!(snapshot.sampled_entries.len(), 3);
    let encoded = serde_json::to_vec(&snapshot).expect("snapshot should serialize");
    assert!(
        encoded.len() < 4_096,
        "bounded dispatcher snapshot should stay compact under load: {} bytes",
        encoded.len()
    );

    let snapshot = metrics.snapshot();
    assert_all_metric_attributes_are_bounded(&snapshot);
    assert!(metric_series_count(&snapshot, METRIC_AGENT_DISPATCHER_FLEET) <= 2);
    assert!(metric_series_count(&snapshot, METRIC_AGENT_DISPATCHER_IN_FLIGHT) <= 1);
    assert!(metric_series_count(&snapshot, METRIC_AGENT_DISPATCHER_BACKLOG) <= 1);
}

#[tokio::test]
async fn timer_backlog_load_uses_batch_limit_and_bounded_metrics() {
    let workflow = workflow();
    let run_store = RunStore::new();
    let workflow_store = WorkflowStore::new();
    let timer_store = TimerStoreBackend::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(1_000));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());

    for index in 0..TIMER_RUN_COUNT {
        let run_id = AgentRunId::new(format!("run-load-timer-{index:03}"));
        put_run_in_timer_wait(&workflow, &run_store, &run_id).await;
    }

    let mut timers = AgentTimerStore::new(timer_store);
    timers
        .recover(ts(100))
        .await
        .expect("timer store should recover");
    for index in 0..TIMER_RUN_COUNT {
        let run_id = AgentRunId::new(format!("run-load-timer-{index:03}"));
        timers
            .schedule_timer(timer(
                &workflow,
                &run_id,
                &AgentTimerId::new(format!("timer-load-{index:03}")),
                500 + index as u64,
            ))
            .await
            .expect("timer should schedule");
    }

    let mut scanner = AgentTimerScanner::with_clock_and_metrics(
        workflow,
        timers,
        workflow_store,
        run_store,
        clock,
        AgentTimerScannerSettings::new(TIMER_BATCH_SIZE),
        metrics.clone(),
    );
    let scan = scanner.scan_due().await.expect("bounded scan should run");
    assert_eq!(scan.due_timer_count, TIMER_RUN_COUNT);
    assert_eq!(scan.max_batch_size, TIMER_BATCH_SIZE);
    assert_eq!(scan.fired.len(), TIMER_BATCH_SIZE);
    assert!(scan.backpressure_limited);
    assert!(scan
        .fired
        .iter()
        .all(|firing| firing.inbox_acceptance.is_accepted()));
    assert!(scan.fired.iter().all(|firing| {
        firing
            .transition
            .as_ref()
            .is_some_and(|transition| transition.kind == AgentRunTransitionKind::Resume)
    }));

    let remaining = scanner
        .timers_mut()
        .due_timer_count(ts(1_000))
        .await
        .expect("remaining due timers should count");
    assert_eq!(remaining, TIMER_RUN_COUNT - TIMER_BATCH_SIZE);

    let snapshot = metrics.snapshot();
    assert_all_metric_attributes_are_bounded(&snapshot);
    assert!(metric_series_count(&snapshot, METRIC_AGENT_TIMERS) <= 1);
    assert!(metric_series_count(&snapshot, METRIC_AGENT_TIMERS_LATE_BY_MS) <= 1);
    assert!(metric_series_count(&snapshot, METRIC_AGENT_INBOX_COMMANDS) <= 1);
}

#[tokio::test]
async fn query_views_stay_bounded_under_large_run_counts() {
    let mut index = InMemoryAgentWorkflowQueryIndex::new();

    for run_number in 0..QUERY_RUN_COUNT {
        let status = match run_number % 4 {
            0 => AgentRunStatus::Running,
            1 => AgentRunStatus::WaitingForTimer,
            2 => AgentRunStatus::WaitingForHuman,
            _ => AgentRunStatus::Failed,
        };
        let checkpoint = (status == AgentRunStatus::WaitingForHuman).then(|| {
            checkpoint(
                format!("checkpoint-load-{run_number:03}"),
                run_number as u64,
            )
        });
        let run = run_state(
            format!("run-query-load-{run_number:03}"),
            status,
            100 + run_number as u64,
            checkpoint,
        );
        index
            .upsert_run(AgentRunIndexEntry::from_run_state(&run, "load-test").namespace("prod"))
            .await
            .expect("run should index");

        if status == AgentRunStatus::WaitingForTimer {
            index
                .upsert_timer(
                    AgentTimerIndexEntry::from_timer_entry(&timer(
                        &workflow(),
                        &run.run_id,
                        &AgentTimerId::new(format!("timer-query-load-{run_number:03}")),
                        600 + run_number as u64,
                    ))
                    .namespace("prod"),
                )
                .await
                .expect("timer should index");
        }
    }

    let waiting = index
        .query_runs(AgentWorkflowRunQuery::new().waiting().limit(13))
        .await
        .expect("waiting query should run");
    assert_eq!(waiting.len(), 13);
    assert!(waiting.iter().all(|entry| matches!(
        entry.status,
        AgentRunStatus::WaitingForTimer | AgentRunStatus::WaitingForHuman
    )));

    let due_timer_runs = index
        .query_runs(
            AgentWorkflowRunQuery::new()
                .due_timer_at_or_before(ts(700))
                .limit(11),
        )
        .await
        .expect("due timer run query should run");
    assert_eq!(due_timer_runs.len(), 11);
    assert!(due_timer_runs
        .iter()
        .all(|entry| entry.status == AgentRunStatus::WaitingForTimer));

    let due_timers = index
        .query_timers(
            AgentTimerQuery::new()
                .status(AgentTimerStatus::Pending)
                .due_at_or_before(ts(700))
                .limit(17),
        )
        .await
        .expect("timer query should run");
    assert_eq!(due_timers.len(), 17);
    assert!(due_timers
        .windows(2)
        .all(|window| window[0].due_at <= window[1].due_at));

    let too_wide = index
        .query_runs(AgentWorkflowRunQuery::new().limit(0))
        .await
        .expect_err("zero limit should be rejected");
    assert_eq!(too_wide.code(), "invalid-workflow-query");
}

#[test]
fn graph_scheduler_load_runs_large_linear_graph_deterministically() {
    let scheduler = AgentGraphScheduler::new();
    let plan = large_linear_graph_plan(LARGE_LINEAR_GRAPH_STEPS);
    let mut state = scheduler
        .initialize_state(&plan, ts(100))
        .expect("large linear graph should initialize");

    let mut expected_order = Vec::with_capacity(LARGE_LINEAR_GRAPH_STEPS + 2);
    expected_order.push("input".to_string());
    for index in 0..LARGE_LINEAR_GRAPH_STEPS {
        expected_order.push(format!("step-{index:03}"));
    }
    expected_order.push("terminal".to_string());

    for (index, node_id) in expected_order.iter().enumerate() {
        let transition = scheduler
            .mark_ready_nodes_runnable(&plan, state, ts(1_000 + index as u64 * 10))
            .expect("next linear node should become runnable");
        assert_eq!(
            compiled_node_ids(&transition.changed_node_ids),
            vec![node_id.clone()]
        );
        assert_eq!(
            compiled_node_ids(&transition.runnable_node_ids),
            vec![node_id.clone()]
        );
        state = start_and_complete_graph_node(
            &scheduler,
            &plan,
            transition.state,
            node_id,
            ts(1_001 + index as u64 * 10),
        );
    }

    assert_eq!(state.node_states.len(), LARGE_LINEAR_GRAPH_STEPS + 2);
    assert!(state.terminal_status.is_some());
    assert_eq!(
        graph_node_status(&state, "terminal"),
        AgentGraphNodeStatus::Terminal
    );
}

#[test]
fn graph_scheduler_load_handles_wide_fan_out_and_many_joins_deterministically() {
    let scheduler = AgentGraphScheduler::new();
    let plan = wide_join_graph_plan(WIDE_GRAPH_LEAF_COUNT, WIDE_GRAPH_JOIN_WIDTH);
    let mut state = scheduler
        .initialize_state(&plan, ts(100))
        .expect("wide join graph should initialize");

    state = run_next_single_ready_node(&scheduler, &plan, state, "input", 110);

    let leaves = numbered_ids("leaf", WIDE_GRAPH_LEAF_COUNT);
    let transition = scheduler
        .mark_ready_nodes_runnable(&plan, state, ts(200))
        .expect("fan-out leaves should become runnable");
    assert_eq!(compiled_node_ids(&transition.changed_node_ids), leaves);
    state = transition.state;

    for index in 0..WIDE_GRAPH_LEAF_COUNT {
        state = start_and_complete_graph_node(
            &scheduler,
            &plan,
            state,
            &format!("leaf-{index:03}"),
            ts(210 + index as u64),
        );
    }

    let join_count = WIDE_GRAPH_LEAF_COUNT / WIDE_GRAPH_JOIN_WIDTH;
    let joins = numbered_ids("join", join_count);
    let transition = scheduler
        .mark_ready_nodes_runnable(&plan, state, ts(300))
        .expect("group joins should become runnable");
    assert_eq!(compiled_node_ids(&transition.changed_node_ids), joins);
    state = transition.state;

    for index in 0..join_count {
        state = start_and_complete_graph_node(
            &scheduler,
            &plan,
            state,
            &format!("join-{index:03}"),
            ts(310 + index as u64),
        );
    }

    state = run_next_single_ready_node(&scheduler, &plan, state, "join-final", 400);
    state = run_next_single_ready_node(&scheduler, &plan, state, "terminal", 500);

    let projection = AgentGraphRunProjection::from_graph_state(&state);
    assert_eq!(
        projection.node_count,
        WIDE_GRAPH_LEAF_COUNT + join_count + 3
    );
    assert_eq!(projection.failed_node_count, 0);
    assert_eq!(projection.waiting_node_count, 0);
    assert!(state.terminal_status.is_some());
}

#[test]
fn graph_scheduler_load_tracks_many_waiting_nodes() {
    let scheduler = AgentGraphScheduler::new();
    let plan = wide_waiting_graph_plan(WAITING_GRAPH_NODE_COUNT);
    let mut state = scheduler
        .initialize_state(&plan, ts(100))
        .expect("waiting graph should initialize");

    state = run_next_single_ready_node(&scheduler, &plan, state, "input", 110);
    let waiting_nodes = numbered_ids("wait", WAITING_GRAPH_NODE_COUNT);
    let transition = scheduler
        .mark_ready_nodes_runnable(&plan, state, ts(200))
        .expect("waiting nodes should become runnable");
    assert_eq!(
        compiled_node_ids(&transition.changed_node_ids),
        waiting_nodes
    );
    state = transition.state;

    for index in 0..WAITING_GRAPH_NODE_COUNT {
        let node_id = format!("wait-{index:03}");
        state = scheduler
            .start_node(&plan, state, node_id.as_str(), ts(210 + index as u64 * 2))
            .expect("waiting node should start")
            .state;
        let reason = match index % 3 {
            0 => AgentGraphWaitReason::Effect,
            1 => AgentGraphWaitReason::Timer,
            _ => AgentGraphWaitReason::Human,
        };
        state = scheduler
            .wait_node(&plan, state, node_id, reason, ts(211 + index as u64 * 2))
            .expect("running node should enter durable wait")
            .state;
    }

    let projection = AgentGraphRunProjection::from_graph_state(&state);
    assert_eq!(projection.waiting_node_count, WAITING_GRAPH_NODE_COUNT);
    assert_eq!(projection.runnable_node_count, 0);
    assert_eq!(projection.running_node_count, 0);
    assert_eq!(projection.failed_node_count, 0);
    assert_eq!(
        projection
            .nodes
            .iter()
            .filter(|node| node.wait_reason == Some(AgentGraphWaitReason::Effect))
            .count(),
        WAITING_GRAPH_NODE_COUNT / 3
    );
    let encoded = serde_json::to_vec(&projection).expect("projection should serialize");
    assert!(
        encoded.len() < 32_768,
        "bounded graph projection should stay compact under waiting-node load: {} bytes",
        encoded.len()
    );
}

#[tokio::test]
async fn runtime_event_load_preserves_order_and_rejects_cardinality_leaks() {
    let mut sink = InMemoryAgentRuntimeEventSink::new();
    let mut graph = AgentGraphRunState::new(
        AgentCompiledPlanId::new("plan-runtime-event-load"),
        AgentCompiledPlanFingerprint::new("sha256:runtime-event-load"),
    );
    let mut recorded_events = Vec::with_capacity(RUNTIME_EVENT_COUNT);

    for index in 0..RUNTIME_EVENT_COUNT {
        graph.scheduler_revision = index as u64 + 1;
        let kind = if index == 0 {
            AgentRuntimeEventKind::RunStarted
        } else if index == RUNTIME_EVENT_COUNT - 1 {
            AgentRuntimeEventKind::RunCompleted
        } else {
            AgentRuntimeEventKind::NodeCompleted
        };
        let mut draft = AgentRuntimeEventDraft::new(
            AgentWorkflowId::new("workflow-runtime-event-load"),
            AgentRunId::new("run-runtime-event-load"),
            WorkflowDefinitionVersion::new("v1"),
            ts(1_000 + index as u64),
            kind,
            AgentCausationId::new("cause:runtime-event-load"),
            AgentCorrelationId::new("corr:runtime-event-load"),
            AgentTelemetryContext::default(),
        );
        if kind == AgentRuntimeEventKind::NodeCompleted {
            draft = draft
                .node_id(AgentCompiledNodeId::new(format!("node-{index:03}")))
                .attribute("status", "completed");
        }
        let event = draft
            .after_persistence(Some(&graph))
            .expect("event draft should validate")
            .expect("persisted graph should produce event");
        graph.last_event_sequence = event.event_sequence;
        sink.record_runtime_event(event.clone())
            .await
            .expect("event should record");
        recorded_events.push(event);
    }

    let stored = sink
        .runtime_events_for_run(AgentRunId::new("run-runtime-event-load"))
        .await
        .expect("events should query in sequence order");
    assert_eq!(stored, recorded_events);
    assert!(stored
        .windows(2)
        .all(|window| window[0].event_sequence + 1 == window[1].event_sequence));

    let projection =
        AgentRuntimeEventProjection::from_events(&stored).expect("event projection should rebuild");
    assert_eq!(projection.event_count, RUNTIME_EVENT_COUNT as u64);
    assert_eq!(
        projection.node_event_count,
        (RUNTIME_EVENT_COUNT - 2) as u64
    );
    assert_eq!(
        projection.terminal_event_kind,
        Some(AgentRuntimeEventKind::RunCompleted)
    );

    assert!(validate_agent_metric_attributes(&[("status", "completed")]).is_ok());
    assert!(validate_agent_metric_attributes(&[("node_id", "node-001")]).is_err());
    assert!(validate_agent_metric_attributes(&[("run_id", "run-runtime-event-load")]).is_err());
}

fn large_linear_graph_plan(step_count: usize) -> AgentCompiledExecutionPlan {
    let mut plan = AgentCompiledExecutionPlan::new(
        AgentCompiledPlanId::new("plan-load-large-linear"),
        AgentWorkflowId::new("workflow-load-graph"),
        "load-graph",
        WorkflowDefinitionVersion::new("v1"),
        CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
        AgentCompiledPlanFingerprint::new("sha256:load-large-linear"),
    )
    .entry_node("input")
    .node(input_node());

    let mut previous_node = "input".to_string();
    let mut previous_port = "out".to_string();
    for index in 0..step_count {
        let node_id = format!("step-{index:03}");
        plan = plan.node(transform_node(&node_id));
        plan = plan.edge(AgentCompiledPlanEdge::new(
            format!("edge-{previous_node}-{node_id}"),
            previous_node.as_str(),
            previous_port.as_str(),
            node_id.as_str(),
            "in",
        ));
        previous_node = node_id;
        previous_port = "out".to_string();
    }

    plan.node(terminal_node()).edge(AgentCompiledPlanEdge::new(
        "edge-last-terminal",
        previous_node,
        previous_port,
        "terminal",
        "in",
    ))
}

fn wide_join_graph_plan(leaf_count: usize, join_width: usize) -> AgentCompiledExecutionPlan {
    assert_eq!(leaf_count % join_width, 0);
    let join_count = leaf_count / join_width;
    let mut plan = AgentCompiledExecutionPlan::new(
        AgentCompiledPlanId::new("plan-load-wide-joins"),
        AgentWorkflowId::new("workflow-load-graph"),
        "load-graph",
        WorkflowDefinitionVersion::new("v1"),
        CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
        AgentCompiledPlanFingerprint::new("sha256:load-wide-joins"),
    )
    .entry_node("input")
    .node(input_node());

    for index in 0..leaf_count {
        let leaf_id = format!("leaf-{index:03}");
        plan = plan.node(transform_node(&leaf_id));
        plan = plan.edge(AgentCompiledPlanEdge::new(
            format!("edge-input-{leaf_id}"),
            "input",
            "out",
            leaf_id.as_str(),
            "in",
        ));
    }

    for join_index in 0..join_count {
        let join_id = format!("join-{join_index:03}");
        let mut join =
            AgentCompiledPlanNode::new(join_id.as_str(), AgentCompiledNodeKind::Join).output_port(
                AgentCompiledPlanPort::new("out", AgentCompiledPortDirection::Output, "payload"),
            );
        for offset in 0..join_width {
            let leaf_index = join_index * join_width + offset;
            let port_id = format!("in-{offset:03}");
            join = join.input_port(AgentCompiledPlanPort::new(
                port_id.as_str(),
                AgentCompiledPortDirection::Input,
                "payload",
            ));
            plan = plan.edge(
                AgentCompiledPlanEdge::new(
                    format!("edge-leaf-{leaf_index:03}-{join_id}"),
                    format!("leaf-{leaf_index:03}"),
                    "out",
                    join_id.as_str(),
                    port_id,
                )
                .merge_behavior(AgentCompiledEdgeMergeBehavior::WaitForAll),
            );
        }
        plan = plan.node(join);
    }

    let mut final_join =
        AgentCompiledPlanNode::new("join-final", AgentCompiledNodeKind::Join).output_port(
            AgentCompiledPlanPort::new("out", AgentCompiledPortDirection::Output, "payload"),
        );
    for index in 0..join_count {
        let input_port = format!("group-{index:03}");
        final_join = final_join.input_port(AgentCompiledPlanPort::new(
            input_port.as_str(),
            AgentCompiledPortDirection::Input,
            "payload",
        ));
        plan = plan.edge(
            AgentCompiledPlanEdge::new(
                format!("edge-join-{index:03}-final"),
                format!("join-{index:03}"),
                "out",
                "join-final",
                input_port,
            )
            .merge_behavior(AgentCompiledEdgeMergeBehavior::WaitForAll),
        );
    }

    plan.node(final_join)
        .node(terminal_node())
        .edge(AgentCompiledPlanEdge::new(
            "edge-final-terminal",
            "join-final",
            "out",
            "terminal",
            "in",
        ))
}

fn wide_waiting_graph_plan(waiting_count: usize) -> AgentCompiledExecutionPlan {
    let mut plan = AgentCompiledExecutionPlan::new(
        AgentCompiledPlanId::new("plan-load-many-waiting"),
        AgentWorkflowId::new("workflow-load-graph"),
        "load-graph",
        WorkflowDefinitionVersion::new("v1"),
        CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
        AgentCompiledPlanFingerprint::new("sha256:load-many-waiting"),
    )
    .entry_node("input")
    .node(input_node());

    let mut join =
        AgentCompiledPlanNode::new("join-waits", AgentCompiledNodeKind::Join).output_port(
            AgentCompiledPlanPort::new("out", AgentCompiledPortDirection::Output, "payload"),
        );
    for index in 0..waiting_count {
        let wait_id = format!("wait-{index:03}");
        plan = plan.node(waiting_effect_node(&wait_id));
        plan = plan.edge(AgentCompiledPlanEdge::new(
            format!("edge-input-{wait_id}"),
            "input",
            "out",
            wait_id.as_str(),
            "in",
        ));
        let join_port = format!("in-{index:03}");
        join = join.input_port(AgentCompiledPlanPort::new(
            join_port.as_str(),
            AgentCompiledPortDirection::Input,
            "payload",
        ));
        plan = plan.edge(
            AgentCompiledPlanEdge::new(
                format!("edge-{wait_id}-join"),
                wait_id,
                "out",
                "join-waits",
                join_port,
            )
            .merge_behavior(AgentCompiledEdgeMergeBehavior::WaitForAll),
        );
    }

    plan.node(join)
        .node(terminal_node())
        .edge(AgentCompiledPlanEdge::new(
            "edge-wait-join-terminal",
            "join-waits",
            "out",
            "terminal",
            "in",
        ))
}

fn input_node() -> AgentCompiledPlanNode {
    AgentCompiledPlanNode::new("input", AgentCompiledNodeKind::Input).output_port(
        AgentCompiledPlanPort::new("out", AgentCompiledPortDirection::Output, "payload"),
    )
}

fn transform_node(node_id: &str) -> AgentCompiledPlanNode {
    AgentCompiledPlanNode::new(node_id, AgentCompiledNodeKind::Transform)
        .input_port(AgentCompiledPlanPort::new(
            "in",
            AgentCompiledPortDirection::Input,
            "payload",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "out",
            AgentCompiledPortDirection::Output,
            "payload",
        ))
}

fn waiting_effect_node(node_id: &str) -> AgentCompiledPlanNode {
    AgentCompiledPlanNode::new(node_id, AgentCompiledNodeKind::ToolCall)
        .input_port(AgentCompiledPlanPort::new(
            "in",
            AgentCompiledPortDirection::Input,
            "payload",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "out",
            AgentCompiledPortDirection::Output,
            "payload",
        ))
        .target(
            AgentCompiledNodeTarget::new("tool", "load-waiting-tool")
                .address("tool://load-waiting-tool")
                .attribute("target_class", "tool"),
        )
}

fn terminal_node() -> AgentCompiledPlanNode {
    AgentCompiledPlanNode::new("terminal", AgentCompiledNodeKind::Terminal).input_port(
        AgentCompiledPlanPort::new("in", AgentCompiledPortDirection::Input, "payload"),
    )
}

fn run_next_single_ready_node(
    scheduler: &AgentGraphScheduler,
    plan: &AgentCompiledExecutionPlan,
    state: AgentGraphRunState,
    expected_node_id: &str,
    timestamp_millis: u64,
) -> AgentGraphRunState {
    let transition = scheduler
        .mark_ready_nodes_runnable(plan, state, ts(timestamp_millis))
        .expect("exactly one node should become runnable");
    assert_eq!(
        compiled_node_ids(&transition.changed_node_ids),
        vec![expected_node_id.to_string()]
    );
    start_and_complete_graph_node(
        scheduler,
        plan,
        transition.state,
        expected_node_id,
        ts(timestamp_millis + 1),
    )
}

fn start_and_complete_graph_node(
    scheduler: &AgentGraphScheduler,
    plan: &AgentCompiledExecutionPlan,
    state: AgentGraphRunState,
    node_id: &str,
    timestamp: AgentTimestampMillis,
) -> AgentGraphRunState {
    let state = scheduler
        .start_node(plan, state, node_id, timestamp)
        .expect("graph node should start")
        .state;
    scheduler
        .complete_node(
            plan,
            state,
            node_id,
            AgentTimestampMillis::new(timestamp.as_millis() + 1),
        )
        .expect("graph node should complete")
        .state
}

fn compiled_node_ids(node_ids: &[AgentCompiledNodeId]) -> Vec<String> {
    node_ids
        .iter()
        .map(|node_id| node_id.as_str().to_string())
        .collect()
}

fn numbered_ids(prefix: &str, count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("{prefix}-{index:03}"))
        .collect()
}

fn graph_node_status(state: &AgentGraphRunState, node_id: &str) -> AgentGraphNodeStatus {
    state
        .node_states
        .get(&AgentCompiledNodeId::new(node_id))
        .expect("graph node should exist")
        .status
}

fn worker(
    worker_id: &str,
    fleet_store: FleetStore,
    workflow_store: WorkflowStore,
    clock: ManualWorkflowClock,
    metrics: Arc<InMemoryMetricsRecorder>,
    settings: AgentDispatcherFleetSettings,
) -> TestWorker {
    AgentDispatcherWorker::with_clock_and_metrics(
        AgentDispatcherWorkerId::new(worker_id),
        fleet_store,
        workflow_store,
        settings,
        clock,
        metrics,
    )
}

async fn schedule_effect(
    store: &WorkflowStore,
    clock: &ManualWorkflowClock,
    run_id: AgentRunId,
    effect: AgentEffect,
) {
    let mut inbox = AgentRunInbox::with_clock(run_id, store.clone(), clock.clone());
    inbox.recover().await.expect("inbox should recover");
    inbox
        .schedule_effect(effect)
        .await
        .expect("effect should schedule");
}

async fn put_run_in_timer_wait(workflow: &AgentWorkflow, store: &RunStore, run_id: &AgentRunId) {
    let mut runner = AgentStepRunner::new(workflow.clone(), run_id.clone(), store.clone());
    runner.recover().await.expect("runner should recover");
    runner
        .start(accepted_run_state(
            workflow,
            run_id,
            AgentStepId::new("load-review"),
        ))
        .await
        .expect("run should start");
    runner
        .begin_step(ts(200))
        .await
        .expect("run should begin step");
    runner
        .wait(AgentRunWaitReason::Timer, ts(250))
        .await
        .expect("run should wait for timer");
}

fn effect(
    effect_id: impl Into<String>,
    kind: AgentEffectKind,
    target_type: &str,
    target_name: &str,
    due_at: u64,
) -> AgentEffect {
    let effect_id = effect_id.into();
    let metadata = AgentEffectMetadata::new(
        AgentEffectId::new(effect_id.as_str()),
        AgentDurabilityMetadata::new(
            AgentDeduplicationKey::new(format!("dedupe:{effect_id}")),
            AgentCausationId::new(format!("cause:{effect_id}")),
            AgentCorrelationId::new("corr:load"),
        ),
        AgentIdempotencyKey::new(format!("idempotency:{effect_id}")),
        ts(100),
    )
    .expect("effect metadata should validate")
    .due_at(ts(due_at));

    AgentEffectSchedule::new(kind, target(target_type, target_name), metadata)
        .expect("effect schedule should validate")
        .expected_result_type("dispatch.result")
        .expect("expected result type should validate")
        .into_effect()
        .expect("effect should validate")
}

fn timer(
    workflow: &AgentWorkflow,
    run_id: &AgentRunId,
    timer_id: &AgentTimerId,
    due_at: u64,
) -> AgentTimerEntry {
    AgentTimerEntry::new(
        timer_id.clone(),
        workflow.workflow_id.clone(),
        run_id.clone(),
        AgentTenantId::new("tenant-load"),
        ts(due_at),
        AgentDurabilityMetadata::new(
            AgentDeduplicationKey::new(format!("timer:{}", timer_id.as_str())),
            AgentCausationId::new("cause:timer-load"),
            AgentCorrelationId::new("corr:timer-load"),
        ),
        ts(100),
    )
    .expect("timer entry should validate")
    .policy(
        AgentTimerPolicy::new()
            .policy_name("load-timeout")
            .max_lateness_ms(5_000),
    )
    .expect("timer policy should validate")
}

fn run_state(
    run_id: impl Into<String>,
    status: AgentRunStatus,
    updated_at: u64,
    checkpoint: Option<HumanCheckpoint>,
) -> AgentRunState {
    let run_id = run_id.into();
    AgentRunState {
        run_id: AgentRunId::new(run_id),
        workflow_id: workflow().workflow_id,
        tenant: Some(AgentTenantId::new("tenant-load")),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        state_schema_version: StateSchemaVersion::new(1),
        graph_state: None,
        status,
        current_step_id: Some(AgentStepId::new("load-review")),
        current_attempt: 0,
        inputs_ref: None,
        state_payload: AgentStatePayload::Empty,
        checkpoints: checkpoint.into_iter().collect(),
        pending_effects: Vec::new(),
        pending_human_checkpoint: (status == AgentRunStatus::WaitingForHuman)
            .then(|| HumanCheckpointId::new("checkpoint-load")),
        cancellation: None,
        created_at: ts(100),
        updated_at: ts(updated_at),
        completed_at: matches!(
            status,
            AgentRunStatus::Completed | AgentRunStatus::Failed | AgentRunStatus::Cancelled
        )
        .then(|| ts(updated_at)),
    }
}

fn checkpoint(checkpoint_id: impl Into<String>, created_at: u64) -> HumanCheckpoint {
    HumanCheckpoint {
        checkpoint_id: HumanCheckpointId::new(checkpoint_id.into()),
        status: HumanCheckpointStatus::Open,
        summary: "Review load-test checkpoint".to_string(),
        available_decisions: vec![HumanDecisionOption {
            value: "approve".to_string(),
            label: "Approve".to_string(),
            requires_comment: false,
        }],
        required_roles: vec!["reviewer".to_string()],
        due_at: Some(ts(created_at + 1_000)),
        escalation_target: Some("workflow-ops".to_string()),
        context_artifacts: Vec::new(),
        created_by: Some(PrincipalRef {
            principal_type: "service".to_string(),
            principal_id: "load-harness".to_string(),
            display_name: Some("Load Harness".to_string()),
        }),
        resolved_by: None,
        created_at: ts(created_at),
        resolved_at: None,
        audit_event_ids: Vec::new(),
    }
}

fn accepted_run_state(
    workflow: &AgentWorkflow,
    run_id: &AgentRunId,
    step_id: AgentStepId,
) -> AgentRunState {
    AgentRunState {
        run_id: run_id.clone(),
        workflow_id: workflow.workflow_id.clone(),
        tenant: Some(AgentTenantId::new("tenant-load")),
        definition_version: workflow.definition_version.clone(),
        state_schema_version: workflow.state_schema_version,
        graph_state: None,
        status: AgentRunStatus::Accepted,
        current_step_id: Some(step_id),
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

fn workflow() -> AgentWorkflow {
    AgentWorkflow {
        workflow_id: AgentWorkflowId::new("workflow-load-backpressure"),
        workflow_type: "load-backpressure".to_string(),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        state_schema_version: StateSchemaVersion::new(1),
        display_name: Some("Load Backpressure Workflow".to_string()),
        status_labels: vec![
            AgentRunStatus::Accepted.as_label().to_string(),
            AgentRunStatus::Running.as_label().to_string(),
            AgentRunStatus::WaitingForTimer.as_label().to_string(),
            AgentRunStatus::WaitingForHuman.as_label().to_string(),
            AgentRunStatus::Failed.as_label().to_string(),
        ],
        command_types: vec![
            AgentCommandKind::StartRun.type_name().to_string(),
            AgentCommandKind::TimerFired {
                timer_id: "timer".to_string(),
            }
            .type_name()
            .to_string(),
        ],
        steps: vec![AgentStep {
            step_id: AgentStepId::new("load-review"),
            kind: AgentStepKind::Planner,
            display_name: Some("Load Review".to_string()),
            next_step_ids: Vec::new(),
            timeout_ms: Some(5_000),
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

fn target(target_type: &str, target_name: &str) -> AgentEffectTarget {
    AgentEffectTarget {
        target_type: target_type.to_string(),
        name: target_name.to_string(),
        address: Some(format!("{target_type}://{target_name}")),
        attributes: BTreeMap::new(),
    }
}

fn metric_series_count(snapshot: &MetricsSnapshot, name: &str) -> usize {
    snapshot
        .observations_named(name)
        .into_iter()
        .map(|observation| {
            let mut attributes: Vec<_> = observation
                .attributes()
                .iter()
                .map(|attribute| (attribute.key().to_string(), attribute.value().to_string()))
                .collect();
            attributes.sort();
            (observation.kind(), attributes)
        })
        .collect::<BTreeSet<(MetricKind, Vec<(String, String)>)>>()
        .len()
}

fn assert_all_metric_attributes_are_bounded(snapshot: &MetricsSnapshot) {
    let forbidden_runtime_ids = [
        "run_id",
        "effect_id",
        "timer_id",
        "command_id",
        "worker_id",
        "idempotency_key",
        "correlation_id",
        "causation_id",
    ];
    for observation in snapshot.observations() {
        let attributes: Vec<_> = observation
            .attributes()
            .iter()
            .map(|attribute| (attribute.key(), attribute.value()))
            .collect();
        validate_agent_metric_attributes(&attributes).unwrap_or_else(|error| {
            panic!(
                "metric {} has unbounded attributes {:?}: {error}",
                observation.name(),
                observation.attributes()
            )
        });
        for attribute in observation.attributes() {
            assert!(
                !forbidden_runtime_ids.contains(&attribute.key())
                    && !FORBIDDEN_HOT_METRIC_FIELDS.contains(&attribute.key()),
                "metric {} leaked high-cardinality attribute {}={}",
                observation.name(),
                attribute.key(),
                attribute.value()
            );
        }
    }
}

const fn ts(value: u64) -> AgentTimestampMillis {
    AgentTimestampMillis::new(value)
}
