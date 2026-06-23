//! Runtime event contract tests.

use rakka_agent_workflow::{
    next_runtime_event_sequence, validate_runtime_event, validate_runtime_event_follows,
    AgentCausationId, AgentCompiledNodeId, AgentCompiledPlanFingerprint, AgentCompiledPlanId,
    AgentCorrelationId, AgentGraphRunState, AgentRunId, AgentRuntimeEvent, AgentRuntimeEventDraft,
    AgentRuntimeEventKind, AgentRuntimeEventProjection, AgentRuntimeEventSink,
    AgentRuntimeEventWriteStatus, AgentTelemetryContext, AgentTimestampMillis, AgentWorkflowId,
    InMemoryAgentRuntimeEventSink, WorkflowDefinitionVersion, AGENT_LOG_ATTR_CAUSATION_ID,
    AGENT_LOG_ATTR_CORRELATION_ID, AGENT_LOG_ATTR_DEFINITION_VERSION, AGENT_LOG_ATTR_RUN_ID,
    AGENT_LOG_ATTR_WORKFLOW_ID, AGENT_METRIC_ATTR_STATUS,
};
use serde_json::json;

#[test]
fn runtime_event_round_trips_with_stable_kind_name() {
    let graph = graph_state(7, 41);
    let event = event_draft(AgentRuntimeEventKind::NodeCompleted)
        .node_id(AgentCompiledNodeId::new("node-1"))
        .attribute(AGENT_METRIC_ATTR_STATUS, "completed")
        .after_persistence(Some(&graph))
        .expect("event draft should finalize")
        .expect("persistence succeeded should produce event");

    assert_eq!(event.event_sequence, 42);
    assert_eq!(event.scheduler_revision, 7);
    assert_eq!(
        AgentRuntimeEventKind::from_label("node-completed"),
        Some(AgentRuntimeEventKind::NodeCompleted)
    );

    let value = serde_json::to_value(&event).expect("event should serialize");
    assert_eq!(value["kind"], json!("node-completed"));
    assert_eq!(value["node_id"], json!("node-1"));
    assert_eq!(value["event_sequence"], json!(42));

    let decoded: AgentRuntimeEvent =
        serde_json::from_value(value).expect("event should deserialize");
    assert_eq!(decoded, event);
    validate_runtime_event(&decoded).expect("decoded event should validate");
}

#[test]
fn runtime_event_sequence_is_per_run_and_monotonic() {
    let mut graph = graph_state(1, 0);
    let first = event_draft(AgentRuntimeEventKind::RunStarted)
        .after_persistence(Some(&graph))
        .expect("first event should finalize")
        .expect("persistence succeeded should produce first event");
    validate_runtime_event_follows(0, &first).expect("first event should follow zero");
    assert_eq!(first.event_sequence, 1);

    graph.last_event_sequence = first.event_sequence;
    let second = event_draft(AgentRuntimeEventKind::RunWaiting)
        .after_persistence(Some(&graph))
        .expect("second event should finalize")
        .expect("persistence succeeded should produce second event");
    validate_runtime_event_follows(first.event_sequence, &second)
        .expect("second event should follow first");
    assert_eq!(second.event_sequence, 2);

    let stale = AgentRuntimeEvent::new(
        AgentWorkflowId::new("workflow-1"),
        AgentRunId::new("run-1"),
        WorkflowDefinitionVersion::new("v1"),
        graph.plan_fingerprint.clone(),
        graph.scheduler_revision,
        2,
        AgentTimestampMillis::new(125),
        AgentRuntimeEventKind::RunResumed,
        AgentCausationId::new("command-1"),
        AgentCorrelationId::new("corr-1"),
        AgentTelemetryContext::default(),
    )
    .expect("base event should build");
    let error = validate_runtime_event_follows(second.event_sequence, &stale)
        .expect_err("duplicate sequence should fail");
    assert_eq!(error.code(), "invalid-runtime-event-sequence");
    assert_eq!(
        next_runtime_event_sequence(2).expect("sequence should advance"),
        3
    );
}

#[test]
fn failed_persistence_does_not_produce_success_event() {
    let event = event_draft(AgentRuntimeEventKind::RunCompleted)
        .after_persistence(None)
        .expect("failed persistence should not make draft invalid");

    assert_eq!(event, None);
}

#[test]
fn scoped_event_kinds_require_matching_ids() {
    let graph = graph_state(1, 0);
    let error = event_draft(AgentRuntimeEventKind::EffectScheduled)
        .after_persistence(Some(&graph))
        .expect_err("effect event without effect id should fail");

    assert_eq!(error.code(), "invalid-runtime-event");
}

#[test]
fn runtime_event_rejects_high_cardinality_attributes() {
    let graph = graph_state(1, 0);
    let error = event_draft(AgentRuntimeEventKind::RunStarted)
        .attribute("run_id", "run-1")
        .after_persistence(Some(&graph))
        .expect_err("raw ids must not become event attributes");

    assert_eq!(error.code(), "unsafe-runtime-event-attribute");
}

#[test]
fn runtime_event_projection_rebuilds_from_event_stream() {
    let mut graph = graph_state(1, 0);
    let run_started = event_draft(AgentRuntimeEventKind::RunStarted)
        .after_persistence(Some(&graph))
        .expect("run started event should finalize")
        .expect("persistence succeeded should produce event");
    graph.last_event_sequence = run_started.event_sequence;
    graph.scheduler_revision = 2;
    let node_completed = event_draft(AgentRuntimeEventKind::NodeCompleted)
        .node_id(AgentCompiledNodeId::new("node-1"))
        .attribute(AGENT_METRIC_ATTR_STATUS, "completed")
        .after_persistence(Some(&graph))
        .expect("node completed event should finalize")
        .expect("persistence succeeded should produce event");
    graph.last_event_sequence = node_completed.event_sequence;
    graph.scheduler_revision = 3;
    let run_completed = event_draft(AgentRuntimeEventKind::RunCompleted)
        .after_persistence(Some(&graph))
        .expect("run completed event should finalize")
        .expect("persistence succeeded should produce event");

    let projection =
        AgentRuntimeEventProjection::from_events(&[run_started, node_completed, run_completed])
            .expect("projection should rebuild from a full ordered stream");

    assert_eq!(projection.run_id, AgentRunId::new("run-1"));
    assert_eq!(projection.last_scheduler_revision, 3);
    assert_eq!(projection.last_event_sequence, 3);
    assert_eq!(projection.event_count, 3);
    assert_eq!(projection.node_event_count, 1);
    assert_eq!(projection.effect_event_count, 0);
    assert_eq!(
        projection.terminal_event_kind,
        Some(AgentRuntimeEventKind::RunCompleted)
    );
}

#[test]
fn runtime_event_correlation_fields_align_with_log_and_audit_attributes() {
    let graph = graph_state(1, 0);
    let event = event_draft(AgentRuntimeEventKind::NodeCompleted)
        .node_id(AgentCompiledNodeId::new("node-1"))
        .after_persistence(Some(&graph))
        .expect("event should finalize")
        .expect("persistence succeeded should produce event");

    let fields = event.correlation_fields();
    let log_attributes = fields.log_attributes();

    assert_eq!(fields.workflow_id, event.workflow_id);
    assert_eq!(fields.run_id, event.run_id);
    assert_eq!(fields.definition_version, event.definition_version);
    assert_eq!(fields.causation_id, event.causation_id);
    assert_eq!(fields.correlation_id, event.correlation_id);
    assert_eq!(fields.telemetry_context, event.telemetry_context);
    assert_eq!(log_attributes[AGENT_LOG_ATTR_WORKFLOW_ID], "workflow-1");
    assert_eq!(log_attributes[AGENT_LOG_ATTR_RUN_ID], "run-1");
    assert_eq!(log_attributes[AGENT_LOG_ATTR_DEFINITION_VERSION], "v1");
    assert_eq!(log_attributes[AGENT_LOG_ATTR_CAUSATION_ID], "command-1");
    assert_eq!(log_attributes[AGENT_LOG_ATTR_CORRELATION_ID], "corr-1");
    assert_eq!(log_attributes["runtime_event_kind"], "node-completed");
    assert_eq!(log_attributes["node_id"], "node-1");
}

#[test]
fn runtime_event_metric_cardinality_guard_keeps_ids_out_of_hot_attributes() {
    let graph = graph_state(1, 0);
    let error = event_draft(AgentRuntimeEventKind::NodeCompleted)
        .node_id(AgentCompiledNodeId::new("node-1"))
        .attribute("node_id", "node-1")
        .after_persistence(Some(&graph))
        .expect_err("node ids must remain typed fields, not metric labels");

    assert_eq!(error.code(), "unsafe-runtime-event-attribute");
}

#[tokio::test]
async fn in_memory_runtime_event_sink_records_ordered_events() {
    let mut sink = InMemoryAgentRuntimeEventSink::new();
    let mut graph = graph_state(1, 0);
    let first = event_draft(AgentRuntimeEventKind::RunStarted)
        .after_persistence(Some(&graph))
        .expect("first event should finalize")
        .expect("persistence succeeded should produce first event");
    graph.last_event_sequence = first.event_sequence;
    let second = event_draft(AgentRuntimeEventKind::RunWaiting)
        .after_persistence(Some(&graph))
        .expect("second event should finalize")
        .expect("persistence succeeded should produce second event");

    let first_acceptance = sink
        .record_runtime_event(first.clone())
        .await
        .expect("first event should record");
    let second_acceptance = sink
        .record_runtime_event(second.clone())
        .await
        .expect("second event should record");

    assert_eq!(
        first_acceptance.status,
        AgentRuntimeEventWriteStatus::Recorded
    );
    assert_eq!(
        second_acceptance.status,
        AgentRuntimeEventWriteStatus::Recorded
    );
    let events = sink
        .runtime_events_for_run(AgentRunId::new("run-1"))
        .await
        .expect("events should query");
    assert_eq!(events, vec![first, second]);
}

#[tokio::test]
async fn runtime_event_sink_reports_duplicate_records() {
    let mut sink = InMemoryAgentRuntimeEventSink::new();
    let graph = graph_state(1, 0);
    let event = event_draft(AgentRuntimeEventKind::RunStarted)
        .after_persistence(Some(&graph))
        .expect("event should finalize")
        .expect("persistence succeeded should produce event");

    sink.record_runtime_event(event.clone())
        .await
        .expect("first event should record");
    let duplicate = sink
        .record_runtime_event(event)
        .await
        .expect("duplicate event should be reported");

    assert_eq!(duplicate.status, AgentRuntimeEventWriteStatus::Duplicate);
    assert_eq!(sink.events().len(), 1);
}

#[tokio::test]
async fn runtime_event_sink_failure_is_observable_without_state_transition() {
    let mut sink = InMemoryAgentRuntimeEventSink::new().fail_next_write("projection offline");
    let graph = graph_state(5, 10);
    let persisted_graph = graph.clone();
    let event = event_draft(AgentRuntimeEventKind::RunCompleted)
        .after_persistence(Some(&persisted_graph))
        .expect("event should finalize")
        .expect("persistence succeeded should produce event");

    let error = sink
        .record_runtime_event(event)
        .await
        .expect_err("sink failure should be reported");

    assert_eq!(error.code(), "runtime-event-sink");
    assert_eq!(sink.events().len(), 0);
    assert_eq!(
        graph, persisted_graph,
        "sink failure must not mutate durable graph state"
    );
}

fn event_draft(kind: AgentRuntimeEventKind) -> AgentRuntimeEventDraft {
    AgentRuntimeEventDraft::new(
        AgentWorkflowId::new("workflow-1"),
        AgentRunId::new("run-1"),
        WorkflowDefinitionVersion::new("v1"),
        AgentTimestampMillis::new(123),
        kind,
        AgentCausationId::new("command-1"),
        AgentCorrelationId::new("corr-1"),
        AgentTelemetryContext::default(),
    )
}

fn graph_state(scheduler_revision: u64, last_event_sequence: u64) -> AgentGraphRunState {
    let mut graph = AgentGraphRunState::new(
        AgentCompiledPlanId::new("plan-1"),
        AgentCompiledPlanFingerprint::new("fingerprint-1"),
    );
    graph.scheduler_revision = scheduler_revision;
    graph.last_event_sequence = last_event_sequence;
    graph
}
