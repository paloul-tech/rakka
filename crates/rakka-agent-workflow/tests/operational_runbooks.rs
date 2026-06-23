//! Operational runbook and dashboard documentation coverage.

use rakka_agent_workflow::{
    AGENT_LOG_ATTR_AUDIT_EVENT_ID, AGENT_LOG_ATTR_AUDIT_KIND, AGENT_LOG_ATTR_CAUSATION_ID,
    AGENT_LOG_ATTR_CHECKPOINT_ID, AGENT_LOG_ATTR_COMMAND_ID, AGENT_LOG_ATTR_CORRELATION_ID,
    AGENT_LOG_ATTR_DEFINITION_VERSION, AGENT_LOG_ATTR_EFFECT_ID, AGENT_LOG_ATTR_REDACTION,
    AGENT_LOG_ATTR_RUN_ID, AGENT_LOG_ATTR_STEP_ID, AGENT_LOG_ATTR_TENANT_ID,
    AGENT_LOG_ATTR_WORKFLOW_ID, AGENT_LOG_ATTR_WORKFLOW_TYPE, METRIC_AGENT_ACTIVE_RUNS,
    METRIC_AGENT_DISPATCHER_BACKLOG, METRIC_AGENT_DISPATCHER_FLEET,
    METRIC_AGENT_DISPATCHER_IN_FLIGHT, METRIC_AGENT_DISPATCH_LATENCY_MS,
    METRIC_AGENT_DUE_OUTBOX_EFFECTS, METRIC_AGENT_HUMAN_WAITING_RUNS,
    METRIC_AGENT_HUMAN_WAIT_LATENCY_MS, METRIC_AGENT_INBOX_COMMANDS, METRIC_AGENT_MAILBOX_DEPTH,
    METRIC_AGENT_OUTBOX_EFFECTS, METRIC_AGENT_PENDING_INBOX_COMMANDS,
    METRIC_AGENT_POSTGRES_LATENCY_MS, METRIC_AGENT_PROCESS_RUNNING, METRIC_AGENT_RECOVERY_EVENTS,
    METRIC_AGENT_RECOVERY_LATENCY_MS, METRIC_AGENT_RUN_TRANSITIONS,
    METRIC_AGENT_SHARD_OWNERSHIP_COUNT, METRIC_AGENT_STEP_TRANSITIONS,
    METRIC_AGENT_STREAM_PRESSURE, METRIC_AGENT_TIMERS, METRIC_AGENT_TIMERS_LATE_BY_MS,
    SNAPSHOT_AGENT_WORKFLOW_HUMAN_CHECKPOINTS, SNAPSHOT_AGENT_WORKFLOW_OUTBOX,
    SNAPSHOT_AGENT_WORKFLOW_RECOVERY, SNAPSHOT_AGENT_WORKFLOW_RUNTIME,
    SNAPSHOT_AGENT_WORKFLOW_SHARDS,
};
use rakka_core::{
    METRIC_K8S_COMPATIBILITY, METRIC_K8S_READINESS, METRIC_PERSISTENCE_LATENCY_MS,
    METRIC_PROCESS_EXITS, METRIC_SHARD_OWNERSHIP_COUNT, METRIC_SHUTDOWN_TIMEOUTS,
};

const RUNBOOK: &str = include_str!(
    "../../../docs/plans/agentic-workflow/phase-7-4-operational-runbooks-dashboards.md"
);
const GRAPH_RUNBOOK: &str = include_str!(
    "../../../docs/plans/compiled_execution_with_graph_schdlr/compiled-graph-operational-runbooks.md"
);

#[test]
fn operational_runbook_covers_required_incident_paths() {
    for expected in [
        "Runbook: Waiting Runs",
        "Runbook: Stuck Dispatchers",
        "Runbook: Overdue Timers",
        "Runbook: Failed Effects",
        "Runbook: Duplicate Callbacks",
        "Runbook: Human Checkpoint Age",
        "Runbook: Drain Blockers",
        "Dashboard Catalog",
        "Field Catalog",
        "Alert Recommendations",
        "Escalation Checklist",
    ] {
        assert!(RUNBOOK.contains(expected), "runbook missing {expected}");
    }
}

#[test]
fn operational_runbook_names_real_metric_and_snapshot_surfaces() {
    for expected in [
        METRIC_AGENT_ACTIVE_RUNS,
        METRIC_AGENT_PENDING_INBOX_COMMANDS,
        METRIC_AGENT_DUE_OUTBOX_EFFECTS,
        METRIC_AGENT_INBOX_COMMANDS,
        METRIC_AGENT_OUTBOX_EFFECTS,
        METRIC_AGENT_DISPATCHER_BACKLOG,
        METRIC_AGENT_DISPATCHER_IN_FLIGHT,
        METRIC_AGENT_DISPATCHER_FLEET,
        METRIC_AGENT_DISPATCH_LATENCY_MS,
        METRIC_AGENT_TIMERS,
        METRIC_AGENT_TIMERS_LATE_BY_MS,
        METRIC_AGENT_HUMAN_WAITING_RUNS,
        METRIC_AGENT_HUMAN_WAIT_LATENCY_MS,
        METRIC_AGENT_MAILBOX_DEPTH,
        METRIC_AGENT_STREAM_PRESSURE,
        METRIC_AGENT_PROCESS_RUNNING,
        METRIC_AGENT_POSTGRES_LATENCY_MS,
        METRIC_AGENT_SHARD_OWNERSHIP_COUNT,
        METRIC_AGENT_RUN_TRANSITIONS,
        METRIC_AGENT_STEP_TRANSITIONS,
        METRIC_AGENT_RECOVERY_EVENTS,
        METRIC_AGENT_RECOVERY_LATENCY_MS,
        METRIC_K8S_READINESS,
        METRIC_K8S_COMPATIBILITY,
        METRIC_SHUTDOWN_TIMEOUTS,
        METRIC_PERSISTENCE_LATENCY_MS,
        METRIC_PROCESS_EXITS,
        METRIC_SHARD_OWNERSHIP_COUNT,
        SNAPSHOT_AGENT_WORKFLOW_RUNTIME,
        SNAPSHOT_AGENT_WORKFLOW_OUTBOX,
        SNAPSHOT_AGENT_WORKFLOW_RECOVERY,
        SNAPSHOT_AGENT_WORKFLOW_HUMAN_CHECKPOINTS,
        SNAPSHOT_AGENT_WORKFLOW_SHARDS,
    ] {
        assert!(RUNBOOK.contains(expected), "runbook missing {expected}");
    }
}

#[test]
fn operational_runbook_names_query_trace_log_and_audit_fields() {
    for expected in [
        "AgentWorkflowRunQuery::new().waiting()",
        "waiting_reason(AgentRunQueryWaitingReason::Human)",
        "checkpoint_created_at_or_before",
        "stuck_dispatcher_at_or_before",
        "AgentDispatchQuery::new()",
        "stuck_at_or_before",
        "AgentTimerQuery::new()",
        "due_at_or_before",
        "failed_step_id",
        "trace_id",
        "span_id",
        "traceparent",
        "tracestate",
        "span_links",
        "audit_event_id",
        "audit_kind",
        "actor_principal",
        "artifact_refs",
        "content_hashes",
        AGENT_LOG_ATTR_WORKFLOW_ID,
        AGENT_LOG_ATTR_WORKFLOW_TYPE,
        AGENT_LOG_ATTR_DEFINITION_VERSION,
        AGENT_LOG_ATTR_RUN_ID,
        AGENT_LOG_ATTR_TENANT_ID,
        AGENT_LOG_ATTR_STEP_ID,
        AGENT_LOG_ATTR_EFFECT_ID,
        AGENT_LOG_ATTR_CHECKPOINT_ID,
        AGENT_LOG_ATTR_COMMAND_ID,
        AGENT_LOG_ATTR_AUDIT_EVENT_ID,
        AGENT_LOG_ATTR_CAUSATION_ID,
        AGENT_LOG_ATTR_CORRELATION_ID,
        AGENT_LOG_ATTR_REDACTION,
        AGENT_LOG_ATTR_AUDIT_KIND,
    ] {
        assert!(RUNBOOK.contains(expected), "runbook missing {expected}");
    }
}

#[test]
fn operational_runbook_points_to_kubernetes_and_postgres_operational_contracts() {
    for expected in [
        "kubectl -n rakka-system",
        "curl -fsS http://localhost:8080/ready",
        "curl -fsS http://localhost:8080/snapshots",
        "rakka_agent_workflow_run_index",
        "rakka_agent_workflow_timer_index",
        "rakka_agent_workflow_dispatch_index",
        "rakka_agent_workflow_checkpoint_index",
        "rakka_agent_workflow_audit_index",
        "RAKKA_K8S_PRESTOP_TIMEOUT_MS",
        "OTLP Collector",
    ] {
        assert!(RUNBOOK.contains(expected), "runbook missing {expected}");
    }
}

#[test]
fn graph_operational_runbook_covers_required_incident_paths_and_ownership() {
    for expected in [
        "Runbook: Stuck Graph Nodes",
        "Runbook: Failed Graph Effects",
        "Runbook: Late Graph Timers",
        "Runbook: Open Graph Human Checkpoints",
        "Runbook: Runtime Event Sink Failures",
        "Runbook: Graph Drain Blockers",
        "Incident Ownership",
        "Usually product backend owned",
        "Usually Rakka runtime owned",
        "Visual editor, product DSL, compiler",
        "Trigger does not start a run",
        "Third-party credential cannot be resolved",
        "Node is waiting, failed, skipped, cancelled, or stuck after durable run start",
        "UI timeline is stale but durable graph state moved",
    ] {
        assert!(
            GRAPH_RUNBOOK.contains(expected),
            "graph runbook missing {expected}"
        );
    }
}

#[test]
fn graph_operational_runbook_names_real_graph_query_and_event_surfaces() {
    for expected in [
        "AgentWorkflowRunQuery::new()",
        "graph_plan_fingerprint",
        "graph_node_status(AgentGraphNodeStatus::Waiting)",
        "graph_node_status(AgentGraphNodeStatus::Runnable)",
        "graph_node_status(AgentGraphNodeStatus::Failed)",
        "graph_node_kind(AgentCompiledNodeKind::ToolCall)",
        "graph_node_kind(AgentCompiledNodeKind::TimerWait)",
        "graph_node_kind(AgentCompiledNodeKind::HumanCheckpoint)",
        "graph_wait_reason(AgentGraphWaitReason::Effect)",
        "graph_error_code",
        "AgentDispatchQuery::new()",
        "graph_node_id",
        "stuck_at_or_before",
        "AgentRuntimeEventProjection",
        "PostgresAgentWorkflowQueryIndex::runtime_event_projection",
        "event_sequence",
        "NodeRunnable",
        "NodeStarted",
        "RunWaiting",
        "RunResumed",
        "HumanDecisionAccepted",
        "rakka_agent_workflow_graph_node_index",
        "rakka_agent_workflow_runtime_event_projection",
    ] {
        assert!(
            GRAPH_RUNBOOK.contains(expected),
            "graph runbook missing {expected}"
        );
    }
}

#[test]
fn graph_operational_runbook_names_metrics_snapshots_and_cardinality_rules() {
    for expected in [
        METRIC_AGENT_ACTIVE_RUNS,
        METRIC_AGENT_PENDING_INBOX_COMMANDS,
        METRIC_AGENT_DUE_OUTBOX_EFFECTS,
        METRIC_AGENT_DISPATCHER_BACKLOG,
        METRIC_AGENT_DISPATCHER_IN_FLIGHT,
        METRIC_AGENT_DISPATCH_LATENCY_MS,
        METRIC_AGENT_TIMERS,
        METRIC_AGENT_TIMERS_LATE_BY_MS,
        METRIC_AGENT_HUMAN_WAITING_RUNS,
        METRIC_AGENT_HUMAN_WAIT_LATENCY_MS,
        METRIC_AGENT_MAILBOX_DEPTH,
        METRIC_AGENT_POSTGRES_LATENCY_MS,
        METRIC_AGENT_SHARD_OWNERSHIP_COUNT,
        METRIC_AGENT_RECOVERY_EVENTS,
        METRIC_AGENT_RECOVERY_LATENCY_MS,
        METRIC_K8S_READINESS,
        METRIC_K8S_COMPATIBILITY,
        METRIC_SHUTDOWN_TIMEOUTS,
        "curl -fsS http://localhost:8080/snapshots",
        "AgentGraphRunProjection.waiting_node_count",
        "AgentGraphRunProjection.runnable_node_count",
        "raw `run_id`",
        "`node_id`",
        "`effect_id`",
        "`timer_id`",
        "`checkpoint_id`",
        "not hot metric labels",
    ] {
        assert!(
            GRAPH_RUNBOOK.contains(expected),
            "graph runbook missing {expected}"
        );
    }
}

#[test]
fn graph_operational_runbook_links_to_existing_docs_and_gates() {
    for expected in [
        "docs/plans/agentic-workflow/phase-7-4-operational-runbooks-dashboards.md",
        "docs/plans/agentic-workflow/kubernetes-drain-shutdown.md",
        "docs/plans/agentic-workflow/kubernetes-autoscaling-signals.md",
        "docs/plans/agentic-workflow/phase-7-5-production-candidate-gate.md",
        "docs/plans/compiled_execution_with_graph_schdlr/runtime-event-projection-live-stream-guidance.md",
        "cargo test -p rakka-agent-workflow --test compiled_plan_contract",
        "cargo test -p rakka-agent-workflow --test graph_state_contract",
        "cargo test -p rakka-agent-workflow --test graph_scheduler",
        "cargo test -p rakka-agent-workflow --test effect_bridge",
        "cargo test -p rakka-agent-workflow --test runtime_events",
        "cargo test -p rakka-agent-workflow --test failure_injection",
        "cargo test -p rakka-agent-workflow --test load_backpressure_cardinality",
        "cargo test -p rakka-agent-workflow --test api_compatibility",
        "cargo test -p rakka-agent-workflow --test operational_runbooks",
        "cargo test -p rakka-agent-workflow --features sharding --test sharded_run",
        "RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@127.0.0.1:5432/postgres",
        "cargo test -p rakka-agent-workflow --features postgres --test postgres_query_index",
    ] {
        assert!(
            GRAPH_RUNBOOK.contains(expected),
            "graph runbook missing {expected}"
        );
    }
}
