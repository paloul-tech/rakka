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
