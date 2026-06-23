#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Agentic workflow orchestration facade.
//!
//! This crate is intentionally thin in the Phase 0.1 boundary slice. It gives
//! agent workflow work a home without changing the lower-level reliability
//! semantics already implemented by `rakka-workflow`.
//!
//! The crate owns future first-class agent concepts such as runs, steps,
//! effects, human checkpoints, telemetry context, audit events, model/tool
//! adapters, and Kubernetes-scale orchestration helpers.
//!
//! `rakka-workflow` remains the durable inbox/outbox substrate. Core actor,
//! remote, and sharded delivery remain at-most-once; stronger agent workflow
//! behavior must continue to be built from durable state, durable inbox
//! acceptance, durable outbox effects, idempotency keys, and recovery.

pub mod adapters;
pub mod artifacts;
pub mod audit;
pub mod checkpoints;
pub mod compiled_plan;
pub mod credentials;
pub mod definition;
pub mod dispatcher;
pub mod domain;
pub mod effect_bridge;
pub mod facade;
pub mod graph_scheduler;
pub mod graph_state;
pub mod inbox;
#[cfg(feature = "k8s")]
pub mod kubernetes;
pub mod metrics;
pub mod migration;
pub mod otlp;
pub mod outbox;
#[cfg(feature = "postgres")]
pub mod postgres_query;
pub mod query;
pub mod retention;
pub mod runner;
pub mod runtime;
pub mod runtime_events;
#[cfg(feature = "sharding")]
pub mod sharding;
pub mod snapshots;
#[cfg(feature = "testkit")]
pub mod testkit;
pub mod timers;
pub mod trace_context;
pub mod triggers;

#[cfg(feature = "process-tools")]
pub use adapters::ProcessFileWatchToolAdapter;
pub use adapters::{
    AgentAdapterError, AgentAdapterFailureClass, AgentAdapterFuture, AgentAdapterOutcome,
    AgentAdapterReceipt, AgentAdapterRequestMetadata, AgentAdapterResult, AgentAdapterUsage,
    AgentModelAdapter, AgentModelRequest, AgentToolAdapter, AgentToolRequest,
    METRIC_AGENT_MODEL_ADAPTER_CALLS, METRIC_AGENT_MODEL_ADAPTER_LATENCY_MS,
    METRIC_AGENT_MODEL_ADAPTER_TOKENS, METRIC_AGENT_TOOL_ADAPTER_CALLS,
    METRIC_AGENT_TOOL_ADAPTER_LATENCY_MS,
};
pub use artifacts::{
    agent_audit_artifact_refs, agent_effect_artifact_refs, agent_run_artifact_refs,
    validate_artifact_ref, validate_effect_artifact_policy, validate_inline_state,
    validate_run_state_artifact_policy, AgentArtifactError, AgentArtifactPolicy, AgentArtifactRead,
    AgentArtifactResult, AgentArtifactStore, AgentArtifactStoreFuture, AgentArtifactWriteRequest,
    DEFAULT_AGENT_ARTIFACT_RETENTION_CLASS, DEFAULT_AGENT_INLINE_STATE_LIMIT_BYTES,
};
pub use audit::{
    agent_audit_event_kind_label, agent_audit_log_event_name, agent_log_event_from_audit_event,
    validate_agent_audit_event, validate_agent_log_event, AgentAuditAcceptance, AgentAuditError,
    AgentAuditQuery, AgentAuditResult, AgentAuditSink, AgentAuditSinkFuture, AgentAuditWriteStatus,
    AgentInstrumentationScope, AgentLogEvent, AgentLogSeverity, AgentRedactionPolicy,
    InMemoryAgentAuditSink, AGENT_LOG_ATTR_AUDIT_EVENT_ID, AGENT_LOG_ATTR_AUDIT_KIND,
    AGENT_LOG_ATTR_CAUSATION_ID, AGENT_LOG_ATTR_CHECKPOINT_ID, AGENT_LOG_ATTR_COMMAND_ID,
    AGENT_LOG_ATTR_CORRELATION_ID, AGENT_LOG_ATTR_DEFINITION_VERSION, AGENT_LOG_ATTR_EFFECT_ID,
    AGENT_LOG_ATTR_REDACTION, AGENT_LOG_ATTR_RUN_ID, AGENT_LOG_ATTR_STEP_ID,
    AGENT_LOG_ATTR_TENANT_ID, AGENT_LOG_ATTR_WORKFLOW_ID, AGENT_LOG_ATTR_WORKFLOW_TYPE,
    AGENT_LOG_INSTRUMENTATION_SCOPE, DEFAULT_AGENT_LOG_BODY_LIMIT_BYTES,
};
pub use checkpoints::{
    human_decision_command, AgentHumanApprovalRequest, AgentHumanCheckpointError,
    AgentHumanCheckpointOpening, AgentHumanCheckpointResult, AgentHumanCheckpointRuntime,
    AgentHumanDecisionResult, AgentHumanDecisionSubmission, METRIC_AGENT_HUMAN_CHECKPOINTS,
    METRIC_AGENT_HUMAN_WAIT_LATENCY_MS,
};
#[cfg(feature = "http")]
pub use checkpoints::{
    human_decision_http_route, AgentHumanDecisionHttpResponse, DEFAULT_HUMAN_DECISION_HTTP_PATH,
};
pub use compiled_plan::{
    validate_compiled_execution_plan, validate_compiled_execution_plan_with_catalog,
    AgentCompiledEdgeId, AgentCompiledEdgeMergeBehavior, AgentCompiledExecutionPlan,
    AgentCompiledIteratorPolicy, AgentCompiledNodeId, AgentCompiledNodeKind,
    AgentCompiledNodeKindCatalog, AgentCompiledNodeKindDescriptor, AgentCompiledNodeTarget,
    AgentCompiledPlanCompatibility, AgentCompiledPlanEdge, AgentCompiledPlanFingerprint,
    AgentCompiledPlanId, AgentCompiledPlanNode, AgentCompiledPlanPort,
    AgentCompiledPlanRuntimeCapabilities, AgentCompiledPlanSchemaVersion,
    AgentCompiledPlanValidationError, AgentCompiledPlanValidationResult,
    AgentCompiledPortDirection, AgentCompiledPortId, AgentCompiledPortPolicy,
    AgentCredentialBindingRef, CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
};
pub use credentials::{
    credential_binding_ref_from_effect, AgentCredentialError, AgentCredentialResolutionRequest,
    AgentCredentialResolver, AgentCredentialResolverFuture, AgentCredentialResult,
    AgentCredentialUse, AgentEphemeralCredential, AgentEphemeralCredentialMaterial,
    AGENT_CREDENTIAL_BINDING_REF_ATTRIBUTE,
};
pub use definition::{
    AgentCompiledWorkflowRegistration, AgentPayload, AgentWorkflowKey, AgentWorkflowRegistry,
    AgentWorkflowRegistryError, AgentWorkflowRegistryResult,
};
pub use dispatcher::{
    agent_dispatch_id, agent_dispatch_timestamp_from_workflow_timestamp,
    agent_dispatch_timestamp_to_workflow_timestamp, agent_dispatcher_fleet_persistence_id,
    AgentDispatchClaim, AgentDispatchClaimBatch, AgentDispatchCompletion,
    AgentDispatchConcurrencyLimits, AgentDispatchEntry, AgentDispatchJob, AgentDispatchLease,
    AgentDispatchStatus, AgentDispatchTargetClass, AgentDispatcherCycle,
    AgentDispatcherEntrySnapshot, AgentDispatcherError, AgentDispatcherFleet,
    AgentDispatcherFleetSettings, AgentDispatcherFleetState, AgentDispatcherRegistration,
    AgentDispatcherResult, AgentDispatcherSnapshot, AgentDispatcherStatusCount,
    AgentDispatcherTargetClassCount, AgentDispatcherWorker, AgentEffectDispatchFuture,
    AgentEffectDispatcher, AGENT_DISPATCHER_FLEET_PERSISTENCE_PREFIX,
    DEFAULT_AGENT_DISPATCHER_FLEET_ID, METRIC_AGENT_DISPATCHER_BACKLOG,
    METRIC_AGENT_DISPATCHER_FLEET, METRIC_AGENT_DISPATCHER_IN_FLIGHT,
};
pub use domain::{
    AgentAttributes, AgentAuditEvent, AgentAuditEventId, AgentAuditEventKind, AgentCancellation,
    AgentCausationId, AgentCommandId, AgentCorrelationId, AgentDeduplicationKey, AgentDispatchId,
    AgentDispatcherWorkerId, AgentEffect, AgentEffectId, AgentEffectKind, AgentEffectStatus,
    AgentEffectTarget, AgentIdempotencyKey, AgentPayloadDescriptor, AgentRunId, AgentRunState,
    AgentRunStatus, AgentSpanLink, AgentStatePayload, AgentStep, AgentStepId, AgentStepKind,
    AgentTelemetryContext, AgentTenantId, AgentTimerId, AgentTimestampMillis, AgentWorkflow,
    AgentWorkflowId, ArtifactEncryptionRef, ArtifactKind, ArtifactRef, HumanCheckpoint,
    HumanCheckpointId, HumanCheckpointStatus, HumanDecisionOption, InlineState, PrincipalRef,
    RedactionStatus, StateSchemaVersion, WorkflowDefinitionVersion, BOUNDED_METRIC_FIELDS,
    FORBIDDEN_HOT_METRIC_FIELDS, TRACE_LOG_AUDIT_ID_FIELDS,
};
pub use effect_bridge::{
    AgentGraphEffectBridge, AgentGraphEffectBridgeError, AgentGraphEffectBridgeResult,
    AgentGraphEffectCommandOutcome, AgentGraphEffectFailureDisposition,
    AgentGraphEffectScheduleOutcome, AgentGraphEffectScheduleRequest,
    AgentGraphHumanCheckpointScheduleOutcome, AgentGraphHumanCheckpointScheduleRequest,
    AgentGraphTimerScheduleOutcome, AgentGraphTimerScheduleRequest,
};
pub use facade::{
    validate_command, validate_command_metadata, validate_effect_metadata,
    validate_effect_schedule, AgentCommand, AgentCommandKind, AgentCommandMetadata,
    AgentDurabilityMetadata, AgentEffectMetadata, AgentEffectSchedule, AgentFacadeError,
    AgentFacadeResult,
};
pub use graph_scheduler::{
    AgentGraphScheduler, AgentGraphSchedulerError, AgentGraphSchedulerResult,
    AgentGraphSchedulerTransition,
};
pub use graph_state::{
    AgentGraphBlockedReason, AgentGraphLoopInstanceState, AgentGraphNodeProjection,
    AgentGraphNodeState, AgentGraphNodeStatus, AgentGraphRunProjection, AgentGraphRunState,
    AgentGraphStateSchemaVersion, AgentGraphTerminalStatus, AgentGraphWaitReason,
    CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION,
};
pub use inbox::{
    agent_run_workflow_id, AgentInboxAcceptance, AgentInboxDuplicateReason, AgentInboxError,
    AgentInboxResult, AgentRunInbox, METRIC_AGENT_INBOX_COMMANDS,
};
#[cfg(feature = "k8s")]
pub use kubernetes::{
    default_agent_workflow_required_services, parse_agent_workflow_required_services,
    register_agent_workflow_ingress_stop_task, register_agent_workflow_telemetry_flush_task,
    AgentWorkflowDrainError, AgentWorkflowDrainResult, AgentWorkflowIngressGate,
    AgentWorkflowKubernetesStartup, AgentWorkflowStartupSnapshot, AgentWorkflowStartupStep,
    AGENT_WORKFLOW_FLUSH_TELEMETRY_OPERATION, AGENT_WORKFLOW_FLUSH_TELEMETRY_TASK,
    AGENT_WORKFLOW_SHUTDOWN_OPERATION_ATTR, AGENT_WORKFLOW_STARTUP_ACTOR_SYSTEM,
    AGENT_WORKFLOW_STARTUP_ARTIFACT_STORE, AGENT_WORKFLOW_STARTUP_DURABLE_STATE,
    AGENT_WORKFLOW_STARTUP_OPERATIONAL_SNAPSHOTS, AGENT_WORKFLOW_STARTUP_OTLP_EXPORTER,
    AGENT_WORKFLOW_STARTUP_POSTGRES, AGENT_WORKFLOW_STARTUP_QUERY_INDEX,
    AGENT_WORKFLOW_STARTUP_REMOTING, AGENT_WORKFLOW_STARTUP_SHARDING,
    AGENT_WORKFLOW_STARTUP_TELEMETRY_RESOURCE, AGENT_WORKFLOW_STARTUP_WORKFLOW_REGISTRY,
    AGENT_WORKFLOW_STOP_INGRESS_OPERATION, AGENT_WORKFLOW_STOP_INGRESS_TASK,
    DEFAULT_AGENT_WORKFLOW_STARTUP_STEPS,
};
pub use metrics::{
    agent_autoscaling_signal, agent_metric_instrument, is_agent_autoscaling_metric,
    is_bounded_agent_metric_attribute, is_forbidden_agent_metric_attribute, record_agent_counter,
    record_agent_gauge, record_agent_histogram, validate_agent_metric_attributes,
    AgentAutoscalingSignal, AgentAutoscalingSignalRole, AgentMetricError, AgentMetricInstrument,
    AgentMetricResult, AGENT_METRIC_ATTRIBUTE_VALUE_MAX_BYTES, AGENT_METRIC_ATTR_ADAPTER_KIND,
    AGENT_METRIC_ATTR_ARTIFACT_KIND, AGENT_METRIC_ATTR_CHECKPOINT_STATUS,
    AGENT_METRIC_ATTR_COMMAND_TYPE, AGENT_METRIC_ATTR_COMPONENT,
    AGENT_METRIC_ATTR_DATABASE_OPERATION, AGENT_METRIC_ATTR_DEFINITION_VERSION,
    AGENT_METRIC_ATTR_DEPLOYMENT_CHANNEL, AGENT_METRIC_ATTR_DETAIL, AGENT_METRIC_ATTR_DIRECTION,
    AGENT_METRIC_ATTR_EFFECT_KIND, AGENT_METRIC_ATTR_ENTITY_TYPE, AGENT_METRIC_ATTR_ERROR_CODE,
    AGENT_METRIC_ATTR_MESSAGE_TYPE, AGENT_METRIC_ATTR_OPERATION, AGENT_METRIC_ATTR_OUTCOME,
    AGENT_METRIC_ATTR_QUEUE, AGENT_METRIC_ATTR_REDACTION, AGENT_METRIC_ATTR_RETRY_ATTEMPT_BUCKET,
    AGENT_METRIC_ATTR_SIGNAL, AGENT_METRIC_ATTR_STATUS, AGENT_METRIC_ATTR_STEP_KIND,
    AGENT_METRIC_ATTR_TARGET_CLASS, AGENT_METRIC_ATTR_TENANT_TIER, AGENT_METRIC_ATTR_TIMER_STATUS,
    AGENT_METRIC_ATTR_TRANSITION, AGENT_METRIC_ATTR_TRIGGER_KIND, AGENT_METRIC_ATTR_WORKFLOW_TYPE,
    AGENT_WORKFLOW_AUTOSCALING_SIGNALS, AGENT_WORKFLOW_BOUNDED_METRIC_ATTRIBUTES,
    AGENT_WORKFLOW_METRIC_INSTRUMENTS, METRIC_AGENT_ACTIVE_RUNS, METRIC_AGENT_DISPATCH_LATENCY_MS,
    METRIC_AGENT_DUE_OUTBOX_EFFECTS, METRIC_AGENT_HUMAN_WAITING_RUNS, METRIC_AGENT_MAILBOX_DEPTH,
    METRIC_AGENT_PENDING_INBOX_COMMANDS, METRIC_AGENT_POSTGRES_LATENCY_MS,
    METRIC_AGENT_PROCESS_RUNNING, METRIC_AGENT_RECOVERY_EVENTS, METRIC_AGENT_RECOVERY_LATENCY_MS,
    METRIC_AGENT_RUN_TRANSITIONS, METRIC_AGENT_SHARD_OWNERSHIP_COUNT,
    METRIC_AGENT_STEP_TRANSITIONS, METRIC_AGENT_STREAM_PRESSURE, METRIC_AGENT_TIMERS_LATE_BY_MS,
};
pub use migration::{
    plan_agent_workflow_index_backfill, repair_agent_workflow_index, AgentMigrationAssessment,
    AgentMigrationDecision, AgentMigrationReason, AgentWorkflowBackfillAction,
    AgentWorkflowBackfillItem, AgentWorkflowBackfillPlan, AgentWorkflowBackfillSource,
    AgentWorkflowIndexSchemaVersion, AgentWorkflowMigrationPolicy,
    CURRENT_AGENT_WORKFLOW_INDEX_SCHEMA_VERSION,
};
pub use otlp::{
    AgentOtelResource, AgentOtelSpanExport, AgentOtlpBridgeExport, AgentOtlpBridgeReceiver,
    AgentOtlpError, AgentOtlpExporterConfig, AgentOtlpProtocol, AgentOtlpReceiverFuture,
    AgentOtlpResult, AgentOtlpSignal, InMemoryAgentOtlpReceiver, DEFAULT_AGENT_OTLP_GRPC_ENDPOINT,
    DEFAULT_AGENT_OTLP_HTTP_ENDPOINT, OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_HEADERS,
    OTEL_EXPORTER_OTLP_LOGS_ENDPOINT, OTEL_EXPORTER_OTLP_METRICS_ENDPOINT,
    OTEL_EXPORTER_OTLP_PROTOCOL, OTEL_EXPORTER_OTLP_TIMEOUT, OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
    OTEL_RESOURCE_CONTAINER_NAME, OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT_NAME,
    OTEL_RESOURCE_K8S_DEPLOYMENT_NAME, OTEL_RESOURCE_K8S_NAMESPACE_NAME,
    OTEL_RESOURCE_K8S_NODE_NAME, OTEL_RESOURCE_K8S_POD_NAME, OTEL_RESOURCE_K8S_POD_UID,
    OTEL_RESOURCE_RAKKA_NODE_ID, OTEL_RESOURCE_SERVICE_INSTANCE_ID, OTEL_RESOURCE_SERVICE_NAME,
    OTEL_RESOURCE_SERVICE_NAMESPACE, OTEL_RESOURCE_SERVICE_VERSION,
};
pub use outbox::{
    agent_effect_outbox_target, agent_effect_to_outbox_command,
    agent_timestamp_to_workflow_timestamp, AgentDueEffect, AgentOutboxAcceptance,
    AgentOutboxDuplicateReason, AgentOutboxError, AgentOutboxResult, METRIC_AGENT_OUTBOX_EFFECTS,
};
#[cfg(feature = "postgres")]
pub use postgres_query::{
    PostgresAgentWorkflowQueryIndex, PostgresAgentWorkflowQueryIndexBuilder,
    AGENT_WORKFLOW_AUDIT_INDEX_TABLE, AGENT_WORKFLOW_CHECKPOINT_INDEX_TABLE,
    AGENT_WORKFLOW_DISPATCH_INDEX_TABLE, AGENT_WORKFLOW_GRAPH_NODE_INDEX_TABLE,
    AGENT_WORKFLOW_QUERY_MIGRATION_LOCK_ID, AGENT_WORKFLOW_QUERY_MIGRATION_SQL,
    AGENT_WORKFLOW_RUNTIME_EVENT_PROJECTION_TABLE, AGENT_WORKFLOW_RUN_INDEX_TABLE,
    AGENT_WORKFLOW_TIMER_INDEX_TABLE, DEFAULT_AGENT_WORKFLOW_QUERY_NAMESPACE,
};
pub use query::{
    AgentDispatchIndexEntry, AgentDispatchQuery, AgentRunIndexEntry, AgentRunQueryWaitingReason,
    AgentTimerIndexEntry, AgentTimerQuery, AgentWorkflowQueryError, AgentWorkflowQueryFuture,
    AgentWorkflowQueryIndex, AgentWorkflowQueryResult, AgentWorkflowRunQuery,
    AgentWorkflowShardOwnership, InMemoryAgentWorkflowQueryIndex,
};
pub use retention::{
    compact_agent_audit_events, compact_agent_run_state, AgentAuditCompaction,
    AgentRetentionArchiveKind, AgentRetentionArchiveReason, AgentRetentionArchiveRecord,
    AgentRetentionCompactionReport, AgentRetentionPolicy, AgentRunStateCompaction,
};
pub use runner::{
    agent_run_persistence_id, AgentRunEngineError, AgentRunEngineResult, AgentRunTransition,
    AgentRunTransitionKind, AgentRunWaitReason, AgentStepRunner, AgentStepSuccess,
    AGENT_RUN_PERSISTENCE_PREFIX,
};
pub use runtime::{
    AgentGraphRuntime, AgentGraphRuntimeEffectOutcome, AgentGraphRuntimeTransition, AgentRunActor,
    AgentRunActorCommand, AgentRunActorSnapshot, AgentRunRuntimeError, AgentRunRuntimeResult,
};
pub use runtime_events::{
    next_runtime_event_sequence, validate_runtime_event, validate_runtime_event_follows,
    AgentRuntimeEvent, AgentRuntimeEventAcceptance, AgentRuntimeEventCorrelationFields,
    AgentRuntimeEventDraft, AgentRuntimeEventError, AgentRuntimeEventKind,
    AgentRuntimeEventProjection, AgentRuntimeEventResult, AgentRuntimeEventSink,
    AgentRuntimeEventSinkFuture, AgentRuntimeEventWriteStatus, InMemoryAgentRuntimeEventSink,
};
#[cfg(feature = "sharding")]
pub use sharding::{
    agent_run_entity_id, agent_run_entity_ref, agent_run_entity_type_key, forget_agent_run,
    init_agent_run_sharding, init_agent_run_sharding_with_clock_and_metrics,
    init_agent_run_sharding_with_metrics, passivate_agent_run, registered_agent_run_entity_ref,
    AgentRunEntityRef, AgentRunEntityRegistration, AgentRunEntityTypeKey, AgentRunShardingSettings,
    DEFAULT_AGENT_RUN_ENTITY_TYPE,
};
#[cfg(feature = "http")]
pub use snapshots::register_agent_workflow_operational_snapshots;
#[cfg(all(feature = "http", feature = "sharding"))]
pub use snapshots::register_agent_workflow_shard_snapshot;
#[cfg(feature = "sharding")]
pub use snapshots::{
    agent_workflow_shards_snapshot, AgentWorkflowShardEntityTypeSnapshot,
    AgentWorkflowShardSnapshot,
};
pub use snapshots::{
    AgentRunHumanCheckpointSnapshot, AgentRunOperationalSnapshot, AgentRunOutboxSnapshot,
    AgentRunRecoveryErrorSnapshot, AgentRunStatusCount, AgentWorkflowHumanCheckpointSnapshot,
    AgentWorkflowOutboxSnapshot, AgentWorkflowRecoverySnapshot, AgentWorkflowRuntimeSnapshot,
    AgentWorkflowSnapshotRegistry, SNAPSHOT_AGENT_WORKFLOW_HUMAN_CHECKPOINTS,
    SNAPSHOT_AGENT_WORKFLOW_OUTBOX, SNAPSHOT_AGENT_WORKFLOW_RECOVERY,
    SNAPSHOT_AGENT_WORKFLOW_RUNTIME, SNAPSHOT_AGENT_WORKFLOW_SHARDS,
};
pub use timers::{
    agent_timer_store_persistence_id, timer_fired_command, AgentTimerEntry, AgentTimerError,
    AgentTimerFiring, AgentTimerPolicy, AgentTimerResult, AgentTimerScan, AgentTimerScanner,
    AgentTimerScannerSettings, AgentTimerStatus, AgentTimerStore, AgentTimerStoreState,
    AGENT_TIMER_PERSISTENCE_PREFIX, DEFAULT_AGENT_TIMER_STORE_ID, METRIC_AGENT_TIMERS,
};
pub use trace_context::{
    agent_child_telemetry_context, agent_durable_resume_telemetry_context,
    extract_agent_trace_context, inject_agent_trace_context, parse_agent_trace_context,
    require_agent_trace_context, validate_agent_span_link, validate_agent_telemetry_context,
    AgentTraceContext, AgentTraceError, AgentTraceResult, TRACEPARENT_HEADER, TRACESTATE_HEADER,
};
pub use triggers::{
    trigger_cancel_run_command, trigger_human_decision_command, trigger_retry_run_command,
    trigger_start_run_command, trigger_submit_signal_command, AgentTriggerCommandBuilder,
    AgentTriggerSource, AgentTriggerSourceError, AgentTriggerSourceKind, AgentTriggerSourceResult,
    AGENT_TRIGGER_DEPLOYMENT_CHANNEL_ATTRIBUTE, AGENT_TRIGGER_KIND_ATTRIBUTE,
    AGENT_TRIGGER_TENANT_TIER_ATTRIBUTE,
};

/// Crate name used in diagnostics, docs, and feature-boundary notes.
pub const CRATE_NAME: &str = "rakka-agent-workflow";

/// Lower-level durable workflow substrate re-exports.
///
/// These items remain owned by `rakka-workflow`. They are exposed here so the
/// agent facade can compose the durable inbox/outbox substrate without moving
/// or redefining its reliability boundary.
pub mod substrate {
    pub use rakka_workflow::{
        DeduplicationKey, DurableInbox, InboxAcceptance, InboxCommand, InboxEntry, InboxStatus,
        ManualWorkflowClock, OutboxAcceptance, OutboxCommand, OutboxDispatchFuture,
        OutboxDispatchResult, OutboxDispatcher, OutboxEntry, OutboxFailureTransition,
        OutboxMessageId, OutboxStatus, OutboxTarget, RetryAttempt, RetryJitter, RetryPolicy,
        SystemWorkflowClock, WorkflowClock, WorkflowError, WorkflowId, WorkflowMessageId,
        WorkflowResult, WorkflowState, WorkflowStateCompactionPolicy,
        WorkflowStateCompactionReport, WorkflowStatus, WorkflowTelemetryEvent, WorkflowTimestamp,
    };
}

/// Common imports for early agent workflow consumers.
///
/// Later phases should add first-class agent domain types here as they become
/// stable enough for application code.
pub mod prelude {
    pub use crate::{
        agent_audit_artifact_refs, agent_audit_event_kind_label, agent_audit_log_event_name,
        agent_autoscaling_signal, agent_child_telemetry_context, agent_dispatch_id,
        agent_dispatch_timestamp_from_workflow_timestamp,
        agent_dispatch_timestamp_to_workflow_timestamp, agent_dispatcher_fleet_persistence_id,
        agent_durable_resume_telemetry_context, agent_effect_artifact_refs,
        agent_log_event_from_audit_event, agent_metric_instrument, agent_run_artifact_refs,
        agent_timer_store_persistence_id, compact_agent_audit_events, compact_agent_run_state,
        extract_agent_trace_context, human_decision_command, inject_agent_trace_context,
        is_agent_autoscaling_metric, is_bounded_agent_metric_attribute,
        is_forbidden_agent_metric_attribute, next_runtime_event_sequence,
        parse_agent_trace_context, plan_agent_workflow_index_backfill, record_agent_counter,
        record_agent_gauge, record_agent_histogram, repair_agent_workflow_index,
        require_agent_trace_context, timer_fired_command, trigger_cancel_run_command,
        trigger_human_decision_command, trigger_retry_run_command, trigger_start_run_command,
        trigger_submit_signal_command, validate_agent_audit_event, validate_agent_log_event,
        validate_agent_metric_attributes, validate_agent_span_link,
        validate_agent_telemetry_context, validate_artifact_ref, validate_effect_artifact_policy,
        validate_inline_state, validate_run_state_artifact_policy, validate_runtime_event,
        validate_runtime_event_follows, AgentAdapterError, AgentAdapterFailureClass,
        AgentAdapterFuture, AgentAdapterOutcome, AgentAdapterReceipt, AgentAdapterRequestMetadata,
        AgentAdapterResult, AgentAdapterUsage, AgentArtifactError, AgentArtifactPolicy,
        AgentArtifactRead, AgentArtifactResult, AgentArtifactStore, AgentArtifactStoreFuture,
        AgentArtifactWriteRequest, AgentAuditAcceptance, AgentAuditCompaction, AgentAuditError,
        AgentAuditEvent, AgentAuditEventId, AgentAuditEventKind, AgentAuditQuery, AgentAuditResult,
        AgentAuditSink, AgentAuditSinkFuture, AgentAuditWriteStatus, AgentAutoscalingSignal,
        AgentAutoscalingSignalRole, AgentCausationId, AgentCommand, AgentCommandId,
        AgentCommandKind, AgentCommandMetadata, AgentCorrelationId, AgentDeduplicationKey,
        AgentDispatchClaim, AgentDispatchClaimBatch, AgentDispatchCompletion,
        AgentDispatchConcurrencyLimits, AgentDispatchEntry, AgentDispatchId,
        AgentDispatchIndexEntry, AgentDispatchJob, AgentDispatchLease, AgentDispatchQuery,
        AgentDispatchStatus, AgentDispatchTargetClass, AgentDispatcherCycle,
        AgentDispatcherEntrySnapshot, AgentDispatcherError, AgentDispatcherFleet,
        AgentDispatcherFleetSettings, AgentDispatcherFleetState, AgentDispatcherRegistration,
        AgentDispatcherResult, AgentDispatcherSnapshot, AgentDispatcherStatusCount,
        AgentDispatcherTargetClassCount, AgentDispatcherWorker, AgentDispatcherWorkerId,
        AgentDueEffect, AgentDurabilityMetadata, AgentEffect, AgentEffectDispatchFuture,
        AgentEffectDispatcher, AgentEffectId, AgentEffectKind, AgentEffectMetadata,
        AgentEffectSchedule, AgentEffectStatus, AgentEffectTarget, AgentFacadeError,
        AgentFacadeResult, AgentGraphRuntime, AgentGraphRuntimeEffectOutcome,
        AgentGraphRuntimeTransition, AgentHumanApprovalRequest, AgentHumanCheckpointError,
        AgentHumanCheckpointOpening, AgentHumanCheckpointResult, AgentHumanCheckpointRuntime,
        AgentHumanDecisionResult, AgentHumanDecisionSubmission, AgentIdempotencyKey,
        AgentInboxAcceptance, AgentInboxDuplicateReason, AgentInboxError, AgentInboxResult,
        AgentInstrumentationScope, AgentLogEvent, AgentLogSeverity, AgentMetricError,
        AgentMetricInstrument, AgentMetricResult, AgentMigrationAssessment, AgentMigrationDecision,
        AgentMigrationReason, AgentModelAdapter, AgentModelRequest, AgentOtelResource,
        AgentOtelSpanExport, AgentOtlpBridgeExport, AgentOtlpBridgeReceiver, AgentOtlpError,
        AgentOtlpExporterConfig, AgentOtlpProtocol, AgentOtlpReceiverFuture, AgentOtlpResult,
        AgentOtlpSignal, AgentOutboxAcceptance, AgentOutboxDuplicateReason, AgentOutboxError,
        AgentOutboxResult, AgentPayload, AgentPayloadDescriptor, AgentRedactionPolicy,
        AgentRetentionArchiveKind, AgentRetentionArchiveReason, AgentRetentionArchiveRecord,
        AgentRetentionCompactionReport, AgentRetentionPolicy, AgentRunActor, AgentRunActorCommand,
        AgentRunActorSnapshot, AgentRunEngineError, AgentRunEngineResult,
        AgentRunHumanCheckpointSnapshot, AgentRunId, AgentRunInbox, AgentRunIndexEntry,
        AgentRunQueryWaitingReason, AgentRunRuntimeError, AgentRunRuntimeResult, AgentRunState,
        AgentRunStateCompaction, AgentRunStatus, AgentRunTransition, AgentRunTransitionKind,
        AgentRunWaitReason, AgentRuntimeEvent, AgentRuntimeEventAcceptance,
        AgentRuntimeEventCorrelationFields, AgentRuntimeEventDraft, AgentRuntimeEventError,
        AgentRuntimeEventKind, AgentRuntimeEventProjection, AgentRuntimeEventResult,
        AgentRuntimeEventSink, AgentRuntimeEventSinkFuture, AgentRuntimeEventWriteStatus,
        AgentStatePayload, AgentStep, AgentStepId, AgentStepKind, AgentStepRunner,
        AgentStepSuccess, AgentTelemetryContext, AgentTenantId, AgentTimerEntry, AgentTimerError,
        AgentTimerFiring, AgentTimerId, AgentTimerIndexEntry, AgentTimerPolicy, AgentTimerQuery,
        AgentTimerResult, AgentTimerScan, AgentTimerScanner, AgentTimerScannerSettings,
        AgentTimerStatus, AgentTimerStore, AgentTimerStoreState, AgentToolAdapter,
        AgentToolRequest, AgentTraceContext, AgentTraceError, AgentTraceResult,
        AgentTriggerCommandBuilder, AgentTriggerSource, AgentTriggerSourceError,
        AgentTriggerSourceKind, AgentTriggerSourceResult, AgentWorkflow,
        AgentWorkflowBackfillAction, AgentWorkflowBackfillItem, AgentWorkflowBackfillPlan,
        AgentWorkflowBackfillSource, AgentWorkflowHumanCheckpointSnapshot, AgentWorkflowId,
        AgentWorkflowIndexSchemaVersion, AgentWorkflowMigrationPolicy, AgentWorkflowOutboxSnapshot,
        AgentWorkflowQueryError, AgentWorkflowQueryFuture, AgentWorkflowQueryIndex,
        AgentWorkflowQueryResult, AgentWorkflowRecoverySnapshot, AgentWorkflowRegistry,
        AgentWorkflowRegistryError, AgentWorkflowRunQuery, AgentWorkflowRuntimeSnapshot,
        AgentWorkflowShardOwnership, AgentWorkflowSnapshotRegistry, ArtifactEncryptionRef,
        ArtifactKind, ArtifactRef, HumanCheckpoint, HumanCheckpointId, HumanCheckpointStatus,
        HumanDecisionOption, InMemoryAgentAuditSink, InMemoryAgentOtlpReceiver,
        InMemoryAgentRuntimeEventSink, InMemoryAgentWorkflowQueryIndex, RedactionStatus,
        StateSchemaVersion, WorkflowDefinitionVersion, AGENT_DISPATCHER_FLEET_PERSISTENCE_PREFIX,
        AGENT_LOG_ATTR_AUDIT_EVENT_ID, AGENT_LOG_ATTR_AUDIT_KIND, AGENT_LOG_ATTR_CAUSATION_ID,
        AGENT_LOG_ATTR_CHECKPOINT_ID, AGENT_LOG_ATTR_COMMAND_ID, AGENT_LOG_ATTR_CORRELATION_ID,
        AGENT_LOG_ATTR_DEFINITION_VERSION, AGENT_LOG_ATTR_EFFECT_ID, AGENT_LOG_ATTR_REDACTION,
        AGENT_LOG_ATTR_RUN_ID, AGENT_LOG_ATTR_STEP_ID, AGENT_LOG_ATTR_TENANT_ID,
        AGENT_LOG_ATTR_WORKFLOW_ID, AGENT_LOG_ATTR_WORKFLOW_TYPE, AGENT_LOG_INSTRUMENTATION_SCOPE,
        AGENT_METRIC_ATTRIBUTE_VALUE_MAX_BYTES, AGENT_METRIC_ATTR_ADAPTER_KIND,
        AGENT_METRIC_ATTR_ARTIFACT_KIND, AGENT_METRIC_ATTR_CHECKPOINT_STATUS,
        AGENT_METRIC_ATTR_COMMAND_TYPE, AGENT_METRIC_ATTR_COMPONENT,
        AGENT_METRIC_ATTR_DATABASE_OPERATION, AGENT_METRIC_ATTR_DEFINITION_VERSION,
        AGENT_METRIC_ATTR_DEPLOYMENT_CHANNEL, AGENT_METRIC_ATTR_DETAIL,
        AGENT_METRIC_ATTR_DIRECTION, AGENT_METRIC_ATTR_EFFECT_KIND, AGENT_METRIC_ATTR_ENTITY_TYPE,
        AGENT_METRIC_ATTR_ERROR_CODE, AGENT_METRIC_ATTR_MESSAGE_TYPE, AGENT_METRIC_ATTR_OPERATION,
        AGENT_METRIC_ATTR_OUTCOME, AGENT_METRIC_ATTR_QUEUE, AGENT_METRIC_ATTR_REDACTION,
        AGENT_METRIC_ATTR_RETRY_ATTEMPT_BUCKET, AGENT_METRIC_ATTR_SIGNAL, AGENT_METRIC_ATTR_STATUS,
        AGENT_METRIC_ATTR_STEP_KIND, AGENT_METRIC_ATTR_TARGET_CLASS, AGENT_METRIC_ATTR_TENANT_TIER,
        AGENT_METRIC_ATTR_TIMER_STATUS, AGENT_METRIC_ATTR_TRANSITION,
        AGENT_METRIC_ATTR_TRIGGER_KIND, AGENT_METRIC_ATTR_WORKFLOW_TYPE,
        AGENT_TIMER_PERSISTENCE_PREFIX, AGENT_TRIGGER_DEPLOYMENT_CHANNEL_ATTRIBUTE,
        AGENT_TRIGGER_KIND_ATTRIBUTE, AGENT_TRIGGER_TENANT_TIER_ATTRIBUTE,
        AGENT_WORKFLOW_AUTOSCALING_SIGNALS, AGENT_WORKFLOW_BOUNDED_METRIC_ATTRIBUTES,
        AGENT_WORKFLOW_METRIC_INSTRUMENTS, CRATE_NAME, CURRENT_AGENT_WORKFLOW_INDEX_SCHEMA_VERSION,
        DEFAULT_AGENT_ARTIFACT_RETENTION_CLASS, DEFAULT_AGENT_DISPATCHER_FLEET_ID,
        DEFAULT_AGENT_INLINE_STATE_LIMIT_BYTES, DEFAULT_AGENT_LOG_BODY_LIMIT_BYTES,
        DEFAULT_AGENT_OTLP_GRPC_ENDPOINT, DEFAULT_AGENT_OTLP_HTTP_ENDPOINT,
        DEFAULT_AGENT_TIMER_STORE_ID, METRIC_AGENT_ACTIVE_RUNS, METRIC_AGENT_DISPATCHER_BACKLOG,
        METRIC_AGENT_DISPATCHER_FLEET, METRIC_AGENT_DISPATCHER_IN_FLIGHT,
        METRIC_AGENT_DISPATCH_LATENCY_MS, METRIC_AGENT_DUE_OUTBOX_EFFECTS,
        METRIC_AGENT_HUMAN_CHECKPOINTS, METRIC_AGENT_HUMAN_WAITING_RUNS,
        METRIC_AGENT_HUMAN_WAIT_LATENCY_MS, METRIC_AGENT_MAILBOX_DEPTH,
        METRIC_AGENT_MODEL_ADAPTER_CALLS, METRIC_AGENT_MODEL_ADAPTER_LATENCY_MS,
        METRIC_AGENT_MODEL_ADAPTER_TOKENS, METRIC_AGENT_PENDING_INBOX_COMMANDS,
        METRIC_AGENT_POSTGRES_LATENCY_MS, METRIC_AGENT_PROCESS_RUNNING,
        METRIC_AGENT_RECOVERY_EVENTS, METRIC_AGENT_RECOVERY_LATENCY_MS,
        METRIC_AGENT_RUN_TRANSITIONS, METRIC_AGENT_SHARD_OWNERSHIP_COUNT,
        METRIC_AGENT_STEP_TRANSITIONS, METRIC_AGENT_STREAM_PRESSURE, METRIC_AGENT_TIMERS,
        METRIC_AGENT_TIMERS_LATE_BY_MS, METRIC_AGENT_TOOL_ADAPTER_CALLS,
        METRIC_AGENT_TOOL_ADAPTER_LATENCY_MS, OTEL_EXPORTER_OTLP_ENDPOINT,
        OTEL_EXPORTER_OTLP_HEADERS, OTEL_EXPORTER_OTLP_LOGS_ENDPOINT,
        OTEL_EXPORTER_OTLP_METRICS_ENDPOINT, OTEL_EXPORTER_OTLP_PROTOCOL,
        OTEL_EXPORTER_OTLP_TIMEOUT, OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
        OTEL_RESOURCE_CONTAINER_NAME, OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT_NAME,
        OTEL_RESOURCE_K8S_DEPLOYMENT_NAME, OTEL_RESOURCE_K8S_NAMESPACE_NAME,
        OTEL_RESOURCE_K8S_NODE_NAME, OTEL_RESOURCE_K8S_POD_NAME, OTEL_RESOURCE_K8S_POD_UID,
        OTEL_RESOURCE_RAKKA_NODE_ID, OTEL_RESOURCE_SERVICE_INSTANCE_ID, OTEL_RESOURCE_SERVICE_NAME,
        OTEL_RESOURCE_SERVICE_NAMESPACE, OTEL_RESOURCE_SERVICE_VERSION,
        SNAPSHOT_AGENT_WORKFLOW_HUMAN_CHECKPOINTS, SNAPSHOT_AGENT_WORKFLOW_OUTBOX,
        SNAPSHOT_AGENT_WORKFLOW_RECOVERY, SNAPSHOT_AGENT_WORKFLOW_RUNTIME,
        SNAPSHOT_AGENT_WORKFLOW_SHARDS, TRACEPARENT_HEADER, TRACESTATE_HEADER,
    };

    #[cfg(feature = "process-tools")]
    pub use crate::ProcessFileWatchToolAdapter;

    #[cfg(feature = "http")]
    pub use crate::{
        human_decision_http_route, AgentHumanDecisionHttpResponse, DEFAULT_HUMAN_DECISION_HTTP_PATH,
    };

    #[cfg(feature = "k8s")]
    pub use crate::{
        default_agent_workflow_required_services, parse_agent_workflow_required_services,
        register_agent_workflow_ingress_stop_task, register_agent_workflow_telemetry_flush_task,
        AgentWorkflowDrainError, AgentWorkflowDrainResult, AgentWorkflowIngressGate,
        AgentWorkflowKubernetesStartup, AgentWorkflowStartupSnapshot, AgentWorkflowStartupStep,
        AGENT_WORKFLOW_FLUSH_TELEMETRY_OPERATION, AGENT_WORKFLOW_FLUSH_TELEMETRY_TASK,
        AGENT_WORKFLOW_SHUTDOWN_OPERATION_ATTR, AGENT_WORKFLOW_STARTUP_ACTOR_SYSTEM,
        AGENT_WORKFLOW_STARTUP_ARTIFACT_STORE, AGENT_WORKFLOW_STARTUP_DURABLE_STATE,
        AGENT_WORKFLOW_STARTUP_OPERATIONAL_SNAPSHOTS, AGENT_WORKFLOW_STARTUP_OTLP_EXPORTER,
        AGENT_WORKFLOW_STARTUP_POSTGRES, AGENT_WORKFLOW_STARTUP_QUERY_INDEX,
        AGENT_WORKFLOW_STARTUP_REMOTING, AGENT_WORKFLOW_STARTUP_SHARDING,
        AGENT_WORKFLOW_STARTUP_TELEMETRY_RESOURCE, AGENT_WORKFLOW_STARTUP_WORKFLOW_REGISTRY,
        AGENT_WORKFLOW_STOP_INGRESS_OPERATION, AGENT_WORKFLOW_STOP_INGRESS_TASK,
        DEFAULT_AGENT_WORKFLOW_STARTUP_STEPS,
    };

    #[cfg(feature = "postgres")]
    pub use crate::{
        PostgresAgentWorkflowQueryIndex, PostgresAgentWorkflowQueryIndexBuilder,
        AGENT_WORKFLOW_AUDIT_INDEX_TABLE, AGENT_WORKFLOW_CHECKPOINT_INDEX_TABLE,
        AGENT_WORKFLOW_DISPATCH_INDEX_TABLE, AGENT_WORKFLOW_GRAPH_NODE_INDEX_TABLE,
        AGENT_WORKFLOW_QUERY_MIGRATION_LOCK_ID, AGENT_WORKFLOW_QUERY_MIGRATION_SQL,
        AGENT_WORKFLOW_RUNTIME_EVENT_PROJECTION_TABLE, AGENT_WORKFLOW_RUN_INDEX_TABLE,
        AGENT_WORKFLOW_TIMER_INDEX_TABLE, DEFAULT_AGENT_WORKFLOW_QUERY_NAMESPACE,
    };

    #[cfg(feature = "sharding")]
    pub use crate::{
        agent_run_entity_id, agent_run_entity_ref, agent_run_entity_type_key, forget_agent_run,
        init_agent_run_sharding, init_agent_run_sharding_with_clock_and_metrics,
        init_agent_run_sharding_with_metrics, passivate_agent_run, registered_agent_run_entity_ref,
        AgentRunEntityRef, AgentRunEntityRegistration, AgentRunEntityTypeKey,
        AgentRunShardingSettings, DEFAULT_AGENT_RUN_ENTITY_TYPE,
    };

    #[cfg(feature = "sharding")]
    pub use crate::{
        agent_workflow_shards_snapshot, AgentWorkflowShardEntityTypeSnapshot,
        AgentWorkflowShardSnapshot,
    };

    #[cfg(feature = "http")]
    pub use crate::register_agent_workflow_operational_snapshots;

    #[cfg(all(feature = "http", feature = "sharding"))]
    pub use crate::register_agent_workflow_shard_snapshot;
}
