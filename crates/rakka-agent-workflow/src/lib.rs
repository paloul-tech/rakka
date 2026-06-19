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
pub mod checkpoints;
pub mod definition;
pub mod dispatcher;
pub mod domain;
pub mod facade;
pub mod inbox;
pub mod outbox;
pub mod runner;
pub mod runtime;
#[cfg(feature = "sharding")]
pub mod sharding;
pub mod snapshots;
#[cfg(feature = "testkit")]
pub mod testkit;
pub mod timers;

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
pub use definition::{
    AgentPayload, AgentWorkflowKey, AgentWorkflowRegistry, AgentWorkflowRegistryError,
    AgentWorkflowRegistryResult,
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
    AgentWorkflowId, ArtifactKind, ArtifactRef, HumanCheckpoint, HumanCheckpointId,
    HumanCheckpointStatus, HumanDecisionOption, InlineState, PrincipalRef, RedactionStatus,
    StateSchemaVersion, WorkflowDefinitionVersion, BOUNDED_METRIC_FIELDS,
    FORBIDDEN_HOT_METRIC_FIELDS, TRACE_LOG_AUDIT_ID_FIELDS,
};
pub use facade::{
    validate_command, validate_command_metadata, validate_effect_metadata,
    validate_effect_schedule, AgentCommand, AgentCommandKind, AgentCommandMetadata,
    AgentDurabilityMetadata, AgentEffectMetadata, AgentEffectSchedule, AgentFacadeError,
    AgentFacadeResult,
};
pub use inbox::{
    agent_run_workflow_id, AgentInboxAcceptance, AgentInboxDuplicateReason, AgentInboxError,
    AgentInboxResult, AgentRunInbox, METRIC_AGENT_INBOX_COMMANDS,
};
pub use outbox::{
    agent_effect_outbox_target, agent_effect_to_outbox_command,
    agent_timestamp_to_workflow_timestamp, AgentDueEffect, AgentOutboxAcceptance,
    AgentOutboxDuplicateReason, AgentOutboxError, AgentOutboxResult, METRIC_AGENT_OUTBOX_EFFECTS,
};
pub use runner::{
    agent_run_persistence_id, AgentRunEngineError, AgentRunEngineResult, AgentRunTransition,
    AgentRunTransitionKind, AgentRunWaitReason, AgentStepRunner, AgentStepSuccess,
    AGENT_RUN_PERSISTENCE_PREFIX,
};
pub use runtime::{
    AgentRunActor, AgentRunActorCommand, AgentRunActorSnapshot, AgentRunRuntimeError,
    AgentRunRuntimeResult,
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
        WorkflowResult, WorkflowState, WorkflowStatus, WorkflowTelemetryEvent, WorkflowTimestamp,
    };
}

/// Common imports for early agent workflow consumers.
///
/// Later phases should add first-class agent domain types here as they become
/// stable enough for application code.
pub mod prelude {
    pub use crate::{
        agent_dispatch_id, agent_dispatch_timestamp_from_workflow_timestamp,
        agent_dispatch_timestamp_to_workflow_timestamp, agent_dispatcher_fleet_persistence_id,
        agent_timer_store_persistence_id, human_decision_command, timer_fired_command,
        AgentAdapterError, AgentAdapterFailureClass, AgentAdapterFuture, AgentAdapterOutcome,
        AgentAdapterReceipt, AgentAdapterRequestMetadata, AgentAdapterResult, AgentAdapterUsage,
        AgentAuditEvent, AgentAuditEventId, AgentAuditEventKind, AgentCausationId, AgentCommand,
        AgentCommandId, AgentCommandKind, AgentCommandMetadata, AgentCorrelationId,
        AgentDeduplicationKey, AgentDispatchClaim, AgentDispatchClaimBatch,
        AgentDispatchCompletion, AgentDispatchConcurrencyLimits, AgentDispatchEntry,
        AgentDispatchId, AgentDispatchJob, AgentDispatchLease, AgentDispatchStatus,
        AgentDispatchTargetClass, AgentDispatcherCycle, AgentDispatcherEntrySnapshot,
        AgentDispatcherError, AgentDispatcherFleet, AgentDispatcherFleetSettings,
        AgentDispatcherFleetState, AgentDispatcherRegistration, AgentDispatcherResult,
        AgentDispatcherSnapshot, AgentDispatcherStatusCount, AgentDispatcherTargetClassCount,
        AgentDispatcherWorker, AgentDispatcherWorkerId, AgentDueEffect, AgentDurabilityMetadata,
        AgentEffect, AgentEffectDispatchFuture, AgentEffectDispatcher, AgentEffectId,
        AgentEffectKind, AgentEffectMetadata, AgentEffectSchedule, AgentEffectStatus,
        AgentEffectTarget, AgentFacadeError, AgentFacadeResult, AgentHumanApprovalRequest,
        AgentHumanCheckpointError, AgentHumanCheckpointOpening, AgentHumanCheckpointResult,
        AgentHumanCheckpointRuntime, AgentHumanDecisionResult, AgentHumanDecisionSubmission,
        AgentIdempotencyKey, AgentInboxAcceptance, AgentInboxDuplicateReason, AgentInboxError,
        AgentInboxResult, AgentModelAdapter, AgentModelRequest, AgentOutboxAcceptance,
        AgentOutboxDuplicateReason, AgentOutboxError, AgentOutboxResult, AgentPayload,
        AgentPayloadDescriptor, AgentRunActor, AgentRunActorCommand, AgentRunActorSnapshot,
        AgentRunEngineError, AgentRunEngineResult, AgentRunHumanCheckpointSnapshot, AgentRunId,
        AgentRunInbox, AgentRunRuntimeError, AgentRunRuntimeResult, AgentRunState, AgentRunStatus,
        AgentRunTransition, AgentRunTransitionKind, AgentRunWaitReason, AgentStatePayload,
        AgentStep, AgentStepId, AgentStepKind, AgentStepRunner, AgentStepSuccess,
        AgentTelemetryContext, AgentTenantId, AgentTimerEntry, AgentTimerError, AgentTimerFiring,
        AgentTimerId, AgentTimerPolicy, AgentTimerResult, AgentTimerScan, AgentTimerScanner,
        AgentTimerScannerSettings, AgentTimerStatus, AgentTimerStore, AgentTimerStoreState,
        AgentToolAdapter, AgentToolRequest, AgentWorkflow, AgentWorkflowHumanCheckpointSnapshot,
        AgentWorkflowId, AgentWorkflowOutboxSnapshot, AgentWorkflowRecoverySnapshot,
        AgentWorkflowRegistry, AgentWorkflowRegistryError, AgentWorkflowRuntimeSnapshot,
        AgentWorkflowSnapshotRegistry, ArtifactKind, ArtifactRef, HumanCheckpoint,
        HumanCheckpointId, HumanCheckpointStatus, HumanDecisionOption, RedactionStatus,
        StateSchemaVersion, WorkflowDefinitionVersion, AGENT_DISPATCHER_FLEET_PERSISTENCE_PREFIX,
        AGENT_TIMER_PERSISTENCE_PREFIX, CRATE_NAME, DEFAULT_AGENT_DISPATCHER_FLEET_ID,
        DEFAULT_AGENT_TIMER_STORE_ID, METRIC_AGENT_DISPATCHER_BACKLOG,
        METRIC_AGENT_DISPATCHER_FLEET, METRIC_AGENT_DISPATCHER_IN_FLIGHT,
        METRIC_AGENT_HUMAN_CHECKPOINTS, METRIC_AGENT_HUMAN_WAIT_LATENCY_MS,
        METRIC_AGENT_MODEL_ADAPTER_CALLS, METRIC_AGENT_MODEL_ADAPTER_LATENCY_MS,
        METRIC_AGENT_MODEL_ADAPTER_TOKENS, METRIC_AGENT_TIMERS, METRIC_AGENT_TOOL_ADAPTER_CALLS,
        METRIC_AGENT_TOOL_ADAPTER_LATENCY_MS, SNAPSHOT_AGENT_WORKFLOW_HUMAN_CHECKPOINTS,
        SNAPSHOT_AGENT_WORKFLOW_OUTBOX, SNAPSHOT_AGENT_WORKFLOW_RECOVERY,
        SNAPSHOT_AGENT_WORKFLOW_RUNTIME, SNAPSHOT_AGENT_WORKFLOW_SHARDS,
    };

    #[cfg(feature = "process-tools")]
    pub use crate::ProcessFileWatchToolAdapter;

    #[cfg(feature = "http")]
    pub use crate::{
        human_decision_http_route, AgentHumanDecisionHttpResponse, DEFAULT_HUMAN_DECISION_HTTP_PATH,
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
