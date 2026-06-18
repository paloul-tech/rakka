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

pub mod definition;
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

pub use definition::{
    AgentPayload, AgentWorkflowKey, AgentWorkflowRegistry, AgentWorkflowRegistryError,
    AgentWorkflowRegistryResult,
};
pub use domain::{
    AgentAttributes, AgentAuditEvent, AgentAuditEventId, AgentAuditEventKind, AgentCancellation,
    AgentCausationId, AgentCommandId, AgentCorrelationId, AgentDeduplicationKey, AgentEffect,
    AgentEffectId, AgentEffectKind, AgentEffectStatus, AgentEffectTarget, AgentIdempotencyKey,
    AgentPayloadDescriptor, AgentRunId, AgentRunState, AgentRunStatus, AgentSpanLink,
    AgentStatePayload, AgentStep, AgentStepId, AgentStepKind, AgentTelemetryContext, AgentTenantId,
    AgentTimestampMillis, AgentWorkflow, AgentWorkflowId, ArtifactKind, ArtifactRef,
    HumanCheckpoint, HumanCheckpointId, HumanCheckpointStatus, HumanDecisionOption, InlineState,
    PrincipalRef, RedactionStatus, StateSchemaVersion, WorkflowDefinitionVersion,
    BOUNDED_METRIC_FIELDS, FORBIDDEN_HOT_METRIC_FIELDS, TRACE_LOG_AUDIT_ID_FIELDS,
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
    AgentRunOperationalSnapshot, AgentRunOutboxSnapshot, AgentRunRecoveryErrorSnapshot,
    AgentRunStatusCount, AgentWorkflowOutboxSnapshot, AgentWorkflowRecoverySnapshot,
    AgentWorkflowRuntimeSnapshot, AgentWorkflowSnapshotRegistry, SNAPSHOT_AGENT_WORKFLOW_OUTBOX,
    SNAPSHOT_AGENT_WORKFLOW_RECOVERY, SNAPSHOT_AGENT_WORKFLOW_RUNTIME,
    SNAPSHOT_AGENT_WORKFLOW_SHARDS,
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
        AgentAuditEvent, AgentAuditEventId, AgentAuditEventKind, AgentCausationId, AgentCommand,
        AgentCommandId, AgentCommandKind, AgentCommandMetadata, AgentCorrelationId,
        AgentDeduplicationKey, AgentDueEffect, AgentDurabilityMetadata, AgentEffect, AgentEffectId,
        AgentEffectKind, AgentEffectMetadata, AgentEffectSchedule, AgentEffectStatus,
        AgentEffectTarget, AgentFacadeError, AgentFacadeResult, AgentIdempotencyKey,
        AgentInboxAcceptance, AgentInboxDuplicateReason, AgentInboxError, AgentInboxResult,
        AgentOutboxAcceptance, AgentOutboxDuplicateReason, AgentOutboxError, AgentOutboxResult,
        AgentPayload, AgentPayloadDescriptor, AgentRunActor, AgentRunActorCommand,
        AgentRunActorSnapshot, AgentRunEngineError, AgentRunEngineResult, AgentRunId,
        AgentRunInbox, AgentRunRuntimeError, AgentRunRuntimeResult, AgentRunState, AgentRunStatus,
        AgentRunTransition, AgentRunTransitionKind, AgentRunWaitReason, AgentStatePayload,
        AgentStep, AgentStepId, AgentStepKind, AgentStepRunner, AgentStepSuccess,
        AgentTelemetryContext, AgentTenantId, AgentWorkflow, AgentWorkflowId,
        AgentWorkflowOutboxSnapshot, AgentWorkflowRecoverySnapshot, AgentWorkflowRegistry,
        AgentWorkflowRegistryError, AgentWorkflowRuntimeSnapshot, AgentWorkflowSnapshotRegistry,
        ArtifactKind, ArtifactRef, HumanCheckpoint, HumanCheckpointId, HumanCheckpointStatus,
        HumanDecisionOption, RedactionStatus, StateSchemaVersion, WorkflowDefinitionVersion,
        CRATE_NAME, SNAPSHOT_AGENT_WORKFLOW_OUTBOX, SNAPSHOT_AGENT_WORKFLOW_RECOVERY,
        SNAPSHOT_AGENT_WORKFLOW_RUNTIME, SNAPSHOT_AGENT_WORKFLOW_SHARDS,
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
