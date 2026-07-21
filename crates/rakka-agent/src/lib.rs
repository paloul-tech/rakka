#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Durable agent domain, loop runtime, and provider-neutral model adapter.
//!
//! This crate is the M1 home for the Rakka agent surface: the goal, typed-task,
//! run, evaluation, handoff, delegation, team, moderation, and workflow-tool
//! domain, the typed client, the durable loop runtime, the model adapter trait,
//! the continuous wake controller, the escrow budget ledger, autonomy
//! admission, guardrails, gates, tool binding and dispatch grants, execution
//! policy references, bounded operational queries, memory traits, structured
//! telemetry, and deterministic test support.
//!
//! Only the module map exists today. Each module documents the specification
//! section it implements and the implementation slice that fills it, so the
//! crate shape is reviewable before any behavior lands.
//!
//! # Boundaries
//!
//! `rakka-agent-workflow` remains the durable execution substrate: inbox,
//! outbox, dispatcher, effect bridge, timers, and triggers. This crate adds the
//! agent domain on top of it and does not weaken the reliability boundaries
//! below it. Core actor, remote, and sharded delivery are at-most-once; every
//! stronger agent guarantee is built from durable state, durable inbox
//! acceptance, durable outbox effects, stable operation identifiers, and
//! recovery.
//!
//! Two rules cut across every module and hold from the first commit:
//!
//! - Resolved credentials and secret material are never persisted in durable
//!   state, effects, memory, runtime events, telemetry, or snapshots.
//!   Credentials are resolved at dispatch time and never outlive the attempt.
//! - Every persisted record carries a schema version, and an unsupported
//!   version fails closed rather than being interpreted optimistically.
//!
//! Sibling crates own the rest of the agent surface: `rakka-agent-postgres` the
//! PostgreSQL memory and retrieval adapters, `rakka-agent-knowledge-graph` the
//! communal graph, and the `rakka-a2a` `agents` feature the external protocol
//! boundary. This crate does not depend on any of them.
//!
//! # Features
//!
//! - `rig` (default): the Rig-backed implementation of [`model`]'s adapter
//!   trait, owning the pinned Rig version. The crate builds and passes its
//!   tests with `--no-default-features`, the deterministic [`testkit`] adapter
//!   never requires this feature, and Rig types never appear in the non-`rig`
//!   public API or in persisted state.
//! - `otel`: the pinned OpenTelemetry GenAI semantic-convention mapping over
//!   the existing agent-workflow OTLP bridge. It does not own application
//!   exporter credentials and does not install a global SDK.
//!
//! Both features are propagated by the `rakka` facade as `rakka-agent?/rig` and
//! `rakka-agent?/otel`.

pub mod admission;
pub mod agent;
pub mod budget;
pub mod checkpoints;
pub mod choreography;
pub mod client;
pub mod coordination;
pub mod definition;
pub mod delegation;
pub mod dispatch;
pub mod effect;
pub mod evaluation;
pub mod goal;
pub mod guardrails;
pub mod identity;
pub mod loop_runtime;
pub mod memory;
pub mod model;
pub mod observability;
#[cfg(feature = "otel")]
pub mod otel;
pub mod query;
#[cfg(feature = "rig")]
pub mod rig;
pub mod run;
pub mod schema;
pub mod task;
pub mod testkit;
pub mod tools;
pub mod wake;
pub mod workflow_tool;

pub use admission::{
    AgentAdmissionError, AgentAdmissionEvaluator, AgentAdmissionRefusal, AgentAdmissionRequirement,
    AgentAdmissionResult, AutonomyAdmissionDecision, AGENT_ADMISSION_CONSTRAINT_CAPACITY,
    AGENT_ADMISSION_DETAIL_MAX_LENGTH,
};
pub use agent::{
    agent_entity_id, agent_entity_persistence_id, agent_entity_ref, agent_entity_type_key,
    init_agent_entity_remote_sharding, init_agent_entity_sharding, load_agent_entity_state,
    passivate_agent_entity, registered_agent_entity_ref, AgentAdmissionRetraction, AgentEntity,
    AgentEntityCommand, AgentEntityError, AgentEntityMessage, AgentEntityOutcome, AgentEntityRef,
    AgentEntityRegistration, AgentEntityReply, AgentEntityResult, AgentEntityShardingSettings,
    AgentEntitySnapshot, AgentEntityState, AgentEntityStore, AgentEntityTypeKey,
    AgentLifecycleStatus, AgentOperationLog, AGENT_ENTITY_OPERATION_LOG_CAPACITY,
    DEFAULT_AGENT_ENTITY_TYPE,
};
pub use budget::{
    AgentBudgetAllocation, AgentBudgetConsumption, AgentBudgetDimension, AgentBudgetExhaustion,
    AgentBudgetGrant, AgentBudgetLimits, AgentChildEscrow, AgentEscrowChildId, AgentEscrowError,
    AgentEscrowLedger, AgentEscrowResult, AgentRunBudget, AGENT_ESCROW_CHILD_CAPACITY,
    AGENT_ESCROW_REFUSAL_CHILD_UNKNOWN,
};
pub use checkpoints::{
    AgentApprovalDecision, AgentCheckpoint, AgentCheckpointDecision, AgentCheckpointDecisionOption,
    AgentCheckpointEffectBinding, AgentCheckpointError, AgentCheckpointGrant,
    AgentCheckpointGrantError, AgentCheckpointKind, AgentCheckpointOutcome,
    AgentCheckpointResolutionReport, AgentCheckpointResult, AgentCheckpointSla,
    AgentCheckpointStatus, AgentCheckpointTimerOutcome, AgentCompensationRef,
    AgentReconciliationDecision, AgentRecordedDecision, AGENT_CHECKPOINT_APPLIED_KEY_CAPACITY,
    AGENT_CHECKPOINT_DETAIL_MAX_LENGTH, AGENT_CHECKPOINT_MAX_AUDIT_EVENTS,
    AGENT_CHECKPOINT_MAX_CAPABILITIES, AGENT_CHECKPOINT_MAX_CONTEXT_ARTIFACTS,
    AGENT_CHECKPOINT_MAX_ROLES, AGENT_CHECKPOINT_ROLE_MAX_LENGTH,
    AGENT_CHECKPOINT_SUMMARY_MAX_LENGTH,
};
pub use choreography::{
    drive_pending_exchanges, register_agent_exchange_codecs, AgentChoreographyError,
    AgentChoreographyResult, AgentEntityAddress, AgentEntityClass, AgentExchangeDeliveryError,
    AgentExchangeDeliveryFuture, AgentExchangeDeliveryResult, AgentExchangeDriveReport,
    AgentExchangeEnvelope, AgentExchangeHost, AgentExchangeInitiation, AgentExchangeJournal,
    AgentExchangeKind, AgentExchangeMessage, AgentExchangeParticipant, AgentExchangePayload,
    AgentExchangeReply, AgentExchangeResult, AgentExchangeRouter, AgentExchangeSettlement,
    AgentExchangeState, AgentExchangeStatus, AgentExchangeTransport, PendingExchange,
    ShardedExchangeRoute, AGENT_EXCHANGE_CODEC_ID, AGENT_EXCHANGE_ENVELOPE_TYPE_ID,
    AGENT_EXCHANGE_LOG_CAPACITY, AGENT_EXCHANGE_PAYLOAD_MAX_BYTES, AGENT_EXCHANGE_PENDING_CAPACITY,
    AGENT_EXCHANGE_REMOTE_SCHEMA_VERSION, AGENT_EXCHANGE_REPLY_TYPE_ID,
};
pub use client::{
    AgentClientAgentStatus, AgentClientError, AgentClientFuture, AgentClientManagementCommand,
    AgentClientManagementResponse, AgentClientPollPolicy, AgentClientResult, AgentClientTaskEvent,
    AgentClientTaskRequest, AgentClientTaskState, AgentClientTaskView, AgentClientTransport,
    RakkaAgentClient,
};
pub use dispatch::{
    workflow_run_id, AgentCompensationExecutor, AgentDispatchAuthority, AgentDispatchDecision,
    AgentDispatchError, AgentDispatchFuture, AgentDispatchPass, AgentDispatchProbe,
    AgentDispatchResult, AgentDispatchToolExecutor, AgentDispatchWindow,
    AgentEffectCredentialResolver, AgentEffectReconciler, AgentEntityAuthority,
    AgentReconciliationFinding, AgentRunEffectDispatcher, AgentRunResultDelivery,
    AgentRunSetupResolver, WorkflowAgentRunEffectSink,
};
pub use effect::{
    compensation_call_id, effect_id_for, external_idempotency_key_for, AgentEffectError,
    AgentEffectFuture, AgentEffectGeneration, AgentEffectPolicies, AgentEffectResolution,
    AgentEffectResult, AgentEffectSafety, AgentEffectSpec, AgentExternalIdempotencyKey,
    AgentReconciliationProtocolRef, AgentRunEffect, AgentRunEffectKind, AgentRunEffectOutcome,
    AgentRunEffectRequest, AgentRunEffectSink, AgentRunEffectStatus, AgentToolResult,
    InMemoryAgentRunEffectSink, AGENT_EXTERNAL_IDEMPOTENCY_KEY_MAX_LENGTH,
    AGENT_RUN_MAX_PENDING_EFFECTS, AGENT_TOOL_RESULT_MAX_BYTES, ATTR_AGENT_EFFECT_ARGUMENT_DIGEST,
    ATTR_AGENT_EFFECT_EXECUTION_POLICY, ATTR_AGENT_EFFECT_GENERATION, ATTR_AGENT_EFFECT_ID,
    ATTR_AGENT_EFFECT_RECONCILIATION_PROTOCOL, ATTR_AGENT_EFFECT_SAFETY_CLASS,
    ATTR_AGENT_EFFECT_SETTINGS_REVISION, ATTR_AGENT_TELEMETRY_LINK_KIND,
    LINK_KIND_SUPERSEDED_GENERATION,
};
pub use guardrails::{
    AgentGuardrail, AgentGuardrailBoundary, AgentGuardrailChain, AgentGuardrailContext,
    AgentGuardrailDecision, AgentGuardrailDisposition, AgentGuardrailError, AgentGuardrailOutcome,
    AgentGuardrailReport, AgentGuardrailResult, AgentGuardrailStage, AgentGuardrailTransform,
    AGENT_GUARDRAIL_CONTENT_MAX_BYTES, AGENT_GUARDRAIL_MAX_STAGES,
    AGENT_GUARDRAIL_REASON_MAX_LENGTH,
};
pub use loop_runtime::{
    AgentLoopPhase, AgentLoopState, AgentPendingTopUp, AgentRunProposal,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
pub use memory::{
    assemble_session_context, check_memory_schema, AgentContextSnapshotId, AgentContextSnapshotRef,
    AgentPrivateMemory, AgentPrivateMemoryId, AgentPrivateMemoryKind, AgentPrivateMemoryStore,
    AgentRunMemory, ContextSnapshotStore, InMemoryContextSnapshotStore, InMemorySessionMemoryStore,
    MemoryClassification, MemoryContextSnapshot, MemoryEntryId, MemoryEntryRole, MemoryError,
    MemoryFuture, MemoryOperationId, MemorySequence, MemoryTrust, PrivateMemoryScope,
    SessionMemoryCursor, SessionMemoryEntry, SessionMemoryPage, SessionMemoryStore,
    SessionWindowPolicy, SnapshotBudget, SnapshotRetrieval, SnapshotSessionEntry,
    AGENT_SESSION_MEMORY_ENTRY_MAX_BYTES, AGENT_SESSION_WINDOW_MAX_ENTRIES,
};
pub use model::{
    AgentModelAdapter, AgentModelError, AgentModelFuture, AgentModelRequest, AgentModelResult,
    AgentModelRetryPolicy, AgentModelTurn, AgentModelUsage, AgentToolCallId, AgentToolCallRequest,
    AGENT_MODEL_MAX_TOOL_CALLS, AGENT_MODEL_TEXT_MAX_LENGTH, AGENT_MODEL_TURN_MAX_BYTES,
    AGENT_TOOL_ARGUMENTS_MAX_BYTES,
};
pub use observability::{
    sanitize_agent_telemetry_context, AgentDecisionDraft, AgentDecisionEvent,
    AgentDecisionEventSink, AgentDecisionKind, AgentDecisionSource, AgentDecisionWriteStatus,
    AgentObservabilityError, AgentObservabilityFuture, AgentObservabilityResult,
    InMemoryAgentDecisionEventSink, AGENT_DECISION_EVENT_RETENTION,
    AGENT_DECISION_REASON_MAX_LENGTH, AGENT_TELEMETRY_MAX_SPAN_LINKS,
};
pub use run::{
    agent_run_entity_id, agent_run_entity_persistence_id, agent_run_entity_ref,
    agent_run_entity_type_key, init_agent_run_entity_remote_sharding,
    init_agent_run_entity_sharding, ledger_operation_id, load_agent_run_state,
    passivate_agent_run_entity, proposal_operation_id, registered_agent_run_entity_ref,
    system_run_clock, AgentRun, AgentRunClock, AgentRunEntity, AgentRunEntityCommand,
    AgentRunEntityMessage, AgentRunEntityRef, AgentRunEntityRegistration, AgentRunEntityReply,
    AgentRunEntityShardingSettings, AgentRunEntityStore, AgentRunEntityTypeKey, AgentRunError,
    AgentRunOperationLog, AgentRunOutcome, AgentRunParticipant, AgentRunProgress, AgentRunResult,
    AgentRunSettlementStatus, AgentRunSnapshot, AgentRunState, AgentRunStatus,
    AgentRunTerminalReason, AGENT_RUN_DETAIL_MAX_LENGTH, AGENT_RUN_MATERIALIZED_MAX_BYTES,
    AGENT_RUN_MAX_LOOP_STEPS_PER_PASS, AGENT_RUN_MAX_SETTLE_ROUNDS,
    AGENT_RUN_OPERATION_LOG_CAPACITY, AGENT_RUN_STATE_GROWTH_RESERVE_BYTES,
    DEFAULT_AGENT_RUN_ENTITY_TYPE,
};

pub use definition::{
    effective_settings_for_turn, AgentAuthorityEnvelope, AgentBudgetCeilings, AgentCapabilityId,
    AgentCoordinationCapabilityKind, AgentCredentialBindingRef, AgentDefinition,
    AgentDefinitionError, AgentDefinitionId, AgentDefinitionResult, AgentDefinitionRevision,
    AgentEffectSafetyClass, AgentEnvelopeDimension, AgentEnvelopeViolation,
    AgentExecutionPolicyRef, AgentGuardrailStageId, AgentModelProfileId, AgentOperationClass,
    AgentPolicyRef, AgentPolicyRefs, AgentRevisionNumber, AgentRevisionProvenance,
    AgentSamplingSettings, AgentSettings, AgentSettingsChange, AgentSetupRevision,
    AgentTaskDefinitionId, AgentToolDeclaration, AgentToolId, AgentWorkflowToolId,
    SettingsRevision, SettingsTimingClass, AGENT_DESCRIPTION_MAX_LENGTH,
    AGENT_SETTINGS_MAX_CHANGES,
};
pub use identity::{
    validate_identity_segment, validate_tenant, AgentDelegationId, AgentEnvironmentRef,
    AgentGoalId, AgentId, AgentIdentityError, AgentIdentityResult, AgentMemoryNamespace,
    AgentOperationId, AgentOperationKind, AgentRunBinding, AgentRunId, AgentRunScope, AgentScope,
    AgentTaskId, AgentTaskScope, AgentWakeId, KnowledgeSpaceId, TenantId,
    AGENT_ENTITY_PERSISTENCE_PREFIX, AGENT_IDENTITY_MAX_LENGTH, AGENT_MEMORY_NAMESPACE_PREFIX,
    AGENT_PERSISTENCE_SEPARATOR, AGENT_RUN_ENTITY_PERSISTENCE_PREFIX, AGENT_SCOPE_SEPARATOR,
    AGENT_TASK_ENTITY_PERSISTENCE_PREFIX,
};
pub use schema::{
    previous_schema_version, AgentRecordKind, AgentSchemaCompatibility, AgentSchemaError,
    AgentSchemaPolicy, AgentSchemaResult, VersionedAgentRecord,
    CURRENT_AGENT_CHECKPOINT_SCHEMA_VERSION, CURRENT_AGENT_DECISION_EVENT_SCHEMA_VERSION,
    CURRENT_AGENT_DEFINITION_SCHEMA_VERSION, CURRENT_AGENT_ENTITY_STATE_SCHEMA_VERSION,
    CURRENT_AGENT_EXCHANGE_ENVELOPE_SCHEMA_VERSION, CURRENT_AGENT_EXCHANGE_JOURNAL_SCHEMA_VERSION,
    CURRENT_AGENT_EXCHANGE_REPLY_SCHEMA_VERSION, CURRENT_AGENT_LOOP_STATE_SCHEMA_VERSION,
    CURRENT_AGENT_MODEL_TURN_SCHEMA_VERSION, CURRENT_AGENT_RUN_EFFECT_SCHEMA_VERSION,
    CURRENT_AGENT_RUN_STATE_SCHEMA_VERSION, CURRENT_AGENT_SETTINGS_SCHEMA_VERSION,
    CURRENT_AGENT_SETUP_SCHEMA_VERSION, CURRENT_AGENT_TASK_DEFINITION_SCHEMA_VERSION,
    CURRENT_AGENT_TASK_HISTORY_SCHEMA_VERSION, CURRENT_AGENT_TASK_STATE_SCHEMA_VERSION,
};
pub use task::{
    agent_task_entity_id, agent_task_entity_persistence_id, agent_task_entity_ref,
    agent_task_entity_type_key, assignment_operation_id, init_agent_task_entity_remote_sharding,
    init_agent_task_entity_sharding, load_agent_task_state, passivate_agent_task_entity,
    registered_agent_task_entity_ref, run_id_for_assignment, system_task_clock,
    AgentAcceptedResult, AgentAssignmentGeneration, AgentAssignmentReadiness,
    AgentAssignmentRefusal, AgentAssignmentRefusalReason, AgentAssignmentStatus,
    AgentBudgetLedgerOutcome, AgentBudgetReturn, AgentBudgetSettlement, AgentBudgetTopUpRequest,
    AgentContentDigest, AgentDependencyFailurePolicy, AgentDigestAlgorithm, AgentRunAcceptance,
    AgentRunAssignment, AgentSchemaId, AgentSchemaRef, AgentTask, AgentTaskClock, AgentTaskContent,
    AgentTaskCreation, AgentTaskDecision, AgentTaskDefinition, AgentTaskDependency,
    AgentTaskDependencyDeclaration, AgentTaskDependencyOutcome, AgentTaskEntity,
    AgentTaskEntityCommand, AgentTaskEntityMessage, AgentTaskEntityRef,
    AgentTaskEntityRegistration, AgentTaskEntityReply, AgentTaskEntityShardingSettings,
    AgentTaskEntityStore, AgentTaskEntityTypeKey, AgentTaskError, AgentTaskHistoryCursor,
    AgentTaskHistoryEntry, AgentTaskHistoryFuture, AgentTaskHistoryKind, AgentTaskHistoryPage,
    AgentTaskHistorySequence, AgentTaskHistoryStore, AgentTaskLimits, AgentTaskOperationLog,
    AgentTaskOutcome, AgentTaskOwnership, AgentTaskParticipant, AgentTaskProgress,
    AgentTaskRejection, AgentTaskRejectionCause, AgentTaskResult, AgentTaskResultCheck,
    AgentTaskResultProposal, AgentTaskResultRule, AgentTaskRuleId, AgentTaskSnapshot,
    AgentTaskState, AgentTaskStatus, AgentTaskTerminalReason, InMemoryAgentTaskHistoryStore,
    TypedTask, AGENT_BUDGET_LEDGER_OUTCOME_PAYLOAD_TYPE, AGENT_BUDGET_RETURN_PAYLOAD_TYPE,
    AGENT_BUDGET_SETTLEMENT_PAYLOAD_TYPE, AGENT_BUDGET_TOP_UP_PAYLOAD_TYPE,
    AGENT_RUN_ACCEPTANCE_PAYLOAD_TYPE, AGENT_RUN_ASSIGNMENT_PAYLOAD_TYPE,
    AGENT_TASK_ASSIGNABLE_ID_MAX_LENGTH, AGENT_TASK_CREATION_OUTCOME_PAYLOAD_TYPE,
    AGENT_TASK_CREATION_PAYLOAD_TYPE, AGENT_TASK_DECISION_PAYLOAD_TYPE,
    AGENT_TASK_DESCRIPTION_MAX_LENGTH, AGENT_TASK_DETAIL_MAX_LENGTH,
    AGENT_TASK_HISTORY_DEFAULT_PAGE_SIZE, AGENT_TASK_HISTORY_MAX_PAGE_SIZE,
    AGENT_TASK_INLINE_CONTENT_MAX_BYTES, AGENT_TASK_MATERIALIZED_MAX_BYTES,
    AGENT_TASK_MAX_DEPENDENCIES, AGENT_TASK_MAX_DEPENDENCY_DEPTH,
    AGENT_TASK_MAX_EVIDENCE_ARTIFACTS, AGENT_TASK_MAX_HISTORY_PER_TRANSITION,
    AGENT_TASK_MAX_RESULT_RULES, AGENT_TASK_OPERATION_LOG_CAPACITY,
    AGENT_TASK_PENDING_HISTORY_CAPACITY, AGENT_TASK_RESULT_PROPOSAL_PAYLOAD_TYPE,
    AGENT_TASK_RULE_ONE_OF_MAX_VALUES, AGENT_TASK_RULE_POINTER_MAX_LENGTH,
    AGENT_TASK_RULE_VALUE_MAX_LENGTH, AGENT_TASK_STATE_GROWTH_RESERVE_BYTES,
    DEFAULT_AGENT_TASK_ENTITY_TYPE,
};
pub use tools::{
    AgentAuthorityContext, AgentAuthorityRefusal, AgentDispatchGrant, AgentExecutionPolicyRouter,
    AgentGrantDescriptor, AgentGrantedDispatch, AgentToolAuthority, AgentToolBinding,
    AgentToolDescriptor, AgentToolError, AgentToolKind, AgentToolRegistry, AgentToolResultBehavior,
    AGENT_DISPATCH_GRANT_DEFAULT_TTL_MS, AGENT_EVALUATED_GUARDRAIL_BOUNDARIES,
    AGENT_TOOL_DESCRIPTION_MAX_LENGTH, AGENT_TOOL_PARAMETERS_MAX_BYTES,
    AGENT_TOOL_REGISTRY_MAX_TOOLS,
};
