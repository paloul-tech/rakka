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
pub mod fan_in;
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
pub mod retrieval;
#[cfg(feature = "rig")]
pub mod rig;
pub mod run;
pub mod schema;
pub mod task;
pub mod testkit;
pub mod tools;
pub mod wake;
pub mod wake_scanner;
pub mod wake_timers;
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
    workflow_run_id, AgentA2aSendExecutor, AgentA2aSendFinding, AgentClaimAppendExecutor,
    AgentClaimAppendFinding, AgentCompensationExecutor, AgentDispatchAuthority,
    AgentDispatchDecision, AgentDispatchError, AgentDispatchFuture, AgentDispatchPass,
    AgentDispatchProbe, AgentDispatchResult, AgentDispatchToolExecutor, AgentDispatchWindow,
    AgentEffectCredentialResolver, AgentEffectReconciler, AgentEntityAuthority,
    AgentGoalEvaluationExecutor, AgentGoalEvaluationFinding, AgentMemoryPromotionExecutor,
    AgentMemoryPromotionFinding, AgentReconciliationFinding, AgentRunEffectDispatcher,
    AgentRunResultDelivery, AgentRunSetupResolver, AgentWorkflowCancelExecutor,
    AgentWorkflowCancelFinding, AgentWorkflowStartExecutor, AgentWorkflowStartFinding,
    SessionMemoryPromotionExecutor, WorkflowAgentRunEffectSink,
};
pub use effect::{
    compensation_call_id, effect_id_for, external_idempotency_key_for, AgentClaimAppendProvenance,
    AgentClaimAppendRequest, AgentClaimObjectRequest, AgentEffectError, AgentEffectFuture,
    AgentEffectGeneration, AgentEffectPolicies, AgentEffectResolution, AgentEffectResult,
    AgentEffectSafety, AgentEffectSpec, AgentExternalIdempotencyKey,
    AgentMemoryConsolidationTarget, AgentMemoryPromotionRequest, AgentReconciliationProtocolRef,
    AgentRunEffect, AgentRunEffectKind, AgentRunEffectOutcome, AgentRunEffectRequest,
    AgentRunEffectSink, AgentRunEffectStatus, AgentToolResult, InMemoryAgentRunEffectSink,
    AGENT_CLAIM_APPEND_DEFAULT_MAX_ATTEMPTS, AGENT_CLAIM_APPEND_MAX_EVIDENCE,
    AGENT_CLAIM_APPEND_OBJECT_MAX_BYTES, AGENT_EXTERNAL_IDEMPOTENCY_KEY_MAX_LENGTH,
    AGENT_MEMORY_PROMOTION_DEFAULT_MAX_ATTEMPTS, AGENT_MEMORY_PROMOTION_MAX_ENTRIES,
    AGENT_RUN_MAX_PENDING_EFFECTS, AGENT_TOOL_RESULT_MAX_BYTES, ATTR_AGENT_EFFECT_ARGUMENT_DIGEST,
    ATTR_AGENT_EFFECT_EXECUTION_POLICY, ATTR_AGENT_EFFECT_GENERATION, ATTR_AGENT_EFFECT_ID,
    ATTR_AGENT_EFFECT_RECONCILIATION_PROTOCOL, ATTR_AGENT_EFFECT_SAFETY_CLASS,
    ATTR_AGENT_EFFECT_SETTINGS_REVISION, ATTR_AGENT_TELEMETRY_LINK_KIND,
    LINK_KIND_SUPERSEDED_GENERATION,
};
pub use evaluation::{
    goal_evaluation_record_id, AgentGoalEvaluationError, AgentGoalEvaluationMethod,
    AgentGoalEvaluationMethodKind, AgentGoalEvaluationOutcome, AgentGoalEvaluationRecord,
    AgentGoalEvaluationRequest, AgentGoalEvaluationResult, AgentGoalEvidenceRef,
    AgentGoalStagnationAction, AgentGoalStagnationPolicy, AgentStagnationTrigger,
    AGENT_GOAL_EVALUATION_DEFAULT_MAX_ATTEMPTS, AGENT_GOAL_EVALUATION_HUMAN_DECISION_CLASS,
    AGENT_GOAL_EVALUATION_MAX_EVIDENCE,
};
pub use guardrails::{
    AgentGuardrail, AgentGuardrailBoundary, AgentGuardrailChain, AgentGuardrailContext,
    AgentGuardrailDecision, AgentGuardrailDisposition, AgentGuardrailError, AgentGuardrailOutcome,
    AgentGuardrailReport, AgentGuardrailResult, AgentGuardrailStage, AgentGuardrailTransform,
    AGENT_GUARDRAIL_CONTENT_MAX_BYTES, AGENT_GUARDRAIL_MAX_STAGES,
    AGENT_GUARDRAIL_REASON_MAX_LENGTH,
};
pub use loop_runtime::{
    AgentGoalEvaluationCell, AgentLoopPhase, AgentLoopState, AgentMemoryPromotionRecord,
    AgentPendingTopUp, AgentRunProposal, AGENT_RUN_MAX_MEMORY_PROMOTIONS,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
pub use memory::{
    assemble_session_context, check_memory_schema, check_private_memory_schema,
    AgentContextSnapshotId, AgentContextSnapshotRef, AgentPrivateMemory, AgentPrivateMemoryId,
    AgentPrivateMemoryKind, AgentPrivateMemorySource, AgentPrivateMemoryStore,
    AgentPromotedMemoryRef, AgentRunMemory, ContextSnapshotStore, InMemoryAgentPrivateMemoryStore,
    InMemoryContextSnapshotStore, InMemorySessionMemoryStore, MemoryClassification,
    MemoryContextSnapshot, MemoryEmbeddingRef, MemoryEntryId, MemoryEntryRole, MemoryError,
    MemoryFuture, MemoryOperationId, MemoryRetention, MemorySequence, MemoryTombstone,
    MemoryTombstoneReason, MemoryTrust, PrivateMemoryCursor, PrivateMemoryDeleteRequest,
    PrivateMemoryExpectation, PrivateMemoryPage, PrivateMemoryScope, PrivateMemoryTombstoneRequest,
    SessionMemoryCursor, SessionMemoryEntry, SessionMemoryPage, SessionMemoryStore,
    SessionPurgeOutcome, SessionRetentionPolicy, SessionWindowPolicy, SnapshotBudget,
    SnapshotIngressRecord, SnapshotPrivateMemory, SnapshotRetrieval, SnapshotSessionEntry,
    AGENT_MEMORY_EMBEDDING_MODEL_MAX_LENGTH, AGENT_PRIVATE_MEMORY_INLINE_MAX_BYTES,
    AGENT_PRIVATE_MEMORY_PAGE_MAX_ENTRIES, AGENT_SESSION_MEMORY_ENTRY_MAX_BYTES,
    AGENT_SESSION_WINDOW_MAX_ENTRIES, AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_BYTES,
    AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_ENTRIES,
};
pub use model::{
    AgentModelAdapter, AgentModelError, AgentModelFuture, AgentModelRequest, AgentModelResult,
    AgentModelRetryPolicy, AgentModelTurn, AgentModelUsage, AgentToolCallId, AgentToolCallRequest,
    AGENT_MODEL_MAX_TOOL_CALLS, AGENT_MODEL_TEXT_MAX_LENGTH, AGENT_MODEL_TURN_MAX_BYTES,
    AGENT_TOOL_ARGUMENTS_MAX_BYTES,
};
pub use observability::{
    record_agent_domain_counter, record_agent_domain_gauge, sanitize_agent_telemetry_context,
    validate_agent_domain_metric_attributes, AgentDecisionDraft, AgentDecisionEvent,
    AgentDecisionEventSink, AgentDecisionKind, AgentDecisionSource, AgentDecisionWriteStatus,
    AgentObservabilityError, AgentObservabilityFuture, AgentObservabilityResult,
    InMemoryAgentDecisionEventSink, AGENT_DECISION_EVENT_RETENTION,
    AGENT_DECISION_REASON_MAX_LENGTH, AGENT_METRIC_FIELDS, AGENT_TELEMETRY_MAX_SPAN_LINKS,
    METRIC_AGENT_DECISIONS, METRIC_AGENT_DECISION_DROPS, METRIC_AGENT_DELEGATION_RESULTS,
    METRIC_AGENT_EFFECT_OUTCOMES, METRIC_AGENT_EPOCHS, METRIC_AGENT_FAN_IN_RESOLUTIONS,
    METRIC_AGENT_GOAL_LIFECYCLE, METRIC_AGENT_GOAL_STAGNATION, METRIC_AGENT_GOAL_STATUS,
    METRIC_AGENT_MEMORY_INGRESS_OUTCOMES, METRIC_AGENT_MEMORY_RETRIEVALS,
    METRIC_AGENT_RECOVERY_EVENTS, METRIC_AGENT_RUN_TRANSITIONS,
    METRIC_AGENT_TELEMETRY_FLUSH_FAILURES, METRIC_AGENT_WAKE_DISPOSITIONS,
    METRIC_AGENT_WORKFLOW_RESULTS,
};
#[cfg(feature = "otel")]
pub use otel::{
    agent_instrumentation_scope, decision_span_event, usage_attributes, AgentGenAiIdentity,
    AgentGenAiOperation, AGENT_DECISION_SPAN_EVENT, AGENT_GENAI_CONVENTION_REVISION,
    AGENT_GENAI_SCHEMA_URL, AGENT_OTEL_SCOPE_NAME, AGENT_OTEL_SCOPE_VERSION, ATTR_GEN_AI_AGENT_ID,
    ATTR_GEN_AI_AGENT_NAME, ATTR_GEN_AI_AGENT_VERSION, ATTR_GEN_AI_CONVERSATION_ID,
    ATTR_GEN_AI_OPERATION_NAME, ATTR_GEN_AI_PROVIDER_NAME, ATTR_GEN_AI_TOOL_NAME,
    ATTR_GEN_AI_TOOL_TYPE, ATTR_GEN_AI_USAGE_INPUT_TOKENS, ATTR_GEN_AI_USAGE_OUTPUT_TOKENS,
    ATTR_RAKKA_AGENT_DELEGATION_ID, ATTR_RAKKA_AGENT_GOAL_ID, ATTR_RAKKA_AGENT_TASK_ID,
};
pub use query::{
    agent_operational_snapshot, agent_task_operational_snapshot, assemble_agent_session_view,
    next_pending_wake_for_task, AgentCancellationProgress, AgentCheckpointView,
    AgentOperationalSnapshot, AgentPendingEffectView, AgentSessionSegmentSource,
    AgentSessionTraceSegment, AgentSessionView, AgentTaskOperationalSnapshot,
};
pub use retrieval::{
    assemble_context, derive_retrieval_query, embed_memory_vector, memory_embedding_text,
    AgentMemoryEmbedder, AgentMemoryRetrieval, AgentPrivateMemoryRetriever, AssembledContext,
    InMemoryPrivateMemoryRetriever, MemoryRetrievalOutcome, MemoryRetrievalPolicy,
    MemoryRetrievalQuery, RetrievalReport, RetrievedPrivateMemory,
    AGENT_MEMORY_INDEX_WATERMARK_MAX_LENGTH, AGENT_MEMORY_RETRIEVAL_QUERY_MAX_BYTES,
    AGENT_MEMORY_RETRIEVAL_QUERY_SOURCE_ENTRIES, AGENT_MEMORY_RETRIEVAL_SCAN_MAX_ENTRIES,
};
pub use run::{
    agent_run_entity_id, agent_run_entity_persistence_id, agent_run_entity_ref,
    agent_run_entity_type_key, claim_append_operation_id, evaluation_operation_id,
    init_agent_run_entity_remote_sharding, init_agent_run_entity_sharding, ledger_operation_id,
    load_agent_run_state, passivate_agent_run_entity, promotion_operation_id,
    proposal_operation_id, registered_agent_run_entity_ref, system_run_clock, AgentRun,
    AgentRunClock, AgentRunEntity, AgentRunEntityCommand, AgentRunEntityMessage, AgentRunEntityRef,
    AgentRunEntityRegistration, AgentRunEntityReply, AgentRunEntityShardingSettings,
    AgentRunEntityStore, AgentRunEntityTypeKey, AgentRunError, AgentRunOperationLog,
    AgentRunOutcome, AgentRunParticipant, AgentRunProgress, AgentRunResult,
    AgentRunSettlementStatus, AgentRunSnapshot, AgentRunState, AgentRunStatus,
    AgentRunTerminalReason, AGENT_RUN_DETAIL_MAX_LENGTH, AGENT_RUN_MATERIALIZED_MAX_BYTES,
    AGENT_RUN_MAX_LOOP_STEPS_PER_PASS, AGENT_RUN_MAX_SETTLE_ROUNDS,
    AGENT_RUN_OPERATION_LOG_CAPACITY, AGENT_RUN_STATE_GROWTH_RESERVE_BYTES,
    DEFAULT_AGENT_RUN_ENTITY_TYPE,
};

pub use goal::{
    AgentContinuousGoalSpec, AgentEpochSpec, AgentGoalCriteria, AgentGoalCriteriaSource,
    AgentGoalDecision, AgentGoalDelegationBudget, AgentGoalError, AgentGoalEvaluationRef,
    AgentGoalExhaustionAction, AgentGoalExhaustionPolicy, AgentGoalMode, AgentGoalObjective,
    AgentGoalOutcome, AgentGoalResult, AgentGoalSpec, AgentGoalSpecDraft, AgentGoalSpecRevision,
    AgentGoalState, AgentGoalStatus, AgentGoalStatusView, AgentGoalTerminalDecision,
    AgentGoalTerminalReason, AgentGoalWaitReason, AGENT_GOAL_EVIDENCE_CLASS_MAX_LENGTH,
    AGENT_GOAL_MAX_ALLOWED_REFS, AGENT_GOAL_REASON_MAX_LENGTH, AGENT_GOAL_SPEC_MAX_BYTES,
    AGENT_GOAL_SUMMARY_MAX_LENGTH,
};
pub use wake::{
    epoch_admission_operation_id, epoch_result_operation_id, epoch_task_id_for_wake,
    wake_admission_operation_id, wake_id_for_occurrence, AgentActiveWake, AgentBudgetWindow,
    AgentCalendarUnit, AgentEpochOutcomeClass, AgentEpochRef, AgentGoalLifecycleState,
    AgentGoalLifecycleStatus, AgentGoalWindowCeiling, AgentMissedOccurrencePolicy,
    AgentWakeBackoffPolicy, AgentWakeBinding, AgentWakeCallbackId, AgentWakeControllerState,
    AgentWakeCounters, AgentWakeDisposition, AgentWakeError, AgentWakeEventId,
    AgentWakeLifecyclePolicy, AgentWakeOccurrence, AgentWakeOutcome, AgentWakeOverlapPolicy,
    AgentWakePolicy, AgentWakePolicyRevision, AgentWakeRelease, AgentWakeRenewalPolicy,
    AgentWakeResult, AgentWakeRetirementPolicy, AgentWakeRewake, AgentWakeRewakeCause,
    AgentWakeRewakes, AgentWakeStatusView, AgentWakeSuspensionPolicy, AgentWakeTriggerKind,
    AgentWakeWindowLedger, ScheduleRevision, AGENT_WAKE_ACTIVE_CAPACITY, AGENT_WAKE_ID_PREFIX,
    AGENT_WAKE_PENDING_CAPACITY, AGENT_WAKE_REASON_MAX_LENGTH, AGENT_WAKE_RECENT_CAPACITY,
};
pub use wake_scanner::{
    wake_admission_command, AgentWakeDelivery, AgentWakeDeliveryFuture, AgentWakeScan,
    AgentWakeScanError, AgentWakeScanOutcome, AgentWakeScanResult, AgentWakeScanner,
    AgentWakeScannerSettings, ShardedWakeDelivery, METRIC_AGENT_WAKES,
};
pub use wake_timers::{
    agent_wake_timer_store_persistence_id, AgentWakeRewakeParkFuture, AgentWakeRewakeParker,
    AgentWakeTimerEntry, AgentWakeTimerError, AgentWakeTimerResult, AgentWakeTimerScheduled,
    AgentWakeTimerStatus, AgentWakeTimerStore, AgentWakeTimerStoreState, SharedWakeTimerParker,
    AGENT_WAKE_TIMER_PERSISTENCE_PREFIX, DEFAULT_AGENT_WAKE_TIMER_STORE_ID,
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
pub use delegation::{
    delegation_cancel_operation_id, delegation_id_for, delegation_result_operation_id,
    AgentA2aSendReceipt, AgentDelegationCancelOutcome, AgentDelegationCatalog, AgentDelegationCell,
    AgentDelegationChildResult, AgentDelegationError, AgentDelegationRecord,
    AgentDelegationResolutionError, AgentDelegationResult, AgentDelegationStatus,
    AgentDelegationTarget, AgentDelegationToolCall, AgentRunDelegationConfig,
    AgentRunDelegationEnvelope, AgentTaskDelegationProvenance, StaticAgentDelegationCatalog,
    AGENT_A2A_SEND_DEFAULT_MAX_ATTEMPTS, AGENT_A2A_SEND_STATUS_MAX_BYTES,
    AGENT_DELEGATION_ENDPOINT_MAX_BYTES, AGENT_DELEGATION_ID_PREFIX, AGENT_DELEGATION_MAX_LINEAGE,
    AGENT_DELEGATION_PROVENANCE_MAX_BYTES, AGENT_DELEGATION_RECORD_MAX_BYTES,
    AGENT_RUN_MAX_DELEGATIONS,
};
pub use fan_in::{
    evaluate_fan_in, AgentFanInCell, AgentFanInMemberId, AgentFanInPolicy, AgentFanInResolution,
    AgentFanInToolCall, AGENT_RUN_MAX_FAN_IN_MEMBERS,
};
pub use identity::{
    validate_identity_segment, validate_tenant, AgentCommunalClaimId, AgentDelegationId,
    AgentEnvironmentRef, AgentGoalId, AgentId, AgentIdentityError, AgentIdentityResult,
    AgentMemoryNamespace, AgentOperationId, AgentOperationKind, AgentRunBinding, AgentRunId,
    AgentRunScope, AgentScope, AgentTaskId, AgentTaskScope, AgentWakeId, AgentWorkflowInvocationId,
    KnowledgeSpaceId, TenantId, AGENT_ENTITY_PERSISTENCE_PREFIX, AGENT_IDENTITY_MAX_LENGTH,
    AGENT_MEMORY_NAMESPACE_PREFIX, AGENT_PERSISTENCE_SEPARATOR,
    AGENT_RUN_ENTITY_PERSISTENCE_PREFIX, AGENT_SCOPE_SEPARATOR,
    AGENT_TASK_ENTITY_PERSISTENCE_PREFIX,
};
pub use schema::{
    previous_schema_version, AgentRecordKind, AgentSchemaCompatibility, AgentSchemaError,
    AgentSchemaPolicy, AgentSchemaResult, VersionedAgentRecord,
    CURRENT_AGENT_CHECKPOINT_SCHEMA_VERSION, CURRENT_AGENT_DECISION_EVENT_SCHEMA_VERSION,
    CURRENT_AGENT_DEFINITION_SCHEMA_VERSION, CURRENT_AGENT_ENTITY_STATE_SCHEMA_VERSION,
    CURRENT_AGENT_EXCHANGE_ENVELOPE_SCHEMA_VERSION, CURRENT_AGENT_EXCHANGE_JOURNAL_SCHEMA_VERSION,
    CURRENT_AGENT_EXCHANGE_REPLY_SCHEMA_VERSION, CURRENT_AGENT_GOAL_EVALUATION_SCHEMA_VERSION,
    CURRENT_AGENT_GOAL_SPEC_SCHEMA_VERSION, CURRENT_AGENT_LOOP_STATE_SCHEMA_VERSION,
    CURRENT_AGENT_MODEL_TURN_SCHEMA_VERSION, CURRENT_AGENT_RUN_EFFECT_SCHEMA_VERSION,
    CURRENT_AGENT_RUN_STATE_SCHEMA_VERSION, CURRENT_AGENT_SETTINGS_SCHEMA_VERSION,
    CURRENT_AGENT_SETUP_SCHEMA_VERSION, CURRENT_AGENT_TASK_DEFINITION_SCHEMA_VERSION,
    CURRENT_AGENT_TASK_HISTORY_SCHEMA_VERSION, CURRENT_AGENT_TASK_STATE_SCHEMA_VERSION,
    CURRENT_AGENT_WAKE_POLICY_SCHEMA_VERSION, CURRENT_AGENT_WAKE_TIMER_SCHEMA_VERSION,
};
pub use task::{
    agent_task_entity_id, agent_task_entity_persistence_id, agent_task_entity_ref,
    agent_task_entity_type_key, assignment_operation_id, init_agent_task_entity_remote_sharding,
    init_agent_task_entity_sharding, load_agent_task_state, passivate_agent_task_entity,
    registered_agent_task_entity_ref, run_cancel_operation_id, run_id_for_assignment,
    system_task_clock, AgentAcceptedResult, AgentAssignmentGeneration, AgentAssignmentReadiness,
    AgentAssignmentRefusal, AgentAssignmentRefusalReason, AgentAssignmentStatus,
    AgentBudgetLedgerOutcome, AgentBudgetReturn, AgentBudgetSettlement, AgentBudgetTopUpRequest,
    AgentContentDigest, AgentDelegationCancelReceipt, AgentDelegationCancelRequest,
    AgentDelegationReport, AgentDependencyFailurePolicy, AgentDigestAlgorithm, AgentEpochResult,
    AgentRunAcceptance, AgentRunAssignment, AgentRunCancelReceipt, AgentRunCancelRequest,
    AgentSchemaId, AgentSchemaRef, AgentTask, AgentTaskCancellation, AgentTaskClock,
    AgentTaskContent, AgentTaskCreation, AgentTaskDecision, AgentTaskDefinition,
    AgentTaskDependency, AgentTaskDependencyDeclaration, AgentTaskDependencyOutcome,
    AgentTaskEntity, AgentTaskEntityCommand, AgentTaskEntityMessage, AgentTaskEntityRef,
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
    AGENT_DELEGATION_CANCEL_PAYLOAD_TYPE, AGENT_DELEGATION_CANCEL_RECEIPT_PAYLOAD_TYPE,
    AGENT_DELEGATION_RESULT_OUTCOME_PAYLOAD_TYPE, AGENT_DELEGATION_RESULT_PAYLOAD_TYPE,
    AGENT_EPOCH_RESULT_OUTCOME_PAYLOAD_TYPE, AGENT_EPOCH_RESULT_PAYLOAD_TYPE,
    AGENT_GOAL_EVALUATION_OUTCOME_PAYLOAD_TYPE, AGENT_GOAL_EVALUATION_PAYLOAD_TYPE,
    AGENT_RUN_ACCEPTANCE_PAYLOAD_TYPE, AGENT_RUN_ASSIGNMENT_PAYLOAD_TYPE,
    AGENT_RUN_CANCEL_PAYLOAD_TYPE, AGENT_RUN_CANCEL_RECEIPT_PAYLOAD_TYPE,
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
    AgentAuthorityContext, AgentAuthorityRefusal, AgentDispatchGrant,
    AgentEnvironmentConcurrencyProtocol, AgentExecutionPolicyRouter, AgentGrantDescriptor,
    AgentGrantedDispatch, AgentToolAuthority, AgentToolBinding, AgentToolDescriptor,
    AgentToolError, AgentToolKind, AgentToolRegistry, AgentToolResultBehavior,
    AGENT_DISPATCH_GRANT_DEFAULT_TTL_MS, AGENT_EVALUATED_GUARDRAIL_BOUNDARIES,
    AGENT_TOOL_DESCRIPTION_MAX_LENGTH, AGENT_TOOL_PARAMETERS_MAX_BYTES,
    AGENT_TOOL_REGISTRY_MAX_TOOLS,
};
pub use workflow_tool::{
    child_workflow_run_id, workflow_cancel_command, workflow_cancel_command_id,
    workflow_invocation_id_for, workflow_result_operation_id, workflow_start_command,
    workflow_start_command_id, AgentRunWorkflowConfig, AgentWorkflowCancelDisposition,
    AgentWorkflowChildResult, AgentWorkflowInvocationCell, AgentWorkflowInvocationRecord,
    AgentWorkflowInvocationStatus, AgentWorkflowStartReceipt, AgentWorkflowTerminalStatus,
    AgentWorkflowToolDescriptor, AgentWorkflowToolError, AgentWorkflowToolResult,
    AGENT_RUN_MAX_WORKFLOW_INVOCATIONS, AGENT_RUN_MAX_WORKFLOW_TOOLS,
    AGENT_WORKFLOW_CANCEL_DEFAULT_MAX_ATTEMPTS, AGENT_WORKFLOW_INVOCATION_CONFLICT_CODE,
    AGENT_WORKFLOW_INVOCATION_ID_PREFIX, AGENT_WORKFLOW_INVOCATION_RECORD_MAX_BYTES,
    AGENT_WORKFLOW_START_DEFAULT_MAX_ATTEMPTS, AGENT_WORKFLOW_TOOL_DESCRIPTOR_MAX_BYTES,
};
