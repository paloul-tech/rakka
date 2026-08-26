//! Durable dispatcher fleet for agent workflow outbox effects.
//!
//! The per-run durable outbox remains the source of execution truth. This
//! module adds a fleet-level index for cross-run discovery, leases, fencing,
//! target concurrency limits, and bounded health snapshots.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rakka_core::{MetricsRecorder, NoopMetricsRecorder};
use rakka_persistence::{DurableError, DurableStateStore, PersistenceId, Revision, StateRecord};
use rakka_workflow::{
    OutboxDispatchResult, OutboxEntry, OutboxMessageId, SystemWorkflowClock, WorkflowClock,
    WorkflowError, WorkflowState, WorkflowTelemetryEvent, WorkflowTimestamp,
};
use serde::{Deserialize, Serialize};

use crate::{
    AgentA2APeerAdapter, AgentA2APeerRequest, AgentAttributes, AgentCompiledNodeId,
    AgentCompiledNodeKind, AgentCompiledPlanFingerprint, AgentDispatchId, AgentDispatcherWorkerId,
    AgentDueEffect, AgentEffect, AgentEffectId, AgentEffectKind, AgentEffectTarget,
    AgentInboxError, AgentModelAdapter, AgentModelRequest, AgentOutboxError, AgentRunId,
    AgentRunInbox, AgentTimestampMillis, AgentToolAdapter, AgentToolRequest, AgentWorkflowId,
};

const ATTR_COMPILED_NODE_ID: &str = "compiled_node_id";
const ATTR_COMPILED_PLAN_FINGERPRINT: &str = "compiled_plan_fingerprint";
const ATTR_LOOP_INSTANCE_ID: &str = "loop_instance_id";
const ATTR_NODE_KIND: &str = "node_kind";
const ATTR_TARGET_CLASS: &str = "target_class";

/// Prefix used for durable dispatcher fleet state persistence ids.
pub const AGENT_DISPATCHER_FLEET_PERSISTENCE_PREFIX: &str = "agent-dispatcher-fleet";

/// Default dispatcher fleet persistence id.
pub const DEFAULT_AGENT_DISPATCHER_FLEET_ID: &str = "default";

/// Counter for dispatcher fleet attempts and outcomes.
pub const METRIC_AGENT_DISPATCHER_FLEET: &str = "rakka.agent_workflow.dispatcher.fleet";

/// Gauge for currently leased dispatcher work.
pub const METRIC_AGENT_DISPATCHER_IN_FLIGHT: &str = "rakka.agent_workflow.dispatcher.in_flight";

/// Gauge for due dispatcher work that could not be claimed in one pass.
pub const METRIC_AGENT_DISPATCHER_BACKLOG: &str = "rakka.agent_workflow.dispatcher.backlog";

/// The most bytes of failure detail one fleet index entry keeps in
/// [`AgentDispatchEntry::last_error_code`].
///
/// The fleet index is a *single* durable record that every worker loads and
/// re-persists on every claim pass, and the string that reaches this field is
/// supplied by an application dispatcher or an application dispatch
/// authority. An unbounded one becomes unbounded growth on the hottest shared
/// record in the system — and on a retry path, which repeats every backoff
/// interval.
///
/// The bound lives here, at the record, rather than at each writer: a bound
/// applied by callers holds only for the callers that remember it, and this
/// field has writers in this crate, in `rakka-agent`, and in any deployment
/// that drives a fleet of its own.
pub const AGENT_DISPATCH_LAST_ERROR_MAX_LENGTH: usize = 512;

/// Bounds one persisted failure detail: newlines folded to spaces, truncated
/// on a character boundary at [`AGENT_DISPATCH_LAST_ERROR_MAX_LENGTH`].
///
/// This is *bounding*, not sanitizing. It cannot remove secret material a
/// collaborator chose to put in its error text — that stays the
/// collaborator's own contract — but it keeps an unbounded body out of a
/// durable record.
#[must_use]
pub fn bounded_dispatch_detail(detail: &str) -> String {
    let single_line: String = detail
        .chars()
        .map(|character| {
            if character == '\n' || character == '\r' {
                ' '
            } else {
                character
            }
        })
        .collect();
    if single_line.len() <= AGENT_DISPATCH_LAST_ERROR_MAX_LENGTH {
        return single_line;
    }
    let mut end = AGENT_DISPATCH_LAST_ERROR_MAX_LENGTH;
    while end > 0 && !single_line.is_char_boundary(end) {
        end -= 1;
    }
    single_line[..end].to_string()
}

/// Creates the default dispatcher fleet persistence id.
#[must_use]
pub fn agent_dispatcher_fleet_persistence_id() -> PersistenceId {
    PersistenceId::new(format!(
        "{AGENT_DISPATCHER_FLEET_PERSISTENCE_PREFIX}:{DEFAULT_AGENT_DISPATCHER_FLEET_ID}"
    ))
}

/// Creates a stable dispatcher work id from a run id and effect id.
#[must_use]
pub fn agent_dispatch_id(run_id: &AgentRunId, effect_id: &AgentEffectId) -> AgentDispatchId {
    AgentDispatchId::new(format!("{}:{}", run_id.as_str(), effect_id.as_str()))
}

/// Converts an agent timestamp into a workflow timestamp.
#[must_use]
pub const fn agent_dispatch_timestamp_to_workflow_timestamp(
    timestamp: AgentTimestampMillis,
) -> WorkflowTimestamp {
    WorkflowTimestamp::from_millis(timestamp.as_millis())
}

/// Converts a workflow timestamp into an agent timestamp.
#[must_use]
pub const fn agent_dispatch_timestamp_from_workflow_timestamp(
    timestamp: WorkflowTimestamp,
) -> AgentTimestampMillis {
    AgentTimestampMillis::new(timestamp.as_millis())
}

/// Shared result type for dispatcher fleet operations.
pub type AgentDispatcherResult<T> = Result<T, AgentDispatcherError>;

/// Dispatcher fleet failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentDispatcherError {
    /// Dispatcher entry validation failed.
    InvalidEntry {
        /// Dispatch id.
        dispatch_id: AgentDispatchId,
        /// Invalid field.
        field: &'static str,
        /// Stable reason.
        reason: &'static str,
    },
    /// Requested dispatch work does not exist.
    DispatchNotFound {
        /// Dispatch id.
        dispatch_id: AgentDispatchId,
    },
    /// Requested dispatch work is not currently claimable.
    ClaimUnavailable {
        /// Dispatch id.
        dispatch_id: AgentDispatchId,
    },
    /// A worker tried to use an expired or superseded claim.
    ClaimFenced {
        /// Dispatch id.
        dispatch_id: AgentDispatchId,
        /// Worker that attempted to use the claim.
        worker_id: AgentDispatcherWorkerId,
        /// Fencing token carried by the worker.
        fencing_token: u64,
    },
    /// A persisted outbox payload could not be decoded into an agent effect.
    Deserialization {
        /// Deserialization failure detail.
        message: String,
    },
    /// Lower-level durable workflow operation failed.
    Workflow {
        /// Workflow substrate failure.
        error: WorkflowError,
    },
    /// Agent inbox operation failed.
    Inbox {
        /// Agent inbox failure.
        error: AgentInboxError,
    },
    /// Agent outbox operation failed.
    Outbox {
        /// Agent outbox failure.
        error: AgentOutboxError,
    },
    /// Dispatcher fleet persistence failed.
    Persistence {
        /// Durable persistence failure.
        error: DurableError,
    },
}

impl AgentDispatcherError {
    /// Stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidEntry { .. } => "invalid-dispatch-entry",
            Self::DispatchNotFound { .. } => "dispatch-not-found",
            Self::ClaimUnavailable { .. } => "claim-unavailable",
            Self::ClaimFenced { .. } => "claim-fenced",
            Self::Deserialization { .. } => "effect-deserialization",
            Self::Workflow { error } => error.code(),
            Self::Inbox { error } => error.code(),
            Self::Outbox { error } => error.code(),
            Self::Persistence { error } => error.code(),
        }
    }
}

impl Display for AgentDispatcherError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEntry {
                dispatch_id,
                field,
                reason,
            } => write!(
                f,
                "agent dispatch entry {dispatch_id} has invalid {field}: {reason}"
            ),
            Self::DispatchNotFound { dispatch_id } => {
                write!(f, "agent dispatch entry {dispatch_id} was not found")
            }
            Self::ClaimUnavailable { dispatch_id } => {
                write!(f, "agent dispatch entry {dispatch_id} is not claimable")
            }
            Self::ClaimFenced {
                dispatch_id,
                worker_id,
                fencing_token,
            } => write!(
                f,
                "agent dispatch entry {dispatch_id} claim for worker {worker_id} with token {fencing_token} is fenced"
            ),
            Self::Deserialization { message } => {
                write!(f, "agent dispatch effect deserialization failed: {message}")
            }
            Self::Workflow { error } => Display::fmt(error, f),
            Self::Inbox { error } => Display::fmt(error, f),
            Self::Outbox { error } => Display::fmt(error, f),
            Self::Persistence { error } => Display::fmt(error, f),
        }
    }
}

impl Error for AgentDispatcherError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Workflow { error } => Some(error),
            Self::Inbox { error } => Some(error),
            Self::Outbox { error } => Some(error),
            Self::Persistence { error } => Some(error),
            Self::InvalidEntry { .. }
            | Self::DispatchNotFound { .. }
            | Self::ClaimUnavailable { .. }
            | Self::ClaimFenced { .. }
            | Self::Deserialization { .. } => None,
        }
    }
}

impl From<WorkflowError> for AgentDispatcherError {
    fn from(error: WorkflowError) -> Self {
        Self::Workflow { error }
    }
}

impl From<AgentInboxError> for AgentDispatcherError {
    fn from(error: AgentInboxError) -> Self {
        Self::Inbox { error }
    }
}

impl From<AgentOutboxError> for AgentDispatcherError {
    fn from(error: AgentOutboxError) -> Self {
        Self::Outbox { error }
    }
}

impl From<DurableError> for AgentDispatcherError {
    fn from(error: DurableError) -> Self {
        Self::Persistence { error }
    }
}

/// Coarse target class used for bounded concurrency and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentDispatchTargetClass {
    /// Model provider request.
    Model,
    /// Tool adapter request.
    Tool,
    /// Process actor request.
    Process,
    /// A2A peer-agent request.
    A2aPeer,
    /// HTTP request.
    Http,
    /// gRPC request.
    Grpc,
    /// Webhook callback request.
    Webhook,
    /// Notification request.
    Notification,
    /// A2A push notification request.
    PushNotification,
    /// Human checkpoint request.
    Human,
    /// Stream publication.
    Stream,
    /// Artifact write.
    Artifact,
    /// Child workflow command.
    ChildWorkflow,
    /// Audit event write.
    Audit,
    /// Any target not covered by the first-class categories.
    Other,
}

impl AgentDispatchTargetClass {
    /// Classifies an effect target.
    ///
    /// The `target_class` attribute and target type may refine the kind-based
    /// class, but only to a class that can actually route the effect kind —
    /// an incompatible refinement falls back to the kind-based class instead
    /// of routing the effect to a dispatcher that deterministically rejects
    /// it.
    #[must_use]
    pub fn classify(effect_kind: AgentEffectKind, target: &AgentEffectTarget) -> Self {
        let base = match effect_kind {
            AgentEffectKind::ModelCall => Self::Model,
            AgentEffectKind::ToolCall => Self::Tool,
            AgentEffectKind::ProcessCall => Self::Process,
            AgentEffectKind::HttpCall => Self::Http,
            AgentEffectKind::GrpcCall => Self::Grpc,
            AgentEffectKind::Notification => Self::Notification,
            AgentEffectKind::HumanApprovalRequest => Self::Human,
            AgentEffectKind::StreamPublish => Self::Stream,
            AgentEffectKind::ArtifactWrite => Self::Artifact,
            AgentEffectKind::ChildWorkflowCommand => Self::ChildWorkflow,
            AgentEffectKind::AuditEvent => Self::Audit,
        };
        base.refine_with_target_type(effect_kind, target)
    }

    /// Returns true when this dispatch class can route the effect kind.
    #[must_use]
    pub const fn accepts_effect_kind(self, kind: AgentEffectKind) -> bool {
        match self {
            Self::Model => matches!(kind, AgentEffectKind::ModelCall),
            Self::Tool => matches!(kind, AgentEffectKind::ToolCall),
            Self::Process => matches!(kind, AgentEffectKind::ProcessCall),
            Self::A2aPeer => {
                // `ToolCall` joined the accepted kinds when the agent domain's
                // outbound A2A send landed: its run effects ride the
                // executor-routed tool family with target type `a2a-peer`,
                // and refusing the refinement would misclassify a genuine
                // peer send as a plain tool.
                matches!(
                    kind,
                    AgentEffectKind::HttpCall
                        | AgentEffectKind::GrpcCall
                        | AgentEffectKind::ToolCall
                )
            }
            Self::Http => matches!(kind, AgentEffectKind::HttpCall),
            Self::Grpc => matches!(kind, AgentEffectKind::GrpcCall),
            Self::Webhook => {
                matches!(
                    kind,
                    AgentEffectKind::HttpCall | AgentEffectKind::Notification
                )
            }
            Self::Notification | Self::PushNotification => {
                matches!(kind, AgentEffectKind::Notification)
            }
            Self::Human => matches!(kind, AgentEffectKind::HumanApprovalRequest),
            Self::Stream => matches!(kind, AgentEffectKind::StreamPublish),
            Self::Artifact => matches!(kind, AgentEffectKind::ArtifactWrite),
            Self::ChildWorkflow => {
                // `ToolCall` joined the accepted kinds with agent-domain
                // workflows-as-tools (slice 4.5): a workflow start rides the
                // executor-routed tool family with target type
                // `workflow-tool`, and refusing the refinement would leave
                // the class's concurrency limit and autonomy governance
                // inert for exactly the invocations they were added for.
                matches!(
                    kind,
                    AgentEffectKind::ChildWorkflowCommand | AgentEffectKind::ToolCall
                )
            }
            Self::Audit => matches!(kind, AgentEffectKind::AuditEvent),
            Self::Other => true,
        }
    }

    /// Stable lowercase label for metrics and snapshots.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Model => "model",
            Self::Tool => "tool",
            Self::Process => "process",
            Self::A2aPeer => "a2a-peer",
            Self::Http => "http",
            Self::Grpc => "grpc",
            Self::Webhook => "webhook",
            Self::Notification => "notification",
            Self::PushNotification => "push-notification",
            Self::Human => "human",
            Self::Stream => "stream",
            Self::Artifact => "artifact",
            Self::ChildWorkflow => "child-workflow",
            Self::Audit => "audit",
            Self::Other => "other",
        }
    }

    fn refine_with_target_type(
        self,
        effect_kind: AgentEffectKind,
        target: &AgentEffectTarget,
    ) -> Self {
        if let Some(class) = target
            .attributes
            .get(ATTR_TARGET_CLASS)
            .and_then(|value| dispatch_class_from_label(value))
            .filter(|class| class.accepts_effect_kind(effect_kind))
        {
            return class;
        }
        if target
            .attributes
            .get("notification_protocol")
            .is_some_and(|value| value == "a2a-push")
            && Self::PushNotification.accepts_effect_kind(effect_kind)
        {
            return Self::PushNotification;
        }
        let by_target_type = match target.target_type.as_str() {
            "model" => Self::Model,
            "tool" => Self::Tool,
            "process" => Self::Process,
            "a2a-peer" => Self::A2aPeer,
            // A workflow-tool start is a child-workflow invocation, so the
            // ChildWorkflow class limit and autonomy policy govern it. The
            // cancel deliberately stays in the tool family: it is wind-down
            // work the run already owes, and a class policy that blocks
            // starting new children must never block stopping started ones.
            "workflow-tool" => Self::ChildWorkflow,
            "webhook" => Self::Webhook,
            "push" | "a2a-push" => Self::PushNotification,
            "http" => Self::Http,
            "grpc" => Self::Grpc,
            "notification" => Self::Notification,
            "human" => Self::Human,
            _ => self,
        };
        if by_target_type.accepts_effect_kind(effect_kind) {
            by_target_type
        } else {
            self
        }
    }
}

/// Durable dispatcher lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentDispatchStatus {
    /// Work is due or waiting for its due timestamp.
    Pending,
    /// Work is leased by one worker.
    Claimed,
    /// Work completed successfully.
    Completed,
    /// Work failed and is waiting for retry-after.
    RetryScheduled,
    /// Work exhausted its retry budget.
    Exhausted,
    /// Work was cancelled.
    Cancelled,
}

impl AgentDispatchStatus {
    /// Stable lowercase label for diagnostics and metrics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Claimed => "claimed",
            Self::Completed => "completed",
            Self::RetryScheduled => "retry-scheduled",
            Self::Exhausted => "exhausted",
            Self::Cancelled => "cancelled",
        }
    }

    /// Returns true when this status is terminal.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Exhausted | Self::Cancelled)
    }
}

/// One active dispatcher lease plus its fencing token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDispatchLease {
    /// Worker holding the lease.
    pub worker_id: AgentDispatcherWorkerId,
    /// Monotonic fencing token for this dispatch entry.
    pub fencing_token: u64,
    /// Claim timestamp.
    pub claimed_at: AgentTimestampMillis,
    /// Lease expiration timestamp.
    pub lease_expires_at: AgentTimestampMillis,
}

impl AgentDispatchLease {
    /// Returns true when the lease is still active at `now`.
    #[must_use]
    pub const fn is_active_at(&self, now: AgentTimestampMillis) -> bool {
        now.as_millis() < self.lease_expires_at.as_millis()
    }
}

/// One durable dispatcher fleet entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDispatchEntry {
    /// Stable dispatch work id.
    pub dispatch_id: AgentDispatchId,
    /// Optional workflow definition id. The run id remains the execution key.
    pub workflow_id: Option<AgentWorkflowId>,
    /// Durable run that owns the source outbox entry.
    pub run_id: AgentRunId,
    /// Effect id, also used as the lower-level outbox message id.
    pub effect_id: AgentEffectId,
    /// Effect kind.
    pub effect_kind: AgentEffectKind,
    /// Dispatch target.
    pub target: AgentEffectTarget,
    /// Target class used for fleet concurrency.
    pub target_class: AgentDispatchTargetClass,
    /// Compiled plan fingerprint for graph-scheduled effects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_plan_fingerprint: Option<AgentCompiledPlanFingerprint>,
    /// Compiled graph node id for graph-scheduled effects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_node_id: Option<AgentCompiledNodeId>,
    /// Compiled graph node kind for graph-scheduled effects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_node_kind: Option<AgentCompiledNodeKind>,
    /// Loop instance id for graph-scheduled effects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_loop_instance_id: Option<String>,
    /// Due timestamp for first dispatch or retry.
    pub due_at: AgentTimestampMillis,
    /// Dispatcher lifecycle status.
    pub status: AgentDispatchStatus,
    /// Current active or expired lease.
    pub lease: Option<AgentDispatchLease>,
    /// Last issued fencing token.
    pub last_fencing_token: u64,
    /// Last known outbox attempt count.
    pub attempts: u32,
    /// True when durable cancellation was requested while a lease was active.
    ///
    /// A cancellation-requested entry is never claimable again. Its in-flight
    /// worker may still finish and record a truthful completion; if the lease
    /// expires instead, the entry is finalized as `Cancelled` on the next
    /// worker refresh and its outbox message is settled as cancelled.
    #[serde(default)]
    pub cancellation_requested: bool,
    /// Stable error code or summary for the last failed dispatch.
    pub last_error_code: Option<String>,
    /// Creation timestamp.
    pub created_at: AgentTimestampMillis,
    /// Last update timestamp.
    pub updated_at: AgentTimestampMillis,
    /// Completion timestamp.
    pub completed_at: Option<AgentTimestampMillis>,
    /// Exhaustion timestamp.
    pub exhausted_at: Option<AgentTimestampMillis>,
    /// Bounded dispatcher attributes.
    pub attributes: AgentAttributes,
}

impl AgentDispatchEntry {
    /// Creates a dispatcher entry from one due effect.
    #[must_use]
    pub fn from_due_effect(
        run_id: AgentRunId,
        workflow_id: Option<AgentWorkflowId>,
        due: &AgentDueEffect,
        now: AgentTimestampMillis,
    ) -> Self {
        let effect = &due.effect;
        let dispatch_id = agent_dispatch_id(&run_id, &effect.effect_id);
        let due_at = effect.due_at.unwrap_or_else(|| {
            agent_dispatch_timestamp_from_workflow_timestamp(due.entry.scheduled_at())
        });
        let target_class = AgentDispatchTargetClass::classify(effect.kind, &effect.target);
        let graph_context = graph_dispatch_context_from_effect(effect);
        Self {
            dispatch_id,
            workflow_id,
            run_id,
            effect_id: effect.effect_id.clone(),
            effect_kind: effect.kind,
            target: effect.target.clone(),
            target_class,
            graph_plan_fingerprint: graph_context.plan_fingerprint,
            graph_node_id: graph_context.node_id,
            graph_node_kind: graph_context.node_kind,
            graph_loop_instance_id: graph_context.loop_instance_id,
            due_at,
            status: AgentDispatchStatus::Pending,
            lease: None,
            last_fencing_token: 0,
            attempts: due.entry.attempts().attempts(),
            cancellation_requested: false,
            last_error_code: effect.last_error_code.clone(),
            created_at: now,
            updated_at: now,
            completed_at: None,
            exhausted_at: None,
            attributes: graph_context.attributes,
        }
    }

    /// Returns true when this entry can be claimed at `now`.
    #[must_use]
    pub fn is_claimable_at(&self, now: AgentTimestampMillis) -> bool {
        if self.cancellation_requested {
            return false;
        }
        match self.status {
            AgentDispatchStatus::Pending | AgentDispatchStatus::RetryScheduled => {
                self.due_at <= now
            }
            AgentDispatchStatus::Claimed => {
                self.due_at <= now
                    && self
                        .lease
                        .as_ref()
                        .is_none_or(|lease| !lease.is_active_at(now))
            }
            AgentDispatchStatus::Completed
            | AgentDispatchStatus::Exhausted
            | AgentDispatchStatus::Cancelled => false,
        }
    }

    /// Returns true when this entry has an active lease at `now`.
    #[must_use]
    pub fn is_in_flight_at(&self, now: AgentTimestampMillis) -> bool {
        self.status == AgentDispatchStatus::Claimed
            && self
                .lease
                .as_ref()
                .is_some_and(|lease| lease.is_active_at(now))
    }

    /// Returns true when the provided claim is still current and unexpired.
    #[must_use]
    pub fn is_current_claim(&self, claim: &AgentDispatchClaim, now: AgentTimestampMillis) -> bool {
        self.lease.as_ref().is_some_and(|lease| {
            self.status == AgentDispatchStatus::Claimed
                && lease.worker_id == claim.worker_id
                && lease.fencing_token == claim.fencing_token
                && lease.is_active_at(now)
        })
    }

    fn upsert_from_due_effect(
        mut self,
        workflow_id: Option<AgentWorkflowId>,
        due: &AgentDueEffect,
        now: AgentTimestampMillis,
    ) -> Self {
        if self.status.is_terminal() || self.cancellation_requested {
            return self;
        }

        let effect = &due.effect;
        self.workflow_id = workflow_id.or(self.workflow_id);
        self.effect_kind = effect.kind;
        self.target = effect.target.clone();
        self.target_class = AgentDispatchTargetClass::classify(effect.kind, &effect.target);
        let graph_context = graph_dispatch_context_from_effect(effect);
        self.graph_plan_fingerprint = graph_context.plan_fingerprint;
        self.graph_node_id = graph_context.node_id;
        self.graph_node_kind = graph_context.node_kind;
        self.graph_loop_instance_id = graph_context.loop_instance_id;
        self.attributes = graph_context.attributes;
        self.due_at = effect.due_at.unwrap_or_else(|| {
            agent_dispatch_timestamp_from_workflow_timestamp(due.entry.scheduled_at())
        });
        self.attempts = due.entry.attempts().attempts();
        self.last_error_code = effect.last_error_code.clone();
        if self
            .lease
            .as_ref()
            .is_some_and(|lease| !lease.is_active_at(now))
        {
            self.status = AgentDispatchStatus::Pending;
            self.lease = None;
        }
        self.updated_at = now;
        self
    }

    fn claim(
        mut self,
        worker_id: AgentDispatcherWorkerId,
        now: AgentTimestampMillis,
        lease_duration_ms: u64,
    ) -> (Self, AgentDispatchClaim) {
        let fencing_token = self.last_fencing_token.saturating_add(1);
        let lease_expires_at =
            AgentTimestampMillis::new(now.as_millis().saturating_add(lease_duration_ms));
        let lease = AgentDispatchLease {
            worker_id: worker_id.clone(),
            fencing_token,
            claimed_at: now,
            lease_expires_at,
        };
        self.status = AgentDispatchStatus::Claimed;
        self.lease = Some(lease);
        self.last_fencing_token = fencing_token;
        self.updated_at = now;
        let claim = AgentDispatchClaim {
            dispatch_id: self.dispatch_id.clone(),
            run_id: self.run_id.clone(),
            effect_id: self.effect_id.clone(),
            worker_id,
            fencing_token,
            claimed_at: now,
            lease_expires_at,
            target_class: self.target_class,
            effect_kind: self.effect_kind,
        };
        (self, claim)
    }

    fn mark_completed(mut self, now: AgentTimestampMillis) -> Self {
        self.status = AgentDispatchStatus::Completed;
        self.lease = None;
        self.updated_at = now;
        self.completed_at = Some(now);
        self
    }

    fn mark_retry(
        mut self,
        next_retry_at: AgentTimestampMillis,
        attempts: u32,
        message: String,
        now: AgentTimestampMillis,
    ) -> Self {
        self.status = AgentDispatchStatus::RetryScheduled;
        self.lease = None;
        self.due_at = next_retry_at;
        self.attempts = attempts;
        // Bounded here, where the field is written, so no writer can bypass
        // it — see [`AGENT_DISPATCH_LAST_ERROR_MAX_LENGTH`]. This is the
        // retry path, so an unbounded string would be re-persisted on every
        // backoff interval for as long as the condition lasts.
        self.last_error_code = Some(bounded_dispatch_detail(&message));
        self.updated_at = now;
        self
    }

    fn mark_exhausted(mut self, attempts: u32, message: String, now: AgentTimestampMillis) -> Self {
        self.status = AgentDispatchStatus::Exhausted;
        self.lease = None;
        self.attempts = attempts;
        self.last_error_code = Some(bounded_dispatch_detail(&message));
        self.updated_at = now;
        self.exhausted_at = Some(now);
        self
    }

    fn mark_cancelled(mut self, now: AgentTimestampMillis) -> Self {
        self.status = AgentDispatchStatus::Cancelled;
        self.lease = None;
        self.updated_at = now;
        self.completed_at = Some(now);
        self
    }

    fn validate(&self) -> AgentDispatcherResult<()> {
        require_dispatch(&self.dispatch_id, self.dispatch_id.as_str(), "dispatch_id")?;
        require_dispatch(&self.dispatch_id, self.run_id.as_str(), "run_id")?;
        require_dispatch(&self.dispatch_id, self.effect_id.as_str(), "effect_id")?;
        require_dispatch(
            &self.dispatch_id,
            &self.target.target_type,
            "target.target_type",
        )?;
        require_dispatch(&self.dispatch_id, &self.target.name, "target.name")?;
        for key in self.attributes.keys() {
            require_dispatch(&self.dispatch_id, key, "attributes.key")?;
        }
        Ok(())
    }
}

/// One claimed dispatch item returned to a worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDispatchClaim {
    /// Stable dispatch work id.
    pub dispatch_id: AgentDispatchId,
    /// Run that owns the outbox entry.
    pub run_id: AgentRunId,
    /// Effect id.
    pub effect_id: AgentEffectId,
    /// Worker holding the claim.
    pub worker_id: AgentDispatcherWorkerId,
    /// Fencing token for this claim.
    pub fencing_token: u64,
    /// Claim timestamp.
    pub claimed_at: AgentTimestampMillis,
    /// Lease expiration timestamp.
    pub lease_expires_at: AgentTimestampMillis,
    /// Target class.
    pub target_class: AgentDispatchTargetClass,
    /// Effect kind.
    pub effect_kind: AgentEffectKind,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct AgentDispatchGraphContext {
    plan_fingerprint: Option<AgentCompiledPlanFingerprint>,
    node_id: Option<AgentCompiledNodeId>,
    node_kind: Option<AgentCompiledNodeKind>,
    loop_instance_id: Option<String>,
    attributes: AgentAttributes,
}

/// Target concurrency limits for dispatcher claims.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDispatchConcurrencyLimits {
    default_limit: usize,
    class_limits: BTreeMap<AgentDispatchTargetClass, usize>,
    target_limits: BTreeMap<String, usize>,
}

impl AgentDispatchConcurrencyLimits {
    /// Creates concurrency limits with a default limit.
    #[must_use]
    pub fn new(default_limit: usize) -> Self {
        let default_limit = default_limit.max(1);
        let mut class_limits = BTreeMap::new();
        for class in [
            AgentDispatchTargetClass::Model,
            AgentDispatchTargetClass::Tool,
            AgentDispatchTargetClass::Process,
            AgentDispatchTargetClass::A2aPeer,
            AgentDispatchTargetClass::Http,
            AgentDispatchTargetClass::Grpc,
            AgentDispatchTargetClass::Webhook,
            AgentDispatchTargetClass::Notification,
            AgentDispatchTargetClass::PushNotification,
            AgentDispatchTargetClass::ChildWorkflow,
        ] {
            class_limits.insert(class, default_limit);
        }
        Self {
            default_limit,
            class_limits,
            target_limits: BTreeMap::new(),
        }
    }

    /// Sets a class-level limit.
    #[must_use]
    pub fn class_limit(mut self, class: AgentDispatchTargetClass, limit: usize) -> Self {
        self.class_limits.insert(class, limit.max(1));
        self
    }

    /// Sets a target-level limit. Target keys are `class:name`.
    #[must_use]
    pub fn target_limit(
        mut self,
        class: AgentDispatchTargetClass,
        target_name: impl AsRef<str>,
        limit: usize,
    ) -> Self {
        self.target_limits
            .insert(target_limit_key(class, target_name.as_ref()), limit.max(1));
        self
    }

    /// Returns the effective limit for an entry.
    #[must_use]
    pub fn limit_for(&self, entry: &AgentDispatchEntry) -> usize {
        self.target_limits
            .get(&target_limit_key(entry.target_class, &entry.target.name))
            .copied()
            .or_else(|| self.class_limits.get(&entry.target_class).copied())
            .unwrap_or(self.default_limit)
            .max(1)
    }
}

impl Default for AgentDispatchConcurrencyLimits {
    fn default() -> Self {
        Self::new(32)
    }
}

/// Dispatcher fleet settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDispatcherFleetSettings {
    max_batch_size: usize,
    lease_duration_ms: u64,
    concurrency_limits: AgentDispatchConcurrencyLimits,
}

impl AgentDispatcherFleetSettings {
    /// Creates settings with the given batch size and lease duration.
    #[must_use]
    pub fn new(max_batch_size: usize, lease_duration_ms: u64) -> Self {
        Self {
            max_batch_size: max_batch_size.max(1),
            lease_duration_ms: lease_duration_ms.max(1),
            concurrency_limits: AgentDispatchConcurrencyLimits::default(),
        }
    }

    /// Sets target concurrency limits.
    #[must_use]
    pub fn concurrency_limits(mut self, limits: AgentDispatchConcurrencyLimits) -> Self {
        self.concurrency_limits = limits;
        self
    }

    /// Max claims returned in one pass.
    #[must_use]
    pub const fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    /// Lease duration in milliseconds.
    #[must_use]
    pub const fn lease_duration_ms(&self) -> u64 {
        self.lease_duration_ms
    }

    /// Concurrency limits.
    #[must_use]
    pub const fn concurrency_limits_ref(&self) -> &AgentDispatchConcurrencyLimits {
        &self.concurrency_limits
    }
}

impl Default for AgentDispatcherFleetSettings {
    fn default() -> Self {
        Self::new(32, 30_000)
    }
}

/// Durable dispatcher fleet state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDispatcherFleetState {
    entries: BTreeMap<AgentDispatchId, AgentDispatchEntry>,
    updated_at: AgentTimestampMillis,
}

impl AgentDispatcherFleetState {
    /// Creates an empty dispatcher fleet state.
    #[must_use]
    pub fn empty(now: AgentTimestampMillis) -> Self {
        Self {
            entries: BTreeMap::new(),
            updated_at: now,
        }
    }

    /// All dispatcher entries by id.
    #[must_use]
    pub const fn entries(&self) -> &BTreeMap<AgentDispatchId, AgentDispatchEntry> {
        &self.entries
    }

    /// Last update timestamp.
    #[must_use]
    pub const fn updated_at(&self) -> AgentTimestampMillis {
        self.updated_at
    }

    /// Returns one dispatcher entry.
    #[must_use]
    pub fn entry(&self, dispatch_id: &AgentDispatchId) -> Option<&AgentDispatchEntry> {
        self.entries.get(dispatch_id)
    }

    /// Returns the number of claimable entries at `now`.
    #[must_use]
    pub fn due_dispatch_count(&self, now: AgentTimestampMillis) -> usize {
        self.entries
            .values()
            .filter(|entry| entry.is_claimable_at(now))
            .count()
    }

    /// Returns the number of actively leased entries at `now`.
    #[must_use]
    pub fn in_flight_count(&self, now: AgentTimestampMillis) -> usize {
        self.entries
            .values()
            .filter(|entry| entry.is_in_flight_at(now))
            .count()
    }

    /// Builds a bounded health snapshot.
    #[must_use]
    pub fn snapshot(
        &self,
        now: AgentTimestampMillis,
        sample_limit: usize,
    ) -> AgentDispatcherSnapshot {
        AgentDispatcherSnapshot::from_state(self, now, sample_limit)
    }
}

/// Result of registering due effects into the fleet index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDispatcherRegistration {
    /// Run that was observed.
    pub run_id: AgentRunId,
    /// Number of due effects observed from the run outbox.
    pub observed_due_effects: usize,
    /// Number of fleet entries inserted or refreshed.
    pub registered_effects: usize,
}

/// Result of marking dispatch entries cancelled or cancellation requested.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDispatcherCancellation {
    /// Run whose dispatch entries were considered.
    pub run_id: AgentRunId,
    /// Timestamp used for the cancellation pass.
    pub cancelled_at: AgentTimestampMillis,
    /// Entries moved to `Cancelled`.
    pub cancelled_entries: usize,
    /// Entries already terminal before the cancellation pass.
    pub already_terminal_entries: usize,
    /// Actively leased entries marked cancellation requested. They are left
    /// for the in-flight worker to finish truthfully; if the lease expires
    /// they are finalized as cancelled on the next worker refresh.
    pub in_flight_entries: usize,
    /// Effect ids of entries moved to `Cancelled` by this pass, to settle at
    /// the durable outbox layer.
    pub cancelled_effect_ids: Vec<AgentEffectId>,
}

/// Result of one claim pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDispatchClaimBatch {
    /// Worker that claimed work.
    pub worker_id: AgentDispatcherWorkerId,
    /// Timestamp used for the claim pass.
    pub claimed_at: AgentTimestampMillis,
    /// Claimable entries before concurrency and batch limits.
    pub due_dispatch_count: usize,
    /// True when more work was due than this pass could claim.
    pub backpressure_limited: bool,
    /// Entries skipped because target concurrency was exhausted.
    pub concurrency_limited: usize,
    /// Entries this worker's claim filter refused, because they name an
    /// execution class it does not serve.
    ///
    /// A persistently non-zero value beside a non-zero `due_dispatch_count`
    /// is what distinguishes "waiting for the worker that serves this class"
    /// from "no worker in the fleet serves it" — the anti-stall signal a
    /// heterogeneous fleet needs, and one that costs no durable write.
    pub class_filtered: usize,
    /// Claims issued to the worker.
    pub claims: Vec<AgentDispatchClaim>,
}

/// Which dispatch work one worker may claim, by a bounded target attribute.
///
/// The fleet index is one shared record: every worker registers into it and
/// every worker reads it, which is what makes the durable backlog recoverable
/// on any pod. Isolation is therefore enforced where the work is *taken* —
/// here — and again where it is *authorized*, by the agent domain's dispatch
/// authority. It is never enforced by hiding the index, which carries only
/// bounded routing metadata and no secret material
/// ([specification 11.8](../../../docs/plans/rakka-agent/spec.md),
/// [16](../../../docs/plans/rakka-agent/spec.md)).
///
/// Filtering at claim time rather than refusing after the claim is what makes
/// a heterogeneous fleet work at all. A worker that refused *after* claiming
/// would hold the entry's lease while it did so, starving the worker that can
/// actually run it; and a refusal is a durable write, so every worker would
/// pay one per class it does not serve.
///
/// The attribute is matched against [`AgentEffectTarget::attributes`]. An
/// entry that does not carry the attribute at all is accepted by default:
/// unclassified work routes anywhere, and refusing it is a policy decision
/// belonging to the authorization layer, not to the fleet. A deployment that
/// wants the stricter rule at this layer too says so with
/// [`Self::without_unclassified`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AgentDispatchClaimFilter {
    attribute: Option<String>,
    accepted: BTreeSet<String>,
    accept_unclassified: bool,
}

impl AgentDispatchClaimFilter {
    /// A filter that accepts every entry.
    ///
    /// The default, and the behaviour of every worker built before this filter
    /// existed: a homogeneous fleet needs no routing, and one that has not
    /// declared its classes must not silently stop claiming work.
    #[must_use]
    pub fn any() -> Self {
        Self {
            attribute: None,
            accepted: BTreeSet::new(),
            accept_unclassified: true,
        }
    }

    /// Accepts only entries whose `attribute` names one of `accepted`.
    ///
    /// An empty `accepted` set means this worker serves no classified work at
    /// all — which is a coherent thing to configure for a worker that exists
    /// only to run unclassified effects.
    #[must_use]
    pub fn by_target_attribute(
        attribute: impl Into<String>,
        accepted: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            attribute: Some(attribute.into()),
            accepted: accepted.into_iter().map(Into::into).collect(),
            accept_unclassified: true,
        }
    }

    /// Refuses an entry that does not carry the attribute.
    #[must_use]
    pub fn without_unclassified(mut self) -> Self {
        self.accept_unclassified = false;
        self
    }

    /// Whether this worker may claim the entry.
    #[must_use]
    pub fn accepts(&self, entry: &AgentDispatchEntry) -> bool {
        let Some(attribute) = self.attribute.as_ref() else {
            return true;
        };
        match entry.target.attributes.get(attribute) {
            Some(value) => self.accepted.contains(value),
            None => self.accept_unclassified,
        }
    }
}

/// Durable dispatcher fleet facade.
pub struct AgentDispatcherFleet<Store, Clock = SystemWorkflowClock>
where
    Store: DurableStateStore<AgentDispatcherFleetState>,
    Clock: WorkflowClock,
{
    persistence_id: PersistenceId,
    store: Store,
    clock: Clock,
    settings: AgentDispatcherFleetSettings,
    claim_filter: AgentDispatchClaimFilter,
    metrics: Arc<dyn MetricsRecorder>,
    record: Option<StateRecord<AgentDispatcherFleetState>>,
}

impl<Store> AgentDispatcherFleet<Store, SystemWorkflowClock>
where
    Store: DurableStateStore<AgentDispatcherFleetState>,
{
    /// Creates a dispatcher fleet with default settings, system clock, and
    /// no-op metrics.
    #[must_use]
    pub fn new(store: Store) -> Self {
        Self::with_settings(store, AgentDispatcherFleetSettings::default())
    }

    /// Creates a dispatcher fleet with settings and the system clock.
    #[must_use]
    pub fn with_settings(store: Store, settings: AgentDispatcherFleetSettings) -> Self {
        Self::with_clock_and_metrics(
            store,
            agent_dispatcher_fleet_persistence_id(),
            settings,
            SystemWorkflowClock,
            Arc::new(NoopMetricsRecorder),
        )
    }
}

impl<Store, Clock> AgentDispatcherFleet<Store, Clock>
where
    Store: DurableStateStore<AgentDispatcherFleetState>,
    Clock: WorkflowClock,
{
    /// Restricts what this worker's handle may claim.
    ///
    /// Per *worker*, not per fleet: two workers over one shared index have
    /// different filters, which is the entire point. It deliberately does not
    /// live on [`AgentDispatcherFleetSettings`], which is serialized as part
    /// of the fleet's configuration and describes the fleet rather than the
    /// caller.
    #[must_use]
    pub fn with_claim_filter(mut self, filter: AgentDispatchClaimFilter) -> Self {
        self.set_claim_filter(filter);
        self
    }

    /// Restricts what this worker's handle may claim, in place.
    ///
    /// The `&mut` form exists because a consuming builder is unreachable from
    /// behind a `&mut self` accessor — which is how
    /// [`AgentDispatcherWorker::fleet_mut`] hands out this handle, and why
    /// that worker could not install a filter at all.
    pub fn set_claim_filter(&mut self, filter: AgentDispatchClaimFilter) {
        self.claim_filter = filter;
    }

    /// The filter this handle claims under.
    #[must_use]
    pub const fn claim_filter(&self) -> &AgentDispatchClaimFilter {
        &self.claim_filter
    }

    /// Creates a dispatcher fleet with explicit dependencies.
    #[must_use]
    pub const fn with_clock_and_metrics(
        store: Store,
        persistence_id: PersistenceId,
        settings: AgentDispatcherFleetSettings,
        clock: Clock,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self {
            persistence_id,
            store,
            clock,
            settings,
            claim_filter: AgentDispatchClaimFilter {
                attribute: None,
                accepted: BTreeSet::new(),
                accept_unclassified: true,
            },
            metrics,
            record: None,
        }
    }

    /// Fleet persistence id.
    #[must_use]
    pub const fn persistence_id(&self) -> &PersistenceId {
        &self.persistence_id
    }

    /// Fleet settings.
    #[must_use]
    pub const fn settings(&self) -> &AgentDispatcherFleetSettings {
        &self.settings
    }

    /// Current clock.
    #[must_use]
    pub const fn clock(&self) -> &Clock {
        &self.clock
    }

    /// Recovers the latest dispatcher fleet state.
    pub async fn recover(&mut self) -> AgentDispatcherResult<&AgentDispatcherFleetState> {
        let now = current_agent_timestamp(&self.clock);
        self.record = Some(
            self.store
                .load(&self.persistence_id)
                .await?
                .unwrap_or_else(|| StateRecord::missing(AgentDispatcherFleetState::empty(now))),
        );
        Ok(&self.record.as_ref().expect("record set above").state)
    }

    /// Current recovered state.
    pub fn state(&self) -> AgentDispatcherResult<&AgentDispatcherFleetState> {
        self.record
            .as_ref()
            .map(|record| &record.state)
            .ok_or_else(|| AgentDispatcherError::Persistence {
                error: DurableError::store(
                    self.store.backend_name(),
                    "dispatcher fleet is not recovered",
                ),
            })
    }

    /// Registers due effects observed from one run outbox into the fleet index.
    pub async fn register_due_effects(
        &mut self,
        run_id: AgentRunId,
        workflow_id: Option<AgentWorkflowId>,
        effects: Vec<AgentDueEffect>,
    ) -> AgentDispatcherResult<AgentDispatcherRegistration> {
        if self.record.is_none() {
            self.recover().await?;
        }

        let now = current_agent_timestamp(&self.clock);
        let observed_due_effects = effects.len();
        let record = self.current_record()?;
        let mut next = record.state;
        let mut registered_effects = 0;

        for due in effects {
            let candidate =
                AgentDispatchEntry::from_due_effect(run_id.clone(), workflow_id.clone(), &due, now);
            candidate.validate()?;
            let dispatch_id = candidate.dispatch_id.clone();
            let entry = next
                .entries
                .remove(&dispatch_id)
                .map_or(candidate, |existing| {
                    existing.upsert_from_due_effect(workflow_id.clone(), &due, now)
                });
            next.updated_at = now;
            next.entries.insert(dispatch_id, entry);
            registered_effects += 1;
        }

        self.persist(record.revision, next).await?;
        self.record_metric(
            "register",
            "registered",
            "none",
            observed_due_effects as u64,
        );
        Ok(AgentDispatcherRegistration {
            run_id,
            observed_due_effects,
            registered_effects,
        })
    }

    /// Claims due dispatch entries for one worker.
    pub async fn claim_due(
        &mut self,
        worker_id: AgentDispatcherWorkerId,
    ) -> AgentDispatcherResult<AgentDispatchClaimBatch> {
        if self.record.is_none() {
            self.recover().await?;
        }

        let now = current_agent_timestamp(&self.clock);
        let record = self.current_record()?;
        let mut next = record.state;
        let due_dispatch_count = next.due_dispatch_count(now);
        let mut in_flight_by_class = in_flight_counts_by_class(&next, now);
        let mut in_flight_by_target = in_flight_counts_by_target(&next, now);
        let mut claimable: Vec<_> = next
            .entries
            .values()
            .filter(|entry| entry.is_claimable_at(now))
            .cloned()
            .collect();
        claimable.sort_by(|left, right| {
            left.due_at
                .cmp(&right.due_at)
                .then_with(|| left.dispatch_id.cmp(&right.dispatch_id))
        });

        let mut claims = Vec::new();
        let mut concurrency_limited = 0;
        let mut class_filtered = 0;
        for entry in claimable {
            if claims.len() >= self.settings.max_batch_size {
                break;
            }
            // Before the lease, not after it: a worker that cannot run this
            // entry must not hold it away from one that can.
            if !self.claim_filter.accepts(&entry) {
                class_filtered += 1;
                continue;
            }
            let class_count = in_flight_by_class
                .get(&entry.target_class)
                .copied()
                .unwrap_or_default();
            let target_key = target_limit_key(entry.target_class, &entry.target.name);
            let target_count = in_flight_by_target
                .get(&target_key)
                .copied()
                .unwrap_or_default();
            let limit = self.settings.concurrency_limits.limit_for(&entry);
            if class_count >= limit || target_count >= limit {
                concurrency_limited += 1;
                continue;
            }

            let dispatch_id = entry.dispatch_id.clone();
            let (claimed, claim) =
                entry.claim(worker_id.clone(), now, self.settings.lease_duration_ms);
            next.updated_at = now;
            next.entries.insert(dispatch_id, claimed);
            *in_flight_by_class.entry(claim.target_class).or_default() += 1;
            *in_flight_by_target.entry(target_key).or_default() += 1;
            claims.push(claim);
        }

        let backpressure_limited = due_dispatch_count > claims.len();
        if !claims.is_empty() {
            self.persist(record.revision, next).await?;
        }
        self.record_metric("claim", "claimed", "none", claims.len() as u64);
        if class_filtered > 0 {
            self.record_metric("claim", "class-filtered", "none", class_filtered as u64);
        }
        self.record_gauges(now);
        Ok(AgentDispatchClaimBatch {
            worker_id,
            claimed_at: now,
            due_dispatch_count,
            backpressure_limited,
            concurrency_limited,
            class_filtered,
            claims,
        })
    }

    /// Marks claimable dispatch entries for a run cancelled in the fleet index.
    ///
    /// Active leases are not forcibly completed. They are marked cancellation
    /// requested and left for the worker's in-flight side effect to finish
    /// truthfully; a cancellation-requested entry is never claimable again,
    /// and if its lease expires it is finalized by
    /// [`Self::finalize_run_cancellations`]. This pass updates only the fleet
    /// index — use [`AgentDispatcherWorker::cancel_run_dispatches`] to also
    /// settle the cancelled effects at the durable outbox layer.
    pub async fn cancel_run_dispatches(
        &mut self,
        run_id: &AgentRunId,
    ) -> AgentDispatcherResult<AgentDispatcherCancellation> {
        if self.record.is_none() {
            self.recover().await?;
        }

        let now = current_agent_timestamp(&self.clock);
        let record = self.current_record()?;
        let mut next = record.state;
        let mut cancelled_entries = 0;
        let mut already_terminal_entries = 0;
        let mut in_flight_entries = 0;
        let mut cancelled_effect_ids = Vec::new();
        let mut changed = false;

        for entry in next
            .entries
            .values_mut()
            .filter(|entry| &entry.run_id == run_id)
        {
            if entry.status.is_terminal() {
                already_terminal_entries += 1;
            } else if entry.is_in_flight_at(now) {
                in_flight_entries += 1;
                entry.cancellation_requested = true;
                entry.last_error_code = Some("cancellation-requested".to_string());
                entry.updated_at = now;
                changed = true;
            } else {
                let updated = entry.clone().mark_cancelled(now);
                *entry = updated;
                cancelled_effect_ids.push(entry.effect_id.clone());
                cancelled_entries += 1;
                changed = true;
            }
        }

        if changed {
            next.updated_at = now;
            self.persist(record.revision, next).await?;
            self.record_gauges(now);
        }
        self.record_metric("cancel", "cancelled", "none", cancelled_entries as u64);
        Ok(AgentDispatcherCancellation {
            run_id: run_id.clone(),
            cancelled_at: now,
            cancelled_entries,
            already_terminal_entries,
            in_flight_entries,
            cancelled_effect_ids,
        })
    }

    /// Finalizes cancellation-requested entries for one run whose leases are
    /// no longer active, returning the effect ids to settle at the durable
    /// outbox layer.
    ///
    /// A cancellation-requested entry whose worker completes normally records
    /// a truthful terminal outcome instead; this pass only converges entries
    /// whose worker crashed or lost its lease after cancellation.
    pub async fn finalize_run_cancellations(
        &mut self,
        run_id: &AgentRunId,
    ) -> AgentDispatcherResult<Vec<AgentEffectId>> {
        if self.record.is_none() {
            self.recover().await?;
        }

        let now = current_agent_timestamp(&self.clock);
        let record = self.current_record()?;
        let mut next = record.state;
        let mut finalized_effect_ids = Vec::new();

        for entry in next.entries.values_mut().filter(|entry| {
            &entry.run_id == run_id
                && entry.cancellation_requested
                && !entry.status.is_terminal()
                && !entry.is_in_flight_at(now)
        }) {
            let updated = entry.clone().mark_cancelled(now);
            *entry = updated;
            finalized_effect_ids.push(entry.effect_id.clone());
        }

        if !finalized_effect_ids.is_empty() {
            next.updated_at = now;
            self.persist(record.revision, next).await?;
            self.record_gauges(now);
            self.record_metric(
                "cancel",
                "finalized",
                "lease-expired",
                finalized_effect_ids.len() as u64,
            );
        }
        Ok(finalized_effect_ids)
    }

    /// Completes a current claim.
    pub async fn complete_claim(
        &mut self,
        claim: &AgentDispatchClaim,
    ) -> AgentDispatcherResult<AgentDispatchEntry> {
        self.update_current_claim(claim, |entry, now| entry.mark_completed(now))
            .await
    }

    /// Records a failed claim according to the durable outbox transition.
    pub async fn record_claim_failure(
        &mut self,
        claim: &AgentDispatchClaim,
        event: &WorkflowTelemetryEvent,
    ) -> AgentDispatcherResult<AgentDispatchEntry> {
        self.update_current_claim(claim, |entry, now| match event {
            WorkflowTelemetryEvent::OutboxDispatchRetried {
                attempt,
                next_retry_at,
                message,
                ..
            }
            | WorkflowTelemetryEvent::OutboxDispatchTimedOut {
                attempt,
                next_retry_at: Some(next_retry_at),
                message,
                ..
            } => entry.mark_retry(
                agent_dispatch_timestamp_from_workflow_timestamp(*next_retry_at),
                *attempt,
                message.clone(),
                now,
            ),
            WorkflowTelemetryEvent::OutboxDispatchExhausted {
                attempts, message, ..
            }
            | WorkflowTelemetryEvent::OutboxDispatchTimedOut {
                attempt: attempts,
                next_retry_at: None,
                message,
                ..
            } => entry.mark_exhausted(*attempts, message.clone(), now),
            WorkflowTelemetryEvent::OutboxDispatchSucceeded { .. } => entry.mark_completed(now),
            WorkflowTelemetryEvent::OutboxDispatchCancelled { .. } => entry.mark_cancelled(now),
        })
        .await
    }

    /// Returns a bounded dispatcher health snapshot.
    pub fn snapshot(&self, sample_limit: usize) -> AgentDispatcherSnapshot {
        let now = current_agent_timestamp(&self.clock);
        self.state()
            .map(|state| state.snapshot(now, sample_limit))
            .unwrap_or_else(|_| AgentDispatcherSnapshot::empty(now))
    }

    /// Returns true when the claim is still current and unexpired.
    pub fn is_current_claim(&self, claim: &AgentDispatchClaim) -> AgentDispatcherResult<bool> {
        let now = current_agent_timestamp(&self.clock);
        let entry = self.state()?.entry(&claim.dispatch_id).ok_or_else(|| {
            AgentDispatcherError::DispatchNotFound {
                dispatch_id: claim.dispatch_id.clone(),
            }
        })?;
        Ok(entry.is_current_claim(claim, now))
    }

    async fn update_current_claim(
        &mut self,
        claim: &AgentDispatchClaim,
        update: impl FnOnce(AgentDispatchEntry, AgentTimestampMillis) -> AgentDispatchEntry,
    ) -> AgentDispatcherResult<AgentDispatchEntry> {
        if self.record.is_none() {
            self.recover().await?;
        }
        let now = current_agent_timestamp(&self.clock);
        let record = self.current_record()?;
        let entry = record
            .state
            .entries
            .get(&claim.dispatch_id)
            .cloned()
            .ok_or_else(|| AgentDispatcherError::DispatchNotFound {
                dispatch_id: claim.dispatch_id.clone(),
            })?;
        if !entry.is_current_claim(claim, now) {
            return Err(AgentDispatcherError::ClaimFenced {
                dispatch_id: claim.dispatch_id.clone(),
                worker_id: claim.worker_id.clone(),
                fencing_token: claim.fencing_token,
            });
        }

        let updated = update(entry, now);
        let mut next = record.state;
        next.updated_at = updated.updated_at;
        next.entries
            .insert(claim.dispatch_id.clone(), updated.clone());
        self.persist(record.revision, next).await?;
        self.record_gauges(now);
        Ok(updated)
    }

    async fn persist(
        &mut self,
        expected_revision: Revision,
        next: AgentDispatcherFleetState,
    ) -> AgentDispatcherResult<StateRecord<AgentDispatcherFleetState>> {
        let persisted = self
            .store
            .compare_and_set(&self.persistence_id, expected_revision, next)
            .await?;
        self.record = Some(persisted.clone());
        Ok(persisted)
    }

    fn current_record(&self) -> AgentDispatcherResult<StateRecord<AgentDispatcherFleetState>> {
        self.record
            .clone()
            .ok_or_else(|| AgentDispatcherError::Persistence {
                error: DurableError::store(
                    self.store.backend_name(),
                    "dispatcher fleet is not recovered",
                ),
            })
    }

    fn record_metric(
        &self,
        operation: &'static str,
        outcome: &'static str,
        detail: &'static str,
        value: u64,
    ) {
        self.metrics.increment_counter(
            METRIC_AGENT_DISPATCHER_FLEET,
            value,
            &[
                ("operation", operation),
                ("outcome", outcome),
                ("detail", detail),
            ],
        );
    }

    fn record_gauges(&self, now: AgentTimestampMillis) {
        if let Ok(state) = self.state() {
            self.metrics.record_gauge(
                METRIC_AGENT_DISPATCHER_IN_FLIGHT,
                state.in_flight_count(now) as f64,
                &[],
            );
            self.metrics.record_gauge(
                METRIC_AGENT_DISPATCHER_BACKLOG,
                state.due_dispatch_count(now) as f64,
                &[],
            );
        }
    }
}

/// One dispatch job handed to an application dispatcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDispatchJob {
    /// Current fleet claim.
    pub claim: AgentDispatchClaim,
    /// Lower-level outbox entry after it has been marked dispatching.
    pub entry: OutboxEntry,
    /// Deserialized agent effect payload.
    pub effect: AgentEffect,
}

/// Boxed future returned by agent effect dispatchers.
pub type AgentEffectDispatchFuture<'a> =
    Pin<Box<dyn Future<Output = OutboxDispatchResult> + Send + 'a>>;

/// Application-supplied dispatcher for claimed agent effects.
pub trait AgentEffectDispatcher: Send {
    /// Dispatches one claimed effect.
    fn dispatch<'a>(&'a mut self, job: &'a AgentDispatchJob) -> AgentEffectDispatchFuture<'a>;
}

/// Registry that routes claimed effects to target-class dispatchers.
#[derive(Default)]
pub struct AgentEffectDispatcherRegistry {
    dispatchers: BTreeMap<AgentDispatchTargetClass, Box<dyn AgentEffectDispatcher>>,
    fallback: Option<Box<dyn AgentEffectDispatcher>>,
}

impl AgentEffectDispatcherRegistry {
    /// Creates an empty dispatcher registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a dispatcher for one target class.
    #[must_use]
    pub fn with_dispatcher<D>(
        mut self,
        target_class: AgentDispatchTargetClass,
        dispatcher: D,
    ) -> Self
    where
        D: AgentEffectDispatcher + 'static,
    {
        self.dispatchers.insert(target_class, Box::new(dispatcher));
        self
    }

    /// Registers a fallback dispatcher used when no class-specific dispatcher exists.
    #[must_use]
    pub fn with_fallback<D>(mut self, dispatcher: D) -> Self
    where
        D: AgentEffectDispatcher + 'static,
    {
        self.fallback = Some(Box::new(dispatcher));
        self
    }

    /// Returns true when a dispatcher is registered for the class.
    #[must_use]
    pub fn contains(&self, target_class: AgentDispatchTargetClass) -> bool {
        self.dispatchers.contains_key(&target_class)
    }
}

impl AgentEffectDispatcher for AgentEffectDispatcherRegistry {
    fn dispatch<'a>(&'a mut self, job: &'a AgentDispatchJob) -> AgentEffectDispatchFuture<'a> {
        if let Some(dispatcher) = self.dispatchers.get_mut(&job.claim.target_class) {
            dispatcher.dispatch(job)
        } else if let Some(dispatcher) = self.fallback.as_mut() {
            dispatcher.dispatch(job)
        } else {
            Box::pin(async move {
                OutboxDispatchResult::failure(format!(
                    "dispatcher-unregistered:{}",
                    job.claim.target_class.as_label()
                ))
            })
        }
    }
}

/// Dispatcher that invokes an [`AgentModelAdapter`].
pub struct AgentModelEffectDispatcher<A> {
    adapter: A,
}

impl<A> AgentModelEffectDispatcher<A> {
    /// Creates a model effect dispatcher.
    #[must_use]
    pub const fn new(adapter: A) -> Self {
        Self { adapter }
    }

    /// Mutable access to the wrapped adapter.
    #[must_use]
    pub fn adapter_mut(&mut self) -> &mut A {
        &mut self.adapter
    }

    /// Consumes the dispatcher and returns the wrapped adapter.
    #[must_use]
    pub fn into_inner(self) -> A {
        self.adapter
    }
}

impl<A> AgentEffectDispatcher for AgentModelEffectDispatcher<A>
where
    A: AgentModelAdapter,
{
    fn dispatch<'a>(&'a mut self, job: &'a AgentDispatchJob) -> AgentEffectDispatchFuture<'a> {
        Box::pin(async move {
            let request = match AgentModelRequest::from_effect(job.effect.clone()) {
                Ok(request) => request,
                Err(error) => return OutboxDispatchResult::failure(error.code()),
            };
            match self.adapter.invoke_model(request).await {
                Ok(outcome) => outcome.to_outbox_dispatch_result(),
                Err(error) => OutboxDispatchResult::failure(error.code()),
            }
        })
    }
}

/// Dispatcher that invokes an [`AgentToolAdapter`].
pub struct AgentToolEffectDispatcher<A> {
    adapter: A,
}

impl<A> AgentToolEffectDispatcher<A> {
    /// Creates a tool effect dispatcher.
    #[must_use]
    pub const fn new(adapter: A) -> Self {
        Self { adapter }
    }

    /// Mutable access to the wrapped adapter.
    #[must_use]
    pub fn adapter_mut(&mut self) -> &mut A {
        &mut self.adapter
    }

    /// Consumes the dispatcher and returns the wrapped adapter.
    #[must_use]
    pub fn into_inner(self) -> A {
        self.adapter
    }
}

impl<A> AgentEffectDispatcher for AgentToolEffectDispatcher<A>
where
    A: AgentToolAdapter,
{
    fn dispatch<'a>(&'a mut self, job: &'a AgentDispatchJob) -> AgentEffectDispatchFuture<'a> {
        Box::pin(async move {
            let request = match AgentToolRequest::from_effect(job.effect.clone()) {
                Ok(request) => request,
                Err(error) => return OutboxDispatchResult::failure(error.code()),
            };
            match self.adapter.invoke_tool(request).await {
                Ok(outcome) => outcome.to_outbox_dispatch_result(),
                Err(error) => OutboxDispatchResult::failure(error.code()),
            }
        })
    }
}

/// Dispatcher that invokes an [`AgentA2APeerAdapter`].
pub struct AgentA2APeerEffectDispatcher<A> {
    adapter: A,
}

impl<A> AgentA2APeerEffectDispatcher<A> {
    /// Creates an A2A peer effect dispatcher.
    #[must_use]
    pub const fn new(adapter: A) -> Self {
        Self { adapter }
    }

    /// Mutable access to the wrapped adapter.
    #[must_use]
    pub fn adapter_mut(&mut self) -> &mut A {
        &mut self.adapter
    }

    /// Consumes the dispatcher and returns the wrapped adapter.
    #[must_use]
    pub fn into_inner(self) -> A {
        self.adapter
    }
}

impl<A> AgentEffectDispatcher for AgentA2APeerEffectDispatcher<A>
where
    A: AgentA2APeerAdapter,
{
    fn dispatch<'a>(&'a mut self, job: &'a AgentDispatchJob) -> AgentEffectDispatchFuture<'a> {
        Box::pin(async move {
            let request = match AgentA2APeerRequest::from_effect(job.effect.clone()) {
                Ok(request) => request,
                Err(error) => return OutboxDispatchResult::failure(error.code()),
            };
            match self.adapter.invoke_peer(request).await {
                Ok(outcome) => outcome.to_outbox_dispatch_result(),
                Err(error) => OutboxDispatchResult::failure(error.code()),
            }
        })
    }
}

/// Result of one dispatched claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDispatchCompletion {
    /// Claim that was dispatched.
    pub claim: AgentDispatchClaim,
    /// Dispatcher result.
    pub result: OutboxDispatchResult,
    /// Durable outbox telemetry event recorded for the result.
    pub telemetry_event: WorkflowTelemetryEvent,
    /// Updated fleet entry.
    pub entry: AgentDispatchEntry,
}

/// One dispatcher worker cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDispatcherCycle {
    /// Due-effect registrations performed before claiming.
    pub registrations: Vec<AgentDispatcherRegistration>,
    /// Claim batch.
    pub claims: AgentDispatchClaimBatch,
    /// Claim dispatch completions.
    pub completions: Vec<AgentDispatchCompletion>,
}

/// Runtime facade for one dispatcher worker.
pub struct AgentDispatcherWorker<FleetStore, WorkflowStore, Clock = SystemWorkflowClock>
where
    FleetStore: DurableStateStore<AgentDispatcherFleetState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    worker_id: AgentDispatcherWorkerId,
    fleet: AgentDispatcherFleet<FleetStore, Clock>,
    workflow_store: WorkflowStore,
    clock: Clock,
    metrics: Arc<dyn MetricsRecorder>,
}

impl<FleetStore, WorkflowStore>
    AgentDispatcherWorker<FleetStore, WorkflowStore, SystemWorkflowClock>
where
    FleetStore: DurableStateStore<AgentDispatcherFleetState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
{
    /// Creates a dispatcher worker using default fleet settings.
    #[must_use]
    pub fn new(
        worker_id: AgentDispatcherWorkerId,
        fleet_store: FleetStore,
        workflow_store: WorkflowStore,
    ) -> Self {
        Self::with_settings(
            worker_id,
            fleet_store,
            workflow_store,
            AgentDispatcherFleetSettings::default(),
        )
    }

    /// Creates a dispatcher worker with explicit settings.
    #[must_use]
    pub fn with_settings(
        worker_id: AgentDispatcherWorkerId,
        fleet_store: FleetStore,
        workflow_store: WorkflowStore,
        settings: AgentDispatcherFleetSettings,
    ) -> Self {
        Self::with_clock_and_metrics(
            worker_id,
            fleet_store,
            workflow_store,
            settings,
            SystemWorkflowClock,
            Arc::new(NoopMetricsRecorder),
        )
    }
}

impl<FleetStore, WorkflowStore, Clock> AgentDispatcherWorker<FleetStore, WorkflowStore, Clock>
where
    FleetStore: DurableStateStore<AgentDispatcherFleetState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    /// Creates a dispatcher worker with explicit dependencies.
    #[must_use]
    pub fn with_clock_and_metrics(
        worker_id: AgentDispatcherWorkerId,
        fleet_store: FleetStore,
        workflow_store: WorkflowStore,
        settings: AgentDispatcherFleetSettings,
        clock: Clock,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        let fleet = AgentDispatcherFleet::with_clock_and_metrics(
            fleet_store,
            agent_dispatcher_fleet_persistence_id(),
            settings,
            clock.clone(),
            metrics.clone(),
        );
        Self {
            worker_id,
            fleet,
            workflow_store,
            clock,
            metrics,
        }
    }

    /// Worker id.
    #[must_use]
    pub const fn worker_id(&self) -> &AgentDispatcherWorkerId {
        &self.worker_id
    }

    /// Fleet facade.
    #[must_use]
    pub const fn fleet(&self) -> &AgentDispatcherFleet<FleetStore, Clock> {
        &self.fleet
    }

    /// Mutable fleet facade.
    #[must_use]
    pub fn fleet_mut(&mut self) -> &mut AgentDispatcherFleet<FleetStore, Clock> {
        &mut self.fleet
    }

    /// Restricts what this worker may claim.
    ///
    /// Without this the worker served everything, with no way to say
    /// otherwise: it builds its fleet handle privately and hands it out only
    /// behind `&mut`, which a consuming builder cannot reach. A deployment
    /// mixing this worker with class-restricted ones over the same fleet
    /// index therefore kept the race the claim filter exists to remove — the
    /// unfiltered worker claims a ticket it cannot run, and its dispatcher
    /// fails the effect permanently while a worker that serves the class
    /// stands by.
    ///
    /// The filter is per *worker*, not per fleet, which is the whole point:
    /// several workers share one index and accept different classes. Build one
    /// with [`AgentDispatchClaimFilter::by_target_attribute`] over whatever
    /// attribute the effects carry — `rakka-agent` routes on
    /// `agent_effect_execution_policy`.
    #[must_use]
    pub fn with_claim_filter(mut self, filter: AgentDispatchClaimFilter) -> Self {
        self.fleet.set_claim_filter(filter);
        self
    }

    /// Restricts what this worker may claim, in place.
    pub fn set_claim_filter(&mut self, filter: AgentDispatchClaimFilter) {
        self.fleet.set_claim_filter(filter);
    }

    /// Recovers the fleet state.
    pub async fn recover(&mut self) -> AgentDispatcherResult<&AgentDispatcherFleetState> {
        self.fleet.recover().await
    }

    /// Refreshes due effects for one run.
    ///
    /// Cancellation-requested entries whose leases expired are finalized as
    /// cancelled before registration, and every cancelled fleet entry whose
    /// outbox message is still unsettled is settled as cancelled — so a
    /// cancelled run's effects are never redelivered after a worker crash,
    /// even one that interrupted an earlier cancellation pass between its
    /// fleet and outbox writes.
    pub async fn refresh_run(
        &mut self,
        run_id: AgentRunId,
        workflow_id: Option<AgentWorkflowId>,
    ) -> AgentDispatcherResult<AgentDispatcherRegistration> {
        let mut inbox = AgentRunInbox::with_clock_and_metrics(
            run_id.clone(),
            self.workflow_store.clone(),
            self.clock.clone(),
            self.metrics.clone(),
        );
        inbox.recover().await?;
        self.fleet.finalize_run_cancellations(&run_id).await?;
        let cancelled_effect_ids: Vec<AgentEffectId> = self
            .fleet
            .state()?
            .entries()
            .values()
            .filter(|entry| {
                entry.run_id == run_id && entry.status == AgentDispatchStatus::Cancelled
            })
            .map(|entry| entry.effect_id.clone())
            .collect();
        self.settle_cancelled_effects(&mut inbox, &cancelled_effect_ids, "dispatch-cancelled")
            .await?;
        let effects = inbox.due_effects()?;
        self.fleet
            .register_due_effects(run_id, workflow_id, effects)
            .await
    }

    /// Cancels a run's dispatch entries and settles the cancelled effects at
    /// the durable outbox layer.
    ///
    /// Unclaimed entries are marked cancelled in both the fleet index and the
    /// run's durable outbox, so they are never dispatched and their durable
    /// state converges. Actively leased entries are marked cancellation
    /// requested and left for the in-flight side effect to finish truthfully
    /// or be finalized after lease expiry by [`Self::refresh_run`].
    pub async fn cancel_run_dispatches(
        &mut self,
        run_id: &AgentRunId,
    ) -> AgentDispatcherResult<AgentDispatcherCancellation> {
        let cancellation = self.fleet.cancel_run_dispatches(run_id).await?;
        if !cancellation.cancelled_effect_ids.is_empty() {
            let mut inbox = AgentRunInbox::with_clock_and_metrics(
                run_id.clone(),
                self.workflow_store.clone(),
                self.clock.clone(),
                self.metrics.clone(),
            );
            inbox.recover().await?;
            self.settle_cancelled_effects(
                &mut inbox,
                &cancellation.cancelled_effect_ids,
                "run-cancelled",
            )
            .await?;
        }
        Ok(cancellation)
    }

    /// Settles cancelled effects at the durable outbox layer.
    ///
    /// Outbox entries that already reached a terminal state (or were
    /// compacted away) are skipped, so the settlement pass is idempotent and
    /// safe to repeat on every refresh.
    async fn settle_cancelled_effects(
        &self,
        inbox: &mut AgentRunInbox<WorkflowStore, Clock>,
        effect_ids: &[AgentEffectId],
        reason: &str,
    ) -> AgentDispatcherResult<()> {
        for effect_id in effect_ids {
            let message_id = OutboxMessageId::new(effect_id.as_str());
            let cancellable = inbox
                .inner()
                .state()
                .map_err(AgentDispatcherError::from)?
                .outbox_entry(&message_id)
                .is_some_and(OutboxEntry::is_cancellable);
            if cancellable {
                inbox
                    .inner_mut()
                    .record_outbox_cancelled(&message_id, reason)
                    .await
                    .map_err(AgentDispatcherError::from)?;
            }
        }
        Ok(())
    }

    /// Claims due work for this worker.
    pub async fn claim_due(&mut self) -> AgentDispatcherResult<AgentDispatchClaimBatch> {
        self.fleet.claim_due(self.worker_id.clone()).await
    }

    /// Dispatches one current claim.
    pub async fn dispatch_claim<D>(
        &mut self,
        claim: AgentDispatchClaim,
        dispatcher: &mut D,
    ) -> AgentDispatcherResult<AgentDispatchCompletion>
    where
        D: AgentEffectDispatcher,
    {
        if !self.fleet.is_current_claim(&claim)? {
            return Err(AgentDispatcherError::ClaimFenced {
                dispatch_id: claim.dispatch_id.clone(),
                worker_id: claim.worker_id.clone(),
                fencing_token: claim.fencing_token,
            });
        }

        let message_id = OutboxMessageId::new(claim.effect_id.as_str());
        let mut inbox = AgentRunInbox::with_clock_and_metrics(
            claim.run_id.clone(),
            self.workflow_store.clone(),
            self.clock.clone(),
            self.metrics.clone(),
        );
        inbox.recover().await?;
        let entry = inbox
            .inner_mut()
            .mark_outbox_dispatching(&message_id)
            .await?;
        let effect = decode_agent_effect(&entry)?;
        let job = AgentDispatchJob {
            claim: claim.clone(),
            entry,
            effect,
        };
        self.record_dispatch_metric(&job, "dispatch", "started", "none");
        let result = dispatcher.dispatch(&job).await;

        if !self.fleet.is_current_claim(&claim)? {
            self.record_dispatch_metric(&job, "dispatch", "fenced", "claim-fenced");
            return Err(AgentDispatcherError::ClaimFenced {
                dispatch_id: claim.dispatch_id.clone(),
                worker_id: claim.worker_id.clone(),
                fencing_token: claim.fencing_token,
            });
        }

        let telemetry_event = match &result {
            OutboxDispatchResult::Success => {
                inbox.inner_mut().record_outbox_success(&message_id).await?
            }
            // The dispatcher is application-implemented and its message is
            // unbounded. It reaches the durable outbox row and, through the
            // telemetry event, the fleet index — so it is bounded before the
            // write, exactly as `AgentDispatchEntry` bounds what it keeps.
            OutboxDispatchResult::Failure { message } => {
                inbox
                    .inner_mut()
                    .record_outbox_failure(&message_id, bounded_dispatch_detail(message), false)
                    .await?
            }
            OutboxDispatchResult::Timeout { message } => {
                inbox
                    .inner_mut()
                    .record_outbox_failure(&message_id, bounded_dispatch_detail(message), true)
                    .await?
            }
        };

        let entry = match &result {
            OutboxDispatchResult::Success => self.fleet.complete_claim(&claim).await?,
            OutboxDispatchResult::Failure { .. } | OutboxDispatchResult::Timeout { .. } => {
                self.fleet
                    .record_claim_failure(&claim, &telemetry_event)
                    .await?
            }
        };
        let (outcome, detail) = dispatch_result_labels(&result, &telemetry_event);
        self.record_dispatch_metric(&job, "dispatch", outcome, detail);
        Ok(AgentDispatchCompletion {
            claim,
            result,
            telemetry_event,
            entry,
        })
    }

    /// Runs one bounded worker cycle over a provided run set.
    pub async fn run_once<D>(
        &mut self,
        run_ids: impl IntoIterator<Item = AgentRunId>,
        dispatcher: &mut D,
    ) -> AgentDispatcherResult<AgentDispatcherCycle>
    where
        D: AgentEffectDispatcher,
    {
        self.recover().await?;
        let mut registrations = Vec::new();
        for run_id in run_ids {
            registrations.push(self.refresh_run(run_id, None).await?);
        }
        let claims = self.claim_due().await?;
        let mut completions = Vec::new();
        for claim in claims.claims.clone() {
            completions.push(self.dispatch_claim(claim, dispatcher).await?);
        }
        Ok(AgentDispatcherCycle {
            registrations,
            claims,
            completions,
        })
    }

    fn record_dispatch_metric(
        &self,
        job: &AgentDispatchJob,
        operation: &'static str,
        outcome: &'static str,
        detail: &'static str,
    ) {
        self.metrics.increment_counter(
            METRIC_AGENT_DISPATCHER_FLEET,
            1,
            &[
                ("operation", operation),
                ("effect_kind", job.effect.kind.as_label()),
                ("target_class", job.claim.target_class.as_label()),
                ("outcome", outcome),
                ("detail", detail),
            ],
        );
    }
}

/// Bounded dispatcher status count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDispatcherStatusCount {
    /// Status label.
    pub status: AgentDispatchStatus,
    /// Number of entries in this status.
    pub count: usize,
}

/// Bounded target-class count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDispatcherTargetClassCount {
    /// Target class.
    pub target_class: AgentDispatchTargetClass,
    /// Number of entries in this class.
    pub count: usize,
}

/// Bounded sample of one dispatcher entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDispatcherEntrySnapshot {
    /// Dispatch id.
    pub dispatch_id: AgentDispatchId,
    /// Run id.
    pub run_id: AgentRunId,
    /// Effect id.
    pub effect_id: AgentEffectId,
    /// Status.
    pub status: AgentDispatchStatus,
    /// Target class.
    pub target_class: AgentDispatchTargetClass,
    /// Compiled plan fingerprint for graph-scheduled effects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_plan_fingerprint: Option<AgentCompiledPlanFingerprint>,
    /// Compiled graph node id for graph-scheduled effects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_node_id: Option<AgentCompiledNodeId>,
    /// Compiled graph node kind for graph-scheduled effects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_node_kind: Option<AgentCompiledNodeKind>,
    /// Loop instance id for graph-scheduled effects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_loop_instance_id: Option<String>,
    /// Due timestamp.
    pub due_at: AgentTimestampMillis,
    /// True when durable cancellation was requested while a lease was active.
    #[serde(default)]
    pub cancellation_requested: bool,
    /// Worker holding the lease, when claimed.
    pub worker_id: Option<AgentDispatcherWorkerId>,
    /// Fencing token, when claimed.
    pub fencing_token: Option<u64>,
    /// Lease expiration timestamp, when claimed.
    pub lease_expires_at: Option<AgentTimestampMillis>,
}

impl AgentDispatcherEntrySnapshot {
    fn from_entry(entry: &AgentDispatchEntry) -> Self {
        Self {
            dispatch_id: entry.dispatch_id.clone(),
            run_id: entry.run_id.clone(),
            effect_id: entry.effect_id.clone(),
            status: entry.status,
            target_class: entry.target_class,
            graph_plan_fingerprint: entry.graph_plan_fingerprint.clone(),
            graph_node_id: entry.graph_node_id.clone(),
            graph_node_kind: entry.graph_node_kind,
            graph_loop_instance_id: entry.graph_loop_instance_id.clone(),
            due_at: entry.due_at,
            cancellation_requested: entry.cancellation_requested,
            worker_id: entry.lease.as_ref().map(|lease| lease.worker_id.clone()),
            fencing_token: entry.lease.as_ref().map(|lease| lease.fencing_token),
            lease_expires_at: entry.lease.as_ref().map(|lease| lease.lease_expires_at),
        }
    }
}

/// Bounded dispatcher fleet health snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDispatcherSnapshot {
    /// Snapshot timestamp.
    pub observed_at: AgentTimestampMillis,
    /// Total known dispatch entries.
    pub observed_dispatch_count: usize,
    /// Claimable dispatch entries.
    pub due_dispatch_count: usize,
    /// Actively leased dispatch entries.
    pub in_flight_count: usize,
    /// Expired claimed entries.
    pub expired_lease_count: usize,
    /// Entries grouped by status.
    pub status_counts: Vec<AgentDispatcherStatusCount>,
    /// In-flight entries grouped by target class.
    pub in_flight_by_target_class: Vec<AgentDispatcherTargetClassCount>,
    /// Bounded sample of due or in-flight entries.
    pub sampled_entries: Vec<AgentDispatcherEntrySnapshot>,
}

impl AgentDispatcherSnapshot {
    /// Creates an empty snapshot.
    #[must_use]
    pub fn empty(observed_at: AgentTimestampMillis) -> Self {
        Self {
            observed_at,
            observed_dispatch_count: 0,
            due_dispatch_count: 0,
            in_flight_count: 0,
            expired_lease_count: 0,
            status_counts: Vec::new(),
            in_flight_by_target_class: Vec::new(),
            sampled_entries: Vec::new(),
        }
    }

    fn from_state(
        state: &AgentDispatcherFleetState,
        now: AgentTimestampMillis,
        sample_limit: usize,
    ) -> Self {
        let mut statuses = BTreeMap::<AgentDispatchStatus, usize>::new();
        let mut in_flight_classes = BTreeMap::<AgentDispatchTargetClass, usize>::new();
        let mut expired_lease_count = 0;
        for entry in state.entries.values() {
            *statuses.entry(entry.status).or_default() += 1;
            if entry.is_in_flight_at(now) {
                *in_flight_classes.entry(entry.target_class).or_default() += 1;
            }
            if entry.status == AgentDispatchStatus::Claimed
                && entry
                    .lease
                    .as_ref()
                    .is_some_and(|lease| !lease.is_active_at(now))
            {
                expired_lease_count += 1;
            }
        }

        let mut sampled_entries: Vec<_> = state
            .entries
            .values()
            .filter(|entry| entry.is_claimable_at(now) || entry.is_in_flight_at(now))
            .map(AgentDispatcherEntrySnapshot::from_entry)
            .collect();
        sampled_entries.sort_by(|left, right| {
            left.due_at
                .cmp(&right.due_at)
                .then_with(|| left.dispatch_id.cmp(&right.dispatch_id))
        });
        sampled_entries.truncate(sample_limit.max(1));

        Self {
            observed_at: now,
            observed_dispatch_count: state.entries.len(),
            due_dispatch_count: state.due_dispatch_count(now),
            in_flight_count: state.in_flight_count(now),
            expired_lease_count,
            status_counts: statuses
                .into_iter()
                .map(|(status, count)| AgentDispatcherStatusCount { status, count })
                .collect(),
            in_flight_by_target_class: in_flight_classes
                .into_iter()
                .map(|(target_class, count)| AgentDispatcherTargetClassCount {
                    target_class,
                    count,
                })
                .collect(),
            sampled_entries,
        }
    }
}

fn decode_agent_effect(entry: &OutboxEntry) -> AgentDispatcherResult<AgentEffect> {
    serde_json::from_slice(entry.payload()).map_err(|error| AgentDispatcherError::Deserialization {
        message: error.to_string(),
    })
}

fn current_agent_timestamp(clock: &impl WorkflowClock) -> AgentTimestampMillis {
    agent_dispatch_timestamp_from_workflow_timestamp(clock.now())
}

fn target_limit_key(class: AgentDispatchTargetClass, target_name: &str) -> String {
    format!("{}:{target_name}", class.as_label())
}

fn dispatch_class_from_label(value: &str) -> Option<AgentDispatchTargetClass> {
    Some(match value {
        "model" => AgentDispatchTargetClass::Model,
        "tool" => AgentDispatchTargetClass::Tool,
        "process" | "process-tool" => AgentDispatchTargetClass::Process,
        "a2a-peer" => AgentDispatchTargetClass::A2aPeer,
        "http" => AgentDispatchTargetClass::Http,
        "grpc" => AgentDispatchTargetClass::Grpc,
        "webhook" => AgentDispatchTargetClass::Webhook,
        "notification" => AgentDispatchTargetClass::Notification,
        "push" | "a2a-push" | "push-notification" => AgentDispatchTargetClass::PushNotification,
        "human" | "human-checkpoint" => AgentDispatchTargetClass::Human,
        "stream" => AgentDispatchTargetClass::Stream,
        "artifact" => AgentDispatchTargetClass::Artifact,
        "child-workflow" => AgentDispatchTargetClass::ChildWorkflow,
        "audit" => AgentDispatchTargetClass::Audit,
        "other" => AgentDispatchTargetClass::Other,
        _ => return None,
    })
}

fn graph_dispatch_context_from_effect(effect: &AgentEffect) -> AgentDispatchGraphContext {
    let mut context = AgentDispatchGraphContext::default();
    let attrs = &effect.target.attributes;

    context.plan_fingerprint = attrs
        .get(ATTR_COMPILED_PLAN_FINGERPRINT)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .map(AgentCompiledPlanFingerprint::new);
    context.node_id = attrs
        .get(ATTR_COMPILED_NODE_ID)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .map(AgentCompiledNodeId::new);
    context.node_kind = attrs
        .get(ATTR_NODE_KIND)
        .and_then(|value| AgentCompiledNodeKind::from_label(value));
    context.loop_instance_id = attrs
        .get(ATTR_LOOP_INSTANCE_ID)
        .filter(|value| !value.trim().is_empty())
        .cloned();

    for key in [
        ATTR_TARGET_CLASS,
        ATTR_NODE_KIND,
        ATTR_COMPILED_NODE_ID,
        ATTR_COMPILED_PLAN_FINGERPRINT,
        ATTR_LOOP_INSTANCE_ID,
    ] {
        if let Some(value) = attrs.get(key).filter(|value| !value.trim().is_empty()) {
            context.attributes.insert(key.to_string(), value.clone());
        }
    }

    context
}

fn in_flight_counts_by_class(
    state: &AgentDispatcherFleetState,
    now: AgentTimestampMillis,
) -> BTreeMap<AgentDispatchTargetClass, usize> {
    let mut counts = BTreeMap::new();
    for entry in state.entries.values() {
        if entry.is_in_flight_at(now) {
            *counts.entry(entry.target_class).or_default() += 1;
        }
    }
    counts
}

fn in_flight_counts_by_target(
    state: &AgentDispatcherFleetState,
    now: AgentTimestampMillis,
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for entry in state.entries.values() {
        if entry.is_in_flight_at(now) {
            *counts
                .entry(target_limit_key(entry.target_class, &entry.target.name))
                .or_default() += 1;
        }
    }
    counts
}

fn dispatch_result_labels(
    result: &OutboxDispatchResult,
    event: &WorkflowTelemetryEvent,
) -> (&'static str, &'static str) {
    match (result, event) {
        (OutboxDispatchResult::Success, _) => ("succeeded", "none"),
        (
            OutboxDispatchResult::Failure { .. },
            WorkflowTelemetryEvent::OutboxDispatchRetried { .. },
        ) => ("retry-scheduled", "failure"),
        (
            OutboxDispatchResult::Timeout { .. },
            WorkflowTelemetryEvent::OutboxDispatchTimedOut { .. },
        ) => ("retry-scheduled", "timeout"),
        (
            OutboxDispatchResult::Failure { .. },
            WorkflowTelemetryEvent::OutboxDispatchExhausted { .. },
        )
        | (
            OutboxDispatchResult::Timeout { .. },
            WorkflowTelemetryEvent::OutboxDispatchExhausted { .. },
        ) => ("exhausted", "retry-budget"),
        _ => ("recorded", "outbox-transition"),
    }
}

fn require_dispatch(
    dispatch_id: &AgentDispatchId,
    value: &str,
    field: &'static str,
) -> AgentDispatcherResult<()> {
    if value.trim().is_empty() {
        return Err(AgentDispatcherError::InvalidEntry {
            dispatch_id: dispatch_id.clone(),
            field,
            reason: "must not be empty",
        });
    }
    Ok(())
}
