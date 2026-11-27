//! Durable dispatcher fleet for agent workflow outbox effects.
//!
//! The per-run durable outbox remains the source of execution truth. This
//! module adds a fleet-level index for cross-run discovery, leases, fencing,
//! target concurrency limits, and bounded health snapshots.

use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::hash::{BuildHasher, Hasher};
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use rakka_core::{MetricsRecorder, NoopMetricsRecorder};
use rakka_persistence::{DurableError, DurableStateStore, PersistenceId, Revision, StateRecord};
use rakka_workflow::{
    OutboxDispatchResult, OutboxEntry, OutboxMessageId, OutboxStatus, SystemWorkflowClock,
    WorkflowClock, WorkflowError, WorkflowState, WorkflowTelemetryEvent, WorkflowTimestamp,
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

/// Gauge for the entries the fleet record holds, by dispatch status.
///
/// One sample per [`AgentDispatchStatus`] under the label `status`, a status
/// with no entries reported as zero. The finished statuses are what a
/// deployment that never calls [`AgentDispatcherFleet::prune_terminal`]
/// watches grow.
pub const METRIC_AGENT_DISPATCHER_ENTRIES: &str = "rakka.agent_workflow.dispatcher.entries";

/// How many more times a fleet write that lost its compare-and-set is read
/// again and re-applied before the loss is returned.
///
/// The same bound the agent dispatcher gives its outbox writes: a
/// convergence aid for a record several handles write, never the fence.
const FLEET_WRITE_REAPPLICATIONS: u32 = 3;

/// The `detail` a counted re-application carries: which re-application of
/// its write it is. One label for each the bound allows, so the bound cannot
/// move without them.
const FLEET_REAPPLICATION_LABELS: [&str; FLEET_WRITE_REAPPLICATIONS as usize] = ["1", "2", "3"];

/// The least the first re-application of a fleet write waits, in
/// milliseconds. Each later one waits at least twice what the one before it
/// did, and each waits less than twice its own least.
const FLEET_REAPPLICATION_PAUSE_MS: u64 = 5;

/// What a write's first attempt is made from, which decides what its first
/// loss means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FirstAttempt {
    /// The copy an earlier call of this handle left. A settle's: its copy
    /// dates from its claim, so it is as old as the call it settles, and
    /// any other worker's write in that time makes it lose. A first loss
    /// says the copy was old, and the settle is made again at once.
    FromAnOlderCopy,
    /// A read made for this write or, for a claim and a registration,
    /// usually just before it. A first loss is already a write that landed
    /// between a read and the write that followed it, so every
    /// re-application waits. Where the copy was in fact old, the cost is
    /// that first wait: under 10 ms.
    FromARead,
}

/// How long the `nth` re-application of a fleet write waits before it reads
/// the record again, from 64 random bits: between 5 and 10 ms for the
/// first, 10 and 20 ms for the second, 20 and 40 ms for the third.
///
/// Writers that lose to the same write would otherwise read again together
/// and lose to each other, so each waits a different time. A wait separates
/// writers. It does not shorten the time between a read and its write,
/// which is where a write is lost and which grows with the record.
fn reapplication_pause(nth: u32, entropy: u64) -> Duration {
    let least_ms = FLEET_REAPPLICATION_PAUSE_MS << nth.saturating_sub(1);
    // Whole milliseconds: timers are seldom finer, and round up.
    Duration::from_millis(least_ms + entropy % least_ms)
}

/// 64 bits that differ from one call to the next, from one thread to the
/// next and from one process to the next: the standard library's hash
/// keys, which are seeded for each thread and moved on for each new
/// `RandomState`. That is all a spread of waits needs.
fn reapplication_entropy() -> u64 {
    RandomState::new().build_hasher().finish()
}

/// The future a fleet's wait answers: it ends after about the time it was
/// asked for.
pub type AgentFleetPauseFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// How a fleet handle waits before it makes a lost write again
/// ([`AgentDispatcherFleet::set_write_pause`]).
///
/// A fleet write that loses its compare-and-set is read again and made
/// again, and most re-applications wait a short random time first. The
/// fleet asks for the time; this is what does the waiting.
///
/// The fleet never waits on its own clock. A deployment may hand it a
/// logical clock that moves only when it is told to, and a wait that
/// watched that clock would not end.
#[derive(Clone, Default)]
pub enum AgentFleetWritePause {
    /// A thread sleeps for the time asked and wakes the task. The default.
    ///
    /// It needs no async runtime and no timer, so a fleet handle waits the
    /// same way inside any executor and cannot panic for want of one. Each
    /// wait starts one short-lived thread; a handle makes one write at a
    /// time, so a process has at most one such thread for each handle. If
    /// the thread cannot be started, the re-application follows at once.
    #[default]
    Thread,
    /// The application's own wait: given how long, it answers a future that
    /// ends after about that long. `AgentFleetWritePause::with` builds it.
    ///
    /// For an application that would wait on its runtime's timer, or that
    /// runs under a simulated time of its own. A future that ends at once
    /// turns the waits off.
    With(Arc<dyn Fn(Duration) -> AgentFleetPauseFuture + Send + Sync>),
}

impl AgentFleetWritePause {
    /// The application's own wait, from a function such as a runtime's
    /// `sleep`.
    #[must_use]
    pub fn with<Wait, Waiting>(wait: Wait) -> Self
    where
        Wait: Fn(Duration) -> Waiting + Send + Sync + 'static,
        Waiting: Future<Output = ()> + Send + 'static,
    {
        Self::With(Arc::new(move |pause| Box::pin(wait(pause))))
    }

    /// The wait itself, for the time asked.
    fn wait(&self, pause: Duration) -> AgentFleetPauseFuture {
        match self {
            Self::Thread => Box::pin(ThreadPause::new(pause)),
            Self::With(wait) => wait(pause),
        }
    }
}

impl fmt::Debug for AgentFleetWritePause {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Thread => "AgentFleetWritePause::Thread",
            Self::With(_) => "AgentFleetWritePause::With(..)",
        })
    }
}

/// A wait that needs no async runtime: on its first poll it starts a thread
/// that sleeps for the time asked and then wakes the task.
struct ThreadPause {
    /// The time asked, until the thread is started.
    pause: Option<Duration>,
    shared: Arc<Mutex<ThreadPauseState>>,
}

/// What a [`ThreadPause`] and its thread share.
#[derive(Default)]
struct ThreadPauseState {
    over: bool,
    waker: Option<Waker>,
}

impl ThreadPause {
    fn new(pause: Duration) -> Self {
        Self {
            pause: Some(pause),
            shared: Arc::default(),
        }
    }
}

impl Future for ThreadPause {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        {
            let mut state = self.shared.lock().unwrap_or_else(PoisonError::into_inner);
            if state.over {
                return Poll::Ready(());
            }
            state.waker = Some(context.waker().clone());
        }
        let Some(pause) = self.pause.take() else {
            return Poll::Pending;
        };
        let shared = self.shared.clone();
        let started = std::thread::Builder::new()
            .name("rakka-fleet-write-pause".to_string())
            .spawn(move || {
                std::thread::sleep(pause);
                let waker = {
                    let mut state = shared.lock().unwrap_or_else(PoisonError::into_inner);
                    state.over = true;
                    state.waker.take()
                };
                if let Some(waker) = waker {
                    waker.wake();
                }
            });
        match started {
            Ok(_) => Poll::Pending,
            // No thread to wait on: the re-application follows at once.
            Err(_) => Poll::Ready(()),
        }
    }
}

/// The stable code of a lost compare-and-set, under whichever wrapper
/// carries it.
const REVISION_CONFLICT_CODE: &str = "revision-conflict";

/// The code an entry carries from the moment its run's cancellation was
/// requested while a worker held its lease.
const CANCELLATION_REQUESTED: &str = "cancellation-requested";

/// The last failure line of an outbox row a cancellation settled while an
/// attempt was in flight (issue #209).
///
/// A `Dispatching` row's attempt started, and its outcome was never
/// recorded. [`AgentDispatcherWorker::cancel_run_dispatches`] and
/// [`AgentDispatcherWorker::refresh_run`] cancel such a row through
/// `DurableInbox::record_outbox_cancelled_in_flight`, which counts that
/// attempt under this line. A reader tells the row by it from one whose
/// next attempt never started: the attempt it counts may have reached its
/// target. The agent dispatcher parks such a call for a decision.
///
/// The line is stored on rows, so it must never change.
pub const DISPATCH_CANCELLED_IN_FLIGHT_LINE: &str = "cancelled while an attempt was in flight";

/// The code a fleet entry carries when its claim was settled to match an
/// outbox row that was already finished: another worker finished the row, or
/// a crash fell between the outbox write and the fleet write.
const TICKET_ROW_SETTLED: &str = "ticket-row-settled";

/// The code a fleet entry carries when its claim was settled because the
/// run's outbox holds no row for it.
const TICKET_ROW_MISSING: &str = "ticket-row-missing";

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
    /// The claimed work has nothing to dispatch: its outbox row is already
    /// finished, or the run's outbox holds no row for it. The claim's fleet
    /// entry was settled to match, and nothing was dispatched.
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
                write!(
                    f,
                    "agent dispatch entry {dispatch_id} has nothing to dispatch: its outbox row is finished or absent"
                )
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
    /// Every dispatch status, in declaration order.
    pub const ALL: [Self; 6] = [
        Self::Pending,
        Self::Claimed,
        Self::Completed,
        Self::RetryScheduled,
        Self::Exhausted,
        Self::Cancelled,
    ];

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
    /// When the outbox row this entry was first registered from was
    /// scheduled.
    ///
    /// A row's scheduling stamp is written once, when the row is created,
    /// and no retry changes it, so it tells the row the entry indexes from
    /// a row scheduled later under the same message id: what an outbox
    /// owner writes when the first row was lost. Set by the registration
    /// that creates the entry and by nothing after it. `None` on an entry
    /// written before the field existed, which indexes whatever row it is
    /// shown ([`Self::indexes_row`]).
    ///
    /// The stamp is the instant the row was written only for an effect that
    /// names no due time of its own; one that does is stamped with that
    /// time, whenever its row is written, and a second row for it carries
    /// the same stamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_scheduled_at: Option<AgentTimestampMillis>,
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
    /// Stable error code or summary for the last failed dispatch, or — on a
    /// ticket settled as cancelled through
    /// [`AgentDispatcherFleet::record_claim_failure`] — the reason it was
    /// settled (`ticket-stale`, `intent-already-resolved`, `run-cancelled`,
    /// or an application refusal's code). The outbox row keeps only the
    /// cancelled status, so this is where a settle reason is durable.
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
            row_scheduled_at: Some(agent_dispatch_timestamp_from_workflow_timestamp(
                due.entry.scheduled_at(),
            )),
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

    /// Returns true when this entry is fenced by a cancellation request,
    /// not yet finished, and held by no active lease at `now`: its outbox
    /// row is the next thing to settle, and then the entry can be finished.
    #[must_use]
    pub fn awaits_cancellation_settlement(&self, now: AgentTimestampMillis) -> bool {
        self.cancellation_requested && !self.status.is_terminal() && !self.is_in_flight_at(now)
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

    /// Returns true when `row` is the outbox row this entry was registered
    /// from, as far as the entry can tell.
    ///
    /// False only when the entry recorded its row's scheduling stamp
    /// ([`Self::row_scheduled_at`]) and `row` carries another: the row the
    /// entry indexed is gone, and `row` was scheduled in its place, with
    /// none of its attempt history. An entry with no stamp answers true for
    /// every row. So does a row that carries the stamp the first one did: a
    /// row written in the same millisecond, or any row of an effect that
    /// names its own due time.
    #[must_use]
    pub fn indexes_row(&self, row: &OutboxEntry) -> bool {
        self.row_scheduled_at.is_none_or(|scheduled_at| {
            scheduled_at == agent_dispatch_timestamp_from_workflow_timestamp(row.scheduled_at())
        })
    }

    /// Whether this entry differs from `other` in anything but `updated_at`,
    /// which nothing reads to decide anything.
    fn differs_beyond_updated_at(&self, other: &Self) -> bool {
        let mut probe = self.clone();
        probe.updated_at = other.updated_at;
        probe != *other
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
        if !self.indexes_row(&due.entry) {
            // Another row under this entry's message id: the row the entry
            // was registered from is gone, and its owner scheduled the
            // ticket again. What the entry holds is the last the fleet
            // learned from the row that was lost, the attempts it counted
            // included, and a row that never saw them must not overwrite
            // it. Whoever claims the entry decides what the ticket is now.
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

    /// Gives a claimed entry back: `Pending`, with no lease. Everything
    /// else but `updated_at` stays as it was, the last fencing token
    /// included, so the next claim takes the next token and a write under
    /// this one is fenced.
    ///
    /// `Pending`, whatever the entry was before it was claimed: the claim
    /// did not keep what it replaced, and this is what a registration
    /// already makes of a claim whose lease lapsed.
    fn release(mut self, now: AgentTimestampMillis) -> Self {
        self.status = AgentDispatchStatus::Pending;
        self.lease = None;
        self.updated_at = now;
        self
    }

    /// Settles a claimed ticket as cancelled and keeps why, bounded where
    /// it is written like every other writer of the field.
    fn mark_settled_cancelled(self, reason: &str, now: AgentTimestampMillis) -> Self {
        let mut entry = self.mark_cancelled(now);
        entry.last_error_code = Some(bounded_dispatch_detail(reason));
        entry
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
    /// Lease expiration timestamp, as the claim pass set it.
    ///
    /// A worker that renews the claims it holds
    /// ([`AgentDispatcherFleet::hold_claims`]) moves the entry's lease and
    /// not this value, which can then be earlier than the lease the entry
    /// holds, never later. Read the entry for the lease as it stands.
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

    /// The effects of one run whose entries a cancellation request fenced
    /// and that are ready to be settled at the outbox and then finished:
    /// fenced, unfinished, and held by no active lease at `now`.
    ///
    /// The list is stable between a settle and the finalize that follows
    /// it: a fenced entry is never claimable, so it cannot gain a lease.
    #[must_use]
    pub fn cancellations_awaiting_settlement(
        &self,
        run_id: &AgentRunId,
        now: AgentTimestampMillis,
    ) -> Vec<AgentEffectId> {
        self.entries
            .values()
            .filter(|entry| &entry.run_id == run_id && entry.awaits_cancellation_settlement(now))
            .map(|entry| entry.effect_id.clone())
            .collect()
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

    /// Extends the lease of every claim in `held` that is still current at
    /// `now` to `lease_expires_at`, and answers how many it extended.
    ///
    /// A claim is current when the entry is leased to its worker under its
    /// fencing token and the lease is active: one that lapsed, was taken
    /// over, finished or is gone is left as it is, which covers the claim
    /// the same write settles: a settled entry holds no lease. A lease
    /// already running to `lease_expires_at` or past it is left too, so a
    /// renewal never shortens one.
    ///
    /// An entry a cancellation request fenced is left as well. The record
    /// wants that lease to end: once it lapses, the cancellation can settle
    /// the entry, and extending it would keep a cancelled run's effect in a
    /// worker's hands for as long as the worker went on settling.
    fn renew_held(
        &mut self,
        held: &[AgentDispatchClaim],
        now: AgentTimestampMillis,
        lease_expires_at: AgentTimestampMillis,
    ) -> u64 {
        let mut renewed = 0;
        for claim in held {
            let Some(entry) = self.entries.get_mut(&claim.dispatch_id) else {
                continue;
            };
            if entry.cancellation_requested || !entry.is_current_claim(claim, now) {
                continue;
            }
            let Some(lease) = entry.lease.as_mut() else {
                continue;
            };
            if lease.lease_expires_at < lease_expires_at {
                lease.lease_expires_at = lease_expires_at;
                entry.updated_at = now;
                renewed += 1;
            }
        }
        renewed
    }
}

/// Result of registering due effects into the fleet index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDispatcherRegistration {
    /// Run that was observed.
    pub run_id: AgentRunId,
    /// Number of due effects observed from the run outbox.
    pub observed_due_effects: usize,
    /// Number of fleet entries this registration inserted or changed.
    ///
    /// An entry already indexed exactly as the outbox describes it — a call
    /// in flight under a live lease, say — is neither counted nor rewritten,
    /// and a registration that changes no entry writes nothing.
    pub registered_effects: usize,
}

/// Result of [`AgentDispatcherWorker::cancel_run_dispatches`]: what one
/// cancellation fenced, settled and finished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDispatcherCancellation {
    /// Run whose dispatch entries were considered.
    pub run_id: AgentRunId,
    /// Timestamp used for the cancellation pass.
    pub cancelled_at: AgentTimestampMillis,
    /// Entries moved to `Cancelled`, each after its outbox row was settled.
    pub cancelled_entries: usize,
    /// Entries already terminal before the cancellation pass.
    pub already_terminal_entries: usize,
    /// Actively leased entries marked cancellation requested. They are left
    /// for the in-flight worker to finish truthfully; if the lease expires
    /// they are settled and finished as cancelled on the next worker refresh.
    pub in_flight_entries: usize,
    /// Effect ids of the entries this pass moved to `Cancelled`.
    pub cancelled_effect_ids: Vec<AgentEffectId>,
}

/// What one [`AgentDispatcherFleet::request_run_cancellation`] fenced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDispatcherCancellationRequest {
    /// Run whose entries were considered.
    pub run_id: AgentRunId,
    /// Timestamp of the request.
    pub requested_at: AgentTimestampMillis,
    /// Effects of fenced entries with no active lease, including ones an
    /// earlier request fenced: settle these at the outbox, then finalize
    /// them.
    pub awaiting_settlement: Vec<AgentEffectId>,
    /// Entries with an active lease, fenced and left to their worker.
    pub in_flight_entries: usize,
    /// Entries already finished before the request.
    pub already_terminal_entries: usize,
}

/// What one [`AgentDispatcherFleet::prune_terminal`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDispatcherPrune {
    /// Entries this call removed.
    pub pruned: usize,
    /// Entries left in the record, live and finished.
    pub remaining: usize,
}

/// Result of one claim pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDispatchClaimBatch {
    /// Worker that claimed work.
    pub worker_id: AgentDispatcherWorkerId,
    /// Timestamp used for the claim pass.
    pub claimed_at: AgentTimestampMillis,
    /// Claimable entries before concurrency and batch limits, across the
    /// whole fleet index and regardless of this worker's claim filter.
    ///
    /// This is the fleet's backlog, not this worker's: [`Self::class_filtered`]
    /// is the part of it this worker does not serve.
    pub due_dispatch_count: usize,
    /// True when more work this worker *can serve* was due than this pass
    /// claimed.
    ///
    /// Deliberately not measured against [`Self::due_dispatch_count`]: an
    /// entry whose class another worker serves is that worker's backlog, and
    /// counting it here would make every class-restricted worker report
    /// backpressure forever.
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
    ///
    /// Complete over the claimable entries, not over the ones this pass had
    /// room to consider: the walk keeps counting after the batch fills, so a
    /// busy fleet reports the same number an idle one would. That matters
    /// because a busy fleet is precisely when an unservable class is most
    /// likely to be sitting behind work that keeps getting claimed.
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
    /// The claims this handle's worker has taken and not started
    /// ([`Self::hold_claims`]).
    held: Vec<AgentDispatchClaim>,
    /// How this handle waits before it makes a lost write again
    /// ([`Self::set_write_pause`]).
    write_pause: AgentFleetWritePause,
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
            held: Vec::new(),
            write_pause: AgentFleetWritePause::Thread,
        }
    }

    /// Replaces how this handle waits before it makes a lost write again.
    ///
    /// The default, [`AgentFleetWritePause::Thread`], needs no runtime and
    /// no timer. An application replaces it to wait on its runtime's timer,
    /// to wait in a simulated time of its own, or, with a future that ends
    /// at once, not to wait.
    #[must_use]
    pub fn with_write_pause(mut self, pause: AgentFleetWritePause) -> Self {
        self.set_write_pause(pause);
        self
    }

    /// Replaces how this handle waits before it makes a lost write again,
    /// in place ([`Self::with_write_pause`]).
    pub fn set_write_pause(&mut self, pause: AgentFleetWritePause) {
        self.write_pause = pause;
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
    ///
    /// Writes only when an entry is inserted or changed; see
    /// [`AgentDispatcherRegistration::registered_effects`].
    ///
    /// **One try, against this handle's copy of the record.** A registration
    /// that loses its compare-and-set is not read again here, unlike a
    /// claim, a settle, a release and a prune, because its lost write is a
    /// fence. The
    /// list it is given must be no older than the record it is written
    /// against: a list read before the record can name an effect another
    /// worker has since run, finished and pruned, and registering it would
    /// put a finished effect back. So a caller reads the record first and
    /// the list second, and a write that loses means the record moved after
    /// both. Re-applying this list to a newer record would undo that. The
    /// caller re-applies instead: [`Self::registration_lost`] says how.
    ///
    /// # Errors
    ///
    /// The store's error, including the lost compare-and-set
    /// (`revision-conflict`) on its first loss.
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
            let entry = match next.entries.get(&dispatch_id) {
                None => candidate,
                Some(existing) => {
                    let upserted =
                        existing
                            .clone()
                            .upsert_from_due_effect(workflow_id.clone(), &due, now);
                    if upserted.differs_beyond_updated_at(existing) {
                        upserted
                    } else {
                        // Indexed exactly as the outbox describes it — a call
                        // in flight under a live lease, re-registered on every
                        // pump of its run. Rewriting it would cost the hottest
                        // shared record a compare-and-set for a timestamp.
                        continue;
                    }
                }
            };
            next.updated_at = now;
            next.entries.insert(dispatch_id, entry);
            registered_effects += 1;
        }

        if registered_effects > 0 {
            self.persist(record.revision, next).await?;
        }
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
    ///
    /// The first attempt is made from this handle's copy of the record. A
    /// claim that loses its compare-and-set reads the record again and
    /// claims again, up to three more times, each
    /// after a short random wait ([`AgentFleetWritePause`]). Each
    /// attempt is made whole from the record it read: what is due, the
    /// class and target limits, the batch and the fencing tokens, at the
    /// instant of that attempt. The claims of an attempt that lost are
    /// dropped. Its write did not land, so it leased nothing, and nothing of
    /// it is answered. The batch answered, and what is counted, is the
    /// attempt that ended the call. An attempt that finds nothing to claim
    /// writes nothing.
    ///
    /// # Errors
    ///
    /// The store's error, or the lost compare-and-set (`revision-conflict`)
    /// once the bound is spent. Nothing is claimed then.
    pub async fn claim_due(
        &mut self,
        worker_id: AgentDispatcherWorkerId,
    ) -> AgentDispatcherResult<AgentDispatchClaimBatch> {
        if self.record.is_none() {
            self.recover().await?;
        }
        let mut reapplied = 0;
        loop {
            match self.claim_due_once(&worker_id).await {
                Ok(batch) => return Ok(batch),
                Err(error) => {
                    self.reapply("claim", FirstAttempt::FromARead, &mut reapplied, error)
                        .await?;
                    self.recover().await?;
                }
            }
        }
    }

    /// One attempt of [`Self::claim_due`], from the record this handle
    /// holds.
    async fn claim_due_once(
        &mut self,
        worker_id: &AgentDispatcherWorkerId,
    ) -> AgentDispatcherResult<AgentDispatchClaimBatch> {
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
        let mut servable = 0usize;
        for entry in claimable {
            // The filter is consulted first, and the scan runs to the end of
            // the claimable list instead of stopping once the batch is full.
            // `class_filtered` and `servable` describe *this worker against
            // this index* — how much of the due work it cannot serve, and how
            // much it can — and neither may depend on how much of that work
            // one pass happened to have room for. With the batch-size break
            // above this check, a busy fleet filled its batch from the earlier
            // entries and returned before reaching the unservable ones, so
            // every pass reported `class_filtered: 0` exactly when a class
            // nobody serves was stalling: the one condition the counter
            // exists to make visible.
            //
            // Before the lease, too: a worker that cannot run this entry must
            // not hold it away from one that can.
            if !self.claim_filter.accepts(&entry) {
                class_filtered += 1;
                continue;
            }
            servable += 1;
            if claims.len() >= self.settings.max_batch_size {
                // No room in this batch. Keep scanning rather than breaking:
                // the list is already materialized and the remaining work is
                // a filter check per entry, with no I/O and no durable write.
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

        // Servable work this pass left behind — never fleet-wide due work.
        // An entry whose class another worker serves is *that* worker's
        // backlog, so counting it here made every class-restricted worker
        // report permanent backpressure. Accumulated in the walk rather than
        // derived as `due_dispatch_count - class_filtered`: the two are equal
        // only while both use the same claimable predicate, and a bound that
        // depends on two things agreeing is one nothing checks.
        let backpressure_limited = servable > claims.len();
        if !claims.is_empty() {
            self.persist(record.revision, next).await?;
        }
        self.record_metric("claim", "claimed", "none", claims.len() as u64);
        if class_filtered > 0 {
            self.record_metric("claim", "class-filtered", "none", class_filtered as u64);
        }
        self.record_gauges(now);
        Ok(AgentDispatchClaimBatch {
            worker_id: worker_id.clone(),
            claimed_at: now,
            due_dispatch_count,
            backpressure_limited,
            concurrency_limited,
            class_filtered,
            claims,
        })
    }

    /// Fences every unfinished entry of a run against being claimed.
    ///
    /// This is the first of a cancellation's three steps, and it finishes
    /// nothing. Each unfinished entry of the run is marked
    /// `cancellation_requested`, which makes it never claimable again. The
    /// caller then settles the rows of the entries that have no active
    /// lease ([`AgentDispatcherCancellationRequest::awaiting_settlement`])
    /// at the durable outbox layer, and only then finishes them with
    /// [`Self::finalize_run_cancellations`]. In that order a fleet entry is
    /// never finished before its outbox row is, which is what lets
    /// [`Self::prune_terminal`] remove a finished entry at any time.
    ///
    /// An entry with an active lease is left to its worker, which may still
    /// finish and record a truthful completion; it is also given the code
    /// `cancellation-requested`. An entry without one keeps the code it had.
    ///
    /// A caller that stops after this step leaves fenced, unfinished
    /// entries: no worker claims them and no prune removes them, and the
    /// next request or [`AgentDispatcherWorker::refresh_run`] for the run
    /// lists them again. One write, and none when nothing changed. The
    /// settlement that follows cancels a `Dispatching` row with its attempt
    /// counted under [`DISPATCH_CANCELLED_IN_FLIGHT_LINE`] (issue #209).
    ///
    /// Use [`AgentDispatcherWorker::cancel_run_dispatches`] for the three
    /// steps together.
    pub async fn request_run_cancellation(
        &mut self,
        run_id: &AgentRunId,
    ) -> AgentDispatcherResult<AgentDispatcherCancellationRequest> {
        if self.record.is_none() {
            self.recover().await?;
        }

        let now = current_agent_timestamp(&self.clock);
        let record = self.current_record()?;
        let mut next = record.state;
        let mut fenced_entries = 0;
        let mut already_terminal_entries = 0;
        let mut in_flight_entries = 0;

        for entry in next
            .entries
            .values_mut()
            .filter(|entry| &entry.run_id == run_id)
        {
            if entry.status.is_terminal() {
                already_terminal_entries += 1;
                continue;
            }
            let in_flight = entry.is_in_flight_at(now);
            if in_flight {
                in_flight_entries += 1;
            }
            if !entry.cancellation_requested {
                entry.cancellation_requested = true;
                if in_flight {
                    entry.last_error_code = Some(CANCELLATION_REQUESTED.to_string());
                }
                entry.updated_at = now;
                fenced_entries += 1;
            }
        }

        let awaiting_settlement = next.cancellations_awaiting_settlement(run_id, now);
        if fenced_entries > 0 {
            next.updated_at = now;
            self.persist(record.revision, next).await?;
            self.record_gauges(now);
        }
        self.record_metric("cancel", "requested", "none", fenced_entries as u64);
        Ok(AgentDispatcherCancellationRequest {
            run_id: run_id.clone(),
            requested_at: now,
            awaiting_settlement,
            in_flight_entries,
            already_terminal_entries,
        })
    }

    /// Finishes the named entries of a run as cancelled, returning the
    /// effects it finished.
    ///
    /// The last of a cancellation's three steps: `settled` names the effects
    /// whose outbox rows the caller has settled. An entry is finished only
    /// when it is named, belongs to the run, was fenced by
    /// [`Self::request_run_cancellation`], is not already finished, and has
    /// no active lease at the time of this call. Anything else named is left
    /// as it is.
    ///
    /// **That the named rows are settled is the caller's word.** This method
    /// cannot see an outbox: it finishes any fenced, unfinished entry with
    /// no active lease that is named, whether or not its row was settled.
    /// The rule that a fleet entry is finished only after its outbox row
    /// rests, at this method, on naming only what was settled, as
    /// [`AgentDispatcherWorker::cancel_run_dispatches`] and
    /// [`AgentDispatcherWorker::refresh_run`] do. An entry that was in
    /// flight at the request is not in that request's list, so a caller that
    /// passes the list back never finishes one whose lease lapsed since.
    /// Those two settle a `Dispatching` row through
    /// `DurableInbox::record_outbox_cancelled_in_flight`, with
    /// [`DISPATCH_CANCELLED_IN_FLIGHT_LINE`], and a caller that settles its
    /// own rows should too (issue #209).
    ///
    /// One write, and none when it finishes nothing.
    pub async fn finalize_run_cancellations(
        &mut self,
        run_id: &AgentRunId,
        settled: &[AgentEffectId],
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
                && entry.awaits_cancellation_settlement(now)
                && settled.contains(&entry.effect_id)
        }) {
            let updated = entry.clone().mark_cancelled(now);
            *entry = updated;
            finalized_effect_ids.push(entry.effect_id.clone());
        }

        if !finalized_effect_ids.is_empty() {
            next.updated_at = now;
            self.persist(record.revision, next).await?;
            self.record_gauges(now);
        }
        self.record_metric(
            "cancel",
            "cancelled",
            "none",
            finalized_effect_ids.len() as u64,
        );
        Ok(finalized_effect_ids)
    }

    /// Removes the finished entries that finished before `before`.
    ///
    /// Nothing else removes an entry: without this the record grows by one
    /// entry for every effect a deployment ever dispatched, and every claim
    /// and settle writes the whole record back. Pruning is an explicit act
    /// of the application, called on its own schedule with its own horizon.
    ///
    /// An entry is removed when it is `Completed`, `Exhausted` or
    /// `Cancelled` and its last update, which is the moment it finished, is
    /// earlier than `before`. Nothing else is touched: a pending, claimed or
    /// retry-scheduled entry stays, and so does a claimed entry whose lease
    /// has lapsed and an entry whose cancellation was requested and has not
    /// finished. Any horizon is safe, including one in the future, which
    /// removes every finished entry: a finished entry is history, and the
    /// rule that makes it so is that an entry is finished only after its
    /// outbox row is.
    ///
    /// What goes with an entry is its `last_error_code`, the only durable
    /// copy of a cancelled ticket's settle reason. The horizon is how long
    /// that stays readable.
    ///
    /// The record is read fresh on every attempt, never from this handle's
    /// copy. A prune that finds nothing to remove writes nothing. One that
    /// loses its compare-and-set to another writer reads again and
    /// re-applies, up to three more times, each
    /// after a short random wait ([`AgentFleetWritePause`]). Each loss is
    /// counted.
    ///
    /// # Errors
    ///
    /// The store's error, or the lost compare-and-set (`revision-conflict`)
    /// once the bound is spent.
    pub async fn prune_terminal(
        &mut self,
        before: AgentTimestampMillis,
    ) -> AgentDispatcherResult<AgentDispatcherPrune> {
        let mut reapplied = 0;
        loop {
            self.recover().await?;
            let now = current_agent_timestamp(&self.clock);
            let record = self.current_record()?;
            let mut next = record.state;
            let held = next.entries.len();
            next.entries
                .retain(|_, entry| !(entry.status.is_terminal() && entry.updated_at < before));
            let remaining = next.entries.len();
            let pruned = held - remaining;
            if pruned == 0 {
                return Ok(AgentDispatcherPrune { pruned, remaining });
            }
            next.updated_at = now;
            match self.persist(record.revision, next).await {
                Ok(_) => {
                    self.record_metric("prune", "pruned", "none", pruned as u64);
                    self.record_gauges(now);
                    return Ok(AgentDispatcherPrune { pruned, remaining });
                }
                Err(error) => {
                    self.reapply("prune", FirstAttempt::FromARead, &mut reapplied, error)
                        .await?;
                }
            }
        }
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
            WorkflowTelemetryEvent::OutboxDispatchCancelled { message, .. } => {
                entry.mark_settled_cancelled(message, now)
            }
        })
        .await
    }

    /// Settles a current claim to match an outbox row that is already
    /// finished, or absent.
    ///
    /// The outbox row is the authority on whether an effect is done, and the
    /// fleet entry follows it. A worker that finds its claim's row already
    /// finished has nothing to dispatch and nothing to record at the outbox;
    /// what is left is to bring the entry in line, so the ticket is not
    /// claimed again on every lease.
    ///
    /// | Row | The entry becomes | `last_error_code` |
    /// |---|---|---|
    /// | `Dispatched` | `Completed` | unchanged |
    /// | `Exhausted` | `Exhausted`, with the row's attempt count | `ticket-row-settled` |
    /// | `Cancelled` | `Cancelled` | `ticket-row-settled` |
    /// | absent | `Cancelled` | `ticket-row-missing` |
    ///
    /// It is a settle like any other: fenced on the claim, and read again
    /// when it loses its write.
    ///
    /// # Errors
    ///
    /// [`AgentDispatcherError::InvalidEntry`] (`invalid-dispatch-entry`) for
    /// a row that is not finished, which is not this method's to settle.
    /// [`AgentDispatcherError::ClaimFenced`] (`claim-fenced`) when the claim
    /// is no longer current.
    pub async fn settle_claim_from_row(
        &mut self,
        claim: &AgentDispatchClaim,
        row: Option<&OutboxEntry>,
    ) -> AgentDispatcherResult<AgentDispatchEntry> {
        enum Settlement {
            Completed,
            Exhausted { attempts: u32 },
            Cancelled { code: &'static str },
        }
        let settlement = match row.map(|row| (row.status(), row.attempts().attempts())) {
            None => Settlement::Cancelled {
                code: TICKET_ROW_MISSING,
            },
            Some((OutboxStatus::Dispatched, _)) => Settlement::Completed,
            Some((OutboxStatus::Exhausted, attempts)) => Settlement::Exhausted { attempts },
            Some((OutboxStatus::Cancelled, _)) => Settlement::Cancelled {
                code: TICKET_ROW_SETTLED,
            },
            Some((OutboxStatus::Pending | OutboxStatus::Dispatching | OutboxStatus::Failed, _)) => {
                return Err(AgentDispatcherError::InvalidEntry {
                    dispatch_id: claim.dispatch_id.clone(),
                    field: "row",
                    reason: "not-finished",
                });
            }
        };
        self.update_current_claim(claim, |entry, now| match settlement {
            Settlement::Completed => entry.mark_completed(now),
            Settlement::Exhausted { attempts } => {
                entry.mark_exhausted(attempts, TICKET_ROW_SETTLED.to_string(), now)
            }
            Settlement::Cancelled { code } => entry.mark_settled_cancelled(code, now),
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
    ///
    /// A claim whose entry is no longer in the record is not current: a
    /// finished entry can be removed by [`Self::prune_terminal`], and a
    /// worker still holding a lapsed claim on it gets the answer it would
    /// have had while the entry was there.
    pub fn is_current_claim(&self, claim: &AgentDispatchClaim) -> AgentDispatcherResult<bool> {
        let now = current_agent_timestamp(&self.clock);
        Ok(self
            .state()?
            .entry(&claim.dispatch_id)
            .is_some_and(|entry| entry.is_current_claim(claim, now)))
    }

    /// Returns true when the claim is current and its lease is still active
    /// `for_ms` from now: work that takes no longer than that ends while the
    /// claim is still this worker's.
    ///
    /// Nothing renews the claim a worker is running
    /// ([`Self::hold_claims`]), so a worker that means to do more under one
    /// claim than it was claimed for, such as another attempt after the
    /// first, asks this before it starts. A lease is over at the instant it
    /// expires, so work that would end at that instant is not covered, and
    /// `for_ms` of zero asks what [`Self::is_current_claim`] asks.
    ///
    /// Like that question, this is answered from the handle's own copy of
    /// the record and its clock, and reads and writes nothing. The lease is
    /// the entry's, which a renewal moves, and not the expiry the claim was
    /// handed. Another worker can take the ticket only once the lease has
    /// lapsed, and that is visible on the clock alone.
    pub fn is_current_claim_for(
        &self,
        claim: &AgentDispatchClaim,
        for_ms: u64,
    ) -> AgentDispatcherResult<bool> {
        let now = current_agent_timestamp(&self.clock);
        let until = AgentTimestampMillis::new(now.as_millis().saturating_add(for_ms));
        Ok(self
            .state()?
            .entry(&claim.dispatch_id)
            .is_some_and(|entry| entry.is_current_claim(claim, until)))
    }

    /// Names the claims this handle's worker has taken and not yet started,
    /// for the next settle the handle writes.
    ///
    /// A claim pass leases every claim of its batch at one instant, and a
    /// worker that runs them one after another reaches the last one with
    /// whatever the ones before it left of that lease. So before each claim
    /// it runs, a worker tells its handle which claims it still holds, and
    /// **the settle that ends that claim renews them**: in the same write,
    /// each held claim that is still current is leased for a full lease
    /// from that settle
    /// ([`AgentDispatcherFleetSettings::lease_duration_ms`]). A batch then
    /// needs a lease that covers its longest single dispatch, not their sum,
    /// and renewing it costs no write of its own.
    ///
    /// The fence is the one a settle keeps. A held claim is renewed only
    /// while the entry is leased to its worker under its fencing token and
    /// the lease is active, checked against the record the write is applied
    /// to. One whose lease lapsed, that another worker took over, that is
    /// finished or gone is left as it is. So is one whose run's cancellation
    /// was requested: that lease is meant to end. A renewal never shortens a
    /// lease.
    ///
    /// **A hold is spent by the settle that lands.** It replaces what was
    /// held, an empty list holds nothing, and a settle that is written
    /// leaves nothing held, so a claim is renewed once for each time it is
    /// named. A worker that stops naming a claim stops renewing it: a claim
    /// nobody will start is leased for at most one more lease, never for as
    /// long as its worker goes on settling other work. A worker whose batch
    /// ends short should hold nothing, so that not even that one settle
    /// renews what it abandoned. It reads and writes nothing.
    ///
    /// The claim being run is not renewed by anything, so a dispatch that
    /// outlives a lease lapses as it always did, and the claims held behind
    /// it lapse with it.
    ///
    /// The [`AgentDispatchClaim`] a worker holds keeps the expiry its claim
    /// pass gave it. A renewal moves the entry's lease, not that value.
    pub fn hold_claims(&mut self, claims: Vec<AgentDispatchClaim>) {
        self.held = claims;
    }

    /// Gives claims back: each one that is still current goes back to
    /// `Pending` with no lease, claimable at once.
    ///
    /// For a worker that stops before it has started every claim of its
    /// batch. Left alone, those claims stay leased until the batch's lease
    /// lapses, and count against their target's concurrency limit
    /// meanwhile.
    ///
    /// A claim is released only while the entry is leased to its worker
    /// under its fencing token and the lease is active. One that is not
    /// current is passed over: its lease lapsed, another worker holds it, or
    /// it is finished or gone. A released entry changes its status, its
    /// lease and its `updated_at`, and nothing else. It keeps its due time,
    /// its attempt count, its last error code and its last fencing token,
    /// so the next claim takes the next token and a late write under the
    /// released one is answered `claim-fenced`. It keeps a cancellation
    /// request too: such an entry is not claimable, and is finished by the
    /// cancellation, which no longer has a lease to wait for.
    ///
    /// Release only a claim whose dispatch never started. The entry says
    /// nothing about what was done under the claim: the outbox row does, and
    /// a claim released after its row was marked is taken again at once and
    /// recovered as any attempt that disappeared.
    ///
    /// The record is read fresh on every attempt, never from this handle's
    /// copy. Every current claim is released in one write, and a release
    /// that finds none writes nothing. One that loses its compare-and-set
    /// reads again and re-applies, up to three more
    /// times, each after a short random wait ([`AgentFleetWritePause`]).
    /// Each loss is counted. Answers how many entries it released.
    ///
    /// # Errors
    ///
    /// The store's error, or the lost compare-and-set (`revision-conflict`)
    /// once the bound is spent. The claims are then as they were.
    pub async fn release_claims(
        &mut self,
        claims: &[AgentDispatchClaim],
    ) -> AgentDispatcherResult<usize> {
        if claims.is_empty() {
            return Ok(0);
        }
        let mut reapplied = 0;
        loop {
            self.recover().await?;
            let now = current_agent_timestamp(&self.clock);
            let record = self.current_record()?;
            let mut next = record.state;
            let mut released = 0;
            for claim in claims {
                let Some(entry) = next.entries.get_mut(&claim.dispatch_id) else {
                    continue;
                };
                if entry.is_current_claim(claim, now) {
                    *entry = entry.clone().release(now);
                    released += 1;
                }
            }
            if released == 0 {
                return Ok(0);
            }
            next.updated_at = now;
            match self.persist(record.revision, next).await {
                Ok(_) => {
                    self.record_metric("release", "released", "none", released as u64);
                    self.record_gauges(now);
                    return Ok(released);
                }
                Err(error) => {
                    self.reapply("release", FirstAttempt::FromARead, &mut reapplied, error)
                        .await?;
                }
            }
        }
    }

    /// Takes a registration that failed to its next attempt, for the
    /// caller that made it: decides whether there is one, counts the loss,
    /// and waits.
    ///
    /// [`Self::register_due_effects`] tries once, because the list it is
    /// given has to be read again before it is registered again. A caller
    /// that can read it again re-applies the registration itself:
    ///
    /// 1. read the fleet record ([`Self::recover`]);
    /// 2. read the run's due effects, after the record and never before;
    /// 3. register them;
    /// 4. on an error, call this with how many times this registration has
    ///    been re-applied so far: 0 the first time, one more each time.
    ///    `Ok` means go back to step 1. An error is the one to return.
    ///
    /// **The caller keeps the count, and the bound is in it.** A caller
    /// that passes the same number every time is never told to stop.
    ///
    /// This follows the rule a claim, a release and a prune follow. Only a
    /// lost compare-and-set (`revision-conflict`) is re-applied, at most
    /// three times. Each call with a lost
    /// compare-and-set is counted on [`METRIC_AGENT_DISPATCHER_FLEET`]
    /// under `operation=register`, on the caller's word that a write was
    /// lost. Before it answers `Ok` it waits a short random time, longer
    /// for each re-application and under 70 ms asked for in all, on this
    /// handle's [`AgentFleetWritePause`] and never on this fleet's clock.
    ///
    /// The registration that lands answers what it changed. An effect
    /// another worker registered in between counts for nothing in it.
    ///
    /// # Errors
    ///
    /// `error` itself: when it is not a lost compare-and-set, or when
    /// `reapplied` has reached the bound.
    pub async fn registration_lost(
        &mut self,
        reapplied: u32,
        error: AgentDispatcherError,
    ) -> AgentDispatcherResult<()> {
        let mut reapplied = reapplied;
        self.reapply("register", FirstAttempt::FromARead, &mut reapplied, error)
            .await
    }

    /// Applies `update` to the entry a current claim holds, and writes it.
    ///
    /// The write is a compare-and-set of the whole record, and every other
    /// handle writes the same record: another worker's claim, a registration
    /// and a prune all move its revision. A settle computed from the record
    /// this handle read at claim time would lose to any of them, after the
    /// outbox row it follows was already written. So a lost write is read
    /// again and re-applied, up to [`FLEET_WRITE_REAPPLICATIONS`] more times.
    ///
    /// The re-read cannot settle a claim that is no longer this worker's:
    /// the fence is in the record, and every attempt checks it against the
    /// record it is about to write. An entry that is absent was finished and
    /// pruned, so its claim is fenced like any other that is not current.
    ///
    /// Only a lost compare-and-set is retried, recognized by its stable code
    /// (`revision-conflict`). Any other failure is returned as it is. Each
    /// loss is counted, and the second and third re-applications wait a
    /// short random time first ([`Self::after_failed_write`]).
    ///
    /// The same write renews the claims this handle holds
    /// ([`Self::hold_claims`]), each under its own fence and from the record
    /// the write is applied to, so a re-read renews from what it read.
    async fn update_current_claim(
        &mut self,
        claim: &AgentDispatchClaim,
        update: impl Fn(AgentDispatchEntry, AgentTimestampMillis) -> AgentDispatchEntry,
    ) -> AgentDispatcherResult<AgentDispatchEntry> {
        if self.record.is_none() {
            self.recover().await?;
        }
        let mut reapplied = 0;
        loop {
            let now = current_agent_timestamp(&self.clock);
            let record = self.current_record()?;
            let fenced = || AgentDispatcherError::ClaimFenced {
                dispatch_id: claim.dispatch_id.clone(),
                worker_id: claim.worker_id.clone(),
                fencing_token: claim.fencing_token,
            };
            let entry = record
                .state
                .entries
                .get(&claim.dispatch_id)
                .cloned()
                .ok_or_else(fenced)?;
            if !entry.is_current_claim(claim, now) {
                return Err(fenced());
            }

            let updated = update(entry, now);
            let mut next = record.state;
            next.updated_at = updated.updated_at;
            next.entries
                .insert(claim.dispatch_id.clone(), updated.clone());
            let renewed = next.renew_held(
                &self.held,
                now,
                AgentTimestampMillis::new(
                    now.as_millis()
                        .saturating_add(self.settings.lease_duration_ms),
                ),
            );
            match self.persist(record.revision, next).await {
                Ok(_) => {
                    // A hold is good for the one settle that lands.
                    self.held.clear();
                    if renewed > 0 {
                        self.record_metric("renew", "renewed", "none", renewed);
                    }
                    self.record_gauges(now);
                    return Ok(updated);
                }
                Err(error) => {
                    self.reapply(
                        "settle",
                        FirstAttempt::FromAnOlderCopy,
                        &mut reapplied,
                        error,
                    )
                    .await?;
                    self.recover().await?;
                }
            }
        }
    }

    /// Decides what follows a fleet write that failed, and counts it.
    ///
    /// The one rule every re-applied write follows: a registration, a
    /// claim, a settle, a release and a prune.
    ///
    /// - Only a lost compare-and-set is re-applied, recognized by its stable
    ///   code (`revision-conflict`). Any other failure is returned as it is
    ///   and is not counted.
    /// - A write is re-applied at most [`FLEET_WRITE_REAPPLICATIONS`] times.
    ///   The loss after that is returned.
    /// - Each loss is counted once on
    ///   [`METRIC_AGENT_DISPATCHER_FLEET`] under the write's `operation`:
    ///   `outcome=reapplied` with the number of the re-application it led
    ///   to as `detail`, or `outcome=revision-conflict` when it is returned.
    ///
    /// Answers how long the re-application waits before it reads again
    /// ([`reapplication_pause`]), and `None` for the one that does not
    /// wait: the first re-application of a write whose first attempt was
    /// made from an older copy ([`FirstAttempt`]).
    fn after_failed_write(
        &self,
        operation: &'static str,
        first: FirstAttempt,
        reapplied: u32,
        error: AgentDispatcherError,
    ) -> AgentDispatcherResult<Option<Duration>> {
        if error.code() != REVISION_CONFLICT_CODE {
            return Err(error);
        }
        let Some(nth) = FLEET_REAPPLICATION_LABELS.get(reapplied as usize) else {
            self.record_metric(operation, REVISION_CONFLICT_CODE, "none", 1);
            return Err(error);
        };
        self.record_metric(operation, "reapplied", nth, 1);
        if reapplied == 0 && first == FirstAttempt::FromAnOlderCopy {
            return Ok(None);
        }
        Ok(Some(reapplication_pause(
            reapplied + 1,
            reapplication_entropy(),
        )))
    }

    /// Takes a failed fleet write to its next attempt, or returns its error
    /// ([`Self::after_failed_write`]).
    ///
    /// On `Ok` the loss has been counted, `reapplied` is one more, and any
    /// wait is over: the caller reads the record again and re-applies.
    ///
    /// The wait is this handle's [`AgentFleetWritePause`], never this
    /// fleet's clock, and is already boxed: it is not laid out inside every
    /// fleet future for the sake of a write that lost.
    async fn reapply(
        &mut self,
        operation: &'static str,
        first: FirstAttempt,
        reapplied: &mut u32,
        error: AgentDispatcherError,
    ) -> AgentDispatcherResult<()> {
        let pause = self.after_failed_write(operation, first, *reapplied, error)?;
        *reapplied += 1;
        if let Some(pause) = pause {
            self.write_pause.wait(pause).await;
        }
        Ok(())
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
            for status in AgentDispatchStatus::ALL {
                let held = state
                    .entries
                    .values()
                    .filter(|entry| entry.status == status)
                    .count();
                self.metrics.record_gauge(
                    METRIC_AGENT_DISPATCHER_ENTRIES,
                    held as f64,
                    &[("status", status.as_label())],
                );
            }
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
    /// Before it registers anything, it finishes what a cancellation of the
    /// run left unfinished: every entry a cancellation request fenced that
    /// no worker holds any more has its outbox row settled as cancelled and
    /// is then finished, in that order. So a cancelled run's effects are
    /// never redelivered after a worker crash, wherever in the cancellation
    /// the crash fell.
    ///
    /// The fleet record is read before the run's outbox, on a handle that
    /// holds no record yet as on any other. A registration is fenced on the
    /// fleet revision it writes against, and that only protects it if the
    /// list of due effects is no older than that revision: a list read
    /// first could name an effect another worker has since run, finished
    /// and pruned, and registering it would put a finished effect back.
    ///
    /// A fleet entry that is already `Cancelled` while its outbox row is
    /// still unsettled is what a build from before this order could leave
    /// behind; its row is settled here too.
    ///
    /// A `Dispatching` row settled here, an attempt whose worker was lost,
    /// is cancelled with that attempt counted under
    /// [`DISPATCH_CANCELLED_IN_FLIGHT_LINE`], so that the row still says the
    /// attempt started (issue #209).
    ///
    /// A registration that loses its compare-and-set is made again, at most
    /// three more times, from a fresh read of the
    /// fleet record and then of the run's outbox
    /// ([`AgentDispatcherFleet::registration_lost`]). The registration
    /// answered is the one that landed. The cancellation step before it is
    /// not repeated: what a re-read shows newly fenced is finished by the
    /// next refresh.
    ///
    /// # Errors
    ///
    /// A store's error, or the lost compare-and-set (`revision-conflict`)
    /// once the bound is spent or when a cancellation's write loses.
    pub async fn refresh_run(
        &mut self,
        run_id: AgentRunId,
        workflow_id: Option<AgentWorkflowId>,
    ) -> AgentDispatcherResult<AgentDispatcherRegistration> {
        if self.fleet.state().is_err() {
            self.fleet.recover().await?;
        }
        let mut inbox = AgentRunInbox::with_clock_and_metrics(
            run_id.clone(),
            self.workflow_store.clone(),
            self.clock.clone(),
            self.metrics.clone(),
        );
        inbox.recover().await?;

        let now = current_agent_timestamp(&self.clock);
        let awaiting_settlement = self
            .fleet
            .state()?
            .cancellations_awaiting_settlement(&run_id, now);
        if !awaiting_settlement.is_empty() {
            self.settle_cancelled_effects(&mut inbox, &awaiting_settlement, "dispatch-cancelled")
                .await?;
            self.fleet
                .finalize_run_cancellations(&run_id, &awaiting_settlement)
                .await?;
        }

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
        // A registration that loses its write is made again from a fresh
        // read of the record and then of the list, in that order
        // ([`AgentDispatcherFleet::registration_lost`]).
        let mut reapplied = 0;
        loop {
            let effects = inbox.due_effects()?;
            match self
                .fleet
                .register_due_effects(run_id.clone(), workflow_id.clone(), effects)
                .await
            {
                Ok(registration) => return Ok(registration),
                Err(error) => {
                    self.fleet.registration_lost(reapplied, error).await?;
                    reapplied += 1;
                    self.fleet.recover().await?;
                    inbox.recover().await?;
                }
            }
        }
    }

    /// Cancels a run's dispatch entries and settles the cancelled effects at
    /// the durable outbox layer.
    ///
    /// Three steps, in this order: the run's unfinished entries are fenced
    /// against being claimed
    /// ([`AgentDispatcherFleet::request_run_cancellation`]); the outbox rows
    /// of the fenced entries no worker holds are settled as cancelled, a
    /// `Dispatching` row among them with its attempt counted under
    /// [`DISPATCH_CANCELLED_IN_FLIGHT_LINE`] (issue #209); and
    /// those entries are finished
    /// ([`AgentDispatcherFleet::finalize_run_cancellations`]). A fleet entry
    /// is therefore never finished before its outbox row is.
    ///
    /// Actively leased entries are fenced and left for the in-flight side
    /// effect to finish truthfully, or to be settled and finished after
    /// lease expiry by [`Self::refresh_run`]. A crash between any two steps
    /// leaves fenced, unfinished entries, which that same refresh finishes.
    pub async fn cancel_run_dispatches(
        &mut self,
        run_id: &AgentRunId,
    ) -> AgentDispatcherResult<AgentDispatcherCancellation> {
        let request = self.fleet.request_run_cancellation(run_id).await?;
        let mut cancelled_effect_ids = Vec::new();
        if !request.awaiting_settlement.is_empty() {
            let mut inbox = AgentRunInbox::with_clock_and_metrics(
                run_id.clone(),
                self.workflow_store.clone(),
                self.clock.clone(),
                self.metrics.clone(),
            );
            inbox.recover().await?;
            self.settle_cancelled_effects(
                &mut inbox,
                &request.awaiting_settlement,
                "run-cancelled",
            )
            .await?;
            cancelled_effect_ids = self
                .fleet
                .finalize_run_cancellations(run_id, &request.awaiting_settlement)
                .await?;
        }
        Ok(AgentDispatcherCancellation {
            run_id: request.run_id,
            cancelled_at: request.requested_at,
            cancelled_entries: cancelled_effect_ids.len(),
            already_terminal_entries: request.already_terminal_entries,
            in_flight_entries: request.in_flight_entries,
            cancelled_effect_ids,
        })
    }

    /// Settles cancelled effects at the durable outbox layer.
    ///
    /// Outbox entries that already reached a terminal state (or were
    /// compacted away) are skipped, so the settlement pass is idempotent and
    /// safe to repeat on every refresh.
    ///
    /// A `Dispatching` row is an attempt that started and whose outcome was
    /// never recorded. It is cancelled with its attempt counted under
    /// [`DISPATCH_CANCELLED_IN_FLIGHT_LINE`], so that the row still says the
    /// attempt started (issue #209). Cancelled as the other rows are, it read
    /// as a ticket that never started.
    async fn settle_cancelled_effects(
        &self,
        inbox: &mut AgentRunInbox<WorkflowStore, Clock>,
        effect_ids: &[AgentEffectId],
        reason: &str,
    ) -> AgentDispatcherResult<()> {
        // The rule lives in one place, shared with a graph's cancelled nodes
        // (issue #71 item 4).
        crate::effect_bridge::settle_cancelled_outbox_entries(inbox, effect_ids, reason)
            .await
            .map(|_| ())
            .map_err(AgentDispatcherError::from)
    }

    /// Claims due work for this worker.
    pub async fn claim_due(&mut self) -> AgentDispatcherResult<AgentDispatchClaimBatch> {
        self.fleet.claim_due(self.worker_id.clone()).await
    }

    /// Dispatches one current claim.
    ///
    /// # Errors
    ///
    /// [`AgentDispatcherError::ClaimUnavailable`] (`claim-unavailable`) when
    /// the claim's outbox row is already finished or absent. Nothing is
    /// dispatched, and the fleet entry is settled to match the row through
    /// [`AgentDispatcherFleet::settle_claim_from_row`], so the ticket is not
    /// claimed again. [`AgentDispatcherError::ClaimFenced`] when the claim is
    /// no longer current, before or after the dispatch.
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

        // The outbox row is the authority on whether this effect is done.
        // A claim can outlive it: a crash, or a fleet write lost for good,
        // between the outbox success and the fleet completion leaves the row
        // finished and the entry claimable again once its lease lapses.
        // Marking such a row `Dispatching` would run the effect a second
        // time, so a finished or absent row is never dispatched. The entry
        // is settled to match it instead, and the claim is over.
        let row = inbox
            .inner()
            .state()
            .map_err(AgentDispatcherError::from)?
            .outbox_entry(&message_id)
            .cloned();
        let finished = row.as_ref().is_none_or(|row| {
            matches!(
                row.status(),
                OutboxStatus::Dispatched | OutboxStatus::Exhausted | OutboxStatus::Cancelled
            )
        });
        if finished {
            let detail = if row.is_some() {
                TICKET_ROW_SETTLED
            } else {
                TICKET_ROW_MISSING
            };
            self.fleet
                .settle_claim_from_row(&claim, row.as_ref())
                .await?;
            self.fleet.record_metric("dispatch", "skipped", detail, 1);
            return Err(AgentDispatcherError::ClaimUnavailable {
                dispatch_id: claim.dispatch_id,
            });
        }

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
    ///
    /// A claim whose outbox row was already finished or absent is settled
    /// and passed over: it is counted among the cycle's claims and not among
    /// its completions.
    ///
    /// The cycle dispatches its claims one after another, and each settle
    /// renews the claims behind it ([`AgentDispatcherFleet::hold_claims`]),
    /// so the lease has to cover one dispatch and not the batch. A cycle
    /// that ends on an error gives back the claims it did not start
    /// ([`AgentDispatcherFleet::release_claims`]) before it returns the
    /// error, and holds none of them any more. The claim that failed keeps
    /// its lease. A release that fails too is logged, and the cycle still
    /// returns the error it had.
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
        let mut unstarted: VecDeque<AgentDispatchClaim> = claims.claims.iter().cloned().collect();
        while let Some(claim) = unstarted.pop_front() {
            // The claims behind this one are renewed by its settle.
            self.fleet.hold_claims(unstarted.iter().cloned().collect());
            match self.dispatch_claim(claim, dispatcher).await {
                Ok(completion) => completions.push(completion),
                // The claim's row was already finished or gone: its entry
                // was settled to match and nothing was dispatched, which is
                // not a failure of the cycle.
                Err(AgentDispatcherError::ClaimUnavailable { .. }) => {}
                Err(error) => {
                    // The cycle ends here. Nothing will start the claims
                    // behind this one, so nothing may renew them either:
                    // the handle holds nothing, whatever the release does.
                    self.fleet.hold_claims(Vec::new());
                    // What the cycle never started is given back instead of
                    // left leased. The error the cycle had is the one
                    // returned: a release that fails too is logged, and
                    // leaves those claims to their lease, as before.
                    let unstarted: Vec<AgentDispatchClaim> = unstarted.into();
                    if let Err(release_error) = self.fleet.release_claims(&unstarted).await {
                        tracing::warn!(
                            worker_id = self.worker_id.as_str(),
                            claims = unstarted.len(),
                            code = release_error.code(),
                            "a failed dispatcher cycle could not give back the claims it did \
                             not start; they stay leased until their lease lapses"
                        );
                    }
                    return Err(error);
                }
            }
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
