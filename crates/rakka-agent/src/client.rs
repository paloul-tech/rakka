//! The typed agent client.
//!
//! Owns [`RakkaAgentClient`], the typed facade applications use to create
//! tasks, submit settings and administrative commands, and subscribe to
//! replayable task events. Every call travels the same durable command path
//! as an external request: the client is defined over the
//! [`AgentClientTransport`] port, whose contract is durable, deduplicated
//! acceptance through `rakka-a2a` — there is no local actor shortcut, so a
//! client call and an A2A call converge on the same durable inbox and the
//! same deduplication (specification 14.5).
//!
//! This crate owns the port and the bounded client vocabulary; the
//! `rakka-a2a` `agents` feature provides the transport implementation. The
//! dependency stays one-directional: nothing here names an A2A type.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use rakka_agent_workflow::{AgentTimestampMillis, PrincipalRef};

use crate::definition::{AgentRevisionNumber, AgentSettingsChange};
use crate::identity::AgentTaskId;

/// Result alias for client operations.
pub type AgentClientResult<T> = Result<T, AgentClientError>;

/// Boxed future returned by [`AgentClientTransport`] operations, following
/// the crate's boxed-future trait idiom.
pub type AgentClientFuture<'a, T> = Pin<Box<dyn Future<Output = AgentClientResult<T>> + Send + 'a>>;

/// One failure of a typed client operation.
#[derive(Debug)]
#[non_exhaustive]
pub enum AgentClientError {
    /// The service refused the request with a stable domain code.
    Refused {
        /// Stable machine-readable code.
        code: String,
        /// Bounded message.
        message: String,
    },
    /// The referenced task does not exist.
    TaskNotFound {
        /// Public task id.
        task: String,
    },
    /// The requested replay cursor is older than the retained event window;
    /// the caller must resync from current state (specification 14.5).
    ReplayWindowExpired,
    /// The task did not reach a terminal state within the polling budget.
    PollBudgetExhausted {
        /// Attempts made.
        attempts: u32,
    },
    /// The transport failed outside domain semantics.
    Transport {
        /// Stable machine-readable code.
        code: String,
        /// Bounded message.
        message: String,
    },
}

impl AgentClientError {
    /// Stable machine-readable code.
    #[must_use]
    pub fn code(&self) -> &str {
        match self {
            Self::Refused { code, .. } | Self::Transport { code, .. } => code,
            Self::TaskNotFound { .. } => "task-not-found",
            Self::ReplayWindowExpired => "replay-window-expired",
            Self::PollBudgetExhausted { .. } => "poll-budget-exhausted",
        }
    }
}

impl Display for AgentClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused { code, message } => write!(f, "refused ({code}): {message}"),
            Self::TaskNotFound { task } => write!(f, "task {task} does not exist"),
            Self::ReplayWindowExpired => {
                write!(f, "the replay window expired; resync from current state")
            }
            Self::PollBudgetExhausted { attempts } => {
                write!(f, "task not terminal after {attempts} polls")
            }
            Self::Transport { code, message } => write!(f, "transport ({code}): {message}"),
        }
    }
}

impl Error for AgentClientError {}

/// Public task state as the client sees it — the A2A projection vocabulary
/// in Rakka terms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentClientTaskState {
    /// Accepted, not yet in progress.
    Submitted,
    /// In progress (including suspension and cancellation propagation).
    Working,
    /// Waiting on input, an approval, or a reconciliation decision.
    InputRequired,
    /// Waiting on a security authorization.
    AuthRequired,
    /// Completed with an accepted typed result.
    Completed,
    /// Failed.
    Failed,
    /// Cancelled.
    Canceled,
    /// Rejected before acceptance.
    Rejected,
    /// A state this client version does not recognize.
    Unknown,
}

impl AgentClientTaskState {
    /// True for states where no further change is expected.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Canceled | Self::Rejected
        )
    }
}

/// A typed task creation request.
#[derive(Debug, Clone, Default)]
pub struct AgentClientTaskRequest {
    /// Typed task input, validated against the resolved task definition.
    pub input: Value,
    /// Target agent selection, when the caller names one.
    pub agent: Option<String>,
    /// Typed task-definition selection, when the caller names one.
    pub task_definition: Option<String>,
    /// Explicit durable deduplication key; two requests that share it
    /// converge on one task.
    pub deduplication_key: Option<String>,
    /// Opaque public grouping id.
    pub context: Option<String>,
    /// Authenticated principal submitting the request.
    pub principal: Option<PrincipalRef>,
    /// The caller's trace context, injected into the egress request so the
    /// created task's segments link back to the caller's
    /// ([specification 17.5](../../../docs/plans/rakka-agent/spec.md)). An
    /// absent context sends nothing and the session starts a root.
    pub telemetry: Option<rakka_agent_workflow::AgentTelemetryContext>,
}

/// A typed-result submission completing a human-owned task
/// ([specification 8.12](../../../docs/plans/rakka-agent/spec.md),
/// [14.5](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Default)]
pub struct AgentClientTaskResultRequest {
    /// The task the submission completes.
    pub task: String,
    /// The typed result, validated by the task's deterministic rules.
    pub result: Value,
    /// The task-definition id the result claims to fulfill; a mismatch is a
    /// committed rejection.
    pub definition: String,
    /// The claimed revision of that definition.
    pub definition_version: u64,
    /// The schema the result is expressed in.
    pub result_schema: String,
    /// The claimed revision of that schema.
    pub result_schema_version: u64,
    /// The claimed evidence digest, when the caller carries one. Advisory
    /// for the deployment authorizer; the surface accepts no evidence
    /// artifacts yet.
    pub evidence_digest: Option<String>,
    /// The conversation this submission belongs to, forwarded as the A2A
    /// `context_id` exactly as [`AgentClientTaskRequest::context`] is. Left
    /// unset, the surface correlates the task with itself, which silently
    /// drops it from whatever conversation created it.
    pub context: Option<String>,
    /// Explicit durable deduplication key. A retry that reuses it converges
    /// on the original decision — a recorded rejection included; a corrected
    /// resubmission after a rejection must carry a new key.
    ///
    /// Left unset, the transport derives one from the submission's own
    /// content, so an ordinary retry still converges: the durable identity of
    /// a submission is what it says, not when it was sent. Deriving it is the
    /// safe default precisely because the alternative — a fresh id per call —
    /// spends a rejection per retry and can walk a task to
    /// `ResultRejectionsExhausted` on a submission the caller only ever made
    /// once. Set it explicitly when two submissions carrying identical
    /// content must be told apart.
    pub deduplication_key: Option<String>,
    /// Authenticated principal submitting the result. Required by the
    /// surface: a human-owned task completes only under an authenticated
    /// human or service.
    pub principal: Option<PrincipalRef>,
    /// The caller's trace context, injected into the egress request.
    pub telemetry: Option<rakka_agent_workflow::AgentTelemetryContext>,
}

/// The bounded public view of one task.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentClientTaskView {
    /// The durable task identity (equal to the public A2A task id).
    pub task: AgentTaskId,
    /// Opaque public grouping id.
    pub context: String,
    /// Public task state.
    pub state: AgentClientTaskState,
    /// Bounded public metadata (condition labels, wait reason).
    pub metadata: serde_json::Map<String, Value>,
}

/// One management command (specification 7.2 entry, resolved decision 10).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum AgentClientManagementCommand {
    /// Apply a settings update against an expected current revision.
    UpdateSettings {
        /// Target agent id.
        agent: String,
        /// Settings revision the caller believes is current.
        expected_revision: AgentRevisionNumber,
        /// Field-level changes to apply.
        changes: Vec<AgentSettingsChange>,
    },
    /// Suspend the agent before any further dispatch.
    Suspend {
        /// Target agent id.
        agent: String,
        /// Lifecycle revision the caller believes is current.
        expected_lifecycle_revision: AgentRevisionNumber,
    },
    /// Resume a suspended agent.
    Resume {
        /// Target agent id.
        agent: String,
        /// Lifecycle revision the caller believes is current.
        expected_lifecycle_revision: AgentRevisionNumber,
    },
    /// Permanently retire the agent.
    Terminate {
        /// Target agent id.
        agent: String,
        /// Lifecycle revision the caller believes is current.
        expected_lifecycle_revision: AgentRevisionNumber,
    },
    /// Read the agent's bounded status and revisions.
    Describe {
        /// Target agent id.
        agent: String,
    },
}

/// Bounded agent status and revision surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentClientAgentStatus {
    /// Stable lifecycle status label.
    pub status: String,
    /// Current lifecycle revision.
    pub lifecycle_revision: AgentRevisionNumber,
    /// Current definition revision.
    pub definition_revision: AgentRevisionNumber,
    /// Current settings revision.
    pub settings_revision: AgentRevisionNumber,
}

/// The immediate management outcome.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum AgentClientManagementResponse {
    /// The command transitioned the agent.
    Applied(AgentClientAgentStatus),
    /// The command had already been accepted; this is its original outcome.
    Duplicate(AgentClientAgentStatus),
    /// The agent's current description.
    Described(AgentClientAgentStatus),
    /// The service refused the command — including the stale-revision
    /// conflict a caller rebases on.
    Refused {
        /// Stable domain code, e.g. `stale-settings-revision`.
        code: String,
        /// Bounded message.
        message: String,
    },
}

/// One replayable public task event.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentClientTaskEvent {
    /// Monotonic per-task sequence.
    pub sequence: u64,
    /// Opaque replay cursor positioned after this event.
    pub cursor: String,
    /// Stable event kind label.
    pub kind: String,
    /// Public task state after this event, when the event carries one.
    pub state: Option<AgentClientTaskState>,
    /// Event time.
    pub occurred_at: AgentTimestampMillis,
}

/// Polling policy for the run-task convenience.
#[derive(Debug, Clone, Copy)]
pub struct AgentClientPollPolicy {
    /// Delay between polls.
    pub interval: Duration,
    /// Maximum polls before giving up.
    pub attempts: u32,
}

impl Default for AgentClientPollPolicy {
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(50),
            attempts: 100,
        }
    }
}

/// The durable command port the client is defined over.
///
/// Implementations encode every state-changing operation through
/// `rakka-a2a` and the same durable command/deduplication path an external
/// caller uses. An implementation that bypasses durable acceptance — a
/// direct actor ask, an in-memory mutation — violates the port contract
/// (specification 14.5) and must not exist.
pub trait AgentClientTransport: Send + Sync + 'static {
    /// Creates (or deduplicates onto) one typed task.
    fn create_task(
        &self,
        request: AgentClientTaskRequest,
    ) -> AgentClientFuture<'_, AgentClientTaskView>;

    /// Submits an authenticated typed result to a human-owned task
    /// (specification 8.12).
    fn submit_task_result(
        &self,
        request: AgentClientTaskResultRequest,
    ) -> AgentClientFuture<'_, AgentClientTaskView>;

    /// Reads one task's public view, or `None` when it does not exist.
    fn task<'a>(&'a self, task: &'a str) -> AgentClientFuture<'a, Option<AgentClientTaskView>>;

    /// Requests cancellation of one task.
    fn cancel_task<'a>(&'a self, task: &'a str) -> AgentClientFuture<'a, AgentClientTaskView>;

    /// Applies one management command.
    fn manage(
        &self,
        command: AgentClientManagementCommand,
        principal: Option<PrincipalRef>,
    ) -> AgentClientFuture<'_, AgentClientManagementResponse>;

    /// Replays public task events after an optional cursor.
    ///
    /// A cursor older than the retained window fails with
    /// [`AgentClientError::ReplayWindowExpired`]; the caller resyncs by
    /// reading current state and subscribing from the head.
    fn task_events<'a>(
        &'a self,
        task: &'a str,
        after_cursor: Option<&'a str>,
    ) -> AgentClientFuture<'a, Vec<AgentClientTaskEvent>>;
}

/// The typed facade applications use to drive Rakka Agents.
///
/// A thin, bounded wrapper over the [`AgentClientTransport`] port: the
/// transport owns encoding and durability, the client owns ergonomics
/// (typed requests, terminal polling, resync semantics).
#[derive(Debug, Clone)]
pub struct RakkaAgentClient<T: AgentClientTransport> {
    transport: T,
}

impl<T: AgentClientTransport> RakkaAgentClient<T> {
    /// Wraps a transport.
    #[must_use]
    pub const fn new(transport: T) -> Self {
        Self { transport }
    }

    /// The underlying transport.
    #[must_use]
    pub const fn transport(&self) -> &T {
        &self.transport
    }

    /// Creates (or deduplicates onto) one typed task.
    ///
    /// # Errors
    ///
    /// Propagates transport and domain refusals.
    pub async fn create_task(
        &self,
        request: AgentClientTaskRequest,
    ) -> AgentClientResult<AgentClientTaskView> {
        self.transport.create_task(request).await
    }

    /// Submits an authenticated typed result to a human-owned task
    /// ([specification 8.12](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// An acceptance returns a terminal `Completed` view. A *committed
    /// validation rejection* returns `Ok` too — the nonterminal
    /// `InputRequired` view (or terminal `Failed`, when the rejection
    /// exhausted the budget) carrying the rule code in the view's
    /// `io.rakka.agent.last-rejection` metadata: the decision durably
    /// committed, and an error reply would misreport it as "nothing
    /// happened". A retry under the same deduplication key converges on the
    /// original decision; a corrected resubmission must carry a new key.
    ///
    /// # Errors
    ///
    /// Fails with [`AgentClientError::Refused`] on the non-committing
    /// refusals — an unknown task, an agent-owned target
    /// (`task-not-human-owned`), a terminal or cancelling task — and with
    /// transport failures.
    pub async fn submit_task_result(
        &self,
        request: AgentClientTaskResultRequest,
    ) -> AgentClientResult<AgentClientTaskView> {
        self.transport.submit_task_result(request).await
    }

    /// Reads one task's public view.
    ///
    /// # Errors
    ///
    /// Propagates transport failures; an unknown task is `Ok(None)`.
    pub async fn task(&self, task: &str) -> AgentClientResult<Option<AgentClientTaskView>> {
        self.transport.task(task).await
    }

    /// Requests cancellation of one task. The returned view follows the
    /// authoritative condition: cancellation propagating is not terminal.
    ///
    /// # Errors
    ///
    /// Propagates transport and domain refusals.
    pub async fn cancel_task(&self, task: &str) -> AgentClientResult<AgentClientTaskView> {
        self.transport.cancel_task(task).await
    }

    /// Applies one management command as the given principal.
    ///
    /// # Errors
    ///
    /// Propagates transport failures; a domain refusal (e.g. a
    /// stale-revision conflict) is a normal
    /// [`AgentClientManagementResponse::Refused`] response.
    pub async fn manage(
        &self,
        command: AgentClientManagementCommand,
        principal: Option<PrincipalRef>,
    ) -> AgentClientResult<AgentClientManagementResponse> {
        self.transport.manage(command, principal).await
    }

    /// Replays public task events after an optional cursor.
    ///
    /// # Errors
    ///
    /// Fails with [`AgentClientError::ReplayWindowExpired`] when the cursor
    /// left the retained window.
    pub async fn task_events(
        &self,
        task: &str,
        after_cursor: Option<&str>,
    ) -> AgentClientResult<Vec<AgentClientTaskEvent>> {
        self.transport.task_events(task, after_cursor).await
    }

    /// Creates one task and polls it to a terminal state — the
    /// run-single-task convenience of specification 14.5. Polling holds no
    /// server-side residency: between polls every entity involved may be
    /// fully passivated.
    ///
    /// # Errors
    ///
    /// Fails with [`AgentClientError::PollBudgetExhausted`] when the task
    /// is still nonterminal after the policy's attempts.
    pub async fn run_task(
        &self,
        request: AgentClientTaskRequest,
        poll: AgentClientPollPolicy,
    ) -> AgentClientResult<AgentClientTaskView> {
        let view = self.transport.create_task(request).await?;
        if view.state.is_terminal() {
            return Ok(view);
        }
        let task = view.task.as_str().to_string();
        for _ in 0..poll.attempts {
            tokio::time::sleep(poll.interval).await;
            let Some(view) = self.transport.task(&task).await? else {
                return Err(AgentClientError::TaskNotFound { task });
            };
            if view.state.is_terminal() {
                return Ok(view);
            }
        }
        Err(AgentClientError::PollBudgetExhausted {
            attempts: poll.attempts,
        })
    }
}
