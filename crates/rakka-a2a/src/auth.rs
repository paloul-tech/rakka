//! Authorization hook for public A2A operations.

use async_trait::async_trait;
use rakka_agent_workflow::PrincipalRef;

/// Public A2A operation classes an authorizer can gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum A2AOperation {
    /// `message:send` (blocking or streaming).
    SendMessage,
    /// A `message:send` carrying the collaboration extension's handoff
    /// cluster: the state-mutating same-task transfer of responsibility.
    ///
    /// A distinct operation class from [`Self::SendMessage`] so a deployment
    /// authorizer can gate transfers separately from ordinary sends — the
    /// request carries the claimed transfer as
    /// [`A2AAuthorizationRequest::handoff`], which is what lets the
    /// authorizer bind the authenticated caller to the source run the
    /// cluster claims.
    RecordHandoff,
    /// A `message:send` carrying the collaboration extension's team cluster:
    /// a state-mutating board or membership command over a team entity
    /// (claim, release, transfer, post-task, message, join, leave).
    ///
    /// One operation class with the per-command granularity riding
    /// [`A2AAuthorizationRequest::team`]: the claim carries the cluster's
    /// operation label and the member/task/target it names, so a deployment
    /// authorizer can gate each verb and bind the authenticated caller to
    /// the member the cluster claims to speak for.
    TeamCommand,
    /// A `message:send` carrying the collaboration extension's conversation
    /// cluster: a state-mutating turn-protocol command over a moderated
    /// conversation entity (submit-turn, end).
    ///
    /// One operation class with the per-command granularity riding
    /// [`A2AAuthorizationRequest::conversation`]: the claim carries the
    /// cluster's operation label and the participant/coordinate it names, so
    /// a deployment authorizer can gate each verb and bind the authenticated
    /// caller to the speaker the cluster claims to be.
    ConversationCommand,
    /// A `message:send` naming an existing task and carrying a typed-result
    /// submission: the authenticated completion of a human-owned task
    /// (specification 8.12).
    ///
    /// A distinct operation class from [`Self::SendMessage`] so a deployment
    /// authorizer can permit ordinary sends yet gate completions — the
    /// request carries the claimed result contract as
    /// [`A2AAuthorizationRequest::task_result`], which is what lets the
    /// authorizer bind the authenticated caller to the definition and schema
    /// the submission claims to fulfill.
    SubmitTaskResult,
    /// `tasks/get`.
    GetTask,
    /// `tasks/list`.
    ListTasks,
    /// `tasks/cancel`.
    CancelTask,
    /// `tasks/subscribe` streaming.
    SubscribeToTask,
    /// Push config create/delete.
    PushConfigWrite,
    /// Push config get/list.
    PushConfigRead,
    /// Extended agent card retrieval.
    ExtendedAgentCard,
    /// Agent-management extension write (settings update, lifecycle command).
    AgentManagementWrite,
    /// Agent-management extension read (describe).
    AgentManagementRead,
    /// A scoped replay of one entity's durable coordination event log
    /// (specification 17.13; scenario 45).
    ///
    /// Its own operation class because a coordination log names who assigned,
    /// claimed, handed off, and spoke — a strictly wider read than the public
    /// task projection [`Self::GetTask`] serves, and one a deployment may want
    /// to grant to operators without granting it to every task caller. The
    /// request carries the addressed scope as
    /// [`A2AAuthorizationRequest::coordination`], so the authorizer binds the
    /// caller to the entity whose history it is about to read.
    CoordinationEventRead,
    /// A read of the authorized goal projection (specification 17.18).
    ///
    /// Distinct from [`Self::GetTask`] because the view spans a whole goal
    /// tree — every contributing task and run, the delegation graph, budgets,
    /// and the terminal decision — so a caller permitted one task is not
    /// thereby permitted the tree it belongs to. The request carries the goal
    /// as [`A2AAuthorizationRequest::goal_view`]. A denial must be answered
    /// exactly as a missing goal is, or the domain's deny-is-absent contract
    /// reopens as an existence oracle at the wire.
    GoalViewRead,
}

impl A2AOperation {
    /// Stable lowercase label for bounded metrics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::SendMessage => "send-message",
            Self::RecordHandoff => "record-handoff",
            Self::TeamCommand => "team-command",
            Self::ConversationCommand => "conversation-command",
            Self::SubmitTaskResult => "submit-task-result",
            Self::GetTask => "get-task",
            Self::ListTasks => "list-tasks",
            Self::CancelTask => "cancel-task",
            Self::SubscribeToTask => "subscribe-to-task",
            Self::PushConfigWrite => "push-config-write",
            Self::PushConfigRead => "push-config-read",
            Self::ExtendedAgentCard => "extended-agent-card",
            Self::AgentManagementWrite => "agent-management-write",
            Self::AgentManagementRead => "agent-management-read",
            Self::CoordinationEventRead => "coordination-event-read",
            Self::GoalViewRead => "goal-view-read",
        }
    }
}

/// The transfer claim riding one [`A2AOperation::RecordHandoff`] check.
///
/// Every field is the wire cluster's *claim*, surfaced before anything
/// durable happens so the deployment authorizer can bind the authenticated
/// caller to the source run the cluster names — the durable transition then
/// re-validates the same claims against task state, which gates consistency
/// but never identity.
#[derive(Debug, Clone, Copy)]
pub struct A2AHandoffClaim<'a> {
    /// The handoff identity the cluster carries.
    pub handoff: &'a str,
    /// The agent the caller claims initiated the transfer.
    pub source_agent: &'a str,
    /// The source run id the caller claims to speak for.
    pub source_run: &'a str,
    /// The assignment generation the caller claims the source serves.
    pub source_generation: u64,
    /// The agent the transfer targets.
    pub target_agent: &'a str,
}

/// The team claim riding one [`A2AOperation::TeamCommand`] check.
///
/// Every field is the wire cluster's *claim*, surfaced before anything
/// durable happens so the deployment authorizer can gate each board verb
/// and bind the authenticated caller to the member the cluster names — the
/// team entity's transition then re-validates the same claims against
/// durable state, which gates consistency but never identity.
#[derive(Debug, Clone, Copy)]
pub struct A2ATeamClaim<'a> {
    /// The team the command addresses.
    pub team: &'a str,
    /// The cluster's operation label (`claim`, `release`, `transfer`,
    /// `post-task`, `message`, `join`, `leave`).
    pub operation: &'a str,
    /// The member the caller claims to act as, when the operation names one.
    pub member: Option<&'a str>,
    /// The board task the operation touches, when it names one.
    pub task: Option<&'a str>,
    /// The member a transfer targets or a message addresses, when named.
    pub target_member: Option<&'a str>,
}

/// The conversation claim riding one [`A2AOperation::ConversationCommand`]
/// check.
///
/// Every field is the wire cluster's *claim*, surfaced before anything
/// durable happens so the deployment authorizer can gate each turn-protocol
/// verb and bind the authenticated caller to the speaker the cluster names —
/// the conversation entity's transition then re-validates the same claims
/// against durable state (the roster gate and the cursor's derived owner
/// fence), which gates consistency but never identity.
#[derive(Debug, Clone, Copy)]
pub struct A2AConversationClaim<'a> {
    /// The conversation the command addresses.
    pub conversation: &'a str,
    /// The cluster's operation label (`submit-turn`, `end`).
    pub operation: &'a str,
    /// The speaker the caller claims to be, when the operation names one.
    pub participant: Option<&'a str>,
    /// The round the command claims, when it names one.
    pub round: Option<u64>,
    /// The turn index the command claims, when it names one.
    pub turn: Option<u32>,
}

/// The result-contract claim riding one [`A2AOperation::SubmitTaskResult`]
/// check.
///
/// Every field is the submission's *claim*, surfaced before anything durable
/// happens so the deployment authorizer can bind the authenticated caller —
/// who rides [`A2AAuthorizationRequest::principal`], with the task on
/// [`A2AAuthorizationRequest::task_id`] — to the contract the submission
/// claims to fulfill. The task entity then re-validates the same claims
/// against its durable definition, which gates consistency but never
/// identity. Fields are optional because the claim rides the check even when
/// the binding is half-absent; the command build fails closed afterwards.
#[derive(Debug, Clone, Copy)]
pub struct A2ATaskResultClaim<'a> {
    /// The task-definition contract the submission claims to fulfill.
    pub definition: Option<&'a str>,
    /// The claimed revision of that definition.
    pub definition_version: Option<u64>,
    /// The schema the result claims to be expressed in.
    pub result_schema: Option<&'a str>,
    /// The claimed revision of that schema.
    pub result_schema_version: Option<u64>,
    /// The claimed evidence digest, when the submission carries one.
    pub evidence_digest: Option<&'a str>,
}

/// The scope claim riding one [`A2AOperation::CoordinationEventRead`] check.
///
/// The claim is the *addressed* scope, surfaced before any log is read, so the
/// deployment authorizer can bind the authenticated caller to the entity whose
/// coordination history it is asking for. The replay itself then fences the
/// caller's cursor against this same scope, which gates consistency but never
/// identity.
#[derive(Debug, Clone, Copy)]
pub struct A2ACoordinationClaim<'a> {
    /// The entity class addressed: `task`, `run`, `team`, or `conversation`.
    pub scope_class: &'a str,
    /// The full scope key, as supplied.
    pub scope: &'a str,
}

/// The goal claim riding one [`A2AOperation::GoalViewRead`] check.
///
/// A denial here must be indistinguishable from a goal that does not exist, so
/// the check tells the authorizer which goal is asked for without letting the
/// answer tell the caller whether it is there.
#[derive(Debug, Clone, Copy)]
pub struct A2AGoalViewClaim<'a> {
    /// The goal the view is asked for.
    pub goal: &'a str,
    /// The traversal budget the caller asked for, when it named one.
    pub max_tasks: Option<usize>,
}

/// One authorization check.
///
/// Construct through [`Self::new`] and the `with_*` setters rather than a struct
/// literal: every operation class that needs a typed claim adds a field, and a
/// literal makes each addition a breaking change at every call site — which is
/// how the claim list grew to six without any of them being optional in
/// practice.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct A2AAuthorizationRequest<'a> {
    /// Operation being attempted.
    pub operation: A2AOperation,
    /// Canonical tenant, when resolved.
    pub tenant: Option<&'a str>,
    /// Target task id, when the operation names one.
    pub task_id: Option<&'a str>,
    /// Authenticated principal, when the request supplied one.
    pub principal: Option<&'a PrincipalRef>,
    /// The claimed transfer, on [`A2AOperation::RecordHandoff`] checks.
    pub handoff: Option<A2AHandoffClaim<'a>>,
    /// The claimed team command, on [`A2AOperation::TeamCommand`] checks.
    pub team: Option<A2ATeamClaim<'a>>,
    /// The claimed conversation command, on
    /// [`A2AOperation::ConversationCommand`] checks.
    pub conversation: Option<A2AConversationClaim<'a>>,
    /// The claimed result contract, on [`A2AOperation::SubmitTaskResult`]
    /// checks.
    pub task_result: Option<A2ATaskResultClaim<'a>>,
    /// The addressed scope, on [`A2AOperation::CoordinationEventRead`] checks.
    pub coordination: Option<A2ACoordinationClaim<'a>>,
    /// The addressed goal, on [`A2AOperation::GoalViewRead`] checks.
    pub goal_view: Option<A2AGoalViewClaim<'a>>,
}

impl<'a> A2AAuthorizationRequest<'a> {
    /// A check for `operation`, carrying no tenant, principal, or claim yet.
    #[must_use]
    pub const fn new(operation: A2AOperation) -> Self {
        Self {
            operation,
            tenant: None,
            task_id: None,
            principal: None,
            handoff: None,
            team: None,
            conversation: None,
            task_result: None,
            coordination: None,
            goal_view: None,
        }
    }

    /// Sets the canonical tenant.
    #[must_use]
    pub const fn with_tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }

    /// Sets the target task id.
    #[must_use]
    pub const fn with_task_id(mut self, task_id: &'a str) -> Self {
        self.task_id = Some(task_id);
        self
    }

    /// Sets the target task id when the caller has one.
    #[must_use]
    pub const fn with_optional_task_id(mut self, task_id: Option<&'a str>) -> Self {
        self.task_id = task_id;
        self
    }

    /// Sets the authenticated principal when the request supplied one.
    #[must_use]
    pub const fn with_principal(mut self, principal: Option<&'a PrincipalRef>) -> Self {
        self.principal = principal;
        self
    }

    /// Binds the claimed transfer.
    #[must_use]
    pub const fn with_handoff(mut self, handoff: A2AHandoffClaim<'a>) -> Self {
        self.handoff = Some(handoff);
        self
    }

    /// Binds the claimed team command.
    #[must_use]
    pub const fn with_team(mut self, team: A2ATeamClaim<'a>) -> Self {
        self.team = Some(team);
        self
    }

    /// Binds the claimed conversation command.
    #[must_use]
    pub const fn with_conversation(mut self, conversation: A2AConversationClaim<'a>) -> Self {
        self.conversation = Some(conversation);
        self
    }

    /// Binds the claimed result contract.
    #[must_use]
    pub const fn with_task_result(mut self, task_result: A2ATaskResultClaim<'a>) -> Self {
        self.task_result = Some(task_result);
        self
    }

    /// Binds the addressed coordination scope.
    #[must_use]
    pub const fn with_coordination(mut self, coordination: A2ACoordinationClaim<'a>) -> Self {
        self.coordination = Some(coordination);
        self
    }

    /// Binds the addressed goal.
    #[must_use]
    pub const fn with_goal_view(mut self, goal_view: A2AGoalViewClaim<'a>) -> Self {
        self.goal_view = Some(goal_view);
        self
    }
}

/// Authorization decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum A2AAuthorizationDecision {
    /// Proceed with the operation.
    Allow,
    /// Refuse the operation. For task-scoped operations the refusal is
    /// surfaced as task-not-found so authorization failures stay
    /// indistinguishable from missing tasks.
    Deny,
}

/// Application hook gating public A2A operations.
///
/// The default ([`AllowAllAuthorizer`]) allows everything; production
/// deployments should authenticate at the ingress and enforce tenancy and
/// principal policy here.
#[async_trait]
pub trait A2AAuthorizer: Send + Sync + 'static {
    /// Decides one operation.
    async fn authorize(&self, request: &A2AAuthorizationRequest<'_>) -> A2AAuthorizationDecision;
}

/// Permits every operation (default).
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAllAuthorizer;

#[async_trait]
impl A2AAuthorizer for AllowAllAuthorizer {
    async fn authorize(&self, _request: &A2AAuthorizationRequest<'_>) -> A2AAuthorizationDecision {
        A2AAuthorizationDecision::Allow
    }
}
