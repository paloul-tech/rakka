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
}

impl A2AOperation {
    /// Stable lowercase label for bounded metrics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::SendMessage => "send-message",
            Self::RecordHandoff => "record-handoff",
            Self::GetTask => "get-task",
            Self::ListTasks => "list-tasks",
            Self::CancelTask => "cancel-task",
            Self::SubscribeToTask => "subscribe-to-task",
            Self::PushConfigWrite => "push-config-write",
            Self::PushConfigRead => "push-config-read",
            Self::ExtendedAgentCard => "extended-agent-card",
            Self::AgentManagementWrite => "agent-management-write",
            Self::AgentManagementRead => "agent-management-read",
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

/// One authorization check.
#[derive(Debug, Clone)]
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
