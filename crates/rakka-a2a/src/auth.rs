//! Authorization hook for public A2A operations.

use async_trait::async_trait;
use rakka_agent_workflow::PrincipalRef;

/// Public A2A operation classes an authorizer can gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum A2AOperation {
    /// `message:send` (blocking or streaming).
    SendMessage,
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
