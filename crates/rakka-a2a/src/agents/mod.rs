//! Typed Rakka Agent surface over `rakka-agent` entities.
//!
//! This module is the A2A adaptation of the `rakka-agent` domain
//! (specification section 14): public A2A task identity equals
//! [`rakka_agent::AgentTaskId`] verbatim, ingress is durably accepted and
//! deduplicated by the owning entity's operation-id inbox before it is
//! acknowledged, task state is projected row-for-row from the authoritative
//! task/run condition (specification 14.3), and settings/administrative
//! commands enter through the versioned agent-management extension
//! ([`management`]) rather than internal actor remoting.
//!
//! The projection is a public view, never the correctness source: durable
//! task, run, and inbox/outbox state remain authoritative, exactly as for the
//! workflow-substrate surface this crate already serves. The two surfaces are
//! siblings — nothing here changes the existing workflow handler — and the
//! dependency is one-directional: `rakka-agent` never depends on this crate.

pub mod catalog;
pub mod client;
pub mod collaboration;
pub mod delegation;
pub mod error;
pub mod handoff;
pub mod ingress;
pub mod management;
pub mod projection;
pub mod service;
mod sync;

pub use catalog::{A2AAgentCatalog, A2AAgentSelector, A2AAgentTarget, A2AStaticAgentCatalog};
pub use client::A2AAgentClientTransport;
pub use collaboration::{
    agent_collaboration_extension, collaboration_echo, handoff_echo, is_collaboration_message,
    parse_collaboration_envelope, parse_collaboration_metadata, AgentCollaborationBudget,
    AgentCollaborationEnvelope, AgentCollaborationMetadata, AgentCollaborationSchemaRef,
    AgentHandoffCollaborationMetadata, AGENT_COLLABORATION_EXTENSION_PREFIX,
    AGENT_COLLABORATION_EXTENSION_URI, AGENT_COLLABORATION_SCHEMA_VERSION, META_COLLABORATION,
};
pub use delegation::A2AAgentDelegationSendExecutor;
pub use error::{RakkaAgentA2AError, RakkaAgentA2AResult};
pub use handoff::A2AAgentHandoffSendExecutor;
pub use ingress::{NormalizedAgentCommand, META_AGENT_ID, META_TASK_DEFINITION};
pub use management::{
    agent_management_extension, management_request_message, parse_management_response,
    AgentManagementCommand, AgentManagementDescription, AgentManagementOutcome,
    AgentManagementRequest, AgentManagementResponse, AGENT_MANAGEMENT_EXTENSION_PREFIX,
    AGENT_MANAGEMENT_EXTENSION_URI, AGENT_MANAGEMENT_SCHEMA_VERSION,
};
pub use projection::{
    agent_task_state, agent_task_state_metadata, AgentTaskCondition, META_AGENT_RUN_CONDITION,
    META_AGENT_TASK_CONDITION, META_AGENT_WAIT_REASON,
};
pub use service::{A2AAgentClock, RakkaAgentA2AService, SystemA2AAgentClock};
