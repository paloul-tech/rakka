//! Agent card construction for the Phase 2 A2A surface.

use a2a::{
    AgentCapabilities, AgentCard, AgentInterface, AgentProvider, AgentSkill,
    TRANSPORT_PROTOCOL_HTTP_JSON, TRANSPORT_PROTOCOL_JSONRPC,
};

use crate::config::ExampleConfig;
use crate::support::WORKFLOW_TYPE;

/// Builds the conservative Phase 2 agent card.
#[must_use]
pub fn build_agent_card(config: &ExampleConfig) -> AgentCard {
    let base_url = config
        .public_url
        .clone()
        .unwrap_or_else(|| config.local_public_url());

    AgentCard {
        name: "Rakka Phase 2 A2A Agent".to_string(),
        description: "A Phase 2 Rakka A2A example with durable local command acceptance, task projections, and clustered sharded run hosting.".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        supported_interfaces: vec![
            // Interface URLs are the base the SDK's transport-relative paths
            // are appended to, so both must include their server.rs nest prefix.
            AgentInterface::new(format!("{base_url}/a2a"), TRANSPORT_PROTOCOL_HTTP_JSON),
            AgentInterface::new(format!("{base_url}/a2a/jsonrpc"), TRANSPORT_PROTOCOL_JSONRPC),
        ],
        capabilities: AgentCapabilities {
            streaming: Some(false),
            push_notifications: Some(false),
            extensions: None,
            extended_agent_card: Some(false),
        },
        default_input_modes: vec!["text/plain".to_string(), "application/json".to_string()],
        default_output_modes: vec!["text/plain".to_string(), "application/json".to_string()],
        skills: vec![AgentSkill {
            id: WORKFLOW_TYPE.to_string(),
            name: "Phase 2 demo workflow".to_string(),
            description: "Advertised workflow skill for durable A2A send, read, list, and cancel paths in local mode.".to_string(),
            tags: vec!["rakka".to_string(), "phase-2".to_string(), "durable-agent".to_string()],
            examples: Some(vec!["Send a message and retry the same message id to observe durable deduplication.".to_string()]),
            input_modes: Some(vec!["text/plain".to_string(), "application/json".to_string()]),
            output_modes: Some(vec!["text/plain".to_string(), "application/json".to_string()]),
            security_requirements: None,
        }],
        provider: Some(AgentProvider {
            organization: "Rakka".to_string(),
            url: "https://github.com/rakka-rs/rakka".to_string(),
        }),
        documentation_url: None,
        icon_url: None,
        security_schemes: None,
        security_requirements: None,
        signatures: None,
    }
}
