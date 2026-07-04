//! Agent card construction for the Phase 0 A2A surface.

use a2a::{
    AgentCapabilities, AgentCard, AgentInterface, AgentProvider, AgentSkill,
    TRANSPORT_PROTOCOL_HTTP_JSON, TRANSPORT_PROTOCOL_JSONRPC,
};

use crate::config::ExampleConfig;
use crate::support::WORKFLOW_TYPE;

/// Builds the conservative Phase 0 agent card.
#[must_use]
pub fn build_agent_card(config: &ExampleConfig) -> AgentCard {
    let base_url = config
        .public_url
        .clone()
        .unwrap_or_else(|| config.local_public_url());

    AgentCard {
        name: "Rakka Phase 0 A2A Agent".to_string(),
        description: "A Phase 0 Rakka A2A skeleton with clustered sharded run hosting.".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        supported_interfaces: vec![
            AgentInterface::new(base_url.clone(), TRANSPORT_PROTOCOL_HTTP_JSON),
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
            name: "Phase 0 demo workflow".to_string(),
            description: "Advertised workflow skill; command execution is intentionally disabled until Phase 2.".to_string(),
            tags: vec!["rakka".to_string(), "phase-0".to_string(), "durable-agent".to_string()],
            examples: Some(vec!["Send a message once Phase 2 durable acceptance is implemented.".to_string()]),
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
