//! Agent card construction for the clustered A2A surface.

use a2a::{
    AgentCapabilities, AgentCard, AgentInterface, AgentProvider, AgentSkill,
    TRANSPORT_PROTOCOL_HTTP_JSON, TRANSPORT_PROTOCOL_JSONRPC,
};

use crate::config::ExampleConfig;
use crate::support::WORKFLOW_TYPE;

/// Builds the conservative clustered example agent card.
#[must_use]
pub fn build_agent_card(config: &ExampleConfig) -> AgentCard {
    let base_url = config
        .public_url
        .clone()
        .unwrap_or_else(|| config.local_public_url());

    AgentCard {
        name: "Rakka Clustered A2A Agent".to_string(),
        description: "A Rakka A2A example with durable command acceptance, task projections, and clustered sharded run hosting.".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        supported_interfaces: vec![
            // Interface URLs are the base the SDK's transport-relative paths
            // are appended to, so both must include their server.rs nest prefix.
            AgentInterface::new(format!("{base_url}/a2a"), TRANSPORT_PROTOCOL_HTTP_JSON),
            AgentInterface::new(format!("{base_url}/a2a/jsonrpc"), TRANSPORT_PROTOCOL_JSONRPC),
        ],
        capabilities: AgentCapabilities {
            streaming: Some(true),
            push_notifications: Some(false),
            extensions: None,
            extended_agent_card: Some(false),
        },
        default_input_modes: vec!["text/plain".to_string(), "application/json".to_string()],
        default_output_modes: vec!["text/plain".to_string(), "application/json".to_string()],
        skills: vec![AgentSkill {
            id: WORKFLOW_TYPE.to_string(),
            name: "Clustered demo workflow".to_string(),
            description: "Advertised workflow skill for durable A2A send, read, list, cancel, and owner-routed clustered paths.".to_string(),
            tags: vec!["rakka".to_string(), "clustered".to_string(), "durable-agent".to_string()],
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::{DiscoveryProviderKind, PersistenceKind};

    #[test]
    fn card_advertises_load_balancer_url_and_implemented_features() {
        let card = build_agent_card(&ExampleConfig {
            bind_host: "0.0.0.0".parse().expect("bind host"),
            advertise_host: "10.0.0.10".to_string(),
            rakka_port: 25_580,
            http_port: 35_580,
            node_logical_id: "rakka-a2a-0".to_string(),
            node_incarnation: "pod-uid".to_string(),
            discovery_provider: DiscoveryProviderKind::Etcd,
            discovery_dir: std::env::temp_dir(),
            etcd_endpoints: vec!["http://rakka-a2a-etcd:2379".to_string()],
            etcd_prefix: "/rakka/examples/a2a-agents".to_string(),
            etcd_lease_ttl_seconds: 10,
            persistence: PersistenceKind::Postgres,
            postgres_dsn: Some("host=rakka-postgres dbname=postgres".to_string()),
            state_dir: std::env::temp_dir(),
            self_fence: true,
            self_fence_after: Duration::from_secs(15),
            self_fence_rejoin_after: Duration::from_secs(10),
            public_url: Some("https://agents.example.test/rakka-a2a".to_string()),
        });

        assert_eq!(card.capabilities.streaming, Some(true));
        assert_eq!(card.capabilities.push_notifications, Some(false));
        assert!(card.supported_interfaces.iter().all(|interface| interface
            .url
            .starts_with("https://agents.example.test/rakka-a2a")));
    }
}
