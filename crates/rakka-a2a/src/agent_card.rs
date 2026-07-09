//! Dynamic agent-card production.
//!
//! The agent card is built from the service's actual configuration —
//! transport routes, hosted workflows, security schemes, and whether push
//! delivery is wired — rather than hardcoded. Capabilities reflect reality:
//! streaming is advertised, and `push_notifications=true` is set only when a
//! push dispatcher is configured, so the card never over-promises delivery.

use std::collections::HashMap;

use a2a::{
    AgentCapabilities, AgentCard, AgentInterface, AgentProvider, AgentSkill, SecurityScheme,
    TRANSPORT_PROTOCOL_HTTP_JSON, TRANSPORT_PROTOCOL_JSONRPC,
};

use crate::catalog::A2AWorkflowCatalog;

/// Which A2A transports the service exposes, relative to a public base URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct A2ATransports {
    /// Advertise the REST (`HTTP+JSON`) transport.
    pub rest: bool,
    /// Advertise the JSON-RPC transport.
    pub jsonrpc: bool,
}

impl Default for A2ATransports {
    fn default() -> Self {
        Self {
            rest: true,
            jsonrpc: true,
        }
    }
}

/// Builds a dynamic [`AgentCard`] from bounded service metadata.
///
/// The advertised interface URLs are appended to the caller-supplied public
/// base URL (the load balancer address), so a card built on any node points
/// clients at the load-balanced ingress rather than a per-node address.
#[derive(Clone)]
pub struct A2AAgentCardBuilder {
    name: String,
    description: String,
    version: String,
    public_base_url: Option<String>,
    rest_path: String,
    jsonrpc_path: String,
    transports: A2ATransports,
    streaming: bool,
    push_notifications: bool,
    extended_agent_card: bool,
    default_input_modes: Vec<String>,
    default_output_modes: Vec<String>,
    provider: Option<AgentProvider>,
    documentation_url: Option<String>,
    security_schemes: Option<HashMap<String, SecurityScheme>>,
    skill_tags: Vec<String>,
}

impl std::fmt::Debug for A2AAgentCardBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("A2AAgentCardBuilder")
            .field("name", &self.name)
            .field("public_base_url", &self.public_base_url)
            .field("push_notifications", &self.push_notifications)
            .finish_non_exhaustive()
    }
}

impl A2AAgentCardBuilder {
    /// Starts a card builder with the service name and description.
    #[must_use]
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            public_base_url: None,
            rest_path: "/a2a".to_string(),
            jsonrpc_path: "/a2a/jsonrpc".to_string(),
            transports: A2ATransports::default(),
            streaming: true,
            push_notifications: false,
            extended_agent_card: false,
            default_input_modes: vec!["text/plain".to_string(), "application/json".to_string()],
            default_output_modes: vec!["text/plain".to_string(), "application/json".to_string()],
            provider: None,
            documentation_url: None,
            security_schemes: None,
            skill_tags: vec!["rakka".to_string(), "durable-agent".to_string()],
        }
    }

    /// Sets the advertised card version.
    #[must_use]
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    /// Sets the load-balanced public base URL interface URLs are built from.
    ///
    /// Required for a production card: without it the interface URLs use only
    /// the nest paths, which are not dialable.
    #[must_use]
    pub fn public_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.public_base_url = Some(base_url.into());
        self
    }

    /// Overrides the REST and JSON-RPC nest paths (defaults `/a2a` and
    /// `/a2a/jsonrpc`).
    #[must_use]
    pub fn transport_paths(
        mut self,
        rest_path: impl Into<String>,
        jsonrpc_path: impl Into<String>,
    ) -> Self {
        self.rest_path = rest_path.into();
        self.jsonrpc_path = jsonrpc_path.into();
        self
    }

    /// Selects which transports are advertised.
    #[must_use]
    pub fn transports(mut self, transports: A2ATransports) -> Self {
        self.transports = transports;
        self
    }

    /// Advertises push notification support.
    ///
    /// Set this only when a push dispatcher is actually configured; the crate
    /// deliberately defaults it off so the card never implies delivery that
    /// is not wired.
    #[must_use]
    pub fn push_notifications(mut self, configured: bool) -> Self {
        self.push_notifications = configured;
        self
    }

    /// Advertises extended-agent-card support.
    #[must_use]
    pub fn extended_agent_card(mut self, supported: bool) -> Self {
        self.extended_agent_card = supported;
        self
    }

    /// Overrides the default input/output content-type modes.
    #[must_use]
    pub fn default_modes(mut self, input_modes: Vec<String>, output_modes: Vec<String>) -> Self {
        self.default_input_modes = input_modes;
        self.default_output_modes = output_modes;
        self
    }

    /// Sets the provider block.
    #[must_use]
    pub fn provider(mut self, organization: impl Into<String>, url: impl Into<String>) -> Self {
        self.provider = Some(AgentProvider {
            organization: organization.into(),
            url: url.into(),
        });
        self
    }

    /// Sets the documentation URL.
    #[must_use]
    pub fn documentation_url(mut self, url: impl Into<String>) -> Self {
        self.documentation_url = Some(url.into());
        self
    }

    /// Sets the advertised security schemes.
    #[must_use]
    pub fn security_schemes(mut self, schemes: HashMap<String, SecurityScheme>) -> Self {
        self.security_schemes = Some(schemes);
        self
    }

    /// Overrides the tags applied to every projected workflow skill.
    #[must_use]
    pub fn skill_tags(mut self, tags: Vec<String>) -> Self {
        self.skill_tags = tags;
        self
    }

    /// Builds the card, projecting one skill per hosted workflow.
    #[must_use]
    pub fn build(&self, catalog: &dyn A2AWorkflowCatalog) -> AgentCard {
        let base = self.public_base_url.clone().unwrap_or_default();
        let mut supported_interfaces = Vec::new();
        if self.transports.rest {
            supported_interfaces.push(AgentInterface::new(
                format!("{base}{}", self.rest_path),
                TRANSPORT_PROTOCOL_HTTP_JSON,
            ));
        }
        if self.transports.jsonrpc {
            supported_interfaces.push(AgentInterface::new(
                format!("{base}{}", self.jsonrpc_path),
                TRANSPORT_PROTOCOL_JSONRPC,
            ));
        }

        let skills = catalog
            .workflows()
            .into_iter()
            .map(|workflow| self.skill_for_workflow(workflow))
            .collect::<Vec<_>>();

        AgentCard {
            name: self.name.clone(),
            description: self.description.clone(),
            version: self.version.clone(),
            supported_interfaces,
            capabilities: AgentCapabilities {
                streaming: Some(self.streaming),
                push_notifications: Some(self.push_notifications),
                extensions: None,
                extended_agent_card: Some(self.extended_agent_card),
            },
            default_input_modes: self.default_input_modes.clone(),
            default_output_modes: self.default_output_modes.clone(),
            skills,
            provider: self.provider.clone(),
            documentation_url: self.documentation_url.clone(),
            icon_url: None,
            security_schemes: self.security_schemes.clone(),
            security_requirements: None,
            signatures: None,
        }
    }

    fn skill_for_workflow(&self, workflow: &rakka_agent_workflow::AgentWorkflow) -> AgentSkill {
        AgentSkill {
            id: workflow.workflow_type.clone(),
            name: workflow
                .display_name
                .clone()
                .unwrap_or_else(|| workflow.workflow_type.clone()),
            description: format!(
                "Durable A2A workflow {} (definition {})",
                workflow.workflow_id.as_str(),
                workflow.definition_version.as_str()
            ),
            tags: self.skill_tags.clone(),
            examples: None,
            input_modes: Some(self.default_input_modes.clone()),
            output_modes: Some(self.default_output_modes.clone()),
            security_requirements: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::A2AStaticWorkflowCatalog;
    use crate::testing::fixture_workflow;

    #[test]
    fn card_advertises_load_balanced_urls_and_configured_capabilities() {
        let catalog = A2AStaticWorkflowCatalog::single(fixture_workflow());
        let card = A2AAgentCardBuilder::new("Rakka A2A", "durable agent")
            .public_base_url("https://agents.example.test/rakka-a2a")
            .push_notifications(true)
            .provider("Rakka", "https://github.com/rakka-rs/rakka")
            .build(&catalog);

        assert_eq!(card.capabilities.streaming, Some(true));
        assert_eq!(card.capabilities.push_notifications, Some(true));
        assert!(card.supported_interfaces.iter().all(|interface| interface
            .url
            .starts_with("https://agents.example.test/rakka-a2a")));
        assert_eq!(card.skills.len(), 1);
        assert_eq!(card.skills[0].id, fixture_workflow().workflow_type);
    }

    #[test]
    fn push_notifications_default_off_and_skills_track_catalog() {
        let mut second = fixture_workflow();
        second.workflow_id = rakka_agent_workflow::AgentWorkflowId::new("workflow-second");
        second.workflow_type = "second-type".to_string();
        let catalog =
            A2AStaticWorkflowCatalog::new(vec![fixture_workflow(), second]).expect("catalog");
        let card = A2AAgentCardBuilder::new("Rakka A2A", "durable agent").build(&catalog);

        assert_eq!(
            card.capabilities.push_notifications,
            Some(false),
            "push must default off until a dispatcher is configured"
        );
        assert_eq!(card.skills.len(), 2, "one skill per hosted workflow");
    }
}
