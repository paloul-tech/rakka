//! Trigger source metadata for normalized agent commands.
//!
//! Application ingress code owns trigger registration, routing, schedules, and
//! auth. Rakka only carries low-cardinality trigger metadata on durable
//! commands after that ingress layer has normalized a trigger execution.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::{
    is_bounded_agent_metric_attribute, is_forbidden_agent_metric_attribute, AgentAttributes,
    AgentCommand, AGENT_METRIC_ATTRIBUTE_VALUE_MAX_BYTES,
};

/// Command attribute key containing the normalized trigger kind.
pub const AGENT_TRIGGER_KIND_ATTRIBUTE: &str = "trigger_kind";

/// Command attribute key containing a bounded deployment channel label.
pub const AGENT_TRIGGER_DEPLOYMENT_CHANNEL_ATTRIBUTE: &str = "deployment_channel";

/// Command attribute key containing a bounded tenant tier label.
pub const AGENT_TRIGGER_TENANT_TIER_ATTRIBUTE: &str = "tenant_tier";

/// Stable category for the application-owned trigger source that produced a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentTriggerSourceKind {
    /// User or service request accepted by the application's API boundary.
    Api,
    /// External webhook event accepted by application-owned webhook routing.
    Webhook,
    /// Scheduled fire accepted by application-owned schedule management.
    Schedule,
    /// Manual or explicit run request.
    OnDemand,
    /// Internal system command.
    System,
    /// Command produced by a parent workflow run.
    ChildWorkflow,
    /// External callback normalized by application-owned callback routing.
    ExternalCallback,
    /// Human decision submitted through application-owned human UI or API.
    HumanDecision,
}

impl AgentTriggerSourceKind {
    /// Returns all supported trigger source categories in stable order.
    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::Api,
            Self::Webhook,
            Self::Schedule,
            Self::OnDemand,
            Self::System,
            Self::ChildWorkflow,
            Self::ExternalCallback,
            Self::HumanDecision,
        ]
    }

    /// Stable lowercase label for command attributes and hot metrics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Webhook => "webhook",
            Self::Schedule => "schedule",
            Self::OnDemand => "on-demand",
            Self::System => "system",
            Self::ChildWorkflow => "child-workflow",
            Self::ExternalCallback => "external-callback",
            Self::HumanDecision => "human-decision",
        }
    }

    /// Parses a stable trigger source label.
    #[must_use]
    pub fn from_label(value: &str) -> Option<Self> {
        Self::all()
            .iter()
            .copied()
            .find(|kind| kind.as_label() == value)
    }
}

/// Bounded metadata describing the trigger source for a durable command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTriggerSource {
    /// Stable trigger source category.
    pub kind: AgentTriggerSourceKind,
    /// Supplemental low-cardinality labels safe for command projections and metrics.
    #[serde(default)]
    pub labels: AgentAttributes,
}

impl AgentTriggerSource {
    /// Creates trigger source metadata for a trigger kind.
    #[must_use]
    pub fn new(kind: AgentTriggerSourceKind) -> Self {
        Self {
            kind,
            labels: BTreeMap::new(),
        }
    }

    /// Creates API trigger source metadata.
    #[must_use]
    pub fn api() -> Self {
        Self::new(AgentTriggerSourceKind::Api)
    }

    /// Creates webhook trigger source metadata.
    #[must_use]
    pub fn webhook() -> Self {
        Self::new(AgentTriggerSourceKind::Webhook)
    }

    /// Creates schedule trigger source metadata.
    #[must_use]
    pub fn schedule() -> Self {
        Self::new(AgentTriggerSourceKind::Schedule)
    }

    /// Creates on-demand trigger source metadata.
    #[must_use]
    pub fn on_demand() -> Self {
        Self::new(AgentTriggerSourceKind::OnDemand)
    }

    /// Creates system trigger source metadata.
    #[must_use]
    pub fn system() -> Self {
        Self::new(AgentTriggerSourceKind::System)
    }

    /// Creates child-workflow trigger source metadata.
    #[must_use]
    pub fn child_workflow() -> Self {
        Self::new(AgentTriggerSourceKind::ChildWorkflow)
    }

    /// Creates external-callback trigger source metadata.
    #[must_use]
    pub fn external_callback() -> Self {
        Self::new(AgentTriggerSourceKind::ExternalCallback)
    }

    /// Creates human-decision trigger source metadata.
    #[must_use]
    pub fn human_decision() -> Self {
        Self::new(AgentTriggerSourceKind::HumanDecision)
    }

    /// Adds a bounded low-cardinality trigger label.
    pub fn label(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> AgentTriggerSourceResult<Self> {
        self.labels.insert(key.into(), value.into());
        self.validate()?;
        Ok(self)
    }

    /// Adds the bounded deployment channel label.
    pub fn deployment_channel(
        self,
        deployment_channel: impl Into<String>,
    ) -> AgentTriggerSourceResult<Self> {
        self.label(
            AGENT_TRIGGER_DEPLOYMENT_CHANNEL_ATTRIBUTE,
            deployment_channel,
        )
    }

    /// Adds the bounded tenant tier label.
    pub fn tenant_tier(self, tenant_tier: impl Into<String>) -> AgentTriggerSourceResult<Self> {
        self.label(AGENT_TRIGGER_TENANT_TIER_ATTRIBUTE, tenant_tier)
    }

    /// Converts this trigger source into command attributes.
    pub fn command_attributes(&self) -> AgentTriggerSourceResult<AgentAttributes> {
        self.validate()?;

        let mut attributes = self.labels.clone();
        attributes.insert(
            AGENT_TRIGGER_KIND_ATTRIBUTE.to_string(),
            self.kind.as_label().to_string(),
        );
        Ok(attributes)
    }

    /// Attaches this trigger source to a command without changing durable ids.
    pub fn attach_to_command(
        &self,
        mut command: AgentCommand,
    ) -> AgentTriggerSourceResult<AgentCommand> {
        for (key, value) in self.command_attributes()? {
            if let Some(existing) = command.attributes.get(&key) {
                if existing != &value {
                    return Err(AgentTriggerSourceError::ConflictingCommandAttribute { key });
                }
                continue;
            }
            command.attributes.insert(key, value);
        }
        command
            .validate()
            .map_err(AgentTriggerSourceError::Command)?;
        Ok(command)
    }

    /// Validates that supplemental labels are hot-metric safe.
    pub fn validate(&self) -> AgentTriggerSourceResult<()> {
        for (key, value) in &self.labels {
            validate_trigger_label(key, value)?;
        }
        Ok(())
    }
}

/// Shared result type for trigger source validation.
pub type AgentTriggerSourceResult<T> = Result<T, AgentTriggerSourceError>;

/// Validation failures returned by trigger source helpers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentTriggerSourceError {
    /// A trigger label is not safe to attach to command metadata.
    InvalidLabel {
        /// Label key that failed validation.
        key: String,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// Attaching trigger metadata would overwrite an existing different command attribute.
    ConflictingCommandAttribute {
        /// Conflicting command attribute key.
        key: String,
    },
    /// The command became invalid after trigger attributes were attached.
    Command(crate::AgentFacadeError),
}

impl AgentTriggerSourceError {
    /// Stable error code for programmatic assertions and logs.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidLabel { .. } => "invalid-trigger-label",
            Self::ConflictingCommandAttribute { .. } => "conflicting-command-attribute",
            Self::Command(_) => "invalid-trigger-command",
        }
    }
}

impl Display for AgentTriggerSourceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLabel { key, reason } => {
                write!(f, "invalid trigger label `{key}`: {reason}")
            }
            Self::ConflictingCommandAttribute { key } => {
                write!(
                    f,
                    "trigger label `{key}` conflicts with an existing command attribute"
                )
            }
            Self::Command(error) => write!(f, "invalid trigger command: {error}"),
        }
    }
}

impl Error for AgentTriggerSourceError {}

fn validate_trigger_label(key: &str, value: &str) -> AgentTriggerSourceResult<()> {
    if key.trim().is_empty() {
        return Err(invalid_label(key, "label keys must not be empty"));
    }
    if key.len() > AGENT_METRIC_ATTRIBUTE_VALUE_MAX_BYTES {
        return Err(invalid_label(key, "label keys must be bounded"));
    }
    if key.contains('\n') || key.contains('\r') {
        return Err(invalid_label(key, "label keys must be single-line"));
    }
    if key == AGENT_TRIGGER_KIND_ATTRIBUTE {
        return Err(invalid_label(
            key,
            "trigger kind is derived from AgentTriggerSourceKind",
        ));
    }
    if is_forbidden_agent_metric_attribute(key)
        || !is_bounded_agent_metric_attribute(key)
        || is_sensitive_trigger_key(key)
    {
        return Err(invalid_label(
            key,
            "label keys must be bounded hot-metric attributes and must not contain ids or secrets",
        ));
    }
    if value.trim().is_empty() {
        return Err(invalid_label(key, "label values must not be empty"));
    }
    if value.len() > AGENT_METRIC_ATTRIBUTE_VALUE_MAX_BYTES {
        return Err(invalid_label(key, "label values must be bounded"));
    }
    if value.contains('\n') || value.contains('\r') {
        return Err(invalid_label(
            key,
            "label values must be single-line bounded labels",
        ));
    }
    if looks_like_sensitive_value(value) {
        return Err(invalid_label(
            key,
            "label values must not contain URLs, credentials, or secret material",
        ));
    }
    Ok(())
}

fn invalid_label(key: &str, reason: &'static str) -> AgentTriggerSourceError {
    AgentTriggerSourceError::InvalidLabel {
        key: key.to_string(),
        reason,
    }
}

fn is_sensitive_trigger_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    normalized.contains("secret")
        || normalized.contains("token")
        || normalized.contains("password")
        || normalized.contains("api_key")
        || normalized.contains("apikey")
        || normalized.contains("authorization")
        || normalized.contains("credential")
        || normalized.contains("signature")
        || normalized.contains("webhook_url")
        || normalized.contains("request_body")
        || normalized.contains("user_id")
}

fn looks_like_sensitive_value(value: &str) -> bool {
    let trimmed = value.trim();
    let lower = trimmed.to_ascii_lowercase();
    lower.starts_with("http://")
        || lower.starts_with("https://")
        || lower.starts_with("bearer ")
        || lower.starts_with("basic ")
        || trimmed.starts_with("sk-")
        || trimmed.starts_with("xoxb-")
        || trimmed.starts_with("ghp_")
        || trimmed.starts_with("github_pat_")
        || trimmed.starts_with("AKIA")
        || trimmed.starts_with("-----BEGIN ")
}
