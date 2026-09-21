//! The model profile record and catalog
//! ([specification 10.1](../../../docs/plans/rakka-agent/spec.md); Phase 7
//! design section 4.1).
//!
//! A profile is a deployment-owned, secret-free description of one approved
//! model: which provider, which model name, an optional non-default endpoint,
//! the logical credential binding a dispatch attempt may resolve, default
//! sampling, and bounded non-secret attributes. [`AgentModelProfileId`] stays
//! the opaque id the definition envelope approves; the record behind it is
//! resolved on demand through [`AgentModelProfileCatalog`], so a deployment
//! whose profiles are release data implements the trait in one function over
//! its own record and stores nothing new. [`StaticAgentModelProfileCatalog`]
//! is one implementation, for tests, examples, and deployments with no
//! release data.
//!
//! [`AgentModelRouter`] is the provider-neutral half of the adapter story: an
//! [`AgentModelAdapter`] that selects one of several adapters by the
//! request's profile.

use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::definition::{
    AgentCredentialBindingRef, AgentModelProfileId, AgentRevisionNumber, AgentSamplingSettings,
};
use crate::model::{
    AgentModelAdapter, AgentModelError, AgentModelFuture, AgentModelRequest, AgentModelRetryPolicy,
};
use crate::task::AgentContentDigest;
use rakka_agent_workflow::AgentEphemeralCredential;

/// Longest provider model name a profile may carry, in bytes.
pub const AGENT_MODEL_PROFILE_MODEL_MAX_BYTES: usize = 128;

/// Longest attribute value a profile may carry, in bytes.
pub const AGENT_MODEL_PROFILE_ATTRIBUTE_MAX_BYTES: usize = 256;

/// Attribute keys a profile may never carry, compared case-insensitively:
/// a value under one of these is secret-shaped whatever it holds.
pub const AGENT_MODEL_PROFILE_FORBIDDEN_ATTRIBUTE_KEYS: [&str; 4] =
    ["api_key", "token", "authorization", "secret"];

/// The provider family a profile addresses.
///
/// Rig does not gate providers individually, so every kind is reachable
/// once the `rig` feature is on; `Custom` names an OpenAI-compatible
/// endpoint by a deployment's own label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentModelProviderKind {
    /// Anthropic's Messages API.
    Anthropic,
    /// OpenAI's Responses API.
    OpenAiResponses,
    /// OpenAI's Chat Completions API.
    OpenAiCompletions,
    /// Azure OpenAI.
    AzureOpenAi,
    /// Google Gemini's generateContent API.
    Gemini,
    /// A local or remote Ollama server.
    Ollama,
    /// OpenRouter.
    OpenRouter,
    /// An OpenAI-compatible endpoint under a deployment-chosen label.
    Custom(String),
}

impl AgentModelProviderKind {
    /// Stable kebab-case label; `Custom` answers its own label.
    #[must_use]
    pub fn as_label(&self) -> String {
        match self {
            Self::Anthropic => "anthropic".to_string(),
            Self::OpenAiResponses => "openai-responses".to_string(),
            Self::OpenAiCompletions => "openai-completions".to_string(),
            Self::AzureOpenAi => "azure-openai".to_string(),
            Self::Gemini => "gemini".to_string(),
            Self::Ollama => "ollama".to_string(),
            Self::OpenRouter => "openrouter".to_string(),
            Self::Custom(label) => label.clone(),
        }
    }
}

/// What a profile's model can do, as the deployment declares it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AgentModelCapabilities {
    /// Whether the model accepts tool definitions and may answer with tool calls.
    pub tool_calls: bool,
}

/// One approved model, described without any secret.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentModelProfile {
    /// The id the definition envelope approves.
    pub profile_id: AgentModelProfileId,
    /// The record's revision, part of its digest.
    pub revision: AgentRevisionNumber,
    /// The provider family.
    pub provider: AgentModelProviderKind,
    /// The provider's model name, at most [`AGENT_MODEL_PROFILE_MODEL_MAX_BYTES`].
    pub model: String,
    /// A non-default endpoint; no userinfo, no query string.
    pub base_url: Option<String>,
    /// The logical credential binding a dispatch attempt resolves; the only
    /// way a profile reaches a secret.
    pub credential_binding: Option<AgentCredentialBindingRef>,
    /// Sampling defaults a settings revision may narrow.
    pub default_sampling: AgentSamplingSettings,
    /// Declared capabilities.
    pub capabilities: AgentModelCapabilities,
    /// Bounded, non-secret provider attributes (an API version, a deployment name).
    pub attributes: BTreeMap<String, String>,
}

/// Why a profile record is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentModelProfileError {
    /// The model name exceeds its bound.
    ModelNameTooLong {
        /// Length in bytes.
        bytes: usize,
    },
    /// The base URL is not a plain URL without userinfo or a query string.
    InvalidBaseUrl {
        /// The refused property.
        reason: &'static str,
    },
    /// An attribute key is secret-shaped.
    ForbiddenAttribute {
        /// The key.
        key: String,
    },
    /// An attribute value exceeds its bound.
    AttributeTooLong {
        /// The key.
        key: String,
        /// Length in bytes.
        bytes: usize,
    },
}

impl AgentModelProfileError {
    /// Stable code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidBaseUrl { .. } => "model-profile-invalid-base-url",
            Self::ModelNameTooLong { .. }
            | Self::ForbiddenAttribute { .. }
            | Self::AttributeTooLong { .. } => "model-profile-invalid-attribute",
        }
    }
}

impl Display for AgentModelProfileError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ModelNameTooLong { bytes } => write!(
                f,
                "the model name is {bytes} bytes; at most {AGENT_MODEL_PROFILE_MODEL_MAX_BYTES} are allowed"
            ),
            Self::InvalidBaseUrl { reason } => write!(f, "the base URL is refused: {reason}"),
            Self::ForbiddenAttribute { key } => {
                write!(f, "the attribute key {key} is secret-shaped and never allowed on a profile")
            }
            Self::AttributeTooLong { key, bytes } => write!(
                f,
                "the attribute {key} is {bytes} bytes; at most {AGENT_MODEL_PROFILE_ATTRIBUTE_MAX_BYTES} are allowed"
            ),
        }
    }
}

impl std::error::Error for AgentModelProfileError {}

/// Splits `scheme://authority/rest` and refuses anything that is not that shape.
fn check_base_url(url: &str) -> Result<(), AgentModelProfileError> {
    let Some((scheme, rest)) = url.split_once("://") else {
        return Err(AgentModelProfileError::InvalidBaseUrl {
            reason: "it does not parse as scheme://host",
        });
    };
    if !matches!(scheme, "http" | "https") {
        return Err(AgentModelProfileError::InvalidBaseUrl {
            reason: "the scheme is not http or https",
        });
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    if authority.is_empty() {
        return Err(AgentModelProfileError::InvalidBaseUrl {
            reason: "the host is empty",
        });
    }
    if authority.contains('@') {
        return Err(AgentModelProfileError::InvalidBaseUrl {
            reason: "it carries userinfo",
        });
    }
    if rest.contains('?') || rest.contains('#') {
        return Err(AgentModelProfileError::InvalidBaseUrl {
            reason: "it carries a query string or fragment",
        });
    }
    if url.chars().any(char::is_whitespace) {
        return Err(AgentModelProfileError::InvalidBaseUrl {
            reason: "it carries whitespace",
        });
    }
    Ok(())
}

impl AgentModelProfile {
    /// Refuses a record that could carry a secret or exceed a bound.
    ///
    /// # Errors
    ///
    /// [`AgentModelProfileError`] with its stable code.
    pub fn validate(&self) -> Result<(), AgentModelProfileError> {
        if self.model.is_empty() || self.model.len() > AGENT_MODEL_PROFILE_MODEL_MAX_BYTES {
            return Err(AgentModelProfileError::ModelNameTooLong {
                bytes: self.model.len(),
            });
        }
        if let Some(url) = &self.base_url {
            check_base_url(url)?;
        }
        for (key, value) in &self.attributes {
            let folded = key.to_ascii_lowercase();
            if AGENT_MODEL_PROFILE_FORBIDDEN_ATTRIBUTE_KEYS
                .iter()
                .any(|forbidden| folded == *forbidden || folded.contains(forbidden))
            {
                return Err(AgentModelProfileError::ForbiddenAttribute { key: key.clone() });
            }
            if value.len() > AGENT_MODEL_PROFILE_ATTRIBUTE_MAX_BYTES {
                return Err(AgentModelProfileError::AttributeTooLong {
                    key: key.clone(),
                    bytes: value.len(),
                });
            }
        }
        Ok(())
    }

    /// A digest over the record's canonical serialization: what a dispatch
    /// grant records, and what two nodes projecting one record agree on.
    #[must_use]
    pub fn digest(&self) -> AgentContentDigest {
        let value = serde_json::to_value(self).unwrap_or(serde_json::Value::Null);
        AgentContentDigest::of_json(&value)
    }
}

/// Resolves a profile record on demand.
pub trait AgentModelProfileCatalog: Send + Sync + 'static {
    /// The record behind an approved id, or `None` when the deployment knows no such profile.
    fn profile(&self, id: &AgentModelProfileId) -> Option<AgentModelProfile>;
}

/// An in-memory catalog validated on insert; one implementation among others.
#[derive(Debug, Clone, Default)]
pub struct StaticAgentModelProfileCatalog {
    profiles: BTreeMap<AgentModelProfileId, AgentModelProfile>,
}

impl StaticAgentModelProfileCatalog {
    /// An empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a profile after validating it.
    ///
    /// # Errors
    ///
    /// The profile's own refusal.
    pub fn with_profile(
        mut self,
        profile: AgentModelProfile,
    ) -> Result<Self, AgentModelProfileError> {
        profile.validate()?;
        self.profiles.insert(profile.profile_id.clone(), profile);
        Ok(self)
    }
}

impl AgentModelProfileCatalog for StaticAgentModelProfileCatalog {
    fn profile(&self, id: &AgentModelProfileId) -> Option<AgentModelProfile> {
        self.profiles.get(id).cloned()
    }
}

/// An adapter that selects one of several adapters by the request's profile.
///
/// Every route must stamp the same adapter version as the router declares,
/// because the version is persisted with each turn and an upgrade is an
/// explicit migration ([specification 10.2](../../../docs/plans/rakka-agent/spec.md)):
/// a router mixing versions would write turns no single migration describes.
pub struct AgentModelRouter {
    adapter_version: AgentRevisionNumber,
    routes: BTreeMap<AgentModelProfileId, Arc<dyn AgentModelAdapter>>,
    default: Option<Arc<dyn AgentModelAdapter>>,
}

impl fmt::Debug for AgentModelRouter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentModelRouter")
            .field("adapter_version", &self.adapter_version)
            .field("routes", &self.routes.keys().collect::<Vec<_>>())
            .field("default", &self.default.is_some())
            .finish()
    }
}

impl AgentModelRouter {
    /// A router with no routes, declaring the adapter version every route must share.
    #[must_use]
    pub fn new(adapter_version: AgentRevisionNumber) -> Self {
        Self {
            adapter_version,
            routes: BTreeMap::new(),
            default: None,
        }
    }

    fn check_version(&self, adapter: &Arc<dyn AgentModelAdapter>) -> Result<(), AgentModelError> {
        let version = adapter.adapter_version();
        if version != self.adapter_version {
            return Err(AgentModelError::Refused {
                code: "model-router-adapter-version-mismatch",
                message: format!(
                    "the router stamps adapter version {} and the route stamps {version}",
                    self.adapter_version
                ),
            });
        }
        Ok(())
    }

    /// Routes one profile to an adapter.
    ///
    /// # Errors
    ///
    /// `model-router-adapter-version-mismatch` when the adapter's version differs.
    pub fn with_route(
        mut self,
        profile: AgentModelProfileId,
        adapter: Arc<dyn AgentModelAdapter>,
    ) -> Result<Self, AgentModelError> {
        self.check_version(&adapter)?;
        self.routes.insert(profile, adapter);
        Ok(self)
    }

    /// The adapter an unprofiled request goes to.
    ///
    /// # Errors
    ///
    /// `model-router-adapter-version-mismatch` when the adapter's version differs.
    pub fn with_default(
        mut self,
        adapter: Arc<dyn AgentModelAdapter>,
    ) -> Result<Self, AgentModelError> {
        self.check_version(&adapter)?;
        self.default = Some(adapter);
        Ok(self)
    }

    fn route(
        &self,
        request: &AgentModelRequest,
    ) -> Result<&Arc<dyn AgentModelAdapter>, AgentModelError> {
        match &request.profile {
            Some(profile) => self
                .routes
                .get(profile)
                .ok_or_else(|| AgentModelError::Refused {
                    code: "model-profile-unknown",
                    message: format!("no adapter is routed for the model profile {profile}"),
                }),
            None => self
                .default
                .as_ref()
                .ok_or_else(|| AgentModelError::Refused {
                    code: "model-profile-unknown",
                    message: "the request names no profile and the router has no default adapter"
                        .to_string(),
                }),
        }
    }
}

impl AgentModelAdapter for AgentModelRouter {
    fn adapter_version(&self) -> AgentRevisionNumber {
        self.adapter_version
    }

    fn retry_policy(&self) -> AgentModelRetryPolicy {
        AgentModelRetryPolicy::DEFAULT
    }

    fn call<'a>(&'a self, request: &'a AgentModelRequest) -> AgentModelFuture<'a> {
        Box::pin(async move { self.route(request)?.call(request).await })
    }

    fn call_with<'a>(
        &'a self,
        request: &'a AgentModelRequest,
        credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentModelFuture<'a> {
        Box::pin(async move { self.route(request)?.call_with(request, credential).await })
    }
}
