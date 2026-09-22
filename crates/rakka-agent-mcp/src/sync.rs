//! `sync_mcp_descriptors`, the stored descriptor set, hint narrowing, and
//! staleness.
//!
//! Descriptors are **release data**: one network round happens here, at
//! publish time, and what it produces is a durable
//! [`McpDescriptorSet`] the caller stores. Nothing downstream — building a
//! registry, dispatching a tool — may call an MCP server to learn a tool's
//! shape, so a drift between the stored set and the server is a decision the
//! operator takes with [`mcp_descriptor_staleness`], never one a dispatch
//! makes for itself.
//!
//! Two bounds and one rule guard what crosses in:
//!
//! - A schema over [`MCP_DESCRIPTOR_SCHEMA_MAX_BYTES`] is refused outright.
//! - A schema over [`AGENT_TOOL_PARAMETERS_MAX_BYTES`] is still synced, but
//!   does not ride the model-visible descriptor; the raw schema comes back for
//!   the caller to store behind an [`ArtifactRef`].
//! - A server-reported hint never *changes* an operator's declaration. When
//!   the binding opts into honoring hints, a hint that contradicts the
//!   declaration refuses the sync; otherwise it is ignored.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_agent::{
    AgentContentDigest, AgentEffectSafetyClass, AgentRevisionNumber, AgentSchemaId, AgentSchemaRef,
    AgentToolBinding, AgentToolDescriptor, AgentToolKind, AGENT_TOOL_DESCRIPTION_MAX_LENGTH,
    AGENT_TOOL_PARAMETERS_MAX_BYTES,
};
use rakka_agent_workflow::{AgentEphemeralCredential, AgentTimestampMillis, ArtifactRef};
use rmcp::model::{Tool, ToolAnnotations};
use rmcp::transport::streamable_http_client::StreamableHttpClient;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::binding::{
    McpRegistrationError, McpServerBinding, McpServerId, McpToolPolicy,
    MCP_DESCRIPTOR_SCHEMA_MAX_BYTES,
};
use crate::client::{connect, service_error, McpClientError};

/// One tool as this adapter stored it at publish time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpSyncedDescriptor {
    /// The server's own tool name, unprefixed.
    pub tool: String,
    /// The Rakka binding derived from the server's tool and the operator's
    /// policy.
    pub binding: AgentToolBinding,
    /// The server's raw input schema, whether or not it rides the descriptor.
    pub input_schema: Value,
    /// Fingerprint of [`Self::input_schema`], the staleness signal.
    pub schema_digest: AgentContentDigest,
    /// Fingerprint of the server's output schema, when it declared one.
    pub output_schema_digest: Option<AgentContentDigest>,
    /// Where the caller stored the raw input schema, when it stored one.
    ///
    /// Always `None` as the sync returns it: the sync holds no artifact store.
    pub input_schema_artifact: Option<ArtifactRef>,
}

impl McpSyncedDescriptor {
    /// Records where the caller stored the raw input schema.
    #[must_use]
    pub fn with_input_schema_artifact(mut self, artifact: ArtifactRef) -> Self {
        self.input_schema_artifact = Some(artifact);
        self
    }

    /// Whether the schema is small enough to ride the model-visible
    /// descriptor.
    #[must_use]
    pub const fn carries_inline_schema(&self) -> bool {
        self.binding.descriptor().parameters.is_some()
    }
}

/// Everything one publish-time sync of one server produced.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpDescriptorSet {
    /// The server this set belongs to.
    pub server_id: McpServerId,
    /// The server's self-reported implementation name at sync time.
    pub server_name: String,
    /// The protocol version the sync negotiated.
    pub protocol_version: String,
    /// When the sync ran.
    pub synced_at: AgentTimestampMillis,
    /// The allow-listed tools, in the binding's own order.
    pub descriptors: Vec<McpSyncedDescriptor>,
}

impl McpDescriptorSet {
    /// Fingerprints the shapes this set pinned: every tool's name and its
    /// schema digests, and nothing else.
    ///
    /// Deliberately not over the whole set — [`Self::synced_at`] and the
    /// server's reported name move without the tools moving, and this digest
    /// is what a release compares.
    #[must_use]
    pub fn digest(&self) -> AgentContentDigest {
        let triples: Vec<Value> = self
            .descriptors
            .iter()
            .map(|descriptor| {
                json!([
                    descriptor.tool,
                    descriptor.schema_digest,
                    descriptor.output_schema_digest,
                ])
            })
            .collect();
        AgentContentDigest::of_json(&Value::Array(triples))
    }

    /// The bindings a registry is built from, with no network call.
    pub fn bindings(&self) -> impl Iterator<Item = &AgentToolBinding> {
        self.descriptors
            .iter()
            .map(|descriptor| &descriptor.binding)
    }

    /// One tool's synced descriptor, by the server's own tool name.
    #[must_use]
    pub fn descriptor(&self, tool: &str) -> Option<&McpSyncedDescriptor> {
        self.descriptors
            .iter()
            .find(|descriptor| descriptor.tool == tool)
    }
}

/// Whether a stored descriptor set still matches what the server exposes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpDescriptorStaleness {
    /// Every tool is present on both sides with the same schema digests.
    Fresh,
    /// The named tools were added, removed, or reshaped.
    Stale {
        /// The tool names that differ, sorted.
        changed: Vec<String>,
    },
}

/// Why a publish-time descriptor sync was refused.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum McpSyncError {
    /// The session could not be established, or could not be trusted.
    Client(McpClientError),
    /// A tool's schema is larger than this adapter will store.
    SchemaTooLarge {
        /// The server the tool belongs to.
        server: String,
        /// The tool whose schema is too large.
        tool: String,
        /// The schema's encoded size.
        bytes: usize,
    },
    /// The server reported a hint the operator's own declaration denies, and
    /// the binding opted into honoring hints.
    HintContradictsDeclaration {
        /// The server that reported the hint.
        server: String,
        /// The tool the hint was reported for.
        tool: String,
        /// The contradicted hint.
        hint: &'static str,
    },
    /// The binding itself is not dispatchable.
    Registration(McpRegistrationError),
    /// The server's tool could not become a Rakka descriptor.
    Descriptor {
        /// The server the tool belongs to.
        server: String,
        /// The tool that could not be described.
        tool: String,
        /// Why. A Rakka-side validation message, never server text.
        reason: String,
    },
}

impl McpSyncError {
    /// Stable, machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Client(error) => match error {
                // A transport or protocol failure *of the sync* is one fact to
                // the operator — the descriptors could not be refreshed — so
                // both collapse onto one code here, while the executor keeps
                // the finer client codes for a dispatch attempt.
                McpClientError::Transport { .. } | McpClientError::Protocol { .. } => {
                    "mcp-descriptor-sync-failed"
                }
                _ => error.code(),
            },
            Self::SchemaTooLarge { .. } => "mcp-descriptor-schema-too-large",
            Self::HintContradictsDeclaration { .. } => "mcp-hint-contradicts-declaration",
            Self::Registration(error) => error.code(),
            Self::Descriptor { .. } => "mcp-binding-invalid",
        }
    }
}

impl Display for McpSyncError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(error) => Display::fmt(error, f),
            Self::SchemaTooLarge {
                server,
                tool,
                bytes,
            } => write!(
                f,
                "the MCP server {server}'s tool {tool} schema is {bytes} bytes, over the \
                 {MCP_DESCRIPTOR_SCHEMA_MAX_BYTES}-byte limit"
            ),
            Self::HintContradictsDeclaration { server, tool, hint } => write!(
                f,
                "the MCP server {server}'s tool {tool} hint {hint:?} contradicts its declaration"
            ),
            Self::Registration(error) => Display::fmt(error, f),
            Self::Descriptor {
                server,
                tool,
                reason,
            } => write!(
                f,
                "the MCP server {server}'s tool {tool} could not be described: {reason}"
            ),
        }
    }
}

impl Error for McpSyncError {}

impl From<McpClientError> for McpSyncError {
    fn from(error: McpClientError) -> Self {
        Self::Client(error)
    }
}

impl From<McpRegistrationError> for McpSyncError {
    fn from(error: McpRegistrationError) -> Self {
        Self::Registration(error)
    }
}

/// Syncs one MCP server's allow-listed tools into a durable descriptor set.
///
/// One session, one `tools/list`, then the session is closed — including on
/// every refusal raised after it opened.
///
/// # Errors
///
/// [`McpSyncError`] with its stable code.
pub async fn sync_mcp_descriptors<C>(
    http: &C,
    binding: &McpServerBinding,
    credential: Option<&AgentEphemeralCredential>,
    synced_at: AgentTimestampMillis,
) -> Result<McpDescriptorSet, McpSyncError>
where
    C: StreamableHttpClient + Sync,
{
    binding.validate()?;
    let server = binding.server_id.to_string();
    let session = connect(http, binding, credential).await?;
    let listed = session.peer().list_all_tools().await;
    let protocol_version = session.negotiated_version().as_str().to_string();
    let server_name = session.server_name().to_string();
    // The session is closed before the answer is judged: a refusal must not
    // leave a transport (and its credential-bearing client) alive.
    let built = listed
        .map_err(|error| McpSyncError::Client(service_error(&server, &error)))
        .and_then(|listed| synced_descriptors(binding, &server, &listed));
    session.close().await;
    Ok(McpDescriptorSet {
        server_id: binding.server_id.clone(),
        server_name,
        protocol_version,
        synced_at,
        descriptors: built?,
    })
}

/// Reports which tools a stored set and a fresh one disagree about: added,
/// removed, or reshaped.
#[must_use]
pub fn mcp_descriptor_staleness(
    stored: &McpDescriptorSet,
    fresh: &McpDescriptorSet,
) -> McpDescriptorStaleness {
    let stored_shapes = shapes(stored);
    let fresh_shapes = shapes(fresh);
    let mut changed: Vec<String> = stored_shapes
        .keys()
        .chain(fresh_shapes.keys())
        .filter(|tool| stored_shapes.get(*tool) != fresh_shapes.get(*tool))
        .map(|tool| (*tool).to_string())
        .collect();
    changed.sort();
    changed.dedup();
    if changed.is_empty() {
        McpDescriptorStaleness::Fresh
    } else {
        McpDescriptorStaleness::Stale { changed }
    }
}

/// The comparable shape of every tool in a set: the two schema digests.
fn shapes(
    set: &McpDescriptorSet,
) -> BTreeMap<&str, (&AgentContentDigest, Option<&AgentContentDigest>)> {
    set.descriptors
        .iter()
        .map(|descriptor| {
            (
                descriptor.tool.as_str(),
                (
                    &descriptor.schema_digest,
                    descriptor.output_schema_digest.as_ref(),
                ),
            )
        })
        .collect()
}

/// Turns the server's listing into the allow-listed descriptors, in the
/// binding's own order.
fn synced_descriptors(
    binding: &McpServerBinding,
    server: &str,
    listed: &[Tool],
) -> Result<Vec<McpSyncedDescriptor>, McpSyncError> {
    binding
        .tools
        .iter()
        .map(|(tool, policy)| {
            let found = listed
                .iter()
                .find(|candidate| candidate.name == tool.as_str())
                .ok_or_else(|| McpSyncError::Descriptor {
                    server: server.to_string(),
                    tool: tool.clone(),
                    reason: "the server does not list it".to_string(),
                })?;
            synced_descriptor(binding, server, tool, policy, found)
        })
        .collect()
}

/// One allow-listed tool: the bounds, the hint rule, and the derived binding.
fn synced_descriptor(
    binding: &McpServerBinding,
    server: &str,
    tool: &str,
    policy: &McpToolPolicy,
    listed: &Tool,
) -> Result<McpSyncedDescriptor, McpSyncError> {
    let input_schema = Value::Object((*listed.input_schema).clone());
    let bytes = encoded_len(&input_schema, server, tool)?;
    if bytes > MCP_DESCRIPTOR_SCHEMA_MAX_BYTES {
        return Err(McpSyncError::SchemaTooLarge {
            server: server.to_string(),
            tool: tool.to_string(),
            bytes,
        });
    }
    if policy.honor_hints {
        if let Some(hint) =
            hint_contradiction(listed.annotations.as_ref(), policy.declaration.safety)
        {
            return Err(McpSyncError::HintContradictsDeclaration {
                server: server.to_string(),
                tool: tool.to_string(),
                hint,
            });
        }
    }
    let described = |error: rakka_agent::AgentToolError| McpSyncError::Descriptor {
        server: server.to_string(),
        tool: tool.to_string(),
        reason: error.to_string(),
    };
    let fallback = format!("MCP tool {tool} on {server}");
    let description = listed
        .description
        .as_deref()
        .filter(|description| !description.is_empty())
        .unwrap_or(&fallback);
    let mut descriptor = AgentToolDescriptor::new(
        binding.tool_id(tool)?,
        AgentToolKind::RemoteMcp,
        bounded_description(description),
        schema_ref(server, tool, "input")?,
        schema_ref(server, tool, "output")?,
    )
    .map_err(described)?
    .with_result_behavior(policy.result_behavior);
    if bytes <= AGENT_TOOL_PARAMETERS_MAX_BYTES {
        descriptor = descriptor
            .with_parameters(input_schema.clone())
            .map_err(described)?;
    }
    let mut derived =
        AgentToolBinding::new(descriptor, policy.declaration.clone(), policy.max_attempts);
    if let Some(timeout_ms) = policy.timeout_ms {
        derived = derived.with_timeout_ms(timeout_ms);
    }
    let output_schema_digest = listed
        .output_schema
        .as_ref()
        .map(|schema| AgentContentDigest::of_json(&Value::Object((**schema).clone())));
    Ok(McpSyncedDescriptor {
        tool: tool.to_string(),
        binding: derived,
        schema_digest: AgentContentDigest::of_json(&input_schema),
        input_schema,
        output_schema_digest,
        input_schema_artifact: None,
    })
}

/// The schema's encoded size. A schema that cannot even be re-encoded is a
/// descriptor refusal, not a panic.
fn encoded_len(schema: &Value, server: &str, tool: &str) -> Result<usize, McpSyncError> {
    serde_json::to_vec(schema)
        .map(|encoded| encoded.len())
        .map_err(|error| McpSyncError::Descriptor {
            server: server.to_string(),
            tool: tool.to_string(),
            reason: error.to_string(),
        })
}

/// The versioned schema reference a synced tool's input or output points at.
fn schema_ref(server: &str, tool: &str, side: &str) -> Result<AgentSchemaRef, McpSyncError> {
    let schema_id = AgentSchemaId::new(format!("mcp.{server}.{tool}.{side}")).map_err(|error| {
        McpSyncError::Descriptor {
            server: server.to_string(),
            tool: tool.to_string(),
            reason: error.to_string(),
        }
    })?;
    Ok(AgentSchemaRef::new(schema_id, AgentRevisionNumber::INITIAL))
}

/// Truncates a server-supplied description to the descriptor bound, at a
/// character boundary: the server's text is untrusted input, and a descriptor
/// that exceeds the bound would be refused wholesale instead.
fn bounded_description(description: &str) -> String {
    if description.len() <= AGENT_TOOL_DESCRIPTION_MAX_LENGTH {
        return description.to_string();
    }
    let mut end = AGENT_TOOL_DESCRIPTION_MAX_LENGTH;
    while end > 0 && !description.is_char_boundary(end) {
        end -= 1;
    }
    description[..end].to_string()
}

/// Which server hint, if any, the operator's declared safety class denies.
///
/// A hint never narrows or widens a declaration — the operator's class is the
/// authority — so the only thing a contradiction can do is refuse the sync.
/// `destructiveHint` contradicts every class below [`AgentEffectSafetyClass::Reconcileable`]:
/// `ReadOnly` claims the tool changes nothing and `Idempotent` claims a repeat
/// is harmless, and a destructive tool denies both. A class at or above
/// `Reconcileable` already expects damage, so the hint tells it nothing new.
fn hint_contradiction(
    annotations: Option<&ToolAnnotations>,
    safety: AgentEffectSafetyClass,
) -> Option<&'static str> {
    let annotations = annotations?;
    if annotations.destructive_hint == Some(true)
        && safety.strictness() < AgentEffectSafetyClass::Reconcileable.strictness()
    {
        return Some("destructiveHint");
    }
    if annotations.idempotent_hint == Some(false) && safety == AgentEffectSafetyClass::Idempotent {
        return Some("idempotentHint");
    }
    if annotations.read_only_hint == Some(false) && safety == AgentEffectSafetyClass::ReadOnly {
        return Some("readOnlyHint");
    }
    None
}

#[cfg(test)]
mod tests {
    use rakka_agent::AgentEffectSafetyClass;
    use rmcp::model::ToolAnnotations;

    use super::{bounded_description, hint_contradiction};
    use rakka_agent::AGENT_TOOL_DESCRIPTION_MAX_LENGTH;

    #[test]
    fn no_annotations_can_contradict_nothing() {
        assert_eq!(
            hint_contradiction(None, AgentEffectSafetyClass::ReadOnly),
            None
        );
    }

    #[test]
    fn a_destructive_hint_contradicts_read_only_and_idempotent_only() {
        let destructive = ToolAnnotations::new().destructive(true);
        for safety in [
            AgentEffectSafetyClass::ReadOnly,
            AgentEffectSafetyClass::Idempotent,
        ] {
            assert_eq!(
                hint_contradiction(Some(&destructive), safety),
                Some("destructiveHint"),
                "{safety:?}"
            );
        }
        for safety in [
            AgentEffectSafetyClass::Reconcileable,
            AgentEffectSafetyClass::NonIdempotent,
        ] {
            assert_eq!(
                hint_contradiction(Some(&destructive), safety),
                None,
                "{safety:?}"
            );
        }
    }

    #[test]
    fn a_non_idempotent_hint_contradicts_only_the_idempotent_declaration() {
        let hinted = ToolAnnotations::new().idempotent(false);
        assert_eq!(
            hint_contradiction(Some(&hinted), AgentEffectSafetyClass::Idempotent),
            Some("idempotentHint")
        );
        assert_eq!(
            hint_contradiction(Some(&hinted), AgentEffectSafetyClass::NonIdempotent),
            None
        );
    }

    #[test]
    fn a_mutating_hint_contradicts_only_the_read_only_declaration() {
        let hinted = ToolAnnotations::new().read_only(false);
        assert_eq!(
            hint_contradiction(Some(&hinted), AgentEffectSafetyClass::ReadOnly),
            Some("readOnlyHint")
        );
        assert_eq!(
            hint_contradiction(Some(&hinted), AgentEffectSafetyClass::Idempotent),
            None
        );
    }

    #[test]
    fn a_description_is_truncated_at_a_character_boundary() {
        let short = "a short description";
        assert_eq!(bounded_description(short), short);
        let wide = "é".repeat(AGENT_TOOL_DESCRIPTION_MAX_LENGTH);
        let bounded = bounded_description(&wide);
        assert!(bounded.len() <= AGENT_TOOL_DESCRIPTION_MAX_LENGTH);
        assert!(wide.starts_with(&bounded));
        assert_eq!(
            bounded.chars().count(),
            AGENT_TOOL_DESCRIPTION_MAX_LENGTH / 2
        );
    }
}
