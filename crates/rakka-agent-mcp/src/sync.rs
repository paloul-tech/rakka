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
//! The connection itself passes the host's
//! [`McpEgressCheck`] first: a sync is the same
//! outbound request, to the same operator-supplied URL, carrying the same
//! resolved credential as a dispatch. A child-process binding is synced by
//! [`sync_mcp_descriptors_over`] instead, over the transport the deployment's
//! launcher produced — the same listing, bounds, and rules, with no URL to
//! judge and no credential to carry.
//!
//! Three bounds and one rule then guard what crosses in:
//!
//! - A listing that has not ended after [`MCP_LIST_PAGES_MAX`] pages is
//!   refused.
//! - An input or output schema over [`MCP_DESCRIPTOR_SCHEMA_MAX_BYTES`] is
//!   refused outright.
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
use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};

use crate::binding::{
    McpRegistrationError, McpServerBinding, McpServerId, McpToolPolicy,
    MCP_DESCRIPTOR_SCHEMA_MAX_BYTES, MCP_LIST_PAGES_MAX,
};
use crate::client::{
    connect, connect_over, service_error, McpClientError, McpClientSession, McpEgressCheck,
};
use crate::launcher::McpChildTransport;

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
    /// SHA-256 digest of [`Self::input_schema`], the staleness signal.
    ///
    /// Cryptographic, not the default FNV fingerprint: this digest is what
    /// [`mcp_descriptor_staleness`] compares, and the value it is taken over
    /// comes from the server being checked. A reshaping server must not be
    /// able to hold `Fresh` by choosing a second schema that collides with the
    /// stored one.
    pub schema_digest: AgentContentDigest,
    /// SHA-256 digest of the server's output schema, when it declared one.
    ///
    /// Cryptographic for the same reason as [`Self::schema_digest`].
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

/// The schema version of the stored [`McpDescriptorSet`] shape this build
/// writes, and the newest it reads.
pub const MCP_DESCRIPTOR_SET_SCHEMA_VERSION: u32 = 1;

/// Everything one publish-time sync of one server produced.
///
/// Release data a deployment stores, so it carries its own
/// [`Self::schema_version`]: a set written by a newer build is refused on
/// decode rather than read as whatever this build's fields make of it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpDescriptorSet {
    /// The stored shape's schema version: [`MCP_DESCRIPTOR_SET_SCHEMA_VERSION`]
    /// as the sync writes it. Decoding refuses a newer one.
    #[serde(deserialize_with = "known_set_schema_version")]
    pub schema_version: u32,
    /// The server this set belongs to.
    pub server_id: McpServerId,
    /// The server's self-reported implementation name at sync time.
    pub server_name: String,
    /// The protocol version the sync negotiated.
    pub protocol_version: String,
    /// When the sync ran.
    pub synced_at: AgentTimestampMillis,
    /// The allow-listed tools, in tool-name order.
    ///
    /// `McpServerBinding::tools` is a `BTreeMap`, so this is its iteration
    /// order — which is what keeps [`Self::digest`] stable when an operator
    /// rewrites the allow-list in a different sequence without changing a
    /// single tool.
    pub descriptors: Vec<McpSyncedDescriptor>,
}

impl McpDescriptorSet {
    /// SHA-256 digest of the shapes this set pinned: every tool's name and its
    /// schema digests, and nothing else.
    ///
    /// Deliberately not over the whole set — [`Self::synced_at`] and the
    /// server's reported name move without the tools moving, and this digest
    /// is what a release compares.
    ///
    /// Cryptographic, not the default FNV fingerprint: the tool names and
    /// schemas underneath it come from the server being checked, and a
    /// reshaping server must not be able to hold an unchanged release digest.
    /// Its inputs are already SHA-256 and its tools are in tool-name order, so
    /// the value is stable against an allow-list the operator reordered.
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
        AgentContentDigest::sha256_of_json(&Value::Array(triples))
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

/// Decodes a descriptor set's schema version, refusing one newer than this
/// build knows.
fn known_set_schema_version<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let version = u32::deserialize(deserializer)?;
    if version > MCP_DESCRIPTOR_SET_SCHEMA_VERSION {
        return Err(DeserializeError::custom(format!(
            "the MCP descriptor set's schema version {version} is newer than this build's \
             {MCP_DESCRIPTOR_SET_SCHEMA_VERSION}"
        )));
    }
    Ok(version)
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
    ///
    /// Borrowed rather than `&'static str`: a
    /// [`McpClientError::Egress`] refusal carries the *host's* own code
    /// through unchanged, so an operator reads back the rule that fired
    /// rather than a code this crate invented for it.
    #[must_use]
    pub fn code(&self) -> &str {
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
/// `egress` is not optional and has no default. A publish-time sync opens the
/// same outbound connection, to the same operator-supplied URL, carrying the
/// same resolved credential as a dispatch, so the host's egress rule decides
/// it on the same terms — and decides it *before* a client exists. A
/// deployment that genuinely reaches anything passes
/// [`McpAllowAllEgress`](crate::McpAllowAllEgress) and so records that the
/// decision was taken. The check judges the configured URL only: `http` must
/// be built with no proxy and no redirects, as [`McpEgressCheck`] describes,
/// for the check to govern where the request actually goes.
///
/// The sync sets no deadline of its own — neither does rmcp, nor an injected
/// `reqwest` client by default — so the caller bounds it: a publish step that
/// awaits it under its own timeout, as a dispatch attempt awaits under the
/// effect's.
///
/// # Errors
///
/// [`McpSyncError`] with its stable code.
pub async fn sync_mcp_descriptors<C>(
    http: &C,
    binding: &McpServerBinding,
    credential: Option<&AgentEphemeralCredential>,
    synced_at: AgentTimestampMillis,
    egress: &dyn McpEgressCheck,
) -> Result<McpDescriptorSet, McpSyncError>
where
    C: StreamableHttpClient + Sync,
{
    // Validation first: an endpoint URL that fails the URL rule is a binding
    // refusal, and an egress rule should never be asked about a URL this
    // adapter would not dial anyway.
    binding.validate()?;
    let server = binding.server_id.to_string();
    // The egress rule fires inside `connect`, before a client exists and
    // before the credential is read; a refusal arrives here as
    // `McpClientError::Egress`, whose code is the host's own.
    let session = connect(http, binding, credential, egress).await?;
    sync_over_session(session, binding, &server, synced_at).await
}

/// Syncs one child-process MCP server's allow-listed tools into a durable
/// descriptor set, over the transport the deployment's launcher produced.
///
/// The same function as [`sync_mcp_descriptors`] from the handshake on — one
/// session, one `tools/list`, the same bounds and hint rule, the session
/// closed on every path — for the binding kind that has no URL to dial. It
/// takes no egress check, because nothing is dialed, and no credential,
/// because a stdio child has no header to carry one.
///
/// # Errors
///
/// [`McpSyncError`] with its stable code; a binding that is not a child
/// process is refused as `mcp-descriptor-sync-failed` without a handshake.
pub async fn sync_mcp_descriptors_over(
    transport: McpChildTransport,
    binding: &McpServerBinding,
    synced_at: AgentTimestampMillis,
) -> Result<McpDescriptorSet, McpSyncError> {
    binding.validate()?;
    let server = binding.server_id.to_string();
    let session = connect_over(transport, binding).await?;
    sync_over_session(session, binding, &server, synced_at).await
}

/// Everything after the handshake that both syncs share: one listing, the
/// session closed, then the answer judged.
async fn sync_over_session(
    session: McpClientSession,
    binding: &McpServerBinding,
    server: &str,
    synced_at: AgentTimestampMillis,
) -> Result<McpDescriptorSet, McpSyncError> {
    let listed = session.list_all_tools().await;
    let protocol_version = session.negotiated_version().as_str().to_string();
    let server_name = session.server_name().to_string();
    // The session is closed before the answer is judged: a refusal must not
    // leave a transport (and its credential-bearing client, or its child
    // process) alive.
    let built = listed
        .map_err(|error| McpSyncError::Client(service_error(server, &error)))
        .and_then(|listed| {
            listed.ok_or_else(|| {
                McpSyncError::Client(McpClientError::Protocol {
                    server: server.to_string(),
                    reason: format!("tools/list did not end within {MCP_LIST_PAGES_MAX} pages"),
                })
            })
        })
        .and_then(|listed| synced_descriptors(binding, server, &listed));
    session.close().await;
    Ok(McpDescriptorSet {
        schema_version: MCP_DESCRIPTOR_SET_SCHEMA_VERSION,
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

/// Turns the server's listing into the allow-listed descriptors.
///
/// `binding.tools` is a `BTreeMap`, so they come out in tool-name order rather
/// than the order the operator wrote them in — which is what makes
/// [`McpDescriptorSet::digest`] invariant under a reordered allow-list.
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
    let output_schema = listed
        .output_schema
        .as_ref()
        .map(|schema| Value::Object((**schema).clone()));
    // Both schemas are bounded: the output schema is digested and pinned just
    // as the input one is, and the server chooses its size too.
    let output_bytes = match &output_schema {
        Some(schema) => encoded_len(schema, server, tool)?,
        None => 0,
    };
    if let Some(bytes) = [bytes, output_bytes]
        .into_iter()
        .find(|bytes| *bytes > MCP_DESCRIPTOR_SCHEMA_MAX_BYTES)
    {
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
    // The server-level credential binding reaches every tool that names
    // none: the dispatcher resolves the *effect's* binding, and the effect
    // takes it from this declaration, so a binding left only on the server
    // would never be resolved at all. `McpServerBinding::validate` has
    // already refused a tool that names a different one.
    let mut declaration = policy.declaration.clone();
    if declaration.credential_binding.is_none() {
        declaration
            .credential_binding
            .clone_from(&binding.credential_binding);
    }
    let mut derived = AgentToolBinding::new(descriptor, declaration, policy.max_attempts);
    if let Some(timeout_ms) = policy.timeout_ms {
        derived = derived.with_timeout_ms(timeout_ms);
    }
    let output_schema_digest = output_schema
        .as_ref()
        .map(AgentContentDigest::sha256_of_json);
    Ok(McpSyncedDescriptor {
        tool: tool.to_string(),
        binding: derived,
        schema_digest: AgentContentDigest::sha256_of_json(&input_schema),
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
/// The rule, for a tool declared `ReadOnly` or `Idempotent` (the two classes
/// that promise a repeat is harmless):
///
/// - `destructiveHint: true` contradicts both: a destructive tool is neither.
/// - `idempotentHint: false` contradicts both: `ReadOnly` claims the tool
///   changes nothing, which a repeat therefore cannot change either, and
///   `Idempotent` claims it outright.
/// - `readOnlyHint: false` contradicts `ReadOnly`.
///
/// A class at or above [`AgentEffectSafetyClass::Reconcileable`] already
/// expects damage, so no hint tells it anything new. And `readOnlyHint: true`
/// silences the other two: in MCP's own semantics `destructiveHint` and
/// `idempotentHint` are meaningful only when `readOnlyHint` is false, and a
/// server that calls a tool read-only has contradicted no class by it.
fn hint_contradiction(
    annotations: Option<&ToolAnnotations>,
    safety: AgentEffectSafetyClass,
) -> Option<&'static str> {
    let annotations = annotations?;
    if annotations.read_only_hint == Some(true) {
        return None;
    }
    let promises_harmless_repeat = matches!(
        safety,
        AgentEffectSafetyClass::ReadOnly | AgentEffectSafetyClass::Idempotent
    );
    if annotations.destructive_hint == Some(true) && promises_harmless_repeat {
        return Some("destructiveHint");
    }
    if annotations.idempotent_hint == Some(false) && promises_harmless_repeat {
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
    fn a_non_idempotent_hint_contradicts_the_read_only_and_idempotent_declarations() {
        let hinted = ToolAnnotations::new().idempotent(false);
        for safety in [
            AgentEffectSafetyClass::ReadOnly,
            AgentEffectSafetyClass::Idempotent,
        ] {
            assert_eq!(
                hint_contradiction(Some(&hinted), safety),
                Some("idempotentHint"),
                "{safety:?}"
            );
        }
        for safety in [
            AgentEffectSafetyClass::Reconcileable,
            AgentEffectSafetyClass::NonIdempotent,
        ] {
            assert_eq!(
                hint_contradiction(Some(&hinted), safety),
                None,
                "{safety:?}"
            );
        }
    }

    #[test]
    fn a_read_only_hint_silences_the_destructive_and_idempotent_hints() {
        let hinted = ToolAnnotations::new()
            .read_only(true)
            .destructive(true)
            .idempotent(false);
        for safety in [
            AgentEffectSafetyClass::ReadOnly,
            AgentEffectSafetyClass::Idempotent,
            AgentEffectSafetyClass::Reconcileable,
            AgentEffectSafetyClass::NonIdempotent,
        ] {
            assert_eq!(
                hint_contradiction(Some(&hinted), safety),
                None,
                "{safety:?}"
            );
        }
        // Without it, the same two hints refuse a `ReadOnly` declaration.
        let unmarked = ToolAnnotations::new().destructive(true).idempotent(false);
        assert_eq!(
            hint_contradiction(Some(&unmarked), AgentEffectSafetyClass::ReadOnly),
            Some("destructiveHint")
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
