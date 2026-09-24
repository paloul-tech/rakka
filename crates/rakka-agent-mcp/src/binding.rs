//! Server ids, transports, tool policies, the binding record, and its
//! refusals: every type an operator uses to name and constrain one MCP
//! server.
//!
//! A binding is release-shaped data, never a secret: [`McpServerBinding`]
//! carries only a logical [`AgentCredentialBindingRef`], which the dispatcher
//! resolves inside an attempt, never here.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_agent::identity::validate_identity_segment;
use rakka_agent::{
    AgentCredentialBindingRef, AgentToolDeclaration, AgentToolId, AgentToolResultBehavior,
};
use rakka_agent_workflow::ArtifactRef;
use rmcp::model::ProtocolVersion;
use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer, Serialize};

/// The protocol versions this adapter negotiates when a binding does not
/// declare its own, newest first.
///
/// Every entry is one the pinned rmcp SDK knows; [`McpServerBinding::validate`]
/// holds that property for an operator-supplied list too.
pub const MCP_DEFAULT_PROTOCOL_VERSIONS: [&str; 2] = ["2026-07-28", "2025-11-25"];

/// Largest a synced tool descriptor's JSON schema may be, in bytes.
pub const MCP_DESCRIPTOR_SCHEMA_MAX_BYTES: usize = 64 * 1024;

/// Largest an inline-bounded tool result may be, in bytes.
pub const MCP_INLINE_RESULT_MAX_BYTES: usize = 2 * 1024;

/// Largest a mapped tool error's detail text may be, in bytes.
pub const MCP_TOOL_ERROR_DETAIL_MAX_BYTES: usize = 512;

/// Default time-to-live, in milliseconds, before a synced descriptor set is
/// considered stale and due for a recheck.
pub const MCP_DESCRIPTOR_RECHECK_TTL_DEFAULT_MS: u64 = 60_000;

/// The client name this adapter identifies itself with during MCP
/// initialization.
pub const MCP_CLIENT_NAME: &str = "rakka-agent-mcp";

/// Prefix an MCP server's self-reported name must not carry: a server that
/// identifies itself as a Rakka agent is refused (specification 14.4 of
/// `docs/plans/rakka-agent/spec.md`; MCP is never an agent-to-agent channel).
pub const MCP_PEER_AGENT_SERVER_PREFIX: &str = "rakka-agent";

/// Metadata key an effect's idempotency key rides under when it is forwarded
/// to an MCP tool call.
pub const MCP_META_IDEMPOTENCY_KEY: &str = "io.rakka.idempotency-key";

/// Prefix every derived MCP tool id carries: `mcp.<server>.<tool>`.
pub const MCP_TOOL_ID_PREFIX: &str = "mcp.";

/// Stable identity of one configured MCP server.
///
/// A value that passes `rakka_agent::identity::validate_identity_segment` and
/// additionally refuses a `.`. A tool name may itself contain dots (MCP
/// servers commonly name tools `a.b`), so a derived tool id does not split
/// into parts; what the server id's own `.`-refusal buys instead is that
/// `mcp.<server>.` is always a stable, unambiguous prefix every one of that
/// server's derived tool ids shares, which is what the sync and executor
/// route on.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct McpServerId(String);

impl McpServerId {
    /// Creates the server id, rejecting a value that cannot key a binding or
    /// that would make the server's `mcp.<server>.` tool-id prefix ambiguous.
    ///
    /// # Errors
    ///
    /// [`McpRegistrationError::InvalidServerId`], stable code
    /// `mcp-binding-invalid`.
    pub fn new(value: impl Into<String>) -> Result<Self, McpRegistrationError> {
        let value = value.into();
        validate_identity_segment("mcp_server_id", &value).map_err(|error| {
            McpRegistrationError::InvalidServerId {
                reason: error.to_string(),
            }
        })?;
        if value.contains('.') {
            return Err(McpRegistrationError::InvalidServerId {
                reason: "must not contain '.': the server's mcp.<server>. tool-id prefix would not be stable and unambiguous".to_string(),
            });
        }
        Ok(Self(value))
    }

    /// Returns the server id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for McpServerId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<&str> for McpServerId {
    type Error = McpRegistrationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for McpServerId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(DeserializeError::custom)
    }
}

/// How this adapter reaches one MCP server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum McpTransport {
    /// The Streamable HTTP transport: rmcp's client over its own `reqwest`.
    StreamableHttp {
        /// The server's MCP endpoint URL.
        url: String,
    },
    /// A stdio child process, launched through `McpChildProcessLauncher`
    /// (feature `child-process`).
    ChildProcess {
        /// Durable reference to the process launch specification. Boxed:
        /// [`ArtifactRef`] is much larger than [`McpTransport::StreamableHttp`]'s
        /// `url`, and clippy's `large_enum_variant` flags the unboxed form.
        spec_ref: Box<ArtifactRef>,
    },
}

/// Operator-declared dispatch policy for one tool an MCP server exposes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpToolPolicy {
    /// The safety class, capabilities, and credential/environment
    /// declaration this policy authorizes for the tool.
    pub declaration: AgentToolDeclaration,
    /// Maximum dispatch attempts. `0` is a policy [`McpServerBinding::validate`]
    /// refuses; this field is not itself gated at construction, since it is
    /// `pub` and also reachable by decoding.
    pub max_attempts: u32,
    /// Per-attempt timeout, in milliseconds; `None` defers to the
    /// dispatcher's own default.
    pub timeout_ms: Option<u64>,
    /// How the tool's result is bounded.
    pub result_behavior: AgentToolResultBehavior,
    /// Whether the dispatcher honors the server's own annotations/hints for
    /// this tool, rather than trusting only the operator's declaration.
    pub honor_hints: bool,
}

impl McpToolPolicy {
    /// Declares a policy for one dispatch attempt, inline-bounded, with the
    /// server's own hints ignored.
    #[must_use]
    pub fn new(declaration: AgentToolDeclaration) -> Self {
        Self {
            declaration,
            max_attempts: 1,
            timeout_ms: None,
            result_behavior: AgentToolResultBehavior::InlineBounded,
            honor_hints: false,
        }
    }

    /// Sets the maximum dispatch attempts. Stored as given: `0` is only
    /// refused later, by [`McpServerBinding::validate`].
    #[must_use]
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Sets a per-attempt timeout.
    #[must_use]
    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    /// Sets how the tool's result is bounded.
    #[must_use]
    pub fn with_result_behavior(mut self, result_behavior: AgentToolResultBehavior) -> Self {
        self.result_behavior = result_behavior;
        self
    }

    /// Sets whether the dispatcher honors the server's own hints for this
    /// tool.
    #[must_use]
    pub fn with_honor_hints(mut self, honor_hints: bool) -> Self {
        self.honor_hints = honor_hints;
        self
    }
}

/// When a server's synced tool descriptor set is rechecked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case")]
pub enum McpDescriptorRefresh {
    /// Only an operator-triggered resync.
    #[default]
    Manual,
    /// Rechecked at least this often.
    Interval {
        /// Recheck interval, in milliseconds.
        millis: u64,
    },
}

/// One MCP server's binding: identity, transport, logical credential,
/// negotiated protocol versions, and the allow-list of tools an operator has
/// declared safety and policy for.
///
/// Never carries a resolved secret: [`Self::credential_binding`] is a logical
/// reference the dispatcher resolves inside an attempt, never a value stored
/// here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpServerBinding {
    /// The server's identity.
    pub server_id: McpServerId,
    /// How this adapter reaches the server.
    pub transport: McpTransport,
    /// Logical credential binding a dispatch attempt may resolve.
    pub credential_binding: Option<AgentCredentialBindingRef>,
    /// Protocol versions this binding negotiates, newest first.
    #[serde(default)]
    pub protocol_versions: Vec<String>,
    /// The allow-list of tools this server exposes, keyed by the server's own
    /// tool name.
    pub tools: BTreeMap<String, McpToolPolicy>,
    /// When the synced descriptor set is rechecked.
    #[serde(default)]
    pub refresh: McpDescriptorRefresh,
}

impl McpServerBinding {
    /// Starts a binding over the Streamable HTTP transport, with the default
    /// protocol versions, manual refresh, and no tools yet.
    #[must_use]
    pub fn streamable_http(server_id: McpServerId, url: impl Into<String>) -> Self {
        Self {
            server_id,
            transport: McpTransport::StreamableHttp { url: url.into() },
            credential_binding: None,
            protocol_versions: MCP_DEFAULT_PROTOCOL_VERSIONS.map(str::to_string).to_vec(),
            tools: BTreeMap::new(),
            refresh: McpDescriptorRefresh::Manual,
        }
    }

    /// Starts a binding over the child-process transport, with the default
    /// protocol versions, manual refresh, and no tools yet.
    #[must_use]
    pub fn child_process(server_id: McpServerId, spec_ref: ArtifactRef) -> Self {
        Self {
            server_id,
            transport: McpTransport::ChildProcess {
                spec_ref: Box::new(spec_ref),
            },
            credential_binding: None,
            protocol_versions: MCP_DEFAULT_PROTOCOL_VERSIONS.map(str::to_string).to_vec(),
            tools: BTreeMap::new(),
            refresh: McpDescriptorRefresh::Manual,
        }
    }

    /// Binds the server to a logical credential reference.
    #[must_use]
    pub fn with_credential_binding(mut self, binding: AgentCredentialBindingRef) -> Self {
        self.credential_binding = Some(binding);
        self
    }

    /// Replaces the negotiated protocol versions.
    #[must_use]
    pub fn with_protocol_versions(mut self, protocol_versions: Vec<String>) -> Self {
        self.protocol_versions = protocol_versions;
        self
    }

    /// Sets when the synced descriptor set is rechecked.
    #[must_use]
    pub fn with_refresh(mut self, refresh: McpDescriptorRefresh) -> Self {
        self.refresh = refresh;
        self
    }

    /// Adds one tool to the allow-list, rejecting a name whose derived id is
    /// invalid.
    ///
    /// # Errors
    ///
    /// [`McpRegistrationError::ToolNameInvalid`], stable code
    /// `mcp-binding-invalid`.
    pub fn with_tool(
        mut self,
        tool: impl Into<String>,
        policy: McpToolPolicy,
    ) -> Result<Self, McpRegistrationError> {
        let tool = tool.into();
        self.tool_id(&tool)?;
        self.tools.insert(tool, policy);
        Ok(self)
    }

    /// The server's endpoint URL, when the transport carries one.
    #[must_use]
    pub fn url(&self) -> Option<&str> {
        match &self.transport {
            McpTransport::StreamableHttp { url } => Some(url.as_str()),
            McpTransport::ChildProcess { .. } => None,
        }
    }

    /// Derives the stable [`AgentToolId`] for one of this server's tools:
    /// `mcp.<server>.<tool>`.
    ///
    /// # Errors
    ///
    /// [`McpRegistrationError::ToolNameInvalid`], stable code
    /// `mcp-binding-invalid`.
    pub fn tool_id(&self, tool: &str) -> Result<AgentToolId, McpRegistrationError> {
        let server = &self.server_id;
        let candidate = format!("{MCP_TOOL_ID_PREFIX}{server}.{tool}");
        AgentToolId::new(candidate).map_err(|error| McpRegistrationError::ToolNameInvalid {
            server: self.server_id.to_string(),
            tool: tool.to_string(),
            reason: error.to_string(),
        })
    }

    /// Refuses a binding that cannot be dispatched: an invalid endpoint URL,
    /// a child-process binding that names a credential binding (on the server
    /// or in any tool's declaration), an empty tool allow-list, a tool whose
    /// derived id is invalid, a tool policy with `max_attempts` of `0`, or an
    /// empty or malformed protocol version list.
    ///
    /// # Errors
    ///
    /// [`McpRegistrationError`] with its stable code.
    pub fn validate(&self) -> Result<(), McpRegistrationError> {
        match &self.transport {
            McpTransport::StreamableHttp { url } => {
                check_streamable_http_url(url).map_err(|reason| {
                    McpRegistrationError::InvalidUrl {
                        server: self.server_id.to_string(),
                        reason,
                    }
                })?;
            }
            // Refused here, at the operator's gate, rather than on every
            // attempt: a binding that names a credential would have the
            // dispatcher resolve a real secret lease per attempt only for the
            // executor to refuse it unread.
            McpTransport::ChildProcess { .. } => {
                let names_credential = self.credential_binding.is_some()
                    || self
                        .tools
                        .values()
                        .any(|policy| policy.declaration.credential_binding.is_some());
                if names_credential {
                    return Err(McpRegistrationError::CredentialOnChildProcess {
                        server: self.server_id.to_string(),
                    });
                }
            }
        }
        if self.tools.is_empty() {
            return Err(McpRegistrationError::NoTools {
                server: self.server_id.to_string(),
            });
        }
        for (tool, policy) in &self.tools {
            self.tool_id(tool)?;
            if policy.max_attempts == 0 {
                return Err(McpRegistrationError::MaxAttemptsInvalid {
                    server: self.server_id.to_string(),
                    tool: tool.clone(),
                });
            }
        }
        if self.protocol_versions.is_empty() {
            return Err(McpRegistrationError::ProtocolVersionsEmpty {
                server: self.server_id.to_string(),
            });
        }
        for version in &self.protocol_versions {
            if !is_protocol_version_shaped(version) {
                return Err(McpRegistrationError::ProtocolVersionInvalid {
                    server: self.server_id.to_string(),
                    value: version.clone(),
                });
            }
            // Well-shaped is not enough: a version this SDK cannot name is one
            // `connect` could never offer, so it is a misconfigured binding —
            // named here, at the operator's own gate — rather than a sync that
            // reads back as a network failure later.
            if !is_known_protocol_version(version) {
                return Err(McpRegistrationError::ProtocolVersionUnknown {
                    server: self.server_id.to_string(),
                    value: version.clone(),
                });
            }
        }
        Ok(())
    }
}

/// Why a binding, server id, or tool registration is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum McpRegistrationError {
    /// The server's endpoint URL fails the URL rule.
    InvalidUrl {
        /// The server the URL belongs to.
        server: String,
        /// Why the URL was refused. Never the URL itself: the message never
        /// repeats a query string.
        reason: &'static str,
    },
    /// A tool's derived id is not a valid identity segment.
    ToolNameInvalid {
        /// The server the tool belongs to.
        server: String,
        /// The tool name that was refused.
        tool: String,
        /// Why the derived id was refused.
        reason: String,
    },
    /// The server id is already registered.
    DuplicateServer {
        /// The server that was already registered.
        server: String,
    },
    /// No descriptor set has been synced for this server yet.
    DescriptorSetMissing {
        /// The server with no synced descriptor set.
        server: String,
    },
    /// No binding is registered for this server.
    BindingMissing {
        /// The server with no registered binding.
        server: String,
    },
    /// A tool the binding allow-lists has no descriptor in the server's
    /// synced set: the tool is named validly, but no schema was pinned for it,
    /// so no attempt could ever recheck it.
    DescriptorMissing {
        /// The server the tool belongs to.
        server: String,
        /// The allow-listed tool with no synced descriptor.
        tool: String,
    },
    /// The binding's transport is not enabled in this build.
    TransportUnsupported {
        /// The server whose transport is unsupported.
        server: String,
    },
    /// A child-process binding names a credential binding — on the server,
    /// or in one of its tools' declarations. A stdio child has no request to
    /// carry a credential, so a secret resolved for it on every attempt could
    /// only ever be refused; credentials reach MCP servers over the Streamable
    /// HTTP transport alone.
    CredentialOnChildProcess {
        /// The child-process server that names a credential.
        server: String,
    },
    /// A server-reported hint contradicts the operator's own tool
    /// declaration.
    HintContradictsDeclaration {
        /// The server that reported the hint.
        server: String,
        /// The tool the hint was reported for.
        tool: String,
        /// The contradicted hint.
        hint: &'static str,
    },
    /// The server identifies itself as a Rakka agent. MCP is never an
    /// agent-to-agent channel (specification 14.4 of
    /// `docs/plans/rakka-agent/spec.md`).
    PeerAgentChannel {
        /// The server that identified as a Rakka agent.
        server: String,
        /// The self-reported name that triggered the refusal.
        name: String,
    },
    /// The binding declares no protocol versions.
    ProtocolVersionsEmpty {
        /// The server with no declared protocol versions.
        server: String,
    },
    /// A declared protocol version is well-shaped but is not one the pinned
    /// rmcp SDK knows, so no session could ever negotiate it.
    ProtocolVersionUnknown {
        /// The server that declared the unknown version.
        server: String,
        /// The unknown value.
        value: String,
    },
    /// A declared protocol version is not a `YYYY-MM-DD` string of exactly
    /// ten ASCII bytes.
    ProtocolVersionInvalid {
        /// The server that declared the malformed version.
        server: String,
        /// The malformed value.
        value: String,
    },
    /// The binding lists no tools.
    NoTools {
        /// The server whose binding lists no tools.
        server: String,
    },
    /// A tool's policy declares `max_attempts` of `0`: no attempt could ever
    /// be dispatched.
    MaxAttemptsInvalid {
        /// The server the tool belongs to.
        server: String,
        /// The tool whose policy declares zero attempts.
        tool: String,
    },
    /// The server id itself is invalid.
    InvalidServerId {
        /// Why the server id was refused.
        reason: String,
    },
}

impl McpRegistrationError {
    /// Stable, machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::TransportUnsupported { .. } => "mcp-transport-unsupported",
            Self::HintContradictsDeclaration { .. } => "mcp-hint-contradicts-declaration",
            Self::PeerAgentChannel { .. } => "mcp-peer-agent-channel-refused",
            Self::InvalidUrl { .. }
            | Self::ToolNameInvalid { .. }
            | Self::DuplicateServer { .. }
            | Self::DescriptorSetMissing { .. }
            | Self::BindingMissing { .. }
            | Self::DescriptorMissing { .. }
            | Self::ProtocolVersionsEmpty { .. }
            | Self::ProtocolVersionInvalid { .. }
            | Self::ProtocolVersionUnknown { .. }
            | Self::NoTools { .. }
            | Self::MaxAttemptsInvalid { .. }
            | Self::CredentialOnChildProcess { .. }
            | Self::InvalidServerId { .. } => "mcp-binding-invalid",
        }
    }
}

impl Display for McpRegistrationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUrl { server, reason } => {
                write!(f, "the MCP server {server}'s endpoint URL is refused: {reason}")
            }
            Self::ToolNameInvalid {
                server,
                tool,
                reason,
            } => write!(
                f,
                "the MCP server {server}'s tool {tool} name is invalid: {reason}"
            ),
            Self::DuplicateServer { server } => {
                write!(f, "the MCP server {server} is already registered")
            }
            Self::DescriptorSetMissing { server } => {
                write!(f, "the MCP server {server} has no synced descriptor set")
            }
            Self::BindingMissing { server } => {
                write!(f, "no MCP binding is registered for server {server}")
            }
            Self::DescriptorMissing { server, tool } => write!(
                f,
                "the MCP server {server}'s tool {tool} has no synced descriptor; re-sync the \
                 server before binding it"
            ),
            Self::TransportUnsupported { server } => write!(
                f,
                "the MCP server {server}'s transport is not enabled in this build"
            ),
            Self::CredentialOnChildProcess { server } => write!(
                f,
                "the MCP server {server} is a child process and names a credential binding; \
                 a child process has no header to carry one"
            ),
            Self::HintContradictsDeclaration { server, tool, hint } => write!(
                f,
                "the MCP server {server}'s tool {tool} hint {hint:?} contradicts its declaration"
            ),
            Self::PeerAgentChannel { server, name } => write!(
                f,
                "the MCP server {server} identifies as a Rakka agent ({name}); MCP is never an agent-to-agent channel"
            ),
            Self::ProtocolVersionsEmpty { server } => {
                write!(f, "the MCP server {server} declares no protocol versions")
            }
            Self::ProtocolVersionInvalid { server, value } => write!(
                f,
                "the MCP server {server} declares an invalid protocol version {value:?}"
            ),
            Self::ProtocolVersionUnknown { server, value } => write!(
                f,
                "the MCP server {server} declares the protocol version {value:?}, \
                 which this build's MCP SDK does not know"
            ),
            Self::NoTools { server } => {
                write!(f, "the MCP server {server} binding lists no tools")
            }
            Self::MaxAttemptsInvalid { server, tool } => write!(
                f,
                "the MCP server {server}'s tool {tool} policy declares 0 max_attempts; no attempt could be dispatched"
            ),
            Self::InvalidServerId { reason } => {
                write!(f, "the MCP server id is invalid: {reason}")
            }
        }
    }
}

impl Error for McpRegistrationError {}

/// Checks the MCP endpoint URL rule: scheme `http` or `https`, a non-empty
/// host, no userinfo, and no fragment. A query string is allowed: MCP
/// endpoints may carry one.
///
/// Mirrors the hand-rolled check shape `rakka_agent::model_profile` uses for
/// a model profile's base URL, loosened to allow a query string.
fn check_streamable_http_url(url: &str) -> Result<(), &'static str> {
    let Some((scheme, rest)) = url.split_once("://") else {
        return Err("it does not parse as scheme://host");
    };
    if !matches!(scheme, "http" | "https") {
        return Err("the scheme is not http or https");
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    if authority.is_empty() {
        return Err("the host is empty");
    }
    if authority.contains('@') {
        return Err("it carries userinfo");
    }
    if rest.contains('#') {
        return Err("it carries a fragment");
    }
    if url.chars().any(char::is_whitespace) {
        return Err("it carries whitespace");
    }
    Ok(())
}

/// Whether `value` names a protocol version the pinned rmcp SDK knows.
///
/// [`ProtocolVersion`] has no public constructor from a string, so membership
/// in [`ProtocolVersion::KNOWN_VERSIONS`] (`rmcp-3.4.0/src/model.rs:174`) is
/// what decides it; a version outside that list is one `client::connect` could
/// never put on the wire.
fn is_known_protocol_version(value: &str) -> bool {
    ProtocolVersion::KNOWN_VERSIONS
        .iter()
        .any(|known| known.as_str() == value)
}

/// Whether `value` is exactly ten ASCII bytes shaped `YYYY-MM-DD`.
fn is_protocol_version_shaped(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 10
        && bytes[0..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[7] == b'-'
        && bytes[8..10].iter().all(u8::is_ascii_digit)
}

#[cfg(test)]
mod tests {
    use super::{check_streamable_http_url, is_protocol_version_shaped};

    #[test]
    fn accepts_http_and_https_with_a_query() {
        assert_eq!(check_streamable_http_url("http://h/mcp"), Ok(()));
        assert_eq!(check_streamable_http_url("https://h/mcp?x=1"), Ok(()));
    }

    #[test]
    fn refuses_a_non_http_scheme() {
        assert_eq!(
            check_streamable_http_url("ftp://h/mcp"),
            Err("the scheme is not http or https")
        );
    }

    #[test]
    fn refuses_an_unparseable_url() {
        assert_eq!(
            check_streamable_http_url("not a url"),
            Err("it does not parse as scheme://host")
        );
    }

    #[test]
    fn refuses_an_empty_host() {
        assert_eq!(
            check_streamable_http_url("https:///mcp"),
            Err("the host is empty")
        );
    }

    #[test]
    fn refuses_userinfo() {
        assert_eq!(
            check_streamable_http_url("https://u:p@h/mcp"),
            Err("it carries userinfo")
        );
    }

    #[test]
    fn refuses_a_fragment() {
        assert_eq!(
            check_streamable_http_url("https://h/mcp#f"),
            Err("it carries a fragment")
        );
    }

    #[test]
    fn refuses_whitespace() {
        assert_eq!(
            check_streamable_http_url("https://h /mcp"),
            Err("it carries whitespace")
        );
    }

    #[test]
    fn a_known_protocol_version_is_one_the_pinned_sdk_names() {
        use super::{is_known_protocol_version, MCP_DEFAULT_PROTOCOL_VERSIONS};

        for version in MCP_DEFAULT_PROTOCOL_VERSIONS {
            assert!(is_known_protocol_version(version), "{version}");
        }
        assert!(!is_known_protocol_version("2099-01-01"));
        assert!(!is_known_protocol_version(""));
    }

    #[test]
    fn protocol_version_shape_is_exactly_ten_ascii_bytes() {
        assert!(is_protocol_version_shaped("2026-07-28"));
        assert!(!is_protocol_version_shaped("2026-7-28"));
        assert!(!is_protocol_version_shaped(""));
        assert!(!is_protocol_version_shaped("2026-07-284"));
    }
}
