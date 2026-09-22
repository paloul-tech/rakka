//! Per-attempt rmcp client construction shared by `sync` and `executor`:
//! credential to header, protocol negotiation, the peer-agent rule, error
//! mapping.
//!
//! Three properties this module is the single place to hold:
//!
//! - A resolved credential becomes a header value and nothing else. It is
//!   never logged, never stored, and never reaches a refusal: every variant of
//!   [`McpClientError`] names only the server, the material *kind*, and an
//!   rmcp error *variant name* — never a response body, which is what an
//!   rmcp error's own `Display` would splice in
//!   ([`StreamableHttpError::UnexpectedServerResponse`] carries a body
//!   preview).
//! - Protocol versions are negotiated from the binding's declared list, newest
//!   first, and an unknown version is refused rather than sent.
//! - A server that identifies as a Rakka agent is refused after the session is
//!   closed ([specification 14.4]): MCP is never an agent-to-agent channel.
//! - Every outbound connection passes the host's [`McpEgressCheck`] here, in
//!   this module's own `connect`, before a client exists and before the
//!   credential is read — at publish time as much as at dispatch time. The
//!   check lives in this module rather than in each caller so that *being
//!   connected* and *having passed the rule* are the same event: a new caller
//!   cannot reach a server by forgetting to ask.
//!
//! [`StreamableHttpError::UnexpectedServerResponse`]: rmcp::transport::streamable_http_client::StreamableHttpError::UnexpectedServerResponse
//! [specification 14.4]: ../../../docs/plans/rakka-agent/spec.md

use std::collections::HashMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use http::{HeaderName, HeaderValue};
use rakka_agent::AgentAuthorityRefusal;
use rakka_agent_workflow::{AgentEphemeralCredential, AgentEphemeralCredentialMaterial};
use rmcp::model::{ClientCapabilities, ClientConfig, Implementation, ProtocolVersion};
use rmcp::service::{
    serve_client_with_lifecycle, ClientInitializeError, ClientLifecycleMode, Peer, RoleClient,
    RunningService, ServiceError,
};
use rmcp::transport::streamable_http_client::{
    StreamableHttpClient, StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
};
use rmcp::transport::IntoTransport;

use crate::binding::{
    McpServerBinding, McpServerId, McpTransport, MCP_CLIENT_NAME, MCP_PEER_AGENT_SERVER_PREFIX,
};

/// Why a client session could not be established, or could not be trusted.
///
/// No variant carries credential material, a response body, or a URL query
/// string.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum McpClientError {
    /// No protocol version is common to the binding and the server.
    ProtocolUnsupported {
        /// The server that was contacted.
        server: String,
        /// The versions this client offered.
        client: Vec<String>,
        /// The versions the server reported.
        server_versions: Vec<String>,
    },
    /// The server identifies itself as a Rakka agent. MCP is never an
    /// agent-to-agent channel.
    PeerAgentChannel {
        /// The server that was contacted.
        server: String,
        /// The self-reported name that triggered the refusal.
        name: String,
    },
    /// The resolved credential's material cannot be carried as an HTTP header.
    CredentialMaterialUnsupported {
        /// The server that was contacted.
        server: String,
        /// The material kind that was refused. A stable label, never a value.
        material: &'static str,
    },
    /// The transport could not carry the session.
    Transport {
        /// The server that was contacted.
        server: String,
        /// An rmcp error variant name, and a JSON-RPC code where one exists.
        reason: String,
    },
    /// The peer answered, but not in a way the protocol allows.
    Protocol {
        /// The server that was contacted.
        server: String,
        /// An rmcp error variant name, and a JSON-RPC code where one exists.
        reason: String,
    },
    /// The host's egress rule refused the server's URL, so no connection was
    /// opened and no credential left the process.
    Egress {
        /// The server whose URL was refused.
        server: String,
        /// The host's own stable refusal code.
        code: String,
        /// The host's own message. Never a credential: the check is given the
        /// URL and the server id, and nothing else.
        message: String,
        /// Whether the refusing condition may clear without a new
        /// configuration, as the host declared it.
        retryable: bool,
    },
}

impl McpClientError {
    /// Stable, machine-readable error code.
    ///
    /// Borrowed rather than `&'static str`: an [`Self::Egress`] refusal
    /// carries the *host's* own code through unchanged, so an operator reads
    /// back the rule that fired rather than a code this crate invented for it.
    #[must_use]
    pub fn code(&self) -> &str {
        match self {
            Self::ProtocolUnsupported { .. } => "mcp-protocol-unsupported",
            Self::PeerAgentChannel { .. } => "mcp-peer-agent-channel-refused",
            Self::CredentialMaterialUnsupported { .. } => "mcp-credential-material-unsupported",
            Self::Egress { code, .. } => code,
            // One code, not two: `mcp-transport-failed` is the
            // `AgentDispatchError::Invocation` code the executor raises for a
            // failed call, and an operator acting on "the server did not
            // answer usefully" does the same thing either way. A second code
            // no caller could branch on would only widen the compatibility
            // surface.
            Self::Transport { .. } | Self::Protocol { .. } => "mcp-transport-failed",
        }
    }
}

impl Display for McpClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProtocolUnsupported {
                server,
                client,
                server_versions,
            } => write!(
                f,
                "the MCP server {server} shares no protocol version with this client \
                 (offered {client:?}, server reported {server_versions:?})"
            ),
            Self::PeerAgentChannel { server, name } => write!(
                f,
                "the MCP server {server} identifies as a Rakka agent ({name}); \
                 MCP is never an agent-to-agent channel"
            ),
            // Deliberately free of the letter `p`: the sync's credential proof
            // asserts that a `Basic` refusal cannot echo a one-character
            // password, and it can only do that by checking the whole message.
            Self::CredentialMaterialUnsupported { server, material } => write!(
                f,
                "the server {server} credential material {material} is not one that can \
                 be sent as a header"
            ),
            Self::Transport { server, reason } => {
                write!(f, "the MCP server {server} transport failed: {reason}")
            }
            Self::Protocol { server, reason } => {
                write!(f, "the MCP server {server} broke the protocol: {reason}")
            }
            Self::Egress {
                server,
                code,
                message,
                ..
            } => write!(
                f,
                "the egress rule refused the MCP server {server} ({code}): {message}"
            ),
        }
    }
}

impl Error for McpClientError {}

/// One negotiated MCP client session.
///
/// Transport-agnostic on purpose: the Streamable HTTP path and the
/// child-process path both hand rmcp a `RunningService`, and everything after
/// the handshake — the negotiated version, the peer, the close — is the same.
pub struct McpClientSession {
    running: RunningService<RoleClient, ClientConfig>,
    server_name: String,
    negotiated: ProtocolVersion,
}

impl McpClientSession {
    /// The peer every request goes through.
    #[must_use]
    pub fn peer(&self) -> &Peer<RoleClient> {
        self.running.peer()
    }

    /// The protocol version this session negotiated.
    #[must_use]
    pub const fn negotiated_version(&self) -> &ProtocolVersion {
        &self.negotiated
    }

    /// The server's self-reported implementation name, empty when the peer
    /// reported none (a discovery response need not carry one).
    #[must_use]
    pub fn server_name(&self) -> &str {
        &self.server_name
    }

    /// Cancels the session and waits for its transport to close.
    ///
    /// A join failure is not actionable — the session is gone either way — so
    /// it is dropped rather than raised as a refusal of the caller's work.
    pub async fn close(self) {
        let _ = self.running.cancel().await;
    }
}

/// The egress rule every outbound MCP connection passes before a client
/// exists.
///
/// This adapter reaches a URL an operator supplied and, for a bound server,
/// carries a resolved credential to it. Both an SSRF probe and a credential
/// exfiltration are therefore one misconfigured URL away, so the host — not
/// this crate — decides which destinations are reachable, and decides it
/// *before* the connection is opened rather than after a response comes back.
///
/// The check applies at publish time as much as at dispatch time: a descriptor
/// sync is the same outbound connection carrying the same credential.
pub trait McpEgressCheck: Send + Sync + 'static {
    /// Admits or refuses one outbound connection to `url` on `server`'s
    /// behalf.
    ///
    /// # Errors
    ///
    /// An [`AgentAuthorityRefusal`] carrying the host's own stable code.
    fn check(&self, server: &McpServerId, url: &str) -> Result<(), AgentAuthorityRefusal>;
}

/// The egress check that admits every destination.
///
/// The **explicit opt-out**, not a default: a deployment whose MCP servers are
/// all in-cluster, and a test driving a loopback fake, name this type and so
/// record that the decision was taken. Nothing constructs it implicitly, and
/// no signature defaults to it — an unconsidered deployment cannot reach the
/// network by omission.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct McpAllowAllEgress;

impl McpEgressCheck for McpAllowAllEgress {
    fn check(&self, _server: &McpServerId, _url: &str) -> Result<(), AgentAuthorityRefusal> {
        Ok(())
    }
}

/// The identity this adapter presents at MCP initialization.
#[must_use]
pub fn client_info() -> ClientConfig {
    ClientConfig::new(
        ClientCapabilities::default(),
        Implementation::new(MCP_CLIENT_NAME, env!("CARGO_PKG_VERSION")),
    )
}

/// Opens a Streamable HTTP session to the binding's server over the injected
/// client, once `egress` has admitted the destination.
///
/// The order is the property: the egress check runs before the credential is
/// read, so a refused destination never resolves material into a header at
/// all, and before a transport exists, so a refused destination is never
/// dialed.
///
/// The credential — when one is supplied — becomes an `Authorization` header
/// or one custom header, and nothing else.
///
/// # Errors
///
/// [`McpClientError`] with its stable code.
pub(crate) async fn connect<C>(
    http: &C,
    binding: &McpServerBinding,
    credential: Option<&AgentEphemeralCredential>,
    egress: &dyn McpEgressCheck,
) -> Result<McpClientSession, McpClientError>
where
    C: StreamableHttpClient + Sync,
{
    let server = binding.server_id.to_string();
    let McpTransport::StreamableHttp { url } = &binding.transport else {
        return Err(McpClientError::Transport {
            server,
            reason: "the binding names no HTTP endpoint".to_string(),
        });
    };
    egress
        .check(&binding.server_id, url)
        .map_err(|refusal| McpClientError::Egress {
            server,
            code: refusal.code,
            message: refusal.message,
            retryable: refusal.retryable,
        })?;
    // Below this line only: the credential is read once the destination is
    // admitted, and never above it.
    let (auth_header, custom_headers) = credential_headers(binding, credential)?;
    let mut config = StreamableHttpClientTransportConfig::with_uri(url.as_str());
    config.allow_stateless = true;
    config.auth_header = auth_header;
    config.custom_headers = custom_headers;
    let transport = StreamableHttpClientTransport::with_client(http.clone(), config);
    connect_over(transport, binding).await
}

/// Opens a session over an already-built transport: the child-process
/// launcher's path, where the credential never becomes a header because there
/// is no request to put one on.
///
/// # Errors
///
/// [`McpClientError`] with its stable code.
pub(crate) async fn connect_over<T, E, A>(
    transport: T,
    binding: &McpServerBinding,
) -> Result<McpClientSession, McpClientError>
where
    T: IntoTransport<RoleClient, E, A>,
    E: std::error::Error + Send + Sync + 'static,
{
    let server = binding.server_id.to_string();
    let preferred_versions = preferred_versions(binding)?;
    let running = serve_client_with_lifecycle(
        client_info(),
        transport,
        ClientLifecycleMode::Discover { preferred_versions },
    )
    .await
    .map_err(|error| initialize_error(&server, &error))?;
    finish(running, &server).await
}

/// The step `connect` and `connect_over` share once rmcp has a running
/// service: read the negotiated version and the peer's name, and refuse a peer
/// that identifies as a Rakka agent — closing the session first, so the
/// refusal leaves nothing open.
async fn finish(
    running: RunningService<RoleClient, ClientConfig>,
    server: &str,
) -> Result<McpClientSession, McpClientError> {
    let Some(peer_info) = running.peer_info() else {
        let _ = running.cancel().await;
        return Err(McpClientError::Protocol {
            server: server.to_string(),
            reason: "the peer reported no negotiated information".to_string(),
        });
    };
    let negotiated = peer_info.protocol_version.clone();
    let server_name = peer_info
        .server_info
        .as_ref()
        .map(|info| info.name.clone())
        .unwrap_or_default();
    if server_name.starts_with(MCP_PEER_AGENT_SERVER_PREFIX) {
        let _ = running.cancel().await;
        return Err(McpClientError::PeerAgentChannel {
            server: server.to_string(),
            name: server_name,
        });
    }
    Ok(McpClientSession {
        running,
        server_name,
        negotiated,
    })
}

/// Turns the binding's declared protocol versions into the rmcp constants
/// rmcp negotiates with.
///
/// [`ProtocolVersion`] has no public constructor from a string — its only
/// string-to-value path is its `Deserialize` impl
/// (`rmcp-3.4.0/src/model.rs:241`), which silently admits an unknown value —
/// so a declared version is matched against
/// [`ProtocolVersion::KNOWN_VERSIONS`] and an unknown one is never offered on
/// the wire.
///
/// [`McpServerBinding::validate`] refuses an unknown version first, as
/// `mcp-binding-invalid`, which is what an operator reading the refusal needs:
/// the binding is misconfigured, not the network. This arm is the internal
/// invariant behind that gate — unreachable for a validated binding, and a
/// [`McpClientError::Protocol`] rather than a panic for one that skipped it.
fn preferred_versions(binding: &McpServerBinding) -> Result<Vec<ProtocolVersion>, McpClientError> {
    binding
        .protocol_versions
        .iter()
        .map(|declared| {
            ProtocolVersion::KNOWN_VERSIONS
                .iter()
                .find(|known| known.as_str() == declared.as_str())
                .cloned()
                .ok_or_else(|| McpClientError::Protocol {
                    server: binding.server_id.to_string(),
                    reason: "unknown protocol version".to_string(),
                })
        })
        .collect()
}

/// Turns a resolved credential into the transport's auth header and custom
/// headers.
///
/// The returned auth value is the **bare token**: rmcp's reqwest client passes
/// the transport's `auth_header` to `RequestBuilder::bearer_auth`
/// (`rmcp-3.4.0/src/transport/common/reqwest/streamable_http_client.rs:89`,
/// `:155`, `:202`), and reqwest is what writes `Authorization: Bearer <value>`.
/// Prefixing here would put `Bearer Bearer …` on the wire.
///
/// # Errors
///
/// [`McpClientError::CredentialMaterialUnsupported`] for material that is not
/// an HTTP header — including an API key whose name or value is not a legal
/// header. The refused value never reaches the error.
fn credential_headers(
    binding: &McpServerBinding,
    credential: Option<&AgentEphemeralCredential>,
) -> Result<(Option<String>, HashMap<HeaderName, HeaderValue>), McpClientError> {
    let mut custom_headers = HashMap::new();
    let Some(credential) = credential else {
        return Ok((None, custom_headers));
    };
    let unsupported = |material: &'static str| McpClientError::CredentialMaterialUnsupported {
        server: binding.server_id.to_string(),
        material,
    };
    match credential.material() {
        AgentEphemeralCredentialMaterial::BearerToken { token } => {
            Ok((Some(token.clone()), custom_headers))
        }
        AgentEphemeralCredentialMaterial::ApiKey { name, value } => {
            let name =
                HeaderName::from_bytes(name.as_bytes()).map_err(|_| unsupported("api-key"))?;
            let value = HeaderValue::from_str(value).map_err(|_| unsupported("api-key"))?;
            custom_headers.insert(name, value);
            Ok((None, custom_headers))
        }
        material @ (AgentEphemeralCredentialMaterial::Basic { .. }
        | AgentEphemeralCredentialMaterial::Custom { .. }) => {
            Err(unsupported(material.kind_label()))
        }
    }
}

/// Maps an rmcp initialization failure by variant name.
///
/// Never by `Display`: [`ClientInitializeError::JsonRpcError`] and the
/// transport variants embed peer-supplied text, and
/// [`ClientInitializeError::ExpectedInitResponse`] embeds the whole response.
/// A JSON-RPC code is a number the peer cannot steer text through, so it is
/// the one detail carried through.
fn initialize_error(server: &str, error: &ClientInitializeError) -> McpClientError {
    let server = server.to_string();
    match error {
        ClientInitializeError::NoCompatibleProtocolVersion {
            client_supported,
            server_supported,
        } => McpClientError::ProtocolUnsupported {
            server,
            client: client_supported
                .iter()
                .map(|version| version.as_str().to_string())
                .collect(),
            server_versions: server_supported
                .iter()
                .map(|version| version.as_str().to_string())
                .collect(),
        },
        ClientInitializeError::TransportError { .. } => McpClientError::Transport {
            server,
            reason: "TransportError".to_string(),
        },
        ClientInitializeError::ConnectionClosed(_) => McpClientError::Transport {
            server,
            reason: "ConnectionClosed".to_string(),
        },
        ClientInitializeError::Cancelled => McpClientError::Transport {
            server,
            reason: "Cancelled".to_string(),
        },
        ClientInitializeError::JsonRpcError(data) => McpClientError::Protocol {
            server,
            reason: format!("JsonRpcError({})", data.code.0),
        },
        ClientInitializeError::ExpectedInitResponse(_) => McpClientError::Protocol {
            server,
            reason: "ExpectedInitResponse".to_string(),
        },
        ClientInitializeError::ExpectedInitResult(_) => McpClientError::Protocol {
            server,
            reason: "ExpectedInitResult".to_string(),
        },
        ClientInitializeError::ConflictInitResponseId(..) => McpClientError::Protocol {
            server,
            reason: "ConflictInitResponseId".to_string(),
        },
        ClientInitializeError::UncorrelatedErrorResponse { .. } => McpClientError::Protocol {
            server,
            reason: "UncorrelatedErrorResponse".to_string(),
        },
        ClientInitializeError::NoPreferredProtocolVersion => McpClientError::Protocol {
            server,
            reason: "NoPreferredProtocolVersion".to_string(),
        },
        ClientInitializeError::LegacyFallbackFailed { .. } => McpClientError::Protocol {
            server,
            reason: "LegacyFallbackFailed".to_string(),
        },
        // `ClientInitializeError` is `#[non_exhaustive]`: a version rmcp adds
        // later must not become an unmapped panic or a leaked `Display`.
        _ => McpClientError::Protocol {
            server,
            reason: "Unrecognized".to_string(),
        },
    }
}

/// Maps an rmcp request failure by variant name, on the same terms as
/// [`initialize_error`].
pub(crate) fn service_error(server: &str, error: &ServiceError) -> McpClientError {
    let server = server.to_string();
    match error {
        ServiceError::McpError(data) => McpClientError::Protocol {
            server,
            reason: format!("McpError({})", data.code.0),
        },
        ServiceError::TransportSend(_) => McpClientError::Transport {
            server,
            reason: "TransportSend".to_string(),
        },
        ServiceError::TransportClosed => McpClientError::Transport {
            server,
            reason: "TransportClosed".to_string(),
        },
        ServiceError::UnexpectedResponse => McpClientError::Protocol {
            server,
            reason: "UnexpectedResponse".to_string(),
        },
        ServiceError::SubscriptionLagged { .. } => McpClientError::Protocol {
            server,
            reason: "SubscriptionLagged".to_string(),
        },
        ServiceError::Cancelled { .. } => McpClientError::Transport {
            server,
            reason: "Cancelled".to_string(),
        },
        ServiceError::Timeout { .. } => McpClientError::Transport {
            server,
            reason: "Timeout".to_string(),
        },
        ServiceError::InputRequiredRoundsExceeded { .. } => McpClientError::Protocol {
            server,
            reason: "InputRequiredRoundsExceeded".to_string(),
        },
        // `ServiceError` is `#[non_exhaustive]`; see `initialize_error`.
        _ => McpClientError::Protocol {
            server,
            reason: "Unrecognized".to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use rakka_agent::{AgentEffectSafetyClass, AgentToolDeclaration};
    use rakka_agent_workflow::AgentEphemeralCredential;

    use super::{
        client_info, credential_headers, preferred_versions, McpAllowAllEgress, McpClientError,
        McpEgressCheck,
    };
    use crate::binding::{McpServerBinding, McpServerId, McpToolPolicy, MCP_CLIENT_NAME};

    fn binding() -> McpServerBinding {
        McpServerBinding::streamable_http(
            McpServerId::new("crm").expect("the server id is valid"),
            "https://example.test/mcp",
        )
        .with_tool(
            "search",
            McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)),
        )
        .expect("the tool name is valid")
    }

    #[test]
    fn the_client_identifies_itself_by_the_pinned_name_and_the_crate_version() {
        let info = client_info();
        assert_eq!(info.client_info.name, MCP_CLIENT_NAME);
        assert_eq!(info.client_info.version, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn a_bearer_token_is_the_bare_value_because_rmcp_adds_the_scheme() {
        let credential = AgentEphemeralCredential::bearer_token("t0ken");
        let (auth, custom) =
            credential_headers(&binding(), Some(&credential)).expect("bearer is a header");
        assert_eq!(auth.as_deref(), Some("t0ken"));
        assert!(custom.is_empty());
    }

    #[test]
    fn an_api_key_becomes_the_named_custom_header() {
        let credential = AgentEphemeralCredential::api_key("x-api-key", "k3y");
        let (auth, custom) =
            credential_headers(&binding(), Some(&credential)).expect("api key is a header");
        assert_eq!(auth, None);
        let value = custom
            .get(&http::HeaderName::from_static("x-api-key"))
            .expect("the header is set");
        assert_eq!(value, "k3y");
    }

    #[test]
    fn an_api_key_with_an_illegal_header_name_is_refused_without_its_value() {
        let credential = AgentEphemeralCredential::api_key("bad name", "k3y-sentinel");
        let error =
            credential_headers(&binding(), Some(&credential)).expect_err("the name is illegal");
        assert_eq!(error.code(), "mcp-credential-material-unsupported");
        assert!(!error.to_string().contains("k3y-sentinel"), "{error}");
    }

    #[test]
    fn basic_and_custom_material_are_refused_by_kind_label() {
        for (credential, material) in [
            (
                AgentEphemeralCredential::basic("u", "secret-sentinel"),
                "basic",
            ),
            (
                AgentEphemeralCredential::custom("scheme", "secret-sentinel"),
                "custom",
            ),
        ] {
            let error =
                credential_headers(&binding(), Some(&credential)).expect_err("not a header");
            assert_eq!(error.code(), "mcp-credential-material-unsupported");
            assert!(error.to_string().contains(material), "{error}");
            assert!(!error.to_string().contains("secret-sentinel"), "{error}");
        }
    }

    #[test]
    fn an_unsupported_material_refusal_reads_back_no_character_of_its_value() {
        // The sync's credential proof asserts a `Basic` refusal for the
        // password `"p"` contains no `p` at all — the strongest check a
        // one-character sentinel admits. That makes this message's wording
        // load-bearing, so it is pinned here, next to the message, rather than
        // only in a distant integration test.
        let credential = AgentEphemeralCredential::basic("u", "p");
        let error = credential_headers(&binding(), Some(&credential)).expect_err("not a header");
        assert!(
            !error.to_string().contains(['p', 'P']),
            "the refusal must stay free of the letter p: {error}"
        );
    }

    #[test]
    fn no_credential_sets_no_header_at_all() {
        let (auth, custom) = credential_headers(&binding(), None).expect("no credential");
        assert_eq!(auth, None);
        assert!(custom.is_empty());
    }

    #[test]
    fn the_declared_versions_map_to_the_rmcp_constants_newest_first() {
        let versions = preferred_versions(&binding()).expect("the defaults are known");
        let labels: Vec<&str> = versions
            .iter()
            .map(rmcp::model::ProtocolVersion::as_str)
            .collect();
        assert_eq!(labels, vec!["2026-07-28", "2025-11-25"]);
    }

    #[test]
    fn an_unknown_declared_version_is_refused_rather_than_offered() {
        // The operator-facing gate is `McpServerBinding::validate`
        // (`mcp-binding-invalid`, proved in `tests/binding.rs`); this is the
        // backstop for a binding that reached `connect` unvalidated.
        let binding = binding().with_protocol_versions(vec!["2099-01-01".to_string()]);
        let error = preferred_versions(&binding).expect_err("unknown");
        assert_eq!(error.code(), "mcp-transport-failed");
        assert!(
            error.to_string().contains("unknown protocol version"),
            "{error}"
        );
    }

    #[test]
    fn a_transport_and_a_protocol_refusal_share_one_code() {
        let transport = McpClientError::Transport {
            server: "crm".to_string(),
            reason: "ConnectionClosed".to_string(),
        };
        let protocol = McpClientError::Protocol {
            server: "crm".to_string(),
            reason: "UnexpectedResponse".to_string(),
        };
        assert_eq!(transport.code(), "mcp-transport-failed");
        assert_eq!(
            protocol.code(),
            transport.code(),
            "a protocol failure is not its own registered code"
        );
    }

    #[test]
    fn the_allow_all_egress_check_is_the_explicit_opt_out() {
        let server = McpServerId::new("crm").expect("the server id is valid");
        assert_eq!(
            McpAllowAllEgress.check(&server, "https://anywhere.test/mcp"),
            Ok(())
        );
    }
}
