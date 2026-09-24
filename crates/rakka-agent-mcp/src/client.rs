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
//!   first, and an unknown version is refused rather than sent. A binding that
//!   lists a revision older than 2026-07-28 is reached through rmcp's `Auto`
//!   lifecycle — `server/discover` first, the legacy `initialize` handshake
//!   when the server answers it as a legacy server does — and every session,
//!   whichever handshake opened it, is held to a version the binding lists.
//! - A server that identifies as a Rakka agent is refused after the session is
//!   closed ([specification 14.4]): MCP is never an agent-to-agent channel.
//! - Every outbound connection passes the host's [`McpEgressCheck`] here, in
//!   this module's own `connect`, before a client exists and before the
//!   credential is read — at publish time as much as at dispatch time. The
//!   check lives in this module rather than in each caller so that *being
//!   connected* and *having passed the rule* are the same event: a new caller
//!   cannot reach a server by forgetting to ask.
//! - A child process is reached only through [`connect_over`], over the
//!   transport a deployment's launcher produced. It dials nothing, so there is
//!   no URL for the egress rule to judge, and it takes no credential, so none
//!   can reach a process that has no header to carry it.
//!
//! [`StreamableHttpError::UnexpectedServerResponse`]: rmcp::transport::streamable_http_client::StreamableHttpError::UnexpectedServerResponse
//! [specification 14.4]: ../../../docs/plans/rakka-agent/spec.md

use std::collections::HashMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::time::Duration;

use http::{HeaderName, HeaderValue};
use rakka_agent::AgentAuthorityRefusal;
use rakka_agent_workflow::{AgentEphemeralCredential, AgentEphemeralCredentialMaterial};
use rmcp::model::{ClientCapabilities, ClientConfig, ErrorCode, Implementation, ProtocolVersion};
use rmcp::service::{
    serve_client_with_lifecycle, ClientInitializeError, ClientLifecycleMode, Peer, RoleClient,
    RunningService, ServiceError,
};
use rmcp::transport::streamable_http_client::{
    StreamableHttpClient, StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
};
use rmcp::transport::IntoTransport;

use crate::binding::{
    is_protocol_version_shaped, McpServerBinding, McpServerId, McpTransport, MCP_CLIENT_NAME,
    MCP_PEER_AGENT_SERVER_PREFIX,
};
use crate::launcher::{concrete_pair, McpChildTransport};

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

/// How long [`McpClientSession::close`] waits for a session's transport to
/// finish closing before it stops waiting.
///
/// A bound of its own, never an effect's timeout: a close is owed on every
/// path out of an attempt, including the one where the effect's deadline has
/// just fired, so it cannot be charged to that deadline — but a wedged server
/// must not be able to hang the drop either. What is abandoned at the bound is
/// the wait, not the cleanup: the session is cancelled before the wait starts,
/// and rmcp bounds its own session-delete request separately
/// (`SESSION_CLEANUP_TIMEOUT`, `rmcp-3.4.0/src/transport/streamable_http_client.rs:42`).
const SESSION_CLOSE_BOUND: Duration = Duration::from_secs(3);

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

    /// Cancels the session and waits, for at most a few seconds, for its
    /// transport to close.
    ///
    /// The cancellation is immediate and unconditional; only the wait is
    /// bounded, so a server that never acknowledges the close costs the
    /// caller a fixed few seconds rather than the rest of its life. A join
    /// failure or an abandoned wait is not actionable — the session is
    /// cancelled either way — so neither is raised as a refusal of the
    /// caller's work.
    pub async fn close(mut self) {
        let _ = self.running.close_with_timeout(SESSION_CLOSE_BOUND).await;
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
    open_session(transport, binding).await
}

/// Opens a session to a child-process binding's server over the stdio a
/// launcher produced — the launcher's path, where the credential never
/// becomes a header because there is no request to put one on.
///
/// Takes no credential and applies no egress rule, and both are the point: a
/// stdio child is reached over a pipe the deployment handed over, not a URL
/// anything could dial, and it has no header a credential could ride. A
/// binding over the Streamable HTTP transport is refused here — its session
/// is opened only by the path that checks egress first.
///
/// # Errors
///
/// [`McpClientError`] with its stable code: `mcp-transport-failed` for a
/// binding that is not a child process, or for a peer that broke the
/// handshake.
pub async fn connect_over(
    transport: McpChildTransport,
    binding: &McpServerBinding,
) -> Result<McpClientSession, McpClientError> {
    if !matches!(binding.transport, McpTransport::ChildProcess { .. }) {
        return Err(McpClientError::Transport {
            server: binding.server_id.to_string(),
            reason: "the binding names no child process".to_string(),
        });
    }
    match transport {
        // The launched pair *is* rmcp's transport: `(R, W)` of an `AsyncRead`
        // and an `AsyncWrite` implements `IntoTransport`
        // (`rmcp-3.4.0/src/transport/async_rw.rs:24`). `concrete_pair` is what
        // makes each half concrete; see its own documentation for why that is
        // load-bearing.
        McpChildTransport::Pair { reader, writer } => {
            open_session(concrete_pair(reader, writer), binding).await
        }
        #[cfg(feature = "child-process")]
        McpChildTransport::Process(process) => open_session(process, binding).await,
    }
}

/// The handshake every session shares, over whichever transport the caller
/// built: negotiate from the binding's declared versions under the lifecycle
/// they call for ([`lifecycle`]), then [`finish`].
async fn open_session<T, E, A>(
    transport: T,
    binding: &McpServerBinding,
) -> Result<McpClientSession, McpClientError>
where
    T: IntoTransport<RoleClient, E, A>,
    E: std::error::Error + Send + Sync + 'static,
{
    let server = binding.server_id.to_string();
    let offered = preferred_versions(binding)?;
    let running = serve_client_with_lifecycle(client_info(), transport, lifecycle(offered.clone()))
        .await
        .map_err(|error| initialize_error(&server, &offered, &error))?;
    finish(running, &server, &offered).await
}

/// The first MCP revision that has no `initialize` handshake: from it on, a
/// session is negotiated by `server/discover` alone.
const DISCOVER_ONLY_SINCE: ProtocolVersion = ProtocolVersion::V_2026_07_28;

/// The lifecycle the declared versions call for.
///
/// A binding that lists only 2026-07-28 or later is negotiated by `Discover`,
/// which does not fall back: a server that cannot answer `server/discover`
/// cannot speak any version the binding lists.
///
/// A binding that lists an older revision is negotiated by `Auto`: rmcp probes
/// with `server/discover` first, and when the server answers the probe as a
/// server that predates 2026-07-28 does — a correlated JSON-RPC error, such as
/// method-not-found, that is not a modern-era rejection
/// (`rmcp-3.4.0/src/service/client.rs:1032`–`1043`) — or does not answer it
/// within rmcp's ten-second probe window (`:657`), it runs the legacy
/// `initialize` handshake on the same transport (`:799`–`:842`). The version
/// that handshake requests is the *newest* legacy revision the binding lists,
/// the one the 2025-11-25 lifecycle says a client should send; a server that
/// answers with an older one is still held to the binding's list by
/// [`finish`], because rmcp's legacy handshake accepts whatever version the
/// server answers.
fn lifecycle(preferred_versions: Vec<ProtocolVersion>) -> ClientLifecycleMode {
    let legacy_version = preferred_versions
        .iter()
        .filter(|version| version.as_str() < DISCOVER_ONLY_SINCE.as_str())
        .max_by(|left, right| left.as_str().cmp(right.as_str()))
        .cloned();
    match legacy_version {
        None => ClientLifecycleMode::Discover { preferred_versions },
        legacy_version => ClientLifecycleMode::Auto {
            preferred_versions,
            legacy_version,
        },
    }
}

/// The step every session shares once rmcp has a running service: read the
/// negotiated version and the peer's name, refuse a version the binding does
/// not list, and refuse a peer that identifies as a Rakka agent — closing the
/// session first, so a refusal leaves nothing open.
///
/// The version check matters for the legacy handshake alone — `server/discover`
/// only ever selects from the offered list — but it runs for every session, so
/// no path can hold one the binding did not declare.
///
/// The identity check reads what the server reported: `serverInfo` from the
/// `initialize` result, or from the `_meta` of a `server/discover` result
/// (`rmcp-3.4.0/src/model.rs:1290`–`1295`, `:1373`). A server that reports no
/// `serverInfo` at all reads as an empty name and passes. That is the
/// cooperative threat model the rule is written for — it keeps a Rakka agent
/// served over MCP, which identifies itself, from becoming a side channel
/// between agents — and not a defence against a server that hides what it is.
async fn finish(
    running: RunningService<RoleClient, ClientConfig>,
    server: &str,
    offered: &[ProtocolVersion],
) -> Result<McpClientSession, McpClientError> {
    let Some(peer_info) = running.peer_info() else {
        let _ = running.cancel().await;
        return Err(McpClientError::Protocol {
            server: server.to_string(),
            reason: "the peer reported no negotiated information".to_string(),
        });
    };
    let negotiated = peer_info.protocol_version.clone();
    if !offered.contains(&negotiated) {
        let _ = running.cancel().await;
        return Err(McpClientError::ProtocolUnsupported {
            server: server.to_string(),
            client: version_labels(offered),
            server_versions: reported_versions([negotiated.as_str()]),
        });
    }
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

/// The most protocol versions a refusal reports the server as supporting.
const REPORTED_VERSIONS_MAX: usize = 16;

/// The labels of versions this client offered.
fn version_labels(versions: &[ProtocolVersion]) -> Vec<String> {
    versions
        .iter()
        .map(|version| version.as_str().to_string())
        .collect()
}

/// The versions a server reported, as a refusal may carry them: only entries
/// shaped `YYYY-MM-DD`, and at most [`REPORTED_VERSIONS_MAX`] of them.
///
/// The list is server-chosen text — rmcp's `ProtocolVersion` decodes any
/// string (`rmcp-3.4.0/src/model.rs:241`) — and a refusal is written to the
/// run's durable outbox row, so neither an arbitrary string nor an unbounded
/// list of them may ride it.
fn reported_versions<'a>(versions: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    versions
        .into_iter()
        .filter(|version| is_protocol_version_shaped(version))
        .take(REPORTED_VERSIONS_MAX)
        .map(str::to_string)
        .collect()
}

/// Maps an rmcp initialization failure by variant name.
///
/// Never by `Display`: [`ClientInitializeError::JsonRpcError`] and the
/// transport variants embed peer-supplied text, and
/// [`ClientInitializeError::ExpectedInitResponse`] embeds the whole response.
/// A JSON-RPC code is a number the peer cannot steer text through, so it is
/// the one detail carried through.
///
/// A version refusal is [`McpClientError::ProtocolUnsupported`] wherever it
/// surfaces: rmcp's own `NoCompatibleProtocolVersion`, and a JSON-RPC
/// `UNSUPPORTED_PROTOCOL_VERSION` error (-32022) — which is how a server
/// refuses the legacy `initialize` handshake when it supports no version that
/// still has one.
fn initialize_error(
    server: &str,
    offered: &[ProtocolVersion],
    error: &ClientInitializeError,
) -> McpClientError {
    let server = server.to_string();
    match error {
        ClientInitializeError::NoCompatibleProtocolVersion {
            client_supported,
            server_supported,
        } => McpClientError::ProtocolUnsupported {
            server,
            client: version_labels(client_supported),
            server_versions: reported_versions(
                server_supported.iter().map(ProtocolVersion::as_str),
            ),
        },
        ClientInitializeError::JsonRpcError(data)
            if data.code == ErrorCode::UNSUPPORTED_PROTOCOL_VERSION =>
        {
            let supported = data
                .data
                .as_ref()
                .and_then(|data| data.get("supported"))
                .and_then(serde_json::Value::as_array);
            McpClientError::ProtocolUnsupported {
                server,
                client: version_labels(offered),
                server_versions: reported_versions(
                    supported
                        .into_iter()
                        .flatten()
                        .filter_map(serde_json::Value::as_str),
                ),
            }
        }
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
        ClientInitializeError::LegacyFallbackFailed { discover, fallback } => {
            legacy_fallback_failed(
                server.clone(),
                initialize_error(&server, offered, discover),
                initialize_error(&server, offered, fallback),
            )
        }
        // `ClientInitializeError` is `#[non_exhaustive]`: a version rmcp adds
        // later must not become an unmapped panic or a leaked `Display`.
        _ => McpClientError::Protocol {
            server,
            reason: "Unrecognized".to_string(),
        },
    }
}

/// Maps a failed `Auto` lifecycle — the `server/discover` probe read as a
/// legacy server, and then the legacy `initialize` handshake failed too — from
/// what each phase itself failed on.
///
/// A version refusal from either phase is the truthful answer: the server
/// shares no version with the binding, so the code is
/// `mcp-protocol-unsupported`. Otherwise the fallback decides the variant — a
/// transport failure is [`McpClientError::Transport`], anything else
/// [`McpClientError::Protocol`] — and the reason names both phases, each by
/// its own variant name, so an operator can tell a server that is down from
/// one that answers neither handshake.
fn legacy_fallback_failed(
    server: String,
    discover: McpClientError,
    fallback: McpClientError,
) -> McpClientError {
    if matches!(fallback, McpClientError::ProtocolUnsupported { .. }) {
        return fallback;
    }
    if matches!(discover, McpClientError::ProtocolUnsupported { .. }) {
        return discover;
    }
    let reason = format!(
        "LegacyFallbackFailed(discover: {}; fallback: {})",
        phase_reason(&discover),
        phase_reason(&fallback)
    );
    match fallback {
        McpClientError::Transport { .. } => McpClientError::Transport { server, reason },
        _ => McpClientError::Protocol { server, reason },
    }
}

/// One phase's own reason: the variant name a transport or protocol failure
/// carries, and the stable code for anything else.
fn phase_reason(error: &McpClientError) -> &str {
    match error {
        McpClientError::Transport { reason, .. } | McpClientError::Protocol { reason, .. } => {
            reason
        }
        other => other.code(),
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

    use rmcp::model::{ErrorCode, ProtocolVersion};
    use rmcp::service::{ClientInitializeError, ClientLifecycleMode};
    use rmcp::ErrorData;

    use super::{
        client_info, credential_headers, initialize_error, lifecycle, preferred_versions,
        McpAllowAllEgress, McpClientError, McpEgressCheck,
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

    #[test]
    fn a_binding_that_lists_a_legacy_version_keeps_the_initialize_fallback() {
        let offered = preferred_versions(&binding()).expect("the defaults are known");
        assert_eq!(
            lifecycle(offered.clone()),
            ClientLifecycleMode::Auto {
                preferred_versions: offered,
                legacy_version: Some(ProtocolVersion::V_2025_11_25),
            },
            "the default list reaches a 2025-11-25 server through initialize"
        );
        let modern = vec![ProtocolVersion::V_2026_07_28];
        assert_eq!(
            lifecycle(modern.clone()),
            ClientLifecycleMode::Discover {
                preferred_versions: modern
            },
            "a 2026-07-28-only list has no handshake to fall back to"
        );
        // The legacy handshake requests the newest legacy version listed,
        // whatever the list's order.
        let several = vec![
            ProtocolVersion::V_2025_06_18,
            ProtocolVersion::V_2026_07_28,
            ProtocolVersion::V_2025_11_25,
        ];
        assert!(
            matches!(
                lifecycle(several),
                ClientLifecycleMode::Auto { legacy_version: Some(ref version), .. }
                    if *version == ProtocolVersion::V_2025_11_25
            ),
            "the newest legacy version is requested"
        );
    }

    /// `server/discover` answered as a legacy server answers it.
    fn discover_not_found() -> Box<ClientInitializeError> {
        Box::new(ClientInitializeError::JsonRpcError(ErrorData::new(
            ErrorCode::METHOD_NOT_FOUND,
            "server/discover",
            None,
        )))
    }

    #[test]
    fn a_failed_fallback_is_mapped_from_both_phases() {
        let offered = [ProtocolVersion::V_2026_07_28, ProtocolVersion::V_2025_11_25];

        // The fallback's transport failed: a transport failure naming both
        // phases, and none of the peer's text.
        let transport = initialize_error(
            "crm",
            &offered,
            &ClientInitializeError::LegacyFallbackFailed {
                discover: discover_not_found(),
                fallback: Box::new(ClientInitializeError::ConnectionClosed(
                    "peer-text-sentinel".to_string(),
                )),
            },
        );
        assert!(
            matches!(transport, McpClientError::Transport { ref reason, .. }
                if reason == "LegacyFallbackFailed(discover: JsonRpcError(-32601); fallback: ConnectionClosed)"),
            "{transport:?}"
        );
        assert_eq!(transport.code(), "mcp-transport-failed");
        assert!(!transport.to_string().contains("peer-text-sentinel"));

        // The fallback answered, but not with a handshake: a protocol failure.
        let protocol = initialize_error(
            "crm",
            &offered,
            &ClientInitializeError::LegacyFallbackFailed {
                discover: discover_not_found(),
                fallback: Box::new(ClientInitializeError::ExpectedInitResult(None)),
            },
        );
        assert!(
            matches!(protocol, McpClientError::Protocol { ref reason, .. }
                if reason == "LegacyFallbackFailed(discover: JsonRpcError(-32601); fallback: ExpectedInitResult)"),
            "{protocol:?}"
        );

        // The fallback was refused for its version: the truthful code is the
        // version refusal, carrying only the version-shaped entries the server
        // reported.
        let version = initialize_error(
            "crm",
            &offered,
            &ClientInitializeError::LegacyFallbackFailed {
                discover: discover_not_found(),
                fallback: Box::new(ClientInitializeError::JsonRpcError(ErrorData::new(
                    ErrorCode::UNSUPPORTED_PROTOCOL_VERSION,
                    "Unsupported protocol version",
                    Some(serde_json::json!({
                        "requested": "2025-11-25",
                        "supported": ["2026-07-28", "secret-sentinel", 7],
                    })),
                ))),
            },
        );
        assert_eq!(version.code(), "mcp-protocol-unsupported");
        assert_eq!(
            version,
            McpClientError::ProtocolUnsupported {
                server: "crm".to_string(),
                client: vec!["2026-07-28".to_string(), "2025-11-25".to_string()],
                server_versions: vec!["2026-07-28".to_string()],
            }
        );
    }

    #[test]
    fn a_reported_version_list_is_shaped_and_bounded() {
        let many: Vec<ProtocolVersion> = (0..40).map(|_| ProtocolVersion::V_2024_11_05).collect();
        let error = initialize_error(
            "crm",
            &[ProtocolVersion::V_2026_07_28],
            &ClientInitializeError::NoCompatibleProtocolVersion {
                client_supported: vec![ProtocolVersion::V_2026_07_28],
                server_supported: many,
            },
        );
        let McpClientError::ProtocolUnsupported {
            server_versions, ..
        } = &error
        else {
            panic!("a version refusal: {error:?}")
        };
        assert_eq!(server_versions.len(), 16, "{server_versions:?}");
    }
}
