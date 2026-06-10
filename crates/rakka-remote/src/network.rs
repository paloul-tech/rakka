//! Tokio TCP remote transport foundation.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prost::Message;
use rakka_cluster::{ClusterNode, ClusterProtocol, CompatibilityRange, NodeId, ProtocolVersion};
use rakka_core::{MetricsRecorder, NoopMetricsRecorder, METRIC_REMOTE_FAILURES};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use crate::proto::ProtoRemoteHandshake;
use crate::{
    ProtobufEnvelopeCodec, RemoteEndpoint, RemoteEndpointError, RemoteEnvelope, RemoteError,
    RemoteTransport, RemoteTransportError, RemoteTransportResult,
};

/// Default remote envelope wire version for TCP remoting.
pub const DEFAULT_REMOTE_ENVELOPE_VERSION: u32 = 1;

/// TCP remote connection state gauge.
pub const METRIC_TCP_REMOTE_CONNECTION_STATE: &str = "rakka.remote.tcp.connection.state";

/// TCP remote envelope send counter.
pub const METRIC_TCP_REMOTE_SENDS: &str = "rakka.remote.tcp.sends";

/// TCP remote envelope receive counter.
pub const METRIC_TCP_REMOTE_RECEIVES: &str = "rakka.remote.tcp.receives";

/// TCP remote reconnect counter.
pub const METRIC_TCP_REMOTE_RECONNECTS: &str = "rakka.remote.tcp.reconnects";

/// Remote TCP connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TcpRemoteConnectionLifecycle {
    /// A connection attempt is in progress.
    Connecting,
    /// Handshake completed and frames can be sent.
    Ready,
    /// A transient failure occurred and reconnect backoff is active.
    Backoff,
    /// The connection is gracefully draining and rejects new sends.
    Draining,
    /// The connection is closed.
    Closed,
    /// The connection failed.
    Failed,
}

impl TcpRemoteConnectionLifecycle {
    /// Stable lifecycle label used in snapshots and metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Connecting => "connecting",
            Self::Ready => "ready",
            Self::Backoff => "backoff",
            Self::Draining => "draining",
            Self::Closed => "closed",
            Self::Failed => "failed",
        }
    }
}

/// Configuration for Tokio TCP remoting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpRemoteTransportConfig {
    bind_addr: SocketAddr,
    outbound_queue_capacity: usize,
    connect_timeout: Duration,
    reconnect_backoff: Duration,
    idle_timeout: Duration,
    max_frame_bytes: usize,
    envelope_version: u32,
}

impl Default for TcpRemoteTransportConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 2552),
            outbound_queue_capacity: 1024,
            connect_timeout: Duration::from_secs(2),
            reconnect_backoff: Duration::from_millis(100),
            idle_timeout: Duration::from_secs(30),
            max_frame_bytes: 16 * 1024 * 1024,
            envelope_version: DEFAULT_REMOTE_ENVELOPE_VERSION,
        }
    }
}

impl TcpRemoteTransportConfig {
    /// Creates a config with default values.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the bind address for the local TCP listener.
    #[must_use]
    pub const fn bind_addr(mut self, bind_addr: SocketAddr) -> Self {
        self.bind_addr = bind_addr;
        self
    }

    /// Sets the bounded outbound queue capacity per peer.
    #[must_use]
    pub const fn outbound_queue_capacity(mut self, capacity: usize) -> Self {
        self.outbound_queue_capacity = capacity;
        self
    }

    /// Sets the TCP connect timeout.
    #[must_use]
    pub const fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Sets reconnect backoff after transient failures.
    #[must_use]
    pub const fn reconnect_backoff(mut self, backoff: Duration) -> Self {
        self.reconnect_backoff = backoff;
        self
    }

    /// Sets idle timeout before an unused connection is closed.
    #[must_use]
    pub const fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Sets the maximum accepted frame size.
    #[must_use]
    pub const fn max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes;
        self
    }

    /// Sets the advertised remote envelope wire version.
    #[must_use]
    pub const fn envelope_version(mut self, envelope_version: u32) -> Self {
        self.envelope_version = envelope_version;
        self
    }
}

/// Handshake metadata exchanged before TCP envelope delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpRemoteHandshake {
    node_id: NodeId,
    protocol: ClusterProtocol,
    envelope_version: u32,
    capabilities: BTreeSet<String>,
}

impl TcpRemoteHandshake {
    /// Creates a TCP remote handshake.
    #[must_use]
    pub fn new(
        node_id: NodeId,
        protocol: ClusterProtocol,
        envelope_version: u32,
        capabilities: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            node_id,
            protocol,
            envelope_version,
            capabilities: capabilities.into_iter().collect(),
        }
    }

    /// Advertised node id.
    #[must_use]
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Advertised cluster protocol.
    #[must_use]
    pub const fn protocol(&self) -> ClusterProtocol {
        self.protocol
    }

    /// Advertised remote envelope wire version.
    #[must_use]
    pub const fn envelope_version(&self) -> u32 {
        self.envelope_version
    }

    /// Advertised optional capabilities.
    #[must_use]
    pub fn capabilities(&self) -> &BTreeSet<String> {
        &self.capabilities
    }

    fn local(node_id: NodeId, protocol: ClusterProtocol, envelope_version: u32) -> Self {
        Self::new(
            node_id,
            protocol,
            envelope_version,
            ["tcp-remoting-v1".to_string()],
        )
    }

    fn encode(&self) -> Vec<u8> {
        let compatible = self.protocol.compatible_with();
        let proto = ProtoRemoteHandshake {
            node_id: self.node_id.to_string(),
            protocol_major: u32::from(self.protocol.version().major()),
            protocol_minor: u32::from(self.protocol.version().minor()),
            compatible_min_major: u32::from(compatible.min().major()),
            compatible_min_minor: u32::from(compatible.min().minor()),
            compatible_max_major: u32::from(compatible.max().major()),
            compatible_max_minor: u32::from(compatible.max().minor()),
            envelope_version: self.envelope_version,
            capabilities: self.capabilities.iter().cloned().collect(),
        };
        let mut bytes = Vec::with_capacity(proto.encoded_len());
        proto
            .encode(&mut bytes)
            .expect("handshake encoding to Vec should not fail");
        bytes
    }

    fn decode(bytes: &[u8]) -> TcpRemoteTransportResult<Self> {
        let proto = ProtoRemoteHandshake::decode(bytes).map_err(|error| {
            TcpRemoteTransportError::Decode {
                message: error.to_string(),
            }
        })?;
        let node_id = NodeId::from_str(&proto.node_id).map_err(|error| {
            TcpRemoteTransportError::InvalidHandshake {
                message: error.to_string(),
            }
        })?;
        let protocol = ClusterProtocol::new(
            protocol_version(proto.protocol_major, proto.protocol_minor)?,
            CompatibilityRange::new(
                protocol_version(proto.compatible_min_major, proto.compatible_min_minor)?,
                protocol_version(proto.compatible_max_major, proto.compatible_max_minor)?,
            ),
        );
        Ok(Self::new(
            node_id,
            protocol,
            proto.envelope_version,
            proto.capabilities,
        ))
    }
}

/// Point-in-time state for one TCP peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpRemotePeerSnapshot {
    node_id: NodeId,
    lifecycle: TcpRemoteConnectionLifecycle,
    accepting_sends: bool,
    queued: usize,
    sent: u64,
    reconnects: u64,
    failures: u64,
    last_error: Option<String>,
}

impl TcpRemotePeerSnapshot {
    /// Peer node id.
    #[must_use]
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Current connection lifecycle.
    #[must_use]
    pub const fn lifecycle(&self) -> TcpRemoteConnectionLifecycle {
        self.lifecycle
    }

    /// Returns true when `send` can enqueue new envelopes for this peer.
    #[must_use]
    pub const fn accepting_sends(&self) -> bool {
        self.accepting_sends
    }

    /// Last observed bounded outbound queue depth.
    #[must_use]
    pub const fn queued(&self) -> usize {
        self.queued
    }

    /// Number of envelopes successfully written to this peer.
    #[must_use]
    pub const fn sent(&self) -> u64 {
        self.sent
    }

    /// Number of reconnect/backoff events.
    #[must_use]
    pub const fn reconnects(&self) -> u64 {
        self.reconnects
    }

    /// Number of peer worker failures.
    #[must_use]
    pub const fn failures(&self) -> u64 {
        self.failures
    }

    /// Last peer failure, when known.
    #[must_use]
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
}

/// Point-in-time state for one TCP remote transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpRemoteTransportSnapshot {
    local_node_id: NodeId,
    local_addr: SocketAddr,
    peer_count: usize,
    inbound_connections: u64,
    inbound_envelopes: u64,
    inbound_failures: u64,
    last_inbound_error: Option<String>,
    peers: Vec<TcpRemotePeerSnapshot>,
}

impl TcpRemoteTransportSnapshot {
    /// Local node id.
    #[must_use]
    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    /// Bound local TCP address.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Registered peer count.
    #[must_use]
    pub const fn peer_count(&self) -> usize {
        self.peer_count
    }

    /// Accepted inbound TCP connections.
    #[must_use]
    pub const fn inbound_connections(&self) -> u64 {
        self.inbound_connections
    }

    /// Inbound envelopes dispatched to the local endpoint.
    #[must_use]
    pub const fn inbound_envelopes(&self) -> u64 {
        self.inbound_envelopes
    }

    /// Inbound connection or dispatch failures.
    #[must_use]
    pub const fn inbound_failures(&self) -> u64 {
        self.inbound_failures
    }

    /// Last inbound failure, when known.
    #[must_use]
    pub fn last_inbound_error(&self) -> Option<&str> {
        self.last_inbound_error.as_deref()
    }

    /// Registered peer snapshots.
    #[must_use]
    pub fn peers(&self) -> &[TcpRemotePeerSnapshot] {
        &self.peers
    }
}

/// Failure returned by TCP remote transport setup and network loops.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TcpRemoteTransportError {
    /// TCP transport configuration is invalid.
    InvalidConfig {
        /// Failure detail.
        message: String,
    },
    /// TCP bind/connect/read/write operation failed.
    Io {
        /// Failure detail.
        message: String,
    },
    /// Handshake payload could not be decoded.
    Decode {
        /// Decode detail.
        message: String,
    },
    /// Handshake was structurally invalid.
    InvalidHandshake {
        /// Failure detail.
        message: String,
    },
    /// Frame was invalid or unexpected.
    InvalidFrame {
        /// Failure detail.
        message: String,
    },
    /// Remote node was not registered as an allowed peer.
    UnknownPeer {
        /// Unknown node id.
        node_id: NodeId,
    },
    /// Connected peer did not match the expected node id.
    UnexpectedPeer {
        /// Expected node id.
        expected: NodeId,
        /// Actual node id.
        actual: NodeId,
    },
    /// The remote node is not mutually compatible with the local node.
    IncompatibleProtocol {
        /// Remote node id.
        node_id: NodeId,
        /// Local protocol.
        local: ClusterProtocol,
        /// Remote protocol.
        remote: ClusterProtocol,
    },
    /// Remote envelope codec failed.
    Envelope {
        /// Codec failure.
        error: RemoteError,
    },
    /// Local endpoint rejected an inbound envelope.
    Endpoint {
        /// Endpoint failure.
        error: RemoteEndpointError,
    },
}

impl Display for TcpRemoteTransportError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { message } => {
                write!(f, "tcp remote config is invalid: {message}")
            }
            Self::Io { message } => write!(f, "tcp remote io failed: {message}"),
            Self::Decode { message } => write!(f, "tcp remote handshake decode failed: {message}"),
            Self::InvalidHandshake { message } => {
                write!(f, "tcp remote handshake is invalid: {message}")
            }
            Self::InvalidFrame { message } => write!(f, "tcp remote frame is invalid: {message}"),
            Self::UnknownPeer { node_id } => {
                write!(f, "tcp remote peer {node_id} is not registered")
            }
            Self::UnexpectedPeer { expected, actual } => write!(
                f,
                "tcp remote expected peer {expected}, but connected to {actual}"
            ),
            Self::IncompatibleProtocol {
                node_id,
                local,
                remote,
            } => write!(
                f,
                "tcp remote peer {node_id} advertises incompatible protocol {remote}; local is {local}"
            ),
            Self::Envelope { error } => write!(f, "tcp remote envelope codec failed: {error}"),
            Self::Endpoint { error } => write!(f, "tcp remote endpoint failed: {error}"),
        }
    }
}

impl Error for TcpRemoteTransportError {}

/// Convenient result alias for TCP remote transport operations.
pub type TcpRemoteTransportResult<T> = Result<T, TcpRemoteTransportError>;

/// Tokio TCP remote transport.
#[derive(Clone)]
pub struct TcpRemoteTransport {
    inner: Arc<TcpRemoteTransportInner>,
}

impl TcpRemoteTransport {
    /// Binds a TCP remote transport with a no-op metrics recorder.
    pub async fn bind(
        local_node_id: NodeId,
        local_protocol: ClusterProtocol,
        endpoint: RemoteEndpoint,
        config: TcpRemoteTransportConfig,
    ) -> TcpRemoteTransportResult<Self> {
        Self::bind_with_metrics(
            local_node_id,
            local_protocol,
            endpoint,
            config,
            Arc::new(NoopMetricsRecorder),
        )
        .await
    }

    /// Binds a TCP remote transport with a caller-provided metrics recorder.
    pub async fn bind_with_metrics(
        local_node_id: NodeId,
        local_protocol: ClusterProtocol,
        endpoint: RemoteEndpoint,
        config: TcpRemoteTransportConfig,
        recorder: Arc<dyn MetricsRecorder>,
    ) -> TcpRemoteTransportResult<Self> {
        validate_config(&config)?;
        let listener = TcpListener::bind(config.bind_addr).await.map_err(|error| {
            TcpRemoteTransportError::Io {
                message: error.to_string(),
            }
        })?;
        let local_addr = listener
            .local_addr()
            .map_err(|error| TcpRemoteTransportError::Io {
                message: error.to_string(),
            })?;
        let inner = Arc::new(TcpRemoteTransportInner {
            local_node_id,
            local_protocol,
            local_addr,
            endpoint,
            config,
            peers: Mutex::new(BTreeMap::new()),
            inbound: Mutex::new(InboundState::default()),
            recorder,
        });
        spawn_accept_loop(listener, Arc::clone(&inner));
        Ok(Self { inner })
    }

    /// Local node id advertised by this transport.
    #[must_use]
    pub fn local_node_id(&self) -> &NodeId {
        &self.inner.local_node_id
    }

    /// Local bound TCP address.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr
    }

    /// Registers one allowed outbound/inbound TCP peer.
    pub fn register_peer(&self, node: ClusterNode) -> RemoteTransportResult<()> {
        let node_id = node.id().clone();
        let mut peers = self
            .inner
            .peers
            .lock()
            .expect("tcp remote peer mutex poisoned");
        if peers.contains_key(&node_id) {
            return Err(RemoteTransportError::DuplicateNode { node_id });
        }

        let (sender, receiver) = mpsc::channel(self.inner.config.outbound_queue_capacity);
        let state = Arc::new(Mutex::new(PeerState::new(node_id.clone())));
        spawn_peer_worker(
            Arc::clone(&self.inner),
            node.clone(),
            receiver,
            Arc::clone(&state),
        );
        peers.insert(
            node_id,
            PeerHandle {
                node,
                sender,
                state,
            },
        );
        Ok(())
    }

    /// Returns a registered peer node.
    #[must_use]
    pub fn peer(&self, node_id: &NodeId) -> Option<ClusterNode> {
        self.inner
            .peers
            .lock()
            .expect("tcp remote peer mutex poisoned")
            .get(node_id)
            .map(|peer| peer.node.clone())
    }

    /// Gracefully drains a peer connection and rejects future sends to it.
    pub async fn drain_peer(&self, node_id: &NodeId) -> RemoteTransportResult<()> {
        let peer = self
            .peer_handle(node_id)
            .ok_or_else(|| RemoteTransportError::UnknownNode {
                node_id: node_id.clone(),
            })?;
        peer.state
            .lock()
            .expect("tcp remote peer state mutex poisoned")
            .begin_draining();
        self.inner
            .record_peer_state(node_id, TcpRemoteConnectionLifecycle::Draining);
        let (reply_to, reply) = oneshot::channel();
        peer.sender
            .send(PeerCommand::Drain { reply_to })
            .await
            .map_err(|_closed| RemoteTransportError::Closed {
                node_id: node_id.clone(),
            })?;
        reply
            .await
            .map_err(|_closed| RemoteTransportError::Closed {
                node_id: node_id.clone(),
            })?
    }

    /// Forces a peer connection closed. Future sends may reconnect.
    pub async fn force_close_peer(&self, node_id: &NodeId) -> RemoteTransportResult<()> {
        let peer = self
            .peer_handle(node_id)
            .ok_or_else(|| RemoteTransportError::UnknownNode {
                node_id: node_id.clone(),
            })?;
        let (reply_to, reply) = oneshot::channel();
        peer.sender
            .send(PeerCommand::ForceClose { reply_to })
            .await
            .map_err(|_closed| RemoteTransportError::Closed {
                node_id: node_id.clone(),
            })?;
        reply
            .await
            .map_err(|_closed| RemoteTransportError::Closed {
                node_id: node_id.clone(),
            })?
    }

    /// Returns a point-in-time peer snapshot.
    #[must_use]
    pub fn peer_snapshot(&self, node_id: &NodeId) -> Option<TcpRemotePeerSnapshot> {
        self.peer_handle(node_id).map(|peer| {
            peer.state
                .lock()
                .expect("tcp remote peer state mutex poisoned")
                .snapshot()
        })
    }

    /// Returns a point-in-time transport snapshot.
    #[must_use]
    pub fn snapshot(&self) -> TcpRemoteTransportSnapshot {
        let peers = self
            .inner
            .peers
            .lock()
            .expect("tcp remote peer mutex poisoned")
            .values()
            .map(|peer| {
                peer.state
                    .lock()
                    .expect("tcp remote peer state mutex poisoned")
                    .snapshot()
            })
            .collect::<Vec<_>>();
        let inbound = self
            .inner
            .inbound
            .lock()
            .expect("tcp remote inbound state mutex poisoned")
            .clone();
        TcpRemoteTransportSnapshot {
            local_node_id: self.inner.local_node_id.clone(),
            local_addr: self.inner.local_addr,
            peer_count: peers.len(),
            inbound_connections: inbound.connections,
            inbound_envelopes: inbound.envelopes,
            inbound_failures: inbound.failures,
            last_inbound_error: inbound.last_error,
            peers,
        }
    }

    fn peer_handle(&self, node_id: &NodeId) -> Option<PeerHandle> {
        self.inner
            .peers
            .lock()
            .expect("tcp remote peer mutex poisoned")
            .get(node_id)
            .cloned()
    }
}

impl RemoteTransport for TcpRemoteTransport {
    fn send(&self, destination: &NodeId, envelope: RemoteEnvelope) -> RemoteTransportResult<()> {
        let peer =
            self.peer_handle(destination)
                .ok_or_else(|| RemoteTransportError::UnknownNode {
                    node_id: destination.clone(),
                })?;
        {
            let mut state = peer
                .state
                .lock()
                .expect("tcp remote peer state mutex poisoned");
            if !state.accepting_sends {
                return Err(RemoteTransportError::Draining {
                    node_id: destination.clone(),
                });
            }
            state.queued = state.queued.saturating_add(1);
        }

        match peer.sender.try_send(PeerCommand::Send(envelope)) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_command)) => {
                peer.state
                    .lock()
                    .expect("tcp remote peer state mutex poisoned")
                    .decrement_queue();
                self.inner.recorder.increment_counter(
                    METRIC_REMOTE_FAILURES,
                    1,
                    &[("operation", "tcp-send"), ("error", "queue-full")],
                );
                Err(RemoteTransportError::QueueFull {
                    node_id: destination.clone(),
                    capacity: self.inner.config.outbound_queue_capacity,
                })
            }
            Err(mpsc::error::TrySendError::Closed(_command)) => {
                peer.state
                    .lock()
                    .expect("tcp remote peer state mutex poisoned")
                    .decrement_queue();
                Err(RemoteTransportError::Closed {
                    node_id: destination.clone(),
                })
            }
        }
    }
}

impl Debug for TcpRemoteTransport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpRemoteTransport")
            .field("local_node_id", &self.inner.local_node_id)
            .field("local_addr", &self.inner.local_addr)
            .field("peer_count", &self.snapshot().peer_count())
            .finish_non_exhaustive()
    }
}

struct TcpRemoteTransportInner {
    local_node_id: NodeId,
    local_protocol: ClusterProtocol,
    local_addr: SocketAddr,
    endpoint: RemoteEndpoint,
    config: TcpRemoteTransportConfig,
    peers: Mutex<BTreeMap<NodeId, PeerHandle>>,
    inbound: Mutex<InboundState>,
    recorder: Arc<dyn MetricsRecorder>,
}

impl TcpRemoteTransportInner {
    fn local_handshake(&self) -> TcpRemoteHandshake {
        TcpRemoteHandshake::local(
            self.local_node_id.clone(),
            self.local_protocol,
            self.config.envelope_version,
        )
    }

    fn peer_node(&self, node_id: &NodeId) -> Option<ClusterNode> {
        self.peers
            .lock()
            .expect("tcp remote peer mutex poisoned")
            .get(node_id)
            .map(|peer| peer.node.clone())
    }

    fn validate_peer(
        &self,
        expected: Option<&NodeId>,
        handshake: &TcpRemoteHandshake,
    ) -> TcpRemoteTransportResult<ClusterNode> {
        if let Some(expected) = expected {
            if handshake.node_id() != expected {
                return Err(TcpRemoteTransportError::UnexpectedPeer {
                    expected: expected.clone(),
                    actual: handshake.node_id().clone(),
                });
            }
        }

        if handshake.envelope_version() != self.config.envelope_version {
            return Err(TcpRemoteTransportError::InvalidHandshake {
                message: format!(
                    "unsupported envelope version {}; local supports {}",
                    handshake.envelope_version(),
                    self.config.envelope_version
                ),
            });
        }

        let node = self.peer_node(handshake.node_id()).ok_or_else(|| {
            TcpRemoteTransportError::UnknownPeer {
                node_id: handshake.node_id().clone(),
            }
        })?;
        if !self.local_protocol.is_compatible_with(handshake.protocol()) {
            return Err(TcpRemoteTransportError::IncompatibleProtocol {
                node_id: handshake.node_id().clone(),
                local: self.local_protocol,
                remote: handshake.protocol(),
            });
        }
        Ok(node)
    }

    fn record_inbound_connection(&self) {
        let mut inbound = self
            .inbound
            .lock()
            .expect("tcp remote inbound state mutex poisoned");
        inbound.connections = inbound.connections.saturating_add(1);
    }

    fn record_inbound_envelope(&self) {
        let mut inbound = self
            .inbound
            .lock()
            .expect("tcp remote inbound state mutex poisoned");
        inbound.envelopes = inbound.envelopes.saturating_add(1);
        self.recorder.increment_counter(
            METRIC_TCP_REMOTE_RECEIVES,
            1,
            &[("local_node", self.local_node_id.logical_id())],
        );
    }

    fn record_inbound_failure(&self, error: &TcpRemoteTransportError) {
        let mut inbound = self
            .inbound
            .lock()
            .expect("tcp remote inbound state mutex poisoned");
        inbound.failures = inbound.failures.saturating_add(1);
        inbound.last_error = Some(error.to_string());
        self.recorder.increment_counter(
            METRIC_REMOTE_FAILURES,
            1,
            &[("operation", "tcp-inbound"), ("error", error_code(error))],
        );
        warn!(
            target: "rakka.remote.tcp",
            local_node = %self.local_node_id,
            error = %error,
            "tcp inbound remoting failure"
        );
    }

    fn record_peer_state(&self, node_id: &NodeId, lifecycle: TcpRemoteConnectionLifecycle) {
        self.recorder.record_gauge(
            METRIC_TCP_REMOTE_CONNECTION_STATE,
            1.0,
            &[
                ("peer", node_id.logical_id()),
                ("state", lifecycle.as_str()),
            ],
        );
        debug!(
            target: "rakka.remote.tcp",
            local_node = %self.local_node_id,
            peer = %node_id,
            state = lifecycle.as_str(),
            "tcp remote peer state"
        );
    }

    fn record_peer_send(&self, node_id: &NodeId) {
        self.recorder.increment_counter(
            METRIC_TCP_REMOTE_SENDS,
            1,
            &[("peer", node_id.logical_id())],
        );
    }

    fn record_peer_reconnect(&self, node_id: &NodeId) {
        self.recorder.increment_counter(
            METRIC_TCP_REMOTE_RECONNECTS,
            1,
            &[("peer", node_id.logical_id())],
        );
    }
}

#[derive(Clone)]
struct PeerHandle {
    node: ClusterNode,
    sender: mpsc::Sender<PeerCommand>,
    state: Arc<Mutex<PeerState>>,
}

enum PeerCommand {
    Send(RemoteEnvelope),
    Drain {
        reply_to: oneshot::Sender<RemoteTransportResult<()>>,
    },
    ForceClose {
        reply_to: oneshot::Sender<RemoteTransportResult<()>>,
    },
}

#[derive(Debug, Clone)]
struct PeerState {
    node_id: NodeId,
    lifecycle: TcpRemoteConnectionLifecycle,
    accepting_sends: bool,
    queued: usize,
    sent: u64,
    reconnects: u64,
    failures: u64,
    last_error: Option<String>,
}

impl PeerState {
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            lifecycle: TcpRemoteConnectionLifecycle::Closed,
            accepting_sends: true,
            queued: 0,
            sent: 0,
            reconnects: 0,
            failures: 0,
            last_error: None,
        }
    }

    fn snapshot(&self) -> TcpRemotePeerSnapshot {
        TcpRemotePeerSnapshot {
            node_id: self.node_id.clone(),
            lifecycle: self.lifecycle,
            accepting_sends: self.accepting_sends,
            queued: self.queued,
            sent: self.sent,
            reconnects: self.reconnects,
            failures: self.failures,
            last_error: self.last_error.clone(),
        }
    }

    fn set_lifecycle(&mut self, lifecycle: TcpRemoteConnectionLifecycle) {
        self.lifecycle = lifecycle;
    }

    fn begin_draining(&mut self) {
        self.accepting_sends = false;
        self.lifecycle = TcpRemoteConnectionLifecycle::Draining;
    }

    fn decrement_queue(&mut self) {
        self.queued = self.queued.saturating_sub(1);
    }

    fn record_sent(&mut self) {
        self.sent = self.sent.saturating_add(1);
        self.lifecycle = TcpRemoteConnectionLifecycle::Ready;
        self.last_error = None;
    }

    fn record_reconnect(&mut self) {
        self.reconnects = self.reconnects.saturating_add(1);
        self.lifecycle = TcpRemoteConnectionLifecycle::Backoff;
    }

    fn record_failure(&mut self, error: &TcpRemoteTransportError) {
        self.failures = self.failures.saturating_add(1);
        self.lifecycle = TcpRemoteConnectionLifecycle::Failed;
        self.last_error = Some(error.to_string());
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct InboundState {
    connections: u64,
    envelopes: u64,
    failures: u64,
    last_error: Option<String>,
}

fn spawn_accept_loop(listener: TcpListener, inner: Arc<TcpRemoteTransportInner>) {
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let inner = Arc::clone(&inner);
                    tokio::spawn(async move {
                        if let Err(error) = handle_inbound_connection(stream, inner.clone()).await {
                            inner.record_inbound_failure(&error);
                        }
                    });
                }
                Err(error) => {
                    inner.record_inbound_failure(&TcpRemoteTransportError::Io {
                        message: error.to_string(),
                    });
                    break;
                }
            }
        }
    });
}

fn spawn_peer_worker(
    inner: Arc<TcpRemoteTransportInner>,
    node: ClusterNode,
    receiver: mpsc::Receiver<PeerCommand>,
    state: Arc<Mutex<PeerState>>,
) {
    tokio::spawn(peer_worker(inner, node, receiver, state));
}

async fn peer_worker(
    inner: Arc<TcpRemoteTransportInner>,
    node: ClusterNode,
    mut receiver: mpsc::Receiver<PeerCommand>,
    state: Arc<Mutex<PeerState>>,
) {
    let mut stream = None::<TcpStream>;
    loop {
        let command = if stream.is_some() {
            match tokio::time::timeout(inner.config.idle_timeout, receiver.recv()).await {
                Ok(command) => command,
                Err(_elapsed) => {
                    if let Some(mut open) = stream.take() {
                        let _shutdown = open.shutdown().await;
                    }
                    state
                        .lock()
                        .expect("tcp remote peer state mutex poisoned")
                        .set_lifecycle(TcpRemoteConnectionLifecycle::Closed);
                    inner.record_peer_state(node.id(), TcpRemoteConnectionLifecycle::Closed);
                    continue;
                }
            }
        } else {
            receiver.recv().await
        };

        let Some(command) = command else {
            break;
        };

        match command {
            PeerCommand::Send(envelope) => {
                state
                    .lock()
                    .expect("tcp remote peer state mutex poisoned")
                    .decrement_queue();
                match send_with_reconnect(&inner, &node, &state, &mut stream, envelope).await {
                    Ok(()) => {
                        {
                            state
                                .lock()
                                .expect("tcp remote peer state mutex poisoned")
                                .record_sent();
                        }
                        inner.record_peer_send(node.id());
                        inner.record_peer_state(node.id(), TcpRemoteConnectionLifecycle::Ready);
                    }
                    Err(error) => {
                        state
                            .lock()
                            .expect("tcp remote peer state mutex poisoned")
                            .record_failure(&error);
                        inner.record_peer_state(node.id(), TcpRemoteConnectionLifecycle::Failed);
                        inner.recorder.increment_counter(
                            METRIC_REMOTE_FAILURES,
                            1,
                            &[("operation", "tcp-outbound"), ("error", error_code(&error))],
                        );
                    }
                }
            }
            PeerCommand::Drain { reply_to } => {
                if let Some(mut open) = stream.take() {
                    let _close = write_frame(&mut open, FrameKind::Close, &[]).await;
                    let _shutdown = open.shutdown().await;
                }
                state
                    .lock()
                    .expect("tcp remote peer state mutex poisoned")
                    .set_lifecycle(TcpRemoteConnectionLifecycle::Closed);
                inner.record_peer_state(node.id(), TcpRemoteConnectionLifecycle::Closed);
                let _sent = reply_to.send(Ok(()));
            }
            PeerCommand::ForceClose { reply_to } => {
                if let Some(mut open) = stream.take() {
                    let _shutdown = open.shutdown().await;
                }
                state
                    .lock()
                    .expect("tcp remote peer state mutex poisoned")
                    .set_lifecycle(TcpRemoteConnectionLifecycle::Closed);
                inner.record_peer_state(node.id(), TcpRemoteConnectionLifecycle::Closed);
                let _sent = reply_to.send(Ok(()));
            }
        }
    }
}

async fn send_with_reconnect(
    inner: &TcpRemoteTransportInner,
    node: &ClusterNode,
    state: &Arc<Mutex<PeerState>>,
    stream: &mut Option<TcpStream>,
    envelope: RemoteEnvelope,
) -> TcpRemoteTransportResult<()> {
    let wire = ProtobufEnvelopeCodec::encode(&envelope)
        .map_err(|error| TcpRemoteTransportError::Envelope { error })?;

    for attempt in 0..=1 {
        if stream.is_none() {
            *stream = Some(connect_peer(inner, node, state).await?);
        }

        if let Some(open) = stream.as_mut() {
            match write_frame(open, FrameKind::Envelope, &wire).await {
                Ok(()) => return Ok(()),
                Err(error) if attempt == 0 => {
                    *stream = None;
                    state
                        .lock()
                        .expect("tcp remote peer state mutex poisoned")
                        .record_reconnect();
                    inner.record_peer_reconnect(node.id());
                    inner.record_peer_state(node.id(), TcpRemoteConnectionLifecycle::Backoff);
                    inner.recorder.increment_counter(
                        METRIC_REMOTE_FAILURES,
                        1,
                        &[
                            ("operation", "tcp-reconnect"),
                            ("error", error_code(&error)),
                        ],
                    );
                    tokio::time::sleep(inner.config.reconnect_backoff).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    Err(TcpRemoteTransportError::Io {
        message: "tcp remote send exhausted reconnect attempts".to_string(),
    })
}

async fn connect_peer(
    inner: &TcpRemoteTransportInner,
    node: &ClusterNode,
    state: &Arc<Mutex<PeerState>>,
) -> TcpRemoteTransportResult<TcpStream> {
    state
        .lock()
        .expect("tcp remote peer state mutex poisoned")
        .set_lifecycle(TcpRemoteConnectionLifecycle::Connecting);
    inner.record_peer_state(node.id(), TcpRemoteConnectionLifecycle::Connecting);
    let host = node.address().host().to_string();
    let port = node.address().port();
    let mut stream = tokio::time::timeout(
        inner.config.connect_timeout,
        TcpStream::connect((host.as_str(), port)),
    )
    .await
    .map_err(|_elapsed| TcpRemoteTransportError::Io {
        message: format!("connect to {} timed out", node.address().endpoint()),
    })?
    .map_err(|error| TcpRemoteTransportError::Io {
        message: error.to_string(),
    })?;

    write_frame(
        &mut stream,
        FrameKind::Handshake,
        &inner.local_handshake().encode(),
    )
    .await?;
    let frame = read_frame(&mut stream, inner.config.max_frame_bytes)
        .await?
        .ok_or_else(|| TcpRemoteTransportError::InvalidFrame {
            message: "connection closed before handshake response".to_string(),
        })?;
    if frame.kind != FrameKind::Handshake {
        return Err(TcpRemoteTransportError::InvalidFrame {
            message: format!("expected handshake response, got {:?}", frame.kind),
        });
    }
    let handshake = TcpRemoteHandshake::decode(&frame.payload)?;
    inner.validate_peer(Some(node.id()), &handshake)?;
    state
        .lock()
        .expect("tcp remote peer state mutex poisoned")
        .set_lifecycle(TcpRemoteConnectionLifecycle::Ready);
    inner.record_peer_state(node.id(), TcpRemoteConnectionLifecycle::Ready);
    Ok(stream)
}

async fn handle_inbound_connection(
    mut stream: TcpStream,
    inner: Arc<TcpRemoteTransportInner>,
) -> TcpRemoteTransportResult<()> {
    let frame = read_frame(&mut stream, inner.config.max_frame_bytes)
        .await?
        .ok_or_else(|| TcpRemoteTransportError::InvalidFrame {
            message: "connection closed before handshake".to_string(),
        })?;
    if frame.kind != FrameKind::Handshake {
        return Err(TcpRemoteTransportError::InvalidFrame {
            message: format!("expected handshake, got {:?}", frame.kind),
        });
    }

    let handshake = TcpRemoteHandshake::decode(&frame.payload)?;
    inner.validate_peer(None, &handshake)?;
    write_frame(
        &mut stream,
        FrameKind::Handshake,
        &inner.local_handshake().encode(),
    )
    .await?;
    inner.record_inbound_connection();

    loop {
        let Some(frame) = read_frame(&mut stream, inner.config.max_frame_bytes).await? else {
            return Ok(());
        };

        match frame.kind {
            FrameKind::Envelope => {
                inner
                    .endpoint
                    .receive_wire(&frame.payload)
                    .map_err(|error| TcpRemoteTransportError::Endpoint { error })?;
                inner.record_inbound_envelope();
            }
            FrameKind::Close => return Ok(()),
            FrameKind::Handshake => {
                return Err(TcpRemoteTransportError::InvalidFrame {
                    message: "unexpected handshake after connection setup".to_string(),
                });
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FrameKind {
    Handshake,
    Envelope,
    Close,
}

impl FrameKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Handshake => 1,
            Self::Envelope => 2,
            Self::Close => 3,
        }
    }

    fn from_tag(tag: u8) -> TcpRemoteTransportResult<Self> {
        match tag {
            1 => Ok(Self::Handshake),
            2 => Ok(Self::Envelope),
            3 => Ok(Self::Close),
            _ => Err(TcpRemoteTransportError::InvalidFrame {
                message: format!("unknown frame kind {tag}"),
            }),
        }
    }
}

struct Frame {
    kind: FrameKind,
    payload: Vec<u8>,
}

async fn write_frame<W>(
    writer: &mut W,
    kind: FrameKind,
    payload: &[u8],
) -> TcpRemoteTransportResult<()>
where
    W: AsyncWrite + Unpin,
{
    let len = payload
        .len()
        .checked_add(1)
        .and_then(|len| u32::try_from(len).ok())
        .ok_or_else(|| TcpRemoteTransportError::InvalidFrame {
            message: "frame payload is too large".to_string(),
        })?;
    writer
        .write_all(&len.to_be_bytes())
        .await
        .map_err(io_error)?;
    writer.write_all(&[kind.tag()]).await.map_err(io_error)?;
    writer.write_all(payload).await.map_err(io_error)?;
    writer.flush().await.map_err(io_error)
}

async fn read_frame<R>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> TcpRemoteTransportResult<Option<Frame>>
where
    R: AsyncRead + Unpin,
{
    let mut len = [0u8; 4];
    match reader.read_exact(&mut len).await {
        Ok(_read) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(io_error(error)),
    }
    let len = u32::from_be_bytes(len) as usize;
    if len == 0 || len > max_frame_bytes {
        return Err(TcpRemoteTransportError::InvalidFrame {
            message: format!("frame length {len} exceeds allowed range 1..={max_frame_bytes}"),
        });
    }
    let mut bytes = vec![0u8; len];
    reader.read_exact(&mut bytes).await.map_err(io_error)?;
    let kind = FrameKind::from_tag(bytes[0])?;
    Ok(Some(Frame {
        kind,
        payload: bytes[1..].to_vec(),
    }))
}

fn protocol_version(major: u32, minor: u32) -> TcpRemoteTransportResult<ProtocolVersion> {
    Ok(ProtocolVersion::new(
        u16::try_from(major).map_err(|_error| TcpRemoteTransportError::InvalidHandshake {
            message: format!("protocol major {major} exceeds u16"),
        })?,
        u16::try_from(minor).map_err(|_error| TcpRemoteTransportError::InvalidHandshake {
            message: format!("protocol minor {minor} exceeds u16"),
        })?,
    ))
}

fn io_error(error: std::io::Error) -> TcpRemoteTransportError {
    TcpRemoteTransportError::Io {
        message: error.to_string(),
    }
}

fn validate_config(config: &TcpRemoteTransportConfig) -> TcpRemoteTransportResult<()> {
    if config.outbound_queue_capacity == 0 {
        return Err(TcpRemoteTransportError::InvalidConfig {
            message: "outbound_queue_capacity must be greater than zero".to_string(),
        });
    }
    if config.max_frame_bytes == 0 {
        return Err(TcpRemoteTransportError::InvalidConfig {
            message: "max_frame_bytes must be greater than zero".to_string(),
        });
    }
    Ok(())
}

fn error_code(error: &TcpRemoteTransportError) -> &'static str {
    match error {
        TcpRemoteTransportError::InvalidConfig { .. } => "invalid-config",
        TcpRemoteTransportError::Io { .. } => "io-error",
        TcpRemoteTransportError::Decode { .. } => "decode-error",
        TcpRemoteTransportError::InvalidHandshake { .. } => "invalid-handshake",
        TcpRemoteTransportError::InvalidFrame { .. } => "invalid-frame",
        TcpRemoteTransportError::UnknownPeer { .. } => "unknown-peer",
        TcpRemoteTransportError::UnexpectedPeer { .. } => "unexpected-peer",
        TcpRemoteTransportError::IncompatibleProtocol { .. } => "incompatible-protocol",
        TcpRemoteTransportError::Envelope { .. } => "envelope-error",
        TcpRemoteTransportError::Endpoint { .. } => "endpoint-error",
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Duration;

    use rakka_cluster::{ClusterNode, ClusterProtocol, NodeAddress, NodeId, ProtocolVersion};
    use rakka_core::{InMemoryMetricsRecorder, METRIC_REMOTE_FAILURES};
    use tokio::net::TcpStream;
    use tokio::sync::mpsc;

    use crate::{
        EncodedPayload, RemoteDestination, RemoteEndpoint, RemoteEnvelope, RemoteEnvelopeMetadata,
        RemoteTransport, RemoteTransportError,
    };

    use super::{
        read_frame, write_frame, FrameKind, TcpRemoteConnectionLifecycle, TcpRemoteHandshake,
        TcpRemoteTransport, TcpRemoteTransportConfig, DEFAULT_REMOTE_ENVELOPE_VERSION,
    };

    #[tokio::test]
    async fn tcp_transport_routes_envelope_between_loopback_nodes() {
        let node_a_id = NodeId::new("rakka-0", "uid-a");
        let node_b_id = NodeId::new("rakka-1", "uid-b");
        let (received_tx, mut received_rx) = mpsc::unbounded_channel();
        let endpoint_b = RemoteEndpoint::new(node_b_id.clone());
        endpoint_b
            .register_entity_handler("cart", move |envelope: RemoteEnvelope| {
                let _sent = received_tx.send(envelope);
                Ok(())
            })
            .expect("entity handler should register");

        let Some(transport_b) = bind_transport(node_b_id.clone(), endpoint_b).await else {
            return;
        };
        let endpoint_a = RemoteEndpoint::new(node_a_id.clone());
        let Some(transport_a) = bind_transport(node_a_id.clone(), endpoint_a).await else {
            return;
        };
        transport_a
            .register_peer(node(&node_b_id, transport_b.local_addr().port()))
            .expect("node b peer should register");
        transport_b
            .register_peer(node(&node_a_id, transport_a.local_addr().port()))
            .expect("node a peer should register");

        transport_a
            .send(&node_b_id, test_envelope("cart", "cart-1", b"apple"))
            .expect("send should enqueue");

        let received = tokio::time::timeout(Duration::from_secs(1), received_rx.recv())
            .await
            .expect("inbound envelope should arrive")
            .expect("inbound channel should remain open");
        assert_eq!(received.payload, b"apple");
        assert_eq!(transport_b.snapshot().inbound_envelopes(), 1);
        assert_eq!(
            transport_a
                .peer_snapshot(&node_b_id)
                .expect("peer snapshot")
                .lifecycle(),
            TcpRemoteConnectionLifecycle::Ready
        );
    }

    #[tokio::test]
    async fn inbound_unknown_node_fails_closed_with_snapshot_error() {
        let node_a_id = NodeId::new("rakka-0", "uid-a");
        let node_b_id = NodeId::new("rakka-1", "uid-b");
        let Some(transport_b) =
            bind_transport(node_b_id.clone(), RemoteEndpoint::new(node_b_id)).await
        else {
            return;
        };
        let Some(transport_a) =
            bind_transport(node_a_id.clone(), RemoteEndpoint::new(node_a_id.clone())).await
        else {
            return;
        };
        transport_a
            .register_peer(node(
                transport_b.local_node_id(),
                transport_b.local_addr().port(),
            ))
            .expect("node b peer should register only on node a");

        transport_a
            .send(
                transport_b.local_node_id(),
                test_envelope("cart", "cart-1", b"apple"),
            )
            .expect("send should enqueue before inbound rejects");

        wait_for(|| transport_b.snapshot().inbound_failures() > 0).await;
        assert!(transport_b
            .snapshot()
            .last_inbound_error()
            .expect("inbound error")
            .contains("not registered"));
    }

    #[tokio::test]
    async fn incompatible_protocol_is_rejected_during_handshake() {
        let node_a_id = NodeId::new("rakka-0", "uid-a");
        let node_b_id = NodeId::new("rakka-1", "uid-b");
        let protocol_a = ClusterProtocol::exact(ProtocolVersion::new(2, 0));
        let protocol_b = ClusterProtocol::exact(ProtocolVersion::new(1, 0));
        let Some(transport_b) = bind_transport_with_protocol(
            node_b_id.clone(),
            protocol_b,
            RemoteEndpoint::new(node_b_id.clone()),
        )
        .await
        else {
            return;
        };
        let Some(transport_a) = bind_transport_with_protocol(
            node_a_id.clone(),
            protocol_a,
            RemoteEndpoint::new(node_a_id.clone()),
        )
        .await
        else {
            return;
        };
        transport_a
            .register_peer(node_with_protocol(
                &node_b_id,
                transport_b.local_addr().port(),
                protocol_b,
            ))
            .expect("node b peer should register");
        transport_b
            .register_peer(node_with_protocol(
                &node_a_id,
                transport_a.local_addr().port(),
                protocol_a,
            ))
            .expect("node a peer should register");

        transport_a
            .send(&node_b_id, test_envelope("cart", "cart-1", b"apple"))
            .expect("send should enqueue before compatibility rejection");

        wait_for(|| transport_b.snapshot().inbound_failures() > 0).await;
        assert!(transport_b
            .snapshot()
            .last_inbound_error()
            .expect("inbound error")
            .contains("incompatible protocol"));
    }

    #[tokio::test]
    async fn malformed_inbound_envelope_records_decode_failure() {
        let node_a_id = NodeId::new("rakka-0", "uid-a");
        let node_b_id = NodeId::new("rakka-1", "uid-b");
        let Some(transport_b) =
            bind_transport(node_b_id.clone(), RemoteEndpoint::new(node_b_id.clone())).await
        else {
            return;
        };
        let node_a = node(&node_a_id, 2552);
        transport_b
            .register_peer(node_a)
            .expect("node a peer should register");

        let mut stream = TcpStream::connect(("127.0.0.1", transport_b.local_addr().port()))
            .await
            .expect("tcp connect should succeed");
        let handshake = TcpRemoteHandshake::new(
            node_a_id,
            ClusterProtocol::default(),
            DEFAULT_REMOTE_ENVELOPE_VERSION,
            ["test".to_string()],
        );
        write_frame(&mut stream, FrameKind::Handshake, &handshake.encode())
            .await
            .expect("handshake should write");
        let _server_handshake = read_frame(&mut stream, 1024 * 1024)
            .await
            .expect("handshake response should read")
            .expect("handshake response should exist");
        write_frame(&mut stream, FrameKind::Envelope, &[255, 0, 255])
            .await
            .expect("bad envelope frame should write");

        wait_for(|| transport_b.snapshot().inbound_failures() > 0).await;
        assert!(transport_b
            .snapshot()
            .last_inbound_error()
            .expect("inbound error")
            .contains("decode"));
    }

    #[tokio::test]
    async fn bounded_outbound_queue_reports_backpressure() {
        let node_a_id = NodeId::new("rakka-0", "uid-a");
        let node_b_id = NodeId::new("rakka-1", "uid-b");
        let config = test_config()
            .outbound_queue_capacity(1)
            .connect_timeout(Duration::from_millis(250))
            .reconnect_backoff(Duration::from_millis(250));
        let Some(transport_a) = try_bind_transport(
            node_a_id,
            ClusterProtocol::default(),
            RemoteEndpoint::new(NodeId::new("rakka-0", "uid-a2")),
            config,
        )
        .await
        else {
            return;
        };
        let Some(port) = unused_port() else {
            return;
        };
        transport_a
            .register_peer(node(&node_b_id, port))
            .expect("peer should register");

        let mut saw_full = false;
        for _attempt in 0..16 {
            match transport_a.send(&node_b_id, test_envelope("cart", "cart-1", b"apple")) {
                Ok(()) => {}
                Err(RemoteTransportError::QueueFull { capacity, .. }) => {
                    assert_eq!(capacity, 1);
                    saw_full = true;
                    break;
                }
                Err(error) => panic!("unexpected send error: {error}"),
            }
        }
        assert!(saw_full, "bounded outbound queue should report full");
    }

    #[tokio::test]
    async fn drain_rejects_future_sends_and_force_close_can_reconnect() {
        let node_a_id = NodeId::new("rakka-0", "uid-a");
        let node_b_id = NodeId::new("rakka-1", "uid-b");
        let (received_tx, mut received_rx) = mpsc::unbounded_channel();
        let endpoint_b = RemoteEndpoint::new(node_b_id.clone());
        endpoint_b
            .register_entity_handler("cart", move |envelope: RemoteEnvelope| {
                let _sent = received_tx.send(envelope);
                Ok(())
            })
            .expect("entity handler should register");

        let Some(transport_b) = bind_transport(node_b_id.clone(), endpoint_b).await else {
            return;
        };
        let Some(transport_a) =
            bind_transport(node_a_id.clone(), RemoteEndpoint::new(node_a_id.clone())).await
        else {
            return;
        };
        transport_a
            .register_peer(node(&node_b_id, transport_b.local_addr().port()))
            .expect("node b peer should register");
        transport_b
            .register_peer(node(&node_a_id, transport_a.local_addr().port()))
            .expect("node a peer should register");

        transport_a
            .send(&node_b_id, test_envelope("cart", "cart-1", b"first"))
            .expect("send should enqueue");
        let _first = tokio::time::timeout(Duration::from_secs(1), received_rx.recv())
            .await
            .expect("first envelope should arrive");

        transport_a
            .force_close_peer(&node_b_id)
            .await
            .expect("force close should succeed");
        transport_a
            .send(&node_b_id, test_envelope("cart", "cart-1", b"second"))
            .expect("send after force close should reconnect");
        let second = tokio::time::timeout(Duration::from_secs(1), received_rx.recv())
            .await
            .expect("second envelope should arrive")
            .expect("inbound channel should remain open");
        assert_eq!(second.payload, b"second");

        transport_a
            .drain_peer(&node_b_id)
            .await
            .expect("drain should succeed");
        assert!(matches!(
            transport_a.send(&node_b_id, test_envelope("cart", "cart-1", b"third")),
            Err(RemoteTransportError::Draining { .. })
        ));
    }

    #[tokio::test]
    async fn idle_timeout_closes_ready_connection() {
        let node_a_id = NodeId::new("rakka-0", "uid-a");
        let node_b_id = NodeId::new("rakka-1", "uid-b");
        let endpoint_b = RemoteEndpoint::new(node_b_id.clone());
        endpoint_b
            .register_entity_handler("cart", |_envelope: RemoteEnvelope| Ok(()))
            .expect("entity handler should register");
        let Some(transport_b) = bind_transport(node_b_id.clone(), endpoint_b).await else {
            return;
        };
        let config = test_config().idle_timeout(Duration::from_millis(20));
        let Some(transport_a) = try_bind_transport(
            node_a_id.clone(),
            ClusterProtocol::default(),
            RemoteEndpoint::new(node_a_id.clone()),
            config,
        )
        .await
        else {
            return;
        };
        transport_a
            .register_peer(node(&node_b_id, transport_b.local_addr().port()))
            .expect("node b peer should register");
        transport_b
            .register_peer(node(&node_a_id, transport_a.local_addr().port()))
            .expect("node a peer should register");

        transport_a
            .send(&node_b_id, test_envelope("cart", "cart-1", b"apple"))
            .expect("send should enqueue");
        wait_for(|| {
            transport_a
                .peer_snapshot(&node_b_id)
                .is_some_and(|snapshot| snapshot.lifecycle() == TcpRemoteConnectionLifecycle::Ready)
        })
        .await;
        wait_for(|| {
            transport_a
                .peer_snapshot(&node_b_id)
                .is_some_and(|snapshot| {
                    snapshot.lifecycle() == TcpRemoteConnectionLifecycle::Closed
                })
        })
        .await;
    }

    #[tokio::test]
    async fn remote_failures_are_recorded_to_metrics() {
        let recorder = std::sync::Arc::new(InMemoryMetricsRecorder::new());
        let node_a_id = NodeId::new("rakka-0", "uid-a");
        let node_b_id = NodeId::new("rakka-1", "uid-b");
        let Some(transport_a) = try_bind_transport_with_metrics(
            node_a_id,
            ClusterProtocol::default(),
            RemoteEndpoint::new(NodeId::new("rakka-0", "uid-a2")),
            test_config().outbound_queue_capacity(1),
            recorder.clone(),
        )
        .await
        else {
            return;
        };
        let Some(port) = unused_port() else {
            return;
        };
        transport_a
            .register_peer(node(&node_b_id, port))
            .expect("peer should register");

        for _attempt in 0..16 {
            let _sent = transport_a.send(&node_b_id, test_envelope("cart", "cart-1", b"apple"));
        }

        assert!(recorder.snapshot().counter_total(METRIC_REMOTE_FAILURES) >= 1.0);
    }

    #[tokio::test]
    async fn invalid_zero_queue_capacity_is_rejected_before_binding() {
        let error = TcpRemoteTransport::bind(
            NodeId::new("rakka-0", "uid-a"),
            ClusterProtocol::default(),
            RemoteEndpoint::new(NodeId::new("rakka-0", "uid-a")),
            test_config().outbound_queue_capacity(0),
        )
        .await
        .expect_err("zero queue capacity should fail before tcp bind");

        assert!(matches!(
            error,
            super::TcpRemoteTransportError::InvalidConfig { .. }
        ));
    }

    async fn bind_transport(
        node_id: NodeId,
        endpoint: RemoteEndpoint,
    ) -> Option<TcpRemoteTransport> {
        bind_transport_with_protocol(node_id, ClusterProtocol::default(), endpoint).await
    }

    async fn bind_transport_with_protocol(
        node_id: NodeId,
        protocol: ClusterProtocol,
        endpoint: RemoteEndpoint,
    ) -> Option<TcpRemoteTransport> {
        try_bind_transport(node_id, protocol, endpoint, test_config()).await
    }

    async fn try_bind_transport(
        node_id: NodeId,
        protocol: ClusterProtocol,
        endpoint: RemoteEndpoint,
        config: TcpRemoteTransportConfig,
    ) -> Option<TcpRemoteTransport> {
        match TcpRemoteTransport::bind(node_id, protocol, endpoint, config).await {
            Ok(transport) => Some(transport),
            Err(error) if bind_denied(&error) => {
                eprintln!("skipping tcp remoting test; loopback bind denied: {error}");
                None
            }
            Err(error) => panic!("transport should bind: {error:?}"),
        }
    }

    async fn try_bind_transport_with_metrics(
        node_id: NodeId,
        protocol: ClusterProtocol,
        endpoint: RemoteEndpoint,
        config: TcpRemoteTransportConfig,
        recorder: std::sync::Arc<dyn rakka_core::MetricsRecorder>,
    ) -> Option<TcpRemoteTransport> {
        match TcpRemoteTransport::bind_with_metrics(node_id, protocol, endpoint, config, recorder)
            .await
        {
            Ok(transport) => Some(transport),
            Err(error) if bind_denied(&error) => {
                eprintln!("skipping tcp remoting test; loopback bind denied: {error}");
                None
            }
            Err(error) => panic!("transport should bind: {error:?}"),
        }
    }

    fn test_config() -> TcpRemoteTransportConfig {
        TcpRemoteTransportConfig::new()
            .bind_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .connect_timeout(Duration::from_millis(500))
            .reconnect_backoff(Duration::from_millis(10))
            .idle_timeout(Duration::from_secs(10))
    }

    fn node(node_id: &NodeId, port: u16) -> ClusterNode {
        node_with_protocol(node_id, port, ClusterProtocol::default())
    }

    fn node_with_protocol(node_id: &NodeId, port: u16, protocol: ClusterProtocol) -> ClusterNode {
        ClusterNode::new(node_id.clone(), NodeAddress::new("127.0.0.1", port))
            .with_protocol(protocol)
    }

    fn test_envelope(entity_type: &str, entity_id: &str, payload: &[u8]) -> RemoteEnvelope {
        RemoteEnvelope::new(
            RemoteDestination::Entity {
                entity_type: entity_type.to_string(),
                entity_id: entity_id.to_string(),
            },
            EncodedPayload::new(
                RemoteEnvelopeMetadata::protobuf("rakka.test.Message", 1),
                payload.to_vec(),
            ),
        )
    }

    fn unused_port() -> Option<u16> {
        let listener = match std::net::TcpListener::bind(("127.0.0.1", 0)) {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping tcp remoting test; unused port probe denied: {error}");
                return None;
            }
            Err(error) => panic!("unused port probe should bind: {error}"),
        };
        Some(
            listener
                .local_addr()
                .expect("local address should exist")
                .port(),
        )
    }

    fn bind_denied(error: &super::TcpRemoteTransportError) -> bool {
        matches!(
            error,
            super::TcpRemoteTransportError::Io { message }
                if message.contains("Operation not permitted")
                    || message.contains("Permission denied")
        )
    }

    async fn wait_for(mut condition: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if condition() {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for condition"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
