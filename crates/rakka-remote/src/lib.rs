#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Remote envelopes, typed transport errors, and payload serialization registry.

pub mod actor_ref;
pub mod clustered_receptionist;
pub mod endpoint;
pub mod envelope;
pub mod error;
pub mod network;
mod proto;
pub mod receptionist;
pub mod registry;
pub mod request;
pub mod shutdown;
pub mod transport;

use rakka_core::Subsystem;

pub use actor_ref::{RemoteActorRefInbound, RemoteActorRefInboundError};
pub use clustered_receptionist::{
    RemoteClusteredReceptionist, RemoteClusteredReceptionistError,
    RemoteClusteredReceptionistResult,
};
pub use endpoint::{
    RemoteEndpoint, RemoteEndpointError, RemoteEndpointResult, RemoteEnvelopeHandler,
};
pub use envelope::{
    EncodedPayload, ProtobufEnvelopeCodec, RemoteActorRef, RemoteDestination, RemoteEnvelope,
    RemoteEnvelopeMetadata,
};
pub use error::{RemoteError, RemoteResult};
pub use network::{
    TcpRemoteConnectionLifecycle, TcpRemoteHandshake, TcpRemotePeerSnapshot, TcpRemoteTransport,
    TcpRemoteTransportConfig, TcpRemoteTransportError, TcpRemoteTransportResult,
    TcpRemoteTransportSnapshot, DEFAULT_REMOTE_ENVELOPE_VERSION, DEFAULT_TCP_REMOTE_BIND_ADDR,
    DEFAULT_TCP_REMOTE_CONNECT_TIMEOUT, DEFAULT_TCP_REMOTE_IDLE_TIMEOUT,
    DEFAULT_TCP_REMOTE_MAX_FRAME_BYTES, DEFAULT_TCP_REMOTE_OUTBOUND_QUEUE_CAPACITY,
    DEFAULT_TCP_REMOTE_PORT, DEFAULT_TCP_REMOTE_RECONNECT_BACKOFF,
    METRIC_TCP_REMOTE_CONNECTION_STATE, METRIC_TCP_REMOTE_RECEIVES, METRIC_TCP_REMOTE_RECONNECTS,
    METRIC_TCP_REMOTE_SENDS, TCP_REMOTE_REQUIRES_REGISTERED_PEERS,
};
pub use receptionist::{
    RemoteReceptionistListing, RemoteReceptionistListingCodec, RemoteServiceProxy,
    RemoteServiceProxyError, RemoteServiceProxyRegistry, RemoteServiceProxyRegistrySnapshot,
    RemoteServiceProxyResult, RemoteServiceRoutee, RemoteServiceRouteeKey,
    REMOTE_RECEPTIONIST_LISTING_CODEC_ID, REMOTE_RECEPTIONIST_LISTING_MESSAGE_TYPE_ID,
    REMOTE_RECEPTIONIST_LISTING_SCHEMA_VERSION,
};
pub use registry::{
    CodecKey, PayloadCodec, ProtobufMessage, ProtobufPayloadCodec, SchemaCompatibilityPolicy,
    SerializationRegistry,
};
pub use request::{
    RemotePendingReply, RemoteRequestError, RemoteRequestRegistry, RemoteRequestResult,
};
pub use shutdown::{
    register_remote_service_proxy_expire_task, register_remote_service_proxy_remove_node_task,
    register_tcp_remote_drain_task, register_tcp_remote_force_close_task,
};
pub use transport::{
    InMemoryRemoteTransport, RemoteTransport, RemoteTransportError, RemoteTransportResult,
};

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-remote";

/// Default codec id for remote payload messages.
pub const DEFAULT_CODEC_ID: &str = "protobuf";

/// Subsystem associated with remoting.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Remote
}
