//! Remote request/reply correlation for ask-style interactions.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::oneshot;

use crate::{
    RemoteDestination, RemoteEndpointError, RemoteEndpointResult, RemoteEnvelope,
    RemoteEnvelopeHandler, RemoteError, SerializationRegistry,
};

/// Convenient result alias for remote request operations.
pub type RemoteRequestResult<T> = Result<T, RemoteRequestError>;

/// Failure returned by remote request/reply correlation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteRequestError {
    /// A pending request already exists for this id.
    DuplicateRequestId {
        /// Request id.
        request_id: String,
    },
    /// No pending request exists for this id.
    UnknownRequestId {
        /// Request id.
        request_id: String,
    },
    /// Reply envelope did not target a reply destination.
    UnexpectedDestination {
        /// Destination carried by the envelope.
        destination: RemoteDestination,
    },
    /// Reply envelope had no request id metadata.
    MissingRequestId,
    /// Reply destination and envelope metadata carried different request ids.
    RequestIdMismatch {
        /// Request id in the reply destination.
        destination_request_id: String,
        /// Request id in envelope metadata.
        envelope_request_id: String,
    },
    /// Reply payload could not be decoded.
    Decode {
        /// Decode failure reported by the serialization registry.
        error: RemoteError,
    },
    /// Pending reply receiver was dropped before completion.
    ReplyDropped,
    /// Timed out waiting for the reply.
    Timeout,
}

impl Display for RemoteRequestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateRequestId { request_id } => {
                write!(f, "remote request {request_id} is already pending")
            }
            Self::UnknownRequestId { request_id } => {
                write!(f, "remote request {request_id} is not pending")
            }
            Self::UnexpectedDestination { destination } => {
                write!(f, "remote reply handler cannot accept {destination:?}")
            }
            Self::MissingRequestId => f.write_str("remote reply envelope is missing request_id"),
            Self::RequestIdMismatch {
                destination_request_id,
                envelope_request_id,
            } => write!(
                f,
                "remote reply destination request {destination_request_id} does not match envelope request {envelope_request_id}"
            ),
            Self::Decode { error } => write!(f, "remote reply decode failed: {error}"),
            Self::ReplyDropped => f.write_str("remote reply receiver was dropped"),
            Self::Timeout => f.write_str("remote request timed out"),
        }
    }
}

impl Error for RemoteRequestError {}

/// Registry of pending remote replies for one local endpoint.
#[derive(Clone)]
pub struct RemoteRequestRegistry {
    registry: SerializationRegistry,
    pending: Arc<Mutex<BTreeMap<String, Arc<dyn PendingRemoteReply>>>>,
    sequence: Arc<AtomicU64>,
}

impl RemoteRequestRegistry {
    /// Creates an empty request registry backed by a serialization registry.
    #[must_use]
    pub fn new(registry: SerializationRegistry) -> Self {
        Self {
            registry,
            pending: Arc::new(Mutex::new(BTreeMap::new())),
            sequence: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Returns the serialization registry used to decode reply payloads.
    #[must_use]
    pub const fn registry(&self) -> &SerializationRegistry {
        &self.registry
    }

    /// Allocates a monotonically increasing request id with a stable prefix.
    #[must_use]
    pub fn next_request_id(&self, prefix: impl AsRef<str>) -> String {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        format!("{}-{sequence}", prefix.as_ref())
    }

    /// Registers a typed pending reply.
    pub fn register<R>(
        &self,
        request_id: impl Into<String>,
    ) -> RemoteRequestResult<RemotePendingReply<R>>
    where
        R: Send + Sync + 'static,
    {
        let request_id = request_id.into();
        let (sender, receiver) = oneshot::channel();
        let pending = Arc::new(TypedPendingRemoteReply::<R> {
            sender: Mutex::new(Some(sender)),
        });
        let mut pending_replies = self
            .pending
            .lock()
            .expect("remote request registry mutex poisoned");

        if pending_replies.contains_key(&request_id) {
            return Err(RemoteRequestError::DuplicateRequestId { request_id });
        }

        pending_replies.insert(request_id.clone(), pending);
        Ok(RemotePendingReply {
            request_id,
            registry: self.clone(),
            receiver,
        })
    }

    /// Completes a pending reply from an inbound reply envelope.
    pub fn complete_reply(&self, envelope: RemoteEnvelope) -> RemoteRequestResult<()> {
        let request_id = reply_request_id(&envelope)?;
        let pending = self
            .pending
            .lock()
            .expect("remote request registry mutex poisoned")
            .remove(&request_id)
            .ok_or_else(|| RemoteRequestError::UnknownRequestId {
                request_id: request_id.clone(),
            })?;
        pending.complete(&self.registry, &envelope)
    }

    /// Removes a pending request without completing it.
    #[must_use]
    pub fn remove(&self, request_id: &str) -> bool {
        self.pending
            .lock()
            .expect("remote request registry mutex poisoned")
            .remove(request_id)
            .is_some()
    }

    /// Number of currently pending replies.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending
            .lock()
            .expect("remote request registry mutex poisoned")
            .len()
    }
}

impl RemoteEnvelopeHandler for RemoteRequestRegistry {
    fn handle(&self, envelope: RemoteEnvelope) -> RemoteEndpointResult<()> {
        let destination = envelope.destination.clone();
        self.complete_reply(envelope)
            .map_err(|error| RemoteEndpointError::HandlerRejected {
                destination,
                message: error.to_string(),
            })
    }
}

impl Debug for RemoteRequestRegistry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteRequestRegistry")
            .field("pending_count", &self.pending_count())
            .finish_non_exhaustive()
    }
}

/// Future-like handle for one pending remote reply.
pub struct RemotePendingReply<R>
where
    R: Send + Sync + 'static,
{
    request_id: String,
    registry: RemoteRequestRegistry,
    receiver: oneshot::Receiver<RemoteRequestResult<R>>,
}

impl<R> RemotePendingReply<R>
where
    R: Send + Sync + 'static,
{
    /// Request id associated with this pending reply.
    #[must_use]
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    /// Waits for the reply and removes the request on timeout.
    pub async fn wait(self, timeout: Duration) -> RemoteRequestResult<R> {
        let request_id = self.request_id.clone();
        match tokio::time::timeout(timeout, self.receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_closed)) => Err(RemoteRequestError::ReplyDropped),
            Err(_elapsed) => {
                let _ = self.registry.remove(&request_id);
                Err(RemoteRequestError::Timeout)
            }
        }
    }
}

impl<R> Debug for RemotePendingReply<R>
where
    R: Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemotePendingReply")
            .field("request_id", &self.request_id)
            .finish_non_exhaustive()
    }
}

trait PendingRemoteReply: Send + Sync {
    fn complete(
        &self,
        registry: &SerializationRegistry,
        envelope: &RemoteEnvelope,
    ) -> RemoteRequestResult<()>;
}

struct TypedPendingRemoteReply<R>
where
    R: Send + Sync + 'static,
{
    sender: Mutex<Option<oneshot::Sender<RemoteRequestResult<R>>>>,
}

impl<R> PendingRemoteReply for TypedPendingRemoteReply<R>
where
    R: Send + Sync + 'static,
{
    fn complete(
        &self,
        registry: &SerializationRegistry,
        envelope: &RemoteEnvelope,
    ) -> RemoteRequestResult<()> {
        let reply = registry
            .decode_envelope::<R>(envelope)
            .map_err(|error| RemoteRequestError::Decode { error });
        let result_for_endpoint = reply.as_ref().map(|_reply| ()).map_err(Clone::clone);
        let sender = self
            .sender
            .lock()
            .expect("remote pending reply mutex poisoned")
            .take()
            .ok_or(RemoteRequestError::ReplyDropped)?;
        sender
            .send(reply)
            .map_err(|_reply| RemoteRequestError::ReplyDropped)?;
        result_for_endpoint
    }
}

fn reply_request_id(envelope: &RemoteEnvelope) -> RemoteRequestResult<String> {
    let destination_request_id = match &envelope.destination {
        RemoteDestination::Reply { request_id } => request_id,
        destination => {
            return Err(RemoteRequestError::UnexpectedDestination {
                destination: destination.clone(),
            });
        }
    };
    let envelope_request_id = envelope
        .request_id
        .as_ref()
        .ok_or(RemoteRequestError::MissingRequestId)?;

    if destination_request_id != envelope_request_id {
        return Err(RemoteRequestError::RequestIdMismatch {
            destination_request_id: destination_request_id.clone(),
            envelope_request_id: envelope_request_id.clone(),
        });
    }

    Ok(destination_request_id.clone())
}
