//! Where an executor writes a tool result too large to stay inline.
//!
//! [`McpArtifactSink`] is the write seam. It takes `&self` and is `Sync`, so
//! one sink is shared by every concurrent attempt without a lock of this
//! crate's making, and it is handed the run scope, because a deployment's
//! store files an artifact under a tenant and a run and the write request
//! names neither.
//!
//! [`AgentArtifactStore`](rakka_agent_workflow::AgentArtifactStore) cannot be
//! that seam: `put_artifact` takes `&mut self` and the trait is `Send` but
//! not `Sync`. A store of that shape still works — an
//! [`McpArtifactStore`](crate::McpArtifactStore) converts into
//! [`McpArtifacts`], behind the same async mutex as before.
//!
//! The executor **requests** an artifact id and does not require it back. A
//! content-addressed store mints its own and ignores the request; what the
//! run records is the reference the sink returned, once it passes
//! [`validate_artifact_ref`](rakka_agent_workflow::validate_artifact_ref).

use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rakka_agent::AgentRunScope;
use rakka_agent_workflow::{AgentArtifactError, AgentArtifactWriteRequest, ArtifactRef};

use crate::executor::McpArtifactStore;

/// What one artifact write answers.
pub type McpArtifactFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ArtifactRef, AgentArtifactError>> + Send + 'a>>;

/// The store an executor writes an over-large tool result to.
///
/// # Contract
///
/// - `scope` is the run the attempt belongs to. It is the only source of the
///   tenant and the run a store may file the artifact under; nothing in
///   `request` names them.
/// - `request.artifact_id` is a request. It is derived from the effect, its
///   generation, and the call, so a store that honors it rewrites one
///   artifact when an attempt is re-driven. A store that mints its own id
///   must be idempotent on the bytes instead.
/// - The reference returned must pass the default artifact policy: a
///   checksum, a content type, a byte length, a retention class, and a
///   redaction status other than `Unknown`. The executor refuses one that
///   does not, under the artifact error's own code.
/// - The write runs inside the attempt's deadline.
/// - An error's text is persisted, bounded, on the outbox row and the fleet
///   index. It must carry no credential and no content.
pub trait McpArtifactSink: Send + Sync {
    /// Writes one tool result and answers its durable reference.
    fn put_result<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        request: AgentArtifactWriteRequest,
    ) -> McpArtifactFuture<'a>;
}

/// The artifact write path an executor is built with.
///
/// Built from a sink with [`McpArtifacts::sink`], or converted from an
/// [`McpArtifactStore`] with `into()`.
#[derive(Clone)]
pub struct McpArtifacts(Arc<dyn McpArtifactSink>);

impl McpArtifacts {
    /// Wraps one deployment-owned sink.
    #[must_use]
    pub fn sink<S>(sink: S) -> Self
    where
        S: McpArtifactSink + 'static,
    {
        Self(Arc::new(sink))
    }

    /// Writes through the wrapped sink.
    pub(crate) fn put_result<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        request: AgentArtifactWriteRequest,
    ) -> McpArtifactFuture<'a> {
        self.0.put_result(scope, request)
    }
}

impl Debug for McpArtifacts {
    /// Opaque: a sink is a deployment's own type, and nothing about it
    /// belongs in a log line.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("McpArtifacts(..)")
    }
}

impl From<Arc<dyn McpArtifactSink>> for McpArtifacts {
    fn from(sink: Arc<dyn McpArtifactSink>) -> Self {
        Self(sink)
    }
}

impl From<McpArtifactStore> for McpArtifacts {
    fn from(store: McpArtifactStore) -> Self {
        Self(Arc::new(StoreSink { store }))
    }
}

/// An [`McpArtifactStore`] as a sink: the scope is dropped, because
/// `put_artifact` has nowhere to take it, and writes serialize behind the
/// store's mutex.
struct StoreSink {
    store: McpArtifactStore,
}

impl McpArtifactSink for StoreSink {
    fn put_result<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        request: AgentArtifactWriteRequest,
    ) -> McpArtifactFuture<'a> {
        Box::pin(async move {
            let mut store = self.store.lock().await;
            store.put_artifact(request).await
        })
    }
}
