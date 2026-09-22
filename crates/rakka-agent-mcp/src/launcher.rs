//! The child-process transport seam and, behind feature `child-process`, the
//! unsandboxed reference launcher.
//!
//! No MCP server runs as a child process unless the deployment supplies the
//! launcher that starts it. This crate declares only the seam — the trait, the
//! stdio pair it hands back, and the refusals it may raise — so that a
//! deployment which runs tools inside its own sandbox implements
//! [`McpChildProcessLauncher`] over that sandbox and never enables a feature
//! that spawns processes directly.
//!
//! The launcher is given the run scope and the durable
//! [`ArtifactRef`] of the launch
//! specification. It never sees a resolved credential: a child-process server
//! has no request to put a header on, so the credential path of the Streamable
//! HTTP transport simply does not exist here.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use rakka_agent::AgentRunScope;
use rakka_agent_workflow::ArtifactRef;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// One launched child process's stdio pair, as rmcp's transport consumes it.
///
/// Boxed and dynamic on purpose: what a deployment's sandbox hands back may be
/// a pipe, a socket, or a stream tunnelled through its own supervisor, and the
/// seam should not name any of them.
pub struct McpChildTransport {
    /// The child's standard output, read by the client.
    pub reader: Box<dyn AsyncRead + Send + Unpin>,
    /// The child's standard input, written by the client.
    pub writer: Box<dyn AsyncWrite + Send + Unpin>,
}

impl McpChildTransport {
    /// Pairs a launched child's output and input.
    #[must_use]
    pub fn new(
        reader: Box<dyn AsyncRead + Send + Unpin>,
        writer: Box<dyn AsyncWrite + Send + Unpin>,
    ) -> Self {
        Self { reader, writer }
    }

    /// The pair rmcp's `(R, W)` transport is built from, as two *concrete*
    /// types.
    ///
    /// The wrapping is not cosmetic. rmcp's `Transport` methods return
    /// `-> impl Future + Send` (`rmcp-3.4.0/src/transport.rs:140`), and when
    /// the transport's type parameters are trait objects, the resulting
    /// opaque types leave rustc asking for a *higher-ranked* `Send` bound
    /// ("implementation of `Send` is not general enough") that no caller can
    /// give — which would make a child-process session impossible to await
    /// inside the dispatch executor's own `Send` future. Naming a concrete
    /// type for each half, whose auto traits rustc can compute outright,
    /// is what keeps the seam dynamic and the session awaitable at once.
    pub(crate) fn into_pair(self) -> (McpChildReader, McpChildWriter) {
        (McpChildReader(self.reader), McpChildWriter(self.writer))
    }
}

/// A launched child's output, as one concrete type. See
/// [`McpChildTransport::into_pair`].
pub(crate) struct McpChildReader(Box<dyn AsyncRead + Send + Unpin>);

impl AsyncRead for McpChildReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

/// A launched child's input, as one concrete type. See
/// [`McpChildTransport::into_pair`].
pub(crate) struct McpChildWriter(Box<dyn AsyncWrite + Send + Unpin>);

impl AsyncWrite for McpChildWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

/// Boxed future returned by a [`McpChildProcessLauncher`].
pub type McpLaunchFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, McpLaunchError>> + Send + 'a>>;

/// Why a child-process MCP server could not be launched.
///
/// No variant carries the specification's contents, an environment value, or a
/// command line: a launch refusal is an operator-facing fact about the
/// deployment, never a window into what the process was about to be given.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum McpLaunchError {
    /// The launch specification could not be read from its artifact.
    SpecUnreadable {
        /// Why the specification could not be read.
        reason: String,
    },
    /// The process could not be started.
    SpawnFailed {
        /// Why the process could not be started.
        reason: String,
    },
    /// This build, or this deployment's launcher, does not run child
    /// processes at all.
    Unsupported,
}

impl McpLaunchError {
    /// Stable, machine-readable error code.
    ///
    /// One code for every variant: to an operator, each of them is the same
    /// fact — this deployment did not start the server the binding names — and
    /// the same action, which is to fix the launcher or the specification. The
    /// message says which of the three it was.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        "mcp-transport-unsupported"
    }
}

impl Display for McpLaunchError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::SpecUnreadable { reason } => write!(
                f,
                "the MCP child-process launch specification could not be read: {reason}"
            ),
            Self::SpawnFailed { reason } => {
                write!(f, "the MCP child process could not be started: {reason}")
            }
            Self::Unsupported => f.write_str("this deployment does not launch MCP child processes"),
        }
    }
}

impl Error for McpLaunchError {}

/// Starts one child-process MCP server and hands back its stdio pair.
///
/// The deployment owns everything this crate deliberately does not: the
/// sandbox, the user, the filesystem and network namespace, the environment,
/// and the process's lifetime after the transport closes.
pub trait McpChildProcessLauncher: Send + Sync + 'static {
    /// Launches the server the specification names, for one attempt of one
    /// run.
    ///
    /// # Errors
    ///
    /// [`McpLaunchError`] with its stable code.
    fn launch<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        spec: &'a ArtifactRef,
    ) -> McpLaunchFuture<'a, McpChildTransport>;
}
