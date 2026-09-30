//! The child-process transport seam and, behind feature `child-process`, the
//! unsandboxed reference launcher.
//!
//! No MCP server runs as a child process unless the deployment supplies the
//! launcher that starts it. This crate declares only the seam — the trait, the
//! stdio it hands back, and the refusals it may raise — so that a
//! deployment which runs tools inside its own sandbox implements
//! [`McpChildProcessLauncher`] over that sandbox and never enables a feature
//! that spawns processes directly.
//!
//! The launcher is given the run scope and the durable
//! [`ArtifactRef`] of the launch
//! specification. It never sees a resolved credential: a child-process server
//! has no request to put a header on, so the credential path of the Streamable
//! HTTP transport simply does not exist here — the dispatch executor refuses
//! an attempt that arrives with one before a launcher is asked for anything.
//!
//! Every launched transport must take its process down with it. An effect's
//! deadline that fires mid-handshake drops the transport without closing it,
//! so a launcher whose process outlived the drop would leak one server per
//! timed-out attempt. The same holds for the launch future, if it is dropped
//! before returning: `launch` is cancelled by the same deadline, so a process
//! must be armed to die with the future that spawned it (kill-on-drop or
//! equivalent) before the first await.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use rakka_agent::AgentRunScope;
use rakka_agent_workflow::ArtifactRef;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// One launched child process's stdio, as rmcp's transport consumes it.
///
/// [`Self::Pair`] is the seam a deployment fills: boxed and dynamic on
/// purpose, because what a sandbox hands back may be a pipe, a socket, or a
/// stream tunnelled through its own supervisor, and the seam should not name
/// any of them. [`McpChildTransport::new`] builds one.
///
/// `Process` (feature `child-process`) is the reference launcher's own
/// transport: rmcp's `TokioChildProcess` owns the child together with its
/// stdio and exposes no way to take the two pipes out of it
/// (`rmcp-3.4.0/src/transport/child_process.rs:36`–`39`, private fields; it
/// only implements `Transport`, `:168`), so it is carried whole rather than
/// split into a pair.
///
/// Non-exhaustive because `Process` exists only in builds that enable
/// `child-process`: a downstream `match` must not stop compiling when a
/// different crate in the same graph turns the feature on.
#[non_exhaustive]
pub enum McpChildTransport {
    /// A child's standard output and standard input.
    Pair {
        /// The child's standard output, read by the client.
        reader: Box<dyn AsyncRead + Send + Unpin>,
        /// The child's standard input, written by the client.
        writer: Box<dyn AsyncWrite + Send + Unpin>,
    },
    /// A child spawned by rmcp's own process transport, which kills the
    /// process when it is dropped.
    ///
    /// Dropping one needs a Tokio runtime: rmcp's drop hands the kill to a
    /// spawned task (`rmcp-3.4.0/src/transport/child_process.rs:45`–`57`), and
    /// `tokio::spawn` panics outside one. The dispatch executor always drops
    /// it inside the runtime its attempt runs on.
    #[cfg(feature = "child-process")]
    Process(rmcp::transport::TokioChildProcess),
}

impl fmt::Debug for McpChildTransport {
    /// The variant alone: neither half of a pair, nor rmcp's process
    /// transport, has anything to show that is not a live handle.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pair { .. } => f.write_str("McpChildTransport::Pair"),
            #[cfg(feature = "child-process")]
            Self::Process(_) => f.write_str("McpChildTransport::Process"),
        }
    }
}

impl McpChildTransport {
    /// Pairs a launched child's output and input.
    #[must_use]
    pub fn new(
        reader: Box<dyn AsyncRead + Send + Unpin>,
        writer: Box<dyn AsyncWrite + Send + Unpin>,
    ) -> Self {
        Self::Pair { reader, writer }
    }
}

/// The pair rmcp's `(R, W)` transport is built from, as two *concrete*
/// types.
///
/// The wrapping is not cosmetic. rmcp's `Transport` methods return
/// `-> impl Future + Send` (`rmcp-3.4.0/src/transport.rs:140`), and when the
/// transport's type parameters are trait objects, the resulting opaque types
/// leave rustc asking for a *higher-ranked* `Send` bound ("implementation of
/// `Send` is not general enough") that no caller can give — which would make a
/// child-process session impossible to await inside the dispatch executor's
/// own `Send` future. Naming a concrete type for each half, whose auto traits
/// rustc can compute outright, is what keeps the seam dynamic and the session
/// awaitable at once.
pub(crate) fn concrete_pair(
    reader: Box<dyn AsyncRead + Send + Unpin>,
    writer: Box<dyn AsyncWrite + Send + Unpin>,
) -> (McpChildReader, McpChildWriter) {
    (McpChildReader(reader), McpChildWriter(writer))
}

/// A launched child's output, as one concrete type. See [`concrete_pair`].
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

/// A launched child's input, as one concrete type. See [`concrete_pair`].
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

/// Starts one child-process MCP server and hands back its stdio.
///
/// The deployment owns everything this crate deliberately does not: the
/// sandbox, the user, the filesystem and network namespace, and the
/// environment.
///
/// **A launched process must not outlive the transport it returned** — nor
/// the launch future, if it is dropped before returning. The executor closes
/// a session on every path it controls, but an effect's deadline that fires
/// during the handshake abandons the attempt by dropping the transport, with
/// no close at all. A launcher therefore ties the process's life to the
/// transport's own — kill on drop, as `tokio::process::Command::kill_on_drop`
/// does — rather than to a close it may never receive. And `launch` is
/// cancelled by the same deadline, so a process must be armed to die with the
/// future that spawned it (kill-on-drop or equivalent) before the first
/// await: a launcher that spawns and then waits for a readiness signal would
/// otherwise leak one process per attempt that timed out while it waited.
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

#[cfg(feature = "child-process")]
pub use reference::{TokioChildProcessLauncher, MCP_LAUNCH_SPEC_MAX_BYTES};

/// The unsandboxed reference launcher (feature `child-process`).
#[cfg(feature = "child-process")]
mod reference {
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::process::Stdio;

    use rakka_agent::{AgentContentDigest, AgentRunScope};
    use rakka_agent_workflow::ArtifactRef;
    use rmcp::transport::TokioChildProcess;
    use serde::Deserialize;
    use serde_json::error::Category;

    use super::{McpChildProcessLauncher, McpChildTransport, McpLaunchError, McpLaunchFuture};
    use crate::executor::McpArtifactStore;

    /// The largest launch specification [`TokioChildProcessLauncher`] will
    /// decode, in bytes. A specification is a command, its arguments, and an
    /// environment: anything larger is not one.
    pub const MCP_LAUNCH_SPEC_MAX_BYTES: usize = 64 * 1024;

    /// Launches the command a binding's launch specification names, directly
    /// on the host, as rmcp's own child-process transport.
    ///
    /// **Unsandboxed, and for tests and examples only.** The process runs as
    /// this process's user, in its filesystem and network namespace, with
    /// nothing between the server and the host but the operating system. A
    /// deployment that runs third-party MCP servers implements
    /// [`McpChildProcessLauncher`] over its own sandbox and leaves the
    /// `child-process` feature off.
    ///
    /// The specification is the JSON artifact `spec` refers to:
    /// `{ "command": "...", "args": ["..."], "env": { "NAME": "value" } }`,
    /// with `args` and `env` optional and no other field admitted. Before it
    /// is decoded it must be at most [`MCP_LAUNCH_SPEC_MAX_BYTES`], and when
    /// `spec` carries a checksum it must be `sha256:<hex>` and match the bytes
    /// read — a checksum this launcher cannot verify is refused, not skipped.
    ///
    /// **The specification is durable artifact content, so it must carry no
    /// secret material** — not in `args`, and not in `env`. A secret reaches
    /// an MCP server only over the Streamable HTTP transport's credential
    /// path, resolved per attempt and never stored; a child process has no
    /// such path, and a binding that names a credential for one is refused.
    ///
    /// What the process is not given is as deliberate as what it is:
    ///
    /// - **No inherited environment.** `env` is the child's *entire*
    ///   environment. A server binary never sees this process's variables —
    ///   among them, whatever credentials the host was started with. Every
    ///   name must be non-empty and free of `=`.
    /// - **No implicit command resolution.** With the environment cleared,
    ///   the operating system's fallback would search libc's default path for
    ///   a bare name, and a relative path would resolve against the host's
    ///   working directory. So the command must be absolute, or a bare name
    ///   with a `PATH` in `env` to search.
    /// - **No standard error.** It is discarded rather than inherited, so a
    ///   server's diagnostics never interleave with, or leak into, the host's
    ///   own log stream.
    /// - **No life after the transport, or after the launch.** The process is
    ///   killed when the transport is dropped — by `kill_on_drop` on the
    ///   command, and by rmcp's own drop of `TokioChildProcess`
    ///   (`rmcp-3.4.0/src/transport/child_process.rs:45`) — and nothing is
    ///   awaited between the spawn and the return, so a cancelled launch
    ///   cannot strand one either.
    ///
    /// No refusal repeats the command, an argument, an environment name or
    /// value, or a checksum.
    pub struct TokioChildProcessLauncher {
        artifacts: McpArtifactStore,
    }

    impl TokioChildProcessLauncher {
        /// A launcher that reads launch specifications from `artifacts`.
        #[must_use]
        pub fn new(artifacts: McpArtifactStore) -> Self {
            Self { artifacts }
        }
    }

    /// The launch specification's stored shape.
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct LaunchSpec {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: BTreeMap<String, String>,
    }

    impl McpChildProcessLauncher for TokioChildProcessLauncher {
        fn launch<'a>(
            &'a self,
            _scope: &'a AgentRunScope,
            spec: &'a ArtifactRef,
        ) -> McpLaunchFuture<'a, McpChildTransport> {
            Box::pin(async move {
                let read = {
                    let store = self.artifacts.lock().await;
                    store.get_artifact(spec).await
                }
                .map_err(|error| McpLaunchError::SpecUnreadable {
                    reason: format!("the artifact store refused the read ({})", error.code()),
                })?;
                let launch = decode(spec, &read.bytes)?;
                let mut command = tokio::process::Command::new(&launch.command);
                command
                    .args(&launch.args)
                    .env_clear()
                    .envs(&launch.env)
                    .kill_on_drop(true);
                // The error *kind* only: an I/O error's own text can carry the
                // path the operating system failed on. No await follows the
                // spawn, so the process is never held by a future that could
                // be cancelled before the transport owns it.
                let (process, _no_stderr) = TokioChildProcess::builder(command)
                    .stderr(Stdio::null())
                    .spawn()
                    .map_err(|error| McpLaunchError::SpawnFailed {
                        reason: error.kind().to_string(),
                    })?;
                Ok(McpChildTransport::Process(process))
            })
        }
    }

    /// Bounds, verifies, decodes, and checks one stored specification.
    fn decode(spec: &ArtifactRef, bytes: &[u8]) -> Result<LaunchSpec, McpLaunchError> {
        let unreadable = |reason: &str| McpLaunchError::SpecUnreadable {
            reason: reason.to_string(),
        };
        if bytes.len() > MCP_LAUNCH_SPEC_MAX_BYTES {
            return Err(McpLaunchError::SpecUnreadable {
                reason: format!(
                    "the specification is larger than {MCP_LAUNCH_SPEC_MAX_BYTES} bytes"
                ),
            });
        }
        if let Some(checksum) = &spec.checksum {
            let Some(expected) = checksum.strip_prefix("sha256:") else {
                return Err(unreadable(
                    "the specification's checksum is not a sha256 digest this launcher can verify",
                ));
            };
            let actual = AgentContentDigest::sha256_of_bytes(bytes).value;
            if !expected.eq_ignore_ascii_case(&actual) {
                return Err(unreadable(
                    "the specification does not match its reference's checksum",
                ));
            }
        }
        // Never serde's own message: a type mismatch quotes the value it met,
        // and that value is a command, an argument, or an environment entry.
        let launch: LaunchSpec = serde_json::from_slice(bytes)
            .map_err(|error| unreadable(shape_reason(error.classify())))?;
        if launch.command.is_empty() {
            return Err(unreadable("the specification names no command"));
        }
        if !Path::new(&launch.command).is_absolute() {
            if launch.command.contains('/') {
                return Err(unreadable(
                    "command must be absolute: a relative path would resolve against the \
                     host's working directory",
                ));
            }
            if !launch.env.contains_key("PATH") {
                return Err(unreadable("command must be absolute or PATH must be set"));
            }
        }
        if launch
            .env
            .keys()
            .any(|name| name.is_empty() || name.contains('='))
        {
            return Err(unreadable(
                "an environment name in the specification is empty or contains '='",
            ));
        }
        Ok(launch)
    }

    /// Why a specification did not decode, by serde's error category alone.
    const fn shape_reason(category: Category) -> &'static str {
        match category {
            Category::Syntax | Category::Eof => "the specification is not valid JSON",
            Category::Data => "the specification is not a { command, args, env } object of strings",
            Category::Io => "the specification could not be read",
        }
    }
}
