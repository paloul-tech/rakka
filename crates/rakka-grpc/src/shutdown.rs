//! Coordinated shutdown helpers for gRPC servers.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use rakka_core::{
    CoordinatedShutdown, RakkaResult, ShutdownPhase, ShutdownTask, ShutdownTaskOptions,
};
use tokio::sync::watch;

/// Cloneable shutdown handle shared between coordinated shutdown and gRPC server tasks.
#[derive(Clone)]
pub struct GrpcShutdownHandle {
    inner: Arc<GrpcShutdownState>,
}

impl GrpcShutdownHandle {
    /// Creates a shutdown handle with no shutdown request recorded.
    #[must_use]
    pub fn new() -> Self {
        let (sender, _receiver) = watch::channel(false);
        Self {
            inner: Arc::new(GrpcShutdownState {
                requested: AtomicBool::new(false),
                sender,
                server_result: Mutex::new(None),
            }),
        }
    }

    /// Creates a signal future source for a gRPC server.
    #[must_use]
    pub fn signal(&self) -> GrpcShutdownSignal {
        GrpcShutdownSignal {
            receiver: self.inner.sender.subscribe(),
        }
    }

    /// Requests graceful shutdown and wakes every signal waiter.
    pub fn request_shutdown(&self) {
        self.inner.requested.store(true, Ordering::Release);
        self.inner.sender.send_replace(true);
    }

    /// Returns true after shutdown has been requested.
    #[must_use]
    pub fn is_shutdown_requested(&self) -> bool {
        self.inner.requested.load(Ordering::Acquire)
    }

    /// Records a successful gRPC server completion.
    pub fn record_server_completed(&self) {
        *self
            .inner
            .server_result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(GrpcServerShutdownResult::Completed);
    }

    /// Records a gRPC server failure.
    pub fn record_server_failed(&self, message: impl Into<String>) {
        *self
            .inner
            .server_result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(GrpcServerShutdownResult::Failed {
                message: message.into(),
            });
    }

    /// Returns a point-in-time shutdown snapshot.
    #[must_use]
    pub fn snapshot(&self) -> GrpcShutdownSnapshot {
        GrpcShutdownSnapshot::new(
            self.is_shutdown_requested(),
            self.inner
                .server_result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        )
    }
}

impl Default for GrpcShutdownHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for GrpcShutdownHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcShutdownHandle")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

/// Future source passed to a gRPC server graceful shutdown hook.
pub struct GrpcShutdownSignal {
    receiver: watch::Receiver<bool>,
}

impl GrpcShutdownSignal {
    /// Waits until shutdown is requested or the signal channel closes.
    pub async fn wait(mut self) {
        loop {
            if *self.receiver.borrow() {
                break;
            }

            if self.receiver.changed().await.is_err() {
                break;
            }
        }
    }
}

impl std::fmt::Debug for GrpcShutdownSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcShutdownSignal")
            .field("requested", &*self.receiver.borrow())
            .finish()
    }
}

/// Point-in-time gRPC shutdown state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcShutdownSnapshot {
    shutdown_requested: bool,
    server_result: Option<GrpcServerShutdownResult>,
}

impl GrpcShutdownSnapshot {
    /// Creates a gRPC shutdown snapshot.
    #[must_use]
    pub const fn new(
        shutdown_requested: bool,
        server_result: Option<GrpcServerShutdownResult>,
    ) -> Self {
        Self {
            shutdown_requested,
            server_result,
        }
    }

    /// Returns true after shutdown has been requested.
    #[must_use]
    pub const fn shutdown_requested(&self) -> bool {
        self.shutdown_requested
    }

    /// Last recorded gRPC server result, if any.
    #[must_use]
    pub const fn server_result(&self) -> Option<&GrpcServerShutdownResult> {
        self.server_result.as_ref()
    }
}

/// Recorded result of a gRPC server task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrpcServerShutdownResult {
    /// Server completed without returning an error.
    Completed,
    /// Server returned an error.
    Failed {
        /// Human-readable failure detail.
        message: String,
    },
}

struct GrpcShutdownState {
    requested: AtomicBool,
    sender: watch::Sender<bool>,
    server_result: Mutex<Option<GrpcServerShutdownResult>>,
}

/// Registers a stop-ingress task that requests gRPC graceful shutdown.
pub fn register_grpc_shutdown_task(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    handle: GrpcShutdownHandle,
) -> RakkaResult<ShutdownTask> {
    shutdown.add_task_with_options(
        ShutdownPhase::stop_ingress(),
        task_name,
        ShutdownTaskOptions::default(),
        move |_context| {
            let handle = handle.clone();
            async move {
                handle.request_shutdown();
                Ok(())
            }
        },
    )
}
