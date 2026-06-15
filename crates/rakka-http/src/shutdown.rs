//! Coordinated shutdown helpers for HTTP servers.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use rakka_core::{
    CoordinatedShutdown, RakkaResult, ShutdownPhase, ShutdownTask, ShutdownTaskOptions,
};
use tokio::sync::watch;

use crate::{serve_with_graceful_shutdown, HttpError, HttpResult, HttpServerConfig, Router};

/// Cloneable shutdown handle shared between coordinated shutdown and HTTP server tasks.
#[derive(Clone)]
pub struct HttpShutdownHandle {
    inner: Arc<HttpShutdownState>,
}

impl HttpShutdownHandle {
    /// Creates a shutdown handle with no shutdown request recorded.
    #[must_use]
    pub fn new() -> Self {
        let (sender, _receiver) = watch::channel(false);
        Self {
            inner: Arc::new(HttpShutdownState {
                requested: AtomicBool::new(false),
                sender,
                server_result: Mutex::new(None),
            }),
        }
    }

    /// Creates a signal future source for an HTTP server.
    #[must_use]
    pub fn signal(&self) -> HttpShutdownSignal {
        HttpShutdownSignal {
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

    /// Records a successful HTTP server completion.
    pub fn record_server_completed(&self) {
        *self
            .inner
            .server_result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(HttpServerShutdownResult::Completed);
    }

    /// Records an HTTP server failure.
    pub fn record_server_failed(&self, message: impl Into<String>) {
        *self
            .inner
            .server_result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(HttpServerShutdownResult::Failed {
                message: message.into(),
            });
    }

    /// Returns a point-in-time shutdown snapshot.
    #[must_use]
    pub fn snapshot(&self) -> HttpShutdownSnapshot {
        HttpShutdownSnapshot::new(
            self.is_shutdown_requested(),
            self.inner
                .server_result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        )
    }
}

impl Default for HttpShutdownHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for HttpShutdownHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpShutdownHandle")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

/// Future source passed to an HTTP server graceful shutdown hook.
pub struct HttpShutdownSignal {
    receiver: watch::Receiver<bool>,
}

impl HttpShutdownSignal {
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

impl std::fmt::Debug for HttpShutdownSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpShutdownSignal")
            .field("requested", &*self.receiver.borrow())
            .finish()
    }
}

/// Point-in-time HTTP shutdown state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpShutdownSnapshot {
    shutdown_requested: bool,
    server_result: Option<HttpServerShutdownResult>,
}

impl HttpShutdownSnapshot {
    /// Creates an HTTP shutdown snapshot.
    #[must_use]
    pub const fn new(
        shutdown_requested: bool,
        server_result: Option<HttpServerShutdownResult>,
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

    /// Last recorded HTTP server result, if any.
    #[must_use]
    pub const fn server_result(&self) -> Option<&HttpServerShutdownResult> {
        self.server_result.as_ref()
    }
}

/// Recorded result of an HTTP server task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpServerShutdownResult {
    /// Server completed without returning an error.
    Completed,
    /// Server returned an error.
    Failed {
        /// Human-readable failure detail.
        message: String,
    },
}

struct HttpShutdownState {
    requested: AtomicBool,
    sender: watch::Sender<bool>,
    server_result: Mutex<Option<HttpServerShutdownResult>>,
}

/// Registers a stop-ingress task that requests HTTP graceful shutdown.
pub fn register_http_shutdown_task(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    handle: HttpShutdownHandle,
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

/// Starts an Axum server and records its final result on the shutdown handle.
pub async fn serve_with_coordinated_shutdown(
    router: Router,
    config: HttpServerConfig,
    handle: HttpShutdownHandle,
) -> HttpResult<()> {
    let result = serve_with_graceful_shutdown(router, config, handle.signal().wait()).await;
    match &result {
        Ok(()) => handle.record_server_completed(),
        Err(HttpError::Bind { message, .. }) | Err(HttpError::Serve { message }) => {
            handle.record_server_failed(message.clone());
        }
        Err(error) => handle.record_server_failed(error.to_string()),
    }
    result
}
