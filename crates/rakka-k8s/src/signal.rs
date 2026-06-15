//! OS signal bridge for Kubernetes-style coordinated shutdown.

use rakka_core::{
    CoordinatedShutdown, CoordinatedShutdownReason, CoordinatedShutdownReport,
    CoordinatedShutdownResult,
};

/// Waits for Ctrl-C or SIGTERM, then runs coordinated shutdown with the supplied reason.
pub async fn run_coordinated_shutdown_on_os_signal(
    shutdown: CoordinatedShutdown,
    reason: CoordinatedShutdownReason,
) -> CoordinatedShutdownResult<CoordinatedShutdownReport> {
    wait_for_os_shutdown_signal().await;
    shutdown.run(reason).await
}

/// Waits for Ctrl-C or SIGTERM, then runs coordinated shutdown as Kubernetes pre-stop.
pub async fn run_kubernetes_prestop_shutdown_on_os_signal(
    shutdown: CoordinatedShutdown,
) -> CoordinatedShutdownResult<CoordinatedShutdownReport> {
    run_coordinated_shutdown_on_os_signal(shutdown, CoordinatedShutdownReason::kubernetes_prestop())
        .await
}

async fn wait_for_os_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut terminate = signal(SignalKind::terminate()).ok();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = async {
                if let Some(signal) = terminate.as_mut() {
                    let _received = signal.recv().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _received = tokio::signal::ctrl_c().await;
    }
}
