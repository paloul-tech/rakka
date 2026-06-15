//! Coordinated shutdown helpers for remote transports and remote receptionist proxies.

use rakka_core::{
    CoordinatedShutdown, RakkaError, RakkaResult, ShutdownPhase, ShutdownTask, ShutdownTaskOptions,
    Subsystem,
};

use crate::{RemoteServiceProxyRegistry, TcpRemoteTransport};

/// Registers a remoting task that drains all currently registered TCP peers.
pub fn register_tcp_remote_drain_task(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    transport: TcpRemoteTransport,
) -> RakkaResult<ShutdownTask> {
    shutdown.add_task_with_options(
        ShutdownPhase::stop_remoting(),
        task_name,
        remote_shutdown_options("tcp-remote-drain"),
        move |_context| {
            let transport = transport.clone();
            async move {
                let peers = transport
                    .snapshot()
                    .peers()
                    .iter()
                    .map(|peer| peer.node_id().clone())
                    .collect::<Vec<_>>();
                for peer in peers {
                    transport
                        .drain_peer(&peer)
                        .await
                        .map_err(remote_transport_error)?;
                }
                Ok(())
            }
        },
    )
}

/// Registers a remoting task that force-closes all currently registered TCP peers.
pub fn register_tcp_remote_force_close_task(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    transport: TcpRemoteTransport,
) -> RakkaResult<ShutdownTask> {
    shutdown.add_task_with_options(
        ShutdownPhase::stop_remoting(),
        task_name,
        remote_shutdown_options("tcp-remote-force-close"),
        move |_context| {
            let transport = transport.clone();
            async move {
                let peers = transport
                    .snapshot()
                    .peers()
                    .iter()
                    .map(|peer| peer.node_id().clone())
                    .collect::<Vec<_>>();
                for peer in peers {
                    transport
                        .force_close_peer(&peer)
                        .await
                        .map_err(remote_transport_error)?;
                }
                Ok(())
            }
        },
    )
}

/// Registers a remoting task that stops remote service proxies for one source node.
pub fn register_remote_service_proxy_remove_node_task(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    registry: RemoteServiceProxyRegistry,
    node_id: rakka_cluster::NodeId,
) -> RakkaResult<ShutdownTask> {
    shutdown.add_task_with_options(
        ShutdownPhase::stop_remoting(),
        task_name,
        remote_shutdown_options("remote-service-proxy-remove-node"),
        move |_context| {
            let registry = registry.clone();
            let node_id = node_id.clone();
            async move {
                let _removed = registry.remove_remote_node(&node_id);
                Ok(())
            }
        },
    )
}

/// Registers a remoting task that expires stale remote service proxy listings.
pub fn register_remote_service_proxy_expire_task(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    registry: RemoteServiceProxyRegistry,
    older_than_millis: u64,
) -> RakkaResult<ShutdownTask> {
    shutdown.add_task_with_options(
        ShutdownPhase::stop_remoting(),
        task_name,
        remote_shutdown_options("remote-service-proxy-expire"),
        move |_context| {
            let registry = registry.clone();
            async move {
                let _expired = registry.expire_stale_listings(older_than_millis);
                Ok(())
            }
        },
    )
}

fn remote_shutdown_options(operation: &'static str) -> ShutdownTaskOptions {
    ShutdownTaskOptions::default()
        .with_attribute("operation", operation)
        .expect("static shutdown attribute should be valid")
}

fn remote_transport_error(error: crate::RemoteTransportError) -> RakkaError {
    RakkaError::new(
        Subsystem::Remote,
        "remote-transport-error",
        error.to_string(),
    )
}
