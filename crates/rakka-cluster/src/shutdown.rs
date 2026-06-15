//! Coordinated shutdown helpers for cluster membership and receptionist state.

use rakka_core::{
    CoordinatedShutdown, RakkaResult, ShutdownPhase, ShutdownTask, ShutdownTaskOptions,
};

use crate::{Cluster, ClusterUpdate, ClusteredReceptionist, MembershipState};

/// Registers a leave-cluster task that marks the local cluster member leaving.
pub fn register_cluster_leave_task(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    cluster: Cluster,
) -> RakkaResult<ShutdownTask> {
    shutdown.add_task_with_options(
        ShutdownPhase::leave_cluster(),
        task_name,
        cluster_shutdown_options("cluster-leave"),
        move |_context| {
            let cluster = cluster.clone();
            async move { leave_local_cluster(cluster).map(|_update| ()) }
        },
    )
}

/// Registers an explicit down-self task for forced or test-only shutdown flows.
///
/// Graceful application shutdown should normally use
/// [`register_cluster_leave_task`]. This helper exists for callers that
/// intentionally want the local member to transition to `Down` during shutdown.
pub fn register_cluster_down_self_task(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    cluster: Cluster,
) -> RakkaResult<ShutdownTask> {
    shutdown.add_task_with_options(
        ShutdownPhase::leave_cluster(),
        task_name,
        cluster_shutdown_options("cluster-down-self"),
        move |_context| {
            let cluster = cluster.clone();
            async move { down_local_cluster(cluster).map(|_update| ()) }
        },
    )
}

/// Registers a remoting-shutdown task that prunes clustered receptionist listings.
pub fn register_clustered_receptionist_prune_task(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    receptionist: ClusteredReceptionist,
) -> RakkaResult<ShutdownTask> {
    shutdown.add_task_with_options(
        ShutdownPhase::stop_remoting(),
        task_name,
        cluster_shutdown_options("clustered-receptionist-prune"),
        move |_context| {
            let receptionist = receptionist.clone();
            async move {
                let _removed_sources = receptionist.prune_unreachable_members();
                Ok(())
            }
        },
    )
}

fn leave_local_cluster(cluster: Cluster) -> RakkaResult<Option<ClusterUpdate>> {
    match cluster
        .self_member()
        .map_err(crate::ClusterError::into_rakka_error)?
        .state()
    {
        MembershipState::Joining | MembershipState::Up => cluster
            .manager()
            .leave(&cluster.local_node_id())
            .map(Some)
            .map_err(crate::ClusterError::into_rakka_error),
        MembershipState::Leaving
        | MembershipState::Unreachable
        | MembershipState::Down
        | MembershipState::Removed => Ok(None),
    }
}

fn down_local_cluster(cluster: Cluster) -> RakkaResult<Option<ClusterUpdate>> {
    match cluster
        .self_member()
        .map_err(crate::ClusterError::into_rakka_error)?
        .state()
    {
        MembershipState::Removed | MembershipState::Down => Ok(None),
        MembershipState::Joining
        | MembershipState::Up
        | MembershipState::Leaving
        | MembershipState::Unreachable => cluster
            .manager()
            .down(&cluster.local_node_id())
            .map(Some)
            .map_err(crate::ClusterError::into_rakka_error),
    }
}

fn cluster_shutdown_options(operation: &'static str) -> ShutdownTaskOptions {
    ShutdownTaskOptions::default()
        .with_attribute("operation", operation)
        .expect("static shutdown attribute should be valid")
}
