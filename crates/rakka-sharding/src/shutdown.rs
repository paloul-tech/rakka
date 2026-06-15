//! Coordinated shutdown helpers for cluster sharding runtimes.

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use rakka_cluster::{MembershipState, NodeId};
use rakka_core::{
    CoordinatedShutdown, RakkaError, RakkaResult, ShutdownPhase, ShutdownTask, ShutdownTaskOptions,
    Subsystem,
};
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    ClusterNodeRuntime, ClusterNodeRuntimeUpdate, ClusterShardingRuntime, ClusterShardingUpdate,
};

/// Shared synchronous sharding runtime handle for coordinated shutdown tasks.
#[derive(Clone)]
pub struct ClusterShardingShutdownHandle {
    inner: Arc<Mutex<ClusterShardingShutdownState>>,
}

impl ClusterShardingShutdownHandle {
    /// Creates a shared shutdown handle around a mutable sharding runtime.
    #[must_use]
    pub fn new(runtime: ClusterShardingRuntime) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ClusterShardingShutdownState {
                runtime,
                last_update: None,
            })),
        }
    }

    /// Runs local graceful leave and records the last update.
    pub fn leave_local(&self) -> RakkaResult<ClusterShardingUpdate> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let node_id = state.runtime.membership().local_node_id().clone();
        if local_leave_is_already_started(&state.runtime, &node_id) {
            let update = ClusterShardingUpdate::default();
            state.last_update = Some(update.clone());
            return Ok(update);
        }
        let update = state
            .runtime
            .mark_leaving(&node_id, current_timestamp_millis())
            .map_err(cluster_sharding_error)?;
        state.last_update = Some(update.clone());
        Ok(update)
    }

    /// Last update produced by this shutdown handle.
    #[must_use]
    pub fn last_update(&self) -> Option<ClusterShardingUpdate> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .last_update
            .clone()
    }
}

impl std::fmt::Debug for ClusterShardingShutdownHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterShardingShutdownHandle")
            .field("last_update", &self.last_update())
            .finish_non_exhaustive()
    }
}

/// Shared async sharding runtime handle for async coordinator stores or leases.
#[derive(Clone)]
pub struct AsyncClusterShardingShutdownHandle {
    inner: Arc<AsyncMutex<AsyncClusterShardingShutdownState>>,
}

impl AsyncClusterShardingShutdownHandle {
    /// Creates a shared async shutdown handle around a mutable sharding runtime.
    #[must_use]
    pub fn new(runtime: ClusterShardingRuntime) -> Self {
        Self {
            inner: Arc::new(AsyncMutex::new(AsyncClusterShardingShutdownState {
                runtime,
                last_update: None,
            })),
        }
    }

    /// Runs local graceful leave through the async sharding API.
    pub async fn leave_local(&self) -> RakkaResult<ClusterShardingUpdate> {
        let mut state = self.inner.lock().await;
        let node_id = state.runtime.membership().local_node_id().clone();
        if local_leave_is_already_started(&state.runtime, &node_id) {
            let update = ClusterShardingUpdate::default();
            state.last_update = Some(update.clone());
            return Ok(update);
        }
        let update = state
            .runtime
            .mark_leaving_async(&node_id, current_timestamp_millis())
            .await
            .map_err(cluster_sharding_error)?;
        state.last_update = Some(update.clone());
        Ok(update)
    }

    /// Last update produced by this shutdown handle.
    #[must_use]
    pub async fn last_update(&self) -> Option<ClusterShardingUpdate> {
        self.inner.lock().await.last_update.clone()
    }
}

impl std::fmt::Debug for AsyncClusterShardingShutdownHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncClusterShardingShutdownHandle")
            .finish_non_exhaustive()
    }
}

/// Shared synchronous networked node runtime handle for coordinated shutdown.
#[derive(Clone)]
pub struct ClusterNodeShutdownHandle {
    inner: Arc<Mutex<ClusterNodeShutdownState>>,
}

impl ClusterNodeShutdownHandle {
    /// Creates a shared shutdown handle around a mutable node runtime.
    #[must_use]
    pub fn new(runtime: ClusterNodeRuntime) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ClusterNodeShutdownState {
                runtime,
                last_update: None,
            })),
        }
    }

    /// Runs local graceful leave and records the last node-runtime update.
    pub fn leave_local(&self) -> RakkaResult<ClusterNodeRuntimeUpdate> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let node_id = state.runtime.local_node().id().clone();
        if local_leave_is_already_started(state.runtime.sharding(), &node_id) {
            let update = ClusterNodeRuntimeUpdate::new(ClusterShardingUpdate::default(), 0);
            state.last_update = Some(update.clone());
            return Ok(update);
        }
        let update = state
            .runtime
            .leave_local(current_timestamp_millis())
            .map_err(cluster_node_runtime_error)?;
        state.last_update = Some(update.clone());
        Ok(update)
    }

    /// Last update produced by this shutdown handle.
    #[must_use]
    pub fn last_update(&self) -> Option<ClusterNodeRuntimeUpdate> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .last_update
            .clone()
    }
}

impl std::fmt::Debug for ClusterNodeShutdownHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterNodeShutdownHandle")
            .field("last_update", &self.last_update())
            .finish_non_exhaustive()
    }
}

/// Shared async networked node runtime handle for async coordinator stores or leases.
#[derive(Clone)]
pub struct AsyncClusterNodeShutdownHandle {
    inner: Arc<AsyncMutex<AsyncClusterNodeShutdownState>>,
}

impl AsyncClusterNodeShutdownHandle {
    /// Creates a shared async shutdown handle around a mutable node runtime.
    #[must_use]
    pub fn new(runtime: ClusterNodeRuntime) -> Self {
        Self {
            inner: Arc::new(AsyncMutex::new(AsyncClusterNodeShutdownState {
                runtime,
                last_update: None,
            })),
        }
    }

    /// Runs local graceful leave through the async node-runtime API.
    pub async fn leave_local(&self) -> RakkaResult<ClusterNodeRuntimeUpdate> {
        let mut state = self.inner.lock().await;
        let node_id = state.runtime.local_node().id().clone();
        if local_leave_is_already_started(state.runtime.sharding(), &node_id) {
            let update = ClusterNodeRuntimeUpdate::new(ClusterShardingUpdate::default(), 0);
            state.last_update = Some(update.clone());
            return Ok(update);
        }
        let update = state
            .runtime
            .leave_local_async(current_timestamp_millis())
            .await
            .map_err(cluster_node_runtime_error)?;
        state.last_update = Some(update.clone());
        Ok(update)
    }

    /// Last update produced by this shutdown handle.
    #[must_use]
    pub async fn last_update(&self) -> Option<ClusterNodeRuntimeUpdate> {
        self.inner.lock().await.last_update.clone()
    }
}

impl std::fmt::Debug for AsyncClusterNodeShutdownHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncClusterNodeShutdownHandle")
            .finish_non_exhaustive()
    }
}

/// Registers a handoff-shards task for a synchronous sharding runtime handle.
pub fn register_cluster_sharding_leave_task(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    handle: ClusterShardingShutdownHandle,
) -> RakkaResult<ShutdownTask> {
    shutdown.add_task_with_options(
        ShutdownPhase::handoff_shards(),
        task_name,
        sharding_shutdown_options("cluster-sharding-leave"),
        move |_context| {
            let handle = handle.clone();
            async move { handle.leave_local().map(|_update| ()) }
        },
    )
}

/// Registers a handoff-shards task for an async sharding runtime handle.
pub fn register_async_cluster_sharding_leave_task(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    handle: AsyncClusterShardingShutdownHandle,
) -> RakkaResult<ShutdownTask> {
    shutdown.add_task_with_options(
        ShutdownPhase::handoff_shards(),
        task_name,
        sharding_shutdown_options("cluster-sharding-leave-async"),
        move |_context| {
            let handle = handle.clone();
            async move { handle.leave_local().await.map(|_update| ()) }
        },
    )
}

/// Registers a handoff-shards task for a synchronous networked node runtime.
pub fn register_cluster_node_leave_task(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    handle: ClusterNodeShutdownHandle,
) -> RakkaResult<ShutdownTask> {
    shutdown.add_task_with_options(
        ShutdownPhase::handoff_shards(),
        task_name,
        sharding_shutdown_options("cluster-node-leave"),
        move |_context| {
            let handle = handle.clone();
            async move { handle.leave_local().map(|_update| ()) }
        },
    )
}

/// Registers a handoff-shards task for an async networked node runtime.
pub fn register_async_cluster_node_leave_task(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    handle: AsyncClusterNodeShutdownHandle,
) -> RakkaResult<ShutdownTask> {
    shutdown.add_task_with_options(
        ShutdownPhase::handoff_shards(),
        task_name,
        sharding_shutdown_options("cluster-node-leave-async"),
        move |_context| {
            let handle = handle.clone();
            async move { handle.leave_local().await.map(|_update| ()) }
        },
    )
}

struct ClusterShardingShutdownState {
    runtime: ClusterShardingRuntime,
    last_update: Option<ClusterShardingUpdate>,
}

struct AsyncClusterShardingShutdownState {
    runtime: ClusterShardingRuntime,
    last_update: Option<ClusterShardingUpdate>,
}

struct ClusterNodeShutdownState {
    runtime: ClusterNodeRuntime,
    last_update: Option<ClusterNodeRuntimeUpdate>,
}

struct AsyncClusterNodeShutdownState {
    runtime: ClusterNodeRuntime,
    last_update: Option<ClusterNodeRuntimeUpdate>,
}

fn sharding_shutdown_options(operation: &'static str) -> ShutdownTaskOptions {
    ShutdownTaskOptions::default()
        .with_attribute("operation", operation)
        .expect("static shutdown attribute should be valid")
}

fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn local_leave_is_already_started(runtime: &ClusterShardingRuntime, node_id: &NodeId) -> bool {
    runtime.membership().member(node_id).is_some_and(|member| {
        matches!(
            member.state(),
            MembershipState::Leaving
                | MembershipState::Unreachable
                | MembershipState::Down
                | MembershipState::Removed
        )
    })
}

fn cluster_sharding_error(error: crate::ClusterShardingError) -> RakkaError {
    RakkaError::new(
        Subsystem::Sharding,
        "cluster-sharding-error",
        error.to_string(),
    )
}

fn cluster_node_runtime_error(error: crate::ClusterNodeRuntimeError) -> RakkaError {
    RakkaError::new(
        Subsystem::Sharding,
        "cluster-node-runtime-error",
        error.to_string(),
    )
}
