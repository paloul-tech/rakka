//! Cluster sharding integration for actor-backed agent runs.
//!
//! This module keeps the durable run runtime as the correctness boundary while
//! making each run addressable as a sharded entity. Entity ids are the stable
//! `AgentRunId` strings, so commands can route to the same durable run across
//! local passivation, shard movement, and process-local actor restarts.

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

use rakka_core::{ActorOptions, MetricsRecorder, NoopMetricsRecorder};
use rakka_persistence::DurableStateStore;
use rakka_sharding::{
    ClusterSharding, ClusterShardingResult, Entity, EntityId, EntityTypeKey,
    EntityTypeRegistration, RememberedEntities, ShardBufferConfig, ShardedEntityRef,
};
use rakka_workflow::{SystemWorkflowClock, WorkflowClock, WorkflowState};

use crate::{
    AgentRunActor, AgentRunActorCommand, AgentRunId, AgentRunState, AgentWorkflow,
    AgentWorkflowSnapshotRegistry,
};

/// Default sharded entity type used for agent runs.
pub const DEFAULT_AGENT_RUN_ENTITY_TYPE: &str = "AgentRun";

const DEFAULT_AGENT_RUN_PASSIVATION_BUFFER_DURATION: Duration = Duration::from_millis(25);

/// Agent run entity type key.
pub type AgentRunEntityTypeKey = EntityTypeKey<AgentRunActorCommand>;

/// Registration returned after initializing sharded agent runs.
pub type AgentRunEntityRegistration = EntityTypeRegistration<AgentRunActorCommand>;

/// Sharded reference to one actor-backed agent run.
pub type AgentRunEntityRef = ShardedEntityRef<AgentRunActorCommand>;

/// Sharding settings for actor-backed agent runs.
#[derive(Clone)]
pub struct AgentRunShardingSettings {
    key: AgentRunEntityTypeKey,
    actor_options: ActorOptions,
    idle_passivation_timeout: Option<Duration>,
    buffer_config: Option<ShardBufferConfig>,
    passivation_buffer_duration: Duration,
    remembered_entities: Option<RememberedEntities>,
    snapshot_registry: Option<AgentWorkflowSnapshotRegistry>,
}

impl Debug for AgentRunShardingSettings {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentRunShardingSettings")
            .field("entity_type", self.key.entity_type())
            .field("number_of_shards", &self.key.config().number_of_shards())
            .field("actor_options", &self.actor_options)
            .field("idle_passivation_timeout", &self.idle_passivation_timeout)
            .field("buffer_config", &self.buffer_config)
            .field(
                "passivation_buffer_duration",
                &self.passivation_buffer_duration,
            )
            .field("remembered_entities", &self.remembered_entities)
            .field("snapshot_registry", &self.snapshot_registry.is_some())
            .finish()
    }
}

impl AgentRunShardingSettings {
    /// Creates settings from an explicit entity type key.
    #[must_use]
    pub fn new(key: AgentRunEntityTypeKey) -> Self {
        Self {
            key,
            actor_options: ActorOptions::default(),
            idle_passivation_timeout: None,
            buffer_config: Some(ShardBufferConfig::default()),
            passivation_buffer_duration: DEFAULT_AGENT_RUN_PASSIVATION_BUFFER_DURATION,
            remembered_entities: None,
            snapshot_registry: None,
        }
    }

    /// Entity type key used for agent runs.
    #[must_use]
    pub const fn key(&self) -> &AgentRunEntityTypeKey {
        &self.key
    }

    /// Actor options used for newly spawned agent run actors.
    #[must_use]
    pub const fn actor_options(&self) -> &ActorOptions {
        &self.actor_options
    }

    /// Configured idle passivation timeout.
    #[must_use]
    pub const fn idle_passivation_timeout(&self) -> Option<Duration> {
        self.idle_passivation_timeout
    }

    /// Configured shard buffering policy, when enabled.
    #[must_use]
    pub const fn buffer_config(&self) -> Option<&ShardBufferConfig> {
        self.buffer_config.as_ref()
    }

    /// Explicit passivation buffering window.
    #[must_use]
    pub const fn passivation_buffer_duration(&self) -> Duration {
        self.passivation_buffer_duration
    }

    /// Remembered entity settings, when enabled.
    #[must_use]
    pub const fn remembered_entities(&self) -> Option<&RememberedEntities> {
        self.remembered_entities.as_ref()
    }

    /// Operational snapshot registry used by spawned run actors, when enabled.
    #[must_use]
    pub const fn snapshot_registry(&self) -> Option<&AgentWorkflowSnapshotRegistry> {
        self.snapshot_registry.as_ref()
    }

    /// Sets options used when each run actor is spawned.
    #[must_use]
    pub fn with_actor_options(mut self, actor_options: ActorOptions) -> Self {
        self.actor_options = actor_options;
        self
    }

    /// Enables idle passivation for inactive run actors.
    #[must_use]
    pub const fn with_idle_passivation(mut self, timeout: Duration) -> Self {
        self.idle_passivation_timeout = Some(timeout);
        self
    }

    /// Disables idle passivation.
    #[must_use]
    pub const fn without_idle_passivation(mut self) -> Self {
        self.idle_passivation_timeout = None;
        self
    }

    /// Configures bounded buffering during shard handoff and passivation.
    #[must_use]
    pub fn with_buffering(mut self, config: ShardBufferConfig) -> Self {
        self.buffer_config = Some(config);
        self
    }

    /// Disables shard-level buffering.
    #[must_use]
    pub const fn without_buffering(mut self) -> Self {
        self.buffer_config = None;
        self
    }

    /// Sets how long explicit facade passivation buffers incoming run messages.
    #[must_use]
    pub const fn with_passivation_buffer_duration(mut self, duration: Duration) -> Self {
        self.passivation_buffer_duration = duration;
        self
    }

    /// Enables remembered entities for runs that must be eagerly restarted when
    /// a shard is acquired.
    #[must_use]
    pub fn with_remembered_entities(mut self, remembered_entities: RememberedEntities) -> Self {
        self.remembered_entities = Some(remembered_entities);
        self
    }

    /// Disables remembered entities.
    #[must_use]
    pub fn without_remembered_entities(mut self) -> Self {
        self.remembered_entities = None;
        self
    }

    /// Publishes bounded run snapshots from spawned run actors.
    #[must_use]
    pub fn with_snapshot_registry(
        mut self,
        snapshot_registry: AgentWorkflowSnapshotRegistry,
    ) -> Self {
        self.snapshot_registry = Some(snapshot_registry);
        self
    }

    /// Disables run snapshot publication for spawned run actors.
    #[must_use]
    pub fn without_snapshot_registry(mut self) -> Self {
        self.snapshot_registry = None;
        self
    }
}

impl Default for AgentRunShardingSettings {
    fn default() -> Self {
        Self::new(agent_run_entity_type_key())
    }
}

/// Creates the default sharded entity type key for agent runs.
#[must_use]
pub fn agent_run_entity_type_key() -> AgentRunEntityTypeKey {
    EntityTypeKey::new(DEFAULT_AGENT_RUN_ENTITY_TYPE)
}

/// Maps an agent run id to its sharded entity id.
#[must_use]
pub fn agent_run_entity_id(run_id: &AgentRunId) -> EntityId {
    EntityId::new(run_id.as_str())
}

/// Initializes sharded agent run actors with the system clock and no-op metrics.
pub fn init_agent_run_sharding<RunStore, WorkflowStore>(
    sharding: &ClusterSharding,
    workflow: AgentWorkflow,
    run_store: RunStore,
    workflow_store: WorkflowStore,
    settings: AgentRunShardingSettings,
) -> ClusterShardingResult<AgentRunEntityRegistration>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
{
    init_agent_run_sharding_with_metrics(
        sharding,
        workflow,
        run_store,
        workflow_store,
        settings,
        Arc::new(NoopMetricsRecorder),
    )
}

/// Initializes sharded agent run actors with the system clock and explicit
/// metrics recorder.
pub fn init_agent_run_sharding_with_metrics<RunStore, WorkflowStore>(
    sharding: &ClusterSharding,
    workflow: AgentWorkflow,
    run_store: RunStore,
    workflow_store: WorkflowStore,
    settings: AgentRunShardingSettings,
    metrics: Arc<dyn MetricsRecorder>,
) -> ClusterShardingResult<AgentRunEntityRegistration>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
{
    init_agent_run_sharding_with_clock_and_metrics(
        sharding,
        workflow,
        run_store,
        workflow_store,
        SystemWorkflowClock,
        settings,
        metrics,
    )
}

/// Initializes sharded agent run actors with an explicit clock and metrics
/// recorder.
pub fn init_agent_run_sharding_with_clock_and_metrics<RunStore, WorkflowStore, Clock>(
    sharding: &ClusterSharding,
    workflow: AgentWorkflow,
    run_store: RunStore,
    workflow_store: WorkflowStore,
    clock: Clock,
    settings: AgentRunShardingSettings,
    metrics: Arc<dyn MetricsRecorder>,
) -> ClusterShardingResult<AgentRunEntityRegistration>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    let key = settings.key.clone();
    let workflow_for_factory = workflow;
    let run_store_for_factory = run_store;
    let workflow_store_for_factory = workflow_store;
    let clock_for_factory = clock;
    let metrics_for_factory = metrics;
    let snapshot_registry_for_factory = settings.snapshot_registry.clone();

    let mut entity = Entity::of(key.clone(), move |context| {
        let actor = AgentRunActor::with_clock_and_metrics(
            workflow_for_factory.clone(),
            AgentRunId::new(context.entity_id().as_str()),
            run_store_for_factory.clone(),
            workflow_store_for_factory.clone(),
            clock_for_factory.clone(),
            metrics_for_factory.clone(),
        );
        if let Some(snapshot_registry) = &snapshot_registry_for_factory {
            actor.with_snapshot_registry(snapshot_registry.clone())
        } else {
            actor
        }
    })
    .with_actor_options(settings.actor_options)
    .with_passivation_buffer_duration(settings.passivation_buffer_duration);

    if let Some(timeout) = settings.idle_passivation_timeout {
        entity = entity.with_idle_passivation(timeout);
    }
    if let Some(buffer_config) = settings.buffer_config {
        entity = entity.with_buffering(buffer_config);
    } else {
        entity = entity.without_buffering();
    }
    if let Some(remembered_entities) = settings.remembered_entities {
        entity = entity.with_remembered_entities(remembered_entities);
    }

    sharding.init(entity)
}

/// Returns a sharded reference for one durable agent run.
pub fn agent_run_entity_ref(
    sharding: &ClusterSharding,
    key: &AgentRunEntityTypeKey,
    run_id: &AgentRunId,
) -> ClusterShardingResult<AgentRunEntityRef> {
    sharding.entity_ref_for(key, run_id.as_str())
}

/// Returns a sharded reference for one durable agent run from an entity
/// registration.
#[must_use]
pub fn registered_agent_run_entity_ref(
    registration: &AgentRunEntityRegistration,
    run_id: &AgentRunId,
) -> AgentRunEntityRef {
    registration.entity_ref_for(run_id.as_str())
}

/// Explicitly passivates one local agent run entity.
pub fn passivate_agent_run(
    sharding: &ClusterSharding,
    key: &AgentRunEntityTypeKey,
    run_id: &AgentRunId,
) -> ClusterShardingResult<bool> {
    sharding.passivate_entity_id(key, &agent_run_entity_id(run_id))
}

/// Forgets one remembered agent run id and passivates the local entity when it
/// is active.
pub async fn forget_agent_run(
    sharding: &ClusterSharding,
    key: &AgentRunEntityTypeKey,
    run_id: &AgentRunId,
) -> ClusterShardingResult<bool> {
    sharding
        .forget_entity_id(key, &agent_run_entity_id(run_id))
        .await
}
