//! Sharded agent-run registration.
//!
//! Phase 2 registers the entity type so the runtime shape is real, while A2A
//! requests are still accepted through the local durable handler.

use rakka::agent_workflow::{
    init_agent_run_sharding, AgentRunEntityRegistration, AgentRunShardingSettings, AgentWorkflow,
};
use rakka::prelude::ClusterSharding;
use rakka::sharding::EntityTypeKey;

use crate::durable_stores::{RunStore, WorkflowStore};
use crate::support::{ENTITY_TYPE, NUMBER_OF_SHARDS};

/// Initializes sharded run actors for the demo workflow.
pub fn init_demo_run_sharding(
    sharding: &ClusterSharding,
    workflow: AgentWorkflow,
    run_store: RunStore,
    workflow_store: WorkflowStore,
) -> rakka::sharding::ClusterShardingResult<AgentRunEntityRegistration> {
    let key = EntityTypeKey::new(ENTITY_TYPE).with_number_of_shards(NUMBER_OF_SHARDS)?;
    let settings = AgentRunShardingSettings::new(key).without_buffering();
    init_agent_run_sharding(sharding, workflow, run_store, workflow_store, settings)
}
