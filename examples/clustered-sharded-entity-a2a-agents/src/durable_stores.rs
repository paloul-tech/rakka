//! Durable-store selection for the Phase 1 example.
//!
//! Phase 1 still uses in-memory stores. They are enough to boot the sharded run
//! entity type without creating any public durable mutation path.

use rakka::agent_workflow::substrate::WorkflowState;
use rakka::agent_workflow::AgentRunState;
use rakka::persistence::InMemoryDurableStateStore;

/// In-memory durable store for agent run state.
pub type RunStore = InMemoryDurableStateStore<AgentRunState>;

/// In-memory durable store for workflow inbox/outbox state.
pub type WorkflowStore = InMemoryDurableStateStore<WorkflowState>;

/// Builds the local Phase 1 stores.
#[must_use]
pub fn build_stores() -> (RunStore, WorkflowStore) {
    (
        InMemoryDurableStateStore::new(),
        InMemoryDurableStateStore::new(),
    )
}
