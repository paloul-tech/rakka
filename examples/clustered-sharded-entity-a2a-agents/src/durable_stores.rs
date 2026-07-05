//! Durable-store selection for the Phase 2 example.
//!
//! Phase 2 still uses in-memory stores. They are enough for local durable A2A
//! acceptance and task projection recovery without introducing external
//! infrastructure.

use rakka::agent_workflow::substrate::WorkflowState;
use rakka::agent_workflow::AgentRunState;
use rakka::persistence::InMemoryDurableStateStore;

/// In-memory durable store for agent run state.
pub type RunStore = InMemoryDurableStateStore<AgentRunState>;

/// In-memory durable store for workflow inbox/outbox state.
pub type WorkflowStore = InMemoryDurableStateStore<WorkflowState>;

/// Builds the local Phase 2 stores.
#[must_use]
pub fn build_stores() -> (RunStore, WorkflowStore) {
    (
        InMemoryDurableStateStore::new(),
        InMemoryDurableStateStore::new(),
    )
}
