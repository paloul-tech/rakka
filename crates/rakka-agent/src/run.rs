//! The sharded run entity and its durable status.
//!
//! Owns `AgentRunEntity`, keyed by `(TenantId, AgentId, AgentRunId)`, with a
//! serializable command protocol; the run status enum including the
//! `WaitingForHuman` compatibility mapping; and the run's participation in the
//! result proposal and decision exchange with its task. A run is bound to one
//! task for its lifetime and never makes the public task terminal by itself.
//!
//! Passivation is the default: after any persisted wait the entity is idle and
//! holds no per-run live resources.
//!
//! Specification: sections 6.5 and 9.3. Filled by slice 1.5. The loop state it
//! carries across those waits belongs to [`crate::loop_runtime`].
