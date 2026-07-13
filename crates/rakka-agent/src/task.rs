//! The typed task entity, its definition, and its lifecycle.
//!
//! Owns `AgentTaskEntity`, keyed by `(TenantId, AgentTaskId)`, with a
//! serializable command protocol; the versioned `AgentTaskDefinition` carrying
//! typed input and result schema references, deterministic result rules,
//! rejection limits, dependency policy, and per-task budgets; the task
//! lifecycle and its durable, bounded, acyclic dependencies; and the bounded
//! materialized state that keeps history and content behind cursors and
//! artifact references.
//!
//! Result proposals are validated in-entity by deterministic rules only.
//! Model-assisted evaluation is a durable effect, never in-entity I/O. Tasks
//! deliberately left unassigned to an agent and completed by an authenticated
//! human or service travel the same typed validation path.
//!
//! Specification: sections 9.1, 9.2, and 9.6; human-owned tasks in section
//! 8.12. Filled by slice 1.4; human-owned tasks by slice 5.4.
