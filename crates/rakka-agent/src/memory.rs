//! Memory scopes, stores, and context snapshots.
//!
//! Owns the session-memory trait scoped `(TenantId, AgentId, AgentRunId)` with
//! idempotent appends keyed by `MemoryOperationId`, an ordered sequence, and
//! classification metadata; the agent-private long-term memory trait scoped
//! `(TenantId, AgentId)`; and the immutable `MemoryContextSnapshot` persisted
//! before every model effect, which a retry reuses so that drift in a store or
//! an index cannot change a retried model input.
//!
//! Retrieved memory is untrusted context and passes the guardrail chain like
//! any other model input. The in-memory implementations live here; the
//! PostgreSQL and `pgvector` stores live in `rakka-agent-postgres`, and the
//! communal knowledge graph lives in `rakka-agent-knowledge-graph`.
//!
//! Specification: sections 13.1, 13.2, 13.5, and the short-term clauses of
//! 13.6; the private trait of 13.3 is declared here so scopes are fixed early.
//! Filled by slice 1.11; the private and communal stores by phase 2.
