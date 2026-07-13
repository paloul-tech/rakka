//! The sharded agent entity.
//!
//! Owns `AgentEntity`, keyed by `(TenantId, AgentId)`, together with its
//! serializable command protocol. The entity holds the durable definition and
//! lifecycle status, the current settings revision, policy and logical
//! credential-binding references, the agent-private memory namespace, and the
//! administrative suspend, resume, and terminate commands.
//!
//! Routine run creation never round-trips through this entity synchronously:
//! assignment reads its durable definition and admission state through the
//! choreographed exchanges of [`crate::choreography`] instead.
//!
//! Specification: sections 6.2 and 6.11. Filled by slice 1.2.
