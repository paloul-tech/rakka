//! Tenant-scoped agent identities and stable operation identifiers.
//!
//! Owns the newtype identities of the agent domain — `AgentId`, `AgentGoalId`,
//! `AgentTaskId`, `AgentRunId`, `AgentDelegationId`, `AgentWakeId`,
//! `AgentEnvironmentRef`, and `KnowledgeSpaceId` — which stay distinct types
//! even where their initial values coincide, and the construction helpers for
//! the stable operation and deduplication identifiers every durable exchange
//! keys on.
//!
//! Specification: sections 6.1 through 6.10. Filled by slice 1.2; the wake and
//! knowledge-space scopes are fixed here so the memory and continuous-goal
//! milestones cannot bake in an incompatible scope later.
