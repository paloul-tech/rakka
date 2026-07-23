//! Durable delegation and the collaboration graph.
//!
//! Owns the `AgentDelegationId` record, persisted before the send so that a
//! replay resolves to the same child or to an explicit conflict rather than a
//! second child; the durable fan-out groups and the deterministic fan-in policy
//! — all, any, quorum, or explicit — fixed before any result is accepted, with
//! the parent passivated while it waits; the depth, fan-out, descendant, and
//! concurrency ceilings enforced through the escrow ledger; lineage-based cycle
//! rejection; and the durable propagation of cancellation, deadline, and
//! revocation to children.
//!
//! A peer is reached only through the outbox and `rakka-a2a`, carrying the
//! versioned collaboration metadata. The model cannot reach a peer through a
//! generic tool, and the catalog that resolves a requested skill to a concrete
//! agent is application-owned.
//!
//! Specification: sections 8.4, 6.6, 8.7, and 14.4. Filled by slices 4.3, 4.4,
//! and 4.6.
