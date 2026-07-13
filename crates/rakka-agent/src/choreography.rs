//! The inter-entity choreography substrate.
//!
//! Every cross-entity exchange in this crate — creation, assignment, run
//! acceptance, result proposal and decision, budget allocation, and settlement
//! or return — is a deduplicated outbox/inbox saga re-driven by the initiator.
//! This module owns the primitives built over the `rakka-agent-workflow` inbox
//! and outbox: the initiator's pending-exchange record, operation-identifier
//! re-drive on recovery, and receiver-side deduplication that replays the
//! original logical result rather than acting twice.
//!
//! There is no colocated shortcut. The exchange path is identical whether the
//! two entities share a node or not, so a shard move can never change the
//! observable outcome of an exchange.
//!
//! Specification: sections 9.8 and 6.10. Filled by slice 1.3, which also
//! commits the per-exchange failure-window table naming the test that proves
//! each window.
