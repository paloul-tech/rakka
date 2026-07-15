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
//!
//! # What slice 1.5 landed
//!
//! [`AgentContextSnapshotRef`]: the *reference* the durable loop state carries
//! when a model call is prepared
//! ([specification 9.4](../../../docs/plans/rakka-agent/spec.md)). It is
//! deliberately opaque — an identity and a version, no content — because the
//! loop only needs to name the snapshot a model effect was prepared against,
//! and a retry only needs to name the *same* one. Slice 1.11 fills in the
//! `MemoryContextSnapshot` the reference points at without moving the reference
//! or changing what the loop persists.

use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::definition::AgentRevisionNumber;
use crate::identity::{validated_id, AgentIdentityResult, AgentRunScope};
use crate::task::AgentContentDigest;

validated_id! {
    /// Stable identity of one immutable context snapshot
    /// ([specification 13.5](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// It is *derived* from the run's full scope — tenant, agent, and run — and
    /// the turn, never generated, so a model effect retried after a crash names
    /// the snapshot it was first prepared against rather than a freshly
    /// assembled one. That is what will make index or store drift unable to
    /// change a retried model input once slice 1.11 gives the snapshot its
    /// content.
    pub AgentContextSnapshotId, "agent_context_snapshot_id"
}

/// The versioned reference to the immutable context one model effect was
/// prepared against.
///
/// The loop persists it before every model effect and reuses it on every retry
/// of that effect. The snapshot's *content* — assembled instructions, session
/// history, retrieved memory — is slice 1.11's `MemoryContextSnapshot`; until
/// then the reference is opaque, which is all the loop needs in order to be
/// correct about it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AgentContextSnapshotRef {
    /// Stable snapshot identity.
    pub snapshot_id: AgentContextSnapshotId,
    /// Monotonic snapshot revision.
    pub version: AgentRevisionNumber,
}

impl AgentContextSnapshotRef {
    /// Creates a snapshot reference.
    #[must_use]
    pub const fn new(snapshot_id: AgentContextSnapshotId, version: AgentRevisionNumber) -> Self {
        Self {
            snapshot_id,
            version,
        }
    }

    /// Derives the reference of the snapshot one run's turn is prepared against.
    ///
    /// The derivation is a pure function of the run's full scope and the turn,
    /// so preparing the same turn twice — after a crash, or on another shard
    /// owner — names one snapshot rather than two, and two tenants naming
    /// their agents and runs alike never derive the same snapshot identity.
    ///
    /// The scope enters through a digest of its injective key rather than by
    /// joining the ids literally: ids may themselves contain the join
    /// character, which would let two different runs flatten to one name, and
    /// three maximal ids would overflow the identity bound — a run stranded at
    /// its first turn by the length of its own name.
    pub fn for_turn(scope: &AgentRunScope, turn: u64) -> AgentIdentityResult<Self> {
        let scope_digest = AgentContentDigest::of_bytes(scope.key().as_bytes());
        Ok(Self::new(
            AgentContextSnapshotId::new(format!("run-{}-turn-{turn}", scope_digest.value))?,
            AgentRevisionNumber::INITIAL,
        ))
    }
}

impl Display for AgentContextSnapshotRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.snapshot_id, self.version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{AgentId, AgentRunId, TenantId};

    fn scope(tenant: &str, agent: &str, run: &str) -> AgentRunScope {
        AgentRunScope::new(
            TenantId::new(tenant),
            AgentId::new(agent).expect("the agent id is valid"),
            AgentRunId::new(run).expect("the run id is valid"),
        )
        .expect("the scope is valid")
    }

    #[test]
    fn the_snapshot_identity_is_tenant_scoped_pure_and_bounded() {
        // Run ids are derived from task ids, so two tenants holding the same
        // task derive the same run id — their snapshots must still be distinct.
        let acme = AgentContextSnapshotRef::for_turn(&scope("acme", "support", "t-gen-1"), 1)
            .expect("the reference derives");
        let globex = AgentContextSnapshotRef::for_turn(&scope("globex", "support", "t-gen-1"), 1)
            .expect("the reference derives");
        assert_ne!(acme, globex);

        // Ids may contain the byte a literal join would use, so two different
        // scopes must not flatten to one name.
        let joined_left = AgentContextSnapshotRef::for_turn(&scope("t", "a-b", "c"), 1)
            .expect("the reference derives");
        let joined_right = AgentContextSnapshotRef::for_turn(&scope("t", "a", "b-c"), 1)
            .expect("the reference derives");
        assert_ne!(joined_left, joined_right);

        // Three maximal ids must still derive: a run must never be stranded at
        // its first turn by the length of its own name.
        let long = "a".repeat(256);
        let maximal = AgentContextSnapshotRef::for_turn(&scope(&long, &long, &long), u64::MAX)
            .expect("a maximal scope still derives its snapshot");

        // And the derivation is pure: the same scope and turn name one snapshot.
        assert_eq!(
            maximal,
            AgentContextSnapshotRef::for_turn(&scope(&long, &long, &long), u64::MAX)
                .expect("the reference derives"),
        );
    }
}
