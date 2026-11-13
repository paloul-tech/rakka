//! Tombstones: withdrawing a claim's content while keeping an auditable stub.
//!
//! A tombstone is a statement about *content*, never a verdict on truth
//! ([specification 13.1](../../../docs/plans/rakka-agent/spec.md),
//! [13.4](../../../docs/plans/rakka-agent/spec.md)). It clears the claim's
//! whole statement — subject, predicate and object — with its evidence and
//! embedding, and keeps the claim's identity, provenance, classification,
//! access set, trust status, transition history and statement fingerprint. The
//! whole statement goes, not only an attribute value, because for an edge claim
//! the subject, predicate and object node *are* the content.
//!
//! The stub keeps the statement fields' types. They hold fixed markers instead:
//! [`CLAIM_TOMBSTONE_MARKER`] in the subject and the predicate, and the null
//! inline content the private memory tier uses in the object. A stub that
//! carries anything else is refused with `claim-tombstone-content`.

use rakka_agent_workflow::{AgentTimestampMillis, PrincipalRef};
use serde::{Deserialize, Serialize};

use crate::claim::{ClaimId, ClaimOperationId};

/// The value a tombstoned claim holds in its subject and in its predicate.
///
/// A reserved marker rather than an empty or optional field, so the durable
/// record keeps its field types across the N/N+1 window. A live claim may
/// spell the same value — nothing reads the marker to *decide* a claim is a
/// stub; only the record's tombstone does.
pub const CLAIM_TOMBSTONE_MARKER: &str = "rakka-claim-tombstoned";

/// Why a claim's content was withdrawn.
///
/// There is no `Retracted` or `Superseded` member: retraction is a trust
/// status in this crate, and superseding is done by a new claim that references
/// the old one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum ClaimTombstoneReason {
    /// Its asserter or an operator withdrew the content.
    Withdrawn,
    /// A retention or classification policy removed the content.
    Policy,
}

impl ClaimTombstoneReason {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(&self) -> &'static str {
        match self {
            Self::Withdrawn => "withdrawn",
            Self::Policy => "policy",
        }
    }
}

/// The record a tombstoned claim carries of its own withdrawal.
///
/// The actor is recorded because the graph is communal: in the private tier
/// the owner is implied by the scope, and here the withdrawer is not.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaimTombstone {
    /// The idempotent key of the tombstone operation.
    pub operation_id: ClaimOperationId,
    /// Why the content was withdrawn.
    pub reason: ClaimTombstoneReason,
    /// Who withdrew it.
    pub actor: PrincipalRef,
    /// When it was withdrawn. The retention sweep ages a stub from here.
    pub tombstoned_at: AgentTimestampMillis,
}

/// One tombstone of one claim, idempotent on its operation id.
///
/// The request names no trust status and no policy: any trust status can be
/// tombstoned, and an explicit tombstone is not subject to a retention
/// policy's legal hold — the hold governs the retention sweep only.
#[derive(Debug, Clone, PartialEq)]
pub struct ClaimTombstoneRequest {
    /// The claim whose content is withdrawn.
    pub claim_id: ClaimId,
    /// The idempotent key of this tombstone; see
    /// [`ClaimOperationId::derive_tombstone`].
    pub operation_id: ClaimOperationId,
    /// Why the content is withdrawn.
    pub reason: ClaimTombstoneReason,
    /// Who withdraws it.
    pub actor: PrincipalRef,
    /// When it is withdrawn.
    pub tombstoned_at: AgentTimestampMillis,
}

impl ClaimTombstoneRequest {
    /// The record the stub will carry of this withdrawal.
    #[must_use]
    pub fn to_tombstone(&self) -> ClaimTombstone {
        ClaimTombstone {
            operation_id: self.operation_id.clone(),
            reason: self.reason,
            actor: self.actor.clone(),
            tombstoned_at: self.tombstoned_at,
        }
    }
}
