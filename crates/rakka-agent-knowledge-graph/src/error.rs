//! Error and schema-window vocabulary for the communal knowledge graph.
//!
//! The crate follows the agent-domain house style: one `#[non_exhaustive]`
//! error enum with a stable kebab-case [`ClaimError::code`] per variant, and a
//! crate-owned N/N+1 schema window per persisted record kind
//! ([specification 20](../../../docs/plans/rakka-agent/spec.md)). The window
//! deliberately reuses the stable `schema-version-ahead` /
//! `schema-version-too-old` codes of `rakka-agent`'s schema policy, so
//! operators see one vocabulary — the cross-crate contract is the codes, not
//! the enum: widening `rakka-agent`'s non-exhaustive record-kind array for
//! records it does not own would couple the base crate to every sibling's
//! records and invert its documented dependency promise.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

use rakka_agent::{
    AgentCheckpointGrantError, AgentCheckpointKind, AgentContentDigest, AgentIdentityError,
};
use rakka_agent_workflow::StateSchemaVersion;

use crate::claim::{ClaimId, ClaimOperationId, ClaimTrustStatus};

/// Result alias for every fallible operation in this crate.
pub type ClaimResult<T> = Result<T, ClaimError>;

/// Boxed future returned by the [`crate::store::KnowledgeGraphStore`] SPI.
///
/// Boxed so the trait stays object-safe and callers can hold
/// `Arc<dyn KnowledgeGraphStore>`, exactly like the memory-store futures in
/// `rakka-agent`.
pub type ClaimFuture<'a, T> = Pin<Box<dyn Future<Output = ClaimResult<T>> + Send + 'a>>;

/// Kind of durable record this crate persists, named in schema errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum ClaimRecordKind {
    /// A communal claim record.
    Claim,
    /// An append-only trust transition record.
    TrustTransition,
}

impl ClaimRecordKind {
    /// Stable kebab-case label for logs and error detail.
    #[must_use]
    pub const fn as_label(&self) -> &'static str {
        match self {
            Self::Claim => "claim",
            Self::TrustTransition => "claim-trust-transition",
        }
    }
}

/// Accepts a persisted record version under the N/N+1 window for `current`,
/// or fails closed.
///
/// `current` is the version this binary writes; the minimum supported version
/// is the one immediately before it (floored at one). This is the default
/// policy of `rakka-agent`'s schema module, owned locally for the records this
/// crate owns.
pub const fn check_schema_window(
    kind: ClaimRecordKind,
    version: StateSchemaVersion,
    current: StateSchemaVersion,
) -> ClaimResult<()> {
    if version.get() > current.get() {
        return Err(ClaimError::SchemaVersionAhead {
            record: kind,
            version,
            current,
        });
    }
    let minimum_supported = if current.get() > 1 {
        current.get() - 1
    } else {
        1
    };
    if version.get() < minimum_supported {
        return Err(ClaimError::SchemaVersionTooOld {
            record: kind,
            version,
            minimum_supported: StateSchemaVersion::new(minimum_supported),
        });
    }
    Ok(())
}

/// Why a communal knowledge-graph operation failed.
///
/// Every variant carries a stable [`ClaimError::code`]. Absent and
/// out-of-scope claims fail with the same [`ClaimError::NotFound`] — an
/// unauthorized caller learns nothing, not even existence (scenario 18).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClaimError {
    /// The record was written by a newer schema than this binary reads.
    SchemaVersionAhead {
        /// Record kind that failed the window.
        record: ClaimRecordKind,
        /// Version the record carries.
        version: StateSchemaVersion,
        /// Version this binary writes.
        current: StateSchemaVersion,
    },
    /// The record was written by a schema older than the supported window.
    SchemaVersionTooOld {
        /// Record kind that failed the window.
        record: ClaimRecordKind,
        /// Version the record carries.
        version: StateSchemaVersion,
        /// Oldest version this binary reads.
        minimum_supported: StateSchemaVersion,
    },
    /// An identifier or scope failed identity validation.
    Identity(AgentIdentityError),
    /// The claim object's inline content exceeds the bounded size.
    ContentTooLarge {
        /// Inline size the object carried.
        bytes: usize,
        /// Largest inline size a claim object may carry.
        maximum: usize,
    },
    /// The claim carries more evidence artifacts than the bound allows.
    EvidenceOverflow {
        /// Number of evidence references carried.
        count: usize,
        /// Largest evidence count allowed.
        maximum: usize,
    },
    /// The claim's access set carries more capabilities than the bound allows.
    AccessOverflow {
        /// Number of capabilities carried.
        count: usize,
        /// Largest capability count allowed.
        maximum: usize,
    },
    /// The confidence value exceeds 10_000 basis points.
    ConfidenceOutOfRange {
        /// Confidence the record carried.
        confidence_bps: u16,
    },
    /// A bounded reference field exceeds its length bound.
    ReferenceTooLong {
        /// Field that carried the reference.
        field: &'static str,
        /// Length the value carried.
        length: usize,
        /// Largest length the field allows.
        maximum: usize,
    },
    /// The embedding metadata is malformed.
    InvalidEmbeddingRef {
        /// What was malformed.
        message: String,
    },
    /// A loaded claim's trust status and transition count contradict each
    /// other: `Proposed` if and only if zero transitions.
    TrustIncoherent {
        /// Claim that failed the invariant.
        claim_id: ClaimId,
    },
    /// A claim's recorded statement fingerprint does not describe its own
    /// subject/predicate/object.
    StatementDigestMismatch {
        /// Claim that failed the invariant.
        claim_id: ClaimId,
        /// Fingerprint the record carried.
        recorded: AgentContentDigest,
        /// Fingerprint the record's own statement derives.
        derived: AgentContentDigest,
    },
    /// A record could not be encoded or decoded.
    Encoding {
        /// What failed.
        message: String,
    },
    /// The backing store failed.
    Backend {
        /// Backend that failed.
        backend: String,
        /// What failed.
        message: String,
    },
    /// A different claim already exists under the claim id.
    AlreadyExists {
        /// Claim id that collided.
        claim_id: ClaimId,
    },
    /// The claim does not exist in the addressed scope.
    ///
    /// Deliberately identical for absent and out-of-scope claims, so an
    /// unauthorized caller cannot distinguish them (scenario 18).
    NotFound {
        /// Claim that was addressed.
        claim_id: ClaimId,
    },
    /// An appended claim must be born `Proposed` with zero transitions
    /// (open decision 3).
    AppendNotProposed {
        /// Claim that was refused.
        claim_id: ClaimId,
    },
    /// An appended claim must carry the identity its own append operation
    /// derives, so a claim id can never be squatted ahead of the writer that
    /// derives it.
    AppendIdNotDerived {
        /// Claim id the record carried.
        claim_id: ClaimId,
        /// Claim id the record's operation id derives in the addressed scope.
        derived: ClaimId,
    },
    /// The operation id was already spent by a different operation.
    OperationConflict {
        /// Operation id that collided.
        operation_id: ClaimOperationId,
    },
    /// The requested trust transition is not in the legal table.
    IllegalTransition {
        /// Claim that was addressed.
        claim_id: ClaimId,
        /// Trust status the claim currently holds.
        from: ClaimTrustStatus,
        /// Trust status the request named.
        to: ClaimTrustStatus,
    },
    /// The claim's bounded transition history is full.
    TransitionHistoryFull {
        /// Claim that was addressed.
        claim_id: ClaimId,
        /// Largest transition count a claim may accumulate.
        maximum: u32,
    },
    /// Policy marks the claim consequential and the promotion carried no
    /// grant.
    PromotionGrantRequired {
        /// Claim whose promotion was refused.
        claim_id: ClaimId,
    },
    /// The promotion grant's checkpoint kind cannot authorize a promotion.
    PromotionGrantKind {
        /// Kind the grant carried.
        kind: AgentCheckpointKind,
    },
    /// The promotion grant was issued under a different tenant.
    PromotionGrantScope {
        /// Claim whose promotion was refused.
        claim_id: ClaimId,
    },
    /// The promotion grant does not cover this exact promotion.
    PromotionGrantRejected {
        /// Claim whose promotion was refused.
        claim_id: ClaimId,
        /// Why the grant validation failed.
        reason: AgentCheckpointGrantError,
    },
    /// The backend does not support the requested optional capability.
    CapabilityUnsupported {
        /// Stable label of the unsupported capability.
        capability: String,
    },
}

impl ClaimError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::SchemaVersionAhead { .. } => "schema-version-ahead",
            Self::SchemaVersionTooOld { .. } => "schema-version-too-old",
            Self::Identity(inner) => inner.code(),
            Self::ContentTooLarge { .. } => "claim-content-too-large",
            Self::EvidenceOverflow { .. } => "claim-evidence-overflow",
            Self::AccessOverflow { .. } => "claim-access-overflow",
            Self::ConfidenceOutOfRange { .. } => "claim-confidence-out-of-range",
            Self::ReferenceTooLong { .. } => "claim-reference-too-long",
            Self::InvalidEmbeddingRef { .. } => "claim-embedding-invalid",
            Self::TrustIncoherent { .. } => "claim-trust-incoherent",
            Self::StatementDigestMismatch { .. } => "claim-statement-digest-mismatch",
            Self::Encoding { .. } => "claim-encoding-failed",
            Self::Backend { .. } => "claim-backend-failed",
            Self::AlreadyExists { .. } => "claim-already-exists",
            Self::NotFound { .. } => "claim-not-found",
            Self::AppendNotProposed { .. } => "claim-append-not-proposed",
            Self::AppendIdNotDerived { .. } => "claim-append-id-not-derived",
            Self::OperationConflict { .. } => "claim-operation-conflict",
            Self::IllegalTransition { .. } => "claim-transition-illegal",
            Self::TransitionHistoryFull { .. } => "claim-transition-history-full",
            Self::PromotionGrantRequired { .. } => "claim-promotion-grant-required",
            Self::PromotionGrantKind { .. } => "claim-promotion-grant-kind",
            Self::PromotionGrantScope { .. } => "claim-promotion-grant-scope",
            Self::PromotionGrantRejected { .. } => "claim-promotion-grant-rejected",
            Self::CapabilityUnsupported { .. } => "claim-capability-unsupported",
        }
    }
}

impl Display for ClaimError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaVersionAhead {
                record,
                version,
                current,
            } => write!(
                f,
                "the {} record carries schema version {}, newer than the current version {}",
                record.as_label(),
                version.get(),
                current.get()
            ),
            Self::SchemaVersionTooOld {
                record,
                version,
                minimum_supported,
            } => write!(
                f,
                "the {} record carries schema version {}, older than the oldest supported \
                 version {}",
                record.as_label(),
                version.get(),
                minimum_supported.get()
            ),
            Self::Identity(inner) => Display::fmt(inner, f),
            Self::ContentTooLarge { bytes, maximum } => write!(
                f,
                "the claim object carries {bytes} inline bytes; at most {maximum} are allowed"
            ),
            Self::EvidenceOverflow { count, maximum } => write!(
                f,
                "the claim carries {count} evidence references; at most {maximum} are allowed"
            ),
            Self::AccessOverflow { count, maximum } => write!(
                f,
                "the claim's access set carries {count} capabilities; at most {maximum} are \
                 allowed"
            ),
            Self::ConfidenceOutOfRange { confidence_bps } => write!(
                f,
                "the confidence value {confidence_bps} exceeds 10000 basis points"
            ),
            Self::ReferenceTooLong {
                field,
                length,
                maximum,
            } => write!(
                f,
                "the {field} reference is {length} bytes long; at most {maximum} are allowed"
            ),
            Self::InvalidEmbeddingRef { message } => {
                write!(f, "the embedding metadata is malformed: {message}")
            }
            Self::TrustIncoherent { claim_id } => write!(
                f,
                "claim {claim_id} carries a trust status and transition count that contradict \
                 each other"
            ),
            Self::StatementDigestMismatch {
                claim_id,
                recorded,
                derived,
            } => write!(
                f,
                "claim {claim_id} records statement fingerprint {recorded}, but its own \
                 statement derives {derived}"
            ),
            Self::Encoding { message } => write!(f, "the record could not be encoded: {message}"),
            Self::Backend { backend, message } => {
                write!(f, "the {backend} backend failed: {message}")
            }
            Self::AlreadyExists { claim_id } => {
                write!(f, "a different claim already exists under id {claim_id}")
            }
            Self::NotFound { claim_id } => {
                write!(f, "claim {claim_id} does not exist in the addressed scope")
            }
            Self::AppendNotProposed { claim_id } => write!(
                f,
                "claim {claim_id} was refused: an appended claim must be born proposed with \
                 zero transitions"
            ),
            Self::AppendIdNotDerived { claim_id, derived } => write!(
                f,
                "claim {claim_id} was refused: its append operation derives claim id {derived} \
                 in the addressed scope"
            ),
            Self::OperationConflict { operation_id } => write!(
                f,
                "operation {operation_id} was already spent by a different operation"
            ),
            Self::IllegalTransition { claim_id, from, to } => write!(
                f,
                "claim {claim_id} cannot transition from {} to {}",
                from.as_label(),
                to.as_label()
            ),
            Self::TransitionHistoryFull { claim_id, maximum } => write!(
                f,
                "claim {claim_id} already carries {maximum} transitions, the bounded maximum"
            ),
            Self::PromotionGrantRequired { claim_id } => write!(
                f,
                "policy marks claim {claim_id} consequential and the promotion carried no grant"
            ),
            Self::PromotionGrantKind { kind } => write!(
                f,
                "a {kind:?} checkpoint grant cannot authorize a claim promotion"
            ),
            Self::PromotionGrantScope { claim_id } => write!(
                f,
                "the promotion grant for claim {claim_id} was issued under a different tenant"
            ),
            Self::PromotionGrantRejected { claim_id, reason } => write!(
                f,
                "the promotion grant for claim {claim_id} was rejected: {} ({reason})",
                reason.code()
            ),
            Self::CapabilityUnsupported { capability } => write!(
                f,
                "the backend does not support the optional {capability} capability"
            ),
        }
    }
}

impl Error for ClaimError {}

impl From<AgentIdentityError> for ClaimError {
    fn from(error: AgentIdentityError) -> Self {
        Self::Identity(error)
    }
}
