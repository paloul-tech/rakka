//! Memory scopes, stores, and context snapshots.
//!
//! Owns the short-term session-memory trait scoped `(TenantId, AgentId,
//! AgentRunId)` with idempotent appends keyed by [`MemoryOperationId`], an ordered
//! monotonic sequence, and classification metadata; the agent-private long-term
//! memory trait scoped `(TenantId, AgentId)`, declared here so the session and
//! snapshot identities cannot bake in an incompatible scope before phase 2 fills
//! the stores; and the immutable [`MemoryContextSnapshot`] persisted before every
//! model effect, which a retry reuses so that drift in a store or an index cannot
//! change a retried model input.
//!
//! Retrieved memory is untrusted context: it passes the guardrail chain like any
//! other model input, and it can never replace system instructions or widen tool
//! capabilities ([specification 13.5](../../../docs/plans/rakka-agent/spec.md)).
//! The in-memory implementations live here; the PostgreSQL session and snapshot
//! stores live in `rakka-agent-postgres`, and the communal knowledge graph is a
//! later milestone.
//!
//! Specification: sections 13.1, 13.2, 13.5, and the short-term clauses of 13.6;
//! the private trait of 13.3 is declared here so scopes are fixed early. Filled
//! by slice 1.11; the private store by slice 2.1; the retrieval path that fills
//! a snapshot's private selections by slice 2.2 ([`crate::retrieval`]); the
//! communal store by the phase-2 graph slice.
//!
//! # What memory is, and is not
//!
//! Memory is application-domain context. It is never the correctness source: the
//! durable run, inbox, outbox, timer, checkpoint, and effect records are, and a
//! session store that is empty, lagging, or unavailable can never make a run
//! resume incorrectly ([specification 13.1](../../../docs/plans/rakka-agent/spec.md)).
//! The loop keeps no turn content of its own — it records the task's bounded
//! input once as the session's opening [`MemoryEntryRole::User`] entry when the
//! first model call is prepared, hands each turn to session memory at
//! [`crate::loop_runtime::AgentLoopPhase::RecordingTurn`], and drops them — so a
//! run that iterates a hundred times persists no more of its own state than one
//! that iterates once
//! ([specification 9.6](../../../docs/plans/rakka-agent/spec.md)).
//!
//! # What slice 1.5 landed
//!
//! [`AgentContextSnapshotRef`]: the *reference* the durable loop state carries
//! when a model call is prepared
//! ([specification 9.4](../../../docs/plans/rakka-agent/spec.md)). It is an
//! identity and a version, no content, because the loop only needs to name the
//! snapshot a model effect was prepared against, and a retry only needs to name
//! the *same* one. Slice 1.11 fills in the [`MemoryContextSnapshot`] the reference
//! points at — persisted immutably in a [`ContextSnapshotStore`] — without moving
//! the reference or changing what the loop persists.

use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use rakka_agent_workflow::{AgentTimestampMillis, StateSchemaVersion};
use serde::{Deserialize, Deserializer, Serialize};

use crate::definition::{AgentGuardrailStageId, AgentRevisionNumber};
use crate::identity::{validated_id, AgentIdentityResult, AgentRunScope, AgentScope};
use crate::schema::{
    AgentRecordKind, AgentSchemaError, AgentSchemaPolicy, VersionedAgentRecord,
    CURRENT_AGENT_MEMORY_CONTEXT_SNAPSHOT_SCHEMA_VERSION,
    CURRENT_AGENT_PRIVATE_MEMORY_SCHEMA_VERSION, CURRENT_AGENT_SESSION_MEMORY_SCHEMA_VERSION,
};
use crate::task::{AgentContentDigest, AgentTaskContent};

validated_id! {
    /// Stable identity of one immutable context snapshot
    /// ([specification 13.5](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// It is *derived* from the run's full scope — tenant, agent, and run — and
    /// the turn, never generated, so a model effect retried after a crash names
    /// the snapshot it was first prepared against rather than a freshly
    /// assembled one. That is what makes index or store drift unable to change a
    /// retried model input.
    pub AgentContextSnapshotId, "agent_context_snapshot_id"
}

/// The versioned reference to the immutable context one model effect was
/// prepared against.
///
/// The loop persists it before every model effect and reuses it on every retry
/// of that effect. The snapshot's *content* — assembled instructions, session
/// history, retrieved memory — is the [`MemoryContextSnapshot`] a
/// [`ContextSnapshotStore`] holds under this reference; the loop carries only the
/// reference, which is all it needs in order to be correct about it.
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
    /// joining the ids literally: three maximal ids would overflow the
    /// identity bound — a run stranded at its first turn by the length of its
    /// own name — and the digest keeps the derivation bounded without giving
    /// up the key's injectivity.
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

// ===========================================================================
// Short-term session memory ([specification 13.2]).
// ===========================================================================

/// Largest inline session-memory entry, in serialized bytes.
///
/// A larger message, tool result, or reasoning trace belongs behind an artifact
/// reference: the authoritative session record stays bounded, and content
/// loading happens through bounded adapters, never inside a run transition
/// ([specification 13.1](../../../docs/plans/rakka-agent/spec.md)).
pub const AGENT_SESSION_MEMORY_ENTRY_MAX_BYTES: usize = 8 * 1024;

/// Largest bounded session window one context snapshot may assemble.
///
/// The window is what a model turn is computed from, so it is capped: a run's
/// history can grow without bound in the store, but the working set handed to a
/// model turn cannot.
pub const AGENT_SESSION_WINDOW_MAX_ENTRIES: usize = 64;

/// The monotonic order key of one session-memory entry within its run
/// ([specification 13.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// Sequences are assigned by the run that produces the entry, counting from one,
/// so re-driving an interrupted flush writes the same entry at the same position
/// rather than reordering the session. Ordering is per run; two runs of the same
/// agent order independently, because their session memory is isolated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MemorySequence(u64);

impl MemorySequence {
    /// The first sequence a run assigns.
    pub const FIRST: Self = Self(1);

    /// Creates a sequence from a raw value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The raw sequence value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next sequence after this one, saturating at the maximum.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl Display for MemorySequence {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

validated_id! {
    /// Stable identity of one session-memory entry
    /// ([specification 13.2](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Derived from the run scope and the entry's logical slot, so the same
    /// logical turn record reconstructed after a crash names one entry rather
    /// than two.
    pub MemoryEntryId, "memory_entry_id"
}

validated_id! {
    /// The idempotent append key of one session-memory write
    /// ([specification 13.2](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// An append replay under the same operation id returns the original logical
    /// result without creating another entry, which is what makes a re-driven
    /// flush harmless. It is derived, never generated: the run reconstructs the
    /// same value on any node, after any restart.
    pub MemoryOperationId, "memory_operation_id"
}

impl MemoryOperationId {
    /// Derives the append key of one turn-produced entry.
    ///
    /// The run scope and the discriminator both enter through a digest, so the
    /// result stays within the identity bound however long a tool call id makes
    /// the discriminator, and two runs naming their agents and runs alike never
    /// collide because the scope's injective key is part of the digest input.
    /// The derivation is pure, so a re-driven flush produces the same key and the
    /// store deduplicates it. The `op` salt distinguishes it from the matching
    /// [`MemoryEntryId`] over the same slot.
    pub fn derive(
        scope: &AgentRunScope,
        discriminator: impl AsRef<str>,
    ) -> AgentIdentityResult<Self> {
        let input = format!("op|{}|{}", scope.key(), discriminator.as_ref());
        let digest = AgentContentDigest::of_bytes(input.as_bytes());
        Self::new(format!("mem-op-{}", digest.value))
    }

    /// Derives an agent-scoped operation key for a private-memory write no
    /// single run owns — a tombstone, a deletion, an administrative
    /// consolidation ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The `agent-op` salt keeps this derivation domain disjoint from the
    /// run-scoped `op` domain above, so no discriminator, however adversarial,
    /// can make an agent-scoped key collide with a run-scoped one.
    pub fn derive_for_agent(
        scope: &AgentScope,
        discriminator: impl AsRef<str>,
    ) -> AgentIdentityResult<Self> {
        let input = format!("agent-op|{}|{}", scope.key(), discriminator.as_ref());
        let digest = AgentContentDigest::of_bytes(input.as_bytes());
        Self::new(format!("mem-op-{}", digest.value))
    }
}

impl MemoryEntryId {
    /// Derives the stable identity of one turn-produced entry, over the same
    /// scope and discriminator as its [`MemoryOperationId`] but with an `entry`
    /// salt so the two identities are distinct values.
    pub fn derive(
        scope: &AgentRunScope,
        discriminator: impl AsRef<str>,
    ) -> AgentIdentityResult<Self> {
        let input = format!("entry|{}|{}", scope.key(), discriminator.as_ref());
        let digest = AgentContentDigest::of_bytes(input.as_bytes());
        Self::new(format!("mem-entry-{}", digest.value))
    }
}

/// The role a session-memory entry plays in the conversation
/// ([specification 13.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// The role is durable metadata, never a trust grant: an [`Self::Assistant`] or
/// [`Self::ToolResult`] entry is untrusted context when it is read back into a
/// later turn, exactly like retrieved memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum MemoryEntryRole {
    /// A system or developer instruction.
    System,
    /// An external or user-supplied input.
    User,
    /// A turn the model produced.
    Assistant,
    /// The result of one tool call.
    ToolResult,
    /// A rolling summary that stands in for compacted history.
    Summary,
}

impl MemoryEntryRole {
    /// Stable kebab-case label for errors, logs, and bounded metric labels.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::ToolResult => "tool-result",
            Self::Summary => "summary",
        }
    }
}

impl Display for MemoryEntryRole {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Classification and redaction status of one memory record
/// ([specification 13.1](../../../docs/plans/rakka-agent/spec.md),
/// [17.14](../../../docs/plans/rakka-agent/spec.md)).
///
/// It travels with every entry and snapshot selection so a downstream policy can
/// decide what a principal may read without loading the content, and so a
/// redacted entry preserves its digest and provenance while omitting the bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum MemoryClassification {
    /// Ordinary application content, retained inline under policy.
    Unclassified,
    /// Content a policy marks sensitive; retained, but flagged for access
    /// control and audit.
    Sensitive,
    /// Content whose bytes were withheld by a redaction policy; the digest and
    /// provenance remain, the inline value does not.
    Redacted,
}

impl MemoryClassification {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Unclassified => "unclassified",
            Self::Sensitive => "sensitive",
            Self::Redacted => "redacted",
        }
    }

    /// Whether the record's inline bytes were withheld.
    #[must_use]
    pub const fn is_redacted(self) -> bool {
        matches!(self, Self::Redacted)
    }
}

impl Display for MemoryClassification {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// One ordered short-term session-memory entry
/// ([specification 13.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// It is bounded in every dimension and carries no resolved credential or secret
/// material. Content is either a bounded inline value or an immutable artifact
/// reference, and its digest identifies the exact value the entry recorded even
/// when the bytes are redacted or held behind an artifact
/// ([specification 13.1](../../../docs/plans/rakka-agent/spec.md)).
///
/// The fields are public so a store adapter can rebuild an entry on load;
/// [`Self::validate`] runs inside [`Self::new`] and on deserialization, so an
/// out-of-bounds entry can neither cross the wire nor load from a durable record.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SessionMemoryEntry {
    schema_version: StateSchemaVersion,
    /// Stable identity of the entry.
    pub entry_id: MemoryEntryId,
    /// The idempotent append key the store deduplicates on.
    pub operation_id: MemoryOperationId,
    /// The monotonic order key within the run.
    pub sequence: MemorySequence,
    /// The role the entry plays in the conversation.
    pub role: MemoryEntryRole,
    /// The bounded inline content or immutable artifact reference.
    pub content: AgentTaskContent,
    /// A fingerprint of the content, stable even when the bytes are redacted.
    pub content_digest: AgentContentDigest,
    /// The turn that produced the entry.
    pub turn: u64,
    /// The source operation that produced the entry, when one applies (a model
    /// or tool effect result). It is observability provenance, never authority.
    pub source: Option<String>,
    /// The classification and redaction status of the content.
    pub classification: MemoryClassification,
    /// When the run recorded the entry.
    pub recorded_at: AgentTimestampMillis,
    /// The record revision, so a later compaction can supersede an entry in
    /// place rather than losing its provenance.
    pub revision: AgentRevisionNumber,
}

impl SessionMemoryEntry {
    /// Builds a session-memory entry, rejecting content that exceeds the bound.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        entry_id: MemoryEntryId,
        operation_id: MemoryOperationId,
        sequence: MemorySequence,
        role: MemoryEntryRole,
        content: AgentTaskContent,
        turn: u64,
        source: Option<String>,
        classification: MemoryClassification,
        recorded_at: AgentTimestampMillis,
    ) -> Result<Self, MemoryError> {
        let content_digest = content.digest();
        let entry = Self {
            schema_version: CURRENT_AGENT_SESSION_MEMORY_SCHEMA_VERSION,
            entry_id,
            operation_id,
            sequence,
            role,
            content,
            content_digest,
            turn,
            source,
            classification,
            recorded_at,
            revision: AgentRevisionNumber::INITIAL,
        };
        entry.validate()?;
        Ok(entry)
    }

    /// Serialized size of the entry, in bytes.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .unwrap_or(0)
    }

    /// Rejects an entry whose inline content exceeds
    /// [`AGENT_SESSION_MEMORY_ENTRY_MAX_BYTES`].
    pub fn validate(&self) -> Result<(), MemoryError> {
        if let Some(value) = self.content.inline_value() {
            let bytes = serde_json::to_vec(value)
                .map_err(|error| MemoryError::Encoding {
                    message: error.to_string(),
                })?
                .len();
            if bytes > AGENT_SESSION_MEMORY_ENTRY_MAX_BYTES {
                return Err(MemoryError::EntryTooLarge {
                    bytes,
                    maximum: AGENT_SESSION_MEMORY_ENTRY_MAX_BYTES,
                });
            }
        }
        Ok(())
    }
}

impl VersionedAgentRecord for SessionMemoryEntry {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::SessionMemoryEntry;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// The wire and durable shape of [`SessionMemoryEntry`], validated on load.
#[derive(Deserialize)]
struct SessionMemoryEntryRecord {
    schema_version: StateSchemaVersion,
    entry_id: MemoryEntryId,
    operation_id: MemoryOperationId,
    sequence: MemorySequence,
    role: MemoryEntryRole,
    content: AgentTaskContent,
    content_digest: AgentContentDigest,
    turn: u64,
    #[serde(default)]
    source: Option<String>,
    classification: MemoryClassification,
    recorded_at: AgentTimestampMillis,
    revision: AgentRevisionNumber,
}

impl<'de> Deserialize<'de> for SessionMemoryEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let record = SessionMemoryEntryRecord::deserialize(deserializer)?;
        let entry = Self {
            schema_version: record.schema_version,
            entry_id: record.entry_id,
            operation_id: record.operation_id,
            sequence: record.sequence,
            role: record.role,
            content: record.content,
            content_digest: record.content_digest,
            turn: record.turn,
            source: record.source,
            classification: record.classification,
            recorded_at: record.recorded_at,
            revision: record.revision,
        };
        entry.validate().map_err(serde::de::Error::custom)?;
        Ok(entry)
    }
}

/// A bounded read cursor over one run's session memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionMemoryCursor {
    after: Option<MemorySequence>,
    limit: usize,
}

impl SessionMemoryCursor {
    /// The default page size a read returns when none is requested.
    pub const DEFAULT_LIMIT: usize = 32;

    /// A cursor over the earliest entries, from the start.
    #[must_use]
    pub const fn start() -> Self {
        Self {
            after: None,
            limit: Self::DEFAULT_LIMIT,
        }
    }

    /// A cursor over the entries after `sequence`.
    #[must_use]
    pub const fn after(sequence: MemorySequence) -> Self {
        Self {
            after: Some(sequence),
            limit: Self::DEFAULT_LIMIT,
        }
    }

    /// Sets the page size, clamped to [`AGENT_SESSION_WINDOW_MAX_ENTRIES`].
    #[must_use]
    pub const fn with_limit(mut self, limit: usize) -> Self {
        self.limit = if limit > AGENT_SESSION_WINDOW_MAX_ENTRIES {
            AGENT_SESSION_WINDOW_MAX_ENTRIES
        } else if limit == 0 {
            1
        } else {
            limit
        };
        self
    }

    /// The sequence this cursor reads after, when it is not at the start.
    #[must_use]
    pub const fn position(self) -> Option<MemorySequence> {
        self.after
    }

    /// The page size.
    #[must_use]
    pub const fn limit(self) -> usize {
        self.limit
    }
}

impl Default for SessionMemoryCursor {
    fn default() -> Self {
        Self::start()
    }
}

/// One bounded page of session-memory entries, oldest first.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionMemoryPage {
    /// The entries in this page, ordered by ascending sequence.
    pub entries: Vec<SessionMemoryEntry>,
    /// The cursor for the next page, when more entries remain.
    pub next: Option<SessionMemoryCursor>,
}

/// Tenant-configurable retention of a terminal run's session records
/// (open decision 7, [specification 13.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// The policy is deployment configuration — owned per tenant and passed to
/// each purge call, never stored per row — because retention is evaluated at
/// sweep time, and a policy frozen into the row at write time could never
/// tighten. What is durable is the rows themselves and the run's terminal
/// timestamp; the store's job is to enforce the hold and the due time
/// *inside* the call, so no sweep can forget them. Export is the ordinary
/// bounded cursor read, taken before the purge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRetentionPolicy {
    retain_for_millis: u64,
    legal_hold: bool,
}

impl SessionRetentionPolicy {
    /// The bounded default retention after a terminal run: 30 days.
    pub const DEFAULT_RETAIN_FOR_MILLIS: u64 = 30 * 24 * 60 * 60 * 1000;

    /// The bounded default policy: 30 days, no hold.
    #[must_use]
    pub const fn bounded_default() -> Self {
        Self {
            retain_for_millis: Self::DEFAULT_RETAIN_FOR_MILLIS,
            legal_hold: false,
        }
    }

    /// Sets how long a terminal run's records are retained.
    #[must_use]
    pub const fn with_retain_for_millis(mut self, millis: u64) -> Self {
        self.retain_for_millis = millis;
        self
    }

    /// Places or lifts a legal hold. A held run's records survive every purge.
    #[must_use]
    pub const fn with_legal_hold(mut self, hold: bool) -> Self {
        self.legal_hold = hold;
        self
    }

    /// How long a terminal run's records are retained, in milliseconds.
    #[must_use]
    pub const fn retain_for_millis(self) -> u64 {
        self.retain_for_millis
    }

    /// Whether a legal hold is in force.
    #[must_use]
    pub const fn legal_hold(self) -> bool {
        self.legal_hold
    }

    /// The instant a run that went terminal at `terminal_at` becomes purgeable.
    #[must_use]
    pub const fn purge_due_at(self, terminal_at: AgentTimestampMillis) -> AgentTimestampMillis {
        AgentTimestampMillis::new(
            terminal_at
                .as_millis()
                .saturating_add(self.retain_for_millis),
        )
    }
}

impl Default for SessionRetentionPolicy {
    fn default() -> Self {
        Self::bounded_default()
    }
}

/// What one terminal-run purge call did.
///
/// Held and not-yet-due are values, not errors, so a fleet sweep over many
/// runs reports what it skipped instead of aborting on the first held run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionPurgeOutcome {
    /// The run's records were deleted; a replay purges nothing and reports
    /// zero.
    Purged {
        /// How many records this call removed.
        entries: u64,
    },
    /// A legal hold is in force; nothing was deleted.
    Held,
    /// The retention window has not elapsed; nothing was deleted.
    NotYetDue,
}

/// The future a [`SessionMemoryStore`] or [`ContextSnapshotStore`] operation
/// returns.
pub type MemoryFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, MemoryError>> + Send + 'a>>;

/// The durable short-term session-memory store, scoped `(TenantId, AgentId,
/// AgentRunId)` ([specification 13.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// Reads or writes under one run are never visible through another run's session
/// API, including another run of the same agent: the scope is the isolation
/// boundary, and every method addresses it. An append is idempotent on the
/// entry's [`MemoryOperationId`] — a replay returns the original logical result
/// without creating a second entry — which is what makes a re-driven flush after
/// a crash between the run's transition and the store write harmless.
///
/// The trait is object-safe so a run can hold `Arc<dyn SessionMemoryStore>` and
/// swap backends without touching the run entity; the in-memory implementation
/// lives here and the PostgreSQL one in `rakka-agent-postgres`.
pub trait SessionMemoryStore: Send + Sync + 'static {
    /// Stable backend name, used in telemetry.
    fn backend_name(&self) -> &'static str;

    /// Appends one entry, idempotently on its operation id.
    ///
    /// A replay with the same operation id returns the entry already stored
    /// under it rather than writing a second: the first durable append under an
    /// operation id wins, and the store never overwrites what it holds.
    /// Operation ids are derived purely, so two logically distinct writes never
    /// share one; a write under a *new* operation id that claims an
    /// already-taken sequence fails closed with
    /// [`MemoryError::SequenceConflict`].
    fn append<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        entry: &'a SessionMemoryEntry,
    ) -> MemoryFuture<'a, SessionMemoryEntry>;

    /// Reads one bounded page of the run's session, oldest first.
    fn read<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        cursor: SessionMemoryCursor,
    ) -> MemoryFuture<'a, SessionMemoryPage>;

    /// Deletes a terminal run's session entries once its retention has
    /// elapsed, honoring the policy's legal hold (open decision 7,
    /// [specification 13.2](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The caller supplies the run's durable terminal timestamp; the store
    /// enforces the hold and the due time inside the call, so no sweep can
    /// forget them. The purge is idempotent — a replay finds nothing left and
    /// reports zero — and it is a bounded, deployment-invoked operation,
    /// never a resident sweeper.
    fn purge_run<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        policy: &'a SessionRetentionPolicy,
        terminal_at: AgentTimestampMillis,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, SessionPurgeOutcome>;
}

/// An in-memory session-memory store, for tests and single-process deployments.
#[derive(Debug, Clone, Default)]
pub struct InMemorySessionMemoryStore {
    entries: Arc<Mutex<BTreeMap<String, BTreeMap<u64, SessionMemoryEntry>>>>,
    operations: Arc<Mutex<BTreeMap<String, BTreeMap<String, u64>>>>,
}

impl InMemorySessionMemoryStore {
    /// Creates an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// How many entries one run's session holds.
    #[must_use]
    pub fn len(&self, scope: &AgentRunScope) -> usize {
        self.entries
            .lock()
            .expect("the session store should not be poisoned")
            .get(&scope.key())
            .map_or(0, BTreeMap::len)
    }

    /// Whether one run's session is empty.
    #[must_use]
    pub fn is_empty(&self, scope: &AgentRunScope) -> bool {
        self.len(scope) == 0
    }
}

impl SessionMemoryStore for InMemorySessionMemoryStore {
    fn backend_name(&self) -> &'static str {
        "in-memory"
    }

    fn append<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        entry: &'a SessionMemoryEntry,
    ) -> MemoryFuture<'a, SessionMemoryEntry> {
        Box::pin(async move {
            let key = scope.key();
            let mut operations = self
                .operations
                .lock()
                .expect("the session store should not be poisoned");
            let mut entries = self
                .entries
                .lock()
                .expect("the session store should not be poisoned");

            let op_key = entry.operation_id.as_str().to_string();
            if let Some(sequence) = operations
                .get(&key)
                .and_then(|ops| ops.get(&op_key))
                .copied()
            {
                // The operation replayed. Return the entry already stored under
                // it, so the append is harmless and answers with the original
                // logical result.
                let existing = entries
                    .get(&key)
                    .and_then(|run| run.get(&sequence))
                    .cloned();
                return existing.ok_or(MemoryError::OperationConflict {
                    operation_id: entry.operation_id.clone(),
                });
            }

            let run = entries.entry(key.clone()).or_default();
            match run.get(&entry.sequence.get()) {
                Some(existing) if existing == entry => {}
                Some(_) => {
                    return Err(MemoryError::SequenceConflict {
                        sequence: entry.sequence,
                    })
                }
                None => {
                    run.insert(entry.sequence.get(), entry.clone());
                }
            }
            operations
                .entry(key)
                .or_default()
                .insert(op_key, entry.sequence.get());
            Ok(entry.clone())
        })
    }

    fn read<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        cursor: SessionMemoryCursor,
    ) -> MemoryFuture<'a, SessionMemoryPage> {
        Box::pin(async move {
            let entries = self
                .entries
                .lock()
                .expect("the session store should not be poisoned");
            let Some(run) = entries.get(&scope.key()) else {
                return Ok(SessionMemoryPage {
                    entries: Vec::new(),
                    next: None,
                });
            };

            let start = cursor.position().map_or(0, |after| after.get() + 1);
            let mut page: Vec<SessionMemoryEntry> = run
                .range(start..)
                .map(|(_, entry)| entry.clone())
                .take(cursor.limit() + 1)
                .collect();

            let next = (page.len() > cursor.limit())
                .then(|| {
                    page.pop();
                    page.last().map(|entry| {
                        SessionMemoryCursor::after(entry.sequence).with_limit(cursor.limit())
                    })
                })
                .flatten();

            Ok(SessionMemoryPage {
                entries: page,
                next,
            })
        })
    }

    fn purge_run<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        policy: &'a SessionRetentionPolicy,
        terminal_at: AgentTimestampMillis,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, SessionPurgeOutcome> {
        Box::pin(async move {
            if policy.legal_hold() {
                return Ok(SessionPurgeOutcome::Held);
            }
            if now < policy.purge_due_at(terminal_at) {
                return Ok(SessionPurgeOutcome::NotYetDue);
            }
            let key = scope.key();
            let mut operations = self
                .operations
                .lock()
                .expect("the session store should not be poisoned");
            let mut entries = self
                .entries
                .lock()
                .expect("the session store should not be poisoned");
            let removed = entries.remove(&key).map_or(0, |run| run.len() as u64);
            operations.remove(&key);
            Ok(SessionPurgeOutcome::Purged { entries: removed })
        })
    }
}

// ===========================================================================
// Immutable memory context snapshot ([specification 13.5]).
// ===========================================================================

/// The trust status of assembled context handed to a model turn.
///
/// Everything a snapshot assembles — the run's own prior turns, retrieved
/// private memory, communal claims — is [`Self::Untrusted`]: it is contextual
/// data that a guardrail chain evaluates at the memory boundary, and it can
/// never replace a system instruction or expand a tool capability
/// ([specification 13.5](../../../docs/plans/rakka-agent/spec.md)). The enum
/// exists so the type system records that fact rather than a comment alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum MemoryTrust {
    /// Contextual data that must be treated as untrusted model input.
    Untrusted,
}

impl MemoryTrust {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Untrusted => "untrusted",
        }
    }
}

/// One session entry, or its immutable reference, exactly as it entered a
/// snapshot's bounded window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotSessionEntry {
    /// Identity of the entry this selection points at.
    pub entry_id: MemoryEntryId,
    /// The sequence the entry held in the run's session.
    pub sequence: MemorySequence,
    /// The role the entry played.
    pub role: MemoryEntryRole,
    /// The exact bounded content or immutable artifact reference used.
    pub content: AgentTaskContent,
    /// The content digest, so the snapshot pins the exact value it used.
    pub content_digest: AgentContentDigest,
    /// The classification of the entry.
    pub classification: MemoryClassification,
}

/// One retrieval query a snapshot ran, with the retriever version that ran it
/// ([specification 13.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// Phase 2 populates these for private and communal retrieval; a session-only
/// snapshot records the bounded-window read that assembled it, so the exact
/// retrieval that produced a model input is always reconstructable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRetrieval {
    /// The bounded query text or window descriptor.
    pub query: String,
    /// The retriever that ran it.
    pub retriever: String,
    /// The retriever version, so an upgrade is an explicit change.
    pub retriever_version: AgentRevisionNumber,
    /// An index watermark or embedding version, when the backend reports one.
    pub index_watermark: Option<String>,
}

/// One memory-ingress guardrail finding recorded on a snapshot selection
/// ([specification 16](../../../docs/plans/rakka-agent/spec.md)).
///
/// Snapshot-owned rather than reusing the chain's decision types, because those
/// are serialize-only telemetry shapes and a snapshot must round-trip through
/// its store. The stage revision is what makes a recorded transform
/// reconstructable: the pair (revision, input) fixes the output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotIngressRecord {
    /// The stage that decided.
    pub stage: AgentGuardrailStageId,
    /// The revision the stage evaluated under.
    pub revision: AgentRevisionNumber,
    /// Stable machine-readable reason code.
    pub reason_code: String,
}

/// One private memory, exactly as it entered the snapshot
/// ([specification 13.5](../../../docs/plans/rakka-agent/spec.md): "selected
/// private memory IDs and exact content/references").
///
/// The snapshot embeds the content, not just the identity, for the same reason
/// [`SnapshotSessionEntry`] does: a model-effect retry must reuse the exact
/// input the first assembly selected, and an identity alone would let a
/// concurrent memory update or an eventually consistent index change a retried
/// model input (scenario 17). `(memory_id, revision)` still names the
/// authoritative record; store-internal state — retention, ledger operations,
/// audit references — deliberately stays out of model-adjacent data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotPrivateMemory {
    /// Identity of the private memory this selection used.
    pub memory_id: AgentPrivateMemoryId,
    /// The store revision the selection read.
    pub revision: AgentRevisionNumber,
    /// The memory type.
    pub kind: AgentPrivateMemoryKind,
    /// The exact content used — post-transform, when a memory-ingress stage
    /// rewrote it.
    pub content: AgentTaskContent,
    /// The digest of the content as included, recomputed after any transform.
    pub content_digest: AgentContentDigest,
    /// The classification of the content.
    pub classification: MemoryClassification,
    /// The record's confidence score in basis points.
    pub confidence_bps: u16,
    /// The retriever's deterministic relevance score in basis points.
    pub relevance_bps: u16,
    /// Metadata of the derived vector that ranked this memory, when the
    /// retriever reported one ([specification 13.5](../../../docs/plans/rakka-agent/spec.md):
    /// embedding version when available).
    #[serde(default)]
    pub embedding: Option<MemoryEmbeddingRef>,
    /// Every memory-ingress transform applied to the content, in stage order.
    #[serde(default)]
    pub transforms: Vec<SnapshotIngressRecord>,
    /// Every memory-ingress report-only finding, in stage order.
    #[serde(default)]
    pub reports: Vec<SnapshotIngressRecord>,
}

/// The prompt and context budget one snapshot accounted for
/// ([specification 13.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// It records what the assembled window cost, in entries and bytes, so a
/// downstream policy can reason about a turn's context budget without loading
/// the content.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotBudget {
    /// How many session entries the window carried.
    pub session_entries: usize,
    /// The serialized byte size of the assembled session content.
    pub session_bytes: usize,
    /// How many private memories the snapshot selected.
    pub private_memories: usize,
    /// The serialized byte size of the selected private-memory content.
    #[serde(default)]
    pub private_memory_bytes: usize,
    /// How many communal claims the snapshot selected.
    pub communal_claims: usize,
}

/// The immutable context one model effect was prepared against
/// ([specification 13.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// It is persisted before the model effect and reused on every retry, so a
/// dispatcher retry can never silently change the model's input through a
/// concurrent memory write or an eventually consistent index. Its content is
/// untrusted contextual data ([`Self::trust`]); its digest pins the exact bytes
/// it assembled.
///
/// The private selections are filled by the slice 2.2 retrieval path when a
/// retriever is wired ([`crate::retrieval::assemble_context`]); the communal
/// selections stay empty until the phase-2 graph slice delivers that store.
/// The scopes were fixed in phase 1 so a session-only snapshot and a phase-2
/// snapshot share one record shape.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MemoryContextSnapshot {
    schema_version: StateSchemaVersion,
    /// The reference this snapshot answers.
    pub reference: AgentContextSnapshotRef,
    /// The run whose session the snapshot assembled.
    pub scope: AgentRunScope,
    /// The turn the snapshot was assembled for.
    pub turn: u64,
    /// The trust status of all assembled context.
    pub trust: MemoryTrust,
    /// The exact bounded session entries or references the model turn used.
    pub session: Vec<SnapshotSessionEntry>,
    /// The retrievals that assembled the snapshot.
    pub retrievals: Vec<SnapshotRetrieval>,
    /// The private memory selections, exactly as retrieval selected them.
    pub private_memory: Vec<SnapshotPrivateMemory>,
    /// The communal claim selections; empty until the phase-2 graph slice.
    pub communal_claims: Vec<MemoryEntryId>,
    /// The trust, classification, and ranking policy revision in force.
    pub policy_revision: AgentRevisionNumber,
    /// The memory-ingress guardrail chain revision the private selections were
    /// evaluated under; `None` when no retrieval ran
    /// ([specification 16](../../../docs/plans/rakka-agent/spec.md)).
    #[serde(default)]
    pub ingress_revision: Option<AgentRevisionNumber>,
    /// The prompt and context budget the snapshot accounted for.
    pub budget: SnapshotBudget,
    /// A digest over the assembled content, so a corrupted or altered snapshot
    /// is detectable.
    pub content_digest: AgentContentDigest,
    /// When the snapshot was assembled.
    pub assembled_at: AgentTimestampMillis,
}

impl MemoryContextSnapshot {
    /// The snapshot identity.
    #[must_use]
    pub const fn reference(&self) -> &AgentContextSnapshotRef {
        &self.reference
    }

    /// Whether the assembled context is untrusted (it always is).
    #[must_use]
    pub const fn is_untrusted(&self) -> bool {
        matches!(self.trust, MemoryTrust::Untrusted)
    }

    /// Serialized size of the snapshot, in bytes.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .unwrap_or(0)
    }

    /// Recomputes the content digest over the assembled selections.
    ///
    /// The digest covers the session, retrieval, and selection content but not
    /// the timestamp, so two assemblies of the same content agree.
    #[must_use]
    pub fn compute_digest(&self) -> AgentContentDigest {
        let payload = serde_json::json!({
            "reference": self.reference,
            "session": self.session,
            "retrievals": self.retrievals,
            "private_memory": self.private_memory,
            "communal_claims": self.communal_claims,
            "policy_revision": self.policy_revision,
            "ingress_revision": self.ingress_revision,
        });
        AgentContentDigest::of_json(&payload)
    }
}

impl VersionedAgentRecord for MemoryContextSnapshot {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::MemoryContextSnapshot;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// The wire and durable shape of [`MemoryContextSnapshot`], schema-checked on
/// load.
#[derive(Deserialize)]
struct MemoryContextSnapshotRecord {
    schema_version: StateSchemaVersion,
    reference: AgentContextSnapshotRef,
    scope: AgentRunScope,
    turn: u64,
    trust: MemoryTrust,
    session: Vec<SnapshotSessionEntry>,
    retrievals: Vec<SnapshotRetrieval>,
    #[serde(default)]
    private_memory: Vec<SnapshotPrivateMemory>,
    #[serde(default)]
    communal_claims: Vec<MemoryEntryId>,
    policy_revision: AgentRevisionNumber,
    #[serde(default)]
    ingress_revision: Option<AgentRevisionNumber>,
    budget: SnapshotBudget,
    content_digest: AgentContentDigest,
    assembled_at: AgentTimestampMillis,
}

impl<'de> Deserialize<'de> for MemoryContextSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let record = MemoryContextSnapshotRecord::deserialize(deserializer)?;
        Ok(Self {
            schema_version: record.schema_version,
            reference: record.reference,
            scope: record.scope,
            turn: record.turn,
            trust: record.trust,
            session: record.session,
            retrievals: record.retrievals,
            private_memory: record.private_memory,
            communal_claims: record.communal_claims,
            policy_revision: record.policy_revision,
            ingress_revision: record.ingress_revision,
            budget: record.budget,
            content_digest: record.content_digest,
            assembled_at: record.assembled_at,
        })
    }
}

/// The durable store of immutable context snapshots
/// ([specification 13.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// A snapshot is content-addressed by its [`AgentContextSnapshotRef`] and never
/// changes once persisted. [`Self::persist`] is idempotent: the *first* assembly
/// of a reference wins, and every later persist of the same reference returns the
/// original — which is exactly what makes a model-effect retry reuse the snapshot
/// even when session memory has been appended to since. The store is object-safe
/// so a run can hold `Arc<dyn ContextSnapshotStore>`.
pub trait ContextSnapshotStore: Send + Sync + 'static {
    /// Stable backend name, used in telemetry.
    fn backend_name(&self) -> &'static str;

    /// Persists a snapshot immutably, returning the snapshot now stored under
    /// its reference.
    ///
    /// If a snapshot already exists under the reference, the original is
    /// returned unchanged: the store never overwrites an immutable snapshot, so
    /// a retry that re-assembles from newer memory still reads the first one.
    fn persist<'a>(
        &'a self,
        snapshot: &'a MemoryContextSnapshot,
    ) -> MemoryFuture<'a, MemoryContextSnapshot>;

    /// Loads the snapshot stored under a reference, if any.
    fn load<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        reference: &'a AgentContextSnapshotRef,
    ) -> MemoryFuture<'a, Option<MemoryContextSnapshot>>;

    /// Deletes a terminal run's snapshots once its retention has elapsed,
    /// honoring the policy's legal hold (open decision 7).
    ///
    /// Snapshots embed copies of the session entries — and, since slice 2.2,
    /// of the private-memory content — they were assembled from, so
    /// discharging a run's retention means purging its snapshots alongside its
    /// session rows: one without the other would keep the content the policy
    /// said to delete. A private memory tombstoned or deleted *after* a
    /// snapshot embedded it keeps that embedded copy until this purge removes
    /// the run's snapshots — immutability wins over withdrawal, the same rule
    /// a redacted session entry follows. Same contract as
    /// [`SessionMemoryStore::purge_run`]: bounded, idempotent,
    /// deployment-invoked.
    fn purge_run<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        policy: &'a SessionRetentionPolicy,
        terminal_at: AgentTimestampMillis,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, SessionPurgeOutcome>;
}

/// An in-memory snapshot store, for tests and single-process deployments.
#[derive(Debug, Clone, Default)]
pub struct InMemoryContextSnapshotStore {
    snapshots: Arc<Mutex<BTreeMap<String, BTreeMap<String, MemoryContextSnapshot>>>>,
}

impl InMemoryContextSnapshotStore {
    /// Creates an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// How many distinct snapshots one run holds.
    #[must_use]
    pub fn len(&self, scope: &AgentRunScope) -> usize {
        self.snapshots
            .lock()
            .expect("the snapshot store should not be poisoned")
            .get(&scope.key())
            .map_or(0, BTreeMap::len)
    }

    /// Whether one run holds no snapshots.
    #[must_use]
    pub fn is_empty(&self, scope: &AgentRunScope) -> bool {
        self.len(scope) == 0
    }
}

impl ContextSnapshotStore for InMemoryContextSnapshotStore {
    fn backend_name(&self) -> &'static str {
        "in-memory"
    }

    fn persist<'a>(
        &'a self,
        snapshot: &'a MemoryContextSnapshot,
    ) -> MemoryFuture<'a, MemoryContextSnapshot> {
        Box::pin(async move {
            let mut snapshots = self
                .snapshots
                .lock()
                .expect("the snapshot store should not be poisoned");
            let run = snapshots.entry(snapshot.scope.key()).or_default();
            let key = snapshot.reference.snapshot_id.as_str().to_string();
            // First writer wins: a snapshot is immutable, so a later assembly of
            // the same reference reads the original rather than replacing it.
            let stored = run.entry(key).or_insert_with(|| snapshot.clone());
            Ok(stored.clone())
        })
    }

    fn load<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        reference: &'a AgentContextSnapshotRef,
    ) -> MemoryFuture<'a, Option<MemoryContextSnapshot>> {
        Box::pin(async move {
            let snapshots = self
                .snapshots
                .lock()
                .expect("the snapshot store should not be poisoned");
            Ok(snapshots
                .get(&scope.key())
                .and_then(|run| run.get(reference.snapshot_id.as_str()))
                .cloned())
        })
    }

    fn purge_run<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        policy: &'a SessionRetentionPolicy,
        terminal_at: AgentTimestampMillis,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, SessionPurgeOutcome> {
        Box::pin(async move {
            if policy.legal_hold() {
                return Ok(SessionPurgeOutcome::Held);
            }
            if now < policy.purge_due_at(terminal_at) {
                return Ok(SessionPurgeOutcome::NotYetDue);
            }
            let mut snapshots = self
                .snapshots
                .lock()
                .expect("the snapshot store should not be poisoned");
            let removed = snapshots
                .remove(&scope.key())
                .map_or(0, |run| run.len() as u64);
            Ok(SessionPurgeOutcome::Purged { entries: removed })
        })
    }
}

// ===========================================================================
// Snapshot assembly and the windowing policy ([specification 10.3, 13.5]).
// ===========================================================================

/// How a snapshot's bounded session window is shaped
/// ([specification 10.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// This is the Rakka-owned write path a Rig memory policy shapes *behind*: a
/// deployment may choose how large the recent window is and whether rolling
/// summaries are included, but the authoritative session store and its stable
/// [`MemoryOperationId`] values are never bypassed, and an automatic memory
/// callback can never write a session entry outside the durable, deduplicated
/// append boundary ([specification 10.3](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionWindowPolicy {
    max_entries: usize,
    include_summaries: bool,
}

impl SessionWindowPolicy {
    /// The default window: the most recent [`AGENT_SESSION_WINDOW_MAX_ENTRIES`]
    /// entries, summaries included.
    #[must_use]
    pub const fn recent_window() -> Self {
        Self {
            max_entries: AGENT_SESSION_WINDOW_MAX_ENTRIES,
            include_summaries: true,
        }
    }

    /// Sets the maximum number of recent entries the window carries, clamped to
    /// [`AGENT_SESSION_WINDOW_MAX_ENTRIES`].
    #[must_use]
    pub const fn with_max_entries(mut self, max_entries: usize) -> Self {
        self.max_entries = if max_entries > AGENT_SESSION_WINDOW_MAX_ENTRIES {
            AGENT_SESSION_WINDOW_MAX_ENTRIES
        } else if max_entries == 0 {
            1
        } else {
            max_entries
        };
        self
    }

    /// The maximum number of entries the window carries.
    #[must_use]
    pub const fn max_entries(self) -> usize {
        self.max_entries
    }

    /// Whether rolling summaries are included in the window.
    #[must_use]
    pub const fn include_summaries(self) -> bool {
        self.include_summaries
    }
}

impl Default for SessionWindowPolicy {
    fn default() -> Self {
        Self::recent_window()
    }
}

/// Assembles the session half of the immutable context snapshot one model turn
/// is computed from.
///
/// It reads a bounded recent window from the session store, shapes it with the
/// window policy, and builds a [`MemoryContextSnapshot`] under the given
/// reference. It performs no write: persistence is the caller's, through a
/// [`ContextSnapshotStore`] whose idempotent persist is what makes a retry reuse
/// the first assembly. It is the session building block of
/// [`crate::retrieval::assemble_context`], which fills the private selections
/// when a retriever is wired; called directly, the private and communal
/// selections stay empty.
pub async fn assemble_session_context(
    session: &dyn SessionMemoryStore,
    scope: &AgentRunScope,
    reference: &AgentContextSnapshotRef,
    turn: u64,
    policy: &SessionWindowPolicy,
    policy_revision: AgentRevisionNumber,
    now: AgentTimestampMillis,
) -> Result<MemoryContextSnapshot, MemoryError> {
    // Read the run's session to its end in bounded pages, then keep the most
    // recent window. The store orders oldest-first, so the tail is the recent
    // window; reading to the end is what makes that tail the *true* most-recent
    // set rather than the tail of an arbitrary prefix. The read is bounded
    // because a run's session grows only with its loop iterations, which its
    // budget ceilings bound ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    let mut all: Vec<SessionMemoryEntry> = Vec::new();
    let mut cursor = SessionMemoryCursor::start().with_limit(AGENT_SESSION_WINDOW_MAX_ENTRIES);
    loop {
        let page = session.read(scope, cursor).await?;
        all.extend(page.entries);
        match page.next {
            Some(next) => cursor = next,
            None => break,
        }
    }

    if !policy.include_summaries() {
        all.retain(|entry| entry.role != MemoryEntryRole::Summary);
    }

    // The window is the most recent `max_entries`, kept in ascending order.
    let window_start = all.len().saturating_sub(policy.max_entries());
    let window = &all[window_start..];

    let mut session_selection = Vec::with_capacity(window.len());
    let mut session_bytes = 0usize;
    for entry in window {
        session_bytes = session_bytes.saturating_add(entry.content.size_bytes());
        session_selection.push(SnapshotSessionEntry {
            entry_id: entry.entry_id.clone(),
            sequence: entry.sequence,
            role: entry.role,
            content: entry.content.clone(),
            content_digest: entry.content_digest.clone(),
            classification: entry.classification,
        });
    }

    let retrievals = vec![SnapshotRetrieval {
        query: format!("session-window:turn-{turn}"),
        retriever: "session-window".to_string(),
        retriever_version: AgentRevisionNumber::INITIAL,
        index_watermark: None,
    }];

    let budget = SnapshotBudget {
        session_entries: session_selection.len(),
        session_bytes,
        private_memories: 0,
        private_memory_bytes: 0,
        communal_claims: 0,
    };

    let mut snapshot = MemoryContextSnapshot {
        schema_version: CURRENT_AGENT_MEMORY_CONTEXT_SNAPSHOT_SCHEMA_VERSION,
        reference: reference.clone(),
        scope: scope.clone(),
        turn,
        trust: MemoryTrust::Untrusted,
        session: session_selection,
        retrievals,
        private_memory: Vec::new(),
        communal_claims: Vec::new(),
        policy_revision,
        ingress_revision: None,
        budget,
        content_digest: AgentContentDigest::of_bytes(b""),
        assembled_at: now,
    };
    snapshot.content_digest = snapshot.compute_digest();
    Ok(snapshot)
}

// ===========================================================================
// Agent-private long-term memory ([specification 13.3]).
// ===========================================================================

/// Largest inline private-memory content, in serialized bytes.
///
/// Its own bound, separate from [`AGENT_SESSION_MEMORY_ENTRY_MAX_BYTES`], so
/// the two can evolve independently; larger content belongs behind an
/// immutable artifact reference
/// ([specification 13.1](../../../docs/plans/rakka-agent/spec.md)).
pub const AGENT_PRIVATE_MEMORY_INLINE_MAX_BYTES: usize = 8 * 1024;

/// Largest page one private-memory list returns.
pub const AGENT_PRIVATE_MEMORY_PAGE_MAX_ENTRIES: usize = 64;

/// Longest embedding-model name recorded in embedding metadata.
pub const AGENT_MEMORY_EMBEDDING_MODEL_MAX_LENGTH: usize = 128;

/// Most private memories one context snapshot may select
/// ([specification 13.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// The selection is part of the immutable snapshot a model turn is computed
/// from, so it is bounded like the session window: retrieval ranks, the
/// snapshot keeps at most this many.
pub const AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_ENTRIES: usize = 16;

/// Largest total serialized private-memory content one snapshot may embed, in
/// bytes.
///
/// Each inline private memory is already bounded by
/// [`AGENT_PRIVATE_MEMORY_INLINE_MAX_BYTES`]; this caps the whole selection so
/// a snapshot's private half stays comfortably bounded even at the entry
/// limit.
pub const AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_BYTES: usize = 64 * 1024;

validated_id! {
    /// Stable identity of one agent-private long-term memory
    /// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
    pub AgentPrivateMemoryId, "agent_private_memory_id"
}

/// The type of an agent-private long-term memory
/// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentPrivateMemoryKind {
    /// A durable fact or concept.
    Semantic,
    /// A remembered episode or event.
    Episodic,
    /// A learned preference.
    Preference,
    /// An application-defined type.
    Application,
}

impl AgentPrivateMemoryKind {
    /// Stable kebab-case label for errors, logs, and derived identities.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Semantic => "semantic",
            Self::Episodic => "episodic",
            Self::Preference => "preference",
            Self::Application => "application",
        }
    }
}

impl Display for AgentPrivateMemoryKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Provenance of one private memory
/// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md): source
/// run/effect/entry references).
///
/// Provenance only, never authority: recording the run that originated a
/// memory does not widen access to that run or to another agent. Every field
/// is optional — a memory written administratively has no originating run —
/// and the reference lengths are bounded by [`AgentPrivateMemory::validate`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentPrivateMemorySource {
    /// The run that originated the memory.
    #[serde(default)]
    pub run: Option<crate::identity::AgentRunId>,
    /// The durable effect that promoted it.
    #[serde(default)]
    pub effect: Option<rakka_agent_workflow::AgentEffectId>,
    /// The session entry it was promoted from.
    #[serde(default)]
    pub entry: Option<MemoryEntryId>,
}

/// Content-free embedding metadata
/// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// Records which model, dimension count, and version produced a memory's
/// derived vectors, so a rebuild is an explicit versioned change. The vectors
/// themselves are rebuildable derived projections owned by the retrieval
/// adapter (slice 2.2), never the only copy of the content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryEmbeddingRef {
    /// The embedding model that produced the vectors.
    pub model: String,
    /// The vector dimension count.
    pub dimensions: u32,
    /// The embedding pipeline version.
    pub version: AgentRevisionNumber,
}

/// Retention state of one private memory
/// ([specification 13.1](../../../docs/plans/rakka-agent/spec.md),
/// [13.3](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum MemoryRetention {
    /// Retained until explicitly tombstoned or deleted.
    Persistent,
    /// Invisible to reads at and after the instant, and eligible for
    /// [`AgentPrivateMemoryStore::purge_expired`].
    ExpiresAt {
        /// The expiry instant.
        at: AgentTimestampMillis,
    },
}

impl MemoryRetention {
    /// The expiry instant, when one is set.
    #[must_use]
    pub const fn expires_at(self) -> Option<AgentTimestampMillis> {
        match self {
            Self::Persistent => None,
            Self::ExpiresAt { at } => Some(at),
        }
    }

    /// Whether the memory is expired at `now`.
    ///
    /// Expiry is a read-visibility and purge rule, enforced from the instant
    /// itself: an expired-but-unpurged memory is already invisible, whether or
    /// not a sweep has run.
    #[must_use]
    pub fn is_expired(self, now: AgentTimestampMillis) -> bool {
        matches!(self, Self::ExpiresAt { at } if at <= now)
    }
}

/// Why a memory was tombstoned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum MemoryTombstoneReason {
    /// A newer memory supersedes it.
    Superseded,
    /// Its owner or a policy withdrew it.
    Retracted,
    /// A retention or classification policy removed its content.
    Policy,
}

impl MemoryTombstoneReason {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Superseded => "superseded",
            Self::Retracted => "retracted",
            Self::Policy => "policy",
        }
    }
}

impl Display for MemoryTombstoneReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Tombstone state: the auditable stub of a withdrawn memory
/// ([specification 13.1](../../../docs/plans/rakka-agent/spec.md)).
///
/// The record's digest and provenance remain — the withdrawal itself must
/// stay visible to the owner — but the content bytes do not, the same rule
/// [`MemoryClassification::Redacted`] applies to a session entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryTombstone {
    /// The idempotent operation that tombstoned the memory.
    pub operation_id: MemoryOperationId,
    /// Why it was tombstoned.
    pub reason: MemoryTombstoneReason,
    /// When it was tombstoned.
    pub tombstoned_at: AgentTimestampMillis,
}

/// One promoted memory, as the bounded promotion receipt names it: identity
/// and revision only, never content
/// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentPromotedMemoryRef {
    /// The private memory the promotion wrote or converged on.
    pub memory_id: AgentPrivateMemoryId,
    /// The revision the store holds after the promotion.
    pub revision: AgentRevisionNumber,
    /// The session entry the memory was promoted from.
    pub source_entry: MemoryEntryId,
}

/// One agent-private long-term memory, scoped `(TenantId, AgentId)`
/// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// The originating run is recorded as provenance but never broadens access to
/// another agent, and embeddings are rebuildable derived projections, never
/// the only copy of the content. The fields are public so a store adapter can
/// rebuild a record on load; [`Self::validate`] runs inside [`Self::new`] and
/// on deserialization, so an out-of-bounds record can neither cross the wire
/// nor load from a durable row.
///
/// The `revision` is the record's compare-and-set fence: a store stamps it —
/// [`AgentRevisionNumber::INITIAL`] on create, the next revision on update —
/// and an update that names a stale expected revision is refused rather than
/// overwriting, which is what keeps concurrent runs of one agent from losing
/// each other's writes (scenario 15).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AgentPrivateMemory {
    schema_version: StateSchemaVersion,
    /// Stable identity of the memory.
    pub memory_id: AgentPrivateMemoryId,
    /// The idempotent operation that created or last mutated it.
    pub operation_id: MemoryOperationId,
    /// The compare-and-set revision the store stamps on every write.
    pub revision: AgentRevisionNumber,
    /// The memory type.
    pub kind: AgentPrivateMemoryKind,
    /// The bounded content or immutable artifact reference; the null inline
    /// marker once tombstoned.
    pub content: AgentTaskContent,
    /// A digest of the original content. It survives tombstoning, pinning
    /// exactly what was withdrawn.
    pub content_digest: AgentContentDigest,
    /// Where the memory came from, as provenance only.
    #[serde(default)]
    pub source: AgentPrivateMemorySource,
    /// A confidence score in basis points (0-10000).
    pub confidence_bps: u16,
    /// The classification of the content.
    pub classification: MemoryClassification,
    /// Embedding metadata, when the memory has been embedded.
    #[serde(default)]
    pub embedding: Option<MemoryEmbeddingRef>,
    /// The retention state.
    pub retention: MemoryRetention,
    /// The tombstone state, once the memory has been withdrawn.
    #[serde(default)]
    pub tombstone: Option<MemoryTombstone>,
    /// The policy in force when the memory was written.
    #[serde(default)]
    pub policy: Option<crate::definition::AgentPolicyRef>,
    /// An audit-trail reference.
    #[serde(default)]
    pub audit: Option<rakka_agent_workflow::AgentAuditEventId>,
    /// When the memory was created.
    pub created_at: AgentTimestampMillis,
    /// When the memory was last updated.
    pub updated_at: AgentTimestampMillis,
}

impl AgentPrivateMemory {
    /// Builds a private memory with the default provenance, persistent
    /// retention, and no embedding, policy, or audit reference.
    pub fn new(
        memory_id: AgentPrivateMemoryId,
        operation_id: MemoryOperationId,
        kind: AgentPrivateMemoryKind,
        content: AgentTaskContent,
        confidence_bps: u16,
        classification: MemoryClassification,
        created_at: AgentTimestampMillis,
    ) -> Result<Self, MemoryError> {
        let content_digest = content.digest();
        let memory = Self {
            schema_version: CURRENT_AGENT_PRIVATE_MEMORY_SCHEMA_VERSION,
            memory_id,
            operation_id,
            revision: AgentRevisionNumber::INITIAL,
            kind,
            content,
            content_digest,
            source: AgentPrivateMemorySource::default(),
            confidence_bps,
            classification,
            embedding: None,
            retention: MemoryRetention::Persistent,
            tombstone: None,
            policy: None,
            audit: None,
            created_at,
            updated_at: created_at,
        };
        memory.validate()?;
        Ok(memory)
    }

    /// The content an already-tombstoned record carries: the null inline
    /// marker, so a tombstone provably holds no bytes.
    #[must_use]
    pub fn tombstone_content() -> AgentTaskContent {
        AgentTaskContent::Inline(serde_json::Value::Null)
    }

    /// Sets the provenance.
    #[must_use]
    pub fn with_source(mut self, source: AgentPrivateMemorySource) -> Self {
        self.source = source;
        self
    }

    /// Sets the retention state.
    #[must_use]
    pub fn with_retention(mut self, retention: MemoryRetention) -> Self {
        self.retention = retention;
        self
    }

    /// Sets the policy reference.
    #[must_use]
    pub fn with_policy(mut self, policy: crate::definition::AgentPolicyRef) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Sets the audit reference, re-validating its bound.
    pub fn with_audit(
        mut self,
        audit: rakka_agent_workflow::AgentAuditEventId,
    ) -> Result<Self, MemoryError> {
        self.audit = Some(audit);
        self.validate()?;
        Ok(self)
    }

    /// Sets the embedding metadata, re-validating its bounds.
    pub fn with_embedding(mut self, embedding: MemoryEmbeddingRef) -> Result<Self, MemoryError> {
        self.embedding = Some(embedding);
        self.validate()?;
        Ok(self)
    }

    /// Whether the memory has been withdrawn.
    #[must_use]
    pub const fn is_tombstoned(&self) -> bool {
        self.tombstone.is_some()
    }

    /// Whether the memory is expired at `now`.
    #[must_use]
    pub fn is_expired(&self, now: AgentTimestampMillis) -> bool {
        self.retention.is_expired(now)
    }

    /// Rejects a record that exceeds any of its bounds, and a tombstoned
    /// record that still carries content.
    pub fn validate(&self) -> Result<(), MemoryError> {
        if let Some(value) = self.content.inline_value() {
            let bytes = serde_json::to_vec(value)
                .map_err(|error| MemoryError::Encoding {
                    message: error.to_string(),
                })?
                .len();
            if bytes > AGENT_PRIVATE_MEMORY_INLINE_MAX_BYTES {
                return Err(MemoryError::EntryTooLarge {
                    bytes,
                    maximum: AGENT_PRIVATE_MEMORY_INLINE_MAX_BYTES,
                });
            }
        }
        if self.confidence_bps > 10_000 {
            return Err(MemoryError::ConfidenceOutOfRange {
                confidence_bps: self.confidence_bps,
            });
        }
        if let Some(effect) = &self.source.effect {
            check_reference_bound("source.effect", effect.as_str())?;
        }
        if let Some(audit) = &self.audit {
            check_reference_bound("audit", audit.as_str())?;
        }
        if let Some(embedding) = &self.embedding {
            if embedding.model.is_empty()
                || embedding.model.len() > AGENT_MEMORY_EMBEDDING_MODEL_MAX_LENGTH
                || embedding.dimensions == 0
            {
                return Err(MemoryError::InvalidEmbeddingRef {
                    message: format!(
                        "the embedding model must be non-empty and at most \
                         {AGENT_MEMORY_EMBEDDING_MODEL_MAX_LENGTH} bytes, and the dimension \
                         count at least one; got model of {} bytes and {} dimensions",
                        embedding.model.len(),
                        embedding.dimensions
                    ),
                });
            }
        }
        if self.tombstone.is_some() && self.content != Self::tombstone_content() {
            return Err(MemoryError::TombstoneCarriesContent {
                memory_id: self.memory_id.clone(),
            });
        }
        Ok(())
    }
}

/// Rejects an unbounded external reference on a private-memory record.
fn check_reference_bound(field: &'static str, value: &str) -> Result<(), MemoryError> {
    if value.len() > crate::identity::AGENT_IDENTITY_MAX_LENGTH {
        return Err(MemoryError::ReferenceTooLong {
            field,
            length: value.len(),
            maximum: crate::identity::AGENT_IDENTITY_MAX_LENGTH,
        });
    }
    Ok(())
}

impl VersionedAgentRecord for AgentPrivateMemory {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::PrivateMemory;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// The wire and durable shape of [`AgentPrivateMemory`], validated on load.
#[derive(Deserialize)]
struct AgentPrivateMemoryRecord {
    schema_version: StateSchemaVersion,
    memory_id: AgentPrivateMemoryId,
    operation_id: MemoryOperationId,
    revision: AgentRevisionNumber,
    kind: AgentPrivateMemoryKind,
    content: AgentTaskContent,
    content_digest: AgentContentDigest,
    #[serde(default)]
    source: AgentPrivateMemorySource,
    confidence_bps: u16,
    classification: MemoryClassification,
    #[serde(default)]
    embedding: Option<MemoryEmbeddingRef>,
    retention: MemoryRetention,
    #[serde(default)]
    tombstone: Option<MemoryTombstone>,
    #[serde(default)]
    policy: Option<crate::definition::AgentPolicyRef>,
    #[serde(default)]
    audit: Option<rakka_agent_workflow::AgentAuditEventId>,
    created_at: AgentTimestampMillis,
    updated_at: AgentTimestampMillis,
}

impl<'de> Deserialize<'de> for AgentPrivateMemory {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let record = AgentPrivateMemoryRecord::deserialize(deserializer)?;
        let memory = Self {
            schema_version: record.schema_version,
            memory_id: record.memory_id,
            operation_id: record.operation_id,
            revision: record.revision,
            kind: record.kind,
            content: record.content,
            content_digest: record.content_digest,
            source: record.source,
            confidence_bps: record.confidence_bps,
            classification: record.classification,
            embedding: record.embedding,
            retention: record.retention,
            tombstone: record.tombstone,
            policy: record.policy,
            audit: record.audit,
            created_at: record.created_at,
            updated_at: record.updated_at,
        };
        memory.validate().map_err(serde::de::Error::custom)?;
        Ok(memory)
    }
}

impl AgentPrivateMemoryId {
    /// Derives the stable identity of the private memory one session entry
    /// promotes into ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The derivation is pure over the agent scope, the source entry, and the
    /// kind: any node, any attempt, any generation — and any *second*
    /// promotion of the same entry — resolves to the same memory, so replay
    /// converges instead of duplicating. Two runs of one agent never collide,
    /// because an entry id embeds its run scope's digest; two agents never
    /// collide, because the agent scope's injective key is part of the input.
    pub fn derive_promoted(
        scope: &AgentScope,
        source_entry: &MemoryEntryId,
        kind: AgentPrivateMemoryKind,
    ) -> AgentIdentityResult<Self> {
        let input = format!(
            "private|{}|{}|{}",
            scope.key(),
            kind.as_label(),
            source_entry
        );
        let digest = AgentContentDigest::of_bytes(input.as_bytes());
        Self::new(format!("mem-private-{}", digest.value))
    }
}

/// What an upsert expects to find, making a blind overwrite unrepresentable
/// (open decision 1, [specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivateMemoryExpectation {
    /// The memory must not exist: a create.
    Absent,
    /// The memory must exist at exactly this revision: a compare-and-set
    /// update. Consolidation targets an existing memory this way.
    Revision(AgentRevisionNumber),
}

/// A bounded read cursor over one agent's private memories.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateMemoryCursor {
    after: Option<AgentPrivateMemoryId>,
    limit: usize,
    include_tombstoned: bool,
}

impl PrivateMemoryCursor {
    /// The default page size a list returns when none is requested.
    pub const DEFAULT_LIMIT: usize = 32;

    /// A cursor over the memories with the lowest identities, from the start.
    #[must_use]
    pub const fn start() -> Self {
        Self {
            after: None,
            limit: Self::DEFAULT_LIMIT,
            include_tombstoned: false,
        }
    }

    /// A cursor over the memories after `memory_id`.
    #[must_use]
    pub const fn after(memory_id: AgentPrivateMemoryId) -> Self {
        Self {
            after: Some(memory_id),
            limit: Self::DEFAULT_LIMIT,
            include_tombstoned: false,
        }
    }

    /// Sets the page size, clamped to [`AGENT_PRIVATE_MEMORY_PAGE_MAX_ENTRIES`].
    #[must_use]
    pub const fn with_limit(mut self, limit: usize) -> Self {
        self.limit = if limit > AGENT_PRIVATE_MEMORY_PAGE_MAX_ENTRIES {
            AGENT_PRIVATE_MEMORY_PAGE_MAX_ENTRIES
        } else if limit == 0 {
            1
        } else {
            limit
        };
        self
    }

    /// Includes tombstoned stubs in the page, for audit listings.
    #[must_use]
    pub const fn include_tombstoned(mut self) -> Self {
        self.include_tombstoned = true;
        self
    }

    /// The identity this cursor reads after, when it is not at the start.
    #[must_use]
    pub const fn position(&self) -> Option<&AgentPrivateMemoryId> {
        self.after.as_ref()
    }

    /// The page size.
    #[must_use]
    pub const fn limit(&self) -> usize {
        self.limit
    }

    /// Whether tombstoned stubs are included.
    #[must_use]
    pub const fn tombstoned_included(&self) -> bool {
        self.include_tombstoned
    }
}

impl Default for PrivateMemoryCursor {
    fn default() -> Self {
        Self::start()
    }
}

/// One bounded page of private memories, in ascending identity order.
#[derive(Debug, Clone, PartialEq)]
pub struct PrivateMemoryPage {
    /// The memories in this page.
    pub memories: Vec<AgentPrivateMemory>,
    /// The cursor for the next page, when more remain.
    pub next: Option<PrivateMemoryCursor>,
}

/// A request to tombstone one private memory: withdraw its content while
/// keeping the auditable stub.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateMemoryTombstoneRequest {
    /// The memory to tombstone.
    pub memory_id: AgentPrivateMemoryId,
    /// The idempotent operation id of this tombstone.
    pub operation_id: MemoryOperationId,
    /// Why it is being tombstoned.
    pub reason: MemoryTombstoneReason,
    /// When it is being tombstoned.
    pub tombstoned_at: AgentTimestampMillis,
}

/// A request to delete one private memory entirely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateMemoryDeleteRequest {
    /// The memory to delete.
    pub memory_id: AgentPrivateMemoryId,
    /// The idempotent operation id of this deletion.
    pub operation_id: MemoryOperationId,
}

/// The durable agent-private long-term memory store, scoped `(TenantId, AgentId)`
/// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// Every method addresses the explicit scope, and the scope is the isolation
/// boundary: a read under the wrong scope answers exactly as if the memory
/// never existed — `None`, or an empty page — so an unauthorized caller
/// learns nothing, not even existence (scenario 18,
/// [specification 13.1](../../../docs/plans/rakka-agent/spec.md)).
///
/// Writes follow open decision 1: a create is idempotent on its operation id
/// — a replay returns the original logical result recorded in the store's
/// operation ledger, even when later operations have since moved the record —
/// and an update is a compare-and-set on an expected revision, so a stale
/// writer is refused rather than overwriting (scenario 15). A deletion erases
/// the ledger's payloads for the memory, so no replayed operation can
/// resurrect deleted content: such a replay fails closed as
/// [`MemoryError::OperationErased`].
///
/// The trait is object-safe so callers hold `Arc<dyn AgentPrivateMemoryStore>`;
/// the in-memory implementation lives here and the PostgreSQL one in
/// `rakka-agent-postgres`. Vector retrieval is the slice 2.2 adapter; this
/// store owns the authoritative records.
pub trait AgentPrivateMemoryStore: Send + Sync + 'static {
    /// Stable backend name, used in telemetry.
    fn backend_name(&self) -> &'static str;

    /// Creates or compare-and-set-updates one private memory, idempotently on
    /// its operation id.
    ///
    /// The store stamps the stored revision itself —
    /// [`AgentRevisionNumber::INITIAL`] on a create, the expectation's next
    /// revision on an update — and returns the record it now holds. A create
    /// over an existing memory fails with [`MemoryError::AlreadyExists`]; an
    /// update of an absent or out-of-scope memory with
    /// [`MemoryError::NotFound`] (indistinguishable, deliberately); an update
    /// of a tombstoned memory with [`MemoryError::Tombstoned`]; a stale
    /// expected revision with [`MemoryError::RevisionConflict`].
    fn upsert<'a>(
        &'a self,
        scope: &'a AgentScope,
        memory: &'a AgentPrivateMemory,
        expected: PrivateMemoryExpectation,
    ) -> MemoryFuture<'a, AgentPrivateMemory>;

    /// Loads one private memory by identity.
    ///
    /// Out-of-scope and expired memories answer `None`, exactly like absent
    /// ones; a tombstoned memory answers its content-free stub, because the
    /// withdrawal itself must stay visible to the owner.
    fn get<'a>(
        &'a self,
        scope: &'a AgentScope,
        memory_id: &'a AgentPrivateMemoryId,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, Option<AgentPrivateMemory>>;

    /// Lists one bounded page of the agent's memories, ascending by identity.
    ///
    /// Expired memories are excluded; tombstoned stubs are excluded unless the
    /// cursor opts in via [`PrivateMemoryCursor::include_tombstoned`].
    fn list<'a>(
        &'a self,
        scope: &'a AgentScope,
        cursor: PrivateMemoryCursor,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, PrivateMemoryPage>;

    /// Withdraws one memory's content, keeping the auditable stub, idempotently
    /// on the request's operation id.
    ///
    /// The stub keeps the identity, digest, and provenance, carries the
    /// tombstone state, and takes the next revision; the store erases its
    /// ledger's earlier content payloads for the memory, so no replay can
    /// recover the withdrawn bytes. Tombstoning an already-tombstoned memory
    /// under a different operation fails with [`MemoryError::Tombstoned`].
    fn tombstone<'a>(
        &'a self,
        scope: &'a AgentScope,
        request: &'a PrivateMemoryTombstoneRequest,
    ) -> MemoryFuture<'a, AgentPrivateMemory>;

    /// Deletes one memory entirely, idempotently on the request's operation id.
    ///
    /// The row and every ledger content payload for the memory are removed;
    /// the deletion records its own content-free success marker, so a replay
    /// answers success while a replayed *earlier* write fails closed as
    /// [`MemoryError::OperationErased`]. Deleting an absent or out-of-scope
    /// memory fails with [`MemoryError::NotFound`], indistinguishably.
    fn delete<'a>(
        &'a self,
        scope: &'a AgentScope,
        request: &'a PrivateMemoryDeleteRequest,
    ) -> MemoryFuture<'a, ()>;

    /// Hard-deletes up to `limit` expired memories, returning how many were
    /// removed.
    ///
    /// Bounded and idempotent: a deployment invokes it repeatedly until it
    /// returns zero, on its own schedule — there is no resident sweeper, and
    /// expiry already hides the rows from reads whether or not a sweep has
    /// run.
    fn purge_expired<'a>(
        &'a self,
        scope: &'a AgentScope,
        now: AgentTimestampMillis,
        limit: usize,
    ) -> MemoryFuture<'a, u64>;
}

/// One entry in the in-memory store's operation ledger.
///
/// The ledger is what makes a replay answer with the *original* logical
/// result, and what makes deletion final: erasing a payload turns the replay
/// of the operation that wrote it into a fail-closed refusal instead of a
/// resurrection.
#[derive(Debug, Clone)]
enum PrivateMemoryLedgerEntry {
    /// The operation applied; this is the record it returned.
    Applied(Box<AgentPrivateMemory>),
    /// The operation was a deletion; a replay answers success with no payload.
    Deleted,
    /// The operation's payload was erased by a later deletion or purge.
    Erased,
}

/// An in-memory private-memory store, for tests and single-process
/// deployments.
///
/// It implements the exact write table the trait documents — operation-ledger
/// replays, compare-and-set updates, tombstone and delete erasure — under one
/// mutex, so the contract tests that run against it and against the
/// PostgreSQL adapter prove the same semantics.
#[derive(Debug, Clone, Default)]
pub struct InMemoryAgentPrivateMemoryStore {
    memories: Arc<Mutex<BTreeMap<String, BTreeMap<String, AgentPrivateMemory>>>>,
    operations: Arc<Mutex<BTreeMap<String, BTreeMap<String, PrivateMemoryLedgerEntry>>>>,
}

impl InMemoryAgentPrivateMemoryStore {
    /// Creates an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// How many memories one agent holds, tombstoned stubs included.
    #[must_use]
    pub fn len(&self, scope: &AgentScope) -> usize {
        self.memories
            .lock()
            .expect("the private memory store should not be poisoned")
            .get(&scope.key())
            .map_or(0, BTreeMap::len)
    }

    /// Whether one agent holds no memories.
    #[must_use]
    pub fn is_empty(&self, scope: &AgentScope) -> bool {
        self.len(scope) == 0
    }

    /// Rewrites every applied payload for `memory_id` to erased, so no
    /// replayed write can resurrect deleted or withdrawn content.
    fn erase_payloads(
        operations: &mut BTreeMap<String, PrivateMemoryLedgerEntry>,
        memory_id: &AgentPrivateMemoryId,
    ) {
        for entry in operations.values_mut() {
            if matches!(entry, PrivateMemoryLedgerEntry::Applied(memory) if memory.memory_id == *memory_id)
            {
                *entry = PrivateMemoryLedgerEntry::Erased;
            }
        }
    }
}

impl AgentPrivateMemoryStore for InMemoryAgentPrivateMemoryStore {
    fn backend_name(&self) -> &'static str {
        "in-memory"
    }

    fn upsert<'a>(
        &'a self,
        scope: &'a AgentScope,
        memory: &'a AgentPrivateMemory,
        expected: PrivateMemoryExpectation,
    ) -> MemoryFuture<'a, AgentPrivateMemory> {
        Box::pin(async move {
            let key = scope.key();
            let mut operations = self
                .operations
                .lock()
                .expect("the private memory store should not be poisoned");
            let mut memories = self
                .memories
                .lock()
                .expect("the private memory store should not be poisoned");

            let op_key = memory.operation_id.as_str().to_string();
            if let Some(entry) = operations.get(&key).and_then(|ops| ops.get(&op_key)) {
                return match entry {
                    PrivateMemoryLedgerEntry::Applied(original) => Ok(original.as_ref().clone()),
                    PrivateMemoryLedgerEntry::Deleted | PrivateMemoryLedgerEntry::Erased => {
                        Err(MemoryError::OperationErased {
                            operation_id: memory.operation_id.clone(),
                        })
                    }
                };
            }

            let agent = memories.entry(key.clone()).or_default();
            let row_key = memory.memory_id.as_str().to_string();
            let stored = match expected {
                PrivateMemoryExpectation::Absent => {
                    if agent.contains_key(&row_key) {
                        return Err(MemoryError::AlreadyExists {
                            memory_id: memory.memory_id.clone(),
                        });
                    }
                    let mut stamped = memory.clone();
                    stamped.revision = AgentRevisionNumber::INITIAL;
                    agent.insert(row_key, stamped.clone());
                    stamped
                }
                PrivateMemoryExpectation::Revision(expected_revision) => {
                    let Some(current) = agent.get(&row_key) else {
                        return Err(MemoryError::NotFound {
                            memory_id: memory.memory_id.clone(),
                        });
                    };
                    if current.is_tombstoned() {
                        return Err(MemoryError::Tombstoned {
                            memory_id: memory.memory_id.clone(),
                        });
                    }
                    if current.revision != expected_revision {
                        return Err(MemoryError::RevisionConflict {
                            memory_id: memory.memory_id.clone(),
                            expected: expected_revision,
                            actual: current.revision,
                        });
                    }
                    let mut stamped = memory.clone();
                    stamped.revision = expected_revision.next();
                    agent.insert(row_key, stamped.clone());
                    stamped
                }
            };
            operations.entry(key).or_default().insert(
                op_key,
                PrivateMemoryLedgerEntry::Applied(Box::new(stored.clone())),
            );
            Ok(stored)
        })
    }

    fn get<'a>(
        &'a self,
        scope: &'a AgentScope,
        memory_id: &'a AgentPrivateMemoryId,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, Option<AgentPrivateMemory>> {
        Box::pin(async move {
            let memories = self
                .memories
                .lock()
                .expect("the private memory store should not be poisoned");
            Ok(memories
                .get(&scope.key())
                .and_then(|agent| agent.get(memory_id.as_str()))
                .filter(|memory| !memory.is_expired(now))
                .cloned())
        })
    }

    fn list<'a>(
        &'a self,
        scope: &'a AgentScope,
        cursor: PrivateMemoryCursor,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, PrivateMemoryPage> {
        Box::pin(async move {
            let memories = self
                .memories
                .lock()
                .expect("the private memory store should not be poisoned");
            let Some(agent) = memories.get(&scope.key()) else {
                return Ok(PrivateMemoryPage {
                    memories: Vec::new(),
                    next: None,
                });
            };

            use std::ops::Bound;
            let lower = cursor.position().map_or(Bound::Unbounded, |after| {
                Bound::Excluded(after.as_str().to_string())
            });
            let mut page: Vec<AgentPrivateMemory> = agent
                .range((lower, Bound::Unbounded))
                .map(|(_, memory)| memory)
                .filter(|memory| !memory.is_expired(now))
                .filter(|memory| cursor.tombstoned_included() || !memory.is_tombstoned())
                .take(cursor.limit() + 1)
                .cloned()
                .collect();

            let next = (page.len() > cursor.limit())
                .then(|| {
                    page.pop();
                    page.last().map(|memory| {
                        let next = PrivateMemoryCursor::after(memory.memory_id.clone())
                            .with_limit(cursor.limit());
                        if cursor.tombstoned_included() {
                            next.include_tombstoned()
                        } else {
                            next
                        }
                    })
                })
                .flatten();

            Ok(PrivateMemoryPage {
                memories: page,
                next,
            })
        })
    }

    fn tombstone<'a>(
        &'a self,
        scope: &'a AgentScope,
        request: &'a PrivateMemoryTombstoneRequest,
    ) -> MemoryFuture<'a, AgentPrivateMemory> {
        Box::pin(async move {
            let key = scope.key();
            let mut operations = self
                .operations
                .lock()
                .expect("the private memory store should not be poisoned");
            let mut memories = self
                .memories
                .lock()
                .expect("the private memory store should not be poisoned");

            let op_key = request.operation_id.as_str().to_string();
            if let Some(entry) = operations.get(&key).and_then(|ops| ops.get(&op_key)) {
                return match entry {
                    PrivateMemoryLedgerEntry::Applied(original) => Ok(original.as_ref().clone()),
                    PrivateMemoryLedgerEntry::Deleted | PrivateMemoryLedgerEntry::Erased => {
                        Err(MemoryError::OperationErased {
                            operation_id: request.operation_id.clone(),
                        })
                    }
                };
            }

            let agent = memories.entry(key.clone()).or_default();
            let row_key = request.memory_id.as_str().to_string();
            let Some(current) = agent.get(&row_key) else {
                return Err(MemoryError::NotFound {
                    memory_id: request.memory_id.clone(),
                });
            };
            if current.is_tombstoned() {
                return Err(MemoryError::Tombstoned {
                    memory_id: request.memory_id.clone(),
                });
            }

            let mut stub = current.clone();
            stub.content = AgentPrivateMemory::tombstone_content();
            stub.tombstone = Some(MemoryTombstone {
                operation_id: request.operation_id.clone(),
                reason: request.reason,
                tombstoned_at: request.tombstoned_at,
            });
            stub.operation_id = request.operation_id.clone();
            stub.revision = current.revision.next();
            stub.updated_at = request.tombstoned_at;
            agent.insert(row_key, stub.clone());

            let scope_ops = operations.entry(key).or_default();
            InMemoryAgentPrivateMemoryStore::erase_payloads(scope_ops, &request.memory_id);
            scope_ops.insert(
                op_key,
                PrivateMemoryLedgerEntry::Applied(Box::new(stub.clone())),
            );
            Ok(stub)
        })
    }

    fn delete<'a>(
        &'a self,
        scope: &'a AgentScope,
        request: &'a PrivateMemoryDeleteRequest,
    ) -> MemoryFuture<'a, ()> {
        Box::pin(async move {
            let key = scope.key();
            let mut operations = self
                .operations
                .lock()
                .expect("the private memory store should not be poisoned");
            let mut memories = self
                .memories
                .lock()
                .expect("the private memory store should not be poisoned");

            let op_key = request.operation_id.as_str().to_string();
            if let Some(entry) = operations.get(&key).and_then(|ops| ops.get(&op_key)) {
                return match entry {
                    PrivateMemoryLedgerEntry::Deleted => Ok(()),
                    PrivateMemoryLedgerEntry::Applied(_) | PrivateMemoryLedgerEntry::Erased => {
                        Err(MemoryError::OperationConflict {
                            operation_id: request.operation_id.clone(),
                        })
                    }
                };
            }

            let agent = memories.entry(key.clone()).or_default();
            if agent.remove(request.memory_id.as_str()).is_none() {
                return Err(MemoryError::NotFound {
                    memory_id: request.memory_id.clone(),
                });
            }
            let scope_ops = operations.entry(key).or_default();
            InMemoryAgentPrivateMemoryStore::erase_payloads(scope_ops, &request.memory_id);
            scope_ops.insert(op_key, PrivateMemoryLedgerEntry::Deleted);
            Ok(())
        })
    }

    fn purge_expired<'a>(
        &'a self,
        scope: &'a AgentScope,
        now: AgentTimestampMillis,
        limit: usize,
    ) -> MemoryFuture<'a, u64> {
        Box::pin(async move {
            let key = scope.key();
            let mut operations = self
                .operations
                .lock()
                .expect("the private memory store should not be poisoned");
            let mut memories = self
                .memories
                .lock()
                .expect("the private memory store should not be poisoned");

            let Some(agent) = memories.get_mut(&key) else {
                return Ok(0);
            };
            let victims: Vec<AgentPrivateMemoryId> = agent
                .values()
                .filter(|memory| memory.is_expired(now))
                .take(limit)
                .map(|memory| memory.memory_id.clone())
                .collect();
            let scope_ops = operations.entry(key).or_default();
            for memory_id in &victims {
                agent.remove(memory_id.as_str());
                InMemoryAgentPrivateMemoryStore::erase_payloads(scope_ops, memory_id);
            }
            Ok(victims.len() as u64)
        })
    }
}

// ===========================================================================
// The run's memory collaborator.
// ===========================================================================

/// The memory backend a run entity uses to persist context snapshots and append
/// session memory.
///
/// It bundles the two short-term stores a run touches — the immutable
/// [`ContextSnapshotStore`] it persists a snapshot into before every model
/// effect, and the [`SessionMemoryStore`] it appends each recorded turn to —
/// plus the window policy that shapes an assembled snapshot. It is an *optional*
/// collaborator: a run without one keeps only the opaque context reference and
/// retains no session memory, which is the interim behavior every slice before
/// 1.11 had. Both stores are held behind `Arc`, so wiring memory into a run does
/// not change the run entity's own generic parameters.
#[derive(Clone)]
pub struct AgentRunMemory {
    session: Arc<dyn SessionMemoryStore>,
    snapshots: Arc<dyn ContextSnapshotStore>,
    private: Option<Arc<dyn AgentPrivateMemoryStore>>,
    retrieval: Option<crate::retrieval::AgentMemoryRetrieval>,
    window: SessionWindowPolicy,
}

impl AgentRunMemory {
    /// Bundles a session store and a snapshot store with the default window.
    #[must_use]
    pub fn new(
        session: Arc<dyn SessionMemoryStore>,
        snapshots: Arc<dyn ContextSnapshotStore>,
    ) -> Self {
        Self {
            session,
            snapshots,
            private: None,
            retrieval: None,
            window: SessionWindowPolicy::recent_window(),
        }
    }

    /// Uses an explicit window policy.
    #[must_use]
    pub fn with_window(mut self, window: SessionWindowPolicy) -> Self {
        self.window = window;
        self
    }

    /// Wires the agent-private long-term store
    /// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The run entity itself never touches it — transitions perform no I/O,
    /// and promotion is executed dispatcher-side — but the bundle is where a
    /// deployment names the agent's stores together, and the slice 2.2
    /// retrieval path assembles snapshots from it.
    #[must_use]
    pub fn with_private_store(mut self, private: Arc<dyn AgentPrivateMemoryStore>) -> Self {
        self.private = Some(private);
        self
    }

    /// Wires the private-memory retrieval bundle
    /// ([specification 13.5](../../../docs/plans/rakka-agent/spec.md),
    /// [16](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// With a bundle wired, [`crate::retrieval::assemble_context`] fills the
    /// snapshot's private selections through the bundle's retriever, evaluated
    /// against the bundle's memory-ingress guardrail chain. The chain must be
    /// the same one the deployment's dispatch authority carries: the
    /// authority's coverage check cannot see this bundle, so wiring two
    /// different chains would let a required memory-ingress stage satisfy
    /// coverage there while a different chain runs here.
    #[must_use]
    pub fn with_retrieval(mut self, retrieval: crate::retrieval::AgentMemoryRetrieval) -> Self {
        self.retrieval = Some(retrieval);
        self
    }

    /// The session-memory store.
    #[must_use]
    pub fn session(&self) -> &dyn SessionMemoryStore {
        self.session.as_ref()
    }

    /// The context-snapshot store.
    #[must_use]
    pub fn snapshots(&self) -> &dyn ContextSnapshotStore {
        self.snapshots.as_ref()
    }

    /// The window policy that shapes an assembled snapshot.
    #[must_use]
    pub const fn window(&self) -> &SessionWindowPolicy {
        &self.window
    }

    /// The agent-private long-term store, when one is wired.
    #[must_use]
    pub fn private(&self) -> Option<&Arc<dyn AgentPrivateMemoryStore>> {
        self.private.as_ref()
    }

    /// The private-memory retrieval bundle, when one is wired.
    #[must_use]
    pub fn retrieval(&self) -> Option<&crate::retrieval::AgentMemoryRetrieval> {
        self.retrieval.as_ref()
    }
}

impl fmt::Debug for AgentRunMemory {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentRunMemory")
            .field("session", &self.session.backend_name())
            .field("snapshots", &self.snapshots.backend_name())
            .field(
                "private",
                &self
                    .private
                    .as_ref()
                    .map_or("none", |store| store.backend_name()),
            )
            .field(
                "retrieval",
                &self
                    .retrieval
                    .as_ref()
                    .map_or("none", |retrieval| retrieval.retriever().backend_name()),
            )
            .field("window", &self.window)
            .finish()
    }
}

// ===========================================================================
// Errors.
// ===========================================================================

/// A memory store or record operation that failed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MemoryError {
    /// A persisted record carried an unsupported schema version.
    Schema(AgentSchemaError),
    /// An inline entry exceeded its bound.
    EntryTooLarge {
        /// Size of the rejected content, in bytes.
        bytes: usize,
        /// Maximum accepted size, in bytes.
        maximum: usize,
    },
    /// Two logically distinct writes claimed one sequence.
    SequenceConflict {
        /// The contested sequence.
        sequence: MemorySequence,
    },
    /// Two logically distinct writes claimed one operation id.
    OperationConflict {
        /// The contested operation id.
        operation_id: MemoryOperationId,
    },
    /// A run's session-memory outbox is full, so the transition that would
    /// record another entry fails closed rather than persist an unbounded record.
    OutboxOverflow {
        /// The outbox capacity that was reached.
        maximum: usize,
    },
    /// A value could not be encoded.
    Encoding {
        /// The encoding failure detail.
        message: String,
    },
    /// A backend operation failed.
    Backend {
        /// The backend that failed.
        backend: String,
        /// The failure detail.
        message: String,
    },
    /// A compare-and-set update named a stale revision; the write was refused
    /// rather than overwriting a concurrent writer's memory.
    RevisionConflict {
        /// The contested memory.
        memory_id: AgentPrivateMemoryId,
        /// The revision the update expected.
        expected: AgentRevisionNumber,
        /// The revision the store holds.
        actual: AgentRevisionNumber,
    },
    /// A create found the memory already present under a different operation.
    AlreadyExists {
        /// The already-present memory.
        memory_id: AgentPrivateMemoryId,
    },
    /// The addressed memory does not exist in the caller's scope.
    ///
    /// Deliberately identical for absent and out-of-scope memories, so an
    /// unauthorized caller learns nothing, not even existence.
    NotFound {
        /// The addressed memory.
        memory_id: AgentPrivateMemoryId,
    },
    /// The addressed memory was withdrawn; a tombstone accepts no update.
    Tombstoned {
        /// The withdrawn memory.
        memory_id: AgentPrivateMemoryId,
    },
    /// A replayed operation's payload was erased by a later deletion or
    /// purge; the replay fails closed rather than resurrect deleted content.
    OperationErased {
        /// The replayed operation.
        operation_id: MemoryOperationId,
    },
    /// A confidence score exceeded the 10000 basis-point bound.
    ConfidenceOutOfRange {
        /// The rejected score.
        confidence_bps: u16,
    },
    /// An external reference on a private memory exceeded the identity bound.
    ReferenceTooLong {
        /// Which field carried the reference.
        field: &'static str,
        /// The rejected length, in bytes.
        length: usize,
        /// The maximum accepted length, in bytes.
        maximum: usize,
    },
    /// Embedding metadata was structurally invalid.
    InvalidEmbeddingRef {
        /// What was invalid.
        message: String,
    },
    /// A tombstoned record still carried content, which fails closed on load.
    TombstoneCarriesContent {
        /// The offending memory.
        memory_id: AgentPrivateMemoryId,
    },
}

impl MemoryError {
    /// Stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Schema(error) => error.code(),
            Self::EntryTooLarge { .. } => "memory-entry-too-large",
            Self::SequenceConflict { .. } => "memory-sequence-conflict",
            Self::OperationConflict { .. } => "memory-operation-conflict",
            Self::OutboxOverflow { .. } => "memory-outbox-overflow",
            Self::Encoding { .. } => "memory-encoding-failed",
            Self::Backend { .. } => "memory-backend-failed",
            Self::RevisionConflict { .. } => "memory-revision-conflict",
            Self::AlreadyExists { .. } => "memory-already-exists",
            Self::NotFound { .. } => "memory-not-found",
            Self::Tombstoned { .. } => "memory-tombstoned",
            Self::OperationErased { .. } => "memory-operation-erased",
            Self::ConfidenceOutOfRange { .. } => "memory-confidence-out-of-range",
            Self::ReferenceTooLong { .. } => "memory-reference-too-long",
            Self::InvalidEmbeddingRef { .. } => "memory-embedding-invalid",
            Self::TombstoneCarriesContent { .. } => "memory-tombstone-content",
        }
    }
}

impl Display for MemoryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Schema(error) => Display::fmt(error, f),
            Self::EntryTooLarge { bytes, maximum } => write!(
                f,
                "the session memory entry is {bytes} bytes, which exceeds the {maximum} byte limit"
            ),
            Self::SequenceConflict { sequence } => write!(
                f,
                "session memory sequence {sequence} is already claimed by a different entry"
            ),
            Self::OperationConflict { operation_id } => write!(
                f,
                "session memory operation {operation_id} is already claimed by a different entry"
            ),
            Self::OutboxOverflow { maximum } => write!(
                f,
                "the session memory outbox is full at its {maximum} entry bound"
            ),
            Self::Encoding { message } => {
                write!(f, "a memory value could not be encoded: {message}")
            }
            Self::Backend { backend, message } => {
                write!(f, "the {backend} memory backend failed: {message}")
            }
            Self::RevisionConflict {
                memory_id,
                expected,
                actual,
            } => write!(
                f,
                "private memory {memory_id} is at revision {actual}, not the expected {expected}; \
                 the stale update was refused"
            ),
            Self::AlreadyExists { memory_id } => {
                write!(f, "private memory {memory_id} already exists")
            }
            Self::NotFound { memory_id } => {
                write!(f, "private memory {memory_id} does not exist in this scope")
            }
            Self::Tombstoned { memory_id } => {
                write!(
                    f,
                    "private memory {memory_id} was withdrawn and accepts no update"
                )
            }
            Self::OperationErased { operation_id } => write!(
                f,
                "memory operation {operation_id} was erased by a later deletion; \
                 the replay fails closed rather than resurrect deleted content"
            ),
            Self::ConfidenceOutOfRange { confidence_bps } => write!(
                f,
                "the confidence score of {confidence_bps} basis points exceeds the 10000 bound"
            ),
            Self::ReferenceTooLong {
                field,
                length,
                maximum,
            } => write!(
                f,
                "the private memory reference {field} is {length} bytes, which exceeds the \
                 {maximum} byte limit"
            ),
            Self::InvalidEmbeddingRef { message } => {
                write!(f, "the embedding metadata is invalid: {message}")
            }
            Self::TombstoneCarriesContent { memory_id } => write!(
                f,
                "tombstoned private memory {memory_id} still carries content, which fails closed"
            ),
        }
    }
}

impl std::error::Error for MemoryError {}

impl From<AgentSchemaError> for MemoryError {
    fn from(error: AgentSchemaError) -> Self {
        Self::Schema(error)
    }
}

/// Fails closed on a session entry or snapshot this binary cannot interpret.
pub fn check_memory_schema(
    policy: &AgentSchemaPolicy,
    entry: &SessionMemoryEntry,
) -> Result<(), AgentSchemaError> {
    policy.check_record(entry)
}

/// Fails closed on a private memory this binary cannot interpret.
pub fn check_private_memory_schema(
    policy: &AgentSchemaPolicy,
    memory: &AgentPrivateMemory,
) -> Result<(), AgentSchemaError> {
    policy.check_record(memory)
}

/// A convenience alias over the tenant and agent that own private memory.
///
/// It is the same `(TenantId, AgentId)` boundary [`AgentScope`] addresses; the
/// alias exists so a caller reading memory code sees the ownership scope named.
pub type PrivateMemoryScope = AgentScope;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{AgentId, AgentRunId, AgentRunScope, TenantId};

    fn scope(tenant: &str, agent: &str, run: &str) -> AgentRunScope {
        AgentRunScope::new(
            TenantId::new(tenant),
            AgentId::new(agent).expect("the agent id is valid"),
            AgentRunId::new(run).expect("the run id is valid"),
        )
        .expect("the scope is valid")
    }

    fn entry(scope: &AgentRunScope, turn: u64, slot: &str, sequence: u64) -> SessionMemoryEntry {
        SessionMemoryEntry::new(
            MemoryEntryId::derive(scope, format!("turn-{turn}-{slot}")).expect("entry id"),
            MemoryOperationId::derive(scope, format!("turn-{turn}-{slot}")).expect("op id"),
            MemorySequence::new(sequence),
            MemoryEntryRole::Assistant,
            AgentTaskContent::inline(serde_json::json!({ "slot": slot })).expect("content"),
            turn,
            None,
            MemoryClassification::Unclassified,
            AgentTimestampMillis::new(sequence),
        )
        .expect("the entry is bounded")
    }

    #[test]
    fn the_snapshot_identity_is_tenant_scoped_pure_and_bounded() {
        let acme = AgentContextSnapshotRef::for_turn(&scope("acme", "support", "t-gen-1"), 1)
            .expect("the reference derives");
        let globex = AgentContextSnapshotRef::for_turn(&scope("globex", "support", "t-gen-1"), 1)
            .expect("the reference derives");
        assert_ne!(acme, globex);

        let joined_left = AgentContextSnapshotRef::for_turn(&scope("t", "a-b", "c"), 1)
            .expect("the reference derives");
        let joined_right = AgentContextSnapshotRef::for_turn(&scope("t", "a", "b-c"), 1)
            .expect("the reference derives");
        assert_ne!(joined_left, joined_right);

        let long = "a".repeat(256);
        let maximal = AgentContextSnapshotRef::for_turn(&scope(&long, &long, &long), u64::MAX)
            .expect("a maximal scope still derives its snapshot");
        assert_eq!(
            maximal,
            AgentContextSnapshotRef::for_turn(&scope(&long, &long, &long), u64::MAX)
                .expect("the reference derives"),
        );
    }

    #[tokio::test]
    async fn an_append_replay_returns_the_original_without_a_second_entry() {
        // Scenario 16: replayed memory writes are idempotent.
        let store = InMemorySessionMemoryStore::new();
        let scope = scope("acme", "support", "run-1");
        let first = entry(&scope, 1, "assistant", 1);

        let a = store.append(&scope, &first).await.expect("first append");
        let b = store.append(&scope, &first).await.expect("replay append");
        assert_eq!(a, b);
        assert_eq!(store.len(&scope), 1);
    }

    #[tokio::test]
    async fn session_memory_is_isolated_by_agent_and_run() {
        // Scenario 14: short-term memory is isolated by both agent id and run id.
        let store = InMemorySessionMemoryStore::new();
        let run_a = scope("acme", "support", "run-a");
        let run_b = scope("acme", "support", "run-b");
        let other_agent = scope("acme", "billing", "run-a");

        store
            .append(&run_a, &entry(&run_a, 1, "assistant", 1))
            .await
            .expect("append to run a");

        // Another run of the same agent sees nothing.
        assert!(store.is_empty(&run_b));
        let page_b = store
            .read(&run_b, SessionMemoryCursor::start())
            .await
            .expect("read run b");
        assert!(page_b.entries.is_empty());

        // A different agent with an identically named run sees nothing either.
        assert!(store.is_empty(&other_agent));

        // The owning run sees its own entry.
        let page_a = store
            .read(&run_a, SessionMemoryCursor::start())
            .await
            .expect("read run a");
        assert_eq!(page_a.entries.len(), 1);
    }

    #[tokio::test]
    async fn the_assembled_window_is_the_most_recent_entries() {
        // A session larger than the window keeps the *most recent* entries, not
        // an arbitrary prefix.
        let session = InMemorySessionMemoryStore::new();
        let scope = scope("acme", "support", "run-1");
        for sequence in 1..=10 {
            session
                .append(
                    &scope,
                    &entry(&scope, 1, &format!("slot-{sequence}"), sequence),
                )
                .await
                .expect("append");
        }

        let reference = AgentContextSnapshotRef::for_turn(&scope, 11).expect("ref");
        let window = SessionWindowPolicy::recent_window().with_max_entries(3);
        let snapshot = assemble_session_context(
            &session,
            &scope,
            &reference,
            11,
            &window,
            AgentRevisionNumber::INITIAL,
            AgentTimestampMillis::new(100),
        )
        .await
        .expect("assemble");

        let sequences: Vec<u64> = snapshot
            .session
            .iter()
            .map(|entry| entry.sequence.get())
            .collect();
        assert_eq!(
            sequences,
            vec![8, 9, 10],
            "the window is the most recent three"
        );
        assert_eq!(snapshot.budget.session_entries, 3);
    }

    #[tokio::test]
    async fn a_persisted_snapshot_is_immutable_across_reassembly() {
        // Scenario 17 (store half): a re-assembly after newer memory reuses the
        // first snapshot.
        let session = InMemorySessionMemoryStore::new();
        let snapshots = InMemoryContextSnapshotStore::new();
        let scope = scope("acme", "support", "run-1");

        session
            .append(&scope, &entry(&scope, 1, "assistant", 1))
            .await
            .expect("first turn recorded");

        let reference = AgentContextSnapshotRef::for_turn(&scope, 2).expect("ref");
        let window = SessionWindowPolicy::recent_window();
        let assembled = assemble_session_context(
            &session,
            &scope,
            &reference,
            2,
            &window,
            AgentRevisionNumber::INITIAL,
            AgentTimestampMillis::new(10),
        )
        .await
        .expect("assemble");
        let stored = snapshots.persist(&assembled).await.expect("persist");
        assert_eq!(stored.session.len(), 1);

        // Newer memory arrives, and the turn's snapshot is re-assembled.
        session
            .append(&scope, &entry(&scope, 1, "extra", 2))
            .await
            .expect("newer memory");
        let reassembled = assemble_session_context(
            &session,
            &scope,
            &reference,
            2,
            &window,
            AgentRevisionNumber::INITIAL,
            AgentTimestampMillis::new(20),
        )
        .await
        .expect("reassemble");
        // The re-assembly saw the newer entry ...
        assert_eq!(reassembled.session.len(), 2);
        // ... but persisting under the same reference returns the original.
        let stored_again = snapshots
            .persist(&reassembled)
            .await
            .expect("persist again");
        assert_eq!(stored_again.session.len(), 1);
        assert_eq!(stored_again, stored);

        let loaded = snapshots
            .load(&scope, &reference)
            .await
            .expect("load")
            .expect("the snapshot exists");
        assert_eq!(loaded, stored);
        assert!(loaded.is_untrusted());
    }

    #[tokio::test]
    async fn the_snapshot_digest_covers_the_private_selections() {
        // Two snapshots differing only in a private selection or the ingress
        // revision must not agree on a digest — otherwise a corrupted or
        // altered selection would be undetectable.
        let session = InMemorySessionMemoryStore::new();
        let scope = scope("acme", "support", "run-1");
        session
            .append(&scope, &entry(&scope, 1, "assistant", 1))
            .await
            .expect("append");
        let reference = AgentContextSnapshotRef::for_turn(&scope, 2).expect("ref");
        let snapshot = assemble_session_context(
            &session,
            &scope,
            &reference,
            2,
            &SessionWindowPolicy::recent_window(),
            AgentRevisionNumber::INITIAL,
            AgentTimestampMillis::new(10),
        )
        .await
        .expect("assemble");

        let mut with_selection = snapshot.clone();
        let content =
            AgentTaskContent::inline(serde_json::json!("remembered fact")).expect("content");
        with_selection.private_memory.push(SnapshotPrivateMemory {
            memory_id: AgentPrivateMemoryId::new("mem-1").expect("id"),
            revision: AgentRevisionNumber::INITIAL,
            kind: AgentPrivateMemoryKind::Semantic,
            content_digest: content.digest(),
            content,
            classification: MemoryClassification::Unclassified,
            confidence_bps: 9_000,
            relevance_bps: 8_000,
            embedding: None,
            transforms: Vec::new(),
            reports: Vec::new(),
        });
        assert_ne!(
            with_selection.compute_digest(),
            snapshot.compute_digest(),
            "a private selection moves the digest"
        );

        let mut with_ingress = snapshot.clone();
        with_ingress.ingress_revision = Some(AgentRevisionNumber::new(3));
        assert_ne!(
            with_ingress.compute_digest(),
            snapshot.compute_digest(),
            "the recorded ingress revision moves the digest"
        );
    }

    #[tokio::test]
    async fn a_pre_reshape_snapshot_record_loads_with_empty_selections() {
        // Every snapshot persisted before slice 2.2 carried
        // `private_memory: []` and none of the phase-2 fields; the reshaped
        // record still loads such a record under the unreleased-branch rule.
        // The pre-reshape wire form is derived from a real snapshot by
        // stripping exactly what this slice added, so the test cannot drift
        // from the actual serialization.
        let session = InMemorySessionMemoryStore::new();
        let scope = scope("acme", "support", "run-1");
        session
            .append(&scope, &entry(&scope, 1, "assistant", 1))
            .await
            .expect("append");
        let reference = AgentContextSnapshotRef::for_turn(&scope, 2).expect("ref");
        let snapshot = assemble_session_context(
            &session,
            &scope,
            &reference,
            2,
            &SessionWindowPolicy::recent_window(),
            AgentRevisionNumber::INITIAL,
            AgentTimestampMillis::new(10),
        )
        .await
        .expect("assemble");

        let mut record = serde_json::to_value(&snapshot).expect("serializes");
        let object = record.as_object_mut().expect("a snapshot is an object");
        object.remove("ingress_revision");
        object
            .get_mut("budget")
            .and_then(serde_json::Value::as_object_mut)
            .expect("a budget is an object")
            .remove("private_memory_bytes");

        let loaded: MemoryContextSnapshot =
            serde_json::from_value(record).expect("the pre-reshape record loads");
        assert!(loaded.private_memory.is_empty());
        assert_eq!(loaded.ingress_revision, None);
        assert_eq!(loaded.budget.private_memory_bytes, 0);
        assert_eq!(loaded, snapshot);
    }

    // =======================================================================
    // Agent-private long-term memory (slice 2.1).
    // =======================================================================

    fn agent_scope(tenant: &str, agent: &str) -> AgentScope {
        AgentScope::new(
            TenantId::new(tenant),
            AgentId::new(agent).expect("the agent id is valid"),
        )
        .expect("the scope is valid")
    }

    fn memory(scope: &AgentScope, slot: &str, at: u64) -> AgentPrivateMemory {
        AgentPrivateMemory::new(
            AgentPrivateMemoryId::new(format!("mem-{slot}")).expect("memory id"),
            MemoryOperationId::derive_for_agent(scope, format!("write-{slot}-{at}"))
                .expect("op id"),
            AgentPrivateMemoryKind::Semantic,
            AgentTaskContent::inline(serde_json::json!({ "slot": slot })).expect("content"),
            9_000,
            MemoryClassification::Unclassified,
            AgentTimestampMillis::new(at),
        )
        .expect("the memory is bounded")
    }

    #[tokio::test]
    async fn a_replayed_private_create_returns_the_original_without_a_second_memory() {
        // Scenario 16, private half: replayed memory writes are idempotent.
        let store = InMemoryAgentPrivateMemoryStore::new();
        let scope = agent_scope("acme", "support");
        let fact = memory(&scope, "fact", 10);

        let a = store
            .upsert(&scope, &fact, PrivateMemoryExpectation::Absent)
            .await
            .expect("first create");
        let b = store
            .upsert(&scope, &fact, PrivateMemoryExpectation::Absent)
            .await
            .expect("replayed create");
        assert_eq!(a, b);
        assert_eq!(a.revision, AgentRevisionNumber::INITIAL);
        assert_eq!(store.len(&scope), 1);
    }

    #[tokio::test]
    async fn a_replayed_update_returns_its_own_result_even_after_later_updates() {
        let store = InMemoryAgentPrivateMemoryStore::new();
        let scope = agent_scope("acme", "support");
        let fact = memory(&scope, "fact", 10);
        let created = store
            .upsert(&scope, &fact, PrivateMemoryExpectation::Absent)
            .await
            .expect("create");

        let mut second = memory(&scope, "fact", 20);
        second.content = AgentTaskContent::inline(serde_json::json!({ "slot": "fact", "v": 2 }))
            .expect("content");
        let updated = store
            .upsert(
                &scope,
                &second,
                PrivateMemoryExpectation::Revision(created.revision),
            )
            .await
            .expect("first update");
        assert_eq!(updated.revision, created.revision.next());

        let mut third = memory(&scope, "fact", 30);
        third.content = AgentTaskContent::inline(serde_json::json!({ "slot": "fact", "v": 3 }))
            .expect("content");
        store
            .upsert(
                &scope,
                &third,
                PrivateMemoryExpectation::Revision(updated.revision),
            )
            .await
            .expect("second update");

        // The first update's replay answers with the first update's own
        // result, not the record as the second update since left it.
        let replayed = store
            .upsert(
                &scope,
                &second,
                PrivateMemoryExpectation::Revision(created.revision),
            )
            .await
            .expect("replayed first update");
        assert_eq!(replayed, updated);
    }

    #[tokio::test]
    async fn concurrent_cas_updates_admit_exactly_one_writer() {
        // Scenario 15: concurrent runs append private memory without stale
        // overwrite. Two writers race the same expected revision; exactly one
        // wins and the loser is refused rather than overwriting.
        let store = InMemoryAgentPrivateMemoryStore::new();
        let scope = agent_scope("acme", "support");
        let created = store
            .upsert(
                &scope,
                &memory(&scope, "fact", 10),
                PrivateMemoryExpectation::Absent,
            )
            .await
            .expect("create");

        let left = memory(&scope, "fact", 20);
        let right = memory(&scope, "fact", 21);
        let expectation = PrivateMemoryExpectation::Revision(created.revision);
        let (a, b) = tokio::join!(
            store.upsert(&scope, &left, expectation),
            store.upsert(&scope, &right, expectation),
        );

        let outcomes = [a, b];
        assert_eq!(outcomes.iter().filter(|r| r.is_ok()).count(), 1);
        let refusal = outcomes
            .iter()
            .find_map(|r| r.as_ref().err())
            .expect("one writer is refused");
        assert_eq!(refusal.code(), "memory-revision-conflict");
    }

    #[tokio::test]
    async fn creates_and_updates_fail_closed_on_the_wrong_precondition() {
        let store = InMemoryAgentPrivateMemoryStore::new();
        let scope = agent_scope("acme", "support");
        let created = store
            .upsert(
                &scope,
                &memory(&scope, "fact", 10),
                PrivateMemoryExpectation::Absent,
            )
            .await
            .expect("create");

        // A create over an existing memory is refused.
        let duplicate = memory(&scope, "fact", 20);
        let refused = store
            .upsert(&scope, &duplicate, PrivateMemoryExpectation::Absent)
            .await
            .expect_err("the create is refused");
        assert_eq!(refused.code(), "memory-already-exists");

        // An update of an absent memory is refused exactly like an
        // out-of-scope one.
        let absent = memory(&scope, "missing", 20);
        let refused = store
            .upsert(
                &scope,
                &absent,
                PrivateMemoryExpectation::Revision(created.revision),
            )
            .await
            .expect_err("the update is refused");
        assert_eq!(refused.code(), "memory-not-found");
    }

    #[tokio::test]
    async fn a_tombstone_strips_content_but_keeps_digest_and_provenance() {
        let store = InMemoryAgentPrivateMemoryStore::new();
        let scope = agent_scope("acme", "support");
        let created = store
            .upsert(
                &scope,
                &memory(&scope, "fact", 10),
                PrivateMemoryExpectation::Absent,
            )
            .await
            .expect("create");

        let request = PrivateMemoryTombstoneRequest {
            memory_id: created.memory_id.clone(),
            operation_id: MemoryOperationId::derive_for_agent(&scope, "tombstone-fact")
                .expect("op id"),
            reason: MemoryTombstoneReason::Retracted,
            tombstoned_at: AgentTimestampMillis::new(50),
        };
        let stub = store
            .tombstone(&scope, &request)
            .await
            .expect("the tombstone applies");
        assert!(stub.is_tombstoned());
        assert_eq!(stub.content, AgentPrivateMemory::tombstone_content());
        assert_eq!(stub.content_digest, created.content_digest);
        assert_eq!(stub.revision, created.revision.next());

        // The stub stays visible in scope through get; a list excludes it
        // unless the cursor opts in.
        let read = store
            .get(&scope, &created.memory_id, AgentTimestampMillis::new(60))
            .await
            .expect("get")
            .expect("the stub is visible");
        assert!(read.is_tombstoned());
        let page = store
            .list(
                &scope,
                PrivateMemoryCursor::start(),
                AgentTimestampMillis::new(60),
            )
            .await
            .expect("list");
        assert!(page.memories.is_empty());
        let audit_page = store
            .list(
                &scope,
                PrivateMemoryCursor::start().include_tombstoned(),
                AgentTimestampMillis::new(60),
            )
            .await
            .expect("audit list");
        assert_eq!(audit_page.memories.len(), 1);

        // A tombstone accepts no update, replays idempotently, and refuses a
        // second withdrawal under a different operation.
        let update = memory(&scope, "fact", 70);
        let refused = store
            .upsert(
                &scope,
                &update,
                PrivateMemoryExpectation::Revision(stub.revision),
            )
            .await
            .expect_err("the update is refused");
        assert_eq!(refused.code(), "memory-tombstoned");
        let replayed = store
            .tombstone(&scope, &request)
            .await
            .expect("the replay is harmless");
        assert_eq!(replayed, stub);
        let second = PrivateMemoryTombstoneRequest {
            operation_id: MemoryOperationId::derive_for_agent(&scope, "tombstone-fact-again")
                .expect("op id"),
            ..request
        };
        let refused = store
            .tombstone(&scope, &second)
            .await
            .expect_err("a second withdrawal is refused");
        assert_eq!(refused.code(), "memory-tombstoned");
    }

    #[tokio::test]
    async fn a_delete_erases_the_memory_and_its_operation_payloads() {
        let store = InMemoryAgentPrivateMemoryStore::new();
        let scope = agent_scope("acme", "support");
        let fact = memory(&scope, "fact", 10);
        store
            .upsert(&scope, &fact, PrivateMemoryExpectation::Absent)
            .await
            .expect("create");

        let request = PrivateMemoryDeleteRequest {
            memory_id: fact.memory_id.clone(),
            operation_id: MemoryOperationId::derive_for_agent(&scope, "delete-fact")
                .expect("op id"),
        };
        store.delete(&scope, &request).await.expect("delete");
        assert!(store.is_empty(&scope));
        assert!(store
            .get(&scope, &fact.memory_id, AgentTimestampMillis::new(20))
            .await
            .expect("get")
            .is_none());

        // The delete replays harmlessly; the create's replay fails closed
        // rather than resurrect the deleted content.
        store
            .delete(&scope, &request)
            .await
            .expect("replayed delete");
        let refused = store
            .upsert(&scope, &fact, PrivateMemoryExpectation::Absent)
            .await
            .expect_err("the erased create fails closed");
        assert_eq!(refused.code(), "memory-operation-erased");

        // Deleting an absent memory answers exactly like a cross-scope one.
        let absent = PrivateMemoryDeleteRequest {
            memory_id: AgentPrivateMemoryId::new("mem-missing").expect("memory id"),
            operation_id: MemoryOperationId::derive_for_agent(&scope, "delete-missing")
                .expect("op id"),
        };
        let missing = store
            .delete(&scope, &absent)
            .await
            .expect_err("the delete is refused");
        let other = agent_scope("acme", "billing");
        let foreign = store
            .delete(
                &other,
                &PrivateMemoryDeleteRequest {
                    memory_id: fact.memory_id.clone(),
                    operation_id: MemoryOperationId::derive_for_agent(&other, "delete-fact")
                        .expect("op id"),
                },
            )
            .await
            .expect_err("the cross-scope delete is refused");
        assert_eq!(missing.code(), foreign.code());
    }

    #[tokio::test]
    async fn expired_memory_is_invisible_and_purge_expired_is_bounded() {
        let store = InMemoryAgentPrivateMemoryStore::new();
        let scope = agent_scope("acme", "support");
        for slot in ["a", "b", "c"] {
            let expiring = memory(&scope, slot, 10).with_retention(MemoryRetention::ExpiresAt {
                at: AgentTimestampMillis::new(100),
            });
            store
                .upsert(&scope, &expiring, PrivateMemoryExpectation::Absent)
                .await
                .expect("create");
        }

        // Visible before expiry, invisible from the instant itself — sweep or
        // no sweep.
        let before = store
            .list(
                &scope,
                PrivateMemoryCursor::start(),
                AgentTimestampMillis::new(99),
            )
            .await
            .expect("list");
        assert_eq!(before.memories.len(), 3);
        let after = store
            .list(
                &scope,
                PrivateMemoryCursor::start(),
                AgentTimestampMillis::new(100),
            )
            .await
            .expect("list");
        assert!(after.memories.is_empty());

        // The sweep is bounded by its limit and converges to zero.
        let first = store
            .purge_expired(&scope, AgentTimestampMillis::new(100), 2)
            .await
            .expect("purge");
        assert_eq!(first, 2);
        let second = store
            .purge_expired(&scope, AgentTimestampMillis::new(100), 2)
            .await
            .expect("purge");
        assert_eq!(second, 1);
        let third = store
            .purge_expired(&scope, AgentTimestampMillis::new(100), 2)
            .await
            .expect("purge");
        assert_eq!(third, 0);
        assert!(store.is_empty(&scope));
    }

    #[tokio::test]
    async fn cross_scope_private_reads_reveal_nothing() {
        // Scenario 18, private half: an unauthorized read is byte-identical
        // to reading a memory that never existed.
        let store = InMemoryAgentPrivateMemoryStore::new();
        let owner = agent_scope("acme", "support");
        let sibling = agent_scope("acme", "billing");
        let foreign = agent_scope("globex", "support");
        let fact = memory(&owner, "fact", 10);
        store
            .upsert(&owner, &fact, PrivateMemoryExpectation::Absent)
            .await
            .expect("create");

        for scope in [&sibling, &foreign] {
            assert!(store
                .get(scope, &fact.memory_id, AgentTimestampMillis::new(20))
                .await
                .expect("get")
                .is_none());
            let page = store
                .list(
                    scope,
                    PrivateMemoryCursor::start(),
                    AgentTimestampMillis::new(20),
                )
                .await
                .expect("list");
            assert!(page.memories.is_empty());
        }

        // The same memory id string coexists independently in both scopes.
        let twin = memory(&sibling, "fact", 30);
        store
            .upsert(&sibling, &twin, PrivateMemoryExpectation::Absent)
            .await
            .expect("create the twin");
        let owner_view = store
            .get(&owner, &fact.memory_id, AgentTimestampMillis::new(40))
            .await
            .expect("get")
            .expect("the owner's memory");
        assert_eq!(owner_view.created_at, AgentTimestampMillis::new(10));
        let sibling_view = store
            .get(&sibling, &twin.memory_id, AgentTimestampMillis::new(40))
            .await
            .expect("get")
            .expect("the sibling's memory");
        assert_eq!(sibling_view.created_at, AgentTimestampMillis::new(30));
    }

    #[test]
    fn validate_rejects_out_of_bounds_private_memories() {
        let scope = agent_scope("acme", "support");

        let mut confident = memory(&scope, "fact", 10);
        confident.confidence_bps = 10_001;
        assert_eq!(
            confident.validate().expect_err("refused").code(),
            "memory-confidence-out-of-range"
        );

        let mut oversized = memory(&scope, "fact", 10);
        oversized.content = AgentTaskContent::Inline(serde_json::json!({
            "blob": "x".repeat(AGENT_PRIVATE_MEMORY_INLINE_MAX_BYTES + 1),
        }));
        assert_eq!(
            oversized.validate().expect_err("refused").code(),
            "memory-entry-too-large"
        );

        let overlong = memory(&scope, "fact", 10).with_audit(
            rakka_agent_workflow::AgentAuditEventId::new("a".repeat(257)),
        );
        assert_eq!(
            overlong.expect_err("refused").code(),
            "memory-reference-too-long"
        );

        let flat = memory(&scope, "fact", 10).with_embedding(MemoryEmbeddingRef {
            model: "embedder".to_string(),
            dimensions: 0,
            version: AgentRevisionNumber::INITIAL,
        });
        assert_eq!(
            flat.expect_err("refused").code(),
            "memory-embedding-invalid"
        );

        // A tombstoned record that still carries content fails closed on
        // load, and so does a version this binary does not write.
        let stocked = memory(&scope, "fact", 10);
        let mut value = serde_json::to_value(&stocked).expect("encode");
        value["tombstone"] = serde_json::json!({
            "operation_id": "mem-op-tombstone",
            "reason": "retracted",
            "tombstoned_at": 50,
        });
        let carried = serde_json::from_value::<AgentPrivateMemory>(value.clone());
        assert!(carried
            .expect_err("refused")
            .to_string()
            .contains("still carries content"));

        let mut ahead = serde_json::to_value(&stocked).expect("encode");
        ahead["schema_version"] = serde_json::json!(2);
        let decoded = serde_json::from_value::<AgentPrivateMemory>(ahead)
            .expect("a future version decodes; the policy refuses it");
        let policy = AgentSchemaPolicy::default();
        assert!(check_private_memory_schema(&policy, &decoded).is_err());
    }

    #[test]
    fn promoted_identities_derive_purely_and_never_collide_across_scopes() {
        let owner = agent_scope("acme", "support");
        let sibling = agent_scope("acme", "billing");
        let run_a = scope("acme", "support", "run-a");
        let run_b = scope("acme", "support", "run-b");
        let entry_a = MemoryEntryId::derive(&run_a, "turn-1-assistant").expect("entry id");
        let entry_b = MemoryEntryId::derive(&run_b, "turn-1-assistant").expect("entry id");

        let first = AgentPrivateMemoryId::derive_promoted(
            &owner,
            &entry_a,
            AgentPrivateMemoryKind::Semantic,
        )
        .expect("derives");
        // Pure: the same inputs derive the same identity.
        assert_eq!(
            first,
            AgentPrivateMemoryId::derive_promoted(
                &owner,
                &entry_a,
                AgentPrivateMemoryKind::Semantic
            )
            .expect("derives"),
        );
        // Two runs' same-slot entries derive distinct memories, and so do two
        // agents and two kinds.
        assert_ne!(
            first,
            AgentPrivateMemoryId::derive_promoted(
                &owner,
                &entry_b,
                AgentPrivateMemoryKind::Semantic
            )
            .expect("derives"),
        );
        assert_ne!(
            first,
            AgentPrivateMemoryId::derive_promoted(
                &sibling,
                &entry_a,
                AgentPrivateMemoryKind::Semantic
            )
            .expect("derives"),
        );
        assert_ne!(
            first,
            AgentPrivateMemoryId::derive_promoted(
                &owner,
                &entry_a,
                AgentPrivateMemoryKind::Episodic
            )
            .expect("derives"),
        );

        // The agent-scoped operation domain is disjoint from the run-scoped
        // one, whatever the discriminator.
        let agent_op = MemoryOperationId::derive_for_agent(&owner, "x").expect("op id");
        let run_op = MemoryOperationId::derive(&run_a, "x").expect("op id");
        assert_ne!(agent_op, run_op);
    }

    #[tokio::test]
    async fn session_purge_honors_hold_due_time_and_replays_harmlessly() {
        // Open decision 7: terminal-run session retention.
        let session = InMemorySessionMemoryStore::new();
        let snapshots = InMemoryContextSnapshotStore::new();
        let scope = scope("acme", "support", "run-1");
        session
            .append(&scope, &entry(&scope, 1, "assistant", 1))
            .await
            .expect("append");
        let window = SessionWindowPolicy::recent_window();
        let reference = AgentContextSnapshotRef::for_turn(&scope, 1).expect("reference");
        let assembled = assemble_session_context(
            &session,
            &scope,
            &reference,
            1,
            &window,
            AgentRevisionNumber::INITIAL,
            AgentTimestampMillis::new(5),
        )
        .await
        .expect("assemble");
        snapshots.persist(&assembled).await.expect("persist");

        let terminal_at = AgentTimestampMillis::new(100);
        let held = SessionRetentionPolicy::bounded_default()
            .with_retain_for_millis(50)
            .with_legal_hold(true);
        let due = SessionRetentionPolicy::bounded_default().with_retain_for_millis(50);

        assert_eq!(
            session
                .purge_run(&scope, &held, terminal_at, AgentTimestampMillis::new(1_000))
                .await
                .expect("purge"),
            SessionPurgeOutcome::Held,
        );
        assert_eq!(
            session
                .purge_run(&scope, &due, terminal_at, AgentTimestampMillis::new(149))
                .await
                .expect("purge"),
            SessionPurgeOutcome::NotYetDue,
        );
        assert_eq!(session.len(&scope), 1);

        assert_eq!(
            session
                .purge_run(&scope, &due, terminal_at, AgentTimestampMillis::new(150))
                .await
                .expect("purge"),
            SessionPurgeOutcome::Purged { entries: 1 },
        );
        assert_eq!(
            session
                .purge_run(&scope, &due, terminal_at, AgentTimestampMillis::new(151))
                .await
                .expect("replayed purge"),
            SessionPurgeOutcome::Purged { entries: 0 },
        );
        assert!(session.is_empty(&scope));

        // Snapshots embed session content, so their purge is part of the same
        // retention discharge.
        assert_eq!(
            snapshots
                .purge_run(&scope, &held, terminal_at, AgentTimestampMillis::new(1_000))
                .await
                .expect("purge"),
            SessionPurgeOutcome::Held,
        );
        assert_eq!(
            snapshots
                .purge_run(&scope, &due, terminal_at, AgentTimestampMillis::new(150))
                .await
                .expect("purge"),
            SessionPurgeOutcome::Purged { entries: 1 },
        );
        assert!(snapshots.is_empty(&scope));
    }
}
