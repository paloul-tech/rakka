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
//! by slice 1.11; the private and communal stores by phase 2.
//!
//! # What memory is, and is not
//!
//! Memory is application-domain context. It is never the correctness source: the
//! durable run, inbox, outbox, timer, checkpoint, and effect records are, and a
//! session store that is empty, lagging, or unavailable can never make a run
//! resume incorrectly ([specification 13.1](../../../docs/plans/rakka-agent/spec.md)).
//! The loop keeps no turn content of its own — it hands each turn to session
//! memory at [`crate::loop_runtime::AgentLoopPhase::RecordingTurn`] and drops it —
//! so a run that iterates a hundred times persists no more of its own state than
//! one that iterates once
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

use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

use rakka_agent_workflow::{AgentTimestampMillis, StateSchemaVersion};
use serde::{Deserialize, Deserializer, Serialize};

use crate::definition::AgentRevisionNumber;
use crate::identity::{validated_id, AgentIdentityResult, AgentRunScope, AgentScope};
use crate::schema::{
    AgentRecordKind, AgentSchemaError, AgentSchemaPolicy, VersionedAgentRecord,
    CURRENT_AGENT_MEMORY_CONTEXT_SNAPSHOT_SCHEMA_VERSION,
    CURRENT_AGENT_SESSION_MEMORY_SCHEMA_VERSION,
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
    /// under it rather than writing a second. A store that finds a *different*
    /// entry already under the operation id fails closed rather than overwrite
    /// it — that would mean two logically distinct writes claimed one key.
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
}

/// An in-memory session-memory store, for tests and single-process deployments.
#[derive(Debug, Clone, Default)]
pub struct InMemorySessionMemoryStore {
    entries: std::sync::Arc<
        std::sync::Mutex<
            std::collections::BTreeMap<String, std::collections::BTreeMap<u64, SessionMemoryEntry>>,
        >,
    >,
    operations: std::sync::Arc<
        std::sync::Mutex<
            std::collections::BTreeMap<String, std::collections::BTreeMap<String, u64>>,
        >,
    >,
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
            .map_or(0, std::collections::BTreeMap::len)
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
/// The private and communal selections are present but empty until phase 2
/// delivers those stores; the scopes are fixed here so a session-only snapshot
/// and a phase-2 snapshot share one record shape.
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
    /// The private memory selections; empty until phase 2.
    pub private_memory: Vec<MemoryEntryId>,
    /// The communal claim selections; empty until phase 2.
    pub communal_claims: Vec<MemoryEntryId>,
    /// The trust, classification, and ranking policy revision in force.
    pub policy_revision: AgentRevisionNumber,
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
    private_memory: Vec<MemoryEntryId>,
    #[serde(default)]
    communal_claims: Vec<MemoryEntryId>,
    policy_revision: AgentRevisionNumber,
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
}

/// An in-memory snapshot store, for tests and single-process deployments.
#[derive(Debug, Clone, Default)]
pub struct InMemoryContextSnapshotStore {
    snapshots: std::sync::Arc<
        std::sync::Mutex<
            std::collections::BTreeMap<
                String,
                std::collections::BTreeMap<String, MemoryContextSnapshot>,
            >,
        >,
    >,
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
            .map_or(0, std::collections::BTreeMap::len)
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

/// Assembles the immutable context snapshot one model turn is computed from.
///
/// It reads a bounded recent window from the session store, shapes it with the
/// window policy, and builds a [`MemoryContextSnapshot`] under the given
/// reference. It performs no write: persistence is the caller's, through a
/// [`ContextSnapshotStore`] whose idempotent persist is what makes a retry reuse
/// the first assembly. The private and communal selections are empty until phase
/// 2 delivers those stores.
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
        budget,
        content_digest: AgentContentDigest::of_bytes(b""),
        assembled_at: now,
    };
    snapshot.content_digest = snapshot.compute_digest();
    Ok(snapshot)
}

// ===========================================================================
// Agent-private long-term memory ([specification 13.3]) — interface only.
// ===========================================================================

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

/// One agent-private long-term memory, scoped `(TenantId, AgentId)`
/// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// The record shape is fixed here so session and snapshot identities cannot bake
/// in an incompatible scope; the stores that persist and retrieve it arrive in
/// phase 2. The originating run is recorded as provenance but never broadens
/// access to another agent, and embeddings are rebuildable derived projections,
/// never the only copy of the content.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentPrivateMemory {
    /// Stable identity of the memory.
    pub memory_id: AgentPrivateMemoryId,
    /// The idempotent operation that created or last updated it.
    pub operation_id: MemoryOperationId,
    /// The memory type.
    pub kind: AgentPrivateMemoryKind,
    /// The bounded content or immutable artifact reference.
    pub content: AgentTaskContent,
    /// A digest of the content.
    pub content_digest: AgentContentDigest,
    /// The run that originated the memory, as provenance only.
    pub source_run: Option<String>,
    /// A confidence score in basis points (0-10000).
    pub confidence_bps: u16,
    /// The classification of the content.
    pub classification: MemoryClassification,
    /// When the memory was created.
    pub created_at: AgentTimestampMillis,
    /// When the memory was last updated.
    pub updated_at: AgentTimestampMillis,
}

/// The durable agent-private long-term memory store, scoped `(TenantId, AgentId)`
/// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// Declared here — with no implementation — so the ownership scope is fixed
/// before session and snapshot identities depend on it. Promotion,
/// consolidation, and demotion from short-term memory are idempotent durable
/// effects, so an append is idempotent on its operation id. Phase 2 delivers the
/// PostgreSQL and `pgvector` implementations.
pub trait AgentPrivateMemoryStore: Send + Sync + 'static {
    /// Stable backend name, used in telemetry.
    fn backend_name(&self) -> &'static str;

    /// Appends or updates one private memory, idempotently on its operation id.
    fn upsert<'a>(
        &'a self,
        scope: &'a AgentScope,
        memory: &'a AgentPrivateMemory,
    ) -> MemoryFuture<'a, AgentPrivateMemory>;

    /// Loads one private memory by identity, if the caller is authorized and it
    /// exists.
    fn get<'a>(
        &'a self,
        scope: &'a AgentScope,
        memory_id: &'a AgentPrivateMemoryId,
    ) -> MemoryFuture<'a, Option<AgentPrivateMemory>>;
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
    session: std::sync::Arc<dyn SessionMemoryStore>,
    snapshots: std::sync::Arc<dyn ContextSnapshotStore>,
    window: SessionWindowPolicy,
}

impl AgentRunMemory {
    /// Bundles a session store and a snapshot store with the default window.
    #[must_use]
    pub fn new(
        session: std::sync::Arc<dyn SessionMemoryStore>,
        snapshots: std::sync::Arc<dyn ContextSnapshotStore>,
    ) -> Self {
        Self {
            session,
            snapshots,
            window: SessionWindowPolicy::recent_window(),
        }
    }

    /// Uses an explicit window policy.
    #[must_use]
    pub fn with_window(mut self, window: SessionWindowPolicy) -> Self {
        self.window = window;
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
}

impl fmt::Debug for AgentRunMemory {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentRunMemory")
            .field("session", &self.session.backend_name())
            .field("snapshots", &self.snapshots.backend_name())
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
}
