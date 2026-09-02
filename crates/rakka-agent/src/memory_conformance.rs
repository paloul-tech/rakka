//! The memory contract, as one suite every backend runs unchanged.
//!
//! Four traits — [`SessionMemoryStore`], [`ContextSnapshotStore`],
//! [`AgentPrivateMemoryStore`], and [`AgentPrivateMemoryRetriever`] — with two
//! implementations each, and until this slice their semantics were asserted
//! *twice by hand*: once in `memory.rs`'s unit tests and once, copied, in the
//! PostgreSQL adapter's. Two hand-written copies of one contract drift, and
//! the security clauses are exactly the ones a drifted copy would lose.
//!
//! Specification: sections 13.1 (every clause below), 13.2, 13.3, 13.5, 13.6,
//! and the retrieval clauses of 16; scenarios 14, 15, 16, and 18.
//!
//! This module follows the knowledge graph's
//! [`crate`-external `conformance`](../../../rakka-agent-knowledge-graph/src/conformance.rs)
//! idiom — free clause functions over a scopes struct, plus an aggregate — and
//! not the testkit's `assert_*_store_contract` shape, for two structural
//! reasons. The subject is inherently *three*-scoped: proving a read reveals
//! nothing needs a primary scope, a foreign one, and a third genuinely-empty
//! one to compare against. And a live-database runner needs per-run
//! namespacing, which [`MemoryConformanceScopes::unique`] provides once
//! instead of at every call site.
//!
//! # Why the isolation clauses compare against an empty scope
//!
//! `is_empty()` and `is_none()` are not enough, and the existing hand-written
//! tests used both. A backend that answered "empty" *differently* from how it
//! answers an unknown scope — a distinguishable `Ok` versus a distinguishable
//! error, a different page shape, a different cursor — would satisfy them
//! while still telling an unauthorized caller that something is there. The
//! clauses here compare the foreign answer to the empty answer by **whole
//! value**, which is the only form that closes it
//! ([specification 13.1](../../../docs/plans/rakka-agent/spec.md): "authorize
//! before revealing existence or content").
//!
//! # What is deliberately backend-owned
//!
//! Outage behaviour — a store answering [`MemoryError::Backend`] when its
//! backend is unreachable — cannot be exercised against a healthy store, so
//! each backend owes its own proof. So does schema migration idempotence: the
//! portable traits have no migration surface. Both exclusions are stated here
//! rather than left unsaid, the same way the graph's suite states its own.
//!
//! Communal knowledge-graph clauses are absent because there is no communal
//! read path into a model context yet (slice 4.6 deferred it); the graph's own
//! `check_knowledge_graph_contract` covers its store.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use rakka_agent_workflow::AgentTimestampMillis;

use crate::definition::AgentRevisionNumber;
use crate::identity::{AgentId, AgentRunId, AgentRunScope, AgentScope, TenantId};
use crate::memory::{
    AgentContextSnapshotRef, AgentPrivateMemory, AgentPrivateMemoryId, AgentPrivateMemoryKind,
    AgentPrivateMemoryStore, ContextSnapshotStore, MemoryClassification, MemoryEntryId,
    MemoryEntryRole, MemoryError, MemoryFuture, MemoryOperationId, MemorySequence,
    MemoryTombstoneReason, PrivateMemoryCursor, PrivateMemoryDeleteRequest,
    PrivateMemoryExpectation, PrivateMemoryTombstoneRequest, SessionMemoryCursor,
    SessionMemoryEntry, SessionMemoryStore, SessionPurgeOutcome, SessionRetentionPolicy,
};
use crate::retrieval::{AgentPrivateMemoryRetriever, MemoryRetrievalQuery};
use crate::task::{AgentContentDigest, AgentTaskContent};

/// Environment variable pinning the run namespace, so a failing run's rows can
/// be found afterwards.
pub const MEMORY_CONFORMANCE_RUN_ENV: &str = "RAKKA_AGENT_MEMORY_CONFORMANCE_RUN";

/// Hex digits of the per-run nonce.
const RUN_NONCE_HEX_DIGITS: usize = 12;

static CONFORMANCE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn run_namespace() -> &'static str {
    static RUN_NAMESPACE: OnceLock<String> = OnceLock::new();
    RUN_NAMESPACE.get_or_init(|| {
        if let Some(pinned) = std::env::var(MEMORY_CONFORMANCE_RUN_ENV)
            .ok()
            .filter(|pinned| !pinned.is_empty())
        {
            return pinned;
        }
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        let seed = format!("{}|{elapsed}", std::process::id());
        AgentContentDigest::of_bytes(seed.as_bytes())
            .value
            .chars()
            .take(RUN_NONCE_HEX_DIGITS)
            .collect()
    })
}

/// The scopes every isolation clause works with.
///
/// `foreign` differs from `primary` in **tenant, agent, and run at once**, so
/// a backend that fences on only one of the three cannot pass by accident;
/// `sibling_run` and `sibling_agent` isolate those two dimensions
/// individually (scenario 14); and `empty` is a fourth scope nothing ever
/// writes to — the reference answer that makes "reveals nothing" a whole-value
/// equality.
#[derive(Debug, Clone)]
pub struct MemoryConformanceScopes {
    /// The scope under test, which the clause populates.
    pub primary: AgentRunScope,
    /// A different tenant, agent, and run.
    pub foreign: AgentRunScope,
    /// A scope nothing writes to, in the primary tenant.
    pub empty: AgentRunScope,
    /// A second run of the primary's own agent.
    pub sibling_run: AgentRunScope,
    /// A second agent of the primary's own tenant.
    pub sibling_agent: AgentRunScope,
}

impl MemoryConformanceScopes {
    /// Fresh scopes for one clause run.
    #[must_use]
    pub fn unique(label: &str) -> Self {
        Self::unique_in(run_namespace(), label)
    }

    /// Fresh scopes in an explicitly named namespace.
    #[must_use]
    pub fn unique_in(namespace: &str, label: &str) -> Self {
        let sequence = CONFORMANCE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let scope = |tenant: &str, agent: &str, run: &str| {
            AgentRunScope::new(
                TenantId::new(format!("conf-{namespace}-{label}-{sequence}-{tenant}")),
                AgentId::new(format!("agent-{agent}")).expect("the agent id is valid"),
                AgentRunId::new(format!("run-{run}")).expect("the run id is valid"),
            )
            .expect("the conformance namespace and label satisfy the identity rules")
        };
        Self {
            primary: scope("a", "a", "a"),
            foreign: scope("b", "b", "b"),
            empty: scope("a", "empty", "empty"),
            sibling_run: scope("a", "a", "b"),
            sibling_agent: scope("a", "b", "a"),
        }
    }

    /// Every scope that must answer as if it holds nothing the primary wrote.
    #[must_use]
    pub fn outsiders(&self) -> [&AgentRunScope; 3] {
        [&self.foreign, &self.sibling_run, &self.sibling_agent]
    }

    /// The `(TenantId, AgentId)` projections the private tier addresses.
    #[must_use]
    pub fn agent_scopes(&self) -> MemoryConformanceAgentScopes {
        MemoryConformanceAgentScopes {
            primary: self.primary.agent_scope(),
            foreign: self.foreign.agent_scope(),
            empty: self.empty.agent_scope(),
            sibling_agent: self.sibling_agent.agent_scope(),
        }
    }
}

/// The agent-level projections of [`MemoryConformanceScopes`].
#[derive(Debug, Clone)]
pub struct MemoryConformanceAgentScopes {
    /// The scope under test.
    pub primary: AgentScope,
    /// A different tenant and agent.
    pub foreign: AgentScope,
    /// A scope nothing writes to.
    pub empty: AgentScope,
    /// A second agent of the primary's own tenant.
    pub sibling_agent: AgentScope,
}

impl MemoryConformanceAgentScopes {
    /// Every scope that must answer as if it holds nothing the primary wrote.
    #[must_use]
    pub fn outsiders(&self) -> [&AgentScope; 2] {
        [&self.foreign, &self.sibling_agent]
    }
}

// ===========================================================================
// Exhaustiveness: one operation enum per trait.
// ===========================================================================

/// One scope-addressed operation on [`SessionMemoryStore`].
///
/// The isolation clause matches on this, so a variant added here fails to
/// compile until its arm is written — exhaustiveness by construction rather
/// than by review. Deliberately *not* `#[non_exhaustive]`: that is the point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionMemoryOperation {
    /// `append`.
    Append,
    /// `read`.
    Read,
    /// `purge_run`.
    PurgeRun,
}

impl SessionMemoryOperation {
    /// Every scope-addressed operation.
    pub const ALL: [Self; 3] = [Self::Append, Self::Read, Self::PurgeRun];
}

/// One scope-addressed operation on [`ContextSnapshotStore`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextSnapshotOperation {
    /// `persist`.
    Persist,
    /// `load`.
    Load,
    /// `purge_run`.
    PurgeRun,
}

impl ContextSnapshotOperation {
    /// Every scope-addressed operation.
    pub const ALL: [Self; 3] = [Self::Persist, Self::Load, Self::PurgeRun];
}

/// One scope-addressed operation on [`AgentPrivateMemoryStore`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivateMemoryOperation {
    /// `upsert`.
    Upsert,
    /// `get`.
    Get,
    /// `list`.
    List,
    /// `tombstone`.
    Tombstone,
    /// `delete`.
    Delete,
    /// `purge_expired`.
    PurgeExpired,
}

impl PrivateMemoryOperation {
    /// Every scope-addressed operation.
    pub const ALL: [Self; 6] = [
        Self::Upsert,
        Self::Get,
        Self::List,
        Self::Tombstone,
        Self::Delete,
        Self::PurgeExpired,
    ];
}

/// Trait methods deliberately outside the isolation sweep: they take no scope,
/// so there is nothing for them to leak.
///
/// Naming them makes the exclusion a decision rather than an omission.
pub const MEMORY_UNSCOPED_METHODS: [&str; 2] = ["backend_name", "retriever_version"];

// ===========================================================================
// Record builders.
// ===========================================================================

/// A session entry for one scope at one sequence.
#[must_use]
pub fn conformance_session_entry(
    scope: &AgentRunScope,
    sequence: u64,
    text: &str,
) -> SessionMemoryEntry {
    SessionMemoryEntry::new(
        MemoryEntryId::derive(scope, format!("entry-{sequence}")).expect("the entry id derives"),
        MemoryOperationId::derive(scope, format!("append-{sequence}")).expect("the op id derives"),
        MemorySequence::new(sequence),
        MemoryEntryRole::User,
        AgentTaskContent::inline(serde_json::json!(text)).expect("the content is inline-bounded"),
        1,
        None,
        MemoryClassification::Unclassified,
        AgentTimestampMillis::new(1),
    )
    .expect("the entry is bounded")
}

/// A private memory for one agent scope, keyed by a discriminator.
///
/// The content embeds the discriminator so a token-overlap retriever ranks it,
/// which the retriever clauses depend on.
#[must_use]
pub fn conformance_private_memory(
    scope: &AgentScope,
    discriminator: &str,
    text: &str,
) -> AgentPrivateMemory {
    AgentPrivateMemory::new(
        AgentPrivateMemoryId::new(format!("mem-{discriminator}")).expect("the memory id is valid"),
        MemoryOperationId::derive_for_agent(scope, format!("create-{discriminator}"))
            .expect("the op id derives"),
        AgentPrivateMemoryKind::Semantic,
        AgentTaskContent::inline(serde_json::json!(format!("{discriminator} {text}")))
            .expect("the content is inline-bounded"),
        9_000,
        MemoryClassification::Unclassified,
        AgentTimestampMillis::new(1),
    )
    .expect("the memory is bounded")
}

/// Attempts one withdrawal from `scope` and answers the refusal, or `None`
/// when the store allowed it.
///
/// Returns the whole [`MemoryError`] rather than its code: the isolation
/// clauses compare answers by whole value, and a code comparison would admit
/// a backend that distinguished deny from absent in any other field.
async fn outsider_tombstone(
    store: &dyn AgentPrivateMemoryStore,
    scope: &AgentScope,
    memory_id: &AgentPrivateMemoryId,
    discriminator: &str,
) -> Option<MemoryError> {
    store
        .tombstone(
            scope,
            &PrivateMemoryTombstoneRequest {
                memory_id: memory_id.clone(),
                operation_id: MemoryOperationId::derive_for_agent(scope, discriminator)
                    .expect("the op id derives"),
                reason: MemoryTombstoneReason::Retracted,
                tombstoned_at: AgentTimestampMillis::new(2),
            },
        )
        .await
        .err()
}

/// How a conformance run makes one memory *rankable*.
///
/// The authoritative write is always the store's `upsert`; making the record
/// rankable is the backend's own step — the in-memory reference retriever
/// scans the store and needs none, a vector adapter derives and writes an
/// embedding row. The suite cannot know which, so a backend supplies this one
/// seam and every retriever clause runs unchanged.
pub trait MemoryCorpusSeeder: Send + Sync {
    /// Makes `memory` retrievable in `scope`. The authoritative upsert has
    /// already happened.
    fn index<'a>(
        &'a self,
        scope: &'a AgentScope,
        memory: &'a AgentPrivateMemory,
    ) -> MemoryFuture<'a, ()>;
}

/// A seeder for backends whose retriever reads the authoritative store
/// directly and needs no index step.
#[derive(Debug, Clone, Copy, Default)]
pub struct StoreOnlySeeder;

impl MemoryCorpusSeeder for StoreOnlySeeder {
    fn index<'a>(
        &'a self,
        _scope: &'a AgentScope,
        _memory: &'a AgentPrivateMemory,
    ) -> MemoryFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

fn now() -> AgentTimestampMillis {
    AgentTimestampMillis::new(1_000)
}

fn purge_everything() -> SessionRetentionPolicy {
    SessionRetentionPolicy::bounded_default().with_retain_for_millis(0)
}

// ===========================================================================
// Session tier.
// ===========================================================================

/// A replayed append returns the original result and creates no second entry
/// (scenario 16).
///
/// # Panics
///
/// On any violation.
pub async fn session_idempotent_append(store: &dyn SessionMemoryStore) {
    let scopes = MemoryConformanceScopes::unique("session-idempotent");
    let entry = conformance_session_entry(&scopes.primary, 1, "first");

    let first = store
        .append(&scopes.primary, &entry)
        .await
        .expect("the first append lands");
    let replay = store
        .append(&scopes.primary, &entry)
        .await
        .expect("a replay is not an error");
    assert_eq!(first, replay, "a replayed append answered differently");

    let page = store
        .read(&scopes.primary, SessionMemoryCursor::start())
        .await
        .expect("the read succeeds");
    assert_eq!(page.entries.len(), 1, "the replay created a second entry");
}

/// Every scope-addressed session operation isolates (scenarios 14 and 18).
///
/// # Panics
///
/// On any violation.
pub async fn session_scope_isolation(store: &dyn SessionMemoryStore) {
    let scopes = MemoryConformanceScopes::unique("session-isolation");
    store
        .append(
            &scopes.primary,
            &conformance_session_entry(&scopes.primary, 1, "owned"),
        )
        .await
        .expect("the primary append lands");

    let empty_page = store
        .read(&scopes.empty, SessionMemoryCursor::start())
        .await
        .expect("the empty read succeeds");

    // Reads first, for every outsider, before any of them has written: a
    // later clause's write would otherwise make the *next* outsider's read
    // differ for a reason that has nothing to do with isolation.
    for outsider in scopes.outsiders() {
        let page = store
            .read(outsider, SessionMemoryCursor::start())
            .await
            .expect("the outsider read succeeds");
        assert_eq!(
            page, empty_page,
            "an outsider's read differs from an empty scope's, which reveals existence"
        );
    }

    for operation in SessionMemoryOperation::ALL {
        for outsider in scopes.outsiders() {
            match operation {
                SessionMemoryOperation::Read => {
                    // Covered above, before any outsider wrote. The arm stays
                    // so the match is exhaustive and a new variant fails to
                    // compile until it is handled.
                }
                SessionMemoryOperation::Append => {
                    // An outsider's own append lands in the outsider's scope
                    // and is invisible in the primary's.
                    store
                        .append(outsider, &conformance_session_entry(outsider, 1, "theirs"))
                        .await
                        .expect("the outsider append lands in its own scope");
                    let primary = store
                        .read(&scopes.primary, SessionMemoryCursor::start())
                        .await
                        .expect("the primary read succeeds");
                    assert_eq!(
                        primary.entries.len(),
                        1,
                        "an outsider's append reached the primary scope"
                    );
                }
                SessionMemoryOperation::PurgeRun => {
                    store
                        .purge_run(
                            outsider,
                            &purge_everything(),
                            AgentTimestampMillis::new(1),
                            now(),
                        )
                        .await
                        .expect("the outsider purge succeeds");
                    let primary = store
                        .read(&scopes.primary, SessionMemoryCursor::start())
                        .await
                        .expect("the primary read succeeds");
                    assert_eq!(
                        primary.entries.len(),
                        1,
                        "an outsider's purge deleted the primary's records"
                    );
                }
            }
        }
    }
}

/// Retention: held, not yet due, and purged, with a replay purging zero.
///
/// # Panics
///
/// On any violation.
pub async fn session_retention_purge(store: &dyn SessionMemoryStore) {
    let scopes = MemoryConformanceScopes::unique("session-retention");
    store
        .append(
            &scopes.primary,
            &conformance_session_entry(&scopes.primary, 1, "owned"),
        )
        .await
        .expect("the append lands");
    let terminal_at = AgentTimestampMillis::new(10);

    let held = store
        .purge_run(
            &scopes.primary,
            &purge_everything().with_legal_hold(true),
            terminal_at,
            now(),
        )
        .await
        .expect("the held purge succeeds");
    assert_eq!(held, SessionPurgeOutcome::Held);

    let early = store
        .purge_run(
            &scopes.primary,
            &SessionRetentionPolicy::bounded_default().with_retain_for_millis(1_000_000),
            terminal_at,
            now(),
        )
        .await
        .expect("the early purge succeeds");
    assert_eq!(early, SessionPurgeOutcome::NotYetDue);

    let purged = store
        .purge_run(&scopes.primary, &purge_everything(), terminal_at, now())
        .await
        .expect("the due purge succeeds");
    assert!(
        matches!(purged, SessionPurgeOutcome::Purged { entries } if entries > 0),
        "the due purge deleted nothing: {purged:?}"
    );

    let replay = store
        .purge_run(&scopes.primary, &purge_everything(), terminal_at, now())
        .await
        .expect("the replayed purge succeeds");
    assert_eq!(replay, SessionPurgeOutcome::Purged { entries: 0 });
}

// ===========================================================================
// Snapshot tier.
// ===========================================================================

/// A snapshot is immutable: first-writer-wins, and a second persist of a
/// different snapshot under the same reference answers the original.
///
/// # Panics
///
/// On any violation.
pub async fn snapshot_immutability(store: &dyn ContextSnapshotStore) {
    let scopes = MemoryConformanceScopes::unique("snapshot-immutability");
    let session = crate::memory::InMemorySessionMemoryStore::new();
    session
        .append(
            &scopes.primary,
            &conformance_session_entry(&scopes.primary, 1, "first"),
        )
        .await
        .expect("the seed append lands");
    let reference =
        AgentContextSnapshotRef::for_turn(&scopes.primary, 1).expect("the reference derives");
    let first = crate::memory::assemble_session_context(
        &session,
        &scopes.primary,
        &reference,
        1,
        &crate::memory::SessionWindowPolicy::default(),
        AgentRevisionNumber::INITIAL,
        now(),
    )
    .await
    .expect("the first assembly succeeds");

    store
        .persist(&first)
        .await
        .expect("the first persist lands");

    session
        .append(
            &scopes.primary,
            &conformance_session_entry(&scopes.primary, 2, "second"),
        )
        .await
        .expect("the second append lands");
    let second = crate::memory::assemble_session_context(
        &session,
        &scopes.primary,
        &reference,
        1,
        &crate::memory::SessionWindowPolicy::default(),
        AgentRevisionNumber::INITIAL,
        now(),
    )
    .await
    .expect("the second assembly succeeds");
    store
        .persist(&second)
        .await
        .expect("a second persist is not an error");

    let loaded = store
        .load(&scopes.primary, &reference)
        .await
        .expect("the load succeeds")
        .expect("the snapshot is there");
    assert_eq!(
        loaded.content_digest, first.content_digest,
        "a second persist overwrote an immutable snapshot"
    );
    assert_eq!(
        loaded.compute_digest(),
        loaded.content_digest,
        "the loaded snapshot's digest does not describe its own content"
    );
}

/// Every scope-addressed snapshot operation isolates (scenario 18).
///
/// The clause that catches a keying bug nothing else can: `persist` takes no
/// scope argument, so the record's own `scope` field is the whole fence, and a
/// backend that keyed on the reference alone would leak here.
///
/// # Panics
///
/// On any violation.
pub async fn snapshot_scope_isolation(store: &dyn ContextSnapshotStore) {
    let scopes = MemoryConformanceScopes::unique("snapshot-isolation");
    let session = crate::memory::InMemorySessionMemoryStore::new();
    session
        .append(
            &scopes.primary,
            &conformance_session_entry(&scopes.primary, 1, "owned"),
        )
        .await
        .expect("the seed append lands");
    let reference =
        AgentContextSnapshotRef::for_turn(&scopes.primary, 1).expect("the reference derives");
    let snapshot = crate::memory::assemble_session_context(
        &session,
        &scopes.primary,
        &reference,
        1,
        &crate::memory::SessionWindowPolicy::default(),
        AgentRevisionNumber::INITIAL,
        now(),
    )
    .await
    .expect("the assembly succeeds");
    store.persist(&snapshot).await.expect("the persist lands");

    let empty_reference =
        AgentContextSnapshotRef::for_turn(&scopes.empty, 1).expect("the reference derives");
    let empty_answer = store
        .load(&scopes.empty, &empty_reference)
        .await
        .expect("the empty load succeeds");

    for operation in ContextSnapshotOperation::ALL {
        for outsider in scopes.outsiders() {
            match operation {
                ContextSnapshotOperation::Load => {
                    // The primary's *own reference*, read under an outsider's
                    // scope: the record's `scope` field is the fence.
                    let answer = store
                        .load(outsider, &reference)
                        .await
                        .expect("the outsider load succeeds");
                    assert_eq!(
                        answer, empty_answer,
                        "an outsider loaded the primary's snapshot by its reference"
                    );
                }
                ContextSnapshotOperation::Persist => {
                    // Persisting is scoped by the record, so nothing an
                    // outsider writes may appear under the primary.
                    let outsider_reference =
                        AgentContextSnapshotRef::for_turn(outsider, 1).expect("reference");
                    let outsider_session = crate::memory::InMemorySessionMemoryStore::new();
                    outsider_session
                        .append(outsider, &conformance_session_entry(outsider, 1, "theirs"))
                        .await
                        .expect("the outsider append lands");
                    let theirs = crate::memory::assemble_session_context(
                        &outsider_session,
                        outsider,
                        &outsider_reference,
                        1,
                        &crate::memory::SessionWindowPolicy::default(),
                        AgentRevisionNumber::INITIAL,
                        now(),
                    )
                    .await
                    .expect("the outsider assembly succeeds");
                    store
                        .persist(&theirs)
                        .await
                        .expect("the outsider persist lands");
                    let primary = store
                        .load(&scopes.primary, &reference)
                        .await
                        .expect("the primary load succeeds")
                        .expect("the primary's snapshot is still there");
                    assert_eq!(
                        primary.content_digest, snapshot.content_digest,
                        "an outsider's persist changed the primary's snapshot"
                    );
                }
                ContextSnapshotOperation::PurgeRun => {
                    store
                        .purge_run(
                            outsider,
                            &purge_everything(),
                            AgentTimestampMillis::new(1),
                            now(),
                        )
                        .await
                        .expect("the outsider purge succeeds");
                    assert!(
                        store
                            .load(&scopes.primary, &reference)
                            .await
                            .expect("the primary load succeeds")
                            .is_some(),
                        "an outsider's purge deleted the primary's snapshot"
                    );
                }
            }
        }
    }
}

// ===========================================================================
// Private tier.
// ===========================================================================

/// The open-decision-1 write table: create, compare-and-set, and every
/// refusal (scenario 15).
///
/// # Panics
///
/// On any violation.
pub async fn private_write_preconditions(store: &dyn AgentPrivateMemoryStore) {
    let scopes = MemoryConformanceScopes::unique("private-writes").agent_scopes();
    let memory = conformance_private_memory(&scopes.primary, "one", "renewal terms");

    let created = store
        .upsert(&scopes.primary, &memory, PrivateMemoryExpectation::Absent)
        .await
        .expect("the create lands");
    assert_eq!(
        created.revision,
        AgentRevisionNumber::INITIAL,
        "the store did not stamp the initial revision itself"
    );

    // A *different* operation, not a replay: reusing the original's derived
    // operation id would be answered from the ledger with the original
    // result, which is the idempotence contract rather than a violation.
    let mut different = conformance_private_memory(&scopes.primary, "one", "different");
    different.operation_id =
        MemoryOperationId::derive_for_agent(&scopes.primary, "create-one-again")
            .expect("the op id derives");
    let duplicate = store
        .upsert(
            &scopes.primary,
            &different,
            PrivateMemoryExpectation::Absent,
        )
        .await;
    assert_eq!(
        duplicate.err().map(|error| error.code()),
        Some("memory-already-exists"),
        "a create over an existing memory was not refused"
    );

    let mut stale_write = conformance_private_memory(&scopes.primary, "one", "stale");
    stale_write.operation_id =
        MemoryOperationId::derive_for_agent(&scopes.primary, "update-one-stale")
            .expect("the op id derives");
    let stale = store
        .upsert(
            &scopes.primary,
            &stale_write,
            PrivateMemoryExpectation::Revision(AgentRevisionNumber::new(99)),
        )
        .await;
    assert_eq!(
        stale.err().map(|error| error.code()),
        Some("memory-revision-conflict"),
        "a stale expectation overwrote a concurrent write"
    );

    let absent = store
        .upsert(
            &scopes.primary,
            &conformance_private_memory(&scopes.primary, "missing", "nothing"),
            PrivateMemoryExpectation::Revision(AgentRevisionNumber::INITIAL),
        )
        .await;
    assert_eq!(
        absent.err().map(|error| error.code()),
        Some("memory-not-found"),
        "an update of an absent memory was not refused"
    );
}

/// Tombstone and delete both erase the ledger's earlier payloads, so a
/// replayed pre-withdrawal write fails closed.
///
/// # Panics
///
/// On any violation.
pub async fn private_tombstone_and_delete_erasure(store: &dyn AgentPrivateMemoryStore) {
    let scopes = MemoryConformanceScopes::unique("private-erasure").agent_scopes();
    let memory = conformance_private_memory(&scopes.primary, "withdrawn", "renewal terms");
    store
        .upsert(&scopes.primary, &memory, PrivateMemoryExpectation::Absent)
        .await
        .expect("the create lands");

    store
        .tombstone(
            &scopes.primary,
            &PrivateMemoryTombstoneRequest {
                memory_id: memory.memory_id.clone(),
                operation_id: MemoryOperationId::derive_for_agent(&scopes.primary, "withdraw")
                    .expect("the op id derives"),
                reason: MemoryTombstoneReason::Retracted,
                tombstoned_at: AgentTimestampMillis::new(2),
            },
        )
        .await
        .expect("the tombstone lands");

    let stub = store
        .get(&scopes.primary, &memory.memory_id, now())
        .await
        .expect("the read succeeds")
        .expect("the tombstone stub stays visible to its owner");
    assert!(
        stub.tombstone.is_some(),
        "the withdrawal is not visible on the record"
    );
    assert_eq!(
        stub.content_digest, memory.content_digest,
        "the stub lost the digest that makes the withdrawal auditable"
    );

    let replay = store
        .upsert(&scopes.primary, &memory, PrivateMemoryExpectation::Absent)
        .await;
    assert!(
        replay.is_err(),
        "a replayed pre-withdrawal write resurrected withdrawn content"
    );

    store
        .delete(
            &scopes.primary,
            &PrivateMemoryDeleteRequest {
                memory_id: memory.memory_id.clone(),
                operation_id: MemoryOperationId::derive_for_agent(&scopes.primary, "delete")
                    .expect("the op id derives"),
            },
        )
        .await
        .expect("the delete lands");
    assert!(
        store
            .get(&scopes.primary, &memory.memory_id, now())
            .await
            .expect("the read succeeds")
            .is_none(),
        "a deleted memory is still readable"
    );
}

/// Every scope-addressed private operation isolates (scenario 18).
///
/// # Panics
///
/// On any violation.
pub async fn private_scope_isolation(store: &dyn AgentPrivateMemoryStore) {
    let all = MemoryConformanceScopes::unique("private-isolation");
    let scopes = all.agent_scopes();
    let memory = conformance_private_memory(&scopes.primary, "owned", "renewal terms");
    store
        .upsert(&scopes.primary, &memory, PrivateMemoryExpectation::Absent)
        .await
        .expect("the create lands");

    // A second primary-owned record, under an id the twin-identity clause
    // never creates anywhere else. The withdrawal clause below needs one: by
    // the time it runs, `PrivateMemoryOperation::ALL` has already put every
    // outsider's own `mem-owned` in its own scope, so withdrawing *that* id
    // from an outsider would be an outsider withdrawing its own memory and
    // would prove nothing about isolation.
    let witness = conformance_private_memory(&scopes.primary, "witness", "escalation path");
    store
        .upsert(&scopes.primary, &witness, PrivateMemoryExpectation::Absent)
        .await
        .expect("the create lands");

    let empty_get = store
        .get(&scopes.empty, &memory.memory_id, now())
        .await
        .expect("the empty read succeeds");
    let empty_list = store
        .list(&scopes.empty, PrivateMemoryCursor::start(), now())
        .await
        .expect("the empty list succeeds");
    // The reference answer for a *withdrawal* of an id the scope does not
    // hold. Taken here, against the same id the outsiders will aim at, because
    // the comparison the clause needs is "the outsider's answer for this id"
    // against "an uninvolved scope's answer for this id" — not against the
    // answer for some other id, which differs by the caller's own input
    // (`MemoryError::NotFound` echoes the id asked for) and so could never
    // compare equal.
    let empty_tombstone =
        outsider_tombstone(store, &scopes.empty, &witness.memory_id, "withdraw-empty").await;

    // Reads first, for every outsider, before any of them has written: the
    // twin-identity clause below deliberately populates the outsider scopes,
    // and a read after that would differ for a reason unrelated to isolation.
    for outsider in scopes.outsiders() {
        let answer = store
            .get(outsider, &memory.memory_id, now())
            .await
            .expect("the outsider read succeeds");
        assert_eq!(
            answer, empty_get,
            "an outsider's read of the primary's id differs from an empty scope's"
        );
        let page = store
            .list(outsider, PrivateMemoryCursor::start(), now())
            .await
            .expect("the outsider list succeeds");
        assert_eq!(page, empty_list, "an outsider's listing reveals existence");
    }

    for operation in PrivateMemoryOperation::ALL {
        for outsider in scopes.outsiders() {
            match operation {
                PrivateMemoryOperation::Get | PrivateMemoryOperation::List => {
                    // Covered above, before any outsider wrote. The arms stay
                    // so the match is exhaustive and a new variant fails to
                    // compile until it is handled.
                }
                PrivateMemoryOperation::Upsert => {
                    // The same id in another scope is a different memory, and
                    // creating it must succeed rather than collide.
                    let twin = conformance_private_memory(outsider, "owned", "theirs");
                    store
                        .upsert(outsider, &twin, PrivateMemoryExpectation::Absent)
                        .await
                        .expect("the same id in another scope is a different memory");
                    let primary = store
                        .get(&scopes.primary, &memory.memory_id, now())
                        .await
                        .expect("the primary read succeeds")
                        .expect("the primary's memory is still there");
                    assert_eq!(
                        primary.content_digest, memory.content_digest,
                        "an outsider's write reached the primary's record"
                    );
                }
                PrivateMemoryOperation::Tombstone => {
                    // The *primary's* record, from the outsider's scope —
                    // the shape every sibling arm uses, and the only one that
                    // can be got wrong. Aimed at an id that exists nowhere,
                    // this clause proved a not-found path exists and nothing
                    // about isolation: a backend that let a foreign scope
                    // withdraw the primary's memory passed it.
                    let foreign =
                        outsider_tombstone(store, outsider, &witness.memory_id, "withdraw-witness")
                            .await;
                    // By whole value against the empty scope's answer for the
                    // same id, as every clause in this module does: deny must
                    // be indistinguishable from absent, which "both are
                    // errors" would not establish.
                    assert_eq!(
                        foreign, empty_tombstone,
                        "an outsider's withdrawal of the primary's memory is \
                         distinguishable from an uninvolved scope's"
                    );
                    assert_eq!(
                        foreign.as_ref().map(MemoryError::code),
                        Some("memory-not-found"),
                        "an outsider's tombstone of the primary's memory was not refused"
                    );
                    let held = store
                        .get(&scopes.primary, &witness.memory_id, now())
                        .await
                        .expect("the primary read succeeds")
                        .expect("the primary's memory is still there");
                    assert!(
                        !held.is_tombstoned(),
                        "an outsider withdrew the primary's memory"
                    );
                    assert_eq!(
                        held.content_digest, witness.content_digest,
                        "an outsider's withdrawal reached the primary's record"
                    );
                }
                PrivateMemoryOperation::Delete => {
                    store
                        .delete(
                            outsider,
                            &PrivateMemoryDeleteRequest {
                                memory_id: memory.memory_id.clone(),
                                operation_id: MemoryOperationId::derive_for_agent(
                                    outsider,
                                    "delete-foreign",
                                )
                                .expect("the op id derives"),
                            },
                        )
                        .await
                        .expect("a delete of what the scope does not hold is idempotent");
                    assert!(
                        store
                            .get(&scopes.primary, &memory.memory_id, now())
                            .await
                            .expect("the primary read succeeds")
                            .is_some(),
                        "an outsider deleted the primary's memory"
                    );
                }
                PrivateMemoryOperation::PurgeExpired => {
                    store
                        .purge_expired(outsider, now(), 64)
                        .await
                        .expect("the outsider purge succeeds");
                    assert!(
                        store
                            .get(&scopes.primary, &memory.memory_id, now())
                            .await
                            .expect("the primary read succeeds")
                            .is_some(),
                        "an outsider's expiry sweep deleted the primary's memory"
                    );
                }
            }
        }
    }
    // The empty scope is still empty: nothing above wrote to it.
    assert_eq!(
        store
            .list(&scopes.empty, PrivateMemoryCursor::start(), now())
            .await
            .expect("the empty list succeeds"),
        empty_list,
        "the reference scope stopped being empty, so the comparisons were vacuous"
    );
    drop(all);
}

// ===========================================================================
// Retriever.
// ===========================================================================

/// A retrieval under a foreign scope is whole-value equal to one under an
/// empty scope (scenario 18).
///
/// # Panics
///
/// On any violation.
pub async fn retriever_scope_isolation(
    retriever: &dyn AgentPrivateMemoryRetriever,
    authority: &dyn AgentPrivateMemoryStore,
    seeder: &dyn MemoryCorpusSeeder,
) {
    let scopes = MemoryConformanceScopes::unique("retriever-isolation").agent_scopes();
    let memory = conformance_private_memory(&scopes.primary, "renewal", "contract terms");
    authority
        .upsert(&scopes.primary, &memory, PrivateMemoryExpectation::Absent)
        .await
        .expect("the create lands");
    seeder
        .index(&scopes.primary, &memory)
        .await
        .expect("the index step succeeds");

    let query = MemoryRetrievalQuery::new("renewal contract terms");
    let owned = retriever
        .retrieve(&scopes.primary, &query, now())
        .await
        .expect("the primary retrieval succeeds");
    assert!(
        !owned.memories.is_empty(),
        "the primary retrieval found nothing, so this clause is vacuous"
    );

    let empty = retriever
        .retrieve(&scopes.empty, &query, now())
        .await
        .expect("the empty retrieval succeeds");
    assert!(
        empty.memories.is_empty(),
        "the empty scope's retrieval answered records, so the comparison below \
         would be satisfied by a retriever that ignores scope altogether — it \
         would answer the primary's corpus to every scope, including this one"
    );
    for outsider in scopes.outsiders() {
        let foreign = retriever
            .retrieve(outsider, &query, now())
            .await
            .expect("the outsider retrieval succeeds");
        // By whole value, which is what this module's own rule requires and
        // what a length comparison does not give: a backend answering a
        // foreign scope with a distinguishable-but-same-length outcome — a
        // different record, a different relevance, a different watermark —
        // still tells an unauthorized caller that something is there.
        assert_eq!(
            foreign, empty,
            "an outsider's retrieval differs from an empty scope's"
        );
    }
}

/// Every filter is applied *before* ranking, so a bounded query still answers
/// a full page of admissible records.
///
/// Seeds a corpus whose highest-scoring records are inadmissible and asks for
/// a limit smaller than the corpus: a conformant backend answers admissible
/// records, while one that filters *after* its `LIMIT` answers short. This is
/// the exact defect slice 2.2 fixed in the pgvector adapter, promoted from one
/// backend's bug to a contract every backend owes
/// ([specification 16](../../../docs/plans/rakka-agent/spec.md)).
///
/// # Panics
///
/// On any violation.
pub async fn retriever_filters_before_ranking(
    retriever: &dyn AgentPrivateMemoryRetriever,
    authority: &dyn AgentPrivateMemoryStore,
    seeder: &dyn MemoryCorpusSeeder,
) {
    let scopes = MemoryConformanceScopes::unique("retriever-filters").agent_scopes();

    // Three inadmissible records, then two admissible ones. A post-filter
    // backend that takes the top three answers nothing.
    for name in ["sensitive-a", "sensitive-b", "sensitive-c"] {
        let mut record =
            conformance_private_memory(&scopes.primary, name, "renewal contract terms");
        record.classification = MemoryClassification::Sensitive;
        authority
            .upsert(&scopes.primary, &record, PrivateMemoryExpectation::Absent)
            .await
            .expect("the create lands");
        seeder
            .index(&scopes.primary, &record)
            .await
            .expect("the index step succeeds");
    }
    for name in ["ok-a", "ok-b"] {
        let record = conformance_private_memory(&scopes.primary, name, "renewal contract terms");
        authority
            .upsert(&scopes.primary, &record, PrivateMemoryExpectation::Absent)
            .await
            .expect("the create lands");
        seeder
            .index(&scopes.primary, &record)
            .await
            .expect("the index step succeeds");
    }

    let query = MemoryRetrievalQuery::new("renewal contract terms").with_limit(2);
    let outcome = retriever
        .retrieve(&scopes.primary, &query, now())
        .await
        .expect("the retrieval succeeds");

    assert_eq!(
        outcome.memories.len(),
        2,
        "the backend answered short, which is what filtering after the limit looks like"
    );
    for retrieved in &outcome.memories {
        assert_ne!(
            retrieved.memory.classification,
            MemoryClassification::Sensitive,
            "an inadmissible record survived a pre-ranking filter"
        );
    }
}

/// Every returned record matches the authoritative store's, field for field.
///
/// [specification 13.1](../../../docs/plans/rakka-agent/spec.md): authoritative
/// records are distinguished from derived embeddings and indexes. It is also
/// what makes the snapshot assembly's re-read a *contract check* rather than a
/// workaround.
///
/// # Panics
///
/// On any violation.
pub async fn retriever_answers_authoritative_records(
    retriever: &dyn AgentPrivateMemoryRetriever,
    authority: &dyn AgentPrivateMemoryStore,
    seeder: &dyn MemoryCorpusSeeder,
) {
    let scopes = MemoryConformanceScopes::unique("retriever-authoritative").agent_scopes();
    let memory = conformance_private_memory(&scopes.primary, "renewal", "contract terms");
    authority
        .upsert(&scopes.primary, &memory, PrivateMemoryExpectation::Absent)
        .await
        .expect("the create lands");
    seeder
        .index(&scopes.primary, &memory)
        .await
        .expect("the index step succeeds");

    let outcome = retriever
        .retrieve(
            &scopes.primary,
            &MemoryRetrievalQuery::new("renewal contract terms"),
            now(),
        )
        .await
        .expect("the retrieval succeeds");
    assert!(!outcome.memories.is_empty(), "the retrieval found nothing");

    for retrieved in &outcome.memories {
        let authoritative = authority
            .get(&scopes.primary, &retrieved.memory.memory_id, now())
            .await
            .expect("the authoritative read succeeds")
            .expect("the retriever named a record the store does not hold");
        assert_eq!(
            retrieved.memory, authoritative,
            "the ranked copy differs from the authoritative record"
        );
    }
}

// ===========================================================================
// Aggregates.
// ===========================================================================

/// Every session-store clause.
///
/// # Panics
///
/// On any violation.
pub async fn check_session_memory_store_contract(store: &dyn SessionMemoryStore) {
    session_idempotent_append(store).await;
    session_scope_isolation(store).await;
    session_retention_purge(store).await;
}

/// Every snapshot-store clause.
///
/// # Panics
///
/// On any violation.
pub async fn check_context_snapshot_store_contract(store: &dyn ContextSnapshotStore) {
    snapshot_immutability(store).await;
    snapshot_scope_isolation(store).await;
}

/// Every private-store clause.
///
/// # Panics
///
/// On any violation.
pub async fn check_private_memory_store_contract(store: &dyn AgentPrivateMemoryStore) {
    private_write_preconditions(store).await;
    private_tombstone_and_delete_erasure(store).await;
    private_scope_isolation(store).await;
}

/// Every retriever clause.
///
/// # Panics
///
/// On any violation.
pub async fn check_private_memory_retriever_contract(
    retriever: &dyn AgentPrivateMemoryRetriever,
    authority: &dyn AgentPrivateMemoryStore,
    seeder: &dyn MemoryCorpusSeeder,
) {
    retriever_scope_isolation(retriever, authority, seeder).await;
    retriever_filters_before_ranking(retriever, authority, seeder).await;
    retriever_answers_authoritative_records(retriever, authority, seeder).await;
}

/// The whole memory contract, as a backend crate runs it: unchanged.
///
/// The acceptance shape slice 2.4 established for the knowledge graph — a
/// backend's commit touches zero agent-domain code and calls one function.
///
/// # Panics
///
/// On any violation.
pub async fn check_agent_memory_contract(
    session: &dyn SessionMemoryStore,
    snapshots: &dyn ContextSnapshotStore,
    private: &dyn AgentPrivateMemoryStore,
    retriever: &dyn AgentPrivateMemoryRetriever,
    seeder: &dyn MemoryCorpusSeeder,
) {
    check_session_memory_store_contract(session).await;
    check_context_snapshot_store_contract(snapshots).await;
    check_private_memory_store_contract(private).await;
    check_private_memory_retriever_contract(retriever, private, seeder).await;
}
