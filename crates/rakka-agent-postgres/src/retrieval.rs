//! The pgvector private-memory retrieval adapter
//! ([specification 13.3, 13.6](../../../docs/plans/rakka-agent/spec.md)).
//!
//! Implements [`AgentPrivateMemoryRetriever`] over PostgreSQL with the
//! `pgvector` extension. The derived-vector table
//! ([`EMBEDDING_TABLE_NAME`]) holds **rebuildable projections only** — a
//! vector, and content-free metadata about how it was derived — never content:
//! every retrieval joins back to the authoritative
//! `rakka_agent_private_memory` row and decodes content from there, so a
//! derived row can never be a content source and a vector whose authoritative
//! record is gone, tombstoned, expired, or has moved to a newer revision is
//! *not a candidate*. Eventual consistency manifests as absence, never as
//! ranking current content by stale geometry (scenario 18 holds even
//! mid-crash between a delete and its [`PgvectorPrivateMemoryRetriever::deindex_memory`]).
//!
//! # Filters before ranking
//!
//! Tenant, agent, embedder compatibility (model, dimensions, version),
//! revision currency, tombstone, expiry, classification, and the confidence
//! floor are all `WHERE` predicates evaluated before the `ORDER BY` distance —
//! never a post-`LIMIT` filter, which would let inadmissible records consume
//! result slots and answer short of what the corpus holds — the
//! [specification 16](../../../docs/plans/rakka-agent/spec.md) before-ranking
//! rule as SQL shape, not convention. The scope columns lead the table's
//! primary key, so the scope filter is a btree prefix in the schema itself
//! ([specification 13.6](../../../docs/plans/rakka-agent/spec.md): preserved
//! "even when this reduces index performance").
//!
//! # Recall characteristics
//!
//! The shipped configuration uses an **exact scan**: the `(tenant, agent)`
//! primary-key prefix bounds the candidate set to one agent's corpus, and the
//! distance is computed exactly within it — recall 1.0, cost linear in the
//! agent's live indexed corpus. Private corpora are promotion-fed and small,
//! and pgvector's approximate indexes (HNSW, IVFFlat) post-filter their
//! candidates, silently losing recall under selective filters like the
//! mandatory scope predicate.
//!
//! Deployments with large per-agent corpora may add an approximate index as
//! deployment-applied DDL (not this crate's migration), for example an
//! expression HNSW over a fixed-dimension cast:
//!
//! ```sql
//! CREATE INDEX ON rakka_agent_private_memory_embedding
//!     USING hnsw ((embedding::vector(1536)) vector_cosine_ops);
//! ```
//!
//! Queries only use such an index when they use the matching cast expression,
//! index dimension caps apply, and restoring recall under the scope filter
//! requires pgvector ≥ 0.8 with iterative index scans
//! (`SET hnsw.iterative_scan = strict_order`). Until then, the exact scan is
//! the correct-recall path and the default.
//!
//! The cast refuses a vector of any other dimension count (SQLSTATE `22000`)
//! on every write, so such an index is dropped or rebuilt before the
//! embedder's dimension count changes. Until it is, `index_memory` and
//! `reindex` return pgvector's error.
//!
//! # Deriving, rebuilding, and retention
//!
//! Vectors are written by deployment-invoked maintenance, mirroring the
//! authoritative store's `purge_expired` pattern — there is no resident
//! sweeper, and correctness never depends on a sweep having run:
//!
//! - [`PgvectorPrivateMemoryRetriever::index_memory`] derives one memory's
//!   vector; [`PgvectorPrivateMemoryRetriever::reindex`] sweeps for live
//!   records missing a current vector, which is the **rebuild** path: drop
//!   every derived row and repeated `reindex` restores identical retrieval
//!   ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)). A
//!   memory the pass could not give a vector is counted in
//!   [`ReindexPage::failed`] and stays unretrievable until a later sweep
//!   embeds it.
//! - [`PgvectorPrivateMemoryRetriever::deindex_memory`] and
//!   [`PgvectorPrivateMemoryRetriever::purge_orphaned`] remove residual
//!   derived rows after tombstone/delete/purge of authoritative records —
//!   retention hygiene, not a correctness need, because the retrieval join
//!   already refuses them.
//!
//! The slice 2.1 store's tombstone/delete/purge statements are deliberately
//! **not** extended to touch the derived table: they must stay green on
//! databases that never ran the vector migration.
//!
//! Nothing here writes [`AgentPrivateMemory::embedding`] back through the
//! store: a compare-and-set stamp from the indexing path would bump the
//! revision the just-derived vector was keyed to. The derived row carries the
//! metadata, and retrieval output surfaces it per item.
//!
//! # Prerequisites
//!
//! The `vector` extension package must be installed on the server;
//! [`PgvectorPrivateMemoryRetriever::migrate`] runs
//! `CREATE EXTENSION IF NOT EXISTS vector` (a no-op where it already exists;
//! on older pgvector releases the creating role may need superuser) and fails
//! closed with the server's message where the extension is unavailable.
//! [`pgvector_available`] is the preflight probe.

use std::sync::Arc;

use rakka_agent::{
    check_private_memory_schema, embed_memory_vector, memory_embedding_text, AgentMemoryEmbedder,
    AgentPrivateMemory, AgentPrivateMemoryId, AgentPrivateMemoryRetriever, AgentRevisionNumber,
    AgentSchemaPolicy, AgentScope, MemoryEmbeddingRef, MemoryError, MemoryFuture,
    MemoryRetrievalOutcome, MemoryRetrievalQuery, RetrievedPrivateMemory,
    AGENT_PRIVATE_MEMORY_PAGE_MAX_ENTRIES,
};
use rakka_agent_workflow::AgentTimestampMillis;
use tokio_postgres::Client;

use crate::{apply_sql_under_migration_lock, map_error, BACKEND_NAME};

/// The derived-vector table.
pub const EMBEDDING_TABLE_NAME: &str = "rakka_agent_private_memory_embedding";

/// Stable retriever backend name, reported in telemetry and recorded on
/// snapshots.
pub const PGVECTOR_RETRIEVER_NAME: &str = "postgres-pgvector";

/// The retriever version recorded on snapshots; bumped when the retrieval
/// semantics of this adapter change.
pub const PGVECTOR_RETRIEVER_VERSION: AgentRevisionNumber = AgentRevisionNumber::INITIAL;

/// The pgvector `vector` type's dimension ceiling; a declared embedder above
/// it fails closed before any SQL runs.
pub const PGVECTOR_MAX_DIMENSIONS: u32 = 16_000;

/// Idempotent schema for the derived-vector table.
///
/// The `vector` column is deliberately typmod-less: one shared migration must
/// serve every deployment's embedder, and a fixed `vector(N)` would bake one
/// configuration into DDL. The per-row `dimensions` column plus its `CHECK`
/// carries the constraint instead, and every query filters on it. The primary
/// key leads with the scope columns, and holds one row per memory: one
/// retriever binds one configured embedder, so a model or version switch
/// makes existing rows non-candidates until `reindex` overwrites them —
/// rebuildable derived data, exactly as
/// [specification 13.3](../../../docs/plans/rakka-agent/spec.md) requires.
/// No content bytes, ever: the vector plus content-free metadata only.
///
/// `classification` and `confidence_bps` are denormalized here for the same
/// single reason: [specification 16](../../../docs/plans/rakka-agent/spec.md)
/// requires them enforced *before* ranking, and a predicate can only sit ahead
/// of the `ORDER BY` if the column is in the ranked table. Neither can go
/// stale, because `source_revision = revision` is a retrieval predicate and
/// every authoritative change to a record is a compare-and-set revision bump —
/// a row whose policy metadata has moved is not a candidate at all.
pub const VECTOR_MIGRATION_SQL: &str = "
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS rakka_agent_private_memory_embedding (
    tenant TEXT NOT NULL,
    agent TEXT NOT NULL,
    memory_id TEXT NOT NULL,
    model TEXT NOT NULL,
    dimensions INTEGER NOT NULL CHECK (dimensions > 0),
    version BIGINT NOT NULL CHECK (version > 0),
    source_revision BIGINT NOT NULL CHECK (source_revision > 0),
    classification TEXT NOT NULL,
    confidence_bps INTEGER NOT NULL CHECK (confidence_bps BETWEEN 0 AND 10000),
    content_digest TEXT NOT NULL,
    derived_at BIGINT NOT NULL,
    embedding vector NOT NULL CHECK (vector_dims(embedding) = dimensions),
    PRIMARY KEY (tenant, agent, memory_id)
);
";

/// The distance metric a retrieval ranks by.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum PgvectorDistance {
    /// Cosine distance (`<=>`), magnitude-invariant; the default, and the
    /// right choice for most text embedders without client-side
    /// normalization.
    #[default]
    Cosine,
    /// Euclidean distance (`<->`), for embedders whose geometry prefers it.
    Euclidean,
}

impl PgvectorDistance {
    /// The pgvector operator this metric ranks by.
    #[must_use]
    pub const fn operator(self) -> &'static str {
        match self {
            Self::Cosine => "<=>",
            Self::Euclidean => "<->",
        }
    }

    /// Maps a distance onto the deterministic relevance scale, in basis
    /// points.
    ///
    /// Cosine distance `d ∈ [0, 2]` maps as `(1 − d/2) · 10000` — the same
    /// scale the in-memory reference retriever's cosine similarity uses.
    /// Euclidean distance is unbounded, so it maps as `10000 / (1 + d)`:
    /// monotone, deterministic, and 10000 at distance zero.
    ///
    /// A non-finite distance scores zero. pgvector's cosine operator answers
    /// `NaN` when either side has zero magnitude — which a bag-of-words
    /// embedder produces for text with no tokens at all — and an undefined
    /// direction carries no relevance. This mirrors the reference retriever's
    /// zero-magnitude rule, so both backends score such a pair the same, and
    /// it is stated as a branch rather than left to the saturating float cast
    /// below to arrive at by accident.
    #[must_use]
    pub fn relevance_bps(self, distance: f64) -> u16 {
        if !distance.is_finite() {
            return 0;
        }
        let bps = match self {
            Self::Cosine => (1.0 - distance / 2.0) * 10_000.0,
            Self::Euclidean => 10_000.0 / (1.0 + distance.max(0.0)),
        }
        .round();
        if bps <= 0.0 {
            0
        } else if bps >= 10_000.0 {
            10_000
        } else {
            // The clamp above bounds the value, so the cast is lossless.
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            {
                bps as u16
            }
        }
    }
}

/// Configuration of one [`PgvectorPrivateMemoryRetriever`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgvectorRetrieverConfig {
    /// The distance metric retrievals rank by.
    pub distance: PgvectorDistance,
    /// The adapter-side ceiling on results per retrieval, clamping whatever
    /// the query asks for.
    pub max_results: usize,
}

impl Default for PgvectorRetrieverConfig {
    fn default() -> Self {
        Self {
            distance: PgvectorDistance::Cosine,
            max_results: AGENT_PRIVATE_MEMORY_PAGE_MAX_ENTRIES,
        }
    }
}

/// What [`PgvectorPrivateMemoryRetriever::index_memory`] did for one memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum IndexOutcome {
    /// A vector was derived and stored for the authoritative revision.
    Indexed {
        /// The authoritative revision the vector was derived from.
        source_revision: AgentRevisionNumber,
    },
    /// The memory does not exist in this scope; nothing was indexed.
    SkippedAbsent,
    /// The memory is tombstoned; a withdrawn memory is never indexed.
    SkippedTombstoned,
    /// The memory is expired; an invisible memory is never indexed.
    SkippedExpired,
    /// The memory's content is redacted; withheld bytes are never embedded.
    SkippedRedacted,
    /// The memory's content is artifact-backed or carries no embeddable
    /// text; this adapter never loads artifact bytes.
    SkippedArtifact,
}

/// One bounded page of [`PgvectorPrivateMemoryRetriever::reindex`] progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReindexPage {
    /// How many vectors this page derived and stored.
    pub indexed: u64,
    /// How many candidates this page skipped by policy (redacted or
    /// artifact-backed content). Skipped candidates remain candidates; page
    /// past them with [`Self::next`].
    pub skipped: u64,
    /// How many candidates this page could not give a vector: a row whose
    /// id or record this binary cannot read, a record its schema policy
    /// refuses, a memory whose embedding failed, or a memory whose vector
    /// the database refused to store for the memory's own data. A failed
    /// embedding is an error from the embedder, or an answer of the wrong
    /// length or with a component that is not finite.
    ///
    /// These are counted and paged past rather than failing the sweep, so
    /// no one unreadable record, input an embedder always refuses, or memory
    /// the database will not store can stall a rebuild (see
    /// [`PgvectorPrivateMemoryRetriever::reindex`]). A non-zero count is an
    /// operational signal: the memories behind it have no current vector and
    /// are therefore unretrievable. They are still candidates, so a later
    /// sweep reads them again: run it once whatever failed is fixed.
    ///
    /// The count does not say which cause it was, and a page cannot tell an
    /// embedder that is unavailable or misconfigured from memories it
    /// refuses: both are a page of failures. To learn why one memory fails,
    /// call [`PgvectorPrivateMemoryRetriever::index_memory`] for it, which
    /// returns the error.
    pub failed: u64,
    /// The cursor for the next page, `None` when the scan completed.
    pub next: Option<AgentPrivateMemoryId>,
}

/// The pgvector-backed private-memory retriever.
///
/// See the module documentation for the recall characteristics, maintenance
/// runbook, and prerequisites.
#[derive(Clone)]
pub struct PgvectorPrivateMemoryRetriever {
    client: Arc<Client>,
    embedder: Arc<dyn AgentMemoryEmbedder>,
    policy: AgentSchemaPolicy,
    config: PgvectorRetrieverConfig,
}

impl PgvectorPrivateMemoryRetriever {
    /// Creates a retriever over an owned client and the deployment's
    /// embedder.
    #[must_use]
    pub fn new(client: Client, embedder: Arc<dyn AgentMemoryEmbedder>) -> Self {
        Self::from_shared_client(Arc::new(client), embedder)
    }

    /// Creates a retriever that shares an already-`Arc`-wrapped client.
    #[must_use]
    pub fn from_shared_client(client: Arc<Client>, embedder: Arc<dyn AgentMemoryEmbedder>) -> Self {
        Self {
            client,
            embedder,
            policy: AgentSchemaPolicy::default(),
            config: PgvectorRetrieverConfig::default(),
        }
    }

    /// Uses an explicit schema-compatibility policy for fail-closed loads.
    #[must_use]
    pub fn with_schema_policy(mut self, policy: AgentSchemaPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Uses an explicit retriever configuration.
    #[must_use]
    pub fn with_config(mut self, config: PgvectorRetrieverConfig) -> Self {
        self.config = config;
        self
    }

    /// Applies the idempotent vector schema, extension included, under the
    /// crate's migration lock. The base stores' migration is untouched and
    /// stays green on databases without pgvector.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryError::Backend`] when the extension is unavailable or
    /// the DDL cannot be applied; the statement batch is one implicit
    /// transaction, so a failure applies nothing.
    pub async fn migrate(&self) -> Result<(), MemoryError> {
        apply_sql_under_migration_lock(&self.client, VECTOR_MIGRATION_SQL).await
    }

    /// The embedder identity this retriever indexes and retrieves under,
    /// refusing every identity the derived table cannot store: no model, no
    /// dimensions, more dimensions than the pgvector ceiling, version 0 (the
    /// table's `CHECK` starts versions at 1), a model holding a NUL (which a
    /// text column refuses), or a dimension count or version outside its
    /// column. So a refused character in the derived row's write is the
    /// memory's own ([`is_row_data_refusal`]).
    fn embedder_identity(&self) -> Result<(MemoryEmbeddingRef, i32, i64), MemoryError> {
        let reference = self.embedder.embedding_ref();
        if reference.model.is_empty() || reference.dimensions == 0 {
            return Err(MemoryError::InvalidEmbeddingRef {
                message: "the embedder declares an empty model or zero dimensions".to_string(),
            });
        }
        if reference.version.get() == 0 {
            return Err(MemoryError::InvalidEmbeddingRef {
                message: "the embedder declares version 0; stored versions start at 1".to_string(),
            });
        }
        if reference.model.contains('\0') {
            return Err(MemoryError::InvalidEmbeddingRef {
                message: "the embedder's model holds a NUL, which the derived table cannot store"
                    .to_string(),
            });
        }
        if reference.dimensions > PGVECTOR_MAX_DIMENSIONS {
            return Err(MemoryError::InvalidEmbeddingRef {
                message: format!(
                    "the embedder declares {} dimensions, above the pgvector ceiling of {}",
                    reference.dimensions, PGVECTOR_MAX_DIMENSIONS
                ),
            });
        }
        let dimensions =
            i32::try_from(reference.dimensions).map_err(|_| MemoryError::InvalidEmbeddingRef {
                message: "the embedder's dimension count exceeds the storable range".to_string(),
            })?;
        let version = i64::try_from(reference.version.get()).map_err(|_| {
            MemoryError::InvalidEmbeddingRef {
                message: "the embedder's version exceeds the storable range".to_string(),
            }
        })?;
        Ok((reference, dimensions, version))
    }

    /// Embeds one text through the domain's shared check, so this adapter
    /// refuses an embedder's misdescribed output identically to every other
    /// backend ([`embed_memory_vector`]).
    async fn embed_checked(&self, text: &str) -> Result<Vec<f32>, MemoryError> {
        embed_memory_vector(self.embedder.as_ref(), text).await
    }

    /// Decodes a stored memory and fails closed on an unsupported schema
    /// version.
    fn decode_memory(&self, bytes: &[u8]) -> Result<AgentPrivateMemory, MemoryError> {
        let memory: AgentPrivateMemory =
            serde_json::from_slice(bytes).map_err(|error| MemoryError::Backend {
                backend: BACKEND_NAME.to_string(),
                message: format!("a stored private memory could not be decoded: {error}"),
            })?;
        check_private_memory_schema(&self.policy, &memory)?;
        Ok(memory)
    }

    /// Derives and stores one memory's vector. An embedder identity that
    /// cannot be stored is refused before the model is called.
    async fn index_decoded(
        &self,
        scope: &AgentScope,
        memory: &AgentPrivateMemory,
        text: &str,
        now: AgentTimestampMillis,
    ) -> Result<IndexOutcome, MemoryError> {
        let identity = self.embedder_identity()?;
        let vector = self.embed_checked(text).await?;
        self.store_vector(scope, memory, &vector, &identity, now)
            .await
            .map_err(StoreFailure::into_error)
    }

    /// Stores the vector derived from one memory under `identity`, upserting
    /// the derived row keyed to the authoritative revision it was derived
    /// from.
    ///
    /// Apart from the model call this is everything indexing one memory
    /// does, so [`Self::reindex`] can tell an embedder that refused one
    /// input from a write that failed, and a write refused for the memory's
    /// own data ([`StoreFailure::RowRefused`]) from any other.
    async fn store_vector(
        &self,
        scope: &AgentScope,
        memory: &AgentPrivateMemory,
        vector: &[f32],
        identity: &(MemoryEmbeddingRef, i32, i64),
        now: AgentTimestampMillis,
    ) -> Result<IndexOutcome, StoreFailure> {
        let (reference, dimensions, version) = identity;
        let encoded = encode_vector(vector).map_err(StoreFailure::Backend)?;
        let source_revision = i64::try_from(memory.revision.get()).map_err(|_| {
            StoreFailure::RowRefused(MemoryError::Backend {
                backend: BACKEND_NAME.to_string(),
                message: "the memory revision exceeds the storable range".to_string(),
            })
        })?;
        let derived_at = i64::try_from(now.as_millis()).unwrap_or(i64::MAX);

        // The conflict guard makes concurrent indexers converge on the newest
        // source revision (and on the configured embedder) rather than
        // last-writer-wins.
        self.client
            .execute(
                "INSERT INTO rakka_agent_private_memory_embedding
                     (tenant, agent, memory_id, model, dimensions, version,
                      source_revision, classification, confidence_bps,
                      content_digest, derived_at, embedding)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::text::vector)
                 ON CONFLICT (tenant, agent, memory_id) DO UPDATE SET
                     model = EXCLUDED.model,
                     dimensions = EXCLUDED.dimensions,
                     version = EXCLUDED.version,
                     source_revision = EXCLUDED.source_revision,
                     classification = EXCLUDED.classification,
                     confidence_bps = EXCLUDED.confidence_bps,
                     content_digest = EXCLUDED.content_digest,
                     derived_at = EXCLUDED.derived_at,
                     embedding = EXCLUDED.embedding
                 WHERE rakka_agent_private_memory_embedding.source_revision
                           <= EXCLUDED.source_revision
                    OR rakka_agent_private_memory_embedding.model <> EXCLUDED.model
                    OR rakka_agent_private_memory_embedding.dimensions <> EXCLUDED.dimensions
                    OR rakka_agent_private_memory_embedding.version <> EXCLUDED.version",
                &[
                    &scope.tenant().as_str(),
                    &scope.agent().as_str(),
                    &memory.memory_id.as_str(),
                    &reference.model.as_str(),
                    &dimensions,
                    &version,
                    &source_revision,
                    &memory.classification.as_label(),
                    &i32::from(memory.confidence_bps),
                    &memory.content_digest.value.as_str(),
                    &derived_at,
                    &encoded.as_str(),
                ],
            )
            .await
            .map_err(|error| {
                let refused = error.as_db_error().is_some_and(|db_error| {
                    is_row_data_refusal(db_error.code().code(), db_error.constraint())
                });
                if refused {
                    StoreFailure::RowRefused(map_error(error))
                } else {
                    StoreFailure::Backend(map_error(error))
                }
            })?;
        Ok(IndexOutcome::Indexed {
            source_revision: memory.revision,
        })
    }

    /// Derives and stores the vector of one live memory, or reports why it
    /// was skipped.
    ///
    /// Absent, tombstoned, and expired memories are skipped — a vector for
    /// an invisible record would be an orphan at birth — as are redacted
    /// records and artifact-backed content (this adapter never loads
    /// artifact bytes).
    ///
    /// # Errors
    ///
    /// Returns [`MemoryError::Backend`] on SQL failure, including a write
    /// the database refuses for this memory's own data,
    /// [`MemoryError::InvalidEmbeddingRef`] before the model is called when
    /// the embedder declares an identity that cannot be stored (as
    /// [`Self::reindex`] lists) and after it when the embedder's output does
    /// not match its declared identity, and schema errors on a record this
    /// binary cannot interpret.
    pub async fn index_memory(
        &self,
        scope: &AgentScope,
        memory_id: &AgentPrivateMemoryId,
        now: AgentTimestampMillis,
    ) -> Result<IndexOutcome, MemoryError> {
        let now_raw = i64::try_from(now.as_millis()).unwrap_or(i64::MAX);
        let row = self
            .client
            .query_opt(
                "SELECT record, tombstoned, expires_at FROM rakka_agent_private_memory \
                 WHERE tenant = $1 AND agent = $2 AND memory_id = $3",
                &[
                    &scope.tenant().as_str(),
                    &scope.agent().as_str(),
                    &memory_id.as_str(),
                ],
            )
            .await
            .map_err(map_error)?;
        let Some(row) = row else {
            return Ok(IndexOutcome::SkippedAbsent);
        };
        if row.get::<_, bool>("tombstoned") {
            return Ok(IndexOutcome::SkippedTombstoned);
        }
        if row
            .get::<_, Option<i64>>("expires_at")
            .is_some_and(|at| at <= now_raw)
        {
            return Ok(IndexOutcome::SkippedExpired);
        }
        let memory = self.decode_memory(&row.get::<_, Vec<u8>>("record"))?;
        if memory.classification.is_redacted() {
            return Ok(IndexOutcome::SkippedRedacted);
        }
        let Some(text) = memory_embedding_text(&memory.content) else {
            return Ok(IndexOutcome::SkippedArtifact);
        };
        self.index_decoded(scope, &memory, &text, now).await
    }

    /// Sweeps one bounded page of live records lacking a vector current for
    /// this retriever's embedder, deriving and storing up to `limit` of them.
    ///
    /// This is the rebuild path
    /// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)):
    /// dropping every derived row and paging `reindex` to completion restores
    /// identical retrieval, for every memory no page counted in
    /// [`ReindexPage::failed`]. Bounded, idempotent, deployment-invoked —
    /// page with the returned cursor until [`ReindexPage::next`] is `None`.
    ///
    /// # A single unreadable record cannot stall the rebuild
    ///
    /// A record this binary cannot decode — or whose schema version its
    /// fail-closed policy refuses, which is exactly what a rolling upgrade
    /// produces when a newer node has written a newer record shape — is
    /// counted into [`ReindexPage::failed`] and paged past. Propagating it
    /// instead would wedge the sweep permanently: the caller's cursor would
    /// never advance beyond the offending row, so every retry would re-read
    /// the same page and fail identically, and the rebuild path would be
    /// unavailable for that agent until the record was repaired by hand.
    ///
    /// # Nor can a row whose id this binary refuses
    ///
    /// A row whose memory id this binary refuses (one written past the store,
    /// by a later build with a wider id alphabet or by a hand repair) is
    /// counted into [`ReindexPage::failed`] and read past, and does not count
    /// toward `limit`: the call reads on until it has handled `limit` rows
    /// whose id it reads, or the rows run out (issue #200). So
    /// [`ReindexPage::next`] is always an id the call read, and a page of
    /// such rows can never answer the end of a sweep that has more to read.
    /// One call may read more than `limit` rows; it never makes more than
    /// `limit` model calls.
    ///
    /// # Nor can a memory the embedder refuses
    ///
    /// A memory whose embedding fails is counted into [`ReindexPage::failed`]
    /// and paged past in the same way: an error from the embedder, or an
    /// answer of the wrong length or with a component that is not finite. An
    /// input one provider always rejects (a content filter, a text the model
    /// finds too long) would otherwise be the first row of every retry, and
    /// no memory after it would be embedded (issue #151). The embedder's
    /// error is dropped, because it can quote the text it was given.
    ///
    /// So an embedder that fails for every input is not an error either,
    /// whether it is unavailable, refused for its credentials or its quota,
    /// or answers vectors that contradict the identity it declares. Each
    /// page makes one failing call for every memory it tries to embed and
    /// counts them all, and the sweep runs to its end having embedded
    /// nothing. The memories stay candidates, so a later sweep embeds them.
    ///
    /// A page does not say which of these happened. One refused memory in a
    /// page of one, and an outage, are the same page, so a caller must not
    /// stop a sweep because a page embedded nothing: that would hold it on
    /// the refused memory again. A caller that needs to know whether its
    /// embedder works can ask it, with a text of its own.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryError::InvalidEmbeddingRef`] before it reads a page
    /// when the embedder declares an identity that cannot be stored: no
    /// model, no dimensions, more dimensions than the pgvector ceiling,
    /// version 0, a model holding a NUL, or a dimension count or version
    /// outside the storable range. Returns [`MemoryError::Backend`] when the
    /// page cannot be read or a vector cannot be written. What the call
    /// already stored stays, and a retry of the same page reads what is
    /// still missing. A failed embedding is never an error.
    ///
    /// Nor is a write the shipped schema refuses for one memory's own data
    /// (a NUL or another character its digest's text column cannot hold, or
    /// a failed check on its revision or confidence), or a memory whose
    /// revision is outside the storable range: every retry would meet the
    /// same refusal, so it is counted into [`ReindexPage::failed`] and paged
    /// past (issue #199). [`Self::index_memory`] for that memory returns the
    /// database's error. Any other refusal is returned, whatever its
    /// SQLSTATE class, because it may refuse every write: a deployment's own
    /// constraint or trigger, or the HNSW index below, which refuses an
    /// embedder of another dimension count.
    pub async fn reindex(
        &self,
        scope: &AgentScope,
        after: Option<&AgentPrivateMemoryId>,
        now: AgentTimestampMillis,
        limit: usize,
    ) -> Result<ReindexPage, MemoryError> {
        let identity = self.embedder_identity()?;
        let now_raw = i64::try_from(now.as_millis()).unwrap_or(i64::MAX);
        // The cursor inside the call is the raw text of the last row read,
        // whatever it holds, so the next read starts past a row whose id
        // this binary refuses. Every memory id sorts after the empty string.
        let mut after_raw = after.map_or_else(String::new, |id| id.as_str().to_string());
        let limit = limit.max(1);

        let mut indexed = 0u64;
        let mut skipped = 0u64;
        let mut failed = 0u64;
        let mut last: Option<AgentPrivateMemoryId> = None;
        // Rows whose id this binary reads; only they count toward `limit`.
        let mut handled = 0usize;
        let mut ended = false;
        while handled < limit {
            let want = limit - handled;
            let rows = self
                .unindexed_rows(scope, &identity, now_raw, &after_raw, want)
                .await?;
            let read = rows.len();
            for row in rows {
                let raw: String = row.get("memory_id");
                let parsed = AgentPrivateMemoryId::new(raw.as_str());
                after_raw = raw;
                // A row whose id this binary refuses is counted and read
                // past, and does not count toward `limit`: the page ends on
                // an id it read, or at the end of the rows (issue #200).
                let Ok(memory_id) = parsed else {
                    failed += 1;
                    continue;
                };
                handled += 1;
                last = Some(memory_id);
                let Ok(memory) = self.decode_memory(&row.get::<_, Vec<u8>>("record")) else {
                    failed += 1;
                    continue;
                };
                if memory.classification.is_redacted() {
                    skipped += 1;
                    continue;
                }
                let Some(text) = memory_embedding_text(&memory.content) else {
                    skipped += 1;
                    continue;
                };
                // A memory the embedder refuses is counted and paged past, as
                // an unreadable record is: an input one provider always
                // rejects would otherwise be the first row of every retry
                // (issue #151). The failure itself is dropped, because a
                // model's error can quote the text it was given.
                let Ok(vector) = self.embed_checked(&text).await else {
                    failed += 1;
                    continue;
                };
                // A write the shipped schema refuses for the memory's own data
                // is counted and paged past: every retry would meet the same
                // refusal (issue #199). Any other failure ends the call.
                match self
                    .store_vector(scope, &memory, &vector, &identity, now)
                    .await
                {
                    Ok(_) => indexed += 1,
                    Err(StoreFailure::RowRefused(_)) => failed += 1,
                    Err(StoreFailure::Backend(error)) => return Err(error),
                }
            }
            if read < want {
                ended = true;
                break;
            }
        }
        Ok(ReindexPage {
            indexed,
            skipped,
            failed,
            next: if ended { None } else { last },
        })
    }

    /// One read of [`Self::reindex`]: up to `limit` live rows after
    /// `after_raw`, in memory-id order, that have no vector current for
    /// `identity`.
    async fn unindexed_rows(
        &self,
        scope: &AgentScope,
        identity: &(MemoryEmbeddingRef, i32, i64),
        now_raw: i64,
        after_raw: &str,
        limit: usize,
    ) -> Result<Vec<tokio_postgres::Row>, MemoryError> {
        let (reference, dimensions, version) = identity;
        let limit_raw = i64::try_from(limit).unwrap_or(i64::MAX);
        self.client
            .query(
                "SELECT m.memory_id, m.record FROM rakka_agent_private_memory m
                 LEFT JOIN rakka_agent_private_memory_embedding e
                   ON e.tenant = m.tenant AND e.agent = m.agent AND e.memory_id = m.memory_id
                  AND e.model = $3 AND e.dimensions = $4 AND e.version = $5
                  AND e.source_revision = m.revision
                 WHERE m.tenant = $1 AND m.agent = $2
                   AND m.tombstoned = FALSE
                   AND (m.expires_at IS NULL OR m.expires_at > $6)
                   AND e.memory_id IS NULL
                   AND m.memory_id > $7
                 ORDER BY m.memory_id
                 LIMIT $8",
                &[
                    &scope.tenant().as_str(),
                    &scope.agent().as_str(),
                    &reference.model.as_str(),
                    &dimensions,
                    &version,
                    &now_raw,
                    &after_raw,
                    &limit_raw,
                ],
            )
            .await
            .map_err(map_error)
    }

    /// Removes one memory's derived row, returning whether one existed.
    ///
    /// Retention hygiene after a tombstone or deletion; retrieval already
    /// refuses the row through its authoritative join, so this only removes
    /// the residual derived bytes.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryError::Backend`] on SQL failure.
    pub async fn deindex_memory(
        &self,
        scope: &AgentScope,
        memory_id: &AgentPrivateMemoryId,
    ) -> Result<bool, MemoryError> {
        let removed = self
            .client
            .execute(
                "DELETE FROM rakka_agent_private_memory_embedding \
                 WHERE tenant = $1 AND agent = $2 AND memory_id = $3",
                &[
                    &scope.tenant().as_str(),
                    &scope.agent().as_str(),
                    &memory_id.as_str(),
                ],
            )
            .await
            .map_err(map_error)?;
        Ok(removed > 0)
    }

    /// Deletes up to `limit` derived rows whose authoritative record is
    /// gone, tombstoned, or expired, returning how many were removed.
    ///
    /// Bounded and idempotent, invoked by deployments alongside the
    /// authoritative store's `purge_expired`; repeat until it returns zero.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryError::Backend`] on SQL failure.
    pub async fn purge_orphaned(
        &self,
        scope: &AgentScope,
        now: AgentTimestampMillis,
        limit: usize,
    ) -> Result<u64, MemoryError> {
        let now_raw = i64::try_from(now.as_millis()).unwrap_or(i64::MAX);
        let limit_raw = i64::try_from(limit.max(1)).unwrap_or(i64::MAX);
        let row = self
            .client
            .query_one(
                "WITH victims AS (
                     SELECT e.memory_id FROM rakka_agent_private_memory_embedding e
                     LEFT JOIN rakka_agent_private_memory m
                       ON m.tenant = e.tenant AND m.agent = e.agent
                      AND m.memory_id = e.memory_id
                     WHERE e.tenant = $1 AND e.agent = $2
                       AND (m.memory_id IS NULL
                            OR m.tombstoned
                            OR (m.expires_at IS NOT NULL AND m.expires_at <= $3))
                     ORDER BY e.memory_id LIMIT $4
                 ), del AS (
                     DELETE FROM rakka_agent_private_memory_embedding
                     WHERE tenant = $1 AND agent = $2
                       AND memory_id IN (SELECT memory_id FROM victims)
                     RETURNING memory_id
                 )
                 SELECT count(*) AS purged FROM del",
                &[
                    &scope.tenant().as_str(),
                    &scope.agent().as_str(),
                    &now_raw,
                    &limit_raw,
                ],
            )
            .await
            .map_err(map_error)?;
        Ok(u64::try_from(row.get::<_, i64>("purged")).unwrap_or(0))
    }
}

impl std::fmt::Debug for PgvectorPrivateMemoryRetriever {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgvectorPrivateMemoryRetriever")
            .field("embedder", &self.embedder.embedding_ref().model)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl AgentPrivateMemoryRetriever for PgvectorPrivateMemoryRetriever {
    fn backend_name(&self) -> &'static str {
        PGVECTOR_RETRIEVER_NAME
    }

    fn retriever_version(&self) -> AgentRevisionNumber {
        PGVECTOR_RETRIEVER_VERSION
    }

    fn retrieve<'a>(
        &'a self,
        scope: &'a AgentScope,
        query: &'a MemoryRetrievalQuery,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, MemoryRetrievalOutcome> {
        Box::pin(async move {
            let (reference, dimensions, version) = self.embedder_identity()?;
            let query_vector = self.embed_checked(query.text()).await?;
            let encoded = encode_vector(&query_vector)?;
            let now_raw = i64::try_from(now.as_millis()).unwrap_or(i64::MAX);
            let classifications: Vec<String> = query
                .classifications()
                .iter()
                .filter(|classification| !classification.is_redacted())
                .map(|classification| classification.as_label().to_string())
                .collect();
            let limit = query.limit().min(self.config.max_results).max(1);
            let limit_raw = i64::try_from(limit).unwrap_or(i64::MAX);
            let min_confidence = i32::from(query.min_confidence_bps());

            // One statement; every filter the query carries is a WHERE
            // predicate ahead of the ORDER BY distance — including the
            // confidence floor, so a nearer low-confidence record cannot
            // consume a LIMIT slot and shorten the answer — and content comes
            // only from the joined authoritative record. The scope_index CTE
            // rides along so a zero-hit retrieval still reports its watermark.
            let operator = self.config.distance.operator();
            let sql = format!(
                "WITH scope_index AS (
                     SELECT count(*) AS indexed, max(derived_at) AS latest
                     FROM rakka_agent_private_memory_embedding
                     WHERE tenant = $1 AND agent = $2
                       AND model = $3 AND dimensions = $4 AND version = $5
                 ), hits AS (
                     SELECT m.record AS record,
                            e.memory_id AS hit_memory_id,
                            (e.embedding {operator} $6::text::vector) AS distance
                     FROM rakka_agent_private_memory_embedding e
                     JOIN rakka_agent_private_memory m
                       ON m.tenant = e.tenant AND m.agent = e.agent
                      AND m.memory_id = e.memory_id
                     WHERE e.tenant = $1 AND e.agent = $2
                       AND e.model = $3 AND e.dimensions = $4 AND e.version = $5
                       AND e.source_revision = m.revision
                       AND m.tombstoned = FALSE
                       AND (m.expires_at IS NULL OR m.expires_at > $7)
                       AND e.classification = ANY($8)
                       AND e.confidence_bps >= $9
                     ORDER BY distance ASC, e.memory_id ASC
                     LIMIT $10
                 )
                 SELECT s.indexed, s.latest, h.record, h.distance
                 FROM scope_index s LEFT JOIN hits h ON TRUE
                 ORDER BY h.distance ASC NULLS LAST, h.hit_memory_id ASC NULLS LAST"
            );
            let rows = self
                .client
                .query(
                    &sql,
                    &[
                        &scope.tenant().as_str(),
                        &scope.agent().as_str(),
                        &reference.model.as_str(),
                        &dimensions,
                        &version,
                        &encoded.as_str(),
                        &now_raw,
                        &classifications,
                        &min_confidence,
                        &limit_raw,
                    ],
                )
                .await
                .map_err(map_error)?;

            let mut indexed = 0i64;
            let mut latest: Option<i64> = None;
            let mut memories = Vec::new();
            for row in &rows {
                indexed = row.get("indexed");
                latest = row.get("latest");
                let Some(bytes) = row.get::<_, Option<Vec<u8>>>("record") else {
                    continue;
                };
                let memory = self.decode_memory(&bytes)?;
                // Defense in depth, and nothing more: the statement's
                // predicates already enforce every filter this re-check
                // repeats, so a rejection here means the denormalized policy
                // columns disagreed with the authoritative record — which the
                // `source_revision = revision` fence should have made
                // impossible. Dropping the record is the fail-closed answer
                // either way. It is deliberately not the *only* enforcement of
                // any filter: a post-LIMIT drop cannot be, because the record
                // it silently removes has already consumed a result slot.
                if !query.admits(&memory, now) {
                    continue;
                }
                let distance: f64 = row.get("distance");
                memories.push(RetrievedPrivateMemory {
                    relevance_bps: self.config.distance.relevance_bps(distance),
                    embedding: Some(reference.clone()),
                    memory,
                });
            }

            let index_watermark = (indexed > 0).then(|| {
                format!(
                    "pgvector model={} v{} dims={} indexed={indexed} latest={}",
                    reference.model,
                    reference.version,
                    reference.dimensions,
                    latest.unwrap_or(0)
                )
            });
            Ok(MemoryRetrievalOutcome {
                memories,
                index_watermark,
            })
        })
    }
}

/// Probes whether the `vector` extension is available to this database.
///
/// `true` means the extension package is installed on the server (created in
/// this database or creatable); tests use it to skip with a message, and
/// deployments as a preflight.
///
/// # Errors
///
/// Returns [`MemoryError::Backend`] when the catalog query itself fails.
pub async fn pgvector_available(client: &Client) -> Result<bool, MemoryError> {
    let row = client
        .query_one(
            "SELECT count(*) AS present FROM pg_available_extensions WHERE name = 'vector'",
            &[],
        )
        .await
        .map_err(map_error)?;
    Ok(row.get::<_, i64>("present") > 0)
}

/// Encodes a vector as the pgvector text literal `[v1,v2,...]`.
///
/// Rust's shortest-round-trip `f32` formatting restores the identical value
/// through pgvector's parser, and the adapter never reads vectors back —
/// distance is computed server-side — so the text path is lossless where it
/// matters. Non-finite components fail closed before any SQL runs; pgvector
/// would reject them anyway, but with a less stable error.
fn encode_vector(vector: &[f32]) -> Result<String, MemoryError> {
    if vector.is_empty() {
        return Err(MemoryError::InvalidEmbeddingRef {
            message: "an empty vector cannot be encoded".to_string(),
        });
    }
    let mut encoded = String::with_capacity(vector.len() * 10 + 2);
    encoded.push('[');
    for (index, value) in vector.iter().enumerate() {
        if !value.is_finite() {
            return Err(MemoryError::InvalidEmbeddingRef {
                message: format!("vector component {index} is not finite"),
            });
        }
        if index > 0 {
            encoded.push(',');
        }
        encoded.push_str(&value.to_string());
    }
    encoded.push(']');
    Ok(encoded)
}

/// The `CHECK` constraints of the derived table whose values come from the
/// memory itself (its revision and its confidence), as PostgreSQL names the
/// unnamed checks of [`VECTOR_MIGRATION_SQL`].
const ROW_DATA_CHECKS: [&str; 2] = [
    "rakka_agent_private_memory_embedding_source_revision_check",
    "rakka_agent_private_memory_embedding_confidence_bps_check",
];

/// Whether a failed write of the derived row was refused for the memory's
/// own data, by the shipped schema: a character a text column cannot hold
/// (SQLSTATE `22021`, which is how PostgreSQL refuses a NUL, or `22P05`), or
/// one of the [`ROW_DATA_CHECKS`] (`23514`, by constraint name). In this
/// write the only free text taken from the memory is its digest:
/// [`PgvectorPrivateMemoryRetriever::embedder_identity`] refuses a model the
/// table cannot store, and the other text values are validated ids and fixed
/// labels.
///
/// Nothing else is read that way, whatever its class (issue #199). A
/// deployment's own constraint, trigger or index can refuse every write: the
/// HNSW expression index the module documentation offers refuses an embedder
/// of another dimension count with `22000`. Counting such a refusal per
/// memory would hide it behind a page of failures and a model call for each.
fn is_row_data_refusal(code: &str, constraint: Option<&str>) -> bool {
    match code {
        "22021" | "22P05" => true,
        "23514" => constraint.is_some_and(|name| ROW_DATA_CHECKS.contains(&name)),
        _ => false,
    }
}

/// Why [`PgvectorPrivateMemoryRetriever::store_vector`] stored nothing.
#[derive(Debug)]
enum StoreFailure {
    /// The memory's own data was refused: the shipped schema refused the
    /// write for it ([`is_row_data_refusal`]), or its revision is outside the
    /// storable range. Every write of this memory fails the same way, so a
    /// sweep counts it and pages past it.
    RowRefused(MemoryError),
    /// Anything else; a sweep returns it.
    Backend(MemoryError),
}

impl StoreFailure {
    /// The error, whichever kind of failure it was.
    fn into_error(self) -> MemoryError {
        match self {
            Self::RowRefused(error) | Self::Backend(error) => error,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_encoding_is_exact_and_fails_closed() {
        let encoded =
            encode_vector(&[0.1, -2.5, 16_777_217.0, f32::MIN_POSITIVE]).expect("encodes");
        assert!(encoded.starts_with('[') && encoded.ends_with(']'));
        // Round-trip through the text form restores identical f32 values.
        let parsed: Vec<f32> = encoded[1..encoded.len() - 1]
            .split(',')
            .map(|component| component.parse().expect("parses"))
            .collect();
        assert_eq!(parsed, vec![0.1, -2.5, 16_777_217.0, f32::MIN_POSITIVE]);

        assert_eq!(
            encode_vector(&[]).expect_err("empty fails").code(),
            "memory-embedding-invalid"
        );
        assert_eq!(
            encode_vector(&[f32::NAN]).expect_err("nan fails").code(),
            "memory-embedding-invalid"
        );
        assert_eq!(
            encode_vector(&[f32::INFINITY])
                .expect_err("infinity fails")
                .code(),
            "memory-embedding-invalid"
        );
    }

    #[test]
    fn distance_maps_onto_the_documented_relevance_scale() {
        assert_eq!(PgvectorDistance::Cosine.relevance_bps(0.0), 10_000);
        assert_eq!(PgvectorDistance::Cosine.relevance_bps(1.0), 5_000);
        assert_eq!(PgvectorDistance::Cosine.relevance_bps(2.0), 0);
        // Out-of-range distances clamp instead of wrapping.
        assert_eq!(PgvectorDistance::Cosine.relevance_bps(3.0), 0);
        assert_eq!(PgvectorDistance::Euclidean.relevance_bps(0.0), 10_000);
        assert_eq!(PgvectorDistance::Euclidean.relevance_bps(1.0), 5_000);
        assert!(PgvectorDistance::Euclidean.relevance_bps(1_000.0) < 100);

        // An undefined distance carries no relevance, by branch rather than by
        // saturating cast: pgvector answers NaN for a zero-magnitude vector.
        for metric in [PgvectorDistance::Cosine, PgvectorDistance::Euclidean] {
            assert_eq!(metric.relevance_bps(f64::NAN), 0);
            assert_eq!(metric.relevance_bps(f64::INFINITY), 0);
            assert_eq!(metric.relevance_bps(f64::NEG_INFINITY), 0);
        }
    }

    // =======================================================================
    // DSN-gated tests. They additionally probe for the pgvector extension
    // and skip with a message when the test database lacks it, so a plain
    // PostgreSQL database keeps the whole crate green.
    // =======================================================================

    use std::time::{SystemTime, UNIX_EPOCH};

    use rakka_agent::testkit::DeterministicEmbedder;
    use rakka_agent::{
        AgentId, AgentPrivateMemoryKind, AgentPrivateMemoryStore, AgentTaskContent,
        MemoryClassification, MemoryOperationId, MemoryRetention, MemoryTombstoneReason,
        PrivateMemoryExpectation, PrivateMemoryTombstoneRequest, TenantId,
    };
    use tokio_postgres::NoTls;

    use crate::PostgresAgentPrivateMemoryStore;

    fn unique() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_nanos();
        format!("t{nanos}")
    }

    fn scope(tenant: &str, agent: &str) -> AgentScope {
        AgentScope::new(
            TenantId::new(tenant),
            AgentId::new(agent).expect("agent id"),
        )
        .expect("agent scope")
    }

    fn now(millis: u64) -> AgentTimestampMillis {
        AgentTimestampMillis::new(millis)
    }

    fn memory(scope: &AgentScope, name: &str, text: &str) -> AgentPrivateMemory {
        AgentPrivateMemory::new(
            AgentPrivateMemoryId::new(format!("mem-{name}")).expect("memory id"),
            MemoryOperationId::derive_for_agent(scope, format!("create-{name}")).expect("op id"),
            AgentPrivateMemoryKind::Semantic,
            AgentTaskContent::inline(serde_json::json!(text)).expect("content"),
            9_000,
            MemoryClassification::Unclassified,
            now(1),
        )
        .expect("the memory is bounded")
    }

    /// Connects when the DSN is set *and* the database offers pgvector;
    /// otherwise skips, keeping plain-PostgreSQL runs green.
    async fn vector_client() -> Option<Client> {
        let dsn = match std::env::var("RAKKA_POSTGRES_TEST_DSN") {
            Ok(dsn) => dsn,
            Err(_) => return None,
        };
        let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
            .await
            .expect("the PostgreSQL test database should connect");
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("postgres test connection error: {error}");
            }
        });
        if !pgvector_available(&client)
            .await
            .expect("the extension probe should answer")
        {
            // The conformance suite's rule (`tests/memory_conformance.rs`):
            // where the variable is set, a skip is a failure.
            assert!(
                std::env::var("RAKKA_POSTGRES_PGVECTOR_REQUIRED").as_deref() != Ok("1"),
                "the test database lacks the pgvector extension, and \
                 RAKKA_POSTGRES_PGVECTOR_REQUIRED=1 makes that a failure"
            );
            eprintln!("skipping: the test database lacks the pgvector extension");
            return None;
        }
        Some(client)
    }

    /// One migrated world: the authoritative store and a retriever over the
    /// deterministic embedder, sharing a client.
    async fn vector_world() -> Option<(
        PostgresAgentPrivateMemoryStore,
        PgvectorPrivateMemoryRetriever,
    )> {
        vector_world_over(Arc::new(DeterministicEmbedder::new())).await
    }

    /// [`vector_world`], with the retriever over `embedder`.
    async fn vector_world_over(
        embedder: Arc<dyn AgentMemoryEmbedder>,
    ) -> Option<(
        PostgresAgentPrivateMemoryStore,
        PgvectorPrivateMemoryRetriever,
    )> {
        let client = Arc::new(vector_client().await?);
        let store = PostgresAgentPrivateMemoryStore::from_shared_client(client.clone());
        store.migrate().await.expect("base migration");
        let retriever = PgvectorPrivateMemoryRetriever::from_shared_client(client, embedder);
        retriever.migrate().await.expect("vector migration");
        Some((store, retriever))
    }

    async fn seed_and_index(
        store: &PostgresAgentPrivateMemoryStore,
        retriever: &PgvectorPrivateMemoryRetriever,
        scope: &AgentScope,
        name: &str,
        text: &str,
    ) -> AgentPrivateMemory {
        let record = store
            .upsert(
                scope,
                &memory(scope, name, text),
                PrivateMemoryExpectation::Absent,
            )
            .await
            .expect("seed upsert");
        let outcome = retriever
            .index_memory(scope, &record.memory_id, now(100))
            .await
            .expect("index");
        assert_eq!(
            outcome,
            IndexOutcome::Indexed {
                source_revision: record.revision
            }
        );
        record
    }

    #[tokio::test]
    async fn pgvector_migration_is_idempotent_when_dsn_is_set() {
        let Some((store, retriever)) = vector_world().await else {
            return;
        };
        retriever.migrate().await.expect("second vector migration");
        store.migrate().await.expect("base migration beside it");
    }

    #[tokio::test]
    async fn pgvector_retrieval_is_scoped_per_tenant_and_agent_when_dsn_is_set() {
        // Scenario 18 at the vector layer: identical content indexed for an
        // owner, a same-tenant sibling agent, and a foreign tenant — under the
        // *same* memory id — never crosses a scope, and an empty scope's
        // answer is indistinguishable from a never-indexed one.
        let Some((store, retriever)) = vector_world().await else {
            return;
        };
        let tenant = unique();
        let owner = scope(&tenant, "support");
        let sibling = scope(&tenant, "billing");
        let foreign = scope(&format!("{tenant}-rival"), "support");
        for holder in [&owner, &sibling, &foreign] {
            seed_and_index(
                &store,
                &retriever,
                holder,
                "secret",
                "the launch date is friday",
            )
            .await;
        }

        let query = MemoryRetrievalQuery::new("the launch date is friday");
        for holder in [&owner, &sibling, &foreign] {
            let outcome = retriever
                .retrieve(holder, &query, now(200))
                .await
                .expect("retrieval");
            assert_eq!(outcome.memories.len(), 1, "each scope sees exactly its own");
        }

        let empty = scope(&tenant, "brand-new");
        let outcome = retriever
            .retrieve(&empty, &query, now(200))
            .await
            .expect("empty-scope retrieval");
        assert!(outcome.memories.is_empty());
        assert_eq!(
            outcome.index_watermark, None,
            "an empty scope reports no watermark — indistinguishable from never-indexed"
        );
    }

    #[tokio::test]
    async fn pgvector_retrieval_excludes_tombstoned_deleted_and_expired_when_dsn_is_set() {
        // The reveal-nothing guarantee is the retrieval join, not the
        // maintenance sweeps: without any deindex, withdrawn records are
        // already unretrievable; the sweeps then remove the residual rows.
        let Some((store, retriever)) = vector_world().await else {
            return;
        };
        let owner = scope(&unique(), "support");
        seed_and_index(
            &store,
            &retriever,
            &owner,
            "live",
            "the launch date is friday",
        )
        .await;
        seed_and_index(
            &store,
            &retriever,
            &owner,
            "withdrawn",
            "the launch date is friday",
        )
        .await;
        seed_and_index(
            &store,
            &retriever,
            &owner,
            "deleted",
            "the launch date is friday",
        )
        .await;
        let expiring = store
            .upsert(
                &owner,
                &memory(&owner, "expiring", "the launch date is friday")
                    .with_retention(MemoryRetention::ExpiresAt { at: now(500) }),
                PrivateMemoryExpectation::Absent,
            )
            .await
            .expect("seed expiring");
        retriever
            .index_memory(&owner, &expiring.memory_id, now(100))
            .await
            .expect("index expiring");

        store
            .tombstone(
                &owner,
                &PrivateMemoryTombstoneRequest {
                    memory_id: AgentPrivateMemoryId::new("mem-withdrawn").expect("id"),
                    operation_id: MemoryOperationId::derive_for_agent(&owner, "withdraw")
                        .expect("op id"),
                    reason: MemoryTombstoneReason::Retracted,
                    tombstoned_at: now(300),
                },
            )
            .await
            .expect("tombstone");
        store
            .delete(
                &owner,
                &rakka_agent::PrivateMemoryDeleteRequest {
                    memory_id: AgentPrivateMemoryId::new("mem-deleted").expect("id"),
                    operation_id: MemoryOperationId::derive_for_agent(&owner, "erase")
                        .expect("op id"),
                },
            )
            .await
            .expect("delete");

        let outcome = retriever
            .retrieve(
                &owner,
                &MemoryRetrievalQuery::new("the launch date is friday"),
                now(1_000),
            )
            .await
            .expect("retrieval");
        let names: Vec<&str> = outcome
            .memories
            .iter()
            .map(|retrieved| retrieved.memory.memory_id.as_str())
            .collect();
        assert_eq!(
            names,
            vec!["mem-live"],
            "withdrawn, deleted, and expired memories are unretrievable with no deindex run"
        );

        // The sweeps remove the three residual derived rows and converge.
        assert!(retriever
            .deindex_memory(
                &owner,
                &AgentPrivateMemoryId::new("mem-withdrawn").expect("id")
            )
            .await
            .expect("deindex"));
        let mut purged_total = 0;
        loop {
            let purged = retriever
                .purge_orphaned(&owner, now(1_000), 1)
                .await
                .expect("purge");
            if purged == 0 {
                break;
            }
            purged_total += purged;
        }
        assert_eq!(
            purged_total, 2,
            "the deleted and expired rows were residuals"
        );
        assert_eq!(
            retriever
                .purge_orphaned(&owner, now(1_000), 10)
                .await
                .expect("purge again"),
            0,
            "the sweep converged"
        );
    }

    #[tokio::test]
    async fn pgvector_reindex_rebuilds_dropped_vectors_when_dsn_is_set() {
        // Specification 13.3: embeddings are rebuildable derived projections.
        // Drop every derived row; paged reindex restores identical retrieval.
        let Some((store, retriever)) = vector_world().await else {
            return;
        };
        let owner = scope(&unique(), "support");
        seed_and_index(
            &store,
            &retriever,
            &owner,
            "alpha",
            "the launch date is friday",
        )
        .await;
        seed_and_index(
            &store,
            &retriever,
            &owner,
            "beta",
            "friday launch checklist",
        )
        .await;

        let query = MemoryRetrievalQuery::new("the launch date is friday");
        let original = retriever
            .retrieve(&owner, &query, now(200))
            .await
            .expect("retrieval");
        assert_eq!(original.memories.len(), 2);

        // Drop the derived rows out from under the retriever.
        retriever
            .client
            .execute(
                "DELETE FROM rakka_agent_private_memory_embedding \
                 WHERE tenant = $1 AND agent = $2",
                &[&owner.tenant().as_str(), &owner.agent().as_str()],
            )
            .await
            .expect("drop vectors");
        let emptied = retriever
            .retrieve(&owner, &query, now(200))
            .await
            .expect("retrieval");
        assert!(emptied.memories.is_empty());
        assert_eq!(emptied.index_watermark, None);

        // Rebuild in pages of one, driving the cursor to completion.
        let mut after: Option<AgentPrivateMemoryId> = None;
        let mut indexed = 0;
        loop {
            let page = retriever
                .reindex(&owner, after.as_ref(), now(300), 1)
                .await
                .expect("reindex page");
            indexed += page.indexed;
            match page.next {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        assert_eq!(indexed, 2);

        let rebuilt = retriever
            .retrieve(&owner, &query, now(200))
            .await
            .expect("retrieval");
        assert_eq!(
            rebuilt.memories, original.memories,
            "the rebuilt index retrieves identically"
        );
    }

    #[tokio::test]
    async fn pgvector_stale_source_revision_is_not_a_candidate_when_dsn_is_set() {
        // Eventual consistency manifests as absence: a vector derived from a
        // superseded revision never ranks the newer content.
        let Some((store, retriever)) = vector_world().await else {
            return;
        };
        let owner = scope(&unique(), "support");
        let record = seed_and_index(
            &store,
            &retriever,
            &owner,
            "fact",
            "the launch date is friday",
        )
        .await;

        let mut updated = memory(&owner, "fact", "the launch date moved to monday");
        updated.operation_id =
            MemoryOperationId::derive_for_agent(&owner, "update-fact").expect("op id");
        store
            .upsert(
                &owner,
                &updated,
                PrivateMemoryExpectation::Revision(record.revision),
            )
            .await
            .expect("the update lands");

        let query = MemoryRetrievalQuery::new("the launch date");
        let outcome = retriever
            .retrieve(&owner, &query, now(200))
            .await
            .expect("retrieval");
        assert!(
            outcome.memories.is_empty(),
            "the stale vector is not a candidate for the updated record"
        );

        let page = retriever
            .reindex(&owner, None, now(300), 10)
            .await
            .expect("reindex");
        assert_eq!(page.indexed, 1);
        let outcome = retriever
            .retrieve(&owner, &query, now(200))
            .await
            .expect("retrieval");
        assert_eq!(outcome.memories.len(), 1);
        assert_eq!(
            outcome.memories[0].memory.content.inline_value(),
            Some(&serde_json::json!("the launch date moved to monday")),
            "content always comes from the authoritative record"
        );
    }

    #[tokio::test]
    async fn pgvector_model_or_version_mismatch_is_not_a_candidate_when_dsn_is_set() {
        let Some((store, retriever)) = vector_world().await else {
            return;
        };
        let owner = scope(&unique(), "support");
        seed_and_index(
            &store,
            &retriever,
            &owner,
            "fact",
            "the launch date is friday",
        )
        .await;

        // The same client, a retriever under an upgraded embedder version:
        // rows derived under v1 are invisible until reindexed under v2.
        let upgraded = PgvectorPrivateMemoryRetriever::from_shared_client(
            retriever.client.clone(),
            Arc::new(
                DeterministicEmbedder::new().with_version(rakka_agent::AgentRevisionNumber::new(2)),
            ),
        );
        let query = MemoryRetrievalQuery::new("the launch date is friday");
        let outcome = upgraded
            .retrieve(&owner, &query, now(200))
            .await
            .expect("retrieval");
        assert!(outcome.memories.is_empty());
        assert_eq!(outcome.index_watermark, None);

        let page = upgraded
            .reindex(&owner, None, now(300), 10)
            .await
            .expect("reindex under the new version");
        assert_eq!(page.indexed, 1);
        let outcome = upgraded
            .retrieve(&owner, &query, now(200))
            .await
            .expect("retrieval");
        assert_eq!(outcome.memories.len(), 1);
        assert!(
            outcome
                .index_watermark
                .as_ref()
                .is_some_and(|watermark| watermark.contains("v2")),
            "the watermark names the embedder version the index serves"
        );
    }

    #[tokio::test]
    async fn pgvector_dimension_mismatch_and_nonfinite_fail_closed_when_dsn_is_set() {
        struct LyingEmbedder(Vec<f32>);
        impl AgentMemoryEmbedder for LyingEmbedder {
            fn embedding_ref(&self) -> MemoryEmbeddingRef {
                MemoryEmbeddingRef {
                    model: "liar".to_string(),
                    dimensions: 8,
                    version: rakka_agent::AgentRevisionNumber::INITIAL,
                }
            }
            fn embed<'a>(&'a self, _text: &'a str) -> MemoryFuture<'a, Vec<f32>> {
                let vector = self.0.clone();
                Box::pin(async move { Ok(vector) })
            }
        }

        let Some((store, _retriever)) = vector_world().await else {
            return;
        };
        let owner = scope(&unique(), "support");
        let record = store
            .upsert(
                &owner,
                &memory(&owner, "fact", "the launch date is friday"),
                PrivateMemoryExpectation::Absent,
            )
            .await
            .expect("seed");

        let client = {
            // A second connection so the lying embedders cannot disturb the
            // shared one.
            let Some(client) = vector_client().await else {
                return;
            };
            Arc::new(client)
        };
        for lying in [
            LyingEmbedder(vec![1.0; 3]),
            LyingEmbedder(vec![f32::NAN; 8]),
        ] {
            let lying =
                PgvectorPrivateMemoryRetriever::from_shared_client(client.clone(), Arc::new(lying));
            let error = lying
                .index_memory(&owner, &record.memory_id, now(100))
                .await
                .expect_err("indexing fails closed");
            assert_eq!(error.code(), "memory-embedding-invalid");
            let error = lying
                .retrieve(&owner, &MemoryRetrievalQuery::new("launch"), now(100))
                .await
                .expect_err("retrieval fails closed");
            assert_eq!(error.code(), "memory-embedding-invalid");
        }
    }

    #[tokio::test]
    async fn pgvector_vector_encoding_round_trips_when_dsn_is_set() {
        // Server-side proof of the text-literal path: a vector compared with
        // its own re-encoding is at distance exactly zero, for awkward f32
        // values included (subnormals, the first integer f32 cannot
        // distinguish from its neighbor, and large magnitudes).
        let Some((_store, retriever)) = vector_world().await else {
            return;
        };
        let client = &retriever.client;
        let awkward = vec![0.1f32, f32::MIN_POSITIVE, 16_777_217.0, -2.5e7, 1.0e-10];
        let encoded = encode_vector(&awkward).expect("encodes");
        let row = client
            .query_one(
                "SELECT ($1::text::vector <-> $1::text::vector) AS l2,
                        ($1::text::vector <=> $2::text::vector) AS cosine_self",
                &[&encoded.as_str(), &encoded.as_str()],
            )
            .await
            .expect("the distance query runs");
        assert_eq!(row.get::<_, f64>("l2"), 0.0);
        assert_eq!(row.get::<_, f64>("cosine_self"), 0.0);
    }

    #[tokio::test]
    async fn pgvector_concurrent_index_writes_converge_when_dsn_is_set() {
        let Some((store, retriever_a)) = vector_world().await else {
            return;
        };
        let Some(client_b) = vector_client().await else {
            return;
        };
        let retriever_b = PgvectorPrivateMemoryRetriever::from_shared_client(
            Arc::new(client_b),
            Arc::new(DeterministicEmbedder::new()),
        );
        let owner = scope(&unique(), "support");
        let record = store
            .upsert(
                &owner,
                &memory(&owner, "fact", "the launch date is friday"),
                PrivateMemoryExpectation::Absent,
            )
            .await
            .expect("seed");

        let (a, b) = tokio::join!(
            retriever_a.index_memory(&owner, &record.memory_id, now(100)),
            retriever_b.index_memory(&owner, &record.memory_id, now(101)),
        );
        a.expect("indexer a");
        b.expect("indexer b");

        let row = retriever_a
            .client
            .query_one(
                "SELECT count(*) AS rows, max(source_revision) AS revision
                 FROM rakka_agent_private_memory_embedding
                 WHERE tenant = $1 AND agent = $2 AND memory_id = $3",
                &[
                    &owner.tenant().as_str(),
                    &owner.agent().as_str(),
                    &record.memory_id.as_str(),
                ],
            )
            .await
            .expect("the count reads");
        assert_eq!(row.get::<_, i64>("rows"), 1, "one derived row per memory");
        assert_eq!(row.get::<_, i64>("revision"), 1);
    }

    #[tokio::test]
    async fn pgvector_classification_filter_applies_before_ranking_when_dsn_is_set() {
        // A sensitive memory sits at distance zero from the query — the
        // nearest possible hit — and still never surfaces under a query that
        // does not allow its classification: the filter is a predicate, not a
        // ranking penalty.
        let Some((store, retriever)) = vector_world().await else {
            return;
        };
        let owner = scope(&unique(), "support");
        let mut nearest = memory(&owner, "sensitive", "the launch date is friday");
        nearest.classification = MemoryClassification::Sensitive;
        let nearest = store
            .upsert(&owner, &nearest, PrivateMemoryExpectation::Absent)
            .await
            .expect("seed sensitive");
        retriever
            .index_memory(&owner, &nearest.memory_id, now(100))
            .await
            .expect("index sensitive");
        seed_and_index(
            &store,
            &retriever,
            &owner,
            "plain",
            "friday launch checklist",
        )
        .await;

        let unclassified_only = MemoryRetrievalQuery::new("the launch date is friday");
        let outcome = retriever
            .retrieve(&owner, &unclassified_only, now(200))
            .await
            .expect("retrieval");
        let names: Vec<&str> = outcome
            .memories
            .iter()
            .map(|retrieved| retrieved.memory.memory_id.as_str())
            .collect();
        assert_eq!(
            names,
            vec!["mem-plain"],
            "the nearer sensitive record never surfaces at any rank"
        );

        let widened = MemoryRetrievalQuery::new("the launch date is friday").with_classifications(
            std::collections::BTreeSet::from([
                MemoryClassification::Unclassified,
                MemoryClassification::Sensitive,
            ]),
        );
        let outcome = retriever
            .retrieve(&owner, &widened, now(200))
            .await
            .expect("retrieval");
        assert_eq!(
            outcome.memories[0].memory.memory_id.as_str(),
            "mem-sensitive",
            "with the classification allowed, the same record ranks first — \
             proving the exclusion above was a filter, not distance"
        );
        assert_eq!(outcome.memories[0].relevance_bps, 10_000);
    }

    #[tokio::test]
    async fn pgvector_confidence_floor_applies_before_ranking_when_dsn_is_set() {
        // The confidence floor is a predicate, not a post-LIMIT drop: a nearer
        // record below the floor must not consume a result slot and leave the
        // retrieval answering short of what the corpus holds.
        let Some((store, retriever)) = vector_world().await else {
            return;
        };
        let owner = scope(&unique(), "support");

        // The exact-match record is *below* the floor; the qualifying record is
        // farther away. A post-LIMIT filter would return nothing at limit 1.
        let mut nearest = memory(&owner, "unsure", "the launch date is friday");
        nearest.confidence_bps = 5_000;
        let nearest = store
            .upsert(&owner, &nearest, PrivateMemoryExpectation::Absent)
            .await
            .expect("seed the low-confidence record");
        retriever
            .index_memory(&owner, &nearest.memory_id, now(100))
            .await
            .expect("index the low-confidence record");
        let farther = seed_and_index(
            &store,
            &retriever,
            &owner,
            "confident",
            "friday launch checklist",
        )
        .await;
        assert_eq!(
            farther.confidence_bps, 9_000,
            "the seeded record is above the floor the query will set"
        );

        let floored = MemoryRetrievalQuery::new("the launch date is friday")
            .with_min_confidence_bps(8_000)
            .with_limit(1);
        let outcome = retriever
            .retrieve(&owner, &floored, now(200))
            .await
            .expect("retrieval");
        let names: Vec<&str> = outcome
            .memories
            .iter()
            .map(|retrieved| retrieved.memory.memory_id.as_str())
            .collect();
        assert_eq!(
            names,
            vec!["mem-confident"],
            "the below-floor record was filtered before ranking, so the single \
             result slot went to the record that qualifies"
        );

        // And the exclusion above was the floor, not distance: drop the floor
        // and the same nearer record ranks first.
        let unfloored = MemoryRetrievalQuery::new("the launch date is friday").with_limit(1);
        let outcome = retriever
            .retrieve(&owner, &unfloored, now(200))
            .await
            .expect("retrieval");
        assert_eq!(
            outcome.memories[0].memory.memory_id.as_str(),
            "mem-unsure",
            "without the floor the low-confidence record is the nearest hit"
        );
        assert_eq!(outcome.memories[0].relevance_bps, 10_000);
    }

    #[tokio::test]
    async fn pgvector_a_zero_magnitude_query_scores_zero_when_dsn_is_set() {
        // A bag-of-words embedder maps text with no tokens onto the zero
        // vector, and pgvector's cosine operator answers NaN against it. The
        // retrieval stays well-defined: the record is still a candidate —
        // every filter passed, only the ranking is undefined — and it scores
        // zero, the same as the reference retriever's zero-magnitude rule.
        let Some((store, retriever)) = vector_world().await else {
            return;
        };
        let owner = scope(&unique(), "support");
        seed_and_index(
            &store,
            &retriever,
            &owner,
            "fact",
            "the launch date is friday",
        )
        .await;

        let tokenless = MemoryRetrievalQuery::new("??? ...");
        let outcome = retriever
            .retrieve(&owner, &tokenless, now(200))
            .await
            .expect("a zero-magnitude query is answered, not refused");
        assert_eq!(
            outcome.memories.len(),
            1,
            "the filters passed, so the record is a candidate at an undefined rank"
        );
        assert_eq!(
            outcome.memories[0].relevance_bps, 0,
            "an undefined distance carries no relevance"
        );
    }

    #[tokio::test]
    async fn pgvector_reindex_pages_past_an_unreadable_record_when_dsn_is_set() {
        // One record this binary cannot decode must not wedge the rebuild
        // path: the cursor advances past it, the sweep completes, and the
        // failure is reported as a value.
        let Some((store, retriever)) = vector_world().await else {
            return;
        };
        let owner = scope(&unique(), "support");
        for name in ["a", "b"] {
            store
                .upsert(
                    &owner,
                    &memory(&owner, name, "friday launch checklist"),
                    PrivateMemoryExpectation::Absent,
                )
                .await
                .expect("seed");
        }

        // Corrupt the first record's stored bytes, the way a record written by
        // a newer binary would read to this one's fail-closed decode.
        let corrupt: &[u8] = b"not a private memory record";
        let updated = retriever
            .client
            .execute(
                "UPDATE rakka_agent_private_memory SET record = $1 \
                 WHERE tenant = $2 AND agent = $3 AND memory_id = $4",
                &[
                    &corrupt,
                    &owner.tenant().as_str(),
                    &owner.agent().as_str(),
                    &"mem-a",
                ],
            )
            .await
            .expect("corrupt the record");
        assert_eq!(updated, 1);

        // Page to completion. The bound is the assertion: a wedged sweep would
        // re-read the same page every time and never reach `next: None`.
        let mut after: Option<AgentPrivateMemoryId> = None;
        let mut indexed = 0u64;
        let mut failed = 0u64;
        let mut pages = 0u32;
        let completed = loop {
            if pages == 8 {
                break false;
            }
            pages += 1;
            let page = retriever
                .reindex(&owner, after.as_ref(), now(300), 1)
                .await
                .expect("one unreadable record never fails the sweep");
            indexed += page.indexed;
            failed += page.failed;
            match page.next {
                Some(next) => after = Some(next),
                None => break true,
            }
        };
        assert!(completed, "the sweep paged past the unreadable record");
        assert_eq!(failed, 1, "the unreadable record is reported, not hidden");
        assert_eq!(indexed, 1, "the readable record behind it was indexed");

        // The readable record is retrievable; the unreadable one has no vector,
        // so it is absent rather than wrong.
        let outcome = retriever
            .retrieve(
                &owner,
                &MemoryRetrievalQuery::new("friday launch checklist"),
                now(400),
            )
            .await
            .expect("retrieval");
        let names: Vec<&str> = outcome
            .memories
            .iter()
            .map(|retrieved| retrieved.memory.memory_id.as_str())
            .collect();
        assert_eq!(names, vec!["mem-b"]);
    }

    /// What a [`PickyEmbedder`] answers for a text it does not embed.
    #[derive(Debug, Clone, Copy)]
    enum Refusal {
        /// An error, as a provider that rejects the input answers.
        Errors,
        /// A vector one component short of what it declares.
        OneComponentShort,
        /// A vector of the declared length with a component that is not a
        /// number.
        NotFinite,
    }

    /// The deterministic embedder, except for the texts that hold `refused`.
    /// It counts its calls, and can declare an identity that is not the
    /// deterministic embedder's.
    struct PickyEmbedder {
        inner: DeterministicEmbedder,
        /// `None` refuses nothing, and `Some("")` refuses everything.
        refused: Option<&'static str>,
        refusal: Refusal,
        calls: std::sync::atomic::AtomicUsize,
        identity: Option<MemoryEmbeddingRef>,
    }

    impl PickyEmbedder {
        fn refusing(refused: Option<&'static str>, refusal: Refusal) -> Self {
            Self {
                inner: DeterministicEmbedder::new(),
                refused,
                refusal,
                calls: std::sync::atomic::AtomicUsize::new(0),
                identity: None,
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl AgentMemoryEmbedder for PickyEmbedder {
        fn embedding_ref(&self) -> MemoryEmbeddingRef {
            self.identity
                .clone()
                .unwrap_or_else(|| self.inner.embedding_ref())
        }

        fn embed<'a>(&'a self, text: &'a str) -> MemoryFuture<'a, Vec<f32>> {
            Box::pin(async move {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if !self.refused.is_some_and(|needle| text.contains(needle)) {
                    return self.inner.embed(text).await;
                }
                match self.refusal {
                    Refusal::Errors => Err(MemoryError::Backend {
                        backend: "picky-embedder".to_string(),
                        message: "the provider refused this input".to_string(),
                    }),
                    Refusal::OneComponentShort => {
                        let mut vector = self.inner.embed(text).await?;
                        vector.pop();
                        Ok(vector)
                    }
                    Refusal::NotFinite => {
                        let mut vector = self.inner.embed(text).await?;
                        vector[0] = f32::NAN;
                        Ok(vector)
                    }
                }
            })
        }
    }

    const EMBEDDABLE: &str = "friday launch checklist";
    const REFUSED: &str = "an unembeddable note";

    /// Seeds one memory per `(name, text)`, none of them indexed.
    async fn seed(
        store: &PostgresAgentPrivateMemoryStore,
        owner: &AgentScope,
        memories: &[(&str, &str)],
    ) {
        for (name, text) in memories {
            store
                .upsert(
                    owner,
                    &memory(owner, name, text),
                    PrivateMemoryExpectation::Absent,
                )
                .await
                .expect("seed");
        }
    }

    /// The memory ids of one agent that hold a derived row, in id order.
    async fn embedded(
        retriever: &PgvectorPrivateMemoryRetriever,
        owner: &AgentScope,
    ) -> Vec<String> {
        retriever
            .client
            .query(
                "SELECT memory_id FROM rakka_agent_private_memory_embedding \
                 WHERE tenant = $1 AND agent = $2 ORDER BY memory_id",
                &[&owner.tenant().as_str(), &owner.agent().as_str()],
            )
            .await
            .expect("read the derived rows")
            .iter()
            .map(|row| row.get::<_, String>("memory_id"))
            .collect()
    }

    /// Writes a copy of `source`'s row under `raw_id` past the store, as a
    /// later build with a wider id alphabet would: an id this build refuses.
    /// `tombstoned` writes the copy withdrawn.
    async fn plant_refused_id(
        retriever: &PgvectorPrivateMemoryRetriever,
        owner: &AgentScope,
        source: &str,
        raw_id: &str,
        tombstoned: bool,
    ) {
        assert!(
            AgentPrivateMemoryId::new(raw_id).is_err(),
            "{raw_id} is an id this build refuses"
        );
        let planted = retriever
            .client
            .execute(
                "INSERT INTO rakka_agent_private_memory
                     (tenant, agent, memory_id, operation_id, revision, expires_at,
                      tombstoned, record)
                 SELECT tenant, agent, $3, operation_id || $3, revision, expires_at,
                        $5, record
                 FROM rakka_agent_private_memory
                 WHERE tenant = $1 AND agent = $2 AND memory_id = $4",
                &[
                    &owner.tenant().as_str(),
                    &owner.agent().as_str(),
                    &raw_id,
                    &source,
                    &tombstoned,
                ],
            )
            .await
            .expect("plant the row");
        assert_eq!(planted, 1, "one copy of {source} is planted as {raw_id}");
    }

    /// The memory ids of one agent in the database's own order.
    async fn server_order(
        retriever: &PgvectorPrivateMemoryRetriever,
        owner: &AgentScope,
    ) -> Vec<String> {
        retriever
            .client
            .query(
                "SELECT memory_id FROM rakka_agent_private_memory \
                 WHERE tenant = $1 AND agent = $2 ORDER BY memory_id",
                &[&owner.tenant().as_str(), &owner.agent().as_str()],
            )
            .await
            .expect("read the memory ids")
            .iter()
            .map(|row| row.get::<_, String>("memory_id"))
            .collect()
    }

    /// One page as `(indexed, skipped, failed, next)`.
    type PageCounts = (u64, u64, u64, Option<String>);

    fn counts(page: &ReindexPage) -> PageCounts {
        (
            page.indexed,
            page.skipped,
            page.failed,
            page.next.as_ref().map(|id| id.as_str().to_string()),
        )
    }

    fn page(indexed: u64, skipped: u64, failed: u64, next: Option<&str>) -> PageCounts {
        (indexed, skipped, failed, next.map(str::to_string))
    }

    /// Pages `reindex` from the start, `limit` at a time, following `next`
    /// until it is `None`, and answers each page. A sweep that does not end
    /// within sixteen pages is held on one, and fails here.
    async fn sweep(
        retriever: &PgvectorPrivateMemoryRetriever,
        owner: &AgentScope,
        limit: usize,
    ) -> Result<Vec<PageCounts>, MemoryError> {
        let mut after: Option<AgentPrivateMemoryId> = None;
        let mut pages = Vec::new();
        for _ in 0..16 {
            let page = retriever
                .reindex(owner, after.as_ref(), now(300), limit)
                .await?;
            pages.push(counts(&page));
            match page.next {
                Some(next) => after = Some(next),
                None => return Ok(pages),
            }
        }
        panic!("the sweep did not end within sixteen pages: {pages:?}");
    }

    #[tokio::test]
    async fn pgvector_reindex_pages_past_a_memory_its_embedder_refuses_when_dsn_is_set() {
        // Issue #151: one memory the embedder refuses must not stop the pass,
        // whichever way it refuses: an error, a vector of the wrong length,
        // or a component that is not a number. It used to end the call with
        // the embedder's error and no page, so the caller had no cursor to go
        // on from, and every retry met the same memory first.
        for refusal in [
            Refusal::Errors,
            Refusal::OneComponentShort,
            Refusal::NotFinite,
        ] {
            let embedder = Arc::new(PickyEmbedder::refusing(Some("unembeddable"), refusal));
            let Some((store, retriever)) = vector_world_over(embedder).await else {
                return;
            };
            let owner = scope(&unique(), "support");
            // The refused memory sorts first by id, so it is the first row of
            // the first page.
            seed(
                &store,
                &owner,
                &[("a", REFUSED), ("b", EMBEDDABLE), ("c", EMBEDDABLE)],
            )
            .await;

            let pages = sweep(&retriever, &owner, 1).await.unwrap_or_else(|error| {
                panic!("{refusal:?}: one refused memory never fails the sweep: {error}")
            });
            assert_eq!(
                pages,
                vec![
                    page(0, 0, 1, Some("mem-a")),
                    page(1, 0, 0, Some("mem-b")),
                    page(1, 0, 0, Some("mem-c")),
                    page(0, 0, 0, None),
                ],
                "{refusal:?}: the two memories behind the refused one were embedded"
            );
            assert_eq!(
                embedded(&retriever, &owner).await,
                vec!["mem-b", "mem-c"],
                "{refusal:?}"
            );

            // The refused memory still has no vector, so the next sweep reads
            // it again and nothing else.
            let again = sweep(&retriever, &owner, 1)
                .await
                .expect("the second sweep ends too");
            assert_eq!(
                again,
                vec![page(0, 0, 1, Some("mem-a")), page(0, 0, 0, None)],
                "{refusal:?}: a rerun reads exactly what is still missing"
            );
        }
    }

    #[tokio::test]
    async fn pgvector_reindex_embeds_what_follows_a_refused_memory_in_its_page_when_dsn_is_set() {
        // The same within one page: a refusal counts that memory and no
        // other. A pass that gave up on the rest of a page at its first
        // refusal would embed nothing behind one for any page size above
        // one.
        let embedder = Arc::new(PickyEmbedder::refusing(
            Some("unembeddable"),
            Refusal::Errors,
        ));
        let Some((store, retriever)) = vector_world_over(embedder.clone()).await else {
            return;
        };
        let owner = scope(&unique(), "support");
        seed(
            &store,
            &owner,
            &[("a", EMBEDDABLE), ("b", REFUSED), ("c", EMBEDDABLE)],
        )
        .await;

        let pages = sweep(&retriever, &owner, 3).await.expect("the sweep ends");
        assert_eq!(
            pages,
            vec![page(2, 0, 1, Some("mem-c")), page(0, 0, 0, None)]
        );
        assert_eq!(embedded(&retriever, &owner).await, vec!["mem-a", "mem-c"]);
        assert_eq!(
            embedder.calls(),
            3,
            "one call for each memory of the page; the cursor is then past them all"
        );
    }

    #[tokio::test]
    async fn pgvector_reindex_through_an_embedder_outage_counts_every_memory_when_dsn_is_set() {
        // What issue #151's fix costs: an embedder that refuses everything is
        // not an error either. Each page counts every memory it tried, at one
        // call each, and the sweep ends.
        let embedder = Arc::new(PickyEmbedder::refusing(Some(""), Refusal::Errors));
        let Some((store, retriever)) = vector_world_over(embedder.clone()).await else {
            return;
        };
        let owner = scope(&unique(), "support");
        seed(
            &store,
            &owner,
            &[("a", EMBEDDABLE), ("b", EMBEDDABLE), ("c", EMBEDDABLE)],
        )
        .await;

        let pages = sweep(&retriever, &owner, 2)
            .await
            .expect("an outage is counted, not returned");
        assert_eq!(
            pages,
            vec![page(0, 0, 2, Some("mem-b")), page(0, 0, 1, None)]
        );
        assert_eq!(embedder.calls(), 3, "one failing call for each memory");
        assert!(embedded(&retriever, &owner).await.is_empty());
    }

    #[tokio::test]
    async fn pgvector_reindex_reads_past_ids_this_build_refuses_when_dsn_is_set() {
        // Issue #200's rows: a page made only of a refused id used to answer
        // no cursor, the answer of a finished sweep, which ended the sweep
        // before `mem-d` and `mem-e`.
        for limit in [1, 2] {
            let embedder = Arc::new(PickyEmbedder::refusing(None, Refusal::Errors));
            let Some((store, retriever)) = vector_world_over(embedder.clone()).await else {
                return;
            };
            let owner = scope(&unique(), "support");
            seed(
                &store,
                &owner,
                &[
                    ("a", EMBEDDABLE),
                    ("b", EMBEDDABLE),
                    ("d", EMBEDDABLE),
                    ("e", EMBEDDABLE),
                ],
            )
            .await;
            plant_refused_id(&retriever, &owner, "mem-b", "mem-c|x", false).await;
            assert_eq!(
                server_order(&retriever, &owner).await,
                ["mem-a", "mem-b", "mem-c|x", "mem-d", "mem-e"]
            );

            let expected = if limit == 1 {
                vec![
                    page(1, 0, 0, Some("mem-a")),
                    page(1, 0, 0, Some("mem-b")),
                    page(1, 0, 1, Some("mem-d")),
                    page(1, 0, 0, Some("mem-e")),
                    page(0, 0, 0, None),
                ]
            } else {
                vec![
                    page(2, 0, 0, Some("mem-b")),
                    page(2, 0, 1, Some("mem-e")),
                    page(0, 0, 0, None),
                ]
            };
            assert_eq!(
                sweep(&retriever, &owner, limit)
                    .await
                    .expect("the sweep ends"),
                expected,
                "limit {limit}"
            );
            assert_eq!(
                embedded(&retriever, &owner).await,
                ["mem-a", "mem-b", "mem-d", "mem-e"],
                "limit {limit}"
            );
            assert_eq!(
                embedder.calls(),
                4,
                "limit {limit}: no model call for the refused id"
            );
            // Each refused row is counted once a sweep.
            assert_eq!(
                sweep(&retriever, &owner, limit)
                    .await
                    .expect("the second sweep ends"),
                vec![page(0, 0, 1, None)],
                "limit {limit}"
            );
        }
    }

    /// One arrangement of refused ids for
    /// `pgvector_reindex_reads_past_every_arrangement_of_refused_ids_when_dsn_is_set`.
    struct RefusedIdCase {
        name: &'static str,
        /// Memories seeded through the store, embeddable.
        seeded: &'static [&'static str],
        /// A memory seeded through the store with its classification
        /// `Redacted`, which the pass skips by policy.
        redacted: Option<&'static str>,
        /// Rows planted past the store: `(source id, raw id, tombstoned)`.
        planted: &'static [(&'static str, &'static str, bool)],
        limit: usize,
        order: &'static [&'static str],
        pages: Vec<PageCounts>,
        embedded: &'static [&'static str],
        calls: usize,
    }

    #[tokio::test]
    async fn pgvector_reindex_reads_past_every_arrangement_of_refused_ids_when_dsn_is_set() {
        let cases = [
            RefusedIdCase {
                name: "pair",
                seeded: &["a", "b", "e"],
                redacted: None,
                planted: &[("mem-b", "mem-c|1", false), ("mem-b", "mem-c|2", false)],
                limit: 2,
                order: &["mem-a", "mem-b", "mem-c|1", "mem-c|2", "mem-e"],
                pages: vec![page(2, 0, 0, Some("mem-b")), page(1, 0, 2, None)],
                embedded: &["mem-a", "mem-b", "mem-e"],
                calls: 3,
            },
            RefusedIdCase {
                name: "first",
                seeded: &["b", "c"],
                redacted: None,
                planted: &[("mem-b", "mem-a|x", false)],
                limit: 1,
                order: &["mem-a|x", "mem-b", "mem-c"],
                pages: vec![
                    page(1, 0, 1, Some("mem-b")),
                    page(1, 0, 0, Some("mem-c")),
                    page(0, 0, 0, None),
                ],
                embedded: &["mem-b", "mem-c"],
                calls: 2,
            },
            RefusedIdCase {
                name: "last",
                seeded: &["a", "b"],
                redacted: None,
                planted: &[("mem-a", "mem-z|x", false)],
                limit: 1,
                order: &["mem-a", "mem-b", "mem-z|x"],
                pages: vec![
                    page(1, 0, 0, Some("mem-a")),
                    page(1, 0, 0, Some("mem-b")),
                    page(0, 0, 1, None),
                ],
                embedded: &["mem-a", "mem-b"],
                calls: 2,
            },
            RefusedIdCase {
                name: "run",
                seeded: &["a", "d"],
                redacted: None,
                planted: &[
                    ("mem-a", "mem-c|1", false),
                    ("mem-a", "mem-c|2", false),
                    ("mem-a", "mem-c|3", false),
                    ("mem-a", "mem-c|4", false),
                    ("mem-a", "mem-c|5", false),
                ],
                limit: 1,
                order: &[
                    "mem-a", "mem-c|1", "mem-c|2", "mem-c|3", "mem-c|4", "mem-c|5", "mem-d",
                ],
                pages: vec![
                    page(1, 0, 0, Some("mem-a")),
                    page(1, 0, 5, Some("mem-d")),
                    page(0, 0, 0, None),
                ],
                embedded: &["mem-a", "mem-d"],
                calls: 2,
            },
            RefusedIdCase {
                name: "withdrawn",
                seeded: &["a", "b"],
                redacted: None,
                planted: &[("mem-a", "mem-a|x", true)],
                limit: 1,
                order: &["mem-a", "mem-a|x", "mem-b"],
                pages: vec![
                    page(1, 0, 0, Some("mem-a")),
                    page(1, 0, 0, Some("mem-b")),
                    page(0, 0, 0, None),
                ],
                embedded: &["mem-a", "mem-b"],
                calls: 2,
            },
            RefusedIdCase {
                name: "redacted",
                seeded: &["a", "e"],
                redacted: Some("d"),
                planted: &[("mem-a", "mem-c|1", false), ("mem-a", "mem-c|2", false)],
                limit: 1,
                order: &["mem-a", "mem-c|1", "mem-c|2", "mem-d", "mem-e"],
                pages: vec![
                    page(1, 0, 0, Some("mem-a")),
                    page(0, 1, 2, Some("mem-d")),
                    page(1, 0, 0, Some("mem-e")),
                    page(0, 0, 0, None),
                ],
                embedded: &["mem-a", "mem-e"],
                calls: 2,
            },
        ];
        for case in cases {
            let name = case.name;
            let embedder = Arc::new(PickyEmbedder::refusing(None, Refusal::Errors));
            let Some((store, retriever)) = vector_world_over(embedder.clone()).await else {
                return;
            };
            let owner = scope(&unique(), "support");
            let seeded: Vec<(&str, &str)> = case
                .seeded
                .iter()
                .map(|seeded| (*seeded, EMBEDDABLE))
                .collect();
            seed(&store, &owner, &seeded).await;
            if let Some(redacted) = case.redacted {
                let mut withheld = memory(&owner, redacted, EMBEDDABLE);
                withheld.classification = MemoryClassification::Redacted;
                store
                    .upsert(&owner, &withheld, PrivateMemoryExpectation::Absent)
                    .await
                    .expect("seed the redacted memory");
            }
            for (source, raw_id, tombstoned) in case.planted {
                plant_refused_id(&retriever, &owner, source, raw_id, *tombstoned).await;
            }
            assert_eq!(server_order(&retriever, &owner).await, case.order, "{name}");

            assert_eq!(
                sweep(&retriever, &owner, case.limit)
                    .await
                    .unwrap_or_else(|error| panic!("{name}: the sweep ends: {error}")),
                case.pages,
                "{name}"
            );
            assert_eq!(embedded(&retriever, &owner).await, case.embedded, "{name}");
            assert_eq!(
                embedder.calls(),
                case.calls,
                "{name}: one model call per readable memory the pass embeds"
            );
        }
    }

    #[test]
    fn a_write_error_is_the_rows_own_data_only_for_the_shipped_schemas_codes() {
        const SOURCE_REVISION: &str = "rakka_agent_private_memory_embedding_source_revision_check";
        const CONFIDENCE: &str = "rakka_agent_private_memory_embedding_confidence_bps_check";
        // What the shipped schema raises for a memory's own data: a NUL or a
        // character the server encoding cannot hold in its digest, and the
        // two CHECKs on values taken from the memory.
        for (code, constraint) in [
            ("22021", None),
            ("22P05", None),
            ("23514", Some(SOURCE_REVISION)),
            ("23514", Some(CONFIDENCE)),
        ] {
            assert!(
                is_row_data_refusal(code, constraint),
                "{code} {constraint:?}"
            );
        }
        // Everything else, of any class: pgvector's dimension cast in a
        // deployment's HNSW expression index (22000), a deployment's own
        // CHECK, the CHECKs on the embedder's identity, and every class that
        // says nothing about a row.
        for (code, constraint) in [
            ("22000", None),
            ("22003", None),
            ("22P02", None),
            ("23514", None),
            ("23514", Some("a_deployment_check")),
            (
                "23514",
                Some("rakka_agent_private_memory_embedding_version_check"),
            ),
            ("23514", Some("rakka_agent_private_memory_embedding_check")),
            ("23502", None),
            ("23505", None),
            ("", None),
            ("21000", None),
            ("24000", None),
            ("08006", None),
            ("40001", None),
            ("42501", None),
            ("57014", None),
            ("XX000", None),
        ] {
            assert!(
                !is_row_data_refusal(code, constraint),
                "{code} {constraint:?}"
            );
        }
    }

    /// Raises `sqlstate`, naming `constraint` when one is given, on every
    /// write of `memory_id`'s derived row in this test's own tenant, through
    /// a function and a trigger named for the tenant, so no other test's
    /// writes meet it. Answers the SQL that removes both; run it before
    /// asserting, so a failed assertion leaves nothing behind.
    async fn raise_on_derived_write(
        retriever: &PgvectorPrivateMemoryRetriever,
        owner: &AgentScope,
        memory_id: &str,
        sqlstate: &str,
        constraint: Option<&str>,
    ) -> String {
        // `unique()` makes the tenant `t<nanos>`: safe as an identifier and
        // as a literal.
        let tenant = owner.tenant().as_str();
        let name = format!("rakka_test_raise_{tenant}");
        retriever
            .client
            .batch_execute(&format!(
                "CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$
                 BEGIN
                     IF TG_ARGV[1] = '' THEN
                         RAISE EXCEPTION 'raised by a test' USING ERRCODE = TG_ARGV[0];
                     ELSE
                         RAISE EXCEPTION 'raised by a test'
                             USING ERRCODE = TG_ARGV[0], CONSTRAINT = TG_ARGV[1];
                     END IF;
                 END $$;
                 CREATE TRIGGER {name}
                     BEFORE INSERT OR UPDATE ON rakka_agent_private_memory_embedding
                     FOR EACH ROW
                     WHEN (NEW.tenant = '{tenant}' AND NEW.memory_id = '{memory_id}')
                     EXECUTE FUNCTION {name}('{sqlstate}', '{constraint}');",
                constraint = constraint.unwrap_or("")
            ))
            .await
            .expect("install the raising trigger");
        format!(
            "DROP TRIGGER {name} ON rakka_agent_private_memory_embedding; \
             DROP FUNCTION {name}();"
        )
    }

    #[tokio::test]
    async fn pgvector_reindex_counts_only_the_shipped_schemas_row_refusals_when_dsn_is_set() {
        // Issue #199: a write the shipped schema refuses for the memory's own
        // data is counted and paged past. Every other write failure still
        // ends the call, whatever its class, and what the call stored before
        // it stays: a deployment's own constraint or index can refuse every
        // write, and counting that per memory would hide it.
        const SOURCE_REVISION: &str = "rakka_agent_private_memory_embedding_source_revision_check";
        let Some((_, retriever)) = vector_world().await else {
            return;
        };
        let checks: Vec<String> = retriever
            .client
            .query(
                "SELECT conname::text FROM pg_constraint \
                 WHERE conrelid = 'rakka_agent_private_memory_embedding'::regclass \
                   AND contype = 'c'",
                &[],
            )
            .await
            .expect("read the table's checks")
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect();
        for name in ROW_DATA_CHECKS {
            assert!(
                checks.iter().any(|check| check == name),
                "{name} is a check of the migrated table: {checks:?}"
            );
        }
        for (sqlstate, constraint, counted) in [
            ("22021", None, true),
            ("22P05", None, true),
            ("23514", Some(SOURCE_REVISION), true),
            ("22000", None, false),
            ("22003", None, false),
            ("23514", None, false),
            ("23514", Some("a_deployment_check"), false),
            ("23505", None, false),
            ("21000", None, false),
            ("24000", None, false),
            ("40001", None, false),
            ("42501", None, false),
            ("57014", None, false),
            ("XX000", None, false),
        ] {
            let Some((store, retriever)) = vector_world().await else {
                return;
            };
            let owner = scope(&unique(), "support");
            seed(
                &store,
                &owner,
                &[("a", EMBEDDABLE), ("b", EMBEDDABLE), ("c", EMBEDDABLE)],
            )
            .await;
            let cleanup =
                raise_on_derived_write(&retriever, &owner, "mem-b", sqlstate, constraint).await;
            let answer = retriever.reindex(&owner, None, now(300), 10).await;
            retriever
                .client
                .batch_execute(&cleanup)
                .await
                .expect("remove the raising trigger");
            if counted {
                let answer = answer.unwrap_or_else(|error| {
                    panic!("{sqlstate} {constraint:?}: counted, not returned: {error}")
                });
                assert_eq!(
                    counts(&answer),
                    page(2, 0, 1, None),
                    "{sqlstate} {constraint:?}"
                );
                assert_eq!(
                    embedded(&retriever, &owner).await,
                    ["mem-a", "mem-c"],
                    "{sqlstate} {constraint:?}"
                );
            } else {
                let error = answer
                    .map(|page| counts(&page))
                    .expect_err("returned, not counted");
                assert_eq!(
                    error.code(),
                    "memory-backend-failed",
                    "{sqlstate} {constraint:?}"
                );
                assert_eq!(
                    embedded(&retriever, &owner).await,
                    ["mem-a"],
                    "{sqlstate} {constraint:?}: what was stored before stays, and nothing after it is tried"
                );
            }
        }
    }

    #[tokio::test]
    async fn pgvector_reindex_returns_a_dimension_cast_refused_by_a_deployments_hnsw_index_when_dsn_is_set(
    ) {
        // The approximate index the module documentation offers casts every
        // vector to a fixed dimension count, and pgvector refuses an embedder
        // of another count with SQLSTATE 22000, on every write. That says
        // nothing about one memory, so the sweep returns it, after one model
        // call, as before issue #199, instead of counting every memory after
        // a model call each. The index is partial on this test's own tenant,
        // so no other test's writes meet it.
        let embedder = Arc::new(PickyEmbedder::refusing(None, Refusal::Errors));
        let Some((store, retriever)) = vector_world_over(embedder.clone()).await else {
            return;
        };
        let owner = scope(&unique(), "support");
        seed(
            &store,
            &owner,
            &[("a", EMBEDDABLE), ("b", EMBEDDABLE), ("c", EMBEDDABLE)],
        )
        .await;
        let tenant = owner.tenant().as_str();
        let index = format!("rakka_test_hnsw_{tenant}");
        retriever
            .client
            .batch_execute(&format!(
                "CREATE INDEX {index} ON rakka_agent_private_memory_embedding
                     USING hnsw ((embedding::vector(1536)) vector_cosine_ops)
                     WHERE tenant = '{tenant}'"
            ))
            .await
            .expect("create the deployment's index");
        let answer = retriever.reindex(&owner, None, now(300), 10).await;
        let single = retriever
            .index_memory(
                &owner,
                &AgentPrivateMemoryId::new("mem-a").expect("id"),
                now(300),
            )
            .await;
        retriever
            .client
            .batch_execute(&format!("DROP INDEX {index}"))
            .await
            .expect("drop the deployment's index");

        let error = answer
            .map(|page| counts(&page))
            .expect_err("a refusal of every write is returned, not counted");
        assert_eq!(error.code(), "memory-backend-failed");
        assert_eq!(
            embedder.calls(),
            2,
            "one model call for the sweep, one for index_memory"
        );
        let error = single.expect_err("index_memory names the cause");
        assert!(
            error.to_string().contains("expected 1536 dimensions"),
            "{error}"
        );
        assert!(embedded(&retriever, &owner).await.is_empty());
    }

    #[tokio::test]
    async fn pgvector_reindex_pages_past_a_memory_the_derived_table_refuses_when_dsn_is_set() {
        // Issue #199: a NUL in the digest is kept by the store, inside the
        // record's bytes, and refused by the derived table's text column
        // (SQLSTATE 22021). It used to hold every sweep on that memory.
        let Some((store, retriever)) = vector_world().await else {
            return;
        };
        let seed_three = |owner: AgentScope| {
            let store = &store;
            async move {
                seed(store, &owner, &[("a", EMBEDDABLE)]).await;
                let mut unwritable = memory(&owner, "b", EMBEDDABLE);
                unwritable.content_digest.value = "00\u{0}00".to_string();
                store
                    .upsert(&owner, &unwritable, PrivateMemoryExpectation::Absent)
                    .await
                    .expect("the store accepts the record");
                seed(store, &owner, &[("c", EMBEDDABLE)]).await;
                owner
            }
        };

        // In a sweep of pages of one.
        let owner = seed_three(scope(&unique(), "support")).await;
        assert_eq!(
            sweep(&retriever, &owner, 1)
                .await
                .expect("a refused write never fails the sweep"),
            vec![
                page(1, 0, 0, Some("mem-a")),
                page(0, 0, 1, Some("mem-b")),
                page(1, 0, 0, Some("mem-c")),
                page(0, 0, 0, None),
            ]
        );
        assert_eq!(embedded(&retriever, &owner).await, ["mem-a", "mem-c"]);
        assert_eq!(
            sweep(&retriever, &owner, 1)
                .await
                .expect("the second sweep ends"),
            vec![page(0, 0, 1, Some("mem-b")), page(0, 0, 0, None)],
            "a rerun reads exactly what is still missing"
        );

        // In the middle of one page.
        let other = seed_three(scope(&unique(), "support")).await;
        assert_eq!(
            sweep(&retriever, &other, 10).await.expect("the sweep ends"),
            vec![page(2, 0, 1, None)]
        );
        assert_eq!(embedded(&retriever, &other).await, ["mem-a", "mem-c"]);

        // Indexing that one memory still answers why it fails.
        let error = retriever
            .index_memory(
                &owner,
                &AgentPrivateMemoryId::new("mem-b").expect("id"),
                now(300),
            )
            .await
            .expect_err("one memory's refused write is still an error");
        assert_eq!(error.code(), "memory-backend-failed");
    }

    #[tokio::test]
    async fn pgvector_an_identity_the_table_cannot_store_is_refused_before_the_model_when_dsn_is_set(
    ) {
        // An embedder version of 0 and a model holding a NUL fail the
        // derived table's write for every memory, so they are refused before
        // the model is called, by every path that takes the identity.
        let declared = DeterministicEmbedder::new().embedding_ref();
        for identity in [
            MemoryEmbeddingRef {
                version: AgentRevisionNumber::new(0),
                ..declared.clone()
            },
            MemoryEmbeddingRef {
                model: "model\u{0}name".to_string(),
                ..declared.clone()
            },
        ] {
            let mut embedder = PickyEmbedder::refusing(None, Refusal::Errors);
            embedder.identity = Some(identity.clone());
            let embedder = Arc::new(embedder);
            let Some((store, retriever)) = vector_world_over(embedder.clone()).await else {
                return;
            };
            let owner = scope(&unique(), "support");
            seed(&store, &owner, &[("a", EMBEDDABLE)]).await;
            let id = AgentPrivateMemoryId::new("mem-a").expect("id");

            let error = retriever
                .index_memory(&owner, &id, now(100))
                .await
                .expect_err("index_memory refuses the identity");
            assert_eq!(error.code(), "memory-embedding-invalid", "{identity:?}");
            let error = retriever
                .reindex(&owner, None, now(100), 10)
                .await
                .expect_err("reindex refuses the identity");
            assert_eq!(error.code(), "memory-embedding-invalid", "{identity:?}");
            let error = retriever
                .retrieve(&owner, &MemoryRetrievalQuery::new(EMBEDDABLE), now(100))
                .await
                .expect_err("retrieve refuses the identity");
            assert_eq!(error.code(), "memory-embedding-invalid", "{identity:?}");
            assert_eq!(
                embedder.calls(),
                0,
                "{identity:?}: the model was never called"
            );
            assert!(
                embedded(&retriever, &owner).await.is_empty(),
                "{identity:?}"
            );
        }

        // Only a NUL is refused: another control character is a model name
        // the table stores.
        let mut embedder = PickyEmbedder::refusing(None, Refusal::Errors);
        embedder.identity = Some(MemoryEmbeddingRef {
            model: "model\tname".to_string(),
            ..declared
        });
        let Some((store, retriever)) = vector_world_over(Arc::new(embedder)).await else {
            return;
        };
        let owner = scope(&unique(), "support");
        seed(&store, &owner, &[("a", EMBEDDABLE)]).await;
        let outcome = retriever
            .index_memory(
                &owner,
                &AgentPrivateMemoryId::new("mem-a").expect("id"),
                now(100),
            )
            .await
            .expect("a tab in the model name is stored");
        assert!(
            matches!(outcome, IndexOutcome::Indexed { .. }),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn pgvector_index_memory_refuses_an_unstorable_identity_before_the_model_when_dsn_is_set()
    {
        // Indexing one memory is as it was: an embedder identity that cannot
        // be stored is refused before the memory's text is sent anywhere,
        // and a failed embedding is still an error.
        let mut embedder = PickyEmbedder::refusing(None, Refusal::Errors);
        embedder.identity = Some(MemoryEmbeddingRef {
            model: "too-wide".to_string(),
            dimensions: PGVECTOR_MAX_DIMENSIONS + 1,
            version: AgentRevisionNumber::INITIAL,
        });
        let embedder = Arc::new(embedder);
        let Some((store, retriever)) = vector_world_over(embedder.clone()).await else {
            return;
        };
        let owner = scope(&unique(), "support");
        seed(&store, &owner, &[("a", EMBEDDABLE)]).await;
        let id = AgentPrivateMemoryId::new("mem-a").expect("id");

        let error = retriever
            .index_memory(&owner, &id, now(100))
            .await
            .expect_err("an identity above the ceiling is refused");
        assert_eq!(error.code(), "memory-embedding-invalid");
        assert_eq!(embedder.calls(), 0, "the model was not called");
        let error = retriever
            .reindex(&owner, None, now(100), 10)
            .await
            .expect_err("the pass refuses it too, before it reads a page");
        assert_eq!(error.code(), "memory-embedding-invalid");
        assert_eq!(embedder.calls(), 0);

        let refusing = Arc::new(PickyEmbedder::refusing(Some(""), Refusal::Errors));
        let Some((_, retriever)) = vector_world_over(refusing).await else {
            return;
        };
        let error = retriever
            .index_memory(&owner, &id, now(100))
            .await
            .expect_err("one memory's failed embedding is still an error");
        assert_eq!(error.code(), "memory-backend-failed");
    }
}
