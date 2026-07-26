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
//!   ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
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
    /// How many candidates this page could not interpret — a stored record
    /// this binary cannot decode, or one its schema policy refuses.
    ///
    /// These are counted and paged past rather than failing the sweep, so one
    /// unreadable record cannot stall a rebuild (see
    /// [`PgvectorPrivateMemoryRetriever::reindex`]). A non-zero count is an
    /// operational signal: the records behind it have no current vector and
    /// are therefore unretrievable until whatever cannot read them is fixed.
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
    /// validated against the pgvector dimension ceiling.
    fn embedder_identity(&self) -> Result<(MemoryEmbeddingRef, i32, i64), MemoryError> {
        let reference = self.embedder.embedding_ref();
        if reference.model.is_empty() || reference.dimensions == 0 {
            return Err(MemoryError::InvalidEmbeddingRef {
                message: "the embedder declares an empty model or zero dimensions".to_string(),
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

    /// Derives and stores one memory's vector, upserting the derived row
    /// keyed to the authoritative revision it was derived from.
    async fn index_decoded(
        &self,
        scope: &AgentScope,
        memory: &AgentPrivateMemory,
        text: &str,
        now: AgentTimestampMillis,
    ) -> Result<IndexOutcome, MemoryError> {
        let (reference, dimensions, version) = self.embedder_identity()?;
        let vector = self.embed_checked(text).await?;
        let encoded = encode_vector(&vector)?;
        let source_revision =
            i64::try_from(memory.revision.get()).map_err(|_| MemoryError::Backend {
                backend: BACKEND_NAME.to_string(),
                message: "the memory revision exceeds the storable range".to_string(),
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
            .map_err(map_error)?;
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
    /// Returns [`MemoryError::Backend`] on SQL failure,
    /// [`MemoryError::InvalidEmbeddingRef`] when the embedder's output does
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
    /// identical retrieval. Bounded, idempotent, deployment-invoked — page
    /// with the returned cursor until [`ReindexPage::next`] is `None`.
    ///
    /// # A single unreadable record cannot stall the rebuild
    ///
    /// The cursor advances on each row's own id before anything fallible runs,
    /// and a record this binary cannot decode — or whose schema version its
    /// fail-closed policy refuses, which is exactly what a rolling upgrade
    /// produces when a newer node has written a newer record shape — is
    /// counted into [`ReindexPage::failed`] and paged past. Propagating it
    /// instead would wedge the sweep permanently: the caller's cursor would
    /// never advance beyond the offending row, so every retry would re-read
    /// the same page and fail identically, and the rebuild path would be
    /// unavailable for that agent until the record was repaired by hand.
    ///
    /// # Errors
    ///
    /// Returns the same errors as
    /// [`PgvectorPrivateMemoryRetriever::index_memory`] — SQL failures and
    /// embedder-identity mismatches, which are conditions of the deployment
    /// rather than of one record, so retrying the same page is the correct
    /// response to them.
    pub async fn reindex(
        &self,
        scope: &AgentScope,
        after: Option<&AgentPrivateMemoryId>,
        now: AgentTimestampMillis,
        limit: usize,
    ) -> Result<ReindexPage, MemoryError> {
        let (reference, dimensions, version) = self.embedder_identity()?;
        let now_raw = i64::try_from(now.as_millis()).unwrap_or(i64::MAX);
        let after_raw = after.map_or(String::new(), |id| id.as_str().to_string());
        let limit = limit.max(1);
        let limit_raw = i64::try_from(limit).unwrap_or(i64::MAX);

        let rows = self
            .client
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
                    &after_raw.as_str(),
                    &limit_raw,
                ],
            )
            .await
            .map_err(map_error)?;

        let more_may_remain = rows.len() == limit;
        let mut indexed = 0u64;
        let mut skipped = 0u64;
        let mut failed = 0u64;
        let mut last: Option<AgentPrivateMemoryId> = None;
        for row in rows {
            // The cursor advances on the row's own id, ahead of every fallible
            // step, so no single record can hold the sweep on this page.
            let Ok(memory_id) = AgentPrivateMemoryId::new(row.get::<_, &str>("memory_id")) else {
                failed += 1;
                continue;
            };
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
            self.index_decoded(scope, &memory, &text, now).await?;
            indexed += 1;
        }
        Ok(ReindexPage {
            indexed,
            skipped,
            failed,
            next: if more_may_remain { last } else { None },
        })
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
        let client = Arc::new(vector_client().await?);
        let store = PostgresAgentPrivateMemoryStore::from_shared_client(client.clone());
        store.migrate().await.expect("base migration");
        let retriever = PgvectorPrivateMemoryRetriever::from_shared_client(
            client,
            Arc::new(DeterministicEmbedder::new()),
        );
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
}
