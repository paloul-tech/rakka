//! Meaning-based search: a claim's embedding as four columns on its row.
//!
//! Opt-in. A store built with
//! [`PostgresKnowledgeGraphStore::with_semantic_search`] reports
//! `semantic-search`, and its `migrate` also applies
//! [`SEMANTIC_MIGRATION_SQL`], which needs the `vector` extension (pgvector).
//! A store built without it never names the extension, the columns or the
//! operators, so the base schema keeps working on a database that has no
//! pgvector.
//!
//! # The embedding is on the claim row
//!
//! `embedding` holds the vector, and `embedding_model`,
//! `embedding_dimensions` and `embedding_version` hold the reference it was
//! produced under. A search filters on the columns the word index already
//! keeps on the same row and ranks by cosine distance in one statement, with
//! no join. The vector column has no fixed dimension, so a deployment can
//! change its embedding model without a schema change; a check holds each
//! stored vector to the dimension count beside it.
//!
//! # An embedding never outlives its claim's content
//!
//! - The write ([`CLAIM_EMBEDDING_WRITE_SQL`]) changes only a row that is
//!   not tombstoned, so an embedding computed before a tombstone and written
//!   after it is stored nowhere.
//! - A row trigger ([`EMBEDDING_TRIGGER_NAME`]) clears the four columns of
//!   any inserted or updated row that is tombstoned. It is what makes the
//!   rule hold for a build that does not know the columns: that build's
//!   tombstone statement names none of them.
//! - A delete removes the row.
//!
//! # The search is an exact scan
//!
//! No index is shipped. The statement reads every claim of one tenant and
//! space that passes the filters, so its cost is linear in those, and every
//! restriction is applied before the ordering
//! ([specification 16](../../../docs/plans/rakka-agent/spec.md)).
//!
//! **Nor can the statement use an approximate index a deployment adds.** The
//! vector column has no fixed dimension, pgvector indexes only a
//! fixed-dimension expression, and the statement orders by the bare column,
//! so the planner would not choose such an index. An approximate index also
//! picks its candidates before a filter runs, so a selective filter would
//! silently lose results behind one. Ranking through one is not available.
//!
//! # The record still decides
//!
//! The columns nominate. Every row a search returns is decoded and checked
//! against its own record, and a row the record does not admit is refused as
//! drift, exactly as the word search refuses it. The page of claims owed an
//! embedding is what a caller sends to a model, so it too lists by the
//! record: a row whose record contradicts its columns is not listed.

use std::collections::BTreeSet;

use rakka_agent::{MemoryClassification, MemoryEmbeddingRef};
use rakka_agent_knowledge_graph::claim::{ClaimId, ClaimTrustStatus};
use rakka_agent_knowledge_graph::error::{ClaimError, ClaimResult};
use rakka_agent_knowledge_graph::scope::KnowledgeSpaceScope;
use rakka_agent_knowledge_graph::{
    claim_semantic_distance_relevance_bps, rank_claim_search_hits, ClaimEmbedding,
    ClaimEmbeddingPage, ClaimSearchHit, ClaimSearchReport, ClaimSemanticSearch,
    KnowledgeGraphCapability, CLAIM_EMBEDDING_PAGE_MAX_CLAIMS,
};
use tokio_postgres::types::ToSql;

use crate::{decode_claim_row, drift_error, map_error, PostgresKnowledgeGraphStore};

/// The trigger that clears a tombstoned row's embedding. Fixed once shipped:
/// the migration finds it by this name.
pub const EMBEDDING_TRIGGER_NAME: &str = "rakka_agent_knowledge_claim_clear_embedding";

/// The check that holds a stored vector to the reference beside it. Fixed
/// once shipped: the migration finds it by this name.
pub const EMBEDDING_CHECK_NAME: &str = "rakka_agent_knowledge_claim_embedding_check";

/// The opt-in migration: the `vector` extension, the four embedding columns,
/// their check and the tombstone trigger.
///
/// Applied by [`PostgresKnowledgeGraphStore::migrate`] of a store built
/// [`with_semantic_search`](PostgresKnowledgeGraphStore::with_semantic_search),
/// after the base migration and under the same migration lock. One batch, so
/// one transaction: where the extension is unavailable, the first statement
/// fails and nothing of this migration is applied.
///
/// - `CREATE EXTENSION IF NOT EXISTS vector` creates the extension in the
///   first schema of the session's search path when the database does not
///   have it yet. The schema that holds it must be on the search path of
///   every session of a store that opted in, because that is where the
///   `vector` type and the `<=>` operator are found.
/// - The four columns are nullable with no default and are added by one
///   `ALTER TABLE`, which is a catalog change: the table is not rewritten.
/// - The check ([`EMBEDDING_CHECK_NAME`]) says the four columns are all null
///   or all set, and that a stored vector has the dimension count beside it.
///   It is added `NOT VALID`, so adding it scans nothing, and validated by
///   [`SEMANTIC_MIGRATION_VALIDATE_SQL`] in a statement of its own. It
///   refuses new rows from the moment it is added.
/// - The row trigger ([`EMBEDDING_TRIGGER_NAME`]) sets the four columns to
///   `NULL` on any inserted or updated row whose `tombstoned` is true and
///   whose `embedding` is not null.
/// - **No index.** A concurrent index build must not run while another
///   session waits inside the migration lock; this migration builds none.
///
/// Every part is skipped when it is already there, so a repeat run changes
/// nothing.
///
/// # What an earlier build does to a migrated table
///
/// A build that does not know the columns names none of them. Its inserts
/// leave them null, which is what "no embedding" means; its trust
/// transitions and deletes do not involve them; and its tombstone is covered
/// by the trigger.
pub const SEMANTIC_MIGRATION_SQL: &str = r#"
CREATE EXTENSION IF NOT EXISTS vector;

DO $semantic$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_attribute
        WHERE attrelid = 'rakka_agent_knowledge_claim'::regclass
          AND attname = 'embedding'
          AND NOT attisdropped
    ) THEN
        ALTER TABLE rakka_agent_knowledge_claim
            ADD COLUMN embedding vector,
            ADD COLUMN embedding_model TEXT,
            ADD COLUMN embedding_dimensions INTEGER,
            ADD COLUMN embedding_version BIGINT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'rakka_agent_knowledge_claim'::regclass
          AND conname = 'rakka_agent_knowledge_claim_embedding_check'
    ) THEN
        ALTER TABLE rakka_agent_knowledge_claim
            ADD CONSTRAINT rakka_agent_knowledge_claim_embedding_check
            CHECK (
                (embedding IS NULL AND embedding_model IS NULL
                 AND embedding_dimensions IS NULL AND embedding_version IS NULL)
                OR (embedding IS NOT NULL AND embedding_model IS NOT NULL
                    AND embedding_dimensions IS NOT NULL AND embedding_version IS NOT NULL
                    AND vector_dims(embedding) = embedding_dimensions)
            )
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'rakka_agent_knowledge_claim'::regclass
          AND tgname = 'rakka_agent_knowledge_claim_clear_embedding'
          AND NOT tgisinternal
    ) THEN
        CREATE OR REPLACE FUNCTION rakka_agent_knowledge_claim_clear_embedding() RETURNS trigger
        LANGUAGE plpgsql AS $clear$
        BEGIN
            NEW.embedding := NULL;
            NEW.embedding_model := NULL;
            NEW.embedding_dimensions := NULL;
            NEW.embedding_version := NULL;
            RETURN NEW;
        END
        $clear$;

        CREATE TRIGGER rakka_agent_knowledge_claim_clear_embedding
            BEFORE INSERT OR UPDATE ON rakka_agent_knowledge_claim
            FOR EACH ROW
            WHEN (NEW.tombstoned AND NEW.embedding IS NOT NULL)
            EXECUTE FUNCTION rakka_agent_knowledge_claim_clear_embedding();
    END IF;
END
$semantic$;
"#;

/// Validates the embedding check, when [`SEMANTIC_MIGRATION_SQL`] left it
/// unvalidated.
///
/// A statement of its own, and so a transaction of its own, for the reason
/// [`MIGRATION_VALIDATE_SQL`](crate::MIGRATION_VALIDATE_SQL) gives: the
/// validating scan must not run under the exclusive lock the check was added
/// under. Every row that existed before the check has four null columns and
/// passes it, so validation cannot fail. A repeat run does nothing.
pub const SEMANTIC_MIGRATION_VALIDATE_SQL: &str = r#"
DO $validate$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'rakka_agent_knowledge_claim'::regclass
          AND conname = 'rakka_agent_knowledge_claim_embedding_check'
          AND NOT convalidated
    ) THEN
        ALTER TABLE rakka_agent_knowledge_claim
            VALIDATE CONSTRAINT rakka_agent_knowledge_claim_embedding_check;
    END IF;
END
$validate$;
"#;

/// The meaning-based search statement: the admitted live claims of one space
/// embedded under the query's reference, nearest first.
///
/// Every restriction is applied before the ordering, and each on a column,
/// so a claim the reader may not see is never a candidate: `$1`/`$2` the
/// tenant and space, `$3` the query vector as pgvector's text literal,
/// `$4`/`$5`/`$6` the query's model, dimension count and pipeline version,
/// `$7` the admitted trust labels, `$8` the admitted classification labels,
/// `$9` the confidence floor, `$10` the capabilities the reader holds, `$11`
/// the limit.
///
/// A row with no filter columns (one a build from before the word index
/// appended, which `index_claims` has not reached) holds `NULL` in them and
/// never matches. The order is cosine distance, then the claim id. A vector
/// with no direction has a distance that is not a number, which PostgreSQL
/// orders after every number. Nothing bounds how many rows are considered;
/// `$11` bounds how many are returned.
///
/// Public so that a test can ask the planner about exactly this text. Not a
/// read path: every row it returns must still be decoded and checked against
/// its record, which the store's `search_semantic` does.
pub const CLAIM_SEMANTIC_SEARCH_SQL: &str = "
SELECT claim_id, record, tombstoned,
       (embedding <=> $3::text::vector)::float8 AS distance
FROM rakka_agent_knowledge_claim
WHERE tenant = $1 AND space = $2
  AND NOT tombstoned
  AND embedding IS NOT NULL
  AND embedding_model = $4
  AND embedding_dimensions = $5
  AND embedding_version = $6
  AND trust = ANY($7)
  AND search_classification = ANY($8)
  AND search_confidence_bps >= $9
  AND search_read_capabilities <@ $10
ORDER BY distance, claim_id
LIMIT $11
";

/// The probe: whether one space holds a live claim this search could not
/// see. Such a claim either passes the search's four filters and has no
/// embedding under the query's reference, or has no filter columns at all
/// and a trust state the search admits, so that the rest of its admission
/// cannot be judged. Parameters as [`CLAIM_SEMANTIC_SEARCH_SQL`], without
/// the vector and the limit.
const CLAIM_SEMANTIC_UNINDEXED_SQL: &str = "
SELECT EXISTS (
    SELECT 1 FROM rakka_agent_knowledge_claim
    WHERE tenant = $1 AND space = $2
      AND NOT tombstoned
      AND trust = ANY($6)
      AND (search_tokens IS NULL
           OR (search_classification = ANY($7)
               AND search_confidence_bps >= $8
               AND search_read_capabilities <@ $9
               AND (embedding IS NULL
                    OR embedding_model IS DISTINCT FROM $3
                    OR embedding_dimensions IS DISTINCT FROM $4
                    OR embedding_version IS DISTINCT FROM $5)))
) AS unindexed
";

/// The embedding write: one claim's four embedding columns, only where the
/// row is not tombstoned.
///
/// `$1`/`$2`/`$3` the tenant, space and claim id, `$4` the vector as
/// pgvector's text literal, `$5`/`$6`/`$7` the model, dimension count and
/// pipeline version. The statement reports how many rows it changed: one
/// when the embedding was stored, none when the scope holds no such claim or
/// the claim is tombstoned.
///
/// That condition is what makes a late write safe. A tombstone that commits
/// between a caller's read of a claim and this write wins: the row is a
/// stub, the condition refuses it, and the withdrawn statement's embedding
/// never reaches it (the claim table's trigger would clear it if it did).
/// Writing the same embedding twice leaves one.
///
/// Public so that a test can run exactly this text against a row a caller
/// could no longer see.
pub const CLAIM_EMBEDDING_WRITE_SQL: &str = "
UPDATE rakka_agent_knowledge_claim
SET embedding = $4::text::vector, embedding_model = $5,
    embedding_dimensions = $6, embedding_version = $7
WHERE tenant = $1 AND space = $2 AND claim_id = $3
  AND NOT tombstoned
";

/// The page's read: the live, unretracted rows of one space, of the
/// classifications given, that have no embedding under a reference, after a
/// cursor, in claim-id order. `$1`/`$2` the tenant and space, `$3` the
/// classification labels, `$4`/`$5`/`$6` the reference, `$7` the cursor,
/// `$8` the limit. A row with no filter columns is not read: its
/// classification cannot be judged without them.
const CLAIM_UNEMBEDDED_READ_SQL: &str = "
SELECT claim_id, record, tombstoned
FROM rakka_agent_knowledge_claim
WHERE tenant = $1 AND space = $2
  AND NOT tombstoned
  AND trust <> 'retracted'
  AND search_classification = ANY($3)
  AND (embedding IS NULL
       OR embedding_model IS DISTINCT FROM $4
       OR embedding_dimensions IS DISTINCT FROM $5
       OR embedding_version IS DISTINCT FROM $6)
  AND claim_id > $7
ORDER BY claim_id
LIMIT $8
";

/// A reference as the three column values that hold it.
struct ReferenceColumns<'a> {
    model: &'a str,
    dimensions: i32,
    version: i64,
}

/// The column values of a reference. One whose dimension count or pipeline
/// version does not fit its column can name no stored embedding and is
/// refused.
fn reference_columns(reference: &MemoryEmbeddingRef) -> ClaimResult<ReferenceColumns<'_>> {
    let out_of_range = |what: &str| ClaimError::InvalidEmbeddingRef {
        message: format!(
            "the {what} of the embedding model {} is beyond what this backend stores",
            reference.model
        ),
    };
    Ok(ReferenceColumns {
        model: &reference.model,
        dimensions: i32::try_from(reference.dimensions)
            .map_err(|_| out_of_range("dimension count"))?,
        version: i64::try_from(reference.version.get())
            .map_err(|_| out_of_range("pipeline version"))?,
    })
}

/// A vector as pgvector's text literal, `[v1,v2,...]`.
///
/// Rust's shortest-round-trip `f32` formatting restores the identical value
/// through pgvector's parser. The caller has validated the vector, so every
/// component is finite.
fn vector_literal(vector: &[f32]) -> String {
    let mut literal = String::with_capacity(vector.len() * 10 + 2);
    literal.push('[');
    for (index, value) in vector.iter().enumerate() {
        if index > 0 {
            literal.push(',');
        }
        literal.push_str(&value.to_string());
    }
    literal.push(']');
    literal
}

fn classification_labels(classifications: &BTreeSet<MemoryClassification>) -> Vec<String> {
    classifications
        .iter()
        .map(|classification| classification.as_label().to_string())
        .collect()
}

impl PostgresKnowledgeGraphStore {
    /// The refusal of a store that did not opt in.
    fn require_semantic_search(&self) -> ClaimResult<()> {
        if self.serves_semantic_search() {
            return Ok(());
        }
        Err(ClaimError::CapabilityUnsupported {
            capability: KnowledgeGraphCapability::SemanticSearch
                .as_label()
                .to_string(),
        })
    }

    /// Applies the opt-in migration. The caller holds the migration lock.
    pub(crate) async fn apply_semantic_migration(&self) -> Result<(), tokio_postgres::Error> {
        self.client.batch_execute(SEMANTIC_MIGRATION_SQL).await?;
        self.client
            .batch_execute(SEMANTIC_MIGRATION_VALIDATE_SQL)
            .await
    }

    /// The meaning-based search; see `KnowledgeGraphStore::search_semantic`.
    ///
    /// The columns nominate and the record decides. The statement restricts
    /// on the row's columns and orders by the database's cosine distance;
    /// every row it returns is then decoded and checked with
    /// [`ClaimSemanticSearch::admits`] against its own record. A row the
    /// record does not admit means a column has drifted from its record: the
    /// search is refused (`claim-backend-failed`), as every other read of a
    /// drifted row is, rather than answered with a claim its reader may not
    /// see. A row that scores nothing is dropped, not refused: that is a
    /// vector with no direction, or one pointing exactly away.
    pub(crate) async fn search_semantic_claims(
        &self,
        scope: &KnowledgeSpaceScope,
        search: &ClaimSemanticSearch,
    ) -> ClaimResult<ClaimSearchReport> {
        self.require_semantic_search()?;
        let query = search.query();
        query.validate()?;
        let reference = reference_columns(&query.reference)?;
        let vector = vector_literal(&query.vector);
        let filters = search.filters();
        let trust: Vec<String> = filters
            .trust()
            .iter()
            .map(|status| status.as_label().to_string())
            .collect();
        let classifications = classification_labels(filters.classifications());
        let min_confidence_bps = i32::from(filters.min_confidence_bps());
        let reader: Vec<String> = filters
            .reader()
            .iter()
            .map(|capability| capability.as_str().to_string())
            .collect();
        // The limit is clamped to 64 by the request itself.
        let limit = i64::try_from(search.limit()).unwrap_or(i64::MAX);
        let tenant = scope.tenant().as_str();
        let space = scope.space().as_str();
        let params: [&(dyn ToSql + Sync); 11] = [
            &tenant,
            &space,
            &vector,
            &reference.model,
            &reference.dimensions,
            &reference.version,
            &trust,
            &classifications,
            &min_confidence_bps,
            &reader,
            &limit,
        ];
        let rows = self
            .client
            .query(CLAIM_SEMANTIC_SEARCH_SQL, &params)
            .await
            .map_err(map_error)?;

        let mut hits = Vec::with_capacity(rows.len());
        for row in rows {
            let claim = decode_claim_row(&row)?;
            let claim_id: String = row.get("claim_id");
            if claim.claim_id.as_str() != claim_id || !search.admits(&claim) {
                return Err(drift_error(&claim.claim_id));
            }
            let distance: f64 = row.get("distance");
            let relevance_bps = claim_semantic_distance_relevance_bps(distance);
            if relevance_bps > 0 {
                hits.push(ClaimSearchHit {
                    claim,
                    relevance_bps,
                });
            }
        }
        // The answer is in the shared rule's order, whatever order the
        // database's own distances produced among equal scores.
        rank_claim_search_hits(&mut hits, search.limit());

        let probe: [&(dyn ToSql + Sync); 9] = [
            &tenant,
            &space,
            &reference.model,
            &reference.dimensions,
            &reference.version,
            &trust,
            &classifications,
            &min_confidence_bps,
            &reader,
        ];
        let unindexed: bool = self
            .client
            .query_one(CLAIM_SEMANTIC_UNINDEXED_SQL, &probe)
            .await
            .map_err(map_error)?
            .get("unindexed");
        Ok(ClaimSearchReport { hits, unindexed })
    }

    /// Stores one claim's embedding; see
    /// `KnowledgeGraphStore::store_claim_embedding`.
    pub(crate) async fn store_embedding(
        &self,
        scope: &KnowledgeSpaceScope,
        claim_id: &ClaimId,
        embedding: &ClaimEmbedding,
    ) -> ClaimResult<bool> {
        self.require_semantic_search()?;
        embedding.validate()?;
        let reference = reference_columns(&embedding.reference)?;
        let vector = vector_literal(&embedding.vector);
        let params: [&(dyn ToSql + Sync); 7] = [
            &scope.tenant().as_str(),
            &scope.space().as_str(),
            &claim_id.as_str(),
            &vector,
            &reference.model,
            &reference.dimensions,
            &reference.version,
        ];
        let written = self
            .client
            .execute(CLAIM_EMBEDDING_WRITE_SQL, &params)
            .await
            .map_err(map_error)?;
        Ok(written > 0)
    }

    /// One page of the claims owed an embedding; see
    /// `KnowledgeGraphStore::unembedded_claims`.
    ///
    /// What this answers is what a caller sends to a model, so the record
    /// decides here too. A row whose record cannot be decoded, is not the
    /// claim its row names, or contradicts the columns the statement
    /// selected it by (its classification is outside the set, or it is
    /// retracted) is counted in [`ClaimEmbeddingPage::undecodable`] and not
    /// listed. The cursor moves past it.
    ///
    /// So is a row whose claim id this build refuses (one written past the
    /// store, by a later build with a wider id alphabet or by a hand
    /// repair), which also does not count toward `limit`: the call reads on
    /// until it has read `limit` rows whose claim id it reads, or the rows
    /// run out, so [`ClaimEmbeddingPage::next`] is always a claim id. One
    /// call may read more than `limit` rows.
    pub(crate) async fn unembedded(
        &self,
        scope: &KnowledgeSpaceScope,
        reference: &MemoryEmbeddingRef,
        classifications: &BTreeSet<MemoryClassification>,
        after: Option<&ClaimId>,
        limit: usize,
    ) -> ClaimResult<ClaimEmbeddingPage> {
        self.require_semantic_search()?;
        let limit = limit.clamp(1, CLAIM_EMBEDDING_PAGE_MAX_CLAIMS);
        let reference = reference_columns(reference)?;
        let labels = classification_labels(classifications);
        // The cursor inside the call is the raw text of the last row read,
        // whatever it holds, so the next read starts past a row whose claim
        // id this build refuses. Every claim id sorts after the empty string.
        let mut after_raw = after.map_or_else(String::new, |id| id.as_str().to_string());

        let mut page = ClaimEmbeddingPage::default();
        let mut last: Option<ClaimId> = None;
        // Rows whose claim id this build reads; only they count toward
        // `limit`.
        let mut handled = 0usize;
        let mut ended = false;
        while handled < limit {
            let want = limit - handled;
            let params: [&(dyn ToSql + Sync); 8] = [
                &scope.tenant().as_str(),
                &scope.space().as_str(),
                &labels,
                &reference.model,
                &reference.dimensions,
                &reference.version,
                &after_raw,
                &i64::try_from(want).unwrap_or(i64::MAX),
            ];
            let rows = self
                .client
                .query(CLAIM_UNEMBEDDED_READ_SQL, &params)
                .await
                .map_err(map_error)?;
            let read = rows.len();
            for row in rows {
                let claim_id: String = row.get("claim_id");
                let parsed = ClaimId::new(claim_id.as_str());
                after_raw = claim_id;
                // A row whose claim id this build refuses is counted and read
                // past, and does not count toward `limit`: the page ends on a
                // claim id it read, or at the end of the rows.
                let Ok(parsed) = parsed else {
                    page.undecodable += 1;
                    continue;
                };
                handled += 1;
                let claim = decode_claim_row(&row).ok().filter(|claim| {
                    claim.claim_id == parsed
                        && !claim.is_tombstoned()
                        && claim.trust() != ClaimTrustStatus::Retracted
                        && classifications.contains(&claim.classification)
                });
                match claim {
                    Some(claim) => page.claims.push(claim),
                    None => page.undecodable += 1,
                }
                last = Some(parsed);
            }
            if read < want {
                ended = true;
                break;
            }
        }
        page.next = if ended { None } else { last };
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use rakka_agent::AgentRevisionNumber;

    use super::*;

    #[test]
    fn a_vector_is_written_as_pgvectors_text_literal() {
        assert_eq!(vector_literal(&[1.0, 0.0, -0.5]), "[1,0,-0.5]");
        assert_eq!(vector_literal(&[0.6]), "[0.6]");
        // No exponent form, which the parser would also take but a reader
        // would not expect.
        assert_eq!(vector_literal(&[1.0e-7]), "[0.0000001]");
    }

    #[test]
    fn a_reference_beyond_the_columns_is_refused() {
        let reference = |version: u64| MemoryEmbeddingRef {
            model: "model-a".to_string(),
            dimensions: 3,
            version: AgentRevisionNumber::new(version),
        };
        let fitting = reference(7);
        let columns = reference_columns(&fitting).expect("the reference fits");
        assert_eq!(
            (columns.model, columns.dimensions, columns.version),
            ("model-a", 3, 7)
        );
        assert_eq!(
            reference_columns(&reference(u64::MAX))
                .map(|_| ())
                .expect_err("a version beyond a BIGINT names no stored embedding")
                .code(),
            "claim-embedding-invalid"
        );
    }
}
