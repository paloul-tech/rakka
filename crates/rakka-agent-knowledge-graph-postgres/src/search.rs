//! The claim search index: what a claim row's four index columns hold.
//!
//! The index is columns on the claim row, written by the statement that
//! inserts the claim, so there is no second write that could be lost. Three
//! of the four copy fields of the record that never change after the append;
//! only the words are taken from the claim's content, and those are what a
//! tombstone clears (see [`MIGRATION_SQL`](crate::MIGRATION_SQL)).
//!
//! The record stays the authority. A search uses these columns to find and
//! order candidates; every row it returns is then decoded and checked against
//! its own record, and a column that disagrees is refused as drift.

use rakka_agent_knowledge_graph::claim::Claim;
use rakka_agent_knowledge_graph::error::ClaimResult;
use rakka_agent_knowledge_graph::scope::KnowledgeSpaceScope;
use rakka_agent_knowledge_graph::{
    claim_search_relevance_bps, claim_search_tokens, rank_claim_search_hits, ClaimSearch,
    ClaimSearchHit, ClaimSearchReport,
};
use tokio_postgres::types::ToSql;

use crate::{decode_claim_row, drift_error, map_error, PostgresKnowledgeGraphStore};

/// The values of one live claim's four index columns.
pub(crate) struct SearchColumns {
    /// `search_tokens`: the claim's search words, sorted. Empty for a claim
    /// with no words, which is indexed all the same.
    pub(crate) tokens: Vec<String>,
    /// `search_classification`: the classification's stable label.
    pub(crate) classification: &'static str,
    /// `search_confidence_bps`: the asserting confidence.
    pub(crate) confidence_bps: i32,
    /// `search_read_capabilities`: every capability a reader must hold,
    /// sorted. Empty for a claim that requires none.
    pub(crate) read_capabilities: Vec<String>,
}

/// Derives the index columns from a claim: the one derivation, shared by the
/// append and by the indexing pass, so a row indexed later holds exactly what
/// it would have held had it been appended by this build.
pub(crate) fn search_columns(claim: &Claim) -> SearchColumns {
    SearchColumns {
        tokens: claim_search_tokens(claim).into_iter().collect(),
        classification: claim.classification.as_label(),
        confidence_bps: i32::from(claim.confidence_bps),
        read_capabilities: claim
            .access
            .required_read_capabilities
            .iter()
            .map(|capability| capability.as_str().to_string())
            .collect(),
    }
}

/// The search statement: the admitted live claims of one space that share a
/// word with the query, best first.
///
/// Every restriction is applied before the ordering, and each on a column, so
/// a claim the reader may not see is never a candidate
/// ([specification 16](../../../docs/plans/rakka-agent/spec.md)): `$1`/`$2`
/// the tenant and space, `$3` the query's words, `$4` the admitted trust
/// labels, `$5` the admitted classification labels, `$6` the confidence
/// floor, `$7` the capabilities the reader holds, `$8` the limit.
///
/// `search_tokens && $3` is the predicate the GIN index
/// ([`SEARCH_TOKENS_INDEX_NAME`](crate::SEARCH_TOKENS_INDEX_NAME)) answers; a
/// row that is not indexed holds `NULL` there and never matches. The order is
/// the number of query words the row holds, then the claim id, which is the
/// shared scorer's order because a score is that number over the size of the
/// query. Nothing bounds how many rows are considered; `$8` bounds how many
/// are returned.
///
/// Public so that a test can ask the planner about exactly this text. Not a
/// read path: every row it returns must still be decoded and checked against
/// its record, which [`PostgresKnowledgeGraphStore`](crate::PostgresKnowledgeGraphStore)'s
/// `search` does.
pub const CLAIM_SEARCH_SQL: &str = "
SELECT claim_id, record, tombstoned
FROM rakka_agent_knowledge_claim
WHERE tenant = $1 AND space = $2
  AND NOT tombstoned
  AND search_tokens && $3
  AND trust = ANY($4)
  AND search_classification = ANY($5)
  AND search_confidence_bps >= $6
  AND search_read_capabilities <@ $7
ORDER BY (SELECT count(*) FROM unnest($3::TEXT[]) AS q(token)
          WHERE q.token = ANY(search_tokens)) DESC,
         claim_id
LIMIT $8
";

/// The probe: whether one space holds a live claim the index does not cover.
/// The partial index [`UNINDEXED_INDEX_NAME`](crate::UNINDEXED_INDEX_NAME)
/// holds exactly those rows, so the probe reads at most one entry of it.
pub(crate) const CLAIM_SEARCH_UNINDEXED_SQL: &str = "
SELECT EXISTS (
    SELECT 1 FROM rakka_agent_knowledge_claim
    WHERE tenant = $1 AND space = $2 AND search_tokens IS NULL AND NOT tombstoned
) AS unindexed
";

impl PostgresKnowledgeGraphStore {
    /// The search operation; see `KnowledgeGraphStore::search`.
    ///
    /// The index nominates and the record decides. The statement restricts
    /// and orders on the index columns; every row it returns is then decoded,
    /// checked with [`ClaimSearch::admits`] and scored with the shared scorer
    /// against its own record. A row the record does not admit, or that
    /// shares no word with the query, means an index column has drifted from
    /// its record: the search is refused (`claim-backend-failed`), as every
    /// other read of a drifted row is, rather than answered short or — worse
    /// — answered with a claim its reader may not see.
    pub(crate) async fn search_claims(
        &self,
        scope: &KnowledgeSpaceScope,
        search: &ClaimSearch,
    ) -> ClaimResult<ClaimSearchReport> {
        let tokens: Vec<String> = search.query_tokens().iter().cloned().collect();
        let trust: Vec<String> = search
            .trust()
            .iter()
            .map(|status| status.as_label().to_string())
            .collect();
        let classifications: Vec<String> = search
            .classifications()
            .iter()
            .map(|classification| classification.as_label().to_string())
            .collect();
        let min_confidence_bps = i32::from(search.min_confidence_bps());
        let reader: Vec<String> = search
            .reader()
            .iter()
            .map(|capability| capability.as_str().to_string())
            .collect();
        // The limit is clamped to 64 by the request itself.
        let limit = i64::try_from(search.limit()).unwrap_or(i64::MAX);
        let params: [&(dyn ToSql + Sync); 8] = [
            &scope.tenant().as_str(),
            &scope.space().as_str(),
            &tokens,
            &trust,
            &classifications,
            &min_confidence_bps,
            &reader,
            &limit,
        ];
        let rows = self
            .client
            .query(CLAIM_SEARCH_SQL, &params)
            .await
            .map_err(map_error)?;

        let mut hits = Vec::with_capacity(rows.len());
        for row in rows {
            let claim = decode_claim_row(&row)?;
            let relevance_bps = claim_search_relevance_bps(search.query_tokens(), &claim);
            if !search.admits(&claim) || relevance_bps == 0 {
                return Err(drift_error(&claim.claim_id));
            }
            hits.push(ClaimSearchHit {
                claim,
                relevance_bps,
            });
        }
        // The answer is in the shared scorer's order, whatever order the
        // database's own count produced.
        rank_claim_search_hits(&mut hits, search.limit());

        let unindexed: bool = self
            .client
            .query_one(
                CLAIM_SEARCH_UNINDEXED_SQL,
                &[&scope.tenant().as_str(), &scope.space().as_str()],
            )
            .await
            .map_err(map_error)?
            .get("unindexed");
        Ok(ClaimSearchReport { hits, unindexed })
    }
}
