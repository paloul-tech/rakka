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

use rakka_agent_knowledge_graph::claim::{Claim, ClaimId};
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

/// Largest number of rows one [`PostgresKnowledgeGraphStore::index_claims`]
/// page reads.
pub const CLAIM_SEARCH_INDEX_PAGE_MAX_ROWS: usize = 256;

/// One bounded page of [`PostgresKnowledgeGraphStore::index_claims`]
/// progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimSearchIndexPage {
    /// How many claims this page indexed: rows this call's own write changed.
    /// A row another pass indexed first, or a tombstone withdrew first, is
    /// not counted.
    pub indexed: u64,
    /// How many rows this page read and could not index because this build
    /// cannot decode their record or refuses their claim id. Such a row
    /// stays unindexed, so its space keeps reporting `unindexed` until the
    /// row is repaired or erased; a non-zero count is the signal to look.
    pub skipped: u64,
    /// The cursor for the next page: the last claim this page read, when the
    /// page was full. `None` when the rows ran out before the page was full,
    /// which is the end of the pass. Always a claim id this build reads: a
    /// row whose claim id it refuses is read past and does not fill the
    /// page.
    pub next: Option<ClaimId>,
}

/// The pass's read: the live rows of one space that have no search words,
/// after a cursor, in claim-id order. Exactly the rows the partial index
/// [`UNINDEXED_INDEX_NAME`](crate::UNINDEXED_INDEX_NAME) holds.
const CLAIM_SEARCH_INDEX_READ_SQL: &str = "
SELECT claim_id, record, tombstoned
FROM rakka_agent_knowledge_claim
WHERE tenant = $1 AND space = $2
  AND search_tokens IS NULL AND NOT tombstoned
  AND claim_id > $3
ORDER BY claim_id
LIMIT $4
";

/// The pass's write: one row's four index columns, only where the row is
/// still live and still unindexed.
///
/// That condition is what makes the pass safe beside everything else. A
/// tombstone that lands between the pass's read and this write wins: the row
/// is a stub, the condition refuses it, and the withdrawn statement's words
/// never reach it (the claim table's trigger would clear them if they did).
/// A second pass over the same row finds it indexed and changes nothing, so
/// a row is indexed once however many passes run. The statement reports how
/// many rows it changed, which is how the pass counts.
///
/// Public so that a test can run exactly this text against a row the pass
/// could no longer see.
pub const CLAIM_SEARCH_INDEX_WRITE_SQL: &str = "
UPDATE rakka_agent_knowledge_claim
SET search_tokens = $4, search_classification = $5,
    search_confidence_bps = $6, search_read_capabilities = $7
WHERE tenant = $1 AND space = $2 AND claim_id = $3
  AND search_tokens IS NULL AND NOT tombstoned
";

impl PostgresKnowledgeGraphStore {
    /// Indexes one bounded page of the claims of one space that have no
    /// search index yet.
    ///
    /// A claim has no index when it was appended before the search migration,
    /// or by a node on a build from before it. Search does not find such a
    /// claim and reports `unindexed` for its space; this pass is what indexes
    /// it. **No loop in Rakka calls it.** A deployment calls it for each
    /// space, feeding each page's [`ClaimSearchIndexPage::next`] back as
    /// `after`, until a page answers no cursor.
    ///
    /// Each page reads at most `limit` rows (clamped to
    /// `1..=`[`CLAIM_SEARCH_INDEX_PAGE_MAX_ROWS`]) in claim-id order after
    /// the cursor, derives each row's index from its record, and writes it
    /// only where the row is still live and still unindexed
    /// ([`CLAIM_SEARCH_INDEX_WRITE_SQL`]). A record this build cannot decode
    /// is counted in [`ClaimSearchIndexPage::skipped`] and passed over; it
    /// does not stop the pass.
    ///
    /// Nor does a row whose claim id this build refuses (one written past
    /// the store, by a later build with a wider id alphabet or by a hand
    /// repair). It is counted in [`ClaimSearchIndexPage::skipped`] and read
    /// past, and does not count toward `limit`: the call reads on until it
    /// has read `limit` rows whose claim id it reads, or the rows run out,
    /// so [`ClaimSearchIndexPage::next`] is always a claim id. One call may
    /// read more than `limit` rows.
    ///
    /// The pass is idempotent and safe to run at any time, on any number of
    /// nodes at once, while the store is serving. It only ever touches the
    /// addressed tenant and space.
    ///
    /// # Errors
    ///
    /// Returns `claim-backend-failed` when the database cannot be reached. A
    /// damaged record is never an error.
    pub async fn index_claims(
        &self,
        scope: &KnowledgeSpaceScope,
        after: Option<&ClaimId>,
        limit: usize,
    ) -> ClaimResult<ClaimSearchIndexPage> {
        let limit = limit.clamp(1, CLAIM_SEARCH_INDEX_PAGE_MAX_ROWS);
        // The cursor inside the call is the raw text of the last row read,
        // whatever it holds, so the next read starts past a row whose claim
        // id this build refuses. Every claim id sorts after the empty string.
        let mut after_raw = after.map_or_else(String::new, |id| id.as_str().to_string());

        let mut page = ClaimSearchIndexPage {
            indexed: 0,
            skipped: 0,
            next: None,
        };
        let mut last: Option<ClaimId> = None;
        // Rows whose claim id this build reads; only they count toward
        // `limit`.
        let mut handled = 0usize;
        let mut ended = false;
        while handled < limit {
            let want = limit - handled;
            let rows = self
                .client
                .query(
                    CLAIM_SEARCH_INDEX_READ_SQL,
                    &[
                        &scope.tenant().as_str(),
                        &scope.space().as_str(),
                        &after_raw,
                        &i64::try_from(want).unwrap_or(i64::MAX),
                    ],
                )
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
                    page.skipped += 1;
                    continue;
                };
                handled += 1;
                // The record is the only source of the index. One that cannot
                // be decoded, or that is not the claim its own row names, has
                // no index this build could vouch for.
                let claim = decode_claim_row(&row)
                    .ok()
                    .filter(|claim| claim.claim_id == parsed);
                match claim {
                    Some(claim) => {
                        let index = search_columns(&claim);
                        let written = self
                            .client
                            .execute(
                                CLAIM_SEARCH_INDEX_WRITE_SQL,
                                &[
                                    &scope.tenant().as_str(),
                                    &scope.space().as_str(),
                                    &parsed.as_str(),
                                    &index.tokens,
                                    &index.classification,
                                    &index.confidence_bps,
                                    &index.read_capabilities,
                                ],
                            )
                            .await
                            .map_err(map_error)?;
                        page.indexed += written;
                    }
                    None => page.skipped += 1,
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
