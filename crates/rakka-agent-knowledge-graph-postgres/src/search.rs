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
use rakka_agent_knowledge_graph::claim_search_tokens;

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
