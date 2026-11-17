//! Keyword search over the claims of one knowledge space.
//!
//! [Specification 13.6](../../../docs/plans/rakka-agent/spec.md) lets a
//! backend report optional query features; keyword search is one
//! ([`KnowledgeGraphCapability::KeywordSearch`](crate::store::KnowledgeGraphCapability::KeywordSearch)).
//! This module is what every backend that reports it shares: the request, the
//! one admission rule, the one word rule and the one scorer. A backend may
//! apply them inside an index, but it may not replace them, which is why two
//! backends answer the same hits in the same order.
//!
//! Search ranks; it does not authorize. [`ClaimSearch::admits`] is applied
//! before ranking, so a claim its reader may not see never takes a hit slot
//! ([specification 16](../../../docs/plans/rakka-agent/spec.md)), and a caller
//! that puts a hit in front of a model still resolves it through the store.

use std::collections::BTreeSet;

use rakka_agent::{
    memory_embedding_text, tokenize_search_text, AgentCapabilityId, MemoryClassification,
};

use crate::claim::{Claim, ClaimObject, ClaimTrustStatus};

/// Longest query text one search carries, in bytes. A longer text is cut at
/// the last character boundary at or under the bound.
pub const CLAIM_SEARCH_TEXT_MAX_BYTES: usize = 1024;

/// Largest number of hits one search answers.
pub const CLAIM_SEARCH_MAX_HITS: usize = 64;

/// One keyword search of one knowledge space: the query and everything that
/// restricts who and what it may find.
///
/// Every field sits behind an accessor so that a backend can apply it inside
/// its own index. A new search admits no trust state and no classification
/// until it is told which: an empty admitted set admits nothing, so a caller
/// that forgets to say what it may read finds nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimSearch {
    text: String,
    query_tokens: BTreeSet<String>,
    trust: BTreeSet<ClaimTrustStatus>,
    classifications: BTreeSet<MemoryClassification>,
    min_confidence_bps: u16,
    reader: BTreeSet<AgentCapabilityId>,
    limit: usize,
}

impl ClaimSearch {
    /// Creates a search for `text`, answering at most `limit` hits.
    ///
    /// The text is clamped to [`CLAIM_SEARCH_TEXT_MAX_BYTES`] at a character
    /// boundary and the limit to `1..=`[`CLAIM_SEARCH_MAX_HITS`]. The search
    /// admits nothing until [`Self::with_trust`] and
    /// [`Self::with_classifications`] say what it may find, and its reader
    /// holds no capability until [`Self::with_reader`] says otherwise.
    #[must_use]
    pub fn new(text: &str, limit: usize) -> Self {
        let mut end = text.len().min(CLAIM_SEARCH_TEXT_MAX_BYTES);
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        let text = text[..end].to_string();
        let query_tokens = claim_search_query_tokens(&text);
        Self {
            text,
            query_tokens,
            trust: BTreeSet::new(),
            classifications: BTreeSet::new(),
            min_confidence_bps: 0,
            reader: BTreeSet::new(),
            limit: limit.clamp(1, CLAIM_SEARCH_MAX_HITS),
        }
    }

    /// Sets the trust states the search admits.
    #[must_use]
    pub fn with_trust(mut self, trust: BTreeSet<ClaimTrustStatus>) -> Self {
        self.trust = trust;
        self
    }

    /// Sets the classifications the search admits.
    #[must_use]
    pub fn with_classifications(mut self, classifications: BTreeSet<MemoryClassification>) -> Self {
        self.classifications = classifications;
        self
    }

    /// Sets the confidence floor, in basis points.
    #[must_use]
    pub fn with_min_confidence_bps(mut self, min_confidence_bps: u16) -> Self {
        self.min_confidence_bps = min_confidence_bps;
        self
    }

    /// Sets the capabilities the reader holds. A claim is admitted only when
    /// every capability it requires is among them.
    #[must_use]
    pub fn with_reader(mut self, reader: BTreeSet<AgentCapabilityId>) -> Self {
        self.reader = reader;
        self
    }

    /// The query text, as clamped.
    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }

    /// The words of the query: [`claim_search_query_tokens`] of
    /// [`Self::text`].
    #[must_use]
    pub const fn query_tokens(&self) -> &BTreeSet<String> {
        &self.query_tokens
    }

    /// The admitted trust states.
    #[must_use]
    pub const fn trust(&self) -> &BTreeSet<ClaimTrustStatus> {
        &self.trust
    }

    /// The admitted classifications.
    #[must_use]
    pub const fn classifications(&self) -> &BTreeSet<MemoryClassification> {
        &self.classifications
    }

    /// The confidence floor, in basis points.
    #[must_use]
    pub const fn min_confidence_bps(&self) -> u16 {
        self.min_confidence_bps
    }

    /// The capabilities the reader holds.
    #[must_use]
    pub const fn reader(&self) -> &BTreeSet<AgentCapabilityId> {
        &self.reader
    }

    /// The largest number of hits the search answers.
    #[must_use]
    pub const fn limit(&self) -> usize {
        self.limit
    }

    /// Whether the search may find `claim`: the one admission rule every
    /// backend applies before it ranks.
    ///
    /// True when the claim is live (a tombstoned stub never matches), its
    /// trust state and its classification are admitted, its confidence meets
    /// the floor, and every read capability it requires is one the reader
    /// holds.
    #[must_use]
    pub fn admits(&self, claim: &Claim) -> bool {
        !claim.is_tombstoned()
            && self.trust.contains(&claim.trust())
            && self.classifications.contains(&claim.classification)
            && claim.confidence_bps >= self.min_confidence_bps
            && claim
                .access
                .required_read_capabilities
                .is_subset(&self.reader)
    }
}

/// The words a search can match a claim by: the words of its subject, of its
/// predicate and of its object.
///
/// The object gives its node id when it is an edge and its text when it is an
/// inline value (a string as written, any other value as its canonical JSON).
/// A value held by artifact reference gives nothing: this crate never loads
/// artifact bytes, and the reference itself is not content. A tombstoned stub
/// has no words at all, because its statement was withdrawn.
///
/// A backend that keeps an index stores exactly this set, so the rule is a
/// compatibility surface (see [`claim_search_query_tokens`]).
#[must_use]
pub fn claim_search_tokens(claim: &Claim) -> BTreeSet<String> {
    if claim.is_tombstoned() {
        return BTreeSet::new();
    }
    let mut tokens = tokenize_search_text(claim.subject.as_str());
    tokens.extend(tokenize_search_text(claim.predicate.as_str()));
    match &claim.object {
        ClaimObject::Node(node) => tokens.extend(tokenize_search_text(node.as_str())),
        ClaimObject::Value(content) => {
            if let Some(text) = memory_embedding_text(content) {
                tokens.extend(tokenize_search_text(&text));
            }
        }
    }
    tokens
}

/// The words of a query: the text split on every character that is not
/// alphanumeric, lowercased, without duplicates.
///
/// The same rule gives a claim its words, and a backend's index stores them,
/// so changing it would strand every stored word. A golden test pins its
/// output.
#[must_use]
pub fn claim_search_query_tokens(text: &str) -> BTreeSet<String> {
    tokenize_search_text(text)
}

/// The one scorer: the fraction of the query's words that are among the
/// claim's words, in basis points. An empty query scores zero.
#[must_use]
pub fn claim_search_relevance_bps(query_tokens: &BTreeSet<String>, claim: &Claim) -> u16 {
    if query_tokens.is_empty() {
        return 0;
    }
    let overlap = query_tokens
        .intersection(&claim_search_tokens(claim))
        .count();
    let bps = overlap.saturating_mul(10_000) / query_tokens.len();
    u16::try_from(bps.min(10_000)).unwrap_or(10_000)
}

/// One claim a search found, with how well it matched.
#[derive(Debug, Clone, PartialEq)]
pub struct ClaimSearchHit {
    /// The claim, whole, as the store holds it.
    pub claim: Claim,
    /// How well the claim matched, in basis points. For a keyword search,
    /// [`claim_search_relevance_bps`] of the query against the claim; for a
    /// meaning-based search,
    /// [`claim_semantic_relevance_bps`](crate::semantic::claim_semantic_relevance_bps)
    /// of the query's embedding against the claim's. Never zero in either: a
    /// claim that shares no word with the query, or whose embedding scores
    /// nothing against the query's, is not a hit.
    pub relevance_bps: u16,
}

/// What one search answered: a keyword search, or a meaning-based one.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ClaimSearchReport {
    /// The admitted claims the search found, best first, ties by ascending
    /// claim id, at most the search's limit. A keyword search finds the
    /// claims that share a word with the query; a meaning-based search finds
    /// the claims embedded under the query's reference.
    pub hits: Vec<ClaimSearchHit>,
    /// Whether the space holds live claims this search could not see because
    /// the backend's index does not cover them. For a keyword search, a claim
    /// with no word index; a backend with no index to fall behind always
    /// answers `false`. For a meaning-based search, a claim the search admits
    /// (or whose admission the backend cannot judge) that has no embedding
    /// under the query's reference.
    pub unindexed: bool,
}

/// Orders hits best first, ties by ascending claim id, and keeps `limit`.
///
/// Shared so that every backend's answer is in the scorer's order whatever
/// order its own storage produced.
pub fn rank_claim_search_hits(hits: &mut Vec<ClaimSearchHit>, limit: usize) {
    hits.sort_by(|a, b| {
        b.relevance_bps
            .cmp(&a.relevance_bps)
            .then_with(|| a.claim.claim_id.as_str().cmp(b.claim.claim_id.as_str()))
    });
    hits.truncate(limit);
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rakka_agent::{AgentCapabilityId, AgentId, AgentTaskContent, MemoryClassification};
    use rakka_agent_workflow::{
        AgentAttributes, AgentTimestampMillis, ArtifactKind, ArtifactRef, RedactionStatus,
    };

    use super::*;
    use crate::claim::{
        Claim, ClaimAccess, ClaimNodeId, ClaimObject, ClaimOperationId, ClaimPredicate,
        ClaimProvenance, ClaimTrustStatus,
    };
    use crate::scope::KnowledgeSpaceScope;
    use crate::tombstone::{ClaimTombstone, ClaimTombstoneReason};

    fn words(text: &str) -> Vec<String> {
        claim_search_query_tokens(text).into_iter().collect()
    }

    fn scope() -> KnowledgeSpaceScope {
        KnowledgeSpaceScope::new(
            rakka_agent::TenantId::new("acme"),
            rakka_agent::KnowledgeSpaceId::new("space-a").expect("the space id is valid"),
        )
        .expect("the scope is valid")
    }

    fn claim(discriminator: &str, subject: &str, predicate: &str, object: ClaimObject) -> Claim {
        let scope = scope();
        Claim::new(
            &scope,
            ClaimOperationId::derive_append(&scope, discriminator)
                .expect("the operation id derives"),
            ClaimNodeId::new(subject).expect("the node id is valid"),
            ClaimPredicate::new(predicate).expect("the predicate is valid"),
            object,
            ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid")),
            5_000,
            MemoryClassification::Unclassified,
            AgentTimestampMillis::new(1),
        )
        .expect("the claim is valid")
    }

    fn inline(value: serde_json::Value) -> ClaimObject {
        ClaimObject::Value(AgentTaskContent::inline(value).expect("the value is bounded"))
    }

    fn artifact() -> ClaimObject {
        ClaimObject::Value(AgentTaskContent::artifact(ArtifactRef {
            artifact_id: "secretartifactid".to_string(),
            kind: ArtifactKind::File,
            uri: "s3://evidence/secreturi".to_string(),
            checksum: Some("sha256:secretchecksum".to_string()),
            content_type: Some("application/json".to_string()),
            byte_len: Some(64),
            retention_class: None,
            encryption: None,
            redaction: RedactionStatus::Unredacted,
            created_at: AgentTimestampMillis::new(1),
            metadata: AgentAttributes::default(),
        }))
    }

    fn set(words: &[&str]) -> BTreeSet<String> {
        words.iter().map(|word| (*word).to_string()).collect()
    }

    /// The word rule is a compatibility surface: the PostgreSQL backend stores
    /// its output, so a change here strands every stored word. These outputs
    /// must never change.
    #[test]
    fn the_word_rule_is_frozen() {
        assert_eq!(words("Hello, WORLD! hello"), ["hello", "world"]);
        assert_eq!(words("dark-mode_v2 x9"), ["dark", "mode", "v2", "x9"]);
        assert_eq!(words("Straße ÉCOLE 東京"), ["straße", "école", "東京"]);
        assert_eq!(words("  ...  "), [] as [&str; 0]);
    }

    #[test]
    fn a_claims_words_are_its_subject_predicate_and_object() {
        let edge = claim(
            "edge",
            "Order-42",
            "shipped_by",
            ClaimObject::Node(ClaimNodeId::new("Carrier North").expect("the node id is valid")),
        );
        assert_eq!(
            claim_search_tokens(&edge),
            set(&["order", "42", "shipped", "by", "carrier", "north"])
        );

        let text = claim(
            "text",
            "order-42",
            "status",
            inline(serde_json::json!("Delayed at customs")),
        );
        assert_eq!(
            claim_search_tokens(&text),
            set(&["order", "42", "status", "delayed", "at", "customs"])
        );

        let structured = claim(
            "structured",
            "order-42",
            "status",
            inline(serde_json::json!({ "state": "held", "days": 3 })),
        );
        assert_eq!(
            claim_search_tokens(&structured),
            set(&["order", "42", "status", "state", "held", "days", "3"])
        );

        // A value held by reference gives no words: nothing of the reference
        // itself (its id, its location, its checksum) is searchable.
        let referenced = claim("referenced", "order-42", "status", artifact());
        assert_eq!(
            claim_search_tokens(&referenced),
            set(&["order", "42", "status"])
        );
    }

    #[test]
    fn a_tombstoned_stub_has_no_words() {
        let live = claim("stub", "order-42", "status", inline(serde_json::json!("x")));
        let stub = live
            .apply_tombstone(ClaimTombstone {
                operation_id: ClaimOperationId::derive_tombstone(&scope(), &live.claim_id, "t")
                    .expect("the operation id derives"),
                reason: ClaimTombstoneReason::Withdrawn,
                actor: crate::conformance::conformance_resolver(),
                tombstoned_at: AgentTimestampMillis::new(2),
            })
            .expect("the tombstone applies");
        assert!(claim_search_tokens(&stub).is_empty());
        assert_eq!(
            claim_search_relevance_bps(&claim_search_query_tokens("order status"), &stub),
            0
        );
    }

    #[test]
    fn relevance_is_the_fraction_of_query_words_the_claim_holds() {
        let text = claim(
            "score",
            "order-42",
            "status",
            inline(serde_json::json!("delayed at customs")),
        );
        let score =
            |query: &str| claim_search_relevance_bps(&claim_search_query_tokens(query), &text);
        assert_eq!(score("delayed"), 10_000);
        assert_eq!(score("delayed customs absent"), 6_666);
        assert_eq!(score("absent"), 0);
        assert_eq!(score("..."), 0, "an empty query scores nothing");
    }

    #[test]
    fn a_search_clamps_its_text_and_its_limit() {
        assert_eq!(ClaimSearch::new("x", 0).limit(), 1);
        assert_eq!(ClaimSearch::new("x", 10_000).limit(), CLAIM_SEARCH_MAX_HITS);

        // The text is cut at a character boundary at or under the bound.
        let long = "é".repeat(CLAIM_SEARCH_TEXT_MAX_BYTES);
        let search = ClaimSearch::new(&long, 8);
        assert_eq!(search.text().len(), CLAIM_SEARCH_TEXT_MAX_BYTES);
        assert!(long.starts_with(search.text()));
        let odd = format!("a{long}");
        assert_eq!(
            ClaimSearch::new(&odd, 8).text().len(),
            CLAIM_SEARCH_TEXT_MAX_BYTES - 1,
            "a two-byte character that straddles the bound is dropped whole"
        );
        assert_eq!(
            search.query_tokens(),
            &claim_search_query_tokens(search.text())
        );
    }

    #[test]
    fn a_search_admits_only_what_it_was_told_to() {
        let capability = |id: &str| AgentCapabilityId::new(id).expect("the capability id");
        let guarded = claim("admit", "a", "b", inline(serde_json::json!("c")))
            .with_access(ClaimAccess {
                required_read_capabilities: BTreeSet::from([capability("cap-a")]),
            })
            .expect("the access set is bounded");
        let admitting = ClaimSearch::new("c", 8)
            .with_trust(BTreeSet::from([ClaimTrustStatus::Proposed]))
            .with_classifications(BTreeSet::from([MemoryClassification::Unclassified]))
            .with_reader(BTreeSet::from([capability("cap-a")]));
        assert!(admitting.admits(&guarded));

        assert!(
            !ClaimSearch::new("c", 8).admits(&guarded),
            "a new search admits no trust state and no classification"
        );
        assert!(!admitting
            .clone()
            .with_trust(BTreeSet::from([ClaimTrustStatus::Verified]))
            .admits(&guarded));
        assert!(!admitting
            .clone()
            .with_classifications(BTreeSet::from([MemoryClassification::Sensitive]))
            .admits(&guarded));
        assert!(!admitting
            .clone()
            .with_min_confidence_bps(5_001)
            .admits(&guarded));
        assert!(admitting
            .clone()
            .with_min_confidence_bps(5_000)
            .admits(&guarded));
        assert!(!admitting
            .clone()
            .with_reader(BTreeSet::new())
            .admits(&guarded));

        let stub = guarded
            .apply_tombstone(ClaimTombstone {
                operation_id: ClaimOperationId::derive_tombstone(&scope(), &guarded.claim_id, "t")
                    .expect("the operation id derives"),
                reason: ClaimTombstoneReason::Withdrawn,
                actor: crate::conformance::conformance_resolver(),
                tombstoned_at: AgentTimestampMillis::new(2),
            })
            .expect("the tombstone applies");
        assert!(!admitting.admits(&stub), "a stub is never admitted");
    }
}
