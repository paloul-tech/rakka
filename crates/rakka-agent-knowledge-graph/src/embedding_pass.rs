//! The embedding pass: what gives the claims of a knowledge space the
//! embeddings a meaning-based search ranks by.
//!
//! A claim is embedded when it is appended, best effort (see
//! [`KnowledgeGraphClaimAppendExecutor`](crate::KnowledgeGraphClaimAppendExecutor)).
//! [`embed_claims`] embeds whatever that left: a claim appended before the
//! deployment opted in, one whose embedding failed at append, and, after a
//! change of model or pipeline version, every claim.
//!
//! **No loop in Rakka calls the pass.** A deployment calls it for each space,
//! feeding each page's [`ClaimEmbeddingPassPage::next`] back as `after`,
//! until a page answers no cursor.
//!
//! # What is sent to the model
//!
//! Embedding a claim sends its text ([`claim_embedding_text`]) to the
//! embedder the deployment wired, which may be a service outside the
//! deployment. The caller states which classifications of claim may be sent;
//! there is no default. Whatever the caller states, a `Redacted` claim is
//! never sent, and neither is a `Retracted` one or a tombstoned one.

use std::collections::BTreeSet;

use rakka_agent::{embed_memory_vector, AgentMemoryEmbedder, MemoryClassification};

use crate::claim::{Claim, ClaimId, ClaimTrustStatus};
use crate::error::ClaimResult;
use crate::scope::KnowledgeSpaceScope;
use crate::semantic::{claim_embedding_text, ClaimEmbedding};
use crate::store::KnowledgeGraphStore;

/// One bounded page of [`embed_claims`] progress.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClaimEmbeddingPassPage {
    /// How many claims this page embedded and stored.
    pub embedded: u64,
    /// How many claims this page could not embed: the model failed for them,
    /// or answered a vector that does not match its own reference. Such a
    /// claim stays unembedded and the pass moves on. A non-zero count means:
    /// run the pass again later. A claim that fails every time is one the
    /// model will not embed; searches that admit it keep reporting an
    /// incomplete index.
    pub failed: u64,
    /// How many records the store read and could not answer
    /// ([`ClaimEmbeddingPage::undecodable`](crate::semantic::ClaimEmbeddingPage::undecodable)).
    pub undecodable: u64,
    /// The cursor for the next page. `None` is the end of the pass.
    pub next: Option<ClaimId>,
}

/// Whether a caller that may embed `classifications` may send `claim` to an
/// embedder: the claim is live, is not `Retracted`, is not `Redacted`, and
/// its classification is in the set.
///
/// The one rule the append executor and the pass share. `Redacted` is
/// refused whatever the set says, as the private memory tier never embeds
/// redacted content; a `Retracted` claim is one no search ever admits.
pub(crate) fn may_embed(claim: &Claim, classifications: &BTreeSet<MemoryClassification>) -> bool {
    !claim.is_tombstoned()
        && claim.trust() != ClaimTrustStatus::Retracted
        && claim.classification != MemoryClassification::Redacted
        && classifications.contains(&claim.classification)
}

/// Embeds one claim's text. `None` when the claim has no text, the model
/// failed, or its answer does not match its own reference.
///
/// The failure is dropped on purpose: neither caller may fail because of it,
/// and a model's error can quote the text it was given.
pub(crate) async fn embed_claim(
    embedder: &dyn AgentMemoryEmbedder,
    claim: &Claim,
) -> Option<ClaimEmbedding> {
    let text = claim_embedding_text(claim)?;
    let vector = embed_memory_vector(embedder, &text).await.ok()?;
    let embedding = ClaimEmbedding {
        reference: embedder.embedding_ref(),
        vector,
    };
    embedding.validate().ok()?;
    Some(embedding)
}

/// Embeds one bounded page of the claims of one space that have no embedding
/// under the embedder's reference, and stores each.
///
/// The page is
/// [`KnowledgeGraphStore::unembedded_claims`] under the embedder's reference
/// and `classifications` with `Redacted` removed, so `limit` is clamped as
/// that operation clamps it and the claims are taken in claim-id order after
/// `after`.
///
/// - **A claim the model refuses does not stop the pass.** It is counted in
///   [`ClaimEmbeddingPassPage::failed`] and the cursor moves past it, so one
///   input a provider always rejects cannot keep every later claim of the
///   space unembedded.
/// - A claim withdrawn between the page's read and its write is stored
///   nothing and counted nowhere.
/// - The pass is idempotent and safe to run at any time, on any number of
///   nodes at once, while the store is serving: a claim that is embedded is
///   not listed again, and storing an embedding twice leaves one.
/// - After a change of model, dimension count or pipeline version every
///   claim is unembedded under the new reference, so the pass embeds them
///   all again.
///
/// # Errors
///
/// Returns the store's error when the store cannot read the page or store
/// an embedding (`claim-capability-unsupported` from a store that does not
/// serve meaning-based search). What the call already stored stays, and a
/// re-run from the same cursor continues. A model failure is never an error.
pub async fn embed_claims(
    store: &dyn KnowledgeGraphStore,
    embedder: &dyn AgentMemoryEmbedder,
    classifications: &BTreeSet<MemoryClassification>,
    scope: &KnowledgeSpaceScope,
    after: Option<&ClaimId>,
    limit: usize,
) -> ClaimResult<ClaimEmbeddingPassPage> {
    let reference = embedder.embedding_ref();
    let mut sendable = classifications.clone();
    sendable.remove(&MemoryClassification::Redacted);
    let listed = store
        .unembedded_claims(scope, &reference, &sendable, after, limit)
        .await?;

    let mut page = ClaimEmbeddingPassPage {
        embedded: 0,
        failed: 0,
        undecodable: listed.undecodable,
        next: listed.next,
    };
    for claim in &listed.claims {
        // The store listed by the set; the record is checked once more here,
        // because this is the last step before a claim's text leaves.
        if !may_embed(claim, &sendable) {
            continue;
        }
        let Some(embedding) = embed_claim(embedder, claim).await else {
            page.failed += 1;
            continue;
        };
        if store
            .store_claim_embedding(scope, &claim.claim_id, &embedding)
            .await?
        {
            page.embedded += 1;
        }
    }
    Ok(page)
}
