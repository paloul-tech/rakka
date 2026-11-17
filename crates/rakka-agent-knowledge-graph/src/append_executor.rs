//! The graph-backed claim-append executor: the bridge from a run's committed
//! `ClaimAppend` effect to a durable store append
//! ([specification 8.5 and 13.4](../../docs/plans/rakka-agent/spec.md),
//! scenario 33).
//!
//! `rakka-agent` declares the executor trait and cannot see this crate — the
//! dependency runs graph → agent — so the application wires this
//! implementation into its dispatcher. The store-side append operation id
//! derives from the intent's external idempotency key: stable across every
//! attempt of a generation, so the store's operation ledger answers a replay
//! with the original claim, and a deliberately re-decided generation is a new
//! logical claim. The provenance written is the transition-stamped record
//! riding on the intent — goal, task, source agent and run, delegation, and
//! the carrying effect — never anything invented here.
//!
//! # Embedding at append
//!
//! An executor given an embedder
//! ([`KnowledgeGraphClaimAppendExecutor::with_embedder`]) also embeds the
//! claim it appended and stores the embedding, so that a meaning-based
//! search finds a fresh claim without waiting for a pass. That step is best
//! effort: **a failed embedding never fails the append.** A claim it could
//! not embed is left for [`embed_claims`](crate::embedding_pass::embed_claims).

use std::collections::BTreeSet;
use std::sync::Arc;

use rakka_agent::{
    AgentClaimAppendExecutor, AgentClaimAppendFinding, AgentClaimAppendProvenance,
    AgentClaimAppendRequest, AgentClaimObjectRequest, AgentCommunalClaimId, AgentDispatchError,
    AgentDispatchFuture, AgentMemoryEmbedder, AgentRunEffect, AgentRunScope, MemoryClassification,
};
use rakka_agent_workflow::AgentTimestampMillis;

use crate::claim::{
    Claim, ClaimNodeId, ClaimObject, ClaimOperationId, ClaimPredicate, ClaimProvenance,
};
use crate::embedding_pass::{embed_claim, may_embed};
use crate::error::ClaimError;
use crate::scope::KnowledgeSpaceScope;
use crate::store::{KnowledgeGraphCapability, KnowledgeGraphStore};

/// What an executor embeds with, and which claims it may send there.
struct AppendEmbedding {
    embedder: Arc<dyn AgentMemoryEmbedder>,
    classifications: BTreeSet<MemoryClassification>,
}

/// Executes one claim append against a wired [`KnowledgeGraphStore`].
pub struct KnowledgeGraphClaimAppendExecutor {
    store: Arc<dyn KnowledgeGraphStore>,
    embedding: Option<AppendEmbedding>,
}

impl KnowledgeGraphClaimAppendExecutor {
    /// Wires the executor over the store it appends into.
    #[must_use]
    pub fn new(store: Arc<dyn KnowledgeGraphStore>) -> Self {
        Self {
            store,
            embedding: None,
        }
    }

    /// Makes the executor embed each claim it appends and store the
    /// embedding, best effort, so a meaning-based search finds a fresh claim
    /// at once.
    ///
    /// **Embedding sends a claim's text to `embedder`**, which may be a
    /// service outside the deployment. `classifications` is the set of
    /// classifications the deployment allows to be sent, and it has no
    /// default: the deployment states it. It should cover every
    /// classification the deployment's read policy admits, because a claim
    /// that is never embedded is never found by meaning. Whatever the set
    /// says, a `Redacted` claim is never sent. The empty set embeds nothing.
    ///
    /// The step runs after the store answers the append, on the first
    /// attempt and on a replay alike, and only when the store reports
    /// [`KnowledgeGraphCapability::SemanticSearch`]. Every failure of it is
    /// dropped: the model failing, an answer that does not match the
    /// embedder's own reference, the store refusing the embedding. The
    /// finding is `Appended` exactly as it is without an embedder, and the
    /// claim is left for [`embed_claims`](crate::embedding_pass::embed_claims).
    #[must_use]
    pub fn with_embedder(
        mut self,
        embedder: Arc<dyn AgentMemoryEmbedder>,
        classifications: BTreeSet<MemoryClassification>,
    ) -> Self {
        self.embedding = Some(AppendEmbedding {
            embedder,
            classifications,
        });
        self
    }

    /// Embeds an appended claim and stores the embedding, best effort.
    async fn embed_appended(&self, scope: &KnowledgeSpaceScope, appended: &Claim) {
        let Some(embedding) = &self.embedding else {
            return;
        };
        if !self
            .store
            .capabilities()
            .supports(KnowledgeGraphCapability::SemanticSearch)
            || !may_embed(appended, &embedding.classifications)
        {
            return;
        }
        let Some(vector) = embed_claim(embedding.embedder.as_ref(), appended).await else {
            return;
        };
        // A claim withdrawn meanwhile answers `false`, and a store failure
        // answers an error. Neither changes what the append found.
        let _ = self
            .store
            .store_claim_embedding(scope, &appended.claim_id, &vector)
            .await;
    }
}

/// Whether a store refusal is definitive for the request that produced it.
///
/// A backend failure is the store's inability, retried under the effect's
/// idempotent attempt bound; everything else — a validation bound, an
/// operation conflict, a foreign claim under the derived id — answers the
/// same however often it is retried.
const fn definitive(error: &ClaimError) -> bool {
    !matches!(error, ClaimError::Backend { .. })
}

impl AgentClaimAppendExecutor for KnowledgeGraphClaimAppendExecutor {
    fn execute<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        intent: &'a AgentRunEffect,
        append: &'a AgentClaimAppendRequest,
        provenance: &'a AgentClaimAppendProvenance,
        now: AgentTimestampMillis,
    ) -> AgentDispatchFuture<'a, AgentClaimAppendFinding> {
        Box::pin(async move {
            let refused = |code: &str, message: String| {
                Ok(AgentClaimAppendFinding::Refused {
                    code: code.to_string(),
                    message,
                })
            };
            let space_scope =
                match KnowledgeSpaceScope::new(scope.tenant().clone(), append.space.clone()) {
                    Ok(scope) => scope,
                    Err(error) => return refused(error.code(), error.to_string()),
                };
            // The discriminator is the generation's external idempotency key:
            // pure within the generation, so every attempt derives the same
            // store operation and the ledger converges replays.
            let Some(external_key) = intent.safety.external_key() else {
                return refused(
                    "claim-append-key-missing",
                    "a claim append dispatches idempotent and always carries its derived \
                     external key"
                        .to_string(),
                );
            };
            let operation_id =
                match ClaimOperationId::derive_append(&space_scope, external_key.as_str()) {
                    Ok(operation_id) => operation_id,
                    Err(error) => return refused(error.code(), error.to_string()),
                };
            let subject = match ClaimNodeId::new(&append.subject) {
                Ok(subject) => subject,
                Err(error) => return refused(error.code(), error.to_string()),
            };
            let predicate = match ClaimPredicate::new(&append.predicate) {
                Ok(predicate) => predicate,
                Err(error) => return refused(error.code(), error.to_string()),
            };
            let object = match &append.object {
                AgentClaimObjectRequest::Node(node) => match ClaimNodeId::new(node) {
                    Ok(node) => ClaimObject::Node(node),
                    Err(error) => return refused(error.code(), error.to_string()),
                },
                AgentClaimObjectRequest::Value(content) => ClaimObject::Value(content.clone()),
                // The request shape is non-exhaustive across crate versions:
                // an object this build cannot interpret fails closed rather
                // than guessing a statement.
                other => {
                    return refused(
                        "claim-object-unsupported",
                        format!("this build cannot interpret the claim object {other:?}"),
                    )
                }
            };
            let mut claim_provenance = ClaimProvenance::for_agent(provenance.agent.clone())
                .with_task(provenance.task.clone())
                .with_run(provenance.run.clone())
                .with_effect(intent.effect_id.clone());
            if let Some(goal) = &provenance.goal {
                claim_provenance = claim_provenance.with_goal(goal.clone());
            }
            if let Some(delegation) = &provenance.delegation {
                claim_provenance = claim_provenance.with_delegation(delegation.clone());
            }
            let claim = Claim::new(
                &space_scope,
                operation_id,
                subject,
                predicate,
                object,
                claim_provenance,
                append.confidence_bps,
                append.classification,
                now,
            )
            .and_then(|claim| claim.with_evidence(append.evidence.clone()));
            let claim = match claim {
                Ok(claim) => claim,
                Err(error) => return refused(error.code(), error.to_string()),
            };
            match self.store.append(&space_scope, &claim).await {
                Ok(appended) => {
                    self.embed_appended(&space_scope, &appended).await;
                    let claim =
                        AgentCommunalClaimId::new(appended.claim_id.as_str()).map_err(|error| {
                            AgentDispatchError::Invocation {
                                code: "claim-append-id-invalid",
                                message: error.to_string(),
                            }
                        })?;
                    Ok(AgentClaimAppendFinding::Appended { claim })
                }
                Err(error) if definitive(&error) => refused(error.code(), error.to_string()),
                Err(error) => Err(AgentDispatchError::Invocation {
                    code: "claim-append-backend-failed",
                    message: error.to_string(),
                }),
            }
        })
    }
}
