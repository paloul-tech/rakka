//! The graph-backed goal-claim source: the join port
//! [`rakka_agent`]'s authorized goal view reads shared-knowledge references
//! through ([specification 17.18](../../docs/plans/rakka-agent/spec.md)).
//!
//! `rakka-agent` declares the [`AgentGoalClaimSource`] port and cannot see
//! this crate — the dependency runs graph → agent — so the application wires
//! this implementation beside its [`KnowledgeGraphStore`], exactly as it
//! wires the append executor. The source serves the spaces it is explicitly
//! constructed with: a claim record deliberately carries no tenant or space,
//! so the space each reference reports is the scope it was queried under,
//! and a space the application does not name here is simply not joined —
//! the view reports the join as available with whatever the named spaces
//! answered, and a store failure degrades it whole.

use std::sync::Arc;

use rakka_agent::{
    AgentCommunalClaimId, AgentGoalClaimFuture, AgentGoalClaimRef, AgentGoalClaimSource,
    AgentGoalClaimSourceError, AgentGoalId, KnowledgeSpaceId, TenantId,
};

use crate::error::ClaimError;
use crate::scope::KnowledgeSpaceScope;
use crate::store::{ClaimCursor, ClaimFilter, KnowledgeGraphStore};

/// Reads goal-scoped claim references from a wired [`KnowledgeGraphStore`]
/// across the knowledge spaces it is constructed with.
pub struct KnowledgeGraphGoalClaimSource {
    store: Arc<dyn KnowledgeGraphStore>,
    spaces: Vec<KnowledgeSpaceId>,
}

impl KnowledgeGraphGoalClaimSource {
    /// Wires the source over the store it queries; add the spaces it serves
    /// with [`Self::with_space`].
    #[must_use]
    pub fn new(store: Arc<dyn KnowledgeGraphStore>) -> Self {
        Self {
            store,
            spaces: Vec::new(),
        }
    }

    /// Adds one knowledge space the source joins claims from, in the order
    /// spaces were added.
    #[must_use]
    pub fn with_space(mut self, space: KnowledgeSpaceId) -> Self {
        self.spaces.push(space);
        self
    }
}

fn source_error(error: &ClaimError) -> AgentGoalClaimSourceError {
    AgentGoalClaimSourceError::new(error.code(), error.to_string())
}

impl AgentGoalClaimSource for KnowledgeGraphGoalClaimSource {
    fn backend_name(&self) -> &'static str {
        "knowledge-graph"
    }

    fn claims_for_goal<'a>(
        &'a self,
        tenant: &'a TenantId,
        goal: &'a AgentGoalId,
        limit: usize,
    ) -> AgentGoalClaimFuture<'a> {
        Box::pin(async move {
            let filter = ClaimFilter::matching_all().with_goal(goal.clone());
            let mut refs: Vec<AgentGoalClaimRef> = Vec::new();
            for space in &self.spaces {
                if refs.len() >= limit {
                    break;
                }
                let scope =
                    KnowledgeSpaceScope::new(tenant.clone(), space.clone()).map_err(|error| {
                        AgentGoalClaimSourceError::new(error.code(), error.to_string())
                    })?;
                let mut cursor = ClaimCursor::start().with_limit(limit - refs.len());
                loop {
                    let page = self
                        .store
                        .query(&scope, &filter, cursor)
                        .await
                        .map_err(|error| source_error(&error))?;
                    for claim in page.claims {
                        if refs.len() >= limit {
                            break;
                        }
                        let claim_id = AgentCommunalClaimId::new(claim.claim_id.as_str()).map_err(
                            |error| AgentGoalClaimSourceError::new(error.code(), error.to_string()),
                        )?;
                        let mut reference = AgentGoalClaimRef::new(
                            claim_id,
                            space.clone(),
                            claim.provenance.agent.clone(),
                        );
                        if let Some(task) = claim.provenance.task {
                            reference = reference.with_task(task);
                        }
                        if let Some(run) = claim.provenance.run {
                            reference = reference.with_run(run);
                        }
                        if let Some(delegation) = claim.provenance.delegation {
                            reference = reference.with_delegation(delegation);
                        }
                        refs.push(reference);
                    }
                    match page.next {
                        Some(next) if refs.len() < limit => {
                            cursor = next.with_limit(limit - refs.len());
                        }
                        _ => break,
                    }
                }
            }
            Ok(refs)
        })
    }
}
