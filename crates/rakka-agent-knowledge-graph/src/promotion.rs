//! The HITL/policy promotion gate for consequential claims.
//!
//! [Specification 13.4](../../../docs/plans/rakka-agent/spec.md): policy may
//! require a human or a verifier service before a claim becomes `Verified`,
//! especially when it can authorize or materially influence a high-impact
//! effect. The gate reuses the slice 1.10 checkpoint machinery verbatim — the
//! evidence a promotion presents *is* an
//! [`AgentCheckpointGrant`], validated through the same
//! `validate_for_binding` code path the dispatch gate uses, so digest-binding,
//! expiry, and use-count semantics can never drift between the two gates.
//!
//! The binding is derived deterministically from the authoritative claim, and
//! its generation is pinned to the transition ordinal the promotion would
//! occupy: a grant authorizes promotion at exactly one history position, so a
//! grant minted before a dispute can never be replayed into the re-promotion
//! after it. When a later milestone makes claim promotion a run-driven durable
//! effect (scenario 33), that effect adopts the same effect id, target, and
//! digest, and the checkpoint it opens yields a grant this gate already
//! accepts.

use std::collections::BTreeSet;

use rakka_agent::{
    AgentCheckpointEffectBinding, AgentCheckpointGrant, AgentContentDigest, AgentEffectGeneration,
    AgentEffectSafetyClass, MemoryClassification,
};
use rakka_agent_workflow::{AgentEffectId, AgentTimestampMillis};

use crate::claim::{Claim, ClaimId, ClaimPredicate, ClaimTrustStatus};
use crate::error::{ClaimError, ClaimResult};
use crate::scope::KnowledgeSpaceScope;
use crate::transition::ClaimPromotionReceipt;

/// Which claims are consequential enough to require a grant before they become
/// `Verified`.
///
/// The default is [`ClaimPromotionPolicy::gate_all`]: every promotion requires
/// a grant until a deployment states otherwise — fail closed, the same stance
/// every other agent-domain gate takes. [`ClaimPromotionPolicy::ungated`] is
/// the explicit opposite statement, exactly as a no-stage deployment passes an
/// empty guardrail chain rather than omitting one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimPromotionPolicy {
    mode: GateMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum GateMode {
    GateAll,
    GateMatching {
        classifications: BTreeSet<MemoryClassification>,
        predicates: BTreeSet<ClaimPredicate>,
    },
    Ungated,
}

impl ClaimPromotionPolicy {
    /// Every promotion requires a grant. The default.
    #[must_use]
    pub const fn gate_all() -> Self {
        Self {
            mode: GateMode::GateAll,
        }
    }

    /// No promotion requires a grant — an explicit deployment statement,
    /// never a default.
    #[must_use]
    pub const fn ungated() -> Self {
        Self {
            mode: GateMode::Ungated,
        }
    }

    /// A promotion requires a grant when the claim's classification or
    /// predicate matches either set. Two empty sets gate nothing — use
    /// [`Self::ungated`] to say that on purpose.
    #[must_use]
    pub const fn gating(
        classifications: BTreeSet<MemoryClassification>,
        predicates: BTreeSet<ClaimPredicate>,
    ) -> Self {
        Self {
            mode: GateMode::GateMatching {
                classifications,
                predicates,
            },
        }
    }

    /// Whether promoting this claim to `Verified` requires a grant.
    #[must_use]
    pub fn requires_grant(&self, claim: &Claim) -> bool {
        match &self.mode {
            GateMode::GateAll => true,
            GateMode::Ungated => false,
            GateMode::GateMatching {
                classifications,
                predicates,
            } => {
                classifications.contains(&claim.classification)
                    || predicates.contains(&claim.predicate)
            }
        }
    }
}

impl Default for ClaimPromotionPolicy {
    fn default() -> Self {
        Self::gate_all()
    }
}

/// The evidence a gated promotion presents: the checkpoint grant its
/// resolution produced.
#[derive(Debug, Clone, PartialEq)]
pub struct ClaimPromotionEvidence {
    /// The digest-bound grant.
    pub grant: AgentCheckpointGrant,
}

/// The deterministic effect id of one claim's promotion.
///
/// `"claim-promotion:{claim_id}"` — derived, never generated, so the durable
/// run effect a later milestone commits for the same promotion carries the
/// same id, and a grant minted against either derivation binds both.
#[must_use]
pub fn claim_promotion_effect_id(claim_id: &ClaimId) -> AgentEffectId {
    AgentEffectId::new(format!("claim-promotion:{claim_id}"))
}

/// Derives the canonical promotion binding from the authoritative claim.
///
/// Everything the grant must bind enters the digest: the scope's injective
/// key (a grant approved for one space can never promote in another), the
/// claim id and full statement (approved content is promoted content — the
/// statement is append-only-immutable, so a digest mismatch means the grant
/// was minted for something else), and the history position (`from` status
/// and the one-based ordinal the promotion would occupy). The generation is
/// pinned to that same ordinal, so a stale grant fails the identity check
/// before the digest is even compared.
pub fn claim_promotion_binding(
    scope: &KnowledgeSpaceScope,
    claim: &Claim,
) -> ClaimResult<AgentCheckpointEffectBinding> {
    let object = serde_json::to_value(&claim.object).map_err(|error| ClaimError::Encoding {
        message: format!("the claim object could not be encoded: {error}"),
    })?;
    let ordinal = claim.transition_count() + 1;
    let payload = serde_json::json!({
        "claim": claim.claim_id.as_str(),
        "from": claim.trust().as_label(),
        "object": object,
        "ordinal": ordinal,
        "predicate": claim.predicate.as_str(),
        "scope": scope.key(),
        "subject": claim.subject.as_str(),
        "to": ClaimTrustStatus::Verified.as_label(),
    });
    Ok(AgentCheckpointEffectBinding {
        effect_id: claim_promotion_effect_id(&claim.claim_id),
        generation: AgentEffectGeneration::new(ordinal),
        target: format!("claim-promotion:{}", claim.claim_id),
        argument_digest: AgentContentDigest::sha256_of_json(&payload),
        safety_class: AgentEffectSafetyClass::NonIdempotent,
        credential_binding: None,
    })
}

/// The one gate every backend calls when a transition targets `Verified`.
///
/// Returns the audit receipt to stamp on the transition when the gate fired,
/// `None` when policy does not require one, and fails closed otherwise. The
/// binding is recomputed here from the authoritative claim — never read back
/// from recorded state — which is the same recompute-don't-trust rule the
/// dispatch gate follows.
pub fn validate_promotion(
    scope: &KnowledgeSpaceScope,
    claim: &Claim,
    policy: &ClaimPromotionPolicy,
    promotion: Option<&ClaimPromotionEvidence>,
    now: AgentTimestampMillis,
) -> ClaimResult<Option<ClaimPromotionReceipt>> {
    if !policy.requires_grant(claim) {
        return Ok(None);
    }
    let Some(evidence) = promotion else {
        return Err(ClaimError::PromotionGrantRequired {
            claim_id: claim.claim_id.clone(),
        });
    };
    if !evidence.grant.kind.is_approval_family() {
        return Err(ClaimError::PromotionGrantKind {
            kind: evidence.grant.kind,
        });
    }
    if evidence.grant.scope.tenant() != scope.tenant() {
        return Err(ClaimError::PromotionGrantScope {
            claim_id: claim.claim_id.clone(),
        });
    }
    let binding = claim_promotion_binding(scope, claim)?;
    evidence
        .grant
        .validate_for_binding(&binding, 1, now)
        .map_err(|reason| ClaimError::PromotionGrantRejected {
            claim_id: claim.claim_id.clone(),
            reason,
        })?;
    Ok(Some(ClaimPromotionReceipt {
        checkpoint_id: evidence.grant.checkpoint_id.clone(),
        resolver: evidence.grant.resolver.clone(),
        argument_digest: binding.argument_digest,
    }))
}

#[cfg(test)]
mod tests {
    use rakka_agent::{AgentId, KnowledgeSpaceId, MemoryClassification, TenantId};

    use crate::claim::{ClaimNodeId, ClaimObject, ClaimOperationId, ClaimProvenance};

    use super::*;

    fn scope() -> KnowledgeSpaceScope {
        KnowledgeSpaceScope::new(
            TenantId::new("acme"),
            KnowledgeSpaceId::new("support-kb").expect("the space id is valid"),
        )
        .expect("the scope is valid")
    }

    fn claim(predicate: &str, classification: MemoryClassification) -> Claim {
        let operation_id =
            ClaimOperationId::derive_append(&scope(), "op-1").expect("the operation id derives");
        Claim::new(
            &scope(),
            operation_id,
            ClaimNodeId::new("customer-1").expect("the node id is valid"),
            ClaimPredicate::new(predicate).expect("the predicate is valid"),
            ClaimObject::Node(ClaimNodeId::new("channel-email").expect("the node id is valid")),
            ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid")),
            9_000,
            classification,
            AgentTimestampMillis::new(1),
        )
        .expect("the claim is valid")
    }

    #[test]
    fn the_policy_matrix_is_exact() {
        let unclassified = claim("prefers", MemoryClassification::Unclassified);
        let sensitive = claim("authorizes", MemoryClassification::Sensitive);

        assert!(ClaimPromotionPolicy::default().requires_grant(&unclassified));
        assert!(ClaimPromotionPolicy::gate_all().requires_grant(&sensitive));
        assert!(!ClaimPromotionPolicy::ungated().requires_grant(&sensitive));

        let matching = ClaimPromotionPolicy::gating(
            BTreeSet::from([MemoryClassification::Sensitive]),
            BTreeSet::from([ClaimPredicate::new("authorizes").expect("the predicate is valid")]),
        );
        assert!(matching.requires_grant(&sensitive));
        assert!(!matching.requires_grant(&unclassified));
        let by_predicate = claim("authorizes", MemoryClassification::Unclassified);
        assert!(matching.requires_grant(&by_predicate));

        // Two empty sets gate nothing — the documented behavior.
        let empty = ClaimPromotionPolicy::gating(BTreeSet::new(), BTreeSet::new());
        assert!(!empty.requires_grant(&sensitive));
    }

    #[test]
    fn the_binding_is_deterministic_and_ordinal_sensitive() {
        let claim = claim("prefers", MemoryClassification::Unclassified);
        let scope = scope();
        let a = claim_promotion_binding(&scope, &claim).expect("the binding derives");
        let b = claim_promotion_binding(&scope, &claim).expect("the binding derives");
        assert_eq!(a, b);
        assert!(a.argument_digest.algorithm.is_cryptographic());
        assert_eq!(a.generation, AgentEffectGeneration::new(1));

        // A later history position is a different generation and digest.
        let disputed = claim
            .apply_transition(crate::claim::ClaimTrustStatus::Disputed)
            .expect("the dispute applies");
        let later = claim_promotion_binding(&scope, &disputed).expect("the binding derives");
        assert_eq!(later.generation, AgentEffectGeneration::new(2));
        assert_ne!(later.argument_digest, a.argument_digest);
        assert_eq!(later.effect_id, a.effect_id);

        // A different space is a different digest for the same claim content.
        let other = KnowledgeSpaceScope::new(
            TenantId::new("acme"),
            KnowledgeSpaceId::new("finance-kb").expect("the space id is valid"),
        )
        .expect("the scope is valid");
        let foreign = claim_promotion_binding(&other, &claim).expect("the binding derives");
        assert_ne!(foreign.argument_digest, a.argument_digest);
    }
}
