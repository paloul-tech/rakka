//! Append-only trust transitions: the auditable history of a claim's trust.
//!
//! Verification, dispute, and retraction never rewrite the claim — they append
//! a transition record that preserves the original provenance
//! ([specification 13.4](../../../docs/plans/rakka-agent/spec.md)). The
//! claim's current trust status is a denormalization the store updates
//! atomically with the transition append; the ordinal-ordered transition
//! history is the audit source.

use rakka_agent::{AgentContentDigest, AgentPolicyRef};
use rakka_agent_workflow::{
    AgentTimestampMillis, ArtifactRef, HumanCheckpointId, PrincipalRef, StateSchemaVersion,
};
use serde::{Deserialize, Serialize};

use crate::claim::{
    Claim, ClaimId, ClaimOperationId, ClaimProvenance, ClaimTrustStatus,
    CLAIM_MAX_EVIDENCE_ARTIFACTS,
};
use crate::error::{check_schema_window, ClaimError, ClaimRecordKind, ClaimResult};
use crate::promotion::ClaimPromotionEvidence;

/// Schema version this binary writes on every [`ClaimTrustTransition`] record.
pub const CURRENT_CLAIM_TRUST_TRANSITION_SCHEMA_VERSION: StateSchemaVersion =
    StateSchemaVersion::new(1);

/// Largest reason length one transition may carry, in bytes.
pub const CLAIM_TRANSITION_REASON_MAX_LENGTH: usize = 512;

/// Audit stub of the grant that authorized a gated promotion — never the
/// grant itself, so a persisted transition can neither replay nor leak the
/// authorization it records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimPromotionReceipt {
    /// The checkpoint whose resolution produced the grant.
    pub checkpoint_id: HumanCheckpointId,
    /// The authenticated principal that resolved it.
    pub resolver: PrincipalRef,
    /// The cryptographic digest the grant bound.
    pub argument_digest: AgentContentDigest,
}

/// One append-only trust transition
/// ([specification 13.4](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ClaimTrustTransition {
    schema_version: StateSchemaVersion,
    /// The claim the transition moves.
    pub claim_id: ClaimId,
    /// The idempotent key of this transition.
    pub operation_id: ClaimOperationId,
    /// One-based position in the claim's transition history, store-stamped.
    pub ordinal: u32,
    /// Trust status the claim held before the transition.
    pub from: ClaimTrustStatus,
    /// Trust status the transition moved the claim to.
    pub to: ClaimTrustStatus,
    /// The principal that decided the transition.
    pub actor: PrincipalRef,
    /// Agent provenance, when an agent or run drove the transition.
    #[serde(default)]
    pub provenance: Option<ClaimProvenance>,
    /// Bounded free-text reason, when one was given.
    #[serde(default)]
    pub reason: Option<String>,
    /// Evidence artifact references supporting the transition.
    #[serde(default)]
    pub evidence: Vec<ArtifactRef>,
    /// The promotion gate's receipt, present exactly when the gate fired.
    #[serde(default)]
    pub gate: Option<ClaimPromotionReceipt>,
    /// When the transition was decided.
    pub occurred_at: AgentTimestampMillis,
    /// The policy the transition was decided under, when any.
    #[serde(default)]
    pub policy: Option<AgentPolicyRef>,
}

impl ClaimTrustTransition {
    /// Creates a transition record for a legal `from -> to` pair.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        claim_id: ClaimId,
        operation_id: ClaimOperationId,
        ordinal: u32,
        from: ClaimTrustStatus,
        to: ClaimTrustStatus,
        actor: PrincipalRef,
        occurred_at: AgentTimestampMillis,
    ) -> ClaimResult<Self> {
        let transition = Self {
            schema_version: CURRENT_CLAIM_TRUST_TRANSITION_SCHEMA_VERSION,
            claim_id,
            operation_id,
            ordinal,
            from,
            to,
            actor,
            provenance: None,
            reason: None,
            evidence: Vec::new(),
            gate: None,
            occurred_at,
            policy: None,
        };
        transition.validate()?;
        Ok(transition)
    }

    /// Attaches agent provenance, re-validating the record.
    pub fn with_provenance(mut self, provenance: ClaimProvenance) -> ClaimResult<Self> {
        self.provenance = Some(provenance);
        self.validate()?;
        Ok(self)
    }

    /// Attaches a bounded reason, re-validating the record.
    pub fn with_reason(mut self, reason: impl Into<String>) -> ClaimResult<Self> {
        self.reason = Some(reason.into());
        self.validate()?;
        Ok(self)
    }

    /// Attaches evidence references, re-validating the bound.
    pub fn with_evidence(mut self, evidence: Vec<ArtifactRef>) -> ClaimResult<Self> {
        self.evidence = evidence;
        self.validate()?;
        Ok(self)
    }

    /// Attaches the promotion gate's receipt.
    #[must_use]
    pub fn with_gate(mut self, gate: ClaimPromotionReceipt) -> Self {
        self.gate = Some(gate);
        self
    }

    /// Attaches the policy the transition was decided under.
    #[must_use]
    pub fn with_policy(mut self, policy: AgentPolicyRef) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Schema version the record carries.
    #[must_use]
    pub const fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }

    /// The wire/durable mirror of this transition.
    #[must_use]
    pub fn to_record(&self) -> ClaimTrustTransitionRecord {
        ClaimTrustTransitionRecord {
            schema_version: self.schema_version,
            claim_id: self.claim_id.clone(),
            operation_id: self.operation_id.clone(),
            ordinal: self.ordinal,
            from: self.from,
            to: self.to,
            actor: self.actor.clone(),
            provenance: self.provenance.clone(),
            reason: self.reason.clone(),
            evidence: self.evidence.clone(),
            gate: self.gate.clone(),
            occurred_at: self.occurred_at,
            policy: self.policy.clone(),
        }
    }

    /// Rebuilds a transition from a persisted record, failing closed on an
    /// unsupported schema version or an out-of-bounds field.
    pub fn restore(record: ClaimTrustTransitionRecord) -> ClaimResult<Self> {
        check_schema_window(
            ClaimRecordKind::TrustTransition,
            record.schema_version,
            CURRENT_CLAIM_TRUST_TRANSITION_SCHEMA_VERSION,
        )?;
        let transition = Self {
            schema_version: record.schema_version,
            claim_id: record.claim_id,
            operation_id: record.operation_id,
            ordinal: record.ordinal,
            from: record.from,
            to: record.to,
            actor: record.actor,
            provenance: record.provenance,
            reason: record.reason,
            evidence: record.evidence,
            gate: record.gate,
            occurred_at: record.occurred_at,
            policy: record.policy,
        };
        transition.validate()?;
        Ok(transition)
    }

    /// Validates the transition's legality and every bound.
    pub fn validate(&self) -> ClaimResult<()> {
        if !self.from.may_transition_to(self.to) {
            return Err(ClaimError::IllegalTransition {
                claim_id: self.claim_id.clone(),
                from: self.from,
                to: self.to,
            });
        }
        if self.ordinal == 0 {
            return Err(ClaimError::Encoding {
                message: "a transition ordinal is one-based; zero is not a history position"
                    .to_string(),
            });
        }
        if let Some(reason) = &self.reason {
            if reason.len() > CLAIM_TRANSITION_REASON_MAX_LENGTH {
                return Err(ClaimError::ReferenceTooLong {
                    field: "reason",
                    length: reason.len(),
                    maximum: CLAIM_TRANSITION_REASON_MAX_LENGTH,
                });
            }
        }
        if self.evidence.len() > CLAIM_MAX_EVIDENCE_ARTIFACTS {
            return Err(ClaimError::EvidenceOverflow {
                count: self.evidence.len(),
                maximum: CLAIM_MAX_EVIDENCE_ARTIFACTS,
            });
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for ClaimTrustTransition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let record = ClaimTrustTransitionRecord::deserialize(deserializer)?;
        Self::restore(record).map_err(serde::de::Error::custom)
    }
}

/// The wire/durable mirror of a [`ClaimTrustTransition`], every field public.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaimTrustTransitionRecord {
    /// Schema version the record carries.
    pub schema_version: StateSchemaVersion,
    /// The claim the transition moves.
    pub claim_id: ClaimId,
    /// The idempotent key of this transition.
    pub operation_id: ClaimOperationId,
    /// One-based history position.
    pub ordinal: u32,
    /// Trust status before.
    pub from: ClaimTrustStatus,
    /// Trust status after.
    pub to: ClaimTrustStatus,
    /// The deciding principal.
    pub actor: PrincipalRef,
    /// Agent provenance, when any.
    #[serde(default)]
    pub provenance: Option<ClaimProvenance>,
    /// Bounded free-text reason, when any.
    #[serde(default)]
    pub reason: Option<String>,
    /// Evidence artifact references.
    #[serde(default)]
    pub evidence: Vec<ArtifactRef>,
    /// The promotion gate's receipt, when the gate fired.
    #[serde(default)]
    pub gate: Option<ClaimPromotionReceipt>,
    /// When the transition was decided.
    pub occurred_at: AgentTimestampMillis,
    /// The policy in force, when any.
    #[serde(default)]
    pub policy: Option<AgentPolicyRef>,
}

/// One trust-transition request, as the store's
/// [`crate::store::KnowledgeGraphStore::transition`] receives it.
///
/// The `from` status is deliberately absent: the store derives it from the
/// claim's current durable state, so a stale caller cannot describe a
/// transition the claim is no longer in a position to make.
#[derive(Debug, Clone, PartialEq)]
pub struct ClaimTrustTransitionRequest {
    /// The claim to transition.
    pub claim_id: ClaimId,
    /// The idempotent key of this transition.
    pub operation_id: ClaimOperationId,
    /// The trust status to move to.
    pub to: ClaimTrustStatus,
    /// The principal deciding the transition.
    pub actor: PrincipalRef,
    /// Agent provenance, when an agent or run drives the transition.
    pub provenance: Option<ClaimProvenance>,
    /// Bounded free-text reason, when any.
    pub reason: Option<String>,
    /// Evidence artifact references supporting the transition.
    pub evidence: Vec<ArtifactRef>,
    /// When the transition was decided.
    pub occurred_at: AgentTimestampMillis,
    /// The policy the transition is decided under, when any.
    pub policy: Option<AgentPolicyRef>,
    /// Promotion evidence, required when `to` is `Verified` and policy marks
    /// the claim consequential.
    pub promotion: Option<Box<ClaimPromotionEvidence>>,
}

impl ClaimTrustTransitionRequest {
    /// Creates a minimal transition request.
    #[must_use]
    pub const fn new(
        claim_id: ClaimId,
        operation_id: ClaimOperationId,
        to: ClaimTrustStatus,
        actor: PrincipalRef,
        occurred_at: AgentTimestampMillis,
    ) -> Self {
        Self {
            claim_id,
            operation_id,
            to,
            actor,
            provenance: None,
            reason: None,
            evidence: Vec::new(),
            occurred_at,
            policy: None,
            promotion: None,
        }
    }

    /// Attaches promotion evidence for a gated `Verified` target.
    #[must_use]
    pub fn with_promotion(mut self, promotion: ClaimPromotionEvidence) -> Self {
        self.promotion = Some(Box::new(promotion));
        self
    }
}

/// The applied transition and the claim as it stands after it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaimTransitionOutcome {
    /// The claim after the transition.
    pub claim: Claim,
    /// The transition that was appended.
    pub transition: ClaimTrustTransition,
}

#[cfg(test)]
mod tests {
    use rakka_agent::{AgentId, KnowledgeSpaceId, TenantId};

    use crate::scope::KnowledgeSpaceScope;

    use super::*;

    fn scope() -> KnowledgeSpaceScope {
        KnowledgeSpaceScope::new(
            TenantId::new("acme"),
            KnowledgeSpaceId::new("support-kb").expect("the space id is valid"),
        )
        .expect("the scope is valid")
    }

    fn actor() -> PrincipalRef {
        PrincipalRef {
            principal_type: "user".to_string(),
            principal_id: "reviewer".to_string(),
            display_name: None,
        }
    }

    fn transition(to: ClaimTrustStatus) -> ClaimResult<ClaimTrustTransition> {
        let claim_id = ClaimId::new("claim-1").expect("the claim id is valid");
        let operation_id = ClaimOperationId::derive_transition(&scope(), &claim_id, "t-1")
            .expect("the operation id derives");
        ClaimTrustTransition::new(
            claim_id,
            operation_id,
            1,
            ClaimTrustStatus::Proposed,
            to,
            actor(),
            AgentTimestampMillis::new(5),
        )
    }

    #[test]
    fn a_transition_record_round_trips_and_fails_closed() {
        let disputed = transition(ClaimTrustStatus::Disputed).expect("the transition is legal");
        let json = serde_json::to_string(&disputed).expect("the transition serializes");
        let restored: ClaimTrustTransition =
            serde_json::from_str(&json).expect("the transition deserializes");
        assert_eq!(restored, disputed);

        // An illegal pair cannot be constructed, restored, or decoded.
        let mut forged = disputed.to_record();
        forged.from = ClaimTrustStatus::Retracted;
        forged.to = ClaimTrustStatus::Verified;
        assert_eq!(
            ClaimTrustTransition::restore(forged.clone())
                .expect_err("a terminal source is refused")
                .code(),
            "claim-transition-illegal"
        );
        let json = serde_json::to_string(&forged).expect("the record serializes");
        assert!(serde_json::from_str::<ClaimTrustTransition>(&json).is_err());

        // A newer schema version fails closed.
        let mut ahead = disputed.to_record();
        ahead.schema_version =
            StateSchemaVersion::new(CURRENT_CLAIM_TRUST_TRANSITION_SCHEMA_VERSION.get() + 1);
        assert_eq!(
            ClaimTrustTransition::restore(ahead)
                .expect_err("a newer schema version is refused")
                .code(),
            "schema-version-ahead"
        );
    }

    #[test]
    fn transition_bounds_are_enforced() {
        assert_eq!(
            transition(ClaimTrustStatus::Disputed)
                .expect("the transition is legal")
                .with_reason("x".repeat(CLAIM_TRANSITION_REASON_MAX_LENGTH + 1))
                .expect_err("an oversized reason is refused")
                .code(),
            "claim-reference-too-long"
        );

        let claim_id = ClaimId::new("claim-1").expect("the claim id is valid");
        let operation_id = ClaimOperationId::derive_transition(&scope(), &claim_id, "t-0")
            .expect("the operation id derives");
        assert_eq!(
            ClaimTrustTransition::new(
                claim_id,
                operation_id,
                0,
                ClaimTrustStatus::Proposed,
                ClaimTrustStatus::Disputed,
                actor(),
                AgentTimestampMillis::new(5),
            )
            .expect_err("a zero ordinal is refused")
            .code(),
            "claim-encoding-failed"
        );
    }

    #[test]
    fn a_request_carries_no_from_status() {
        // The compile-time shape is the assertion: the request names only the
        // target status, and the store derives the source from durable state.
        let claim_id = ClaimId::new("claim-1").expect("the claim id is valid");
        let operation_id = ClaimOperationId::derive_transition(&scope(), &claim_id, "t-1")
            .expect("the operation id derives");
        let mut request = ClaimTrustTransitionRequest::new(
            claim_id,
            operation_id,
            ClaimTrustStatus::Disputed,
            actor(),
            AgentTimestampMillis::new(5),
        );
        request.provenance = Some(ClaimProvenance::for_agent(
            AgentId::new("scout").expect("the agent id is valid"),
        ));
        assert_eq!(request.to, ClaimTrustStatus::Disputed);
    }
}
