//! Durable checkpoints and human-in-the-loop gates.
//!
//! Owns the three checkpoint kinds — [`AgentCheckpointKind::Approval`],
//! [`AgentCheckpointKind::SecurityAuthorization`], and
//! [`AgentCheckpointKind::IndeterminateEffectReconciliation`] — and the
//! checkpoint record itself ([specification 12](../../../docs/plans/rakka-agent/spec.md)).
//! All three share one durable substrate: a checkpoint is state a run commits
//! and then passivates behind, an incoming decision resolves it, and duplicate
//! decisions never resolve it twice. Their resolver policies and A2A
//! projections differ; the wait, notification, timer, and dedup machinery does
//! not.
//!
//! # Grant binding is cryptographic and exact
//!
//! An [`AgentCheckpointKind::Approval`] or
//! [`AgentCheckpointKind::SecurityAuthorization`] resolution produces an
//! [`AgentCheckpointGrant`] bound to the exact effect intent: tenant, goal,
//! task, agent, run, effect id and generation, target, the **cryptographic**
//! ([`AgentContentDigest::sha256_of_json`]) argument digest, safety class,
//! settings/policy revision, resolver, expiry, and allowed use count
//! ([specification 12.3](../../../docs/plans/rakka-agent/spec.md)). The grant
//! never binds the FNV fingerprint of [`crate::task::AgentContentDigest`]'s
//! default constructor: only a collision-resistant digest can decide whether an
//! approval still binds the arguments a dispatch is about to send. A changed
//! argument, target, generation, or credential binding produces a different
//! digest or a mismatch, so [`AgentCheckpointGrant::validate_for`] invalidates
//! the grant before dispatch — the dispatcher rechecks it, and the current
//! immediate-safety policy, on every attempt.
//!
//! # The reconciliation decision set has no plain `Retry`
//!
//! An [`AgentCheckpointKind::IndeterminateEffectReconciliation`] checkpoint
//! resolves through [`AgentReconciliationDecision`], whose set is exactly the
//! one [specification 12.5](../../../docs/plans/rakka-agent/spec.md) allows:
//! `ConfirmedCompleted`, `ConfirmedNotExecuted`, `Compensate`, `Escalate`, and
//! `AbandonAndFail`. There is deliberately no generic `Retry`: an ambiguous
//! non-idempotent effect is never mutated back into a routine attempt.
//! `ConfirmedNotExecuted` is the only decision that authorizes a new effect
//! generation, and it does so through the effect layer's
//! [`AgentEffectResolution`], never by replaying the old invocation.
//!
//! # Waits passivate; timers never auto-approve
//!
//! Once a checkpoint is durable, no live task is required: the run passivates
//! and a later decision, timer, cancellation, or administrative command
//! reactivates it. [`AgentCheckpoint::on_timer`] drives SLA escalation and hard
//! expiration from durable timestamps, and it can only escalate or expire a
//! waiting checkpoint — it can never produce a grant or an approval. Sensitive
//! or non-idempotent work that no one decided in time fails closed
//! ([specification 12.6](../../../docs/plans/rakka-agent/spec.md)).
//!
//! Specification: section 12. Filled by slice 1.10.

use std::collections::{BTreeSet, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_agent_workflow::{
    AgentAuditEventId, AgentEffectId, AgentTelemetryContext, AgentTimestampMillis,
    AgentTraceContext, ArtifactRef, HumanCheckpointId, PrincipalRef, StateSchemaVersion,
};
use serde::{Deserialize, Serialize};

use crate::definition::{
    AgentCapabilityId, AgentCredentialBindingRef, AgentEffectSafetyClass, AgentPolicyRef,
    AgentRevisionNumber,
};
use crate::effect::{AgentEffectGeneration, AgentEffectResolution, AgentRunEffect};
use crate::identity::{
    AgentDelegationId, AgentGoalId, AgentOperationId, AgentRunScope, AgentTaskId,
};
use crate::schema::{
    AgentRecordKind, VersionedAgentRecord, CURRENT_AGENT_CHECKPOINT_SCHEMA_VERSION,
};
use crate::task::AgentContentDigest;

/// Longest bounded checkpoint prompt/decision summary, in characters.
pub const AGENT_CHECKPOINT_SUMMARY_MAX_LENGTH: usize = 1024;

/// Longest bounded reason, comment, or detail string, in characters.
pub const AGENT_CHECKPOINT_DETAIL_MAX_LENGTH: usize = 512;

/// Longest bounded required-role string, in characters.
pub const AGENT_CHECKPOINT_ROLE_MAX_LENGTH: usize = 128;

/// Most required roles one checkpoint may carry.
pub const AGENT_CHECKPOINT_MAX_ROLES: usize = 16;

/// Most required capabilities one checkpoint may carry.
pub const AGENT_CHECKPOINT_MAX_CAPABILITIES: usize = 32;

/// Most immutable context/evidence artifact references one checkpoint may carry.
pub const AGENT_CHECKPOINT_MAX_CONTEXT_ARTIFACTS: usize = 16;

/// Most immutable audit-event references one checkpoint may carry.
pub const AGENT_CHECKPOINT_MAX_AUDIT_EVENTS: usize = 32;

/// Most recent decision keys retained for replay deduplication.
pub const AGENT_CHECKPOINT_APPLIED_KEY_CAPACITY: usize = 8;

crate::identity::validated_id! {
    /// Opaque reference to the application-defined compensation effect a
    /// [`AgentReconciliationDecision::Compensate`] decision schedules
    /// ([specification 12.5](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Rakka persists and routes the reference; the application owns the
    /// compensation behind it. A compensation is an *explicitly defined* effect,
    /// never an automatic reversal Rakka invents.
    pub AgentCompensationRef, "agent_compensation_ref"
}

/// The deployment-configured SLA and expiration deadlines a run stamps onto an
/// approval checkpoint it opens
/// ([specification 12.6](../../../docs/plans/rakka-agent/spec.md)).
///
/// The deadlines are durable: they live on the checkpoint record, survive
/// passivation, and are enforced by [`AgentCheckpoint::on_timer`]. All fields
/// default to `None`, which is a checkpoint that waits indefinitely for a
/// decision and never expires — the fail-closed default, since a timeout must
/// never auto-approve.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AgentCheckpointSla {
    /// Milliseconds after opening at which the checkpoint becomes overdue and
    /// escalates, when an escalation target is set.
    pub due_after_ms: Option<u64>,
    /// Milliseconds after opening at which the checkpoint hard-expires.
    pub expire_after_ms: Option<u64>,
    /// The escalation target notified at the SLA deadline.
    pub escalation_target: Option<String>,
}

impl AgentCheckpointSla {
    /// Whether any deadline is configured.
    #[must_use]
    pub const fn is_set(&self) -> bool {
        self.due_after_ms.is_some() || self.expire_after_ms.is_some()
    }

    /// Resolves the due and expiration instants relative to `opened_at`.
    #[must_use]
    pub fn deadlines(
        &self,
        opened_at: AgentTimestampMillis,
    ) -> (Option<AgentTimestampMillis>, Option<AgentTimestampMillis>) {
        let at =
            |offset: u64| AgentTimestampMillis::new(opened_at.as_millis().saturating_add(offset));
        (self.due_after_ms.map(at), self.expire_after_ms.map(at))
    }
}

/// The three durable checkpoint kinds
/// ([specification 12.1](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentCheckpointKind {
    /// A principal decides whether an already-defined effect may run.
    Approval,
    /// A principal or authorization service supplies a capability or credential
    /// binding the effect needs.
    SecurityAuthorization,
    /// An operator supplies evidence about an effect whose outcome cannot be
    /// established automatically.
    IndeterminateEffectReconciliation,
}

impl AgentCheckpointKind {
    /// Stable kebab-case label for errors, logs, and metrics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Approval => "approval",
            Self::SecurityAuthorization => "security-authorization",
            Self::IndeterminateEffectReconciliation => "indeterminate-effect-reconciliation",
        }
    }

    /// Whether a resolution of this kind is an approval-family decision
    /// ([`AgentApprovalDecision`]) rather than a reconciliation decision.
    #[must_use]
    pub const fn is_approval_family(self) -> bool {
        matches!(self, Self::Approval | Self::SecurityAuthorization)
    }
}

impl Display for AgentCheckpointKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// The lifecycle status of one checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentCheckpointStatus {
    /// Waiting for a decision.
    Open,
    /// Overdue and escalated to its escalation target, still waiting.
    Escalated,
    /// Resolved with an authorizing or reconciling decision.
    Resolved,
    /// Denied; the bound effect must not dispatch.
    Denied,
    /// Resolved with a scheduled compensation effect.
    Compensated,
    /// Abandoned; the bound effect's generation fails terminally.
    Abandoned,
    /// Timed out without a decision. It never auto-approves.
    Expired,
    /// Cancelled because the run was cancelled, superseded, or terminated.
    Cancelled,
}

impl AgentCheckpointStatus {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Escalated => "escalated",
            Self::Resolved => "resolved",
            Self::Denied => "denied",
            Self::Compensated => "compensated",
            Self::Abandoned => "abandoned",
            Self::Expired => "expired",
            Self::Cancelled => "cancelled",
        }
    }

    /// Whether the checkpoint is still awaiting a resolving decision.
    #[must_use]
    pub const fn is_waiting(self) -> bool {
        matches!(self, Self::Open | Self::Escalated)
    }

    /// Whether the checkpoint has reached a status no decision can change.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !self.is_waiting()
    }
}

impl Display for AgentCheckpointStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// One available decision, as a checkpoint advertises it
/// ([specification 12.2](../../../docs/plans/rakka-agent/spec.md): allowed
/// decisions).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCheckpointDecisionOption {
    /// Stable decision value.
    pub value: String,
    /// Whether selecting this decision requires a comment.
    pub requires_comment: bool,
}

impl AgentCheckpointDecisionOption {
    /// Creates a decision option.
    #[must_use]
    pub fn new(value: impl Into<String>, requires_comment: bool) -> Self {
        Self {
            value: value.into(),
            requires_comment,
        }
    }
}

/// The exact effect intent a checkpoint binds
/// ([specification 12.2](../../../docs/plans/rakka-agent/spec.md),
/// [12.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// The `argument_digest` is the cryptographic
/// ([`AgentContentDigest::sha256_of_json`]) digest of the effect's canonical
/// arguments, computed when the checkpoint is opened. It is never the FNV
/// fingerprint the effect carries in [`AgentRunEffect::argument_digest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCheckpointEffectBinding {
    /// The effect the checkpoint gates.
    pub effect_id: AgentEffectId,
    /// The effect generation the checkpoint gates.
    pub generation: AgentEffectGeneration,
    /// The dispatch target, as the intent names it (`"{target_type}:{name}"`).
    pub target: String,
    /// The cryptographic digest of the exact arguments the checkpoint is about.
    pub argument_digest: AgentContentDigest,
    /// The safety class the checkpoint gates.
    pub safety_class: AgentEffectSafetyClass,
    /// The logical credential binding the effect resolves, when any.
    pub credential_binding: Option<AgentCredentialBindingRef>,
}

impl AgentCheckpointEffectBinding {
    /// Derives the binding from a durable effect intent, computing the
    /// cryptographic argument digest.
    pub fn of_effect(effect: &AgentRunEffect) -> AgentCheckpointResult<Self> {
        let argument_digest = effect
            .request
            .cryptographic_argument_digest()
            .map_err(|error| AgentCheckpointError::InvalidBinding {
                message: format!("the effect arguments could not be digested: {error}"),
            })?;
        let target = effect.request.target();
        Ok(Self {
            effect_id: effect.effect_id.clone(),
            generation: effect.generation,
            target: format!("{}:{}", target.target_type, target.name),
            argument_digest,
            safety_class: effect.safety.class(),
            credential_binding: effect.credential_binding.clone(),
        })
    }
}

/// The current authorization to execute one exact effect intent, as a resolved
/// [`AgentCheckpointKind::Approval`] or
/// [`AgentCheckpointKind::SecurityAuthorization`] checkpoint records it
/// ([specification 12.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// It carries no resolved credential and no secret material: only a logical
/// credential binding reference, resolved at dispatch by the application-owned
/// resolver. It binds the full identity of what it authorizes, plus an expiry
/// and an allowed use count, so a grant can never quietly outlive or outspend
/// the decision it records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCheckpointGrant {
    /// The checkpoint that produced the grant.
    pub checkpoint_id: HumanCheckpointId,
    /// The checkpoint kind that produced the grant.
    pub kind: AgentCheckpointKind,
    /// The run the grant covers.
    pub scope: AgentRunScope,
    /// The task the run serves.
    pub task: Option<AgentTaskId>,
    /// The goal the run contributes to.
    pub goal: Option<AgentGoalId>,
    /// The effect the grant covers.
    pub effect_id: AgentEffectId,
    /// The effect generation the grant covers.
    pub generation: AgentEffectGeneration,
    /// The dispatch target, as the intent names it.
    pub target: String,
    /// The cryptographic digest of the exact arguments the grant authorizes.
    pub argument_digest: AgentContentDigest,
    /// The safety class the grant authorizes.
    pub safety_class: AgentEffectSafetyClass,
    /// The settings revision in force when the checkpoint was opened.
    pub settings_revision: AgentRevisionNumber,
    /// The policy reference the decision was made under, when any.
    pub policy: Option<AgentPolicyRef>,
    /// The logical credential binding the dispatch may resolve, when any.
    pub credential_binding: Option<AgentCredentialBindingRef>,
    /// The authenticated principal that resolved the checkpoint.
    pub resolver: PrincipalRef,
    /// When the grant was issued.
    pub issued_at: AgentTimestampMillis,
    /// When the grant expires.
    pub expires_at: AgentTimestampMillis,
    /// The most attempts the grant covers, normally one.
    pub allowed_use_count: u32,
}

impl AgentCheckpointGrant {
    /// Accepts that this grant covers the given attempt of the exact subject
    /// the binding describes, or fails closed
    /// ([specification 12.3](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The binding MUST be recomputed by the caller from the authoritative
    /// content being gated — never read back from recorded state — so a
    /// subject that changed after the decision can never pass under a stale
    /// digest. This is the effect-independent half of [`Self::validate_for`],
    /// and the seam a non-effect gate (the communal claim promotion gate of
    /// specification 13.4) validates through: the same digest-binding, expiry,
    /// and use-count semantics, one code path.
    pub fn validate_for_binding(
        &self,
        binding: &AgentCheckpointEffectBinding,
        attempt: u32,
        now: AgentTimestampMillis,
    ) -> Result<(), AgentCheckpointGrantError> {
        // Defence in depth: neither side of the comparison may bind a
        // non-cryptographic digest, whatever wrote it.
        if !self.argument_digest.algorithm.is_cryptographic()
            || !binding.argument_digest.algorithm.is_cryptographic()
        {
            return Err(AgentCheckpointGrantError::NonCryptographicDigest);
        }
        if self.effect_id != binding.effect_id
            || self.generation != binding.generation
            || self.target != binding.target
            || self.safety_class != binding.safety_class
        {
            return Err(AgentCheckpointGrantError::IntentMismatch);
        }
        if self.credential_binding != binding.credential_binding {
            return Err(AgentCheckpointGrantError::CredentialBindingChanged);
        }
        if self.argument_digest != binding.argument_digest {
            return Err(AgentCheckpointGrantError::ArgumentDigestMismatch);
        }
        // Strictly after: a grant is valid through its expiry instant.
        if now.as_millis() > self.expires_at.as_millis() {
            return Err(AgentCheckpointGrantError::Expired);
        }
        if attempt > self.allowed_use_count {
            return Err(AgentCheckpointGrantError::UsesExhausted);
        }
        Ok(())
    }

    /// Accepts that this grant covers the given attempt of the given intent, or
    /// fails closed ([specification 12.3](../../../docs/plans/rakka-agent/spec.md):
    /// the dispatcher rechecks grant validity before invocation).
    ///
    /// The argument digest is *recomputed* cryptographically from the intent and
    /// compared, so an argument that changed after the human approved it can
    /// never pass — the fingerprint the effect carries is irrelevant here. The
    /// identity checks run before the digest is computed, so the error
    /// precedence of earlier releases is preserved; the delegated
    /// [`Self::validate_for_binding`] additionally compares the dispatch
    /// target, a pure strengthening (a legitimately resolved grant copies its
    /// target from the same intent).
    pub fn validate_for(
        &self,
        scope: &AgentRunScope,
        intent: &AgentRunEffect,
        attempt: u32,
        now: AgentTimestampMillis,
    ) -> Result<(), AgentCheckpointGrantError> {
        // Defence in depth: a grant must never bind a non-cryptographic digest,
        // whatever wrote it.
        if !self.argument_digest.algorithm.is_cryptographic() {
            return Err(AgentCheckpointGrantError::NonCryptographicDigest);
        }
        if &self.scope != scope
            || self.effect_id != intent.effect_id
            || self.generation != intent.generation
            || self.safety_class != intent.safety.class()
        {
            return Err(AgentCheckpointGrantError::IntentMismatch);
        }
        if self.credential_binding != intent.credential_binding {
            return Err(AgentCheckpointGrantError::CredentialBindingChanged);
        }
        let binding = AgentCheckpointEffectBinding::of_effect(intent)
            .map_err(|_| AgentCheckpointGrantError::ArgumentDigestUncomputable)?;
        self.validate_for_binding(&binding, attempt, now)
    }
}

/// Why a checkpoint grant does not authorize an attempt.
///
/// Each variant carries a stable [`AgentCheckpointGrantError::code`] the dispatch
/// authority surfaces as a `checkpoint-*` refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentCheckpointGrantError {
    /// The grant binds a non-cryptographic digest and may not gate dispatch.
    NonCryptographicDigest,
    /// The grant does not bind the intent being dispatched.
    IntentMismatch,
    /// The intent's credential binding changed after the grant was issued.
    CredentialBindingChanged,
    /// The intent's arguments could not be digested for comparison.
    ArgumentDigestUncomputable,
    /// The grant binds different arguments than the intent carries.
    ArgumentDigestMismatch,
    /// The grant expired before the attempt.
    Expired,
    /// The grant's allowed use count is spent.
    UsesExhausted,
}

impl AgentCheckpointGrantError {
    /// Stable machine-readable reason code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::NonCryptographicDigest => "checkpoint-grant-non-cryptographic",
            Self::IntentMismatch => "checkpoint-grant-intent-mismatch",
            Self::CredentialBindingChanged => "checkpoint-grant-credential-changed",
            Self::ArgumentDigestUncomputable => "checkpoint-argument-digest-uncomputable",
            Self::ArgumentDigestMismatch => "checkpoint-argument-digest-mismatch",
            Self::Expired => "checkpoint-grant-expired",
            Self::UsesExhausted => "checkpoint-grant-uses-exhausted",
        }
    }
}

impl Display for AgentCheckpointGrantError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let detail = match self {
            Self::NonCryptographicDigest => {
                "the checkpoint grant binds a non-cryptographic digest and cannot gate dispatch"
            }
            Self::IntentMismatch => {
                "the checkpoint grant does not bind the intent being dispatched"
            }
            Self::CredentialBindingChanged => {
                "the intent's credential binding changed after the checkpoint was resolved"
            }
            Self::ArgumentDigestUncomputable => {
                "the intent's arguments could not be digested for grant comparison"
            }
            Self::ArgumentDigestMismatch => {
                "the checkpoint grant binds different arguments than the intent carries"
            }
            Self::Expired => "the checkpoint grant expired before the attempt",
            Self::UsesExhausted => "the checkpoint grant's allowed use count is spent",
        };
        f.write_str(detail)
    }
}

impl Error for AgentCheckpointGrantError {}

/// How the resolver of a resolved [`AgentCheckpointKind::Approval`] or
/// [`AgentCheckpointKind::SecurityAuthorization`] checkpoint decided.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentApprovalDecision {
    /// The effect may run. The grant is valid until `expires_at` and covers
    /// `allowed_use_count` attempts. A `credential_binding` accompanies a
    /// [`AgentCheckpointKind::SecurityAuthorization`] resolution that supplies a
    /// logical binding; it is a reference only, never secret material.
    Approve {
        /// The logical credential binding the authorization supplies, when any.
        credential_binding: Option<AgentCredentialBindingRef>,
        /// When the resulting grant expires.
        expires_at: AgentTimestampMillis,
        /// The most attempts the grant covers, normally one.
        allowed_use_count: u32,
    },
    /// The effect must not run. It carries a bounded, non-secret reason.
    Deny {
        /// A bounded, stable reason.
        reason: String,
    },
}

/// How the operator of a resolved
/// [`AgentCheckpointKind::IndeterminateEffectReconciliation`] checkpoint decided
/// ([specification 12.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// There is deliberately no generic `Retry`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentReconciliationDecision {
    /// The invocation happened; the effect resolution carries its authoritative
    /// outcome ([`AgentEffectResolution::ConfirmedExecuted`]).
    ConfirmedCompleted {
        /// The effect-layer resolution recording the established outcome.
        resolution: Box<AgentEffectResolution>,
    },
    /// The invocation provably never happened; a new effect generation is
    /// authorized where the run still wants the work.
    ConfirmedNotExecuted,
    /// Schedule an explicitly defined compensation effect.
    Compensate {
        /// The application-defined compensation to schedule.
        compensation: AgentCompensationRef,
    },
    /// Escalate to the checkpoint's escalation target; the checkpoint stays
    /// nonterminal and a later decision resolves it.
    Escalate,
    /// Abandon the ambiguous effect; the run fails terminally.
    AbandonAndFail,
}

impl AgentReconciliationDecision {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(&self) -> &'static str {
        match self {
            Self::ConfirmedCompleted { .. } => "confirmed-completed",
            Self::ConfirmedNotExecuted => "confirmed-not-executed",
            Self::Compensate { .. } => "compensate",
            Self::Escalate => "escalate",
            Self::AbandonAndFail => "abandon-and-fail",
        }
    }

    /// The effect-layer resolution this decision maps to, when it resolves the
    /// effect directly. `Compensate`, `Escalate`, and `AbandonAndFail` have
    /// checkpoint-level semantics and return `None`.
    #[must_use]
    pub fn effect_resolution(&self) -> Option<AgentEffectResolution> {
        match self {
            Self::ConfirmedCompleted { resolution } => Some((**resolution).clone()),
            Self::ConfirmedNotExecuted => Some(AgentEffectResolution::ConfirmedNotExecuted),
            Self::Compensate { .. } | Self::Escalate | Self::AbandonAndFail => None,
        }
    }
}

/// A decision submitted against a checkpoint: the approval-family decision or
/// the reconciliation decision, whichever the kind expects.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentCheckpointDecision {
    /// An approval-family decision, for an [`AgentCheckpointKind::Approval`] or
    /// [`AgentCheckpointKind::SecurityAuthorization`] checkpoint.
    Approval(AgentApprovalDecision),
    /// A reconciliation decision, for an
    /// [`AgentCheckpointKind::IndeterminateEffectReconciliation`] checkpoint.
    Reconciliation(AgentReconciliationDecision),
}

/// What resolving a checkpoint yielded, for the run to act on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentCheckpointOutcome {
    /// The bound effect is authorized under the carried grant.
    Granted(Box<AgentCheckpointGrant>),
    /// The bound effect is denied and must not dispatch.
    Denied {
        /// The bounded, non-secret reason.
        reason: String,
    },
    /// The reconciliation resolved the effect through the effect layer.
    EffectResolution(Box<AgentEffectResolution>),
    /// The reconciliation scheduled a compensation effect.
    Compensate {
        /// The compensation to schedule.
        compensation: AgentCompensationRef,
    },
    /// The checkpoint escalated and stays nonterminal.
    Escalated,
    /// The reconciliation abandoned the effect; the run fails terminally.
    Abandoned,
    /// The checkpoint expired without a decision. It never auto-approves.
    Expired,
    /// The checkpoint was cancelled with the run.
    Cancelled,
}

/// The result of one [`AgentCheckpoint::resolve`] call.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentCheckpointResolutionReport {
    /// What the resolution yielded.
    pub outcome: AgentCheckpointOutcome,
    /// Whether the decision key had already been applied, so this call produced
    /// no second transition ([specification 18](../../../docs/plans/rakka-agent/spec.md)
    /// scenario 11).
    pub deduplicated: bool,
}

/// What a durable timer sweep did to a waiting checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentCheckpointTimerOutcome {
    /// Nothing was due; the checkpoint is unchanged.
    Pending,
    /// The checkpoint became overdue and escalated to its escalation target.
    Escalated,
    /// The checkpoint expired without a decision.
    Expired,
}

/// A resolving decision recorded on a checkpoint, with its deduplication key and
/// resolver.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRecordedDecision {
    /// The decision that was applied.
    pub decision: AgentCheckpointDecision,
    /// The stable operation id the decision deduplicated on.
    pub decision_key: AgentOperationId,
    /// The authenticated principal that resolved the checkpoint.
    pub resolver: PrincipalRef,
    /// When the decision was applied.
    pub resolved_at: AgentTimestampMillis,
}

/// A durable HITL checkpoint record
/// ([specification 12.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// It carries no secret material: the prompt/decision summary is bounded and
/// non-secret, and a credential appears only as a logical binding reference.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentCheckpoint {
    schema_version: StateSchemaVersion,
    /// Stable checkpoint id.
    pub checkpoint_id: HumanCheckpointId,
    /// The checkpoint kind.
    pub kind: AgentCheckpointKind,
    /// The run scope: tenant, agent, and run identity.
    pub scope: AgentRunScope,
    /// The task the run serves, when applicable.
    pub task: Option<AgentTaskId>,
    /// The goal the run contributes to, when applicable.
    pub goal: Option<AgentGoalId>,
    /// The delegation identity, when the run is a delegated child.
    pub delegation: Option<AgentDelegationId>,
    /// Bounded, non-secret prompt/decision summary.
    pub summary: String,
    /// The decisions this checkpoint advertises as allowed.
    pub allowed_decisions: Vec<AgentCheckpointDecisionOption>,
    /// Required roles to resolve the checkpoint.
    pub required_roles: Vec<String>,
    /// Required capabilities to resolve the checkpoint.
    pub required_capabilities: BTreeSet<AgentCapabilityId>,
    /// The policy reference governing the checkpoint, when any.
    pub policy: Option<AgentPolicyRef>,
    /// The exact effect intent the checkpoint binds.
    pub bound_effect: AgentCheckpointEffectBinding,
    /// The settings revision in force when the checkpoint was opened.
    pub settings_revision: AgentRevisionNumber,
    /// The definition (policy) revision in force when the checkpoint was opened.
    pub definition_revision: AgentRevisionNumber,
    /// When the checkpoint was created.
    pub created_at: AgentTimestampMillis,
    /// When the checkpoint becomes overdue and escalates, when set.
    pub due_at: Option<AgentTimestampMillis>,
    /// When the checkpoint hard-expires, when set.
    pub expires_at: Option<AgentTimestampMillis>,
    /// The escalation target, when set.
    pub escalation_target: Option<String>,
    /// Immutable context/evidence artifact references.
    pub context_artifacts: Vec<ArtifactRef>,
    /// The principal that created the checkpoint.
    pub created_by: PrincipalRef,
    /// The principal that resolved the checkpoint, when resolved.
    pub resolved_by: Option<PrincipalRef>,
    /// The current status.
    pub status: AgentCheckpointStatus,
    /// The resolving decision, when the checkpoint reached a terminal status.
    pub decision: Option<AgentRecordedDecision>,
    /// Immutable audit-event references.
    pub audit_event_ids: Vec<AgentAuditEventId>,
    /// The decision keys already applied, for replay deduplication.
    applied_keys: VecDeque<AgentOperationId>,
    /// When the record last changed.
    pub updated_at: AgentTimestampMillis,
    /// Trace context of the segment that opened the checkpoint — the parked
    /// span the resolution segment links back to, alongside its link to the
    /// resolving request, which is how a resume carries both causes
    /// ([specification 17.11](../../../docs/plans/rakka-agent/spec.md)).
    /// Observability only, never correctness: a checkpoint persisted before
    /// this field decodes to the empty context, and resolution never reads it.
    #[serde(default)]
    pub telemetry: AgentTelemetryContext,
}

impl AgentCheckpoint {
    /// The durable span identity of the `checkpoint-open` segment that parks
    /// `checkpoint_id`, derived from the gated effect's trace context.
    ///
    /// Stored on the record at open as [`Self::telemetry`], so the resolution
    /// can link back to the parked span
    /// ([specification 17.11](../../../docs/plans/rakka-agent/spec.md)).
    #[must_use]
    pub fn parked_span_identity(
        effect_telemetry: &AgentTelemetryContext,
        checkpoint_id: &HumanCheckpointId,
    ) -> Option<AgentTraceContext> {
        crate::observability::agent_durable_span_identity(
            effect_telemetry,
            &["checkpoint-open", checkpoint_id.as_str()],
        )
    }

    /// The durable span identity of the `checkpoint-resolve` segment that
    /// will resolve `checkpoint_id`, derived from the gated effect's trace
    /// context — before the resolution exists, which is what lets an
    /// indeterminate park link *forward* to the reconciliation decision it
    /// waits for ([specification 17.9](../../../docs/plans/rakka-agent/spec.md)).
    #[must_use]
    pub fn resolve_span_identity(
        effect_telemetry: &AgentTelemetryContext,
        checkpoint_id: &HumanCheckpointId,
    ) -> Option<AgentTraceContext> {
        crate::observability::agent_durable_span_identity(
            effect_telemetry,
            &["checkpoint-resolve", checkpoint_id.as_str()],
        )
    }

    /// The stable, derived id of the checkpoint of `kind` gating one effect
    /// generation, so a re-driven transition opens the same checkpoint rather
    /// than a second one.
    ///
    /// The kind is folded in because one generation can wait on more than one
    /// kind over its life — an approval before dispatch, a reconciliation after
    /// an ambiguous loss — and the two are different records. The derivation is
    /// public because the dispatcher names the reconciliation checkpoint an
    /// indeterminate park will open *before* the run opens it: the park's span
    /// links forward to the decision that resolves that checkpoint
    /// ([specification 17.9](../../../docs/plans/rakka-agent/spec.md)), which
    /// is only possible when both sides derive the same id from the effect.
    #[must_use]
    pub fn id_for_effect(
        effect_id: &AgentEffectId,
        generation: AgentEffectGeneration,
        kind: AgentCheckpointKind,
    ) -> HumanCheckpointId {
        let tag = match kind {
            AgentCheckpointKind::Approval => "approval",
            AgentCheckpointKind::SecurityAuthorization => "authz",
            AgentCheckpointKind::IndeterminateEffectReconciliation => "reconcile",
        };
        HumanCheckpointId::new(format!(
            "{}#ck-{tag}-g{}",
            effect_id.as_str(),
            generation.get()
        ))
    }

    /// Opens a checkpoint of `kind` bound to `effect`, with the default allowed
    /// decision set for the kind.
    ///
    /// Identity, deadlines, roles, policy, and context are attached through the
    /// `with_*` builders. The checkpoint starts [`AgentCheckpointStatus::Open`].
    pub fn open(
        checkpoint_id: HumanCheckpointId,
        kind: AgentCheckpointKind,
        scope: AgentRunScope,
        effect: &AgentRunEffect,
        summary: impl Into<String>,
        created_by: PrincipalRef,
        created_at: AgentTimestampMillis,
    ) -> AgentCheckpointResult<Self> {
        let bound_effect = AgentCheckpointEffectBinding::of_effect(effect)?;
        let checkpoint = Self {
            schema_version: CURRENT_AGENT_CHECKPOINT_SCHEMA_VERSION,
            checkpoint_id,
            kind,
            scope,
            task: None,
            goal: None,
            delegation: None,
            summary: bounded(summary, AGENT_CHECKPOINT_SUMMARY_MAX_LENGTH),
            allowed_decisions: default_decisions(kind),
            required_roles: Vec::new(),
            required_capabilities: BTreeSet::new(),
            policy: None,
            bound_effect,
            settings_revision: effect.settings_revision,
            definition_revision: AgentRevisionNumber::INITIAL,
            created_at,
            due_at: None,
            expires_at: None,
            escalation_target: None,
            context_artifacts: Vec::new(),
            created_by,
            resolved_by: None,
            status: AgentCheckpointStatus::Open,
            decision: None,
            audit_event_ids: Vec::new(),
            applied_keys: VecDeque::new(),
            updated_at: created_at,
            telemetry: AgentTelemetryContext::default(),
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    /// Stamps the trace context of the segment opening this checkpoint.
    ///
    /// The context is admitted through
    /// [`crate::observability::sanitize_agent_telemetry_context`]: strict on
    /// write so the read side never has to fail closed over telemetry.
    #[must_use]
    pub fn with_telemetry(mut self, telemetry: AgentTelemetryContext) -> Self {
        self.telemetry = crate::observability::sanitize_agent_telemetry_context(telemetry);
        self
    }

    /// Attaches the task the run serves.
    #[must_use]
    pub fn with_task(mut self, task: AgentTaskId) -> Self {
        self.task = Some(task);
        self
    }

    /// Attaches the goal the run contributes to.
    #[must_use]
    pub fn with_goal(mut self, goal: AgentGoalId) -> Self {
        self.goal = Some(goal);
        self
    }

    /// Attaches the delegation identity of a delegated child run.
    #[must_use]
    pub fn with_delegation(mut self, delegation: AgentDelegationId) -> Self {
        self.delegation = Some(delegation);
        self
    }

    /// Records the settings and definition (policy) revisions in force.
    #[must_use]
    pub const fn with_revisions(
        mut self,
        settings_revision: AgentRevisionNumber,
        definition_revision: AgentRevisionNumber,
    ) -> Self {
        self.settings_revision = settings_revision;
        self.definition_revision = definition_revision;
        self
    }

    /// Attaches the policy reference governing the checkpoint.
    #[must_use]
    pub fn with_policy(mut self, policy: AgentPolicyRef) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Adds a required role, bounded and capped.
    #[must_use]
    pub fn with_required_role(mut self, role: impl Into<String>) -> Self {
        if self.required_roles.len() < AGENT_CHECKPOINT_MAX_ROLES {
            self.required_roles
                .push(bounded(role, AGENT_CHECKPOINT_ROLE_MAX_LENGTH));
        }
        self
    }

    /// Adds a required capability, capped.
    #[must_use]
    pub fn with_required_capability(mut self, capability: AgentCapabilityId) -> Self {
        if self.required_capabilities.len() < AGENT_CHECKPOINT_MAX_CAPABILITIES {
            self.required_capabilities.insert(capability);
        }
        self
    }

    /// Sets the SLA and hard-expiration deadlines and, optionally, the
    /// escalation target consulted at the SLA deadline.
    #[must_use]
    pub fn with_deadlines(
        mut self,
        due_at: Option<AgentTimestampMillis>,
        expires_at: Option<AgentTimestampMillis>,
        escalation_target: Option<String>,
    ) -> Self {
        self.due_at = due_at;
        self.expires_at = expires_at;
        self.escalation_target =
            escalation_target.map(|target| bounded(target, AGENT_CHECKPOINT_DETAIL_MAX_LENGTH));
        self
    }

    /// Adds an immutable context/evidence artifact reference, capped.
    #[must_use]
    pub fn with_context_artifact(mut self, artifact: ArtifactRef) -> Self {
        if self.context_artifacts.len() < AGENT_CHECKPOINT_MAX_CONTEXT_ARTIFACTS {
            self.context_artifacts.push(artifact);
        }
        self
    }

    /// Adds an immutable audit-event reference, capped.
    #[must_use]
    pub fn with_audit_event(mut self, audit_event_id: AgentAuditEventId) -> Self {
        if self.audit_event_ids.len() < AGENT_CHECKPOINT_MAX_AUDIT_EVENTS {
            self.audit_event_ids.push(audit_event_id);
        }
        self
    }

    /// The schema version this record carries.
    #[must_use]
    pub const fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }

    /// Resolves the checkpoint with a decision deduplicated on `decision_key`.
    ///
    /// A replay carrying an already-applied `decision_key` produces no second
    /// transition and returns the original outcome with
    /// [`AgentCheckpointResolutionReport::deduplicated`] set — so a duplicate
    /// human or authorization decision never resumes a run twice
    /// ([specification 18](../../../docs/plans/rakka-agent/spec.md) scenario 11).
    /// A *different* decision against an already-terminal checkpoint is refused.
    ///
    /// A [`AgentReconciliationDecision::Escalate`] decision keeps the checkpoint
    /// nonterminal, so a later resolving decision (with a new key) is still
    /// accepted.
    pub fn resolve(
        &mut self,
        decision_key: AgentOperationId,
        resolver: PrincipalRef,
        decision: AgentCheckpointDecision,
        now: AgentTimestampMillis,
    ) -> AgentCheckpointResult<AgentCheckpointResolutionReport> {
        if self.applied_keys.contains(&decision_key) {
            // Exact replay of a decision already applied: return the recorded
            // outcome, no second transition.
            let outcome = self.recorded_outcome()?;
            return Ok(AgentCheckpointResolutionReport {
                outcome,
                deduplicated: true,
            });
        }
        if self.status.is_terminal() {
            return Err(AgentCheckpointError::AlreadyResolved {
                status: self.status,
            });
        }
        self.check_decision_kind(&decision)?;

        let outcome = match &decision {
            AgentCheckpointDecision::Approval(approval) => {
                self.apply_approval(approval, &resolver, now)?
            }
            AgentCheckpointDecision::Reconciliation(reconciliation) => {
                self.apply_reconciliation(reconciliation)
            }
        };

        self.record_decision(decision, decision_key, resolver, now);
        Ok(AgentCheckpointResolutionReport {
            outcome,
            deduplicated: false,
        })
    }

    /// Drives SLA escalation and hard expiration from durable timestamps
    /// ([specification 12.6](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// It can only leave a waiting checkpoint waiting, escalate it, or expire
    /// it. It can never resolve, approve, or grant — a timeout on sensitive or
    /// non-idempotent work fails closed.
    pub fn on_timer(&mut self, now: AgentTimestampMillis) -> AgentCheckpointTimerOutcome {
        if self.status.is_terminal() {
            return AgentCheckpointTimerOutcome::Pending;
        }
        // A reconciliation checkpoint never hard-expires, whatever deadline was
        // written onto it: expiry closes the wait without a decision, and an
        // ambiguous effect's outcome does not become known because a timer
        // fired. It can still escalate below.
        if self.kind != AgentCheckpointKind::IndeterminateEffectReconciliation {
            if let Some(expires_at) = self.expires_at {
                if now.as_millis() >= expires_at.as_millis() {
                    self.status = AgentCheckpointStatus::Expired;
                    self.updated_at = now;
                    return AgentCheckpointTimerOutcome::Expired;
                }
            }
        }
        if self.status == AgentCheckpointStatus::Open {
            if let (Some(due_at), Some(_)) = (self.due_at, self.escalation_target.as_ref()) {
                if now.as_millis() >= due_at.as_millis() {
                    self.status = AgentCheckpointStatus::Escalated;
                    self.updated_at = now;
                    return AgentCheckpointTimerOutcome::Escalated;
                }
            }
        }
        AgentCheckpointTimerOutcome::Pending
    }

    /// Cancels a waiting checkpoint because its run was cancelled, superseded, or
    /// terminated. A terminal checkpoint is left unchanged.
    pub fn cancel(&mut self, now: AgentTimestampMillis) {
        if self.status.is_waiting() {
            self.status = AgentCheckpointStatus::Cancelled;
            self.updated_at = now;
        }
    }

    fn apply_approval(
        &mut self,
        approval: &AgentApprovalDecision,
        resolver: &PrincipalRef,
        now: AgentTimestampMillis,
    ) -> AgentCheckpointResult<AgentCheckpointOutcome> {
        match approval {
            AgentApprovalDecision::Approve {
                credential_binding,
                expires_at,
                allowed_use_count,
            } => {
                if *allowed_use_count == 0 {
                    return Err(AgentCheckpointError::InvalidDecision {
                        message: "an approval must authorize at least one attempt".to_string(),
                    });
                }
                let grant = AgentCheckpointGrant {
                    checkpoint_id: self.checkpoint_id.clone(),
                    kind: self.kind,
                    scope: self.scope.clone(),
                    task: self.task.clone(),
                    goal: self.goal.clone(),
                    effect_id: self.bound_effect.effect_id.clone(),
                    generation: self.bound_effect.generation,
                    target: self.bound_effect.target.clone(),
                    argument_digest: self.bound_effect.argument_digest.clone(),
                    safety_class: self.bound_effect.safety_class,
                    settings_revision: self.settings_revision,
                    policy: self.policy.clone(),
                    credential_binding: credential_binding
                        .clone()
                        .or_else(|| self.bound_effect.credential_binding.clone()),
                    resolver: resolver.clone(),
                    issued_at: now,
                    expires_at: *expires_at,
                    allowed_use_count: *allowed_use_count,
                };
                self.status = AgentCheckpointStatus::Resolved;
                Ok(AgentCheckpointOutcome::Granted(Box::new(grant)))
            }
            AgentApprovalDecision::Deny { reason } => {
                self.status = AgentCheckpointStatus::Denied;
                Ok(AgentCheckpointOutcome::Denied {
                    reason: bounded(reason.clone(), AGENT_CHECKPOINT_DETAIL_MAX_LENGTH),
                })
            }
        }
    }

    fn apply_reconciliation(
        &mut self,
        reconciliation: &AgentReconciliationDecision,
    ) -> AgentCheckpointOutcome {
        match reconciliation {
            AgentReconciliationDecision::ConfirmedCompleted { .. }
            | AgentReconciliationDecision::ConfirmedNotExecuted => {
                self.status = AgentCheckpointStatus::Resolved;
                // `effect_resolution` is `Some` for both these variants.
                let resolution = reconciliation
                    .effect_resolution()
                    .expect("a confirming reconciliation decision maps to an effect resolution");
                AgentCheckpointOutcome::EffectResolution(Box::new(resolution))
            }
            AgentReconciliationDecision::Compensate { compensation } => {
                self.status = AgentCheckpointStatus::Compensated;
                AgentCheckpointOutcome::Compensate {
                    compensation: compensation.clone(),
                }
            }
            AgentReconciliationDecision::Escalate => {
                self.status = AgentCheckpointStatus::Escalated;
                AgentCheckpointOutcome::Escalated
            }
            AgentReconciliationDecision::AbandonAndFail => {
                self.status = AgentCheckpointStatus::Abandoned;
                AgentCheckpointOutcome::Abandoned
            }
        }
    }

    fn record_decision(
        &mut self,
        decision: AgentCheckpointDecision,
        decision_key: AgentOperationId,
        resolver: PrincipalRef,
        now: AgentTimestampMillis,
    ) {
        if self.applied_keys.len() >= AGENT_CHECKPOINT_APPLIED_KEY_CAPACITY {
            self.applied_keys.pop_front();
        }
        self.applied_keys.push_back(decision_key.clone());
        // The escalation step is nonterminal, so it is not the resolving
        // decision; every other decision is.
        if !matches!(
            decision,
            AgentCheckpointDecision::Reconciliation(AgentReconciliationDecision::Escalate)
        ) {
            self.resolved_by = Some(resolver.clone());
        }
        self.decision = Some(AgentRecordedDecision {
            decision,
            decision_key,
            resolver,
            resolved_at: now,
        });
        self.updated_at = now;
    }

    /// Reconstructs the outcome a recorded terminal decision produced, for a
    /// deduplicated replay.
    fn recorded_outcome(&self) -> AgentCheckpointResult<AgentCheckpointOutcome> {
        match self.status {
            AgentCheckpointStatus::Denied => {
                let reason = match self.decision.as_ref().map(|recorded| &recorded.decision) {
                    Some(AgentCheckpointDecision::Approval(AgentApprovalDecision::Deny {
                        reason,
                    })) => reason.clone(),
                    _ => String::new(),
                };
                Ok(AgentCheckpointOutcome::Denied { reason })
            }
            AgentCheckpointStatus::Resolved => match self.decision.as_ref().map(|r| &r.decision) {
                Some(AgentCheckpointDecision::Approval(AgentApprovalDecision::Approve {
                    ..
                })) => {
                    // The grant is re-derivable from the recorded decision.
                    self.regrant()
                }
                Some(AgentCheckpointDecision::Reconciliation(reconciliation)) => {
                    let resolution = reconciliation.effect_resolution().ok_or(
                        AgentCheckpointError::InvalidDecision {
                            message: "a resolved reconciliation must map to an effect resolution"
                                .to_string(),
                        },
                    )?;
                    Ok(AgentCheckpointOutcome::EffectResolution(Box::new(
                        resolution,
                    )))
                }
                _ => Err(AgentCheckpointError::InvalidDecision {
                    message: "the recorded decision does not match a resolved status".to_string(),
                }),
            },
            AgentCheckpointStatus::Compensated => {
                match self.decision.as_ref().map(|r| &r.decision) {
                    Some(AgentCheckpointDecision::Reconciliation(
                        AgentReconciliationDecision::Compensate { compensation },
                    )) => Ok(AgentCheckpointOutcome::Compensate {
                        compensation: compensation.clone(),
                    }),
                    _ => Err(AgentCheckpointError::InvalidDecision {
                        message: "the recorded decision does not match a compensated status"
                            .to_string(),
                    }),
                }
            }
            AgentCheckpointStatus::Abandoned => Ok(AgentCheckpointOutcome::Abandoned),
            AgentCheckpointStatus::Escalated => Ok(AgentCheckpointOutcome::Escalated),
            AgentCheckpointStatus::Expired => Ok(AgentCheckpointOutcome::Expired),
            AgentCheckpointStatus::Cancelled => Ok(AgentCheckpointOutcome::Cancelled),
            AgentCheckpointStatus::Open => Err(AgentCheckpointError::InvalidDecision {
                message: "an open checkpoint has no recorded outcome".to_string(),
            }),
        }
    }

    /// Re-derives the grant a recorded `Approve` decision produced.
    fn regrant(&self) -> AgentCheckpointResult<AgentCheckpointOutcome> {
        let Some(AgentRecordedDecision {
            decision:
                AgentCheckpointDecision::Approval(AgentApprovalDecision::Approve {
                    credential_binding,
                    expires_at,
                    allowed_use_count,
                }),
            resolver,
            resolved_at,
            ..
        }) = self.decision.as_ref()
        else {
            return Err(AgentCheckpointError::InvalidDecision {
                message: "a resolved approval has no recorded grant".to_string(),
            });
        };
        Ok(AgentCheckpointOutcome::Granted(Box::new(
            AgentCheckpointGrant {
                checkpoint_id: self.checkpoint_id.clone(),
                kind: self.kind,
                scope: self.scope.clone(),
                task: self.task.clone(),
                goal: self.goal.clone(),
                effect_id: self.bound_effect.effect_id.clone(),
                generation: self.bound_effect.generation,
                target: self.bound_effect.target.clone(),
                argument_digest: self.bound_effect.argument_digest.clone(),
                safety_class: self.bound_effect.safety_class,
                settings_revision: self.settings_revision,
                policy: self.policy.clone(),
                credential_binding: credential_binding
                    .clone()
                    .or_else(|| self.bound_effect.credential_binding.clone()),
                resolver: resolver.clone(),
                issued_at: *resolved_at,
                expires_at: *expires_at,
                allowed_use_count: *allowed_use_count,
            },
        )))
    }

    fn check_decision_kind(&self, decision: &AgentCheckpointDecision) -> AgentCheckpointResult<()> {
        let matches = match decision {
            AgentCheckpointDecision::Approval(_) => self.kind.is_approval_family(),
            AgentCheckpointDecision::Reconciliation(_) => {
                self.kind == AgentCheckpointKind::IndeterminateEffectReconciliation
            }
        };
        if matches {
            Ok(())
        } else {
            Err(AgentCheckpointError::DecisionKindMismatch { kind: self.kind })
        }
    }

    fn validate(&self) -> AgentCheckpointResult<()> {
        if self.summary.is_empty() {
            return Err(AgentCheckpointError::InvalidBinding {
                message: "a checkpoint requires a non-empty summary".to_string(),
            });
        }
        if !self
            .bound_effect
            .argument_digest
            .algorithm
            .is_cryptographic()
        {
            return Err(AgentCheckpointError::InvalidBinding {
                message: "a checkpoint must bind a cryptographic argument digest".to_string(),
            });
        }
        if let (Some(due_at), Some(expires_at)) = (self.due_at, self.expires_at) {
            if due_at.as_millis() > expires_at.as_millis() {
                return Err(AgentCheckpointError::InvalidBinding {
                    message: "a checkpoint's SLA deadline cannot fall after its expiration"
                        .to_string(),
                });
            }
        }
        Ok(())
    }
}

impl VersionedAgentRecord for AgentCheckpoint {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::Checkpoint;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// The default allowed decision set for a checkpoint kind.
fn default_decisions(kind: AgentCheckpointKind) -> Vec<AgentCheckpointDecisionOption> {
    match kind {
        AgentCheckpointKind::Approval | AgentCheckpointKind::SecurityAuthorization => vec![
            AgentCheckpointDecisionOption::new("approve", false),
            AgentCheckpointDecisionOption::new("deny", true),
        ],
        AgentCheckpointKind::IndeterminateEffectReconciliation => vec![
            AgentCheckpointDecisionOption::new("confirmed-completed", true),
            AgentCheckpointDecisionOption::new("confirmed-not-executed", true),
            AgentCheckpointDecisionOption::new("compensate", true),
            AgentCheckpointDecisionOption::new("escalate", true),
            AgentCheckpointDecisionOption::new("abandon-and-fail", true),
        ],
    }
}

/// Truncates a string to a bounded character length, on a character boundary.
fn bounded(value: impl Into<String>, max: usize) -> String {
    let value = value.into();
    if value.chars().count() <= max {
        value
    } else {
        value.chars().take(max).collect()
    }
}

/// Result type for checkpoint operations.
pub type AgentCheckpointResult<T> = Result<T, AgentCheckpointError>;

/// Why a checkpoint operation failed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentCheckpointError {
    /// The effect binding could not be formed.
    InvalidBinding {
        /// Human-readable detail.
        message: String,
    },
    /// The submitted decision does not fit the checkpoint kind.
    DecisionKindMismatch {
        /// The kind the checkpoint expects a decision for.
        kind: AgentCheckpointKind,
    },
    /// The decision was structurally invalid.
    InvalidDecision {
        /// Human-readable detail.
        message: String,
    },
    /// The checkpoint already reached a terminal status.
    AlreadyResolved {
        /// The terminal status.
        status: AgentCheckpointStatus,
    },
}

impl AgentCheckpointError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidBinding { .. } => "checkpoint-invalid-binding",
            Self::DecisionKindMismatch { .. } => "checkpoint-decision-kind-mismatch",
            Self::InvalidDecision { .. } => "checkpoint-invalid-decision",
            Self::AlreadyResolved { .. } => "checkpoint-already-resolved",
        }
    }
}

impl Display for AgentCheckpointError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBinding { message } | Self::InvalidDecision { message } => {
                f.write_str(message)
            }
            Self::DecisionKindMismatch { kind } => {
                write!(f, "the decision does not fit a {kind} checkpoint")
            }
            Self::AlreadyResolved { status } => {
                write!(f, "the checkpoint is already {status}")
            }
        }
    }
}

impl Error for AgentCheckpointError {}
