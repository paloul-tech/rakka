//! Autonomy admission.
//!
//! Owns [`AutonomyAdmissionDecision`]: the fail-closed check that decides
//! whether an unattended class of work may run at all. Admission is rechecked
//! when an update widens what a run may do, and the immediate-safety dimensions
//! are rechecked again at dispatch, so a settings change during a wait cannot
//! let an already-parked attempt through on stale terms.
//!
//! Specification: section 7.4.
//!
//! # Who decides, and who enforces
//!
//! [Specification 7.4](../../../docs/plans/rakka-agent/spec.md) splits this
//! cleanly: Rakka owns the admission contract, the durable decision, and the
//! enforcement points; the application owns policy authoring, the risk taxonomy,
//! and business or regulatory approval rules. So a decision is *authored*
//! outside Rakka — by whatever authorized evaluator the deployment runs — and
//! submitted to the agent entity as an ordinary deduplicated command, exactly
//! as a definition revision is.
//!
//! That would be a rubber stamp if Rakka recorded whatever it was handed, so it
//! does not. A decision has two halves, and both must hold:
//!
//! - **Attested** requirements ([`AgentAdmissionRequirement::is_attested`]) are
//!   the ones only policy can judge — that completion criteria are *measurable*,
//!   that operational inspection is authorized for the right people. The
//!   evaluator states it verified them, and its principal is on the record
//!   forever.
//! - **Verified** requirements are the ones the definition itself either
//!   satisfies or does not, and Rakka checks them against the definition being
//!   admitted ([`AutonomyAdmissionDecision::verify`]). An evaluator cannot
//!   attest an unbounded budget into a bounded one.
//!
//! An admission for [`AgentOperationClass::Interactive`] needs neither: a human
//! is in the loop of the session, which is what the class means.
//!
//! # Widening is derived, never tracked
//!
//! [Specification 7.4](../../../docs/plans/rakka-agent/spec.md) requires
//! re-admission "whenever an update may widen tools, peers, credentials,
//! environment/knowledge access, schedule, budgets, or other autonomy", and
//! permits reusing an admission for a narrowing update "only when policy proves
//! them monotonic".
//!
//! A decision therefore records the **envelope it admitted**, and
//! [`AutonomyAdmissionDecision::admits`] re-derives the answer at every use by
//! asking the slice 1.2 narrowing check whether what is being proposed now is a
//! narrowing of what was admitted then. Nothing tracks "was that update a
//! widening?", because a flag would have to be set correctly at every future
//! call site to stay true, and one missed site is an unadmitted widening that
//! runs. Deriving it fails closed by construction: an envelope the decision
//! cannot prove is a narrowing does not run, whatever path produced it.
//!
//! The envelope is not the whole of what admission verified, though. The
//! structural requirements Rakka checks ([`AutonomyAdmissionDecision::verify`])
//! reach outside the authority envelope — an approval, authorization, or
//! escalation policy is an [`AgentDefinition`] policy reference, not an envelope
//! entry, so the narrowing check never compares it. A republish that dropped one
//! of those would not widen
//! the envelope, so the envelope check alone would wave it through. Enforcement
//! therefore re-derives *both* halves against the definition now in force
//! ([`AutonomyAdmissionDecision::admits_definition`]): the envelope narrowing
//! *and* the structural requirements, trusting nothing from the revision the
//! decision was recorded against. A guarantee the definition no longer provides
//! is a guarantee the admission no longer carries.
//!
//! Settings narrow rather than widen — a revocation is the only authority
//! settings carry ([`crate::definition::AgentSettings`]) — so in this phase the
//! widening door is a definition publish. The check does not depend on that
//! being true: it re-derives against whatever effective definition it is handed.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_agent_workflow::{AgentTimestampMillis, PrincipalRef, StateSchemaVersion};
use serde::{Deserialize, Serialize};

use crate::definition::{
    AgentAuthorityEnvelope, AgentDefinition, AgentEnvelopeViolation, AgentOperationClass,
    AgentRevisionNumber,
};
use crate::schema::{
    AgentRecordKind, AgentSchemaError, VersionedAgentRecord,
    CURRENT_AGENT_ADMISSION_DECISION_SCHEMA_VERSION,
};

/// How many stable constraint strings one decision may carry.
pub const AGENT_ADMISSION_CONSTRAINT_CAPACITY: usize = 16;

/// Longest stable constraint or reason string one decision may carry, in bytes.
pub const AGENT_ADMISSION_DETAIL_MAX_LENGTH: usize = 256;

/// Result type for admission operations.
pub type AgentAdmissionResult<T> = Result<T, AgentAdmissionError>;

/// One thing an admission policy must verify before unattended execution is
/// admitted ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// The set is closed and stable: an admission decision names exactly which of
/// these it verified, so "admitted" is never a bare boolean an auditor has to
/// take on faith.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentAdmissionRequirement {
    /// Measurable completion, health, or progress criteria.
    CompletionCriteria,
    /// Bounded time, cost, iterations, model and tool calls, effects, and
    /// concurrency.
    BoundedBudgets,
    /// Cancellation, suspension, escalation, and recovery behavior.
    CancellationBehavior,
    /// Authorized operational inspection.
    OperationalInspection,
    /// Classified tool and effect safety with scoped capabilities.
    ClassifiedToolSafety,
    /// Credential bindings scoped to what the definition declares.
    ScopedCredentials,
    /// Approval or security-authorization policy for consequential operations.
    ApprovalPolicy,
    /// Indeterminate-effect reconciliation policy.
    ReconciliationPolicy,
}

impl AgentAdmissionRequirement {
    /// Every requirement an unattended class must satisfy.
    pub const ALL: [Self; 8] = [
        Self::CompletionCriteria,
        Self::BoundedBudgets,
        Self::CancellationBehavior,
        Self::OperationalInspection,
        Self::ClassifiedToolSafety,
        Self::ScopedCredentials,
        Self::ApprovalPolicy,
        Self::ReconciliationPolicy,
    ];

    /// Whether this requirement can only be attested by the evaluator.
    ///
    /// See the module documentation: an attested requirement is one whose
    /// judgement is not in the definition — whether criteria are *measurable*,
    /// whether inspection reaches the right people. The rest are verified
    /// against the definition itself, and no attestation substitutes for that.
    #[must_use]
    pub const fn is_attested(self) -> bool {
        matches!(
            self,
            Self::CompletionCriteria | Self::CancellationBehavior | Self::OperationalInspection
        )
    }

    /// Stable kebab-case label, used as a reason code.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::CompletionCriteria => "completion-criteria",
            Self::BoundedBudgets => "bounded-budgets",
            Self::CancellationBehavior => "cancellation-behavior",
            Self::OperationalInspection => "operational-inspection",
            Self::ClassifiedToolSafety => "classified-tool-safety",
            Self::ScopedCredentials => "scoped-credentials",
            Self::ApprovalPolicy => "approval-policy",
            Self::ReconciliationPolicy => "reconciliation-policy",
        }
    }
}

impl Display for AgentAdmissionRequirement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Who evaluated an admission
/// ([specification 7.4](../../../docs/plans/rakka-agent/spec.md): evaluator
/// principal or service).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentAdmissionEvaluator {
    /// An authenticated human or machine principal.
    Principal(PrincipalRef),
    /// An automated policy service, named by a stable, bounded identifier.
    Service(String),
}

impl Display for AgentAdmissionEvaluator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Principal(principal) => write!(
                f,
                "principal {}:{}",
                principal.principal_type, principal.principal_id
            ),
            Self::Service(service) => write!(f, "service {service}"),
        }
    }
}

/// The immutable record of one accepted admission
/// ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// It is immutable in the strongest sense the type system offers here: every
/// field is read-only after construction, and a change of any kind is a new
/// decision rather than an edit. That is what lets it be quoted in an audit
/// years later and still mean what it meant.
///
/// It carries no resolved credential and no secret material: a credential
/// *binding* is a logical reference, which is the only credential-shaped thing
/// that may ever be persisted
/// ([specification 16](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyAdmissionDecision {
    schema_version: StateSchemaVersion,
    operation_classes: BTreeSet<AgentOperationClass>,
    definition_revision: AgentRevisionNumber,
    settings_revision: AgentRevisionNumber,
    setup_revision: Option<AgentRevisionNumber>,
    admitted_envelope: Box<AgentAuthorityEnvelope>,
    evaluator: AgentAdmissionEvaluator,
    verified: BTreeSet<AgentAdmissionRequirement>,
    constraints: Vec<String>,
    created_at: AgentTimestampMillis,
    expires_at: Option<AgentTimestampMillis>,
}

impl AutonomyAdmissionDecision {
    /// Records an admission decision for the given classes.
    ///
    /// It fails closed rather than recording a decision that cannot mean what
    /// it says: an unattended class must name every requirement of
    /// [`AgentAdmissionRequirement::ALL`], and the envelope must be one this
    /// binary can bound.
    pub fn new(
        operation_classes: BTreeSet<AgentOperationClass>,
        definition_revision: AgentRevisionNumber,
        settings_revision: AgentRevisionNumber,
        admitted_envelope: AgentAuthorityEnvelope,
        evaluator: AgentAdmissionEvaluator,
        verified: BTreeSet<AgentAdmissionRequirement>,
        created_at: AgentTimestampMillis,
    ) -> AgentAdmissionResult<Self> {
        let decision = Self {
            schema_version: CURRENT_AGENT_ADMISSION_DECISION_SCHEMA_VERSION,
            operation_classes,
            definition_revision,
            settings_revision,
            setup_revision: None,
            admitted_envelope: Box::new(admitted_envelope),
            evaluator,
            verified,
            constraints: Vec::new(),
            created_at,
            expires_at: None,
        };
        decision.validate()?;
        Ok(decision)
    }

    /// Binds the decision to the run setup it admitted.
    #[must_use]
    pub const fn with_setup_revision(mut self, revision: AgentRevisionNumber) -> Self {
        self.setup_revision = Some(revision);
        self
    }

    /// Sets the instant after which the decision no longer admits anything.
    #[must_use]
    pub const fn with_expiry(mut self, expires_at: AgentTimestampMillis) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Adds a stable, bounded constraint the evaluator attached.
    ///
    /// The constraint is opaque to Rakka: the risk taxonomy is application
    /// policy. It is recorded so the decision explains itself.
    pub fn with_constraint(mut self, constraint: impl Into<String>) -> AgentAdmissionResult<Self> {
        self.constraints.push(constraint.into());
        self.validate()?;
        Ok(self)
    }

    /// The classes this decision admits.
    pub fn operation_classes(&self) -> impl Iterator<Item = AgentOperationClass> + '_ {
        self.operation_classes.iter().copied()
    }

    /// The definition revision it admitted.
    #[must_use]
    pub const fn definition_revision(&self) -> AgentRevisionNumber {
        self.definition_revision
    }

    /// The settings revision it admitted.
    #[must_use]
    pub const fn settings_revision(&self) -> AgentRevisionNumber {
        self.settings_revision
    }

    /// The run setup it admitted, when it was bound to one.
    #[must_use]
    pub const fn setup_revision(&self) -> Option<AgentRevisionNumber> {
        self.setup_revision
    }

    /// The envelope it admitted, against which a later one is proven a
    /// narrowing.
    #[must_use]
    pub fn admitted_envelope(&self) -> &AgentAuthorityEnvelope {
        &self.admitted_envelope
    }

    /// Who evaluated it.
    #[must_use]
    pub const fn evaluator(&self) -> &AgentAdmissionEvaluator {
        &self.evaluator
    }

    /// The requirements the evaluator verified.
    pub fn verified(&self) -> impl Iterator<Item = AgentAdmissionRequirement> + '_ {
        self.verified.iter().copied()
    }

    /// The stable constraints the evaluator attached.
    pub fn constraints(&self) -> impl Iterator<Item = &str> {
        self.constraints.iter().map(String::as_str)
    }

    /// When it was recorded.
    #[must_use]
    pub const fn created_at(&self) -> AgentTimestampMillis {
        self.created_at
    }

    /// When it expires, if it does.
    #[must_use]
    pub const fn expires_at(&self) -> Option<AgentTimestampMillis> {
        self.expires_at
    }

    /// Whether the decision has expired at `now`.
    ///
    /// A decision is valid *through* its expiry instant, like a dispatch grant:
    /// the boundary belongs to the thing that was granted.
    #[must_use]
    pub fn is_expired(&self, now: AgentTimestampMillis) -> bool {
        self.expires_at
            .is_some_and(|expiry| now.as_millis() > expiry.as_millis())
    }

    /// Whether this decision admits running `class` under `envelope` at `now`,
    /// or why it does not.
    ///
    /// This checks the immediate-safety half of
    /// [specification 7.4](../../../docs/plans/rakka-agent/spec.md): expiry,
    /// class, and the envelope narrowing, derived rather than trusting a
    /// recorded answer (see the module documentation). It is what a later
    /// dispatch-time recheck has on hand.
    ///
    /// Admission *enforcement* must also re-derive the structural requirements
    /// against the definition now in force — that is [`Self::admits_definition`].
    /// A policy the definition has since dropped is not part of `envelope`, so it
    /// would slip past this check alone.
    pub fn admits(
        &self,
        class: AgentOperationClass,
        envelope: &AgentAuthorityEnvelope,
        now: AgentTimestampMillis,
    ) -> Result<(), AgentAdmissionRefusal> {
        if self.is_expired(now) {
            return Err(AgentAdmissionRefusal::Expired {
                expired_at: self.expires_at.unwrap_or(self.created_at),
            });
        }
        if !self.operation_classes.contains(&class) {
            return Err(AgentAdmissionRefusal::ClassNotAdmitted { class });
        }
        let violations = self.admitted_envelope.narrowing_violations(envelope);
        if !violations.is_empty() {
            return Err(AgentAdmissionRefusal::Widened { violations });
        }
        Ok(())
    }

    /// Whether this decision still admits running `class` against `definition`
    /// as it now stands, or why it does not.
    ///
    /// This is the admission enforcement point of
    /// [specification 7.4](../../../docs/plans/rakka-agent/spec.md). It
    /// re-derives *both* halves of the decision against the current definition,
    /// trusting nothing from the revision the decision was recorded against:
    ///
    /// - the envelope narrowing of [`Self::admits`] — a definition that widened
    ///   its authority stops here; and
    /// - the structural requirements of [`Self::verify`] — a definition that has
    ///   since dropped a required approval, authorization, or escalation policy,
    ///   or left a declared tool unscoped, stops here too, even though none of
    ///   those is part of the authority envelope the narrowing check compares.
    ///
    /// Deriving both at every use is what makes a stale admission fail closed: a
    /// republish that removed a verified guarantee cannot be reused, and no call
    /// site has to remember to retract the decision when the definition changes.
    pub fn admits_definition(
        &self,
        class: AgentOperationClass,
        definition: &AgentDefinition,
        now: AgentTimestampMillis,
    ) -> Result<(), AgentAdmissionRefusal> {
        self.admits(class, &definition.envelope, now)?;
        if let Some((requirement, detail)) = self.first_unmet_requirement(definition) {
            return Err(AgentAdmissionRefusal::RequirementRegressed {
                requirement,
                detail,
            });
        }
        Ok(())
    }

    /// Verifies the decision against the definition it claims to admit.
    ///
    /// This is Rakka's half of the split in the module documentation: an
    /// evaluator's attestation is taken on trust, and everything the definition
    /// itself answers is not.
    pub fn verify(&self, definition: &AgentDefinition) -> AgentAdmissionResult<()> {
        match self.first_unmet_requirement(definition) {
            Some((requirement, detail)) => Err(AgentAdmissionError::RequirementUnmet {
                requirement,
                detail,
            }),
            None => Ok(()),
        }
    }

    /// The first structural requirement the current `definition` fails to
    /// satisfy, when this decision admits an unattended class.
    ///
    /// Attested requirements are the evaluator's to judge and are not
    /// re-derivable from the definition, so they are skipped; everything the
    /// definition itself answers is checked here, against whatever revision is
    /// now in force. This is shared by [`Self::verify`] (recording time) and
    /// [`Self::admits_definition`] (enforcement time) so the two never drift.
    fn first_unmet_requirement(
        &self,
        definition: &AgentDefinition,
    ) -> Option<(AgentAdmissionRequirement, String)> {
        // No unattended class means no structural requirement to check.
        self.unattended_classes().next()?;
        for requirement in AgentAdmissionRequirement::ALL {
            if requirement.is_attested() {
                continue;
            }
            if let Some(detail) = unmet_structural_requirement(requirement, definition) {
                return Some((requirement, detail));
            }
        }
        None
    }

    fn unattended_classes(&self) -> impl Iterator<Item = AgentOperationClass> + '_ {
        self.operation_classes
            .iter()
            .copied()
            .filter(|class| !matches!(class, AgentOperationClass::Interactive))
    }

    fn validate(&self) -> AgentAdmissionResult<()> {
        if self.operation_classes.is_empty() {
            return Err(AgentAdmissionError::NoOperationClass);
        }
        if self.constraints.len() > AGENT_ADMISSION_CONSTRAINT_CAPACITY {
            return Err(AgentAdmissionError::TooManyConstraints {
                count: self.constraints.len(),
                capacity: AGENT_ADMISSION_CONSTRAINT_CAPACITY,
            });
        }
        for constraint in &self.constraints {
            if constraint.len() > AGENT_ADMISSION_DETAIL_MAX_LENGTH {
                return Err(AgentAdmissionError::ConstraintTooLong {
                    length: constraint.len(),
                    maximum: AGENT_ADMISSION_DETAIL_MAX_LENGTH,
                });
            }
        }
        if let AgentAdmissionEvaluator::Service(service) = &self.evaluator {
            // "A stable, bounded identifier" is the promise the variant makes,
            // and a durable record enforces the promise itself rather than
            // trusting whoever authored the decision.
            if service.len() > AGENT_ADMISSION_DETAIL_MAX_LENGTH {
                return Err(AgentAdmissionError::DetailTooLong {
                    field: "evaluator service name",
                    length: service.len(),
                    maximum: AGENT_ADMISSION_DETAIL_MAX_LENGTH,
                });
            }
        }
        if self.unattended_classes().next().is_some() {
            // The fail-closed rule of specification 7.4, at the record itself:
            // an unattended class that does not name every requirement is not a
            // partial admission, it is not an admission.
            for requirement in AgentAdmissionRequirement::ALL {
                if !self.verified.contains(&requirement) {
                    return Err(AgentAdmissionError::RequirementUnverified { requirement });
                }
            }
        }
        Ok(())
    }
}

impl VersionedAgentRecord for AutonomyAdmissionDecision {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::AdmissionDecision;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// The definition's answer to one structural requirement, when the answer is
/// no.
fn unmet_structural_requirement(
    requirement: AgentAdmissionRequirement,
    definition: &AgentDefinition,
) -> Option<String> {
    let envelope = &definition.envelope;
    match requirement {
        AgentAdmissionRequirement::BoundedBudgets => {
            let budgets = &envelope.budgets;
            let unbounded: Vec<&str> = [
                ("max_loop_iterations", budgets.max_loop_iterations.is_none()),
                ("max_model_calls", budgets.max_model_calls.is_none()),
                ("max_tool_calls", budgets.max_tool_calls.is_none()),
                ("max_effects", budgets.max_effects.is_none()),
                (
                    "max_effect_attempts",
                    budgets.max_effect_attempts.is_none(),
                ),
                ("max_tokens", budgets.max_tokens.is_none()),
                ("max_cost_micros", budgets.max_cost_micros.is_none()),
                (
                    "max_wall_clock_millis",
                    budgets.max_wall_clock_millis.is_none(),
                ),
                (
                    "max_concurrent_effects",
                    budgets.max_concurrent_effects.is_none(),
                ),
            ]
            .into_iter()
            .filter_map(|(name, unbounded)| unbounded.then_some(name))
            .collect();
            (!unbounded.is_empty()).then(|| {
                format!(
                    "unattended execution needs every budget bounded; these are not: {}",
                    unbounded.join(", ")
                )
            })
        }
        AgentAdmissionRequirement::ClassifiedToolSafety => {
            // A capability set is what scopes a tool's authority. A declared
            // tool with none is not "a tool that may do nothing" — the
            // capability check is what would refuse it — it is a tool whose
            // authority was never described, and unattended work must not run
            // one.
            let unscoped: Vec<String> = envelope
                .tools
                .iter()
                .filter(|(_, declaration)| declaration.capabilities.is_empty())
                .map(|(tool, _)| tool.to_string())
                .collect();
            (!unscoped.is_empty()).then(|| {
                format!(
                    "unattended execution needs every declared tool's capabilities scoped; these declare none: {}",
                    unscoped.join(", ")
                )
            })
        }
        AgentAdmissionRequirement::ScopedCredentials => {
            let unbound: Vec<String> = envelope
                .tools
                .iter()
                .filter_map(|(tool, declaration)| {
                    let binding = declaration.credential_binding.as_ref()?;
                    (!envelope.credential_bindings.contains(binding))
                        .then(|| format!("{tool} -> {binding}"))
                })
                .collect();
            (!unbound.is_empty()).then(|| {
                format!(
                    "a tool may only resolve a credential binding the envelope declares; these do not: {}",
                    unbound.join(", ")
                )
            })
        }
        AgentAdmissionRequirement::ApprovalPolicy => {
            let policies = &definition.policies;
            (policies.approval.is_none() || policies.authorization.is_none()).then(|| {
                "unattended execution needs both an approval and a security-authorization policy for consequential operations"
                    .to_string()
            })
        }
        AgentAdmissionRequirement::ReconciliationPolicy => definition
            .policies
            .escalation
            .is_none()
            .then(|| {
                "unattended execution needs an escalation policy: an indeterminate effect parks for a decision nobody is otherwise waiting to make"
                    .to_string()
            }),
        AgentAdmissionRequirement::CompletionCriteria
        | AgentAdmissionRequirement::CancellationBehavior
        | AgentAdmissionRequirement::OperationalInspection => None,
    }
}

/// Why an admission decision does not admit an operation.
///
/// It is a *refusal*, not an error: the decision is intact and the answer is
/// no. Every variant carries what an operator needs to fix it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentAdmissionRefusal {
    /// No admission decision exists at all. This is the fail-closed default:
    /// unattended work runs only against a decision that says it may
    /// ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
    Missing,
    /// The decision expired.
    Expired {
        /// When it expired.
        expired_at: AgentTimestampMillis,
    },
    /// The decision does not admit this operation class.
    ClassNotAdmitted {
        /// The class that was refused.
        class: AgentOperationClass,
    },
    /// The current authority widens what the decision admitted, so the decision
    /// cannot be reused for it.
    Widened {
        /// Every way the current authority exceeds the admitted one.
        violations: Vec<AgentEnvelopeViolation>,
    },
    /// The definition in force no longer satisfies a requirement the admission
    /// verified, so it can no longer be reused for this class — even though the
    /// authority envelope did not widen. A dropped approval, authorization, or
    /// escalation policy is the usual cause: those are not part of the envelope
    /// the narrowing check compares
    /// ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
    RequirementRegressed {
        /// The requirement the current definition no longer meets.
        requirement: AgentAdmissionRequirement,
        /// What the definition would have to restore.
        detail: String,
    },
}

impl AgentAdmissionRefusal {
    /// Stable machine-readable reason code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Missing => "admission-missing",
            Self::Expired { .. } => "admission-expired",
            Self::ClassNotAdmitted { .. } => "admission-class-not-admitted",
            Self::Widened { .. } => "admission-widened",
            Self::RequirementRegressed { .. } => "admission-requirement-regressed",
        }
    }
}

impl Display for AgentAdmissionRefusal {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => f.write_str("no autonomy admission decision exists for this agent"),
            Self::Expired { expired_at } => write!(
                f,
                "the autonomy admission decision expired at {}",
                expired_at.as_millis()
            ),
            Self::ClassNotAdmitted { class } => {
                write!(f, "the autonomy admission decision does not admit {class}")
            }
            Self::Widened { violations } => {
                write!(
                    f,
                    "the current authority widens what was admitted ({} violations): ",
                    violations.len()
                )?;
                for (index, violation) in violations.iter().enumerate() {
                    if index > 0 {
                        f.write_str("; ")?;
                    }
                    Display::fmt(violation, f)?;
                }
                Ok(())
            }
            Self::RequirementRegressed {
                requirement,
                detail,
            } => write!(
                f,
                "the definition in force no longer satisfies {requirement}: {detail}"
            ),
        }
    }
}

/// Why an admission decision could not be recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentAdmissionError {
    /// The decision admits no operation class at all.
    NoOperationClass,
    /// An unattended class was admitted without naming a requirement.
    RequirementUnverified {
        /// The requirement the decision does not name.
        requirement: AgentAdmissionRequirement,
    },
    /// A requirement the definition itself does not satisfy was attested.
    RequirementUnmet {
        /// The requirement that is not met.
        requirement: AgentAdmissionRequirement,
        /// What the definition would have to change.
        detail: String,
    },
    /// The decision carries more constraints than one record may hold.
    TooManyConstraints {
        /// How many it carries.
        count: usize,
        /// The bound in force.
        capacity: usize,
    },
    /// A constraint string exceeds the bound.
    ConstraintTooLong {
        /// Its length.
        length: usize,
        /// The bound in force.
        maximum: usize,
    },
    /// A free-text field exceeds the bound one durable record may carry.
    DetailTooLong {
        /// Which field crossed the bound.
        field: &'static str,
        /// Its length in bytes.
        length: usize,
        /// The bound in force.
        maximum: usize,
    },
    /// The decision record is not interpretable under the current schema
    /// policy.
    Schema(AgentSchemaError),
}

impl AgentAdmissionError {
    /// Stable machine-readable reason code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::NoOperationClass => "admission-no-operation-class",
            Self::RequirementUnverified { .. } => "admission-requirement-unverified",
            Self::RequirementUnmet { .. } => "admission-requirement-unmet",
            Self::TooManyConstraints { .. } => "admission-constraints-exceeded",
            Self::ConstraintTooLong { .. } => "admission-constraint-too-long",
            Self::DetailTooLong { .. } => "admission-detail-too-long",
            Self::Schema(error) => error.code(),
        }
    }
}

impl Display for AgentAdmissionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoOperationClass => {
                f.write_str("an admission decision must admit at least one operation class")
            }
            Self::RequirementUnverified { requirement } => write!(
                f,
                "unattended execution is not admitted without verifying {requirement}"
            ),
            Self::RequirementUnmet {
                requirement,
                detail,
            } => write!(f, "the requirement {requirement} is not met: {detail}"),
            Self::TooManyConstraints { count, capacity } => write!(
                f,
                "an admission decision carries {count} constraints, more than the {capacity} one record may hold"
            ),
            Self::ConstraintTooLong { length, maximum } => write!(
                f,
                "an admission constraint is {length} bytes, over the {maximum}-byte bound"
            ),
            Self::DetailTooLong {
                field,
                length,
                maximum,
            } => write!(
                f,
                "the {field} is {length} bytes, over the {maximum}-byte bound"
            ),
            Self::Schema(error) => Display::fmt(error, f),
        }
    }
}

impl Error for AgentAdmissionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Schema(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AgentSchemaError> for AgentAdmissionError {
    fn from(error: AgentSchemaError) -> Self {
        Self::Schema(error)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rakka_agent_workflow::AgentTimestampMillis;

    use super::*;
    use crate::definition::{
        AgentBudgetCeilings, AgentCapabilityId, AgentCredentialBindingRef, AgentDefinitionId,
        AgentEffectSafetyClass, AgentPolicyRef, AgentPolicyRefs, AgentToolDeclaration, AgentToolId,
    };

    fn now() -> AgentTimestampMillis {
        AgentTimestampMillis::new(1_752_451_200_000)
    }

    fn bounded_budgets() -> AgentBudgetCeilings {
        AgentBudgetCeilings {
            max_loop_iterations: Some(8),
            max_model_calls: Some(8),
            max_tool_calls: Some(8),
            max_effects: Some(8),
            max_effect_attempts: Some(16),
            max_tokens: Some(100_000),
            max_cost_micros: Some(1_000_000),
            max_wall_clock_millis: Some(600_000),
            max_concurrent_effects: Some(2),
        }
    }

    fn policy(name: &str) -> AgentPolicyRef {
        AgentPolicyRef::new(name).expect("a valid policy reference")
    }

    fn envelope() -> AgentAuthorityEnvelope {
        let binding = AgentCredentialBindingRef::new("crm-api").expect("a valid binding");
        let mut tools = BTreeMap::new();
        tools.insert(
            AgentToolId::new("lookup").expect("a valid tool id"),
            AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)
                .with_capability(AgentCapabilityId::new("read").expect("a valid capability"))
                .with_credential_binding(binding.clone()),
        );
        AgentAuthorityEnvelope {
            tools,
            credential_bindings: [binding].into_iter().collect(),
            budgets: bounded_budgets(),
            ..AgentAuthorityEnvelope::empty()
        }
    }

    fn definition(envelope: AgentAuthorityEnvelope) -> AgentDefinition {
        let mut definition = AgentDefinition::new(
            AgentDefinitionId::new("assistant").expect("a valid definition id"),
            "answers questions about accounts",
            envelope,
        )
        .expect("a valid definition");
        definition.policies = AgentPolicyRefs {
            approval: Some(policy("approval-v1")),
            authorization: Some(policy("authorization-v1")),
            escalation: Some(policy("escalation-v1")),
            guardrail: None,
            retention: None,
        };
        definition
    }

    fn evaluator() -> AgentAdmissionEvaluator {
        AgentAdmissionEvaluator::Service("risk-policy-service".to_string())
    }

    fn everything() -> BTreeSet<AgentAdmissionRequirement> {
        AgentAdmissionRequirement::ALL.into_iter().collect()
    }

    fn decision(envelope: AgentAuthorityEnvelope) -> AutonomyAdmissionDecision {
        AutonomyAdmissionDecision::new(
            [AgentOperationClass::BoundedAsync].into_iter().collect(),
            AgentRevisionNumber::INITIAL,
            AgentRevisionNumber::INITIAL,
            envelope,
            evaluator(),
            everything(),
            now(),
        )
        .expect("a complete admission")
    }

    #[test]
    fn an_unattended_class_is_not_admitted_without_every_requirement() {
        let mut incomplete = everything();
        incomplete.remove(&AgentAdmissionRequirement::ReconciliationPolicy);

        let error = AutonomyAdmissionDecision::new(
            [AgentOperationClass::Continuous].into_iter().collect(),
            AgentRevisionNumber::INITIAL,
            AgentRevisionNumber::INITIAL,
            envelope(),
            evaluator(),
            incomplete,
            now(),
        )
        .expect_err("an unattended class needs every requirement");

        assert_eq!(error.code(), "admission-requirement-unverified");
    }

    #[test]
    fn an_interactive_class_needs_no_requirements() {
        // A human is in the loop of the session: that is what the class means.
        AutonomyAdmissionDecision::new(
            [AgentOperationClass::Interactive].into_iter().collect(),
            AgentRevisionNumber::INITIAL,
            AgentRevisionNumber::INITIAL,
            AgentAuthorityEnvelope::empty(),
            evaluator(),
            BTreeSet::new(),
            now(),
        )
        .expect("interactive execution is attended");
    }

    #[test]
    fn an_evaluator_cannot_attest_an_unbounded_budget_into_a_bounded_one() {
        // The verified half of the split: what the definition answers, Rakka
        // checks.
        let unbounded = AgentAuthorityEnvelope {
            budgets: AgentBudgetCeilings {
                max_tokens: None,
                ..bounded_budgets()
            },
            ..envelope()
        };
        let decision = decision(unbounded.clone());

        let error = decision
            .verify(&definition(unbounded))
            .expect_err("the budget is not bounded, whatever the evaluator says");

        assert_eq!(error.code(), "admission-requirement-unmet");
        match error {
            AgentAdmissionError::RequirementUnmet {
                requirement,
                detail,
            } => {
                assert_eq!(requirement, AgentAdmissionRequirement::BoundedBudgets);
                assert!(detail.contains("max_tokens"), "{detail}");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn a_tool_may_not_resolve_a_credential_binding_the_envelope_never_declared() {
        let mut envelope = envelope();
        envelope.credential_bindings.clear();
        let decision = decision(envelope.clone());

        let error = decision
            .verify(&definition(envelope))
            .expect_err("the binding is not declared");
        match error {
            AgentAdmissionError::RequirementUnmet { requirement, .. } => {
                assert_eq!(requirement, AgentAdmissionRequirement::ScopedCredentials);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn unattended_execution_needs_an_approval_and_an_authorization_policy() {
        let mut definition = definition(envelope());
        definition.policies.authorization = None;

        let error = decision(envelope())
            .verify(&definition)
            .expect_err("consequential operations need a policy");
        match error {
            AgentAdmissionError::RequirementUnmet { requirement, .. } => {
                assert_eq!(requirement, AgentAdmissionRequirement::ApprovalPolicy);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn an_unbounded_evaluator_service_name_is_refused() {
        // The variant promises "a stable, bounded identifier", and the durable
        // record enforces the promise itself rather than trusting whoever
        // authored the decision.
        let error = AutonomyAdmissionDecision::new(
            [AgentOperationClass::BoundedAsync].into_iter().collect(),
            AgentRevisionNumber::INITIAL,
            AgentRevisionNumber::INITIAL,
            envelope(),
            AgentAdmissionEvaluator::Service("x".repeat(AGENT_ADMISSION_DETAIL_MAX_LENGTH + 1)),
            everything(),
            now(),
        )
        .expect_err("an unbounded service name must not enter a durable record");
        assert_eq!(error.code(), "admission-detail-too-long");
    }

    #[test]
    fn an_expired_decision_admits_nothing() {
        let decision = decision(envelope()).with_expiry(AgentTimestampMillis::new(
            now().as_millis().saturating_add(1_000),
        ));

        decision
            .admits(
                AgentOperationClass::BoundedAsync,
                &envelope(),
                AgentTimestampMillis::new(now().as_millis().saturating_add(1_000)),
            )
            .expect("a decision is valid through its expiry instant");

        let refusal = decision
            .admits(
                AgentOperationClass::BoundedAsync,
                &envelope(),
                AgentTimestampMillis::new(now().as_millis().saturating_add(1_001)),
            )
            .expect_err("the decision expired");
        assert_eq!(refusal.code(), "admission-expired");
    }

    #[test]
    fn a_widening_update_is_refused_and_a_narrowing_one_reuses_the_admission() {
        // The derived-widening rule: nothing recorded whether an update was a
        // widening. The answer comes from comparing what is proposed now with
        // what was admitted then.
        let decision = decision(envelope());

        let mut widened = envelope();
        widened.tools.insert(
            AgentToolId::new("refund").expect("a valid tool id"),
            AgentToolDeclaration::new(AgentEffectSafetyClass::NonIdempotent),
        );
        let refusal = decision
            .admits(AgentOperationClass::BoundedAsync, &widened, now())
            .expect_err("a tool nobody admitted");
        assert_eq!(refusal.code(), "admission-widened");

        let mut narrowed = envelope();
        narrowed.tools.clear();
        decision
            .admits(AgentOperationClass::BoundedAsync, &narrowed, now())
            .expect("a narrowing update reuses the admission");
    }

    #[test]
    fn a_decision_admits_only_the_classes_it_names() {
        let refusal = decision(envelope())
            .admits(AgentOperationClass::Continuous, &envelope(), now())
            .expect_err("continuous was never admitted");
        assert_eq!(refusal.code(), "admission-class-not-admitted");
    }

    #[test]
    fn a_republish_that_drops_a_required_policy_is_no_longer_admitted() {
        // The fail-open the envelope-only check missed: a policy is not part of
        // the authority envelope, so a republish that removes one is not a
        // widening the narrowing check can see. `admits_definition` re-derives
        // the structural requirements against the definition now in force.
        let decision = decision(envelope());

        let mut without_escalation = definition(envelope());
        without_escalation.policies.escalation = None;

        // The envelope did not change, so the envelope-only check still passes...
        decision
            .admits(AgentOperationClass::BoundedAsync, &envelope(), now())
            .expect("the envelope did not widen");

        // ...but the enforcement point refuses: the escalation policy the
        // admission verified is gone, and an unattended run needs it.
        let refusal = decision
            .admits_definition(
                AgentOperationClass::BoundedAsync,
                &without_escalation,
                now(),
            )
            .expect_err("the definition dropped a verified requirement");
        assert_eq!(refusal.code(), "admission-requirement-regressed");
        match refusal {
            AgentAdmissionRefusal::RequirementRegressed { requirement, .. } => {
                assert_eq!(requirement, AgentAdmissionRequirement::ReconciliationPolicy);
            }
            other => panic!("unexpected refusal: {other:?}"),
        }
    }

    #[test]
    fn a_narrowing_republish_that_keeps_its_policies_still_admits() {
        // The reuse the design intends: re-deriving structural requirements must
        // not refuse a narrowing update that left every verified guarantee in
        // place.
        let decision = decision(envelope());

        let mut narrowed = definition(envelope());
        narrowed.envelope.tools.clear();
        narrowed.envelope.credential_bindings.clear();

        decision
            .admits_definition(AgentOperationClass::BoundedAsync, &narrowed, now())
            .expect("a narrowing that keeps every verified requirement reuses the admission");
    }

    #[test]
    fn a_widening_republish_is_refused_before_the_requirement_check() {
        // Enforcement still reports a widened envelope as such: the immediate
        // safety check runs first, so an operator sees the authority violation
        // rather than a downstream requirement message.
        let decision = decision(envelope());

        let mut widened = definition(envelope());
        widened.envelope.tools.insert(
            AgentToolId::new("refund").expect("a valid tool id"),
            AgentToolDeclaration::new(AgentEffectSafetyClass::NonIdempotent),
        );

        let refusal = decision
            .admits_definition(AgentOperationClass::BoundedAsync, &widened, now())
            .expect_err("the envelope widened");
        assert_eq!(refusal.code(), "admission-widened");
    }
}
