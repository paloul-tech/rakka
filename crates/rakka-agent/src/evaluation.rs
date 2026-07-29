//! Progress, evidence, and completion.
//!
//! Owns the evaluator contract — deterministic assertions, authoritative
//! queries, verification workflows, an evaluator model under a distinct
//! policy, and human review — all executed as durable effects with a
//! persisted outcome, evidence references, and the criteria revision they were
//! judged against. A goal becomes `Satisfied` only through an evaluation of the
//! current criteria revision against durable evidence, never because a model
//! said so.
//!
//! The durable shape is [`AgentGoalEvaluationRecord`]: the run commits an
//! evaluation effect carrying an [`AgentGoalEvaluationRequest`], the dispatcher
//! executes it through an application-owned executor, and the completed record
//! crosses back to the coordinating root task as the goal-evaluation exchange,
//! where it becomes the [`crate::goal::AgentGoalEvaluationRef`] a criteria
//! decision rests on. The record carries references and stable codes only —
//! never model text, never hidden reasoning, never credential material.
//!
//! Also owns stagnation detection — repetition fingerprints and no-progress
//! epochs — through [`AgentGoalStagnationPolicy`]: deterministic thresholds the
//! wake controller accounts at every epoch settlement, feeding a continue,
//! replan, wait, escalate, or terminate action. Detection is disabled unless a
//! goal's spec opts in; `Replan` is typed for later slices and refused at spec
//! validation until one can execute it honestly.
//!
//! Specification: section 8.3. Filled by slice 4.2.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_agent_workflow::{
    AgentEffectId, AgentTimestampMillis, ArtifactRef, PrincipalRef, StateSchemaVersion,
};
use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer, Serialize};

use crate::definition::{
    AgentModelProfileId, AgentPolicyRef, AgentRevisionNumber, AgentWorkflowToolId,
};
use crate::effect::AgentEffectGeneration;
use crate::goal::{bounded_reason_text, AgentGoalEvaluationRef};
use crate::identity::AgentGoalId;
use crate::schema::{
    AgentRecordKind, VersionedAgentRecord, CURRENT_AGENT_GOAL_EVALUATION_SCHEMA_VERSION,
};
use crate::task::AgentContentDigest;

/// Result type for evaluation construction and validation.
pub type AgentGoalEvaluationResult<T> = Result<T, AgentGoalEvaluationError>;

/// Maximum evidence items one evaluation carries.
///
/// The evaluation's evidence rides the coordinating task's bounded record once
/// a decision persists it, so the count is capped below
/// [`crate::goal::AGENT_GOAL_MAX_ALLOWED_REFS`]: a spec whose
/// `required_evidence` demands more classes than one evaluation may present is
/// statically unsatisfiable and refused at spec validation.
pub const AGENT_GOAL_EVALUATION_MAX_EVIDENCE: usize = 16;

/// Default attempt ceiling of the goal-evaluation effect.
///
/// An evaluation is read-only — it judges evidence, it never mutates the world
/// — so a crash-retry is safe and a second attempt is cheap insurance against
/// a transient executor failure.
pub const AGENT_GOAL_EVALUATION_DEFAULT_MAX_ATTEMPTS: u32 = 2;

/// Evidence class of the authorized human decision a human-review evaluation
/// rests on.
///
/// The dispatcher appends exactly one item of this class from the approval
/// grant, which is why [`AgentGoalEvaluationMethod::evidence_reserve`] holds a
/// slot back for it.
pub const AGENT_GOAL_EVALUATION_HUMAN_DECISION_CLASS: &str = "human-decision";

/// Derives the identity of the evaluation one effect generation produced.
///
/// Pure per `(run, turn, slot, generation)`, so a crash-retry of the dispatch
/// or a replay of the transition that applied its outcome reconstructs the
/// same identity — and the exchange that carries the record derives its
/// operation id from this value.
pub fn goal_evaluation_record_id(
    scope: &crate::identity::AgentRunScope,
    turn: u64,
    slot: usize,
    generation: AgentEffectGeneration,
) -> Result<crate::identity::AgentOperationId, crate::identity::AgentIdentityError> {
    crate::identity::AgentOperationId::new(
        crate::identity::AgentOperationKind::GoalEvaluation,
        [
            scope.tenant().as_str(),
            scope.agent().as_str(),
            scope.run().as_str(),
            &format!("t{turn}"),
            &format!("s{slot}"),
            &format!("g{generation}"),
        ],
    )
}

/// The outcome one completed evaluation produced.
///
/// Deliberately two-valued: an evaluation that could not run to a verdict is a
/// *failed effect*, not an outcome — the goal stays `Active` and the caller
/// re-evaluates, rather than a half-verdict entering the durable record.
///
/// **Both values end the goal.** The decision door maps `Satisfied` to
/// [`crate::goal::AgentGoalTerminalReason::CriteriaSatisfied`] and
/// `NotSatisfied` to [`crate::goal::AgentGoalTerminalReason::CriteriaNotMet`],
/// so either verdict is terminal and absorbing — there is no "not yet" in this
/// enum. An evaluator that means *the criteria are not met so far, keep
/// working* must return
/// [`crate::dispatch::AgentGoalEvaluationFinding::Refused`] instead: that
/// fails the effect definitively, leaves the goal `Active` and decidable, and
/// lets the caller re-evaluate once the evidence has moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentGoalEvaluationOutcome {
    /// The current criteria revision is met by the presented evidence; the
    /// goal ends `Satisfied`.
    Satisfied,
    /// The current criteria revision is not met by the presented evidence and
    /// the evaluator says so conclusively; the goal ends `Unsatisfied`.
    ///
    /// This is a verdict, not a progress report — see the type's own note on
    /// what to return for "not yet".
    NotSatisfied,
}

impl AgentGoalEvaluationOutcome {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Satisfied => "satisfied",
            Self::NotSatisfied => "not-satisfied",
        }
    }
}

impl Display for AgentGoalEvaluationOutcome {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// How a goal evaluation is executed
/// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md),
/// technical guidance "Verify Progress and Completion").
///
/// All five kinds are typed from day one; `VerificationWorkflow` is refused at
/// effect commit until workflows-as-tools land in slice 4.5, because a
/// workflow invocation the autonomy classifier cannot class fails closed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentGoalEvaluationMethod {
    /// A deterministic assertion over artifacts or durable state.
    DeterministicAssertion {
        /// The application-owned assertion policy.
        assertion: AgentPolicyRef,
    },
    /// An authoritative environment or tool query.
    AuthoritativeQuery {
        /// The application-owned query policy.
        query: AgentPolicyRef,
    },
    /// A compiled verification workflow. Typed but refused until slice 4.5.
    VerificationWorkflow {
        /// The workflow tool that would verify the goal.
        workflow: AgentWorkflowToolId,
    },
    /// A separately configured evaluator model, resolved under its own
    /// profile: the request's pin is authoritative, and the agent's current
    /// settings profile never overrides it.
    EvaluatorModel {
        /// The evaluator's model profile, when pinned distinct from the
        /// worker's.
        profile: Option<AgentModelProfileId>,
    },
    /// An authorized human review, resolved through an approval checkpoint
    /// bound to the evaluation effect. The durable authorized decision is the
    /// evidence.
    HumanReview,
}

impl AgentGoalEvaluationMethod {
    /// How many evidence slots the dispatcher holds back for the items *it*
    /// appends when this method executes.
    ///
    /// Human review contributes the authorized decision itself as one classed
    /// [`AGENT_GOAL_EVALUATION_HUMAN_DECISION_CLASS`] item, so a request under
    /// that method may present at most
    /// [`AGENT_GOAL_EVALUATION_MAX_EVIDENCE`] `- 1` of its own. Reserving the
    /// slot at the commit door is what keeps the refusal *ahead* of the human:
    /// without it, a request that filled the whole bound would be approved
    /// first and only then fail to build its record, spending the grant for
    /// nothing.
    #[must_use]
    pub const fn evidence_reserve(&self) -> usize {
        match self {
            Self::HumanReview => 1,
            Self::DeterministicAssertion { .. }
            | Self::AuthoritativeQuery { .. }
            | Self::VerificationWorkflow { .. }
            | Self::EvaluatorModel { .. } => 0,
        }
    }

    /// The label-only kind of this method.
    #[must_use]
    pub const fn kind(&self) -> AgentGoalEvaluationMethodKind {
        match self {
            Self::DeterministicAssertion { .. } => {
                AgentGoalEvaluationMethodKind::DeterministicAssertion
            }
            Self::AuthoritativeQuery { .. } => AgentGoalEvaluationMethodKind::AuthoritativeQuery,
            Self::VerificationWorkflow { .. } => {
                AgentGoalEvaluationMethodKind::VerificationWorkflow
            }
            Self::EvaluatorModel { .. } => AgentGoalEvaluationMethodKind::EvaluatorModel,
            Self::HumanReview => AgentGoalEvaluationMethodKind::HumanReview,
        }
    }
}

/// The label-only kind of an [`AgentGoalEvaluationMethod`], carried by the
/// persisted record and its projections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentGoalEvaluationMethodKind {
    /// A deterministic assertion over artifacts or durable state.
    DeterministicAssertion,
    /// An authoritative environment or tool query.
    AuthoritativeQuery,
    /// A compiled verification workflow.
    VerificationWorkflow,
    /// A separately configured evaluator model.
    EvaluatorModel,
    /// An authorized human review.
    HumanReview,
}

impl AgentGoalEvaluationMethodKind {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::DeterministicAssertion => "deterministic-assertion",
            Self::AuthoritativeQuery => "authoritative-query",
            Self::VerificationWorkflow => "verification-workflow",
            Self::EvaluatorModel => "evaluator-model",
            Self::HumanReview => "human-review",
        }
    }
}

impl Display for AgentGoalEvaluationMethodKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// One classed evidence reference an evaluation rests on.
///
/// The class is what [`crate::goal::AgentGoalSpec::required_evidence`] is
/// checked against at the decision door; the artifact and digest are the
/// durable pointers, never the content itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGoalEvidenceRef {
    /// The evidence class, matched against the spec's required classes.
    pub class: String,
    /// The evidence artifact, when captured out of line.
    #[serde(default)]
    pub artifact: Option<ArtifactRef>,
    /// Content digest of the evidence, when fingerprinted.
    #[serde(default)]
    pub digest: Option<AgentContentDigest>,
}

impl AgentGoalEvidenceRef {
    /// Rejects an evidence reference that violates a bounded invariant.
    pub fn validate(&self) -> AgentGoalEvaluationResult<()> {
        if self.class.is_empty()
            || self.class.len() > crate::goal::AGENT_GOAL_EVIDENCE_CLASS_MAX_LENGTH
        {
            return Err(AgentGoalEvaluationError::EvidenceClassInvalid {
                length: self.class.len(),
                maximum: crate::goal::AGENT_GOAL_EVIDENCE_CLASS_MAX_LENGTH,
            });
        }
        Ok(())
    }
}

/// Validates one bounded evidence list, holding `reserve` slots back for the
/// items the dispatcher appends itself.
fn validate_evidence_with_reserve(
    evidence: &[AgentGoalEvidenceRef],
    reserve: usize,
) -> AgentGoalEvaluationResult<()> {
    let maximum = AGENT_GOAL_EVALUATION_MAX_EVIDENCE.saturating_sub(reserve);
    if evidence.len() > maximum {
        return Err(AgentGoalEvaluationError::EvidenceTooLarge {
            length: evidence.len(),
            maximum,
        });
    }
    for item in evidence {
        item.validate()?;
    }
    Ok(())
}

/// Validates one bounded evidence list against the whole bound: what a
/// completed record — appended items included — must fit inside.
fn validate_evidence(evidence: &[AgentGoalEvidenceRef]) -> AgentGoalEvaluationResult<()> {
    validate_evidence_with_reserve(evidence, 0)
}

/// What a committed goal-evaluation effect asks the executor to judge.
///
/// The criteria revision is what the caller read; the decision door re-fences
/// it authoritatively against the revision in force, so a request committed
/// against a revised goal completes, is refused stale, and is re-issued —
/// never silently re-interpreted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGoalEvaluationRequest {
    /// The goal under evaluation.
    pub goal: AgentGoalId,
    /// The evaluator this evaluation runs as; a configured spec evaluator is
    /// enforced at the decision door.
    pub evaluator: AgentPolicyRef,
    /// The criteria revision the caller read.
    pub criteria_revision: AgentRevisionNumber,
    /// How the evaluation executes.
    pub method: AgentGoalEvaluationMethod,
    /// Input evidence references; the executor may extend them.
    #[serde(default)]
    pub evidence: Vec<AgentGoalEvidenceRef>,
    /// Who asked for the evaluation — provenance, never authority.
    pub requested_by: PrincipalRef,
}

impl AgentGoalEvaluationRequest {
    /// Rejects a request that violates a bounded invariant.
    ///
    /// The evidence bound is checked against the room the *method* leaves: a
    /// request must fit inside [`AGENT_GOAL_EVALUATION_MAX_EVIDENCE`] once the
    /// dispatcher has appended its own items
    /// ([`AgentGoalEvaluationMethod::evidence_reserve`]), so the record it
    /// will build is already known to be constructible here.
    pub fn validate(&self) -> AgentGoalEvaluationResult<()> {
        validate_evidence_with_reserve(&self.evidence, self.method.evidence_reserve())
    }
}

/// The durable record of one completed goal evaluation
/// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// References and stable codes only: the record never carries model text,
/// hidden reasoning, tool payloads, or credential material. It is produced by
/// the dispatcher from an executor finding or an authorized human grant,
/// persisted in the run's own state, and carried whole across the
/// goal-evaluation exchange to the coordinating root task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentGoalEvaluationRecord {
    schema_version: StateSchemaVersion,
    /// The derived identity of this evaluation.
    pub evaluation_id: crate::identity::AgentOperationId,
    /// The goal the evaluation judged.
    pub goal: AgentGoalId,
    /// The evaluator the evaluation ran as.
    pub evaluator: AgentPolicyRef,
    /// How the evaluation executed.
    pub method: AgentGoalEvaluationMethodKind,
    /// The criteria revision the evaluation assessed.
    pub criteria_revision: AgentRevisionNumber,
    /// The verdict.
    pub outcome: AgentGoalEvaluationOutcome,
    /// Bounded, stable reason code for the verdict.
    pub reason_code: String,
    /// The classed evidence the verdict rests on.
    pub evidence: Vec<AgentGoalEvidenceRef>,
    /// The human resolver, when the method was an authorized review.
    #[serde(default)]
    pub evaluated_by: Option<PrincipalRef>,
    /// The durable effect that produced this record.
    pub effect_id: AgentEffectId,
    /// The effect generation that produced this record.
    pub generation: AgentEffectGeneration,
    /// When the evaluation completed.
    pub evaluated_at: AgentTimestampMillis,
}

impl AgentGoalEvaluationRecord {
    /// Creates a validated record, truncating the reason code to its bound.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        evaluation_id: crate::identity::AgentOperationId,
        goal: AgentGoalId,
        evaluator: AgentPolicyRef,
        method: AgentGoalEvaluationMethodKind,
        criteria_revision: AgentRevisionNumber,
        outcome: AgentGoalEvaluationOutcome,
        reason_code: impl Into<String>,
        evidence: Vec<AgentGoalEvidenceRef>,
        evaluated_by: Option<PrincipalRef>,
        effect_id: AgentEffectId,
        generation: AgentEffectGeneration,
        evaluated_at: AgentTimestampMillis,
    ) -> AgentGoalEvaluationResult<Self> {
        validate_evidence(&evidence)?;
        Ok(Self {
            schema_version: CURRENT_AGENT_GOAL_EVALUATION_SCHEMA_VERSION,
            evaluation_id,
            goal,
            evaluator,
            method,
            criteria_revision,
            outcome,
            reason_code: bounded_reason_text(reason_code.into()),
            evidence,
            evaluated_by,
            effect_id,
            generation,
            evaluated_at,
        })
    }

    /// Rejects a record that violates a bounded invariant.
    pub fn validate(&self) -> AgentGoalEvaluationResult<()> {
        if self.reason_code.len() > crate::goal::AGENT_GOAL_REASON_MAX_LENGTH {
            return Err(AgentGoalEvaluationError::ReasonCodeTooLong {
                length: self.reason_code.len(),
                maximum: crate::goal::AGENT_GOAL_REASON_MAX_LENGTH,
            });
        }
        validate_evidence(&self.evidence)
    }

    /// Cryptographic digest of this record, binding the decision's reference
    /// to exactly this content.
    ///
    /// The attestation digest follows the checkpoint-grant rule: it is the
    /// collision-resistant one, never the repetition fingerprint.
    #[must_use]
    pub fn attestation_digest(&self) -> AgentContentDigest {
        let value = serde_json::to_value(self).unwrap_or(serde_json::Value::Null);
        AgentContentDigest::sha256_of_json(&value)
    }

    /// Derives the [`AgentGoalEvaluationRef`] a criteria decision carries, so
    /// the coordinating task never hand-builds one.
    #[must_use]
    pub fn to_evaluation_ref(&self) -> AgentGoalEvaluationRef {
        AgentGoalEvaluationRef {
            evaluator: self.evaluator.clone(),
            criteria_revision: self.criteria_revision,
            evidence: None,
            digest: Some(self.attestation_digest()),
            evaluation_id: Some(self.evaluation_id.clone()),
            method: Some(self.method),
            evidence_items: self.evidence.clone(),
        }
    }
}

impl<'de> Deserialize<'de> for AgentGoalEvaluationRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Record {
            schema_version: StateSchemaVersion,
            evaluation_id: crate::identity::AgentOperationId,
            goal: AgentGoalId,
            evaluator: AgentPolicyRef,
            method: AgentGoalEvaluationMethodKind,
            criteria_revision: AgentRevisionNumber,
            outcome: AgentGoalEvaluationOutcome,
            reason_code: String,
            evidence: Vec<AgentGoalEvidenceRef>,
            #[serde(default)]
            evaluated_by: Option<PrincipalRef>,
            effect_id: AgentEffectId,
            generation: AgentEffectGeneration,
            evaluated_at: AgentTimestampMillis,
        }

        let record = Record::deserialize(deserializer)?;
        let record = Self {
            schema_version: record.schema_version,
            evaluation_id: record.evaluation_id,
            goal: record.goal,
            evaluator: record.evaluator,
            method: record.method,
            criteria_revision: record.criteria_revision,
            outcome: record.outcome,
            reason_code: record.reason_code,
            evidence: record.evidence,
            evaluated_by: record.evaluated_by,
            effect_id: record.effect_id,
            generation: record.generation,
            evaluated_at: record.evaluated_at,
        };
        record.validate().map_err(DeserializeError::custom)?;
        Ok(record)
    }
}

impl VersionedAgentRecord for AgentGoalEvaluationRecord {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::GoalEvaluation;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// Error raised by evaluation construction and validation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentGoalEvaluationError {
    /// The evidence list exceeds its bound.
    EvidenceTooLarge {
        /// Actual entry count.
        length: usize,
        /// The bound.
        maximum: usize,
    },
    /// An evidence class label is empty or exceeds its bound.
    EvidenceClassInvalid {
        /// Actual length in bytes.
        length: usize,
        /// The bound.
        maximum: usize,
    },
    /// A persisted reason code exceeds its bound.
    ReasonCodeTooLong {
        /// Actual length in bytes.
        length: usize,
        /// The bound.
        maximum: usize,
    },
}

impl AgentGoalEvaluationError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::EvidenceTooLarge { .. } => "evaluation-evidence-too-large",
            Self::EvidenceClassInvalid { .. } => "evaluation-evidence-class-invalid",
            Self::ReasonCodeTooLong { .. } => "evaluation-reason-code-too-long",
        }
    }
}

impl Display for AgentGoalEvaluationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EvidenceTooLarge { length, maximum } => write!(
                f,
                "the evaluation carries {length} evidence items, which exceeds the {maximum} item bound"
            ),
            Self::EvidenceClassInvalid { length, maximum } => write!(
                f,
                "an evidence class label of {length} bytes is empty or exceeds the {maximum} byte bound"
            ),
            Self::ReasonCodeTooLong { length, maximum } => write!(
                f,
                "the evaluation reason code is {length} bytes, which exceeds the {maximum} byte bound"
            ),
        }
    }
}

impl Error for AgentGoalEvaluationError {}

// ---------------------------------------------------------------------------
// Stagnation detection
// ---------------------------------------------------------------------------

/// Which deterministic stagnation condition tripped
/// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md): bounded
/// repetition and lack of material state change).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentStagnationTrigger {
    /// Consecutive completed epochs produced an identical result fingerprint.
    RepeatedResult,
    /// Consecutive completed epochs produced no new result fingerprint.
    NoProgress,
}

impl AgentStagnationTrigger {
    /// Stable kebab-case code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::RepeatedResult => "repeated-result",
            Self::NoProgress => "no-progress",
        }
    }
}

impl Display for AgentStagnationTrigger {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

/// What a tripped stagnation threshold does
/// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md): continue,
/// replan, wait, escalate, or terminate under deterministic policy).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentGoalStagnationAction {
    /// Record the detection and keep working: observe-only.
    Continue,
    /// Re-issue the work with a changed plan. Typed for later slices and
    /// refused at spec validation: nothing can re-issue an epoch with changed
    /// input yet, and mapping it to a park would silently change behavior when
    /// real replanning lands.
    Replan,
    /// Park the goal `Waiting` with the trigger on record; for a continuous
    /// goal, suspend epoch admission in the same transition.
    #[default]
    Wait,
    /// Park exactly as `Wait`, and record the escalation against the spec's
    /// escalation policy reference.
    Escalate,
    /// Terminate the goal `Failed` with the trigger as its terminal reason,
    /// and the root task with it.
    Terminate,
}

impl AgentGoalStagnationAction {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Continue => "continue",
            Self::Replan => "replan",
            Self::Wait => "wait",
            Self::Escalate => "escalate",
            Self::Terminate => "terminate",
        }
    }
}

impl Display for AgentGoalStagnationAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// The deterministic stagnation policy of one goal
/// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// The thresholds bound the wake controller's detector: `None` disables a
/// trigger, so the default policy detects nothing and a goal opts in through
/// its spec — the same posture as the wake policy's failure-escalation
/// threshold. The thresholds live inside the versioned
/// [`crate::goal::AgentGoalSpec`] so they revise with the contract they bound.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGoalStagnationPolicy {
    /// Consecutive completed epochs with an identical result fingerprint that
    /// trip [`AgentStagnationTrigger::RepeatedResult`]. At least 2 when set;
    /// `None` disables the trigger.
    #[serde(default)]
    pub repeated_result_epochs: Option<u32>,
    /// Consecutive completed epochs without a new result fingerprint that trip
    /// [`AgentStagnationTrigger::NoProgress`]. At least 1 when set; `None`
    /// disables the trigger.
    #[serde(default)]
    pub no_progress_epochs: Option<u32>,
    /// The action taken when no per-trigger override applies.
    #[serde(default)]
    pub default: AgentGoalStagnationAction,
    /// Per-trigger overrides.
    #[serde(default)]
    pub overrides: BTreeMap<AgentStagnationTrigger, AgentGoalStagnationAction>,
}

impl AgentGoalStagnationPolicy {
    /// Whether any trigger is enabled.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.repeated_result_epochs.is_some() || self.no_progress_epochs.is_some()
    }

    /// The action this policy takes for `trigger`.
    #[must_use]
    pub fn action_for(&self, trigger: AgentStagnationTrigger) -> AgentGoalStagnationAction {
        self.overrides
            .get(&trigger)
            .copied()
            .unwrap_or(self.default)
    }

    /// Whether the policy selects `action` for any trigger it could fire.
    #[must_use]
    pub fn selects(&self, action: AgentGoalStagnationAction) -> bool {
        self.default == action || self.overrides.values().any(|chosen| *chosen == action)
    }

    /// Which trigger, if any, the current streaks trip.
    ///
    /// Deterministic and single-valued: `RepeatedResult` is checked before
    /// `NoProgress`, so one settlement fires at most one trigger, and it trips
    /// exactly at the threshold, never before.
    #[must_use]
    pub fn tripped(&self, repeated: u32, no_progress: u32) -> Option<AgentStagnationTrigger> {
        if let Some(threshold) = self.repeated_result_epochs {
            if repeated >= threshold {
                return Some(AgentStagnationTrigger::RepeatedResult);
            }
        }
        if let Some(threshold) = self.no_progress_epochs {
            if no_progress >= threshold {
                return Some(AgentStagnationTrigger::NoProgress);
            }
        }
        None
    }
}
