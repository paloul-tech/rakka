//! The guardrail chain.
//!
//! Owns the versioned, ordered guardrail stages that run at the model, tool,
//! A2A, and memory boundaries
//! ([specification 16](../../../docs/plans/rakka-agent/spec.md)), their bounded
//! outcome set — allow, block, transform, report-only, require-checkpoint —
//! and the rule that a stage may transform deterministically or block, but
//! never introduce nondeterministic I/O into a durable transition. Stages a
//! deployment marks mandatory cannot be removed by an agent definition or a
//! run setup.
//!
//! Specification: section 16. Filled by slice 1.8.
//!
//! # Where the chain is evaluated today
//!
//! Slice 1.8 evaluates the chain at exactly two boundaries: the dispatch
//! authority runs [`AgentGuardrailBoundary::ToolRequest`] and
//! [`AgentGuardrailBoundary::ModelRequest`] before every attempt's durable
//! `Started`. The remaining boundaries — model/tool response, A2A
//! ingress/egress, memory ingress — are declared here so chains can be
//! configured for them, but their evaluation points arrive with the slices
//! that own those flows; a stage bound only to a not-yet-evaluated boundary
//! protects nothing yet.
//!
//! # Determinism is structural, not aspirational
//!
//! [`AgentGuardrail::evaluate`] is a synchronous function of the boundary and
//! the content: the signature has no way to await I/O, so a stage physically
//! cannot consult a service mid-transition. What the type system cannot prove —
//! that an implementation is a pure function for its recorded revision — is the
//! implementor's contract, and it is why every stage carries an explicit
//! [`AgentGuardrailStage::revision`]: a transform is only replayable because
//! the pair (revision, input) fixes the output. A retry of an effect whose
//! arguments a stage transformed re-derives the identical transformed input
//! from the identical durable intent under the identical chain revision — it
//! never re-evaluates against changed policy, because a changed policy is a
//! changed revision, the effect intent pins the chain revision it was
//! committed under, and the dispatch pipeline refuses an attempt whose
//! current chain no longer matches that pin.
//!
//! # What guardrails are not
//!
//! A guardrail is not authentication, capability authorization, typed result
//! validation, credential resolution, effect safety, or goal evaluation
//! ([specification 16](../../../docs/plans/rakka-agent/spec.md)). In
//! particular, [`AgentGuardrailOutcome::ReportOnly`] never grants a capability
//! and never makes a denied effect eligible: it is recorded and evaluation
//! continues, with the disposition unchanged.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::Arc;

use rakka_agent_workflow::ArtifactRef;
use serde::Serialize;
use serde_json::Value;

use crate::definition::{AgentGuardrailStageId, AgentPolicyRef, AgentRevisionNumber};

/// Most stages one guardrail chain may hold.
pub const AGENT_GUARDRAIL_MAX_STAGES: usize = 32;

/// Largest stable reason code a guardrail outcome may carry, in bytes.
///
/// A longer code is truncated deterministically rather than failing the
/// evaluation: the chain is a pure function and has no error channel that
/// would not itself become a policy decision.
pub const AGENT_GUARDRAIL_REASON_MAX_LENGTH: usize = 128;

/// Largest content a guardrail transform may produce, in bytes, at a boundary
/// that declares no tighter bound of its own.
///
/// This is the *default* ceiling [`AgentGuardrailChain::evaluate`] enforces.
/// A boundary whose downstream content is bounded tighter passes its own
/// bound through [`AgentGuardrailChain::evaluate_bounded`] — the tool-request
/// boundary, for example, enforces
/// [`crate::model::AGENT_TOOL_ARGUMENTS_MAX_BYTES`], because a transformed
/// call larger than that could never be executed anyway. A transform that
/// exceeds the effective bound is treated as a block with the stable reason
/// code `guardrail-transform-oversized` — fail closed, deterministically.
pub const AGENT_GUARDRAIL_CONTENT_MAX_BYTES: usize = 8 * 1024;

/// Result type for guardrail chain construction.
pub type AgentGuardrailResult<T> = Result<T, AgentGuardrailError>;

/// The boundary a guardrail stage runs at
/// ([specification 16](../../../docs/plans/rakka-agent/spec.md): "A2A
/// ingress/egress, retrieval/memory ingress, model request/response, and tool
/// request/response boundaries").
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentGuardrailBoundary {
    /// Before a model request is dispatched.
    ModelRequest,
    /// After a model response is received, before the loop acts on it.
    ModelResponse,
    /// Before a tool request is dispatched.
    ToolRequest,
    /// After a tool response is received, before the loop acts on it.
    ToolResponse,
    /// Before an A2A message is accepted.
    A2aIngress,
    /// Before an A2A message is sent.
    A2aEgress,
    /// Before retrieved memory enters a model context.
    MemoryIngress,
}

impl AgentGuardrailBoundary {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::ModelRequest => "model-request",
            Self::ModelResponse => "model-response",
            Self::ToolRequest => "tool-request",
            Self::ToolResponse => "tool-response",
            Self::A2aIngress => "a2a-ingress",
            Self::A2aEgress => "a2a-egress",
            Self::MemoryIngress => "memory-ingress",
        }
    }
}

impl Display for AgentGuardrailBoundary {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// What one guardrail stage decided about one piece of boundary content
/// ([specification 16](../../../docs/plans/rakka-agent/spec.md): "a guardrail
/// outcome MUST be one of an explicit bounded set").
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentGuardrailOutcome {
    /// The content passes unchanged.
    Allow,
    /// The content must not cross the boundary.
    Block {
        /// Stable machine-readable reason code.
        reason_code: String,
        /// Protected evidence supporting the decision, when policy requires it.
        evidence: Option<ArtifactRef>,
    },
    /// The content is deterministically replaced before crossing the boundary.
    ///
    /// The replacement must be a pure function of the input for the stage's
    /// recorded revision, which is what makes a retried effect reuse the
    /// accepted transformed input rather than silently re-evaluating.
    Transform {
        /// The replacement content.
        content: Value,
        /// Stable machine-readable reason code.
        reason_code: String,
    },
    /// The content passes, and the finding is recorded.
    ///
    /// Report-only never grants a capability and never makes a denied effect
    /// eligible; it changes nothing but the report list.
    ReportOnly {
        /// Stable machine-readable reason code.
        reason_code: String,
        /// Protected evidence supporting the finding, when policy requires it.
        evidence: Option<ArtifactRef>,
    },
    /// The content may cross the boundary only under an explicit checkpoint
    /// grant ([specification 12](../../../docs/plans/rakka-agent/spec.md)).
    RequireCheckpoint {
        /// Stable machine-readable reason code.
        reason_code: String,
    },
}

/// One deterministic guardrail rule.
///
/// The signature is deliberately synchronous: a stage evaluates content it was
/// handed and nothing else, so it can run inside a durable transition and at
/// dispatch alike. An implementation must be a pure function of `(boundary,
/// content)` for the revision its stage records; a rule that needs I/O — an
/// external classifier, a moderation service — must instead be executed as an
/// explicit durable effect whose persisted outcome a later stage reads.
pub trait AgentGuardrail: Send + Sync {
    /// Evaluates one piece of boundary content.
    fn evaluate(&self, boundary: AgentGuardrailBoundary, content: &Value) -> AgentGuardrailOutcome;
}

/// One ordered, versioned stage of the guardrail chain.
#[derive(Clone)]
pub struct AgentGuardrailStage {
    id: AgentGuardrailStageId,
    revision: AgentRevisionNumber,
    boundaries: BTreeSet<AgentGuardrailBoundary>,
    mandatory: bool,
    rule: Arc<dyn AgentGuardrail>,
}

impl AgentGuardrailStage {
    /// Creates a stage that runs at no boundary until one is declared.
    #[must_use]
    pub fn new(
        id: AgentGuardrailStageId,
        revision: AgentRevisionNumber,
        rule: Arc<dyn AgentGuardrail>,
    ) -> Self {
        Self {
            id,
            revision,
            boundaries: BTreeSet::new(),
            mandatory: false,
            rule,
        }
    }

    /// Declares a boundary the stage runs at.
    #[must_use]
    pub fn at_boundary(mut self, boundary: AgentGuardrailBoundary) -> Self {
        self.boundaries.insert(boundary);
        self
    }

    /// Marks the stage deployment-mandatory: no definition, setup, or settings
    /// update may remove it
    /// ([specification 16](../../../docs/plans/rakka-agent/spec.md)).
    #[must_use]
    pub const fn mandatory(mut self) -> Self {
        self.mandatory = true;
        self
    }

    /// Stable stage identity.
    #[must_use]
    pub const fn id(&self) -> &AgentGuardrailStageId {
        &self.id
    }

    /// The revision the stage's rule is deterministic under.
    #[must_use]
    pub const fn revision(&self) -> AgentRevisionNumber {
        self.revision
    }

    /// Whether the stage is deployment-mandatory.
    #[must_use]
    pub const fn is_mandatory(&self) -> bool {
        self.mandatory
    }

    /// Whether the stage runs at the given boundary.
    #[must_use]
    pub fn applies_at(&self, boundary: AgentGuardrailBoundary) -> bool {
        self.boundaries.contains(&boundary)
    }
}

impl Debug for AgentGuardrailStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentGuardrailStage")
            .field("id", &self.id)
            .field("revision", &self.revision)
            .field("boundaries", &self.boundaries)
            .field("mandatory", &self.mandatory)
            .finish_non_exhaustive()
    }
}

/// One recorded report-only finding.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AgentGuardrailReport {
    /// The stage that reported.
    pub stage: AgentGuardrailStageId,
    /// The revision the stage evaluated under.
    pub revision: AgentRevisionNumber,
    /// Stable machine-readable reason code.
    pub reason_code: String,
    /// Protected evidence supporting the finding, when policy requires it.
    pub evidence: Option<ArtifactRef>,
}

/// One applied transform, recorded so the reason a stage rewrote the content
/// is observable rather than discarded.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AgentGuardrailTransform {
    /// The stage that transformed.
    pub stage: AgentGuardrailStageId,
    /// The revision the stage evaluated under.
    pub revision: AgentRevisionNumber,
    /// Stable machine-readable reason code.
    pub reason_code: String,
}

/// The final disposition of one chain evaluation.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentGuardrailDisposition {
    /// Every stage allowed the content, transformed or not.
    Allowed,
    /// A stage blocked the content; nothing may cross the boundary.
    Blocked {
        /// The stage that blocked.
        stage: AgentGuardrailStageId,
        /// Stable machine-readable reason code.
        reason_code: String,
        /// Protected evidence supporting the block, when policy requires it.
        /// Boxed so the rare evidence-bearing block does not inflate every
        /// disposition.
        evidence: Option<Box<ArtifactRef>>,
    },
    /// The content may cross the boundary only under an explicit checkpoint
    /// grant. Until one exists, the effect stays undispatchable.
    CheckpointRequired {
        /// The first stage that required a checkpoint.
        stage: AgentGuardrailStageId,
        /// Stable machine-readable reason code.
        reason_code: String,
    },
}

/// What one ordered chain evaluation decided.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AgentGuardrailDecision {
    /// The chain revision the evaluation is deterministic under.
    pub chain_revision: AgentRevisionNumber,
    /// The final disposition.
    pub disposition: AgentGuardrailDisposition,
    /// The content after every accepted transform, in stage order.
    pub content: Value,
    /// Whether any stage transformed the content.
    pub transformed: bool,
    /// Every applied transform, in stage order, with its reason.
    pub transforms: Vec<AgentGuardrailTransform>,
    /// Every report-only finding, in stage order.
    pub reports: Vec<AgentGuardrailReport>,
}

impl AgentGuardrailDecision {
    /// Whether the content may cross the boundary.
    #[must_use]
    pub const fn is_allowed(&self) -> bool {
        matches!(self.disposition, AgentGuardrailDisposition::Allowed)
    }
}

/// The versioned, ordered guardrail chain one deployment configures
/// ([specification 16](../../../docs/plans/rakka-agent/spec.md)).
///
/// The chain is deployment-owned configuration: an agent definition or a run
/// setup can *require* stages (the envelope's mandatory set) and — through
/// [`AgentGuardrailChain::narrowed`] — disable optional ones, but there is no
/// operation by which either can remove a stage the deployment marked
/// mandatory. That absence is the enforcement.
#[derive(Clone)]
pub struct AgentGuardrailChain {
    revision: AgentRevisionNumber,
    policy: Option<AgentPolicyRef>,
    stages: Vec<AgentGuardrailStage>,
}

impl AgentGuardrailChain {
    /// An empty chain at the given revision.
    #[must_use]
    pub const fn new(revision: AgentRevisionNumber) -> Self {
        Self {
            revision,
            policy: None,
            stages: Vec::new(),
        }
    }

    /// Names the guardrail policy this chain implements, so an
    /// immediate-safety [`crate::definition::AgentSettingsChange::GuardrailPolicy`]
    /// selection can be checked against the chain actually deployed. A
    /// settings revision that requires a policy the chain does not carry
    /// refuses dispatch rather than running under the wrong one.
    #[must_use]
    pub fn with_policy_ref(mut self, policy: AgentPolicyRef) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Appends a stage, preserving order.
    ///
    /// A stage that declares no boundary is refused: it could never be
    /// evaluated, so accepting it would let a required stage satisfy the
    /// coverage check while protecting nothing.
    pub fn with_stage(mut self, stage: AgentGuardrailStage) -> AgentGuardrailResult<Self> {
        if self.stages.len() >= AGENT_GUARDRAIL_MAX_STAGES {
            return Err(AgentGuardrailError::TooManyStages {
                maximum: AGENT_GUARDRAIL_MAX_STAGES,
            });
        }
        if stage.boundaries.is_empty() {
            return Err(AgentGuardrailError::StageUnbound { stage: stage.id });
        }
        if self.stages.iter().any(|held| held.id == stage.id) {
            return Err(AgentGuardrailError::DuplicateStage { stage: stage.id });
        }
        self.stages.push(stage);
        Ok(self)
    }

    /// The revision the whole chain is deterministic under.
    #[must_use]
    pub const fn revision(&self) -> AgentRevisionNumber {
        self.revision
    }

    /// The guardrail policy this chain implements, when one is named.
    #[must_use]
    pub const fn policy_ref(&self) -> Option<&AgentPolicyRef> {
        self.policy.as_ref()
    }

    /// The stages, in evaluation order.
    #[must_use]
    pub fn stages(&self) -> &[AgentGuardrailStage] {
        &self.stages
    }

    /// Accepts that every required stage is present, or fails closed.
    ///
    /// This is the dispatch-time half of the mandatory-guardrail rule: an
    /// envelope that requires a stage the deployment's chain cannot run must
    /// not dispatch, because the alternative is silently running without a
    /// guardrail the definition promised.
    pub fn validate_covers(
        &self,
        required: &BTreeSet<AgentGuardrailStageId>,
    ) -> AgentGuardrailResult<()> {
        for stage in required {
            if !self.stages.iter().any(|held| &held.id == stage) {
                return Err(AgentGuardrailError::MissingRequiredStage {
                    stage: stage.clone(),
                });
            }
        }
        Ok(())
    }

    /// Produces the chain with the given optional stages disabled, refusing to
    /// remove a deployment-mandatory stage
    /// ([specification 16](../../../docs/plans/rakka-agent/spec.md):
    /// deployment policy may add mandatory guardrails that a definition,
    /// setup, model, or settings update must not remove or weaken).
    ///
    /// The narrowed chain carries its own revision, supplied by the caller: a
    /// different stage set is a different evaluation, and the recorded
    /// revision is what lets a grant, an audit, or a replay reconstruct which
    /// evaluation actually ran. Two narrowings that share a revision with each
    /// other — or with their parent — would make that reconstruction
    /// impossible, so a revision equal to the parent's is refused. The policy
    /// reference carries over: narrowing selects within the same deployment
    /// policy.
    pub fn narrowed(
        &self,
        disabled: &BTreeSet<AgentGuardrailStageId>,
        revision: AgentRevisionNumber,
    ) -> AgentGuardrailResult<Self> {
        if revision == self.revision {
            return Err(AgentGuardrailError::NarrowedRevisionNotDistinct { revision });
        }
        for stage in &self.stages {
            if stage.mandatory && disabled.contains(&stage.id) {
                return Err(AgentGuardrailError::MandatoryStageRemoved {
                    stage: stage.id.clone(),
                });
            }
        }
        Ok(Self {
            revision,
            policy: self.policy.clone(),
            stages: self
                .stages
                .iter()
                .filter(|stage| !disabled.contains(&stage.id))
                .cloned()
                .collect(),
        })
    }

    /// Evaluates the chain's stages for one boundary, in order, under the
    /// default transform-content ceiling
    /// ([`AGENT_GUARDRAIL_CONTENT_MAX_BYTES`]).
    ///
    /// The fold is deterministic: a block short-circuits and wins outright; a
    /// transform replaces the content every later stage sees; a checkpoint
    /// requirement is recorded and evaluation continues, so a later block
    /// still wins; report-only findings accumulate without changing anything
    /// else. An oversized transform is a deterministic block — fail closed —
    /// because a pure evaluator has no error channel that is not itself a
    /// policy decision.
    #[must_use]
    pub fn evaluate(
        &self,
        boundary: AgentGuardrailBoundary,
        content: &Value,
    ) -> AgentGuardrailDecision {
        self.evaluate_bounded(boundary, content, AGENT_GUARDRAIL_CONTENT_MAX_BYTES)
    }

    /// Evaluates the chain's stages for one boundary under an explicit
    /// transform-content ceiling.
    ///
    /// A boundary whose downstream content is bounded tighter than the chain
    /// default passes its own bound, so a transform that could never be
    /// executed is blocked here — deterministically, with the single stable
    /// reason code `guardrail-transform-oversized` — instead of surfacing a
    /// different failure depending on which layer caught it.
    #[must_use]
    pub fn evaluate_bounded(
        &self,
        boundary: AgentGuardrailBoundary,
        content: &Value,
        max_content_bytes: usize,
    ) -> AgentGuardrailDecision {
        let mut current = content.clone();
        let mut transformed = false;
        let mut transforms = Vec::new();
        let mut reports = Vec::new();
        let mut checkpoint: Option<(AgentGuardrailStageId, String)> = None;
        let max_content_bytes = max_content_bytes.min(AGENT_GUARDRAIL_CONTENT_MAX_BYTES);

        for stage in &self.stages {
            if !stage.applies_at(boundary) {
                continue;
            }
            match stage.rule.evaluate(boundary, &current) {
                AgentGuardrailOutcome::Allow => {}
                AgentGuardrailOutcome::Block {
                    reason_code,
                    evidence,
                } => {
                    return AgentGuardrailDecision {
                        chain_revision: self.revision,
                        disposition: AgentGuardrailDisposition::Blocked {
                            stage: stage.id.clone(),
                            reason_code: bounded_reason(reason_code),
                            evidence: evidence.map(Box::new),
                        },
                        content: current,
                        transformed,
                        transforms,
                        reports,
                    };
                }
                AgentGuardrailOutcome::Transform {
                    content: replacement,
                    reason_code,
                } => {
                    let bytes = serde_json::to_vec(&replacement)
                        .map(|encoded| encoded.len())
                        .unwrap_or(usize::MAX);
                    if bytes > max_content_bytes {
                        return AgentGuardrailDecision {
                            chain_revision: self.revision,
                            disposition: AgentGuardrailDisposition::Blocked {
                                stage: stage.id.clone(),
                                reason_code: "guardrail-transform-oversized".to_string(),
                                evidence: None,
                            },
                            content: current,
                            transformed,
                            transforms,
                            reports,
                        };
                    }
                    current = replacement;
                    transformed = true;
                    transforms.push(AgentGuardrailTransform {
                        stage: stage.id.clone(),
                        revision: stage.revision,
                        reason_code: bounded_reason(reason_code),
                    });
                }
                AgentGuardrailOutcome::ReportOnly {
                    reason_code,
                    evidence,
                } => {
                    reports.push(AgentGuardrailReport {
                        stage: stage.id.clone(),
                        revision: stage.revision,
                        reason_code: bounded_reason(reason_code),
                        evidence,
                    });
                }
                AgentGuardrailOutcome::RequireCheckpoint { reason_code } => {
                    if checkpoint.is_none() {
                        checkpoint = Some((stage.id.clone(), bounded_reason(reason_code)));
                    }
                }
            }
        }

        let disposition = match checkpoint {
            Some((stage, reason_code)) => {
                AgentGuardrailDisposition::CheckpointRequired { stage, reason_code }
            }
            None => AgentGuardrailDisposition::Allowed,
        };
        AgentGuardrailDecision {
            chain_revision: self.revision,
            disposition,
            content: current,
            transformed,
            transforms,
            reports,
        }
    }
}

impl Debug for AgentGuardrailChain {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentGuardrailChain")
            .field("revision", &self.revision)
            .field("policy", &self.policy)
            .field("stages", &self.stages)
            .finish()
    }
}

/// Truncates a reason code to its bounded length, deterministically.
fn bounded_reason(code: String) -> String {
    if code.len() <= AGENT_GUARDRAIL_REASON_MAX_LENGTH {
        return code;
    }
    let mut end = AGENT_GUARDRAIL_REASON_MAX_LENGTH;
    while !code.is_char_boundary(end) {
        end -= 1;
    }
    code[..end].to_string()
}

/// Rejection of a guardrail chain construction or coverage check.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentGuardrailError {
    /// The chain already holds as many stages as it may.
    TooManyStages {
        /// The maximum number of stages.
        maximum: usize,
    },
    /// A stage with the same identity is already in the chain.
    DuplicateStage {
        /// The duplicated stage.
        stage: AgentGuardrailStageId,
    },
    /// A narrowing tried to remove a deployment-mandatory stage.
    MandatoryStageRemoved {
        /// The stage the narrowing tried to remove.
        stage: AgentGuardrailStageId,
    },
    /// A required stage is not present in the chain.
    MissingRequiredStage {
        /// The stage the envelope or binding requires.
        stage: AgentGuardrailStageId,
    },
    /// A stage declared no boundary, so it could never be evaluated.
    StageUnbound {
        /// The stage that declared no boundary.
        stage: AgentGuardrailStageId,
    },
    /// A narrowing reused its parent chain's revision.
    NarrowedRevisionNotDistinct {
        /// The revision both chains would have shared.
        revision: AgentRevisionNumber,
    },
}

impl AgentGuardrailError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::TooManyStages { .. } => "guardrail-too-many-stages",
            Self::DuplicateStage { .. } => "guardrail-duplicate-stage",
            Self::MandatoryStageRemoved { .. } => "guardrail-mandatory-stage-immutable",
            Self::MissingRequiredStage { .. } => "guardrail-stage-missing",
            Self::StageUnbound { .. } => "guardrail-stage-unbound",
            Self::NarrowedRevisionNotDistinct { .. } => "guardrail-narrowed-revision-not-distinct",
        }
    }
}

impl Display for AgentGuardrailError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyStages { maximum } => {
                write!(f, "a guardrail chain may hold at most {maximum} stages")
            }
            Self::DuplicateStage { stage } => {
                write!(f, "the guardrail stage {stage} is already in the chain")
            }
            Self::MandatoryStageRemoved { stage } => write!(
                f,
                "the guardrail stage {stage} is deployment-mandatory and cannot be removed by a \
                 definition, setup, or settings update"
            ),
            Self::MissingRequiredStage { stage } => write!(
                f,
                "the required guardrail stage {stage} is not present in the deployment's chain"
            ),
            Self::StageUnbound { stage } => write!(
                f,
                "the guardrail stage {stage} declares no boundary and could never be evaluated"
            ),
            Self::NarrowedRevisionNotDistinct { revision } => write!(
                f,
                "a narrowed chain must carry a revision distinct from its parent's ({revision}): \
                 a different stage set is a different evaluation"
            ),
        }
    }
}

impl Error for AgentGuardrailError {}

#[cfg(test)]
mod tests {
    use super::*;

    struct ScriptedRule(AgentGuardrailOutcome);

    impl AgentGuardrail for ScriptedRule {
        fn evaluate(&self, _: AgentGuardrailBoundary, _: &Value) -> AgentGuardrailOutcome {
            self.0.clone()
        }
    }

    /// A deterministic transform: uppercases the "q" field.
    struct UppercaseQ;

    impl AgentGuardrail for UppercaseQ {
        fn evaluate(&self, _: AgentGuardrailBoundary, content: &Value) -> AgentGuardrailOutcome {
            let transformed = content
                .get("q")
                .and_then(Value::as_str)
                .map(str::to_uppercase);
            match transformed {
                Some(q) => AgentGuardrailOutcome::Transform {
                    content: serde_json::json!({ "q": q }),
                    reason_code: "normalized".to_string(),
                },
                None => AgentGuardrailOutcome::Allow,
            }
        }
    }

    fn stage_id(id: &str) -> AgentGuardrailStageId {
        AgentGuardrailStageId::new(id).expect("the stage id is valid")
    }

    fn stage(id: &str, outcome: AgentGuardrailOutcome) -> AgentGuardrailStage {
        AgentGuardrailStage::new(
            stage_id(id),
            AgentRevisionNumber::INITIAL,
            Arc::new(ScriptedRule(outcome)),
        )
        .at_boundary(AgentGuardrailBoundary::ToolRequest)
    }

    #[test]
    fn the_ordered_fold_composes_transforms_and_a_block_wins() {
        let chain = AgentGuardrailChain::new(AgentRevisionNumber::new(3))
            .with_stage(
                AgentGuardrailStage::new(
                    stage_id("normalize"),
                    AgentRevisionNumber::INITIAL,
                    Arc::new(UppercaseQ),
                )
                .at_boundary(AgentGuardrailBoundary::ToolRequest),
            )
            .expect("the stage registers")
            .with_stage(stage(
                "audit",
                AgentGuardrailOutcome::ReportOnly {
                    reason_code: "observed".to_string(),
                    evidence: None,
                },
            ))
            .expect("the stage registers");

        let content = serde_json::json!({ "q": "hello" });
        let decision = chain.evaluate(AgentGuardrailBoundary::ToolRequest, &content);
        assert!(decision.is_allowed());
        assert!(decision.transformed);
        assert_eq!(decision.content, serde_json::json!({ "q": "HELLO" }));
        assert_eq!(decision.reports.len(), 1);
        assert_eq!(decision.chain_revision, AgentRevisionNumber::new(3));

        // Determinism: the same revision and input produce the same decision.
        assert_eq!(
            decision,
            chain.evaluate(AgentGuardrailBoundary::ToolRequest, &content)
        );

        // A block anywhere in the order wins over everything after it.
        let blocking = chain
            .with_stage(stage(
                "deny",
                AgentGuardrailOutcome::Block {
                    reason_code: "denied-term".to_string(),
                    evidence: None,
                },
            ))
            .expect("the stage registers");
        let decision = blocking.evaluate(AgentGuardrailBoundary::ToolRequest, &content);
        assert!(matches!(
            decision.disposition,
            AgentGuardrailDisposition::Blocked { ref reason_code, .. }
                if reason_code == "denied-term"
        ));
    }

    #[test]
    fn report_only_never_changes_the_disposition() {
        let chain = AgentGuardrailChain::new(AgentRevisionNumber::INITIAL)
            .with_stage(stage(
                "checkpointer",
                AgentGuardrailOutcome::RequireCheckpoint {
                    reason_code: "consequential".to_string(),
                },
            ))
            .expect("the stage registers")
            .with_stage(stage(
                "reporter",
                AgentGuardrailOutcome::ReportOnly {
                    reason_code: "noted".to_string(),
                    evidence: None,
                },
            ))
            .expect("the stage registers");

        let decision = chain.evaluate(AgentGuardrailBoundary::ToolRequest, &serde_json::json!({}));
        // The checkpoint requirement stands; the report is recorded beside it,
        // granting nothing.
        assert!(matches!(
            decision.disposition,
            AgentGuardrailDisposition::CheckpointRequired { .. }
        ));
        assert_eq!(decision.reports.len(), 1);
    }

    #[test]
    fn a_mandatory_stage_cannot_be_narrowed_away_and_coverage_fails_closed() {
        let chain = AgentGuardrailChain::new(AgentRevisionNumber::INITIAL)
            .with_stage(stage("optional-stage", AgentGuardrailOutcome::Allow))
            .expect("the stage registers")
            .with_stage(stage("required-stage", AgentGuardrailOutcome::Allow).mandatory())
            .expect("the stage registers");

        // An optional stage narrows away, under a revision of its own; a
        // mandatory one refuses.
        let narrowed = chain
            .narrowed(
                &BTreeSet::from([stage_id("optional-stage")]),
                AgentRevisionNumber::new(2),
            )
            .expect("an optional stage may be disabled");
        assert_eq!(narrowed.stages().len(), 1);
        assert_eq!(narrowed.revision(), AgentRevisionNumber::new(2));
        let error = chain
            .narrowed(
                &BTreeSet::from([stage_id("required-stage")]),
                AgentRevisionNumber::new(2),
            )
            .expect_err("a mandatory stage may not be disabled");
        assert_eq!(error.code(), "guardrail-mandatory-stage-immutable");

        // A narrowed chain that reuses the parent revision would make the
        // recorded revision unable to say which evaluation ran.
        let error = chain
            .narrowed(
                &BTreeSet::from([stage_id("optional-stage")]),
                chain.revision(),
            )
            .expect_err("a narrowing must mint its own revision");
        assert_eq!(error.code(), "guardrail-narrowed-revision-not-distinct");

        // Coverage: a required stage missing from the chain fails closed.
        chain
            .validate_covers(&BTreeSet::from([stage_id("required-stage")]))
            .expect("a present stage satisfies coverage");
        let error = chain
            .validate_covers(&BTreeSet::from([stage_id("absent-stage")]))
            .expect_err("an absent stage fails coverage");
        assert_eq!(error.code(), "guardrail-stage-missing");
    }

    #[test]
    fn an_oversized_transform_is_a_deterministic_block() {
        let oversized = "x".repeat(AGENT_GUARDRAIL_CONTENT_MAX_BYTES + 1);
        let chain = AgentGuardrailChain::new(AgentRevisionNumber::INITIAL)
            .with_stage(stage(
                "inflator",
                AgentGuardrailOutcome::Transform {
                    content: Value::String(oversized),
                    reason_code: "inflate".to_string(),
                },
            ))
            .expect("the stage registers");

        let decision = chain.evaluate(AgentGuardrailBoundary::ToolRequest, &serde_json::json!({}));
        assert!(matches!(
            decision.disposition,
            AgentGuardrailDisposition::Blocked { ref reason_code, .. }
                if reason_code == "guardrail-transform-oversized"
        ));
    }

    #[test]
    fn a_bounded_evaluation_blocks_a_transform_the_boundary_could_never_carry() {
        // The transform fits the chain's default ceiling but not the tighter
        // bound the boundary declares: one stable code, wherever it is caught.
        let chain = AgentGuardrailChain::new(AgentRevisionNumber::INITIAL)
            .with_stage(stage(
                "inflator",
                AgentGuardrailOutcome::Transform {
                    content: Value::String("x".repeat(3 * 1024)),
                    reason_code: "inflate".to_string(),
                },
            ))
            .expect("the stage registers");

        let decision = chain.evaluate_bounded(
            AgentGuardrailBoundary::ToolRequest,
            &serde_json::json!({}),
            2 * 1024,
        );
        assert!(matches!(
            decision.disposition,
            AgentGuardrailDisposition::Blocked { ref reason_code, .. }
                if reason_code == "guardrail-transform-oversized"
        ));
    }

    #[test]
    fn a_block_keeps_its_evidence_and_a_transform_keeps_its_reason() {
        let evidence = ArtifactRef {
            artifact_id: "evidence-1".to_string(),
            kind: rakka_agent_workflow::ArtifactKind::File,
            uri: "s3://evidence/evidence-1".to_string(),
            checksum: Some("sha256:evidence-1".to_string()),
            content_type: Some("application/json".to_string()),
            byte_len: Some(12),
            retention_class: Some("protected".to_string()),
            encryption: None,
            redaction: rakka_agent_workflow::RedactionStatus::Unredacted,
            created_at: rakka_agent_workflow::AgentTimestampMillis::new(1),
            metadata: rakka_agent_workflow::AgentAttributes::default(),
        };
        let chain = AgentGuardrailChain::new(AgentRevisionNumber::INITIAL)
            .with_stage(stage(
                "deny",
                AgentGuardrailOutcome::Block {
                    reason_code: "denied-term".to_string(),
                    evidence: Some(evidence.clone()),
                },
            ))
            .expect("the stage registers");
        let decision = chain.evaluate(AgentGuardrailBoundary::ToolRequest, &serde_json::json!({}));
        assert!(matches!(
            decision.disposition,
            AgentGuardrailDisposition::Blocked { evidence: Some(ref held), .. }
                if held.as_ref() == &evidence
        ));

        let chain = AgentGuardrailChain::new(AgentRevisionNumber::INITIAL)
            .with_stage(
                AgentGuardrailStage::new(
                    stage_id("normalize"),
                    AgentRevisionNumber::new(4),
                    Arc::new(UppercaseQ),
                )
                .at_boundary(AgentGuardrailBoundary::ToolRequest),
            )
            .expect("the stage registers");
        let decision = chain.evaluate(
            AgentGuardrailBoundary::ToolRequest,
            &serde_json::json!({ "q": "hello" }),
        );
        assert_eq!(decision.transforms.len(), 1);
        assert_eq!(decision.transforms[0].stage, stage_id("normalize"));
        assert_eq!(decision.transforms[0].revision, AgentRevisionNumber::new(4));
        assert_eq!(decision.transforms[0].reason_code, "normalized");
    }

    #[test]
    fn a_stage_without_a_boundary_is_refused_at_registration() {
        // A boundary-less stage would satisfy presence-based coverage while
        // never being evaluated — the exact fail-open the refusal prevents.
        let unbound = AgentGuardrailStage::new(
            stage_id("pii-filter"),
            AgentRevisionNumber::INITIAL,
            Arc::new(ScriptedRule(AgentGuardrailOutcome::Allow)),
        );
        let error = AgentGuardrailChain::new(AgentRevisionNumber::INITIAL)
            .with_stage(unbound)
            .expect_err("a stage that runs at no boundary is refused");
        assert_eq!(error.code(), "guardrail-stage-unbound");
    }

    #[test]
    fn a_duplicate_stage_and_an_overfull_chain_are_refused() {
        let chain = AgentGuardrailChain::new(AgentRevisionNumber::INITIAL)
            .with_stage(stage("only-once", AgentGuardrailOutcome::Allow))
            .expect("the stage registers");
        let error = chain
            .clone()
            .with_stage(stage("only-once", AgentGuardrailOutcome::Allow))
            .expect_err("a duplicate stage is refused");
        assert_eq!(error.code(), "guardrail-duplicate-stage");

        let mut full = AgentGuardrailChain::new(AgentRevisionNumber::INITIAL);
        for index in 0..AGENT_GUARDRAIL_MAX_STAGES {
            full = full
                .with_stage(stage(
                    &format!("stage-{index}"),
                    AgentGuardrailOutcome::Allow,
                ))
                .expect("the stage registers");
        }
        let error = full
            .with_stage(stage("one-too-many", AgentGuardrailOutcome::Allow))
            .expect_err("an overfull chain is refused");
        assert_eq!(error.code(), "guardrail-too-many-stages");
    }
}
